//! #40: a full in-memory index (no data directory, so no spill index) used to panic the
//! request task on `put_object(..).unwrap()`, and the client saw a dropped connection.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, AwanStorageEngine, S3ServiceState, STREAM_PUT_MIN};
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn assert_retry_after_one(res: &reqwest::Response, label: &str) {
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE, "{}", label);
    let ra = res
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok());
    assert_eq!(ra, Some("1"), "{}: expected Retry-After: 1", label);
}

async fn send_stream_put_expect_503(addr: SocketAddr, path: &str, len: usize, label: &str) {
    let mut s = TcpStream::connect(addr).await.unwrap();
    let head = format!(
        "PUT {} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n",
        path, addr, len
    );
    s.write_all(head.as_bytes()).await.unwrap();
    s.flush().await.unwrap();
    let mut buf = vec![0u8; 4096];
    let n = tokio::time::timeout(Duration::from_secs(5), s.read(&mut buf))
        .await
        .expect("server answered streamed PUT pre-check")
        .unwrap();
    let resp = String::from_utf8_lossy(&buf[..n]);
    assert!(resp.starts_with("HTTP/1.1 503"), "{}: expected 503 without 100 Continue, got: {}", label, resp);
    assert!(resp.contains("<Code>SlowDown</Code>"), "{}: expected SlowDown XML, got: {}", label, resp);
    assert!(
        resp.lines().any(|l| l.trim_end_matches('\r').eq_ignore_ascii_case("retry-after: 1")),
        "{}: expected Retry-After: 1 header, got: {}",
        label,
        resp
    );
}

#[tokio::test]
async fn a_put_past_index_capacity_gets_503_and_the_server_keeps_serving() {
    let state = S3ServiceState::new();
    *state.engine.write() = AwanStorageEngine::new(3, 65536, 65536);
    let state = Arc::new(state);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();

    assert_eq!(c.put(format!("{}/full-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    for i in 0..3 {
        assert_eq!(c.put(format!("{}/full-bkt/k{}", url, i)).body("x").send().await.unwrap().status(), StatusCode::OK);
    }

    let upload_init = c.post(format!("{}/full-bkt/mp?uploads", url)).send().await.unwrap();
    assert_eq!(upload_init.status(), StatusCode::OK);
    let xml = upload_init.text().await.unwrap();
    let id = xml.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap().to_string();
    let part_res = c.put(format!("{}/full-bkt/mp?partNumber=1&uploadId={}", url, id)).body("part").send().await.unwrap();
    assert_eq!(part_res.status(), StatusCode::OK);
    let etag = part_res.headers().get("etag").unwrap().to_str().unwrap().to_string();
    let complete_body = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>", etag);

    let bytes_before = state.total_volume_bytes.load(Ordering::SeqCst);
    let offset_before = state.volume_manager.read().as_ref().unwrap().offset();

    let res = c.put(format!("{}/full-bkt/k3", url)).body("x").send().await.expect("an answer, not a dropped connection");
    assert_retry_after_one(&res, "buffered PutObject past capacity");
    assert!(res.text().await.unwrap().contains("SlowDown"));
    assert_eq!(c.get(format!("{}/full-bkt/k3", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    let stream_len = STREAM_PUT_MIN as usize + 4096;
    send_stream_put_expect_503(
        handle.local_addr,
        "/full-bkt/s3",
        stream_len,
        "streamed PutObject past capacity",
    )
    .await;
    assert_eq!(c.get(format!("{}/full-bkt/s3", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    let res = c
        .put(format!("{}/full-bkt/k4", url))
        .header("x-amz-copy-source", "/full-bkt/k0")
        .send()
        .await
        .expect("an answer to the copy");
    assert_retry_after_one(&res, "CopyObject past capacity");
    assert!(res.text().await.unwrap().contains("SlowDown"));
    assert_eq!(c.get(format!("{}/full-bkt/k4", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    let res = c
        .post(format!("{}/full-bkt/mp?uploadId={}", url, id))
        .body(complete_body)
        .send()
        .await
        .expect("an answer to the complete");
    assert_retry_after_one(&res, "CompleteMultipartUpload past capacity");
    assert!(res.text().await.unwrap().contains("SlowDown"));
    assert_eq!(c.get(format!("{}/full-bkt/mp", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    // Rejected requests wrote nothing to volumes and left upload_id active.
    assert_eq!(state.total_volume_bytes.load(Ordering::SeqCst), bytes_before);
    assert_eq!(state.volume_manager.read().as_ref().unwrap().offset(), offset_before);
    let list_parts = c.get(format!("{}/full-bkt/mp?uploadId={}", url, id)).send().await.unwrap();
    assert_eq!(list_parts.status(), StatusCode::OK, "uploadId must not be consumed on 503");
    assert!(list_parts.text().await.unwrap().contains("<PartNumber>1</PartNumber>"));
    assert_eq!(c.delete(format!("{}/full-bkt/mp?uploadId={}", url, id)).send().await.unwrap().status(), StatusCode::NO_CONTENT);

    // Metrics still export awanfs_memory_bodies_rejected_total.
    let metrics = c.get(format!("{}/_awanfs/metrics", url)).send().await.unwrap().text().await.unwrap();
    assert!(metrics.contains("awanfs_memory_bodies_rejected_total"), "metrics missing rejection counter: {}", metrics);

    // Still serving: existing objects read back and overwrites of indexed keys work.
    assert_eq!(c.get(format!("{}/full-bkt/k1", url)).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{}/full-bkt/k1", url)).body("y").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.get(format!("{}/full-bkt/k1", url)).send().await.unwrap().text().await.unwrap(), "y");

    handle.shutdown();
}

#[tokio::test]
async fn concurrent_slot_reservation_prevents_writes_and_preserves_multipart_for_retry() {
    let state = S3ServiceState::new();
    *state.engine.write() = AwanStorageEngine::new(3, 65536, 65536);
    let state = Arc::new(state);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();

    assert_eq!(c.put(format!("{}/race-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    for i in 0..2 {
        assert_eq!(c.put(format!("{}/race-bkt/k{}", url, i)).body("x").send().await.unwrap().status(), StatusCode::OK);
    }

    // Prepare a multipart upload while 1 index slot remains free.
    let init = c.post(format!("{}/race-bkt/mp?uploads", url)).send().await.unwrap().text().await.unwrap();
    let upload_id = init.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap().to_string();
    let part = c.put(format!("{}/race-bkt/mp?partNumber=1&uploadId={}", url, upload_id)).body("part-data").send().await.unwrap();
    assert_eq!(part.status(), StatusCode::OK);
    let etag = part.headers().get("etag").unwrap().to_str().unwrap().to_string();
    let complete_body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        etag
    );

    // Hold the 3rd (last) slot via an in-flight streamed PUT (> 4 MiB).
    let stream_len = STREAM_PUT_MIN as usize + 4096;
    let mut stalled = TcpStream::connect(handle.local_addr).await.unwrap();
    let head = format!(
        "PUT /race-bkt/stalled HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        handle.local_addr, stream_len
    );
    stalled.write_all(head.as_bytes()).await.unwrap();
    stalled.write_all(&[b'a'; 1024]).await.unwrap();
    stalled.flush().await.unwrap();

    let mut reserved = false;
    for _ in 0..200 {
        if state.volume_writers_in_flight(1) == Some(1) {
            reserved = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(reserved, "stalled streamed PUT did not reserve its needle");

    // While the 3rd slot is reserved, concurrent new-key writes and CompleteMultipartUpload reject with 503 Retry-After: 1.
    let res = c.put(format!("{}/race-bkt/buf", url)).body("z").send().await.unwrap();
    assert_retry_after_one(&res, "buffered PUT while last slot reserved");
    assert_eq!(c.get(format!("{}/race-bkt/buf", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    send_stream_put_expect_503(
        handle.local_addr,
        "/race-bkt/stream2",
        stream_len,
        "streamed PUT while last slot reserved",
    )
    .await;
    assert_eq!(c.get(format!("{}/race-bkt/stream2", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    let res = c
        .put(format!("{}/race-bkt/copy", url))
        .header("x-amz-copy-source", "/race-bkt/k0")
        .send()
        .await
        .unwrap();
    assert_retry_after_one(&res, "CopyObject while last slot reserved");
    assert_eq!(c.get(format!("{}/race-bkt/copy", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    let bytes_before_complete = state.total_volume_bytes.load(Ordering::SeqCst);
    let offset_before_complete = state.volume_manager.read().as_ref().unwrap().offset();
    let res = c
        .post(format!("{}/race-bkt/mp?uploadId={}", url, upload_id))
        .body(complete_body.clone())
        .send()
        .await
        .unwrap();
    assert_retry_after_one(&res, "CompleteMultipartUpload while last slot reserved");
    assert_eq!(c.get(format!("{}/race-bkt/mp", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);
    assert_eq!(state.total_volume_bytes.load(Ordering::SeqCst), bytes_before_complete);
    assert_eq!(state.volume_manager.read().as_ref().unwrap().offset(), offset_before_complete);

    // Abort the stalled streamed PUT, freeing its slot reservation.
    drop(stalled);
    let mut released = false;
    for _ in 0..200 {
        if state.streams.in_flight() == 0 {
            released = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(released, "abandoned streamed PUT did not release its stream/reservation");
    assert_eq!(c.get(format!("{}/race-bkt/stalled", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    // Retrying the exact same CompleteMultipartUpload now succeeds and serves the object.
    let retry = c
        .post(format!("{}/race-bkt/mp?uploadId={}", url, upload_id))
        .body(complete_body)
        .send()
        .await
        .unwrap();
    assert_eq!(retry.status(), StatusCode::OK);
    let got = c.get(format!("{}/race-bkt/mp", url)).send().await.unwrap();
    assert_eq!(got.status(), StatusCode::OK);
    assert_eq!(got.text().await.unwrap(), "part-data");

    handle.shutdown();
}

#[tokio::test]
async fn versioned_err_capacity_rejects_without_mutating_versions_or_uploads() {
    let state = S3ServiceState::new();
    *state.engine.write() = AwanStorageEngine::new(3, 65536, 65536);
    let state = Arc::new(state);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();

    assert_eq!(c.put(format!("{}/ver-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let ver_xml = "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>Enabled</Status></VersioningConfiguration>";
    assert_eq!(c.put(format!("{}/ver-bkt?versioning", url)).body(ver_xml).send().await.unwrap().status(), StatusCode::OK);

    for i in 0..3 {
        assert_eq!(c.put(format!("{}/ver-bkt/k{}", url, i)).body("v1").send().await.unwrap().status(), StatusCode::OK);
    }
    // Delete k0 in versioned bucket: adds a delete marker and drops k0 from key_index while row stays used.
    assert_eq!(c.delete(format!("{}/ver-bkt/k0", url)).send().await.unwrap().status(), StatusCode::NO_CONTENT);

    let init = c.post(format!("{}/ver-bkt/mp?uploads", url)).send().await.unwrap().text().await.unwrap();
    let upload_id = init.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap().to_string();
    let part = c.put(format!("{}/ver-bkt/mp?partNumber=1&uploadId={}", url, upload_id)).body("vpart").send().await.unwrap();
    assert_eq!(part.status(), StatusCode::OK);
    let etag = part.headers().get("etag").unwrap().to_str().unwrap().to_string();
    let complete_body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        etag
    );

    let versions_before = c.get(format!("{}/ver-bkt?versions", url)).send().await.unwrap().text().await.unwrap();
    let bytes_before = state.total_volume_bytes.load(Ordering::SeqCst);
    let offset_before = state.volume_manager.read().as_ref().unwrap().offset();

    let res = c.put(format!("{}/ver-bkt/k0", url)).body("v2").send().await.unwrap();
    assert_retry_after_one(&res, "versioned PutObject after delete marker at capacity");

    let res = c.put(format!("{}/ver-bkt/k3", url)).body("v1").send().await.unwrap();
    assert_retry_after_one(&res, "versioned new key PutObject at capacity");
    assert_eq!(c.get(format!("{}/ver-bkt/k3", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    send_stream_put_expect_503(
        handle.local_addr,
        "/ver-bkt/s3",
        STREAM_PUT_MIN as usize + 4096,
        "versioned streamed PutObject at capacity",
    )
    .await;
    assert_eq!(c.get(format!("{}/ver-bkt/s3", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    let res = c
        .put(format!("{}/ver-bkt/c3", url))
        .header("x-amz-copy-source", "/ver-bkt/k1")
        .send()
        .await
        .unwrap();
    assert_retry_after_one(&res, "versioned CopyObject at capacity");
    assert_eq!(c.get(format!("{}/ver-bkt/c3", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    let res = c
        .post(format!("{}/ver-bkt/mp?uploadId={}", url, upload_id))
        .body(complete_body)
        .send()
        .await
        .unwrap();
    assert_retry_after_one(&res, "versioned CompleteMultipartUpload at capacity");
    assert_eq!(c.get(format!("{}/ver-bkt/mp", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    assert_eq!(c.get(format!("{}/ver-bkt?versions", url)).send().await.unwrap().text().await.unwrap(), versions_before);
    assert_eq!(state.total_volume_bytes.load(Ordering::SeqCst), bytes_before);
    assert_eq!(state.volume_manager.read().as_ref().unwrap().offset(), offset_before);
    assert_eq!(c.get(format!("{}/ver-bkt/mp?uploadId={}", url, upload_id)).send().await.unwrap().status(), StatusCode::OK);

    // Overwriting an existing indexed key still creates a new version cleanly.
    let ok = c.put(format!("{}/ver-bkt/k1", url)).body("v2").send().await.unwrap();
    assert_eq!(ok.status(), StatusCode::OK);
    assert!(ok.headers().contains_key("x-amz-version-id"));
    assert_eq!(c.get(format!("{}/ver-bkt/k1", url)).send().await.unwrap().text().await.unwrap(), "v2");

    handle.shutdown();
}

#[tokio::test]
async fn versioned_complete_retry_and_midstream_delete_at_capacity_leave_no_object() {
    let state = S3ServiceState::new();
    *state.engine.write() = AwanStorageEngine::new(3, 65536, 65536);
    let state = Arc::new(state);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();

    assert_eq!(c.put(format!("{}/vretry", url)).send().await.unwrap().status(), StatusCode::OK);
    let ver_xml = "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>Enabled</Status></VersioningConfiguration>";
    assert_eq!(c.put(format!("{}/vretry?versioning", url)).body(ver_xml).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{}/vretry/k0", url)).body("a").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{}/vretry/k1", url)).body("b").send().await.unwrap().status(), StatusCode::OK);

    let init = c.post(format!("{}/vretry/mp?uploads", url)).send().await.unwrap().text().await.unwrap();
    let upload_id = init.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap().to_string();
    let part = c.put(format!("{}/vretry/mp?partNumber=1&uploadId={}", url, upload_id)).body("vp").send().await.unwrap();
    let etag = part.headers().get("etag").unwrap().to_str().unwrap().to_string();
    let complete_body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        etag
    );

    let stream_len = STREAM_PUT_MIN as usize + 4096;
    let mut stalled = TcpStream::connect(handle.local_addr).await.unwrap();
    let head = format!(
        "PUT /vretry/stalled HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        handle.local_addr, stream_len
    );
    stalled.write_all(head.as_bytes()).await.unwrap();
    stalled.write_all(&[b'x'; 1024]).await.unwrap();
    stalled.flush().await.unwrap();
    for _ in 0..200 {
        if state.volume_writers_in_flight(1) == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(state.volume_writers_in_flight(1), Some(1));

    let versions_before = c.get(format!("{}/vretry?versions", url)).send().await.unwrap().text().await.unwrap();
    let rej = c
        .post(format!("{}/vretry/mp?uploadId={}", url, upload_id))
        .body(complete_body.clone())
        .send()
        .await
        .unwrap();
    assert_retry_after_one(&rej, "versioned CompleteMultipartUpload while slot reserved");
    assert_eq!(c.get(format!("{}/vretry?versions", url)).send().await.unwrap().text().await.unwrap(), versions_before);

    drop(stalled);
    for _ in 0..200 {
        if state.streams.in_flight() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let ok = c
        .post(format!("{}/vretry/mp?uploadId={}", url, upload_id))
        .body(complete_body)
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), StatusCode::OK);
    assert!(ok.headers().contains_key("x-amz-version-id"));
    assert_eq!(c.get(format!("{}/vretry/mp", url)).send().await.unwrap().text().await.unwrap(), "vp");

    // Existing key deleted mid-stream on a full index is rejected at commit before indexing.
    let mut stalled_existing = TcpStream::connect(handle.local_addr).await.unwrap();
    let head2 = format!(
        "PUT /vretry/k0 HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        handle.local_addr, stream_len
    );
    stalled_existing.write_all(head2.as_bytes()).await.unwrap();
    stalled_existing.write_all(&[b'y'; 1024]).await.unwrap();
    stalled_existing.flush().await.unwrap();
    for _ in 0..200 {
        if state.volume_writers_in_flight(1) == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(c.delete(format!("{}/vretry/k0", url)).send().await.unwrap().status(), StatusCode::NO_CONTENT);
    stalled_existing.write_all(&vec![b'y'; stream_len - 1024]).await.unwrap();
    stalled_existing.flush().await.unwrap();
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), stalled_existing.read_to_end(&mut buf))
        .await
        .expect("server answered finished stream after delete")
        .unwrap();
    let resp = String::from_utf8_lossy(&buf);
    assert!(resp.starts_with("HTTP/1.1 503"), "expected 503 after midstream delete at capacity, got: {}", resp);
    assert!(resp.lines().any(|l| l.trim_end_matches('\r').eq_ignore_ascii_case("retry-after: 1")));
    assert_eq!(c.get(format!("{}/vretry/k0", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    handle.shutdown();
}
