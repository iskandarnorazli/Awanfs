use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState, PART_CHUNK, SECTOR_SIZE, STREAM_CHUNK, STREAM_PUT_MIN};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_pipe_inflight_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn pattern(len: usize, seed: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + seed * 7 + 13) % 251) as u8).collect()
}

async fn start(dir: &Path) -> (Arc<S3ServiceState>, String, s3_server::ServerHandle) {
    let state = Arc::new(S3ServiceState::with_data_dir(dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    (state, url, handle)
}

fn parse_metrics(text: &str) -> HashMap<String, f64> {
    let mut out = HashMap::new();
    for line in text.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.rsplit_once(' ') {
            if let Ok(n) = v.parse::<f64>() {
                out.insert(k.to_string(), n);
            }
        }
    }
    out
}

async fn scrape_metrics(client: &reqwest::Client, url: &str) -> HashMap<String, f64> {
    let text = client
        .get(format!("{}/_awanfs/metrics", url))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    parse_metrics(&text)
}

async fn wait_for_disk_writes(state: &S3ServiceState, target: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let got = state.work.snapshot().disk_writes;
        if got >= target {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for disk_writes >= {} (got {})",
            target,
            got
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn wait_for_inflight_zero(client: &reqwest::Client, url: &str) -> HashMap<String, f64> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let m = scrape_metrics(client, url).await;
        if m.get("awanfs_inflight_bytes").copied() == Some(0.0) {
            return m;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for awanfs_inflight_bytes == 0 (got {:?})",
            m.get("awanfs_inflight_bytes")
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn read_http_head(stream: &mut TcpStream) -> String {
    let mut raw = Vec::new();
    let mut chunk = [0u8; 1024];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let rem = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!rem.is_zero(), "timed out waiting for HTTP response headers");
        let n = tokio::time::timeout(rem, stream.read(&mut chunk))
            .await
            .expect("HTTP response header read timeout")
            .unwrap();
        assert!(n > 0, "connection closed before HTTP response headers arrived: {}", String::from_utf8_lossy(&raw));
        raw.extend_from_slice(&chunk[..n]);
        if raw.windows(4).any(|w| w == b"\r\n\r\n") {
            return String::from_utf8_lossy(&raw).into_owned();
        }
    }
}

fn header_value(head: &str, name: &str) -> Option<String> {
    for line in head.lines() {
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case(name) {
                return Some(v.trim().to_string());
            }
        }
    }
    None
}

#[tokio::test]
async fn concurrent_streamed_puts_count_pipeline_buffers_and_release_them() {
    let dir = temp_dir("puts");
    let (state, url, handle) = start(&dir).await;
    let client = reqwest::Client::new();
    assert_eq!(client.put(format!("{}/pipe-put", url)).send().await.unwrap().status(), StatusCode::OK);

    const N: usize = 4;
    let total_len = 5 * 1024 * 1024 + 123;
    assert!(total_len as u64 > STREAM_PUT_MIN);
    let prefix_len = 2 * STREAM_CHUNK + 64 * 1024;
    let bodies: Vec<Vec<u8>> = (0..N).map(|i| pattern(total_len, i + 1)).collect();

    let before_writes = state.work.snapshot().disk_writes;
    let mut streams = Vec::with_capacity(N);
    for (i, body) in bodies.iter().enumerate() {
        let mut s = TcpStream::connect(handle.local_addr).await.unwrap();
        let head = format!(
            "PUT /pipe-put/obj-{} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            i, handle.local_addr, total_len
        );
        s.write_all(head.as_bytes()).await.unwrap();
        s.write_all(&body[..prefix_len]).await.unwrap();
        s.flush().await.unwrap();
        streams.push(s);
    }

    wait_for_disk_writes(&state, before_writes + N as u64).await;

    let min_per_put = (STREAM_CHUNK + SECTOR_SIZE + 2 * STREAM_CHUNK) as u64;
    let expected_min = (N as u64) * min_per_put;
    let m = scrape_metrics(&client, &url).await;
    assert_eq!(m["awanfs_memory_bodies_in_use_bytes"], 0.0, "streamed PUTs must not charge memory_admit");
    assert!(
        m["awanfs_inflight_bytes"] >= expected_min as f64,
        "awanfs_inflight_bytes ({}) must be >= {} ({} * {}) while {} streamed PUTs are in flight",
        m["awanfs_inflight_bytes"],
        expected_min,
        N,
        min_per_put,
        N
    );
    assert!(
        m["awanfs_peak_inflight_bytes"] >= expected_min as f64,
        "awanfs_peak_inflight_bytes ({}) must be >= {} for {} concurrent streamed PUTs",
        m["awanfs_peak_inflight_bytes"],
        expected_min,
        N
    );

    for (mut s, body) in streams.into_iter().zip(bodies.iter()) {
        s.write_all(&body[prefix_len..]).await.unwrap();
        s.flush().await.unwrap();
        let resp = read_http_head(&mut s).await;
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "unexpected PUT response: {}", resp);
    }

    let m = wait_for_inflight_zero(&client, &url).await;
    assert!(m["awanfs_peak_inflight_bytes"] >= expected_min as f64);
    assert_eq!(m["awanfs_memory_bodies_in_use_bytes"], 0.0);

    for (i, expected) in bodies.iter().enumerate() {
        let got = client.get(format!("{}/pipe-put/obj-{}", url, i)).send().await.unwrap();
        assert_eq!(got.status(), StatusCode::OK);
        assert!(got.bytes().await.unwrap().as_ref() == expected.as_slice(), "object {} bytes mismatch", i);
    }

    handle.shutdown();
    drop(state);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn concurrent_streamed_upload_parts_count_pipeline_buffers_and_release_them() {
    let dir = temp_dir("parts");
    let (state, url, handle) = start(&dir).await;
    let client = reqwest::Client::new();
    assert_eq!(client.put(format!("{}/pipe-mpu", url)).send().await.unwrap().status(), StatusCode::OK);
    let init_xml = client.post(format!("{}/pipe-mpu/obj?uploads", url)).send().await.unwrap().text().await.unwrap();
    let upload_id = init_xml.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap().to_string();

    const N: usize = 4;
    let part_len = 5 * 1024 * 1024 + 123;
    assert!(part_len as u64 > STREAM_PUT_MIN);
    let prefix_len = 2 * PART_CHUNK + 64 * 1024;
    let bodies: Vec<Vec<u8>> = (0..N).map(|i| pattern(part_len, i + 10)).collect();

    let before_writes = state.work.snapshot().disk_writes;
    let mut streams = Vec::with_capacity(N);
    for (i, body) in bodies.iter().enumerate() {
        let part_num = i + 1;
        let mut s = TcpStream::connect(handle.local_addr).await.unwrap();
        let head = format!(
            "PUT /pipe-mpu/obj?partNumber={}&uploadId={} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            part_num, upload_id, handle.local_addr, part_len
        );
        s.write_all(head.as_bytes()).await.unwrap();
        s.write_all(&body[..prefix_len]).await.unwrap();
        s.flush().await.unwrap();
        streams.push((part_num, s));
    }

    wait_for_disk_writes(&state, before_writes + (2 * N) as u64).await;

    let min_per_part = (2 * PART_CHUNK) as u64;
    let expected_min = (N as u64) * min_per_part;
    let m = scrape_metrics(&client, &url).await;
    assert_eq!(m["awanfs_memory_bodies_in_use_bytes"], 0.0, "streamed UploadParts must not charge memory_admit");
    assert!(
        m["awanfs_inflight_bytes"] >= expected_min as f64,
        "awanfs_inflight_bytes ({}) must be >= {} ({} * {}) while {} streamed UploadParts are in flight",
        m["awanfs_inflight_bytes"],
        expected_min,
        N,
        min_per_part,
        N
    );
    assert!(
        m["awanfs_peak_inflight_bytes"] >= expected_min as f64,
        "awanfs_peak_inflight_bytes ({}) must be >= {} for {} concurrent streamed UploadParts",
        m["awanfs_peak_inflight_bytes"],
        expected_min,
        N
    );

    let mut etags = Vec::with_capacity(N);
    for ((part_num, mut s), body) in streams.into_iter().zip(bodies.iter()) {
        s.write_all(&body[prefix_len..]).await.unwrap();
        s.flush().await.unwrap();
        let resp = read_http_head(&mut s).await;
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "unexpected UploadPart response: {}", resp);
        let etag = header_value(&resp, "etag").expect("ETag header on UploadPart");
        etags.push((part_num, etag));
    }

    let m = wait_for_inflight_zero(&client, &url).await;
    assert!(m["awanfs_peak_inflight_bytes"] >= expected_min as f64);
    assert_eq!(m["awanfs_memory_bodies_in_use_bytes"], 0.0);

    let mut xml = String::from("<CompleteMultipartUpload>");
    for (part_num, etag) in etags {
        xml.push_str(&format!("<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>", part_num, etag));
    }
    xml.push_str("</CompleteMultipartUpload>");
    assert_eq!(
        client.post(format!("{}/pipe-mpu/obj?uploadId={}", url, upload_id)).body(xml).send().await.unwrap().status(),
        StatusCode::OK
    );

    let got = client.get(format!("{}/pipe-mpu/obj", url)).send().await.unwrap();
    assert_eq!(got.status(), StatusCode::OK);
    let assembled = got.bytes().await.unwrap();
    assert_eq!(assembled.len(), N * part_len);
    for (i, expected) in bodies.iter().enumerate() {
        assert!(&assembled[i * part_len..(i + 1) * part_len] == expected.as_slice(), "part {} mismatch", i + 1);
    }

    handle.shutdown();
    drop(state);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn aborted_streamed_put_and_upload_part_release_all_inflight_bytes() {
    let dir = temp_dir("abort");
    let (state, url, handle) = start(&dir).await;
    let client = reqwest::Client::new();
    assert_eq!(client.put(format!("{}/pipe-abort", url)).send().await.unwrap().status(), StatusCode::OK);
    let init_xml = client.post(format!("{}/pipe-abort/obj?uploads", url)).send().await.unwrap().text().await.unwrap();
    let upload_id = init_xml.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap().to_string();

    let total_len = 5 * 1024 * 1024 + 123;
    let prefix_len = 2 * STREAM_CHUNK + 64 * 1024;
    let payload = pattern(total_len, 99);

    let before_writes = state.work.snapshot().disk_writes;
    let mut put_sock = TcpStream::connect(handle.local_addr).await.unwrap();
    let head = format!(
        "PUT /pipe-abort/cut-put HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        handle.local_addr, total_len
    );
    put_sock.write_all(head.as_bytes()).await.unwrap();
    put_sock.write_all(&payload[..prefix_len]).await.unwrap();
    put_sock.flush().await.unwrap();
    wait_for_disk_writes(&state, before_writes + 1).await;

    let m = scrape_metrics(&client, &url).await;
    assert!(m["awanfs_inflight_bytes"] >= (STREAM_CHUNK + SECTOR_SIZE + 2 * STREAM_CHUNK) as f64);
    drop(put_sock);
    wait_for_inflight_zero(&client, &url).await;

    let before_writes = state.work.snapshot().disk_writes;
    let mut part_sock = TcpStream::connect(handle.local_addr).await.unwrap();
    let head = format!(
        "PUT /pipe-abort/obj?partNumber=1&uploadId={} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        upload_id, handle.local_addr, total_len
    );
    part_sock.write_all(head.as_bytes()).await.unwrap();
    part_sock.write_all(&payload[..prefix_len]).await.unwrap();
    part_sock.flush().await.unwrap();
    wait_for_disk_writes(&state, before_writes + 2).await;

    let m = scrape_metrics(&client, &url).await;
    assert!(m["awanfs_inflight_bytes"] >= (2 * PART_CHUNK) as f64);
    drop(part_sock);
    wait_for_inflight_zero(&client, &url).await;

    let bad_sha = "0".repeat(64);
    let res = client
        .put(format!("{}/pipe-abort/bad-sha", url))
        .header("x-amz-content-sha256", bad_sha)
        .body(payload)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    wait_for_inflight_zero(&client, &url).await;

    handle.shutdown();
    drop(state);
    let _ = std::fs::remove_dir_all(&dir);
}
