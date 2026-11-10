//! awan-storage/server/tests/wire_test.rs
//! Wire Protocol, REST Endpoints, Routing & RFC 9110 Range Tests for Issue #462

use s3_server::{parse_range_header, set_fault_disable_range_clamping, start_s3_reactor, S3ServiceState};
use reqwest::header::{CONTENT_TYPE, ETAG, HOST, RANGE};
use reqwest::StatusCode;
use std::net::SocketAddr;
use std::sync::Arc;

async fn spawn_test_server() -> (String, s3_server::ServerHandle) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let state = Arc::new(S3ServiceState::new());
    let handle = start_s3_reactor(addr, state).await.expect("start reactor");
    let base_url = format!("http://{}", handle.local_addr);
    (base_url, handle)
}

#[tokio::test]
async fn test_bucket_lifecycle_and_batch_delete() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. HeadBucket on non-existent -> 404
    let res = client.head(&format!("{}/mybucket", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    // 2. CreateBucket -> 200 OK
    let res = client.put(&format!("{}/mybucket", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 3. HeadBucket on created -> 200 OK
    let res = client.head(&format!("{}/mybucket", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 4. PutObject -> 200 OK
    let res = client
        .put(&format!("{}/mybucket/data.bin", base_url))
        .body(vec![1u8, 2, 3, 4, 5])
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(res.headers().contains_key(ETAG));

    // 5. DeleteBucket while non-empty -> 409 Conflict
    let res = client.delete(&format!("{}/mybucket", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::CONFLICT);
    let body = res.text().await.unwrap();
    assert!(body.contains("BucketNotEmpty"));

    // 6. Batch DeleteObjects -> 200 OK
    let delete_xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <Delete>\n  \
          <Object><Key>data.bin</Key></Object>\n\
        </Delete>";
    let res = client
        .post(&format!("{}/mybucket?delete", base_url))
        .header(CONTENT_TYPE, "application/xml")
        .body(delete_xml)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let resp_xml = res.text().await.unwrap();
    assert!(resp_xml.contains("<Key>data.bin</Key>"));

    // 7. DeleteBucket on empty -> 204 No Content
    let res = client.delete(&format!("{}/mybucket", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    // 8. HeadBucket on deleted -> 404
    let res = client.head(&format!("{}/mybucket", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_virtual_host_vs_path_style_routing() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Create bucket via path style
    let res = client.put(&format!("{}/vh-bucket", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // PutObject via virtual hosted style: Host: vh-bucket.localhost:PORT
    let port = _handle.local_addr.port();
    let vhost_header = format!("vh-bucket.localhost:{}", port);

    let res = client
        .put(&format!("{}/config.json", base_url))
        .header(HOST, &vhost_header)
        .body("{\"status\":\"ok\"}")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // Read via path style: /{bucket}/{key}
    let res = client.get(&format!("{}/vh-bucket/config.json", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let text = res.text().await.unwrap();
    assert_eq!(text, "{\"status\":\"ok\"}");

    // Read via virtual hosted style: Host: vh-bucket.s3.localhost:PORT, URI: /config.json
    let res = client
        .get(&format!("{}/config.json", base_url))
        .header(HOST, format!("vh-bucket.s3.localhost:{}", port))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let text2 = res.text().await.unwrap();
    assert_eq!(text2, "{\"status\":\"ok\"}");
}

#[tokio::test]
async fn test_rfc9110_range_requests_and_clamping() {
    let (base_url, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Create bucket and put 11-byte object: "0123456789A"
    client.put(&format!("{}/range-bucket", base_url)).send().await.unwrap();

    let payload = b"0123456789A";
    let res = client
        .put(&format!("{}/range-bucket/sample.txt", base_url))
        .body(payload.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 1. Explicit range: bytes=0-4 -> "01234" (5 bytes)
    let res = client
        .get(&format!("{}/range-bucket/sample.txt", base_url))
        .header(RANGE, "bytes=0-4")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(res.text().await.unwrap(), "01234");

    // 2. Offset range: bytes=5- -> "56789A" (6 bytes)
    let res = client
        .get(&format!("{}/range-bucket/sample.txt", base_url))
        .header(RANGE, "bytes=5-")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(res.text().await.unwrap(), "56789A");

    // 3. Suffix range: bytes=-4 -> "789A" (4 bytes)
    let res = client
        .get(&format!("{}/range-bucket/sample.txt", base_url))
        .header(RANGE, "bytes=-4")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(res.text().await.unwrap(), "789A");

    // 4. Clamped range: bytes=0-999999 -> clamped to size - 1 (10) -> full 11 bytes
    let res = client
        .get(&format!("{}/range-bucket/sample.txt", base_url))
        .header(RANGE, "bytes=0-999999")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(res.headers().get("Content-Range").unwrap(), "bytes 0-10/11");
    assert_eq!(res.text().await.unwrap(), "0123456789A");

    // 5. Out of bounds range: bytes=100-200 on 11-byte object -> 416 Range Not Satisfiable
    let res = client
        .get(&format!("{}/range-bucket/sample.txt", base_url))
        .header(RANGE, "bytes=100-200")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(res.headers().get("Content-Range").unwrap(), "bytes */11");
}

#[test]
fn test_range_clamping_negative_control() {
    let object_size = 11u64;

    // Normal behavior: clamping end=999999 to object_size - 1 = 10
    set_fault_disable_range_clamping(false);
    let r = parse_range_header("bytes=0-999999", object_size).unwrap().unwrap();
    assert_eq!(r.start, 0);
    assert_eq!(r.end, 10);

    // Negative control: when clamping is disabled, end stays 999999
    set_fault_disable_range_clamping(true);
    let r_fault = parse_range_header("bytes=0-999999", object_size).unwrap().unwrap();
    assert_eq!(r_fault.end, 999999);
    assert_ne!(r_fault.end, object_size - 1);

    // Restore fix
    set_fault_disable_range_clamping(false);
    let r_restored = parse_range_header("bytes=0-999999", object_size).unwrap().unwrap();
    assert_eq!(r_restored.end, 10);
}
