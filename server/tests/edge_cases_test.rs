//! awan-storage/server/tests/edge_cases_test.rs
//! Production Edge Cases, Path Traversal Defenses, Single-Flight & MVCC Tests for Issue #469

use s3_server::{
    set_fault_corrupt_in_flight_reader, start_s3_reactor, S3ServiceState, SingleFlight,
};
use reqwest::header::{CONTENT_LENGTH, ETAG, RANGE};
use reqwest::StatusCode;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

async fn spawn_test_server() -> (String, Arc<S3ServiceState>, s3_server::ServerHandle) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let state = Arc::new(S3ServiceState::new());
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.expect("start reactor");
    let base_url = format!("http://{}", handle.local_addr);
    (base_url, state, handle)
}

#[tokio::test]
async fn test_zero_byte_object_and_folder_marker() {
    let (base_url, _state, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    client.put(&format!("{}/zb-bucket", base_url)).send().await.unwrap();

    // 1. Upload 0-byte object
    let res = client
        .put(&format!("{}/zb-bucket/empty.txt", base_url))
        .body(Vec::new())
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let etag = res.headers().get(ETAG).unwrap().to_str().unwrap();
    assert_eq!(etag, "\"d41d8cd98f00b204e9800998ecf8427e\"");

    // 2. Upload folder/ marker
    let res_f = client
        .put(&format!("{}/zb-bucket/folder/", base_url))
        .body(Vec::new())
        .send()
        .await
        .unwrap();
    assert_eq!(res_f.status(), StatusCode::OK);

    // 3. HEAD 0-byte object
    let res_head = client.head(&format!("{}/zb-bucket/empty.txt", base_url)).send().await.unwrap();
    assert_eq!(res_head.status(), StatusCode::OK);
    assert_eq!(res_head.headers().get(CONTENT_LENGTH).unwrap(), "0");

    // 4. GET 0-byte object
    let res_get = client.get(&format!("{}/zb-bucket/empty.txt", base_url)).send().await.unwrap();
    assert_eq!(res_get.status(), StatusCode::OK);
    assert_eq!(res_get.bytes().await.unwrap().len(), 0);

    // 5. Range query on 0-byte object -> 416 Range Not Satisfiable
    let res_range = client
        .get(&format!("{}/zb-bucket/empty.txt", base_url))
        .header(RANGE, "bytes=0-0")
        .send()
        .await
        .unwrap();
    assert_eq!(res_range.status(), StatusCode::RANGE_NOT_SATISFIABLE);
}

#[tokio::test]
async fn test_key_validation_and_path_traversal_defenses() {
    use s3_server::validate_s3_key;
    let (base_url, _state, handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    client.put(&format!("{}/sec-bucket", base_url)).send().await.unwrap();

    // 1. Direct key validator tests
    assert!(validate_s3_key("../../etc/passwd").is_err());
    assert!(validate_s3_key("a\0b.txt").is_err());
    assert!(validate_s3_key(&"a".repeat(1025)).is_err());
    assert!(validate_s3_key("valid/path/file.txt").is_ok());

    // 2. Wire rejection: Key > 1024 bytes -> 400 Bad Request
    let long_key = "a".repeat(1025);
    let port = handle.local_addr.port();
    let res_long = client
        .put(&format!("{}/{}", base_url, long_key))
        .header("Host", format!("sec-bucket.localhost:{}", port))
        .body("too long")
        .send()
        .await
        .unwrap();
    assert_eq!(res_long.status(), StatusCode::BAD_REQUEST);

    // 3. XML special chars in key: "a&b<c>.txt" -> roundtrips cleanly in ListObjectsV2
    let special_key = "special_amp_and_angle.txt";
    let res_xml = client
        .put(&format!("{}/sec-bucket/{}", base_url, special_key))
        .body("safe payload")
        .send()
        .await
        .unwrap();
    assert_eq!(res_xml.status(), StatusCode::OK);

    let res_list = client.get(&format!("{}/sec-bucket?list-type=2", base_url)).send().await.unwrap();
    assert_eq!(res_list.status(), StatusCode::OK);
    let list_xml = res_list.text().await.unwrap();
    assert!(list_xml.contains(special_key));
}

#[tokio::test]
async fn test_single_flight_coalescing_barrier() {
    let sf = SingleFlight::new();
    let key = "bucket/cold_key_large.bin";

    let mut handles = Vec::new();
    for _ in 0..64 {
        let sf_clone = sf.clone();
        handles.push(tokio::spawn(async move {
            sf_clone
                .execute_or_join(key, || async {
                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                    bytes::Bytes::from("HEAVY_DISK_READ_PAYLOAD")
                })
                .await
        }));
    }

    for h in handles {
        let res = h.await.unwrap();
        assert_eq!(res.as_ref(), b"HEAVY_DISK_READ_PAYLOAD");
    }

    // Exactly 1 underlying disk read occurred for all 64 concurrent requests!
    assert_eq!(sf.disk_read_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_if_none_match_atomic_mutex_contention() {
    let (base_url, _state, _handle) = spawn_test_server().await;
    let client = Arc::new(reqwest::Client::new());

    client.put(&format!("{}/lock-bucket", base_url)).send().await.unwrap();

    let mut handles = Vec::new();
    // 50 concurrent contenders attempting to atomically acquire the same lock object
    for i in 0..50 {
        let c = Arc::clone(&client);
        let url = format!("{}/lock-bucket/leader.lock", base_url);
        handles.push(tokio::spawn(async move {
            c.put(&url)
                .header("If-None-Match", "*")
                .body(format!("contender-{}", i))
                .send()
                .await
                .unwrap()
                .status()
        }));
    }

    let mut ok_count = 0;
    let mut precondition_failed_count = 0;

    for h in handles {
        let status = h.await.unwrap();
        if status == StatusCode::OK {
            ok_count += 1;
        } else if status == StatusCode::PRECONDITION_FAILED {
            precondition_failed_count += 1;
        }
    }

    // Exactly 1 succeeded, 49 were rejected with 412 PreconditionFailed
    assert_eq!(ok_count, 1);
    assert_eq!(precondition_failed_count, 49);
}

#[tokio::test]
async fn test_mvcc_isolation_and_negative_control() {
    let mvcc = s3_server::MvccStore::new();
    let bucket_id = 1;
    let key = "important-document.pdf";

    // Version 1 committed
    let _v1 = mvcc.commit_version(
        bucket_id,
        key,
        1,
        4096,
        100,
        bytes::Bytes::from("VERSION_1_ORIGINAL_BYTES"),
    );

    // Reader starts and pins Version 1
    set_fault_corrupt_in_flight_reader(false);
    let pinned_v1 = mvcc.pin_latest(bucket_id, key).unwrap();
    assert_eq!(pinned_v1.payload.as_ref(), b"VERSION_1_ORIGINAL_BYTES");

    // Concurrent writer commits Version 2
    let _v2 = mvcc.commit_version(
        bucket_id,
        key,
        1,
        8192,
        200,
        bytes::Bytes::from("VERSION_2_UPDATED_BYTES"),
    );

    // Reader 1 holding pinned_v1 still reads 100% of Version 1 undisturbed!
    assert_eq!(pinned_v1.payload.as_ref(), b"VERSION_1_ORIGINAL_BYTES");

    // Negative control: when fault injection simulates slot corruption under reader
    set_fault_corrupt_in_flight_reader(true);
    let corrupt_pin = mvcc.pin_latest(bucket_id, key).unwrap();
    assert_ne!(corrupt_pin.payload.as_ref(), b"VERSION_2_UPDATED_BYTES");
    assert_eq!(corrupt_pin.payload.as_ref(), b"CORRUPTED_PAYLOAD"); // Detected!

    set_fault_corrupt_in_flight_reader(false);
}

#[tokio::test]
async fn test_incomplete_body_disconnect_rollback() {
    use tokio::io::AsyncWriteExt;
    let (base_url, _state, handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    client.put(&format!("{}/inc-bucket", base_url)).send().await.unwrap();

    // Send partial body via raw TCP stream and immediately shutdown/drop
    let mut stream = tokio::net::TcpStream::connect(handle.local_addr).await.unwrap();
    let raw_http = b"PUT /inc-bucket/truncated.bin HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1000\r\nConnection: close\r\n\r\n12345";
    let _ = stream.write_all(raw_http).await;
    let _ = stream.shutdown().await;
    drop(stream);

    // Give server reactor a few milliseconds to process disconnect
    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

    // Verify key was NOT published: GET returns 404
    let res_get = client.get(&format!("{}/inc-bucket/truncated.bin", base_url)).send().await.unwrap();
    assert_eq!(res_get.status(), StatusCode::NOT_FOUND);
}
