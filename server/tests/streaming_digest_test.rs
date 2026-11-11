//! awan-storage/server/tests/streaming_digest_test.rs
//! Streaming Hardware-Accelerated MD5 & CRC32C Checksum Pipeline Tests (#484)

use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use reqwest::Client;
use s3_server::digest::StreamingDigest;
use s3_server::{start_s3_reactor, S3ServiceState};

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("awan_streaming_digest_{}_{}", name, std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn test_equivalence_hardware_simd_and_software_fallback() {
    // Generate various test buffers including odd-sized and multi-megabyte payloads
    let test_sizes = [0, 1, 7, 64, 4095, 4096, 4097, 65536, 1024 * 1024 * 2];

    for &size in &test_sizes {
        let mut data = Vec::with_capacity(size);
        for i in 0..size {
            data.push(((i * 31) ^ (i >> 3)) as u8);
        }

        // 1. Hardware-accelerated / runtime detected pipeline
        let mut hw_digest = StreamingDigest::new();
        // Feed in irregular chunk sizes to simulate TCP streaming frames
        for chunk in data.chunks(1337) {
            hw_digest.update(chunk);
        }
        let (hw_hi, hw_lo, hw_hex, hw_md5, hw_crc) = hw_digest.finalize();

        // 2. Pure portable software fallback pipeline
        let mut sw_digest = StreamingDigest::new_software_fallback();
        for chunk in data.chunks(1337) {
            sw_digest.update(chunk);
        }
        let (sw_hi, sw_lo, sw_hex, sw_md5, sw_crc) = sw_digest.finalize();

        // Must produce 100% bit-identical hashes
        assert_eq!(hw_hi, sw_hi, "ETag HI mismatch for size {}", size);
        assert_eq!(hw_lo, sw_lo, "ETag LO mismatch for size {}", size);
        assert_eq!(hw_hex, sw_hex, "ETag hex mismatch for size {}", size);
        assert_eq!(hw_md5, sw_md5, "Raw MD5 mismatch for size {}", size);
        assert_eq!(hw_crc, sw_crc, "CRC32C mismatch for size {}", size);

        // Verify standalone software_crc32c matches standard crc32c
        let ref_crc = crc32c::crc32c(&data);
        assert_eq!(sw_crc, ref_crc, "Software CRC32C does not match reference for size {}", size);
    }
}

#[tokio::test]
async fn test_streaming_put_object_computes_digests_during_ingest() {
    let dir = temp_dir("streaming_put");
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.expect("start reactor");
    let base_url = format!("http://{}", handle.local_addr);
    let client = Client::new();

    let bucket = "stream-digest-bucket";
    let key = "streamed-10mb-file.bin";
    client.put(&format!("{}/{}", base_url, bucket)).send().await.unwrap();

    // 10 MiB stream
    let payload = vec![0x37u8; 10 * 1024 * 1024];
    let mut expected_digest = StreamingDigest::new();
    expected_digest.update(&payload);
    let (_, _, expected_etag, _, _) = expected_digest.finalize();

    let put_res = client
        .put(&format!("{}/{}/{}", base_url, bucket, key))
        .body(payload.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(put_res.status(), reqwest::StatusCode::OK);
    let etag_header = put_res.headers().get("ETag").unwrap().to_str().unwrap().trim_matches('"');
    assert_eq!(etag_header, expected_etag, "Streamed ETag must match computed MD5");

    // Fetch back and verify
    let get_res = client.get(&format!("{}/{}/{}", base_url, bucket, key)).send().await.unwrap();
    assert_eq!(get_res.status(), reqwest::StatusCode::OK);
    let fetched = get_res.bytes().await.unwrap();
    assert_eq!(fetched.as_ref(), payload.as_slice());

    handle.shutdown();
}

#[tokio::test]
async fn test_negative_control_content_md5_tamper_detected() {
    let dir = temp_dir("tamper_control");
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.expect("start reactor");
    let base_url = format!("http://{}", handle.local_addr);
    let client = Client::new();

    let bucket = "tamper-bucket";
    let key = "tampered-file.bin";
    client.put(&format!("{}/{}", base_url, bucket)).send().await.unwrap();

    let payload = b"Original clean payload data";
    // Provide a tampered Content-MD5 header (base64 of invalid MD5)
    let bad_md5_b64 = "AAAAAAAAAAAAAAAAAAAAAA==";

    let put_res = client
        .put(&format!("{}/{}/{}", base_url, bucket, key))
        .header("Content-MD5", bad_md5_b64)
        .body(payload.to_vec())
        .send()
        .await
        .unwrap();

    // Must be rejected with BadDigest / 400
    assert_eq!(put_res.status(), reqwest::StatusCode::BAD_REQUEST);
    let xml = put_res.text().await.unwrap();
    assert!(xml.contains("BadDigest"), "Expected BadDigest error in XML: {}", xml);

    handle.shutdown();
}
