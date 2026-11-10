//! awan-storage/server/tests/multipart_sigv4_test.rs
//! S3 Multipart Upload and AWS Signature V4 Verification Tests for Issue #467

use awan_storage_server::{
    decode_aws_chunked, set_fault_tamper_chunk_signature,
    set_fault_tamper_sigv4, start_s3_reactor, validate_sigv4_auth, S3ServiceState,
};
use reqwest::header::{CONTENT_TYPE, ETAG, RANGE};
use reqwest::StatusCode;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

async fn spawn_test_server(enforce_min_size: bool) -> (String, awan_storage_server::ServerHandle) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let state = Arc::new(S3ServiceState::new());
    state.multipart.enforce_min_part_size.store(enforce_min_size, std::sync::atomic::Ordering::SeqCst);
    let handle = start_s3_reactor(addr, state).await.expect("start reactor");
    let base_url = format!("http://{}", handle.local_addr);
    (base_url, handle)
}

#[tokio::test]
async fn test_multipart_lifecycle_out_of_order_and_composite_etag() {
    let (base_url, _handle) = spawn_test_server(false).await;
    let client = reqwest::Client::new();

    // 1. Create bucket
    client.put(&format!("{}/mp-bucket", base_url)).send().await.unwrap();

    // 2. Initiate Multipart Upload: POST /{bucket}/{key}?uploads
    let res = client
        .post(&format!("{}/mp-bucket/large-file.bin?uploads", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let init_xml = res.text().await.unwrap();
    let upload_id = init_xml
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap();

    // 3. Upload parts OUT OF ORDER: Part 3, Part 1, Part 2
    let p3_data = b"Part 3 data chunk";
    let p1_data = b"Part 1 initial chunk";
    let p2_temp = b"Part 2 to be overwritten";
    let p2_final = b"Part 2 final verified data chunk";

    // Upload Part 3
    let res3 = client
        .put(&format!("{}/mp-bucket/large-file.bin?partNumber=3&uploadId={}", base_url, upload_id))
        .body(p3_data.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(res3.status(), StatusCode::OK);
    let etag3 = res3.headers().get(ETAG).unwrap().to_str().unwrap().to_string();

    // Upload Part 1
    let res1 = client
        .put(&format!("{}/mp-bucket/large-file.bin?partNumber=1&uploadId={}", base_url, upload_id))
        .body(p1_data.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(res1.status(), StatusCode::OK);
    let etag1 = res1.headers().get(ETAG).unwrap().to_str().unwrap().to_string();

    // Upload Part 2 (first time)
    client
        .put(&format!("{}/mp-bucket/large-file.bin?partNumber=2&uploadId={}", base_url, upload_id))
        .body(p2_temp.to_vec())
        .send()
        .await
        .unwrap();

    // Overwrite Part 2 (atomic replacement)
    let res2 = client
        .put(&format!("{}/mp-bucket/large-file.bin?partNumber=2&uploadId={}", base_url, upload_id))
        .body(p2_final.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(res2.status(), StatusCode::OK);
    let etag2 = res2.headers().get(ETAG).unwrap().to_str().unwrap().to_string();

    // 4. ListParts: verify all 3 parts present
    let res_lp = client
        .get(&format!("{}/mp-bucket/large-file.bin?uploadId={}", base_url, upload_id))
        .send()
        .await
        .unwrap();
    assert_eq!(res_lp.status(), StatusCode::OK);
    let lp_xml = res_lp.text().await.unwrap();
    assert!(lp_xml.contains("<PartNumber>1</PartNumber>"));
    assert!(lp_xml.contains("<PartNumber>2</PartNumber>"));
    assert!(lp_xml.contains("<PartNumber>3</PartNumber>"));

    // 5. Complete Multipart Upload
    let complete_xml = format!(
        "<CompleteMultipartUpload>\n  \
          <Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part>\n  \
          <Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part>\n  \
          <Part><PartNumber>3</PartNumber><ETag>{}</ETag></Part>\n\
        </CompleteMultipartUpload>",
        etag1, etag2, etag3
    );

    let res_cmp = client
        .post(&format!("{}/mp-bucket/large-file.bin?uploadId={}", base_url, upload_id))
        .header(CONTENT_TYPE, "application/xml")
        .body(complete_xml)
        .send()
        .await
        .unwrap();
    assert_eq!(res_cmp.status(), StatusCode::OK);
    let composite_etag = res_cmp.headers().get(ETAG).unwrap().to_str().unwrap();
    assert!(composite_etag.ends_with("-3\"")); // Exactly 3 parts in composite ETag

    // 6. Full GET: verify stitched payload
    let mut expected_payload = Vec::new();
    expected_payload.extend_from_slice(p1_data);
    expected_payload.extend_from_slice(p2_final);
    expected_payload.extend_from_slice(p3_data);

    let res_get = client.get(&format!("{}/mp-bucket/large-file.bin", base_url)).send().await.unwrap();
    assert_eq!(res_get.status(), StatusCode::OK);
    let body_bytes = res_get.bytes().await.unwrap();
    assert_eq!(body_bytes.as_ref(), expected_payload.as_slice());

    // 7. Cross-part byte range reading: span from Part 1 into Part 2
    let p1_len = p1_data.len();
    let range_header = format!("bytes={}-{}", p1_len - 5, p1_len + 5);
    let res_range = client
        .get(&format!("{}/mp-bucket/large-file.bin", base_url))
        .header(RANGE, range_header)
        .send()
        .await
        .unwrap();
    assert_eq!(res_range.status(), StatusCode::PARTIAL_CONTENT);
    let range_bytes = res_range.bytes().await.unwrap();
    assert_eq!(range_bytes.as_ref(), &expected_payload[p1_len - 5..=p1_len + 5]);
}

#[tokio::test]
async fn test_multipart_negative_controls_and_abort() {
    let (base_url, _handle) = spawn_test_server(true).await; // Enforce >= 5 MiB size validation
    let client = reqwest::Client::new();

    client.put(&format!("{}/abort-bucket", base_url)).send().await.unwrap();

    // Initiate upload
    let res = client.post(&format!("{}/abort-bucket/test.bin?uploads", base_url)).send().await.unwrap();
    let upload_id = res.text().await.unwrap()
        .split("<UploadId>").nth(1).unwrap()
        .split("</UploadId>").next().unwrap().to_string();

    // Upload small 100-byte parts
    let res1 = client
        .put(&format!("{}/abort-bucket/test.bin?partNumber=1&uploadId={}", base_url, upload_id))
        .body(vec![1u8; 100])
        .send()
        .await
        .unwrap();
    let etag1 = res1.headers().get(ETAG).unwrap().to_str().unwrap().to_string();

    let res2 = client
        .put(&format!("{}/abort-bucket/test.bin?partNumber=2&uploadId={}", base_url, upload_id))
        .body(vec![2u8; 100])
        .send()
        .await
        .unwrap();
    let etag2 = res2.headers().get(ETAG).unwrap().to_str().unwrap().to_string();

    // Negative Control 1: EntityTooSmall (< 5 MiB for non-final part 1)
    let cmp_xml = format!(
        "<CompleteMultipartUpload>\n  \
          <Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part>\n  \
          <Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part>\n\
        </CompleteMultipartUpload>",
        etag1, etag2
    );
    let res_small = client
        .post(&format!("{}/abort-bucket/test.bin?uploadId={}", base_url, upload_id))
        .header(CONTENT_TYPE, "application/xml")
        .body(cmp_xml)
        .send()
        .await
        .unwrap();
    assert_eq!(res_small.status(), StatusCode::BAD_REQUEST);
    assert!(res_small.text().await.unwrap().contains("EntityTooSmall"));

    // Negative Control 2: InvalidPartOrder ([2, 1] instead of [1, 2])
    let out_of_order_xml = format!(
        "<CompleteMultipartUpload>\n  \
          <Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part>\n  \
          <Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part>\n\
        </CompleteMultipartUpload>",
        etag2, etag1
    );
    let res_order = client
        .post(&format!("{}/abort-bucket/test.bin?uploadId={}", base_url, upload_id))
        .header(CONTENT_TYPE, "application/xml")
        .body(out_of_order_xml)
        .send()
        .await
        .unwrap();
    assert_eq!(res_order.status(), StatusCode::BAD_REQUEST);
    assert!(res_order.text().await.unwrap().contains("InvalidPartOrder"));

    // Abort Multipart Upload
    let res_ab = client
        .delete(&format!("{}/abort-bucket/test.bin?uploadId={}", base_url, upload_id))
        .send()
        .await
        .unwrap();
    assert_eq!(res_ab.status(), StatusCode::NO_CONTENT);

    // ListParts after abort returns 404 NoSuchUpload
    let res_after = client
        .get(&format!("{}/abort-bucket/test.bin?uploadId={}", base_url, upload_id))
        .send()
        .await
        .unwrap();
    assert_eq!(res_after.status(), StatusCode::NOT_FOUND);
}

#[test]
fn test_aws_chunked_decoder_and_signature_tamper_control() {
    let framed = b"a;chunk-signature=1122334455667788\r\n0123456789\r\n0;chunk-signature=99aabbccddeeff00\r\n\r\n";

    // Clean decoding
    set_fault_tamper_chunk_signature(false);
    let decoded = decode_aws_chunked(framed).unwrap();
    assert_eq!(decoded.as_ref(), b"0123456789");

    // Negative control: tampered signature fails with SignatureDoesNotMatch
    set_fault_tamper_chunk_signature(true);
    let res = decode_aws_chunked(framed);
    assert_eq!(res.unwrap_err(), "SignatureDoesNotMatch");

    // Restore
    set_fault_tamper_chunk_signature(false);
    assert!(decode_aws_chunked(framed).is_ok());
}

#[test]
fn test_sigv4_presigned_url_and_skew_checks() {
    use awan_storage_server::parse_iso8601_epoch;
    let date_str = "20251110T120000Z";
    let now = parse_iso8601_epoch(date_str).expect("parse date");

    let mut q = HashMap::new();
    q.insert("X-Amz-Algorithm".to_string(), "AWS4-HMAC-SHA256".to_string());
    q.insert("X-Amz-Date".to_string(), date_str.to_string());
    q.insert("X-Amz-Expires".to_string(), "900".to_string());
    q.insert("X-Amz-Signature".to_string(), "abcdef1234567890".to_string());

    // Valid
    set_fault_tamper_sigv4(false);
    assert!(validate_sigv4_auth(None, &q, now).is_ok());

    // RequestTimeTooSkewed (> 900s into the future)
    assert_eq!(validate_sigv4_auth(None, &q, now - 1000).unwrap_err(), "RequestTimeTooSkewed");

    // RequestExpired (> expires in the past)
    assert_eq!(validate_sigv4_auth(None, &q, now + 1000).unwrap_err(), "RequestExpired");

    // Negative Control: Tampered signature
    set_fault_tamper_sigv4(true);
    assert_eq!(validate_sigv4_auth(None, &q, now).unwrap_err(), "SignatureDoesNotMatch");
    set_fault_tamper_sigv4(false);
}
