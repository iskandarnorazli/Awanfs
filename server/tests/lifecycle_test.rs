//! awan-storage/server/tests/lifecycle_test.rs
//! S3 Bucket Lifecycle Configuration and Expiration Sweep Tests for Issue #468

use s3_server::{
    run_lifecycle_sweep, set_fault_disable_prefix_check, start_s3_reactor, S3ServiceState,
};
use reqwest::header::CONTENT_TYPE;
use reqwest::StatusCode;
use std::net::SocketAddr;
use std::sync::Arc;

async fn spawn_test_server() -> (String, Arc<S3ServiceState>, s3_server::ServerHandle) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let state = Arc::new(S3ServiceState::new());
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.expect("start reactor");
    let base_url = format!("http://{}", handle.local_addr);
    (base_url, state, handle)
}

#[tokio::test]
async fn test_lifecycle_xml_roundtrip_and_deletion() {
    let (base_url, _state, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    // 1. Create bucket
    client.put(&format!("{}/lc-bucket", base_url)).send().await.unwrap();

    // 2. GET ?lifecycle before setting -> 404 NoSuchLifecycleConfiguration
    let res = client.get(&format!("{}/lc-bucket?lifecycle", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    assert!(res.text().await.unwrap().contains("NoSuchLifecycleConfiguration"));

    // 3. PUT ?lifecycle with rules
    let xml_payload = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <LifecycleConfiguration>\n  \
          <Rule>\n    \
            <ID>purge-logs</ID>\n    \
            <Status>Enabled</Status>\n    \
            <Filter><Prefix>logs/</Prefix></Filter>\n    \
            <Expiration><Days>7</Days></Expiration>\n    \
            <AbortIncompleteMultipartUpload><DaysAfterInitiation>3</DaysAfterInitiation></AbortIncompleteMultipartUpload>\n  \
          </Rule>\n\
        </LifecycleConfiguration>";

    let res = client
        .put(&format!("{}/lc-bucket?lifecycle", base_url))
        .header(CONTENT_TYPE, "application/xml")
        .body(xml_payload)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 4. GET ?lifecycle -> 200 OK with matching XML
    let res = client.get(&format!("{}/lc-bucket?lifecycle", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.text().await.unwrap();
    assert!(body.contains("<ID>purge-logs</ID>"));
    assert!(body.contains("<Prefix>logs/</Prefix>"));
    assert!(body.contains("<Days>7</Days>"));
    assert!(body.contains("<DaysAfterInitiation>3</DaysAfterInitiation>"));

    // 5. DELETE ?lifecycle -> 204 No Content
    let res = client.delete(&format!("{}/lc-bucket?lifecycle", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    // 6. GET ?lifecycle after delete -> 404 NoSuchLifecycleConfiguration
    let res = client.get(&format!("{}/lc-bucket?lifecycle", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_prefix_scoped_expiration_and_negative_control() {
    let (base_url, state, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    client.put(&format!("{}/expire-bucket", base_url)).send().await.unwrap();

    // Configure 7-day expiration for "logs/" prefix only
    let xml_payload = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <LifecycleConfiguration>\n  \
          <Rule>\n    \
            <ID>expire-logs</ID>\n    \
            <Status>Enabled</Status>\n    \
            <Filter><Prefix>logs/</Prefix></Filter>\n    \
            <Expiration><Days>7</Days></Expiration>\n  \
          </Rule>\n\
        </LifecycleConfiguration>";

    client
        .put(&format!("{}/expire-bucket?lifecycle", base_url))
        .header(CONTENT_TYPE, "application/xml")
        .body(xml_payload)
        .send()
        .await
        .unwrap();

    // Upload logs/a.txt and keep/b.txt
    client
        .put(&format!("{}/expire-bucket/logs/a.txt", base_url))
        .body("log payload to expire")
        .send()
        .await
        .unwrap();

    client
        .put(&format!("{}/expire-bucket/keep/b.txt", base_url))
        .body("important permanent document")
        .send()
        .await
        .unwrap();

    // Advance clock past 7 days (e.g. now + 8 days = 8 * 86_400_000 ms)
    let base_created = 1_700_000_000_000u64;
    let simulated_now = base_created + 8 * 86_400_000;

    // Manually set created_at on both objects to base_created
    {
        let mut engine = state.engine.write();
        let b_id = engine.lookup_bucket("expire-bucket").unwrap();
        if let Some(bucket) = engine.buckets.get(&b_id) {
            let row_a = *bucket.key_index.get(b"logs/a.txt".as_ref()).unwrap();
            let row_b = *bucket.key_index.get(b"keep/b.txt".as_ref()).unwrap();
            engine.table.created_ats[row_a] = base_created;
            engine.table.created_ats[row_b] = base_created;
        }
    }

    // Run sweep with clean prefix check
    set_fault_disable_prefix_check(false);
    run_lifecycle_sweep(&state, &state.lifecycle, simulated_now);

    // Verify logs/a.txt was expired (404 NoSuchKey)
    let res_a = client.get(&format!("{}/expire-bucket/logs/a.txt", base_url)).send().await.unwrap();
    assert_eq!(res_a.status(), StatusCode::NOT_FOUND);

    // Verify keep/b.txt was preserved (200 OK)
    let res_b = client.get(&format!("{}/expire-bucket/keep/b.txt", base_url)).send().await.unwrap();
    assert_eq!(res_b.status(), StatusCode::OK);
    assert_eq!(res_b.text().await.unwrap(), "important permanent document");

    // Negative Control: When prefix check is disabled, keep/b.txt is also erroneously expired!
    // Re-upload keep/b.txt
    client
        .put(&format!("{}/expire-bucket/keep/b.txt", base_url))
        .body("important permanent document")
        .send()
        .await
        .unwrap();

    {
        let mut engine = state.engine.write();
        let b_id = engine.lookup_bucket("expire-bucket").unwrap();
        if let Some(bucket) = engine.buckets.get(&b_id) {
            let row_b = *bucket.key_index.get(b"keep/b.txt".as_ref()).unwrap();
            engine.table.created_ats[row_b] = base_created;
        }
    }

    set_fault_disable_prefix_check(true);
    run_lifecycle_sweep(&state, &state.lifecycle, simulated_now);

    let res_b_fault = client.get(&format!("{}/expire-bucket/keep/b.txt", base_url)).send().await.unwrap();
    assert_eq!(res_b_fault.status(), StatusCode::NOT_FOUND); // Fault proven!

    set_fault_disable_prefix_check(false);
}

#[tokio::test]
async fn test_stale_multipart_abort_sweep() {
    let (base_url, state, _handle) = spawn_test_server().await;
    let client = reqwest::Client::new();

    client.put(&format!("{}/stale-mp-bucket", base_url)).send().await.unwrap();

    // Lifecycle rule: abort incomplete multipart uploads after 3 days
    let xml_payload = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <LifecycleConfiguration>\n  \
          <Rule>\n    \
            <ID>abort-stale-mp</ID>\n    \
            <Status>Enabled</Status>\n    \
            <Filter><Prefix>uploads/</Prefix></Filter>\n    \
            <AbortIncompleteMultipartUpload><DaysAfterInitiation>3</DaysAfterInitiation></AbortIncompleteMultipartUpload>\n  \
          </Rule>\n\
        </LifecycleConfiguration>";

    client
        .put(&format!("{}/stale-mp-bucket?lifecycle", base_url))
        .header(CONTENT_TYPE, "application/xml")
        .body(xml_payload)
        .send()
        .await
        .unwrap();

    // Initiate upload under uploads/
    let res = client
        .post(&format!("{}/stale-mp-bucket/uploads/file.tar?uploads", base_url))
        .send()
        .await
        .unwrap();
    let upload_id = res.text().await.unwrap()
        .split("<UploadId>").nth(1).unwrap()
        .split("</UploadId>").next().unwrap().to_string();

    // Stage a part
    client
        .put(&format!("{}/stale-mp-bucket/uploads/file.tar?partNumber=1&uploadId={}", base_url, upload_id))
        .body("partial chunk 1")
        .send()
        .await
        .unwrap();

    // Advance clock past 3 days (e.g. + 4 days)
    let base_time = 1_700_000_000_000u64;
    {
        let mut uploads_guard = state.multipart.uploads.write();
        if let Some(u) = uploads_guard.get_mut(&upload_id) {
            u.initiated_at = base_time;
        }
    }

    let simulated_now = base_time + 4 * 86_400_000;
    run_lifecycle_sweep(&state, &state.lifecycle, simulated_now);

    // Verify upload was aborted: ListMultipartUploads is empty
    let res_list = client.get(&format!("{}/stale-mp-bucket?uploads", base_url)).send().await.unwrap();
    assert_eq!(res_list.status(), StatusCode::OK);
    assert!(!res_list.text().await.unwrap().contains(&upload_id));

    // UploadPart on aborted upload returns 404 NoSuchUpload
    let res_part = client
        .put(&format!("{}/stale-mp-bucket/uploads/file.tar?partNumber=2&uploadId={}", base_url, upload_id))
        .body("chunk 2")
        .send()
        .await
        .unwrap();
    assert_eq!(res_part.status(), StatusCode::NOT_FOUND);
}
