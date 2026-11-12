//! CompleteMultipartUpload must not lose the upload when it fails: a client that gets a 500
//! retries with the same upload ID and the same part list.

use reqwest::header::ETAG;
use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_mpc_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + seed as usize * 7 + 13) % 251) as u8).collect()
}

#[tokio::test]
async fn a_failed_complete_can_be_retried() {
    let dir = temp_dir("retry");
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/bkt", url)).send().await.unwrap().status(), StatusCode::OK);

    let xml = c.post(format!("{}/bkt/big?uploads", url)).send().await.unwrap().text().await.unwrap();
    let upload_id = xml.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap().to_string();
    let parts = [pattern(200 * 1024, 1), pattern(100 * 1024, 2)];
    let mut complete = String::from("<CompleteMultipartUpload>");
    for (i, data) in parts.iter().enumerate() {
        let res = c
            .put(format!("{}/bkt/big?partNumber={}&uploadId={}", url, i + 1, upload_id))
            .body(data.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let etag = res.headers().get(ETAG).unwrap().to_str().unwrap().to_string();
        complete.push_str(&format!("<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>", i + 1, etag));
    }
    complete.push_str("</CompleteMultipartUpload>");

    // The manifest write fails once, as a disk error would.
    state.volume_manager.read().clone().unwrap().fail_next_write();
    let first = c.post(format!("{}/bkt/big?uploadId={}", url, upload_id)).body(complete.clone()).send().await.unwrap();
    assert_eq!(first.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(c.get(format!("{}/bkt/big", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    let retry = c.post(format!("{}/bkt/big?uploadId={}", url, upload_id)).body(complete).send().await.unwrap();
    assert_eq!(retry.status(), StatusCode::OK, "the retry must find the upload: {}", retry.text().await.unwrap_or_default());
    let body = c.get(format!("{}/bkt/big", url)).send().await.unwrap().bytes().await.unwrap();
    let mut whole = parts[0].clone();
    whole.extend_from_slice(&parts[1]);
    assert!(&body[..] == &whole[..], "object bytes after the retried complete");
}
