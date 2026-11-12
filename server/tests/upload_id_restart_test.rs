//! Upload ids restarted at upload-0000000000000001 after a restart, and a completed multipart
//! object keeps its bytes in its upload's staging directory. A new upload after the restart
//! got the same id: its parts overwrote the completed object's part files, and aborting it
//! deleted them.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_upid_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn start(dir: &Path) -> (String, s3_server::ServerHandle) {
    let state = Arc::new(S3ServiceState::with_data_dir(dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, state).await.unwrap();
    (format!("http://{}", handle.local_addr), handle)
}

async fn initiate(c: &reqwest::Client, url: &str, key: &str) -> String {
    let xml = c.post(format!("{}/up-bkt/{}?uploads", url, key)).send().await.unwrap().text().await.unwrap();
    xml.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap().to_string()
}

#[tokio::test]
async fn a_new_upload_after_a_restart_does_not_touch_a_completed_object() {
    let dir = temp_dir("reuse");
    let c = reqwest::Client::new();
    let (url, handle) = start(&dir).await;
    assert_eq!(c.put(format!("{}/up-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let first_id = initiate(&c, &url, "done").await;
    let original = vec![0x11u8; 70_000];
    let res = c.put(format!("{}/up-bkt/done?partNumber=1&uploadId={}", url, first_id)).body(original.clone()).send().await.unwrap();
    let etag = res.headers().get("etag").unwrap().to_str().unwrap().to_string();
    let xml = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>", etag);
    assert_eq!(c.post(format!("{}/up-bkt/done?uploadId={}", url, first_id)).body(xml).send().await.unwrap().status(), StatusCode::OK);
    handle.shutdown();

    let (url, _h) = start(&dir).await;
    let second_id = initiate(&c, &url, "other").await;
    assert_ne!(second_id, first_id, "an upload id is never reused");
    let res = c.put(format!("{}/up-bkt/other?partNumber=1&uploadId={}", url, second_id)).body(vec![0x22u8; 70_000]).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(c.delete(format!("{}/up-bkt/other?uploadId={}", url, second_id)).send().await.unwrap().status(), StatusCode::NO_CONTENT);

    let res = c.get(format!("{}/up-bkt/done", url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "the completed object is still readable");
    assert!(&res.bytes().await.unwrap()[..] == &original[..], "and still has its own bytes");
}
