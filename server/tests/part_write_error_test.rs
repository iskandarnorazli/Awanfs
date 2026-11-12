//! An UploadPart whose staged file cannot be written must not be acknowledged. Before, the
//! write error was dropped (`write_staged_part(..).ok()`), the part got a 200 and an ETag, and
//! CompleteMultipartUpload then built an object whose part had no bytes.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::sync::Arc;

#[tokio::test]
async fn a_part_that_cannot_be_staged_is_a_500_not_a_200() {
    let state = Arc::new(S3ServiceState::new());
    let staging = state.multipart.staging_dir.clone();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();

    assert_eq!(c.put(format!("{}/pw-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let xml = c.post(format!("{}/pw-bkt/obj?uploads", url)).send().await.unwrap().text().await.unwrap();
    let id = xml.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap().to_string();

    // A directory where the part file goes: creating the file fails on Linux and Windows.
    std::fs::create_dir_all(staging.join(&id).join("part_00001.dat")).unwrap();
    let res = c.put(format!("{}/pw-bkt/obj?partNumber=1&uploadId={}", url, id)).body(vec![1u8; 1000]).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR, "the part was not stored");

    let done = "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"x\"</ETag></Part></CompleteMultipartUpload>";
    let res = c.post(format!("{}/pw-bkt/obj?uploadId={}", url, id)).body(done).send().await.unwrap();
    assert!(res.status().is_client_error(), "completing with a part that was never stored: {}", res.status());
    assert_eq!(c.get(format!("{}/pw-bkt/obj", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);
}
