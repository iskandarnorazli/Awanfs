//! #40: a full in-memory index (no data directory, so no spill index) used to panic the
//! request task on `put_object(..).unwrap()`, and the client saw a dropped connection.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, AwanStorageEngine, S3ServiceState};
use std::net::SocketAddr;
use std::sync::Arc;

#[tokio::test]
async fn a_put_past_index_capacity_gets_503_and_the_server_keeps_serving() {
    let state = S3ServiceState::new();
    *state.engine.write() = AwanStorageEngine::new(3, 65536, 65536);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::new(state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();

    assert_eq!(c.put(format!("{}/full-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    for i in 0..3 {
        assert_eq!(c.put(format!("{}/full-bkt/k{}", url, i)).body("x").send().await.unwrap().status(), StatusCode::OK);
    }

    let res = c.put(format!("{}/full-bkt/k3", url)).body("x").send().await.expect("an answer, not a dropped connection");
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(res.text().await.unwrap().contains("SlowDown"));

    let res = c
        .put(format!("{}/full-bkt/k4", url))
        .header("x-amz-copy-source", "/full-bkt/k0")
        .send()
        .await
        .expect("an answer to the copy");
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE, "CopyObject past capacity");

    let res = c.post(format!("{}/full-bkt/mp?uploads", url)).send().await.unwrap();
    let xml = res.text().await.unwrap();
    let id = xml.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap().to_string();
    let res = c.put(format!("{}/full-bkt/mp?partNumber=1&uploadId={}", url, id)).body("part").send().await.unwrap();
    let etag = res.headers().get("etag").unwrap().to_str().unwrap().to_string();
    let body = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>", etag);
    let res = c.post(format!("{}/full-bkt/mp?uploadId={}", url, id)).body(body).send().await.expect("an answer to the complete");
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE, "CompleteMultipartUpload past capacity");

    // Still serving: existing objects read back and overwrites of indexed keys work.
    assert_eq!(c.get(format!("{}/full-bkt/k1", url)).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{}/full-bkt/k1", url)).body("y").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.get(format!("{}/full-bkt/k1", url)).send().await.unwrap().text().await.unwrap(), "y");
}
