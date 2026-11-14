//! #111: UploadPartCopy is not implemented, and until it is, a part request that carries
//! `x-amz-copy-source` is refused with 501 NotImplemented.
//!
//! Before this, the UploadPart arm never read the header. An upload-part-copy has no body, so
//! the server stored an empty part and answered 200 with the ETag of empty input. A client
//! completing the upload with that ETag built an object missing the copied range, with every
//! step answering success.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const EMPTY_MD5: &str = "d41d8cd98f00b204e9800998ecf8427e";

fn between<'a>(s: &'a str, open: &str, close: &str) -> Option<&'a str> {
    s.split(open).nth(1).and_then(|r| r.split(close).next())
}

#[tokio::test]
async fn a_part_with_a_copy_source_is_refused_and_stores_nothing() {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_upc_{}_{}", std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    let st = Arc::new(S3ServiceState::with_data_dir(&dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, st).await.unwrap();
    let base = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();

    assert_eq!(c.put(format!("{}/bkt", base)).send().await.unwrap().status(), StatusCode::OK);
    let src = vec![7u8; 100_000];
    assert_eq!(c.put(format!("{}/bkt/src", base)).body(src.clone()).send().await.unwrap().status(), StatusCode::OK);

    let res = c.post(format!("{}/bkt/dst?uploads", base)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let xml = res.text().await.unwrap();
    let id = between(&xml, "<UploadId>", "</UploadId>").expect("an upload id").to_string();

    // UploadPartCopy, as the aws CLI sends it: no body, the source in a header.
    let res = c
        .put(format!("{}/bkt/dst?partNumber=1&uploadId={}", base, id))
        .header("x-amz-copy-source", "/bkt/src")
        .header("x-amz-copy-source-range", "bytes=0-99999")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED, "refused, not stored as an empty part");
    let etag = res.headers().get("etag").map(|v| v.to_str().unwrap().to_string());
    let body = res.text().await.unwrap();
    assert_eq!(between(&body, "<Code>", "</Code>"), Some("NotImplemented"), "{}", body);
    assert!(etag.as_deref().map_or(true, |e| !e.contains(EMPTY_MD5)), "no empty-part ETag handed back: {:?}", etag);

    // Nothing was stored for that part number.
    let parts = c.get(format!("{}/bkt/dst?uploadId={}", base, id)).send().await.unwrap().text().await.unwrap();
    assert!(!parts.contains("<Part>"), "ListParts after the refused copy: {}", parts);

    // An ordinary part on the same upload is unaffected.
    let res = c.put(format!("{}/bkt/dst?partNumber=1&uploadId={}", base, id)).body(src.clone()).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "a plain part still uploads");

    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
