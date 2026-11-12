//! ListObjectVersions on a bucket without versioning: every object is listed once with
//! VersionId "null" and IsLatest true, and a client can delete them by that version (#31).
//! The ceph s3-tests teardown relies on it to empty buckets.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::sync::Arc;

#[tokio::test]
async fn unversioned_objects_list_as_null_versions_and_can_be_removed() {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::new(S3ServiceState::new())).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/ver-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    for k in ["a", "b/c"] {
        c.put(format!("{}/ver-bkt/{}", url, k)).body("x").send().await.unwrap();
    }
    let xml = c.get(format!("{}/ver-bkt?versions", url)).send().await.unwrap().text().await.unwrap();
    assert!(xml.contains("<ListVersionsResult"), "{}", xml);
    assert_eq!(xml.matches("<Version>").count(), 2, "{}", xml);
    assert!(xml.contains("<Key>b/c</Key>") && xml.contains("<VersionId>null</VersionId>") && xml.contains("<IsLatest>true</IsLatest>"));

    let del = "<Delete><Object><Key>a</Key><VersionId>null</VersionId></Object><Object><Key>b/c</Key><VersionId>null</VersionId></Object></Delete>";
    assert_eq!(c.post(format!("{}/ver-bkt?delete", url)).body(del).send().await.unwrap().status(), StatusCode::OK);
    let xml = c.get(format!("{}/ver-bkt?versions", url)).send().await.unwrap().text().await.unwrap();
    assert_eq!(xml.matches("<Version>").count(), 0, "{}", xml);
    assert_eq!(c.delete(format!("{}/ver-bkt", url)).send().await.unwrap().status(), StatusCode::NO_CONTENT, "the bucket is empty");
}
