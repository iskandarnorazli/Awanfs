//! Multi-object delete (POST ?delete) logs a tombstone per key like DeleteObject. Before, it
//! removed the index rows only: every deleted object came back after a restart. Keys are XML
//! text, so "a&amp;b" names the key "a&b".

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir() -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_delobj_{}_{}", std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test]
async fn deleted_objects_stay_deleted_after_a_restart() {
    let dir = temp_dir();
    let c = reqwest::Client::new();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    assert_eq!(c.put(format!("{}/do-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    for k in ["k1", "k2", "a%26b"] {
        assert_eq!(c.put(format!("{}/do-bkt/{}", url, k)).body("data").send().await.unwrap().status(), StatusCode::OK);
    }
    let body = "<Delete><Object><Key>k1</Key></Object><Object><Key>a&amp;b</Key></Object></Delete>";
    let res = c.post(format!("{}/do-bkt?delete", url)).body(body).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let xml = res.text().await.unwrap();
    assert!(xml.contains("<Key>k1</Key>") && !xml.contains("<Error>"), "{}", xml);
    for k in ["k1", "a%26b"] {
        assert_eq!(c.get(format!("{}/do-bkt/{}", url, k)).send().await.unwrap().status(), StatusCode::NOT_FOUND, "{} deleted", k);
    }
    handle.shutdown();
    drop(state);

    let handle = start_s3_reactor(addr, Arc::new(S3ServiceState::with_data_dir(&dir))).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    for k in ["k1", "a%26b"] {
        assert_eq!(c.get(format!("{}/do-bkt/{}", url, k)).send().await.unwrap().status(), StatusCode::NOT_FOUND, "{} stays deleted after a restart", k);
    }
    assert_eq!(c.get(format!("{}/do-bkt/k2", url)).send().await.unwrap().text().await.unwrap(), "data", "k2 was not in the request");
}
