//! #238 (decision 65): the null version made from an object written before versioning was
//! enabled shows that object's Last-Modified, live and after a restart. Its ordering time stays
//! 0, so the versioned write after it is still the current version.

use reqwest::StatusCode;
use s3_server::clock::{Clock, ManualClock};
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// 2027-01-15T08:00:00Z, a Friday.
const T1: u64 = 1_800_000_000_000_000_000;
const SEC: u64 = 1_000_000_000;
const ENABLE: &str = "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>Enabled</Status></VersioningConfiguration>";

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_nulltime_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn start(dir: &Path, clock: Arc<ManualClock>) -> (Arc<S3ServiceState>, String, s3_server::ServerHandle) {
    let state = Arc::new(S3ServiceState::with_data_dir_and_clock(dir, clock as Arc<dyn Clock>));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    (state, url, handle)
}

/// (version id, IsLatest, LastModified) for each <Version> of the listing, in order.
async fn versions(c: &reqwest::Client, url: &str) -> Vec<(String, String, String)> {
    let xml = c.get(format!("{}/nullbkt?versions", url)).send().await.unwrap().text().await.unwrap();
    let tag = |s: &str, t: &str| s.split(&format!("<{}>", t)).nth(1).and_then(|r| r.split(&format!("</{}>", t)).next()).unwrap_or("").to_string();
    xml.split("<Version>").skip(1).map(|p| (tag(p, "VersionId"), tag(p, "IsLatest"), tag(p, "LastModified"))).collect()
}

async fn check(c: &reqwest::Client, url: &str, when: &str) {
    let list = versions(c, url).await;
    let null = list.iter().find(|(id, _, _)| id == "null").unwrap_or_else(|| panic!("{}: no null version in {:?}", when, list));
    assert_eq!(null.2, "2027-01-15T08:00:00.000Z", "{}: the null version keeps its object's write time", when);
    assert_eq!(null.1, "false", "{}: the versioned write after it is current", when);
    let newer = list.iter().find(|(id, _, _)| id != "null").unwrap();
    assert_eq!((newer.1.as_str(), newer.2.as_str()), ("true", "2027-01-15T08:01:00.000Z"), "{}", when);

    let r = c.get(format!("{}/nullbkt/k?versionId=null", url)).send().await.unwrap();
    assert_eq!(r.status(), StatusCode::OK, "{}", when);
    assert_eq!(r.headers()["last-modified"], "Fri, 15 Jan 2027 08:00:00 GMT", "{}: GET ?versionId=null", when);
    let h = c.head(format!("{}/nullbkt/k?versionId=null", url)).send().await.unwrap();
    assert_eq!(h.headers()["last-modified"], "Fri, 15 Jan 2027 08:00:00 GMT", "{}: HEAD ?versionId=null", when);
}

#[tokio::test]
async fn the_null_version_shows_its_objects_last_modified_live_and_after_a_restart() {
    let dir = temp_dir("restart");
    let c = reqwest::Client::new();
    let clock = Arc::new(ManualClock::new(T1));
    let (state, url, handle) = start(&dir, Arc::clone(&clock)).await;
    assert_eq!(c.put(format!("{}/nullbkt", url)).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{}/nullbkt/k", url)).body("before versioning").send().await.unwrap().status(), StatusCode::OK);
    let before = c.head(format!("{}/nullbkt/k", url)).send().await.unwrap();
    assert_eq!(before.headers()["last-modified"], "Fri, 15 Jan 2027 08:00:00 GMT");

    clock.set(T1 + 60 * SEC);
    assert_eq!(c.put(format!("{}/nullbkt?versioning", url)).body(ENABLE).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{}/nullbkt/k", url)).body("versioned").send().await.unwrap().status(), StatusCode::OK);
    check(&c, &url, "live").await;
    handle.shutdown();
    drop(state);

    clock.set(T1 + 120 * SEC);
    let (_state, url, _h) = start(&dir, Arc::clone(&clock)).await;
    check(&c, &url, "after a restart").await;
}
