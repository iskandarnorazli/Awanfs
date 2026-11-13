//! #31: bucket versioning (owner decision 21). Gate: three versions of one key read back by
//! `versionId` byte-identical; DELETE writes a delete marker (404 for the key, the marker in
//! ListObjectVersions) and old versions stay readable; an unknown `versionId` is 404
//! NoSuchVersion; all of it survives a killed process. Also: compaction keeps every version in
//! order, a bucket never versioned answers `null`, and Suspended writes replace the null
//! version only.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_versions_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Kills the server when dropped, also when an assertion fails.
struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Server {
    fn kill(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn start_binary(dir: &Path) -> (Server, String) {
    let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let child = Command::new(env!("CARGO_BIN_EXE_s3-server"))
        .args(["--port", &port.to_string(), "--data-dir", dir.to_str().unwrap(), "--allow-anonymous", "--auto-compaction", "off"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn s3-server");
    let url = format!("http://127.0.0.1:{}", port);
    let child = Server(child);
    let c = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if c.get(format!("{}/", url)).send().await.is_ok() {
            return (child, url);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("s3-server did not start on port {}", port);
}

async fn set_versioning(c: &reqwest::Client, url: &str, bucket: &str, status: &str) {
    let body = format!("<VersioningConfiguration><Status>{}</Status></VersioningConfiguration>", status);
    let res = c.put(format!("{}/{}?versioning", url, bucket)).body(body).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "PUT ?versioning {}", status);
}

fn version_id(res: &reqwest::Response) -> String {
    res.headers().get("x-amz-version-id").expect("x-amz-version-id").to_str().unwrap().to_string()
}

async fn put(c: &reqwest::Client, url: &str, path: &str, body: &[u8]) -> String {
    let res = c.put(format!("{}/{}", url, path)).body(body.to_vec()).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "PUT {}", path);
    version_id(&res)
}

async fn get_version(c: &reqwest::Client, url: &str, path: &str, v: &str) -> (StatusCode, Vec<u8>) {
    let res = c.get(format!("{}/{}?versionId={}", url, path, v)).send().await.unwrap();
    (res.status(), res.bytes().await.unwrap().to_vec())
}

async fn list_versions(c: &reqwest::Client, url: &str, bucket: &str) -> String {
    let res = c.get(format!("{}/{}?versions", url, bucket)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    res.text().await.unwrap()
}

/// (kind, key, version id, is latest) of each entry, in the order listed.
fn entries(xml: &str) -> Vec<(String, String, String, bool)> {
    let mut out = Vec::new();
    let mut rest = xml;
    loop {
        let v = rest.find("<Version>");
        let d = rest.find("<DeleteMarker>");
        let (kind, at, close) = match (v, d) {
            (Some(a), Some(b)) if a < b => ("Version", a, "</Version>"),
            (Some(a), None) => ("Version", a, "</Version>"),
            (_, Some(b)) => ("DeleteMarker", b, "</DeleteMarker>"),
            (None, None) => break,
        };
        let end = rest[at..].find(close).unwrap() + at;
        let item = &rest[at..end];
        let field = |name: &str| {
            let open = format!("<{}>", name);
            let s = item.find(&open).unwrap() + open.len();
            item[s..].split('<').next().unwrap().to_string()
        };
        out.push((kind.to_string(), field("Key"), field("VersionId"), field("IsLatest") == "true"));
        rest = &rest[end..];
    }
    out
}

/// What the gate checks after the delete, before and after the kill.
async fn check_after_delete(c: &reqwest::Client, url: &str, ids: &[String], marker: &str) {
    for (i, v) in ids.iter().enumerate() {
        let (status, body) = get_version(c, url, "vbkt/k", v).await;
        assert_eq!(status, StatusCode::OK, "version {}", v);
        assert_eq!(body, format!("body {}", i + 1).as_bytes(), "version {}", v);
    }
    let res = c.get(format!("{}/vbkt/k", url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND, "the key is deleted");
    assert_eq!(res.headers().get("x-amz-delete-marker").map(|h| h.to_str().unwrap()), Some("true"));
    let (status, _) = get_version(c, url, "vbkt/k", marker).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "GET of a delete marker by id");
    let (status, body) = get_version(c, url, "vbkt/k", "00000000000000000001").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(String::from_utf8_lossy(&body).contains("NoSuchVersion"));

    let listed = entries(&list_versions(c, url, "vbkt").await);
    let want: Vec<(String, String, String, bool)> = vec![
        ("DeleteMarker".into(), "k".into(), marker.to_string(), true),
        ("Version".into(), "k".into(), ids[2].clone(), false),
        ("Version".into(), "k".into(), ids[1].clone(), false),
        ("Version".into(), "k".into(), ids[0].clone(), false),
    ];
    assert_eq!(listed, want, "newest first, the marker latest");
}

#[tokio::test]
async fn three_versions_a_delete_marker_and_a_killed_process() {
    let dir = temp_dir("gate");
    let c = reqwest::Client::new();
    let (mut child, url) = start_binary(&dir).await;
    assert_eq!(c.put(format!("{}/vbkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let res = c.get(format!("{}/vbkt?versioning", url)).send().await.unwrap();
    assert!(!res.text().await.unwrap().contains("<Status>"), "a new bucket has no versioning status");
    set_versioning(&c, &url, "vbkt", "Enabled").await;
    let res = c.get(format!("{}/vbkt?versioning", url)).send().await.unwrap();
    assert!(res.text().await.unwrap().contains("<Status>Enabled</Status>"));

    let mut ids = Vec::new();
    for i in 1..=3 {
        ids.push(put(&c, &url, "vbkt/k", format!("body {}", i).as_bytes()).await);
    }
    assert!(ids[0] != ids[1] && ids[1] != ids[2] && ids[0] != "null", "{:?}", ids);
    let res = c.get(format!("{}/vbkt/k", url)).send().await.unwrap();
    assert_eq!(version_id(&res), ids[2]);
    assert_eq!(res.bytes().await.unwrap(), &b"body 3"[..]);

    let res = c.delete(format!("{}/vbkt/k", url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert_eq!(res.headers().get("x-amz-delete-marker").map(|h| h.to_str().unwrap()), Some("true"));
    let marker = version_id(&res);
    check_after_delete(&c, &url, &ids, &marker).await;

    child.kill();
    let (mut child, url) = start_binary(&dir).await;
    check_after_delete(&c, &url, &ids, &marker).await;

    // Deleting the marker brings version 3 back; deleting version 2 removes it for good.
    let res = c.delete(format!("{}/vbkt/k?versionId={}", url, marker)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert_eq!(version_id(&res), marker);
    let res = c.delete(format!("{}/vbkt/k?versionId={}", url, ids[1])).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    let after = |url: String| {
        let c = c.clone();
        let ids = ids.clone();
        async move {
            let res = c.get(format!("{}/vbkt/k", url)).send().await.unwrap();
            assert_eq!(res.bytes().await.unwrap(), &b"body 3"[..], "the marker is gone");
            let (status, body) = get_version(&c, &url, "vbkt/k", &ids[1]).await;
            assert_eq!(status, StatusCode::NOT_FOUND);
            assert!(String::from_utf8_lossy(&body).contains("NoSuchVersion"));
            let listed: Vec<String> = entries(&list_versions(&c, &url, "vbkt").await).into_iter().map(|e| e.2).collect();
            assert_eq!(listed, vec![ids[2].clone(), ids[0].clone()]);
        }
    };
    after(url.clone()).await;
    child.kill();
    let (mut child, url) = start_binary(&dir).await;
    after(url).await;
    child.kill();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_version_delete_survives_a_kill() {
    let dir = temp_dir("vdel");
    let c = reqwest::Client::new();
    let (mut child, url) = start_binary(&dir).await;
    assert_eq!(c.put(format!("{}/vbkt", url)).send().await.unwrap().status(), StatusCode::OK);
    set_versioning(&c, &url, "vbkt", "Enabled").await;
    let a = put(&c, &url, "vbkt/k", b"a").await;
    let b = put(&c, &url, "vbkt/k", b"b").await;
    let res = c.delete(format!("{}/vbkt/k?versionId={}", url, b)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    child.kill();
    let (mut child, url) = start_binary(&dir).await;
    let res = c.get(format!("{}/vbkt/k", url)).send().await.unwrap();
    assert_eq!(version_id(&res), a, "deleting the latest version makes the one before it current");
    assert_eq!(res.bytes().await.unwrap(), &b"a"[..]);
    assert_eq!(get_version(&c, &url, "vbkt/k", &b).await.0, StatusCode::NOT_FOUND);
    child.kill();
    let _ = std::fs::remove_dir_all(&dir);
}

async fn spawn(dir: &Path) -> (String, s3_server::ServerHandle, Arc<S3ServiceState>) {
    let st = Arc::new(S3ServiceState::with_data_dir(dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&st)).await.unwrap();
    (format!("http://{}", handle.local_addr), handle, st)
}

#[tokio::test]
async fn compaction_keeps_every_version_and_their_order() {
    let dir = temp_dir("compact");
    let c = reqwest::Client::new();
    let mut ids = Vec::new();
    {
        let (url, handle, st) = spawn(&dir).await;
        st.set_volume_span(64 * 1024);
        assert_eq!(c.put(format!("{}/vbkt", url)).send().await.unwrap().status(), StatusCode::OK);
        set_versioning(&c, &url, "vbkt", "Enabled").await;
        // 6 versions of 20 KB: they spread over several volumes.
        for i in 0..6 {
            ids.push(put(&c, &url, "vbkt/k", &vec![b'a' + i as u8; 20_000]).await);
        }
        // An unrelated overwrite leaves dead needles to reclaim.
        put(&c, &url, "vbkt/other", b"x").await;
        // Compact only the oldest volume: its versions are copied behind the newer ones in
        // the log, which is what replay has to put back in order.
        let volumes = std::fs::read_dir(&dir).unwrap().filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().ends_with(".dat")).count();
        assert!(volumes >= 3, "6 versions of 20 KB span {} volumes of 64 KiB", volumes);
        assert!(st.compact_oldest_volume().await.unwrap().volume_removed, "the oldest volume was compacted");
        for (i, v) in ids.iter().enumerate() {
            assert_eq!(get_version(&c, &url, "vbkt/k", v).await.1, vec![b'a' + i as u8; 20_000], "version {} after compaction", i);
        }
        handle.shutdown();
    }
    // Recovery orders versions by id, not by where compaction left their needles.
    let (url, handle, _) = spawn(&dir).await;
    let res = c.get(format!("{}/vbkt/k", url)).send().await.unwrap();
    assert_eq!(version_id(&res), ids[5], "the newest version is current after the restart");
    let listed: Vec<String> = entries(&list_versions(&c, &url, "vbkt").await).into_iter().filter(|e| e.1 == "k").map(|e| e.2).collect();
    assert_eq!(listed, ids.iter().rev().cloned().collect::<Vec<_>>());
    for (i, v) in ids.iter().enumerate() {
        assert_eq!(get_version(&c, &url, "vbkt/k", v).await.1, vec![b'a' + i as u8; 20_000], "version {} after the restart", i);
    }
    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn unversioned_and_suspended_buckets_use_the_null_version() {
    let dir = temp_dir("null");
    let c = reqwest::Client::new();
    let (url, handle, _) = spawn(&dir).await;
    assert_eq!(c.put(format!("{}/nbkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let res = c.put(format!("{}/nbkt/k", url)).body("one").send().await.unwrap();
    assert!(res.headers().get("x-amz-version-id").is_none(), "no version id before versioning is set");
    c.put(format!("{}/nbkt/k", url)).body("two").send().await.unwrap();
    assert_eq!(get_version(&c, &url, "nbkt/k", "null").await.1, b"two");
    let listed = entries(&list_versions(&c, &url, "nbkt").await);
    assert_eq!(listed, vec![("Version".to_string(), "k".to_string(), "null".to_string(), true)]);

    // Enable, write a version, suspend: new writes are the null version and replace only it.
    set_versioning(&c, &url, "nbkt", "Enabled").await;
    let v = put(&c, &url, "nbkt/k", b"three").await;
    set_versioning(&c, &url, "nbkt", "Suspended").await;
    assert_eq!(put(&c, &url, "nbkt/k", b"four").await, "null");
    assert_eq!(put(&c, &url, "nbkt/k", b"five").await, "null");
    let listed: Vec<(String, bool)> = entries(&list_versions(&c, &url, "nbkt").await).into_iter().map(|e| (e.2, e.3)).collect();
    assert_eq!(listed, vec![("null".to_string(), true), (v.clone(), false)], "one null version, the enabled-era version kept");
    assert_eq!(get_version(&c, &url, "nbkt/k", &v).await.1, b"three");
    assert_eq!(get_version(&c, &url, "nbkt/k", "null").await.1, b"five");
    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
