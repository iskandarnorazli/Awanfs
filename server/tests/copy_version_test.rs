//! #139: CopyObject with `x-amz-copy-source: bucket/key?versionId=<id>` copies that version.
//! Before the fix the `?versionId=` was cut off and the current version was copied with a 200,
//! so a client asking for an old version silently got the newest one.
//!
//! The lookup is the one a versioned GET uses (`find_version`), so the two cannot disagree:
//! an id that does not parse or names no version answers 404 `NoSuchVersion`, as a GET does;
//! an id that names a delete marker answers 400 `InvalidRequest`, as S3 does for a copy.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_copyver_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct Srv {
    base: String,
    handle: s3_server::ServerHandle,
    dir: PathBuf,
    c: reqwest::Client,
}

impl Srv {
    async fn new(name: &str) -> Self {
        let dir = temp_dir(name);
        let st = Arc::new(S3ServiceState::with_data_dir(&dir));
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let handle = start_s3_reactor(addr, st).await.unwrap();
        Srv { base: format!("http://{}", handle.local_addr), handle, dir, c: reqwest::Client::new() }
    }

    async fn versioned_bucket(&self, b: &str) {
        assert_eq!(self.c.put(format!("{}/{}", self.base, b)).send().await.unwrap().status(), StatusCode::OK);
        let body = "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
        let res = self.c.put(format!("{}/{}?versioning", self.base, b)).body(body).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "enable versioning");
    }

    /// PUT and return the version id.
    async fn put(&self, path: &str, body: &[u8]) -> String {
        let res = self.c.put(format!("{}/{}", self.base, path)).body(body.to_vec()).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "PUT {}", path);
        res.headers()["x-amz-version-id"].to_str().unwrap().to_string()
    }

    async fn copy(&self, dst: &str, source: &str) -> reqwest::Response {
        self.c.put(format!("{}/{}", self.base, dst)).header("x-amz-copy-source", source).send().await.unwrap()
    }

    async fn get(&self, path: &str) -> (StatusCode, Vec<u8>) {
        let res = self.c.get(format!("{}/{}", self.base, path)).send().await.unwrap();
        (res.status(), res.bytes().await.unwrap().to_vec())
    }

    fn done(self) {
        self.handle.shutdown();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn error_code(body: &str) -> &str {
    body.split("<Code>").nth(1).and_then(|r| r.split("</Code>").next()).unwrap_or("")
}

#[tokio::test]
async fn a_copy_names_its_source_version_and_gets_that_version() {
    let s = Srv::new("named").await;
    s.versioned_bucket("vbkt").await;
    let v1 = s.put("vbkt/src", b"first version").await;
    let v2 = s.put("vbkt/src", b"second version").await;
    assert_ne!(v1, v2);

    let res = s.copy("vbkt/from-v1", &format!("/vbkt/src?versionId={}", v1)).await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()["x-amz-copy-source-version-id"].to_str().unwrap(), v1, "says which version it copied");
    assert_eq!(s.get("vbkt/from-v1").await, (StatusCode::OK, b"first version".to_vec()), "the named version, not the current one");

    let res = s.copy("vbkt/from-v2", &format!("vbkt/src?versionId={}", v2)).await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(s.get("vbkt/from-v2").await.1, b"second version");

    // Without a version id the current version is copied, as before.
    assert_eq!(s.copy("vbkt/current", "vbkt/src").await.status(), StatusCode::OK);
    assert_eq!(s.get("vbkt/current").await.1, b"second version");
    s.done();
}

#[tokio::test]
async fn an_old_version_can_be_copied_when_the_key_is_deleted() {
    let s = Srv::new("marker").await;
    s.versioned_bucket("vbkt").await;
    let v1 = s.put("vbkt/src", b"kept").await;
    let res = s.c.delete(format!("{}/vbkt/src", s.base)).send().await.unwrap();
    let marker = res.headers()["x-amz-version-id"].to_str().unwrap().to_string();
    assert_eq!(s.get("vbkt/src").await.0, StatusCode::NOT_FOUND, "the key is deleted");

    // The current version is a delete marker, but the old one is still there by id.
    let res = s.copy("vbkt/restored", &format!("vbkt/src?versionId={}", v1)).await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(s.get("vbkt/restored").await.1, b"kept");

    // Naming the delete marker itself is refused.
    let res = s.copy("vbkt/x", &format!("vbkt/src?versionId={}", marker)).await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&res.text().await.unwrap()), "InvalidRequest");
    s.done();
}

#[tokio::test]
async fn an_id_that_names_no_version_answers_no_such_version() {
    let s = Srv::new("missing").await;
    s.versioned_bucket("vbkt").await;
    s.put("vbkt/src", b"only").await;
    for bad in ["0000000000000001", "zzz"] {
        let res = s.copy("vbkt/x", &format!("vbkt/src?versionId={}", bad)).await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND, "versionId={}", bad);
        assert_eq!(error_code(&res.text().await.unwrap()), "NoSuchVersion", "versionId={}", bad);
    }
    assert_eq!(s.get("vbkt/x").await.0, StatusCode::NOT_FOUND, "nothing was written");
    s.done();
}
