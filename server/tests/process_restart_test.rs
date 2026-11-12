//! Gate of AwanFS #12: start the real `s3-server` binary, PUT 50 small and 5 large objects,
//! kill the process (no shutdown), start it again on the same data directory, and read every
//! object back with its length and ETag.
//!
//! A process kill keeps what reached the page cache; it does not cover power loss, which needs
//! a disk sync before the PUT is acknowledged (#29).

use reqwest::header::ETAG;
use reqwest::StatusCode;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_proc_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

async fn start(dir: &Path) -> (Child, String) {
    let port = free_port();
    let child = Command::new(env!("CARGO_BIN_EXE_s3-server"))
        .args(["--port", &port.to_string(), "--data-dir", dir.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn s3-server");
    let url = format!("http://127.0.0.1:{}", port);
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

fn pattern(len: usize, seed: u32) -> Vec<u8> {
    (0..len).map(|i| ((i as u32 * 131 + seed * 7 + 13) % 251) as u8).collect()
}

#[tokio::test]
async fn objects_survive_a_killed_server_process() {
    let dir = temp_dir("kill");
    let c = reqwest::Client::new();
    let mut objects: Vec<(String, Vec<u8>, String)> = Vec::new();

    let (mut child, url) = start(&dir).await;
    assert_eq!(c.put(format!("{}/bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    for i in 0..55u32 {
        let (key, len) = if i < 50 { (format!("small-{}", i), 1000 + 97 * i as usize) } else { (format!("large-{}", i), 3 * 1024 * 1024 + i as usize) };
        let data = pattern(len, i);
        let res = c.put(format!("{}/bkt/{}", url, key)).body(data.clone()).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "PUT {}", key);
        let etag = res.headers().get(ETAG).unwrap().to_str().unwrap().to_string();
        objects.push((key, data, etag));
    }
    child.kill().expect("kill s3-server");
    child.wait().unwrap();

    let (mut child, url) = start(&dir).await;
    for (key, data, etag) in &objects {
        let res = c.get(format!("{}/bkt/{}", url, key)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "GET {} after the kill", key);
        assert_eq!(res.headers().get(ETAG).unwrap().to_str().unwrap(), etag, "ETag of {}", key);
        let body = res.bytes().await.unwrap();
        assert_eq!(body.len(), data.len(), "length of {}", key);
        assert!(&body[..] == &data[..], "bytes of {}", key);
    }
    child.kill().ok();
    child.wait().ok();
}
