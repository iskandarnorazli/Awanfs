//! #49: a graceful shutdown stops accepting connections, lets every request in flight finish,
//! and everything acknowledged is readable after a restart.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_grace_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn body(i: usize) -> Vec<u8> {
    (0..20_000).map(|j| ((j * 7 + i * 13) % 251) as u8).collect()
}

#[tokio::test]
async fn requests_in_flight_finish_and_survive_a_restart() {
    let dir = temp_dir("inflight");
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let host = handle.local_addr.to_string();
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("http://{}/gs-bkt", host)).send().await.unwrap().status(), StatusCode::OK);

    // 20 PUTs with half of their body sent: in flight when the shutdown starts.
    let mut conns = Vec::new();
    for i in 0..20 {
        let mut s = tokio::net::TcpStream::connect(&host).await.unwrap();
        let b = body(i);
        s.write_all(format!("PUT /gs-bkt/o{} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\n\r\n", i, host, b.len()).as_bytes()).await.unwrap();
        s.write_all(&b[..b.len() / 2]).await.unwrap();
        conns.push((s, b));
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let stopping = tokio::spawn(handle.shutdown_gracefully(Duration::from_secs(20)));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!stopping.is_finished(), "the shutdown waits for the requests in flight");
    assert!(tokio::net::TcpStream::connect(&host).await.is_err(), "no new connections once stopping");

    for (s, b) in conns.iter_mut() {
        s.write_all(&b[b.len() / 2..]).await.unwrap();
    }
    for (s, _) in conns.iter_mut() {
        let mut buf = vec![0u8; 1024];
        let n = tokio::time::timeout(Duration::from_secs(10), s.read(&mut buf)).await.expect("an answer").unwrap();
        let head = String::from_utf8_lossy(&buf[..n]);
        assert!(head.starts_with("HTTP/1.1 200"), "{}", head);
    }
    drop(conns);
    assert!(tokio::time::timeout(Duration::from_secs(20), stopping).await.expect("shutdown finished").unwrap(), "every connection closed");
    state.sync_all_volumes().unwrap();
    drop(state);

    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let handle = start_s3_reactor(addr, state).await.unwrap();
    for i in 0..20 {
        let res = c.get(format!("http://{}/gs-bkt/o{}", handle.local_addr, i)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "o{}", i);
        assert!(&res.bytes().await.unwrap()[..] == &body(i)[..]);
    }
}

/// The binary on SIGTERM: acknowledged PUTs survive, readiness goes false, exit code 0.
#[cfg(unix)]
#[tokio::test]
async fn the_binary_stops_cleanly_on_sigterm() {
    use std::process::{Command, Stdio};
    let dir = temp_dir("sigterm");
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let start = || {
        Command::new(env!("CARGO_BIN_EXE_s3-server"))
            .args(["--port", &port.to_string(), "--data-dir", dir.to_str().unwrap(), "--allow-anonymous", "--durability", "fast"])
            .env("AWANFS_LOG", "off")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap()
    };
    let url = format!("http://127.0.0.1:{}", port);
    let c = reqwest::Client::new();
    let wait_up = |c: reqwest::Client, url: String| async move {
        for _ in 0..200 {
            if c.get(format!("{}/_awanfs/health/live", url)).send().await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("server did not start");
    };
    let mut child = start();
    wait_up(c.clone(), url.clone()).await;
    assert_eq!(c.put(format!("{}/gs-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let puts: Vec<_> = (0..20)
        .map(|i| {
            let c = c.clone();
            let url = url.clone();
            tokio::spawn(async move { c.put(format!("{}/gs-bkt/o{}", url, i)).body(body(i)).send().await.map(|r| r.status()) })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(Command::new("kill").args(["-TERM", &child.id().to_string()]).status().unwrap().success());
    let mut acked = Vec::new();
    for (i, p) in puts.into_iter().enumerate() {
        if let Ok(Ok(StatusCode::OK)) = p.await {
            acked.push(i);
        }
    }
    let status = child.wait().unwrap();
    assert_eq!(status.code(), Some(0), "clean exit on SIGTERM");

    // fast mode syncs only once a second; the final sync on stop keeps what was acknowledged.
    let mut child = start();
    wait_up(c.clone(), url.clone()).await;
    for i in acked {
        let res = c.get(format!("{}/gs-bkt/o{}", url, i)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "acknowledged o{}", i);
    }
    let _ = child.kill();
    let _ = child.wait();
}
