//! #155, step two: a client that goes away in the middle of a large GET releases its reader
//! and its buffers at once. Its own test binary: `readers_in_flight` counts the whole process.

use reqwest::StatusCode;
use s3_server::stream_read::readers_in_flight;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn a_client_that_leaves_mid_get_frees_its_reader() {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_pread_gone_{}_{}", std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/gbkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let body: Vec<u8> = (0..32 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    assert_eq!(c.put(format!("{}/gbkt/big", url)).body(body).send().await.unwrap().status(), StatusCode::OK);

    let host = url.trim_start_matches("http://").to_string();
    let mut s = tokio::net::TcpStream::connect(&host).await.unwrap();
    s.write_all(format!("GET /gbkt/big HTTP/1.1\r\nHost: {}\r\n\r\n", host).as_bytes()).await.unwrap();
    let mut buf = vec![0u8; 64 * 1024];
    let n = s.read(&mut buf).await.unwrap();
    assert!(n > 0 && String::from_utf8_lossy(&buf[..n.min(16)]).starts_with("HTTP/1.1 200"));
    // The reader is running, held back by a client that reads no more.
    for _ in 0..200 {
        if readers_in_flight() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(readers_in_flight(), 1);
    drop(s);
    for _ in 0..400 {
        if readers_in_flight() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(readers_in_flight(), 0, "the client went away: its reader stopped");
}
