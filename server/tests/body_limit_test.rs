//! #36: request bodies have size limits, checked before the body is read.
//! S3 limits: 5 GiB for a single PUT and for one part. XML request bodies here are capped at
//! 16 MiB.

use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn server() -> String {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::new(S3ServiceState::new())).await.unwrap();
    let a = handle.local_addr;
    std::mem::forget(handle);
    a.to_string()
}

/// Sends the head of a request whose declared body never arrives, and returns what the server
/// answers within 5 seconds.
async fn answer_to_head(addr: &str, head: &str) -> String {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(head.as_bytes()).await.unwrap();
    let mut buf = vec![0u8; 4096];
    let n = tokio::time::timeout(Duration::from_secs(5), s.read(&mut buf))
        .await
        .expect("the server answered without waiting for the body")
        .unwrap();
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

#[tokio::test]
async fn a_put_declaring_more_than_5_gib_is_refused_before_the_body_is_read() {
    let addr = server().await;
    let c = reqwest::Client::new();
    assert!(c.put(format!("http://{}/lim-bkt", addr)).send().await.unwrap().status().is_success());

    let six_gib = 6u64 << 30;
    let r = answer_to_head(&addr, &format!("PUT /lim-bkt/big HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\n\r\n", addr, six_gib)).await;
    assert!(r.starts_with("HTTP/1.1 400"), "{}", r);
    assert!(r.contains("EntityTooLarge"), "{}", r);

    let r = answer_to_head(&addr, &format!("PUT /lim-bkt/big?partNumber=1&uploadId=x HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\n\r\n", addr, six_gib)).await;
    assert!(r.starts_with("HTTP/1.1 400") && r.contains("EntityTooLarge"), "UploadPart: {}", r);

    let r = answer_to_head(&addr, &format!("POST /lim-bkt?delete HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\n\r\n", addr, 64u64 << 20)).await;
    assert!(r.starts_with("HTTP/1.1 400") && r.contains("EntityTooLarge"), "an XML body over 16 MiB: {}", r);

    // A PUT at the limit is not refused for its size (the body is not sent; the server waits).
    let mut s = tokio::net::TcpStream::connect(&addr).await.unwrap();
    s.write_all(format!("PUT /lim-bkt/ok HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\n\r\n", addr, 5u64 << 30).as_bytes()).await.unwrap();
    let mut buf = [0u8; 64];
    assert!(tokio::time::timeout(Duration::from_millis(500), s.read(&mut buf)).await.is_err(), "5 GiB exactly is allowed");
}

#[tokio::test]
async fn a_chunked_xml_body_past_the_cap_is_refused() {
    let addr = server().await;
    let c = reqwest::Client::new();
    assert!(c.put(format!("http://{}/lim-bkt", addr)).send().await.unwrap().status().is_success());
    // No Content-Length: the cap applies while the body is read.
    let mut s = tokio::net::TcpStream::connect(&addr).await.unwrap();
    s.write_all(format!("POST /lim-bkt?delete HTTP/1.1\r\nHost: {}\r\nTransfer-Encoding: chunked\r\n\r\n", addr).as_bytes()).await.unwrap();
    let chunk = vec![b'a'; 1 << 20];
    for _ in 0..20 {
        if s.write_all(format!("{:x}\r\n", chunk.len()).as_bytes()).await.is_err()
            || s.write_all(&chunk).await.is_err()
            || s.write_all(b"\r\n").await.is_err()
        {
            break; // the server answered and closed
        }
    }
    let _ = s.write_all(b"0\r\n\r\n").await;
    let mut buf = vec![0u8; 4096];
    let n = tokio::time::timeout(Duration::from_secs(10), s.read(&mut buf)).await.expect("an answer").unwrap();
    let r = String::from_utf8_lossy(&buf[..n]);
    assert!(r.starts_with("HTTP/1.1 400") && r.contains("EntityTooLarge"), "{}", r);
}
