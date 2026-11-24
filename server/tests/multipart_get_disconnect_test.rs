//! #155 requirement 5: a full GET of a large multipart object is sent by a reader on the
//! blocking pool, not from mapped parts, and a client that goes away in the middle frees that
//! reader at once. Its own test binary: `readers_in_flight` counts the whole process.

use reqwest::StatusCode;
use s3_server::stream_read::readers_in_flight;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn a_client_that_leaves_a_multipart_get_frees_its_reader() {
    let dir = std::env::temp_dir().join(format!("awanfs_mpu_gone_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/gone-bucket", url)).send().await.unwrap().status(), StatusCode::OK);

    let init = c.post(format!("{}/gone-bucket/big?uploads", url)).send().await.unwrap().text().await.unwrap();
    let id = init.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap().to_string();
    let mut done = String::from("<CompleteMultipartUpload>");
    for n in 1..=6 {
        let part: Vec<u8> = (0..5 * 1024 * 1024).map(|i| ((i + n) % 251) as u8).collect();
        let res = c.put(format!("{}/gone-bucket/big?partNumber={}&uploadId={}", url, n, id)).body(part).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let etag = res.headers().get("etag").unwrap().to_str().unwrap().to_string();
        done.push_str(&format!("<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>", n, etag));
    }
    done.push_str("</CompleteMultipartUpload>");
    assert_eq!(c.post(format!("{}/gone-bucket/big?uploadId={}", url, id)).body(done).send().await.unwrap().status(), StatusCode::OK);

    let host = url.trim_start_matches("http://").to_string();
    let mut s = tokio::net::TcpStream::connect(&host).await.unwrap();
    s.write_all(format!("GET /gone-bucket/big HTTP/1.1\r\nHost: {}\r\n\r\n", host).as_bytes()).await.unwrap();
    let mut buf = vec![0u8; 64 * 1024];
    let n = s.read(&mut buf).await.unwrap();
    assert!(n > 0 && String::from_utf8_lossy(&buf[..n.min(16)]).starts_with("HTTP/1.1 200"));
    // A reader sends the object, held back by a client that reads no more.
    for _ in 0..200 {
        if readers_in_flight() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(readers_in_flight(), 1, "a large multipart GET is sent by one reader");
    assert_eq!(state.multipart.staged_mmaps.read().len(), 0, "and maps no part");
    drop(s);
    for _ in 0..400 {
        if readers_in_flight() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(readers_in_flight(), 0, "the client went away: its reader stopped");
    let _ = std::fs::remove_dir_all(&dir);
}
