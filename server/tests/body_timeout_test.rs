//! #187: header and body idle timeouts.
//! A client that stops sending headers or its body must not hold a connection,
//! a volume writer, or a memory admission guard forever.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState, STREAM_PUT_MIN};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_body_timeout_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn start_with_timeouts(
    dir: &std::path::Path,
    header_idle: Duration,
    body_idle: Duration,
) -> (Arc<S3ServiceState>, String, s3_server::ServerHandle) {
    let state = Arc::new(S3ServiceState::with_data_dir(dir));
    state.set_header_idle_timeout(header_idle);
    state.set_body_idle_timeout(body_idle);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    (state, url, handle)
}

async fn read_http_response(stream: &mut TcpStream, wait: Duration) -> String {
    let mut buf = Vec::new();
    let res = tokio::time::timeout(wait, stream.read_to_end(&mut buf)).await;
    assert!(res.is_ok(), "timed out waiting for server response");
    String::from_utf8_lossy(&buf).into_owned()
}

#[tokio::test]
async fn stalled_buffered_put_returns_408_and_releases_key() {
    let dir = temp_dir("buffered");
    let (state, url, handle) = start_with_timeouts(&dir, Duration::from_secs(5), Duration::from_millis(250)).await;
    let client = reqwest::Client::new();
    assert_eq!(client.put(format!("{}/tmo-bkt", url)).send().await.unwrap().status(), StatusCode::OK);

    let host = url.trim_start_matches("http://");
    let mut stream = TcpStream::connect(host).await.unwrap();
    let head = format!(
        "PUT /tmo-bkt/small-key HTTP/1.1\r\nHost: {}\r\nContent-Length: 1024\r\nConnection: close\r\n\r\n",
        host
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(&[b'a'; 128]).await.unwrap();
    stream.flush().await.unwrap();

    let resp = read_http_response(&mut stream, Duration::from_secs(5)).await;
    assert!(resp.starts_with("HTTP/1.1 408"), "expected 408, got:\n{}", resp);
    assert!(resp.contains("<Code>RequestTimeout</Code>"), "expected RequestTimeout XML, got:\n{}", resp);

    // Nothing was committed.
    assert_eq!(client.get(format!("{}/tmo-bkt/small-key", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    // Subsequent PUT on the same key succeeds.
    assert_eq!(
        client.put(format!("{}/tmo-bkt/small-key", url)).body("hello").send().await.unwrap().status(),
        StatusCode::OK
    );
    let body = client.get(format!("{}/tmo-bkt/small-key", url)).send().await.unwrap().text().await.unwrap();
    assert_eq!(body, "hello");

    handle.shutdown();
    drop(state);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn stalled_streamed_put_returns_408_and_drops_writer_and_key_lock() {
    let dir = temp_dir("streamed");
    let (state, url, handle) = start_with_timeouts(&dir, Duration::from_secs(5), Duration::from_millis(250)).await;
    let client = reqwest::Client::new();
    assert_eq!(client.put(format!("{}/tmo-bkt", url)).send().await.unwrap().status(), StatusCode::OK);

    let total_len = (STREAM_PUT_MIN as usize) + 65536;
    let host = url.trim_start_matches("http://");
    let mut stream = TcpStream::connect(host).await.unwrap();
    let head = format!(
        "PUT /tmo-bkt/big-key HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        host, total_len
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(&vec![b'x'; 64 * 1024]).await.unwrap();
    stream.flush().await.unwrap();

    // Confirm the streamed PUT actually reserved a volume writer before timing out.
    let mut saw_writer = false;
    for _ in 0..40 {
        if state.volume_writers_in_flight(1) == Some(1) {
            saw_writer = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(saw_writer, "streamed PUT should reserve a NeedleWriter");

    let resp = read_http_response(&mut stream, Duration::from_secs(5)).await;
    assert!(resp.starts_with("HTTP/1.1 408"), "expected 408, got:\n{}", resp);
    assert!(resp.contains("<Code>RequestTimeout</Code>"), "expected RequestTimeout XML, got:\n{}", resp);

    // Writer and stream tracking must be released.
    assert_eq!(state.volume_writers_in_flight(1), Some(0), "volume_writers_in_flight must return to 0");
    assert_eq!(state.streams.in_flight(), 0, "streams in_flight must return to 0");

    // Nothing was committed.
    assert_eq!(client.get(format!("{}/tmo-bkt/big-key", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    // Subsequent PUT on the same key succeeds.
    assert_eq!(
        client.put(format!("{}/tmo-bkt/big-key", url)).body("recovered").send().await.unwrap().status(),
        StatusCode::OK
    );
    let body = client.get(format!("{}/tmo-bkt/big-key", url)).send().await.unwrap().text().await.unwrap();
    assert_eq!(body, "recovered");

    handle.shutdown();
    drop(state);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn slow_but_making_progress_put_succeeds() {
    let dir = temp_dir("progress");
    let (state, url, handle) = start_with_timeouts(&dir, Duration::from_secs(5), Duration::from_millis(300)).await;
    let client = reqwest::Client::new();
    assert_eq!(client.put(format!("{}/tmo-bkt", url)).send().await.unwrap().status(), StatusCode::OK);

    // Total upload duration: 6 * 100ms = 600ms > 300ms idle timeout, but each chunk arrives before 300ms.
    let total_len = (STREAM_PUT_MIN as usize) + 60_000;
    let host = url.trim_start_matches("http://");
    let mut stream = TcpStream::connect(host).await.unwrap();
    let head = format!(
        "PUT /tmo-bkt/slow-ok HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        host, total_len
    );
    stream.write_all(head.as_bytes()).await.unwrap();

    let chunk_size = total_len / 6;
    let mut sent = 0;
    for i in 0..6 {
        let end = if i == 5 { total_len } else { sent + chunk_size };
        let piece = vec![b'p'; end - sent];
        stream.write_all(&piece).await.unwrap();
        stream.flush().await.unwrap();
        sent = end;
        if i < 5 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    let resp = read_http_response(&mut stream, Duration::from_secs(5)).await;
    assert!(resp.starts_with("HTTP/1.1 200"), "slow-but-progressing client must succeed, got:\n{}", resp);
    assert_eq!(state.volume_writers_in_flight(1), Some(0));

    let got = client.get(format!("{}/tmo-bkt/slow-ok", url)).send().await.unwrap().bytes().await.unwrap();
    assert_eq!(got.len(), total_len);

    handle.shutdown();
    drop(state);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn stalled_upload_part_returns_408() {
    let dir = temp_dir("mpu");
    let (state, url, handle) = start_with_timeouts(&dir, Duration::from_secs(5), Duration::from_millis(250)).await;
    let client = reqwest::Client::new();
    assert_eq!(client.put(format!("{}/tmo-bkt", url)).send().await.unwrap().status(), StatusCode::OK);

    let init_resp = client.post(format!("{}/tmo-bkt/mpu-obj?uploads", url)).send().await.unwrap();
    assert_eq!(init_resp.status(), StatusCode::OK);
    let init_xml = init_resp.text().await.unwrap();
    let s = init_xml.find("<UploadId>").unwrap() + "<UploadId>".len();
    let e = init_xml[s..].find("</UploadId>").unwrap() + s;
    let upload_id = &init_xml[s..e];

    let total_len = (STREAM_PUT_MIN as usize) + 65536;
    let host = url.trim_start_matches("http://");
    let mut stream = TcpStream::connect(host).await.unwrap();
    let head = format!(
        "PUT /tmo-bkt/mpu-obj?partNumber=1&uploadId={} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        upload_id, host, total_len
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(&vec![b'm'; 64 * 1024]).await.unwrap();
    stream.flush().await.unwrap();

    let resp = read_http_response(&mut stream, Duration::from_secs(5)).await;
    assert!(resp.starts_with("HTTP/1.1 408"), "expected 408 for stalled UploadPart, got:\n{}", resp);
    assert!(resp.contains("<Code>RequestTimeout</Code>"), "expected RequestTimeout XML, got:\n{}", resp);

    handle.shutdown();
    drop(state);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn stalled_header_closes_connection() {
    let dir = temp_dir("hdr");
    let (state, url, handle) = start_with_timeouts(&dir, Duration::from_millis(250), Duration::from_secs(5)).await;
    let host = url.trim_start_matches("http://");
    let mut stream = TcpStream::connect(host).await.unwrap();
    // Send incomplete HTTP request headers (no terminating \r\n\r\n).
    stream.write_all(b"PUT /tmo-bkt/hdr HTTP/1.1\r\nHost: localhost\r\n").await.unwrap();
    stream.flush().await.unwrap();

    let mut buf = [0u8; 256];
    let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("server must close or answer on header timeout")
        .unwrap();
    // hyper closes the connection (0 bytes) or returns 408 on header_read_timeout.
    if n > 0 {
        let text = String::from_utf8_lossy(&buf[..n]);
        assert!(text.starts_with("HTTP/1.1 408"), "unexpected response on header timeout: {}", text);
    }

    handle.shutdown();
    drop(state);
    let _ = std::fs::remove_dir_all(&dir);
}
