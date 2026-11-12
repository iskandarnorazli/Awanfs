//! #36: a PUT above `STREAM_PUT_MIN` is written to the volume as its body arrives, in 1 MiB
//! pieces, with the header sector last; the server does not hold the whole body.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState, STREAM_CHUNK, STREAM_PUT_MIN};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_stream_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn start(dir: &Path) -> (Arc<S3ServiceState>, String, s3_server::ServerHandle) {
    let state = Arc::new(S3ServiceState::with_data_dir(dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    (state, url, handle)
}

fn pattern(len: usize, seed: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + seed * 7 + 13) % 251) as u8).collect()
}

#[tokio::test]
async fn a_large_put_is_written_in_pieces_and_survives_a_restart() {
    let dir = temp_dir("big");
    let c = reqwest::Client::new();
    let (state, url, handle) = start(&dir).await;
    assert_eq!(c.put(format!("{}/st-bkt", url)).send().await.unwrap().status(), StatusCode::OK);

    let len = 20 * 1024 * 1024 + 123;
    assert!(len as u64 > STREAM_PUT_MIN);
    let data = pattern(len, 1);
    let before = state.work.snapshot();
    let res = c.put(format!("{}/st-bkt/big", url)).body(data.clone()).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let d = state.work.snapshot().since(&before);
    let total = (64 + 3 + len as u64).div_ceil(4096) * 4096;
    let pieces = (total - 4096).div_ceil(STREAM_CHUNK as u64);
    assert_eq!(d.disk_writes, pieces + 1, "1 MiB pieces, then the header sector: {:?}", d);
    assert_eq!(d.disk_write_bytes, total);
    assert_eq!(d.disk_syncs, 2, "group durability: volume and journal");
    assert_eq!((d.payload_copies, d.payload_copy_bytes), (1, len as u64), "one copy into the aligned buffers");

    let etag = res.headers().get("etag").unwrap().to_str().unwrap().to_string();
    let got = c.get(format!("{}/st-bkt/big", url)).send().await.unwrap();
    assert_eq!(got.headers().get("etag").unwrap().to_str().unwrap(), etag);
    assert!(&got.bytes().await.unwrap()[..] == &data[..]);
    handle.shutdown();
    drop(state);

    let (_state, url, _h) = start(&dir).await;
    let got = c.get(format!("{}/st-bkt/big", url)).send().await.unwrap();
    assert_eq!(got.status(), StatusCode::OK);
    assert_eq!(got.headers().get("etag").unwrap().to_str().unwrap(), etag, "same ETag (MD5) as a buffered PUT");
    assert!(&got.bytes().await.unwrap()[..] == &data[..], "bytes after the restart");
}

#[tokio::test]
async fn a_body_cut_short_stores_nothing_and_later_objects_survive_a_restart() {
    let dir = temp_dir("short");
    let c = reqwest::Client::new();
    let (state, url, handle) = start(&dir).await;
    let host = url.trim_start_matches("http://").to_string();
    assert_eq!(c.put(format!("{}/st-bkt", url)).send().await.unwrap().status(), StatusCode::OK);

    let mut s = tokio::net::TcpStream::connect(&host).await.unwrap();
    s.write_all(format!("PUT /st-bkt/cut HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\n\r\n", host, 10 << 20).as_bytes()).await.unwrap();
    s.write_all(&pattern(6 << 20, 2)).await.unwrap();
    drop(s);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(c.get(format!("{}/st-bkt/cut", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    // Written after the abandoned needle: recovery must step over its gap (#38).
    let after = pattern(6 << 20, 3);
    assert_eq!(c.put(format!("{}/st-bkt/after", url)).body(after.clone()).send().await.unwrap().status(), StatusCode::OK);
    handle.shutdown();
    drop(state);

    let (_state, url, _h) = start(&dir).await;
    assert_eq!(c.get(format!("{}/st-bkt/cut", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);
    let got = c.get(format!("{}/st-bkt/after", url)).send().await.unwrap();
    assert_eq!(got.status(), StatusCode::OK);
    assert!(&got.bytes().await.unwrap()[..] == &after[..]);
}

#[tokio::test]
async fn a_streamed_body_with_a_wrong_hash_or_md5_is_refused_and_not_stored() {
    let dir = temp_dir("hash");
    let c = reqwest::Client::new();
    let (_state, url, _h) = start(&dir).await;
    assert_eq!(c.put(format!("{}/st-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let data = pattern(5 << 20, 4);

    let res = c
        .put(format!("{}/st-bkt/sha", url))
        .header("x-amz-content-sha256", s3_server::sha256_hex(b"something else"))
        .body(data.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert!(res.text().await.unwrap().contains("XAmzContentSHA256Mismatch"));
    assert_eq!(c.get(format!("{}/st-bkt/sha", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    let res = c.put(format!("{}/st-bkt/md5", url)).header("content-md5", "1B2M2Y8AsgTpgAmY7PhCfg==").body(data.clone()).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert!(res.text().await.unwrap().contains("BadDigest"));
    assert_eq!(c.get(format!("{}/st-bkt/md5", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    // A correct hash passes.
    let res = c.put(format!("{}/st-bkt/ok", url)).header("x-amz-content-sha256", s3_server::sha256_hex(&data)).body(data.clone()).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}
