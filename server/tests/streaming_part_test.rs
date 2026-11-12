//! #36: an UploadPart above `STREAM_PUT_MIN` is written to its staged file as it arrives, and a
//! part file only replaces an earlier one once it is complete and synced.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState, PART_CHUNK};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

fn pattern(len: usize, seed: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + seed * 7 + 13) % 251) as u8).collect()
}

async fn server() -> (Arc<S3ServiceState>, String, s3_server::ServerHandle) {
    let state = Arc::new(S3ServiceState::new());
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    (state, url, handle)
}

async fn initiate(c: &reqwest::Client, url: &str) -> String {
    assert!(c.put(format!("{}/sp-bkt", url)).send().await.unwrap().status().is_success());
    let xml = c.post(format!("{}/sp-bkt/obj?uploads", url)).send().await.unwrap().text().await.unwrap();
    xml.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap().to_string()
}

#[tokio::test]
async fn a_large_part_is_written_in_pieces() {
    let (state, url, _h) = server().await;
    let c = reqwest::Client::new();
    let id = initiate(&c, &url).await;
    let big = pattern(9 * 1024 * 1024 + 5, 1);
    let before = state.work.snapshot();
    let res = c.put(format!("{}/sp-bkt/obj?partNumber=1&uploadId={}", url, id)).body(big.clone()).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let d = state.work.snapshot().since(&before);
    assert_eq!(d.disk_writes, (big.len() as u64).div_ceil(PART_CHUNK as u64), "1 MiB pieces: {:?}", d);
    assert_eq!((d.disk_write_bytes, d.disk_syncs), (big.len() as u64, 1));
    assert_eq!((d.payload_copies, d.payload_copy_bytes), (1, big.len() as u64));
    let e1 = res.headers().get("etag").unwrap().to_str().unwrap().to_string();
    assert_eq!(e1, format!("\"{}\"", s3_server::compute_md5(&big).2), "ETag is the part's MD5");

    let small = pattern(1000, 2);
    let res = c.put(format!("{}/sp-bkt/obj?partNumber=2&uploadId={}", url, id)).body(small.clone()).send().await.unwrap();
    let e2 = res.headers().get("etag").unwrap().to_str().unwrap().to_string();
    let xml = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>", e1, e2);
    assert_eq!(c.post(format!("{}/sp-bkt/obj?uploadId={}", url, id)).body(xml).send().await.unwrap().status(), StatusCode::OK);
    let got = c.get(format!("{}/sp-bkt/obj", url)).send().await.unwrap().bytes().await.unwrap();
    assert_eq!(got.len(), big.len() + small.len());
    assert!(&got[..big.len()] == &big[..] && &got[big.len()..] == &small[..]);
}

#[tokio::test]
async fn a_failed_reupload_of_a_part_keeps_the_earlier_part() {
    let (state, url, h) = server().await;
    let c = reqwest::Client::new();
    let id = initiate(&c, &url).await;
    let first = pattern(6 * 1024 * 1024, 3);
    let res = c.put(format!("{}/sp-bkt/obj?partNumber=1&uploadId={}", url, id)).body(first.clone()).send().await.unwrap();
    let etag = res.headers().get("etag").unwrap().to_str().unwrap().to_string();

    // Re-upload part 1, but stop after 3 of 8 MiB.
    let host = h.local_addr.to_string();
    let mut s = tokio::net::TcpStream::connect(&host).await.unwrap();
    s.write_all(format!("PUT /sp-bkt/obj?partNumber=1&uploadId={} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\n\r\n", id, host, 8 << 20).as_bytes()).await.unwrap();
    s.write_all(&pattern(3 << 20, 4)).await.unwrap();
    drop(s);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let dir = state.multipart.staging_dir.join(&id);
    let leftovers: Vec<_> = std::fs::read_dir(&dir).unwrap().filter_map(|e| e.ok()).map(|e| e.file_name().to_string_lossy().to_string()).filter(|n| n.contains(".partial")).collect();
    assert!(leftovers.is_empty(), "temporary part files left behind: {:?}", leftovers);

    let xml = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>", etag);
    assert_eq!(c.post(format!("{}/sp-bkt/obj?uploadId={}", url, id)).body(xml).send().await.unwrap().status(), StatusCode::OK);
    let got = c.get(format!("{}/sp-bkt/obj", url)).send().await.unwrap().bytes().await.unwrap();
    assert!(&got[..] == &first[..], "the object is the first, acknowledged upload of part 1");
}
