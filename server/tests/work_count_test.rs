//! Layer 1 performance regression tests: the exact work each request type does (disk writes,
//! disk syncs, disk reads, payload copies), from `S3ServiceState::work`. Counts do not depend
//! on machine speed, so these run on every PR on both platforms.
//!
//! A count that goes up is a regression: find the new copy, write or sync before changing the
//! expected value. A count that goes down is an improvement: lower the expected value in the
//! same change.
//!
//! Syncs: in the default `group` durability mode (#29) a lone write waits for one sync of the
//! volume and one of its journal (2); concurrent writers share them (`durability_test.rs`).

use reqwest::header::{ETAG, RANGE};
use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState, WorkSnapshot};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const SECTOR: u64 = 4096;

fn needle_bytes(key_len: u64, payload_len: u64) -> u64 {
    (64 + key_len + payload_len).div_ceil(SECTOR) * SECTOR
}

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_work_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + seed as usize * 7 + 13) % 251) as u8).collect()
}

struct Server {
    state: Arc<S3ServiceState>,
    url: String,
    client: reqwest::Client,
    _handle: s3_server::ServerHandle,
}

impl Server {
    async fn start(name: &str) -> Self {
        let state = Arc::new(S3ServiceState::with_data_dir(&temp_dir(name)));
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let handle = start_s3_reactor(addr, Arc::clone(&state)).await.expect("start reactor");
        let s = Server { url: format!("http://{}", handle.local_addr), state, client: reqwest::Client::new(), _handle: handle };
        let res = s.client.put(format!("{}/bkt", s.url)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        s
    }

    fn snap(&self) -> WorkSnapshot {
        self.state.work.snapshot()
    }

    fn since(&self, before: &WorkSnapshot) -> WorkSnapshot {
        self.snap().since(before)
    }
}

fn work(disk_writes: u64, disk_write_bytes: u64, disk_syncs: u64, payload_copies: u64, payload_copy_bytes: u64) -> WorkSnapshot {
    WorkSnapshot { disk_writes, disk_write_bytes, disk_syncs, disk_reads: 0, payload_copies, payload_copy_bytes }
}

#[tokio::test]
async fn single_object_put_get_range_delete() {
    let s = Server::start("single").await;
    let payload = pattern(64 * 1024, 1);

    let before = s.snap();
    let res = s.client.put(format!("{}/bkt/k", s.url)).body(payload.clone()).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    // put_64k: one aligned volume write; the payload is copied once into the aligned buffer;
    // acknowledged after the volume and journal syncs.
    assert_eq!(s.since(&before), work(1, needle_bytes(1, 65536), 2, 1, 65536), "put_64k");

    let before = s.snap();
    let res = s.client.get(format!("{}/bkt/k", s.url)).send().await.unwrap();
    assert_eq!(&res.bytes().await.unwrap()[..], &payload[..]);
    assert_eq!(s.since(&before), WorkSnapshot::default(), "get_64k from the cache");

    s.state.payload_cache.clear();
    let before = s.snap();
    let res = s.client.get(format!("{}/bkt/k", s.url)).send().await.unwrap();
    assert_eq!(&res.bytes().await.unwrap()[..], &payload[..]);
    assert_eq!(s.since(&before), WorkSnapshot::default(), "get_64k from the volume mmap: no read call, no copy");

    s.state.payload_cache.clear();
    let before = s.snap();
    let res = s.client.get(format!("{}/bkt/k", s.url)).header(RANGE, "bytes=0-4095").send().await.unwrap();
    assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(&res.bytes().await.unwrap()[..], &payload[..4096]);
    assert_eq!(s.since(&before), WorkSnapshot::default(), "range_get_4k: a slice of the mmap");

    let before = s.snap();
    let res = s.client.delete(format!("{}/bkt/k", s.url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert_eq!(s.since(&before), work(1, needle_bytes(1, 0), 2, 0, 0), "delete: one tombstone needle, synced");
}

#[tokio::test]
async fn multipart_upload_and_reads() {
    let s = Server::start("multipart").await;
    let part = 256 * 1024u64;
    let p1 = pattern(part as usize, 2);
    let p2 = pattern(part as usize, 3);

    let res = s.client.post(format!("{}/bkt/m?uploads", s.url)).send().await.unwrap();
    let xml = res.text().await.unwrap();
    let upload_id = xml.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap().to_string();

    let mut complete = String::from("<CompleteMultipartUpload>");
    for (n, data) in [(1, &p1), (2, &p2)] {
        let before = s.snap();
        let res = s
            .client
            .put(format!("{}/bkt/m?partNumber={}&uploadId={}", s.url, n, upload_id))
            .body(data.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        // upload_part_256k: the part goes to its own staged file, written once and synced once.
        assert_eq!(s.since(&before), work(1, part, 1, 0, 0), "upload_part_256k");
        let etag = res.headers().get(ETAG).unwrap().to_str().unwrap().to_string();
        complete.push_str(&format!("<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>", n, etag));
    }
    complete.push_str("</CompleteMultipartUpload>");

    let before = s.snap();
    let res = s.client.post(format!("{}/bkt/m?uploadId={}", s.url, upload_id)).body(complete).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let d = s.since(&before);
    // complete: one manifest needle; the parts are not copied.
    assert_eq!((d.disk_writes, d.disk_syncs, d.disk_reads, d.payload_copies), (1, 2, 0, 1), "complete_multipart: {:?}", d);
    assert!(d.payload_copy_bytes < 4096, "complete_multipart copied {} bytes; only the manifest should be copied", d.payload_copy_bytes);

    let before = s.snap();
    let res = s.client.get(format!("{}/bkt/m", s.url)).header(RANGE, "bytes=0-4095").send().await.unwrap();
    assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(&res.bytes().await.unwrap()[..], &p1[..4096]);
    assert_eq!(s.since(&before), WorkSnapshot::default(), "range_get inside one part: a slice of the part's mmap");

    let before = s.snap();
    let res = s.client.get(format!("{}/bkt/m", s.url)).send().await.unwrap();
    let body = res.bytes().await.unwrap();
    assert_eq!(body.len() as u64, 2 * part);
    assert_eq!(&body[..part as usize], &p1[..]);
    assert_eq!(&body[part as usize..], &p2[..]);
    // full_multipart_get: the parts are sent one after the other from their mmaps (#36).
    assert_eq!(s.since(&before), WorkSnapshot::default(), "full_multipart_get");

    let before = s.snap();
    let res = s.client.get(format!("{}/bkt/m", s.url)).header(RANGE, format!("bytes={}-{}", part - 100, part + 99)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
    let body = res.bytes().await.unwrap();
    assert_eq!(&body[..100], &p1[part as usize - 100..]);
    assert_eq!(&body[100..], &p2[..100]);
    assert_eq!(s.since(&before), WorkSnapshot::default(), "range_get across two parts: two slices, no copy");
}

#[tokio::test]
async fn streamed_put_is_digested_while_it_is_written() {
    // #23 (decision 3A): a PUT above 4 MiB with a Content-Length streams to disk in 1 MiB
    // chunks, digested (MD5, CRC32C) as it goes: no whole-body buffer, one copy into the
    // aligned chunk buffer. 8 MiB with the key "big": 4029 bytes share sector 0 with the
    // header, the rest goes out as 8 chunk writes (the last padded to a sector), then the
    // header sector: 9 writes.
    let s = Server::start("streamed").await;
    let len = 8 * 1024 * 1024;
    let payload = pattern(len, 3);

    let before = s.snap();
    let res = s.client.put(format!("{}/bkt/big", s.url)).body(payload.clone()).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(s.since(&before), work(9, needle_bytes(3, len as u64), 2, 1, len as u64), "put_8m_streamed");

    let res = s.client.get(format!("{}/bkt/big", s.url)).send().await.unwrap();
    assert!(res.bytes().await.unwrap() == payload, "the streamed object reads back");
}
