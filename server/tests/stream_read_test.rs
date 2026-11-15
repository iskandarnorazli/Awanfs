//! #155, step two: objects above 4 MiB are sent with positioned reads, not through the volume
//! mapping, whose pages stayed in the RSS (4441 MiB of file pages in run 43544670813). A full
//! GET is checked as it is sent; a range reads only the range.

use http_body_util::BodyExt;
use reqwest::StatusCode;
use s3_server::{split_offset, start_s3_reactor, S3ServiceState};
use std::io::{Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const LEN: usize = 6 * 1024 * 1024 + 4321;

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_pread_{}_{}_{}", name, std::process::id(), nanos));
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

/// Bytes that do not repeat, so a range read from the wrong place cannot match.
fn noise(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

fn remaps(state: &S3ServiceState) -> u64 {
    state.volume_manager.read().clone().unwrap().remap_count()
}

async fn put_object(c: &reqwest::Client, url: &str, key: &str, body: &[u8]) {
    assert_eq!(c.put(format!("{}/pbkt", url)).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{}/pbkt/{}", url, key)).body(body.to_vec()).send().await.unwrap().status(), StatusCode::OK);
}

#[tokio::test]
async fn a_full_get_and_ranges_of_a_large_object_do_not_use_the_mapping() {
    let dir = temp_dir("nomap");
    let c = reqwest::Client::new();
    let (state, url, _h) = start(&dir).await;
    let body = noise(LEN, 1);
    put_object(&c, &url, "big", &body).await;
    let before = remaps(&state);
    // Both read modes (owner decision 1C): direct I/O, then the page cache.
    for direct_read_min in [0, u64::MAX] {
        state.direct_read_min.store(direct_read_min, Ordering::SeqCst);

        let got = c.get(format!("{}/pbkt/big", url)).send().await.unwrap().bytes().await.unwrap();
        assert!(got[..] == body[..], "the full GET is the object (direct_read_min {})", direct_read_min);

        // Ranges across the 1 MiB read boundaries and the sector boundaries, at both ends.
        let mut x = 7u64;
        let mut ranges = vec![(0u64, 0u64), (0, 4095), (4095, 4096), ((1 << 20) - 1, 1 << 20), (LEN as u64 - 1, LEN as u64 - 1), (0, LEN as u64 - 1)];
        for _ in 0..24 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let a = (x >> 11) % LEN as u64;
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let b = (a + (x >> 11) % (3 << 20)).min(LEN as u64 - 1);
            ranges.push((a, b));
        }
        for (a, b) in ranges {
            let res = c.get(format!("{}/pbkt/big", url)).header("range", format!("bytes={}-{}", a, b)).send().await.unwrap();
            assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
            let got = res.bytes().await.unwrap();
            assert!(got[..] == body[a as usize..=b as usize], "bytes {}-{} (direct_read_min {})", a, b, direct_read_min);
        }
    }
    assert_eq!(remaps(&state), before, "no GET of the large object mapped the volume");
}

#[tokio::test]
async fn no_payload_byte_is_read_before_the_body_is_polled() {
    let dir = temp_dir("lazy");
    let c = reqwest::Client::new();
    let (state, url, _h) = start(&dir).await;
    let body = noise(LEN, 2);
    put_object(&c, &url, "big", &body).await;
    let b_id = state.engine.read().lookup_bucket("pbkt").unwrap();
    let meta = state.engine.read().get_object(b_id, b"big").unwrap();

    let before = state.work.snapshot();
    let mut stream = state.large_payload_body(b_id, "pbkt/big".into(), meta.vol_offset, meta.size, 0, meta.size, true).unwrap();
    let opened = state.work.snapshot().since(&before);
    assert_eq!(opened.disk_reads, 1, "opening reads the header sector, nothing of the payload: {:?}", opened);

    let mut got = Vec::new();
    while let Some(frame) = stream.frame().await {
        let frame = frame.unwrap();
        let data = frame.into_data().unwrap();
        assert!(data.len() <= s3_server::stream_read::READ_CHUNK, "frames are at most one read");
        got.extend_from_slice(&data);
    }
    assert!(got == body);
    let read = state.work.snapshot().since(&before);
    assert!(read.disk_reads >= 1 + (LEN / s3_server::stream_read::READ_CHUNK) as u64, "{:?}", read);
}

/// Flips one byte of the stored payload, behind the server's back.
fn corrupt(dir: &Path, local: u64, key: &str, at: u64) -> u8 {
    let mut f = std::fs::OpenOptions::new().read(true).write(true).open(dir.join("volume_0001.dat")).unwrap();
    let pos = local + 64 + key.len() as u64 + at;
    let mut byte = [0u8; 1];
    f.seek(SeekFrom::Start(pos)).unwrap();
    std::io::Read::read_exact(&mut f, &mut byte).unwrap();
    f.seek(SeekFrom::Start(pos)).unwrap();
    f.write_all(&[byte[0] ^ 0x5A]).unwrap();
    f.sync_all().unwrap();
    byte[0]
}

fn restore(dir: &Path, local: u64, key: &str, at: u64, byte: u8) {
    let mut f = std::fs::OpenOptions::new().write(true).open(dir.join("volume_0001.dat")).unwrap();
    f.seek(SeekFrom::Start(local + 64 + key.len() as u64 + at)).unwrap();
    f.write_all(&[byte]).unwrap();
    f.sync_all().unwrap();
}

#[tokio::test]
async fn a_corrupted_byte_in_the_first_a_middle_or_the_last_chunk_cuts_a_full_get() {
    let dir = temp_dir("corrupt");
    let c = reqwest::Client::new();
    let (state, url, _h) = start(&dir).await;
    let body = noise(LEN, 3);
    put_object(&c, &url, "big", &body).await;
    let b_id = state.engine.read().lookup_bucket("pbkt").unwrap();
    let (_, local) = split_offset(state.engine.read().get_object(b_id, b"big").unwrap().vol_offset);

    for (at, direct_read_min) in [10u64, 3 * 1024 * 1024 + 17, LEN as u64 - 5].into_iter().flat_map(|at| [(at, 0), (at, u64::MAX)]) {
        state.direct_read_min.store(direct_read_min, Ordering::SeqCst);
        let failures = state.read_checksum_failures.load(Ordering::SeqCst);
        let old = corrupt(&dir, local, "big", at);
        let res = c.get(format!("{}/pbkt/big", url)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "the headers went before the bytes were checked");
        match res.bytes().await {
            Ok(b) => assert!(b.len() < LEN, "a corrupted byte at {} must cut the transfer, got all {} bytes", at, b.len()),
            Err(_) => {}
        }
        assert_eq!(state.read_checksum_failures.load(Ordering::SeqCst), failures + 1, "counted, corruption at {}", at);
        restore(&dir, local, "big", at, old);
    }
    let got = c.get(format!("{}/pbkt/big", url)).send().await.unwrap().bytes().await.unwrap();
    assert!(got[..] == body[..], "restored, the object is whole again");
}
