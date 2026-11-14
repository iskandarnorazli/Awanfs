//! #155, decision 46: an object above 4 MiB is verified while it is sent, not before.
//! A GET no longer reads the whole object before the first byte, and a range reads only the
//! range. A corrupted object still cannot be received whole: the transfer fails.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const MIB: usize = 1024 * 1024;

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_large_get_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn start(name: &str) -> (PathBuf, Arc<S3ServiceState>, String, s3_server::ServerHandle) {
    let dir = temp_dir(name);
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    assert_eq!(reqwest::Client::new().put(format!("{}/lg-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    (dir, state, url, handle)
}

fn pattern(len: usize, seed: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + seed * 7 + 13) % 251) as u8).collect()
}

async fn get_range(c: &reqwest::Client, url: &str, first: usize, last: usize) -> (StatusCode, Vec<u8>) {
    let res = c.get(url).header("range", format!("bytes={}-{}", first, last)).send().await.unwrap();
    let status = res.status();
    (status, res.bytes().await.unwrap().to_vec())
}

#[tokio::test]
async fn a_range_of_a_large_object_verifies_nothing_and_a_full_get_verifies_it_once() {
    let (_dir, state, url, _h) = start("work").await;
    let c = reqwest::Client::new();
    let data = pattern(12 * MIB + 77, 1);
    let key = format!("{}/lg-bkt/big", url);
    assert_eq!(c.put(&key).body(data.clone()).send().await.unwrap().status(), StatusCode::OK);

    let verified = state.work.payload_verified();
    let (status, got) = get_range(&c, &key, 5 * MIB, 6 * MIB - 1).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert!(got[..] == data[5 * MIB..6 * MIB]);
    assert_eq!(state.work.payload_verified() - verified, 0, "a range must not checksum the whole object first");

    let res = c.head(&key).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers().get("content-length").unwrap().to_str().unwrap(), data.len().to_string());
    assert_eq!(state.work.payload_verified() - verified, 0, "HEAD reads no payload");

    let before = state.work.snapshot();
    let res = c.get(&key).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers().get("content-length").unwrap().to_str().unwrap(), data.len().to_string());
    assert!(res.bytes().await.unwrap()[..] == data[..]);
    assert_eq!(state.work.payload_verified() - verified, data.len() as u64, "a full GET checksums each byte once, as it sends it");
    let d = state.work.snapshot().since(&before);
    assert_eq!(d.payload_copies, 0, "and still copies nothing: {:?}", d);
    assert_eq!(state.read_checksum_failures.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn an_object_up_to_4_mib_is_still_verified_before_it_is_sent() {
    let (_dir, state, url, _h) = start("small").await;
    let c = reqwest::Client::new();
    let data = pattern(300 * 1024, 2);
    let key = format!("{}/lg-bkt/small", url);
    assert_eq!(c.put(&key).body(data.clone()).send().await.unwrap().status(), StatusCode::OK);
    state.payload_cache.clear();
    let verified = state.work.payload_verified();
    let (status, got) = get_range(&c, &key, 1000, 1999).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert!(got[..] == data[1000..2000]);
    assert_eq!(state.work.payload_verified() - verified, data.len() as u64, "the path for small objects is unchanged");
}

/// Bytes with no repeats to speak of (xorshift64): any 64 of them occur once.
fn noise(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect()
}

/// Flips the byte of `data[at]` in the first volume file; panics if the 64 bytes from `at`
/// are not there, or occur earlier in `data` (the flip would land somewhere else).
fn corrupt(dir: &std::path::Path, data: &[u8], at: usize) {
    assert_eq!(data.windows(64).position(|w| w == &data[at..at + 64]), Some(at), "the bytes at {} must not occur earlier", at);
    let dat = dir.join("volume_0001.dat");
    let mut f = std::fs::OpenOptions::new().read(true).write(true).open(&dat).unwrap();
    let mut head = vec![0u8; data.len() + 2 * MIB];
    let n = f.read(&mut head).unwrap();
    let pos = 4096 + head[4096..n].windows(64).position(|w| w == &data[at..at + 64]).expect("the payload is in the first volume");
    f.seek(SeekFrom::Start(pos as u64)).unwrap();
    f.write_all(&[head[pos] ^ 0xFF]).unwrap();
    f.sync_all().unwrap();
}

#[tokio::test]
async fn a_corrupted_large_object_cannot_be_received_whole() {
    for (name, at) in [("first", 10_000usize), ("middle", 3 * MIB + 5), ("last", 6 * MIB - 100)] {
        let (dir, state, url, _h) = start(name).await;
        let c = reqwest::Client::new();
        let data = noise(6 * MIB + 3, 0x9E3779B97F4A7C15);
        let key = format!("{}/lg-bkt/obj", url);
        assert_eq!(c.put(&key).body(data.clone()).send().await.unwrap().status(), StatusCode::OK);
        corrupt(&dir, &data, at);

        let res = c.get(&key).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "the headers go out before the bytes are checked");
        match res.bytes().await {
            Err(_) => {}
            Ok(b) => panic!("{}: a corrupted object was received as a complete body of {} bytes (equal to the original: {})", name, b.len(), b[..] == data[..]),
        }
        assert_eq!(state.read_checksum_failures.load(Ordering::SeqCst), 1, "{}: the failure is counted", name);

        // Decision 46: a range is not verified. One that misses the bad byte is right...
        let clean = if at < 2 * MIB { (4 * MIB, 5 * MIB - 1) } else { (0, MIB - 1) };
        let (status, got) = get_range(&c, &key, clean.0, clean.1).await;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT);
        assert!(got[..] == data[clean.0..=clean.1], "{}: a clean range", name);
        // ...and one that covers it returns it. The background scrub is what finds this.
        let (status, got) = get_range(&c, &key, at - 10, at + 10).await;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT);
        let differing = got.iter().zip(&data[at - 10..=at + 10]).filter(|(a, b)| a != b).count();
        assert_eq!(differing, 1, "{}: the range holds the flipped byte and nothing else is off", name);
        assert_eq!(state.read_checksum_failures.load(Ordering::SeqCst), 1, "{}: ranges do not count failures", name);
    }
}
