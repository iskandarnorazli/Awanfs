//! #184 and #185, from the review of #166: a streamed PUT (above `STREAM_PUT_MIN`) that finds
//! its key written meanwhile is copied to the end of the log before it is indexed.
//!
//! #184: a write of the same key that failed after its header was on disk (the journal
//! append failed) left a complete record later in the log, but was never noted, so the
//! streamed PUT was not copied and the failed write became the object after a restart.
//! #185: the copy read the object through the volume mapping, which faults every page of a
//! needle just written with direct I/O into the RSS, and ran on an executor thread.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState, STREAM_CHUNK, STREAM_PUT_MIN};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const LEN: usize = 6 * 1024 * 1024;
const WAIT: Duration = Duration::from_secs(20);

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_reloc_{}_{}_{}", name, std::process::id(), nanos));
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

struct Stalled {
    stream: TcpStream,
    rest: Vec<u8>,
}

async fn begin_stalled(state: &S3ServiceState, url: &str, path: &str, body: &[u8]) -> Stalled {
    begin_stalled_nth(state, url, path, body, 1).await
}

/// `begin_stalled` for the `nth` write in flight in volume 1.
async fn begin_stalled_nth(state: &S3ServiceState, url: &str, path: &str, body: &[u8], nth: usize) -> Stalled {
    assert!(body.len() as u64 > STREAM_PUT_MIN);
    let host = url.trim_start_matches("http://").to_string();
    let mut stream = TcpStream::connect(&host).await.unwrap();
    let head = format!("PUT {} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", path, host, body.len());
    stream.write_all(head.as_bytes()).await.unwrap();
    let half = body.len() / 2;
    stream.write_all(&body[..half]).await.unwrap();
    stream.flush().await.unwrap();
    for _ in 0..400 {
        if state.volume_writers_in_flight(1) == Some(nth) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(state.volume_writers_in_flight(1), Some(nth), "the PUT reserved its needle");
    Stalled { stream, rest: body[half..].to_vec() }
}

impl Stalled {
    async fn finish(mut self) -> u16 {
        self.stream.write_all(&self.rest).await.unwrap();
        self.stream.flush().await.unwrap();
        let mut buf = Vec::new();
        tokio::time::timeout(WAIT, self.stream.read_to_end(&mut buf)).await.expect("no answer").unwrap();
        let text = String::from_utf8_lossy(&buf);
        text.split_whitespace().nth(1).unwrap_or_else(|| panic!("no status line in {:?}", text)).parse().unwrap()
    }
}

async fn body_of(c: &reqwest::Client, url: &str) -> (StatusCode, Vec<u8>) {
    let res = c.get(url).send().await.unwrap();
    let status = res.status();
    (status, res.bytes().await.unwrap().to_vec())
}

#[tokio::test]
async fn a_write_that_failed_after_its_header_does_not_win_after_a_restart() {
    let dir = temp_dir("orphan");
    let c = reqwest::Client::new();
    let (state, url, handle) = start(&dir).await;
    assert_eq!(c.put(format!("{}/rbkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let key = format!("{}/rbkt/k", url);
    let streamed = pattern(LEN, 1);

    let stalled = begin_stalled(&state, &url, "/rbkt/k", &streamed).await;
    // The small PUT's needle is written whole, then its journal append fails: the client is
    // told it failed, and a complete record of the key sits after the streamed reservation.
    state.volume_manager.read().clone().unwrap().fail_next_index();
    let res = c.put(&key).body(pattern(2000, 2)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);

    assert_eq!(stalled.finish().await, 200);
    let (status, got) = body_of(&c, &key).await;
    assert_eq!(status, StatusCode::OK);
    assert!(got == streamed, "live: the streamed PUT is the object");
    let relocations = state.stream_relocations.load(Ordering::SeqCst);
    handle.shutdown();
    drop(state);

    let (_state, url, _h) = start(&dir).await;
    let (status, got) = body_of(&c, &format!("{}/rbkt/k", url)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(got == streamed, "after a restart too: the write the client was told failed must not win");
    assert_eq!(relocations, 1, "the failed write was noted, so the streamed PUT was copied");
}

#[tokio::test]
async fn a_relocation_reads_the_needle_in_chunks_not_through_the_mapping() {
    let dir = temp_dir("chunks");
    let c = reqwest::Client::new();
    let (state, url, _h) = start(&dir).await;
    assert_eq!(c.put(format!("{}/rbkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let key = format!("{}/rbkt/k", url);
    let streamed = pattern(LEN, 3);

    let stalled = begin_stalled(&state, &url, "/rbkt/k", &streamed).await;
    assert_eq!(c.put(&key).body(pattern(2000, 4)).send().await.unwrap().status(), StatusCode::OK);
    let before = state.work.snapshot();
    assert_eq!(stalled.finish().await, 200);
    let work = state.work.snapshot().since(&before);
    assert_eq!(state.stream_relocations.load(Ordering::SeqCst), 1);
    // Reading through the mapping counts no disk reads; reading in chunks counts one per chunk.
    assert!(work.disk_reads >= (LEN / STREAM_CHUNK) as u64, "the copy read the needle with aligned reads: {:?}", work);
    let (status, got) = body_of(&c, &key).await;
    assert_eq!(status, StatusCode::OK);
    assert!(got == streamed, "the copy is the object, byte for byte");
}

#[tokio::test]
async fn a_streamed_put_abandoned_before_its_header_does_not_make_another_relocate() {
    // #184, requirement 4: a reservation is not a record. Only a written header can sit after
    // another streamed PUT's needle at replay.
    let dir = temp_dir("abandoned");
    let c = reqwest::Client::new();
    let (state, url, handle) = start(&dir).await;
    assert_eq!(c.put(format!("{}/rbkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let first = pattern(LEN, 5);
    let a = begin_stalled_nth(&state, &url, "/rbkt/k", &first, 1).await;
    let b = begin_stalled_nth(&state, &url, "/rbkt/k", &pattern(LEN, 6), 2).await;
    drop(b);
    for _ in 0..400 {
        if state.volume_writers_in_flight(1) == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(state.volume_writers_in_flight(1), Some(1), "the second PUT's writer is gone");
    assert_eq!(a.finish().await, 200);
    assert_eq!(state.stream_relocations.load(Ordering::SeqCst), 0, "nothing of the key was logged after the first reservation");
    handle.shutdown();
    drop(state);
    let (_state, url, _h) = start(&dir).await;
    let (status, got) = body_of(&c, &format!("{}/rbkt/k", url)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(got == first, "after a restart: the one PUT that committed");
}
