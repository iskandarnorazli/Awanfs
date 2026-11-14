//! #154: a streamed PUT (above `STREAM_PUT_MIN`) takes its key's lock only to commit, so a
//! client that stops sending mid-body blocks nobody. Its offset was reserved before the body
//! arrived, and recovery replays an unversioned key in offset order, so when another record
//! of the key was logged meanwhile, the object is written again at the end of the log
//! (`verification/tla/ObjectVersions.tla`, `Streamers` and `Relocate`). Every case here ends
//! with a restart: what a GET shows must be the same before and after.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState, STREAM_PUT_MIN};
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
    let dir = std::env::temp_dir().join(format!("awanfs_stalled_{}_{}_{}", name, std::process::id(), nanos));
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

/// A PUT of `body` to `path` that has sent its headers and the first half of its body, and
/// then waits. `finish` sends the rest and returns the status code.
struct Stalled {
    stream: TcpStream,
    rest: Vec<u8>,
}

async fn begin_stalled(url: &str, path: &str, body: &[u8], extra_headers: &str) -> Stalled {
    assert!(body.len() as u64 > STREAM_PUT_MIN, "the body must take the streamed path");
    let host = url.trim_start_matches("http://").to_string();
    let mut stream = TcpStream::connect(&host).await.unwrap();
    let head = format!("PUT {} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n", path, host, body.len(), extra_headers);
    stream.write_all(head.as_bytes()).await.unwrap();
    let half = body.len() / 2;
    stream.write_all(&body[..half]).await.unwrap();
    stream.flush().await.unwrap();
    // Let the server take the request and start writing the body to the volume.
    tokio::time::sleep(Duration::from_millis(300)).await;
    Stalled { stream, rest: body[half..].to_vec() }
}

impl Stalled {
    async fn finish(mut self) -> u16 {
        self.stream.write_all(&self.rest).await.unwrap();
        self.stream.flush().await.unwrap();
        let mut buf = Vec::new();
        tokio::time::timeout(WAIT, self.stream.read_to_end(&mut buf)).await.expect("the stalled PUT never answered").unwrap();
        let text = String::from_utf8_lossy(&buf);
        let status = text.split_whitespace().nth(1).unwrap_or_else(|| panic!("no status line in {:?}", text));
        status.parse().unwrap()
    }
}

async fn put(c: &reqwest::Client, url: &str, body: Vec<u8>) -> StatusCode {
    tokio::time::timeout(WAIT, c.put(url).body(body).send()).await.expect("a PUT of the same key waited for the stalled one").unwrap().status()
}

async fn body_of(c: &reqwest::Client, url: &str) -> Option<Vec<u8>> {
    let res = c.get(url).send().await.unwrap();
    match res.status() {
        StatusCode::OK => Some(res.bytes().await.unwrap().to_vec()),
        StatusCode::NOT_FOUND => None,
        other => panic!("GET {}: {}", url, other),
    }
}

async fn make_bucket(c: &reqwest::Client, url: &str) {
    assert_eq!(c.put(format!("{}/stall-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
}

#[tokio::test]
async fn a_stalled_put_does_not_block_other_writers_and_the_last_commit_wins_after_a_restart() {
    let dir = temp_dir("put");
    let c = reqwest::Client::new();
    let (state, url, handle) = start(&dir).await;
    make_bucket(&c, &url).await;
    let key = format!("{}/stall-bkt/k", url);
    let (first, second, small) = (pattern(LEN, 1), pattern(LEN + 4096, 2), pattern(1000, 3));

    let stalled = begin_stalled(&url, "/stall-bkt/k", &first, "").await;
    // A streamed PUT and a buffered PUT of the same key both finish while the first waits.
    assert_eq!(put(&c, &key, second.clone()).await, StatusCode::OK);
    assert!(body_of(&c, &key).await.as_deref() == Some(&second[..]));
    assert_eq!(put(&c, &key, small.clone()).await, StatusCode::OK);
    assert!(body_of(&c, &key).await.as_deref() == Some(&small[..]));
    assert_eq!(state.stream_relocations.load(Ordering::SeqCst), 0, "nothing was logged after those two reserved");

    // The first PUT commits last: it is the object now...
    assert_eq!(stalled.finish().await, 200);
    assert!(body_of(&c, &key).await.as_deref() == Some(&first[..]), "the last PUT to commit is the object");
    let relocations = state.stream_relocations.load(Ordering::SeqCst);
    handle.shutdown();
    drop(state);

    // ...and after a restart, although its first needle sits before the other two in the log.
    // This is the assertion that matters; the count after it says how it was kept true.
    let (_state, url, _h) = start(&dir).await;
    let key = format!("{}/stall-bkt/k", url);
    assert!(body_of(&c, &key).await.as_deref() == Some(&first[..]), "the same object after the restart");
    assert_eq!(relocations, 1, "its needle was reserved before the other two were logged");
}

#[tokio::test]
async fn a_delete_during_a_stalled_put_does_not_win_after_a_restart() {
    let dir = temp_dir("delete");
    let c = reqwest::Client::new();
    let (state, url, handle) = start(&dir).await;
    make_bucket(&c, &url).await;
    let key = format!("{}/stall-bkt/k", url);
    let first = pattern(LEN, 4);

    let stalled = begin_stalled(&url, "/stall-bkt/k", &first, "").await;
    assert_eq!(put(&c, &key, pattern(2000, 5)).await, StatusCode::OK);
    let res = tokio::time::timeout(WAIT, c.delete(&key).send()).await.expect("a DELETE waited for the stalled PUT").unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert!(body_of(&c, &key).await.is_none());

    assert_eq!(stalled.finish().await, 200);
    assert!(body_of(&c, &key).await.as_deref() == Some(&first[..]));
    let relocations = state.stream_relocations.load(Ordering::SeqCst);
    handle.shutdown();
    drop(state);

    let (_state, url, _h) = start(&dir).await;
    let key = format!("{}/stall-bkt/k", url);
    assert!(body_of(&c, &key).await.as_deref() == Some(&first[..]), "the tombstone must not win after the restart");
    assert_eq!(relocations, 1, "a tombstone was logged after the reservation");
}

#[tokio::test]
async fn an_undisturbed_streamed_put_is_written_once() {
    let dir = temp_dir("once");
    let c = reqwest::Client::new();
    let (state, url, _h) = start(&dir).await;
    make_bucket(&c, &url).await;
    // Other keys are written while it is in flight; only records of its own key matter.
    let stalled = begin_stalled(&url, "/stall-bkt/alone", &pattern(LEN, 6), "").await;
    for i in 0..20 {
        assert_eq!(put(&c, &format!("{}/stall-bkt/other-{}", url, i), pattern(3000, i)).await, StatusCode::OK);
    }
    assert_eq!(stalled.finish().await, 200);
    assert_eq!(state.stream_relocations.load(Ordering::SeqCst), 0);
    assert_eq!(state.streams.in_flight(), 0, "a finished PUT is no longer tracked");
}

#[tokio::test]
async fn an_abandoned_streamed_put_is_no_longer_tracked() {
    let dir = temp_dir("abandoned");
    let c = reqwest::Client::new();
    let (state, url, _h) = start(&dir).await;
    make_bucket(&c, &url).await;
    let stalled = begin_stalled(&url, "/stall-bkt/gone", &pattern(LEN, 7), "").await;
    assert_eq!(state.streams.in_flight(), 1);
    drop(stalled);
    for _ in 0..100 {
        if state.streams.in_flight() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(state.streams.in_flight(), 0, "the client went away: its ticket is dropped with the request");
    assert!(body_of(&c, &format!("{}/stall-bkt/gone", url)).await.is_none());
}

#[tokio::test]
async fn if_none_match_is_decided_at_commit() {
    let dir = temp_dir("inm");
    let c = reqwest::Client::new();
    let (state, url, handle) = start(&dir).await;
    make_bucket(&c, &url).await;
    let key = format!("{}/stall-bkt/once", url);
    let (first, second) = (pattern(LEN, 8), pattern(LEN + 1, 9));

    // Both start while the key does not exist; the one that commits first creates it.
    let stalled = begin_stalled(&url, "/stall-bkt/once", &first, "If-None-Match: *\r\n").await;
    let res = tokio::time::timeout(WAIT, c.put(&key).header("if-none-match", "*").body(second.clone()).send()).await.expect("waited").unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(stalled.finish().await, 412, "the key exists by the time the first PUT commits");
    assert!(body_of(&c, &key).await.as_deref() == Some(&second[..]));
    handle.shutdown();
    drop(state);

    let (_state, url, _h) = start(&dir).await;
    assert!(body_of(&c, &format!("{}/stall-bkt/once", url)).await.as_deref() == Some(&second[..]), "the refused PUT left nothing recovery could find");
}

#[tokio::test]
async fn in_a_versioned_bucket_the_last_commit_is_the_current_version_and_nothing_is_copied() {
    let dir = temp_dir("versioned");
    let c = reqwest::Client::new();
    let (state, url, handle) = start(&dir).await;
    make_bucket(&c, &url).await;
    let body = "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>Enabled</Status></VersioningConfiguration>";
    assert_eq!(c.put(format!("{}/stall-bkt?versioning", url)).body(body).send().await.unwrap().status(), StatusCode::OK);
    let key = format!("{}/stall-bkt/v", url);
    let (first, second) = (pattern(LEN, 10), pattern(LEN + 7, 11));

    let stalled = begin_stalled(&url, "/stall-bkt/v", &first, "").await;
    assert_eq!(put(&c, &key, second.clone()).await, StatusCode::OK);
    assert_eq!(stalled.finish().await, 200);
    assert!(body_of(&c, &key).await.as_deref() == Some(&first[..]), "the last commit is the current version");
    assert_eq!(state.stream_relocations.load(Ordering::SeqCst), 0, "versions are ordered by their ids, not by offset");
    let listing = c.get(format!("{}/stall-bkt?versions", url)).send().await.unwrap().text().await.unwrap();
    assert_eq!(listing.matches("<Version>").count(), 2, "{}", listing);
    handle.shutdown();
    drop(state);

    let (_state, url, _h) = start(&dir).await;
    assert!(body_of(&c, &format!("{}/stall-bkt/v", url)).await.as_deref() == Some(&first[..]), "the same current version after the restart");
    let listing = c.get(format!("{}/stall-bkt?versions", url)).send().await.unwrap().text().await.unwrap();
    assert_eq!(listing.matches("<Version>").count(), 2, "{}", listing);
}
