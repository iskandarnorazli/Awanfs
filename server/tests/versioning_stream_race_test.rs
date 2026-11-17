//! #186: a streamed PUT takes its version stamp when it commits, not when it reserves its
//! offset (#154). So the bucket can be unversioned when the PUT reserves and versioned when it
//! commits, and an unversioned write of the same key can sit at a later offset than the
//! versioned one. Live, that write changed the row before the history began. Recovery met it
//! after the history began and added it as a null version at its own creation time: after a
//! restart the versions differed, and when the clock had stepped back the write came out on
//! top (`verification/tla`: `versioning_replay_by_time`, `versioning_null_time`).

use reqwest::StatusCode;
use s3_server::clock::{Clock, ManualClock};
use s3_server::{start_s3_reactor, S3ServiceState, STREAM_PUT_MIN};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const LEN: usize = 6 * 1024 * 1024;
const T0: u64 = 1_800_000_000_000_000_000;
const SEC: u64 = 1_000_000_000;
const ENABLE: &str = "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>Enabled</Status></VersioningConfiguration>";

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_vrace_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn start(dir: &Path, clock: Arc<ManualClock>) -> (Arc<S3ServiceState>, String, s3_server::ServerHandle) {
    let state = Arc::new(S3ServiceState::with_data_dir_and_clock(dir, clock as Arc<dyn Clock>));
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

/// Sends half of a streamed PUT and waits until it has reserved its needle.
async fn begin_stalled(state: &S3ServiceState, url: &str, path: &str, body: &[u8]) -> Stalled {
    assert!(body.len() as u64 > STREAM_PUT_MIN);
    let host = url.trim_start_matches("http://").to_string();
    let mut stream = TcpStream::connect(&host).await.unwrap();
    let head = format!("PUT {} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", path, host, body.len());
    stream.write_all(head.as_bytes()).await.unwrap();
    let half = body.len() / 2;
    stream.write_all(&body[..half]).await.unwrap();
    stream.flush().await.unwrap();
    for _ in 0..400 {
        if state.volume_writers_in_flight(1) == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(state.volume_writers_in_flight(1), Some(1), "the PUT reserved its needle");
    Stalled { stream, rest: body[half..].to_vec() }
}

impl Stalled {
    async fn finish(mut self) -> u16 {
        self.stream.write_all(&self.rest).await.unwrap();
        self.stream.flush().await.unwrap();
        let mut buf = Vec::new();
        tokio::time::timeout(Duration::from_secs(20), self.stream.read_to_end(&mut buf)).await.expect("no answer").unwrap();
        let text = String::from_utf8_lossy(&buf);
        text.split_whitespace().nth(1).unwrap_or_else(|| panic!("no status line in {:?}", text)).parse().unwrap()
    }
}

/// The key's versions as ListObjectVersions shows them: one line per version or marker.
async fn versions(c: &reqwest::Client, url: &str) -> Vec<String> {
    let xml = c.get(format!("{}/vbkt?versions", url)).send().await.unwrap().text().await.unwrap();
    let tag = |s: &str, t: &str| s.split(&format!("<{}>", t)).nth(1).and_then(|r| r.split(&format!("</{}>", t)).next()).unwrap_or("").to_string();
    let mut out = Vec::new();
    for (kind, open) in [("version", "<Version>"), ("marker", "<DeleteMarker>")] {
        for part in xml.split(open).skip(1) {
            out.push(format!("{} id={} latest={} modified={}", kind, tag(part, "VersionId"), tag(part, "IsLatest"), tag(part, "LastModified")));
        }
    }
    out.sort();
    out
}

#[tokio::test]
async fn an_unversioned_delete_during_a_streamed_put_does_not_win_after_the_clock_steps_back() {
    let dir = temp_dir("delete");
    let c = reqwest::Client::new();
    let clock = Arc::new(ManualClock::new(T0));
    let (state, url, handle) = start(&dir, Arc::clone(&clock)).await;
    assert_eq!(c.put(format!("{}/vbkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let key = format!("{}/vbkt/k", url);
    assert_eq!(c.put(&key).body(pattern(2000, 1)).send().await.unwrap().status(), StatusCode::OK);

    // The streamed PUT reserves while the bucket is unversioned.
    let streamed = pattern(LEN, 2);
    let stalled = begin_stalled(&state, &url, "/vbkt/k", &streamed).await;
    // An unversioned DELETE at a later offset, at T0 + 10 s.
    clock.set(T0 + 10 * SEC);
    assert_eq!(c.delete(&key).send().await.unwrap().status(), StatusCode::NO_CONTENT);
    assert_eq!(c.put(format!("{}/vbkt?versioning", url)).body(ENABLE).send().await.unwrap().status(), StatusCode::OK);
    // The clock steps back; the streamed PUT commits versioned, stamped T0.
    clock.set(T0);
    assert_eq!(stalled.finish().await, 200);
    let live = c.get(&key).send().await.unwrap();
    assert_eq!(live.status(), StatusCode::OK, "live: the streamed PUT is the current version");
    assert!(live.bytes().await.unwrap().as_ref() == streamed.as_slice());
    let before = versions(&c, &url).await;
    handle.shutdown();
    drop(state);

    clock.set(T0 + 20 * SEC);
    let (_state, url, _h) = start(&dir, Arc::clone(&clock)).await;
    let after = c.get(format!("{}/vbkt/k", url)).send().await.unwrap();
    assert_eq!(after.status(), StatusCode::OK, "after a restart the DELETE the client saw happen before the PUT must not hide it");
    assert!(after.bytes().await.unwrap().as_ref() == streamed.as_slice());
    assert_eq!(versions(&c, &url).await, before, "the same versions before and after the restart");
}

#[tokio::test]
async fn an_unversioned_put_during_a_streamed_put_lists_the_same_versions_after_a_restart() {
    let dir = temp_dir("put");
    let c = reqwest::Client::new();
    let clock = Arc::new(ManualClock::new(T0));
    let (state, url, handle) = start(&dir, Arc::clone(&clock)).await;
    assert_eq!(c.put(format!("{}/vbkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let key = format!("{}/vbkt/k", url);

    let streamed = pattern(LEN, 3);
    let stalled = begin_stalled(&state, &url, "/vbkt/k", &streamed).await;
    // An unversioned PUT at a later offset; the clock only moves forward here.
    clock.set(T0 + 10 * SEC);
    assert_eq!(c.put(&key).body(pattern(2000, 4)).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{}/vbkt?versioning", url)).body(ENABLE).send().await.unwrap().status(), StatusCode::OK);
    clock.set(T0 + 20 * SEC);
    assert_eq!(stalled.finish().await, 200);
    let live = c.get(&key).send().await.unwrap();
    assert_eq!(live.status(), StatusCode::OK);
    assert!(live.bytes().await.unwrap().as_ref() == streamed.as_slice(), "live: the streamed PUT is current");
    let before = versions(&c, &url).await;
    assert_eq!(before.len(), 2, "the unversioned PUT became the null version: {:?}", before);
    handle.shutdown();
    drop(state);

    let (_state, url, _h) = start(&dir, Arc::clone(&clock)).await;
    let after = c.get(format!("{}/vbkt/k", url)).send().await.unwrap();
    assert_eq!(after.status(), StatusCode::OK);
    assert!(after.bytes().await.unwrap().as_ref() == streamed.as_slice(), "after a restart too");
    assert_eq!(versions(&c, &url).await, before, "the null version keeps its time across a restart");
}
