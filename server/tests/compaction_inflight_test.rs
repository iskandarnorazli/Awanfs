//! #167: compaction must not remove a volume a write is still landing in.
//!
//! Compaction lists the needles of the oldest volume, copies the live ones forward and removes
//! the file. A write that had reserved its offset in that volume but had not written its header
//! yet was not listed, was not copied, and landed in a removed file: acknowledged with a 200,
//! then gone. A streamed PUT (above 4 MiB) keeps that window open for as long as its client
//! sends; `verification/tla` (`compact_no_wait.cfg`) shows a small PUT can hit it too.
//! The volume is now sealed before it is listed, and listed only when no write is in flight.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState, STREAM_PUT_MIN};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const MIB: usize = 1024 * 1024;
const WAIT: Duration = Duration::from_secs(20);
/// Small enough that a 6 MiB streamed PUT and a 3 MiB PUT do not fit in one volume.
const SPAN: u64 = 8 * 1024 * 1024;

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_cinflight_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn start(dir: &Path) -> (Arc<S3ServiceState>, String, s3_server::ServerHandle) {
    let state = Arc::new(S3ServiceState::with_data_dir(dir));
    state.set_volume_span(SPAN);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    (state, url, handle)
}

fn pattern(len: usize, seed: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + seed * 7 + 13) % 251) as u8).collect()
}

/// A PUT that has sent its headers and half its body, and waits.
struct Stalled {
    stream: TcpStream,
    rest: Vec<u8>,
}

async fn begin_stalled(state: &S3ServiceState, url: &str, path: &str, body: &[u8]) -> Stalled {
    assert!(body.len() as u64 > STREAM_PUT_MIN, "the body must take the streamed path");
    let host = url.trim_start_matches("http://").to_string();
    let mut stream = TcpStream::connect(&host).await.unwrap();
    let head = format!("PUT {} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", path, host, body.len());
    stream.write_all(head.as_bytes()).await.unwrap();
    let half = body.len() / 2;
    stream.write_all(&body[..half]).await.unwrap();
    stream.flush().await.unwrap();
    // Wait for the reservation itself, not for a fixed time (a slow runner reserves late).
    for _ in 0..400 {
        if state.volume_writers_in_flight(1) == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(state.volume_writers_in_flight(1), Some(1), "the PUT reserved its needle in volume 1");
    Stalled { stream, rest: body[half..].to_vec() }
}

impl Stalled {
    async fn finish(mut self) -> u16 {
        self.stream.write_all(&self.rest).await.unwrap();
        self.stream.flush().await.unwrap();
        let mut buf = Vec::new();
        tokio::time::timeout(WAIT, self.stream.read_to_end(&mut buf)).await.expect("the stalled PUT never answered").unwrap();
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
async fn compaction_waits_for_a_streamed_put_still_writing_into_the_volume() {
    let dir = temp_dir("stream");
    let c = reqwest::Client::new();
    let (state, url, handle) = start(&dir).await;
    assert_eq!(c.put(format!("{}/cbkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let big = pattern(6 * MIB, 1);
    let key = format!("{}/cbkt/big", url);

    // The streamed PUT reserves its needle in volume 1 and waits.
    let stalled = begin_stalled(&state, &url, "/cbkt/big", &big).await;
    // A 3 MiB PUT does not fit in what is left of volume 1: volume 2 becomes the active one,
    // and volume 1, still receiving the stalled body, is now the oldest that is not active.
    assert_eq!(c.put(format!("{}/cbkt/small", url)).body(pattern(3 * MIB, 2)).send().await.unwrap().status(), StatusCode::OK);
    assert!(state.volume_ids().len() >= 2, "the second PUT rolled the volume: {:?}", state.volume_ids());

    let report = state.compact_oldest_volume().await.unwrap();
    assert!(!report.volume_removed, "a volume with a write in flight must not be removed: {:?}", report);
    assert!(report.busy, "the report says why nothing was done: {:?}", report);

    assert_eq!(stalled.finish().await, 200);
    let (status, got) = body_of(&c, &key).await;
    assert_eq!(status, StatusCode::OK);
    assert!(got == big, "the object the PUT acknowledged is the object");

    // Once the write has ended, the next pass compacts the volume, and the object survives it.
    let report = state.compact_oldest_volume().await.unwrap();
    assert!(!report.busy, "{:?}", report);
    assert!(report.live_needles_copied >= 1, "{:?}", report);
    let (status, got) = body_of(&c, &key).await;
    assert_eq!(status, StatusCode::OK);
    assert!(got == big);
    handle.shutdown();
    drop(state);

    let (_state, url, _h) = start(&dir).await;
    let (status, got) = body_of(&c, &format!("{}/cbkt/big", url)).await;
    assert_eq!(status, StatusCode::OK, "the acknowledged object is still there after a restart");
    assert!(got == big);
}

#[tokio::test]
async fn an_abandoned_streamed_put_does_not_hold_compaction_back() {
    let dir = temp_dir("abandoned");
    let c = reqwest::Client::new();
    let (state, url, _h) = start(&dir).await;
    assert_eq!(c.put(format!("{}/cbkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let stalled = begin_stalled(&state, &url, "/cbkt/gone", &pattern(6 * MIB, 3)).await;
    assert_eq!(c.put(format!("{}/cbkt/small", url)).body(pattern(3 * MIB, 4)).send().await.unwrap().status(), StatusCode::OK);
    assert!(state.compact_oldest_volume().await.unwrap().busy);

    drop(stalled);
    let mut report = state.compact_oldest_volume().await.unwrap();
    for _ in 0..100 {
        if !report.busy {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        report = state.compact_oldest_volume().await.unwrap();
    }
    assert!(!report.busy, "the client went away, so its write ended: {:?}", report);
    assert!(report.volume_removed, "{:?}", report);
    assert_eq!(body_of(&c, &format!("{}/cbkt/gone", url)).await.0, StatusCode::NOT_FOUND);
    assert_eq!(body_of(&c, &format!("{}/cbkt/small", url)).await.0, StatusCode::OK);
}

/// The reviewer's case (#182): an abandoned streamed PUT leaves a gap with no header, and a
/// later PUT lands after it in the same volume. The scan used to stop at the gap (the journal
/// read at open knows nothing of this process's writes), so compaction never listed the later
/// object and removed the file it was in.
#[tokio::test]
async fn an_object_after_an_abandoned_put_in_the_same_volume_survives_compaction() {
    let dir = temp_dir("after_gap");
    let c = reqwest::Client::new();
    let (state, url, handle) = start(&dir).await;
    assert_eq!(c.put(format!("{}/cbkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let stalled = begin_stalled(&state, &url, "/cbkt/gone", &pattern(5 * MIB, 5)).await;
    drop(stalled);
    for _ in 0..400 {
        if state.volume_writers_in_flight(1) == Some(0) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(state.volume_writers_in_flight(1), Some(0), "the abandoned writer is gone");

    // 1 MiB fits after the 5 MiB gap in volume 1; 3 MiB does not, and rolls to volume 2.
    let after = pattern(MIB, 6);
    assert_eq!(c.put(format!("{}/cbkt/after", url)).body(after.clone()).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{}/cbkt/roll", url)).body(pattern(3 * MIB, 7)).send().await.unwrap().status(), StatusCode::OK);
    assert!(state.volume_ids().len() >= 2, "{:?}", state.volume_ids());

    let report = state.compact_oldest_volume().await.unwrap();
    assert!(!report.busy, "{:?}", report);
    assert!(report.live_needles_copied >= 1, "the object after the gap was listed and copied: {:?}", report);
    let (status, got) = body_of(&c, &format!("{}/cbkt/after", url)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(got == after);
    handle.shutdown();
    drop(state);

    let (_state, url, _h) = start(&dir).await;
    let (status, got) = body_of(&c, &format!("{}/cbkt/after", url)).await;
    assert_eq!(status, StatusCode::OK, "and after a restart");
    assert!(got == after);
}

/// The reviewer's second case (#182): a versioned DELETE writes its marker, then records it in
/// the history, holding the key lock throughout. Compaction decided on delete markers before
/// taking that lock, saw a marker no history held yet, and dropped it with the volume: after a
/// restart the delete was gone. The DELETE is held between the two steps while compaction runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_marker_written_but_not_yet_recorded_survives_compaction() {
    use std::sync::atomic::{AtomicU8, Ordering};
    let dir = temp_dir("marker");
    let c = reqwest::Client::new();
    let (state, url, handle) = start(&dir).await;
    assert_eq!(c.put(format!("{}/cbkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let enable = "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>Enabled</Status></VersioningConfiguration>";
    assert_eq!(c.put(format!("{}/cbkt?versioning", url)).body(enable).send().await.unwrap().status(), StatusCode::OK);

    // 0: nothing yet; 1: the marker is written, the DELETE waits; 2: compaction is past it or
    // waits on the key lock, so the DELETE may go on.
    let step = Arc::new(AtomicU8::new(0));
    {
        let step = Arc::clone(&step);
        let hook: s3_server::TestHook = Arc::new(move |point, key| {
            if key != "m" {
                return;
            }
            match point {
                "delete_marker_written" => {
                    step.store(1, Ordering::SeqCst);
                    for _ in 0..500 {
                        if step.load(Ordering::SeqCst) == 2 {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
                "key_lock_contended" => step.store(2, Ordering::SeqCst),
                _ => {}
            }
        });
        *state.test_hook.write() = Some(hook);
    }
    let delete = tokio::spawn({
        let c = c.clone();
        let url = format!("{}/cbkt/m", url);
        async move { c.delete(url).send().await.unwrap().status() }
    });
    for _ in 0..400 {
        if step.load(Ordering::SeqCst) == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(step.load(Ordering::SeqCst), 1, "the DELETE wrote its marker");
    // Rolls volume 1, which holds the marker, and compacts it.
    let report = state.perform_online_compaction().await.unwrap();
    step.store(2, Ordering::SeqCst);
    assert_eq!(delete.await.unwrap(), StatusCode::NO_CONTENT);
    *state.test_hook.write() = None;
    assert!(report.volume_removed, "{:?}", report);

    let markers = |xml: String| xml.matches("<DeleteMarker>").count();
    let listing = c.get(format!("{}/cbkt?versions", url)).send().await.unwrap().text().await.unwrap();
    assert_eq!(markers(listing), 1);
    handle.shutdown();
    drop(state);

    let (_state, url, _h) = start(&dir).await;
    let listing = c.get(format!("{}/cbkt?versions", url)).send().await.unwrap().text().await.unwrap();
    assert_eq!(markers(listing.clone()), 1, "the delete marker is still there after a restart: {}", listing);
}
