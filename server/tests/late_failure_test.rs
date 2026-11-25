//! #184, owner decision 4B (2026-11-16): a write whose needle is whole in the volume, and
//! whose next step then fails (the journal append or the sync), answers 500, and the live
//! index applies it as recovery will. So a GET before and after a restart agree. Before the
//! fix the live index kept the old object and a restart showed the write the client was told
//! failed. The model is `verification/tla/ObjectVersions.tla` (LateFail, RelocateLateFail).

use reqwest::header::ETAG;
use reqwest::StatusCode;
use s3_server::{global_offset, start_s3_reactor, S3ServiceState, STREAM_PUT_MIN};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_late_{}_{}_{}", name, std::process::id(), nanos));
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

#[derive(Clone, Copy, Debug)]
enum Fault {
    /// The journal append fails after the needle is written.
    Journal,
    /// The sync after the write fails.
    Sync,
}

fn arm(state: &S3ServiceState, fault: Fault) {
    let vm = state.volume_manager.read().clone().unwrap();
    match fault {
        Fault::Journal => vm.fail_next_index(),
        Fault::Sync => vm.fail_next_sync(),
    }
}

/// The status and bytes of a GET, live and then after a restart.
async fn get_live_then_restarted(
    state: Arc<S3ServiceState>,
    handle: s3_server::ServerHandle,
    dir: &Path,
    url: &str,
    path: &str,
) -> ((StatusCode, Vec<u8>), (StatusCode, Vec<u8>)) {
    let c = reqwest::Client::new();
    let live = c.get(format!("{}{}", url, path)).send().await.unwrap();
    let live = (live.status(), live.bytes().await.unwrap().to_vec());
    handle.shutdown();
    drop(state);
    let (_state, url, _h) = start(dir).await;
    let after = c.get(format!("{}{}", url, path)).send().await.unwrap();
    (live, (after.status(), after.bytes().await.unwrap().to_vec()))
}

async fn put_then_failed_put(fault: Fault, len: usize) {
    let dir = temp_dir(&format!("put_{:?}_{}", fault, len));
    let (state, url, handle) = start(&dir).await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/late-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let (old, new) = (pattern(len, 1), pattern(len, 2));
    assert_eq!(c.put(format!("{}/late-bkt/k", url)).body(old).send().await.unwrap().status(), StatusCode::OK);
    arm(&state, fault);
    let res = c.put(format!("{}/late-bkt/k", url)).body(new.clone()).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR, "{:?}: the client is told the write failed", fault);

    let (live, after) = get_live_then_restarted(state, handle, &dir, &url, "/late-bkt/k").await;
    assert_eq!(live.0, StatusCode::OK);
    assert!(live.1 == new, "{:?}, {} bytes: live, the index applies the record recovery will replay", fault, len);
    assert_eq!(after.0, StatusCode::OK);
    assert!(after.1 == new, "{:?}, {} bytes: after a restart the same object", fault, len);
}

#[tokio::test]
async fn a_put_whose_journal_append_fails_shows_the_same_object_before_and_after_a_restart() {
    put_then_failed_put(Fault::Journal, 2000).await;
}

#[tokio::test]
async fn a_put_whose_sync_fails_shows_the_same_object_before_and_after_a_restart() {
    put_then_failed_put(Fault::Sync, 2000).await;
}

#[tokio::test]
async fn a_streamed_put_whose_journal_append_fails_shows_the_same_object_before_and_after_a_restart() {
    put_then_failed_put(Fault::Journal, STREAM_PUT_MIN as usize + 4096).await;
}

#[tokio::test]
async fn a_streamed_put_whose_sync_fails_shows_the_same_object_before_and_after_a_restart() {
    put_then_failed_put(Fault::Sync, STREAM_PUT_MIN as usize + 4096).await;
}

#[tokio::test]
async fn a_delete_whose_journal_append_fails_is_gone_before_and_after_a_restart() {
    for fault in [Fault::Journal, Fault::Sync] {
        let dir = temp_dir(&format!("del_{:?}", fault));
        let (state, url, handle) = start(&dir).await;
        let c = reqwest::Client::new();
        assert_eq!(c.put(format!("{}/late-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
        assert_eq!(c.put(format!("{}/late-bkt/k", url)).body(pattern(1000, 3)).send().await.unwrap().status(), StatusCode::OK);
        arm(&state, fault);
        let res = c.delete(format!("{}/late-bkt/k", url)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR, "{:?}", fault);

        let (live, after) = get_live_then_restarted(state, handle, &dir, &url, "/late-bkt/k").await;
        assert_eq!(live.0, StatusCode::NOT_FOUND, "{:?}: live, the tombstone recovery will replay applies", fault);
        assert_eq!(after.0, StatusCode::NOT_FOUND, "{:?}: after a restart too", fault);
    }
}

#[tokio::test]
async fn a_copy_whose_journal_append_fails_shows_the_same_object_before_and_after_a_restart() {
    let dir = temp_dir("copy");
    let (state, url, handle) = start(&dir).await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/late-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let src = pattern(3000, 4);
    assert_eq!(c.put(format!("{}/late-bkt/src", url)).body(src.clone()).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{}/late-bkt/dst", url)).body(pattern(3000, 5)).send().await.unwrap().status(), StatusCode::OK);
    arm(&state, Fault::Journal);
    let res = c.put(format!("{}/late-bkt/dst", url)).header("x-amz-copy-source", "/late-bkt/src").send().await.unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let (live, after) = get_live_then_restarted(state, handle, &dir, &url, "/late-bkt/dst").await;
    assert!(live.0 == StatusCode::OK && live.1 == src, "live: the copy is the object");
    assert!(after.0 == StatusCode::OK && after.1 == src, "after a restart: the copy");
}

#[tokio::test]
async fn a_multipart_complete_whose_journal_append_fails_shows_the_same_object_before_and_after_a_restart() {
    let dir = temp_dir("mpu");
    let (state, url, handle) = start(&dir).await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/late-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{}/late-bkt/big", url)).body(pattern(1000, 6)).send().await.unwrap().status(), StatusCode::OK);
    let xml = c.post(format!("{}/late-bkt/big?uploads", url)).send().await.unwrap().text().await.unwrap();
    let upload_id = xml.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap().to_string();
    let parts = [pattern(200 * 1024, 7), pattern(100 * 1024, 8)];
    let mut complete = String::from("<CompleteMultipartUpload>");
    for (i, data) in parts.iter().enumerate() {
        let res = c
            .put(format!("{}/late-bkt/big?partNumber={}&uploadId={}", url, i + 1, upload_id))
            .body(data.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let etag = res.headers().get(ETAG).unwrap().to_str().unwrap().to_string();
        complete.push_str(&format!("<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>", i + 1, etag));
    }
    complete.push_str("</CompleteMultipartUpload>");
    arm(&state, Fault::Journal);
    let res = c.post(format!("{}/late-bkt/big?uploadId={}", url, upload_id)).body(complete).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let mut whole = parts[0].clone();
    whole.extend_from_slice(&parts[1]);
    let (live, after) = get_live_then_restarted(state, handle, &dir, &url, "/late-bkt/big").await;
    assert!(live.0 == StatusCode::OK && live.1 == whole, "live: the completed object");
    assert!(after.0 == StatusCode::OK && after.1 == whole, "after a restart: the completed object");
}

struct Stalled {
    stream: TcpStream,
    rest: Vec<u8>,
}

async fn begin_stalled(state: &S3ServiceState, url: &str, path: &str, body: &[u8]) -> Stalled {
    assert!(body.len() as u64 > STREAM_PUT_MIN);
    let host = url.trim_start_matches("http://").to_string();
    let mut stream = TcpStream::connect(&host).await.unwrap();
    let head = format!(
        "PUT {} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        path,
        host,
        body.len()
    );
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
    Stalled {
        stream,
        rest: body[half..].to_vec(),
    }
}

impl Stalled {
    async fn finish(mut self) -> u16 {
        self.stream.write_all(&self.rest).await.unwrap();
        self.stream.flush().await.unwrap();
        let mut buf = Vec::new();
        tokio::time::timeout(Duration::from_secs(20), self.stream.read_to_end(&mut buf))
            .await
            .expect("no answer")
            .unwrap();
        let text = String::from_utf8_lossy(&buf);
        text.split_whitespace()
            .nth(1)
            .unwrap_or_else(|| panic!("no status line in {:?}", text))
            .parse()
            .unwrap()
    }
}

#[tokio::test]
async fn a_pre_header_write_failure_does_not_mark_streamed_put_for_relocation() {
    let dir = temp_dir("pre_header_stream");
    let (state, url, handle) = start(&dir).await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/late-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let b_id = state.engine.read().lookup_bucket("late-bkt").unwrap();
    let streamed = pattern(STREAM_PUT_MIN as usize + 4096, 9);

    let stalled = begin_stalled(&state, &url, "/late-bkt/k", &streamed).await;
    let ticket = state.streams.enter(b_id, "k");
    state.volume_manager.read().clone().unwrap().fail_next_write();
    let res = c.put(format!("{}/late-bkt/k", url)).body(pattern(2000, 10)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);

    assert!(
        !ticket.appended_after(0),
        "pre-header write failure wrote no needle header and must not note an append"
    );
    drop(ticket);

    assert_eq!(stalled.finish().await, 200);
    assert_eq!(
        state.stream_relocations.load(Ordering::SeqCst),
        0,
        "pre-header write failure must not trigger streamed PUT relocation"
    );

    let (live, after) = get_live_then_restarted(state, handle, &dir, &url, "/late-bkt/k").await;
    assert_eq!(live.0, StatusCode::OK);
    assert!(live.1 == streamed, "live: the streamed PUT is the object");
    assert_eq!(after.0, StatusCode::OK);
    assert!(after.1 == streamed, "after a restart: the streamed PUT is the object");
}

#[tokio::test]
async fn an_after_record_failure_notes_record_offset_and_relocates_streamed_put() {
    let dir = temp_dir("after_record_stream");
    let (state, url, handle) = start(&dir).await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/late-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let b_id = state.engine.read().lookup_bucket("late-bkt").unwrap();
    let streamed = pattern(STREAM_PUT_MIN as usize + 4096, 11);

    let stalled = begin_stalled(&state, &url, "/late-bkt/k", &streamed).await;
    let ticket = state.streams.enter(b_id, "k");
    let vm = state.volume_manager.read().clone().unwrap();
    let expected_g = global_offset(vm.id(), vm.offset());
    vm.fail_next_index();
    let res = c.put(format!("{}/late-bkt/k", url)).body(pattern(2000, 12)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);

    assert!(
        ticket.appended_after(expected_g - 1),
        "AfterRecord failure must note the written record offset {}",
        expected_g
    );
    assert!(
        !ticket.appended_after(expected_g),
        "AfterRecord failure must note exact offset {} rather than u64::MAX",
        expected_g
    );
    drop(ticket);

    assert_eq!(stalled.finish().await, 200);
    assert_eq!(
        state.stream_relocations.load(Ordering::SeqCst),
        1,
        "AfterRecord write was noted so the streamed PUT relocated"
    );

    let (live, after) = get_live_then_restarted(state, handle, &dir, &url, "/late-bkt/k").await;
    assert_eq!(live.0, StatusCode::OK);
    assert!(live.1 == streamed, "live: the relocated streamed PUT is the object");
    assert_eq!(after.0, StatusCode::OK);
    assert!(after.1 == streamed, "after a restart: the relocated streamed PUT wins over the AfterRecord write");
}
