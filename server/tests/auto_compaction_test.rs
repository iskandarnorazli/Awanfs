//! #14, owner decision 15: automatic compaction runs on the oldest volume once 20 % of it is dead.
//! Tests call `auto_compact_once` directly; the timer only repeats it.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_auto_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn start(dir: &Path, span: Option<u64>) -> (Arc<S3ServiceState>, String, s3_server::ServerHandle) {
    let state = Arc::new(S3ServiceState::with_data_dir(dir));
    if let Some(s) = span {
        state.set_volume_span(s);
    }
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    (state, url, handle)
}

fn body(k: usize, v: usize) -> Vec<u8> {
    (0..3000).map(|j| ((j + k * 7 + v * 13) % 251) as u8).collect()
}

#[tokio::test]
async fn the_oldest_volume_is_compacted_only_past_the_threshold() {
    let dir = temp_dir("threshold");
    let c = reqwest::Client::new();
    let (state, url, handle) = start(&dir, Some(64 * 1024)).await;
    assert_eq!(c.put(format!("{}/ac-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    // Volume 1 gets k0..k14 (one sector each); the rest roll over.
    for k in 0..20 {
        assert_eq!(c.put(format!("{}/ac-bkt/k{}", url, k)).body(body(k, 0)).send().await.unwrap().status(), StatusCode::OK);
    }
    let oldest = *state.volumes.read().keys().next().unwrap();
    let (dead, used) = state.volume_usage(oldest).unwrap();
    assert_eq!(dead, 0, "nothing is dead yet (used {})", used);
    assert!(state.auto_compact_once(0.20, 0).await.unwrap().is_none(), "0 % dead: no compaction");

    // Overwrite 2 of the 15 keys in volume 1: 13 % dead, still below 20 %.
    for k in 0..2 {
        c.put(format!("{}/ac-bkt/k{}", url, k)).body(body(k, 1)).send().await.unwrap();
    }
    assert!(state.auto_compact_once(0.20, 0).await.unwrap().is_none(), "below the threshold");
    // Two more: 4 of 15 = 27 %.
    for k in 2..4 {
        c.put(format!("{}/ac-bkt/k{}", url, k)).body(body(k, 1)).send().await.unwrap();
    }
    let r = state.auto_compact_once(0.20, 0).await.unwrap().expect("past the threshold");
    assert_eq!(r.live_needles_copied, 11, "the 11 current versions in volume 1 moved");
    assert!(!state.volumes.read().contains_key(&oldest), "volume {} is gone", oldest);

    for k in 0..20 {
        let got = c.get(format!("{}/ac-bkt/k{}", url, k)).send().await.unwrap().bytes().await.unwrap();
        assert!(&got[..] == &body(k, if k < 4 { 1 } else { 0 })[..], "k{}", k);
    }
    handle.shutdown();
    drop(state);
    let (_s, url, _h) = start(&dir, None).await;
    for k in 0..20 {
        let got = c.get(format!("{}/ac-bkt/k{}", url, k)).send().await.unwrap().bytes().await.unwrap();
        assert!(&got[..] == &body(k, if k < 4 { 1 } else { 0 })[..], "k{} after a restart", k);
    }
}

#[tokio::test]
async fn a_lone_volume_is_sealed_and_compacted_only_above_the_minimum_size() {
    let dir = temp_dir("lone");
    let c = reqwest::Client::new();
    let (state, url, _h) = start(&dir, None).await;
    assert_eq!(c.put(format!("{}/ac-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    for k in 0..10 {
        c.put(format!("{}/ac-bkt/k{}", url, k)).body(body(k, 0)).send().await.unwrap();
    }
    for k in 0..10 {
        c.delete(format!("{}/ac-bkt/k{}", url, k)).send().await.unwrap();
    }
    assert!(state.auto_compact_once(0.20, 1 << 30).await.unwrap().is_none(), "below 1 GiB: left alone");
    assert_eq!(state.volumes.read().len(), 1);
    let r = state.auto_compact_once(0.20, 0).await.unwrap().expect("no minimum: compacted");
    assert_eq!(r.live_needles_copied, 0);
    assert_eq!(state.volumes.read().len(), 1, "volume 1 sealed, compacted away; the new one is active");
    assert_eq!(c.get(format!("{}/ac-bkt/k3", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);
}
