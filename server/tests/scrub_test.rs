//! #155 Step 4: Background needle scrub tests.

use reqwest::StatusCode;
use s3_server::{split_offset, start_s3_reactor, S3ServiceState};
use std::io::{Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const MIB: usize = 1024 * 1024;

fn temp_dir(name: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let d = std::env::temp_dir().join(format!(
        "awanfs_scrub_{}_{}_{}",
        name,
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

async fn start(name: &str) -> (PathBuf, Arc<S3ServiceState>, String, s3_server::ServerHandle) {
    let dir = temp_dir(name);
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    (dir, state, url, handle)
}

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

fn flip_byte_at(dir: &Path, volume_id: u32, file_offset: u64) {
    let dat = dir.join(format!("volume_{:04}.dat", volume_id));
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&dat)
        .unwrap();
    let mut byte = [0u8; 1];
    f.seek(SeekFrom::Start(file_offset)).unwrap();
    std::io::Read::read_exact(&mut f, &mut byte).unwrap();
    f.seek(SeekFrom::Start(file_offset)).unwrap();
    f.write_all(&[byte[0] ^ 0xFF]).unwrap();
    f.sync_all().unwrap();
}

#[tokio::test]
async fn scrub_detects_payload_and_header_corruption_and_reports_via_http() {
    let (dir, state, url, handle) = start("detect").await;
    let c = reqwest::Client::new();
    assert_eq!(
        c.put(format!("{}/sbkt", url)).send().await.unwrap().status(),
        StatusCode::OK
    );

    let big_data = noise(5 * MIB + 123, 0x1111);
    let small_data = noise(64 * 1024, 0x2222);
    assert_eq!(
        c.put(format!("{}/sbkt/big", url))
            .body(big_data.clone())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        c.put(format!("{}/sbkt/small", url))
            .body(small_data.clone())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    let b_id = state.engine.read().lookup_bucket("sbkt").unwrap();
    let big_meta = state.engine.read().get_object(b_id, b"big").unwrap();
    let small_meta = state.engine.read().get_object(b_id, b"small").unwrap();
    let (big_vol, big_local) = split_offset(big_meta.vol_offset);
    let (small_vol, small_local) = split_offset(small_meta.vol_offset);

    // Clean pass 1 must report 0 bad needles and leave volume mmap untouched.
    let remaps_before = state.volumes.read().get(&big_vol).unwrap().remap_count();
    let r1 = state.scrub_pass();
    assert_eq!(r1.pass, 1);
    assert!(r1.checked >= 2);
    assert_eq!(r1.bad, 0);
    assert!(r1.bad_needles.is_empty());
    assert_eq!(
        state.volumes.read().get(&big_vol).unwrap().remap_count(),
        remaps_before,
        "scrub must use positioned reads without mapping volumes"
    );

    // Corrupt payload byte in big object (middle) and header byte in small object.
    flip_byte_at(&dir, big_vol, big_local + 64 + "big".len() as u64 + 2 * MIB as u64);
    flip_byte_at(&dir, small_vol, small_local + 10);

    // Pass 2 detects both corruptions within one pass.
    let r2 = state.scrub_pass();
    assert_eq!(r2.pass, 2);
    assert_eq!(r2.bad, 2);
    assert_eq!(r2.bad_needles.len(), 2);

    // GET /_awanfs/scrub returns JSON with last pass counts and bad needle list.
    let scrub_res = c.get(format!("{}/_awanfs/scrub", url)).send().await.unwrap();
    assert_eq!(scrub_res.status(), StatusCode::OK);
    let scrub_body = scrub_res.bytes().await.unwrap();
    let report: serde_json::Value = serde_json::from_slice(&scrub_body).unwrap();
    assert_eq!(report["pass"], 2);
    assert_eq!(report["bad"], 2);
    assert!(report["checked"].as_u64().unwrap() >= 2);
    assert!(report["duration_seconds"].as_f64().is_some());
    let bad_arr = report["bad_needles"].as_array().unwrap();
    assert_eq!(bad_arr.len(), 2);
    assert!(bad_arr.iter().any(|n| n["offset"] == big_local && n["key"] == "big"));
    assert!(bad_arr.iter().any(|n| n["offset"] == small_local));

    // Metrics must export scrub duration and corrupt needle counter.
    let metrics = c
        .get(format!("{}/_awanfs/metrics", url))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("awanfs_corrupt_needles_total 2"), "{}", metrics);
    assert!(metrics.contains("awanfs_scrub_pass_duration_seconds "), "{}", metrics);

    // A range or full GET against the corrupt header must return 500 InternalError.
    let hdr_err = c
        .get(format!("{}/sbkt/small", url))
        .header("range", "bytes=0-100")
        .send()
        .await
        .unwrap();
    assert_eq!(hdr_err.status(), StatusCode::INTERNAL_SERVER_ERROR);

    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn scrub_rate_zero_disables_and_scheduler_runs_when_enabled() {
    let dir = temp_dir("sched");
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();
    assert_eq!(
        c.put(format!("{}/sbkt2", url)).send().await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        c.put(format!("{}/sbkt2/obj", url))
            .body(noise(256 * 1024, 7))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // 0 MiB/s disables scrub.
    state.start_scrub(Duration::from_millis(10), 0);
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(state.scrub.report().pass, 0, "rate 0 must not run scrub");

    // Non-zero rate registers and completes passes on the scheduler.
    state.start_scrub(Duration::from_millis(20), 100);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while state.scrub.report().pass == 0 {
        assert!(tokio::time::Instant::now() < deadline, "scheduled scrub never ran");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(state.scrub.report().good >= 1);

    // Rate-limited pass paces reads and respects stop flag.
    let vols: Vec<_> = state.volumes.read().values().cloned().collect();
    let stop = AtomicBool::new(true);
    let stopped_report = state.scrub.run_pass(&vols, 1, &stop);
    assert_eq!(stopped_report.checked, 0, "stopped scrub exits before reading");

    assert!(state.scheduler.shutdown(Duration::from_secs(2)).await);
    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
