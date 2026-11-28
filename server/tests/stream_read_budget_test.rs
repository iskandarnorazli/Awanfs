//! #155 Step 4: Stream reader memory budget, 64 concurrent 256 MiB GETs, fault injection,
//! encryption chunk transform hook, and property tests.

use bytes::Bytes;
use http_body_util::BodyExt;
use reqwest::StatusCode;
use s3_server::memory::BodyBudget;
use s3_server::sim::SimRng;
use s3_server::stream_read::{
    read_chunk, ChunkTransform, Job, PositionedRead, PreadBody, Segment, Source,
};
use s3_server::work::WorkCounters;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const MIB: usize = 1024 * 1024;

fn temp_dir(name: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let d = std::env::temp_dir().join(format!(
        "awanfs_srb_{}_{}_{}",
        name,
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn pattern(len: usize, seed: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + seed * 7 + 13) % 251) as u8).collect()
}

struct FaultingReader {
    data: Vec<u8>,
    fail_at: u64,
    short_read: bool,
}

impl PositionedRead for FaultingReader {
    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
        if offset <= self.fail_at && self.fail_at < offset + buf.len() as u64 {
            let kind = if self.short_read {
                std::io::ErrorKind::UnexpectedEof
            } else {
                std::io::ErrorKind::Other
            };
            return Err(std::io::Error::new(kind, "injected read fault"));
        }
        let end = (offset as usize) + buf.len();
        buf.copy_from_slice(&self.data[offset as usize..end]);
        Ok(())
    }
}

struct XorTransform(u8);

impl ChunkTransform for XorTransform {
    fn transform(&self, _offset: u64, chunk: Bytes) -> Result<Bytes, String> {
        let out: Vec<u8> = chunk.iter().map(|b| b ^ self.0).collect();
        Ok(Bytes::from(out))
    }
}

struct FailingTransform(u64);

impl ChunkTransform for FailingTransform {
    fn transform(&self, offset: u64, chunk: Bytes) -> Result<Bytes, String> {
        if offset >= self.0 {
            return Err("injected decrypt auth tag failure".to_string());
        }
        Ok(chunk)
    }
}

#[tokio::test]
async fn fault_injection_error_and_short_read_abort_and_release_buffers() {
    let data = pattern(5 * MIB + 123, 1);
    let budget = Arc::new(BodyBudget::new(8 * MIB as u64));

    for (fail_at, short_read) in [(0, false), (2 * MIB as u64 + 7, true), (5 * MIB as u64, false)] {
        let reader = Arc::new(FaultingReader {
            data: data.clone(),
            fail_at,
            short_read,
        });
        let mut body = PreadBody::from_job(Job {
            segments: vec![Segment {
                source: Source::Reader(reader),
                at: 0,
                len: data.len() as u64,
                verify: Some(crc32c::crc32c(&data)),
                transform: None,
                step_bytes: None,
                output_len: None,
            }],
            what: "fault-test".to_string(),
            failures: Arc::new(AtomicU64::new(0)),
            work: Arc::new(WorkCounters::default()),
            body_budget: Some(Arc::clone(&budget)),
            on_corrupt: None,
        });

        let mut saw_err = false;
        while let Some(frame) = body.frame().await {
            if frame.is_err() {
                saw_err = true;
                break;
            }
        }
        assert!(saw_err, "expected stream abort at fail_at={} short={}", fail_at, short_read);
        drop(body);

        for _ in 0..100 {
            if budget.in_use() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(budget.in_use(), 0, "budget must return to 0 after fault abort");
    }
}

#[tokio::test]
async fn chunk_transform_applies_and_transform_error_aborts() {
    let data = pattern(5 * MIB + 77, 2);
    let budget = Arc::new(BodyBudget::new(8 * MIB as u64));
    let reader = Arc::new(FaultingReader {
        data: data.clone(),
        fail_at: u64::MAX,
        short_read: false,
    });

    let mut body = PreadBody::from_job(Job {
        segments: vec![Segment {
            source: Source::Reader(reader.clone()),
            at: 0,
            len: data.len() as u64,
            verify: Some(crc32c::crc32c(&data)),
            transform: Some(Arc::new(XorTransform(0x5A))),
            step_bytes: None,
            output_len: None,
        }],
        what: "xform-ok".to_string(),
        failures: Arc::new(AtomicU64::new(0)),
        work: Arc::new(WorkCounters::default()),
        body_budget: Some(Arc::clone(&budget)),
        on_corrupt: None,
    });
    let mut got = Vec::new();
    while let Some(frame) = body.frame().await {
        got.extend_from_slice(&frame.unwrap().into_data().unwrap());
    }
    let want: Vec<u8> = data.iter().map(|b| b ^ 0x5A).collect();
    assert_eq!(got, want);
    assert_eq!(budget.in_use(), 0);

    let mut bad_body = PreadBody::from_job(Job {
        segments: vec![Segment {
            source: Source::Reader(reader),
            at: 0,
            len: data.len() as u64,
            verify: Some(crc32c::crc32c(&data)),
            transform: Some(Arc::new(FailingTransform(2 * MIB as u64))),
            step_bytes: None,
            output_len: None,
        }],
        what: "xform-fail".to_string(),
        failures: Arc::new(AtomicU64::new(0)),
        work: Arc::new(WorkCounters::default()),
        body_budget: Some(Arc::clone(&budget)),
        on_corrupt: None,
    });
    let mut saw_err = false;
    while let Some(frame) = bad_body.frame().await {
        if frame.is_err() {
            saw_err = true;
            break;
        }
    }
    assert!(saw_err);
    drop(bad_body);
    for _ in 0..100 {
        if budget.in_use() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(budget.in_use(), 0);
}

#[tokio::test]
async fn body_budget_bounds_concurrent_streams_and_disconnect_frees_immediately() {
    let dir = temp_dir("budget_bounds");
    let mut state = S3ServiceState::with_data_dir(&dir);
    // Tight 8 MiB budget shared across concurrent large GETs.
    let tight_budget = Arc::new(BodyBudget::new(8 * MIB as u64));
    state.body_budget = Arc::clone(&tight_budget);
    let state = Arc::new(state);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();
    assert_eq!(
        c.put(format!("{}/bbkt", url)).send().await.unwrap().status(),
        StatusCode::OK
    );

    let data = pattern(10 * MIB + 333, 3);
    assert_eq!(
        c.put(format!("{}/bbkt/big", url))
            .body(data.clone())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    let peak_seen = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicU64::new(0));
    let b_mon = Arc::clone(&tight_budget);
    let p_mon = Arc::clone(&peak_seen);
    let d_mon = Arc::clone(&done);
    let monitor = tokio::spawn(async move {
        while d_mon.load(Ordering::Relaxed) == 0 {
            p_mon.fetch_max(b_mon.in_use(), Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let cl = c.clone();
        let u = format!("{}/bbkt/big", url);
        let expected_len = data.len();
        let expected_crc = crc32c::crc32c(&data);
        tasks.push(tokio::spawn(async move {
            let res = cl.get(&u).send().await.unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            let got = res.bytes().await.unwrap();
            assert_eq!(got.len(), expected_len);
            assert_eq!(crc32c::crc32c(&got), expected_crc);
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    done.store(1, Ordering::Relaxed);
    monitor.await.unwrap();

    assert!(
        peak_seen.load(Ordering::Relaxed) <= tight_budget.limit(),
        "body_budget exceeded limit: peak={} limit={}",
        peak_seen.load(Ordering::Relaxed),
        tight_budget.limit()
    );
    assert_eq!(tight_budget.rejected(), 0, "waiting GETs must not increment rejected");
    assert_eq!(tight_budget.in_use(), 0);

    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn sixty_four_concurrent_256_mib_gets_hold_bounded_buffers_and_verify_work() {
    let dir = temp_dir("64_256m");
    let mut state = S3ServiceState::with_data_dir(&dir);
    // 64 MiB read buffer budget across 64 concurrent 256 MiB GETs.
    let budget = Arc::new(BodyBudget::new(64 * MIB as u64));
    state.body_budget = Arc::clone(&budget);
    let state = Arc::new(state);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();
    assert_eq!(
        c.put(format!("{}/c64", url)).send().await.unwrap().status(),
        StatusCode::OK
    );

    let size = 256 * MIB;
    let payload = pattern(size, 11);
    let expected_crc = crc32c::crc32c(&payload);
    assert_eq!(
        c.put(format!("{}/c64/huge", url))
            .body(payload)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // 1. Time to headers on 256 MiB reads 0 payload bytes before body is polled.
    let b_id = state.engine.read().lookup_bucket("c64").unwrap();
    let meta = state.engine.read().get_object(b_id, b"huge").unwrap();
    let before_open = state.work.snapshot();
    let body = state
        .large_payload_body(
            b_id,
            "c64/huge".into(),
            meta.vol_offset,
            meta.size,
            0,
            meta.size,
            true,
        )
        .unwrap();
    assert_eq!(
        state.work.snapshot().since(&before_open).disk_reads,
        1,
        "opening 256 MiB GET reads only header sector"
    );
    drop(body);

    // 2. 1 MiB range on 256 MiB reads <= 1 MiB + 1 buffer (at most 2 payload disk reads + 1 header).
    let before_rng = state.work.snapshot();
    let rng_res = c
        .get(format!("{}/c64/huge", url))
        .header("range", format!("bytes={}-{}", 128 * MIB + 512, 129 * MIB + 511))
        .send()
        .await
        .unwrap();
    assert_eq!(rng_res.status(), StatusCode::PARTIAL_CONTENT);
    let rng_bytes = rng_res.bytes().await.unwrap();
    assert_eq!(rng_bytes.len(), MIB);
    let rng_work = state.work.snapshot().since(&before_rng);
    assert!(
        rng_work.disk_reads <= 3,
        "1 MiB range on 256 MiB did {} disk reads (expected <= 3)",
        rng_work.disk_reads
    );

    // 3. 64 concurrent 256 MiB GETs share the 64 MiB body_budget without blowing heap.
    let peak_budget = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicU64::new(0));
    let b_mon = Arc::clone(&budget);
    let p_mon = Arc::clone(&peak_budget);
    let d_mon = Arc::clone(&done);
    let mon = tokio::spawn(async move {
        while d_mon.load(Ordering::Relaxed) == 0 {
            p_mon.fetch_max(b_mon.in_use(), Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    let mut tasks = Vec::with_capacity(64);
    for _ in 0..64 {
        let cl = c.clone();
        let u = format!("{}/c64/huge", url);
        tasks.push(tokio::spawn(async move {
            let mut res = cl.get(&u).send().await.unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            let mut len = 0usize;
            let mut crc = 0u32;
            while let Some(chunk) = res.chunk().await.unwrap() {
                len += chunk.len();
                crc = crc32c::crc32c_append(crc, &chunk);
            }
            assert_eq!(len, size);
            assert_eq!(crc, expected_crc);
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    done.store(1, Ordering::Relaxed);
    mon.await.unwrap();

    assert!(
        peak_budget.load(Ordering::Relaxed) <= budget.limit(),
        "64 concurrent 256 MiB GETs exceeded budget: {} > {}",
        peak_budget.load(Ordering::Relaxed),
        budget.limit()
    );
    assert_eq!(budget.in_use(), 0);
    let max_per_stream = (s3_server::stream_read::read_ahead() + 3) * (read_chunk() + 2 * 4096);
    assert!(
        peak_budget.load(Ordering::Relaxed) <= (64 * max_per_stream) as u64,
        "peak buffers bounded"
    );

    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn random_sizes_ranges_and_concurrent_same_object_gets_match_stored_bytes() {
    let dir = temp_dir("prop_conc");
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();
    assert_eq!(
        c.put(format!("{}/prbkt", url)).send().await.unwrap().status(),
        StatusCode::OK
    );

    let mut rng = SimRng::new(20261125);
    for case in 0..3 {
        let len = 4 * MIB + 1 + (rng.below(4 * MIB as u64) as usize);
        let data = pattern(len, case + 10);
        let key_url = format!("{}/prbkt/obj_{}", url, case);
        assert_eq!(
            c.put(&key_url).body(data.clone()).send().await.unwrap().status(),
            StatusCode::OK
        );

        // Random ranges across buffer and sector boundaries.
        for _ in 0..12 {
            let a = rng.below(len as u64) as usize;
            let b = (a + rng.below((len - a) as u64).min(2 * MIB as u64) as usize).min(len - 1);
            let res = c
                .get(&key_url)
                .header("range", format!("bytes={}-{}", a, b))
                .send()
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
            let got = res.bytes().await.unwrap();
            assert!(got[..] == data[a..=b], "case {} range {}-{}", case, a, b);
        }

        // 12 concurrent GETs of the same object (verifying lock-free positioned reads).
        let mut tasks = Vec::new();
        let expected_crc = crc32c::crc32c(&data);
        for _ in 0..12 {
            let cl = c.clone();
            let u = key_url.clone();
            tasks.push(tokio::spawn(async move {
                let got = cl.get(&u).send().await.unwrap().bytes().await.unwrap();
                assert_eq!(got.len(), len);
                assert_eq!(crc32c::crc32c(&got), expected_crc);
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
    }

    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
