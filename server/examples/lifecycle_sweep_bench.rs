//! #464: what one lifecycle sweep pass costs on a bucket with many keys, and PUT latency while
//! one runs. The bucket comes from `boot_bench fill`; this boots it, sets an enabled lifecycle
//! rule that expires nothing (Expiration after 36500 days), and then:
//!
//! 1. runs one sweep pass alone: wall time, process CPU time, VmRSS before and after, VmHWM;
//! 2. runs `--puts` PUTs of 4 KiB at concurrency `--concurrency` with no sweep: P50 and P99;
//! 3. runs the same PUTs while sweep passes run back to back: P50 and P99, and passes done.
//!
//!   cargo run --release --example boot_bench -- fill --dir <DIR> --objects 1000000
//!   cargo run --release --example lifecycle_sweep_bench -- --dir <DIR> --puts 20000 --concurrency 16
//!
//! Prints one JSON line per phase. Linux only for CPU time and RSS (from getrusage and
//! /proc/self/status); elsewhere those fields are null.

use s3_server::{run_lifecycle_sweep_async, start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const BUCKET: &str = "boot-bench";

const NOTHING_EXPIRES: &str = "<LifecycleConfiguration><Rule><ID>keep</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status><Expiration><Days>36500</Days></Expiration></Rule></LifecycleConfiguration>";

/// Current objects in the bench bucket (in memory and in the spill index), and how many of
/// them are in the in-memory index.
fn object_counts(state: &S3ServiceState) -> (u64, usize) {
    let engine = state.engine.read();
    let Ok(b) = engine.lookup_bucket(BUCKET) else { return (0, 0) };
    let objects = engine.bucket_usage(b).map_or(0, |u| u.objects);
    let in_memory = engine.buckets.get(&b).map_or(0, |e| e.key_index.len());
    (objects, in_memory)
}

fn status_kib(field: &str) -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = s.lines().find(|l| l.starts_with(field))?;
    line.split_whitespace().nth(1)?.parse().ok()
}

#[cfg(target_os = "linux")]
fn cpu_secs() -> Option<f64> {
    // SAFETY: getrusage fills a zeroed local.
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut ru) != 0 {
            return None;
        }
        let t = |tv: libc::timeval| tv.tv_sec as f64 + tv.tv_usec as f64 / 1e6;
        Some(t(ru.ru_utime) + t(ru.ru_stime))
    }
}

#[cfg(not(target_os = "linux"))]
fn cpu_secs() -> Option<f64> {
    None
}

fn json_opt<T: std::fmt::Display>(v: Option<T>) -> String {
    v.map_or("null".to_string(), |x| x.to_string())
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i]
}

async fn put_load(url: &str, puts: usize, concurrency: usize, tag: &str) -> Vec<f64> {
    let client = reqwest::Client::new();
    let next = Arc::new(AtomicU64::new(0));
    let body = bytes::Bytes::from(vec![7u8; 4096]);
    let mut tasks = Vec::new();
    for _ in 0..concurrency {
        let (client, next, body, url, tag) = (client.clone(), Arc::clone(&next), body.clone(), url.to_string(), tag.to_string());
        tasks.push(tokio::spawn(async move {
            let mut lat = Vec::new();
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i as usize >= puts {
                    break;
                }
                let t = Instant::now();
                let res = client.put(format!("{}/{}/load-{}-{}", url, BUCKET, tag, i)).body(body.clone()).send().await.expect("PUT");
                assert!(res.status().is_success(), "PUT answered {}", res.status());
                lat.push(t.elapsed().as_secs_f64() * 1000.0);
            }
            lat
        }));
    }
    let mut all = Vec::new();
    for t in tasks {
        all.extend(t.await.unwrap());
    }
    all.sort_by(|a, b| a.partial_cmp(b).unwrap());
    all
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut dir: Option<PathBuf> = None;
    let mut puts = 20_000usize;
    let mut concurrency = 16usize;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--dir" => dir = args.next().map(PathBuf::from),
            "--puts" => puts = args.next().and_then(|v| v.parse().ok()).ok_or("--puts needs a number")?,
            "--concurrency" => concurrency = args.next().and_then(|v| v.parse().ok()).ok_or("--concurrency needs a number")?,
            other => return Err(format!("unknown argument {:?}", other).into()),
        }
    }
    let dir = dir.ok_or("--dir is required")?;

    let t = Instant::now();
    let state = Arc::new(S3ServiceState::try_with_data_dir(&dir)?);
    let (objects, in_memory) = object_counts(&state);
    println!("{{\"phase\":\"boot\",\"secs\":{:.3},\"objects\":{},\"in_memory\":{}}}", t.elapsed().as_secs_f64(), objects, in_memory);

    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await?;
    let url = format!("http://{}", handle.local_addr);
    let client = reqwest::Client::new();
    let res = client.put(format!("{}/{}?lifecycle", url, BUCKET)).body(NOTHING_EXPIRES).send().await?;
    assert!(res.status().is_success(), "PUT ?lifecycle answered {}: {}", res.status(), res.text().await?);

    // 1. One pass alone.
    let now_ms = state.clock.now_ns() / 1_000_000;
    let (rss0, cpu0) = (status_kib("VmRSS:"), cpu_secs());
    let t = Instant::now();
    let more = run_lifecycle_sweep_async(&state, now_ms, usize::MAX).await;
    let wall = t.elapsed().as_secs_f64();
    let (rss1, cpu1, hwm) = (status_kib("VmRSS:"), cpu_secs(), status_kib("VmHWM:"));
    let (objects_after, _) = object_counts(&state);
    println!(
        "{{\"phase\":\"sweep\",\"wall_secs\":{:.3},\"cpu_secs\":{},\"rss_before_kib\":{},\"rss_after_kib\":{},\"hwm_kib\":{},\"more\":{},\"objects_after\":{}}}",
        wall,
        json_opt(cpu0.zip(cpu1).map(|(a, b)| format!("{:.3}", b - a))),
        json_opt(rss0),
        json_opt(rss1),
        json_opt(hwm),
        more,
        objects_after
    );

    // 2. PUTs with no sweep.
    let lat = put_load(&url, puts, concurrency, "quiet").await;
    println!(
        "{{\"phase\":\"put_quiet\",\"puts\":{},\"concurrency\":{},\"p50_ms\":{:.2},\"p99_ms\":{:.2}}}",
        lat.len(),
        concurrency,
        percentile(&lat, 0.50),
        percentile(&lat, 0.99)
    );

    // 3. The same PUTs while sweep passes run back to back.
    let stop = Arc::new(AtomicBool::new(false));
    let passes = Arc::new(AtomicU64::new(0));
    let sweeper = {
        let (state, stop, passes) = (Arc::clone(&state), Arc::clone(&stop), Arc::clone(&passes));
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let now_ms = state.clock.now_ns() / 1_000_000;
                run_lifecycle_sweep_async(&state, now_ms, usize::MAX).await;
                passes.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
    };
    let lat = put_load(&url, puts, concurrency, "sweeping").await;
    stop.store(true, Ordering::Relaxed);
    sweeper.await?;
    println!(
        "{{\"phase\":\"put_sweeping\",\"puts\":{},\"concurrency\":{},\"p50_ms\":{:.2},\"p99_ms\":{:.2},\"sweep_passes\":{},\"hwm_kib\":{}}}",
        lat.len(),
        concurrency,
        percentile(&lat, 0.50),
        percentile(&lat, 0.99),
        passes.load(Ordering::Relaxed),
        json_opt(status_kib("VmHWM:"))
    );
    Ok(())
}
