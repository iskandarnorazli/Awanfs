//! #26 (owner decision 2026-11-20, option 3): how long a server takes to boot from a full
//! replay of its volumes, and its peak heap and RSS while it does, with N objects.
//!
//!   cargo run --release --example boot_bench -- fill --dir <DIR> --objects 10000000
//!   cargo run --release --example boot_bench -- scan --dir <DIR>
//!   cargo run --release --example boot_bench -- boot --dir <DIR>
//!
//! `fill` writes N small objects through `S3ServiceState::store_object` with `--durability
//! fast` (setup only: no sync per object), then syncs every volume. `scan` opens each volume
//! in `DIR` and times `VolumeManager::scan_needles` alone, reporting needles per second and
//! `read_calls`. `boot` runs in a fresh process, so its peak RSS and peak heap are the boot's
//! own: it opens the directory with `S3ServiceState::try_with_data_dir` (the server's boot:
//! bucket manifest, then every volume replayed, the hot table and the spill index rebuilt) and
//! checks a sample of objects. Each phase prints one JSON line.

use bytes::Bytes;
use s3_server::{compute_md5, Durability, S3ServiceState, VolumeManager, AWAN_FLAG_ACTIVE};
use std::alloc::{GlobalAlloc, Layout, System};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Counts the heap this process holds, so a boot can tell heap from the rest of its RSS.
struct Counting;

static HEAP_LIVE: AtomicUsize = AtomicUsize::new(0);
static HEAP_PEAK: AtomicUsize = AtomicUsize::new(0);

fn heap_grew(by: usize) {
    let now = HEAP_LIVE.fetch_add(by, Ordering::Relaxed) + by;
    HEAP_PEAK.fetch_max(now, Ordering::Relaxed);
}

fn reset_heap_peak() {
    HEAP_PEAK.store(HEAP_LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
}

// SAFETY: every call goes to the system allocator unchanged; this only counts bytes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            heap_grew(layout.size());
        }
        p
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc_zeroed(layout);
        if !p.is_null() {
            heap_grew(layout.size());
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        HEAP_LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = System.realloc(ptr, layout, new_size);
        if !p.is_null() {
            if new_size >= layout.size() {
                heap_grew(new_size - layout.size());
            } else {
                HEAP_LIVE.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        p
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn mib(bytes: usize) -> f64 {
    bytes as f64 / 1048576.0
}

const BUCKET: &str = "boot-bench";

fn key(i: u64) -> String {
    format!("obj/{:03}/{:010}", i % 997, i)
}

fn payload(i: u64, size: usize) -> Bytes {
    Bytes::from((0..size).map(|j| (i as usize).wrapping_mul(31).wrapping_add(j) as u8).collect::<Vec<u8>>())
}

fn peak_rss_mib() -> Option<f64> {
    #[cfg(target_os = "linux")]
    {
        let s = std::fs::read_to_string("/proc/self/status").ok()?;
        let kb: f64 = s.lines().find_map(|l| l.strip_prefix("VmHWM:")).and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())?;
        Some(kb / 1024.0)
    }
    #[cfg(windows)]
    {
        #[repr(C)]
        struct Counters {
            cb: u32,
            page_fault_count: u32,
            peak_working_set: usize,
            working_set: usize,
            quota_peak_paged: usize,
            quota_paged: usize,
            quota_peak_nonpaged: usize,
            quota_nonpaged: usize,
            pagefile: usize,
            peak_pagefile: usize,
        }
        extern "system" {
            fn GetCurrentProcess() -> *mut std::ffi::c_void;
            fn K32GetProcessMemoryInfo(process: *mut std::ffi::c_void, counters: *mut Counters, cb: u32) -> i32;
        }
        let mut c = Counters {
            cb: std::mem::size_of::<Counters>() as u32,
            page_fault_count: 0,
            peak_working_set: 0,
            working_set: 0,
            quota_peak_paged: 0,
            quota_paged: 0,
            quota_peak_nonpaged: 0,
            quota_nonpaged: 0,
            pagefile: 0,
            peak_pagefile: 0,
        };
        // SAFETY: a valid pseudo-handle and a struct of the size passed.
        let ok = unsafe { K32GetProcessMemoryInfo(GetCurrentProcess(), &mut c, c.cb) };
        (ok != 0).then_some(c.peak_working_set as f64 / 1048576.0)
    }
    #[cfg(not(any(target_os = "linux", windows)))]
    {
        None
    }
}

fn json_num(v: Option<f64>) -> String {
    v.map(|x| format!("{:.3}", x)).unwrap_or_else(|| "null".to_string())
}

fn dir_bytes(dir: &std::path::Path) -> u64 {
    std::fs::read_dir(dir)
        .map(|it| {
            it.flatten()
                .map(|e| match e.file_type() {
                    Ok(t) if t.is_dir() => dir_bytes(&e.path()),
                    _ => e.metadata().map(|m| m.len()).unwrap_or(0),
                })
                .sum()
        })
        .unwrap_or(0)
}

async fn fill(dir: &std::path::Path, objects: u64, size: usize) -> Result<(), Box<dyn std::error::Error>> {
    let t = Instant::now();
    let state = Arc::new(S3ServiceState::try_with_data_dir(dir)?);
    let b_id = state.engine.write().create_bucket(BUCKET).map_err(|c| format!("create_bucket: {}", c))?;
    let created_ns = state.clock.now_ns();
    state.bucket_created_ns.write().insert(b_id, created_ns);
    let _ = state.bucket_configs.lock().apply_bucket_create(b_id, BUCKET, created_ns, "root", "");
    for vm in state.volumes.read().values() {
        vm.set_durability(Durability::Fast);
    }
    for i in 0..objects {
        let data = payload(i, size);
        let (hi, lo, _, _) = compute_md5(&data);
        state
            .store_object(b_id, BUCKET, &key(i), &[], &data, AWAN_FLAG_ACTIVE, hi, lo, Some(created_ns + i))
            .await
            .map_err(|e| format!("store_object {}: {:?}", key(i), e))?;
    }
    state.sync_all_volumes().map_err(|e| format!("sync_all_volumes: {}", e))?;
    let secs = t.elapsed().as_secs_f64();
    println!(
        "{{\"phase\":\"fill\",\"objects\":{},\"size\":{},\"secs\":{:.3},\"objects_per_s\":{:.0},\"volumes\":{},\"bytes_on_disk\":{}}}",
        objects,
        size,
        secs,
        objects as f64 / secs,
        state.volumes.read().len(),
        dir_bytes(dir)
    );
    Ok(())
}

fn boot_with(dir: &std::path::Path, objects: u64, size: usize, out: &mut dyn std::io::Write) -> Result<(), Box<dyn std::error::Error>> {
    reset_heap_peak();
    let t = Instant::now();
    let state = S3ServiceState::try_with_data_dir(dir)?;
    let secs = t.elapsed().as_secs_f64();
    let peak_heap = HEAP_PEAK.load(Ordering::Relaxed);
    let heap_after = HEAP_LIVE.load(Ordering::Relaxed);
    let rss = peak_rss_mib();
    // A sample of objects answers as written: the boot rebuilt what it should.
    let engine = state.engine.read();
    let b_id = engine.lookup_bucket(BUCKET).map_err(|c| format!("bucket missing after boot: {}", c))?;
    let mut wrong = 0u64;
    let step = (objects / 1000).max(1);
    let mut checked = 0u64;
    let mut i = 0;
    while i < objects {
        match engine.get_object(b_id, key(i).as_bytes()) {
            Ok(m) if m.size == size as u64 => {}
            _ => wrong += 1,
        }
        checked += 1;
        i += step;
    }
    writeln!(
        out,
        "{{\"phase\":\"boot\",\"objects\":{},\"secs\":{:.3},\"objects_per_s\":{:.0},\"peak_rss_mib\":{},\"peak_heap_mib\":{:.3},\"heap_after_mib\":{:.3},\"checked\":{},\"wrong\":{}}}",
        objects,
        secs,
        objects as f64 / secs,
        json_num(rss),
        mib(peak_heap),
        mib(heap_after),
        checked,
        wrong
    )?;
    if wrong > 0 {
        return Err(format!("{} of {} sampled objects answered wrong after boot", wrong, checked).into());
    }
    Ok(())
}

fn boot(dir: &std::path::Path, objects: u64, size: usize) -> Result<(), Box<dyn std::error::Error>> {
    boot_with(dir, objects, size, &mut std::io::stdout().lock())
}

fn volume_ids(dir: &std::path::Path) -> Result<Vec<u32>, Box<dyn std::error::Error>> {
    let mut ids: Vec<u32> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().into_string().ok()?;
            name.strip_prefix("volume_")?.strip_suffix(".dat")?.parse().ok()
        })
        .collect();
    ids.sort_unstable();
    Ok(ids)
}

fn scan(dir: &std::path::Path) -> Result<String, Box<dyn std::error::Error>> {
    let ids = volume_ids(dir)?;
    if ids.is_empty() {
        return Err(format!("no volume_*.dat files in {}", dir.display()).into());
    }
    // Open every volume before timing; VolumeManager::open runs an initial recovery scan.
    let mut volumes = Vec::with_capacity(ids.len());
    for id in ids {
        volumes.push(VolumeManager::open(dir, id).map_err(|e| format!("volume {}: {}", id, e))?);
    }
    let before_reads: Vec<u64> = volumes
        .iter()
        .map(|vm| vm.work_counters().snapshot().disk_reads)
        .collect();

    let t = Instant::now();
    let mut needles = 0u64;
    for vm in &volumes {
        vm.scan_needles(|_| {
            needles += 1;
        })
        .map_err(|e| format!("scan_needles: {}", e))?;
    }
    let secs = t.elapsed().as_secs_f64();
    let read_calls: u64 = volumes
        .iter()
        .zip(before_reads.iter())
        .map(|(vm, before)| vm.work_counters().snapshot().disk_reads.saturating_sub(*before))
        .sum();
    let needles_per_s = needles as f64 / secs.max(1e-9);
    Ok(format!(
        "{{\"phase\":\"scan\",\"volumes\":{},\"secs\":{:.3},\"needles\":{},\"needles_per_s\":{:.0},\"read_calls\":{}}}",
        volumes.len(),
        secs,
        needles,
        needles_per_s,
        read_calls
    ))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let arg = |name: &str, default: &str| -> String { args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned().unwrap_or_else(|| default.to_string()) };
    let dir = PathBuf::from(arg("--dir", "boot_bench"));
    let objects: u64 = arg("--objects", "10000000").parse()?;
    let size: usize = arg("--size", "64").parse()?;
    match args.get(1).map(String::as_str) {
        Some("fill") => fill(&dir, objects, size).await,
        Some("scan") => {
            println!("{}", scan(&dir)?);
            Ok(())
        }
        Some("boot") => boot(&dir, objects, size),
        _ => Err("usage: boot_bench fill|scan|boot --dir DIR [--objects N] [--size BYTES]".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[tokio::test]
    async fn scan_reports_needles_and_read_calls() {
        let dir = std::env::temp_dir().join(format!(
            "awan_boot_bench_scan_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let objects = 5u64;
        fill(&dir, objects, 64).await.unwrap();

        let res = scan(&dir);
        let _ = std::fs::remove_dir_all(&dir);
        let line = res.unwrap();

        let v: serde_json::Value = serde_json::from_str(&line).expect("scan output is valid JSON");
        assert_eq!(v["phase"], "scan");
        assert!(v["secs"].as_f64().is_some(), "missing secs in {line}");
        assert_eq!(v["needles"].as_u64(), Some(objects));
        assert!(v["needles_per_s"].as_f64().is_some(), "missing needles_per_s in {line}");
        let read_calls = v["read_calls"].as_u64().expect("missing read_calls");
        assert!(read_calls >= objects, "read_calls {read_calls} < needles {objects}");
    }

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(name: &str) -> Self {
            static N: AtomicU64 = AtomicU64::new(0);
            let n = N.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!("awanfs_boot_bench_{}_{}_{}", name, std::process::id(), n));
            let _ = std::fs::remove_dir_all(&path);
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn boot_reports_peak_and_retained_heap() {
        let dir = TempDir::new("heap");
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(fill(&dir.0, 16, 64)).unwrap();
        drop(rt);

        let mut out = Vec::new();
        boot_with(&dir.0, 16, 64, &mut out).unwrap();
        let line = String::from_utf8(out).unwrap();
        let json: serde_json::Value = serde_json::from_str(line.trim()).unwrap();

        assert_eq!(json["phase"], "boot");
        assert_eq!(json["objects"], 16);
        assert_eq!(json["wrong"], 0);
        let peak = json["peak_heap_mib"].as_f64().expect("boot JSON line missing numeric peak_heap_mib");
        let after = json["heap_after_mib"].as_f64().expect("boot JSON line missing numeric heap_after_mib");
        assert!(peak > 0.0, "expected peak_heap_mib > 0.0, got {}", peak);
        assert!(after >= 0.0, "expected heap_after_mib >= 0.0, got {}", after);
        assert!(peak >= after, "expected peak_heap_mib ({}) >= heap_after_mib ({})", peak, after);

        // An earlier temporary allocation must not inflate a later boot's peak_heap_mib.
        let spike_mib = (peak.ceil() as usize) + 32;
        let spike = std::hint::black_box(vec![0u8; spike_mib << 20]);
        drop(spike);

        let mut out2 = Vec::new();
        boot_with(&dir.0, 16, 64, &mut out2).unwrap();
        let json2: serde_json::Value = serde_json::from_str(String::from_utf8(out2).unwrap().trim()).unwrap();
        let peak2 = json2["peak_heap_mib"].as_f64().unwrap();
        assert!(
            peak2 < peak + 16.0,
            "expected reset_heap_peak to drop prior spike ({} MiB); peak1={}, peak2={}",
            spike_mib,
            peak,
            peak2
        );
    }
}
