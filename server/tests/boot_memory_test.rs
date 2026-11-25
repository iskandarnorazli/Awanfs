//! #323 (Decision 7A): Boot memory stays bounded by streaming `scan_needles` directly into
//! replay, removing the unbounded `needle_crc_map`, and streaming `.idx` journal scans without
//! retaining every needle offset in a `BTreeSet<u64>`.
//!
//! Uses its own `Counting` global allocator so no other test binary's allocations land in the
//! counts.

use bytes::Bytes;
use reqwest::StatusCode;
use s3_server::clock::SystemClock;
use s3_server::{
    compute_md5, current_boot_heap_part, start_s3_reactor, BootHeapPart, Durability, OpenLimits,
    S3ServiceState, AWAN_FLAG_ACTIVE,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static RECORDS_LIVE: AtomicIsize = AtomicIsize::new(0);
static RECORDS_PEAK: AtomicUsize = AtomicUsize::new(0);

fn reset_peak() {
    PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    let r_live = RECORDS_LIVE.load(Ordering::Relaxed).max(0) as usize;
    RECORDS_PEAK.store(r_live, Ordering::Relaxed);
}

// SAFETY: delegates every call to System unchanged; tracks only live and peak byte counts.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            let now = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
            if current_boot_heap_part() == BootHeapPart::NeedleRecords as usize {
                let prev = RECORDS_LIVE.fetch_add(layout.size() as isize, Ordering::Relaxed);
                RECORDS_PEAK.fetch_max((prev + layout.size() as isize).max(0) as usize, Ordering::Relaxed);
            }
        }
        p
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc_zeroed(layout);
        if !p.is_null() {
            let now = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
            if current_boot_heap_part() == BootHeapPart::NeedleRecords as usize {
                let prev = RECORDS_LIVE.fetch_add(layout.size() as isize, Ordering::Relaxed);
                RECORDS_PEAK.fetch_max((prev + layout.size() as isize).max(0) as usize, Ordering::Relaxed);
            }
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        if current_boot_heap_part() == BootHeapPart::NeedleRecords as usize {
            RECORDS_LIVE.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = System.realloc(ptr, layout, new_size);
        if !p.is_null() {
            let is_records = current_boot_heap_part() == BootHeapPart::NeedleRecords as usize;
            if new_size >= layout.size() {
                let delta = new_size - layout.size();
                let now = LIVE.fetch_add(delta, Ordering::Relaxed) + delta;
                PEAK.fetch_max(now, Ordering::Relaxed);
                if is_records {
                    let prev = RECORDS_LIVE.fetch_add(delta as isize, Ordering::Relaxed);
                    RECORDS_PEAK.fetch_max((prev + delta as isize).max(0) as usize, Ordering::Relaxed);
                }
            } else {
                let delta = layout.size() - new_size;
                LIVE.fetch_sub(delta, Ordering::Relaxed);
                if is_records {
                    RECORDS_LIVE.fetch_sub(delta as isize, Ordering::Relaxed);
                }
            }
        }
        p
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        static N: AtomicUsize = AtomicUsize::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!(
            "awanfs_boot_mem_{}_{}_{}_{}",
            name,
            std::process::id(),
            n,
            nanos
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn fill_objects(dir: &Path, count: u64, size: usize) {
    let limits = OpenLimits {
        max_objects: 1_000,
        spill_cache_bytes: Some(2 << 20),
    };
    let state = Arc::new(
        S3ServiceState::try_with_data_dir_limits(dir, Arc::new(SystemClock), limits).unwrap(),
    );
    let b_id = state.engine.write().create_bucket("boot-mem-bkt").unwrap();
    let created_ns = state.clock.now_ns();
    state.bucket_created_ns.write().insert(b_id, created_ns);
    let _ = state
        .bucket_configs
        .lock()
        .apply_bucket_create(b_id, "boot-mem-bkt", created_ns, "root", "");
    for vm in state.volumes.read().values() {
        vm.set_durability(Durability::Fast);
    }
    let payload = Bytes::from(vec![0x5Au8; size]);
    let (hi, lo, _, _) = compute_md5(&payload);
    for i in 0..count {
        let key = format!("k/{:03}/{:08}", i % 251, i);
        state
            .store_object(
                b_id,
                "boot-mem-bkt",
                &key,
                &[],
                &payload,
                AWAN_FLAG_ACTIVE,
                hi,
                lo,
                Some(created_ns + i),
            )
            .await
            .unwrap();
    }
    state.sync_all_volumes().unwrap();
}

fn measure_boot_heap(dir: &Path, count: u64) -> (usize, usize, usize) {
    let limits = OpenLimits {
        max_objects: 1_000,
        spill_cache_bytes: Some(2 << 20),
    };
    let before = LIVE.load(Ordering::Relaxed);
    reset_peak();
    let state =
        S3ServiceState::try_with_data_dir_limits(dir, Arc::new(SystemClock), limits).unwrap();
    let peak = PEAK.load(Ordering::Relaxed).saturating_sub(before);
    let retained = LIVE.load(Ordering::Relaxed).saturating_sub(before);
    let records_peak = RECORDS_PEAK.load(Ordering::Relaxed);
    let engine = state.engine.read();
    let b_id = engine.lookup_bucket("boot-mem-bkt").unwrap();
    for i in (0..count).step_by((count / 200).max(1) as usize) {
        let key = format!("k/{:03}/{:08}", i % 251, i);
        assert_eq!(engine.get_object(b_id, key.as_bytes()).unwrap().size, 64);
    }
    drop(engine);
    drop(state);
    (peak, retained, records_peak)
}

#[test]
fn boot_heap_does_not_retain_per_needle_map_or_buffer_volume_records() {
    let dir_small = TempDir::new("small");
    let dir_large = TempDir::new("large");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(fill_objects(&dir_small.0, 5_000, 64));
    rt.block_on(fill_objects(&dir_large.0, 60_000, 64));
    drop(rt);

    let (_peak_small, retained_small, records_small) = measure_boot_heap(&dir_small.0, 5_000);
    let (_peak_large, retained_large, records_large) = measure_boot_heap(&dir_large.0, 60_000);

    let retained_growth = retained_large.saturating_sub(retained_small);

    // Before the fix: +55,000 needles add ~3.0 MiB in needle_crc_map + ~0.55 MiB in
    // VolumeManager::indexed (> 3.5 MiB retained growth), and Vec<ScannedNeedle> holds ~6 MiB
    // in NeedleRecords at 60k objects.
    // After the fix: retained growth is only SpillIndex fences/Bloom filters (< 1 MiB) and
    // NeedleRecords peak stays flat at the 1 MiB scan window (< 2 MiB) regardless of object count.
    assert!(
        retained_growth < 1_048_576,
        "retained boot heap grew by {} bytes from 5k to 60k objects (expected < 1 MiB without needle_crc_map/indexed); small={}, large={}",
        retained_growth,
        retained_small,
        retained_large
    );
    assert!(
        records_large < 2 << 20,
        "needle_records peak was {} bytes at 60k objects (expected < 2 MiB with streaming scan_needles; small={})",
        records_large,
        records_small
    );
}

#[tokio::test]
async fn header_crc_verification_rejects_corrupted_payload_and_header_after_boot() {
    let dir = TempDir::new("crc_after_boot");
    let payload = vec![0xA7u8; 32 * 1024];

    {
        let state = Arc::new(S3ServiceState::with_data_dir(&dir.0));
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
        let url = format!("http://{}", handle.local_addr);
        let c = reqwest::Client::new();
        assert_eq!(c.put(format!("{}/bkt", url)).send().await.unwrap().status(), StatusCode::OK);
        assert_eq!(
            c.put(format!("{}/bkt/obj", url))
                .body(payload.clone())
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        handle.shutdown();
    }

    // Boot fresh without needle_crc_map and verify GET succeeds, then corrupt payload on disk.
    let state = Arc::new(S3ServiceState::with_data_dir(&dir.0));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();

    let res = c.get(format!("{}/bkt/obj", url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(&res.bytes().await.unwrap()[..], &payload[..]);

    let dat = dir.0.join("volume_0001.dat");
    let mut f = std::fs::OpenOptions::new().read(true).write(true).open(&dat).unwrap();
    let mut head = Vec::new();
    Read::by_ref(&mut f).take(1 << 20).read_to_end(&mut head).unwrap();
    let pos = 4096
        + head[4096..]
            .windows(64)
            .position(|w| w == &payload[512..576])
            .expect("payload bytes in volume");
    f.seek(SeekFrom::Start(pos as u64)).unwrap();
    f.write_all(&[head[pos] ^ 0xFF]).unwrap();
    f.sync_all().unwrap();
    drop(f);
    state.payload_cache.clear();

    let corrupt_get = c.get(format!("{}/bkt/obj", url)).send().await.unwrap();
    assert_eq!(
        corrupt_get.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "corrupted payload after boot must fail header-CRC verification"
    );

    // Also verify header corruption (`payload_crc32c` flipped without updating `header_crc32c`)
    // is rejected, and large (> 4 MiB) + CopyObject + Tagging paths work after boot.
    assert_eq!(
        c.put(format!("{}/bkt/obj2", url))
            .body(payload.clone())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let large_payload = vec![0x3Cu8; 5 * 1024 * 1024];
    assert_eq!(
        c.put(format!("{}/bkt/large", url))
            .body(large_payload.clone())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let copy_res = c
        .put(format!("{}/bkt/copied", url))
        .header("x-amz-copy-source", "/bkt/obj2")
        .send()
        .await
        .unwrap();
    assert_eq!(copy_res.status(), StatusCode::OK);
    let tag_res = c
        .put(format!("{}/bkt/obj2?tagging", url))
        .body("<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>")
        .send()
        .await
        .unwrap();
    assert_eq!(tag_res.status(), StatusCode::OK);
    handle.shutdown();

    // Reboot again and verify large GET, copied GET, tagging GET, and header CRC corruption.
    let state = Arc::new(S3ServiceState::with_data_dir(&dir.0));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);

    let got_large = c.get(format!("{}/bkt/large", url)).send().await.unwrap();
    assert_eq!(got_large.status(), StatusCode::OK);
    assert_eq!(got_large.bytes().await.unwrap().len(), large_payload.len());

    let got_copied = c.get(format!("{}/bkt/copied", url)).send().await.unwrap();
    assert_eq!(got_copied.status(), StatusCode::OK);
    assert_eq!(&got_copied.bytes().await.unwrap()[..], &payload[..]);

    let got_tag = c.get(format!("{}/bkt/obj2?tagging", url)).send().await.unwrap();
    assert_eq!(got_tag.status(), StatusCode::OK);
    assert!(got_tag.text().await.unwrap().contains("<Value>v</Value>"));

    // Flip `payload_crc32c` (byte 8) of `obj2`'s 64-byte header on disk.
    let meta = {
        let engine = state.engine.read();
        let b_id = engine.lookup_bucket("bkt").unwrap();
        engine.get_object(b_id, b"copied").unwrap()
    };
    let mut f = std::fs::OpenOptions::new().read(true).write(true).open(&dat).unwrap();
    let mut b = [0u8; 1];
    f.seek(SeekFrom::Start(meta.vol_offset + 8)).unwrap();
    f.read_exact(&mut b).unwrap();
    f.seek(SeekFrom::Start(meta.vol_offset + 8)).unwrap();
    f.write_all(&[b[0] ^ 0xFF]).unwrap();
    f.sync_all().unwrap();
    drop(f);
    state.payload_cache.clear();

    let hdr_corrupt = c.get(format!("{}/bkt/copied", url)).send().await.unwrap();
    assert_eq!(
        hdr_corrupt.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "corrupted 64-byte needle header must be rejected"
    );
    handle.shutdown();
}

#[tokio::test]
async fn streaming_replay_preserves_later_writes_win_across_volumes() {
    let dir = TempDir::new("replay_order");
    let c = reqwest::Client::new();
    {
        let state = Arc::new(S3ServiceState::with_data_dir(&dir.0));
        state.set_volume_span(64 * 1024);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
        let url = format!("http://{}", handle.local_addr);
        assert_eq!(c.put(format!("{}/bkt", url)).send().await.unwrap().status(), StatusCode::OK);

        // Overwrite and delete across volume rollovers: later writes must win on replay.
        let v1 = vec![0x11u8; 16 * 1024];
        let v2 = vec![0x22u8; 16 * 1024];
        let v3 = vec![0x33u8; 16 * 1024];
        assert_eq!(c.put(format!("{}/bkt/k1", url)).body(v1).send().await.unwrap().status(), StatusCode::OK);
        assert_eq!(c.put(format!("{}/bkt/k2", url)).body(v2.clone()).send().await.unwrap().status(), StatusCode::OK);
        assert_eq!(c.put(format!("{}/bkt/k1", url)).body(v3.clone()).send().await.unwrap().status(), StatusCode::OK);
        assert_eq!(c.delete(format!("{}/bkt/k2", url)).send().await.unwrap().status(), StatusCode::NO_CONTENT);
        assert_eq!(c.put(format!("{}/bkt/k3", url)).body(v2).send().await.unwrap().status(), StatusCode::OK);
        assert!(state.volume_ids().len() >= 2, "expected volume rollover across writes");
        handle.shutdown();
    }

    let state = Arc::new(S3ServiceState::with_data_dir(&dir.0));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);

    let r1 = c.get(format!("{}/bkt/k1", url)).send().await.unwrap();
    assert_eq!(r1.status(), StatusCode::OK);
    assert_eq!(&r1.bytes().await.unwrap()[..], &vec![0x33u8; 16 * 1024][..]);

    let r2 = c.get(format!("{}/bkt/k2", url)).send().await.unwrap();
    assert_eq!(r2.status(), StatusCode::NOT_FOUND);

    let r3 = c.get(format!("{}/bkt/k3", url)).send().await.unwrap();
    assert_eq!(r3.status(), StatusCode::OK);
    assert_eq!(&r3.bytes().await.unwrap()[..], &vec![0x22u8; 16 * 1024][..]);
    handle.shutdown();
}
