//! #324: `VolumeManager::scan_needles` and `open` batch sequential reads into 1 MiB aligned buffers.

use s3_server::clock::ManualClock;
use s3_server::direct_io::SECTOR_SIZE;
use s3_server::sim::{CrashMode, SimFs, SimRng};
use s3_server::vfile::{FileKind, VolFile, VolFs};
use s3_server::{NeedleLocation, ScannedNeedle, VolumeManager, AWAN_FLAG_ACTIVE};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const MIB: u64 = 1 << 20;

static DIR_SEQ: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let seq = DIR_SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("awanfs_scan_batch_{}_{}_{}", name, std::process::id(), seq));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ExpectedNeedle {
    bucket_id: u16,
    key: String,
    offset: u64,
    payload_size: u64,
    etag_hi: u64,
    etag_lo: u64,
    flags: u16,
    payload_crc32c: u32,
    created_at_ns: u64,
}

impl From<ScannedNeedle> for ExpectedNeedle {
    fn from(n: ScannedNeedle) -> Self {
        Self {
            bucket_id: n.bucket_id,
            key: n.key,
            offset: n.offset,
            payload_size: n.payload_size,
            etag_hi: n.etag_hi,
            etag_lo: n.etag_lo,
            flags: n.flags,
            payload_crc32c: n.payload_crc32c,
            created_at_ns: n.created_at_ns,
        }
    }
}

fn pattern(len: usize, seed: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + seed * 17 + 29) % 251) as u8).collect()
}

fn record_write(
    expected: &mut Vec<ExpectedNeedle>,
    bucket_id: u16,
    key: String,
    payload_len: u64,
    etag_hi: u64,
    etag_lo: u64,
    flags: u16,
    created_at_ns: u64,
    loc: NeedleLocation,
) {
    assert_eq!(loc.payload_size, payload_len);
    expected.push(ExpectedNeedle {
        bucket_id,
        key,
        offset: loc.offset,
        payload_size: loc.payload_size,
        etag_hi,
        etag_lo,
        flags,
        payload_crc32c: loc.payload_crc32c,
        created_at_ns,
    });
}

fn collect_scan<Fs: VolFs>(vm: &VolumeManager<Fs>) -> (Vec<ExpectedNeedle>, u64) {
    let before = vm.work_counters().snapshot();
    let mut got = Vec::new();
    vm.scan_needles(|n| got.push(ExpectedNeedle::from(n))).unwrap();
    let reads = vm.work_counters().snapshot().since(&before).disk_reads;
    (got, reads)
}

#[test]
fn scan_batches_reads_across_1mib_boundaries_large_keys_and_gaps() {
    let dir = TempDir::new("disk");
    let clock = Arc::new(ManualClock::new(1_700_000_000_000_000_000));
    let vm = Arc::new(VolumeManager::open_mode_with_clock(&dir.0, 1, false, Arc::clone(&clock) as _).unwrap());
    let mut expected = Vec::new();
    let mut ts = 1_700_000_000_000_000_000u64;

    // 255 1-sector needles fill offsets [4096 .. 1_048_576), ending right at 1 MiB.
    for i in 0..255u64 {
        ts += 1_000;
        let k = format!("k-{:04}", i);
        let p = pattern(128, i as usize);
        let loc = vm.write_needle_at(1, &k, &p, AWAN_FLAG_ACTIVE, i + 1, i + 1001, Some(ts)).unwrap();
        record_write(&mut expected, 1, k, p.len() as u64, i + 1, i + 1001, AWAN_FLAG_ACTIVE, ts, loc);
    }
    assert_eq!(vm.offset(), MIB, "next needle must start at the last sector of the first 1 MiB read batch");

    // Header+key (5064 bytes) and payload straddle the 1 MiB + 4096 read-batch boundary (1_052_672).
    ts += 1_000;
    let big_key = "long-key-".to_string() + &"x".repeat(4991);
    assert_eq!(big_key.len(), 5000);
    let big_payload = pattern(4000, 777);
    let loc = vm.write_needle_at(2, &big_key, &big_payload, AWAN_FLAG_ACTIVE, 91, 92, Some(ts)).unwrap();
    assert_eq!(loc.offset, MIB);
    assert!(loc.offset + 64 + big_key.len() as u64 > MIB + SECTOR_SIZE as u64);
    record_write(&mut expected, 2, big_key, big_payload.len() as u64, 91, 92, AWAN_FLAG_ACTIVE, ts, loc);

    // Fill through the second 1 MiB batch, then inject an abandoned streamed writer and a failed write gap.
    for i in 255..500u64 {
        ts += 1_000;
        let k = format!("k-{:04}", i);
        let p = pattern(256, i as usize);
        let loc = vm.write_needle_at(1, &k, &p, AWAN_FLAG_ACTIVE, i + 1, i + 1001, Some(ts)).unwrap();
        record_write(&mut expected, 1, k, p.len() as u64, i + 1, i + 1001, AWAN_FLAG_ACTIVE, ts, loc);
    }

    let mut abandoned = vm.begin_needle(1, "abandoned-gap", 20_000, AWAN_FLAG_ACTIVE).unwrap();
    abandoned.write(&pattern(4000, 888)).unwrap();
    drop(abandoned);

    vm.fail_next_write();
    assert!(vm.write_needle_at(1, "failed-gap", &pattern(5000, 999), AWAN_FLAG_ACTIVE, 0, 0, Some(ts + 500)).is_err());

    // Payload-straddling needle near 2 MiB, followed by needles pushing total size past 2.9 MiB.
    ts += 1_000;
    let mid_payload = pattern(16_000, 555);
    let loc = vm.write_needle_at(3, "straddle-payload", &mid_payload, AWAN_FLAG_ACTIVE, 33, 44, Some(ts)).unwrap();
    record_write(&mut expected, 3, "straddle-payload".to_string(), mid_payload.len() as u64, 33, 44, AWAN_FLAG_ACTIVE, ts, loc);

    for i in 500..720u64 {
        ts += 1_000;
        let k = format!("k-{:04}", i);
        let p = pattern(300, i as usize);
        let loc = vm.write_needle_at(1, &k, &p, AWAN_FLAG_ACTIVE, i + 1, i + 1001, Some(ts)).unwrap();
        record_write(&mut expected, 1, k, p.len() as u64, i + 1, i + 1001, AWAN_FLAG_ACTIVE, ts, loc);
    }

    let volume_len = vm.offset();
    assert!(volume_len > 2 * MIB + 512 * 1024, "expected > 2.5 MiB volume, got {}", volume_len);
    let max_reads = volume_len.div_ceil(MIB) + 4;

    let (got, reads) = collect_scan(&vm);
    assert_eq!(got, expected, "scan_needles must return every live needle in exact offset order");
    assert!(reads >= 3, "expected >= 3 batched reads for {} bytes, got {}", volume_len, reads);
    assert!(
        reads <= max_reads,
        "expected batched reads <= {} for {} bytes ({} needles), got {}",
        max_reads,
        volume_len,
        expected.len(),
        reads
    );

    drop(vm);
    let reopened = VolumeManager::open_mode_with_clock(&dir.0, 1, false, clock).unwrap();
    let (reopened_got, reopened_reads) = collect_scan(&reopened);
    assert_eq!(reopened_got, expected, "reopened scan_needles must match live needles across gaps");
    assert!(
        reopened_reads <= max_reads,
        "expected reopened batched reads <= {}, got {}",
        max_reads,
        reopened_reads
    );
}

#[test]
fn simfs_scan_and_reopen_clamps_near_eof_and_torn_tail() {
    let dir = Path::new("sim/batch");
    let clock = Arc::new(ManualClock::new(1_700_000_100_000_000_000));
    let fs = SimFs::new();
    let vm = VolumeManager::open_with(&fs, dir, 1, false, Arc::clone(&clock) as _).unwrap();
    let mut synced_expected = Vec::new();
    let mut ts = 1_700_000_100_000_000_000u64;

    // Write > 2.6 MiB of synced needles (including a needle straddling 1 MiB + 4096 and a gap).
    for i in 0..255u64 {
        ts += 1_000;
        let k = format!("s-{:04}", i);
        let p = pattern(100, i as usize);
        let loc = vm.write_needle_at(1, &k, &p, AWAN_FLAG_ACTIVE, i + 1, i + 2001, Some(ts)).unwrap();
        record_write(&mut synced_expected, 1, k, p.len() as u64, i + 1, i + 2001, AWAN_FLAG_ACTIVE, ts, loc);
    }

    ts += 1_000;
    let boundary_payload = pattern(12_000, 42);
    let loc = vm.write_needle_at(1, "boundary", &boundary_payload, AWAN_FLAG_ACTIVE, 7, 8, Some(ts)).unwrap();
    record_write(&mut synced_expected, 1, "boundary".to_string(), boundary_payload.len() as u64, 7, 8, AWAN_FLAG_ACTIVE, ts, loc);

    vm.fail_next_write();
    assert!(vm.write_needle_at(1, "sim-gap", &pattern(5000, 9), AWAN_FLAG_ACTIVE, 0, 0, Some(ts + 500)).is_err());

    for i in 255..670u64 {
        ts += 1_000;
        let k = format!("s-{:04}", i);
        let p = pattern(200, i as usize);
        let loc = vm.write_needle_at(1, &k, &p, AWAN_FLAG_ACTIVE, i + 1, i + 2001, Some(ts)).unwrap();
        record_write(&mut synced_expected, 1, k, p.len() as u64, i + 1, i + 2001, AWAN_FLAG_ACTIVE, ts, loc);
    }
    vm.sync_now().unwrap();

    let volume_len = vm.offset();
    assert!(volume_len > 2 * MIB + 512 * 1024);
    let max_reads = volume_len.div_ceil(MIB) + 4;
    let (got, reads) = collect_scan(&vm);
    assert_eq!(got, synced_expected);
    assert!(reads <= max_reads, "simfs reads {} > {}", reads, max_reads);

    // Append unsynced multi-sector needles so SeededPrefix produces non-sector-aligned torn tails.
    let mut all_expected = synced_expected;
    for i in 0..3u64 {
        ts += 1_000;
        let k = format!("tail-{:02}", i);
        let p = pattern(9000, 700 + i as usize);
        let loc = vm.write_needle_at(1, &k, &p, AWAN_FLAG_ACTIVE, i + 1, i + 3001, Some(ts)).unwrap();
        record_write(&mut all_expected, 1, k, p.len() as u64, i + 1, i + 3001, AWAN_FLAG_ACTIVE, ts, loc);
    }

    let mut saw_torn_tail = false;
    for seed in 0..64u64 {
        let after = fs.crashed(CrashMode::SeededPrefix, &mut SimRng::new(seed));
        let dat = after.open(&dir.join("volume_0001.dat"), FileKind::Data, false).unwrap();
        let raw_len = dat.len().unwrap();
        if raw_len % SECTOR_SIZE as u64 != 0 {
            saw_torn_tail = true;
        }
        let reopened = VolumeManager::open_with(&after, dir, 1, false, Arc::clone(&clock) as _).unwrap();
        let (recovered, r_reads) = collect_scan(&reopened);
        assert!(
            recovered.len() >= all_expected.len() - 3 && all_expected.starts_with(&recovered),
            "seed {}: recovered {} needles did not match prefix of {}",
            seed,
            recovered.len(),
            all_expected.len()
        );
        let bound = reopened.offset().div_ceil(MIB) + 4;
        assert!(r_reads <= bound, "seed {}: reads {} > {}", seed, r_reads, bound);
    }
    assert!(saw_torn_tail, "expected at least one crash seed to leave a non-sector-aligned tail");
}
