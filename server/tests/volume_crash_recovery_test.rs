//! Regression tests for volume.rs crash recovery (#392, prerequisite for #213).

use s3_server::attrs::{Attrs, TAG_CONTENT_TYPE};
use s3_server::clock::{Clock, SystemClock};
use s3_server::direct_io::SECTOR_SIZE;
use s3_server::meta_index::{AWAN_FLAG_ACTIVE, AWAN_FLAG_TOMBSTONE};
use s3_server::sim::{CrashMode, SimFs, SimRng};
use s3_server::vfile::{FileKind, VolFile, VolFs};
use s3_server::volume::{NeedleHeader, ScannedNeedle, VolumeManager, NEEDLE_MAGIC};
use std::collections::HashMap;
use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use xxhash_rust::xxh3::xxh3_64;

const IDX_RECORD: u64 = 42;

fn clock() -> Arc<dyn Clock> {
    Arc::new(SystemClock)
}

fn pattern(len: usize, seed: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + seed * 7 + 13) % 251) as u8).collect()
}

fn scan_all(vm: &VolumeManager<SimFs>) -> Vec<ScannedNeedle> {
    let mut out = Vec::new();
    vm.scan_needles(|n| out.push(n)).unwrap();
    out
}

fn read_via_copy_payload(vm: &VolumeManager<SimFs>, n: &ScannedNeedle) -> Vec<u8> {
    let mut got = Vec::with_capacity(n.payload_size as usize);
    let len = vm
        .copy_payload(n.offset, n.payload_crc32c, |chunk| {
            got.extend_from_slice(chunk);
            Ok(())
        })
        .unwrap_or_else(|e| panic!("copy_payload failed at offset {} for key {:?}: {:?}", n.offset, n.key, e));
    assert_eq!(len, n.payload_size);
    got
}

#[test]
fn torn_idx_tail_is_truncated_on_open_and_synced_delete_does_not_resurrect() {
    let dir = Path::new("sim/torn_idx_tail");
    let idx_path = dir.join("volume_0001.idx");
    let fs = SimFs::new();

    {
        let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
        vm.write_needle(1, "victim", &pattern(5000, 1), AWAN_FLAG_ACTIVE, 0, 0).unwrap();
        vm.sync_now().unwrap();
    }

    {
        let idx = fs.open(&idx_path, FileKind::Journal, false).unwrap();
        assert_eq!(idx.len().unwrap(), IDX_RECORD);
        let torn_tail = [0x5Au8; 17];
        idx.write_at(IDX_RECORD, &torn_tail).unwrap();
        idx.sync_data().unwrap();
        assert_eq!(idx.len().unwrap(), IDX_RECORD + 17);
    }

    {
        let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
        let idx = fs.open(&idx_path, FileKind::Journal, false).unwrap();
        assert_eq!(
            idx.len().unwrap() % IDX_RECORD,
            0,
            "open_with must truncate torn .idx tail bytes (len {})",
            idx.len().unwrap()
        );
        assert_eq!(idx.len().unwrap(), IDX_RECORD);

        vm.fail_next_write();
        assert!(vm.write_needle(1, "gap-obj", &pattern(5000, 2), AWAN_FLAG_ACTIVE, 0, 0).is_err());

        vm.write_needle(1, "victim", &[], AWAN_FLAG_TOMBSTONE, 0, 0).unwrap();
        vm.sync_now().unwrap();
        assert_eq!(idx.len().unwrap(), 2 * IDX_RECORD);
    }

    let vm_after = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
    let idx_after = fs.open(&idx_path, FileKind::Journal, false).unwrap();
    assert_eq!(idx_after.len().unwrap(), 2 * IDX_RECORD);

    let needles = scan_all(&vm_after);
    let mut live: HashMap<String, bool> = HashMap::new();
    for n in &needles {
        live.insert(n.key.clone(), n.flags & AWAN_FLAG_TOMBSTONE == 0);
    }
    assert_eq!(
        live.get("victim").copied(),
        Some(false),
        "synced delete tombstone past gap must remain visible and not resurrect victim; needles={:?}",
        needles.iter().map(|n| (&n.key, n.offset, n.flags)).collect::<Vec<_>>()
    );
}

#[test]
fn seeded_crash_tearing_idx_tail_recovers_aligned_journal_across_reopen() {
    let dir = Path::new("sim/seeded_torn_idx");
    let idx_path = dir.join("volume_0001.idx");
    let fs = SimFs::new();

    {
        let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
        vm.write_needle(1, "k1", &pattern(5000, 1), AWAN_FLAG_ACTIVE, 0, 0).unwrap();
        vm.sync_now().unwrap();
        vm.write_needle(1, "k2", &pattern(5000, 2), AWAN_FLAG_ACTIVE, 0, 0).unwrap();
    }

    let mut saw_torn_idx = false;
    for seed in 0..200u64 {
        let after = fs.crashed(CrashMode::SeededPrefix, &mut SimRng::new(seed));
        let raw_idx_len = after.open(&idx_path, FileKind::Journal, false).unwrap().len().unwrap();
        if raw_idx_len % IDX_RECORD != 0 {
            saw_torn_idx = true;
        }
        let vm = VolumeManager::open_with(&after, dir, 1, false, clock()).unwrap();
        let truncated_idx_len = after.open(&idx_path, FileKind::Journal, false).unwrap().len().unwrap();
        assert_eq!(
            truncated_idx_len % IDX_RECORD,
            0,
            "seed {}: open_with left misaligned .idx of len {}",
            seed,
            truncated_idx_len
        );

        vm.fail_next_write();
        assert!(vm.write_needle(1, "gap", &pattern(5000, 3), AWAN_FLAG_ACTIVE, 0, 0).is_err());
        vm.write_needle(1, "k1", &[], AWAN_FLAG_TOMBSTONE, 0, 0).unwrap();
        vm.sync_now().unwrap();

        let vm2 = VolumeManager::open_with(&after, dir, 1, false, clock()).unwrap();
        let needles = scan_all(&vm2);
        let mut live: HashMap<String, bool> = HashMap::new();
        for n in &needles {
            live.insert(n.key.clone(), n.flags & AWAN_FLAG_TOMBSTONE == 0);
        }
        assert_eq!(live.get("k1").copied(), Some(false), "seed {}: k1 resurrected after restart", seed);
    }
    assert!(saw_torn_idx, "expected at least one seed to tear .idx");
}

#[test]
fn multi_sector_needle_crash_in_preextended_volume_never_exposes_torn_payload() {
    let dir = Path::new("sim/preextended_multi");
    let fs = SimFs::new();
    let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
    vm.set_growth_step(16 * 1024);

    let p1 = pattern(5000, 1);
    let p2 = pattern(9000, 2);
    vm.write_needle(1, "s1", &p1, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
    vm.write_needle(1, "s2", &p2, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
    vm.sync_now().unwrap();
    assert!(
        vm.allocated_len() > vm.offset(),
        "precondition: volume must be pre-extended past offset (allocated={}, offset={})",
        vm.allocated_len(),
        vm.offset()
    );

    let p3 = pattern(12_000, 3);
    vm.write_needle(1, "u3", &p3, AWAN_FLAG_ACTIVE, 0, 0).unwrap();

    let mut saw_partial = false;
    let mut saw_whole = false;
    for seed in 0..300u64 {
        let after = fs.crashed(CrashMode::SeededPrefix, &mut SimRng::new(seed));
        let reopened = VolumeManager::open_with(&after, dir, 1, false, clock()).unwrap();
        let needles = scan_all(&reopened);
        let keys: Vec<&str> = needles.iter().map(|n| n.key.as_str()).collect();
        assert!(
            keys == ["s1", "s2"] || keys == ["s1", "s2", "u3"],
            "seed {}: unexpected recovered keys {:?}",
            seed,
            keys
        );
        if keys.len() == 2 {
            saw_partial = true;
        } else {
            saw_whole = true;
        }
        for n in &needles {
            let got = read_via_copy_payload(&reopened, n);
            let want = match n.key.as_str() {
                "s1" => &p1,
                "s2" => &p2,
                "u3" => &p3,
                other => panic!("unexpected key {}", other),
            };
            assert_eq!(&got[..], &want[..], "seed {}: payload mismatch for {}", seed, n.key);
        }

        let p4 = pattern(6000, 4);
        let loc4 = reopened.write_needle(1, "after", &p4, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
        let mut got4 = Vec::new();
        reopened.copy_payload(loc4.offset, loc4.payload_crc32c, |c| {
            got4.extend_from_slice(c);
            Ok(())
        }).unwrap();
        assert_eq!(got4, p4, "seed {}: post-recovery write/read failed", seed);
    }
    assert!(saw_partial && saw_whole, "expected both truncated and whole crash outcomes (partial={}, whole={})", saw_partial, saw_whole);
}

#[test]
fn multi_and_single_sector_crashes_in_preextended_volume_never_expose_torn_needles() {
    let dir = Path::new("sim/preextended_mixed");
    let fs = SimFs::new();
    let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
    vm.set_growth_step(16 * 1024);

    let p1 = pattern(5000, 11);
    vm.write_needle(1, "base", &p1, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
    vm.sync_now().unwrap();
    assert!(vm.allocated_len() > vm.offset());

    let mut a = Attrs::new();
    a.set(TAG_CONTENT_TYPE, b"application/octet-stream");
    let attrs = a.encode().unwrap();
    let p2 = pattern(1500, 12);
    let p3 = pattern(11_000, 13);
    vm.write_needle_with_attrs(1, "single-unsynced", &attrs, &p2, AWAN_FLAG_ACTIVE, 0, 0, None).unwrap();
    vm.write_needle(1, "multi-unsynced", &p3, AWAN_FLAG_ACTIVE, 0, 0).unwrap();

    for seed in 0..300u64 {
        let after = fs.crashed(CrashMode::SeededPrefix, &mut SimRng::new(seed));
        let reopened = VolumeManager::open_with(&after, dir, 1, false, clock()).unwrap();
        let needles = scan_all(&reopened);
        let keys: Vec<&str> = needles.iter().map(|n| n.key.as_str()).collect();
        assert!(
            keys == ["base"]
                || keys == ["base", "single-unsynced"]
                || keys == ["base", "single-unsynced", "multi-unsynced"],
            "seed {}: unexpected prefix {:?}",
            seed,
            keys
        );
        for n in &needles {
            let got = read_via_copy_payload(&reopened, n);
            let want = match n.key.as_str() {
                "base" => &p1,
                "single-unsynced" => &p2,
                "multi-unsynced" => &p3,
                other => panic!("unexpected key {}", other),
            };
            assert_eq!(&got[..], &want[..], "seed {}: payload mismatch for {}", seed, n.key);
        }

        let p4 = pattern(7000, 14);
        let loc4 = reopened.write_needle(1, "post", &p4, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
        let mut got4 = Vec::new();
        reopened.copy_payload(loc4.offset, loc4.payload_crc32c, |c| {
            got4.extend_from_slice(c);
            Ok(())
        }).unwrap();
        assert_eq!(got4, p4, "seed {}: post-recovery write/read failed", seed);
    }
}

#[test]
fn multi_unsynced_with_independent_idx_survival_never_accepts_torn_needle() {
    let dir = Path::new("sim/multi_unsynced_idx");
    let fs = SimFs::new();
    let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
    vm.set_growth_step(16 * 1024);

    let p0 = pattern(4096, 20);
    vm.write_needle(1, "synced", &p0, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
    vm.sync_now().unwrap();
    assert!(vm.allocated_len() > vm.offset());

    let p1 = pattern(6000, 21);
    let p2 = pattern(6000, 22);
    let p3 = pattern(6000, 23);
    vm.write_needle(1, "u1", &p1, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
    vm.write_needle(1, "u2", &p2, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
    vm.write_needle(1, "u3", &p3, AWAN_FLAG_ACTIVE, 0, 0).unwrap();

    for seed in 0..300u64 {
        let after = fs.crashed(CrashMode::SeededPrefix, &mut SimRng::new(seed));
        let reopened = VolumeManager::open_with(&after, dir, 1, false, clock()).unwrap();
        let needles = scan_all(&reopened);
        let keys: Vec<&str> = needles.iter().map(|n| n.key.as_str()).collect();
        assert!(
            keys == ["synced"]
                || keys == ["synced", "u1"]
                || keys == ["synced", "u1", "u2"]
                || keys == ["synced", "u1", "u2", "u3"],
            "seed {}: unexpected prefix {:?}",
            seed,
            keys
        );
        for n in &needles {
            let got = read_via_copy_payload(&reopened, n);
            let want = match n.key.as_str() {
                "synced" => &p0,
                "u1" => &p1,
                "u2" => &p2,
                "u3" => &p3,
                other => panic!("unexpected key {}", other),
            };
            assert_eq!(&got[..], &want[..], "seed {}: payload mismatch for {}", seed, n.key);
        }
    }
}

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_vcr_{}_{}_{}", name, std::process::id(), nanos));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn journal_shorter_than_one_record_is_truncated_to_zero_on_open() {
    let dir = Path::new("sim/short_idx");
    let idx_path = dir.join("volume_0001.idx");
    let fs = SimFs::new();

    {
        let _vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
        let idx = fs.open(&idx_path, FileKind::Journal, false).unwrap();
        idx.write_at(0, &[0xABu8; 17]).unwrap();
        idx.sync_data().unwrap();
        assert_eq!(idx.len().unwrap(), 17);
    }

    let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
    let idx = fs.open(&idx_path, FileKind::Journal, false).unwrap();
    assert_eq!(idx.len().unwrap(), 0, "sub-record .idx tail must truncate to 0");

    let p = pattern(5000, 30);
    let loc = vm.write_needle(1, "first", &p, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
    vm.sync_now().unwrap();
    assert_eq!(idx.len().unwrap(), IDX_RECORD);

    let vm2 = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
    let needles = scan_all(&vm2);
    assert_eq!(needles.len(), 1);
    assert_eq!(needles[0].offset, loc.offset);
    assert_eq!(read_via_copy_payload(&vm2, &needles[0]), p);
}

#[test]
fn zero_byte_needle_with_long_key_and_attrs_torn_in_sector_zero_is_rejected() {
    let dir = Path::new("sim/torn_key_attrs");
    let fs = SimFs::new();
    let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
    vm.set_growth_step(16 * 1024);

    let base = pattern(5000, 40);
    vm.write_needle(1, "base", &base, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
    vm.sync_now().unwrap();

    let long_key = "long-key-".to_string() + &"k".repeat(250);
    let mut a = Attrs::new();
    a.set(TAG_CONTENT_TYPE, b"text/plain");
    let attrs = a.encode().unwrap();
    vm.write_needle_with_attrs(1, &long_key, &attrs, &[], AWAN_FLAG_ACTIVE, 0, 0, None).unwrap();

    for seed in 0..300u64 {
        let after = fs.crashed(CrashMode::SeededPrefix, &mut SimRng::new(seed));
        let reopened = VolumeManager::open_with(&after, dir, 1, false, clock()).unwrap();
        let needles = scan_all(&reopened);
        for n in &needles {
            assert!(
                n.key == "base" || n.key == long_key,
                "seed {}: recovered torn key {:?}",
                seed,
                n.key
            );
            let _ = read_via_copy_payload(&reopened, n);
        }
    }
}

#[test]
fn disk_sparse_volume_rejects_torn_multi_sector_needle_on_reopen() {
    let dir = temp_dir("sparse_torn");
    let base_end = {
        let vm = VolumeManager::open_sparse(&dir, 1).unwrap();
        let p1 = pattern(5000, 50);
        vm.write_needle(1, "good", &p1, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
        vm.sync_now().unwrap();
        vm.offset()
    };

    let payload = pattern(10_000, 51);
    let mut hdr = NeedleHeader {
        magic: NEEDLE_MAGIC,
        header_crc32c: 0,
        payload_crc32c: crc32c::crc32c(&payload),
        flags: AWAN_FLAG_ACTIVE,
        bucket_id: 1,
        key_len: 4,
        attrs_len: 0,
        key_hash: xxh3_64(b"torn"),
        payload_size: payload.len() as u64,
        created_at_ns: 1_700_000_000_000_000_000,
        etag_hi: 0,
        etag_lo: 0,
        reserved2: 0,
    };
    let mut hdr_bytes = [0u8; 64];
    hdr.encode(&mut hdr_bytes);
    hdr.header_crc32c = crc32c::crc32c(&hdr_bytes[8..64]);
    hdr.encode(&mut hdr_bytes);

    {
        let mut f = std::fs::OpenOptions::new().read(true).write(true).open(dir.join("volume_0001.dat")).unwrap();
        let mut sector0 = vec![0u8; SECTOR_SIZE];
        sector0[..64].copy_from_slice(&hdr_bytes);
        sector0[64..68].copy_from_slice(b"torn");
        f.seek(std::io::SeekFrom::Start(base_end)).unwrap();
        f.write_all(&sector0).unwrap();
        f.sync_all().unwrap();
    }

    let reopened = VolumeManager::open_sparse(&dir, 1).unwrap();
    assert_eq!(reopened.offset(), base_end, "sparse reopen must rewind past torn multi-sector needle");
    let mut keys = Vec::new();
    reopened.scan_needles(|n| keys.push(n.key)).unwrap();
    assert_eq!(keys, vec!["good"]);
    let _ = std::fs::remove_dir_all(&dir);
}
