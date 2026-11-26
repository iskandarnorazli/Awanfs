//! Regression tests for volume.rs crash recovery (#392, prerequisite for #213).

use s3_server::attrs::{Attrs, TAG_CONTENT_TYPE};
use s3_server::clock::{Clock, SystemClock};
use s3_server::direct_io::SECTOR_SIZE;
use s3_server::meta_index::{AWAN_FLAG_ACTIVE, AWAN_FLAG_TOMBSTONE};
use s3_server::sim::{CrashMode, SimFault, SimFile, SimFs, SimOp, SimRng};
use s3_server::vfile::{FileKind, VolFile, VolFs};
use s3_server::volume::{NeedleHeader, ScannedNeedle, VolumeError, VolumeManager, NEEDLE_MAGIC};
use std::collections::HashMap;
use std::io::{self, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
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

fn scan_all<Fs: VolFs>(vm: &VolumeManager<Fs>) -> Vec<ScannedNeedle> {
    let mut out = Vec::new();
    vm.scan_needles(|n| out.push(n)).unwrap();
    out
}

fn read_via_copy_payload<Fs: VolFs>(vm: &VolumeManager<Fs>, n: &ScannedNeedle) -> Vec<u8> {
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
        // #412: k2's record is written by a sync, after the volume sync. The journal's own sync
        // fails, so the record stays unsynced and a crash can tear it.
        assert!(fs.arm(&idx_path, SimOp::Sync, SimFault::Eio));
        assert!(vm.sync_now().is_err());
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

fn idx_has_offset(idx_bytes: &[u8], target: u64) -> bool {
    idx_bytes
        .chunks_exact(IDX_RECORD as usize)
        .any(|rec| u64::from_le_bytes(rec[12..20].try_into().unwrap()) == target)
}

#[test]
fn seed_45_torn_mid_volume_streamed_needle_rejected_and_later_indexed_preserved() {
    let dir = Path::new("sim/seed45_mid_volume");
    let dat_path = dir.join("volume_0001.dat");
    let idx_path = dir.join("volume_0001.idx");
    let fs = SimFs::new();
    let vm = Arc::new(VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap());

    let p2 = pattern(54_650, 45);
    let p3 = pattern(17_285, 46);

    let mut w = vm.begin_needle(1, "k2", p2.len() as u64, AWAN_FLAG_ACTIVE).unwrap();
    assert_eq!(w.offset(), 4096);
    w.write(&p2).unwrap();

    let loc3 = vm.write_needle(1, "k3", &p3, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
    assert_eq!(loc3.offset, 61_440);
    // #412: k3's record reaches the journal only through a sync.
    vm.sync_now().unwrap();

    w.finish(0, 0).unwrap();

    let p2_crc = crc32c::crc32c(&p2);
    let mut saw_torn_mid_volume = false;
    for seed in 0..1000u64 {
        let after = fs.crashed(CrashMode::SeededPrefix, &mut SimRng::new(seed));
        let dat_bytes = after.file(&dat_path).unwrap().contents();
        let idx_bytes = after.file(&idx_path).unwrap().contents();
        let k2_hdr_ok = dat_bytes.len() >= 4096 + 64
            && NeedleHeader::decode(dat_bytes[4096..4096 + 64].try_into().unwrap()).is_ok();
        let k2_payload_ok = dat_bytes.len() >= 4096 + 66 + p2.len()
            && &dat_bytes[4096 + 64..4096 + 66] == b"k2"
            && crc32c::crc32c(&dat_bytes[4096 + 66..4096 + 66 + p2.len()]) == p2_crc;
        let k2_in_idx = idx_has_offset(&idx_bytes, 4096);
        let k3_in_idx = idx_has_offset(&idx_bytes, 61_440);

        let reopened = VolumeManager::open_with(&after, dir, 1, false, clock()).unwrap();
        let needles = scan_all(&reopened);
        for n in &needles {
            let got = read_via_copy_payload(&reopened, n);
            match n.key.as_str() {
                "k2" => assert_eq!(got, p2, "seed {}: torn k2 exposed", seed),
                "k3" => assert_eq!(got, p3, "seed {}: k3 corrupted", seed),
                other => panic!("seed {}: unexpected key {:?}", seed, other),
            }
        }

        if k2_hdr_ok && !k2_payload_ok && !k2_in_idx && k3_in_idx && dat_bytes.len() >= 81_920 {
            saw_torn_mid_volume = true;
            let keys: Vec<&str> = needles.iter().map(|n| n.key.as_str()).collect();
            assert_eq!(
                keys,
                vec!["k3"],
                "seed {}: torn mid-volume k2 at 4096 must be skipped while preserving k3 at 61440",
                seed
            );

            let reopened2 = VolumeManager::open_with(&after, dir, 1, false, clock()).unwrap();
            let needles2 = scan_all(&reopened2);
            let keys2: Vec<&str> = needles2.iter().map(|n| n.key.as_str()).collect();
            assert_eq!(
                keys2,
                vec!["k3"],
                "seed {}: second reopen must still skip torn mid-volume k2 and keep k3",
                seed
            );
            assert_eq!(read_via_copy_payload(&reopened2, &needles2[0]), p3);
        }
    }
    assert!(
        saw_torn_mid_volume,
        "expected seeded crash sweep to hit torn sector 0 at 4096 with k3 indexed at 61440"
    );
}

#[test]
fn torn_mid_volume_sector_zero_key_or_payload_survives_two_reopens() {
    for (name, tear_at) in [("sim/torn_mid_key", 65usize), ("sim/torn_mid_payload", 128usize)] {
        let dir = Path::new(name);
        let dat_path = dir.join("volume_0001.dat");
        let idx_path = dir.join("volume_0001.idx");
        let fs = SimFs::new();

        let p2 = pattern(54_650, 71);
        let p3 = pattern(17_285, 72);
        let p4 = pattern(5_000, 73);

        let vm = Arc::new(VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap());
        let mut w = vm.begin_needle(1, "k2", p2.len() as u64, AWAN_FLAG_ACTIVE).unwrap();
        assert_eq!(w.offset(), 4096);
        w.write(&p2).unwrap();
        let loc3 = vm.write_needle(1, "k3", &p3, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
        assert_eq!(loc3.offset, 61_440);
        w.finish(0, 0).unwrap();
        vm.sync_now().unwrap();
        drop(vm);

        // Tear sector 0 of offset 4096 after byte `tear_at` (leaving NeedleHeader intact)
        // and drop k2's journal record so only k3 (61440) remains in .idx.
        let mut dat = fs.file(&dat_path).unwrap().contents();
        dat[4096 + tear_at..4096 + SECTOR_SIZE].fill(0);
        let dat_file = fs.open(&dat_path, FileKind::Data, false).unwrap();
        dat_file.write_aligned_at(0, &dat).unwrap();
        dat_file.sync_data().unwrap();

        let idx = fs.file(&idx_path).unwrap().contents();
        let kept_idx: Vec<u8> = idx
            .chunks_exact(IDX_RECORD as usize)
            .filter(|rec| u64::from_le_bytes(rec[12..20].try_into().unwrap()) == 61_440)
            .flatten()
            .copied()
            .collect();
        assert_eq!(kept_idx.len(), IDX_RECORD as usize);
        let idx_file = fs.open(&idx_path, FileKind::Journal, false).unwrap();
        idx_file.write_at(0, &kept_idx).unwrap();
        idx_file.set_len(IDX_RECORD).unwrap();
        idx_file.sync_data().unwrap();

        let vm1 = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
        assert_eq!(vm1.offset(), 81_920);
        let n1 = scan_all(&vm1);
        assert_eq!(n1.iter().map(|n| n.key.as_str()).collect::<Vec<_>>(), vec!["k3"]);
        assert_eq!(read_via_copy_payload(&vm1, &n1[0]), p3);

        let loc4 = vm1.write_needle(1, "k4", &p4, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
        assert_eq!(loc4.offset, 81_920);
        vm1.sync_now().unwrap();
        drop(vm1);

        let vm2 = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
        let n2 = scan_all(&vm2);
        assert_eq!(n2.iter().map(|n| n.key.as_str()).collect::<Vec<_>>(), vec!["k3", "k4"]);
        assert_eq!(read_via_copy_payload(&vm2, &n2[0]), p3);
        assert_eq!(read_via_copy_payload(&vm2, &n2[1]), p4);
    }
}

struct FaultReadAfterFile {
    inner: Arc<SimFile>,
    kind: FileKind,
    fail_from_offset: Arc<AtomicU64>,
}

impl VolFile for FaultReadAfterFile {
    fn write_aligned_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        self.inner.write_aligned_at(offset, buf)
    }
    fn read_aligned_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        if self.kind == FileKind::Data && offset >= self.fail_from_offset.load(Ordering::SeqCst) {
            return Err(io::Error::new(io::ErrorKind::Other, "simulated Data::Read EIO"));
        }
        self.inner.read_aligned_at(offset, buf)
    }
    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        self.inner.write_at(offset, buf)
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        self.inner.read_at(offset, buf)
    }
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()
    }
    fn sync_all(&self) -> io::Result<()> {
        self.inner.sync_all()
    }
    fn make_sparse(&self) -> io::Result<()> {
        self.inner.make_sparse()
    }
    fn map(&self) -> io::Result<Option<memmap2::Mmap>> {
        self.inner.map()
    }
}

struct FaultReadAfterFs {
    inner: SimFs,
    fail_from_offset: Arc<AtomicU64>,
}

impl FaultReadAfterFs {
    fn new() -> Self {
        Self {
            inner: SimFs::new(),
            fail_from_offset: Arc::new(AtomicU64::new(u64::MAX)),
        }
    }
}

impl VolFs for FaultReadAfterFs {
    type File = FaultReadAfterFile;

    fn open(&self, path: &Path, kind: FileKind, create: bool) -> io::Result<Arc<Self::File>> {
        let f = self.inner.open(path, kind, create)?;
        Ok(Arc::new(FaultReadAfterFile {
            inner: f,
            kind,
            fail_from_offset: Arc::clone(&self.fail_from_offset),
        }))
    }
    fn exists(&self, path: &Path) -> bool {
        self.inner.exists(path)
    }
    fn remove(&self, path: &Path) -> io::Result<()> {
        self.inner.remove(path)
    }
    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        self.inner.list(dir)
    }
}

#[test]
fn scan_needles_propagates_single_and_double_read_faults_instead_of_eof() {
    // 1. Single-sector volume: to_read == need_bytes, single fault must return Err(VolumeError::Io)
    {
        let dir = Path::new("sim/seed209_scan_single_sector");
        let dat_path = dir.join("volume_0001.dat");
        let fs = SimFs::new();
        let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
        vm.write_needle(1, "tiny", b"abc", AWAN_FLAG_ACTIVE, 0, 0).unwrap();
        vm.sync_now().unwrap();

        let dat = fs.file(&dat_path).unwrap();
        dat.arm(SimOp::Read, SimFault::NoSpace);
        let mut seen = Vec::new();
        let res = vm.scan_needles(|n| seen.push(n.key));
        assert!(
            matches!(res, Err(VolumeError::Io(_))),
            "single-sector scan_needles must propagate NoSpace read fault, got {:?} (seen={:?})",
            res,
            seen
        );
    }

    // 2. Multi-needle volume: both single and double Data::Read faults (Eio and NoSpace) must propagate Err
    let dir = Path::new("sim/seed209_scan_multi");
    let dat_path = dir.join("volume_0001.dat");
    let fs = SimFs::new();
    let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
    let p1 = pattern(5000, 209);
    let p2 = pattern(9000, 210);
    vm.write_needle(1, "k1", &p1, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
    vm.write_needle(1, "k2", &p2, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
    vm.sync_now().unwrap();

    let dat = fs.file(&dat_path).unwrap();

    // Single Eio fault
    dat.arm(SimOp::Read, SimFault::Eio);
    let mut seen = Vec::new();
    let res = vm.scan_needles(|n| seen.push(n.key));
    assert!(
        matches!(res, Err(VolumeError::Io(_))),
        "scan_needles must propagate single Eio read fault without retrying as EOF, got {:?} (seen={:?})",
        res,
        seen
    );

    // Single NoSpace fault
    dat.arm(SimOp::Read, SimFault::NoSpace);
    let mut seen = Vec::new();
    let res = vm.scan_needles(|n| seen.push(n.key));
    assert!(
        matches!(res, Err(VolumeError::Io(_))),
        "scan_needles must propagate single NoSpace read fault without retrying as EOF, got {:?} (seen={:?})",
        res,
        seen
    );

    // Double Eio fault: each call fails on its own fault without retrying non-EOF errors
    dat.arm(SimOp::Read, SimFault::Eio);
    dat.arm(SimOp::Read, SimFault::Eio);
    let mut seen = Vec::new();
    let res1 = vm.scan_needles(|n| seen.push(n.key));
    assert!(
        matches!(res1, Err(VolumeError::Io(_))),
        "scan_needles must propagate first of double Eio read fault instead of returning Ok(()) with 0 needles, got {:?} (seen={:?})",
        res1,
        seen
    );
    let mut seen2 = Vec::new();
    let res2 = vm.scan_needles(|n| seen2.push(n.key));
    assert!(
        matches!(res2, Err(VolumeError::Io(_))),
        "scan_needles must propagate second of double Eio read fault, got {:?} (seen={:?})",
        res2,
        seen2
    );

    // Double NoSpace fault: each call fails on its own fault without retrying non-EOF errors
    dat.arm(SimOp::Read, SimFault::NoSpace);
    dat.arm(SimOp::Read, SimFault::NoSpace);
    let mut seen = Vec::new();
    let res1 = vm.scan_needles(|n| seen.push(n.key));
    assert!(
        matches!(res1, Err(VolumeError::Io(_))),
        "scan_needles must propagate first of double NoSpace read fault instead of returning Ok(()) with 0 needles, got {:?} (seen={:?})",
        res1,
        seen
    );
    let mut seen2 = Vec::new();
    let res2 = vm.scan_needles(|n| seen2.push(n.key));
    assert!(
        matches!(res2, Err(VolumeError::Io(_))),
        "scan_needles must propagate second of double NoSpace read fault, got {:?} (seen={:?})",
        res2,
        seen2
    );

    // Clean scan after transient faults still sees both needles intact
    let needles = scan_all(&vm);
    assert_eq!(needles.iter().map(|n| n.key.as_str()).collect::<Vec<_>>(), vec!["k1", "k2"]);
    assert_eq!(read_via_copy_payload(&vm, &needles[0]), p1);
    assert_eq!(read_via_copy_payload(&vm, &needles[1]), p2);
}

#[test]
fn open_with_propagates_read_fault_during_header_scan_and_never_truncates() {
    let dir = Path::new("sim/seed209_open_header");
    let dat_path = dir.join("volume_0001.dat");
    let fs = FaultReadAfterFs::new();

    let p1 = pattern(5000, 1);
    let p2 = pattern(7000, 2);
    let p3 = pattern(9000, 3);
    let expected_offset = {
        let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
        vm.write_needle(1, "k1", &p1, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
        vm.write_needle(1, "k2", &p2, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
        vm.write_needle(1, "k3", &p3, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
        vm.sync_now().unwrap();
        vm.offset()
    };
    let raw_len = fs.inner.file(&dat_path).unwrap().len().unwrap();
    assert_eq!(raw_len, expected_offset);

    // Fail Data::Read during header scan at offset 4096 while superblock at 0 succeeds.
    fs.fail_from_offset.store(SECTOR_SIZE as u64, Ordering::SeqCst);
    let err = match VolumeManager::open_with(&fs, dir, 1, false, clock()) {
        Ok(vm) => panic!("open_with must fail on Data::Read fault during header scan, succeeded with offset {}", vm.offset()),
        Err(e) => e,
    };
    assert!(
        matches!(err, VolumeError::Io(_)),
        "expected VolumeError::Io from open_with header scan fault, got {:?}",
        err
    );
    assert_eq!(
        fs.inner.file(&dat_path).unwrap().len().unwrap(),
        raw_len,
        "open_with must NEVER truncate volume data file when header scan hits a read I/O error"
    );

    // Clear fault and reopen cleanly: all intact needles must remain recoverable.
    fs.fail_from_offset.store(u64::MAX, Ordering::SeqCst);
    let reopened = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
    assert_eq!(reopened.offset(), expected_offset);
    let needles = scan_all(&reopened);
    assert_eq!(needles.iter().map(|n| n.key.as_str()).collect::<Vec<_>>(), vec!["k1", "k2", "k3"]);
    assert_eq!(read_via_copy_payload(&reopened, &needles[0]), p1);
    assert_eq!(read_via_copy_payload(&reopened, &needles[1]), p2);
    assert_eq!(read_via_copy_payload(&reopened, &needles[2]), p3);
}

#[test]
fn open_with_propagates_read_fault_during_verify_needle_integrity_and_never_truncates() {
    let dir = Path::new("sim/seed209_open_verify");
    let dat_path = dir.join("volume_0001.dat");
    let fs = FaultReadAfterFs::new();

    // Needle > 1 MiB (SCAN_BATCH_BYTES) forces verify_needle_integrity to issue a second read_at past 4096 + 1 MiB.
    let big_payload = pattern(1_200_000, 9);
    let tail_payload = pattern(5000, 10);
    let expected_offset = {
        let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
        vm.write_needle(1, "big", &big_payload, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
        vm.write_needle(1, "tail", &tail_payload, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
        vm.sync_now().unwrap();
        vm.offset()
    };
    let raw_len = fs.inner.file(&dat_path).unwrap().len().unwrap();
    assert_eq!(raw_len, expected_offset);

    // Allow superblock (0) and first 1 MiB scan batch [4096..4096+1MiB], then fail the second batch read inside verify_needle_integrity.
    fs.fail_from_offset.store(SECTOR_SIZE as u64 + (1 << 20), Ordering::SeqCst);
    let err = match VolumeManager::open_with(&fs, dir, 1, false, clock()) {
        Ok(vm) => panic!("open_with must fail on Data::Read fault during verify_needle_integrity, succeeded with offset {}", vm.offset()),
        Err(e) => e,
    };
    assert!(
        matches!(err, VolumeError::Io(_)),
        "expected VolumeError::Io from open_with verify_needle_integrity fault, got {:?}",
        err
    );
    assert_eq!(
        fs.inner.file(&dat_path).unwrap().len().unwrap(),
        raw_len,
        "open_with must NEVER truncate volume data file when verify_needle_integrity hits a read I/O error"
    );

    // Clear fault and reopen cleanly: both needles must still be intact.
    fs.fail_from_offset.store(u64::MAX, Ordering::SeqCst);
    let reopened = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
    assert_eq!(reopened.offset(), expected_offset);
    let needles = scan_all(&reopened);
    assert_eq!(needles.iter().map(|n| n.key.as_str()).collect::<Vec<_>>(), vec!["big", "tail"]);
    assert_eq!(read_via_copy_payload(&reopened, &needles[0]), big_payload);
    assert_eq!(read_via_copy_payload(&reopened, &needles[1]), tail_payload);
}
