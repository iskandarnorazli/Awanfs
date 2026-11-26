//! #412 (owner decision: option B, issuecomment-7183429119): a needle's `.idx` record is written
//! only after the volume sync that covers the needle. So every record on disk names a needle
//! whose bytes are on disk, and the open checks payload CRCs only for needles the journal does
//! not record. Before this, a record was written right after its needle's header with no sync
//! between them, a crash could keep the record and tear the needle, and the open had to read
//! every payload (`measurements/2026-11-26-boot-large-objects.md`).

use s3_server::clock::{Clock, SystemClock};
use s3_server::meta_index::AWAN_FLAG_ACTIVE;
use s3_server::sim::{CrashMode, SimFs, SimRng};
use s3_server::vfile::VolFs;
use s3_server::volume::{NeedleHeader, ScannedNeedle, VolumeManager};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

const IDX_RECORD: usize = 42;

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

fn journal_offsets(idx: &[u8]) -> Vec<u64> {
    idx.chunks_exact(IDX_RECORD).map(|r| u64::from_le_bytes(r[12..20].try_into().unwrap())).collect()
}

#[test]
fn opening_a_volume_whose_needles_are_all_journaled_verifies_no_payload() {
    let dir = Path::new("sim/boot_all_journaled");
    let fs = SimFs::new();
    {
        let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
        for i in 0..5 {
            vm.write_needle(1, &format!("k{}", i), &pattern(3000 + i * 5000, i), AWAN_FLAG_ACTIVE, 0, 0).unwrap();
        }
        vm.sync_now().unwrap();
        assert_eq!(vm.pending_journal_len(), 0, "the sync wrote every record");
    }
    let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
    assert_eq!(vm.payload_verified_at_open(), 0, "every needle is journaled: no payload is read");
    assert_eq!(scan_all(&vm).len(), 5);
}

#[test]
fn opening_a_volume_verifies_exactly_the_needles_the_journal_does_not_record() {
    let dir = Path::new("sim/boot_tail_unjournaled");
    let fs = SimFs::new();
    let (c, d) = (pattern(7000, 3), pattern(20_000, 4));
    {
        let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
        vm.write_needle(1, "a", &pattern(5000, 1), AWAN_FLAG_ACTIVE, 0, 0).unwrap();
        vm.write_needle(1, "b", &pattern(9000, 2), AWAN_FLAG_ACTIVE, 0, 0).unwrap();
        vm.sync_now().unwrap();
        // Written, not synced: their records are still pending, so the journal lacks them.
        vm.write_needle(1, "c", &c, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
        vm.write_needle(1, "d", &d, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
        assert_eq!(vm.pending_journal_len(), 2, "unsynced needles have no record on disk yet");
    }
    let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
    assert_eq!(vm.payload_verified_at_open(), (c.len() + d.len()) as u64, "only c's and d's payloads are checked");
    let keys: Vec<String> = scan_all(&vm).into_iter().map(|n| n.key).collect();
    assert_eq!(keys, ["a", "b", "c", "d"]);
}

/// Streamed needles finish out of offset order: k2 starts first and finishes last, so a
/// journal could record k3 above k2. The open trusts a needle by its own record, not by
/// being below the journal's last offset.
#[test]
fn every_record_a_crash_leaves_names_a_whole_needle_and_no_torn_needle_comes_back() {
    let dir = Path::new("sim/boot_crash_records");
    let dat_path = dir.join("volume_0001.dat");
    let idx_path = dir.join("volume_0001.idx");
    let fs = SimFs::new();
    let vm = Arc::new(VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap());

    let mut want: HashMap<String, Vec<u8>> = HashMap::new();
    let p0 = pattern(4000, 1);
    vm.write_needle(1, "a", &p0, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
    want.insert("a".into(), p0);
    vm.sync_now().unwrap();

    let p2 = pattern(54_650, 2);
    let mut w = vm.begin_needle(1, "k2", p2.len() as u64, AWAN_FLAG_ACTIVE).unwrap();
    w.write(&p2).unwrap();
    let p3 = pattern(17_285, 3);
    vm.write_needle(1, "k3", &p3, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
    vm.sync_now().unwrap();
    w.finish(0, 0).unwrap();
    let p4 = pattern(6000, 4);
    vm.write_needle(1, "k4", &p4, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
    want.insert("k2".into(), p2);
    want.insert("k3".into(), p3);
    want.insert("k4".into(), p4);

    let mut records_seen = 0;
    for seed in 0..2000u64 {
        let after = fs.crashed(CrashMode::SeededPrefix, &mut SimRng::new(seed));
        let dat = after.file(&dat_path).unwrap().contents();
        let idx = after.file(&idx_path).unwrap().contents();
        for off in journal_offsets(&idx) {
            records_seen += 1;
            let o = off as usize;
            let hdr = NeedleHeader::decode(dat[o..o + 64].try_into().unwrap())
                .unwrap_or_else(|e| panic!("seed {}: the journal records {} but its header is gone: {:?}", seed, off, e));
            let p = o + 64 + hdr.key_len as usize + hdr.attrs_len as usize;
            let payload = &dat[p..p + hdr.payload_size as usize];
            assert_eq!(crc32c::crc32c(payload), hdr.payload_crc32c, "seed {}: the journal records a torn needle at {}", seed, off);
        }

        let reopened = VolumeManager::open_with(&after, dir, 1, false, clock()).unwrap();
        for n in scan_all(&reopened) {
            let mut got = Vec::new();
            reopened
                .copy_payload(n.offset, n.payload_crc32c, |chunk| {
                    got.extend_from_slice(chunk);
                    Ok(())
                })
                .unwrap_or_else(|e| panic!("seed {}: {} came back unreadable: {:?}", seed, n.key, e));
            assert!(got == want[&n.key], "seed {}: {} came back with torn bytes", seed, n.key);
        }
    }
    assert!(records_seen > 0, "some seed keeps journal records");
}
