//! #412: can a needle at or before the `.idx` journal's last recorded offset be torn?
//!
//! A needle's journal record is written right after its header, with no sync between them,
//! and the volume and the journal reach the disk independently. So a crash can keep the
//! record and tear the needle's sector 0 after its 64-byte header: the header decodes, the
//! payload is wrong, and the journal names the needle. Only the payload check at open
//! (`verify_needle_integrity`) rejects it. Skipping that check for journaled needles would
//! return the torn bytes as an object.

use s3_server::clock::{Clock, SystemClock};
use s3_server::meta_index::AWAN_FLAG_ACTIVE;
use s3_server::sim::{CrashMode, SimFs, SimRng};
use s3_server::vfile::VolFs;
use s3_server::volume::{NeedleHeader, ScannedNeedle, VolumeManager};
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

/// The needle at `offset` has a header that decodes and a payload whose CRC does not match.
fn torn_after_header(dat: &[u8], offset: usize, key: &[u8], payload: &[u8]) -> bool {
    let p_start = offset + 64 + key.len();
    let p_end = p_start + payload.len();
    dat.len() >= p_end
        && NeedleHeader::decode(dat[offset..offset + 64].try_into().unwrap()).is_ok()
        && &dat[offset + 64..p_start] == key
        && crc32c::crc32c(&dat[p_start..p_end]) != crc32c::crc32c(payload)
}

#[test]
fn a_crash_can_tear_a_needle_the_journal_records_and_open_still_rejects_it() {
    let dir = Path::new("sim/boot_verify_journal");
    let dat_path = dir.join("volume_0001.dat");
    let idx_path = dir.join("volume_0001.idx");
    let fs = SimFs::new();
    let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();

    let p0 = pattern(4000, 1);
    vm.write_needle(1, "a", &p0, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
    vm.sync_now().unwrap();
    // Two unsynced needles of two sectors each: tail sector, then sector 0 with the header.
    let p1 = pattern(6000, 2);
    let p2 = pattern(6000, 3);
    let b = vm.write_needle(1, "b", &p1, AWAN_FLAG_ACTIVE, 0, 0).unwrap().offset as usize;
    let c = vm.write_needle(1, "c", &p2, AWAN_FLAG_ACTIVE, 0, 0).unwrap().offset as usize;

    let mut torn_before_last = 0;
    let mut torn_at_last = 0;
    for seed in 0..2000u64 {
        let after = fs.crashed(CrashMode::SeededPrefix, &mut SimRng::new(seed));
        let dat = after.file(&dat_path).unwrap().contents();
        let idx = after.file(&idx_path).unwrap().contents();
        let journaled = journal_offsets(&idx);
        let last = journaled.iter().copied().max().unwrap_or(0) as usize;

        let b_torn = torn_after_header(&dat, b, b"b", &p1) && journaled.contains(&(b as u64));
        let c_torn = torn_after_header(&dat, c, b"c", &p2) && journaled.contains(&(c as u64));
        if b_torn && b < last {
            torn_before_last += 1;
        }
        if (b_torn && b == last) || (c_torn && c == last) {
            torn_at_last += 1;
        }

        let reopened = VolumeManager::open_with(&after, dir, 1, false, clock()).unwrap();
        for n in scan_all(&reopened) {
            match n.key.as_str() {
                "a" => {}
                "b" => assert!(!b_torn, "seed {}: torn b, recorded in the journal, came back", seed),
                "c" => assert!(!c_torn, "seed {}: torn c, recorded in the journal, came back", seed),
                other => panic!("seed {}: unexpected key {:?}", seed, other),
            }
        }
    }
    eprintln!("torn and journaled: {} before the last journal offset, {} at it", torn_before_last, torn_at_last);
    assert!(torn_before_last > 0, "some seed tears b while the journal records b and c");
    assert!(torn_at_last > 0, "some seed tears the needle at the journal's last offset");
}
