//! #184: a needle whose journal append failed is whole in the volume, and since PR #233 the
//! live index shows it. Recovery steps over a gap only through the journal, so when such a
//! needle sits after a gap, a restart lost it: the scan jumped from the gap to the next
//! journaled needle, or, with none, stopped at the gap and the open cut the file there. The
//! failed journal record now stays pending and is written before the next one or by the next
//! sync.

use s3_server::{VolumeError, VolumeManager};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_jretry_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn pattern(len: usize, seed: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + seed * 7 + 13) % 251) as u8).collect()
}

fn keys(vm: &VolumeManager) -> Vec<String> {
    let mut out = Vec::new();
    vm.recover_needles(|_, key, _, _, _, _, _, _| out.push(key)).unwrap();
    out
}

/// n1 written; n2 fails before its header (a gap); n3's journal append fails after its needle
/// is whole.
fn gap_then_late_record(vm: &VolumeManager) {
    vm.write_needle(1, "n1", &pattern(5000, 1), 1, 0, 0).unwrap();
    vm.fail_next_write();
    assert!(vm.write_needle(1, "n2", &pattern(5000, 2), 1, 0, 0).is_err());
    vm.fail_next_index();
    match vm.write_needle(1, "n3", &pattern(5000, 3), 1, 0, 0) {
        Err(VolumeError::AfterRecord { .. }) => {}
        other => panic!("n3's needle is whole and only its journal append failed: {:?}", other.map(|l| l.offset)),
    }
    assert_eq!(vm.pending_journal_len(), 1, "n3's journal record waits");
}

#[test]
fn a_late_record_after_a_gap_survives_a_restart_once_a_later_write_is_journaled() {
    let dir = temp_dir("later_write");
    {
        let vm = VolumeManager::open(&dir, 1).unwrap();
        gap_then_late_record(&vm);
        vm.write_needle(1, "n4", &pattern(5000, 4), 1, 0, 0).unwrap();
        assert_eq!(vm.pending_journal_len(), 0, "n4's append wrote n3's record first");
        vm.sync_now().unwrap();
    }
    let vm = VolumeManager::open(&dir, 1).unwrap();
    assert_eq!(keys(&vm), vec!["n1", "n3", "n4"], "the scan steps from the gap to n3, not past it");
}

#[test]
fn a_late_record_after_a_gap_at_the_end_survives_a_restart_once_synced() {
    let dir = temp_dir("sync");
    let end = {
        let vm = VolumeManager::open(&dir, 1).unwrap();
        gap_then_late_record(&vm);
        vm.sync_now().unwrap();
        assert_eq!(vm.pending_journal_len(), 0, "the sync wrote n3's record");
        vm.offset()
    };
    let vm = VolumeManager::open(&dir, 1).unwrap();
    assert_eq!(keys(&vm), vec!["n1", "n3"], "n3 is not cut off with the gap before it");
    assert_eq!(vm.offset(), end, "writing resumes after n3, not over it");
}
