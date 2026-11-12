//! Recovery after a gap in the volume (#38) and the .idx journal across reopen. See
//! `verification/tla/ObjectVersions.tla`, config `no_gap_skip.cfg`.

use s3_server::VolumeManager;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_gap_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + seed as usize * 7 + 13) % 251) as u8).collect()
}

fn recovered(vm: &VolumeManager) -> Vec<(String, u64, u32)> {
    let mut out = Vec::new();
    vm.recover_needles(|_b, key, offset, _size, _hi, _lo, _flags, crc| out.push((key, offset, crc))).unwrap();
    out
}

#[test]
fn needles_after_a_failed_write_survive_reopen() {
    let dir = temp_dir("after_gap");
    {
        let vm = VolumeManager::open(&dir, 1).unwrap();
        vm.write_needle(1, "n1", &pattern(5000, 1), 1, 0, 0).unwrap();
        vm.fail_next_write();
        assert!(vm.write_needle(1, "n2", &pattern(5000, 2), 1, 0, 0).is_err());
        vm.write_needle(1, "n3", &pattern(5000, 3), 1, 0, 0).unwrap();
    }
    let vm = VolumeManager::open(&dir, 1).unwrap();
    let found = recovered(&vm);
    let keys: Vec<&str> = found.iter().map(|(k, _, _)| k.as_str()).collect();
    assert_eq!(keys, vec!["n1", "n3"], "recovery must step over the gap left by the failed write");

    // A write after the reopen must not land on n3.
    let (_, off3, crc3) = found[1].clone();
    vm.write_needle(1, "n4", &pattern(5000, 4), 1, 0, 0).unwrap();
    let back = vm.read_needle_bytes(1, off3, crc3).expect("n3 intact after a new write");
    assert_eq!(&back[..], &pattern(5000, 3)[..]);
}

#[test]
fn index_journal_is_appended_across_reopen() {
    let dir = temp_dir("idx_append");
    {
        let vm = VolumeManager::open(&dir, 1).unwrap();
        vm.write_needle(1, "a", b"first", 1, 0, 0).unwrap();
    }
    {
        let vm = VolumeManager::open(&dir, 1).unwrap();
        vm.write_needle(1, "b", b"second", 1, 0, 0).unwrap();
    }
    let len = std::fs::metadata(dir.join("volume_0001.idx")).unwrap().len();
    assert_eq!(len, 2 * 42, "a reopen must append to the .idx journal, not overwrite it from the start");
}
