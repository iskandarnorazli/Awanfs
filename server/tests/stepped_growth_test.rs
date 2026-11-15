//! #178: a growing volume (decision 52) is extended ahead of its writes, so its read mapping,
//! which covers the whole file, is redone once per extension and not once per read of new
//! data. On Windows each redo cost more as the volume grew (2.8 ms at 10 GiB).

use s3_server::{VolumeManager, AWAN_FLAG_ACTIVE};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const MIB: u64 = 1024 * 1024;

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_growth_{}_{}_{}", name, std::process::id(), nanos));
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

#[test]
fn reading_each_object_right_after_its_write_remaps_once_per_extension() {
    let dir = temp_dir("remap");
    let vm = VolumeManager::open(&dir, 1).unwrap();
    assert!(!vm.is_sparse(), "new volumes grow (decision 52)");
    vm.set_growth_step(MIB);
    let n = 64;
    for i in 0..n {
        let body = pattern(128 * 1024, i);
        let loc = vm.write_needle(1, &format!("k{}", i), &body, AWAN_FLAG_ACTIVE, 0, 0).unwrap();
        let got = vm.read_needle_bytes(1, loc.offset, loc.payload_crc32c).unwrap();
        assert!(got[..] == body[..]);
    }
    // 64 objects of 128 KiB plus headers are a little over 8 MiB: about nine 1 MiB steps,
    // plus one remap per object below a quarter of the step (256 KiB: two objects).
    // Before, every read of a new object redid the mapping: one remap per object.
    let written = vm.offset();
    let bound = written.div_ceil(MIB) + 3;
    assert!(vm.remap_count() <= bound, "{} remaps for {} bytes written (at most {})", vm.remap_count(), written, bound);
    assert!(vm.allocated_len() >= written, "the file covers what was written");
    assert!(vm.allocated_len() - written <= MIB, "and at most one step more: {} allocated, {} written", vm.allocated_len(), written);
}

#[test]
fn a_small_volume_grows_by_its_writes_and_a_large_one_by_a_quarter_of_its_length() {
    let dir = temp_dir("geometric");
    let vm = VolumeManager::open(&dir, 1).unwrap();
    vm.set_growth_step(64 * MIB);
    let file = dir.join("volume_0001.dat");
    let mut lengths = Vec::new();
    for i in 0..400 {
        vm.write_needle(1, &format!("k{}", i), &pattern(256 * 1024, i), AWAN_FLAG_ACTIVE, 0, 0).unwrap();
        if vm.offset() <= 16 * MIB {
            // Below a quarter of the step: nothing is allocated ahead of the writes.
            assert_eq!(std::fs::metadata(&file).unwrap().len(), vm.offset(), "a small volume's file is what it holds");
        } else if lengths.last() != Some(&vm.allocated_len()) {
            lengths.push(vm.allocated_len());
        }
    }
    assert!(lengths.len() >= 3, "{:?}", lengths);
    for w in lengths.windows(2) {
        let grew = w[1] - w[0];
        assert!(grew <= 64 * MIB + 512 * 1024, "never more than the step past a write: {:?}", lengths);
        assert!(grew >= (w[0] / 4).clamp(MIB, 64 * MIB) - 4096, "at least a quarter (or 1 MiB): {:?}", lengths);
    }
}

#[test]
fn a_restart_after_an_extension_recovers_to_the_written_end() {
    let dir = temp_dir("crash");
    let written = {
        let vm = VolumeManager::open(&dir, 1).unwrap();
        vm.set_growth_step(4 * MIB);
        // Past a quarter of the step (1 MiB), so the file is extended ahead.
        for i in 0..6 {
            vm.write_needle(1, &format!("k{}", i), &pattern(300_000, i), AWAN_FLAG_ACTIVE, 0, 0).unwrap();
        }
        vm.sync_now().unwrap();
        assert!(vm.allocated_len() > vm.offset(), "the file was extended past the written end");
        vm.offset()
    };
    // The process stops here: the file still holds the zero tail of its last extension.
    let file = dir.join("volume_0001.dat");
    assert!(std::fs::metadata(&file).unwrap().len() > written);

    let vm = VolumeManager::open(&dir, 1).unwrap();
    assert_eq!(keys(&vm), vec!["k0", "k1", "k2", "k3", "k4", "k5"], "the zero tail is not data and not corruption");
    assert_eq!(vm.offset(), written, "writing resumes at the written end");
    assert_eq!(std::fs::metadata(&file).unwrap().len(), written, "the open trimmed the tail");
    vm.write_needle(1, "k6", &pattern(1000, 6), AWAN_FLAG_ACTIVE, 0, 0).unwrap();
    vm.sync_now().unwrap();
    drop(vm);
    let vm = VolumeManager::open(&dir, 1).unwrap();
    assert_eq!(keys(&vm), vec!["k0", "k1", "k2", "k3", "k4", "k5", "k6"]);
}

#[test]
fn a_streamed_needle_is_allocated_once_when_it_is_reserved() {
    let dir = temp_dir("stream");
    let vm = std::sync::Arc::new(VolumeManager::open(&dir, 1).unwrap());
    vm.set_growth_step(MIB);
    let body = pattern(5 * MIB as usize + 123, 9);
    let mut w = vm.begin_needle(1, "big", body.len() as u64, AWAN_FLAG_ACTIVE).unwrap();
    let after_reserve = vm.allocated_len();
    assert!(after_reserve >= vm.offset(), "the whole needle is allocated at its reservation");
    for piece in body.chunks(1 << 20) {
        w.write(piece).unwrap();
    }
    let loc = w.finish(0, 0).unwrap();
    assert_eq!(vm.allocated_len(), after_reserve, "its chunks did not extend the file again");
    let got = vm.read_needle_bytes(1, loc.offset, loc.payload_crc32c).unwrap();
    assert!(got[..] == body[..]);
}
