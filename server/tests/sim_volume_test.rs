//! #213 stage 1b (decision 67): a real `VolumeManager` on simulated files, crashed and reopened.
//! Recovery is today's code, unchanged; only the files are simulated.

use s3_server::clock::{Clock, SystemClock};
use s3_server::sim::{CrashMode, SimFs, SimRng};
use s3_server::volume::VolumeManager;
use std::path::Path;
use std::sync::Arc;

fn clock() -> Arc<dyn Clock> {
    Arc::new(SystemClock)
}

fn pattern(len: usize, seed: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + seed * 7 + 13) % 251) as u8).collect()
}

fn keys(vm: &VolumeManager<SimFs>) -> Vec<String> {
    let mut out = Vec::new();
    vm.recover_needles(|_, key, _, _, _, _, _, _| out.push(key)).unwrap();
    out
}

/// a and b written and synced; c written after the last sync.
fn write_three(fs: &SimFs, dir: &Path) {
    let vm = VolumeManager::open_with(fs, dir, 1, false, clock()).unwrap();
    vm.write_needle(1, "a", &pattern(5000, 1), 1, 0, 0).unwrap();
    vm.write_needle(1, "b", &pattern(9000, 2), 1, 0, 0).unwrap();
    vm.sync_now().unwrap();
    vm.write_needle(1, "c", &pattern(7000, 3), 1, 0, 0).unwrap();
}

#[test]
fn a_crash_keeps_what_was_synced_and_drops_the_rest() {
    let dir = Path::new("sim/vol");
    let fs = SimFs::new();
    write_three(&fs, dir);
    let after = fs.crashed(CrashMode::DropUnsynced, &mut SimRng::new(1));
    let vm = VolumeManager::open_with(&after, dir, 1, false, clock()).unwrap();
    assert_eq!(keys(&vm), vec!["a", "b"], "the synced needles survive; the unsynced one is gone");
}

#[test]
fn every_seeded_crash_recovers_a_prefix_of_the_writes() {
    let dir = Path::new("sim/vol");
    let fs = SimFs::new();
    write_three(&fs, dir);
    let mut seen_c = false;
    for seed in 0..200 {
        let after = fs.crashed(CrashMode::SeededPrefix, &mut SimRng::new(seed));
        let vm = VolumeManager::open_with(&after, dir, 1, false, clock()).unwrap();
        let k = keys(&vm);
        assert!(k == ["a", "b"] || k == ["a", "b", "c"], "seed {}: recovered {:?}", seed, k);
        seen_c |= k.len() == 3;
        // A reopened volume accepts new writes after what it recovered.
        vm.write_needle(1, "d", &pattern(3000, 4), 1, 0, 0).unwrap();
        let mut k2 = keys(&vm);
        assert_eq!(k2.pop().as_deref(), Some("d"), "seed {}", seed);
    }
    assert!(seen_c, "some seed lets the unsynced needle land whole");
}
