//! #412 (owner decision, issuecomment-7184789974): `--verify-payloads-at-boot` makes the open
//! check every payload's CRC, journaled or not, for the first start after upgrading from a
//! build before #412. Its own test binary: the switch is process-wide.

use s3_server::clock::{Clock, SystemClock};
use s3_server::meta_index::AWAN_FLAG_ACTIVE;
use s3_server::sim::SimFs;
use s3_server::volume::{set_verify_payloads_at_boot, VolumeManager};
use std::path::Path;
use std::sync::Arc;

fn clock() -> Arc<dyn Clock> {
    Arc::new(SystemClock)
}

fn pattern(len: usize, seed: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + seed * 7 + 13) % 251) as u8).collect()
}

#[test]
fn the_switch_checks_every_payload_even_when_all_are_journaled() {
    let dir = Path::new("sim/boot_verify_all");
    let fs = SimFs::new();
    // One needle above the 1 MiB scan batch, so the skip path's short refill is also turned off.
    let sizes = [3000usize, 20_000, 3 * 1024 * 1024];
    {
        let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
        for (i, n) in sizes.iter().enumerate() {
            vm.write_needle(1, &format!("k{}", i), &pattern(*n, i), AWAN_FLAG_ACTIVE, 0, 0).unwrap();
        }
        vm.sync_now().unwrap();
    }
    let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
    assert_eq!(vm.payload_verified_at_open(), 0, "without the switch, journaled needles are trusted");
    drop(vm);

    set_verify_payloads_at_boot(true);
    let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
    set_verify_payloads_at_boot(false);
    assert_eq!(vm.payload_verified_at_open(), sizes.iter().sum::<usize>() as u64, "the switch checks every payload");
}
