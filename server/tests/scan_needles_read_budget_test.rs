//! #512 (follow-up from #460): `VolumeManager::scan_needles` must cap its read window
//! after skipping a large needle (`total_len > SCAN_BATCH_BYTES`), just as `open_with` (#412)
//! does, so restarting on 1 MiB objects does not read unrequested payloads into the page cache.

use s3_server::clock::{Clock, SystemClock};
use s3_server::meta_index::AWAN_FLAG_ACTIVE;
use s3_server::sim::{SimFs, SimOp};
use s3_server::volume::VolumeManager;
use std::path::Path;
use std::sync::Arc;

fn clock() -> Arc<dyn Clock> {
    Arc::new(SystemClock)
}

fn pattern(len: usize, seed: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + seed * 7 + 13) % 251) as u8).collect()
}

fn is_dat(p: &Path) -> bool {
    p.extension().is_some_and(|e| e == "dat")
}

#[test]
fn scan_needles_skips_large_payloads_instead_of_reading_1_mib_per_needle() {
    let dir = Path::new("sim/scan_needles_large");
    let fs = SimFs::new();
    let one_mib = 1usize << 20;
    {
        let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
        for i in 0..32usize {
            vm.write_needle(1, &format!("k{:03}", i), &pattern(one_mib, i), AWAN_FLAG_ACTIVE, 0, 0)
                .unwrap();
        }
        vm.sync_now().unwrap();
    }

    let before_open = fs.total_bytes_read(is_dat);
    let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
    let after_open = fs.total_bytes_read(is_dat);

    let mut count = 0usize;
    vm.scan_needles(|n| {
        assert_eq!(n.payload_size as usize, one_mib);
        count += 1;
    })
    .unwrap();
    assert_eq!(count, 32);

    let after_scan = fs.total_bytes_read(is_dat);
    let open_bytes = after_open - before_open;
    let scan_bytes = after_scan - after_open;
    let total_restart_bytes = after_scan - before_open;

    eprintln!(
        "32x1MiB restart bytes: open={} scan={} total={} (written=32MiB)",
        open_bytes, scan_bytes, total_restart_bytes
    );

    assert!(
        scan_bytes <= 3 * (one_mib as u64) + 128 * 1024,
        "scan_needles read {} bytes across 32 x 1 MiB needles (expected <= ~3.1 MiB, not 32 MiB)",
        scan_bytes
    );
    assert!(
        total_restart_bytes <= 6 * (one_mib as u64),
        "total restart (open_with + scan_needles) read {} bytes across 32 x 1 MiB needles (expected <= 6 MiB)",
        total_restart_bytes
    );
}

#[test]
fn scan_needles_still_batches_small_needles_in_1_mib_windows() {
    let dir = Path::new("sim/scan_needles_small");
    let fs = SimFs::new();
    {
        let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
        for i in 0..512usize {
            vm.write_needle(1, &format!("s{:03}", i), &pattern(4096, i), AWAN_FLAG_ACTIVE, 0, 0)
                .unwrap();
        }
        vm.sync_now().unwrap();
    }

    let vm = VolumeManager::open_with(&fs, dir, 1, false, clock()).unwrap();
    let calls_before = fs.total_calls(SimOp::Read, is_dat);

    let mut count = 0usize;
    vm.scan_needles(|n| {
        assert_eq!(n.payload_size, 4096);
        count += 1;
    })
    .unwrap();
    assert_eq!(count, 512);

    let scan_calls = fs.total_calls(SimOp::Read, is_dat) - calls_before;
    assert!(
        scan_calls <= 12,
        "small-needle scan should stay batched (~5 read calls for ~4 MiB), got {}",
        scan_calls
    );
}
