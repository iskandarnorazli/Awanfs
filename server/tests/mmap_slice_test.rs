//! awan-storage/server/tests/mmap_slice_test.rs
//! TDD Unit and Integration Tests for Zero-Copy memmap2 Slice Reads and Direct Range Slicing (#480)

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use s3_server::volume::{VolumeManager, VolumeError};
use s3_server::s3_api::parse_range_header;

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("awan_mmap_slice_test_{}_{}", name, std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn test_subslice_range_reads_across_boundaries() {
    let dir = temp_dir("subslice_boundaries");
    let vol_mgr = VolumeManager::open(&dir, 1).expect("open volume manager");

    // Test across 4 KiB, 64 KiB, and 1 MiB payload boundaries
    let sizes = [4096, 64 * 1024, 1024 * 1024];

    for &size in &sizes {
        let key = format!("boundary_obj_{}", size);
        let mut payload = vec![0u8; size];
        for (i, b) in payload.iter_mut().enumerate() {
            *b = ((i * 31 + 7) % 251) as u8;
        }

        let loc = vol_mgr.write_needle(1, &key, &payload, 1, 0, 0).expect("write needle");

        // Zero-copy read via memory-map
        let zero_copy_bytes = vol_mgr.read_needle_bytes(loc.volume_id, loc.offset, loc.payload_crc32c)
            .expect("read_needle_bytes must succeed");

        assert_eq!(zero_copy_bytes.len(), size);
        assert_eq!(&zero_copy_bytes[..], &payload[..]);

        // Verify sub-slice Range GETs across boundaries
        // 1. First 512 bytes
        let r1 = zero_copy_bytes.slice(0..512);
        assert_eq!(&r1[..], &payload[0..512]);

        // 2. Middle 4096 bytes (spanning 4 KiB boundary)
        let mid_start = size / 2;
        let mid_end = (mid_start + 4096).min(size);
        let r2 = zero_copy_bytes.slice(mid_start..mid_end);
        assert_eq!(&r2[..], &payload[mid_start..mid_end]);

        // 3. Tail 1024 bytes
        let tail_start = size - 1024;
        let r3 = zero_copy_bytes.slice(tail_start..size);
        assert_eq!(&r3[..], &payload[tail_start..size]);
    }
}

#[test]
fn test_concurrent_multi_worker_range_slicing_under_active_appends() {
    let dir = temp_dir("concurrent_mmap_active_appends");
    let vol_mgr = Arc::new(VolumeManager::open(&dir, 1).expect("open volume manager"));

    // Pre-populate with initial objects
    let mut initial_locs = Vec::new();
    for i in 0..8 {
        let key = format!("initial_obj_{}", i);
        let payload = vec![(i * 17) as u8; 64 * 1024]; // 64 KiB
        let loc = vol_mgr.write_needle(1, &key, &payload, 1, 0, 0).expect("write needle");
        initial_locs.push((loc, payload));
    }

    let stop_signal = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Spawn 8 reader threads continuously reading random sub-slices via mmap
    let mut reader_handles = Vec::new();
    for r_id in 0..8 {
        let mgr = Arc::clone(&vol_mgr);
        let locs = initial_locs.clone();
        let stop = Arc::clone(&stop_signal);

        reader_handles.push(thread::spawn(move || {
            let mut read_ops = 0;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let target_idx = (r_id + read_ops) % locs.len();
                let (loc, ref expected_payload) = locs[target_idx];

                let bytes = mgr.read_needle_bytes(loc.volume_id, loc.offset, loc.payload_crc32c)
                    .expect("read_needle_bytes during concurrent writes must succeed");

                // Range slice 0..4096
                let slice = bytes.slice(0..4096);
                assert_eq!(&slice[..], &expected_payload[0..4096]);
                read_ops += 1;
            }
            read_ops
        }));
    }

    // Simultaneously, main thread appends 32 new needles to grow the volume file
    for i in 0..32 {
        let key = format!("active_append_{}", i);
        let payload = vec![0xEEu8; 128 * 1024]; // 128 KiB
        vol_mgr.write_needle(1, &key, &payload, 1, 0, 0).expect("active append write_needle");
    }

    stop_signal.store(true, std::sync::atomic::Ordering::Relaxed);

    let mut total_reads = 0;
    for h in reader_handles {
        total_reads += h.join().expect("reader thread join");
    }
    assert!(total_reads > 50, "Multi-worker mmap range slicing achieved {} reads", total_reads);
}

#[test]
fn test_out_of_bounds_range_clamping_negative_controls() {
    // RFC 9110 Range Header parsing negative controls
    let obj_size = 65536u64; // 64 KiB

    // 1. Valid range
    let r_valid = parse_range_header("bytes=0-4095", obj_size).unwrap().unwrap();
    assert_eq!(r_valid.start, 0);
    assert_eq!(r_valid.end, 4095);

    // 2. Clamped range beyond EOF (bytes=0-100000 -> clamps end to 65535)
    let r_clamped = parse_range_header("bytes=0-100000", obj_size).unwrap().unwrap();
    assert_eq!(r_clamped.start, 0);
    assert_eq!(r_clamped.end, 65535);

    // 3. Negative control: Start beyond object size must return Err(()) (416 Range Not Satisfiable)
    let r_err_start = parse_range_header("bytes=70000-80000", obj_size);
    assert_eq!(r_err_start, Err(()), "Start beyond object size must fail with 416 Range Not Satisfiable");

    // 4. Negative control: Inverted range start > end must return Err(())
    let r_err_invert = parse_range_header("bytes=500-100", obj_size);
    assert_eq!(r_err_invert, Err(()), "Inverted start > end must fail with 416 Range Not Satisfiable");

    // 5. Negative control: Corrupt needle CRC in memory-mapped read returns CrcMismatch
    let dir = temp_dir("mmap_corrupt_crc_control");
    let vol_mgr = VolumeManager::open(&dir, 1).expect("open volume manager");
    let loc = vol_mgr.write_needle(1, "crc_test", b"hello world", 1, 0, 0).expect("write needle");
    let corrupt_crc = vol_mgr.read_needle_bytes(loc.volume_id, loc.offset, 0xDEADBEEF);
    match corrupt_crc {
        Err(VolumeError::CrcMismatch { expected, actual }) => {
            assert_eq!(expected, 0xDEADBEEF);
            assert_eq!(actual, loc.payload_crc32c);
        }
        other => panic!("Expected CrcMismatch, got {:?}", other),
    }
}
