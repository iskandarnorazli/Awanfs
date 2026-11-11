//! awan-storage/server/tests/concurrent_io_test.rs
//! TDD Unit and Integration Tests for Lock-Free Concurrent Positional I/O (#479)

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use s3_server::volume::{VolumeManager, VolumeError, SECTOR_SIZE};

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("awan_lockfree_io_test_{}_{}", name, std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn test_16_concurrent_threads_writing_needles() {
    let dir = temp_dir("16_concurrent_needles");
    let vol_mgr = Arc::new(VolumeManager::open(&dir, 1).expect("open volume manager"));

    const NUM_THREADS: usize = 16;
    const NEEDLE_SIZE: usize = 16 * 1024 * 1024; // 16 MiB per thread = 256 MiB total concurrent I/O

    let t0 = Instant::now();
    let mut handles = Vec::with_capacity(NUM_THREADS);

    for tid in 0..NUM_THREADS {
        let mgr = Arc::clone(&vol_mgr);
        handles.push(thread::spawn(move || {
            let key = format!("concurrent_needle_{:02}", tid);
            // Deterministic payload pattern per thread
            let mut payload = vec![0u8; NEEDLE_SIZE];
            for (idx, b) in payload.iter_mut().enumerate() {
                *b = ((tid * 37 + idx) % 251) as u8;
            }

            let loc = mgr.write_needle(1, &key, &payload, 1, 0, 0)
                .expect("concurrent write_needle must succeed");

            assert_eq!(loc.offset % SECTOR_SIZE as u64, 0, "offset must be 4096-byte aligned");
            assert_eq!(loc.total_len % SECTOR_SIZE as u64, 0, "total_len must be 4096-byte aligned");

            (key, loc, payload)
        }));
    }

    let mut written = Vec::with_capacity(NUM_THREADS);
    for h in handles {
        written.push(h.join().expect("thread join"));
    }

    let elapsed = t0.elapsed();
    let total_bytes = (NUM_THREADS * NEEDLE_SIZE) as f64;
    let mib_per_sec = (total_bytes / (1024.0 * 1024.0)) / elapsed.as_secs_f64();
    println!("16 Concurrent Threads Wrote 256 MiB in {:.3}s ({:.1} MiB/s)", elapsed.as_secs_f64(), mib_per_sec);

    // Verify all 16 needles have non-overlapping disjoint offsets
    let mut offsets: Vec<(u64, u64)> = written.iter().map(|(_, loc, _)| (loc.offset, loc.offset + loc.total_len)).collect();
    offsets.sort_by_key(|&(start, _)| start);
    for i in 1..offsets.len() {
        assert!(
            offsets[i - 1].1 <= offsets[i].0,
            "Needle offsets must be strictly non-overlapping: needle {} ends at {}, needle {} starts at {}",
            i - 1, offsets[i - 1].1, i, offsets[i].0
        );
    }

    // Read back all 16 needles concurrently and verify bit-identity + CRC32C
    let mut read_handles = Vec::with_capacity(NUM_THREADS);
    for (key, loc, expected_payload) in written {
        let mgr = Arc::clone(&vol_mgr);
        read_handles.push(thread::spawn(move || {
            let read_back = mgr.read_needle(loc.volume_id, loc.offset, loc.payload_crc32c)
                .unwrap_or_else(|e| panic!("read_needle failed for key {}: {:?}", key, e));
            assert_eq!(read_back.len(), expected_payload.len());
            assert_eq!(read_back, expected_payload, "Payload for key {} must match bit-identically", key);
        }));
    }

    for h in read_handles {
        h.join().expect("read thread join");
    }
}

#[test]
fn test_negative_control_overlapping_offset_corruption_detected() {
    let dir = temp_dir("negative_control_overlap");
    let vol_mgr = VolumeManager::open(&dir, 1).expect("open volume manager");

    let key = "valid_needle_target";
    let payload = vec![0xABu8; 8192];
    let loc = vol_mgr.write_needle(1, key, &payload, 1, 0, 0).expect("write needle");

    // 1. Normal read succeeds
    let valid_data = vol_mgr.read_needle(loc.volume_id, loc.offset, loc.payload_crc32c).expect("normal read");
    assert_eq!(valid_data, payload);

    // 2. NEGATIVE CONTROL: Checksum tampering is detected
    let corrupt_read = vol_mgr.read_needle(loc.volume_id, loc.offset, 0x12345678);
    match corrupt_read {
        Err(VolumeError::CrcMismatch { expected, actual }) => {
            assert_eq!(expected, 0x12345678);
            assert_eq!(actual, loc.payload_crc32c);
        }
        other => panic!("Expected CrcMismatch error, got: {:?}", other),
    }

    // 3. NEGATIVE CONTROL: Reading beyond valid volume file returns error
    let unwritten_offset = loc.offset + loc.total_len + 4096;
    let empty_read = vol_mgr.read_needle(loc.volume_id, unwritten_offset, 0);
    assert!(empty_read.is_err(), "Reading unwritten needle must fail");
    match empty_read.err().unwrap() {
        VolumeError::InvalidMagic { .. } | VolumeError::HeaderCrcMismatch { .. } | VolumeError::Io(_) | VolumeError::TornNeedleAtOffset(_) => {}
        other => panic!("Expected InvalidMagic, HeaderCrcMismatch, TornNeedleAtOffset or Io error, got: {:?}", other),
    }

    // 4. NEGATIVE CONTROL: Overwriting header magic on disk causes InvalidMagic
    let corrupt_buf = [0xFFu8; 4096];
    vol_mgr.file().write_aligned_at(loc.offset, &corrupt_buf).expect("write corrupt sector");
    let magic_fail = vol_mgr.read_needle(loc.volume_id, loc.offset, loc.payload_crc32c);
    match magic_fail {
        Err(VolumeError::InvalidMagic { expected, actual }) => {
            assert_eq!(expected, 0x41574E33);
            assert_eq!(actual, 0xFFFFFFFF);
        }
        other => panic!("Expected InvalidMagic error, got: {:?}", other),
    }
}
