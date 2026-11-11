//! awan-storage/server/tests/sparse_mmap_test.rs
//! 32 GiB Sparse Virtual Volume Pre-Allocation and Zero-Remap Concurrency Tests (#482)

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use s3_server::volume::{VolumeManager, MAX_VOLUME_SPAN, SECTOR_SIZE};

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("awan_sparse_test_{}_{}", name, std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn test_sparse_volume_preallocation_and_zero_remap_concurrency() {
    let dir = temp_dir("sparse_zero_remap");
    let vol_mgr = Arc::new(VolumeManager::open_sparse(&dir, 1).expect("open sparse volume"));

    // 1. Verify volume file length on disk is exactly MAX_VOLUME_SPAN (32 GiB)
    let vol_path = dir.join("volume_0001.dat");
    let file_len = fs::metadata(&vol_path).unwrap().len();
    assert_eq!(file_len, MAX_VOLUME_SPAN, "Sparse volume file must be 32 GiB virtual size");

    // Initial remap count must be 0
    assert_eq!(vol_mgr.remap_count(), 0, "Initial remap count must be 0");

    // 2. Spawn 32 concurrent workers interleaving 1 MiB PUTs and GETs
    let running = Arc::new(AtomicBool::new(true));
    let mut handles = Vec::new();

    // 16 PUT workers
    for worker_id in 0..16 {
        let mgr = Arc::clone(&vol_mgr);
        let run = Arc::clone(&running);
        handles.push(thread::spawn(move || {
            let mut seq = 0;
            while run.load(Ordering::Relaxed) && seq < 20 {
                let key = format!("worker_{}_put_{}", worker_id, seq);
                let payload = vec![(worker_id as u8).wrapping_add(seq as u8); 1024 * 1024]; // 1 MiB
                let loc = mgr.write_needle(1, &key, &payload, 1, 0, 0).expect("write needle");
                assert_eq!(loc.payload_size, 1024 * 1024);
                seq += 1;
            }
        }));
    }

    // 16 GET workers reading superblock or any written needle
    for _worker_id in 0..16 {
        let mgr = Arc::clone(&vol_mgr);
        let run = Arc::clone(&running);
        handles.push(thread::spawn(move || {
            while run.load(Ordering::Relaxed) {
                // Slicing mmap at any offset within current_offset
                let curr = mgr.current_offset();
                if curr > SECTOR_SIZE as u64 {
                    let mmap = mgr.get_or_remap(curr).expect("get_or_remap must succeed without remap");
                    assert!(mmap.len() as u64 >= MAX_VOLUME_SPAN);
                }
                thread::sleep(Duration::from_millis(1));
            }
        }));
    }

    thread::sleep(Duration::from_millis(300));
    running.store(false, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    // Zero remap allocations must have occurred across all 32 workers
    assert_eq!(vol_mgr.remap_count(), 0, "Must have zero mmap remap calls across concurrent workers");
}

#[test]
fn test_sparse_recovery_stops_at_unwritten_boundary() {
    let dir = temp_dir("sparse_recovery_boundary");
    let vol_mgr = VolumeManager::open_sparse(&dir, 1).expect("open sparse volume");

    // Write 50 needles
    let mut locations = Vec::new();
    for i in 0..50 {
        let key = format!("sparse_rec_key_{}", i);
        let payload = vec![i as u8; 4096];
        let loc = vol_mgr.write_needle(1, &key, &payload, 1, 0, 0).expect("write needle");
        locations.push((key, payload, loc));
    }

    let written_offset = vol_mgr.current_offset();
    drop(vol_mgr);

    // Reopen sparse volume: crash recovery must scan needles and stop cleanly at written_offset
    let reopened_mgr = VolumeManager::open_sparse(&dir, 1).expect("reopen sparse volume");
    assert_eq!(reopened_mgr.current_offset(), written_offset, "Recovery must stop at the unwritten boundary");

    // All written needles must be readable
    for (key, payload, loc) in locations {
        let data = reopened_mgr.read_needle(loc.volume_id, loc.offset, loc.payload_crc32c)
            .expect(&format!("read {}", key));
        assert_eq!(data, payload);
    }
}

#[test]
fn test_negative_control_trailing_garbage_detection() {
    let dir = temp_dir("sparse_negative_control");
    let vol_mgr = VolumeManager::open_sparse(&dir, 1).expect("open sparse volume");

    let loc = vol_mgr.write_needle(1, "valid_key", b"valid_data", 1, 0, 0).expect("write needle");
    let written_offset = vol_mgr.current_offset();
    drop(vol_mgr);

    // Corrupt the sparse sector immediately following the written needle by writing invalid magic/garbage
    let vol_path = dir.join("volume_0001.dat");
    {
        use std::io::Seek;
        let mut file = OpenOptions::new().read(true).write(true).open(&vol_path).unwrap();
        file.seek(std::io::SeekFrom::Start(written_offset)).unwrap();
        // Write invalid non-zero garbage that fails magic check
        let corrupt_sector = [0xAAu8; SECTOR_SIZE];
        file.write_all(&corrupt_sector).unwrap();
        file.sync_all().unwrap();
    }

    // Reopen: recovery must stop cleanly before the corrupt sector and not crash or accept corrupt needle
    let reopened = VolumeManager::open_sparse(&dir, 1).expect("reopen after trailing garbage");
    assert_eq!(reopened.current_offset(), written_offset, "Recovery must halt at the corrupted needle boundary");

    let data = reopened.read_needle(loc.volume_id, loc.offset, loc.payload_crc32c).unwrap();
    assert_eq!(data, b"valid_data");
}
