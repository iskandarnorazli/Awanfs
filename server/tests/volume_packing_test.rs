//! awan-storage/server/tests/volume_packing_test.rs
//! TDD Unit and Integration Tests for Direct I/O and Volume Packing Needle Storage Engine (#470)

use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use awan_storage_server::direct_io::AlignedBuffer;
use awan_storage_server::volume::{VolumeManager, VolumeError, SECTOR_SIZE};
use awan_storage_server::compaction::compact_volume;

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("awan_vol_test_{}_{}", name, std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn test_aligned_buffer_allocation() {
    let buf = AlignedBuffer::new(10);
    assert_eq!(buf.len(), SECTOR_SIZE);
    assert_eq!(buf.as_ptr() as usize % SECTOR_SIZE, 0, "Buffer pointer must be 4096-byte aligned");

    let large_buf = AlignedBuffer::new(5000);
    assert_eq!(large_buf.len(), 8192);
    assert_eq!(large_buf.as_ptr() as usize % SECTOR_SIZE, 0);
}

#[test]
fn test_needle_4096_alignment_across_odd_sizes() {
    let dir = temp_dir("alignment_odd_sizes");
    let vol_mgr = VolumeManager::open(&dir, 1).expect("open volume manager");

    let sizes = [0, 1, 13, 4095, 4096, 4097, 100 * 1024];
    for (i, &sz) in sizes.iter().enumerate() {
        let key = format!("odd_key_{}_{}", i, sz);
        let payload = vec![(i % 255) as u8; sz];
        let loc = vol_mgr.write_needle(1, &key, &payload, 1).expect("write needle");

        assert_eq!(loc.offset % SECTOR_SIZE as u64, 0, "Needle offset must be 4096-byte aligned");
        assert_eq!(loc.total_len % SECTOR_SIZE as u64, 0, "Total needle length must be multiple of 4096");

        let read_back = vol_mgr.read_needle(loc.volume_id, loc.offset, loc.payload_crc32c)
            .expect("read needle");
        assert_eq!(read_back, payload, "Payload must match bit-identically");
    }
}

#[test]
fn test_detects_silent_bitrot_crc32c_negative_control() {
    let dir = temp_dir("silent_bitrot_negative_control");
    let vol_mgr = VolumeManager::open(&dir, 1).expect("open volume manager");

    let key = "bitrot_target_object";
    let payload = b"Super sensitive financial or model weights data that must not corrupt silently!";
    let loc = vol_mgr.write_needle(1, key, payload, 1).expect("write needle");

    // Normal read works bit-identically
    let valid_read = vol_mgr.read_needle(loc.volume_id, loc.offset, loc.payload_crc32c).expect("normal read");
    assert_eq!(valid_read, payload);

    // NEGATIVE CONTROL: Corrupt 1 bit on disk in the payload region
    // Needle layout: 64B header + key_len + payload.
    let vol_file_path = dir.join("volume_0001.dat");
    let mut file = OpenOptions::new().read(true).write(true).open(&vol_file_path).expect("open vol file");
    
    // Calculate exact byte position of the payload: loc.offset + 64 (header) + key.len()
    let payload_disk_offset = loc.offset + 64 + key.len() as u64;
    file.seek(SeekFrom::Start(payload_disk_offset)).expect("seek to payload");
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).expect("read byte");
    byte[0] ^= 0x01; // flip 1 bit
    file.seek(SeekFrom::Start(payload_disk_offset)).expect("seek to payload");
    file.write_all(&byte).expect("write corrupted byte");
    file.sync_all().expect("sync");
    drop(file);

    // Verify: Reading corrupted needle MUST return CrcMismatch error, NOT corrupted data
    let err = vol_mgr.read_needle(loc.volume_id, loc.offset, loc.payload_crc32c);
    match err {
        Err(VolumeError::CrcMismatch { expected, actual }) => {
            assert_eq!(expected, loc.payload_crc32c);
            assert_ne!(actual, expected, "Actual CRC must differ after bitflip");
        }
        other => panic!("Expected VolumeError::CrcMismatch, got {:?}", other),
    }
}

#[test]
fn test_crash_recovery_torn_needle_truncation() {
    let dir = temp_dir("crash_recovery_torn");
    let vol_mgr = VolumeManager::open(&dir, 1).expect("open volume manager");

    // 1. Write 1,000 objects
    let mut written_keys = Vec::new();
    for i in 0..1000 {
        let key = format!("recovered_key_{:04}", i);
        let payload = format!("payload_value_{:04}", i).into_bytes();
        let loc = vol_mgr.write_needle(1, &key, &payload, 1).expect("write needle");
        written_keys.push((key, payload, loc));
    }

    let vol_file_path = dir.join("volume_0001.dat");
    let valid_file_size = fs::metadata(&vol_file_path).unwrap().len();
    assert_eq!(valid_file_size % SECTOR_SIZE as u64, 0);

    // 2. Simulate torn write / unclean power loss: append 1,500 random garbage bytes
    {
        let mut file = OpenOptions::new().append(true).open(&vol_file_path).unwrap();
        let torn_garbage = vec![0xEFu8; 1500];
        file.write_all(&torn_garbage).unwrap();
        file.sync_all().unwrap();
    }
    assert_eq!(fs::metadata(&vol_file_path).unwrap().len(), valid_file_size + 1500);

    // Drop previous manager to simulate process crash
    drop(vol_mgr);

    // 3. Restart engine -> crash recovery scanner runs
    let restarted_mgr = VolumeManager::open(&dir, 1).expect("reopen volume manager with crash recovery");

    // The volume file must be truncated back to the exact last valid 4096-byte boundary
    let recovered_size = fs::metadata(&vol_file_path).unwrap().len();
    assert_eq!(recovered_size, valid_file_size, "File must be truncated to last valid sector boundary");

    // All 1,000 objects must read back bit-identically
    for (key, payload, loc) in &written_keys {
        let data = restarted_mgr.read_needle(loc.volume_id, loc.offset, loc.payload_crc32c)
            .expect(&format!("read recovered key {}", key));
        assert_eq!(&data, payload);
    }

    // Subsequent writes must succeed seamlessly
    let new_loc = restarted_mgr.write_needle(1, "post_recovery_key", b"fresh_data", 1).expect("write after recovery");
    assert_eq!(new_loc.offset, valid_file_size);
    let new_read = restarted_mgr.read_needle(new_loc.volume_id, new_loc.offset, new_loc.payload_crc32c).expect("read fresh");
    assert_eq!(new_read, b"fresh_data");
}

#[test]
fn test_background_compaction_reclaims_dead_space() {
    let dir = temp_dir("compactor_reclaims_dead_space");
    let vol_mgr = VolumeManager::open(&dir, 1).expect("open volume manager");

    // 1. Write 500 objects
    let mut objects = Vec::new();
    for i in 0..500 {
        let key = format!("compact_key_{:04}", i);
        let payload = vec![(i % 255) as u8; 512];
        let loc = vol_mgr.write_needle(1, &key, &payload, 1).expect("write needle");
        objects.push((key, payload, loc));
    }

    // 2. Mark 400 objects as deleted / tombstoned (80% dead space)
    // Keep 100 live objects: indices 400..500
    let live_keys: std::collections::HashSet<String> = objects[400..500].iter().map(|(k, _, _)| k.clone()).collect();

    // Trigger compaction into volume 2
    let compaction_report = compact_volume(&dir, 1, 2, &live_keys).expect("run compaction");
    assert_eq!(compaction_report.live_needles_copied, 100);
    assert_eq!(compaction_report.dead_needles_reclaimed, 400);

    // Old volume file volume_0001.dat must be unlinked/deleted
    let old_vol = dir.join("volume_0001.dat");
    assert!(!old_vol.exists(), "Old compacted volume file must be unlinked");

    // New volume file volume_0002.dat must exist and all 100 live objects must be readable
    let new_mgr = VolumeManager::open(&dir, 2).expect("open new compacted volume");
    for (key, expected_payload, _) in &objects[400..500] {
        let loc = compaction_report.new_locations.get(key).expect("new location in report");
        let data = new_mgr.read_needle(loc.volume_id, loc.offset, loc.payload_crc32c).expect("read from new volume");
        assert_eq!(&data, expected_payload);
    }
}

#[tokio::test]
async fn test_s3_reactor_persists_needles_and_recovers_after_cache_eviction() {
    use awan_storage_server::{start_s3_reactor, S3ServiceState};
    use std::net::SocketAddr;
    use std::sync::Arc;

    let dir = temp_dir("s3_reactor_vol_integration");
    let state = Arc::new(S3ServiceState::with_data_dir(&dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.expect("start reactor");
    let base_url = format!("http://{}", handle.local_addr);
    let client = reqwest::Client::new();

    // 1. Create bucket and PUT object
    client.put(&format!("{}/vol-bucket", base_url)).send().await.unwrap();
    let payload = b"Persistent 4096B-aligned WORM Needle payload via S3 PUT!";
    let put_res = client
        .put(&format!("{}/vol-bucket/needle-obj.bin", base_url))
        .body(payload.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(put_res.status(), reqwest::StatusCode::OK);

    // 2. Verify volume_0001.dat grew on disk (>= 8192 bytes: 4096B superblock + 4096B needle)
    let vol_file = dir.join("volume_0001.dat");
    assert!(vol_file.exists(), "volume_0001.dat must exist on disk");
    let file_len = fs::metadata(&vol_file).unwrap().len();
    assert!(file_len >= 8192 && file_len % SECTOR_SIZE as u64 == 0);

    // 3. Evict in-memory payload_store cache to force GET to read and CRC32C-verify from disk needle
    state.payload_store.write().clear();

    let get_res = client
        .get(&format!("{}/vol-bucket/needle-obj.bin", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(get_res.status(), reqwest::StatusCode::OK);
    let fetched = get_res.bytes().await.unwrap();
    assert_eq!(fetched.as_ref(), payload);
    handle.shutdown();
}

