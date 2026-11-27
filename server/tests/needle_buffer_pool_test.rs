//! #454: a whole-needle write reuses its buffer. A fresh zeroed buffer per PUT cost a page
//! fault for every 4 KiB of payload, about 14% of the server's CPU at PUT 1 MiB (profile in
//! `measurements/2026-11-27-put-cpu-profile.md`). A reused buffer still holds the last
//! needle's bytes, so the padding after each needle must still reach the disk as zeros.

use s3_server::meta_index::AWAN_FLAG_ACTIVE;
use s3_server::volume::VolumeManager;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_pool_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed) | 1).collect()
}

#[test]
fn twenty_one_mib_writes_allocate_one_needle_buffer() {
    let dir = temp_dir("reuse");
    let vm = VolumeManager::open(&dir, 1).unwrap();
    let before = vm.needle_buffer_allocs();
    for i in 0..20u8 {
        let payload = pattern(1024 * 1024 - usize::from(i) * 100, i);
        let loc = vm.write_needle_with_attrs(1, &format!("k{}", i), &[], &payload, AWAN_FLAG_ACTIVE, 1, 2, None).unwrap();
        assert_eq!(&vm.read_needle_bytes_verified(1, loc.offset).unwrap()[..], &payload[..]);
    }
    assert_eq!(vm.needle_buffer_allocs() - before, 1, "every write after the first reuses the buffer");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_reused_buffer_writes_zeros_after_a_shorter_needle() {
    let dir = temp_dir("padding");
    let vm = VolumeManager::open(&dir, 1).unwrap();
    let long = vec![0xAAu8; 1024 * 1024];
    vm.write_needle_with_attrs(1, "long", &[], &long, AWAN_FLAG_ACTIVE, 1, 2, None).unwrap();
    let short = pattern(1024 * 1024 - 3000, 7);
    let loc = vm.write_needle_with_attrs(1, "short", &[], &short, AWAN_FLAG_ACTIVE, 1, 2, None).unwrap();
    vm.sync_now().unwrap();
    assert_eq!(vm.needle_buffer_allocs(), 1, "the second write reused the first one's buffer");

    let raw_len = 64 + "short".len() + short.len();
    let mut f = std::fs::File::open(dir.join("volume_0001.dat")).unwrap();
    let mut needle = vec![0u8; loc.total_len as usize];
    f.seek(SeekFrom::Start(loc.offset)).unwrap();
    f.read_exact(&mut needle).unwrap();
    assert_eq!(&needle[64 + 5..raw_len], &short[..]);
    assert!(needle[raw_len..].iter().all(|&b| b == 0), "padding after the needle is zeros");
    drop(vm);

    let vm = VolumeManager::open(&dir, 1).unwrap();
    assert_eq!(&vm.read_needle_bytes_verified(1, loc.offset).unwrap()[..], &short[..]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_sealed_volume_keeps_no_buffers() {
    let dir = temp_dir("seal");
    let vm = VolumeManager::open(&dir, 1).unwrap();
    let payload = pattern(256 * 1024, 3);
    vm.write_needle_with_attrs(1, "a", &[], &payload, AWAN_FLAG_ACTIVE, 1, 2, None).unwrap();
    vm.seal();
    // A write after the seal finds the pool empty (compaction seals before it lists).
    let _ = vm.write_needle_with_attrs(1, "b", &[], &payload, AWAN_FLAG_ACTIVE, 1, 2, None);
    assert_eq!(vm.needle_buffer_allocs(), 2);
    let _ = std::fs::remove_dir_all(&dir);
}
