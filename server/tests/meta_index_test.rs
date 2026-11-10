//! awan-storage/server/tests/ffi_bridge_test.rs
//! Gate and verification tests for Issue #463 (Awan Native @flat ObjectMeta Index & C-ABI FFI Bridge)

use s3_server::ffi_bridge::*;
use std::time::Instant;

#[test]
fn test_metadata_struct_size_and_alignment() {
    assert_eq!(std::mem::size_of::<AwanObjectMeta>(), 64);
    assert_eq!(std::mem::align_of::<AwanObjectMeta>(), 8);
}

#[test]
fn test_bucket_lifecycle_and_invariants() {
    let mut engine = AwanStorageEngine::new(1000, 4096, 65536);

    // Bucket name length validation (3..=63)
    assert_eq!(engine.create_bucket("ab").unwrap_err(), AWAN_ERR_INVALID_ARG);
    assert_eq!(engine.create_bucket(&"a".repeat(64)).unwrap_err(), AWAN_ERR_INVALID_ARG);

    // Create bucket & idempotency
    let b1 = engine.create_bucket("production-data").unwrap();
    assert_eq!(b1, 1);
    let b1_again = engine.create_bucket("production-data").unwrap();
    assert_eq!(b1, b1_again);

    // Lookup
    assert_eq!(engine.lookup_bucket("production-data").unwrap(), b1);
    assert_eq!(engine.lookup_bucket("non-existent").unwrap_err(), AWAN_ERR_BUCKET_NOT_FOUND);

    // Put an object and verify bucket deletion is blocked with AWAN_ERR_BUCKET_NOT_EMPTY
    engine.put_object(b1, b"key1", 4096, 1024, 0x1111, 0x2222, 0, 0).unwrap();
    assert_eq!(engine.delete_bucket(b1).unwrap_err(), AWAN_ERR_BUCKET_NOT_EMPTY);

    // Delete object, now bucket deletion succeeds
    engine.delete_object(b1, b"key1").unwrap();
    assert_eq!(engine.delete_bucket(b1), Ok(()));
    assert_eq!(engine.lookup_bucket("production-data").unwrap_err(), AWAN_ERR_BUCKET_NOT_FOUND);
}

#[test]
fn test_1m_metadata_index_ingest_lookup_and_overwrite() {
    let count = 100_000; // Fast thorough run; benchmark test below tests 1M
    let mut engine = AwanStorageEngine::new(count, count * 32, 65536);
    let b_id = engine.create_bucket("bench-bucket").unwrap();

    // Ingest
    let t0 = Instant::now();
    for i in 0..count {
        let key = format!("object/key/{:08}", i);
        engine.put_object(
            b_id,
            key.as_bytes(),
            (i as u64) * 4096,
            1024 + (i as u64 % 65536),
            0xAAAA_BBBB_CCCC_DDDD,
            i as u64,
            0,
            AWAN_FLAG_ACTIVE,
        ).unwrap();
    }
    let ingest_dur = t0.elapsed();
    println!("Ingested {} objects in {:?}", count, ingest_dur);

    // Point lookup
    let t_lookup = Instant::now();
    for i in (0..count).step_by(10) {
        let key = format!("object/key/{:08}", i);
        let meta = engine.get_object(b_id, key.as_bytes()).unwrap();
        assert_eq!(meta.vol_offset, (i as u64) * 4096);
        assert_eq!(meta.etag_lo, i as u64);
    }
    let lookup_dur = t_lookup.elapsed();
    println!("10k lookups in {:?}", lookup_dur);

    // Overwrite in place: verify new offset supersedes old offset
    let target_key = b"object/key/00000042";
    let meta_before = engine.get_object(b_id, target_key).unwrap();
    assert_eq!(meta_before.vol_offset, 42 * 4096);

    engine.put_object(
        b_id,
        target_key,
        999_999 * 4096,
        8192,
        0xFFFF,
        0xEEEE,
        0,
        AWAN_FLAG_ACTIVE,
    ).unwrap();

    let meta_after = engine.get_object(b_id, target_key).unwrap();
    assert_eq!(meta_after.vol_offset, 999_999 * 4096);
    assert_eq!(meta_after.size, 8192);

    // Tombstone deletion
    engine.delete_object(b_id, target_key).unwrap();
    assert_eq!(engine.get_object(b_id, target_key).unwrap_err(), AWAN_ERR_NOT_FOUND);
}

#[test]
fn test_hash_collision_full_key_check() {
    let mut engine = AwanStorageEngine::new(1000, 32768, 65536);
    let b_id = engine.create_bucket("collision-bucket").unwrap();

    let forced_hash = 0xDEAD_BEEF_CAFE_BABE;

    // Insert 100 distinct keys forced to the EXACT same hash
    for i in 0..100 {
        let key = format!("forced_collision_key_{:03}", i);
        engine.put_object_with_hash(
            b_id,
            key.as_bytes(),
            forced_hash,
            (i as u64 + 1) * 4096,
            100 + i as u64,
            0x1234,
            i as u64,
            0,
            AWAN_FLAG_ACTIVE,
        ).unwrap();
    }

    // Verify all 100 keys are individually and accurately retrievable
    for i in 0..100 {
        let key = format!("forced_collision_key_{:03}", i);
        let meta = engine.get_object_with_hash(b_id, key.as_bytes(), forced_hash).unwrap();
        assert_eq!(meta.vol_offset, (i as u64 + 1) * 4096);
        assert_eq!(meta.etag_lo, i as u64);
    }

    // Negative control: when verification is disabled, looking up key_000 returns the last inserted key (key_099)
    set_fault_disable_key_verification(true);
    let fault_meta = engine.get_object_with_hash(b_id, b"forced_collision_key_000", forced_hash).unwrap();
    assert_ne!(fault_meta.etag_lo, 0); // Fails strict equality (returns 99 instead of 0)
    assert_eq!(fault_meta.etag_lo, 99);

    // Restore fix
    set_fault_disable_key_verification(false);
    let ok_meta = engine.get_object_with_hash(b_id, b"forced_collision_key_000", forced_hash).unwrap();
    assert_eq!(ok_meta.etag_lo, 0);
}

#[test]
fn bench_1m_metadata_index() {
    let count = 1_000_000;
    let mut engine = AwanStorageEngine::new(count, count * 24, 65536);
    let b_id = engine.create_bucket("bench-1m").unwrap();

    let t0 = Instant::now();
    for i in 0..count {
        let key = format!("k{:07}", i);
        engine.put_object(
            b_id,
            key.as_bytes(),
            (i as u64) * 4096,
            4096,
            0x1,
            i as u64,
            0,
            AWAN_FLAG_ACTIVE,
        ).unwrap();
    }
    let ingest_dur = t0.elapsed();
    println!("BENCHMARK: Ingested {} objects in {:?}", count, ingest_dur);

    let sample_count = 10_000;
    let t1 = Instant::now();
    for i in 0..sample_count {
        let key = format!("k{:07}", i * 97 % count);
        let meta = engine.get_object(b_id, key.as_bytes()).unwrap();
        assert_eq!(meta.size, 4096);
    }
    let lookup_dur = t1.elapsed();
    let per_lookup_ns = lookup_dur.as_nanos() / sample_count as u128;
    println!("BENCHMARK: Point lookup median: {} ns/lookup", per_lookup_ns);
}

#[test]
fn test_lexicographical_prefix_scan() {
    let mut engine = AwanStorageEngine::new(1000, 32768, 65536);
    let b_id = engine.create_bucket("prefix-bucket").unwrap();

    let keys = vec![
        "photos/2026/01/a.jpg",
        "photos/2026/01/b.jpg",
        "photos/2026/02/c.jpg",
        "photos/2026/03/d.jpg",
        "videos/2026/v1.mp4",
        "videos/2026/v2.mp4",
    ];

    for (i, k) in keys.iter().enumerate() {
        engine.put_object(b_id, k.as_bytes(), (i as u64 + 1) * 4096, 500, 0, 0, 0, 0).unwrap();
    }

    let mut out = [AwanObjectMeta::default(); 10];

    // Scan prefix "photos/2026/01/"
    let count = engine.scan_objects(b_id, b"photos/2026/01/", b"", 10, &mut out);
    assert_eq!(count, 2);
    assert_eq!(out[0].vol_offset, 4096);
    assert_eq!(out[1].vol_offset, 8192);

    // Scan with start_after "photos/2026/01/a.jpg"
    let count_after = engine.scan_objects(b_id, b"photos/2026/", b"photos/2026/01/a.jpg", 10, &mut out);
    assert_eq!(count_after, 3);
    assert_eq!(out[0].vol_offset, 8192); // "b.jpg"
    assert_eq!(out[1].vol_offset, 12288); // "c.jpg"
    assert_eq!(out[2].vol_offset, 16384); // "d.jpg"
}

#[test]
fn test_c_abi_ffi_panic_safety() {
    unsafe {
        let engine_ptr = awan_storage_init(100, 1024, 65536);
        assert!(!engine_ptr.is_null());

        let mut bucket_id = 0u32;
        let bname = b"test-c-abi";
        let rc = awan_storage_bucket_create(engine_ptr, bname.as_ptr(), bname.len() as u32, &mut bucket_id);
        assert_eq!(rc, AWAN_OK);
        assert_eq!(bucket_id, 1);

        // Pass null pointers: verify clean structured error without crashing
        let rc_null = awan_storage_bucket_create(std::ptr::null_mut(), bname.as_ptr(), bname.len() as u32, &mut bucket_id);
        assert_eq!(rc_null, AWAN_ERR_INVALID_ARG);

        let key = b"my-file.txt";
        let put_rc = awan_storage_index_put(
            engine_ptr,
            bucket_id,
            key.as_ptr(),
            key.len() as u16,
            4096,
            12345,
            0x1234,
            0x5678,
            0,
            0,
        );
        assert_eq!(put_rc, AWAN_OK);

        let mut out_meta = AwanObjectMeta::default();
        let get_rc = awan_storage_index_get(
            engine_ptr,
            bucket_id,
            key.as_ptr(),
            key.len() as u16,
            &mut out_meta,
        );
        assert_eq!(get_rc, AWAN_OK);
        assert_eq!(out_meta.size, 12345);
        assert_eq!(out_meta.vol_offset, 4096);

        awan_storage_destroy(engine_ptr);
    }
}
