//! awan-storage/server/tests/lru_cache_test.rs
//! Dynamic Auto-Scaling LRU Slab Cache Tests for Issue #478

use bytes::Bytes;
use s3_server::PayloadCache;
use std::sync::Arc;
use std::thread;

#[test]
fn test_lru_cache_capacity_clamp_and_eviction() {
    let max_bytes = 100 * 1024; // 100 KiB
    let cache = PayloadCache::new(max_bytes);

    let chunk_size = 10 * 1024; // 10 KiB
    let payload = Bytes::from(vec![0xAAu8; chunk_size]);

    // Insert 20 chunks of 10 KiB each (200 KiB total, 2x capacity)
    for i in 0..20 {
        cache.insert(1, i as u64, payload.clone());
    }

    // Verify current_bytes does not exceed capacity
    assert!(
        cache.current_bytes() <= max_bytes,
        "Cache current_bytes ({}) exceeded max_bytes ({})",
        cache.current_bytes(),
        max_bytes
    );
    assert_eq!(cache.len(), 10, "Cache should hold exactly 10 items of 10 KiB each");

    // The first 10 items (0..10) must have been evicted in O(1) LRU order
    for i in 0..10 {
        assert!(
            cache.get(1, i as u64).is_none(),
            "Old item {} should have been evicted",
            i
        );
    }

    // The last 10 items (10..20) must be present
    for i in 10..20 {
        let fetched = cache.get(1, i as u64).expect("new item should be present");
        assert_eq!(fetched.len(), chunk_size);
        assert_eq!(fetched[0], 0xAAu8);
    }
}

#[test]
fn test_lru_cache_overwrite_and_remove() {
    let max_bytes = 64 * 1024; // 64 KiB
    let cache = PayloadCache::new(max_bytes);

    let p1 = Bytes::from(vec![1u8; 16 * 1024]); // 16 KiB
    cache.insert(1, 100, p1);
    assert_eq!(cache.current_bytes(), 16 * 1024);

    // Overwrite with larger payload (32 KiB)
    let p2 = Bytes::from(vec![2u8; 32 * 1024]);
    cache.insert(1, 100, p2);
    assert_eq!(cache.current_bytes(), 32 * 1024);
    assert_eq!(cache.get(1, 100).unwrap()[0], 2u8);

    // Remove key
    cache.remove(1, 100);
    assert_eq!(cache.current_bytes(), 0);
    assert!(cache.get(1, 100).is_none());
}

#[test]
fn test_lru_cache_oversized_item_negative_control() {
    let max_bytes = 32 * 1024; // 32 KiB
    let cache = PayloadCache::new(max_bytes);

    let small = Bytes::from(vec![0x55u8; 8 * 1024]);
    cache.insert(1, 10, small.clone());
    assert_eq!(cache.current_bytes(), 8 * 1024);

    // Attempt to insert 64 KiB item (exceeds max_bytes)
    let oversized = Bytes::from(vec![0xFFu8; 64 * 1024]);
    cache.insert(1, 20, oversized);

    // Assert oversized item was rejected and small item was preserved
    assert_eq!(cache.current_bytes(), 8 * 1024);
    assert!(cache.get(1, 20).is_none());
    assert!(cache.get(1, 10).is_some());
}

#[test]
fn test_lru_cache_concurrent_multithreaded_stress() {
    let max_bytes = 512 * 1024; // 512 KiB
    let cache = Arc::new(PayloadCache::new(max_bytes));
    let mut handles = vec![];

    for t in 0..8 {
        let c = cache.clone();
        handles.push(thread::spawn(move || {
            let data = Bytes::from(vec![(t as u8) + 1; 4 * 1024]); // 4 KiB
            for i in 0..100 {
                let off = (t * 1000 + i) as u64;
                c.insert(1, off, data.clone());
                let _ = c.get(1, off);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert!(cache.current_bytes() <= max_bytes);
}
