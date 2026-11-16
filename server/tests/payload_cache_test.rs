//! Regression tests for Issue #211: PayloadCache must not pin oversized request buffers.

use bytes::{Bytes, BytesMut};
use s3_server::PayloadCache;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

struct CountingAlloc;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static TEST_LOCK: Mutex<()> = Mutex::new(());

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            if new_size >= layout.size() {
                LIVE_BYTES.fetch_add(new_size - layout.size(), Ordering::Relaxed);
            } else {
                LIVE_BYTES.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        new_ptr
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

#[test]
fn payload_cache_does_not_pin_oversized_bytesmut_backing_buffers() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    let obj_count = 10_000usize;
    let obj_size = 1024usize; // 1 KiB
    let backing_size = 16 * 1024usize; // 16 KiB request buffer
    let max_bytes = 12 * 1024 * 1024usize; // 12 MiB budget (fits all 10,000 1 KiB objects)

    let cache = PayloadCache::new(max_bytes);
    let before = LIVE_BYTES.load(Ordering::SeqCst);

    for i in 0..obj_count {
        let mut buf = BytesMut::with_capacity(backing_size);
        buf.resize(obj_size, (i & 0xff) as u8);
        let slice = buf.split_to(obj_size).freeze();
        drop(buf);
        cache.insert(1, i as u64, slice);
    }

    let after = LIVE_BYTES.load(Ordering::SeqCst);
    let retained = after.saturating_sub(before);

    assert_eq!(cache.len(), obj_count);
    assert_eq!(cache.current_bytes(), obj_count * obj_size);

    // Verify cached contents are intact.
    for i in [0usize, 1, 5_000, 9_999] {
        let got = cache.get(1, i as u64).expect("cached item should exist");
        assert_eq!(got.len(), obj_size);
        assert!(got.iter().all(|&b| b == (i & 0xff) as u8));
    }

    // 10,000 * 1 KiB = 10 MiB payload + small LRU metadata (~1-2 MiB).
    // If the 16 KiB backing buffers are pinned, retained >= 156 MiB.
    let upper_bound = max_bytes + 4 * 1024 * 1024;
    assert!(
        retained <= upper_bound,
        "PayloadCache retained {} bytes for {} x {}B objects (budget {}B, upper bound {}B); oversized backing buffers are pinned",
        retained,
        obj_count,
        obj_size,
        max_bytes,
        upper_bound
    );
}

#[test]
fn payload_cache_does_not_pin_oversized_bytes_slices() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    let obj_count = 10_000usize;
    let obj_size = 1024usize;
    let backing_size = 16 * 1024usize;
    let max_bytes = 12 * 1024 * 1024usize;

    let cache = PayloadCache::new(max_bytes);
    let before = LIVE_BYTES.load(Ordering::SeqCst);

    for i in 0..obj_count {
        let backing = Bytes::from(vec![(i & 0xff) as u8; backing_size]);
        let slice = backing.slice(512..512 + obj_size);
        drop(backing);
        cache.insert(2, i as u64, slice);
    }

    let after = LIVE_BYTES.load(Ordering::SeqCst);
    let retained = after.saturating_sub(before);

    assert_eq!(cache.len(), obj_count);
    assert_eq!(cache.current_bytes(), obj_count * obj_size);

    let upper_bound = max_bytes + 4 * 1024 * 1024;
    assert!(
        retained <= upper_bound,
        "PayloadCache retained {} bytes for {} x {}B sliced Bytes (upper bound {}B); oversized backing buffers are pinned",
        retained,
        obj_count,
        obj_size,
        upper_bound
    );
}

/// #211 requirement 2: when the entry cap, not the byte cap, evicts the least recently used
/// entry, its size leaves `current_bytes` too. Before, `put` dropped it silently and the count
/// only grew, so a full cache evicted by bytes that were no longer held.
#[test]
fn eviction_by_entry_count_takes_the_entry_out_of_the_byte_count() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let cap = 4usize;
    let cache = PayloadCache::with_entry_cap(1 << 20, cap);
    for i in 0..(cap as u64 + 3) {
        cache.insert(1, i, Bytes::from(vec![i as u8; 100]));
    }
    assert_eq!(cache.len(), cap);
    assert_eq!(cache.current_bytes(), cap * 100, "the evicted entries' bytes were not subtracted");
    for i in 3..(cap as u64 + 3) {
        assert_eq!(cache.get(1, i).as_deref(), Some(&vec![i as u8; 100][..]), "entry {} is held", i);
    }
    assert!(cache.get(1, 0).is_none(), "the oldest entry was evicted");
}

#[test]
fn new_keeps_a_million_entry_cap() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let cache = PayloadCache::new(1 << 20);
    assert_eq!(cache.entry_cap(), 1_000_000);
}
