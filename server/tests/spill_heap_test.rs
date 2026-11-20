//! #208: keys past the in-memory rows cost no heap each. Before #26 stage 6 every spilled key
//! was also kept as an owned `Vec<u8>` in `StorageEngine::spilled_keys`; now the spill index's
//! memtable (bounded by its share of the budget) and its segments' filters are all the heap it
//! takes. Heap is counted by this binary's allocator, the same on every platform. This file
//! holds one test, so no other test's allocations land in the count.

use s3_server::{SpillIndex, StorageEngine};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every call goes to the system allocator unchanged; this only counts bytes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = System.realloc(ptr, layout, new_size);
        if !p.is_null() {
            LIVE.fetch_add(new_size, Ordering::Relaxed);
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        p
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

#[test]
fn a_hundred_thousand_spilled_keys_take_a_bounded_heap() {
    static N: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!("awanfs_spill_heap_{}_{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed)));
    let _ = std::fs::remove_dir_all(&dir);
    // 2 MiB for the spill index: a memtable of 512 KiB.
    let spill = SpillIndex::open_with_cache_size(&dir, 2 << 20).unwrap();
    let mut engine = StorageEngine::new(1_000, 1 << 20, 65536);
    engine.attach_spill_index(spill);
    let b = engine.create_bucket("heap-bucket").unwrap();
    let put = |engine: &mut StorageEngine, i: u64| engine.put_object_at(b, format!("obj-{:07}", i).as_bytes(), 4096 * (i + 1), i, i, !i, 0, 0, i).unwrap();
    for i in 0..1_000 {
        put(&mut engine, i);
    }
    let before = LIVE.load(Ordering::Relaxed);
    for i in 1_000..101_000 {
        put(&mut engine, i);
    }
    let grown = LIVE.load(Ordering::Relaxed).saturating_sub(before);
    // The spilled keys are readable, and one of them can be deleted.
    for i in (1_000..101_000).step_by(997) {
        assert_eq!(engine.get_object(b, format!("obj-{:07}", i).as_bytes()).unwrap().size, i);
    }
    engine.delete_object(b, b"obj-0050000").unwrap();
    assert!(engine.get_object(b, b"obj-0050000").is_err());
    assert!(engine.delete_object(b, b"obj-0050000").is_err(), "deleting it again finds nothing");
    println!("heap grew by {} bytes for 100,000 spilled keys", grown);
    // A set of 100,000 owned keys alone is about 7 MB; the memtable is 512 KiB of entries.
    assert!(grown < 3 << 20, "heap grew by {} bytes for 100,000 spilled keys", grown);
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}
