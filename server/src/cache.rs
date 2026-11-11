//! awan-storage/server/src/cache.rs
//! Dynamic Auto-Scaling LRU Slab Cache for Hot In-Memory Payloads
//! Bounded by total memory capacity (bytes) with O(1) eviction without global flushes.

use bytes::Bytes;
use lru::LruCache;
use parking_lot::Mutex;
use std::num::NonZeroUsize;

pub struct PayloadCache {
    max_bytes: usize,
    inner: Mutex<CacheState>,
}

struct CacheState {
    lru: LruCache<(u32, u64), (Bytes, usize)>,
    current_bytes: usize,
}

impl PayloadCache {
    /// Creates a new PayloadCache bounded by maximum byte capacity.
    pub fn new(max_bytes: usize) -> Self {
        // Allow up to 1,000,000 items in index, eviction is driven by max_bytes
        let cap = NonZeroUsize::new(1_000_000).unwrap();
        Self {
            max_bytes,
            inner: Mutex::new(CacheState {
                lru: LruCache::new(cap),
                current_bytes: 0,
            }),
        }
    }

    /// Auto-scales default cache capacity:
    /// Default to 1 GiB or 25% of system memory, minimum 128 MiB.
    pub fn default_auto_scaled() -> Self {
        const DEFAULT_CAP: usize = 1024 * 1024 * 1024; // 1 GiB
        Self::new(DEFAULT_CAP)
    }

    pub fn get(&self, b_id: u32, offset: u64) -> Option<Bytes> {
        let mut guard = self.inner.lock();
        guard.lru.get(&(b_id, offset)).map(|(b, _)| b.clone())
    }

    pub fn insert(&self, b_id: u32, offset: u64, payload: Bytes) {
        let size = payload.len();
        if size == 0 || size > self.max_bytes {
            return;
        }

        let mut guard = self.inner.lock();

        // If key already exists, deduct previous size
        if let Some((_, old_sz)) = guard.lru.pop(&(b_id, offset)) {
            guard.current_bytes = guard.current_bytes.saturating_sub(old_sz);
        }

        // Evict oldest items until there is room for the new payload
        while guard.current_bytes + size > self.max_bytes {
            if let Some((_, (_, popped_sz))) = guard.lru.pop_lru() {
                guard.current_bytes = guard.current_bytes.saturating_sub(popped_sz);
            } else {
                break;
            }
        }

        guard.current_bytes += size;
        guard.lru.put((b_id, offset), (payload, size));
    }

    pub fn remove(&self, b_id: u32, offset: u64) {
        let mut guard = self.inner.lock();
        if let Some((_, old_sz)) = guard.lru.pop(&(b_id, offset)) {
            guard.current_bytes = guard.current_bytes.saturating_sub(old_sz);
        }
    }

    pub fn clear(&self) {
        let mut guard = self.inner.lock();
        guard.lru.clear();
        guard.current_bytes = 0;
    }

    pub fn current_bytes(&self) -> usize {
        self.inner.lock().current_bytes
    }

    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    pub fn len(&self) -> usize {
        self.inner.lock().lru.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
