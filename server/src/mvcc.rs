//! awan-storage/server/src/mvcc.rs
//! Epoch-Pinned MVCC Read Isolation & In-Flight Extent Lifetime Management
//! Reference: awan-storage/EDGE_CASES_AND_CONTRACTS.md Section 1 Contract 5

use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

static FAULT_CORRUPT_IN_FLIGHT_READER: AtomicBool = AtomicBool::new(false);

pub fn set_fault_corrupt_in_flight_reader(corrupt: bool) {
    FAULT_CORRUPT_IN_FLIGHT_READER.store(corrupt, Ordering::SeqCst);
}

#[derive(Clone, Debug)]
pub struct PinnedExtent {
    pub volume_id: u16,
    pub offset: u64,
    pub length: u64,
    pub payload: Bytes,
    pub epoch_version: u64,
}

pub struct MvccStore {
    pub current_epoch: AtomicU64,
    /// (bucket_id, key) -> active pinned versions
    pub active_versions: Arc<RwLock<HashMap<(u32, String), Vec<Arc<PinnedExtent>>>>>,
}

impl MvccStore {
    pub fn new() -> Self {
        Self {
            current_epoch: AtomicU64::new(1),
            active_versions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn commit_version(
        &self,
        bucket_id: u32,
        key: &str,
        volume_id: u16,
        offset: u64,
        length: u64,
        payload: Bytes,
    ) -> Arc<PinnedExtent> {
        let epoch = self.current_epoch.fetch_add(1, Ordering::SeqCst);
        let extent = Arc::new(PinnedExtent {
            volume_id,
            offset,
            length,
            payload,
            epoch_version: epoch,
        });

        let mut lock = self.active_versions.write();
        let list = lock.entry((bucket_id, key.to_string())).or_default();
        list.push(Arc::clone(&extent));
        extent
    }

    pub fn pin_latest(&self, bucket_id: u32, key: &str) -> Option<Arc<PinnedExtent>> {
        let lock = self.active_versions.read();
        let extent = lock.get(&(bucket_id, key.to_string())).and_then(|v| v.last().cloned())?;

        if FAULT_CORRUPT_IN_FLIGHT_READER.load(Ordering::Relaxed) {
            // Fault injection: simulate overwriting/corrupting payload under reader
            let corrupted = Arc::new(PinnedExtent {
                volume_id: extent.volume_id,
                offset: extent.offset,
                length: extent.length,
                payload: Bytes::from("CORRUPTED_PAYLOAD"),
                epoch_version: extent.epoch_version,
            });
            return Some(corrupted);
        }

        Some(extent)
    }

    pub fn tombstone(&self, bucket_id: u32, key: &str) {
        let _ = self.current_epoch.fetch_add(1, Ordering::SeqCst);
        // Note: existing reader pins held by clients remain valid until Arc drops!
        let _ = bucket_id;
        let _ = key;
    }
}

impl Default for MvccStore {
    fn default() -> Self {
        Self::new()
    }
}
