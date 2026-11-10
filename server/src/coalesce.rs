//! awan-storage/server/src/coalesce.rs
//! Single-Flight Read Coalescing Engine (Thundering Herd Defense)
//! Reference: awan-storage/EDGE_CASES_AND_CONTRACTS.md Section 1 Contract 3

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct SingleFlight {
    in_flight: Arc<Mutex<HashMap<String, broadcast::Sender<Bytes>>>>,
    pub disk_read_count: Arc<AtomicUsize>,
}

impl SingleFlight {
    pub fn new() -> Self {
        Self {
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            disk_read_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub async fn execute_or_join<F, Fut>(&self, key: &str, fetch_fn: F) -> Bytes
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Bytes>,
    {
        let (is_leader, mut rx) = {
            let mut map = self.in_flight.lock();
            if let Some(sender) = map.get(key) {
                (false, sender.subscribe())
            } else {
                let (tx, rx) = broadcast::channel(1);
                map.insert(key.to_string(), tx);
                (true, rx)
            }
        };

        if is_leader {
            self.disk_read_count.fetch_add(1, Ordering::SeqCst);
            let result = fetch_fn().await;

            let mut map = self.in_flight.lock();
            if let Some(sender) = map.remove(key) {
                let _ = sender.send(result.clone());
            }
            result
        } else {
            match rx.recv().await {
                Ok(bytes) => bytes,
                Err(_) => Bytes::new(),
            }
        }
    }
}

impl Default for SingleFlight {
    fn default() -> Self {
        Self::new()
    }
}
