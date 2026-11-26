//! In-process commit event stream (#159, CONFORMANCE_DESIGN.md section 12).
//!
//! Subscribers attach a bounded queue (capacity `SUBSCRIBER_QUEUE_CAP`). Delivery happens at
//! commit without blocking or failing any S3 request: when a queue is full the event is dropped
//! and `awanfs_commit_events_dropped_total{subscriber}` increments (the pattern of Decision 41).
//! When no subscriber is registered, `has_subscribers()` is a single relaxed atomic load.

use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvError, RecvTimeoutError, SyncSender, TryRecvError, TrySendError};
use std::sync::Arc;
use std::time::Duration;

/// Bounded per-subscriber event queue capacity (Decision 41 pattern).
pub const SUBSCRIBER_QUEUE_CAP: usize = 4096;

/// Kind of committed state change carried in a `CommitEvent`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CommitKind {
    Put,
    Copy,
    MultipartComplete,
    Delete,
    DeleteMarker,
    VersionDelete,
    AttrUpdate,
    BucketCreate,
    BucketDelete,
    BucketVersioning,
    BucketConfig,
}

impl CommitKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            CommitKind::Put => "Put",
            CommitKind::Copy => "Copy",
            CommitKind::MultipartComplete => "MultipartComplete",
            CommitKind::Delete => "Delete",
            CommitKind::DeleteMarker => "DeleteMarker",
            CommitKind::VersionDelete => "VersionDelete",
            CommitKind::AttrUpdate => "AttrUpdate",
            CommitKind::BucketCreate => "BucketCreate",
            CommitKind::BucketDelete => "BucketDelete",
            CommitKind::BucketVersioning => "BucketVersioning",
            CommitKind::BucketConfig => "BucketConfig",
        }
    }
}

/// One durable and indexed change emitted from the server commit path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitEvent {
    pub seq: u64,
    pub time: u64,
    pub bucket: String,
    pub key: String,
    pub version: Option<String>,
    pub kind: CommitKind,
    pub size: u64,
    pub etag: String,
    pub principal: String,
    pub request_id: String,
}

struct SubscriberEntry {
    id: u64,
    tx: SyncSender<CommitEvent>,
    dropped: Arc<AtomicU64>,
    alive: bool,
}

#[derive(Default)]
struct BusInner {
    next_id: u64,
    bucket_seqs: HashMap<u32, u64>,
    subscribers: Vec<SubscriberEntry>,
    dropped_totals: BTreeMap<String, Arc<AtomicU64>>,
}

/// Broadcast bus for committed object and bucket configuration events.
pub struct CommitEventBus {
    has_subscribers: AtomicBool,
    inner: Mutex<BusInner>,
}

impl Default for CommitEventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl CommitEventBus {
    pub fn new() -> Self {
        Self {
            has_subscribers: AtomicBool::new(false),
            inner: Mutex::new(BusInner::default()),
        }
    }

    /// Fast-path check: true only when at least one in-process subscriber is registered.
    #[inline]
    pub fn has_subscribers(&self) -> bool {
        self.has_subscribers.load(Ordering::Relaxed)
    }

    /// Subscribes `name` with a bounded queue of `SUBSCRIBER_QUEUE_CAP` events.
    pub fn subscribe(self: &Arc<Self>, name: impl Into<String>) -> CommitSubscriber {
        let name = name.into();
        let (tx, rx) = mpsc::sync_channel(SUBSCRIBER_QUEUE_CAP);
        let mut inner = self.inner.lock();
        inner.next_id += 1;
        let id = inner.next_id;
        let dropped = inner
            .dropped_totals
            .entry(name.clone())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone();
        inner.subscribers.push(SubscriberEntry {
            id,
            tx,
            dropped: Arc::clone(&dropped),
            alive: true,
        });
        self.has_subscribers.store(true, Ordering::Relaxed);
        CommitSubscriber {
            id,
            name,
            rx,
            dropped,
            bus: Arc::clone(self),
        }
    }

    /// Assigns the next per-bucket commit sequence number and delivers `build(seq)` without
    /// blocking to all registered subscribers. Costs one relaxed atomic branch when none exist.
    #[inline]
    pub fn emit_with<F>(&self, b_id: u32, build: F)
    where
        F: FnOnce(u64) -> CommitEvent,
    {
        if !self.has_subscribers.load(Ordering::Relaxed) {
            return;
        }
        let mut inner = self.inner.lock();
        if inner.subscribers.is_empty() {
            self.has_subscribers.store(false, Ordering::Relaxed);
            return;
        }
        let seq_entry = inner.bucket_seqs.entry(b_id).or_insert(0);
        *seq_entry += 1;
        let seq = *seq_entry;
        let ev = build(seq);
        let mut any_dead = false;
        for sub in &mut inner.subscribers {
            match sub.tx.try_send(ev.clone()) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    sub.dropped.fetch_add(1, Ordering::Relaxed);
                }
                Err(TrySendError::Disconnected(_)) => {
                    sub.alive = false;
                    any_dead = true;
                }
            }
        }
        if any_dead {
            inner.subscribers.retain(|s| s.alive);
            if inner.subscribers.is_empty() {
                self.has_subscribers.store(false, Ordering::Relaxed);
            }
        }
    }

    /// Number of events dropped for `subscriber` due to a full bounded queue.
    pub fn dropped(&self, subscriber: &str) -> u64 {
        self.inner
            .lock()
            .dropped_totals
            .get(subscriber)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Renders `awanfs_commit_events_dropped_total{subscriber}` in Prometheus text format.
    pub fn render_metrics(&self, out: &mut String) {
        out.push_str("# HELP awanfs_commit_events_dropped_total Commit events dropped because a subscriber queue was full.\n# TYPE awanfs_commit_events_dropped_total counter\n");
        let inner = self.inner.lock();
        for (name, count) in &inner.dropped_totals {
            let escaped = name.replace('\\', "\\\\").replace('"', "\\\"");
            let _ = writeln!(
                out,
                "awanfs_commit_events_dropped_total{{subscriber=\"{}\"}} {}",
                escaped,
                count.load(Ordering::Relaxed)
            );
        }
    }

    fn unsubscribe(&self, id: u64) {
        let mut inner = self.inner.lock();
        inner.subscribers.retain(|s| s.id != id);
        if inner.subscribers.is_empty() {
            self.has_subscribers.store(false, Ordering::Relaxed);
        }
    }
}

/// Handle to one bounded commit-event subscription. Dropping it unsubscribes automatically.
pub struct CommitSubscriber {
    id: u64,
    name: String,
    rx: Receiver<CommitEvent>,
    dropped: Arc<AtomicU64>,
    bus: Arc<CommitEventBus>,
}

impl CommitSubscriber {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn try_recv(&self) -> Result<CommitEvent, TryRecvError> {
        self.rx.try_recv()
    }

    pub fn recv(&self) -> Result<CommitEvent, RecvError> {
        self.rx.recv()
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<CommitEvent, RecvTimeoutError> {
        self.rx.recv_timeout(timeout)
    }

    pub fn drain(&self) -> Vec<CommitEvent> {
        let mut out = Vec::new();
        while let Ok(ev) = self.rx.try_recv() {
            out.push(ev);
        }
        out
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl Drop for CommitSubscriber {
    fn drop(&mut self) {
        self.bus.unsubscribe(self.id);
    }
}
