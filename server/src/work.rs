//! Exact counts of the work the server does per request: disk writes, disk syncs, disk
//! reads and payload copies. Tests assert these counts per request type, so a change that
//! adds a copy, a write or a sync fails CI on any machine, however fast it is.
//!
//! Not counted: the copy `http_body_util::Collected::to_bytes` makes when a request body
//! arrives in several frames, and reads served from the page cache through the volume mmap
//! (those are not system calls).

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct WorkCounters {
    disk_writes: AtomicU64,
    disk_write_bytes: AtomicU64,
    disk_syncs: AtomicU64,
    disk_reads: AtomicU64,
    payload_copies: AtomicU64,
    payload_copy_bytes: AtomicU64,
    payload_verify_bytes: AtomicU64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WorkSnapshot {
    pub disk_writes: u64,
    pub disk_write_bytes: u64,
    pub disk_syncs: u64,
    pub disk_reads: u64,
    pub payload_copies: u64,
    pub payload_copy_bytes: u64,
}

impl WorkCounters {
    pub fn disk_write(&self, bytes: u64) {
        self.disk_writes.fetch_add(1, Ordering::Relaxed);
        self.disk_write_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn disk_sync(&self) {
        self.disk_syncs.fetch_add(1, Ordering::Relaxed);
    }

    pub fn disk_read(&self) {
        self.disk_reads.fetch_add(1, Ordering::Relaxed);
    }

    /// A memcpy of payload bytes into a buffer the server owns. Zero-length copies are not counted.
    pub fn payload_copy(&self, bytes: u64) {
        if bytes > 0 {
            self.payload_copies.fetch_add(1, Ordering::Relaxed);
            self.payload_copy_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Payload bytes run through the CRC on a read: before the response for objects up to
    /// 4 MiB, as they are sent for larger ones (#155). Kept out of `WorkSnapshot`, whose
    /// tests compare whole snapshots; read it with `payload_verified`.
    pub fn payload_verify(&self, bytes: u64) {
        self.payload_verify_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn payload_verified(&self) -> u64 {
        self.payload_verify_bytes.load(Ordering::Relaxed)
    }

    pub fn snapshot(&self) -> WorkSnapshot {
        WorkSnapshot {
            disk_writes: self.disk_writes.load(Ordering::Relaxed),
            disk_write_bytes: self.disk_write_bytes.load(Ordering::Relaxed),
            disk_syncs: self.disk_syncs.load(Ordering::Relaxed),
            disk_reads: self.disk_reads.load(Ordering::Relaxed),
            payload_copies: self.payload_copies.load(Ordering::Relaxed),
            payload_copy_bytes: self.payload_copy_bytes.load(Ordering::Relaxed),
        }
    }
}

impl WorkSnapshot {
    /// The work done between `earlier` and `self`.
    pub fn since(&self, earlier: &WorkSnapshot) -> WorkSnapshot {
        WorkSnapshot {
            disk_writes: self.disk_writes.saturating_sub(earlier.disk_writes),
            disk_write_bytes: self.disk_write_bytes.saturating_sub(earlier.disk_write_bytes),
            disk_syncs: self.disk_syncs.saturating_sub(earlier.disk_syncs),
            disk_reads: self.disk_reads.saturating_sub(earlier.disk_reads),
            payload_copies: self.payload_copies.saturating_sub(earlier.payload_copies),
            payload_copy_bytes: self.payload_copy_bytes.saturating_sub(earlier.payload_copy_bytes),
        }
    }
}
