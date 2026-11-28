//! Deterministic simulation of a volume's files (#213, decision 67).
//!
//! `SimFile` keeps the bytes made durable by `sync_data` apart from the writes since, so a
//! crash can drop those writes, keep a seeded prefix of them, or tear the next one. Faults are
//! named and armed by the caller, so a test says which point it exercises. Everything is
//! driven by an explicit seed: the same seed gives the same bytes.
//!
//! Used by tests only; the server binary never constructs it.

use crate::vfile::{FileKind, VolFile, VolFs};
use memmap2::Mmap;
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// SplitMix64: small, fast and fully determined by its seed.
#[derive(Clone, Debug)]
pub struct SimRng(u64);

impl SimRng {
    pub fn new(seed: u64) -> Self {
        SimRng(seed)
    }
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    /// A value in `0..n` (`0` when `n` is 0).
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next_u64() % n
        }
    }
}

/// A file operation a fault can be armed on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SimOp {
    Write,
    Read,
    SetLen,
    Sync,
}

/// What an armed fault does when its operation runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SimFault {
    /// The call fails with an I/O error and changes nothing.
    Eio,
    /// A write fails as on a full disk and changes nothing.
    NoSpace,
}

/// Named fault and scenario points tracked for simulation coverage (#213).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FaultPoint {
    DataWriteEio,
    DataWriteNoSpace,
    DataReadEio,
    DataReadNoSpace,
    DataSetLenEio,
    DataSetLenNoSpace,
    DataSyncEio,
    DataSyncNoSpace,
    JournalWriteEio,
    JournalWriteNoSpace,
    JournalReadEio,
    JournalReadNoSpace,
    JournalSetLenEio,
    JournalSetLenNoSpace,
    JournalSyncEio,
    JournalSyncNoSpace,
    CrashDropUnsynced,
    CrashSeededPrefix,
    CrashTornWrite,
    CrashTorn4kBlock,
    CrashBeforeSync,
    CrashAfterSync,
    LateFailJournalAfterRecord,
    LateFailSyncAfterRecord,
    StreamRelocated,
    StreamLateFailNoRelocate,
    StreamAbandonedGap,
    CompactionMovedLive,
    CompactionBusyInFlight,
    MultipartManifestRecovered,
    VolumeRollover,
    JournalRetryFlushed,
    SegmentWriteEio,
    SegmentWriteNoSpace,
    SegmentReadEio,
    SegmentReadNoSpace,
    SegmentSetLenEio,
    SegmentSetLenNoSpace,
    SegmentSyncEio,
    SegmentSyncNoSpace,
    ManifestWriteEio,
    ManifestWriteNoSpace,
    ManifestReadEio,
    ManifestReadNoSpace,
    ManifestSetLenEio,
    ManifestSetLenNoSpace,
    ManifestSyncEio,
    ManifestSyncNoSpace,
    MetaFlushCommitted,
    MetaManifestSlotSwitch,
    MetaCompactL0ToL1,
    MetaCompactDeeperLevel,
    MetaManifestTornFallback,
}

impl FaultPoint {
    pub const ALL: &'static [FaultPoint] = &[
        FaultPoint::DataWriteEio,
        FaultPoint::DataWriteNoSpace,
        FaultPoint::DataReadEio,
        FaultPoint::DataReadNoSpace,
        FaultPoint::DataSetLenEio,
        FaultPoint::DataSetLenNoSpace,
        FaultPoint::DataSyncEio,
        FaultPoint::DataSyncNoSpace,
        FaultPoint::JournalWriteEio,
        FaultPoint::JournalWriteNoSpace,
        FaultPoint::JournalReadEio,
        FaultPoint::JournalReadNoSpace,
        FaultPoint::JournalSetLenEio,
        FaultPoint::JournalSetLenNoSpace,
        FaultPoint::JournalSyncEio,
        FaultPoint::JournalSyncNoSpace,
        FaultPoint::CrashDropUnsynced,
        FaultPoint::CrashSeededPrefix,
        FaultPoint::CrashTornWrite,
        FaultPoint::CrashTorn4kBlock,
        FaultPoint::CrashBeforeSync,
        FaultPoint::CrashAfterSync,
        FaultPoint::LateFailJournalAfterRecord,
        FaultPoint::LateFailSyncAfterRecord,
        FaultPoint::StreamRelocated,
        FaultPoint::StreamLateFailNoRelocate,
        FaultPoint::StreamAbandonedGap,
        FaultPoint::CompactionMovedLive,
        FaultPoint::CompactionBusyInFlight,
        FaultPoint::MultipartManifestRecovered,
        FaultPoint::VolumeRollover,
        FaultPoint::JournalRetryFlushed,
    ];

    /// Named fault and scenario points tracked in the joint VolumeManager + MetaStore simulation (#408).
    pub const JOINT_ALL: &'static [FaultPoint] = &[
        FaultPoint::DataWriteEio,
        FaultPoint::DataWriteNoSpace,
        FaultPoint::DataReadEio,
        FaultPoint::DataReadNoSpace,
        FaultPoint::DataSetLenEio,
        FaultPoint::DataSetLenNoSpace,
        FaultPoint::DataSyncEio,
        FaultPoint::DataSyncNoSpace,
        FaultPoint::JournalWriteEio,
        FaultPoint::JournalWriteNoSpace,
        FaultPoint::JournalReadEio,
        FaultPoint::JournalReadNoSpace,
        FaultPoint::JournalSetLenEio,
        FaultPoint::JournalSetLenNoSpace,
        FaultPoint::JournalSyncEio,
        FaultPoint::JournalSyncNoSpace,
        FaultPoint::SegmentWriteEio,
        FaultPoint::SegmentWriteNoSpace,
        FaultPoint::SegmentReadEio,
        FaultPoint::SegmentReadNoSpace,
        FaultPoint::SegmentSetLenEio,
        FaultPoint::SegmentSetLenNoSpace,
        FaultPoint::SegmentSyncEio,
        FaultPoint::SegmentSyncNoSpace,
        FaultPoint::ManifestWriteEio,
        FaultPoint::ManifestWriteNoSpace,
        FaultPoint::ManifestReadEio,
        FaultPoint::ManifestReadNoSpace,
        FaultPoint::ManifestSetLenEio,
        FaultPoint::ManifestSetLenNoSpace,
        FaultPoint::ManifestSyncEio,
        FaultPoint::ManifestSyncNoSpace,
        FaultPoint::CrashDropUnsynced,
        FaultPoint::CrashSeededPrefix,
        FaultPoint::CrashTornWrite,
        FaultPoint::CrashTorn4kBlock,
        FaultPoint::CrashBeforeSync,
        FaultPoint::CrashAfterSync,
        FaultPoint::LateFailJournalAfterRecord,
        FaultPoint::LateFailSyncAfterRecord,
        FaultPoint::JournalRetryFlushed,
        FaultPoint::VolumeRollover,
        FaultPoint::CompactionMovedLive,
        FaultPoint::MetaFlushCommitted,
        FaultPoint::MetaManifestSlotSwitch,
        FaultPoint::MetaCompactL0ToL1,
        FaultPoint::MetaCompactDeeperLevel,
        FaultPoint::MetaManifestTornFallback,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            FaultPoint::DataWriteEio => "data.write.eio",
            FaultPoint::DataWriteNoSpace => "data.write.nospace",
            FaultPoint::DataReadEio => "data.read.eio",
            FaultPoint::DataReadNoSpace => "data.read.nospace",
            FaultPoint::DataSetLenEio => "data.set_len.eio",
            FaultPoint::DataSetLenNoSpace => "data.set_len.nospace",
            FaultPoint::DataSyncEio => "data.sync.eio",
            FaultPoint::DataSyncNoSpace => "data.sync.nospace",
            FaultPoint::JournalWriteEio => "journal.write.eio",
            FaultPoint::JournalWriteNoSpace => "journal.write.nospace",
            FaultPoint::JournalReadEio => "journal.read.eio",
            FaultPoint::JournalReadNoSpace => "journal.read.nospace",
            FaultPoint::JournalSetLenEio => "journal.set_len.eio",
            FaultPoint::JournalSetLenNoSpace => "journal.set_len.nospace",
            FaultPoint::JournalSyncEio => "journal.sync.eio",
            FaultPoint::JournalSyncNoSpace => "journal.sync.nospace",
            FaultPoint::CrashDropUnsynced => "crash.drop_unsynced",
            FaultPoint::CrashSeededPrefix => "crash.seeded_prefix",
            FaultPoint::CrashTornWrite => "crash.torn_write",
            FaultPoint::CrashTorn4kBlock => "crash.torn_4k_block",
            FaultPoint::CrashBeforeSync => "crash.before_sync",
            FaultPoint::CrashAfterSync => "crash.after_sync",
            FaultPoint::LateFailJournalAfterRecord => "late_fail.journal_after_record",
            FaultPoint::LateFailSyncAfterRecord => "late_fail.sync_after_record",
            FaultPoint::StreamRelocated => "stream.relocated",
            FaultPoint::StreamLateFailNoRelocate => "stream.late_fail_no_relocate",
            FaultPoint::StreamAbandonedGap => "stream.abandoned_gap",
            FaultPoint::CompactionMovedLive => "compaction.moved_live",
            FaultPoint::CompactionBusyInFlight => "compaction.busy_inflight",
            FaultPoint::MultipartManifestRecovered => "multipart.manifest_recovered",
            FaultPoint::VolumeRollover => "volume.rollover",
            FaultPoint::JournalRetryFlushed => "journal.retry_flushed",
            FaultPoint::SegmentWriteEio => "segment.write.eio",
            FaultPoint::SegmentWriteNoSpace => "segment.write.nospace",
            FaultPoint::SegmentReadEio => "segment.read.eio",
            FaultPoint::SegmentReadNoSpace => "segment.read.nospace",
            FaultPoint::SegmentSetLenEio => "segment.set_len.eio",
            FaultPoint::SegmentSetLenNoSpace => "segment.set_len.nospace",
            FaultPoint::SegmentSyncEio => "segment.sync.eio",
            FaultPoint::SegmentSyncNoSpace => "segment.sync.nospace",
            FaultPoint::ManifestWriteEio => "manifest.write.eio",
            FaultPoint::ManifestWriteNoSpace => "manifest.write.nospace",
            FaultPoint::ManifestReadEio => "manifest.read.eio",
            FaultPoint::ManifestReadNoSpace => "manifest.read.nospace",
            FaultPoint::ManifestSetLenEio => "manifest.set_len.eio",
            FaultPoint::ManifestSetLenNoSpace => "manifest.set_len.nospace",
            FaultPoint::ManifestSyncEio => "manifest.sync.eio",
            FaultPoint::ManifestSyncNoSpace => "manifest.sync.nospace",
            FaultPoint::MetaFlushCommitted => "meta.flush_committed",
            FaultPoint::MetaManifestSlotSwitch => "meta.manifest_slot_switch",
            FaultPoint::MetaCompactL0ToL1 => "meta.compact_l0_to_l1",
            FaultPoint::MetaCompactDeeperLevel => "meta.compact_deeper_level",
            FaultPoint::MetaManifestTornFallback => "meta.manifest_torn_fallback",
        }
    }

    pub const fn from_file_op(kind: FileKind, op: SimOp, fault: SimFault) -> Option<FaultPoint> {
        match (kind, op, fault) {
            (FileKind::Data, SimOp::Write, SimFault::Eio) => Some(FaultPoint::DataWriteEio),
            (FileKind::Data, SimOp::Write, SimFault::NoSpace) => Some(FaultPoint::DataWriteNoSpace),
            (FileKind::Data, SimOp::Read, SimFault::Eio) => Some(FaultPoint::DataReadEio),
            (FileKind::Data, SimOp::Read, SimFault::NoSpace) => Some(FaultPoint::DataReadNoSpace),
            (FileKind::Data, SimOp::SetLen, SimFault::Eio) => Some(FaultPoint::DataSetLenEio),
            (FileKind::Data, SimOp::SetLen, SimFault::NoSpace) => Some(FaultPoint::DataSetLenNoSpace),
            (FileKind::Data, SimOp::Sync, SimFault::Eio) => Some(FaultPoint::DataSyncEio),
            (FileKind::Data, SimOp::Sync, SimFault::NoSpace) => Some(FaultPoint::DataSyncNoSpace),
            (FileKind::Journal, SimOp::Write, SimFault::Eio) => Some(FaultPoint::JournalWriteEio),
            (FileKind::Journal, SimOp::Write, SimFault::NoSpace) => Some(FaultPoint::JournalWriteNoSpace),
            (FileKind::Journal, SimOp::Read, SimFault::Eio) => Some(FaultPoint::JournalReadEio),
            (FileKind::Journal, SimOp::Read, SimFault::NoSpace) => Some(FaultPoint::JournalReadNoSpace),
            (FileKind::Journal, SimOp::SetLen, SimFault::Eio) => Some(FaultPoint::JournalSetLenEio),
            (FileKind::Journal, SimOp::SetLen, SimFault::NoSpace) => Some(FaultPoint::JournalSetLenNoSpace),
            (FileKind::Journal, SimOp::Sync, SimFault::Eio) => Some(FaultPoint::JournalSyncEio),
            (FileKind::Journal, SimOp::Sync, SimFault::NoSpace) => Some(FaultPoint::JournalSyncNoSpace),
            (FileKind::Segment, _, _) => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SimFileRole {
    Data,
    Journal,
    Segment,
    Manifest,
    Other,
}

impl SimFileRole {
    fn from_path_kind(path: &Path, kind: FileKind) -> Self {
        match kind {
            FileKind::Data => SimFileRole::Data,
            FileKind::Journal => SimFileRole::Journal,
            FileKind::Segment => {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name.starts_with("manifest.") {
                    SimFileRole::Manifest
                } else if name.starts_with("seg_") && name.ends_with(".seg") {
                    SimFileRole::Segment
                } else {
                    SimFileRole::Other
                }
            }
        }
    }

    const fn fault_point(self, op: SimOp, fault: SimFault) -> Option<FaultPoint> {
        match (self, op, fault) {
            (SimFileRole::Data, _, _) => FaultPoint::from_file_op(FileKind::Data, op, fault),
            (SimFileRole::Journal, _, _) => FaultPoint::from_file_op(FileKind::Journal, op, fault),
            (SimFileRole::Segment, SimOp::Write, SimFault::Eio) => Some(FaultPoint::SegmentWriteEio),
            (SimFileRole::Segment, SimOp::Write, SimFault::NoSpace) => Some(FaultPoint::SegmentWriteNoSpace),
            (SimFileRole::Segment, SimOp::Read, SimFault::Eio) => Some(FaultPoint::SegmentReadEio),
            (SimFileRole::Segment, SimOp::Read, SimFault::NoSpace) => Some(FaultPoint::SegmentReadNoSpace),
            (SimFileRole::Segment, SimOp::SetLen, SimFault::Eio) => Some(FaultPoint::SegmentSetLenEio),
            (SimFileRole::Segment, SimOp::SetLen, SimFault::NoSpace) => Some(FaultPoint::SegmentSetLenNoSpace),
            (SimFileRole::Segment, SimOp::Sync, SimFault::Eio) => Some(FaultPoint::SegmentSyncEio),
            (SimFileRole::Segment, SimOp::Sync, SimFault::NoSpace) => Some(FaultPoint::SegmentSyncNoSpace),
            (SimFileRole::Manifest, SimOp::Write, SimFault::Eio) => Some(FaultPoint::ManifestWriteEio),
            (SimFileRole::Manifest, SimOp::Write, SimFault::NoSpace) => Some(FaultPoint::ManifestWriteNoSpace),
            (SimFileRole::Manifest, SimOp::Read, SimFault::Eio) => Some(FaultPoint::ManifestReadEio),
            (SimFileRole::Manifest, SimOp::Read, SimFault::NoSpace) => Some(FaultPoint::ManifestReadNoSpace),
            (SimFileRole::Manifest, SimOp::SetLen, SimFault::Eio) => Some(FaultPoint::ManifestSetLenEio),
            (SimFileRole::Manifest, SimOp::SetLen, SimFault::NoSpace) => Some(FaultPoint::ManifestSetLenNoSpace),
            (SimFileRole::Manifest, SimOp::Sync, SimFault::Eio) => Some(FaultPoint::ManifestSyncEio),
            (SimFileRole::Manifest, SimOp::Sync, SimFault::NoSpace) => Some(FaultPoint::ManifestSyncNoSpace),
            (SimFileRole::Other, _, _) => None,
        }
    }
}

/// Thread-safe counter map recording how often each named [`FaultPoint`] fired.
#[derive(Debug, Default)]
pub struct FaultTracker {
    counts: Mutex<BTreeMap<FaultPoint, u64>>,
}

impl FaultTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&self, point: FaultPoint) {
        *self.counts.lock().entry(point).or_insert(0) += 1;
    }

    pub fn count(&self, point: FaultPoint) -> u64 {
        self.counts.lock().get(&point).copied().unwrap_or(0)
    }

    pub fn snapshot(&self) -> BTreeMap<FaultPoint, u64> {
        self.counts.lock().clone()
    }

    pub fn merge(&self, other: &FaultTracker) {
        let snap = other.snapshot();
        let mut dst = self.counts.lock();
        for (k, v) in snap {
            *dst.entry(k).or_insert(0) += v;
        }
    }

    pub fn missing(&self, required: &[FaultPoint]) -> Vec<FaultPoint> {
        let counts = self.counts.lock();
        required.iter().copied().filter(|fp| counts.get(fp).copied().unwrap_or(0) == 0).collect()
    }
}

/// Summary of what a simulated crash dropped or tore.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CrashStats {
    pub dropped_files: usize,
    pub torn_writes: usize,
    pub torn_4k_blocks: usize,
}

#[derive(Clone, Debug)]
enum Pending {
    Write { offset: u64, data: Vec<u8> },
    SetLen(u64),
}

fn apply(image: &mut Vec<u8>, op: &Pending) {
    match op {
        Pending::Write { offset, data } => {
            let end = *offset as usize + data.len();
            if image.len() < end {
                image.resize(end, 0);
            }
            image[*offset as usize..end].copy_from_slice(data);
        }
        Pending::SetLen(len) => image.resize(*len as usize, 0),
    }
}

#[derive(Debug)]
struct SimState {
    role: SimFileRole,
    tracker: Option<Arc<FaultTracker>>,
    /// Whether reflink copy-on-write cloning (`clone_range_from`) is enabled on this file (#524).
    reflink_supported: bool,
    /// What survives a crash.
    durable: Vec<u8>,
    /// What reads see: `durable` with `pending` applied.
    current: Vec<u8>,
    /// Changes since the last `sync_data`, in order.
    pending: Vec<Pending>,
    /// Faults armed per operation, fired in order.
    faults: HashMap<SimOp, Vec<SimFault>>,
    /// Clone errors armed explicitly, fired in order (#524).
    clone_errors: Vec<io::ErrorKind>,
    /// How many times each operation ran (fired faults included).
    calls: HashMap<SimOp, u64>,
    /// Total bytes succeeded on write calls.
    bytes_written: u64,
    /// Total bytes succeeded on read calls.
    bytes_read: u64,
    /// How many times `clone_range_from` ran on this file (#524).
    clone_calls: u64,
    /// Total bytes succeeded on clone calls (#524).
    bytes_cloned: u64,
}

impl Default for SimState {
    fn default() -> Self {
        Self {
            role: SimFileRole::Data,
            tracker: None,
            reflink_supported: false,
            durable: Vec::new(),
            current: Vec::new(),
            pending: Vec::new(),
            faults: HashMap::new(),
            clone_errors: Vec::new(),
            calls: HashMap::new(),
            bytes_written: 0,
            bytes_read: 0,
            clone_calls: 0,
            bytes_cloned: 0,
        }
    }
}

impl SimState {
    fn check(&mut self, op: SimOp) -> io::Result<()> {
        *self.calls.entry(op).or_insert(0) += 1;
        if let Some(list) = self.faults.get_mut(&op) {
            if !list.is_empty() {
                let fault = list.remove(0);
                if let Some(tracker) = &self.tracker {
                    if let Some(point) = self.role.fault_point(op, fault) {
                        tracker.record(point);
                    }
                }
                return Err(match fault {
                    SimFault::Eio => io::Error::new(io::ErrorKind::Other, format!("simulated EIO on {:?}", op)),
                    SimFault::NoSpace => io::Error::new(io::ErrorKind::Other, format!("simulated ENOSPC on {:?}", op)),
                });
            }
        }
        Ok(())
    }
}

/// An in-memory file with crash and fault injection.
#[derive(Debug, Default)]
pub struct SimFile {
    state: Mutex<SimState>,
}

/// How a crash treats the writes that were not synced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrashMode {
    /// Every unsynced change is lost.
    DropUnsynced,
    /// A seeded prefix of the unsynced changes reaches the disk, and the next one may be torn:
    /// only a seeded prefix of its bytes lands.
    SeededPrefix,
}

impl SimFile {
    pub fn new() -> Self {
        Self::default()
    }

    fn new_with(role: SimFileRole, tracker: Option<Arc<FaultTracker>>) -> Self {
        SimFile { state: Mutex::new(SimState { role, tracker, ..Default::default() }) }
    }

    fn with_bytes(bytes: Vec<u8>, role: SimFileRole, tracker: Option<Arc<FaultTracker>>) -> Self {
        SimFile {
            state: Mutex::new(SimState {
                role,
                tracker,
                current: bytes.clone(),
                durable: bytes,
                ..Default::default()
            }),
        }
    }

    /// Enables or disables reflink cloning (`clone_range_from`) on this simulated file (#524).
    pub fn set_reflink_supported(&self, enabled: bool) {
        self.state.lock().reflink_supported = enabled;
    }

    /// Arms an `io::ErrorKind` on the next `clone_range_from` call (FIFO) (#524).
    pub fn arm_clone_error(&self, kind: io::ErrorKind) {
        self.state.lock().clone_errors.push(kind);
    }

    /// Arms `fault` on the next run of `op` (several arms fire in order).
    pub fn arm(&self, op: SimOp, fault: SimFault) {
        self.state.lock().faults.entry(op).or_default().push(fault);
    }

    /// How many times `op` ran on this file.
    pub fn calls(&self, op: SimOp) -> u64 {
        self.state.lock().calls.get(&op).copied().unwrap_or(0)
    }

    /// Total bytes written through this file handle.
    pub fn bytes_written(&self) -> u64 {
        self.state.lock().bytes_written
    }

    /// Total bytes read through this file handle.
    pub fn bytes_read(&self) -> u64 {
        self.state.lock().bytes_read
    }

    /// How many times `clone_range_from` ran on this file (#524).
    pub fn clone_calls(&self) -> u64 {
        self.state.lock().clone_calls
    }

    /// Total bytes cloned into this file via `clone_range_from` (#524).
    pub fn bytes_cloned(&self) -> u64 {
        self.state.lock().bytes_cloned
    }

    /// The bytes a read sees now.
    pub fn contents(&self) -> Vec<u8> {
        self.state.lock().current.clone()
    }

    /// The bytes a crash now would certainly keep.
    pub fn durable(&self) -> Vec<u8> {
        self.state.lock().durable.clone()
    }

    /// The file as it is after a crash now, along with what was dropped or torn.
    pub fn crashed_with_stats(&self, mode: CrashMode, rng: &mut SimRng) -> (SimFile, CrashStats) {
        let st = self.state.lock();
        let mut image = st.durable.clone();
        let mut stats = CrashStats::default();
        if mode == CrashMode::DropUnsynced && !st.pending.is_empty() {
            stats.dropped_files = 1;
        }
        if mode == CrashMode::SeededPrefix && !st.pending.is_empty() {
            let kept = rng.below(st.pending.len() as u64 + 1) as usize;
            for op in &st.pending[..kept] {
                apply(&mut image, op);
            }
            if kept < st.pending.len() {
                stats.dropped_files = 1;
            }
            if let Some(Pending::Write { offset, data }) = st.pending.get(kept) {
                let torn = rng.below(data.len() as u64 + 1) as usize;
                if torn > 0 {
                    apply(&mut image, &Pending::Write { offset: *offset, data: data[..torn].to_vec() });
                    if torn < data.len() {
                        stats.torn_writes += 1;
                        if *offset % 4096 == 0 && data.len() >= 4096 && torn % 4096 != 0 {
                            stats.torn_4k_blocks += 1;
                        }
                    }
                }
            }
        }
        let crashed_file = SimFile::with_bytes(image, st.role, st.tracker.clone());
        crashed_file.state.lock().reflink_supported = st.reflink_supported;
        (crashed_file, stats)
    }

    /// The file as it is after a crash now: a new file, so the old handle's later writes
    /// cannot reach it.
    pub fn crashed(&self, mode: CrashMode, rng: &mut SimRng) -> SimFile {
        self.crashed_with_stats(mode, rng).0
    }
}

impl VolFile for SimFile {
    fn write_aligned_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        let mut st = self.state.lock();
        st.check(SimOp::Write)?;
        let op = Pending::Write { offset, data: buf.to_vec() };
        apply(&mut st.current, &op);
        st.pending.push(op);
        st.bytes_written += buf.len() as u64;
        Ok(())
    }

    fn read_aligned_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let mut st = self.state.lock();
        st.check(SimOp::Read)?;
        let end = offset as usize + buf.len();
        if end > st.current.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "read past the end of a simulated file"));
        }
        buf.copy_from_slice(&st.current[offset as usize..end]);
        st.bytes_read += buf.len() as u64;
        Ok(())
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        self.write_aligned_at(offset, buf)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        self.read_aligned_at(offset, buf)
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.state.lock().current.len() as u64)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        let mut st = self.state.lock();
        st.check(SimOp::SetLen)?;
        let op = Pending::SetLen(len);
        apply(&mut st.current, &op);
        st.pending.push(op);
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        let mut st = self.state.lock();
        st.check(SimOp::Sync)?;
        st.durable = st.current.clone();
        st.pending.clear();
        Ok(())
    }

    fn make_sparse(&self) -> io::Result<()> {
        Ok(())
    }

    fn map(&self) -> io::Result<Option<Mmap>> {
        Ok(None)
    }

    fn clone_range_from(&self, src: &Self, src_offset: u64, len: u64, dest_offset: u64) -> io::Result<()> {
        let align = crate::direct_io::SECTOR_SIZE as u64;
        if src_offset % align != 0 || dest_offset % align != 0 || len % align != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "FICLONERANGE requires 4096-byte aligned offset and length",
            ));
        }
        {
            let mut st = self.state.lock();
            st.clone_calls += 1;
            if !st.reflink_supported {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "simulated reflink unsupported",
                ));
            }
            if !st.clone_errors.is_empty() {
                let kind = st.clone_errors.remove(0);
                return Err(io::Error::new(kind, format!("simulated clone error: {:?}", kind)));
            }
        }
        if len == 0 {
            return Ok(());
        }
        let data = if std::ptr::eq(self, src) {
            let st = self.state.lock();
            let end = src_offset as usize + len as usize;
            if end > st.current.len() {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "clone past end of simulated file"));
            }
            st.current[src_offset as usize..end].to_vec()
        } else {
            let src_st = src.state.lock();
            let end = src_offset as usize + len as usize;
            if end > src_st.current.len() {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "clone past end of simulated file"));
            }
            src_st.current[src_offset as usize..end].to_vec()
        };
        let mut st = self.state.lock();
        let op = Pending::Write { offset: dest_offset, data };
        apply(&mut st.current, &op);
        st.pending.push(op);
        st.bytes_cloned += len;
        Ok(())
    }
}

/// A set of simulated files, keyed by path.
#[derive(Debug, Default)]
pub struct SimFs {
    files: Mutex<HashMap<PathBuf, Arc<SimFile>>>,
    pending_faults: Mutex<HashMap<PathBuf, Vec<(SimOp, SimFault)>>>,
    tracker: Arc<FaultTracker>,
    reflink_supported: AtomicBool,
}

impl SimFs {
    pub fn new() -> Self {
        Self::default()
    }

    /// Enables or disables reflink cloning across all files in this simulated filesystem (#524).
    pub fn set_reflink_supported(&self, enabled: bool) {
        self.reflink_supported.store(enabled, Ordering::Relaxed);
        for f in self.files.lock().values() {
            f.set_reflink_supported(enabled);
        }
    }

    pub fn fault_tracker(&self) -> Arc<FaultTracker> {
        Arc::clone(&self.tracker)
    }

    /// The file at `path`, if it exists.
    pub fn file(&self, path: &Path) -> Option<Arc<SimFile>> {
        self.files.lock().get(path).cloned()
    }

    /// Arms a fault on the file at `path`, returning `false` if the file does not exist.
    pub fn arm(&self, path: &Path, op: SimOp, fault: SimFault) -> bool {
        if let Some(f) = self.file(path) {
            f.arm(op, fault);
            true
        } else {
            false
        }
    }

    /// Arms a fault immediately if `path` exists, or attaches it when `path` is next opened.
    pub fn arm_on_open(&self, path: &Path, op: SimOp, fault: SimFault) {
        if let Some(f) = self.file(path) {
            f.arm(op, fault);
        } else {
            self.pending_faults.lock().entry(path.to_path_buf()).or_default().push((op, fault));
        }
    }

    /// Sums `file.calls(op)` over every existing file whose path matches `pred`.
    pub fn total_calls(&self, op: SimOp, mut pred: impl FnMut(&Path) -> bool) -> u64 {
        self.files
            .lock()
            .iter()
            .filter(|(p, _)| pred(p))
            .map(|(_, f)| f.calls(op))
            .sum()
    }

    /// Sums `file.bytes_written()` over every existing file whose path matches `pred`.
    pub fn total_bytes_written(&self, mut pred: impl FnMut(&Path) -> bool) -> u64 {
        self.files
            .lock()
            .iter()
            .filter(|(p, _)| pred(p))
            .map(|(_, f)| f.bytes_written())
            .sum()
    }

    /// Sums `file.bytes_read()` over every existing file whose path matches `pred`.
    pub fn total_bytes_read(&self, mut pred: impl FnMut(&Path) -> bool) -> u64 {
        self.files
            .lock()
            .iter()
            .filter(|(p, _)| pred(p))
            .map(|(_, f)| f.bytes_read())
            .sum()
    }

    /// Every file as it is after a crash now, in path order so the seed decides the same
    /// way for the same files, with crash drop/tear statistics.
    pub fn crashed_with_stats(&self, mode: CrashMode, rng: &mut SimRng) -> (SimFs, CrashStats) {
        let files = self.files.lock();
        let mut paths: Vec<&PathBuf> = files.keys().collect();
        paths.sort();
        let mut out = HashMap::with_capacity(paths.len());
        let mut total = CrashStats::default();
        for p in paths {
            let (f, stats) = files[p].crashed_with_stats(mode, rng);
            total.dropped_files += stats.dropped_files;
            total.torn_writes += stats.torn_writes;
            total.torn_4k_blocks += stats.torn_4k_blocks;
            out.insert(p.clone(), Arc::new(f));
        }
        (
            SimFs {
                files: Mutex::new(out),
                pending_faults: Mutex::new(HashMap::new()),
                tracker: Arc::clone(&self.tracker),
                reflink_supported: AtomicBool::new(self.reflink_supported.load(Ordering::Relaxed)),
            },
            total,
        )
    }

    /// Every file as it is after a crash now, in path order so the seed decides the same
    /// way for the same files.
    pub fn crashed(&self, mode: CrashMode, rng: &mut SimRng) -> SimFs {
        self.crashed_with_stats(mode, rng).0
    }
}

impl VolFs for SimFs {
    type File = SimFile;

    fn open(&self, path: &Path, kind: FileKind, create: bool) -> io::Result<Arc<SimFile>> {
        let mut files = self.files.lock();
        if let Some(f) = files.get(path) {
            if let Some(queued) = self.pending_faults.lock().remove(path) {
                for (op, fault) in queued {
                    f.arm(op, fault);
                }
            }
            return Ok(Arc::clone(f));
        }
        if !create {
            return Err(io::Error::new(io::ErrorKind::NotFound, format!("{} does not exist", path.display())));
        }
        let role = SimFileRole::from_path_kind(path, kind);
        let f = Arc::new(SimFile::new_with(role, Some(Arc::clone(&self.tracker))));
        if self.reflink_supported.load(Ordering::Relaxed) {
            f.set_reflink_supported(true);
        }
        if let Some(queued) = self.pending_faults.lock().remove(path) {
            for (op, fault) in queued {
                f.arm(op, fault);
            }
        }
        files.insert(path.to_path_buf(), Arc::clone(&f));
        Ok(f)
    }

    fn exists(&self, path: &Path) -> bool {
        self.files.lock().contains_key(path)
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        self.files
            .lock()
            .remove(path)
            .map(|_| ())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("{} does not exist", path.display())))
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        let mut out: Vec<PathBuf> = self.files.lock().keys().filter(|p| p.parent() == Some(dir)).cloned().collect();
        out.sort();
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: usize = 4096;

    fn sector(b: u8) -> Vec<u8> {
        vec![b; S]
    }

    #[test]
    fn a_crash_drops_what_was_not_synced() {
        let f = SimFile::new();
        f.write_aligned_at(0, &sector(1)).unwrap();
        f.sync_data().unwrap();
        f.write_aligned_at(S as u64, &sector(2)).unwrap();
        assert_eq!(f.contents().len(), 2 * S, "reads see the unsynced write");
        let c = f.crashed(CrashMode::DropUnsynced, &mut SimRng::new(7));
        assert_eq!(c.contents(), sector(1), "only the synced sector survives");
        assert_eq!(c.durable(), c.contents());
    }

    #[test]
    fn a_synced_write_survives_every_crash() {
        let f = SimFile::new();
        f.write_aligned_at(0, &sector(3)).unwrap();
        f.sync_data().unwrap();
        for seed in 0..200 {
            let c = f.crashed(CrashMode::SeededPrefix, &mut SimRng::new(seed));
            assert_eq!(&c.contents()[..S], &sector(3)[..], "seed {}", seed);
        }
    }

    #[test]
    fn the_same_seed_gives_the_same_bytes_and_seeds_differ() {
        let f = SimFile::new();
        for i in 0..8u8 {
            f.write_aligned_at(i as u64 * S as u64, &sector(i + 1)).unwrap();
        }
        let a = f.crashed(CrashMode::SeededPrefix, &mut SimRng::new(42)).contents();
        let b = f.crashed(CrashMode::SeededPrefix, &mut SimRng::new(42)).contents();
        assert_eq!(a, b, "a crash is a function of its seed");
        let distinct: std::collections::BTreeSet<Vec<u8>> =
            (0..64).map(|s| f.crashed(CrashMode::SeededPrefix, &mut SimRng::new(s)).contents()).collect();
        assert!(distinct.len() > 4, "seeds reach different crash states: {}", distinct.len());
    }

    #[test]
    fn a_crash_keeps_a_prefix_of_the_writes_in_order_and_may_tear_the_next() {
        let f = SimFile::new();
        for i in 0..4u8 {
            f.write_aligned_at(i as u64 * S as u64, &sector(i + 1)).unwrap();
        }
        let mut torn_seen = false;
        for seed in 0..500 {
            let c = f.crashed(CrashMode::SeededPrefix, &mut SimRng::new(seed)).contents();
            // Whole sectors land in write order: sector i is whole only if every earlier one is.
            let whole: Vec<bool> = (0..4).map(|i| c.len() >= (i + 1) * S && c[i * S..(i + 1) * S] == sector(i as u8 + 1)[..]).collect();
            for i in 1..4 {
                assert!(!whole[i] || whole[i - 1], "seed {}: sector {} landed before sector {}", seed, i, i - 1);
            }
            if c.len() % S != 0 {
                torn_seen = true;
            }
        }
        assert!(torn_seen, "some seed tears a write");
    }

    #[test]
    fn set_len_is_lost_unless_synced() {
        let f = SimFile::new();
        f.write_aligned_at(0, &sector(9)).unwrap();
        f.sync_data().unwrap();
        f.set_len(0).unwrap();
        assert_eq!(f.len().unwrap(), 0);
        let c = f.crashed(CrashMode::DropUnsynced, &mut SimRng::new(1));
        assert_eq!(c.len().unwrap(), S as u64, "the truncation was not synced");
    }

    #[test]
    fn an_armed_fault_fails_its_call_once_and_changes_nothing() {
        let f = SimFile::new();
        f.arm(SimOp::Write, SimFault::NoSpace);
        assert!(f.write_aligned_at(0, &sector(1)).is_err());
        assert_eq!(f.len().unwrap(), 0, "a failed write changes nothing");
        f.write_aligned_at(0, &sector(1)).unwrap();
        f.arm(SimOp::Sync, SimFault::Eio);
        assert!(f.sync_data().is_err());
        assert!(f.crashed(CrashMode::DropUnsynced, &mut SimRng::new(0)).contents().is_empty(), "a failed sync made nothing durable");
        f.sync_data().unwrap();
        assert_eq!(f.calls(SimOp::Write), 2);
        assert_eq!(f.calls(SimOp::Sync), 2);
    }

    #[test]
    fn reads_past_the_end_fail_and_there_is_no_mapping() {
        let f = SimFile::new();
        f.write_aligned_at(0, &sector(5)).unwrap();
        let mut buf = vec![0u8; S];
        f.read_aligned_at(0, &mut buf).unwrap();
        assert_eq!(buf, sector(5));
        assert!(f.read_aligned_at(S as u64, &mut buf).is_err());
        assert!(f.map().unwrap().is_none());
    }

    #[test]
    fn a_crashed_fs_is_independent_of_the_old_handles() {
        let fs = SimFs::new();
        let a = fs.open(Path::new("v/volume_0001.dat"), FileKind::Data, true).unwrap();
        a.write_aligned_at(0, &sector(1)).unwrap();
        a.sync_data().unwrap();
        let after = fs.crashed(CrashMode::DropUnsynced, &mut SimRng::new(3));
        a.write_aligned_at(S as u64, &sector(2)).unwrap();
        a.sync_data().unwrap();
        let b = after.open(Path::new("v/volume_0001.dat"), FileKind::Data, false).unwrap();
        assert_eq!(b.contents(), sector(1), "writes to the old handle do not reach the crashed copy");
        assert!(after.open(Path::new("v/missing"), FileKind::Data, false).is_err());
        assert!(after.exists(Path::new("v/volume_0001.dat")));
    }

    #[test]
    fn fault_tracker_records_armed_faults_and_crash_stats_across_crashes() {
        let fs = SimFs::new();
        let data = fs.open(Path::new("v/volume_0001.dat"), FileKind::Data, true).unwrap();
        let idx = fs.open(Path::new("v/volume_0001.idx"), FileKind::Journal, true).unwrap();
        data.arm(SimOp::Write, SimFault::NoSpace);
        idx.arm(SimOp::SetLen, SimFault::Eio);
        assert!(data.write_aligned_at(0, &sector(1)).is_err());
        assert!(idx.set_len(0).is_err());
        assert_eq!(fs.fault_tracker().count(FaultPoint::DataWriteNoSpace), 1);
        assert_eq!(fs.fault_tracker().count(FaultPoint::JournalSetLenEio), 1);

        data.write_aligned_at(0, &sector(2)).unwrap();
        let mut saw_torn_4k = false;
        for seed in 0..100 {
            let (crashed_fs, stats) = fs.crashed_with_stats(CrashMode::SeededPrefix, &mut SimRng::new(seed));
            assert_eq!(crashed_fs.fault_tracker().count(FaultPoint::DataWriteNoSpace), 1);
            if stats.torn_4k_blocks > 0 {
                saw_torn_4k = true;
            }
        }
        assert!(saw_torn_4k, "seeded crash must observe at least one torn 4 KiB block");
    }

    #[test]
    fn arm_on_open_and_segment_manifest_fault_points_and_io_byte_counters() {
        let fs = SimFs::new();
        let seg = Path::new("data/meta/seg_0000000000000001.seg");
        let man = Path::new("data/meta/manifest.0");
        fs.arm_on_open(seg, SimOp::SetLen, SimFault::NoSpace);
        fs.arm_on_open(man, SimOp::Sync, SimFault::Eio);

        let sf = fs.open(seg, FileKind::Segment, true).unwrap();
        assert!(sf.set_len(0).is_err());
        sf.write_at(0, &[1u8; 64]).unwrap();
        let mut buf = [0u8; 32];
        sf.read_at(0, &mut buf).unwrap();
        assert_eq!(sf.bytes_written(), 64);
        assert_eq!(sf.bytes_read(), 32);

        let mf = fs.open(man, FileKind::Segment, true).unwrap();
        mf.write_at(0, &[2u8; 44]).unwrap();
        assert!(mf.sync_data().is_err());

        let tracker = fs.fault_tracker();
        assert_eq!(tracker.count(FaultPoint::SegmentSetLenNoSpace), 1);
        assert_eq!(tracker.count(FaultPoint::ManifestSyncEio), 1);
        assert_eq!(fs.total_bytes_written(|p| p.extension().is_some_and(|e| e == "seg")), 64);
        assert_eq!(fs.total_bytes_read(|p| p.extension().is_some_and(|e| e == "seg")), 32);
        assert_eq!(fs.total_calls(SimOp::Write, |_| true), 2);
    }
}
