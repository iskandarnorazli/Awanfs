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
    kind: FileKind,
    tracker: Option<Arc<FaultTracker>>,
    /// What survives a crash.
    durable: Vec<u8>,
    /// What reads see: `durable` with `pending` applied.
    current: Vec<u8>,
    /// Changes since the last `sync_data`, in order.
    pending: Vec<Pending>,
    /// Faults armed per operation, fired in order.
    faults: HashMap<SimOp, Vec<SimFault>>,
    /// How many times each operation ran (fired faults included).
    calls: HashMap<SimOp, u64>,
}

impl Default for SimState {
    fn default() -> Self {
        Self {
            kind: FileKind::Data,
            tracker: None,
            durable: Vec::new(),
            current: Vec::new(),
            pending: Vec::new(),
            faults: HashMap::new(),
            calls: HashMap::new(),
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
                    if let Some(point) = FaultPoint::from_file_op(self.kind, op, fault) {
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

    fn new_with(kind: FileKind, tracker: Option<Arc<FaultTracker>>) -> Self {
        SimFile { state: Mutex::new(SimState { kind, tracker, ..Default::default() }) }
    }

    fn with_bytes(bytes: Vec<u8>, kind: FileKind, tracker: Option<Arc<FaultTracker>>) -> Self {
        SimFile {
            state: Mutex::new(SimState {
                kind,
                tracker,
                current: bytes.clone(),
                durable: bytes,
                ..Default::default()
            }),
        }
    }

    /// Arms `fault` on the next run of `op` (several arms fire in order).
    pub fn arm(&self, op: SimOp, fault: SimFault) {
        self.state.lock().faults.entry(op).or_default().push(fault);
    }

    /// How many times `op` ran on this file.
    pub fn calls(&self, op: SimOp) -> u64 {
        self.state.lock().calls.get(&op).copied().unwrap_or(0)
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
        (SimFile::with_bytes(image, st.kind, st.tracker.clone()), stats)
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
}

/// A set of simulated files, keyed by path.
#[derive(Debug, Default)]
pub struct SimFs {
    files: Mutex<HashMap<PathBuf, Arc<SimFile>>>,
    tracker: Arc<FaultTracker>,
}

impl SimFs {
    pub fn new() -> Self {
        Self::default()
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
        (SimFs { files: Mutex::new(out), tracker: Arc::clone(&self.tracker) }, total)
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
            return Ok(Arc::clone(f));
        }
        if !create {
            return Err(io::Error::new(io::ErrorKind::NotFound, format!("{} does not exist", path.display())));
        }
        let f = Arc::new(SimFile::new_with(kind, Some(Arc::clone(&self.tracker))));
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
}
