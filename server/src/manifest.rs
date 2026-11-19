//! The metadata index's checkpoint (#26 stage 4): the manifest and the store that keeps it.
//! The bytes are in `FORMAT.md` ("Metadata manifest"); the order of steps is the one
//! `verification/tla/Manifest.tla` checks: sync a segment before a manifest names it, write a
//! new manifest to the other slot, delete a segment only after a durable manifest stops naming it.
//!
//! Nothing in the server uses this yet: stage 6 switches the server from redb.

use crate::segment::{write_segment, Entry, SegEntry, Segment, SegmentError};
use crate::segment_set::SegmentSet;
use crate::vfile::{FileKind, VolFile, VolFs};
use crate::work::WorkCounters;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const MANIFEST_MAGIC: u32 = 0x4D53_5741; // "AWSM"
pub const MANIFEST_FORMAT: u16 = 1;
pub const MAX_LIVE_SEGMENTS: usize = 1024;

/// Where replay starts: needles at this offset of this volume and after, and every needle of a
/// higher volume id. Everything before is in the segments.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReplayFrom {
    pub volume_id: u32,
    pub offset: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Manifest {
    pub generation: u64,
    pub replay_from: ReplayFrom,
    pub next_seq: u64,
    /// Strictly increasing.
    pub segments: Vec<u64>,
}

impl Manifest {
    pub fn encode(&self) -> Result<Vec<u8>, SegmentError> {
        if self.segments.len() > MAX_LIVE_SEGMENTS {
            return Err(SegmentError::Corrupt(format!("{} live segments, at most {}", self.segments.len(), MAX_LIVE_SEGMENTS)));
        }
        if self.segments.windows(2).any(|w| w[0] >= w[1]) {
            return Err(SegmentError::Unsorted);
        }
        let mut b = Vec::with_capacity(44 + 8 * self.segments.len());
        b.extend_from_slice(&MANIFEST_MAGIC.to_le_bytes());
        b.extend_from_slice(&MANIFEST_FORMAT.to_le_bytes());
        b.extend_from_slice(&0u16.to_le_bytes());
        b.extend_from_slice(&self.generation.to_le_bytes());
        b.extend_from_slice(&self.replay_from.volume_id.to_le_bytes());
        b.extend_from_slice(&self.replay_from.offset.to_le_bytes());
        b.extend_from_slice(&self.next_seq.to_le_bytes());
        b.extend_from_slice(&(self.segments.len() as u32).to_le_bytes());
        for s in &self.segments {
            b.extend_from_slice(&s.to_le_bytes());
        }
        let crc = crc32c::crc32c(&b);
        b.extend_from_slice(&crc.to_le_bytes());
        Ok(b)
    }

    /// `None` for a slot that is not a whole, valid manifest (empty, torn, damaged, or newer).
    pub fn decode(b: &[u8]) -> Option<Manifest> {
        if b.len() < 44 {
            return None;
        }
        let u32at = |i: usize| u32::from_le_bytes(b[i..i + 4].try_into().unwrap());
        let u64at = |i: usize| u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
        if u32at(0) != MANIFEST_MAGIC || u16::from_le_bytes(b[4..6].try_into().unwrap()) > MANIFEST_FORMAT {
            return None;
        }
        let n = u32at(36) as usize;
        if n > MAX_LIVE_SEGMENTS || b.len() != 44 + 8 * n {
            return None;
        }
        if crc32c::crc32c(&b[..40 + 8 * n]) != u32at(40 + 8 * n) {
            return None;
        }
        let segments: Vec<u64> = (0..n).map(|i| u64at(40 + 8 * i)).collect();
        if segments.windows(2).any(|w| w[0] >= w[1]) {
            return None;
        }
        Some(Manifest {
            generation: u64at(8),
            replay_from: ReplayFrom { volume_id: u32at(16), offset: u64at(20) },
            next_seq: u64at(28),
            segments,
        })
    }
}

pub fn slot_path(dir: &Path, slot: usize) -> PathBuf {
    dir.join("meta").join(format!("manifest.{}", slot))
}

pub fn segment_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join("meta").join(format!("seg_{:016x}.seg", seq))
}

fn read_slot<Fs: VolFs>(fs: &Fs, dir: &Path, slot: usize) -> Result<Option<Manifest>, SegmentError> {
    let p = slot_path(dir, slot);
    if !fs.exists(&p) {
        return Ok(None);
    }
    let f = fs.open(&p, FileKind::Segment, false)?;
    let len = f.len()?;
    if len > (44 + 8 * MAX_LIVE_SEGMENTS) as u64 {
        return Ok(None);
    }
    let mut b = vec![0u8; len as usize];
    if len > 0 {
        f.read_at(0, &mut b)?;
    }
    Ok(Manifest::decode(&b))
}

/// The valid slot with the higher generation, if either is valid.
pub fn read_manifest<Fs: VolFs>(fs: &Fs, dir: &Path) -> Result<Option<(usize, Manifest)>, SegmentError> {
    let a = read_slot(fs, dir, 0)?.map(|m| (0, m));
    let b = read_slot(fs, dir, 1)?.map(|m| (1, m));
    Ok(match (a, b) {
        (Some(x), Some(y)) => Some(if y.1.generation > x.1.generation { y } else { x }),
        (x, y) => x.or(y),
    })
}

fn write_slot<Fs: VolFs>(fs: &Fs, dir: &Path, slot: usize, m: &Manifest) -> Result<(), SegmentError> {
    let bytes = m.encode()?;
    let f = fs.open(&slot_path(dir, slot), FileKind::Segment, true)?;
    f.set_len(0)?;
    f.write_at(0, &bytes)?;
    f.sync_data()?;
    Ok(())
}

/// The segments of the metadata index and the manifest that names them.
pub struct MetaStore<Fs: VolFs> {
    fs: Arc<Fs>,
    dir: PathBuf,
    set: SegmentSet<Fs::File>,
    manifest: Manifest,
    /// The slot holding `manifest`; `None` before the first one.
    slot: Option<usize>,
}

impl<Fs: VolFs> MetaStore<Fs> {
    /// Opens the store at `dir`: the valid manifest with the higher generation and the segments
    /// it names. Without a valid manifest it is empty and replay starts at the beginning.
    pub fn open(fs: Arc<Fs>, dir: &Path, work: Arc<WorkCounters>) -> Result<Self, SegmentError> {
        let found = read_manifest(&*fs, dir)?;
        let (slot, manifest) = match found {
            Some((s, m)) => (Some(s), m),
            None => (None, Manifest { generation: 0, replay_from: ReplayFrom::default(), next_seq: 1, segments: Vec::new() }),
        };
        let mut set = SegmentSet::new(work);
        for &seq in &manifest.segments {
            set.add(Segment::open(&*fs, &segment_path(dir, seq))?)?;
        }
        Ok(MetaStore { fs, dir: dir.to_path_buf(), set, manifest, slot })
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn segments(&self) -> &SegmentSet<Fs::File> {
        &self.set
    }

    /// Makes `next` current: the other slot, a higher generation, synced. Only then does the
    /// store use it.
    fn switch(&mut self, mut next: Manifest) -> Result<(), SegmentError> {
        next.generation = self.manifest.generation + 1;
        let target = match self.slot {
            Some(s) => 1 - s,
            None => 0,
        };
        write_slot(&*self.fs, &self.dir, target, &next)?;
        self.manifest = next;
        self.slot = Some(target);
        Ok(())
    }

    /// Writes `entries` (sorted, the index rows since the last checkpoint) as a new segment,
    /// syncs it, then switches to a manifest that also names it and covers up to `replay_from`.
    /// A failure at any step leaves the previous manifest current.
    pub fn flush(&mut self, entries: &[SegEntry], replay_from: ReplayFrom, created_at_ns: u64) -> Result<u64, SegmentError> {
        let seq = self.manifest.next_seq;
        let path = segment_path(&self.dir, seq);
        write_segment(&*self.fs, &path, seq, created_at_ns, entries)?;
        let mut next = self.manifest.clone();
        next.segments.push(seq);
        next.next_seq = seq + 1;
        next.replay_from = replay_from;
        self.switch(next)?;
        self.set.add(Segment::open(&*self.fs, &path)?)?;
        Ok(seq)
    }

    /// Merges two live segments into one (stage 5 decides when). The new segment is written and
    /// synced, the manifest switched, and only then are the inputs deleted. Tombstones are kept
    /// unless `drop_tombstones` (only when no older segment remains for them to hide).
    pub fn merge(&mut self, older: u64, newer: u64, drop_tombstones: bool, created_at_ns: u64) -> Result<u64, SegmentError> {
        if older >= newer || !self.manifest.segments.contains(&older) || !self.manifest.segments.contains(&newer) {
            return Err(SegmentError::Corrupt(format!("cannot merge {} and {}", older, newer)));
        }
        let old_entries = Segment::open(&*self.fs, &segment_path(&self.dir, older))?.entries()?;
        let new_entries = Segment::open(&*self.fs, &segment_path(&self.dir, newer))?.entries()?;
        let mut merged: std::collections::BTreeMap<Vec<u8>, Entry> = old_entries.into_iter().map(|e| (e.key, e.entry)).collect();
        for e in new_entries {
            merged.insert(e.key, e.entry);
        }
        let entries: Vec<SegEntry> = merged
            .into_iter()
            .filter(|(_, e)| !(drop_tombstones && *e == Entry::Tombstone))
            .map(|(key, entry)| SegEntry { key, entry })
            .collect();
        let seq = self.manifest.next_seq;
        let path = segment_path(&self.dir, seq);
        write_segment(&*self.fs, &path, seq, created_at_ns, &entries)?;
        let mut next = self.manifest.clone();
        next.segments.retain(|s| *s != older && *s != newer);
        next.segments.push(seq);
        next.segments.sort_unstable();
        next.next_seq = seq + 1;
        self.switch(next)?;
        // The manifest that names the inputs is now the older generation: deleting them is safe.
        let mut set = SegmentSet::new(self.set_work());
        for &s in &self.manifest.segments {
            set.add(Segment::open(&*self.fs, &segment_path(&self.dir, s))?)?;
        }
        self.set = set;
        let _ = self.fs.remove(&segment_path(&self.dir, older));
        let _ = self.fs.remove(&segment_path(&self.dir, newer));
        Ok(seq)
    }

    fn set_work(&self) -> Arc<WorkCounters> {
        self.set.work()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta_index::ObjectMeta;
    use crate::segment::sort_key;
    use crate::sim::{CrashMode, SimFault, SimFs, SimOp, SimRng};
    use std::collections::BTreeMap;

    fn dir() -> &'static Path {
        Path::new("data")
    }

    fn row(i: u64) -> ObjectMeta {
        ObjectMeta { key_hash: i, bucket_flags: 1 << 32, vol_offset: i * 4096, size: i + 1, etag_hi: i, etag_lo: !i, created_at: i, key_slice: 0 }
    }

    fn batch(from: u64, n: u64) -> Vec<SegEntry> {
        (from..from + n).map(|i| SegEntry { key: sort_key(1, format!("k{:05}", i).as_bytes()), entry: Entry::Row(row(i)) }).collect()
    }

    fn at(o: u64) -> ReplayFrom {
        ReplayFrom { volume_id: 1, offset: o }
    }

    fn open(fs: SimFs) -> (Arc<SimFs>, MetaStore<SimFs>) {
        let fs = Arc::new(fs);
        let st = MetaStore::open(Arc::clone(&fs), dir(), Arc::new(WorkCounters::default())).unwrap();
        (fs, st)
    }

    #[test]
    fn the_encoding_round_trips_and_damage_makes_a_slot_invalid() {
        let m = Manifest { generation: 7, replay_from: at(123), next_seq: 9, segments: vec![2, 5, 8] };
        let b = m.encode().unwrap();
        assert_eq!(b.len(), 44 + 24);
        assert_eq!(Manifest::decode(&b), Some(m));
        for i in 0..b.len() {
            let mut d = b.clone();
            d[i] ^= 0x10;
            assert_eq!(Manifest::decode(&d), None, "byte {} damaged", i);
        }
        assert_eq!(Manifest::decode(&b[..b.len() - 1]), None, "torn");
        assert_eq!(Manifest::decode(&[]), None);
        let mut newer = b.clone();
        newer[4..6].copy_from_slice(&2u16.to_le_bytes());
        assert_eq!(Manifest::decode(&newer), None, "a newer format");
    }

    #[test]
    fn flushes_survive_a_restart_with_their_replay_position() {
        let (fs, mut st) = open(SimFs::new());
        assert_eq!(st.manifest().replay_from, ReplayFrom::default(), "no manifest: replay everything");
        st.flush(&batch(0, 300), at(4096), 1).unwrap();
        st.flush(&batch(300, 300), at(8192), 2).unwrap();
        let after = fs.crashed(CrashMode::DropUnsynced, &mut SimRng::new(1));
        let (_, st) = open(after);
        assert_eq!(st.manifest().replay_from, at(8192));
        assert_eq!(st.manifest().segments, vec![1, 2]);
        for e in batch(0, 600) {
            let Entry::Row(r) = e.entry else { unreachable!() };
            assert_eq!(st.segments().get(&e.key).unwrap(), Some(r));
        }
    }

    #[test]
    fn the_slots_alternate_and_a_torn_newer_slot_falls_back_to_the_older() {
        let (fs, mut st) = open(SimFs::new());
        st.flush(&batch(0, 10), at(10), 1).unwrap();
        st.flush(&batch(10, 10), at(20), 2).unwrap();
        st.flush(&batch(20, 10), at(30), 3).unwrap();
        assert_eq!(read_manifest(&*fs, dir()).unwrap().map(|(s, m)| (s, m.generation)), Some((0, 3)), "slot 0, 1, 0");
        // Tear the newer slot: boot uses generation 2 in slot 1.
        let f = fs.file(&slot_path(dir(), 0)).unwrap();
        let len = f.len().unwrap();
        f.set_len(len - 3).unwrap();
        let (s, m) = read_manifest(&*fs, dir()).unwrap().unwrap();
        assert_eq!((s, m.generation, m.replay_from), (1, 2, at(20)));
    }

    #[test]
    fn a_failure_at_any_step_of_a_flush_leaves_a_valid_manifest() {
        // Fail the segment's sync, or the manifest's write, or its sync; crash every way.
        for (file, op) in [("seg", SimOp::Sync), ("man", SimOp::Write), ("man", SimOp::Sync)] {
            for seed in 0..50 {
                let (fs, mut st) = open(SimFs::new());
                st.flush(&batch(0, 100), at(100), 1).unwrap();
                // The next flush writes segment 2 and manifest slot 1.
                let target = if file == "seg" { segment_path(dir(), 2) } else { slot_path(dir(), 1) };
                fs.open(&target, FileKind::Segment, true).unwrap().arm(op, SimFault::Eio);
                assert!(st.flush(&batch(100, 100), at(200), 2).is_err(), "{} {:?}", file, op);
                assert_eq!(st.manifest().replay_from, at(100), "the store keeps the previous manifest");
                let after = fs.crashed(CrashMode::SeededPrefix, &mut SimRng::new(seed));
                let (_, st) = open(after);
                let m = st.manifest();
                let got = (m.replay_from, m.segments.clone());
                // A manifest whose sync failed may still have landed whole: the model allows the
                // new one then (its segment was synced first). Never anything else.
                let new_allowed = file == "man" && op == SimOp::Sync;
                assert!(got == (at(100), vec![1]) || (new_allowed && got == (at(200), vec![1, 2])), "{} {:?} seed {}: {:?}", file, op, seed, got);
                let rows = if got.1.len() == 2 { 200 } else { 100 };
                for e in batch(0, rows) {
                    assert!(st.segments().get(&e.key).unwrap().is_some(), "{} {:?} seed {}", file, op, seed);
                }
            }
        }
    }

    #[test]
    fn a_merge_answers_the_same_and_deletes_its_inputs_only_after_the_switch() {
        let (fs, mut st) = open(SimFs::new());
        st.flush(&batch(0, 200), at(1), 1).unwrap();
        let mut newer = batch(100, 200);
        newer[0].entry = Entry::Tombstone; // k00100 deleted
        st.flush(&newer, at(2), 2).unwrap();
        let before: BTreeMap<_, _> = st.segments().range(&sort_key(1, b""), None, usize::MAX).unwrap().into_iter().collect();
        let seq = st.merge(1, 2, true, 3).unwrap();
        assert_eq!(st.manifest().segments, vec![seq]);
        let after: BTreeMap<_, _> = st.segments().range(&sort_key(1, b""), None, usize::MAX).unwrap().into_iter().collect();
        assert_eq!(before, after, "a merge never changes what a lookup returns");
        assert!(!fs.exists(&segment_path(dir(), 1)) && !fs.exists(&segment_path(dir(), 2)), "inputs deleted after the switch");
        assert_eq!(st.segments().get(&sort_key(1, b"k00100")).unwrap(), None);
    }

    #[test]
    fn a_merge_that_fails_before_its_manifest_is_durable_keeps_its_inputs() {
        for seed in 0..50 {
            let (fs, mut st) = open(SimFs::new());
            st.flush(&batch(0, 100), at(1), 1).unwrap();
            st.flush(&batch(100, 100), at(2), 2).unwrap();
            // The merge writes segment 3 and slot 0 (the current is slot 1).
            fs.open(&slot_path(dir(), 0), FileKind::Segment, true).unwrap().arm(SimOp::Sync, SimFault::Eio);
            assert!(st.merge(1, 2, false, 3).is_err());
            assert!(fs.exists(&segment_path(dir(), 1)) && fs.exists(&segment_path(dir(), 2)), "inputs kept");
            let (_, st) = open(fs.crashed(CrashMode::SeededPrefix, &mut SimRng::new(seed)));
            // The new manifest may have landed whole (its segment was synced first); either way
            // every row is there.
            let segs = st.manifest().segments.clone();
            assert!(segs == vec![1, 2] || segs == vec![3], "seed {}: {:?}", seed, segs);
            assert_eq!(st.segments().range(&sort_key(1, b""), None, usize::MAX).unwrap().len(), 200);
        }
    }

    #[test]
    fn without_a_valid_manifest_the_store_is_empty_and_replay_starts_at_the_beginning() {
        let fs = SimFs::new();
        for s in 0..2 {
            let f = fs.open(&slot_path(dir(), s), FileKind::Segment, true).unwrap();
            f.write_at(0, b"not a manifest").unwrap();
        }
        let (_, st) = open(fs);
        assert_eq!(st.manifest().replay_from, ReplayFrom::default());
        assert!(st.segments().is_empty());
    }
}
