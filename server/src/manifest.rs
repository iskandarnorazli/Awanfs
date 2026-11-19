//! The metadata index's checkpoint (#26 stage 4): the manifest and the store that keeps it.
//! The bytes are in `FORMAT.md` ("Metadata manifest"); the order of steps is the one
//! `verification/tla/Manifest.tla` checks: sync a segment before a manifest names it, write a
//! new manifest to the other slot, delete a segment only after a durable manifest stops naming it.
//! Compaction (stage 5, decision 68) follows `verification/tla/Levels.tla`; which job runs next
//! is `meta_compaction::pick`.
//!
//! Nothing in the server uses this yet: stage 6 switches the server from redb.

use crate::meta_compaction::{pick, Job, Policy};
use crate::segment::{write_segment, Entry, SegEntry, Segment, SegmentError, SegmentWriter};
use crate::segment_set::{SegmentSet, MAX_LEVEL};
use crate::vfile::{FileKind, VolFile, VolFs};
use crate::work::WorkCounters;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const MANIFEST_MAGIC: u32 = 0x4D53_5741; // "AWSM"
pub const MANIFEST_FORMAT: u16 = 2;
/// 65,536 segments of 64 MiB: 4 TiB of index, about 40 billion rows of short keys.
pub const MAX_LIVE_SEGMENTS: usize = 1 << 16;

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
    /// By strictly increasing sequence number.
    pub segments: Vec<Live>,
}

/// A live segment and its level.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Live {
    pub seq: u64,
    pub level: u8,
}

impl Manifest {
    pub fn encode(&self) -> Result<Vec<u8>, SegmentError> {
        if self.segments.len() > MAX_LIVE_SEGMENTS {
            return Err(SegmentError::Corrupt(format!("{} live segments, at most {}", self.segments.len(), MAX_LIVE_SEGMENTS)));
        }
        if self.segments.windows(2).any(|w| w[0].seq >= w[1].seq) {
            return Err(SegmentError::Unsorted);
        }
        if let Some(l) = self.segments.iter().find(|l| l.level > MAX_LEVEL) {
            return Err(SegmentError::Corrupt(format!("segment {} at level {}", l.seq, l.level)));
        }
        let mut b = Vec::with_capacity(44 + 16 * self.segments.len());
        b.extend_from_slice(&MANIFEST_MAGIC.to_le_bytes());
        b.extend_from_slice(&MANIFEST_FORMAT.to_le_bytes());
        b.extend_from_slice(&0u16.to_le_bytes());
        b.extend_from_slice(&self.generation.to_le_bytes());
        b.extend_from_slice(&self.replay_from.volume_id.to_le_bytes());
        b.extend_from_slice(&self.replay_from.offset.to_le_bytes());
        b.extend_from_slice(&self.next_seq.to_le_bytes());
        b.extend_from_slice(&(self.segments.len() as u32).to_le_bytes());
        for s in &self.segments {
            b.extend_from_slice(&s.seq.to_le_bytes());
            b.push(s.level);
            b.extend_from_slice(&[0u8; 7]);
        }
        let crc = crc32c::crc32c(&b);
        b.extend_from_slice(&crc.to_le_bytes());
        Ok(b)
    }

    /// `None` for a slot that is not a whole, valid manifest (empty, torn, damaged, or newer).
    /// Format 1 (stage 4, no levels) is read with every segment at level 0.
    pub fn decode(b: &[u8]) -> Option<Manifest> {
        if b.len() < 44 {
            return None;
        }
        let u32at = |i: usize| u32::from_le_bytes(b[i..i + 4].try_into().unwrap());
        let u64at = |i: usize| u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
        let format = u16::from_le_bytes(b[4..6].try_into().unwrap());
        if u32at(0) != MANIFEST_MAGIC || format == 0 || format > MANIFEST_FORMAT {
            return None;
        }
        let width = if format == 1 { 8 } else { 16 };
        let n = u32at(36) as usize;
        if n > MAX_LIVE_SEGMENTS || b.len() != 44 + width * n {
            return None;
        }
        if crc32c::crc32c(&b[..40 + width * n]) != u32at(40 + width * n) {
            return None;
        }
        let mut segments = Vec::with_capacity(n);
        for i in 0..n {
            let at = 40 + width * i;
            let level = if width == 16 {
                if b[at + 9..at + 16].iter().any(|&x| x != 0) || b[at + 8] > MAX_LEVEL {
                    return None;
                }
                b[at + 8]
            } else {
                0
            };
            segments.push(Live { seq: u64at(at), level });
        }
        if segments.windows(2).any(|w| w[0].seq >= w[1].seq) {
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
    if len > (44 + 16 * MAX_LIVE_SEGMENTS) as u64 {
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
        for l in &manifest.segments {
            set.add(Segment::open(&*fs, &segment_path(dir, l.seq))?, l.level)?;
        }
        // Segment files the manifest does not name are left by a flush or compaction that failed
        // or crashed before its switch, or by a delete after one: nothing will read them.
        if slot.is_some() {
            let named: std::collections::HashSet<PathBuf> = manifest.segments.iter().map(|l| segment_path(dir, l.seq)).collect();
            for p in fs.list(&dir.join("meta"))? {
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name.starts_with("seg_") && name.ends_with(".seg") && !named.contains(&p) {
                    let _ = fs.remove(&p);
                }
            }
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
        next.segments.push(Live { seq, level: 0 });
        next.next_seq = seq + 1;
        next.replay_from = replay_from;
        self.switch(next)?;
        self.set.add(Segment::open(&*self.fs, &path)?, 0)?;
        Ok(seq)
    }

    /// Runs the next compaction `policy` asks for, if any, and returns it. The merge streams:
    /// it reads its inputs block by block and writes output segments of at most
    /// `policy.segment_bytes` as it goes, each synced before the manifest switch that names them
    /// in place of the inputs; the inputs are deleted only after that switch. A failure before
    /// the switch leaves the previous manifest current (and unnamed output files, which the next
    /// open removes). A failure after it (opening an output) leaves this store behind its
    /// manifest: open it again.
    pub fn compact(&mut self, policy: &Policy, created_at_ns: u64) -> Result<Option<Job>, SegmentError> {
        let Some(job) = pick(&self.set.levels(), policy) else { return Ok(None) };
        let to = job.to();
        let mut next = self.manifest.clone();
        if job.is_move() {
            let seq = job.inputs[0];
            for l in next.segments.iter_mut().filter(|l| l.seq == seq) {
                l.level = to;
            }
            self.switch(next)?;
            let seg = self.set.remove(seq).ok_or_else(|| SegmentError::Corrupt(format!("no segment {}", seq)))?;
            self.set.add(seg, to)?;
            return Ok(Some(job));
        }
        // Precedence: level 0 newest first, or the one input; then the level below.
        let mut groups: Vec<Vec<u64>> = if job.from == 0 { job.inputs.iter().map(|&s| vec![s]).collect() } else { vec![job.inputs.clone()] };
        if !job.targets.is_empty() {
            groups.push(job.targets.clone());
        }
        let mut seq = self.manifest.next_seq;
        let mut outputs = Vec::new();
        {
            let mut merged = self.set.merge_of(&groups)?;
            let mut w: Option<SegmentWriter<Fs::File>> = None;
            while let Some(e) = merged.next_entry()? {
                if job.drop_tombstones && e.entry == Entry::Tombstone {
                    continue;
                }
                if w.as_ref().is_some_and(|w| !w.fits(&e)) {
                    w.take().expect("checked").finish()?;
                }
                if w.is_none() {
                    w = Some(SegmentWriter::create(&*self.fs, &segment_path(&self.dir, seq), seq, created_at_ns, policy.segment_bytes)?);
                    outputs.push(seq);
                    seq += 1;
                }
                w.as_mut().expect("just made").push(&e)?;
            }
            if let Some(w) = w {
                w.finish()?;
            }
        }
        let gone: Vec<u64> = job.inputs.iter().chain(job.targets.iter()).copied().collect();
        next.segments.retain(|l| !gone.contains(&l.seq));
        next.segments.extend(outputs.iter().map(|&seq| Live { seq, level: to }));
        next.segments.sort_unstable_by_key(|l| l.seq);
        next.next_seq = seq;
        self.switch(next)?;
        // The manifest that names the inputs is now the older generation: deleting them is safe.
        for &s in &gone {
            self.set.remove(s);
        }
        for &o in &outputs {
            self.set.add(Segment::open(&*self.fs, &segment_path(&self.dir, o))?, to)?;
        }
        for &s in &gone {
            let _ = self.fs.remove(&segment_path(&self.dir, s));
        }
        Ok(Some(job))
    }

    /// Compacts until no level is over its limit; returns how many jobs ran.
    pub fn compact_all(&mut self, policy: &Policy, created_at_ns: u64) -> Result<usize, SegmentError> {
        let mut n = 0;
        while self.compact(policy, created_at_ns)?.is_some() {
            n += 1;
        }
        Ok(n)
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

    fn live(seqs: &[(u64, u8)]) -> Vec<Live> {
        seqs.iter().map(|&(seq, level)| Live { seq, level }).collect()
    }

    fn seqs(m: &Manifest) -> Vec<u64> {
        m.segments.iter().map(|l| l.seq).collect()
    }

    fn open(fs: SimFs) -> (Arc<SimFs>, MetaStore<SimFs>) {
        let fs = Arc::new(fs);
        let st = MetaStore::open(Arc::clone(&fs), dir(), Arc::new(WorkCounters::default())).unwrap();
        (fs, st)
    }

    #[test]
    fn the_encoding_round_trips_and_damage_makes_a_slot_invalid() {
        let m = Manifest { generation: 7, replay_from: at(123), next_seq: 9, segments: live(&[(2, 0), (5, 1), (8, 7)]) };
        let b = m.encode().unwrap();
        assert_eq!(b.len(), 44 + 48);
        assert_eq!(Manifest::decode(&b), Some(m.clone()));
        for i in 0..b.len() {
            let mut d = b.clone();
            d[i] ^= 0x10;
            assert_eq!(Manifest::decode(&d), None, "byte {} damaged", i);
        }
        assert_eq!(Manifest::decode(&b[..b.len() - 1]), None, "torn");
        assert_eq!(Manifest::decode(&[]), None);
        let mut newer = b.clone();
        newer[4..6].copy_from_slice(&3u16.to_le_bytes());
        assert_eq!(Manifest::decode(&newer), None, "a newer format");
        // A level past the deepest, or a reserved byte set, with a CRC that matches: invalid.
        for (at, v) in [(40 + 16 + 8, MAX_LEVEL + 1), (40 + 16 + 12, 1)] {
            let mut d = b.clone();
            d[at] = v;
            let n = d.len();
            let crc = crc32c::crc32c(&d[..n - 4]);
            d[n - 4..].copy_from_slice(&crc.to_le_bytes());
            assert_eq!(Manifest::decode(&d), None, "byte {} = {}", at, v);
        }
        assert!(Manifest { segments: live(&[(1, MAX_LEVEL + 1)]), ..m.clone() }.encode().is_err());
    }

    #[test]
    fn a_format_1_manifest_reads_with_every_segment_at_level_0() {
        let mut b = Vec::new();
        b.extend_from_slice(&MANIFEST_MAGIC.to_le_bytes());
        b.extend_from_slice(&1u16.to_le_bytes());
        b.extend_from_slice(&0u16.to_le_bytes());
        b.extend_from_slice(&4u64.to_le_bytes());
        b.extend_from_slice(&1u32.to_le_bytes());
        b.extend_from_slice(&77u64.to_le_bytes());
        b.extend_from_slice(&6u64.to_le_bytes());
        b.extend_from_slice(&2u32.to_le_bytes());
        b.extend_from_slice(&3u64.to_le_bytes());
        b.extend_from_slice(&5u64.to_le_bytes());
        let crc = crc32c::crc32c(&b);
        b.extend_from_slice(&crc.to_le_bytes());
        assert_eq!(b.len(), 44 + 16);
        let m = Manifest::decode(&b).unwrap();
        assert_eq!((m.generation, m.replay_from, m.next_seq), (4, at(77), 6));
        assert_eq!(m.segments, live(&[(3, 0), (5, 0)]));
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
        assert_eq!(seqs(st.manifest()), vec![1, 2]);
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
                let got = (m.replay_from, seqs(m));
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

    /// A small policy, so a few hundred rows fill several levels.
    fn small(l0_trigger: usize) -> Policy {
        Policy { l0_trigger, level1_bytes: 8 * 4096, ratio: 3, segment_bytes: 8 * 4096, max_level: 4 }
    }

    fn all_rows(st: &MetaStore<SimFs>) -> BTreeMap<Vec<u8>, ObjectMeta> {
        st.segments().range(&sort_key(0, b""), None, usize::MAX).unwrap().into_iter().collect()
    }

    fn named_files_only(fs: &SimFs, st: &MetaStore<SimFs>) {
        let mut want: Vec<PathBuf> = st.manifest().segments.iter().map(|l| segment_path(dir(), l.seq)).collect();
        want.extend((0..2).map(|s| slot_path(dir(), s)).filter(|p| fs.exists(p)));
        want.sort();
        assert_eq!(fs.list(&dir().join("meta")).unwrap(), want, "no segment file the manifest does not name");
    }

    #[test]
    fn an_older_row_never_wins_after_a_compaction() {
        // Stage 4's merge(1, 2) gave its output sequence number 4, above segment 3: k's row from
        // segment 1 then won over segment 3's newer one.
        let k = sort_key(1, b"k");
        let one = |key: &[u8], e: Entry| vec![SegEntry { key: key.to_vec(), entry: e }];
        let (_, mut st) = open(SimFs::new());
        st.flush(&one(&k, Entry::Row(row(1))), at(1), 1).unwrap();
        st.flush(&one(&sort_key(1, b"other"), Entry::Row(row(2))), at(2), 2).unwrap();
        st.flush(&one(&k, Entry::Row(row(3))), at(3), 3).unwrap();
        assert_eq!(st.segments().get(&k).unwrap(), Some(row(3)));
        assert!(st.compact(&small(3), 4).unwrap().is_some());
        assert_eq!(st.segments().get(&k).unwrap(), Some(row(3)), "the newest write still wins");
        // Again with the compacted rows one level down and a newer delete above them.
        st.flush(&one(&k, Entry::Tombstone), at(4), 5).unwrap();
        st.flush(&one(&sort_key(1, b"zz"), Entry::Row(row(6))), at(5), 6).unwrap();
        st.flush(&one(&sort_key(1, b"other"), Entry::Row(row(7))), at(6), 7).unwrap();
        st.compact_all(&small(3), 8).unwrap();
        assert_eq!(st.segments().get(&k).unwrap(), None);
        assert_eq!(st.segments().get(&sort_key(1, b"other")).unwrap(), Some(row(7)));
    }

    #[test]
    fn a_compaction_answers_the_same_and_deletes_its_inputs_only_after_the_switch() {
        let (fs, mut st) = open(SimFs::new());
        st.flush(&batch(0, 200), at(1), 1).unwrap();
        let mut newer = batch(100, 200);
        newer[0].entry = Entry::Tombstone; // k00100 deleted
        st.flush(&newer, at(2), 2).unwrap();
        let before = all_rows(&st);
        let job = st.compact(&small(2), 3).unwrap().unwrap();
        assert_eq!((job.from, job.inputs.clone(), job.drop_tombstones), (0, vec![2, 1], true));
        assert!(st.manifest().segments.iter().all(|l| l.seq >= 3 && l.level == 1), "{:?}", st.manifest().segments);
        assert!(st.manifest().segments.len() > 1, "split at the policy's segment size");
        assert_eq!(all_rows(&st), before, "a compaction never changes what a lookup returns");
        assert!(!fs.exists(&segment_path(dir(), 1)) && !fs.exists(&segment_path(dir(), 2)), "inputs deleted after the switch");
        assert_eq!(st.segments().get(&sort_key(1, b"k00100")).unwrap(), None);
        let entries: u64 = st.manifest().segments.iter().map(|l| Segment::open(&*fs, &segment_path(dir(), l.seq)).unwrap().entry_count).sum();
        assert_eq!(entries, 299, "the tombstone is dropped: nothing deeper holds the key");
        named_files_only(&fs, &st);
        let (_, st) = open(fs.crashed(CrashMode::DropUnsynced, &mut SimRng::new(0)));
        assert_eq!(all_rows(&st), before, "and after a restart");
    }

    #[test]
    fn tombstones_stay_while_a_deeper_level_holds_the_key() {
        let k = sort_key(1, b"k");
        let p = Policy { l0_trigger: 1, level1_bytes: 1, ratio: 2, segment_bytes: 8 * 4096, max_level: 4 };
        let (_, mut st) = open(SimFs::new());
        st.flush(&[SegEntry { key: k.clone(), entry: Entry::Row(row(1)) }], at(1), 1).unwrap();
        st.compact(&p, 2).unwrap().unwrap(); // level 0 to 1
        st.compact(&p, 3).unwrap().unwrap(); // level 1 is over its limit: to 2
        assert_eq!(st.manifest().segments.iter().map(|l| l.level).collect::<Vec<_>>(), vec![2]);
        st.flush(&[SegEntry { key: k.clone(), entry: Entry::Tombstone }], at(2), 4).unwrap();
        while let Some(job) = st.compact(&p, 5).unwrap() {
            assert_eq!(st.segments().get(&k).unwrap(), None, "after {:?}", job);
        }
        assert!(st.manifest().segments.is_empty(), "the row and its tombstone are both gone");
    }

    #[test]
    fn the_store_agrees_with_the_model_over_seeds() {
        for seed in 0..24u64 {
            let policy = small(2 + (seed % 3) as usize);
            let mut rng = SimRng::new(seed);
            let fs = Arc::new(SimFs::new());
            let work = Arc::new(WorkCounters::default());
            let mut st = MetaStore::open(Arc::clone(&fs), dir(), Arc::clone(&work)).unwrap();
            let mut model: BTreeMap<Vec<u8>, ObjectMeta> = BTreeMap::new();
            let mut n = 0u64;
            for step in 0..70 {
                match rng.below(10) {
                    0..=5 => {
                        let mut b = BTreeMap::new();
                        for _ in 0..rng.below(120) + 1 {
                            n += 1;
                            let key = sort_key(rng.below(2) as u32 + 1, format!("k{:04}", rng.below(500)).as_bytes());
                            b.insert(key, if rng.below(4) == 0 { Entry::Tombstone } else { Entry::Row(row(n)) });
                        }
                        for (key, e) in &b {
                            match e {
                                Entry::Row(r) => model.insert(key.clone(), *r),
                                Entry::Tombstone => model.remove(key),
                            };
                        }
                        let es: Vec<SegEntry> = b.into_iter().map(|(key, entry)| SegEntry { key, entry }).collect();
                        st.flush(&es, at(n), n).unwrap();
                    }
                    6 | 7 => {
                        st.compact(&policy, n).unwrap();
                    }
                    8 => {
                        st.compact_all(&policy, n).unwrap();
                    }
                    _ => {
                        drop(st);
                        st = MetaStore::open(Arc::clone(&fs), dir(), Arc::clone(&work)).unwrap();
                    }
                }
                let ctx = format!("seed {} step {}", seed, step);
                for (i, lvl) in st.segments().levels().iter().enumerate().skip(1) {
                    for w in lvl.windows(2) {
                        assert!(w[0].range.as_ref().unwrap().1 < w[1].range.as_ref().unwrap().0, "{}: level {} overlaps", ctx, i);
                    }
                }
                for b in 1..=2u32 {
                    for i in (0..500).step_by(if step % 10 == 9 { 1 } else { 23 }) {
                        let key = sort_key(b, format!("k{:04}", i).as_bytes());
                        assert_eq!(st.segments().get(&key).unwrap(), model.get(&key).copied(), "{} key {:?}", ctx, key);
                    }
                }
                let start = sort_key(rng.below(2) as u32 + 1, format!("k{:04}", rng.below(500)).as_bytes());
                let limit = rng.below(200) as usize + 1;
                let want: Vec<_> = model.range(start.clone()..).take(limit).map(|(k, m)| (k.clone(), *m)).collect();
                assert_eq!(st.segments().range(&start, None, limit).unwrap(), want, "{} range", ctx);
            }
            st.compact_all(&policy, n).unwrap();
            assert_eq!(all_rows(&st), model, "seed {}: after the last compaction", seed);
            let levels = st.segments().levels();
            assert!(levels[0].len() < policy.l0_trigger, "seed {}: level 0 under its trigger", seed);
            for i in 1..policy.max_level {
                let bytes: u64 = levels[i as usize].iter().map(|s| s.bytes).sum();
                assert!(bytes <= policy.level_target(i), "seed {}: level {} holds {} bytes", seed, i, bytes);
            }
            named_files_only(&fs, &st);
        }
    }

    #[test]
    fn lookups_stay_cheap_as_the_levels_fill() {
        let policy = Policy { l0_trigger: 4, level1_bytes: 4 * 65536, ratio: 10, segment_bytes: 65536, max_level: 7 };
        let mut rng = SimRng::new(2026);
        let work = Arc::new(WorkCounters::default());
        let fs = Arc::new(SimFs::new());
        let mut st = MetaStore::open(Arc::clone(&fs), dir(), Arc::clone(&work)).unwrap();
        let mut present = Vec::new();
        for f in 0..80u64 {
            let mut b = BTreeMap::new();
            while b.len() < 500 {
                let key = sort_key(1, format!("obj/{:08}", rng.below(100_000_000)).as_bytes());
                b.insert(key, Entry::Row(row(f)));
            }
            present.extend(b.keys().cloned());
            let es: Vec<SegEntry> = b.into_iter().map(|(key, entry)| SegEntry { key, entry }).collect();
            st.flush(&es, at(f), f).unwrap();
            st.compact_all(&policy, f).unwrap();
        }
        let levels = st.segments().levels();
        let used: Vec<usize> = levels.iter().map(Vec::len).collect();
        assert!(used.iter().skip(2).any(|&n| n > 0), "the rows reach level 2 or deeper: {:?}", used);
        let before = work.snapshot();
        let sample: Vec<_> = present.iter().step_by(4).take(10_000).cloned().collect();
        for k in &sample {
            assert!(st.segments().get(k).unwrap().is_some());
        }
        let reads = work.snapshot().since(&before).disk_reads as f64 / sample.len() as f64;
        assert!(reads <= 1.1, "{:.3} reads per present lookup over {} segments {:?}", reads, st.segments().len(), used);
        let before = work.snapshot();
        for i in 0..10_000u32 {
            assert_eq!(st.segments().get(&sort_key(1, format!("obj/{:08}x", i).as_bytes())).unwrap(), None);
        }
        let reads = work.snapshot().since(&before).disk_reads as f64 / 10_000.0;
        assert!(reads <= 0.15, "{:.3} reads per absent lookup over {} segments {:?}", reads, st.segments().len(), used);
    }

    #[test]
    fn a_compaction_that_fails_at_any_step_keeps_every_row() {
        // Segment 4 is the compaction's first output; slot 1 holds its manifest (flushes went to
        // slots 0, 1, 0).
        let cases = [("seg", SimOp::SetLen), ("seg", SimOp::Write), ("seg", SimOp::Read), ("seg", SimOp::Sync), ("seg5", SimOp::Write), ("man", SimOp::Write), ("man", SimOp::Sync)];
        for (file, op) in cases {
            for seed in 0..20 {
                let (fs, mut st) = open(SimFs::new());
                st.flush(&batch(0, 150), at(1), 1).unwrap();
                st.flush(&batch(100, 150), at(2), 2).unwrap();
                let mut third = batch(50, 100);
                third[7].entry = Entry::Tombstone;
                st.flush(&third, at(3), 3).unwrap();
                let model = all_rows(&st);
                let target = match file {
                    "seg" => segment_path(dir(), 4),
                    "seg5" => segment_path(dir(), 5),
                    _ => slot_path(dir(), 1),
                };
                fs.open(&target, FileKind::Segment, true).unwrap().arm(op, SimFault::Eio);
                // Segments of five blocks: the 250 rows need several, so segment 5 is written.
                let p = Policy { segment_bytes: 5 * 4096, ..small(3) };
                assert!(st.compact(&p, 4).is_err(), "{} {:?}", file, op);
                assert_eq!(seqs(st.manifest()), vec![1, 2, 3], "{} {:?}: the store keeps the previous manifest", file, op);
                assert_eq!(all_rows(&st), model, "{} {:?}: and answers as before", file, op);
                let (fs2, mut st) = open(fs.crashed(CrashMode::SeededPrefix, &mut SimRng::new(seed)));
                let ctx = format!("{} {:?} seed {}", file, op, seed);
                // A manifest whose sync failed may have landed whole: then the compaction is done.
                let done = st.manifest().segments.iter().all(|l| l.seq >= 4);
                assert!(seqs(st.manifest()) == vec![1, 2, 3] || (done && file == "man" && op == SimOp::Sync), "{}: {:?}", ctx, st.manifest().segments);
                assert_eq!(all_rows(&st), model, "{}: every row, nothing back from a tombstone", ctx);
                named_files_only(&fs2, &st);
                st.compact_all(&p, 5).unwrap();
                assert_eq!(all_rows(&st), model, "{}: and after compacting again", ctx);
            }
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
