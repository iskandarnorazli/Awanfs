//! Lookups across the metadata segments (#26 stages 3 and 5, decisions 59 and 68).
//!
//! The segments are in levels. Level 0 holds flushes, which may overlap: the higher sequence
//! number is newer. Each deeper level holds segments whose key ranges do not overlap, and a
//! shallower level is newer than a deeper one. A lookup takes the level-0 segments newest
//! first, then in each deeper level the one segment whose range holds the key; the first entry
//! it finds decides (a tombstone means the key is absent). A range scan merges the same sources
//! in sort-key order with the same rule, opening one cursor per level. Each block read counts in
//! the server's work counters.
//!
//! Nothing in the server uses this yet: stage 6 switches from redb.

use crate::meta_index::ObjectMeta;
use crate::segment::{Cursor, Entry, SegEntry, Segment, SegmentError};
use crate::vfile::VolFile;
use crate::work::WorkCounters;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

/// The deepest level.
pub const MAX_LEVEL: u8 = 7;

/// What compaction planning needs to know about one segment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegInfo {
    pub seq: u64,
    /// First and last sort keys; `None` for a segment with no entries.
    pub range: Option<(Vec<u8>, Vec<u8>)>,
    pub bytes: u64,
}

pub struct SegmentSet<F: VolFile> {
    /// Level 0, newest (highest sequence number) first.
    l0: Vec<Segment<F>>,
    /// `levels[i - 1]` is level `i`, by first key; no two overlap and none is empty.
    levels: Vec<Vec<Segment<F>>>,
    work: Arc<WorkCounters>,
}

fn first_key<F: VolFile>(s: &Segment<F>) -> &[u8] {
    s.key_range().map(|r| r.0).unwrap_or(&[])
}

fn last_key<F: VolFile>(s: &Segment<F>) -> &[u8] {
    s.key_range().map(|r| r.1).unwrap_or(&[])
}

impl<F: VolFile> SegmentSet<F> {
    pub fn new(work: Arc<WorkCounters>) -> Self {
        SegmentSet { l0: Vec::new(), levels: (0..MAX_LEVEL).map(|_| Vec::new()).collect(), work }
    }

    fn all(&self) -> impl Iterator<Item = &Segment<F>> {
        self.l0.iter().chain(self.levels.iter().flatten())
    }

    /// Adds an open segment at `level`. Refused: a second segment with one sequence number
    /// (which entry wins would be ambiguous), a level above `MAX_LEVEL`, and below level 0 an
    /// empty segment or one that overlaps a segment of its level. A manifest asking for any of
    /// these is corrupt.
    pub fn add(&mut self, seg: Segment<F>, level: u8) -> Result<(), SegmentError> {
        if self.all().any(|s| s.seq == seg.seq) {
            return Err(SegmentError::Corrupt(format!("two segments with sequence number {}", seg.seq)));
        }
        if level > MAX_LEVEL {
            return Err(SegmentError::Corrupt(format!("level {} is deeper than {}", level, MAX_LEVEL)));
        }
        let seg = seg.with_work(Arc::clone(&self.work));
        if level == 0 {
            let at = self.l0.partition_point(|s| s.seq > seg.seq);
            self.l0.insert(at, seg);
            return Ok(());
        }
        let Some((lo, hi)) = seg.key_range().map(|(a, b)| (a.to_vec(), b.to_vec())) else {
            return Err(SegmentError::Corrupt(format!("empty segment {} at level {}", seg.seq, level)));
        };
        let lvl = &mut self.levels[level as usize - 1];
        let at = lvl.partition_point(|s| first_key(s) < lo.as_slice());
        let clash = (at > 0 && last_key(&lvl[at - 1]) >= lo.as_slice()) || (at < lvl.len() && first_key(&lvl[at]) <= hi.as_slice());
        if clash {
            return Err(SegmentError::Corrupt(format!("segment {} overlaps another at level {}", seg.seq, level)));
        }
        lvl.insert(at, seg);
        Ok(())
    }

    /// Takes the segment with sequence number `seq` out of the set.
    pub fn remove(&mut self, seq: u64) -> Option<Segment<F>> {
        if let Some(i) = self.l0.iter().position(|s| s.seq == seq) {
            return Some(self.l0.remove(i));
        }
        for lvl in &mut self.levels {
            if let Some(i) = lvl.iter().position(|s| s.seq == seq) {
                return Some(lvl.remove(i));
            }
        }
        None
    }

    pub fn len(&self) -> usize {
        self.all().count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The counters this set's block reads go to.
    pub fn work(&self) -> Arc<WorkCounters> {
        Arc::clone(&self.work)
    }

    /// Every level's segments, level 0 first (newest first), deeper levels by key.
    pub fn levels(&self) -> Vec<Vec<SegInfo>> {
        let info = |s: &Segment<F>| SegInfo { seq: s.seq, range: s.key_range().map(|(a, b)| (a.to_vec(), b.to_vec())), bytes: s.len_bytes() };
        std::iter::once(self.l0.iter().map(info).collect::<Vec<_>>()).chain(self.levels.iter().map(|l| l.iter().map(info).collect::<Vec<_>>())).collect()
    }

    fn find(&self, seq: u64) -> Option<&Segment<F>> {
        self.all().find(|s| s.seq == seq)
    }

    /// The row for `key`, or `None` if no segment has it or the first entry found is a tombstone.
    pub fn get(&self, key: &[u8]) -> Result<Option<ObjectMeta>, SegmentError> {
        let level_pick = self.levels.iter().filter_map(|lvl| {
            let i = lvl.partition_point(|s| first_key(s) <= key);
            (i > 0).then(|| &lvl[i - 1])
        });
        for s in self.l0.iter().chain(level_pick) {
            if !s.spans(key) {
                continue;
            }
            match s.get(key)? {
                Some(Entry::Row(m)) => return Ok(Some(m)),
                Some(Entry::Tombstone) => return Ok(None),
                None => {}
            }
        }
        Ok(None)
    }

    /// Live rows with `start <= key < end` (no end: to the last key), in sort-key order, at
    /// most `limit` of them. Blocks are read only as the scan reaches them.
    pub fn range(&self, start: &[u8], end: Option<&[u8]>, limit: usize) -> Result<Vec<(Vec<u8>, ObjectMeta)>, SegmentError> {
        let mut out = Vec::new();
        if limit == 0 {
            return Ok(out);
        }
        let mut srcs: Vec<Src<'_, F>> = Vec::new();
        for s in &self.l0 {
            if let Some((lo, hi)) = s.key_range() {
                if hi >= start && end.is_none_or(|e| lo < e) {
                    srcs.push(Src::One(s.scan_from(start)));
                }
            }
        }
        for lvl in &self.levels {
            if !lvl.is_empty() {
                srcs.push(Src::level(lvl.iter().collect(), start, end));
            }
        }
        let mut m = Merge::new(srcs)?;
        while let Some(e) = m.next_entry()? {
            if end.is_some_and(|end| e.key.as_slice() >= end) {
                break;
            }
            if let Entry::Row(r) = e.entry {
                out.push((e.key, r));
                if out.len() == limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    /// Every entry of the named segments, tombstones included, merged in sort-key order. Each
    /// group is one source, in precedence order: a group of one segment, or the segments of one
    /// deeper level (disjoint, read in key order). Compaction reads its inputs through this.
    pub fn merge_of(&self, groups: &[Vec<u64>]) -> Result<Merge<'_, F>, SegmentError> {
        let mut srcs = Vec::new();
        for g in groups {
            let mut segs = Vec::with_capacity(g.len());
            for &seq in g {
                segs.push(self.find(seq).ok_or_else(|| SegmentError::Corrupt(format!("no segment {}", seq)))?);
            }
            segs.sort_by(|a, b| first_key(a).cmp(first_key(b)));
            srcs.push(Src::level(segs, &[], None));
        }
        Merge::new(srcs)
    }
}

/// One source of a merge: one segment's cursor, or a level's segments one after another.
enum Src<'a, F: VolFile> {
    One(Cursor<'a, F>),
    Level { segs: Vec<&'a Segment<F>>, next: usize, cur: Option<Cursor<'a, F>>, start: Vec<u8>, end: Option<Vec<u8>> },
}

impl<'a, F: VolFile> Src<'a, F> {
    fn level(segs: Vec<&'a Segment<F>>, start: &[u8], end: Option<&[u8]>) -> Self {
        // Skip the segments that end before the start: they hold nothing the scan wants.
        let next = segs.partition_point(|s| last_key(s) < start);
        Src::Level { segs, next, cur: None, start: start.to_vec(), end: end.map(|e| e.to_vec()) }
    }

    fn next_entry(&mut self) -> Result<Option<SegEntry>, SegmentError> {
        match self {
            Src::One(c) => c.next_entry(),
            Src::Level { segs, next, cur, start, end } => loop {
                if let Some(c) = cur {
                    if let Some(e) = c.next_entry()? {
                        return Ok(Some(e));
                    }
                    *cur = None;
                }
                let Some(&s) = segs.get(*next) else { return Ok(None) };
                if end.as_ref().is_some_and(|e| first_key(s) >= e.as_slice()) {
                    return Ok(None);
                }
                *cur = Some(s.scan_from(start));
                *next += 1;
            },
        }
    }
}

/// A k-way merge of sources in sort-key order: for each key, the entry of the first source (in
/// precedence order) that has it; the others' entries for that key are skipped.
pub struct Merge<'a, F: VolFile> {
    srcs: Vec<Src<'a, F>>,
    heads: Vec<Option<SegEntry>>,
    /// Min-heap on (key, rank): rank 0 is the newest source, so on equal keys it comes first.
    heap: BinaryHeap<Reverse<(Vec<u8>, usize)>>,
}

impl<'a, F: VolFile> Merge<'a, F> {
    fn new(mut srcs: Vec<Src<'a, F>>) -> Result<Self, SegmentError> {
        let mut heap = BinaryHeap::new();
        let mut heads = Vec::with_capacity(srcs.len());
        for (rank, s) in srcs.iter_mut().enumerate() {
            let e = s.next_entry()?;
            if let Some(e) = &e {
                heap.push(Reverse((e.key.clone(), rank)));
            }
            heads.push(e);
        }
        Ok(Merge { srcs, heads, heap })
    }

    fn advance(&mut self, rank: usize) -> Result<(), SegmentError> {
        self.heads[rank] = self.srcs[rank].next_entry()?;
        if let Some(e) = &self.heads[rank] {
            self.heap.push(Reverse((e.key.clone(), rank)));
        }
        Ok(())
    }

    /// The next winning entry, a tombstone included.
    pub fn next_entry(&mut self) -> Result<Option<SegEntry>, SegmentError> {
        let Some(Reverse((key, rank))) = self.heap.pop() else { return Ok(None) };
        let winner = self.heads[rank].take().expect("a heap entry has its head");
        while let Some(Reverse((k, r))) = self.heap.peek() {
            if *k != key {
                break;
            }
            let r = *r;
            self.heap.pop();
            self.advance(r)?;
        }
        self.advance(rank)?;
        Ok(Some(winner))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::{sort_key, write_segment};
    use crate::sim::{SimFs, SimRng};
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn row(i: u64) -> ObjectMeta {
        ObjectMeta { key_hash: i, bucket_flags: 1 << 32, vol_offset: i * 4096, size: i + 1, etag_hi: i, etag_lo: !i, created_at: i, key_slice: 0 }
    }

    fn path(seq: u64) -> PathBuf {
        PathBuf::from(format!("meta/seg_{:016x}.seg", seq))
    }

    /// Writes one segment per map (oldest first, seq 1, 2, ...) and opens them all.
    fn set(fs: &SimFs, maps: &[BTreeMap<Vec<u8>, Entry>], work: Arc<WorkCounters>) -> SegmentSet<crate::sim::SimFile> {
        let mut set = SegmentSet::new(work);
        for (i, m) in maps.iter().enumerate() {
            let seq = i as u64 + 1;
            let es: Vec<SegEntry> = m.iter().map(|(k, e)| SegEntry { key: k.clone(), entry: *e }).collect();
            write_segment(fs, &path(seq), seq, 0, &es).unwrap();
            set.add(Segment::open(fs, &path(seq)).unwrap(), 0).unwrap();
        }
        set
    }

    /// Random segments over a small key space (so they overlap), and the model: the maps
    /// applied oldest to newest.
    fn random(seed: u64, nsegs: usize, per: usize) -> (Vec<BTreeMap<Vec<u8>, Entry>>, BTreeMap<Vec<u8>, ObjectMeta>) {
        let mut rng = SimRng::new(seed);
        let mut maps = Vec::new();
        let mut model = BTreeMap::new();
        let mut n = 0u64;
        for _ in 0..nsegs {
            let mut m = BTreeMap::new();
            for _ in 0..per {
                let key = sort_key(rng.below(3) as u32 + 1, format!("k{:03}", rng.below(400)).as_bytes());
                n += 1;
                let e = if rng.below(5) == 0 { Entry::Tombstone } else { Entry::Row(row(n)) };
                m.insert(key, e);
            }
            for (k, e) in &m {
                match e {
                    Entry::Row(r) => {
                        model.insert(k.clone(), *r);
                    }
                    Entry::Tombstone => {
                        model.remove(k);
                    }
                }
            }
            maps.push(m);
        }
        (maps, model)
    }

    #[test]
    fn the_newest_entry_wins_and_a_tombstone_hides_older_rows() {
        let k = |s: &str| sort_key(1, s.as_bytes());
        let old = BTreeMap::from([(k("a"), Entry::Row(row(1))), (k("b"), Entry::Row(row(2))), (k("c"), Entry::Row(row(3)))]);
        let mid = BTreeMap::from([(k("a"), Entry::Row(row(10))), (k("b"), Entry::Tombstone)]);
        let new = BTreeMap::from([(k("a"), Entry::Row(row(100)))]);
        let fs = SimFs::new();
        let s = set(&fs, &[old, mid, new], Arc::new(WorkCounters::default()));
        assert_eq!(s.get(&k("a")).unwrap(), Some(row(100)), "the newest segment's row");
        assert_eq!(s.get(&k("b")).unwrap(), None, "a newer tombstone hides the older row");
        assert_eq!(s.get(&k("c")).unwrap(), Some(row(3)), "a key only the oldest segment has");
        assert_eq!(s.get(&k("d")).unwrap(), None);
        let all: Vec<_> = s.range(&sort_key(1, b""), None, 100).unwrap().into_iter().map(|(_, m)| m).collect();
        assert_eq!(all, vec![row(100), row(3)]);
    }

    #[test]
    fn gets_and_ranges_agree_with_the_model_over_seeds() {
        for seed in 0..40 {
            let (maps, model) = random(seed, 1 + (seed % 6) as usize, 300);
            let fs = SimFs::new();
            let s = set(&fs, &maps, Arc::new(WorkCounters::default()));
            for b in 1..=3u32 {
                for i in (0..400).step_by(7) {
                    let key = sort_key(b, format!("k{:03}", i).as_bytes());
                    assert_eq!(s.get(&key).unwrap(), model.get(&key).copied(), "seed {} key {:?}", seed, key);
                }
            }
            let mut rng = SimRng::new(seed ^ 0xFEED);
            for _ in 0..20 {
                let b = rng.below(3) as u32 + 1;
                let start = sort_key(b, format!("k{:03}", rng.below(400)).as_bytes());
                let end = if rng.below(2) == 0 { None } else { Some(sort_key(b + rng.below(2) as u32, format!("k{:03}", rng.below(400)).as_bytes())) };
                let limit = rng.below(150) as usize + 1;
                let want: Vec<_> = model
                    .range(start.clone()..)
                    .take_while(|(k, _)| end.as_ref().is_none_or(|e| *k < e))
                    .take(limit)
                    .map(|(k, m)| (k.clone(), *m))
                    .collect();
                assert_eq!(s.range(&start, end.as_deref(), limit).unwrap(), want, "seed {} range", seed);
            }
        }
    }

    #[test]
    fn listing_one_bucket_stays_in_that_bucket() {
        let (maps, model) = random(99, 4, 400);
        let fs = SimFs::new();
        let s = set(&fs, &maps, Arc::new(WorkCounters::default()));
        let got = s.range(&sort_key(2, b""), Some(&sort_key(3, b"")), usize::MAX).unwrap();
        assert!(!got.is_empty());
        assert!(got.iter().all(|(k, _)| k[..4] == 2u32.to_be_bytes()), "only bucket 2");
        assert_eq!(got.len(), model.keys().filter(|k| k[..4] == 2u32.to_be_bytes()).count());
    }

    #[test]
    fn block_reads_are_counted_and_the_bloom_filters_spare_most_of_them() {
        let (maps, _) = random(7, 10, 300);
        let work = Arc::new(WorkCounters::default());
        let fs = SimFs::new();
        let s = set(&fs, &maps, Arc::clone(&work));
        // A key of the newest segment: exactly one block read.
        let newest = maps.last().unwrap().iter().find(|(_, e)| matches!(e, Entry::Row(_))).unwrap().0.clone();
        let before = work.snapshot();
        assert!(s.get(&newest).unwrap().is_some());
        assert_eq!(work.snapshot().since(&before).disk_reads, 1);
        // 10,000 absent keys across 10 segments: only Bloom false positives read.
        let before = work.snapshot();
        for i in 0..10_000u32 {
            assert_eq!(s.get(&sort_key(2, format!("absent-{}", i).as_bytes())).unwrap(), None);
        }
        let reads = work.snapshot().since(&before).disk_reads;
        assert!(reads < 3_000, "reads for 10,000 absent keys over 10 segments: {}", reads);
    }

    #[test]
    fn a_second_segment_with_the_same_sequence_number_is_refused() {
        let fs = SimFs::new();
        write_segment(&fs, &path(5), 5, 0, &[]).unwrap();
        let mut s = SegmentSet::new(Arc::new(WorkCounters::default()));
        s.add(Segment::open(&fs, &path(5)).unwrap(), 0).unwrap();
        assert!(s.add(Segment::open(&fs, &path(5)).unwrap(), 0).is_err());
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn a_damaged_block_is_an_error_for_get_and_range() {
        let (maps, _) = random(3, 2, 300);
        let fs = SimFs::new();
        let s = set(&fs, &maps, Arc::new(WorkCounters::default()));
        let f = fs.file(&path(2)).unwrap();
        let mut b = [0u8; 1];
        f.read_at(4096 + 50, &mut b).unwrap();
        f.write_at(4096 + 50, &[b[0] ^ 0x5A]).unwrap();
        let first_key = maps[1].keys().next().unwrap().clone();
        assert!(s.get(&first_key).is_err(), "never a wrong or missing row");
        assert!(s.range(&sort_key(0, b""), None, usize::MAX).is_err());
    }

    #[test]
    fn a_level_refuses_overlapping_and_empty_segments_and_finds_each_key_in_one() {
        let fs = SimFs::new();
        let seg = |seq: u64, keys: &[&str]| {
            let es: Vec<SegEntry> = keys.iter().map(|k| SegEntry { key: sort_key(1, k.as_bytes()), entry: Entry::Row(row(seq)) }).collect();
            write_segment(&fs, &path(seq), seq, 0, &es).unwrap();
            Segment::open(&fs, &path(seq)).unwrap()
        };
        let work = Arc::new(WorkCounters::default());
        let mut s = SegmentSet::new(Arc::clone(&work));
        s.add(seg(1, &["a", "c"]), 1).unwrap();
        s.add(seg(2, &["m", "p"]), 1).unwrap();
        assert!(s.add(seg(3, &["b", "d"]), 1).is_err(), "overlaps segment 1");
        assert!(s.add(seg(4, &["p", "q"]), 1).is_err(), "shares a key with segment 2");
        assert!(s.add(seg(5, &[]), 2).is_err(), "an empty segment below level 0");
        assert!(s.add(seg(6, &["x"]), MAX_LEVEL + 1).is_err());
        s.add(seg(7, &["d", "k"]), 1).unwrap();
        s.add(seg(8, &["c"]), 0).unwrap();
        // Level 0 wins over level 1; in level 1 one segment is read per key.
        let before = work.snapshot();
        assert_eq!(s.get(&sort_key(1, b"c")).unwrap(), Some(row(8)));
        assert_eq!(s.get(&sort_key(1, b"k")).unwrap(), Some(row(7)));
        assert_eq!(s.get(&sort_key(1, b"p")).unwrap(), Some(row(2)));
        assert_eq!(s.get(&sort_key(1, b"e")).unwrap(), None);
        assert_eq!(s.get(&sort_key(1, b"z")).unwrap(), None);
        assert!(work.snapshot().since(&before).disk_reads <= 5, "at most one read a lookup here");
        let keys: Vec<_> = s.range(&sort_key(1, b""), None, 100).unwrap().into_iter().map(|(k, m)| (k[4..].to_vec(), m.key_hash)).collect();
        assert_eq!(keys, vec![(b"a".to_vec(), 1), (b"c".to_vec(), 8), (b"d".to_vec(), 7), (b"k".to_vec(), 7), (b"m".to_vec(), 2), (b"p".to_vec(), 2)]);
        assert_eq!(s.range(&sort_key(1, b"e"), Some(&sort_key(1, b"n")), 100).unwrap().len(), 2, "k and m");
        assert!(s.remove(7).is_some() && s.remove(7).is_none());
        assert_eq!(s.get(&sort_key(1, b"k")).unwrap(), None);
        assert_eq!(s.levels()[1].iter().map(|i| i.seq).collect::<Vec<_>>(), vec![1, 2]);
    }
}
