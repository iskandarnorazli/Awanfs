//! Lookups across several metadata segments (#26 stage 3, decision 59).
//!
//! Segments are ordered by sequence number, newest first. For one key, the newest segment that
//! has an entry decides: a row is the answer, a tombstone means the key is absent. A range scan
//! merges every segment's entries in sort-key order with the same rule. Each block read counts
//! in the server's work counters.
//!
//! Nothing in the server uses this yet: stage 4 adds the manifest, stage 6 the switch from redb.

use crate::meta_index::ObjectMeta;
use crate::segment::{Cursor, Entry, SegEntry, Segment, SegmentError};
use crate::vfile::VolFile;
use crate::work::WorkCounters;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

pub struct SegmentSet<F: VolFile> {
    /// Newest (highest sequence number) first.
    segs: Vec<Segment<F>>,
    work: Arc<WorkCounters>,
}

impl<F: VolFile> SegmentSet<F> {
    pub fn new(work: Arc<WorkCounters>) -> Self {
        SegmentSet { segs: Vec::new(), work }
    }

    /// Adds an open segment. Two segments with one sequence number would make "newest wins"
    /// ambiguous, so the second is refused.
    pub fn add(&mut self, seg: Segment<F>) -> Result<(), SegmentError> {
        if self.segs.iter().any(|s| s.seq == seg.seq) {
            return Err(SegmentError::Corrupt(format!("two segments with sequence number {}", seg.seq)));
        }
        let seg = seg.with_work(Arc::clone(&self.work));
        let at = self.segs.partition_point(|s| s.seq > seg.seq);
        self.segs.insert(at, seg);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.segs.len()
    }

    /// The counters this set's block reads go to.
    pub fn work(&self) -> Arc<WorkCounters> {
        Arc::clone(&self.work)
    }

    pub fn is_empty(&self) -> bool {
        self.segs.is_empty()
    }

    /// The row for `key`, or `None` if no segment has it or the newest entry is a tombstone.
    pub fn get(&self, key: &[u8]) -> Result<Option<ObjectMeta>, SegmentError> {
        for s in &self.segs {
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
        let mut cursors: Vec<Cursor<'_, F>> = self.segs.iter().map(|s| s.scan_from(start)).collect();
        // Min-heap on (key, rank): rank 0 is the newest segment, so on equal keys it comes first.
        let mut heap: BinaryHeap<Reverse<(Vec<u8>, usize)>> = BinaryHeap::new();
        let mut heads: Vec<Option<SegEntry>> = Vec::with_capacity(cursors.len());
        for (rank, c) in cursors.iter_mut().enumerate() {
            let e = c.next_entry()?;
            if let Some(e) = &e {
                heap.push(Reverse((e.key.clone(), rank)));
            }
            heads.push(e);
        }
        while let Some(Reverse((key, rank))) = heap.pop() {
            if end.is_some_and(|end| key.as_slice() >= end) {
                break;
            }
            let winner = heads[rank].take().expect("a heap entry has its head");
            // Older segments' entries for the same key are hidden: advance past them.
            while let Some(Reverse((k, r))) = heap.peek() {
                if *k != key {
                    break;
                }
                let r = *r;
                heap.pop();
                heads[r] = cursors[r].next_entry()?;
                if let Some(e) = &heads[r] {
                    heap.push(Reverse((e.key.clone(), r)));
                }
            }
            heads[rank] = cursors[rank].next_entry()?;
            if let Some(e) = &heads[rank] {
                heap.push(Reverse((e.key.clone(), rank)));
            }
            if let Entry::Row(m) = winner.entry {
                out.push((winner.key, m));
                if out.len() == limit {
                    break;
                }
            }
        }
        Ok(out)
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
            set.add(Segment::open(fs, &path(seq)).unwrap()).unwrap();
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
        s.add(Segment::open(&fs, &path(5)).unwrap()).unwrap();
        assert!(s.add(Segment::open(&fs, &path(5)).unwrap()).is_err());
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
}
