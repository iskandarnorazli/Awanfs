//! The spill index: object rows past the in-memory hot set (#13), kept in metadata segments
//! (#26 stage 6, decision 59) in place of redb. Writes go to a memtable; when it holds
//! `cache_bytes / 4` it is flushed to a level-0 segment and compacted (decision 68). Lookups
//! read the memtable, then the segments (`segment_set.rs`): a key the Bloom filters reject costs
//! no read, a present one about one 4 KiB read.
//!
//! The server empties the index at boot and rebuilds it from the volume replay (`clear`), as
//! it did with redb: the volumes are the record. Booting from the manifest's checkpoint instead
//! moved to the partitioned store, a checkpoint per partition (#322; owner decision on #26,
//! 2026-11-20).

use crate::manifest::{MetaStore, ReplayFrom};
use crate::meta_compaction::Policy;
use crate::meta_index::ObjectMeta;
use crate::segment::{sort_key, Entry, SegEntry, SegmentError};
use crate::vfile::{DiskFs, VolFs};
use crate::work::WorkCounters;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub type SpillError = SegmentError;

/// The spill index's share of the memory budget's cache half when no budget is set (#13).
pub const SPILL_CACHE_BYTES: usize = 64 << 20;

/// What one memtable entry costs in heap besides its key: the 64-byte row and the map's node
/// share (an estimate: `BTreeMap` keeps up to 11 entries per node).
const ENTRY_OVERHEAD: usize = 64 + 48;

struct State {
    mem: BTreeMap<Vec<u8>, Entry>,
    mem_bytes: usize,
    /// `None` only while `clear` replaces the files.
    store: Option<MetaStore<DiskFs>>,
}

#[derive(Clone)]
pub struct SpillIndex {
    state: Arc<RwLock<State>>,
    dir: PathBuf,
    mem_limit: usize,
    policy: Policy,
    work: Arc<WorkCounters>,
}

fn now_ns() -> u64 {
    use crate::clock::Clock;
    crate::clock::SystemClock.now_ns()
}

/// The smallest sort key after every key that starts with `bucket || prefix`, or `None` when
/// there is none.
fn prefix_end(bucket_id: u32, prefix: &[u8]) -> Option<Vec<u8>> {
    let mut k = sort_key(bucket_id, prefix);
    while let Some(last) = k.pop() {
        if last < 0xFF {
            k.push(last + 1);
            return Some(k);
        }
    }
    None
}

impl SpillIndex {
    /// Opens the index whose files live under `dir/meta`.
    pub fn open(dir: &Path) -> Result<Self, SpillError> {
        Self::open_with_cache_size(dir, SPILL_CACHE_BYTES)
    }

    /// As `open`, with `cache_bytes` of heap for the memtable (a quarter of it) and the
    /// segments' filters and fences.
    pub fn open_with_cache_size(dir: &Path, cache_bytes: usize) -> Result<Self, SpillError> {
        Self::open_with(dir, cache_bytes, Policy::default())
    }

    pub fn open_with(dir: &Path, cache_bytes: usize, policy: Policy) -> Result<Self, SpillError> {
        let _heap_guard = crate::memory::BootHeapGuard::enter(crate::memory::BootHeapPart::SpillIndex);
        std::fs::create_dir_all(dir.join("meta"))?;
        let work = Arc::new(WorkCounters::default());
        let store = MetaStore::open(Arc::new(DiskFs), dir, Arc::clone(&work))?;
        Ok(SpillIndex {
            state: Arc::new(RwLock::new(State { mem: BTreeMap::new(), mem_bytes: 0, store: Some(store) })),
            dir: dir.to_path_buf(),
            // A quarter, not half (run 44522807804, 10M keys): at half, building peaked at 73 MiB
            // RSS on Linux against redb's 66; at a quarter, 56 MiB (Windows 59 against 87), with
            // 236,000 inserts/s on Linux and 119,000 on Windows against 2,121 and 1,231 for redb.
            // At most 32 MiB, so a flush always fits one segment.
            mem_limit: (cache_bytes / 4).clamp(64 << 10, 32 << 20),
            policy,
            work,
        })
    }

    /// The counters the segments' block reads go to.
    pub fn work(&self) -> Arc<WorkCounters> {
        Arc::clone(&self.work)
    }

    /// Empties the index: the memtable, the segments and the manifest. The server does this at
    /// boot and rebuilds the index from the volume replay.
    pub fn clear(&self) -> Result<(), SpillError> {
        let _heap_guard = crate::memory::BootHeapGuard::enter(crate::memory::BootHeapPart::SpillIndex);
        let mut st = self.state.write();
        st.mem.clear();
        st.mem_bytes = 0;
        // Close every segment before deleting it: Windows refuses to delete an open file.
        st.store = None;
        let fs = DiskFs;
        for p in fs.list(&self.dir.join("meta"))? {
            fs.remove(&p)?;
        }
        st.store = Some(MetaStore::open(Arc::new(DiskFs), &self.dir, Arc::clone(&self.work))?);
        Ok(())
    }

    fn insert(&self, st: &mut State, key: Vec<u8>, e: Entry) -> Result<(), SpillError> {
        let add = key.len() + ENTRY_OVERHEAD;
        if st.mem.insert(key, e).is_none() {
            st.mem_bytes += add;
        }
        if st.mem_bytes >= self.mem_limit {
            self.flush_locked(st)?;
        }
        Ok(())
    }

    /// Writes the memtable to a segment, then compacts until no level is over its limit. The
    /// memtable is emptied only once its segment is named by a durable manifest.
    fn flush_locked(&self, st: &mut State) -> Result<(), SpillError> {
        if st.mem.is_empty() {
            return Ok(());
        }
        let now = now_ns();
        let State { mem, mem_bytes, store } = st;
        let store = store.as_mut().expect("open");
        // Streamed: one key copied at a time, not the memtable a second time.
        store.flush_from(mem.iter().map(|(k, e)| SegEntry { key: k.clone(), entry: *e }), ReplayFrom::default(), now)?;
        mem.clear();
        *mem_bytes = 0;
        store.compact_all(&self.policy, now)?;
        Ok(())
    }

    /// Flushes the memtable now (a benchmark's build, before it reopens the index).
    pub fn flush(&self) -> Result<(), SpillError> {
        let _heap_guard = crate::memory::BootHeapGuard::enter(crate::memory::BootHeapPart::SpillIndex);
        let mut st = self.state.write();
        self.flush_locked(&mut st)
    }

    pub fn put(&self, bucket_id: u32, key: &[u8], meta: &ObjectMeta) -> Result<(), SpillError> {
        let _heap_guard = crate::memory::BootHeapGuard::enter(crate::memory::BootHeapPart::SpillIndex);
        let mut st = self.state.write();
        self.insert(&mut st, sort_key(bucket_id, key), Entry::Row(*meta))
    }

    fn get_locked(st: &State, k: &[u8]) -> Result<Option<ObjectMeta>, SpillError> {
        match st.mem.get(k) {
            Some(Entry::Row(m)) => Ok(Some(*m)),
            Some(Entry::Tombstone) => Ok(None),
            None => st.store.as_ref().expect("open").segments().get(k),
        }
    }

    pub fn get(&self, bucket_id: u32, key: &[u8]) -> Result<Option<ObjectMeta>, SpillError> {
        let _heap_guard = crate::memory::BootHeapGuard::enter(crate::memory::BootHeapPart::SpillIndex);
        let st = self.state.read();
        Self::get_locked(&st, &sort_key(bucket_id, key))
    }

    /// Deletes the key; returns whether it was there.
    pub fn delete(&self, bucket_id: u32, key: &[u8]) -> Result<bool, SpillError> {
        let _heap_guard = crate::memory::BootHeapGuard::enter(crate::memory::BootHeapPart::SpillIndex);
        let k = sort_key(bucket_id, key);
        let mut st = self.state.write();
        if Self::get_locked(&st, &k)?.is_none() {
            return Ok(false);
        }
        self.insert(&mut st, k, Entry::Tombstone)?;
        Ok(true)
    }

    /// Up to `limit` rows of the bucket whose keys start with `prefix`, after `start_after`
    /// (exclusive) when given, in key order, with their keys (without the bucket id).
    pub fn scan_prefix(&self, bucket_id: u32, prefix: &[u8], start_after: Option<&[u8]>, limit: usize) -> Result<Vec<(Vec<u8>, ObjectMeta)>, SpillError> {
        let mut start = sort_key(bucket_id, prefix);
        if let Some(sa) = start_after {
            let mut after = sort_key(bucket_id, sa);
            after.push(0); // the smallest key after `sa`
            start = start.max(after);
        }
        let end = prefix_end(bucket_id, prefix);
        let st = self.state.read();
        let segs = st.store.as_ref().expect("open").segments();
        let mut mem = st.mem.range(start.clone()..).take_while(|(k, _)| end.as_ref().is_none_or(|e| *k < e)).peekable();
        let batch = limit.clamp(64, 4096);
        let mut seg_from = start;
        let mut seg_buf: std::collections::VecDeque<(Vec<u8>, ObjectMeta)> = Default::default();
        let mut seg_done = false;
        let mut out = Vec::new();
        while out.len() < limit {
            if seg_buf.is_empty() && !seg_done {
                let b = segs.range(&seg_from, end.as_deref(), batch)?;
                seg_done = b.len() < batch;
                if let Some((last, _)) = b.last() {
                    seg_from = last.clone();
                    seg_from.push(0);
                }
                seg_buf.extend(b);
            }
            use std::cmp::Ordering;
            let ord = match (mem.peek(), seg_buf.front()) {
                (None, None) => break,
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some((mk, _)), Some((sk, _))) => mk.as_slice().cmp(sk.as_slice()),
            };
            if ord == Ordering::Equal {
                seg_buf.pop_front(); // the memtable's entry is newer
            }
            if ord != Ordering::Greater {
                let (k, e) = mem.next().expect("peeked");
                if let Entry::Row(m) = e {
                    out.push((k[4..].to_vec(), *m));
                }
            } else {
                let (mut k, m) = seg_buf.pop_front().expect("checked");
                k.drain(..4); // the bucket id; in place, no new allocation
                out.push((k, m));
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::SimRng;

    fn dir(name: &str) -> PathBuf {
        static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let d = std::env::temp_dir().join(format!("awanfs_spill_{}_{}_{}", name, std::process::id(), n));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    fn row(i: u64) -> ObjectMeta {
        ObjectMeta { key_hash: i, bucket_flags: 1 << 32 | 1, vol_offset: i * 4096, size: i, etag_hi: i, etag_lo: !i, created_at: i, key_slice: 0 }
    }

    #[test]
    fn prefix_ends_are_the_next_key_after_the_prefix() {
        assert_eq!(prefix_end(1, b"ab"), Some(sort_key(1, b"ac")));
        assert_eq!(prefix_end(1, b"a\xff"), Some(sort_key(1, b"b")));
        assert_eq!(prefix_end(1, b""), Some(sort_key(2, b"")));
        assert_eq!(prefix_end(u32::MAX, b"\xff"), None);
    }

    /// #26's gate, counted by the work counters through the memtable and the levels: a key in
    /// the memtable costs no read; across flushed and compacted segments a present key costs at
    /// most 1.1 block reads on average and an absent one at most 0.15 (the lines the 10M-key
    /// benchmark checks, `measurements/2026-11-19-tiered-metadata.md`).
    #[test]
    fn lookups_cost_no_read_in_the_memtable_about_one_for_a_present_key_and_almost_none_for_an_absent_one() {
        let d = dir("reads");
        // A 64 KiB memtable: 60,000 rows flush and compact into several levels.
        let s = SpillIndex::open_with_cache_size(&d, 256 << 10).unwrap();
        let n = 60_000u64;
        let key = |i: u64| format!("obj/{:08}", i).into_bytes();
        for i in 0..n {
            s.put(1, &key(i), &row(i)).unwrap();
        }
        s.flush().unwrap();
        let work = s.work();
        let reads = || work.snapshot().disk_reads;

        let (before, mut present) = (reads(), 0u64);
        for i in (0..n).step_by(7) {
            assert_eq!(s.get(1, &key(i)).unwrap(), Some(row(i)));
            present += 1;
        }
        let per_present = (reads() - before) as f64 / present as f64;
        assert!((1.0..=1.1).contains(&per_present), "{:.3} reads per present lookup", per_present);

        // Absent keys between the stored ones, so every lookup is inside a segment's key range
        // and only the Bloom filters can spare the read.
        let before = reads();
        for i in 0..n {
            assert_eq!(s.get(1, format!("obj/{:08}~", i).as_bytes()).unwrap(), None);
        }
        let per_absent = (reads() - before) as f64 / n as f64;
        assert!(per_absent <= 0.15, "{:.3} reads per absent lookup", per_absent);
        eprintln!("reads per present lookup {:.3}, per absent lookup {:.3}", per_present, per_absent);

        // Rows still in the memtable are answered without a read.
        for i in n..n + 100 {
            s.put(1, &key(i), &row(i)).unwrap();
        }
        let before = reads();
        for i in n..n + 100 {
            assert_eq!(s.get(1, &key(i)).unwrap(), Some(row(i)));
        }
        assert_eq!(reads() - before, 0, "a memtable row costs no read");
        let _ = std::fs::remove_dir_all(&d);
    }

    /// Random puts, deletes, flushes, scans and clears against a BTreeMap, with a memtable
    /// small enough that most rows sit in compacted segments.
    #[test]
    fn the_index_agrees_with_a_map_through_flushes_and_compactions() {
        let d = dir("model");
        let policy = Policy { l0_trigger: 2, level1_bytes: 8 * 4096, ratio: 3, segment_bytes: 8 * 4096, max_level: 4 };
        let s = SpillIndex::open_with(&d, 128 << 10, policy).unwrap();
        let mut model: BTreeMap<(u32, Vec<u8>), ObjectMeta> = BTreeMap::new();
        let mut rng = SimRng::new(208);
        let key = |rng: &mut SimRng| -> (u32, Vec<u8>) {
            let b = rng.below(2) as u32 + 1;
            let dirs = ["a/", "a/b/", "b/", "\u{ff}x/", ""];
            (b, format!("{}{:05}", dirs[rng.below(5) as usize], rng.below(3000)).into_bytes())
        };
        for i in 0..30_000u64 {
            let (b, k) = key(&mut rng);
            match rng.below(10) {
                0..=6 => {
                    s.put(b, &k, &row(i)).unwrap();
                    model.insert((b, k), row(i));
                }
                7 | 8 => {
                    assert_eq!(s.delete(b, &k).unwrap(), model.remove(&(b, k.clone())).is_some(), "delete {:?}", k);
                }
                _ => {
                    assert_eq!(s.get(b, &k).unwrap(), model.get(&(b, k.clone())).copied(), "get {:?}", k);
                }
            }
            if i % 1000 == 999 {
                for prefix in [&b""[..], b"a/", b"a/b/", b"\xc3\xbf"] {
                    let b = rng.below(2) as u32 + 1;
                    let after: Option<Vec<u8>> = if rng.below(2) == 0 { None } else { Some(key(&mut rng).1) };
                    let limit = rng.below(1500) as usize + 1;
                    let want: Vec<(Vec<u8>, ObjectMeta)> = model
                        .range((b, prefix.to_vec())..)
                        .take_while(|((mb, k), _)| *mb == b && k.starts_with(prefix))
                        .filter(|((_, k), _)| after.as_ref().is_none_or(|a| k > a))
                        .take(limit)
                        .map(|((_, k), m)| (k.clone(), *m))
                        .collect();
                    assert_eq!(s.scan_prefix(b, prefix, after.as_deref(), limit).unwrap(), want, "scan {:?} after {:?} limit {}", prefix, after, limit);
                }
            }
        }
        assert!(s.state.read().store.as_ref().unwrap().segments().len() > 1, "rows reached the segments");
        // A reopen sees what was flushed; clear empties everything.
        s.flush().unwrap();
        let again = SpillIndex::open_with(&d, 128 << 10, Policy::default()).unwrap();
        for ((b, k), m) in model.iter().step_by(7) {
            assert_eq!(again.get(*b, k).unwrap(), Some(*m));
        }
        drop(again);
        s.clear().unwrap();
        assert!(model.keys().all(|(b, k)| s.get(*b, k).unwrap().is_none()));
        assert_eq!(DiskFs.list(&d.join("meta")).unwrap(), Vec::<PathBuf>::new());
        let _ = std::fs::remove_dir_all(&d);
    }
}
