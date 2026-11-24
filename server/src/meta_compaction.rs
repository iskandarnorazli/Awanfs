//! Which metadata segments to compact next (#26 stage 5, decision 68): a pure function of the
//! levels, so the rules `verification/tla/Levels.tla` checks are visible in one place.
//! `MetaStore::compact` (in `manifest.rs`) carries the job out.
//!
//! - Level 0 is compacted when it holds `l0_trigger` segments: all of them, with the level-1
//!   segments that overlap their key span, into level 1 (`levels_l0_subset` is the bug of taking
//!   fewer).
//! - Level `i` from 1 is compacted when it holds more than `level1_bytes * ratio^(i-1)` bytes:
//!   one segment, with the level-`i+1` segments that overlap it (`levels_partial_overlap`). The
//!   segment chosen is the one whose overlap below is smallest for its size (RocksDB's
//!   "min overlapping ratio"), so a compaction rewrites as little as it can.
//! - Tombstones are dropped only when no deeper level overlaps the job's key range
//!   (`levels_early_tombstones`).
//! - A single segment with nothing below it to merge, whose tombstones must stay, moves down a
//!   level by a manifest change alone.

use crate::segment::MAX_SEGMENT_BYTES;
use crate::segment_set::{SegInfo, MAX_LEVEL};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Policy {
    pub l0_trigger: usize,
    pub level1_bytes: u64,
    pub ratio: u64,
    /// The largest segment a compaction writes.
    pub segment_bytes: u64,
    /// The deepest level a compaction writes to (at most `MAX_LEVEL`).
    pub max_level: u8,
}

impl Default for Policy {
    fn default() -> Self {
        Policy { l0_trigger: 4, level1_bytes: 4 * MAX_SEGMENT_BYTES, ratio: 10, segment_bytes: MAX_SEGMENT_BYTES, max_level: MAX_LEVEL }
    }
}

impl Policy {
    /// The size above which level `i` (from 1) is compacted.
    pub fn level_target(&self, i: u8) -> u64 {
        (1..i).fold(self.level1_bytes, |t, _| t.saturating_mul(self.ratio.max(2)))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Job {
    pub from: u8,
    /// The segments taken from `from`: every one for level 0, one otherwise.
    pub inputs: Vec<u64>,
    /// The segments of level `from + 1` that overlap the inputs.
    pub targets: Vec<u64>,
    pub drop_tombstones: bool,
}

impl Job {
    pub fn to(&self) -> u8 {
        self.from + 1
    }

    /// Done by changing the segment's level in the manifest; nothing is rewritten.
    pub fn is_move(&self) -> bool {
        self.inputs.len() == 1 && self.targets.is_empty() && !self.drop_tombstones
    }
}

fn overlaps(r: &Option<(Vec<u8>, Vec<u8>)>, lo: &[u8], hi: &[u8]) -> bool {
    r.as_ref().is_some_and(|(a, b)| a.as_slice() <= hi && lo <= b.as_slice())
}

/// The smallest range holding every given range.
fn span<'a>(ranges: impl Iterator<Item = &'a Option<(Vec<u8>, Vec<u8>)>>) -> Option<(Vec<u8>, Vec<u8>)> {
    ranges.flatten().fold(None, |acc: Option<(Vec<u8>, Vec<u8>)>, (a, b)| match acc {
        None => Some((a.clone(), b.clone())),
        Some((lo, hi)) => Some((lo.min(a.clone()), hi.max(b.clone()))),
    })
}

fn job(levels: &[Vec<SegInfo>], from: u8, inputs: &[&SegInfo]) -> Job {
    let to = from as usize + 1;
    let in_span = span(inputs.iter().map(|s| &s.range));
    let targets: Vec<&SegInfo> = match &in_span {
        Some((lo, hi)) => levels.get(to).map(|l| l.iter().filter(|s| overlaps(&s.range, lo, hi)).collect()).unwrap_or_default(),
        None => Vec::new(),
    };
    let all = span(inputs.iter().chain(targets.iter()).map(|s| &s.range));
    let drop_tombstones = match &all {
        Some((lo, hi)) => levels.iter().skip(to + 1).flatten().all(|s| !overlaps(&s.range, lo, hi)),
        None => true,
    };
    Job { from, inputs: inputs.iter().map(|s| s.seq).collect(), targets: targets.iter().map(|s| s.seq).collect(), drop_tombstones }
}

/// The next compaction, if any level is over its limit. `levels[0]` is level 0.
pub fn pick(levels: &[Vec<SegInfo>], p: &Policy) -> Option<Job> {
    let max_level = p.max_level.clamp(1, MAX_LEVEL) as usize;
    let l0 = levels.first()?;
    if !l0.is_empty() && l0.len() >= p.l0_trigger.max(1) {
        return Some(job(levels, 0, &l0.iter().collect::<Vec<_>>()));
    }
    for i in 1..max_level {
        let Some(lvl) = levels.get(i) else { break };
        let bytes: u64 = lvl.iter().map(|s| s.bytes).sum();
        if bytes <= p.level_target(i as u8) {
            continue;
        }
        let below = levels.get(i + 1).map(Vec::as_slice).unwrap_or(&[]);
        let overlap = |s: &SegInfo| -> u64 {
            let Some((lo, hi)) = &s.range else { return 0 };
            below.iter().filter(|t| overlaps(&t.range, lo, hi)).map(|t| t.bytes).sum()
        };
        // Smallest overlap per byte; ties to the lowest sequence number, so the choice is
        // deterministic.
        let chosen = lvl.iter().min_by(|a, b| {
            let (x, y) = (overlap(a) as u128 * b.bytes.max(1) as u128, overlap(b) as u128 * a.bytes.max(1) as u128);
            x.cmp(&y).then(a.seq.cmp(&b.seq))
        })?;
        return Some(job(levels, i as u8, &[chosen]));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg(seq: u64, lo: &str, hi: &str, bytes: u64) -> SegInfo {
        SegInfo { seq, range: Some((lo.as_bytes().to_vec(), hi.as_bytes().to_vec())), bytes }
    }

    fn policy() -> Policy {
        Policy { l0_trigger: 3, level1_bytes: 100, ratio: 10, segment_bytes: 64 << 10, max_level: 3 }
    }

    #[test]
    fn level_0_compacts_whole_with_every_overlapping_level_1_segment() {
        let levels = vec![
            vec![seg(9, "d", "f", 10), seg(8, "a", "b", 10), seg(7, "m", "n", 10)],
            vec![seg(1, "a", "a", 10), seg(2, "c", "e", 10), seg(3, "g", "l", 10), seg(4, "o", "z", 10)],
            vec![],
            vec![],
        ];
        let j = pick(&levels, &policy()).unwrap();
        assert_eq!((j.from, j.to()), (0, 1));
        assert_eq!(j.inputs, vec![9, 8, 7], "every level-0 segment");
        // The level-0 span is a..n: g..l lies inside it though no level-0 range touches it.
        assert_eq!(j.targets, vec![1, 2, 3]);
        assert!(j.drop_tombstones, "nothing deeper");
        assert_eq!(pick(&levels[..1].iter().map(|l| l[..2].to_vec()).collect::<Vec<_>>(), &policy()), None, "two level-0 segments: under the trigger");
    }

    #[test]
    fn a_full_level_gives_one_segment_and_keeps_tombstones_above_deeper_data() {
        let levels = vec![
            vec![],
            vec![seg(1, "a", "f", 60), seg(2, "g", "p", 60)],
            vec![seg(3, "a", "c", 500), seg(4, "d", "h", 50), seg(5, "q", "z", 50)],
            vec![seg(6, "e", "e", 10)],
        ];
        let j = pick(&levels, &policy()).unwrap();
        // Segment 1 overlaps 550 bytes below, segment 2 only 50: segment 2 is chosen.
        assert_eq!((j.from, j.inputs.clone(), j.targets.clone()), (1, vec![2], vec![4]));
        assert!(!j.drop_tombstones, "level 3 holds e, inside d..p");
        let mut l = levels.clone();
        l[3].clear();
        assert!(pick(&l, &policy()).unwrap().drop_tombstones);
        // Level 2's limit is 1,000 bytes: 600 is under it, and level 3 is the last.
        l[1].clear();
        assert_eq!(pick(&l, &policy()), None);
    }

    #[test]
    fn a_segment_with_nothing_below_moves_only_when_its_tombstones_must_stay() {
        let levels = vec![vec![], vec![seg(1, "a", "c", 200)], vec![], vec![seg(2, "b", "b", 1)]];
        let j = pick(&levels, &policy()).unwrap();
        assert!(j.targets.is_empty() && !j.drop_tombstones && j.is_move());
        let levels = vec![vec![], vec![seg(1, "a", "c", 200)], vec![], vec![]];
        let j = pick(&levels, &policy()).unwrap();
        assert!(j.drop_tombstones && !j.is_move(), "rewritten, so its tombstones go");
    }

    #[test]
    fn level_targets_grow_by_the_ratio() {
        let p = policy();
        assert_eq!((p.level_target(1), p.level_target(2), p.level_target(3)), (100, 1_000, 10_000));
        assert_eq!(Policy { ratio: 10, level1_bytes: u64::MAX / 2, ..p }.level_target(5), u64::MAX, "saturates");
    }
}
