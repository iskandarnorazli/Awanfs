//! Seeded operation generator and delta-debugging shrinker for volume simulation (#213, #289).
//!
//! Pure deterministic Rust with no I/O: turns a `u64` seed into a sequence of [`SimOperation`]
//! values using [`crate::sim::SimRng`], and shrinks any failing sequence to a 1-minimal witness.

use crate::sim::{CrashMode, SimFault, SimOp, SimRng};

/// Maximum number of distinct keys (`0..KEY_SPACE`) generated in a run so operations revisit keys.
const KEY_SPACE: u64 = 8;

/// Maximum gap of non-crash operations before a `Crash` is forced ("at least one per 50").
const MAX_OPS_WITHOUT_CRASH: usize = 49;

/// Maximum gap of storage ops before a `Crash` is forced in end-to-end simulation.
const MAX_STORAGE_OPS_WITHOUT_CRASH: usize = 35;

/// Extended storage operation for end-to-end `VolumeManager<SimFs>` simulation (#213).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageOp {
    Basic(SimOperation),
    Copy { src_key: u8, dst_key: u8 },
    StreamStart { stream_id: u32, key: u8, len: u32, fill: u64 },
    StreamFinish { stream_id: u32 },
    StreamAbort { stream_id: u32 },
    MultipartPut { key: u8, parts: Vec<(u32, u64)>, fill: u64 },
    Compact { seal_if_single: bool },
    VerifyRead { key: u8 },
    ProbeJournalReadFault { fault: SimFault },
}

/// Deterministic volume span for a simulation seed: mixes small rollover-heavy spans with
/// multi-chunk (>1 MiB `STREAM_CHUNK`) spans.
pub fn storage_max_span(seed: u64) -> u64 {
    if seed % 4 == 0 {
        2 * 1024 * 1024
    } else {
        128 * 1024
    }
}

/// Which file of a simulated volume a fault is armed on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SimFileKind {
    Data,
    Journal,
}

/// One step in a deterministic volume simulation run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimOperation {
    /// Write `len` bytes, generated from `fill`, to `key` (a needle on the active volume).
    Put { key: u8, len: u32, fill: u64 },
    /// Delete `key` (a tombstone needle).
    Delete { key: u8 },
    /// Sync the volume and its journal.
    Sync,
    /// Arm one fault on the next call of an operation of the data file or the journal.
    Fault { file: SimFileKind, op: crate::sim::SimOp, fault: crate::sim::SimFault },
    /// Crash: unsynced writes are dropped or a seeded prefix lands (`crate::sim::CrashMode`),
    /// then the volume is reopened and recovered.
    Crash { mode: crate::sim::CrashMode },
}

fn random_put_len(rng: &mut SimRng) -> u32 {
    match rng.below(10) {
        // Exact 4096-byte sector multiples up to 64 KiB.
        0..=1 => ((rng.below(16) + 1) * 4096) as u32,
        // Exact 512-byte sector multiples up to 64 KiB.
        2..=3 => ((rng.below(128) + 1) * 512) as u32,
        // Small payloads (`1..=512`), covering tiny/unaligned writes.
        4..=5 => (rng.below(512) + 1) as u32,
        // Medium payloads (`1..=8192`), crossing 1-2 sector boundaries.
        6..=7 => (rng.below(8192) + 1) as u32,
        // Full `1..=65536` spread.
        _ => (rng.below(65536) + 1) as u32,
    }
}

fn random_crash_mode(rng: &mut SimRng) -> CrashMode {
    if rng.below(2) == 0 {
        CrashMode::DropUnsynced
    } else {
        CrashMode::SeededPrefix
    }
}

fn random_fault(rng: &mut SimRng) -> SimOperation {
    let file = if rng.below(2) == 0 {
        SimFileKind::Data
    } else {
        SimFileKind::Journal
    };
    let op = match rng.below(4) {
        0 => SimOp::Write,
        1 => SimOp::Read,
        2 => SimOp::SetLen,
        _ => SimOp::Sync,
    };
    let fault = if rng.below(2) == 0 {
        SimFault::Eio
    } else {
        SimFault::NoSpace
    };
    SimOperation::Fault { file, op, fault }
}

/// Deterministically generates `len` storage operations from `seed`.
pub fn generate(seed: u64, len: usize) -> Vec<SimOperation> {
    let mut rng = SimRng::new(seed);
    let mut ops = Vec::with_capacity(len);
    let mut since_crash = 0usize;

    for _ in 0..len {
        if since_crash >= MAX_OPS_WITHOUT_CRASH {
            ops.push(SimOperation::Crash { mode: random_crash_mode(&mut rng) });
            since_crash = 0;
            continue;
        }
        let op = match rng.below(100) {
            0..=49 => {
                since_crash += 1;
                SimOperation::Put {
                    key: rng.below(KEY_SPACE) as u8,
                    len: random_put_len(&mut rng),
                    fill: rng.next_u64(),
                }
            }
            50..=64 => {
                since_crash += 1;
                SimOperation::Delete { key: rng.below(KEY_SPACE) as u8 }
            }
            65..=79 => {
                since_crash += 1;
                SimOperation::Sync
            }
            80..=89 => {
                since_crash += 1;
                random_fault(&mut rng)
            }
            _ => {
                since_crash = 0;
                SimOperation::Crash { mode: random_crash_mode(&mut rng) }
            }
        };
        ops.push(op);
    }

    ops
}

fn remove_chunks(cur: &mut Vec<SimOperation>, still_fails: &mut impl FnMut(&[SimOperation]) -> bool) {
    let mut chunk_size = cur.len() / 2;
    while chunk_size >= 2 && cur.len() > 2 {
        let mut start = 0;
        while start < cur.len() {
            let end = (start + chunk_size).min(cur.len());
            if end - start < 2 || (start == 0 && end == cur.len()) {
                break;
            }
            let mut candidate = Vec::with_capacity(cur.len() - (end - start));
            candidate.extend_from_slice(&cur[..start]);
            candidate.extend_from_slice(&cur[end..]);
            if still_fails(&candidate) {
                *cur = candidate;
            } else {
                start = end;
            }
        }
        if chunk_size == 2 {
            break;
        }
        chunk_size = (chunk_size / 2).max(2);
    }
}

fn remove_single_ops(cur: &mut Vec<SimOperation>, still_fails: &mut impl FnMut(&[SimOperation]) -> bool) -> bool {
    let mut any_removed = false;
    loop {
        let mut removed_in_pass = false;
        let mut i = 0;
        while i < cur.len() {
            if cur.len() <= 1 {
                // `still_fails(&[])` was already checked at entry to `shrink`.
                break;
            }
            let mut candidate = Vec::with_capacity(cur.len() - 1);
            candidate.extend_from_slice(&cur[..i]);
            candidate.extend_from_slice(&cur[i + 1..]);
            if still_fails(&candidate) {
                *cur = candidate;
                removed_in_pass = true;
                any_removed = true;
            } else {
                i += 1;
            }
        }
        if !removed_in_pass {
            break;
        }
    }
    any_removed
}

fn shrink_put_len(
    cur: &mut [SimOperation],
    idx: usize,
    key: u8,
    orig_len: u32,
    fill: u64,
    still_fails: &mut impl FnMut(&[SimOperation]) -> bool,
) -> u32 {
    if orig_len == 0 {
        return 0;
    }
    cur[idx] = SimOperation::Put { key, len: 0, fill };
    if still_fails(cur) {
        return 0;
    }
    let mut lo = 1u32;
    let mut hi = orig_len;
    let mut best = orig_len;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        cur[idx] = SimOperation::Put { key, len: mid, fill };
        if still_fails(cur) {
            best = mid;
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    cur[idx] = SimOperation::Put { key, len: best, fill };
    best
}

fn simplify_puts(cur: &mut [SimOperation], still_fails: &mut impl FnMut(&[SimOperation]) -> bool) -> bool {
    let mut changed = false;
    for idx in 0..cur.len() {
        let SimOperation::Put { key, len: orig_len, fill: orig_fill } = cur[idx] else {
            continue;
        };
        let mut cur_len = shrink_put_len(cur, idx, key, orig_len, orig_fill, still_fails);
        if cur_len != orig_len {
            changed = true;
        }
        if orig_fill != 0 {
            cur[idx] = SimOperation::Put { key, len: cur_len, fill: 0 };
            if still_fails(cur) {
                changed = true;
                if cur_len > 0 {
                    let after_fill = shrink_put_len(cur, idx, key, cur_len, 0, still_fails);
                    if after_fill != cur_len {
                        cur_len = after_fill;
                    }
                }
            } else {
                cur[idx] = SimOperation::Put { key, len: cur_len, fill: orig_fill };
            }
        }
    }
    changed
}

/// Shrinks `ops` via delta debugging and `Put` simplification to a 1-minimal sequence
/// for which `still_fails` remains `true`.
pub fn shrink(ops: &[SimOperation], mut still_fails: impl FnMut(&[SimOperation]) -> bool) -> Vec<SimOperation> {
    if ops.is_empty() || still_fails(&[]) {
        return Vec::new();
    }
    let mut cur = ops.to_vec();
    remove_chunks(&mut cur, &mut still_fails);
    remove_single_ops(&mut cur, &mut still_fails);
    loop {
        if !simplify_puts(&mut cur, &mut still_fails) {
            break;
        }
        if !remove_single_ops(&mut cur, &mut still_fails) {
            break;
        }
    }
    cur
}

fn storage_stream_len(seed: u64, rng: &mut SimRng) -> u32 {
    if seed % 4 == 0 && rng.below(4) == 0 {
        (1 << 20) + 4096
    } else {
        random_put_len(rng)
    }
}

/// Deterministically generates `len` storage operations from `seed` covering buffered PUTs,
/// overwrites, deletes, copies, streamed PUTs (including multi-chunk >1 MiB), multipart objects,
/// online compaction, I/O faults, and crashes.
pub fn generate_storage(seed: u64, len: usize) -> Vec<StorageOp> {
    let mut rng = SimRng::new(seed ^ 0x517C_C1B7_2722_0A95);
    let mut ops = Vec::with_capacity(len);
    let mut since_crash = 0usize;
    let mut open_streams: Vec<(u32, u8)> = Vec::new();
    let mut next_stream_id = 1u32;

    for idx in 0..len {
        let force_crash = since_crash >= MAX_STORAGE_OPS_WITHOUT_CRASH || idx + 1 == len;
        if force_crash {
            ops.push(StorageOp::Basic(SimOperation::Crash { mode: random_crash_mode(&mut rng) }));
            open_streams.clear();
            since_crash = 0;
            continue;
        }

        let roll = rng.below(100);
        let op = match roll {
            // Put / overwrite (32%), biasing toward open stream keys to exercise relocation.
            0..=31 => {
                since_crash += 1;
                let key = if !open_streams.is_empty() && rng.below(2) == 0 {
                    let i = rng.below(open_streams.len() as u64) as usize;
                    open_streams[i].1
                } else {
                    rng.below(KEY_SPACE) as u8
                };
                StorageOp::Basic(SimOperation::Put {
                    key,
                    len: random_put_len(&mut rng),
                    fill: rng.next_u64(),
                })
            }
            // Delete (9%), biasing toward open stream keys as well.
            32..=40 => {
                since_crash += 1;
                let key = if !open_streams.is_empty() && rng.below(2) == 0 {
                    let i = rng.below(open_streams.len() as u64) as usize;
                    open_streams[i].1
                } else {
                    rng.below(KEY_SPACE) as u8
                };
                StorageOp::Basic(SimOperation::Delete { key })
            }
            // CopyObject (6%).
            41..=46 => {
                since_crash += 1;
                StorageOp::Copy {
                    src_key: rng.below(KEY_SPACE) as u8,
                    dst_key: rng.below(KEY_SPACE) as u8,
                }
            }
            // StreamStart (9%).
            47..=55 => {
                since_crash += 1;
                let key = rng.below(KEY_SPACE) as u8;
                let id = next_stream_id;
                next_stream_id = next_stream_id.wrapping_add(1);
                open_streams.push((id, key));
                StorageOp::StreamStart {
                    stream_id: id,
                    key,
                    len: storage_stream_len(seed, &mut rng),
                    fill: rng.next_u64(),
                }
            }
            // StreamFinish (8%) or fallback Put if no stream is open.
            56..=63 => {
                since_crash += 1;
                if open_streams.is_empty() {
                    StorageOp::Basic(SimOperation::Put {
                        key: rng.below(KEY_SPACE) as u8,
                        len: random_put_len(&mut rng),
                        fill: rng.next_u64(),
                    })
                } else {
                    let i = rng.below(open_streams.len() as u64) as usize;
                    let (id, _) = open_streams.remove(i);
                    StorageOp::StreamFinish { stream_id: id }
                }
            }
            // StreamAbort (2%) or fallback VerifyRead.
            64..=65 => {
                since_crash += 1;
                if !open_streams.is_empty() {
                    let i = rng.below(open_streams.len() as u64) as usize;
                    let (id, _) = open_streams.remove(i);
                    StorageOp::StreamAbort { stream_id: id }
                } else {
                    StorageOp::VerifyRead { key: rng.below(KEY_SPACE) as u8 }
                }
            }
            // MultipartPut (5%).
            66..=70 => {
                since_crash += 1;
                let part_count = (rng.below(3) + 1) as usize;
                let mut parts = Vec::with_capacity(part_count);
                for _ in 0..part_count {
                    let plen = (rng.below(4096) + 1) as u32;
                    parts.push((plen, rng.next_u64()));
                }
                StorageOp::MultipartPut {
                    key: rng.below(KEY_SPACE) as u8,
                    parts,
                    fill: rng.next_u64(),
                }
            }
            // Compact (6%).
            71..=76 => {
                since_crash += 1;
                StorageOp::Compact { seal_if_single: rng.below(3) == 0 }
            }
            // VerifyRead (3%).
            77..=79 => {
                since_crash += 1;
                StorageOp::VerifyRead { key: rng.below(KEY_SPACE) as u8 }
            }
            // Sync (10%).
            80..=89 => {
                since_crash += 1;
                StorageOp::Basic(SimOperation::Sync)
            }
            // Fault (7%) including ProbeJournalReadFault.
            90..=96 => {
                since_crash += 1;
                match random_fault(&mut rng) {
                    SimOperation::Fault { file: SimFileKind::Journal, op: SimOp::Read, fault } => {
                        StorageOp::ProbeJournalReadFault { fault }
                    }
                    other => StorageOp::Basic(other),
                }
            }
            // Crash (3%).
            _ => {
                open_streams.clear();
                since_crash = 0;
                StorageOp::Basic(SimOperation::Crash { mode: random_crash_mode(&mut rng) })
            }
        };
        ops.push(op);
    }

    ops
}

/// Delta-debugging shrinker over [`StorageOp`] sequences to produce a minimal failing trace.
pub fn shrink_storage(ops: &[StorageOp], mut still_fails: impl FnMut(&[StorageOp]) -> bool) -> Vec<StorageOp> {
    if ops.is_empty() || still_fails(&[]) {
        return Vec::new();
    }
    let mut cur = ops.to_vec();
    let mut chunk_size = cur.len() / 2;
    while chunk_size >= 2 && cur.len() > 2 {
        let mut start = 0;
        while start < cur.len() {
            let end = (start + chunk_size).min(cur.len());
            if end - start < 2 || (start == 0 && end == cur.len()) {
                break;
            }
            let mut candidate = Vec::with_capacity(cur.len() - (end - start));
            candidate.extend_from_slice(&cur[..start]);
            candidate.extend_from_slice(&cur[end..]);
            if still_fails(&candidate) {
                cur = candidate;
            } else {
                start = end;
            }
        }
        if chunk_size == 2 {
            break;
        }
        chunk_size = (chunk_size / 2).max(2);
    }

    loop {
        let mut removed = false;
        let mut i = 0;
        while i < cur.len() {
            if cur.len() <= 1 {
                break;
            }
            let mut candidate = Vec::with_capacity(cur.len() - 1);
            candidate.extend_from_slice(&cur[..i]);
            candidate.extend_from_slice(&cur[i + 1..]);
            if still_fails(&candidate) {
                cur = candidate;
                removed = true;
            } else {
                i += 1;
            }
        }
        if !removed {
            break;
        }
    }
    cur
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn has_put_then_delete_key3(ops: &[SimOperation]) -> bool {
        let mut saw_put = false;
        for op in ops {
            match op {
                SimOperation::Put { key: 3, .. } => saw_put = true,
                SimOperation::Delete { key: 3 } if saw_put => return true,
                _ => {}
            }
        }
        false
    }

    #[test]
    fn same_seed_same_sequence_and_different_seeds_differ() {
        assert!(generate(0, 0).is_empty());
        for seed in 0..1000u64 {
            let a = generate(seed, 50);
            let b = generate(seed, 50);
            assert_eq!(a.len(), 50, "seed {seed}");
            assert_eq!(a, b, "seed {seed} must be deterministic");
        }
        let mut different = 0usize;
        for i in 0..1000u64 {
            let a = generate(i, 50);
            let b = generate(i + 10_000, 50);
            if a != b {
                different += 1;
            }
        }
        assert!(different >= 990, "expected >= 990/1000 differing pairs, got {different}");
    }

    #[test]
    fn weights_keys_lengths_and_crash_cadence_hold() {
        let ops = generate(0x5eed_cafe, 100_000);
        assert_eq!(ops.len(), 100_000);

        let mut puts = 0usize;
        let mut deletes = 0usize;
        let mut syncs = 0usize;
        let mut faults = 0usize;
        let mut crashes = 0usize;
        let mut keys = BTreeSet::new();
        let mut has_small = false;
        let mut has_medium = false;
        let mut has_large = false;
        let mut has_512_mult = false;
        let mut has_4096_mult = false;
        let mut has_non_sector = false;
        let mut fault_files = BTreeSet::new();
        let mut fault_ops = BTreeSet::new();
        let mut fault_kinds = BTreeSet::new();
        let mut crash_modes = BTreeSet::new();

        for op in &ops {
            match *op {
                SimOperation::Put { key, len, .. } => {
                    puts += 1;
                    assert!(key < 8, "key out of 0..8: {key}");
                    keys.insert(key);
                    assert!((1..=65536).contains(&len), "len out of 1..=65536: {len}");
                    if len <= 512 {
                        has_small = true;
                    } else if len <= 8192 {
                        has_medium = true;
                    } else if len >= 32768 {
                        has_large = true;
                    }
                    if len % 4096 == 0 {
                        has_4096_mult = true;
                    } else if len % 512 == 0 {
                        has_512_mult = true;
                    } else {
                        has_non_sector = true;
                    }
                }
                SimOperation::Delete { key } => {
                    deletes += 1;
                    assert!(key < 8, "key out of 0..8: {key}");
                    keys.insert(key);
                }
                SimOperation::Sync => syncs += 1,
                SimOperation::Fault { file, op, fault } => {
                    faults += 1;
                    fault_files.insert(matches!(file, SimFileKind::Data));
                    fault_ops.insert(match op {
                        SimOp::Write => 0u8,
                        SimOp::Read => 1,
                        SimOp::SetLen => 2,
                        SimOp::Sync => 3,
                    });
                    fault_kinds.insert(matches!(fault, SimFault::Eio));
                }
                SimOperation::Crash { mode } => {
                    crashes += 1;
                    crash_modes.insert(matches!(mode, CrashMode::DropUnsynced));
                }
            }
        }

        // Within 3 percentage points of 50 / 15 / 15 / 10 / 10 over 100,000 ops.
        assert!((47_000..=53_000).contains(&puts), "puts = {puts}");
        assert!((12_000..=18_000).contains(&deletes), "deletes = {deletes}");
        assert!((12_000..=18_000).contains(&syncs), "syncs = {syncs}");
        assert!((7_000..=13_000).contains(&faults), "faults = {faults}");
        assert!((7_000..=13_000).contains(&crashes), "crashes = {crashes}");

        assert!(keys.len() <= 8 && keys.len() == 8, "distinct keys = {:?}", keys);
        assert!(has_small && has_medium && has_large, "length spread missing: small={has_small} med={has_medium} large={has_large}");
        assert!(has_512_mult && has_4096_mult && has_non_sector, "sector coverage missing: 512={has_512_mult} 4096={has_4096_mult} non={has_non_sector}");
        assert_eq!(fault_files.len(), 2);
        assert_eq!(fault_ops.len(), 4);
        assert_eq!(fault_kinds.len(), 2);
        assert_eq!(crash_modes.len(), 2);

        for (idx, window) in ops.windows(50).enumerate() {
            assert!(
                window.iter().any(|op| matches!(op, SimOperation::Crash { .. })),
                "window starting at {idx} of 50 ops had no Crash"
            );
        }
        for seed in 0..20u64 {
            let short = generate(seed, 50);
            assert!(
                short.iter().any(|op| matches!(op, SimOperation::Crash { .. })),
                "50-op run for seed {seed} had no Crash"
            );
        }
    }

    #[test]
    fn shrink_finds_minimal_put_then_delete_and_empty_when_empty_fails() {
        let short = generate(1, 50);
        assert!(shrink(&short, |_| true).is_empty(), "empty fails -> must shrink to empty");
        assert!(shrink(&[], |_| true).is_empty(), "empty input -> empty");

        let seed = (0..1000u64)
            .find(|&s| has_put_then_delete_key3(&generate(s, 500)))
            .expect("some seed produces Put(3) followed by Delete(3) in 500 ops");
        let ops = generate(seed, 500);
        let shrunk = shrink(&ops, has_put_then_delete_key3);
        assert_eq!(
            shrunk,
            vec![
                SimOperation::Put { key: 3, len: 0, fill: 0 },
                SimOperation::Delete { key: 3 },
            ],
            "seed {seed}: expected exact 2-op minimal Put(3) + Delete(3)"
        );
    }

    #[test]
    fn shrink_output_is_one_minimal_and_simplifies_puts() {
        let mut tested = 0usize;
        for seed in 0..500u64 {
            if tested == 50 {
                break;
            }
            let ops = generate(seed, 200);
            let target_key = (seed % 8) as u8;
            let variant = tested % 5;
            let mut pred = |seq: &[SimOperation]| -> bool {
                match variant {
                    0 => {
                        let mut saw_put = false;
                        for op in seq {
                            match op {
                                SimOperation::Put { key, .. } if *key == target_key => saw_put = true,
                                SimOperation::Crash { .. } if saw_put => return true,
                                _ => {}
                            }
                        }
                        false
                    }
                    1 => {
                        let mut stage = 0u8;
                        for op in seq {
                            match op {
                                SimOperation::Fault { .. } if stage == 0 => stage = 1,
                                SimOperation::Sync if stage == 1 => stage = 2,
                                SimOperation::Crash { .. } if stage == 2 => return true,
                                _ => {}
                            }
                        }
                        false
                    }
                    2 => {
                        let mut stage = 0u8;
                        for op in seq {
                            match op {
                                SimOperation::Put { key, .. } if *key == target_key && stage == 0 => stage = 1,
                                SimOperation::Delete { key } if *key == target_key && stage == 1 => stage = 2,
                                SimOperation::Put { key, .. } if *key == target_key && stage == 2 => return true,
                                _ => {}
                            }
                        }
                        false
                    }
                    3 => {
                        let mut saw_big_put = false;
                        for op in seq {
                            match op {
                                SimOperation::Put { len, .. } if *len >= 4097 => saw_big_put = true,
                                SimOperation::Crash { .. } if saw_big_put => return true,
                                _ => {}
                            }
                        }
                        false
                    }
                    _ => seq.iter().filter(|op| matches!(op, SimOperation::Crash { .. })).count() >= 3,
                }
            };

            if !pred(&ops) {
                continue;
            }
            tested += 1;

            let shrunk = shrink(&ops, &mut pred);
            assert!(pred(&shrunk), "seed {seed} (variant {variant}): shrunk sequence no longer fails");
            assert!(!shrunk.is_empty(), "seed {seed} (variant {variant}): shrunk sequence unexpectedly empty");
            for idx in 0..shrunk.len() {
                let mut without = shrunk.clone();
                without.remove(idx);
                assert!(
                    !pred(&without),
                    "seed {seed} (variant {variant}): shrunk {shrunk:?} is not 1-minimal (removing index {idx} still fails)"
                );
            }
        }
        assert_eq!(tested, 50, "expected 50 seeded failing inputs");

        // Explicit Put simplification checks: len and fill when ignored, when thresholded, and
        // preserving 1-minimality if Put simplification makes another operation redundant.
        let ops_ignore = vec![
            SimOperation::Sync,
            SimOperation::Put { key: 2, len: 12345, fill: 0xdead_beef },
            SimOperation::Crash { mode: CrashMode::DropUnsynced },
        ];
        let shrunk_ignore = shrink(&ops_ignore, |seq| {
            seq.iter().any(|op| matches!(op, SimOperation::Put { key: 2, .. }))
        });
        assert_eq!(shrunk_ignore, vec![SimOperation::Put { key: 2, len: 0, fill: 0 }]);

        let ops_len_thresh = vec![
            SimOperation::Delete { key: 1 },
            SimOperation::Put { key: 2, len: 16384, fill: 0x1234_5678 },
        ];
        let shrunk_len = shrink(&ops_len_thresh, |seq| {
            seq.iter().any(|op| matches!(op, SimOperation::Put { key: 2, len, .. } if *len >= 4097))
        });
        assert_eq!(shrunk_len, vec![SimOperation::Put { key: 2, len: 4097, fill: 0 }]);

        let ops_fill_needed = vec![
            SimOperation::Put { key: 5, len: 500, fill: 77 },
            SimOperation::Sync,
        ];
        let shrunk_fill = shrink(&ops_fill_needed, |seq| {
            seq.iter().any(|op| matches!(op, SimOperation::Put { key: 5, len, fill } if *len >= 1 && *fill != 0))
        });
        assert_eq!(shrunk_fill, vec![SimOperation::Put { key: 5, len: 1, fill: 77 }]);

        // Interaction: simplifying fill -> 0 makes the second op redundant; output must stay 1-minimal.
        let ops_interact = vec![
            SimOperation::Put { key: 1, len: 100, fill: 99 },
            SimOperation::Put { key: 2, len: 100, fill: 0 },
        ];
        let interact_pred = |seq: &[SimOperation]| {
            let has_k1_99 = seq.iter().any(|op| matches!(op, SimOperation::Put { key: 1, fill: 99, .. }));
            let has_k1_0 = seq.iter().any(|op| matches!(op, SimOperation::Put { key: 1, fill: 0, .. }));
            let has_k2 = seq.iter().any(|op| matches!(op, SimOperation::Put { key: 2, .. }));
            (has_k1_99 && has_k2) || has_k1_0
        };
        let shrunk_interact = shrink(&ops_interact, interact_pred);
        assert_eq!(shrunk_interact, vec![SimOperation::Put { key: 1, len: 0, fill: 0 }]);
    }

    #[test]
    fn storage_generator_is_deterministic_and_shrinks() {
        let a = generate_storage(42, 60);
        let b = generate_storage(42, 60);
        assert_eq!(a, b);
        assert_eq!(a.len(), 60);
        assert!(matches!(a.last(), Some(StorageOp::Basic(SimOperation::Crash { .. }))));

        let shrunk = shrink_storage(&a, |seq| {
            seq.iter().any(|op| matches!(op, StorageOp::Compact { .. }))
                && seq.iter().any(|op| matches!(op, StorageOp::Basic(SimOperation::Crash { .. })))
        });
        assert_eq!(shrunk.len(), 2);
    }
}
