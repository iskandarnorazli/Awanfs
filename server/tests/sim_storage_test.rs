//! #213: end-to-end deterministic simulation of the AwanFS storage core over `VolumeManager<SimFs>`,
//! `sim_gen`, and `sim_model`.
//!
//! Exercises buffered PUTs, overwrites, DELETE tombstones, CopyObject, streamed PUTs interleaved
//! with concurrent same-key writes (relocation check), multi-chunk >1 MiB streams, abandoned
//! stream gaps, multipart manifests/extents, online compaction, volume rollover, injected EIO/ENOSPC
//! faults on data and journal files, `DropUnsynced` and `SeededPrefix` crashes (with torn 4 KiB
//! blocks), and recovery restarts checked against `SimModel`.

use s3_server::sim::{CrashMode, FaultPoint, FaultTracker, SimFault, SimOp};
use s3_server::sim_gen::{
    generate_storage, shrink_storage, storage_max_span, SimFileKind, SimOperation, StorageOp,
};
use s3_server::sim_harness::run_ops;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

const OPS_PER_SEED: usize = 64;
const DEFAULT_CI_SEEDS: u64 = 2_000;

fn seed_budget() -> u64 {
    std::env::var("AWAN_SIM_SEEDS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_CI_SEEDS)
}

#[test]
fn seeded_storage_simulation_matches_model_and_covers_all_fault_points() {
    let seeds = seed_budget();
    let coverage = FaultTracker::new();
    let trace_on = std::env::var("AWAN_SIM_TRACE").ok().as_deref() == Some("1");
    let workers = if trace_on {
        1
    } else {
        std::thread::available_parallelism()
            .map(|n| n.get().clamp(1, 8))
            .unwrap_or(1)
            .min(seeds.max(1) as usize)
    };
    let next_seed = AtomicU64::new(0);
    let min_fail = AtomicU64::new(u64::MAX);
    let failure: Mutex<Option<(u64, String)>> = Mutex::new(None);

    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                let local_cov = FaultTracker::new();
                loop {
                    let seed = next_seed.fetch_add(1, Ordering::Relaxed);
                    if seed >= seeds || seed >= min_fail.load(Ordering::Acquire) {
                        break;
                    }
                    let max_span = storage_max_span(seed);
                    let ops = generate_storage(seed, OPS_PER_SEED);
                    if let Err(err) = run_ops(seed, max_span, &ops, Some(&local_cov)) {
                        min_fail.fetch_min(seed, Ordering::AcqRel);
                        let mut guard = failure.lock().unwrap();
                        if guard.as_ref().is_none_or(|(best, _)| seed < *best) {
                            *guard = Some((seed, err));
                        }
                        break;
                    }
                }
                coverage.merge(&local_cov);
            });
        }
    });

    if let Some((seed, err)) = failure.into_inner().unwrap() {
        let max_span = storage_max_span(seed);
        let ops = generate_storage(seed, OPS_PER_SEED);
        let shrunk = shrink_storage(&ops, |cand| run_ops(seed, max_span, cand, None).is_err());
        let shrunk_err = run_ops(seed, max_span, &shrunk, None).unwrap_err();
        panic!(
            "simulation failed on seed {} (AWAN_SIM_SEEDS={}):\n  full error: {}\n  shrunk ({} ops): {:?}\n  shrunk error: {}",
            seed,
            seeds,
            err,
            shrunk.len(),
            shrunk,
            shrunk_err
        );
    }
    if seeds >= 500 {
        let missing = coverage.missing(FaultPoint::ALL);
        let snap = coverage.snapshot();
        if !missing.is_empty() {
            let names: Vec<&str> = missing.iter().map(|f| f.name()).collect();
            panic!(
                "fault-point coverage missing {} points over {} seeds: {:?}\nsnapshot: {:?}",
                names.len(),
                seeds,
                names,
                snap
            );
        }
        let summary: Vec<String> = FaultPoint::ALL
            .iter()
            .map(|fp| format!("{}={}", fp.name(), snap.get(fp).copied().unwrap_or(0)))
            .collect();
        println!(
            "sim_storage_test: {} seeds passed across {} worker(s); {}/{} fault points covered: {}",
            seeds,
            workers,
            FaultPoint::ALL.len(),
            FaultPoint::ALL.len(),
            summary.join(", ")
        );
    }
}

#[test]
fn determinism_same_seed_produces_identical_trace_hashes() {
    let mut hashes = Vec::new();
    for seed in [0u64, 1, 7, 42, 213, 1024, 1999] {
        let max_span = storage_max_span(seed);
        let ops = generate_storage(seed, OPS_PER_SEED);
        let (h1, t1) = run_ops(seed, max_span, &ops, None).expect("first run");
        let (h2, t2) = run_ops(seed, max_span, &ops, None).expect("second run");
        assert_eq!(t1, t2, "seed {} must produce byte-for-byte identical traces", seed);
        assert_eq!(h1, h2, "seed {} must produce identical trace hashes", seed);
        hashes.push(h1);
    }
    hashes.sort_unstable();
    hashes.dedup();
    assert!(hashes.len() >= 5, "distinct seeds must produce distinct traces: {:?}", hashes);
}

#[test]
fn scenario_streamed_put_relocates_over_interleaved_late_fail_and_survives_restart() {
    let ops = vec![
        StorageOp::StreamStart { stream_id: 1, key: 0, len: 6000, fill: 101 },
        StorageOp::Basic(SimOperation::Fault {
            file: SimFileKind::Journal,
            op: SimOp::Write,
            fault: SimFault::Eio,
        }),
        StorageOp::Basic(SimOperation::Put { key: 0, len: 1000, fill: 202 }),
        StorageOp::StreamFinish { stream_id: 1 },
        StorageOp::Basic(SimOperation::Sync),
        StorageOp::Basic(SimOperation::Crash { mode: CrashMode::DropUnsynced }),
    ];
    let tracker = FaultTracker::new();
    run_ops(7, 128 * 1024, &ops, Some(&tracker)).expect("relocated stream survives restart");
    assert!(tracker.count(FaultPoint::LateFailJournalAfterRecord) >= 1);
    assert!(tracker.count(FaultPoint::StreamRelocated) >= 1);
}

#[test]
fn scenario_gap_then_late_fail_journal_retry_and_compaction_with_inflight_stream() {
    let ops = vec![
        StorageOp::Basic(SimOperation::Put { key: 1, len: 4096, fill: 11 }),
        StorageOp::Basic(SimOperation::Fault {
            file: SimFileKind::Data,
            op: SimOp::Write,
            fault: SimFault::NoSpace,
        }),
        StorageOp::Basic(SimOperation::Put { key: 2, len: 4096, fill: 22 }),
        StorageOp::Basic(SimOperation::Fault {
            file: SimFileKind::Journal,
            op: SimOp::Write,
            fault: SimFault::Eio,
        }),
        StorageOp::Basic(SimOperation::Put { key: 3, len: 4096, fill: 33 }),
        StorageOp::Basic(SimOperation::Sync),
        // #412: the armed journal write fault fires at the sync above, where B writes the
        // records; this sync retries them and flushes the pending ones.
        StorageOp::Basic(SimOperation::Sync),
        StorageOp::StreamStart { stream_id: 1, key: 4, len: 32768, fill: 44 },
        StorageOp::Basic(SimOperation::Put { key: 5, len: 49152, fill: 55 }),
        StorageOp::Basic(SimOperation::Put { key: 6, len: 49152, fill: 66 }),
        StorageOp::Compact { seal_if_single: false },
        StorageOp::StreamFinish { stream_id: 1 },
        StorageOp::Compact { seal_if_single: false },
        StorageOp::Basic(SimOperation::Sync),
        StorageOp::Basic(SimOperation::Crash { mode: CrashMode::SeededPrefix }),
    ];
    let tracker = FaultTracker::new();
    run_ops(9, 128 * 1024, &ops, Some(&tracker)).expect("gap retry and compaction busy/move");
    assert!(tracker.count(FaultPoint::JournalRetryFlushed) >= 1);
    assert!(tracker.count(FaultPoint::CompactionBusyInFlight) >= 1);
    assert!(tracker.count(FaultPoint::CompactionMovedLive) >= 1);
}
