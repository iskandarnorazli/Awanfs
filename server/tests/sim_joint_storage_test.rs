//! #408: Joint deterministic simulation of `VolumeManager<SimFs>` and `MetaStore<SimFs>`
//! with deterministic I/O amplification budgets (`SimFile::calls`, `SimFile::bytes_written`,
//! and `WorkCounters`).
//!
//! Exercises buffered PUTs, DELETE tombstones, volume rollover, memtable flushes to L0 metadata
//! segments, two-slot manifest switches, leveled compaction across L0/L1/L2+, volume compaction,
//! `DropUnsynced` and `SeededPrefix` crashes (with torn writes/4 KiB blocks and torn-manifest
//! fallback), and `Eio`/`NoSpace` faults across data, journal, segment, and manifest files.

use s3_server::clock::{Clock, ManualClock};
use s3_server::ffi_bridge::{AWAN_FLAG_ACTIVE, AWAN_FLAG_TOMBSTONE};
use s3_server::manifest::{read_manifest, segment_path, slot_path, Manifest, MetaStore, ReplayFrom};
use s3_server::meta_compaction::Policy;
use s3_server::meta_index::ObjectMeta;
use s3_server::segment::{sort_key, Entry, SegEntry};
use s3_server::sim::{CrashMode, FaultPoint, FaultTracker, SimFault, SimFs, SimOp, SimRng};
use s3_server::sim_gen::{generate_joint, shrink_joint, JointFileKind, JointOp, JOINT_KEY_SPACE};
use s3_server::sim_model::{Outcome, SimModel, Survives};
use s3_server::vfile::{VolFile, VolFs};
use s3_server::volume::{global_offset, split_offset, NeedleLocation, ScannedNeedle, VolumeError, VolumeManager};
use s3_server::work::WorkCounters;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use xxhash_rust::xxh3::xxh3_64;

const OPS_PER_SEED: usize = 64;
const DEFAULT_CI_SEEDS: u64 = 400;
const SECTOR_SIZE: u64 = 4096;
const BUCKET_ID: u16 = 1;

// Explicit upper-bound constants for `io_amplification_bounds_hold_for_writes_recovery_and_compaction`.
// Each bound is derived directly below in terms of per-operation I/O invariants.

/// Unbatched durable write syncs: 1 `file.sync_data()` + 1 `journal.sync_data()` = 2.0 syncs/write.
const MAX_VOL_SYNCS_PER_ACK_WRITE: f64 = 2.0;

/// Joint write + amortized checkpoint syncs: 2.0 volume syncs/write plus 2 checkpoint syncs
/// (1 new L0 segment `sync_data` + 1 manifest slot `sync_data`) every `FLUSH_BATCH = 16` writes
/// -> `2.0 + 2.0 / 16.0 = 2.125`, capped at `2.15`.
const MAX_JOINT_SYNCS_PER_ACK_WRITE: f64 = 2.15;

/// Tail recovery scan reads: `AlignedScanWindow` uses `SCAN_BATCH_BYTES = 1 MiB` per refill.
/// A tail volume spanning `T` bytes needs at most `ceil(T / 1 MiB) + 1` aligned batch reads
/// (plus 0 `scan_needles` reads on older checkpointed volumes `vol_id < replay_from.volume_id`).
const MAX_TAIL_SCAN_READS_PER_MIB: u64 = 2;

/// Volume open reads (`VolumeManager::open_with`): 1 superblock sector read + up to 2 reads for
/// journal scan / 1 MiB aligned tail verification window per open volume <= 4 reads/MiB.
const MAX_OPEN_READS_PER_VOLUME_MIB: u64 = 4;

/// Metadata store open reads (`MetaStore::open`): 2 manifest slot reads total + at most 4 reads
/// per live segment (header block + trailer block + index blocks).
const MAX_META_OPEN_READS_PER_SEG: u64 = 4;

/// Volume compaction rewrite amplification: surviving 4 KiB-aligned needles are copied once
/// (`rewritten_bytes / live_needle_bytes <= 1.0`).
const MAX_VOL_COMPACTION_REWRITE_PER_LIVE_NEEDLE_BYTE: f64 = 1.0;

/// Volume compaction payload amplification for 8 KiB objects: an 8 KiB payload fits in one
/// 4096B header+key sector + two 4096B data sectors = 12 KiB aligned needle (`12 / 8 = 1.50 <= 1.60`).
const MAX_VOL_COMPACTION_REWRITE_PER_LIVE_PAYLOAD_BYTE: f64 = 1.6;

/// Leveled metadata compaction rewrite amplification (`ratio = 3, max_level = 4`): across `L`
/// levels a key is merged up at most once per level (~`<= ratio * L`), well under `8.0x`.
const MAX_META_COMPACTION_REWRITE_PER_LIVE_BYTE: f64 = 8.0;

fn seed_budget() -> u64 {
    std::env::var("AWAN_SIM_JOINT_SEEDS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_CI_SEEDS)
}

fn key_name(key: u8) -> String {
    format!("k{:03}", key)
}

fn parse_key(name: &str) -> Option<u8> {
    name.strip_prefix('k')?.parse::<u8>().ok()
}

fn sort_key_for(key: u8) -> Vec<u8> {
    sort_key(u32::from(BUCKET_ID), key_name(key).as_bytes())
}

fn joint_max_span(seed: u64) -> u64 {
    if seed % 2 == 0 {
        64 * 1024
    } else {
        128 * 1024
    }
}

fn joint_policy() -> Policy {
    Policy {
        l0_trigger: 3,
        level1_bytes: 12 * 1024,
        ratio: 2,
        segment_bytes: 16 * 1024,
        max_level: 4,
    }
}

fn payload_bytes(len: u32, fill: u64) -> Vec<u8> {
    let mut rng = SimRng::new(fill ^ (len as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let mut out = vec![0u8; len as usize];
    let mut i = 0usize;
    while i + 8 <= out.len() {
        out[i..i + 8].copy_from_slice(&rng.next_u64().to_le_bytes());
        i += 8;
    }
    if i < out.len() {
        let rem = out.len() - i;
        out[i..].copy_from_slice(&rng.next_u64().to_le_bytes()[..rem]);
    }
    out
}

fn data_path(dir: &Path, id: u32) -> PathBuf {
    dir.join(format!("volume_{:04}.dat", id))
}

fn idx_path(dir: &Path, id: u32) -> PathBuf {
    dir.join(format!("volume_{:04}.idx", id))
}

fn volume_ids_in_fs(fs: &SimFs, dir: &Path) -> Vec<u32> {
    let mut ids: Vec<u32> = fs
        .list(dir)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|p| {
            let name = p.file_name()?.to_str()?;
            name.strip_prefix("volume_")?.strip_suffix(".dat")?.parse().ok()
        })
        .collect();
    ids.sort_unstable();
    ids
}

fn open_sim_volume(
    fs: &SimFs,
    dir: &Path,
    id: u32,
    max_span: u64,
    clock: Arc<ManualClock>,
    work: &Arc<WorkCounters>,
) -> Result<Arc<VolumeManager<SimFs>>, VolumeError> {
    let mut vm = VolumeManager::open_with(fs, dir, id, false, clock)?;
    vm.set_max_span(max_span);
    vm.set_growth_step(SECTOR_SIZE);
    vm.attach_work_counters(Arc::clone(work));
    Ok(Arc::new(vm))
}

fn read_needle_payload(vm: &VolumeManager<SimFs>, offset: u64, crc: u32) -> Result<Vec<u8>, VolumeError> {
    let mut out = Vec::new();
    vm.copy_payload(offset, crc, |piece| {
        out.extend_from_slice(piece);
        Ok(())
    })?;
    Ok(out)
}

fn meta_from_put(g: u64, len: u32, fill: u64, created_ns: u64) -> ObjectMeta {
    ObjectMeta {
        key_hash: fill ^ (g << 1),
        bucket_flags: (u64::from(BUCKET_ID) << 32) | u64::from(AWAN_FLAG_ACTIVE),
        vol_offset: g,
        size: len as u64,
        etag_hi: fill,
        etag_lo: len as u64,
        created_at: created_ns / 1_000_000,
        key_slice: 0,
    }
}

fn meta_from_scanned(vol_id: u32, n: &ScannedNeedle) -> ObjectMeta {
    ObjectMeta {
        key_hash: n.etag_hi ^ (global_offset(vol_id, n.offset) << 1),
        bucket_flags: (u64::from(BUCKET_ID) << 32) | u64::from(n.flags),
        vol_offset: global_offset(vol_id, n.offset),
        size: n.payload_size,
        etag_hi: n.etag_hi,
        etag_lo: n.etag_lo,
        created_at: n.created_at_ns / 1_000_000,
        key_slice: 0,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LiveObj {
    vol_offset: u64,
    value: (u32, u64),
    crc: u32,
}

fn scan_full_volumes(
    fs: &SimFs,
    dir: &Path,
    max_span: u64,
    clock: Arc<ManualClock>,
    work: &Arc<WorkCounters>,
) -> Result<(BTreeMap<u32, Arc<VolumeManager<SimFs>>>, BTreeMap<u8, LiveObj>, BTreeMap<u8, Option<(u32, u64)>>), String> {
    let mut ids = volume_ids_in_fs(fs, dir);
    if ids.is_empty() {
        ids.push(1);
    }
    let mut volumes = BTreeMap::new();
    let mut live = BTreeMap::new();
    let mut values = BTreeMap::new();

    for id in ids {
        let vm = open_sim_volume(fs, dir, id, max_span, Arc::clone(&clock), work)
            .map_err(|e| format!("reopen volume {}: {:?}", id, e))?;
        let mut scanned: Vec<ScannedNeedle> = Vec::new();
        vm.scan_needles(|n| scanned.push(n))
            .map_err(|e| format!("scan volume {}: {:?}", id, e))?;
        for n in scanned {
            let Some(k) = parse_key(&n.key) else {
                return Err(format!("recovered unexpected key {:?}", n.key));
            };
            if n.flags & AWAN_FLAG_TOMBSTONE != 0 {
                values.insert(k, None);
                live.remove(&k);
                continue;
            }
            let got = read_needle_payload(&vm, n.offset, n.payload_crc32c)
                .map_err(|e| format!("read needle key={} off={}: {:?}", n.key, n.offset, e))?;
            if got.len() as u64 != n.payload_size {
                return Err(format!("key {} length mismatch: {} vs {}", n.key, n.payload_size, got.len()));
            }
            let expected = payload_bytes(n.payload_size as u32, n.etag_hi);
            if got != expected {
                return Err(format!("key {} payload corruption at vol {} off {}", n.key, id, n.offset));
            }
            let g = global_offset(id, n.offset);
            let val = (n.payload_size as u32, n.etag_hi);
            values.insert(k, Some(val));
            live.insert(
                k,
                LiveObj {
                    vol_offset: g,
                    value: val,
                    crc: n.payload_crc32c,
                },
            );
        }
        volumes.insert(id, vm);
    }
    Ok((volumes, live, values))
}

fn named_meta_files_only(fs: &SimFs, dir: &Path, st: &MetaStore<SimFs>) -> Result<(), String> {
    if st.manifest().generation == 0 {
        return Ok(());
    }
    let mut want: Vec<PathBuf> = st
        .manifest()
        .segments
        .iter()
        .map(|l| segment_path(dir, l.seq))
        .collect();
    for s in 0..2 {
        let p = slot_path(dir, s);
        if fs.exists(&p) {
            want.push(p);
        }
    }
    want.sort();
    let got = fs
        .list(&dir.join("meta"))
        .map_err(|e| format!("list meta dir: {}", e))?;
    if got != want {
        return Err(format!("orphan meta files on disk: got={:?}, want={:?}", got, want));
    }
    Ok(())
}

fn replay_meta_and_tail(
    meta: &MetaStore<SimFs>,
    volumes: &BTreeMap<u32, Arc<VolumeManager<SimFs>>>,
) -> Result<(BTreeMap<u8, (u64, u32, u64)>, BTreeMap<Vec<u8>, Entry>), String> {
    // Validate level invariants & get-vs-range consistency across segments.
    for (lvl_idx, lvl) in meta.segments().levels().iter().enumerate().skip(1) {
        for w in lvl.windows(2) {
            if w[0].range.as_ref().unwrap().1 >= w[1].range.as_ref().unwrap().0 {
                return Err(format!("meta level {} overlapping segments", lvl_idx));
            }
        }
    }
    let rows = meta
        .segments()
        .range(&sort_key(0, b""), None, usize::MAX)
        .map_err(|e| format!("meta range scan failed: {:?}", e))?;
    let mut state: BTreeMap<u8, (u64, u32, u64)> = BTreeMap::new();
    for (sk, m) in rows {
        let name = std::str::from_utf8(&sk[4..]).map_err(|_| "non-utf8 sort key".to_string())?;
        let Some(k) = parse_key(name) else {
            return Err(format!("unexpected meta sort key {:?}", name));
        };
        state.insert(k, (m.vol_offset, m.size as u32, m.etag_hi));
    }
    for k in 0..JOINT_KEY_SPACE {
        let sk = sort_key_for(k);
        let got_get = meta
            .segments()
            .get(&sk)
            .map_err(|e| format!("meta get {:?} failed: {:?}", k, e))?
            .map(|m| (m.vol_offset, m.size as u32, m.etag_hi));
        let got_range = state.get(&k).copied();
        if got_get != got_range {
            return Err(format!("meta get vs range mismatch on key {}: {:?} vs {:?}", k, got_get, got_range));
        }
    }

    // Replay needles from `replay_from` onward.
    let rf = meta.manifest().replay_from;
    let mut tail_memtable: BTreeMap<Vec<u8>, Entry> = BTreeMap::new();
    for (&vol_id, vm) in volumes {
        if vol_id < rf.volume_id {
            continue;
        }
        let mut tail_needles = Vec::new();
        vm.scan_needles(|n| {
            if vol_id > rf.volume_id || n.offset >= rf.offset {
                tail_needles.push(n);
            }
        })
        .map_err(|e| format!("tail scan vol {}: {:?}", vol_id, e))?;
        for n in tail_needles {
            let Some(k) = parse_key(&n.key) else {
                return Err(format!("unexpected tail key {:?}", n.key));
            };
            let sk = sort_key_for(k);
            if n.flags & AWAN_FLAG_TOMBSTONE != 0 {
                state.remove(&k);
                tail_memtable.insert(sk, Entry::Tombstone);
            } else {
                let g = global_offset(vol_id, n.offset);
                state.insert(k, (g, n.payload_size as u32, n.etag_hi));
                tail_memtable.insert(sk, Entry::Row(meta_from_scanned(vol_id, &n)));
            }
        }
    }
    Ok((state, tail_memtable))
}

struct JointHarness {
    dir: PathBuf,
    max_span: u64,
    policy: Policy,
    clock: Arc<ManualClock>,
    work: Arc<WorkCounters>,
    fs: Arc<SimFs>,
    volumes: BTreeMap<u32, Arc<VolumeManager<SimFs>>>,
    active_id: u32,
    meta: MetaStore<SimFs>,
    memtable: BTreeMap<Vec<u8>, Entry>,
    live: BTreeMap<u8, LiveObj>,
    model: SimModel,
    writes_since_sync: bool,
    crash_rng: SimRng,
    trace: Vec<u8>,
}

impl JointHarness {
    fn new(seed: u64, max_span: u64) -> Result<Self, String> {
        let dir = PathBuf::from("sim/joint");
        let clock = Arc::new(ManualClock::new(1_800_000_000_000_000_000));
        let work = Arc::new(WorkCounters::default());
        let fs = Arc::new(SimFs::new());
        let vm = open_sim_volume(&fs, &dir, 1, max_span, Arc::clone(&clock), &work)
            .map_err(|e| format!("initial volume open: {:?}", e))?;
        let mut volumes = BTreeMap::new();
        volumes.insert(1, vm);
        let meta = MetaStore::open(Arc::clone(&fs), &dir, Arc::clone(&work))
            .map_err(|e| format!("initial meta open: {:?}", e))?;
        Ok(Self {
            dir,
            max_span,
            policy: joint_policy(),
            clock,
            work,
            fs,
            volumes,
            active_id: 1,
            meta,
            memtable: BTreeMap::new(),
            live: BTreeMap::new(),
            model: SimModel::new(),
            writes_since_sync: false,
            crash_rng: SimRng::new(seed ^ 0x408C_9A1B_55E2_3017),
            trace: Vec::new(),
        })
    }

    fn tracker(&self) -> Arc<FaultTracker> {
        self.fs.fault_tracker()
    }

    fn push_u8(&mut self, tag: u8) {
        self.trace.push(tag);
    }

    fn push_u64(&mut self, v: u64) {
        self.trace.extend_from_slice(&v.to_le_bytes());
    }

    fn active_vm(&self) -> Arc<VolumeManager<SimFs>> {
        Arc::clone(&self.volumes[&self.active_id])
    }

    fn roll_volume(&mut self, full_id: u32) -> Result<(), VolumeError> {
        if self.active_id != full_id {
            return Ok(());
        }
        let next_id = self.volumes.keys().next_back().copied().unwrap_or(full_id).max(full_id) + 1;
        let vm = open_sim_volume(&self.fs, &self.dir, next_id, self.max_span, Arc::clone(&self.clock), &self.work)?;
        self.volumes.insert(next_id, vm);
        self.active_id = next_id;
        self.tracker().record(FaultPoint::VolumeRollover);
        Ok(())
    }

    fn write_to_active(
        &mut self,
        key: u8,
        payload: &[u8],
        flags: u16,
        fill: u64,
        len: u64,
    ) -> Result<(NeedleLocation, u64), VolumeError> {
        let key_str = key_name(key);
        let mut attempts = 0;
        loop {
            let vm = self.active_vm();
            match vm.write_needle_with_attrs(BUCKET_ID, &key_str, &[], payload, flags, fill, len, None) {
                Ok(loc) => {
                    let g = global_offset(vm.id(), loc.offset);
                    return Ok((loc, g));
                }
                Err(VolumeError::VolumeFull { len: l, .. }) if l + SECTOR_SIZE > vm.max_span() => {
                    return Err(VolumeError::VolumeFull { offset: 0, len: l });
                }
                Err(VolumeError::VolumeFull { .. }) if attempts < 8 => {
                    attempts += 1;
                    self.roll_volume(vm.id())?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn exec_put(&mut self, key: u8, len: u32, fill: u64) {
        let payload = payload_bytes(len, fill);
        let flags = if len == 0 {
            s3_server::ffi_bridge::AWAN_FLAG_ACTIVE | s3_server::ffi_bridge::AWAN_FLAG_ZERO_BYTE
        } else {
            s3_server::ffi_bridge::AWAN_FLAG_ACTIVE
        };
        let now = self.clock.now_ns();
        let res = self.write_to_active(key, &payload, flags, fill, len as u64);
        let op = s3_server::sim_gen::SimOperation::Put { key, len, fill };
        match res {
            Ok((loc, g)) => {
                self.writes_since_sync = true;
                self.live.insert(key, LiveObj { vol_offset: g, value: (len, fill), crc: loc.payload_crc32c });
                self.memtable.insert(sort_key_for(key), Entry::Row(meta_from_put(g, len, fill, now)));
                self.model.record(&op, Outcome::Ok);
                self.push_u8(1);
                self.push_u64(g);
            }
            Err(VolumeError::AfterRecord { loc, .. }) => {
                let g = global_offset(loc.volume_id, loc.offset);
                self.writes_since_sync = true;
                self.tracker().record(FaultPoint::LateFailJournalAfterRecord);
                self.live.insert(key, LiveObj { vol_offset: g, value: (len, fill), crc: loc.payload_crc32c });
                self.memtable.insert(sort_key_for(key), Entry::Row(meta_from_put(g, len, fill, now)));
                self.model.record(&op, Outcome::Failed);
                self.push_u8(2);
                self.push_u64(g);
            }
            Err(_) => {
                self.model.record(&op, Outcome::Failed);
                self.push_u8(3);
            }
        }
    }

    fn exec_delete(&mut self, key: u8) {
        let res = self.write_to_active(key, &[], s3_server::ffi_bridge::AWAN_FLAG_TOMBSTONE, 0, 0);
        let op = s3_server::sim_gen::SimOperation::Delete { key };
        match res {
            Ok((_loc, g)) => {
                self.writes_since_sync = true;
                self.live.remove(&key);
                self.memtable.insert(sort_key_for(key), Entry::Tombstone);
                self.model.record(&op, Outcome::Ok);
                self.push_u8(4);
                self.push_u64(g);
            }
            Err(VolumeError::AfterRecord { loc, .. }) => {
                self.writes_since_sync = true;
                self.tracker().record(FaultPoint::LateFailJournalAfterRecord);
                self.live.remove(&key);
                self.memtable.insert(sort_key_for(key), Entry::Tombstone);
                self.model.record(&op, Outcome::Failed);
                self.push_u8(5);
                self.push_u64(global_offset(loc.volume_id, loc.offset));
            }
            Err(_) => {
                self.model.record(&op, Outcome::Failed);
                self.push_u8(6);
            }
        }
    }

    fn sync_all_volumes(&mut self) -> Result<bool, ()> {
        let had_pending = self.volumes.values().any(|vm| vm.pending_journal_len() > 0);
        for vm in self.volumes.values() {
            if vm.sync_now().is_err() {
                if self.writes_since_sync {
                    self.tracker().record(FaultPoint::LateFailSyncAfterRecord);
                }
                self.model.record(&s3_server::sim_gen::SimOperation::Sync, Outcome::Failed);
                return Err(());
            }
        }
        if had_pending && self.volumes.values().all(|vm| vm.pending_journal_len() == 0) {
            self.tracker().record(FaultPoint::JournalRetryFlushed);
        }
        self.writes_since_sync = false;
        self.model.record(&s3_server::sim_gen::SimOperation::Sync, Outcome::Ok);
        Ok(true)
    }

    fn exec_sync(&mut self) -> Result<(), String> {
        if self.sync_all_volumes().is_err() {
            self.push_u8(7);
            return Ok(());
        }
        let snap = self.fs.crashed(CrashMode::DropUnsynced, &mut SimRng::new(0));
        let w = Arc::new(WorkCounters::default());
        let (_, rec_live, _) = scan_full_volumes(&snap, &self.dir, self.max_span, Arc::clone(&self.clock), &w)?;
        let live_vals: BTreeMap<u8, (u32, u64)> = self.live.iter().map(|(&k, e)| (k, e.value)).collect();
        let rec_vals: BTreeMap<u8, (u32, u64)> = rec_live.iter().map(|(&k, e)| (k, e.value)).collect();
        if live_vals != rec_vals {
            return Err(format!("post-sync live vs scan divergence: live={:?}, rec={:?}", live_vals, rec_vals));
        }
        self.push_u8(8);
        Ok(())
    }

    fn exec_flush_meta(&mut self) -> Result<(), String> {
        if self.sync_all_volumes().is_err() {
            self.push_u8(9);
            return Ok(());
        }
        if self.memtable.is_empty() {
            self.push_u8(10);
            return Ok(());
        }
        let rf = ReplayFrom {
            volume_id: self.active_id,
            offset: self.active_vm().offset(),
        };
        let entries: Vec<SegEntry> = self
            .memtable
            .iter()
            .map(|(k, &entry)| SegEntry { key: k.clone(), entry })
            .collect();
        let gen_before = self.meta.manifest().generation;
        match self.meta.flush(&entries, rf, self.clock.now_ns()) {
            Ok(seq) => {
                self.memtable.clear();
                self.tracker().record(FaultPoint::MetaFlushCommitted);
                if self.meta.manifest().generation >= 2 {
                    self.tracker().record(FaultPoint::MetaManifestSlotSwitch);
                }
                self.push_u8(11);
                self.push_u64(seq);
            }
            Err(_) => {
                if self.meta.manifest().generation != gen_before {
                    self.memtable.clear();
                    self.tracker().record(FaultPoint::MetaFlushCommitted);
                    self.meta = MetaStore::open(Arc::clone(&self.fs), &self.dir, Arc::clone(&self.work))
                        .map_err(|e| format!("reopen meta after post-switch flush error: {:?}", e))?;
                }
                self.push_u8(12);
            }
        }
        Ok(())
    }

    fn exec_compact_meta(&mut self, all: bool) -> Result<(), String> {
        let mut ran = 0u64;
        loop {
            let gen_before = self.meta.manifest().generation;
            match self.meta.compact(&self.policy, self.clock.now_ns()) {
                Ok(Some(job)) => {
                    ran += 1;
                    if job.from == 0 {
                        self.tracker().record(FaultPoint::MetaCompactL0ToL1);
                    } else {
                        self.tracker().record(FaultPoint::MetaCompactDeeperLevel);
                    }
                    if self.meta.manifest().generation >= 2 {
                        self.tracker().record(FaultPoint::MetaManifestSlotSwitch);
                    }
                    if !all {
                        break;
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    if self.meta.manifest().generation != gen_before {
                        self.meta = MetaStore::open(Arc::clone(&self.fs), &self.dir, Arc::clone(&self.work))
                            .map_err(|e| format!("reopen meta after post-switch compact error: {:?}", e))?;
                    }
                    self.push_u8(13);
                    self.push_u64(ran);
                    return Ok(());
                }
            }
        }
        self.push_u8(14);
        self.push_u64(ran);
        Ok(())
    }

    fn exec_compact_volume(&mut self, seal_if_single: bool) -> Result<(), String> {
        if self.volumes.len() == 1 && seal_if_single && self.roll_volume(self.active_id).is_err() {
            self.push_u8(15);
            return Ok(());
        }
        if self.volumes.len() < 2 {
            self.push_u8(16);
            return Ok(());
        }
        let old_id = *self.volumes.keys().next().unwrap();
        if old_id == self.active_id {
            self.push_u8(16);
            return Ok(());
        }
        let old_vm = Arc::clone(&self.volumes[&old_id]);
        old_vm.seal();
        let mut needles = Vec::new();
        if old_vm.scan_needles(|n| needles.push(n)).is_err() {
            self.push_u8(17);
            return Ok(());
        }
        let mut copied = 0usize;
        let now = self.clock.now_ns();
        for n in needles {
            if n.flags & s3_server::ffi_bridge::AWAN_FLAG_TOMBSTONE != 0 {
                continue;
            }
            let Some(k) = parse_key(&n.key) else {
                continue;
            };
            let g_old = global_offset(old_id, n.offset);
            let Some(entry) = self.live.get(&k).copied() else {
                continue;
            };
            if entry.vol_offset != g_old {
                continue;
            }
            let bytes = if n.payload_size == 0 {
                Vec::new()
            } else {
                match read_needle_payload(&old_vm, n.offset, n.payload_crc32c) {
                    Ok(b) => b,
                    Err(_) => {
                        self.push_u8(18);
                        return Ok(());
                    }
                }
            };
            match self.write_to_active(k, &bytes, n.flags, n.etag_hi, n.payload_size) {
                Ok((loc, g_new)) => {
                    self.writes_since_sync = true;
                    let (len, fill) = entry.value;
                    self.live.insert(k, LiveObj { vol_offset: g_new, value: (len, fill), crc: loc.payload_crc32c });
                    self.memtable.insert(sort_key_for(k), Entry::Row(meta_from_put(g_new, len, fill, now)));
                    copied += 1;
                }
                Err(VolumeError::AfterRecord { loc, .. }) => {
                    let g_new = global_offset(loc.volume_id, loc.offset);
                    self.writes_since_sync = true;
                    self.tracker().record(FaultPoint::LateFailJournalAfterRecord);
                    let (len, fill) = entry.value;
                    self.live.insert(k, LiveObj { vol_offset: g_new, value: (len, fill), crc: loc.payload_crc32c });
                    self.memtable.insert(sort_key_for(k), Entry::Row(meta_from_put(g_new, len, fill, now)));
                    copied += 1;
                }
                Err(_) => {
                    self.push_u8(19);
                    return Ok(());
                }
            }
        }
        // Sync remaining volumes and commit metadata past `old_id` before removing `old_id`.
        for (&id, vm) in &self.volumes {
            if id != old_id && vm.sync_now().is_err() {
                self.push_u8(20);
                return Ok(());
            }
        }
        if !self.memtable.is_empty() {
            let rf = ReplayFrom {
                volume_id: self.active_id,
                offset: self.active_vm().offset(),
            };
            let entries: Vec<SegEntry> = self
                .memtable
                .iter()
                .map(|(k, &entry)| SegEntry { key: k.clone(), entry })
                .collect();
            let gen_before = self.meta.manifest().generation;
            match self.meta.flush(&entries, rf, self.clock.now_ns()) {
                Ok(_) => {
                    self.memtable.clear();
                    self.tracker().record(FaultPoint::MetaFlushCommitted);
                    if self.meta.manifest().generation >= 2 {
                        self.tracker().record(FaultPoint::MetaManifestSlotSwitch);
                    }
                }
                Err(_) => {
                    if self.meta.manifest().generation != gen_before {
                        self.memtable.clear();
                        self.meta = MetaStore::open(Arc::clone(&self.fs), &self.dir, Arc::clone(&self.work))
                            .map_err(|e| format!("reopen meta after post-switch compact-vol flush: {:?}", e))?;
                    } else {
                        self.push_u8(20);
                        return Ok(());
                    }
                }
            }
        }
        self.volumes.remove(&old_id);
        drop(old_vm);
        let _ = self.fs.remove(&data_path(&self.dir, old_id));
        let _ = self.fs.remove(&idx_path(&self.dir, old_id));
        if copied > 0 {
            self.tracker().record(FaultPoint::CompactionMovedLive);
        }
        self.push_u8(21);
        self.push_u64(copied as u64);
        Ok(())
    }

    fn exec_verify_lookup(&mut self, key: u8) -> Result<(), String> {
        let sk = sort_key_for(key);
        let meta_entry = self
            .meta
            .segments()
            .get(&sk)
            .map_err(|e| format!("live meta get {:?}: {:?}", key, e))?;
        let combined = match self.memtable.get(&sk) {
            Some(Entry::Row(m)) => Some(*m),
            Some(Entry::Tombstone) => None,
            None => meta_entry,
        };
        let want = self.live.get(&key).map(|e| (e.vol_offset, e.value.0 as u64, e.value.1));
        let got = combined.map(|m| (m.vol_offset, m.size, m.etag_hi));
        if want != got {
            return Err(format!("live lookup vs meta/memtable divergence on key {}: want={:?}, got={:?}", key, want, got));
        }
        if let Some(entry) = self.live.get(&key).copied() {
            let (vid, local) = split_offset(entry.vol_offset);
            let vm = &self.volumes[&vid];
            match read_needle_payload(vm, local, entry.crc) {
                Ok(bytes) => {
                    if bytes != payload_bytes(entry.value.0, entry.value.1) {
                        return Err(format!("live verify read byte mismatch on key {}", key));
                    }
                    self.push_u8(22);
                }
                Err(VolumeError::Io(_)) => {
                    self.push_u8(23);
                }
                Err(e) => return Err(format!("live verify read unexpected error on key {}: {:?}", key, e)),
            }
        } else {
            self.push_u8(24);
        }
        Ok(())
    }

    fn exec_fault(&mut self, file: JointFileKind, op: SimOp, fault: SimFault) {
        match file {
            JointFileKind::Data => {
                let p = data_path(&self.dir, self.active_id);
                self.fs.arm(&p, op, fault);
            }
            JointFileKind::Journal => {
                let p = idx_path(&self.dir, self.active_id);
                if op == SimOp::SetLen {
                    // Rollback `set_len` in `flush_pending_journal` runs when `write_at` fails.
                    self.fs.arm(&p, SimOp::Write, fault);
                    self.fs.arm(&p, SimOp::SetLen, fault);
                    if let Some(vm) = self.volumes.get(&self.active_id) {
                        vm.fail_next_index();
                    }
                } else {
                    let armed = self.fs.arm(&p, op, fault);
                    if armed && op == SimOp::Write {
                        if let Some(vm) = self.volumes.get(&self.active_id) {
                            vm.fail_next_index();
                        }
                    }
                }
            }
            JointFileKind::Segment => {
                if op == SimOp::Read {
                    if let Some(live) = self.meta.segments().levels().iter().flatten().next() {
                        let existing = segment_path(&self.dir, live.seq);
                        self.fs.arm(&existing, SimOp::Read, fault);
                    }
                }
                let next_seg = segment_path(&self.dir, self.meta.manifest().next_seq);
                self.fs.arm_on_open(&next_seg, op, fault);
            }
            JointFileKind::Manifest => {
                let next_slot = if self.meta.manifest().generation == 0 {
                    0
                } else {
                    1 - ((self.meta.manifest().generation - 1) as usize % 2)
                };
                let target = slot_path(&self.dir, next_slot);
                self.fs.arm_on_open(&target, op, fault);
            }
        }
        self.push_u8(25);
    }

    fn exec_probe_read_fault(&mut self, file: JointFileKind, fault: SimFault) -> Result<(), String> {
        let snap = Arc::new(self.fs.crashed(CrashMode::DropUnsynced, &mut SimRng::new(0)));
        match file {
            JointFileKind::Journal => {
                let id = self.active_id;
                let idx = idx_path(&self.dir, id);
                let len = snap.file(&idx).and_then(|f| f.len().ok()).unwrap_or(0);
                if len == 0 {
                    self.push_u8(26);
                    return Ok(());
                }
                snap.arm(&idx, SimOp::Read, fault);
                if VolumeManager::open_with(&*snap, &self.dir, id, false, self.clock.clone()).is_ok() {
                    return Err(format!("expected volume open to fail with journal read fault {:?}", fault));
                }
                VolumeManager::open_with(&*snap, &self.dir, id, false, self.clock.clone())
                    .map_err(|e| format!("reopen after journal read fault failed: {:?}", e))?;
            }
            JointFileKind::Manifest => {
                let target = [0usize, 1]
                    .into_iter()
                    .map(|s| slot_path(&self.dir, s))
                    .find(|p| snap.file(p).and_then(|f| f.len().ok()).unwrap_or(0) > 0);
                let Some(p) = target else {
                    self.push_u8(26);
                    return Ok(());
                };
                snap.arm(&p, SimOp::Read, fault);
                if MetaStore::open(Arc::clone(&snap), &self.dir, Arc::clone(&self.work)).is_ok() {
                    return Err(format!("expected MetaStore::open to fail with manifest read fault {:?}", fault));
                }
                MetaStore::open(Arc::clone(&snap), &self.dir, Arc::clone(&self.work))
                    .map_err(|e| format!("reopen after manifest read fault failed: {:?}", e))?;
            }
            JointFileKind::Segment | JointFileKind::Data => {
                let Some(first_seg) = self.meta.manifest().segments.first() else {
                    self.push_u8(26);
                    return Ok(());
                };
                let p = segment_path(&self.dir, first_seg.seq);
                snap.arm(&p, SimOp::Read, fault);
                if MetaStore::open(Arc::clone(&snap), &self.dir, Arc::clone(&self.work)).is_ok() {
                    return Err(format!("expected MetaStore::open to fail with segment read fault {:?}", fault));
                }
                MetaStore::open(Arc::clone(&snap), &self.dir, Arc::clone(&self.work))
                    .map_err(|e| format!("reopen after segment read fault failed: {:?}", e))?;
            }
        }
        self.push_u8(27);
        Ok(())
    }

    fn exec_crash(&mut self, mode: CrashMode) -> Result<(), String> {
        self.volumes.clear();
        self.tracker().record(match mode {
            CrashMode::DropUnsynced => FaultPoint::CrashDropUnsynced,
            CrashMode::SeededPrefix => FaultPoint::CrashSeededPrefix,
        });
        if self.writes_since_sync {
            self.tracker().record(FaultPoint::CrashBeforeSync);
        } else {
            self.tracker().record(FaultPoint::CrashAfterSync);
        }
        let (crashed_fs, stats) = self.fs.crashed_with_stats(mode, &mut self.crash_rng);
        if stats.torn_writes > 0 {
            self.tracker().record(FaultPoint::CrashTornWrite);
        }
        if stats.torn_4k_blocks > 0 {
            self.tracker().record(FaultPoint::CrashTorn4kBlock);
        }
        self.fs = Arc::new(crashed_fs);

        // Detect valid fallback when the other manifest slot exists and is corrupt/torn.
        let mut invalid_slot = false;
        let mut valid_slot = false;
        for s in [0usize, 1] {
            if let Some(f) = self.fs.file(&slot_path(&self.dir, s)) {
                let bytes = f.contents();
                if Manifest::decode(&bytes).is_some() {
                    valid_slot = true;
                } else {
                    invalid_slot = true;
                }
            }
        }
        if invalid_slot && valid_slot && read_manifest(&*self.fs, &self.dir).map_err(|e| format!("{:?}", e))?.is_some() {
            self.tracker().record(FaultPoint::MetaManifestTornFallback);
        }

        let meta = MetaStore::open(Arc::clone(&self.fs), &self.dir, Arc::clone(&self.work))
            .map_err(|e| format!("post-crash MetaStore::open failed: {:?}", e))?;
        named_meta_files_only(&self.fs, &self.dir, &meta)?;

        let (volumes, rec_live, rec_values) = scan_full_volumes(
            &self.fs,
            &self.dir,
            self.max_span,
            Arc::clone(&self.clock),
            &self.work,
        )?;
        let (meta_state, tail_memtable) = replay_meta_and_tail(&meta, &volumes)?;

        let full_state: BTreeMap<u8, (u64, u32, u64)> = rec_live
            .iter()
            .map(|(&k, e)| (k, (e.vol_offset, e.value.0, e.value.1)))
            .collect();
        if meta_state != full_state {
            return Err(format!(
                "joint recovery divergence (replay_from={:?}): full_scan={:?}, meta_plus_tail={:?}",
                meta.manifest().replay_from,
                full_state,
                meta_state
            ));
        }

        self.model.check(&rec_values)?;
        for key in 0..JOINT_KEY_SPACE {
            let found = rec_values.get(&key).copied().flatten();
            if self.model.survival(key, found) == Survives::No {
                return Err(format!("key {} survival check returned No for {:?}", key, found));
            }
        }
        self.model.after_recovery(&rec_values);

        self.volumes = volumes;
        self.active_id = *self.volumes.keys().next_back().unwrap();
        self.meta = meta;
        self.memtable = tail_memtable;
        self.live = rec_live;
        self.writes_since_sync = false;

        self.push_u8(28);
        self.push_u64(rec_values.len() as u64);
        for (&k, &v) in &rec_values {
            self.push_u8(k);
            match v {
                Some((l, f)) => {
                    self.push_u8(1);
                    self.push_u64(l as u64);
                    self.push_u64(f);
                }
                None => self.push_u8(0),
            }
        }
        Ok(())
    }

    fn step(&mut self, op: &JointOp) -> Result<(), String> {
        self.clock.advance(1_000_000);
        match op {
            JointOp::Put { key, len, fill } => {
                self.exec_put(*key, *len, *fill);
                Ok(())
            }
            JointOp::Delete { key } => {
                self.exec_delete(*key);
                Ok(())
            }
            JointOp::Sync => self.exec_sync(),
            JointOp::FlushMeta => self.exec_flush_meta(),
            JointOp::CompactMeta { all } => self.exec_compact_meta(*all),
            JointOp::CompactVolume { seal_if_single } => self.exec_compact_volume(*seal_if_single),
            JointOp::VerifyLookup { key } => self.exec_verify_lookup(*key),
            JointOp::Fault { file, op, fault } => {
                self.exec_fault(*file, *op, *fault);
                Ok(())
            }
            JointOp::ProbeReadFault { file, fault } => self.exec_probe_read_fault(*file, *fault),
            JointOp::Crash { mode } => self.exec_crash(*mode),
        }
    }
}

fn run_joint_ops(
    seed: u64,
    max_span: u64,
    ops: &[JointOp],
    global_tracker: Option<&FaultTracker>,
) -> Result<(u64, Vec<u8>), String> {
    let mut harness = JointHarness::new(seed, max_span)?;
    for (idx, op) in ops.iter().enumerate() {
        if let Err(err) = harness.step(op) {
            return Err(format!("step {} ({:?}): {}", idx, op, err));
        }
    }
    if let Some(gt) = global_tracker {
        gt.merge(&harness.tracker());
    }
    let hash = xxh3_64(&harness.trace);
    Ok((hash, harness.trace))
}

#[test]
fn seeded_joint_simulation_matches_model_and_covers_both_layers() {
    let seeds = seed_budget();
    let coverage = FaultTracker::new();
    let workers = std::thread::available_parallelism()
        .map(|n| n.get().clamp(1, 8))
        .unwrap_or(1)
        .min(seeds.max(1) as usize);
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
                    let max_span = joint_max_span(seed);
                    let ops = generate_joint(seed, OPS_PER_SEED);
                    if let Err(err) = run_joint_ops(seed, max_span, &ops, Some(&local_cov)) {
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
        let max_span = joint_max_span(seed);
        let ops = generate_joint(seed, OPS_PER_SEED);
        let shrunk = shrink_joint(&ops, |cand| run_joint_ops(seed, max_span, cand, None).is_err());
        let shrunk_err = run_joint_ops(seed, max_span, &shrunk, None).unwrap_err();
        panic!(
            "joint simulation failed on seed {} (AWAN_SIM_JOINT_SEEDS={}):\n  full error: {}\n  shrunk ({} ops): {:?}\n  shrunk error: {}",
            seed,
            seeds,
            err,
            shrunk.len(),
            shrunk,
            shrunk_err
        );
    }

    if seeds >= 200 {
        let missing = coverage.missing(FaultPoint::JOINT_ALL);
        let snap = coverage.snapshot();
        if !missing.is_empty() {
            let names: Vec<&str> = missing.iter().map(|f| f.name()).collect();
            panic!(
                "joint fault coverage missing {} points over {} seeds: {:?}\nsnapshot: {:?}",
                names.len(),
                seeds,
                names,
                snap
            );
        }
        let summary: Vec<String> = FaultPoint::JOINT_ALL
            .iter()
            .map(|fp| format!("{}={}", fp.name(), snap.get(fp).copied().unwrap_or(0)))
            .collect();
        println!(
            "sim_joint_storage_test: {} seeds passed across {} worker(s); {}/{} fault points covered: {}",
            seeds,
            workers,
            FaultPoint::JOINT_ALL.len(),
            FaultPoint::JOINT_ALL.len(),
            summary.join(", ")
        );
    }
}

#[test]
fn determinism_same_seed_produces_identical_joint_trace_hashes() {
    let mut hashes = Vec::new();
    for seed in [0u64, 1, 7, 42, 213, 399] {
        let max_span = joint_max_span(seed);
        let ops = generate_joint(seed, OPS_PER_SEED);
        let (h1, t1) = run_joint_ops(seed, max_span, &ops, None).expect("first run");
        let (h2, t2) = run_joint_ops(seed, max_span, &ops, None).expect("second run");
        assert_eq!(t1, t2, "seed {} must produce byte-for-byte identical traces", seed);
        assert_eq!(h1, h2, "seed {} must produce identical trace hashes", seed);
        hashes.push(h1);
    }
    hashes.sort_unstable();
    hashes.dedup();
    assert!(hashes.len() >= 5, "distinct seeds must produce distinct traces: {:?}", hashes);
}

#[test]
fn scenario_compact_then_crash_and_manifest_torn_fallback() {
    let mut ops = Vec::new();
    for i in 0..36u8 {
        ops.push(JointOp::Put {
            key: i,
            len: 4096,
            fill: 100 + i as u64,
        });
        if (i + 1) % 12 == 0 {
            ops.push(JointOp::FlushMeta);
        }
    }
    ops.push(JointOp::CompactMeta { all: true });
    ops.push(JointOp::Delete { key: 5 });
    ops.push(JointOp::Fault {
        file: JointFileKind::Manifest,
        op: SimOp::Sync,
        fault: SimFault::Eio,
    });
    ops.push(JointOp::FlushMeta);
    ops.push(JointOp::Crash {
        mode: CrashMode::SeededPrefix,
    });
    ops.push(JointOp::VerifyLookup { key: 5 });
    ops.push(JointOp::VerifyLookup { key: 6 });

    let tracker = FaultTracker::new();
    run_joint_ops(408, 128 * 1024, &ops, Some(&tracker)).expect("compact + torn manifest crash must recover");
    assert!(tracker.count(FaultPoint::MetaCompactL0ToL1) >= 1);
    assert!(tracker.count(FaultPoint::ManifestSyncEio) >= 1);
}

#[test]
fn io_amplification_bounds_hold_for_writes_recovery_and_compaction() {
    let dir = PathBuf::from("sim/io_budget");
    let clock = Arc::new(ManualClock::new(1_800_000_000_000_000_000));
    let work = Arc::new(WorkCounters::default());
    let fs = Arc::new(SimFs::new());
    let max_span = 256 * 1024u64;

    let mut volumes: BTreeMap<u32, Arc<VolumeManager<SimFs>>> = BTreeMap::new();
    volumes.insert(1, open_sim_volume(&fs, &dir, 1, max_span, Arc::clone(&clock), &work).unwrap());
    let mut active_id = 1u32;
    let mut meta = MetaStore::open(Arc::clone(&fs), &dir, Arc::clone(&work)).unwrap();
    let mut memtable: BTreeMap<Vec<u8>, Entry> = BTreeMap::new();
    let mut live: BTreeMap<u8, LiveObj> = BTreeMap::new();

    // Phase A: 64 unbatched durable writes (4 batches x 16 writes flushed to MetaStore).
    const TOTAL_WRITES: u64 = 64;
    const FLUSH_BATCH: u64 = 16;
    let work_a0 = work.snapshot();
    for i in 0..TOTAL_WRITES {
        let k = (i % 32) as u8;
        let payload = payload_bytes(8192, 1000 + i);
        let key_str = key_name(k);
        let loc = match volumes[&active_id].write_needle_with_attrs(
            BUCKET_ID,
            &key_str,
            &[],
            &payload,
            AWAN_FLAG_ACTIVE,
            1000 + i,
            8192,
            None,
        ) {
            Ok(l) => l,
            Err(VolumeError::VolumeFull { .. }) => {
                active_id += 1;
                volumes.insert(
                    active_id,
                    open_sim_volume(&fs, &dir, active_id, max_span, Arc::clone(&clock), &work).unwrap(),
                );
                volumes[&active_id]
                    .write_needle_with_attrs(BUCKET_ID, &key_str, &[], &payload, AWAN_FLAG_ACTIVE, 1000 + i, 8192, None)
                    .unwrap()
            }
            Err(e) => panic!("Phase A write failed: {:?}", e),
        };
        volumes[&active_id].sync_now().unwrap();
        let g = global_offset(active_id, loc.offset);
        live.insert(k, LiveObj { vol_offset: g, value: (8192, 1000 + i), crc: loc.payload_crc32c });
        memtable.insert(sort_key_for(k), Entry::Row(meta_from_put(g, 8192, 1000 + i, clock.now_ns())));

        if (i + 1) % FLUSH_BATCH == 0 {
            let rf = ReplayFrom { volume_id: active_id, offset: volumes[&active_id].offset() };
            let es: Vec<SegEntry> = memtable.iter().map(|(sk, &e)| SegEntry { key: sk.clone(), entry: e }).collect();
            meta.flush(&es, rf, clock.now_ns()).unwrap();
            memtable.clear();
        }
    }
    let work_a = work.snapshot().since(&work_a0);
    let vol_syncs = fs.total_calls(SimOp::Sync, |p| {
        let s = p.to_string_lossy();
        s.ends_with(".dat") || s.ends_with(".idx")
    });
    let all_syncs = fs.total_calls(SimOp::Sync, |_| true);
    assert_eq!(vol_syncs, work_a.disk_syncs, "volume sync count must match WorkCounters");
    let vol_syncs_per_write = vol_syncs as f64 / TOTAL_WRITES as f64;
    let joint_syncs_per_write = all_syncs as f64 / TOTAL_WRITES as f64;
    assert!(
        vol_syncs_per_write <= MAX_VOL_SYNCS_PER_ACK_WRITE,
        "vol syncs/write {:.3} exceeded {}",
        vol_syncs_per_write,
        MAX_VOL_SYNCS_PER_ACK_WRITE
    );
    assert!(
        joint_syncs_per_write <= MAX_JOINT_SYNCS_PER_ACK_WRITE,
        "joint syncs/write {:.3} exceeded {}",
        joint_syncs_per_write,
        MAX_JOINT_SYNCS_PER_ACK_WRITE
    );

    // Phase B: Tail writes after checkpoint, then clean recovery scan reads check.
    let mut tail_expected = 0u64;
    for t in 0..8u64 {
        let k = (40 + t) as u8;
        let payload = payload_bytes(4096, 2000 + t);
        let key_str = key_name(k);
        let loc = match volumes[&active_id].write_needle_with_attrs(
            BUCKET_ID,
            &key_str,
            &[],
            &payload,
            AWAN_FLAG_ACTIVE,
            2000 + t,
            4096,
            None,
        ) {
            Ok(l) => l,
            Err(VolumeError::VolumeFull { .. }) => {
                active_id += 1;
                volumes.insert(
                    active_id,
                    open_sim_volume(&fs, &dir, active_id, max_span, Arc::clone(&clock), &work).unwrap(),
                );
                volumes[&active_id]
                    .write_needle_with_attrs(BUCKET_ID, &key_str, &[], &payload, AWAN_FLAG_ACTIVE, 2000 + t, 4096, None)
                    .unwrap()
            }
            Err(e) => panic!("Phase B tail write failed: {:?}", e),
        };
        tail_expected += loc.total_len;
        volumes[&active_id].sync_now().unwrap();
    }
    let tail_vol_bytes = tail_expected.max(1);
    let snap = Arc::new(fs.crashed(CrashMode::DropUnsynced, &mut SimRng::new(0)));
    let rec_work = Arc::new(WorkCounters::default());
    let rec_meta = MetaStore::open(Arc::clone(&snap), &dir, Arc::clone(&rec_work)).unwrap();
    let meta_open_reads = snap.total_calls(SimOp::Read, |p| {
        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
        name.starts_with("manifest.") || name.ends_with(".seg")
    });
    let live_segs = rec_meta.manifest().segments.len() as u64;
    assert!(
        meta_open_reads <= 2 + live_segs * MAX_META_OPEN_READS_PER_SEG,
        "meta open reads {} exceeded {}",
        meta_open_reads,
        2 + live_segs * MAX_META_OPEN_READS_PER_SEG
    );

    let vol_ids = volume_ids_in_fs(&snap, &dir);
    let mut rec_vols = BTreeMap::new();
    for id in &vol_ids {
        rec_vols.insert(*id, open_sim_volume(&snap, &dir, *id, max_span, Arc::clone(&clock), &rec_work).unwrap());
    }
    let vol_open_reads = snap.total_calls(SimOp::Read, |p| {
        let s = p.to_string_lossy();
        s.ends_with(".dat") || s.ends_with(".idx")
    });
    assert!(
        vol_open_reads <= (vol_ids.len() as u64) * MAX_OPEN_READS_PER_VOLUME_MIB,
        "volume open reads {} exceeded {} for {} volumes",
        vol_open_reads,
        (vol_ids.len() as u64) * MAX_OPEN_READS_PER_VOLUME_MIB,
        vol_ids.len()
    );

    let rf = rec_meta.manifest().replay_from;
    let tail_work_0 = rec_work.snapshot();
    let mut replayed = 0u64;
    for (&vid, vm) in &rec_vols {
        if vid < rf.volume_id {
            continue;
        }
        vm.scan_needles(|n| {
            if vid > rf.volume_id || n.offset >= rf.offset {
                replayed += 1;
            }
        })
        .unwrap();
    }
    assert_eq!(replayed, 8, "checkpoint + tail replay must recover all 8 post-flush keys");
    let tail_scan_reads = rec_work.snapshot().since(&tail_work_0).disk_reads;
    let tail_mibs = tail_vol_bytes.div_ceil(1 << 20);
    assert!(
        tail_scan_reads <= tail_mibs * MAX_TAIL_SCAN_READS_PER_MIB,
        "tail scan reads {} exceeded {} ({} MiB)",
        tail_scan_reads,
        tail_mibs * MAX_TAIL_SCAN_READS_PER_MIB,
        tail_mibs
    );

    // Phase C1: Volume compaction rewrite amplification (16 x 8 KiB objects, delete 8 -> 50% live).
    let fs_c1 = Arc::new(SimFs::new());
    let work_c1 = Arc::new(WorkCounters::default());
    let dir_c1 = PathBuf::from("sim/io_vol_compact");
    let v1 = open_sim_volume(&fs_c1, &dir_c1, 1, 512 * 1024, Arc::clone(&clock), &work_c1).unwrap();
    let v2 = open_sim_volume(&fs_c1, &dir_c1, 2, 512 * 1024, Arc::clone(&clock), &work_c1).unwrap();
    let mut live_c1: BTreeMap<u8, (u64, u64)> = BTreeMap::new();
    for i in 0..16u8 {
        let p = payload_bytes(8192, 500 + i as u64);
        let loc = v1
            .write_needle_with_attrs(BUCKET_ID, &key_name(i), &[], &p, AWAN_FLAG_ACTIVE, 500 + i as u64, 8192, None)
            .unwrap();
        live_c1.insert(i, (loc.offset, loc.total_len));
    }
    for i in 0..8u8 {
        v1.write_needle_with_attrs(BUCKET_ID, &key_name(i), &[], &[], AWAN_FLAG_TOMBSTONE, 0, 0, None)
            .unwrap();
        live_c1.remove(&i);
    }
    v1.sync_now().unwrap();
    let expected_live_needle_bytes: u64 = live_c1.values().map(|(_, tl)| *tl).sum();
    let expected_live_payload_bytes: u64 = (live_c1.len() as u64) * 8192;

    let work_c1_0 = work_c1.snapshot();
    let v2_written_0 = fs_c1.file(&data_path(&dir_c1, 2)).unwrap().bytes_written();
    v1.seal();
    v1.scan_needles(|n| {
        if n.flags & AWAN_FLAG_TOMBSTONE != 0 {
            return;
        }
        let k = parse_key(&n.key).unwrap();
        if live_c1.get(&k).is_some_and(|&(off, _)| off == n.offset) {
            let bytes = read_needle_payload(&v1, n.offset, n.payload_crc32c).unwrap();
            v2.write_needle_with_attrs(BUCKET_ID, &n.key, &[], &bytes, n.flags, n.etag_hi, n.etag_lo, Some(n.created_at_ns))
                .unwrap();
        }
    })
    .unwrap();
    v2.sync_now().unwrap();
    let vol_rewrite_work = work_c1.snapshot().since(&work_c1_0).disk_write_bytes;
    let vol_rewrite_file = fs_c1.file(&data_path(&dir_c1, 2)).unwrap().bytes_written() - v2_written_0;
    assert_eq!(vol_rewrite_work, vol_rewrite_file);
    let vol_amp_needle = vol_rewrite_file as f64 / expected_live_needle_bytes as f64;
    let vol_amp_payload = vol_rewrite_file as f64 / expected_live_payload_bytes as f64;
    assert!(
        vol_amp_needle <= MAX_VOL_COMPACTION_REWRITE_PER_LIVE_NEEDLE_BYTE,
        "vol compaction needle rewrite {:.3}x exceeded {}",
        vol_amp_needle,
        MAX_VOL_COMPACTION_REWRITE_PER_LIVE_NEEDLE_BYTE
    );
    assert!(
        vol_amp_payload <= MAX_VOL_COMPACTION_REWRITE_PER_LIVE_PAYLOAD_BYTE,
        "vol compaction payload rewrite {:.3}x exceeded {}",
        vol_amp_payload,
        MAX_VOL_COMPACTION_REWRITE_PER_LIVE_PAYLOAD_BYTE
    );

    // Phase C2: Metadata leveled compaction rewrite amplification across 8 flushes into deeper levels.
    let fs_c2 = Arc::new(SimFs::new());
    let work_c2 = Arc::new(WorkCounters::default());
    let dir_c2 = PathBuf::from("sim/io_meta_compact");
    let mut meta_c2 = MetaStore::open(Arc::clone(&fs_c2), &dir_c2, Arc::clone(&work_c2)).unwrap();
    let meta_policy = Policy {
        l0_trigger: 3,
        level1_bytes: 20 * 1024,
        ratio: 3,
        segment_bytes: 16 * 1024,
        max_level: 4,
    };
    let mut compact_written = 0u64;
    let mut flush_seqs: BTreeSet<u64> = BTreeSet::new();
    for round in 0..8u64 {
        let mut batch = BTreeMap::new();
        for idx in 0..64u64 {
            let k_idx = (round * 24 + idx) % 192;
            let sk = sort_key(u32::from(BUCKET_ID), format!("mk/{:06}", k_idx).as_bytes());
            let entry = if idx % 6 == 5 && round > 0 {
                Entry::Tombstone
            } else {
                Entry::Row(meta_from_put(4096 * (round * 64 + idx + 1), 4096, round * 1000 + idx, 10))
            };
            batch.insert(sk, entry);
        }
        let entries: Vec<SegEntry> = batch.into_iter().map(|(key, entry)| SegEntry { key, entry }).collect();
        let seq = meta_c2
            .flush(&entries, ReplayFrom { volume_id: 1, offset: (round + 1) * 4096 }, round + 1)
            .unwrap();
        flush_seqs.insert(seq);
        let prev_next = meta_c2.manifest().next_seq;
        let _ = meta_c2.compact_all(&meta_policy, round + 1).unwrap();
        let cur_next = meta_c2.manifest().next_seq;
        for out_seq in prev_next..cur_next {
            let p = segment_path(&dir_c2, out_seq);
            if let Some(f) = fs_c2.file(&p) {
                compact_written += f.bytes_written();
            }
        }
    }
    let live_meta_bytes: u64 = meta_c2
        .segments()
        .levels()
        .iter()
        .flatten()
        .map(|s| s.bytes)
        .sum::<u64>()
        .max(1);
    let meta_amp = compact_written as f64 / live_meta_bytes as f64;
    assert!(
        meta_amp <= MAX_META_COMPACTION_REWRITE_PER_LIVE_BYTE,
        "meta compaction rewrite {:.3}x ({} / {}) exceeded {}",
        meta_amp,
        compact_written,
        live_meta_bytes,
        MAX_META_COMPACTION_REWRITE_PER_LIVE_BYTE
    );

    println!(
        "io_amplification_bounds: vol_syncs/w={:.3} (<= {}), joint_syncs/w={:.3} (<= {}), tail_reads={} (<= {}), meta_open_reads={}, vol_open_reads={}, vol_rewrite={:.3}x_needle/{:.3}x_payload, meta_rewrite={:.3}x ({}/{} B)",
        vol_syncs_per_write,
        MAX_VOL_SYNCS_PER_ACK_WRITE,
        joint_syncs_per_write,
        MAX_JOINT_SYNCS_PER_ACK_WRITE,
        tail_scan_reads,
        tail_mibs * MAX_TAIL_SCAN_READS_PER_MIB,
        meta_open_reads,
        vol_open_reads,
        vol_amp_needle,
        vol_amp_payload,
        meta_amp,
        compact_written,
        live_meta_bytes,
    );
}
