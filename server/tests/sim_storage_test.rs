//! #213: end-to-end deterministic simulation of the AwanFS storage core over `VolumeManager<SimFs>`,
//! `sim_gen`, and `sim_model`.
//!
//! Exercises buffered PUTs, overwrites, DELETE tombstones, CopyObject, streamed PUTs interleaved
//! with concurrent same-key writes (relocation check), multi-chunk >1 MiB streams, abandoned
//! stream gaps, multipart manifests/extents, online compaction, volume rollover, injected EIO/ENOSPC
//! faults on data and journal files, `DropUnsynced` and `SeededPrefix` crashes (with torn 4 KiB
//! blocks), and recovery restarts checked against `SimModel`.

use s3_server::clock::{Clock, ManualClock};
use s3_server::compute_md5;
use s3_server::ffi_bridge::{AWAN_FLAG_ACTIVE, AWAN_FLAG_MULTIPART, AWAN_FLAG_TOMBSTONE, AWAN_FLAG_ZERO_BYTE};
use s3_server::multipart::{decode_extents_manifest, encode_extents_manifest, PartExtent};
use s3_server::s3_api::{StreamTicket, StreamsInFlight};
use s3_server::sim::{CrashMode, FaultPoint, FaultTracker, SimFault, SimFs, SimOp, SimRng};
use s3_server::sim_gen::{
    generate_storage, shrink_storage, storage_max_span, SimFileKind, SimOperation, StorageOp,
};
use s3_server::sim_model::{Outcome, SimModel, Survives};
use s3_server::vfile::{FileKind, VolFile, VolFs};
use s3_server::volume::{global_offset, NeedleLocation, NeedleWriter, ScannedNeedle, VolumeError, VolumeManager};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use xxhash_rust::xxh3::xxh3_64;

const KEY_SPACE: u8 = 8;
const OPS_PER_SEED: usize = 64;
const DEFAULT_CI_SEEDS: u64 = 2_000;
const SECTOR_SIZE: u64 = 4096;

fn seed_budget() -> u64 {
    std::env::var("AWAN_SIM_SEEDS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_CI_SEEDS)
}

fn key_name(key: u8) -> String {
    format!("k{}", key)
}

fn parse_key(name: &str) -> Option<u8> {
    name.strip_prefix('k')?.parse::<u8>().ok()
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
    clock: Arc<dyn Clock>,
) -> Result<Arc<VolumeManager<SimFs>>, VolumeError> {
    let vm = VolumeManager::open_with(fs, dir, id, false, clock)?;
    vm.set_max_span(max_span);
    vm.set_growth_step(SECTOR_SIZE);
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

fn read_multipart_payload(
    fs: &SimFs,
    staging_dir: &Path,
    vm: &VolumeManager<SimFs>,
    offset: u64,
    crc: u32,
) -> Result<(u32, Vec<u8>), String> {
    let manifest_bytes =
        read_needle_payload(vm, offset, crc).map_err(|e| format!("read multipart manifest failed: {:?}", e))?;
    let (total, extents) = decode_extents_manifest(staging_dir, &manifest_bytes)
        .map_err(|e| format!("decode multipart manifest failed: {:?}", e))?;
    let mut full = Vec::with_capacity(total as usize);
    for ext in extents {
        let Some(path) = ext.staged_path.as_ref() else {
            return Err("multipart extent missing staged_path".to_string());
        };
        let file = fs
            .open(path, FileKind::Segment, false)
            .map_err(|e| format!("open staged part {}: {}", path.display(), e))?;
        let mut part_buf = vec![0u8; ext.part_size as usize];
        file.read_at(0, &mut part_buf)
            .map_err(|e| format!("read staged part {}: {}", path.display(), e))?;
        let (_, _, _, md5) = compute_md5(&part_buf);
        if md5 != ext.raw_md5 {
            return Err(format!("multipart part {} MD5 mismatch", path.display()));
        }
        full.extend_from_slice(&part_buf);
    }
    if full.len() as u64 != total {
        return Err(format!("multipart size mismatch: expected {}, got {}", total, full.len()));
    }
    Ok((total as u32, full))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LiveEntry {
    vol_offset: u64,
    value: (u32, u64),
    flags: u16,
    crc: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RecoveredSnapshot {
    values: BTreeMap<u8, Option<(u32, u64)>>,
    live: BTreeMap<u8, LiveEntry>,
    multipart_count: usize,
}

fn recover_volumes(
    fs: &SimFs,
    dir: &Path,
    staging_dir: &Path,
    max_span: u64,
    clock: Arc<dyn Clock>,
) -> Result<(BTreeMap<u32, Arc<VolumeManager<SimFs>>>, RecoveredSnapshot), String> {
    let mut ids = volume_ids_in_fs(fs, dir);
    if ids.is_empty() {
        ids.push(1);
    }
    let mut volumes = BTreeMap::new();
    let mut values: BTreeMap<u8, Option<(u32, u64)>> = BTreeMap::new();
    let mut live: BTreeMap<u8, LiveEntry> = BTreeMap::new();

    for id in ids {
        let vm = open_sim_volume(fs, dir, id, max_span, Arc::clone(&clock))
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
            let (len, fill, actual_bytes) = if n.flags & AWAN_FLAG_MULTIPART != 0 {
                let (total, full) = read_multipart_payload(fs, staging_dir, &vm, n.offset, n.payload_crc32c)?;
                (total, n.etag_hi, full)
            } else {
                let got = read_needle_payload(&vm, n.offset, n.payload_crc32c)
                    .map_err(|e| format!("read needle key={} off={}: {:?}", n.key, n.offset, e))?;
                if got.len() as u64 != n.payload_size {
                    return Err(format!(
                        "key {} length mismatch: header {}, read {}",
                        n.key,
                        n.payload_size,
                        got.len()
                    ));
                }
                (n.payload_size as u32, n.etag_hi, got)
            };
            let expected = payload_bytes(len, fill);
            if actual_bytes != expected {
                return Err(format!(
                    "key {} payload corruption at vol {} off {} (len={}, fill={})",
                    n.key, id, n.offset, len, fill
                ));
            }
            values.insert(k, Some((len, fill)));
            live.insert(
                k,
                LiveEntry {
                    vol_offset: global_offset(id, n.offset),
                    value: (len, fill),
                    flags: n.flags,
                    crc: n.payload_crc32c,
                },
            );
        }
        volumes.insert(id, vm);
    }

    let multipart_count = live
        .values()
        .filter(|e| e.flags & AWAN_FLAG_MULTIPART != 0)
        .count();

    Ok((
        volumes,
        RecoveredSnapshot {
            values,
            live,
            multipart_count,
        },
    ))
}

struct InFlightStream {
    key: u8,
    len: u32,
    fill: u64,
    writer: NeedleWriter<SimFs>,
    ticket: StreamTicket,
    written: usize,
    payload: Vec<u8>,
}

struct SimStorageHarness {
    dir: PathBuf,
    staging_dir: PathBuf,
    max_span: u64,
    clock: Arc<ManualClock>,
    fs: SimFs,
    volumes: BTreeMap<u32, Arc<VolumeManager<SimFs>>>,
    active_id: u32,
    live: BTreeMap<u8, LiveEntry>,
    streams: Arc<StreamsInFlight>,
    in_flight: BTreeMap<u32, InFlightStream>,
    model: SimModel,
    next_upload_id: u64,
    writes_since_sync: bool,
    crash_rng: SimRng,
    trace: Vec<u8>,
}

impl SimStorageHarness {
    fn new(seed: u64, max_span: u64) -> Result<Self, String> {
        let dir = PathBuf::from("sim/storage");
        let staging_dir = dir.join(".staging");
        let clock = Arc::new(ManualClock::new(1_800_000_000_000_000_000));
        let fs = SimFs::new();
        let vm = open_sim_volume(&fs, &dir, 1, max_span, clock.clone())
            .map_err(|e| format!("initial open failed: {:?}", e))?;
        let mut volumes = BTreeMap::new();
        volumes.insert(1, vm);
        Ok(Self {
            dir,
            staging_dir,
            max_span,
            clock,
            fs,
            volumes,
            active_id: 1,
            live: BTreeMap::new(),
            streams: Arc::new(StreamsInFlight::default()),
            in_flight: BTreeMap::new(),
            model: SimModel::new(),
            next_upload_id: 1,
            writes_since_sync: false,
            crash_rng: SimRng::new(seed ^ 0xA5A5_5A5A_1234_5678),
            trace: Vec::new(),
        })
    }

    fn tracker(&self) -> Arc<FaultTracker> {
        self.fs.fault_tracker()
    }

    fn push_trace_u8(&mut self, tag: u8) {
        self.trace.push(tag);
    }

    fn push_trace_u64(&mut self, v: u64) {
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
        let vm = open_sim_volume(&self.fs, &self.dir, next_id, self.max_span, self.clock.clone())?;
        self.volumes.insert(next_id, vm);
        self.active_id = next_id;
        self.tracker().record(FaultPoint::VolumeRollover);
        Ok(())
    }

    fn write_to_active_at(
        &mut self,
        key: u8,
        payload: &[u8],
        flags: u16,
        etag_hi: u64,
        etag_lo: u64,
        created_at: Option<u64>,
    ) -> Result<(Arc<VolumeManager<SimFs>>, NeedleLocation, u64), VolumeError> {
        let key_str = key_name(key);
        let mut attempts = 0;
        loop {
            let vm = self.active_vm();
            match vm.write_needle_with_attrs(1, &key_str, &[], payload, flags, etag_hi, etag_lo, created_at) {
                Ok(loc) => {
                    let g = global_offset(vm.id(), loc.offset);
                    self.streams.note_append(1, &key_str, g);
                    return Ok((vm, loc, g));
                }
                Err(VolumeError::VolumeFull { len, .. }) if len + SECTOR_SIZE > vm.max_span() => {
                    return Err(VolumeError::VolumeFull { offset: 0, len });
                }
                Err(VolumeError::VolumeFull { .. }) if attempts < 8 => {
                    attempts += 1;
                    self.roll_volume(vm.id())?;
                }
                Err(VolumeError::AfterRecord { loc, flags, cause }) => {
                    let g = global_offset(loc.volume_id, loc.offset);
                    self.streams.note_append(1, &key_str, g);
                    return Err(VolumeError::AfterRecord { loc, flags, cause });
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn begin_needle_on_active(
        &mut self,
        key: u8,
        len: u64,
        flags: u16,
    ) -> Result<NeedleWriter<SimFs>, VolumeError> {
        let key_str = key_name(key);
        let mut attempts = 0;
        loop {
            let vm = self.active_vm();
            match vm.begin_needle_with_attrs(1, &key_str, &[], len, flags) {
                Ok(w) => return Ok(w),
                Err(VolumeError::VolumeFull { len, .. }) if len + SECTOR_SIZE > vm.max_span() => {
                    return Err(VolumeError::VolumeFull { offset: 0, len });
                }
                Err(VolumeError::VolumeFull { .. }) if attempts < 8 => {
                    attempts += 1;
                    self.roll_volume(vm.id())?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn relocate_needle(
        &mut self,
        key: u8,
        from_vm: &Arc<VolumeManager<SimFs>>,
        from: &NeedleLocation,
        flags: u16,
        etag_hi: u64,
        etag_lo: u64,
    ) -> Result<(Arc<VolumeManager<SimFs>>, NeedleLocation, u64), VolumeError> {
        let mut writer = self.begin_needle_on_active(key, from.payload_size, flags)?;
        from_vm.copy_payload(from.offset, from.payload_crc32c, |piece| writer.write(piece))?;
        let vm = Arc::clone(writer.volume());
        let g = global_offset(vm.id(), writer.offset());
        let key_str = key_name(key);
        let (loc, late) = match writer.finish(etag_hi, etag_lo) {
            Ok(loc) => (loc, None),
            Err(VolumeError::AfterRecord { loc, flags, cause }) => {
                (loc, Some(VolumeError::AfterRecord { loc, flags, cause }))
            }
            Err(e) => return Err(e),
        };
        self.streams.note_append(1, &key_str, g);
        match late {
            Some(e) => Err(e),
            None => Ok((vm, loc, g)),
        }
    }

    fn read_live_bytes(&self, entry: &LiveEntry) -> Result<Vec<u8>, VolumeError> {
        let (vol_id, local) = s3_server::volume::split_offset(entry.vol_offset);
        let vm = self.volumes.get(&vol_id).ok_or(VolumeError::CorruptIndex)?;
        if entry.flags & AWAN_FLAG_MULTIPART != 0 {
            let (_, full) = read_multipart_payload(&self.fs, &self.staging_dir, vm, local, entry.crc)
                .map_err(|msg| VolumeError::Io(std::io::Error::new(std::io::ErrorKind::Other, msg)))?;
            Ok(full)
        } else {
            read_needle_payload(vm, local, entry.crc)
        }
    }

    fn apply_put_result(
        &mut self,
        key: u8,
        len: u32,
        fill: u64,
        flags: u16,
        res: Result<(Arc<VolumeManager<SimFs>>, NeedleLocation, u64), VolumeError>,
    ) {
        let op = SimOperation::Put { key, len, fill };
        match res {
            Ok((_vm, loc, g)) => {
                self.writes_since_sync = true;
                self.live.insert(
                    key,
                    LiveEntry {
                        vol_offset: g,
                        value: (len, fill),
                        flags,
                        crc: loc.payload_crc32c,
                    },
                );
                self.model.record(&op, Outcome::Ok);
                self.push_trace_u8(1);
                self.push_trace_u64(g);
            }
            Err(VolumeError::AfterRecord { loc, flags: f, .. }) => {
                let g = global_offset(loc.volume_id, loc.offset);
                self.writes_since_sync = true;
                self.tracker().record(FaultPoint::LateFailJournalAfterRecord);
                // #184 Option B (Decision 62): a needle whole on disk updates the live index.
                self.live.insert(
                    key,
                    LiveEntry {
                        vol_offset: g,
                        value: (len, fill),
                        flags: f,
                        crc: loc.payload_crc32c,
                    },
                );
                self.model.record(&op, Outcome::Failed);
                self.push_trace_u8(2);
                self.push_trace_u64(g);
            }
            Err(_) => {
                self.model.record(&op, Outcome::Failed);
                self.push_trace_u8(3);
            }
        }
    }

    fn exec_put(&mut self, key: u8, len: u32, fill: u64) {
        let payload = payload_bytes(len, fill);
        let flags = if len == 0 {
            AWAN_FLAG_ACTIVE | AWAN_FLAG_ZERO_BYTE
        } else {
            AWAN_FLAG_ACTIVE
        };
        let res = self.write_to_active_at(key, &payload, flags, fill, len as u64, None);
        self.apply_put_result(key, len, fill, flags, res);
    }

    fn exec_delete(&mut self, key: u8) {
        let res = self.write_to_active_at(key, &[], AWAN_FLAG_TOMBSTONE, 0, 0, None);
        let op = SimOperation::Delete { key };
        match res {
            Ok((_vm, _loc, g)) => {
                self.writes_since_sync = true;
                self.live.remove(&key);
                self.model.record(&op, Outcome::Ok);
                self.push_trace_u8(4);
                self.push_trace_u64(g);
            }
            Err(VolumeError::AfterRecord { loc, .. }) => {
                self.writes_since_sync = true;
                self.tracker().record(FaultPoint::LateFailJournalAfterRecord);
                // #184 Option B (Decision 62): tombstone whole on disk deletes from the live index.
                self.live.remove(&key);
                self.model.record(&op, Outcome::Failed);
                self.push_trace_u8(5);
                self.push_trace_u64(global_offset(loc.volume_id, loc.offset));
            }
            Err(_) => {
                self.model.record(&op, Outcome::Failed);
                self.push_trace_u8(6);
            }
        }
    }

    fn exec_copy(&mut self, src_key: u8, dst_key: u8) {
        let Some(entry) = self.live.get(&src_key).cloned() else {
            self.push_trace_u8(7);
            return;
        };
        let bytes = match self.read_live_bytes(&entry) {
            Ok(b) => b,
            Err(_) => {
                self.push_trace_u8(8);
                return;
            }
        };
        let (len, fill) = entry.value;
        let flags = if len == 0 {
            AWAN_FLAG_ACTIVE | AWAN_FLAG_ZERO_BYTE
        } else {
            AWAN_FLAG_ACTIVE
        };
        let res = self.write_to_active_at(dst_key, &bytes, flags, fill, len as u64, None);
        self.apply_put_result(dst_key, len, fill, flags, res);
    }

    fn exec_stream_start(&mut self, stream_id: u32, key: u8, len: u32, fill: u64) {
        let payload = payload_bytes(len, fill);
        let flags = if len == 0 {
            AWAN_FLAG_ACTIVE | AWAN_FLAG_ZERO_BYTE
        } else {
            AWAN_FLAG_ACTIVE
        };
        let key_str = key_name(key);
        let ticket = self.streams.enter(1, &key_str);
        let mut writer = match self.begin_needle_on_active(key, len as u64, flags) {
            Ok(w) => w,
            Err(_) => {
                self.push_trace_u8(9);
                return;
            }
        };
        let first_split = if payload.len() > s3_server::STREAM_CHUNK {
            s3_server::STREAM_CHUNK + 512
        } else {
            payload.len() / 2
        }
        .min(payload.len());
        if first_split > 0 && writer.write(&payload[..first_split]).is_err() {
            self.push_trace_u8(10);
            return;
        }
        self.in_flight.insert(
            stream_id,
            InFlightStream {
                key,
                len,
                fill,
                writer,
                ticket,
                written: first_split,
                payload,
            },
        );
        self.push_trace_u8(11);
    }

    fn exec_stream_finish(&mut self, stream_id: u32) {
        let Some(mut st) = self.in_flight.remove(&stream_id) else {
            self.push_trace_u8(12);
            return;
        };
        let op = SimOperation::Put {
            key: st.key,
            len: st.len,
            fill: st.fill,
        };
        let key_str = key_name(st.key);
        if st.written < st.payload.len() && st.writer.write(&st.payload[st.written..]).is_err() {
            self.model.record(&op, Outcome::Failed);
            self.push_trace_u8(13);
            return;
        }
        let first_vm = Arc::clone(st.writer.volume());
        let first_offset = global_offset(first_vm.id(), st.writer.offset());
        let flags = if st.len == 0 {
            AWAN_FLAG_ACTIVE | AWAN_FLAG_ZERO_BYTE
        } else {
            AWAN_FLAG_ACTIVE
        };
        let (first_loc, late) = match st.writer.finish(st.fill, st.len as u64) {
            Ok(l) => (l, None),
            Err(VolumeError::AfterRecord { loc, flags, cause }) => {
                self.writes_since_sync = true;
                self.tracker().record(FaultPoint::LateFailJournalAfterRecord);
                (loc, Some(VolumeError::AfterRecord { loc, flags, cause }))
            }
            Err(_) => {
                self.model.record(&op, Outcome::Failed);
                self.push_trace_u8(14);
                return;
            }
        };
        self.writes_since_sync = true;
        self.streams.note_append(1, &key_str, first_offset);
        let relocated = st.ticket.appended_after(first_offset);
        if relocated {
            if late.is_some() {
                // #184: a later write of the key already sits past first_offset; do not relocate.
                self.tracker().record(FaultPoint::StreamLateFailNoRelocate);
                self.model.record(&op, Outcome::Failed);
                self.push_trace_u8(15);
                return;
            }
            let res = self.relocate_needle(st.key, &first_vm, &first_loc, flags, st.fill, st.len as u64);
            if res.is_ok() || matches!(res, Err(VolumeError::AfterRecord { .. })) {
                self.tracker().record(FaultPoint::StreamRelocated);
            }
            self.apply_put_result(st.key, st.len, st.fill, flags, res);
        } else {
            let res = match late {
                Some(e) => Err(e),
                None => Ok((first_vm, first_loc, first_offset)),
            };
            self.apply_put_result(st.key, st.len, st.fill, flags, res);
        }
    }

    fn exec_stream_abort(&mut self, stream_id: u32) {
        if self.in_flight.remove(&stream_id).is_some() {
            self.tracker().record(FaultPoint::StreamAbandonedGap);
            self.push_trace_u8(16);
        } else {
            self.push_trace_u8(17);
        }
    }

    fn exec_multipart_put(&mut self, key: u8, parts: &[(u32, u64)], fill: u64) {
        let total_len: u64 = parts.iter().map(|(l, _)| *l as u64).sum();
        let full = payload_bytes(total_len as u32, fill);
        let uid = self.next_upload_id;
        self.next_upload_id += 1;
        let upload_dir = self.staging_dir.join(format!("upload-{:08x}", uid));

        let mut extents = Vec::with_capacity(parts.len());
        let mut cursor = 0usize;
        for (idx, (plen, _)) in parts.iter().enumerate() {
            let end = cursor + *plen as usize;
            let slice = &full[cursor..end];
            cursor = end;
            let p_path = upload_dir.join(format!("part_{:05}.dat", idx + 1));
            let Ok(f) = self.fs.open(&p_path, FileKind::Segment, true) else {
                self.push_trace_u8(18);
                return;
            };
            if f.write_at(0, slice).is_err() || f.sync_data().is_err() {
                self.push_trace_u8(18);
                return;
            }
            let (_, _, _, raw_md5) = compute_md5(slice);
            extents.push(PartExtent {
                vol_offset: 0,
                part_size: *plen as u64,
                cumulative_end: end as u64,
                ref_count: 1,
                raw_md5,
                staged_path: Some(p_path),
                checksum: None,
            });
        }
        let manifest = encode_extents_manifest(&self.staging_dir, total_len, &extents);
        let flags = AWAN_FLAG_ACTIVE | AWAN_FLAG_MULTIPART;
        let res = self.write_to_active_at(key, &manifest, flags, fill, total_len, None);
        self.apply_put_result(key, total_len as u32, fill, flags, res);
    }

    fn exec_compact(&mut self, seal_if_single: bool) {
        if self.volumes.len() == 1 && seal_if_single {
            if self.roll_volume(self.active_id).is_err() {
                self.push_trace_u8(19);
                return;
            }
        }
        if self.volumes.len() < 2 {
            self.push_trace_u8(20);
            return;
        }
        let old_id = *self.volumes.keys().next().unwrap();
        if old_id == self.active_id {
            self.push_trace_u8(20);
            return;
        }
        let old_vm = Arc::clone(&self.volumes[&old_id]);
        old_vm.seal();
        if old_vm.writers_in_flight() > 0 {
            self.tracker().record(FaultPoint::CompactionBusyInFlight);
            self.push_trace_u8(21);
            return;
        }
        let mut needles = Vec::new();
        if old_vm.scan_needles(|n| needles.push(n)).is_err() {
            self.push_trace_u8(22);
            return;
        }
        let mut copied = 0usize;
        for n in needles {
            if n.flags & AWAN_FLAG_TOMBSTONE != 0 {
                continue;
            }
            let Some(k) = parse_key(&n.key) else {
                continue;
            };
            let g_old = global_offset(old_id, n.offset);
            let Some(entry) = self.live.get(&k).cloned() else {
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
                        self.push_trace_u8(23);
                        return;
                    }
                }
            };
            match self.write_to_active_at(k, &bytes, n.flags, n.etag_hi, n.etag_lo, Some(n.created_at_ns)) {
                Ok((_vm, loc, g_new)) => {
                    self.live.insert(
                        k,
                        LiveEntry {
                            vol_offset: g_new,
                            value: entry.value,
                            flags: n.flags,
                            crc: loc.payload_crc32c,
                        },
                    );
                    copied += 1;
                }
                Err(VolumeError::AfterRecord { loc, flags: f, .. }) => {
                    let g_new = global_offset(loc.volume_id, loc.offset);
                    self.tracker().record(FaultPoint::LateFailJournalAfterRecord);
                    self.live.insert(
                        k,
                        LiveEntry {
                            vol_offset: g_new,
                            value: entry.value,
                            flags: f,
                            crc: loc.payload_crc32c,
                        },
                    );
                    copied += 1;
                }
                Err(_) => {
                    self.push_trace_u8(24);
                    return;
                }
            }
        }
        // Sync all remaining volumes before dropping the compacted source volume so live
        // overwrites/deletes in newer volumes are durable before old copies are removed.
        for (&id, vm) in &self.volumes {
            if id != old_id && vm.sync_now().is_err() {
                self.push_trace_u8(25);
                return;
            }
        }
        self.volumes.remove(&old_id);
        drop(old_vm);
        let _ = self.fs.remove(&data_path(&self.dir, old_id));
        let _ = self.fs.remove(&idx_path(&self.dir, old_id));
        if copied > 0 {
            self.tracker().record(FaultPoint::CompactionMovedLive);
        }
        self.push_trace_u8(26);
        self.push_trace_u64(copied as u64);
    }

    fn exec_verify_read(&mut self, key: u8) -> Result<(), String> {
        let Some(entry) = self.live.get(&key).cloned() else {
            self.push_trace_u8(27);
            return Ok(());
        };
        match self.read_live_bytes(&entry) {
            Ok(got) => {
                let (len, fill) = entry.value;
                if got != payload_bytes(len, fill) {
                    return Err(format!("live verify read mismatch on key {}", key));
                }
                self.push_trace_u8(28);
                Ok(())
            }
            Err(VolumeError::Io(_)) => {
                self.push_trace_u8(29);
                Ok(())
            }
            Err(e) => Err(format!("live verify read unexpected error on key {}: {:?}", key, e)),
        }
    }

    fn exec_sync(&mut self) -> Result<(), String> {
        let had_pending = self.volumes.values().any(|vm| vm.pending_journal_len() > 0);
        for vm in self.volumes.values() {
            if vm.sync_now().is_err() {
                if self.writes_since_sync {
                    self.tracker().record(FaultPoint::LateFailSyncAfterRecord);
                }
                self.model.record(&SimOperation::Sync, Outcome::Failed);
                self.push_trace_u8(30);
                return Ok(());
            }
        }
        if had_pending && self.volumes.values().all(|vm| vm.pending_journal_len() == 0) {
            self.tracker().record(FaultPoint::JournalRetryFlushed);
        }
        self.writes_since_sync = false;
        self.model.record(&SimOperation::Sync, Outcome::Ok);

        if std::env::var("AWAN_SIM_TRACE").ok().as_deref() == Some("1") {
            for (&id, vm) in &self.volumes {
                let dlen = self.fs.file(&data_path(&self.dir, id)).and_then(|f| f.len().ok()).unwrap_or(0);
                let jlen = self.fs.file(&idx_path(&self.dir, id)).and_then(|f| f.len().ok()).unwrap_or(0);
                eprintln!(
                    "  [after sync/crash] vol {} offset={} dat_len={} idx_len={} (idx%42={})",
                    id, vm.offset(), dlen, jlen, jlen % 42
                );
            }
        }
        // Live-vs-recovery check: once every volume synced, a clean DropUnsynced restart
        // must recover the exact same key states and byte values as `self.live`.
        let snap = self.fs.crashed(CrashMode::DropUnsynced, &mut SimRng::new(0));
        let (_, recovered) = recover_volumes(&snap, &self.dir, &self.staging_dir, self.max_span, self.clock.clone())?;
        let live_vals: BTreeMap<u8, (u32, u64)> = self.live.iter().map(|(&k, e)| (k, e.value)).collect();
        let rec_vals: BTreeMap<u8, (u32, u64)> = recovered
            .values
            .iter()
            .filter_map(|(&k, v)| v.map(|val| (k, val)))
            .collect();
        if live_vals != rec_vals {
            return Err(format!(
                "post-sync live vs clean-recovery divergence: live={:?}, recovered={:?}",
                live_vals, rec_vals
            ));
        }
        self.push_trace_u8(31);
        Ok(())
    }

    fn exec_fault(&mut self, file: SimFileKind, op: SimOp, fault: SimFault) {
        let target = match file {
            SimFileKind::Data => data_path(&self.dir, self.active_id),
            SimFileKind::Journal => idx_path(&self.dir, self.active_id),
        };
        let armed = self.fs.arm(&target, op, fault);
        // #412: a record is queued at the write and reaches the file at the next sync, where the
        // armed fault fires. The queueing step fails too, so the AfterRecord path stays covered.
        if armed && matches!(file, SimFileKind::Journal) && op == SimOp::Write {
            if let Some(vm) = self.volumes.get(&self.active_id) {
                vm.fail_next_index();
            }
        }
        self.push_trace_u8(if armed { 32 } else { 33 });
    }

    fn exec_probe_journal_read_fault(&mut self, fault: SimFault) -> Result<(), String> {
        let id = self.active_id;
        let idx = idx_path(&self.dir, id);
        let snap = self.fs.crashed(CrashMode::DropUnsynced, &mut SimRng::new(0));
        let len = snap.file(&idx).and_then(|f| f.len().ok()).unwrap_or(0);
        if len == 0 {
            self.push_trace_u8(34);
            return Ok(());
        }
        snap.arm(&idx, SimOp::Read, fault);
        let err = open_sim_volume(&snap, &self.dir, id, self.max_span, self.clock.clone());
        if err.is_ok() {
            return Err(format!("expected open_with to fail when journal read fault {:?} armed", fault));
        }
        open_sim_volume(&snap, &self.dir, id, self.max_span, self.clock.clone())
            .map_err(|e| format!("reopen after journal read fault failed: {:?}", e))?;
        self.push_trace_u8(35);
        Ok(())
    }

    fn exec_crash(&mut self, mode: CrashMode) -> Result<(), String> {
        self.in_flight.clear();
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
        self.fs = crashed_fs;

        let (volumes, recovered) =
            recover_volumes(&self.fs, &self.dir, &self.staging_dir, self.max_span, self.clock.clone())?;
        if std::env::var("AWAN_SIM_TRACE").ok().as_deref() == Some("1") {
            for (&id, vm) in &volumes {
                let dlen = self.fs.file(&data_path(&self.dir, id)).and_then(|f| f.len().ok()).unwrap_or(0);
                let jlen = self.fs.file(&idx_path(&self.dir, id)).and_then(|f| f.len().ok()).unwrap_or(0);
                eprintln!(
                    "  [after crash {:?}] stats={:?} vol {} offset={} dat_len={} idx_len={} (idx%42={})",
                    mode, stats, id, vm.offset(), dlen, jlen, jlen % 42
                );
            }
        }
        if recovered.multipart_count > 0 {
            self.tracker().record(FaultPoint::MultipartManifestRecovered);
        }
        self.model.check(&recovered.values)?;
        for key in 0..KEY_SPACE {
            let found = recovered.values.get(&key).copied().flatten();
            if self.model.survival(key, found) == Survives::No {
                return Err(format!("key {} survival check returned No for {:?}", key, found));
            }
        }
        self.model.after_recovery(&recovered.values);
        self.volumes = volumes;
        self.active_id = *self.volumes.keys().next_back().unwrap();
        self.live = recovered.live;
        self.writes_since_sync = false;

        self.push_trace_u8(36);
        self.push_trace_u64(recovered.values.len() as u64);
        for (&k, &v) in &recovered.values {
            self.push_trace_u8(k);
            match v {
                Some((l, f)) => {
                    self.push_trace_u8(1);
                    self.push_trace_u64(l as u64);
                    self.push_trace_u64(f);
                }
                None => self.push_trace_u8(0),
            }
        }
        Ok(())
    }

    fn step(&mut self, op: &StorageOp) -> Result<(), String> {
        self.clock.advance(1_000_000);
        match op {
            StorageOp::Basic(SimOperation::Put { key, len, fill }) => {
                self.exec_put(*key, *len, *fill);
                Ok(())
            }
            StorageOp::Basic(SimOperation::Delete { key }) => {
                self.exec_delete(*key);
                Ok(())
            }
            StorageOp::Basic(SimOperation::Sync) => self.exec_sync(),
            StorageOp::Basic(SimOperation::Fault { file, op, fault }) => {
                self.exec_fault(*file, *op, *fault);
                Ok(())
            }
            StorageOp::Basic(SimOperation::Crash { mode }) => self.exec_crash(*mode),
            StorageOp::Copy { src_key, dst_key } => {
                self.exec_copy(*src_key, *dst_key);
                Ok(())
            }
            StorageOp::StreamStart { stream_id, key, len, fill } => {
                self.exec_stream_start(*stream_id, *key, *len, *fill);
                Ok(())
            }
            StorageOp::StreamFinish { stream_id } => {
                self.exec_stream_finish(*stream_id);
                Ok(())
            }
            StorageOp::StreamAbort { stream_id } => {
                self.exec_stream_abort(*stream_id);
                Ok(())
            }
            StorageOp::MultipartPut { key, parts, fill } => {
                self.exec_multipart_put(*key, parts, *fill);
                Ok(())
            }
            StorageOp::Compact { seal_if_single } => {
                self.exec_compact(*seal_if_single);
                Ok(())
            }
            StorageOp::VerifyRead { key } => self.exec_verify_read(*key),
            StorageOp::ProbeJournalReadFault { fault } => self.exec_probe_journal_read_fault(*fault),
        }
    }
}

fn run_ops(seed: u64, max_span: u64, ops: &[StorageOp], global_tracker: Option<&FaultTracker>) -> Result<(u64, Vec<u8>), String> {
    let trace_on = std::env::var("AWAN_SIM_TRACE").ok().as_deref() == Some("1");
    let mut harness = SimStorageHarness::new(seed, max_span)?;
    for (idx, op) in ops.iter().enumerate() {
        if trace_on {
            eprintln!("seed {} step {}: {:?}", seed, idx, op);
        }
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
