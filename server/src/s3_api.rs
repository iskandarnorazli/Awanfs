//! awan-storage/server/src/s3_api.rs
//! S3 REST Wire Protocol Dispatcher, Routing & RFC 9110 Range Engine
//! Reference: awan-storage/API_SPECIFICATION.md and awan-storage/HYBRID_POC_SPECIFICATION.md

use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, HOST, LAST_MODIFIED};
use hyper::{Method, Request, Response, StatusCode};
use md5::{Digest, Md5};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::aws_chunked::decode_aws_chunked;
use crate::cache::PayloadCache;
use crate::coalesce::SingleFlight;
use crate::compaction::CompactionReport;
use crate::ffi_bridge::*;
use crate::lifecycle::*;
use crate::multipart::*;
use crate::mvcc::MvccStore;
use crate::sigv4::*;
use crate::volume::{global_offset, split_offset, Durability, NeedleLocation, VolumeError, VolumeManager};
use crate::work::WorkCounters;
use crate::xml::*;
use std::path::{Path, PathBuf};

pub type ResponseBody = BoxBody<Bytes, std::convert::Infallible>;

fn full_body(bytes: Bytes) -> ResponseBody {
    Full::new(bytes).boxed()
}

/// 500 InternalError: the server could not read or write the object's bytes.
fn internal_error_response(resource: &str, detail: &str) -> Response<ResponseBody> {
    let xml = s3_error_xml("InternalError", detail, resource, "req-internal");
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap()
}

/// The answer for an index error from `put_object` or `has_room_for`.
fn engine_error_response(code: i32, resource: &str) -> Response<ResponseBody> {
    let (status, s3_code, msg) = match code {
        ERR_CAPACITY => (StatusCode::SERVICE_UNAVAILABLE, "SlowDown", "The object index is full."),
        ERR_BUCKET_NOT_FOUND => (StatusCode::NOT_FOUND, "NoSuchBucket", "The specified bucket does not exist."),
        ERR_INVALID_ARG => (StatusCode::BAD_REQUEST, "KeyTooLongError", "The key is empty or longer than 1024 bytes."),
        _ => return internal_error_response(resource, &format!("index error {}", code)),
    };
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(s3_error_xml(s3_code, msg, resource, "req-index"))))
        .unwrap()
}

/// The largest object, and the largest part, S3 accepts in one request.
pub const MAX_OBJECT_BODY: u64 = 5 << 30;
/// Request bodies that are XML (multipart completion, multi-object delete, lifecycle).
pub const MAX_XML_BODY: u64 = 16 << 20;
/// Room for aws-chunked framing (chunk sizes and signatures) on top of the payload.
const CHUNK_FRAMING_SLACK: u64 = 16 << 20;

fn body_limit_for(method: &Method, target: &ParsedTarget) -> u64 {
    match (method, target) {
        // PutObject and UploadPart; CompleteMultipartUpload and the other POSTs carry XML.
        (&Method::PUT, ParsedTarget::Object { .. }) => MAX_OBJECT_BODY,
        _ => MAX_XML_BODY,
    }
}

fn entity_too_large(resource: &str, limit: u64) -> Response<ResponseBody> {
    let xml = s3_error_xml(
        "EntityTooLarge",
        &format!("Your proposed upload exceeds the maximum allowed size of {} bytes.", limit),
        resource,
        "req-size",
    );
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .header(CONTENT_TYPE, "application/xml")
        .header("connection", "close")
        .body(full_body(Bytes::from(xml)))
        .unwrap()
}

/// Reads a request body of at most `limit` bytes. `Ok(Err(response))` is the answer to send
/// when the body is too large or cut short.
async fn read_body(body: hyper::body::Incoming, limit: u64, resource: &str) -> Result<Result<Bytes, Response<ResponseBody>>, hyper::Error> {
    match http_body_util::Limited::new(body, limit as usize).collect().await {
        Ok(c) => Ok(Ok(c.to_bytes())),
        Err(e) => {
            if e.downcast_ref::<http_body_util::LengthLimitError>().is_some() {
                return Ok(Err(entity_too_large(resource, limit)));
            }
            match e.downcast::<hyper::Error>() {
                Ok(h) => Err(*h),
                Err(other) => {
                    let xml = s3_error_xml("IncompleteBody", &other.to_string(), resource, "req-body");
                    Ok(Err(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header(CONTENT_TYPE, "application/xml")
                        .body(full_body(Bytes::from(xml)))
                        .unwrap()))
                }
            }
        }
    }
}

/// A response body sent as a list of chunks, one frame each: the parts of a multipart object
/// go out from their mmaps without being copied into one buffer (#36).
struct ChunksBody(std::collections::VecDeque<Bytes>);

impl hyper::body::Body for ChunksBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Bytes>, Self::Error>>> {
        std::task::Poll::Ready(self.0.pop_front().map(|b| Ok(hyper::body::Frame::data(b))))
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_empty()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        hyper::body::SizeHint::with_exact(self.0.iter().map(|b| b.len() as u64).sum())
    }
}

/// Ids of the `volume_NNNN.dat` files in `dir`, ascending.
fn volume_ids(dir: &Path) -> Vec<u32> {
    let mut ids: Vec<u32> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter_map(|e| {
                    let name = e.file_name().into_string().ok()?;
                    name.strip_prefix("volume_")?.strip_suffix(".dat")?.parse().ok()
                })
                .collect()
        })
        .unwrap_or_default();
    ids.sort_unstable();
    ids
}

fn no_volume_error() -> VolumeError {
    VolumeError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "no volume is open"))
}

fn empty_body() -> ResponseBody {
    Empty::new().boxed()
}

// Global fault injection for negative control: Issue #462 Gate
static FAULT_DISABLE_RANGE_CLAMPING: AtomicBool = AtomicBool::new(false);

pub fn set_fault_disable_range_clamping(disable: bool) {
    FAULT_DISABLE_RANGE_CLAMPING.store(disable, Ordering::SeqCst);
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParsedTarget {
    Service,
    Bucket { bucket: String },
    Object { bucket: String, key: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64, // inclusive
}

pub fn parse_range_header(header: &str, object_size: u64) -> Result<Option<ByteRange>, ()> {
    let header = header.trim();
    if !header.starts_with("bytes=") {
        return Ok(None);
    }
    let spec = &header["bytes=".len()..].trim();
    if spec.contains(',') {
        return Err(());
    }

    if object_size == 0 {
        return Err(());
    }

    let disable_clamping = FAULT_DISABLE_RANGE_CLAMPING.load(Ordering::Relaxed);

    if let Some(suffix_str) = spec.strip_prefix('-') {
        let suffix_len: u64 = suffix_str.parse().map_err(|_| ())?;
        if suffix_len == 0 {
            return Err(());
        }
        let take = suffix_len.min(object_size);
        let start = object_size - take;
        let end = object_size - 1;
        return Ok(Some(ByteRange { start, end }));
    }

    if let Some((start_str, end_str)) = spec.split_once('-') {
        let start: u64 = start_str.parse().map_err(|_| ())?;
        if start >= object_size {
            return Err(());
        }
        if end_str.is_empty() {
            return Ok(Some(ByteRange {
                start,
                end: object_size - 1,
            }));
        }
        let parsed_end: u64 = end_str.parse().map_err(|_| ())?;
        if start > parsed_end {
            return Err(());
        }

        let end = if parsed_end >= object_size {
            if disable_clamping {
                parsed_end
            } else {
                object_size - 1
            }
        } else {
            parsed_end
        };

        return Ok(Some(ByteRange { start, end }));
    }

    Err(())
}

pub fn parse_s3_target(uri_path: &str, host_header: Option<&str>) -> ParsedTarget {
    let raw_path = uri_path.split('?').next().unwrap_or(uri_path);
    let trimmed_path = raw_path.trim_start_matches('/');

    if let Some(host) = host_header {
        let host_no_port = host.split(':').next().unwrap_or(host);
        for base in &["s3.localhost", "localhost", "s3.internal", "s3.amazonaws.com", "127.0.0.1"] {
            if host_no_port.ends_with(base) && host_no_port.len() > base.len() + 1 {
                let prefix = &host_no_port[..host_no_port.len() - base.len() - 1];
                if !prefix.is_empty() && !prefix.contains('.') {
                    let bucket = prefix.to_string();
                    if trimmed_path.is_empty() {
                        return ParsedTarget::Bucket { bucket };
                    } else {
                        return ParsedTarget::Object {
                            bucket,
                            key: trimmed_path.to_string(),
                        };
                    }
                }
            }
        }
    }

    if trimmed_path.is_empty() {
        return ParsedTarget::Service;
    }

    let mut parts = trimmed_path.splitn(2, '/');
    let bucket = parts.next().unwrap_or("").to_string();
    if let Some(key) = parts.next() {
        ParsedTarget::Object {
            bucket,
            key: key.to_string(),
        }
    } else {
        ParsedTarget::Bucket { bucket }
    }
}

pub fn percent_decode(s: &str) -> String {
    let mut out = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(hex_val) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(hex_val);
                i += 3;
                continue;
            }
        } else if bytes[i] == b'+' {
            out.push(b' ');
            i += 1;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

pub fn md5_to_etag_hex(hi: u64, lo: u64) -> String {
    let mut bytes = [0u8; 16];
    bytes[0..8].copy_from_slice(&hi.to_be_bytes());
    bytes[8..16].copy_from_slice(&lo.to_be_bytes());
    let mut s = String::with_capacity(32);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

pub fn compute_md5(data: &[u8]) -> (u64, u64, String, [u8; 16]) {
    let mut hasher = Md5::new();
    hasher.update(data);
    let result = hasher.finalize();
    let bytes: [u8; 16] = result.into();
    let hi = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
    let lo = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
    let mut hex = String::with_capacity(32);
    for b in bytes {
        hex.push_str(&format!("{:02x}", b));
    }
    (hi, lo, hex, bytes)
}

pub fn validate_s3_key(key: &str) -> Result<(), crate::error::S3ErrorCode> {
    if key.is_empty() {
        return Err(crate::error::S3ErrorCode::InvalidObjectName);
    }
    if key.len() > 1024 {
        return Err(crate::error::S3ErrorCode::KeyTooLongError);
    }
    for b in key.bytes() {
        if b == 0 || b < 0x20 {
            return Err(crate::error::S3ErrorCode::InvalidObjectName);
        }
    }
    for segment in key.split('/') {
        if segment == ".." {
            return Err(crate::error::S3ErrorCode::InvalidObjectName);
        }
    }
    Ok(())
}

static INSTANCE_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Called at named points of a request, with the object key; tests use it to force an order
/// of steps between concurrent requests. `None` (the default) costs one read-lock per point.
pub type TestHook = Arc<dyn Fn(&'static str, &str) + Send + Sync>;

pub struct S3ServiceState {
    pub engine: Arc<RwLock<AwanStorageEngine>>,
    pub multipart: Arc<MultipartManager>,
    pub sigv4_cache: Arc<SigV4KeyCache>,
    pub lifecycle: Arc<LifecycleManager>,
    pub single_flight: Arc<SingleFlight>,
    pub mvcc: Arc<MvccStore>,
    pub data_dir: Option<PathBuf>,
    pub total_volume_bytes: Arc<AtomicU64>,
    pub tombstone_bytes: Arc<AtomicU64>,
    pub active_volume_id: Arc<AtomicU32>,
    /// The volume new writes go to.
    pub volume_manager: Arc<RwLock<Option<Arc<VolumeManager>>>>,
    /// Every open volume by id, the active one included; reads look up the volume of a
    /// global offset here (#37).
    pub volumes: Arc<RwLock<BTreeMap<u32, Arc<VolumeManager>>>>,
    /// Held while a full volume is sealed and the next one opened.
    roll_lock: Arc<parking_lot::Mutex<()>>,
    pub needle_crc_map: Arc<RwLock<HashMap<(u32, u64), u32>>>,
    pub payload_cache: Arc<PayloadCache>,
    pub next_vol_offset: Arc<RwLock<u64>>,
    /// Disk writes, syncs, reads and payload copies, for the work-count tests.
    pub work: Arc<WorkCounters>,
    pub test_hook: Arc<RwLock<Option<TestHook>>>,
    /// Striped per-key locks: requests that change one key take its lock from before the
    /// volume write until the engine row is updated, so log order equals index order
    /// (`verification/tla/ObjectVersions.tla`, `UseKeyLock`).
    pub key_locks: Arc<Vec<tokio::sync::Mutex<()>>>,
    /// Who may call; `AuthConfig::open()` by default, the binary sets it from flags and env.
    pub auth: crate::auth::AuthConfig,
}

impl S3ServiceState {
    pub fn new() -> Self {
        let id = INSTANCE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let default_dir = std::env::temp_dir().join(format!(
            "s3_vol_{}_{}",
            std::process::id(),
            id
        ));
        let _ = std::fs::create_dir_all(&default_dir);
        let work = Arc::new(WorkCounters::default());
        let vol_mgr = VolumeManager::open_sparse(&default_dir, 1).ok().map(|mut vm| {
            vm.attach_work_counters(Arc::clone(&work));
            Arc::new(vm)
        });
        let initial_bytes = if let Some(ref vm) = vol_mgr { vm.offset() } else { 0 };
        Self {
            data_dir: None,
            total_volume_bytes: Arc::new(AtomicU64::new(initial_bytes)),
            tombstone_bytes: Arc::new(AtomicU64::new(0)),
            active_volume_id: Arc::new(AtomicU32::new(1)),
            engine: Arc::new(RwLock::new(AwanStorageEngine::new(100_000, 4_194_304, 65536))),
            multipart: Arc::new(
                MultipartManager::with_staging_dir(default_dir.join(".staging")).with_work_counters(Arc::clone(&work)),
            ),
            sigv4_cache: Arc::new(SigV4KeyCache::new()),
            lifecycle: Arc::new(LifecycleManager::new()),
            single_flight: Arc::new(SingleFlight::new()),
            mvcc: Arc::new(MvccStore::new()),
            volumes: Arc::new(RwLock::new(vol_mgr.iter().map(|vm| (vm.id(), Arc::clone(vm))).collect())),
            roll_lock: Arc::new(parking_lot::Mutex::new(())),
            volume_manager: Arc::new(RwLock::new(vol_mgr)),
            needle_crc_map: Arc::new(RwLock::new(HashMap::new())),
            payload_cache: Arc::new(PayloadCache::default_auto_scaled()),
            next_vol_offset: Arc::new(RwLock::new(4096)),
            work,
            test_hook: Arc::new(RwLock::new(None)),
            key_locks: Arc::new((0..256).map(|_| tokio::sync::Mutex::new(())).collect()),
            auth: crate::auth::AuthConfig::open(),
        }
    }

    pub fn with_data_dir(dir: &Path) -> Self {
        let _ = std::fs::create_dir_all(dir);
        let work = Arc::new(WorkCounters::default());
        // Every volume in id order; writes go to the last one (#37).
        let mut volumes: BTreeMap<u32, Arc<VolumeManager>> = BTreeMap::new();
        let mut ids = volume_ids(dir);
        if ids.is_empty() {
            ids.push(1);
        }
        for id in ids {
            if let Ok(mut vm) = VolumeManager::open_sparse(dir, id) {
                vm.attach_work_counters(Arc::clone(&work));
                volumes.insert(id, Arc::new(vm));
            }
        }
        let vol_mgr = volumes.values().next_back().cloned();
        let multipart = Arc::new(
            MultipartManager::with_staging_dir(dir.join(".staging")).with_work_counters(Arc::clone(&work)),
        );
        let mut engine = AwanStorageEngine::new(100_000, 4_194_304, 65536);
        let db_path = dir.join("metadata.db");
        if let Ok(spill) = crate::spill::SpillIndex::open(&db_path) {
            engine.attach_spill_index(spill);
        }
        let needle_crc_map = Arc::new(RwLock::new(HashMap::new()));
        let mut initial_vol_offset = 4096;

        // 1. Replay buckets manifest
        let manifest_path = dir.join("buckets.manifest");
        if let Ok(content) = std::fs::read_to_string(&manifest_path) {
            for line in content.lines() {
                let line = line.trim();
                if let Some(rest) = line.strip_prefix('+') {
                    if let Some((id_str, name)) = rest.split_once(':') {
                        if let Ok(id) = id_str.parse::<u32>() {
                            engine.restore_bucket(id, name);
                        }
                    }
                } else if let Some(rest) = line.strip_prefix('-') {
                    if let Some((id_str, _)) = rest.split_once(':') {
                        if let Ok(id) = id_str.parse::<u32>() {
                            let _ = engine.delete_bucket(id);
                        }
                    }
                }
            }
        }

        // 2. Replay the needle volumes, oldest first: writes only move to a newer volume, so
        // volume order is log order.
        for vm in volumes.values() {
            initial_vol_offset = vm.offset().max(4096);
            let mut crcs = needle_crc_map.write();
            // Collect the log first: a multipart manifest needle is read back from the volume,
            // and the events are applied in log order so later writes win.
            let mut events = Vec::new();
            let _ = vm.recover_needles(|b_id, key, offset, size, etag_hi, etag_lo, flags, crc32c| {
                events.push((b_id, key, offset, size, etag_hi, etag_lo, flags, crc32c));
            });
            for (b_id, key, local, size, etag_hi, etag_lo, flags, crc32c) in events {
                let offset = global_offset(vm.id(), local);
                crcs.insert((b_id as u32, offset), crc32c);
                if flags & AWAN_FLAG_TOMBSTONE != 0 {
                    let _ = engine.delete_object(b_id as u32, key.as_bytes());
                } else if flags & AWAN_FLAG_MULTIPART != 0 {
                    let manifest = vm
                        .read_needle_bytes(vm.id(), local, crc32c)
                        .ok()
                        .and_then(|b| decode_extents_manifest(&multipart.staging_dir, &b));
                    if let Some((total, extents)) = manifest {
                        let _ = engine.put_object(
                            b_id as u32,
                            key.as_bytes(),
                            offset,
                            total,
                            etag_hi,
                            etag_lo,
                            extents.len() as u16,
                            flags,
                        );
                        multipart.restore_completed(b_id as u32, &key, extents);
                    }
                } else {
                    let _ = engine.put_object(
                        b_id as u32,
                        key.as_bytes(),
                        offset,
                        size,
                        etag_hi,
                        etag_lo,
                        0,
                        flags,
                    );
                }
            }
        }

        let initial_bytes = volumes.values().map(|vm| vm.offset()).sum();
        let active_id = vol_mgr.as_ref().map(|vm| vm.id()).unwrap_or(1);

        Self {
            data_dir: Some(dir.to_path_buf()),
            total_volume_bytes: Arc::new(AtomicU64::new(initial_bytes)),
            tombstone_bytes: Arc::new(AtomicU64::new(0)),
            active_volume_id: Arc::new(AtomicU32::new(active_id)),
            engine: Arc::new(RwLock::new(engine)),
            multipart,
            sigv4_cache: Arc::new(SigV4KeyCache::new()),
            lifecycle: Arc::new(LifecycleManager::new()),
            single_flight: Arc::new(SingleFlight::new()),
            mvcc: Arc::new(MvccStore::new()),
            volumes: Arc::new(RwLock::new(volumes)),
            roll_lock: Arc::new(parking_lot::Mutex::new(())),
            volume_manager: Arc::new(RwLock::new(vol_mgr)),
            needle_crc_map,
            payload_cache: Arc::new(PayloadCache::default_auto_scaled()),
            next_vol_offset: Arc::new(RwLock::new(initial_vol_offset)),
            work,
            test_hook: Arc::new(RwLock::new(None)),
            key_locks: Arc::new((0..256).map(|_| tokio::sync::Mutex::new(())).collect()),
            auth: crate::auth::AuthConfig::open(),
        }
    }

    pub fn fragmentation_ratio(&self) -> f64 {
        let tot = self.total_volume_bytes.load(Ordering::Relaxed);
        let dead = self.tombstone_bytes.load(Ordering::Relaxed);
        if tot == 0 {
            0.0
        } else {
            (dead as f64) / (tot as f64)
        }
    }

    /// Compacts the oldest volume. With a single volume, seals it first (the next writes go
    /// to a new one). See `compact_oldest_volume`.
    pub async fn perform_online_compaction(&self) -> Result<CompactionReport, VolumeError> {
        if self.data_dir.is_none() {
            return Ok(CompactionReport::default());
        }
        let active = self.volume_manager.read().clone().ok_or_else(no_volume_error)?;
        if self.volumes.read().keys().next() == Some(&active.id()) {
            self.roll_volume(&active)?;
        }
        self.compact_oldest_volume().await
    }

    /// Moves the live needles of the oldest sealed volume to the active volume and deletes
    /// the old volume (#14). Safe while requests run:
    /// - each live needle is copied under its key lock, and the index row moves only if it
    ///   still points at the old needle, so a concurrent PUT or DELETE wins and is never
    ///   followed in the log by an older copy;
    /// - the copies are synced before the old files are removed;
    /// - only the oldest volume is compacted: a tombstone there can only hide needles in the
    ///   same volume, so dropping it cannot bring a deleted object back after a restart.
    /// A volume file that cannot be removed (Windows, while a read still maps it) stays on
    /// disk; replaying it on boot is harmless because every copy is newer.
    pub async fn compact_oldest_volume(&self) -> Result<CompactionReport, VolumeError> {
        let active = self.volume_manager.read().clone().ok_or_else(no_volume_error)?;
        let old = match self.volumes.read().values().next().cloned() {
            Some(v) if v.id() != active.id() => v,
            _ => return Ok(CompactionReport::default()),
        };
        let old_id = old.id();
        let mut needles = Vec::new();
        old.recover_needles(|b_id, key, offset, size, etag_hi, etag_lo, flags, crc| {
            needles.push((b_id as u32, key, offset, size, etag_hi, etag_lo, flags, crc));
        })?;

        let mut report = CompactionReport::default();
        let mut written: BTreeMap<u32, Arc<VolumeManager>> = BTreeMap::new();
        for (b_id, key, local, size, etag_hi, etag_lo, flags, crc) in needles {
            let g_old = global_offset(old_id, local);
            if flags & AWAN_FLAG_TOMBSTONE != 0 {
                report.dead_needles_reclaimed += 1;
                continue;
            }
            let _guard = self.lock_key(b_id, &key).await;
            let live = self.engine.read().get_object(b_id, key.as_bytes()).map(|m| m.vol_offset == g_old).unwrap_or(false);
            if !live {
                report.dead_needles_reclaimed += 1;
                continue;
            }
            let bytes = if size == 0 { Bytes::new() } else { old.read_needle_bytes(old_id, local, crc)? };
            let (vm, loc, g_new) = self.write_to_active(b_id, &key, &bytes, flags, etag_hi, etag_lo)?;
            written.insert(vm.id(), Arc::clone(&vm));
            if let Err(code) = self.engine.write().update_object_offset(b_id, key.as_bytes(), g_new) {
                return Err(VolumeError::Io(std::io::Error::new(std::io::ErrorKind::Other, format!("index error {} while moving {}", code, key))));
            }
            {
                let mut crcs = self.needle_crc_map.write();
                crcs.remove(&(b_id, g_old));
                crcs.insert((b_id, g_new), loc.payload_crc32c);
            }
            self.payload_cache.remove(b_id, g_old);
            report.live_needles_copied += 1;
            report.new_locations.insert(key, loc);
        }

        // The copies must be on disk before the only other copy goes, in every durability mode.
        for vm in written.into_values() {
            tokio::task::spawn_blocking(move || vm.sync_now())
                .await
                .map_err(|e| VolumeError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))??;
        }
        self.volumes.write().remove(&old_id);
        let reclaimed = old.offset();
        let dir = old.dir().to_path_buf();
        drop(old);
        report.volume_removed = std::fs::remove_file(dir.join(format!("volume_{:04}.dat", old_id))).is_ok();
        let _ = std::fs::remove_file(dir.join(format!("volume_{:04}.idx", old_id)));
        let _ = self.total_volume_bytes.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |t| Some(t.saturating_sub(reclaimed)));
        self.tombstone_bytes.store(0, Ordering::Relaxed);
        Ok(report)
    }

    /// Takes the lock of (bucket, key). Calls the test hook with `key_lock_contended` when
    /// another request holds it.
    pub async fn lock_key(&self, b_id: u32, key: &str) -> tokio::sync::MutexGuard<'_, ()> {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        (b_id, key).hash(&mut h);
        let m = &self.key_locks[(h.finish() as usize) % self.key_locks.len()];
        match m.try_lock() {
            Ok(g) => g,
            Err(_) => {
                self.hook("key_lock_contended", key);
                m.lock().await
            }
        }
    }

    /// The bytes of an object, single or multipart, verified.
    pub fn read_object_bytes(&self, b_id: u32, key: &str, vol_offset: u64, size: u64, flags: u16) -> Result<Bytes, VolumeError> {
        if flags & AWAN_FLAG_MULTIPART == 0 {
            return self.load_payload(b_id, vol_offset, size as usize);
        }
        let extents = self.multipart.get_completed_extents(b_id, key).ok_or(VolumeError::CorruptIndex)?;
        let mut out = Vec::with_capacity(size as usize);
        for ext in extents {
            let chunk = self.part_bytes(b_id, &ext)?;
            let n = (ext.part_size as usize).min(chunk.len());
            self.work.payload_copy(n as u64);
            out.extend_from_slice(&chunk[..n]);
        }
        if out.len() as u64 != size {
            return Err(VolumeError::CorruptIndex);
        }
        Ok(Bytes::from(out))
    }

    /// The bytes of one part of a multipart object: its staged file's mmap, or its needle.
    pub fn part_bytes(&self, b_id: u32, ext: &PartExtent) -> Result<Bytes, VolumeError> {
        match ext.staged_path.as_ref().and_then(|p| self.multipart.get_or_mmap_part(p).ok()) {
            Some(mmap) => Ok(Bytes::from_owner(crate::volume::MmapOwner(mmap))),
            None => self.load_payload(b_id, ext.vol_offset, ext.part_size as usize),
        }
    }

    pub fn hook(&self, point: &'static str, key: &str) {
        let hook = self.test_hook.read().clone();
        if let Some(h) = hook {
            h(point, key);
        }
    }

    /// Waits until the volume write numbered `seq` is as durable as the volume's mode
    /// promises (#29). The sync runs on the blocking pool, not on the executor.
    pub async fn make_durable(&self, vm: &Arc<VolumeManager>, seq: u64) -> Result<(), VolumeError> {
        if vm.durability() == Durability::Fast {
            return Ok(());
        }
        let vm = Arc::clone(vm);
        tokio::task::spawn_blocking(move || vm.make_durable(seq))
            .await
            .map_err(|e| VolumeError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?
    }

    /// Sets the durability mode of the open volume; `Fast` also starts its flusher.
    pub fn set_durability(&self, mode: Durability) {
        if let Some(vm) = self.volume_manager.read().clone() {
            vm.set_durability(mode);
            if mode == Durability::Fast {
                vm.start_flusher(std::time::Duration::from_secs(1));
            }
        }
    }

    /// Writes the payload to the volume and waits until it is durable. An error means the
    /// write is not acknowledged.
    pub async fn store_payload(&self, b_id: u32, key: &str, data: &Bytes, flags: u16, etag_hi: u64, etag_lo: u64) -> Result<u64, VolumeError> {
        let (vm, loc, offset) = self.write_to_active(b_id, key, data, flags, etag_hi, etag_lo)?;
        self.make_durable(&vm, loc.seq).await?;
        self.needle_crc_map.write().insert((b_id, offset), loc.payload_crc32c);
        self.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
        // Cache hot payloads up to 4 MiB in dynamic auto-scaling LRU cache
        if !data.is_empty() && data.len() <= 4 * 1024 * 1024 {
            self.payload_cache.insert(b_id, offset, data.clone());
        }
        Ok(offset)
    }

    /// Writes a needle to the active volume. When it is full, seals it, opens the next one
    /// and writes there (#37). Returns the volume, the local location and the global offset.
    pub fn write_to_active(
        &self,
        b_id: u32,
        key: &str,
        data: &[u8],
        flags: u16,
        etag_hi: u64,
        etag_lo: u64,
    ) -> Result<(Arc<VolumeManager>, NeedleLocation, u64), VolumeError> {
        let mut attempts = 0;
        loop {
            let vm = self.volume_manager.read().clone().ok_or_else(no_volume_error)?;
            match vm.write_needle(b_id as u16, key, data, flags, etag_hi, etag_lo) {
                Ok(loc) => {
                    let g = global_offset(vm.id(), loc.offset);
                    return Ok((vm, loc, g));
                }
                // A needle bigger than a whole volume would roll forever: refuse it.
                Err(VolumeError::VolumeFull { len, .. }) if len + crate::direct_io::SECTOR_SIZE as u64 > vm.max_span() => {
                    return Err(VolumeError::VolumeFull { offset: 0, len })
                }
                Err(VolumeError::VolumeFull { .. }) if attempts < 8 => {
                    attempts += 1;
                    self.roll_volume(&vm)?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Opens the volume after `full` and makes it the active one, unless another writer
    /// already did. The full volume stays open for reads.
    fn roll_volume(&self, full: &Arc<VolumeManager>) -> Result<(), VolumeError> {
        let _g = self.roll_lock.lock();
        let active = self.volume_manager.read().clone().ok_or_else(no_volume_error)?;
        if active.id() != full.id() {
            return Ok(());
        }
        let next_id = self.volumes.read().keys().next_back().copied().unwrap_or(full.id()).max(full.id()) + 1;
        // New volumes grow as they are written (not 32 GiB sparse files).
        let mut vm = VolumeManager::open_mode(full.dir(), next_id, false)?;
        vm.attach_work_counters(full.work_counters());
        vm.set_max_span(full.max_span());
        vm.set_durability(full.durability());
        let vm = Arc::new(vm);
        if vm.durability() == Durability::Fast {
            vm.start_flusher(std::time::Duration::from_secs(1));
        }
        // A sealed volume in `fast` mode gets one last sync, so its tail is not left to the
        // flusher alone.
        if full.durability() == Durability::Fast {
            let _ = full.flush_if_dirty();
        }
        self.volumes.write().insert(next_id, Arc::clone(&vm));
        *self.volume_manager.write() = Some(vm);
        self.active_volume_id.store(next_id, Ordering::SeqCst);
        Ok(())
    }

    /// The volume holding a global offset, and the offset inside it.
    fn volume_for(&self, global: u64) -> Result<(Arc<VolumeManager>, u64), VolumeError> {
        let (id, local) = split_offset(global);
        if let Some(vm) = self.volume_manager.read().clone() {
            if vm.id() == id {
                return Ok((vm, local));
            }
        }
        let vm = self.volumes.read().get(&id).cloned().ok_or(VolumeError::CorruptIndex)?;
        Ok((vm, local))
    }

    /// Sets the span of the active volume and of every volume opened after it. Tests use a
    /// small span to make rollover happen.
    pub fn set_volume_span(&self, span: u64) {
        if let Some(vm) = self.volume_manager.read().clone() {
            vm.set_max_span(span);
        }
    }

    /// Reads and verifies a payload. An error means the bytes could not be read or failed
    /// their CRC; callers answer 500, never a body of zeros.
    pub fn load_payload(&self, b_id: u32, vol_offset: u64, expected_size: usize) -> Result<Bytes, VolumeError> {
        if expected_size == 0 {
            return Ok(Bytes::new());
        }
        if vol_offset == 0 {
            return Err(VolumeError::CorruptIndex);
        }
        if let Some(cached) = self.payload_cache.get(b_id, vol_offset) {
            return Ok(cached);
        }
        let (vm, local) = self.volume_for(vol_offset)?;
        let expected_crc = self
            .needle_crc_map
            .read()
            .get(&(b_id, vol_offset))
            .copied()
            .ok_or(VolumeError::CorruptIndex)?;
        let bytes = vm.read_needle_bytes(vm.id(), local, expected_crc)?;
        if bytes.len() != expected_size {
            return Err(VolumeError::CorruptIndex);
        }
        if bytes.len() <= 4 * 1024 * 1024 {
            self.payload_cache.insert(b_id, vol_offset, bytes.clone());
        }
        Ok(bytes)
    }

    pub fn allocate_offset(&self, size: usize) -> u64 {
        let mut off = self.next_vol_offset.write();
        let current = *off;
        let aligned_size = ((size + 4095) / 4096) * 4096;
        *off += aligned_size as u64;
        current
    }
}

impl Default for S3ServiceState {
    fn default() -> Self {
        Self::new()
    }
}

pub async fn handle_s3_request(
    state: Arc<S3ServiceState>,
    req: Request<hyper::body::Incoming>,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let path = uri.path();
    let query = uri.query().unwrap_or("");
    let host_hdr = req.headers().get(HOST).and_then(|v| v.to_str().ok());

    let mut query_map = HashMap::new();
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            let dec_k = percent_decode(k);
            let dec_v = percent_decode(v);
            query_map.insert(dec_k.to_lowercase(), dec_v.clone());
            query_map.insert(dec_k, dec_v);
        } else if !pair.is_empty() {
            let dec_k = percent_decode(pair);
            query_map.insert(dec_k.to_lowercase(), String::new());
            query_map.insert(dec_k, String::new());
        }
    }

    // SigV4 Authentication Check
    let now_sec = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if let Err(code) = crate::auth::authenticate(
        &state.auth,
        &state.sigv4_cache,
        req.method().as_str(),
        req.uri().path(),
        req.uri().query().unwrap_or(""),
        req.headers(),
        now_sec,
    ) {
        let status = match code {
            "AuthorizationHeaderMalformed" | "AuthorizationQueryParametersError" => StatusCode::BAD_REQUEST,
            _ => StatusCode::FORBIDDEN,
        };
        let xml = s3_error_xml(code, "Authentication failed", path, "req-auth");
        return Ok(Response::builder()
            .status(status)
            .header(CONTENT_TYPE, "application/xml")
            .body(full_body(Bytes::from(xml)))
            .unwrap());
    }

    let target = parse_s3_target(path, host_hdr);

    // Size limits (#36), checked on the declared length before any body is read.
    let body_limit = body_limit_for(&method, &target);
    let declared = req
        .headers()
        .get("x-amz-decoded-content-length")
        .or_else(|| req.headers().get(CONTENT_LENGTH))
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok());
    if declared.is_some_and(|n| n > body_limit) {
        return Ok(entity_too_large(path, body_limit));
    }

    match target {
        ParsedTarget::Service => {
            if method == Method::GET {
                let engine = state.engine.read();
                let mut b_list = Vec::new();
                for (name, _) in &engine.bucket_names {
                    b_list.push((name.as_str(), "2026-11-10T00:00:00.000Z"));
                }
                b_list.sort_by_key(|&(n, _)| n);
                let xml = list_buckets_xml(&b_list, "req-list-buckets");
                let resp = Response::builder()
                    .status(StatusCode::OK)
                    .header(CONTENT_TYPE, "application/xml")
                    .body(full_body(Bytes::from(xml)))
                    .unwrap();
                return Ok(resp);
            }
            let err_xml = s3_error_xml("MethodNotAllowed", "Method not allowed on service", "/", "req-err");
            return Ok(Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(err_xml)))
                .unwrap());
        }

        ParsedTarget::Bucket { bucket } => {
            match method {
                Method::PUT => {
                    if query_map.contains_key("lifecycle") {
                        let body_bytes = match read_body(req.into_body(), MAX_XML_BODY, path).await? {
                            Ok(b) => b,
                            Err(resp) => return Ok(resp),
                        };
                        let body_str = String::from_utf8_lossy(&body_bytes);
                        match parse_lifecycle_xml(&body_str) {
                            Ok(rules) => {
                                state.lifecycle.set_rules(&bucket, rules);
                                return Ok(Response::builder()
                                    .status(StatusCode::OK)
                                    .header(CONTENT_LENGTH, "0")
                                    .body(empty_body())
                                    .unwrap());
                            }
                            Err(_) => {
                                let xml = s3_error_xml("MalformedXML", "The XML provided was not well-formed", &bucket, "req-lc");
                                return Ok(Response::builder()
                                    .status(StatusCode::BAD_REQUEST)
                                    .header(CONTENT_TYPE, "application/xml")
                                    .body(full_body(Bytes::from(xml)))
                                    .unwrap());
                            }
                        }
                    }

                    let mut engine = state.engine.write();
                    match engine.create_bucket(&bucket) {
                        Ok(id) => {
                            if let Some(ref d) = state.data_dir {
                                let manifest = d.join("buckets.manifest");
                                if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(&manifest) {
                                    use std::io::Write;
                                    let _ = writeln!(f, "+{}:{}", id, bucket);
                                }
                            }
                            let resp = Response::builder()
                                .status(StatusCode::OK)
                                .header(CONTENT_LENGTH, "0")
                                .body(empty_body())
                                .unwrap();
                            Ok(resp)
                        }
                        Err(AWAN_ERR_INVALID_ARG) => {
                            let xml = s3_error_xml("InvalidBucketName", "The specified bucket is not valid.", &bucket, "req-b-err");
                            Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .header(CONTENT_TYPE, "application/xml")
                                .body(full_body(Bytes::from(xml)))
                                .unwrap())
                        }
                        Err(_) => {
                            let xml = s3_error_xml("InternalError", "Failed to create bucket", &bucket, "req-b-err");
                            Ok(Response::builder()
                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                .header(CONTENT_TYPE, "application/xml")
                                .body(full_body(Bytes::from(xml)))
                                .unwrap())
                        }
                    }
                }

                Method::HEAD => {
                    let engine = state.engine.read();
                    if engine.lookup_bucket(&bucket).is_ok() {
                        Ok(Response::builder()
                            .status(StatusCode::OK)
                            .header(CONTENT_LENGTH, "0")
                            .body(empty_body())
                            .unwrap())
                    } else {
                        Ok(Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .header(CONTENT_LENGTH, "0")
                            .body(empty_body())
                            .unwrap())
                    }
                }

                Method::DELETE => {
                    if query_map.contains_key("lifecycle") {
                        state.lifecycle.delete_rules(&bucket);
                        return Ok(Response::builder()
                            .status(StatusCode::NO_CONTENT)
                            .header(CONTENT_LENGTH, "0")
                            .body(empty_body())
                            .unwrap());
                    }

                    let mut engine = state.engine.write();
                    let b_id = match engine.lookup_bucket(&bucket) {
                        Ok(id) => id,
                        Err(_) => {
                            let xml = s3_error_xml("NoSuchBucket", "The specified bucket does not exist", &bucket, "req-del-b");
                            return Ok(Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .header(CONTENT_TYPE, "application/xml")
                                .body(full_body(Bytes::from(xml)))
                                .unwrap());
                        }
                    };

                    match engine.delete_bucket(b_id) {
                        Ok(()) => {
                            if let Some(ref d) = state.data_dir {
                                let manifest = d.join("buckets.manifest");
                                if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(&manifest) {
                                    use std::io::Write;
                                    let _ = writeln!(f, "-{}:{}", b_id, bucket);
                                }
                            }
                            Ok(Response::builder()
                                .status(StatusCode::NO_CONTENT)
                                .header(CONTENT_LENGTH, "0")
                                .body(empty_body())
                                .unwrap())
                        }
                        Err(AWAN_ERR_BUCKET_NOT_EMPTY) => {
                            let xml = s3_error_xml("BucketNotEmpty", "The bucket you tried to delete is not empty", &bucket, "req-del-b");
                            Ok(Response::builder()
                                .status(StatusCode::CONFLICT)
                                .header(CONTENT_TYPE, "application/xml")
                                .body(full_body(Bytes::from(xml)))
                                .unwrap())
                        }
                        Err(_) => {
                            let xml = s3_error_xml("InternalError", "Bucket delete error", &bucket, "req-del-b");
                            Ok(Response::builder()
                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                .header(CONTENT_TYPE, "application/xml")
                                .body(full_body(Bytes::from(xml)))
                                .unwrap())
                        }
                    }
                }

                Method::GET => {
                    if query_map.contains_key("lifecycle") {
                        match state.lifecycle.get_rules(&bucket) {
                            Some(rules) => {
                                let xml = lifecycle_config_xml(&rules);
                                return Ok(Response::builder()
                                    .status(StatusCode::OK)
                                    .header(CONTENT_TYPE, "application/xml")
                                    .body(full_body(Bytes::from(xml)))
                                    .unwrap());
                            }
                            None => {
                                let xml = s3_error_xml("NoSuchLifecycleConfiguration", "The lifecycle configuration does not exist", &bucket, "req-lc");
                                return Ok(Response::builder()
                                    .status(StatusCode::NOT_FOUND)
                                    .header(CONTENT_TYPE, "application/xml")
                                    .body(full_body(Bytes::from(xml)))
                                    .unwrap());
                            }
                        }
                    }

                    if query_map.contains_key("location") {
                        let xml = location_constraint_xml("us-east-1");
                        return Ok(Response::builder()
                            .status(StatusCode::OK)
                            .header(CONTENT_TYPE, "application/xml")
                            .body(full_body(Bytes::from(xml)))
                            .unwrap());
                    }

                    if query_map.contains_key("uploads") {
                        // ListMultipartUploads
                        let uploads_guard = state.multipart.uploads.read();
                        let active: Vec<_> = uploads_guard.values().filter(|u| u.bucket == bucket).cloned().collect();
                        let xml = list_multipart_uploads_result_xml(&bucket, &active);
                        return Ok(Response::builder()
                            .status(StatusCode::OK)
                            .header(CONTENT_TYPE, "application/xml")
                            .body(full_body(Bytes::from(xml)))
                            .unwrap());
                    }

                    // ListObjectsV2
                    let engine = state.engine.read();
                    let b_id = match engine.lookup_bucket(&bucket) {
                        Ok(id) => id,
                        Err(_) => {
                            let xml = s3_error_xml("NoSuchBucket", "The specified bucket does not exist", &bucket, "req-list-obj");
                            return Ok(Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .header(CONTENT_TYPE, "application/xml")
                                .body(full_body(Bytes::from(xml)))
                                .unwrap());
                        }
                    };

                    let prefix = query_map.get("prefix").map(|s| s.as_str()).unwrap_or("");
                    let start_after = query_map.get("start-after").map(|s| s.as_str()).unwrap_or("");
                    let max_keys = query_map.get("max-keys").and_then(|s| s.parse().ok()).unwrap_or(1000u32);

                    let mut records = vec![AwanObjectMeta::default(); max_keys as usize];
                    let count = engine.scan_objects(
                        b_id,
                        prefix.as_bytes(),
                        start_after.as_bytes(),
                        max_keys as usize,
                        &mut records,
                    );

                    let mut contents = Vec::with_capacity(count);
                    let mut key_strings = Vec::with_capacity(count);
                    let mut etag_strings = Vec::with_capacity(count);

                    for meta in records.iter().take(count) {
                        let (off, len) = unpack_key_slice(meta.key_slice);
                        let k_str = String::from_utf8_lossy(&engine.key_arena[off..off + len]).to_string();
                        key_strings.push(k_str);
                        let etag_hex = md5_to_etag_hex(meta.etag_hi, meta.etag_lo);
                        etag_strings.push(etag_hex);
                    }

                    for (i, meta) in records.iter().take(count).enumerate() {
                        contents.push(S3ObjectEntry {
                            key: &key_strings[i],
                            last_modified: "2026-11-10T00:00:00.000Z",
                            etag: &etag_strings[i],
                            size: meta.size,
                        });
                    }

                    let xml = list_objects_v2_xml(&bucket, prefix, start_after, max_keys, false, None, &contents, &[]);
                    Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header(CONTENT_TYPE, "application/xml")
                        .body(full_body(Bytes::from(xml)))
                        .unwrap())
                }

                Method::POST if query_map.contains_key("delete") => {
                    let body_bytes = match read_body(req.into_body(), MAX_XML_BODY, path).await? {
                        Ok(b) => b,
                        Err(resp) => return Ok(resp),
                    };
                    let body_str = String::from_utf8_lossy(&body_bytes);

                    let mut keys_to_delete = Vec::new();
                    let mut cursor = body_str.as_ref();
                    while let Some(start) = cursor.find("<Key>") {
                        let rest = &cursor[start + 5..];
                        if let Some(end) = rest.find("</Key>") {
                            keys_to_delete.push(rest[..end].to_string());
                            cursor = &rest[end + 6..];
                        } else {
                            break;
                        }
                    }

                    let mut engine = state.engine.write();
                    let b_id = match engine.lookup_bucket(&bucket) {
                        Ok(id) => id,
                        Err(_) => {
                            let xml = s3_error_xml("NoSuchBucket", "The specified bucket does not exist", &bucket, "req-del-batch");
                            return Ok(Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .header(CONTENT_TYPE, "application/xml")
                                .body(full_body(Bytes::from(xml)))
                                .unwrap());
                        }
                    };

                    let mut deleted = Vec::new();
                    let mut offsets_to_remove = Vec::new();
                    for k in &keys_to_delete {
                        if let Ok(m) = engine.get_object(b_id, k.as_bytes()) {
                            if m.vol_offset > 0 {
                                offsets_to_remove.push(m.vol_offset);
                            }
                        }
                        let _ = engine.delete_object(b_id, k.as_bytes());
                        deleted.push(DeleteItemResult {
                            key: k.as_str(),
                            version_id: None,
                        });
                    }
                    drop(engine);
                    for off in offsets_to_remove {
                        state.payload_cache.remove(b_id, off);
                    }

                    let xml = delete_objects_result_xml(&deleted, &[]);
                    Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header(CONTENT_TYPE, "application/xml")
                        .body(full_body(Bytes::from(xml)))
                        .unwrap())
                }

                _ => {
                    let err_xml = s3_error_xml("MethodNotAllowed", "Method not allowed", &bucket, "req-err");
                    Ok(Response::builder()
                        .status(StatusCode::METHOD_NOT_ALLOWED)
                        .header(CONTENT_TYPE, "application/xml")
                        .body(full_body(Bytes::from(err_xml)))
                        .unwrap())
                }
            }
        }

        ParsedTarget::Object { bucket, key } => {
            if let Err(err_code) = validate_s3_key(&key) {
                let xml = s3_error_xml(err_code.as_str(), err_code.description(), &key, "req-val-key");
                return Ok(Response::builder()
                    .status(err_code.status_code())
                    .header(CONTENT_TYPE, "application/xml")
                    .body(full_body(Bytes::from(xml)))
                    .unwrap());
            }

            let b_id = {
                let engine = state.engine.read();
                match engine.lookup_bucket(&bucket) {
                    Ok(id) => id,
                    Err(_) => {
                        let xml = s3_error_xml("NoSuchBucket", "The specified bucket does not exist", &bucket, "req-obj");
                        return Ok(Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .header(CONTENT_TYPE, "application/xml")
                            .body(full_body(Bytes::from(xml)))
                            .unwrap());
                    }
                }
            };

            // 1. Multipart POST /{bucket}/{key}?uploads -> CreateMultipartUpload
            if method == Method::POST && query_map.contains_key("uploads") {
                let upload_id = state.multipart.create_upload(&bucket, b_id, &key);
                let xml = initiate_multipart_upload_result_xml(&bucket, &key, &upload_id);
                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header(CONTENT_TYPE, "application/xml")
                    .body(full_body(Bytes::from(xml)))
                    .unwrap());
            }

            // 2. Multipart PUT /{bucket}/{key}?partNumber=N&uploadId=ID -> UploadPart
            if method == Method::PUT && query_map.contains_key("uploadId") && query_map.contains_key("partNumber") {
                let upload_id = &query_map["uploadId"];
                let part_num: u32 = match query_map["partNumber"].parse() {
                    Ok(n) => n,
                    Err(_) => {
                        let xml = s3_error_xml("InvalidArgument", "Invalid partNumber", &key, "req-up-part");
                        return Ok(Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .header(CONTENT_TYPE, "application/xml")
                            .body(full_body(Bytes::from(xml)))
                            .unwrap());
                    }
                };

                let is_chunked = req.headers().get("x-amz-content-sha256").and_then(|v| v.to_str().ok()) == Some("STREAMING-AWS4-HMAC-SHA256-PAYLOAD");
                let signed_headers = req.headers().clone();
                let raw_bytes = match read_body(req.into_body(), MAX_OBJECT_BODY + CHUNK_FRAMING_SLACK, path).await? {
                    Ok(b) => b,
                    Err(resp) => return Ok(resp),
                };
                if !crate::auth::payload_hash_ok(&signed_headers, &raw_bytes) {
                    let xml = s3_error_xml("XAmzContentSHA256Mismatch", "The provided 'x-amz-content-sha256' header does not match what was computed.", &key, "req-sha256");
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header(CONTENT_TYPE, "application/xml")
                        .body(full_body(Bytes::from(xml)))
                        .unwrap());
                }
                let decoded_bytes = if is_chunked {
                    match decode_aws_chunked(&raw_bytes) {
                        Ok(b) => b,
                        Err(e) => {
                            let xml = s3_error_xml(e, "Error decoding chunked body", &key, "req-up-part");
                            return Ok(Response::builder()
                                .status(StatusCode::FORBIDDEN)
                                .header(CONTENT_TYPE, "application/xml")
                                .body(full_body(Bytes::from(xml)))
                                .unwrap());
                        }
                    }
                } else {
                    raw_bytes
                };

                let (hi, lo, hex, raw_md5) = compute_md5(&decoded_bytes);
                let staged_path = match state.multipart.write_staged_part(upload_id, part_num, &decoded_bytes) {
                    Ok(p) => Some(p),
                    Err(e) => return Ok(internal_error_response(&key, &format!("could not store the part: {}", e))),
                };
                let vol_offset = 0;

                match state.multipart.upload_part(
                    upload_id,
                    part_num,
                    vol_offset,
                    decoded_bytes.len() as u64,
                    raw_md5,
                    hi,
                    lo,
                    hex.clone(),
                    staged_path,
                ) {
                    Ok(_) => {
                        return Ok(Response::builder()
                            .status(StatusCode::OK)
                            .header(ETAG, format!("\"{}\"", hex))
                            .header(CONTENT_LENGTH, "0")
                            .body(empty_body())
                            .unwrap());
                    }
                    Err(err) => {
                        let xml = s3_error_xml(err, "Failed to upload part", &key, "req-up-part");
                        let status = if err == "NoSuchUpload" { StatusCode::NOT_FOUND } else { StatusCode::BAD_REQUEST };
                        return Ok(Response::builder()
                            .status(status)
                            .header(CONTENT_TYPE, "application/xml")
                            .body(full_body(Bytes::from(xml)))
                            .unwrap());
                    }
                }
            }

            // 3. Multipart POST /{bucket}/{key}?uploadId=ID -> CompleteMultipartUpload
            if method == Method::POST && query_map.contains_key("uploadId") {
                let upload_id = &query_map["uploadId"];
                let body_bytes = match read_body(req.into_body(), MAX_XML_BODY, path).await? {
                    Ok(b) => b,
                    Err(resp) => return Ok(resp),
                };
                let body_str = String::from_utf8_lossy(&body_bytes);

                // Parse XML <Part><PartNumber>N</PartNumber><ETag>E</ETag></Part>
                let mut declared_parts = Vec::new();
                let mut cursor = body_str.as_ref();
                while let Some(start) = cursor.find("<Part>") {
                    let rest = &cursor[start + 6..];
                    let part_end = match rest.find("</Part>") {
                        Some(e) => e,
                        None => break,
                    };
                    let part_block = &rest[..part_end];
                    cursor = &rest[part_end + 7..];

                    let p_num = part_block
                        .find("<PartNumber>")
                        .and_then(|s| {
                            let r = &part_block[s + 12..];
                            let e = r.find("</PartNumber>")?;
                            r[..e].parse::<u32>().ok()
                        })
                        .unwrap_or(0);

                    let etag = part_block
                        .find("<ETag>")
                        .and_then(|s| {
                            let r = &part_block[s + 6..];
                            let e = r.find("</ETag>")?;
                            Some(r[..e].to_string())
                        })
                        .unwrap_or_default();

                    declared_parts.push((p_num, etag));
                }

                match state.multipart.prepare_complete(upload_id, &declared_parts) {
                    Ok((composite_etag, total_size, extents)) => {
                        // Store object in @flat ObjectMeta
                        let (hi, lo, _, _) = compute_md5(composite_etag.as_bytes());
                        let _key_guard = state.lock_key(b_id, &key).await;
                        if let Err(code) = state.engine.read().has_room_for(b_id, key.as_bytes()) {
                            return Ok(engine_error_response(code, &key));
                        }
                        // Log the part list as a manifest needle, so recovery rebuilds the object.
                        let manifest = Bytes::from(encode_extents_manifest(&state.multipart.staging_dir, total_size, &extents));
                        let manifest_offset = match state.store_payload(
                            b_id,
                            &key,
                            &manifest,
                            AWAN_FLAG_ACTIVE | AWAN_FLAG_MULTIPART,
                            hi,
                            lo,
                        ).await {
                            Ok(off) => off,
                            Err(e) => return Ok(internal_error_response(&key, &format!("could not record the multipart object: {:?}", e))),
                        };
                        // The manifest is metadata, not object bytes: keep it out of the payload cache.
                        state.payload_cache.remove(b_id, manifest_offset);
                        if let Err(code) = state.multipart.commit_complete(upload_id, extents.clone()) {
                            let xml = s3_error_xml(code, "The upload was aborted or completed while this request ran.", &key, "req-complete");
                            return Ok(Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .header(CONTENT_TYPE, "application/xml")
                                .body(full_body(Bytes::from(xml)))
                                .unwrap());
                        }
                        let mut engine = state.engine.write();
                        if let Err(code) = engine.put_object(
                            b_id,
                            key.as_bytes(),
                            manifest_offset,
                            total_size,
                            hi,
                            lo,
                            extents.len() as u16,
                            AWAN_FLAG_ACTIVE | AWAN_FLAG_MULTIPART,
                        ) {
                            return Ok(engine_error_response(code, &key));
                        }

                        let location = format!("http://127.0.0.1:9000/{}/{}", bucket, key);
                        let xml = complete_multipart_upload_result_xml(&bucket, &key, &composite_etag, &location);
                        return Ok(Response::builder()
                            .status(StatusCode::OK)
                            .header(CONTENT_TYPE, "application/xml")
                            .header(ETAG, format!("\"{}\"", composite_etag))
                            .body(full_body(Bytes::from(xml)))
                            .unwrap());
                    }
                    Err(err) => {
                        let xml = s3_error_xml(err, "CompleteMultipartUpload error", &key, "req-cmp-part");
                        let status = if err == "NoSuchUpload" {
                            StatusCode::NOT_FOUND
                        } else {
                            StatusCode::BAD_REQUEST
                        };
                        return Ok(Response::builder()
                            .status(status)
                            .header(CONTENT_TYPE, "application/xml")
                            .body(full_body(Bytes::from(xml)))
                            .unwrap());
                    }
                }
            }

            // 4. Multipart DELETE /{bucket}/{key}?uploadId=ID -> AbortMultipartUpload
            if method == Method::DELETE && query_map.contains_key("uploadId") {
                let upload_id = &query_map["uploadId"];
                // Same key lock as CompleteMultipartUpload, so an abort cannot land between its
                // manifest write and its commit.
                let _key_guard = state.lock_key(b_id, &key).await;
                match state.multipart.abort_upload(upload_id) {
                    Ok(()) => {
                        return Ok(Response::builder()
                            .status(StatusCode::NO_CONTENT)
                            .header(CONTENT_LENGTH, "0")
                            .body(empty_body())
                            .unwrap());
                    }
                    Err(err) => {
                        let xml = s3_error_xml(err, "NoSuchUpload", &key, "req-ab-part");
                        return Ok(Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .header(CONTENT_TYPE, "application/xml")
                            .body(full_body(Bytes::from(xml)))
                            .unwrap());
                    }
                }
            }

            // 5. Multipart GET /{bucket}/{key}?uploadId=ID -> ListParts
            if method == Method::GET && query_map.contains_key("uploadId") {
                let upload_id = &query_map["uploadId"];
                match state.multipart.get_upload(upload_id) {
                    Some(u) => {
                        let parts: Vec<_> = u.parts.values().cloned().collect();
                        let xml = list_parts_result_xml(&bucket, &key, upload_id, &parts);
                        return Ok(Response::builder()
                            .status(StatusCode::OK)
                            .header(CONTENT_TYPE, "application/xml")
                            .body(full_body(Bytes::from(xml)))
                            .unwrap());
                    }
                    None => {
                        let xml = s3_error_xml("NoSuchUpload", "The specified upload does not exist", &key, "req-lp");
                        return Ok(Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .header(CONTENT_TYPE, "application/xml")
                            .body(full_body(Bytes::from(xml)))
                            .unwrap());
                    }
                }
            }

            // Standard Object operations
            match method {
                Method::PUT => {
                    // Check copy source
                    if let Some(copy_source) = req.headers().get("x-amz-copy-source") {
                        let copy_src_str = copy_source.to_str().unwrap_or("").trim_start_matches('/');
                        let (src_bucket, src_key) = match copy_src_str.split_once('/') {
                            Some((b, k)) => (b, k),
                            None => {
                                let xml = s3_error_xml("InvalidArgument", "Invalid copy source", copy_src_str, "req-cp");
                                return Ok(Response::builder()
                                    .status(StatusCode::BAD_REQUEST)
                                    .header(CONTENT_TYPE, "application/xml")
                                    .body(full_body(Bytes::from(xml)))
                                    .unwrap());
                            }
                        };

                        // Scoped so the engine read guard (not Send) is gone before the awaits below.
                        let (src_b_id, src_meta) = {
                        let engine = state.engine.read();
                        let src_b_id = match engine.lookup_bucket(src_bucket) {
                            Ok(id) => id,
                            Err(_) => {
                                let xml = s3_error_xml("NoSuchBucket", "Source bucket does not exist", src_bucket, "req-cp");
                                return Ok(Response::builder()
                                    .status(StatusCode::NOT_FOUND)
                                    .header(CONTENT_TYPE, "application/xml")
                                    .body(full_body(Bytes::from(xml)))
                                    .unwrap());
                            }
                        };
                        let src_meta = match engine.get_object(src_b_id, src_key.as_bytes()) {
                            Ok(m) => m,
                            Err(_) => {
                                let xml = s3_error_xml("NoSuchKey", "Source object does not exist", src_key, "req-cp");
                                return Ok(Response::builder()
                                    .status(StatusCode::NOT_FOUND)
                                    .header(CONTENT_TYPE, "application/xml")
                                    .body(full_body(Bytes::from(xml)))
                                    .unwrap());
                            }
                        };
                        (src_b_id, src_meta)
                        };

                        // The copy gets a needle of its own: it must survive a restart and an
                        // overwrite or delete of its source.
                        let src_flags = unpack_flags(src_meta.bucket_flags);
                        let bytes = match state.read_object_bytes(src_b_id, src_key, src_meta.vol_offset, src_meta.size, src_flags) {
                            Ok(b) => b,
                            Err(e) => return Ok(internal_error_response(&key, &format!("could not read the copy source: {:?}", e))),
                        };
                        let flags = if bytes.is_empty() { AWAN_FLAG_ACTIVE | AWAN_FLAG_ZERO_BYTE } else { AWAN_FLAG_ACTIVE };
                        let _key_guard = state.lock_key(b_id, &key).await;
                        if let Err(code) = state.engine.read().has_room_for(b_id, key.as_bytes()) {
                            return Ok(engine_error_response(code, &key));
                        }
                        let old_info = state.engine.read().get_object(b_id, key.as_bytes()).ok().map(|m| m.vol_offset);
                        let vol_offset = match state.store_payload(b_id, &key, &bytes, flags, src_meta.etag_hi, src_meta.etag_lo).await {
                            Ok(off) => off,
                            Err(e) => return Ok(internal_error_response(&key, &format!("could not store the copy: {:?}", e))),
                        };
                        if let Some(prev_off) = old_info {
                            if prev_off > 0 && prev_off != vol_offset {
                                state.payload_cache.remove(b_id, prev_off);
                                state.needle_crc_map.write().remove(&(b_id, prev_off));
                            }
                        }
                        if let Err(code) = state.engine.write().put_object(
                            b_id,
                            key.as_bytes(),
                            vol_offset,
                            src_meta.size,
                            src_meta.etag_hi,
                            src_meta.etag_lo,
                            0,
                            flags,
                        ) {
                            return Ok(engine_error_response(code, &key));
                        }

                        let etag_hex = md5_to_etag_hex(src_meta.etag_hi, src_meta.etag_lo);
                        let xml = copy_object_result_xml(&etag_hex, "2026-11-10T00:00:00.000Z");
                        return Ok(Response::builder()
                            .status(StatusCode::OK)
                            .header(CONTENT_TYPE, "application/xml")
                            .header(ETAG, format!("\"{}\"", etag_hex))
                            .body(full_body(Bytes::from(xml)))
                            .unwrap());
                    }

                    let if_none_match_star = req
                        .headers()
                        .get("If-None-Match")
                        .and_then(|v| v.to_str().ok())
                        .map(|v| v.trim() == "*")
                        .unwrap_or(false);
                    // Check If-None-Match: * (Atomic mutex creation)
                    if let Some(inm) = req.headers().get("If-None-Match").and_then(|v| v.to_str().ok()) {
                        if inm.trim() == "*" {
                            let engine = state.engine.read();
                            if engine.get_object(b_id, key.as_bytes()).is_ok() {
                                let xml = s3_error_xml("PreconditionFailed", "At least one of the pre-conditions you specified did not hold.", &key, "req-inm");
                                return Ok(Response::builder()
                                    .status(StatusCode::PRECONDITION_FAILED)
                                    .header(CONTENT_TYPE, "application/xml")
                                    .body(full_body(Bytes::from(xml)))
                                    .unwrap());
                            }
                        }
                    }

                    // Standard PutObject
                    let expected_len = req.headers().get(CONTENT_LENGTH).and_then(|v| v.to_str().ok()).and_then(|s| s.parse::<usize>().ok());
                    let is_chunked = req.headers().get("x-amz-content-sha256").and_then(|v| v.to_str().ok()) == Some("STREAMING-AWS4-HMAC-SHA256-PAYLOAD");
                    let content_md5_hdr = req.headers().get("content-md5").and_then(|v| v.to_str().ok()).map(|s| s.trim().to_string());
                    let signed_headers = req.headers().clone();
                    let raw_bytes = match read_body(req.into_body(), MAX_OBJECT_BODY + CHUNK_FRAMING_SLACK, path).await? {
                        Ok(b) => b,
                        Err(resp) => return Ok(resp),
                    };
                    if !crate::auth::payload_hash_ok(&signed_headers, &raw_bytes) {
                        let xml = s3_error_xml("XAmzContentSHA256Mismatch", "The provided 'x-amz-content-sha256' header does not match what was computed.", &key, "req-sha256");
                        return Ok(Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .header(CONTENT_TYPE, "application/xml")
                            .body(full_body(Bytes::from(xml)))
                            .unwrap());
                    }

                    if let Some(exp) = expected_len {
                        if raw_bytes.len() < exp {
                            let xml = s3_error_xml("IncompleteBody", "You did not provide the number of bytes specified by the Content-Length HTTP header.", &key, "req-inc");
                            return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .header(CONTENT_TYPE, "application/xml")
                                .body(full_body(Bytes::from(xml)))
                                .unwrap());
                        }
                    }

                    let decoded_bytes = if is_chunked {
                        match decode_aws_chunked(&raw_bytes) {
                            Ok(b) => b,
                            Err(e) => {
                                let xml = s3_error_xml(e, "Error decoding chunked body", &key, "req-put-obj");
                                return Ok(Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .header(CONTENT_TYPE, "application/xml")
                                    .body(full_body(Bytes::from(xml)))
                                    .unwrap());
                            }
                        }
                    } else {
                        raw_bytes
                    };

                    let payload_len = decoded_bytes.len() as u64;
                    let mut digest = crate::digest::StreamingDigest::new();
                    digest.update(&decoded_bytes);
                    let (etag_hi, etag_lo, etag_hex, raw_md5, _) = digest.finalize();

                    // Verify Content-MD5 header if present
                    if let Some(ref content_md5) = content_md5_hdr {
                        let expected_b64 = crate::digest::base64_encode(&raw_md5);
                        if content_md5 != &expected_b64 {
                            let xml = s3_error_xml("BadDigest", "The Content-MD5 you specified did not match what we received.", &key, "req-bad-digest");
                            return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .header(CONTENT_TYPE, "application/xml")
                                .body(full_body(Bytes::from(xml)))
                                .unwrap());
                        }
                    }

                    let final_etag_hex = if decoded_bytes.is_empty() {
                        "d41d8cd98f00b204e9800998ecf8427e".to_string()
                    } else {
                        etag_hex
                    };

                    let flags = if key.ends_with('/') || decoded_bytes.is_empty() {
                        AWAN_FLAG_ACTIVE | AWAN_FLAG_ZERO_BYTE
                    } else {
                        AWAN_FLAG_ACTIVE
                    };

                    let _key_guard = state.lock_key(b_id, &key).await;
                    if if_none_match_star && state.engine.read().get_object(b_id, key.as_bytes()).is_ok() {
                        let xml = s3_error_xml("PreconditionFailed", "At least one of the pre-conditions you specified did not hold.", &key, "req-inm");
                        return Ok(Response::builder()
                            .status(StatusCode::PRECONDITION_FAILED)
                            .header(CONTENT_TYPE, "application/xml")
                            .body(full_body(Bytes::from(xml)))
                            .unwrap());
                    }
                    if let Err(code) = state.engine.read().has_room_for(b_id, key.as_bytes()) {
                        return Ok(engine_error_response(code, &key));
                    }
                    let old_info = {
                        let engine = state.engine.read();
                        engine.get_object(b_id, key.as_bytes()).ok().map(|m| (m.vol_offset, m.size))
                    };

                    // Zero-byte objects get a needle too, so they and empty overwrites survive a restart.
                    let vol_offset = match state.store_payload(b_id, &key, &decoded_bytes, flags, etag_hi, etag_lo).await {
                        Ok(off) => off,
                        Err(e) => return Ok(internal_error_response(&key, &format!("could not store the object: {:?}", e))),
                    };
                    state.hook("put_stored", &key);

                    if let Some((prev_off, prev_sz)) = old_info {
                        if prev_off > 0 && prev_off != vol_offset {
                            state.payload_cache.remove(b_id, prev_off);
                            state.needle_crc_map.write().remove(&(b_id, prev_off));
                            let raw_len = 64 + key.as_bytes().len() + prev_sz as usize;
                            let dead_bytes = (((raw_len + 4095) / 4096) * 4096) as u64;
                            state.tombstone_bytes.fetch_add(dead_bytes, Ordering::Relaxed);
                        }
                    }

                    let mut engine = state.engine.write();
                    if let Err(code) = engine.put_object(
                        b_id,
                        key.as_bytes(),
                        vol_offset,
                        payload_len,
                        etag_hi,
                        etag_lo,
                        0,
                        flags,
                    ) {
                        return Ok(engine_error_response(code, &key));
                    }

                    let resp = Response::builder()
                        .status(StatusCode::OK)
                        .header(ETAG, format!("\"{}\"", final_etag_hex))
                        .header(CONTENT_LENGTH, "0")
                        .body(empty_body())
                        .unwrap();
                    Ok(resp)
                }

                Method::GET | Method::HEAD => {
                    let (meta, is_multipart) = {
                        let engine = state.engine.read();
                        let m = match engine.get_object(b_id, key.as_bytes()) {
                            Ok(m) => m,
                            Err(_) => {
                                let xml = s3_error_xml("NoSuchKey", "The specified key does not exist.", &key, "req-get-obj");
                                return Ok(Response::builder()
                                    .status(StatusCode::NOT_FOUND)
                                    .header(CONTENT_TYPE, "application/xml")
                                    .body(full_body(Bytes::from(xml)))
                                    .unwrap());
                            }
                        };
                        let flags = unpack_flags(m.bucket_flags);
                        (m, flags & AWAN_FLAG_MULTIPART != 0)
                    };

                    let etag_val = if is_multipart {
                        let part_count = unpack_part_count(meta.bucket_flags);
                        let hex = md5_to_etag_hex(meta.etag_hi, meta.etag_lo);
                        format!("\"{}-{}\"", hex, part_count)
                    } else {
                        format!("\"{}\"", md5_to_etag_hex(meta.etag_hi, meta.etag_lo))
                    };

                    // Conditional headers: If-Match
                    if let Some(if_match) = req.headers().get("If-Match").and_then(|v| v.to_str().ok()) {
                        let if_match = if_match.trim();
                        if if_match != "*" && if_match != etag_val && if_match != etag_val.trim_matches('"') {
                            return Ok(Response::builder()
                                .status(StatusCode::PRECONDITION_FAILED)
                                .header(CONTENT_LENGTH, "0")
                                .body(empty_body())
                                .unwrap());
                        }
                    }

                    // Conditional headers: If-None-Match
                    if let Some(if_none_match) = req.headers().get("If-None-Match").and_then(|v| v.to_str().ok()) {
                        let if_none_match = if_none_match.trim();
                        if if_none_match == "*" || if_none_match == etag_val || if_none_match == etag_val.trim_matches('"') {
                            return Ok(Response::builder()
                                .status(StatusCode::NOT_MODIFIED)
                                .header(ETAG, etag_val)
                                .header(CONTENT_LENGTH, "0")
                                .body(empty_body())
                                .unwrap());
                        }
                    }

                    let range_hdr = req.headers().get("Range").and_then(|v| v.to_str().ok());
                    let range_res = range_hdr.map(|h| parse_range_header(h, meta.size));
                    let is_head = method == Method::HEAD;

                    if let Some(range_opt) = range_res {
                        match range_opt {
                            Ok(Some(range)) => {
                                let sub_bytes = if is_head {
                                    Bytes::new()
                                } else if is_multipart {
                                    if let Some(extents) = state.multipart.get_completed_extents(b_id, &key) {
                                        // 1. Fast Path: Pure Zero-Copy single-extent slicing
                                        let mut part_start = 0u64;
                                        let mut single_extent_slice = None;
                                        for ext in &extents {
                                            let part_end = ext.cumulative_end;
                                            if range.start >= part_start && range.end < part_end {
                                                let offset_in_part = (range.start - part_start) as usize;
                                                let slice_len = (range.end - range.start + 1) as usize;
                                                if let Some(ref path) = ext.staged_path {
                                                    if let Ok(mmap) = state.multipart.get_or_mmap_part(path) {
                                                        if offset_in_part + slice_len <= mmap.len() {
                                                            let full_bytes = Bytes::from_owner(crate::volume::MmapOwner(mmap));
                                                            single_extent_slice = Some(full_bytes.slice(offset_in_part..offset_in_part + slice_len));
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                            part_start = part_end;
                                        }

                                        if let Some(slice) = single_extent_slice {
                                            slice
                                        } else {
                                            // 2. The range crosses parts: send one slice of each part, no copy.
                                            let mut chunks = std::collections::VecDeque::new();
                                            let mut p_start = 0u64;
                                            for ext in extents {
                                                let p_end = ext.cumulative_end;
                                                if p_end > range.start && p_start <= range.end {
                                                    let chunk = match state.part_bytes(b_id, &ext) {
                                                        Ok(b) => b,
                                                        Err(e) => return Ok(internal_error_response(&key, &format!("could not read a part: {:?}", e))),
                                                    };
                                                    let s_start = range.start.saturating_sub(p_start) as usize;
                                                    let s_end = (((range.end + 1).min(p_end) - p_start) as usize).min(chunk.len());
                                                    if s_start < s_end {
                                                        chunks.push_back(chunk.slice(s_start..s_end));
                                                    }
                                                }
                                                p_start = p_end;
                                            }
                                            if chunks.iter().map(|c| c.len() as u64).sum::<u64>() != range.end - range.start + 1 {
                                                return Ok(internal_error_response(&key, "a part of the object is missing"));
                                            }
                                            return Ok(Response::builder()
                                                .status(StatusCode::PARTIAL_CONTENT)
                                                .header(CONTENT_TYPE, "application/octet-stream")
                                                .header(ETAG, etag_val)
                                                .header(ACCEPT_RANGES, "bytes")
                                                .header(CONTENT_RANGE, format!("bytes {}-{}/{}", range.start, range.end, meta.size))
                                                .header(CONTENT_LENGTH, (range.end - range.start + 1).to_string())
                                                .header(LAST_MODIFIED, "Tue, 10 Nov 2026 00:00:00 GMT")
                                                .body(ChunksBody(chunks).boxed())
                                                .unwrap());
                                        }
                                    } else {
                                        return Ok(internal_error_response(&key, "the multipart object's part list is missing"));
                                    }
                                } else {
                                    let full = match state.load_payload(b_id, meta.vol_offset, meta.size as usize) {
                                        Ok(b) => b,
                                        Err(e) => return Ok(internal_error_response(&key, &format!("could not read the object: {:?}", e))),
                                    };
                                    let start = range.start as usize;
                                    let end = range.end as usize;
                                    if start < full.len() {
                                        full.slice(start..(end + 1).min(full.len()))
                                    } else {
                                        Bytes::new()
                                    }
                                };
                                let content_range = format!("bytes {}-{}/{}", range.start, range.end, meta.size);
                                let body = if is_head { empty_body() } else { full_body(sub_bytes) };

                                let resp = Response::builder()
                                    .status(StatusCode::PARTIAL_CONTENT)
                                    .header(CONTENT_TYPE, "application/octet-stream")
                                    .header(ETAG, etag_val)
                                    .header(ACCEPT_RANGES, "bytes")
                                    .header(CONTENT_RANGE, content_range)
                                    .header(CONTENT_LENGTH, (range.end - range.start + 1).to_string())
                                    .header(LAST_MODIFIED, "Tue, 10 Nov 2026 00:00:00 GMT")
                                    .body(body)
                                    .unwrap();
                                return Ok(resp);
                            }
                            Err(()) => {
                                let xml = s3_error_xml("InvalidRange", "The requested range cannot be satisfied.", &key, "req-range-err");
                                let body = if is_head { empty_body() } else { full_body(Bytes::from(xml)) };
                                let resp = Response::builder()
                                    .status(StatusCode::RANGE_NOT_SATISFIABLE)
                                    .header(CONTENT_TYPE, "application/xml")
                                    .header(CONTENT_RANGE, format!("bytes */{}", meta.size))
                                    .body(body)
                                    .unwrap();
                                return Ok(resp);
                            }
                            Ok(None) => {}
                        }
                    }

                    if is_multipart && !is_head {
                        let extents = match state.multipart.get_completed_extents(b_id, &key) {
                            Some(e) => e,
                            None => return Ok(internal_error_response(&key, "the multipart object's part list is missing")),
                        };
                        let mut chunks = std::collections::VecDeque::with_capacity(extents.len());
                        for ext in extents {
                            let chunk = match state.part_bytes(b_id, &ext) {
                                Ok(b) => b,
                                Err(e) => return Ok(internal_error_response(&key, &format!("could not read a part: {:?}", e))),
                            };
                            let n = (ext.part_size as usize).min(chunk.len());
                            chunks.push_back(chunk.slice(..n));
                        }
                        if chunks.iter().map(|c| c.len() as u64).sum::<u64>() != meta.size {
                            return Ok(internal_error_response(&key, "a part of the object is missing"));
                        }
                        return Ok(Response::builder()
                            .status(StatusCode::OK)
                            .header(CONTENT_TYPE, "application/octet-stream")
                            .header(ETAG, etag_val)
                            .header(ACCEPT_RANGES, "bytes")
                            .header(CONTENT_LENGTH, meta.size.to_string())
                            .header(LAST_MODIFIED, "Tue, 10 Nov 2026 00:00:00 GMT")
                            .body(ChunksBody(chunks).boxed())
                            .unwrap());
                    }

                    // Coalesce concurrent hot GET requests via SingleFlight before reading/stitching payload
                    let payload = if is_head {
                        Bytes::new()
                    } else {
                        let single_flight_key = format!("{}/{}/{}", bucket, key, meta.vol_offset);
                        let state_for_read = Arc::clone(&state);
                        state
                            .single_flight
                            .execute_or_join(&single_flight_key, || async move {
                                state_for_read
                                    .load_payload(b_id, meta.vol_offset, meta.size as usize)
                                    .unwrap_or_default()
                            })
                            .await
                    };

                    if !is_head && payload.len() as u64 != meta.size {
                        return Ok(internal_error_response(&key, "the object's bytes could not be read or failed verification"));
                    }
                    let body = if is_head { empty_body() } else { full_body(payload) };
                    let resp = Response::builder()
                        .status(StatusCode::OK)
                        .header(CONTENT_TYPE, "application/octet-stream")
                        .header(ETAG, etag_val)
                        .header(ACCEPT_RANGES, "bytes")
                        .header(CONTENT_LENGTH, meta.size.to_string())
                        .header(LAST_MODIFIED, "Tue, 10 Nov 2026 00:00:00 GMT")
                        .body(body)
                        .unwrap();
                    Ok(resp)
                }

                Method::DELETE => {
                    let _key_guard = state.lock_key(b_id, &key).await;
                    state.hook("delete_start", &key);
                    // Log the tombstone first; only a logged delete removes the row, so a failed
                    // write is a 500 and the object stays (verification/tla, DeleteLogsFirst).
                    match state.write_to_active(b_id, &key, &[], AWAN_FLAG_TOMBSTONE, 0, 0) {
                        Ok((vm, loc, _)) => {
                            state.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
                            state.tombstone_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
                            if let Err(e) = state.make_durable(&vm, loc.seq).await {
                                return Ok(internal_error_response(&key, &format!("could not sync the delete: {:?}", e)));
                            }
                        }
                        Err(e) => return Ok(internal_error_response(&key, &format!("could not record the delete: {:?}", e))),
                    }
                    state.mvcc.tombstone(b_id, &key);
                    let old_info = {
                        let mut engine = state.engine.write();
                        let info = engine.get_object(b_id, key.as_bytes()).ok().map(|m| (m.vol_offset, m.size));
                        let _ = engine.delete_object(b_id, key.as_bytes());
                        info
                    };
                    if let Some((off, sz)) = old_info {
                        if off > 0 {
                            state.payload_cache.remove(b_id, off);
                            state.needle_crc_map.write().remove(&(b_id, off));
                            let raw_len = 64 + key.as_bytes().len() + sz as usize;
                            let dead_bytes = (((raw_len + 4095) / 4096) * 4096) as u64;
                            state.tombstone_bytes.fetch_add(dead_bytes, Ordering::Relaxed);
                        }
                    }
                    Ok(Response::builder()
                        .status(StatusCode::NO_CONTENT)
                        .header(CONTENT_LENGTH, "0")
                        .body(empty_body())
                        .unwrap())
                }

                _ => {
                    let err_xml = s3_error_xml("MethodNotAllowed", "Method not allowed", &key, "req-err");
                    Ok(Response::builder()
                        .status(StatusCode::METHOD_NOT_ALLOWED)
                        .header(CONTENT_TYPE, "application/xml")
                        .body(full_body(Bytes::from(err_xml)))
                        .unwrap())
                }
            }
        }
    }
}
