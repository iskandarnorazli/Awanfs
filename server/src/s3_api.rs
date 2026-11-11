//! awan-storage/server/src/s3_api.rs
//! S3 REST Wire Protocol Dispatcher, Routing & RFC 9110 Range Engine
//! Reference: awan-storage/API_SPECIFICATION.md and awan-storage/HYBRID_POC_SPECIFICATION.md

use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, HOST, LAST_MODIFIED};
use hyper::{Method, Request, Response, StatusCode};
use md5::{Digest, Md5};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::aws_chunked::decode_aws_chunked;
use crate::cache::PayloadCache;
use crate::coalesce::SingleFlight;
use crate::compaction::{compact_volume, CompactionReport};
use crate::ffi_bridge::*;
use crate::lifecycle::*;
use crate::multipart::*;
use crate::mvcc::MvccStore;
use crate::sigv4::*;
use crate::volume::{VolumeError, VolumeManager};
use crate::xml::*;
use std::path::{Path, PathBuf};

pub type ResponseBody = BoxBody<Bytes, std::convert::Infallible>;

fn full_body(bytes: Bytes) -> ResponseBody {
    Full::new(bytes).boxed()
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
    pub volume_manager: Arc<RwLock<Option<Arc<VolumeManager>>>>,
    pub needle_crc_map: Arc<RwLock<HashMap<(u32, u64), u32>>>,
    pub payload_cache: Arc<PayloadCache>,
    pub next_vol_offset: Arc<RwLock<u64>>,
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
        let vol_mgr = VolumeManager::open(&default_dir, 1).ok().map(Arc::new);
        let initial_bytes = if let Some(ref vm) = vol_mgr { vm.offset() } else { 0 };
        Self {
            data_dir: None,
            total_volume_bytes: Arc::new(AtomicU64::new(initial_bytes)),
            tombstone_bytes: Arc::new(AtomicU64::new(0)),
            active_volume_id: Arc::new(AtomicU32::new(1)),
            engine: Arc::new(RwLock::new(AwanStorageEngine::new(100_000, 4_194_304, 65536))),
            multipart: Arc::new(MultipartManager::with_staging_dir(default_dir.join(".staging"))),
            sigv4_cache: Arc::new(SigV4KeyCache::new()),
            lifecycle: Arc::new(LifecycleManager::new()),
            single_flight: Arc::new(SingleFlight::new()),
            mvcc: Arc::new(MvccStore::new()),
            volume_manager: Arc::new(RwLock::new(vol_mgr)),
            needle_crc_map: Arc::new(RwLock::new(HashMap::new())),
            payload_cache: Arc::new(PayloadCache::default_auto_scaled()),
            next_vol_offset: Arc::new(RwLock::new(4096)),
        }
    }

    pub fn with_data_dir(dir: &Path) -> Self {
        let _ = std::fs::create_dir_all(dir);
        let vol_mgr = VolumeManager::open(dir, 1).ok().map(Arc::new);
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

        // 2. Replay needle volume
        if let Some(ref vm) = vol_mgr {
            initial_vol_offset = vm.offset().max(4096);
            let mut crcs = needle_crc_map.write();
            let _ = vm.recover_needles(|b_id, key, offset, size, etag_hi, etag_lo, flags, crc32c| {
                crcs.insert((b_id as u32, offset), crc32c);
                if flags & AWAN_FLAG_TOMBSTONE != 0 {
                    let _ = engine.delete_object(b_id as u32, key.as_bytes());
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
            });
        }

        let initial_bytes = if let Some(ref vm) = vol_mgr { vm.offset() } else { 0 };

        Self {
            data_dir: Some(dir.to_path_buf()),
            total_volume_bytes: Arc::new(AtomicU64::new(initial_bytes)),
            tombstone_bytes: Arc::new(AtomicU64::new(0)),
            active_volume_id: Arc::new(AtomicU32::new(1)),
            engine: Arc::new(RwLock::new(engine)),
            multipart: Arc::new(MultipartManager::with_staging_dir(dir.join(".staging"))),
            sigv4_cache: Arc::new(SigV4KeyCache::new()),
            lifecycle: Arc::new(LifecycleManager::new()),
            single_flight: Arc::new(SingleFlight::new()),
            mvcc: Arc::new(MvccStore::new()),
            volume_manager: Arc::new(RwLock::new(vol_mgr)),
            needle_crc_map,
            payload_cache: Arc::new(PayloadCache::default_auto_scaled()),
            next_vol_offset: Arc::new(RwLock::new(initial_vol_offset)),
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

    pub fn perform_online_compaction(&self) -> Result<CompactionReport, VolumeError> {
        let dir = match self.data_dir {
            Some(ref d) => d.clone(),
            None => return Ok(CompactionReport::default()),
        };
        let cur_vol = self.active_volume_id.load(Ordering::SeqCst);
        let target_vol = cur_vol + 1;

        let (live_keys, key_buckets) = {
            let engine = self.engine.read();
            let mut keys = HashSet::new();
            let mut kb = HashMap::new();
            for bucket in engine.buckets.values() {
                for k_vec in bucket.key_index.keys() {
                    let k_str = String::from_utf8_lossy(k_vec).to_string();
                    keys.insert(k_str.clone());
                    kb.insert(k_str, bucket.id);
                }
            }
            (keys, kb)
        };

        let report = compact_volume(&dir, cur_vol, target_vol, &live_keys)?;
        let new_mgr = Arc::new(VolumeManager::open(&dir, target_vol)?);
        let new_vol_bytes = new_mgr.offset();

        {
            let mut engine = self.engine.write();
            let mut crc_map = self.needle_crc_map.write();
            for (k_str, loc) in &report.new_locations {
                if let Some(&b_id) = key_buckets.get(k_str) {
                    let _ = engine.update_object_offset(b_id, k_str.as_bytes(), loc.offset);
                    crc_map.insert((b_id, loc.offset), loc.payload_crc32c);
                }
            }
        }

        self.payload_cache.clear();
        *self.volume_manager.write() = Some(new_mgr);
        self.active_volume_id.store(target_vol, Ordering::SeqCst);
        self.total_volume_bytes.store(new_vol_bytes, Ordering::Relaxed);
        self.tombstone_bytes.store(0, Ordering::Relaxed);

        Ok(report)
    }

    pub fn store_payload(&self, b_id: u32, key: &str, data: &Bytes, flags: u16, etag_hi: u64, etag_lo: u64) -> u64 {
        if data.is_empty() {
            return 0;
        }
        let vm_opt = self.volume_manager.read().clone();
        if let Some(ref vm) = vm_opt {
            if let Ok(loc) = vm.write_needle(b_id as u16, key, data, flags, etag_hi, etag_lo) {
                self.needle_crc_map.write().insert((b_id, loc.offset), loc.payload_crc32c);
                self.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
                // Cache hot payloads up to 4 MiB in dynamic auto-scaling LRU cache
                if data.len() <= 4 * 1024 * 1024 {
                    self.payload_cache.insert(b_id, loc.offset, data.clone());
                }
                return loc.offset;
            }
        }
        let off = self.allocate_offset(data.len());
        self.payload_cache.insert(b_id, off, data.clone());
        off
    }

    pub fn load_payload(&self, b_id: u32, vol_offset: u64, expected_size: usize) -> Bytes {
        if expected_size == 0 || vol_offset == 0 {
            return Bytes::new();
        }
        if let Some(cached) = self.payload_cache.get(b_id, vol_offset) {
            return cached;
        }
        let vm_opt = self.volume_manager.read().clone();
        if let Some(ref vm) = vm_opt {
            let expected_crc = self
                .needle_crc_map
                .read()
                .get(&(b_id, vol_offset))
                .copied()
                .unwrap_or(0);
            if let Ok(bytes) = vm.read_needle_bytes(1, vol_offset, expected_crc) {
                if bytes.len() <= 4 * 1024 * 1024 {
                    self.payload_cache.insert(b_id, vol_offset, bytes.clone());
                }
                return bytes;
            }
        }
        Bytes::from(vec![0u8; expected_size])
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
    let auth_header = req.headers().get("Authorization").and_then(|v| v.to_str().ok());
    if let Err(code) = validate_sigv4_auth(auth_header, &query_map, now_sec) {
        let xml = s3_error_xml(code, "Authentication failed", path, "req-auth");
        return Ok(Response::builder()
            .status(StatusCode::FORBIDDEN)
            .header(CONTENT_TYPE, "application/xml")
            .body(full_body(Bytes::from(xml)))
            .unwrap());
    }

    let target = parse_s3_target(path, host_hdr);

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
                        let body_bytes = req.into_body().collect().await?.to_bytes();
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
                    let body_bytes = req.into_body().collect().await?.to_bytes();
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
                let raw_bytes = req.into_body().collect().await?.to_bytes();
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
                let staged_path = state.multipart.write_staged_part(upload_id, part_num, &decoded_bytes).ok();
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
                let body_bytes = req.into_body().collect().await?.to_bytes();
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

                match state.multipart.complete_upload(upload_id, &declared_parts) {
                    Ok((composite_etag, total_size, extents)) => {
                        // Store object in @flat ObjectMeta
                        let (hi, lo, _, _) = compute_md5(composite_etag.as_bytes());
                        let first_vol_offset = extents.first().map(|e| e.vol_offset).unwrap_or(0);
                        let mut engine = state.engine.write();
                        engine.put_object(
                            b_id,
                            key.as_bytes(),
                            first_vol_offset,
                            total_size,
                            hi,
                            lo,
                            extents.len() as u16,
                            AWAN_FLAG_ACTIVE | AWAN_FLAG_MULTIPART,
                        ).unwrap();

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
                        drop(engine);

                        let mut engine = state.engine.write();
                        engine.put_object(
                            b_id,
                            key.as_bytes(),
                            src_meta.vol_offset,
                            src_meta.size,
                            src_meta.etag_hi,
                            src_meta.etag_lo,
                            0,
                            AWAN_FLAG_ACTIVE | AWAN_FLAG_SHARED_REF,
                        ).unwrap();

                        let etag_hex = md5_to_etag_hex(src_meta.etag_hi, src_meta.etag_lo);
                        let xml = copy_object_result_xml(&etag_hex, "2026-11-10T00:00:00.000Z");
                        return Ok(Response::builder()
                            .status(StatusCode::OK)
                            .header(CONTENT_TYPE, "application/xml")
                            .header(ETAG, format!("\"{}\"", etag_hex))
                            .body(full_body(Bytes::from(xml)))
                            .unwrap());
                    }

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
                    let raw_bytes = req.into_body().collect().await?.to_bytes();

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

                    let old_info = {
                        let engine = state.engine.read();
                        engine.get_object(b_id, key.as_bytes()).ok().map(|m| (m.vol_offset, m.size))
                    };

                    let vol_offset = if decoded_bytes.is_empty() {
                        0
                    } else {
                        state.store_payload(b_id, &key, &decoded_bytes, flags, etag_hi, etag_lo)
                    };

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
                    engine.put_object(
                        b_id,
                        key.as_bytes(),
                        vol_offset,
                        payload_len,
                        etag_hi,
                        etag_lo,
                        0,
                        flags,
                    ).unwrap();

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
                                            // 2. Multi-extent boundary crossing: stitch only the cross-boundary slice
                                            let req_len = (range.end - range.start + 1) as usize;
                                            let mut out = Vec::with_capacity(req_len);
                                            let mut p_start = 0u64;
                                            for ext in extents {
                                                let p_end = ext.cumulative_end;
                                                if p_end > range.start && p_start <= range.end {
                                                    let chunk = if let Some(ref path) = ext.staged_path {
                                                        if let Ok(mmap) = state.multipart.get_or_mmap_part(path) {
                                                            Bytes::from_owner(crate::volume::MmapOwner(mmap))
                                                        } else {
                                                            state.load_payload(
                                                                b_id,
                                                                ext.vol_offset,
                                                                ext.part_size as usize,
                                                            )
                                                        }
                                                    } else {
                                                        state.load_payload(
                                                            b_id,
                                                            ext.vol_offset,
                                                            ext.part_size as usize,
                                                        )
                                                    };
                                                    let s_start = range.start.saturating_sub(p_start) as usize;
                                                    let s_end = ((range.end + 1).min(p_end) - p_start) as usize;
                                                    if s_start < chunk.len() {
                                                        out.extend_from_slice(&chunk[s_start..s_end.min(chunk.len())]);
                                                    }
                                                }
                                                p_start = p_end;
                                            }
                                            Bytes::from(out)
                                        }
                                    } else {
                                        Bytes::from(vec![0u8; (range.end - range.start + 1) as usize])
                                    }
                                } else {
                                    let full = state.load_payload(b_id, meta.vol_offset, meta.size as usize);
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

                    // Coalesce concurrent hot GET requests via SingleFlight before reading/stitching payload
                    let payload = if is_head {
                        Bytes::new()
                    } else {
                        let single_flight_key = format!("{}/{}/{}", bucket, key, meta.vol_offset);
                        let state_for_read = Arc::clone(&state);
                        let key_for_read = key.clone();
                        state
                            .single_flight
                            .execute_or_join(&single_flight_key, || async move {
                                if is_multipart {
                                    if let Some(extents) =
                                        state_for_read.multipart.get_completed_extents(b_id, &key_for_read)
                                    {
                                        let mut stitched = Vec::with_capacity(meta.size as usize);
                                        for ext in extents {
                                            let chunk = if let Some(ref path) = ext.staged_path {
                                                if let Ok(mmap) = state_for_read.multipart.get_or_mmap_part(path) {
                                                    Bytes::from_owner(crate::volume::MmapOwner(mmap))
                                                } else {
                                                    state_for_read.load_payload(
                                                        b_id,
                                                        ext.vol_offset,
                                                        ext.part_size as usize,
                                                    )
                                                }
                                            } else {
                                                state_for_read.load_payload(
                                                    b_id,
                                                    ext.vol_offset,
                                                    ext.part_size as usize,
                                                )
                                            };
                                            stitched.extend_from_slice(&chunk);
                                        }
                                        Bytes::from(stitched)
                                    } else {
                                        Bytes::from(vec![0u8; meta.size as usize])
                                    }
                                } else {
                                    state_for_read.load_payload(
                                        b_id,
                                        meta.vol_offset,
                                        meta.size as usize,
                                    )
                                }
                            })
                            .await
                    };

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
                    let vm_opt = state.volume_manager.read().clone();
                    if let Some(ref vm) = vm_opt {
                        if let Ok(loc) = vm.write_needle(b_id as u16, &key, &[], AWAN_FLAG_TOMBSTONE, 0, 0) {
                            state.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
                            state.tombstone_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
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
