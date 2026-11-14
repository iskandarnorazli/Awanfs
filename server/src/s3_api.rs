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
use tracing::Instrument;
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

/// PUT bodies above this size are written to the volume as they arrive instead of being held
/// in memory (#36). Smaller ones are collected, and cached after the write.
pub const STREAM_PUT_MIN: u64 = 4 * 1024 * 1024;

/// Reads and drops what is left of a request body, at most `limit` bytes.
async fn drain_body(body: &mut hyper::body::Incoming, limit: u64) {
    let mut seen = 0u64;
    while seen < limit {
        match body.frame().await {
            Some(Ok(f)) => seen += f.data_ref().map_or(0, |d| d.len() as u64),
            _ => break,
        }
    }
}

/// The end of a PutObject once the needle is durable: index row, bookkeeping, answer.
#[allow(clippy::too_many_arguments)]
fn finish_put(
    state: &S3ServiceState,
    b_id: u32,
    key: &str,
    old_info: Option<(u64, u64)>,
    vol_offset: u64,
    payload_len: u64,
    etag_hi: u64,
    etag_lo: u64,
    flags: u16,
    etag_hex: &str,
) -> Response<ResponseBody> {
    if let Some((prev_off, prev_sz)) = old_info {
        if prev_off > 0 && prev_off != vol_offset {
            state.payload_cache.remove(b_id, prev_off);
            state.needle_crc_map.write().remove(&(b_id, prev_off));
            let raw_len = 64 + key.len() + prev_sz as usize;
            let dead_bytes = raw_len.div_ceil(4096) as u64 * 4096;
            state.tombstone_bytes.fetch_add(dead_bytes, Ordering::Relaxed);
        }
    }
    if let Err(code) = state.engine.write().put_object(b_id, key.as_bytes(), vol_offset, payload_len, etag_hi, etag_lo, 0, flags) {
        return engine_error_response(code, key);
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(ETAG, format!("\"{}\"", etag_hex))
        .header(CONTENT_LENGTH, "0")
        .body(empty_body())
        .unwrap()
}

fn put_ok_response(etag_hex: &str, version_id: Option<&str>) -> Response<ResponseBody> {
    let mut b = Response::builder().status(StatusCode::OK).header(ETAG, format!("\"{}\"", etag_hex)).header(CONTENT_LENGTH, "0");
    if let Some(v) = version_id {
        b = b.header("x-amz-version-id", v);
    }
    b.body(empty_body()).unwrap()
}

fn bad_request(code: &str, msg: &str, resource: &str) -> Response<ResponseBody> {
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(s3_error_xml(code, msg, resource, "req-put"))))
        .unwrap()
}

/// One page of a listing.
struct ListPage {
    objects: Vec<(Vec<u8>, crate::meta_index::ObjectMeta)>,
    prefixes: Vec<Vec<u8>>,
    truncated: bool,
    /// Where the next page starts (exclusive), when truncated.
    next: Option<Vec<u8>>,
    /// The last key or common prefix on this page.
    last_name: Option<Vec<u8>>,
}

/// Lists up to `max` keys and common prefixes after `start`. With a delimiter, keys sharing
/// the part of the key up to the first delimiter after `prefix` collapse into one common prefix,
/// and the scan jumps past them (prefix + 0xFF, a byte no UTF-8 key contains).
fn list_page(state: &S3ServiceState, b_id: u32, prefix: &[u8], delimiter: &[u8], start: &[u8], max: usize) -> ListPage {
    let mut page = ListPage { objects: Vec::new(), prefixes: Vec::new(), truncated: false, next: None, last_name: None };
    let mut cursor = start.to_vec();
    'outer: loop {
        let batch = state.engine.read().list_keys(b_id, prefix, &cursor, 1000);
        if batch.is_empty() {
            break;
        }
        for (k, meta) in batch {
            let group = if delimiter.is_empty() {
                None
            } else {
                k[prefix.len()..].windows(delimiter.len()).position(|w| w == delimiter).map(|p| k[..prefix.len() + p + delimiter.len()].to_vec())
            };
            if page.objects.len() + page.prefixes.len() >= max {
                page.truncated = true;
                page.next = Some(cursor.clone());
                break 'outer;
            }
            match group {
                Some(cp) => {
                    let mut past = cp.clone();
                    past.push(0xFF);
                    if cp.as_slice() <= start {
                        // The marker was this common prefix (V1 NextMarker): already returned.
                        cursor = past;
                        continue 'outer;
                    }
                    page.last_name = Some(cp.clone());
                    page.prefixes.push(cp);
                    cursor = past;
                    continue 'outer;
                }
                None => {
                    cursor = k.clone();
                    page.last_name = Some(k.clone());
                    page.objects.push((k, meta));
                }
            }
        }
    }
    page
}

fn encode_hex_token(b: &[u8]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

fn decode_hex_token(t: &str) -> Option<Vec<u8>> {
    if t.len() % 2 != 0 {
        return None;
    }
    (0..t.len()).step_by(2).map(|i| u8::from_str_radix(t.get(i..i + 2)?, 16).ok()).collect()
}

/// `encoding-type=url`: every byte but unreserved characters and `/` as %XX, so any URL
/// decoder (botocore uses unquote_plus) gives the key back.
pub fn s3_url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~' | b'/') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
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
        let suffix_len: u64 = range_number(suffix_str).ok_or(())?;
        if suffix_len == 0 {
            return Err(());
        }
        let take = suffix_len.min(object_size);
        let start = object_size - take;
        let end = object_size - 1;
        return Ok(Some(ByteRange { start, end }));
    }

    if let Some((start_str, end_str)) = spec.split_once('-') {
        let start: u64 = range_number(start_str).ok_or(())?;
        if start >= object_size {
            return Err(());
        }
        if end_str.is_empty() {
            return Ok(Some(ByteRange {
                start,
                end: object_size - 1,
            }));
        }
        let parsed_end: u64 = range_number(end_str).ok_or(())?;
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
                            key: percent_decode_path(trimmed_path),
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
            key: percent_decode_path(key),
        }
    } else {
        ParsedTarget::Bucket { bucket }
    }
}

/// Decodes %XX in a URL path. Unlike query decoding, `+` stays `+`. S3 object keys are the
/// decoded path: a client sends "my%20file.txt" for the key "my file.txt". Keys used to be
/// stored as sent, encoded.
pub fn percent_decode_path(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let (Some(h), Some(l)) = (hex_digit(b[i + 1]), hex_digit(b[i + 2])) {
                out.push(h << 4 | l);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// RFC 9110 `1*DIGIT`: digits only, saturating at u64::MAX (a last-pos past the end of any
/// object is still a valid range).
fn range_number(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some(s.bytes().fold(0u64, |n, d| n.saturating_mul(10).saturating_add((d - b'0') as u64)))
}

pub fn percent_decode(s: &str) -> String {
    let mut out = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            // Bytes, not a str slice: a slice inside a multi-byte character would panic.
            if let (Some(h), Some(l)) = (hex_digit(bytes[i + 1]), hex_digit(bytes[i + 2])) {
                out.push(h << 4 | l);
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
    /// Bytes per second compaction may copy; 0 = no limit (#28). The binary sets 50 MB/s.
    pub compaction_rate: Arc<AtomicU64>,
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
    /// Why the server is not ready yet (`None` when it is); see `/_awanfs/health/ready` (#44).
    pub not_ready: Arc<RwLock<Option<&'static str>>>,
    /// Request counts, latencies and bytes for `/_awanfs/metrics` (#42).
    pub metrics: Arc<crate::metrics::Metrics>,
    /// Where the per-request JSON log lines go; `None` logs nothing (#41).
    pub access_log: Arc<RwLock<Option<crate::request_log::LogSink>>>,
    /// Where S3-format server access log lines go; `None` (the default) writes none (#46).
    pub audit_log: Arc<RwLock<Option<crate::request_log::LogSink>>>,
    /// Users and policies from the config file (#53); `None` = only the root key pair.
    pub iam: Arc<RwLock<Option<Arc<crate::iam::ReloadingIam>>>>,
    /// Versioning status per bucket id (#31); absent = never set.
    pub versioning: Arc<RwLock<HashMap<u32, crate::versions::Versioning>>>,
    /// Version histories of the keys of versioned buckets (#31). Lock order: never take the
    /// engine lock while holding this one.
    pub versions: Arc<RwLock<crate::versions::VersionStore>>,
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
            compaction_rate: Arc::new(AtomicU64::new(0)),
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
            not_ready: Arc::new(RwLock::new(None)),
            metrics: Arc::new(crate::metrics::Metrics::default()),
            access_log: Arc::new(RwLock::new(None)),
            audit_log: Arc::new(RwLock::new(None)),
            iam: Arc::new(RwLock::new(None)),
            versioning: Arc::new(RwLock::new(HashMap::new())),
            versions: Arc::new(RwLock::new(crate::versions::VersionStore::default())),
        }
    }

    /// Opens a data directory, panicking when it cannot be served; see `try_with_data_dir`.
    pub fn with_data_dir(dir: &Path) -> Self {
        Self::try_with_data_dir(dir).unwrap_or_else(|e| panic!("cannot open {}: {}", dir.display(), e))
    }

    /// Opens a data directory and replays it. Fails, instead of serving a subset, when a volume
    /// cannot be opened or was written in a newer on-disk format (#50).
    pub fn try_with_data_dir(dir: &Path) -> Result<Self, String> {
        let _ = std::fs::create_dir_all(dir);
        let work = Arc::new(WorkCounters::default());
        // Every volume in id order; writes go to the last one (#37).
        let mut volumes: BTreeMap<u32, Arc<VolumeManager>> = BTreeMap::new();
        let mut ids = volume_ids(dir);
        if ids.is_empty() {
            ids.push(1);
        }
        for id in ids {
            // Only a brand-new volume 1 is created sparse; volumes from rollover stay as they are.
            let new_volume = !dir.join(format!("volume_{:04}.dat", id)).exists();
            let mut vm = VolumeManager::open_mode(dir, id, new_volume && id == 1).map_err(|e| format!("volume {}: {}", id, e))?;
            vm.attach_work_counters(Arc::clone(&work));
            volumes.insert(id, Arc::new(vm));
        }
        let vol_mgr = volumes.values().next_back().cloned();
        let multipart = Arc::new(
            MultipartManager::with_staging_dir(dir.join(".staging")).with_work_counters(Arc::clone(&work)),
        );
        let mut engine = AwanStorageEngine::new(100_000, 4_194_304, 65536);
        let db_path = dir.join("metadata.db");
        if let Ok(spill) = crate::spill::SpillIndex::open(&db_path) {
            // Rebuilt from the volume replay below: its commits are not durable (#13).
            let _ = spill.clear();
            engine.attach_spill_index(spill);
        }
        let needle_crc_map = Arc::new(RwLock::new(HashMap::new()));
        let mut initial_vol_offset = 4096;

        // 1. Replay buckets manifest
        let manifest_path = dir.join("buckets.manifest");
        let mut versioning_map: HashMap<u32, crate::versions::Versioning> = HashMap::new();
        let mut version_store = crate::versions::VersionStore::default();
        if let Ok(content) = std::fs::read_to_string(&manifest_path) {
            for line in content.lines() {
                let line = line.trim();
                // `~<id>:<Enabled|Suspended>`: a bucket's versioning status (#31).
                if let Some(rest) = line.strip_prefix('~') {
                    if let Some((id_str, status)) = rest.split_once(':') {
                        if let (Ok(id), Some(v)) = (id_str.parse::<u32>(), crate::versions::Versioning::parse(status)) {
                            versioning_map.insert(id, v);
                        }
                    }
                    continue;
                }
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
                            versioning_map.remove(&id);
                        }
                    }
                }
            }
        }

        // 2. Replay the needle volumes, oldest first: writes only move to a newer volume, so
        // volume order is log order. Versions are ordered by their ids instead (versions.rs).
        let mut mp_extents: HashMap<(u32, u64), Vec<PartExtent>> = HashMap::new();
        for vm in volumes.values() {
            initial_vol_offset = vm.offset().max(4096);
            let mut crcs = needle_crc_map.write();
            // Collect the log first: a multipart manifest needle is read back from the volume,
            // and the events are applied in log order so later writes win.
            let mut events = Vec::new();
            let _ = vm.scan_needles(|n| events.push(n));
            for n in events {
                let (b_id, key, local, size, etag_hi, etag_lo, flags, crc32c) = (n.bucket_id, n.key, n.offset, n.payload_size, n.etag_hi, n.etag_lo, n.flags, n.payload_crc32c);
                let offset = global_offset(vm.id(), local);
                crcs.insert((b_id as u32, offset), crc32c);
                // Keys with a version history replay through the version store (#31).
                if flags & (crate::meta_index::FLAG_VERSIONED | crate::meta_index::FLAG_VERSION_DELETE) != 0 || version_store.has(b_id as u32, &key) {
                    let b = b_id as u32;
                    if !version_store.has(b, &key) {
                        let before = engine.get_object(b, key.as_bytes()).ok().map(|m| null_version_of(&m));
                        if let Some(nv) = before.filter(|v| v.flags & AWAN_FLAG_MULTIPART != 0) {
                            if let Some(ext) = multipart.get_completed_extents(b, &key) {
                                mp_extents.insert((b, nv.vol_offset), ext);
                            }
                        }
                        version_store.start(b, &key, before);
                    }
                    let null = flags & crate::meta_index::FLAG_NULL_ID != 0 || flags & crate::meta_index::FLAG_VERSIONED == 0;
                    let ev = if flags & crate::meta_index::FLAG_VERSION_DELETE != 0 {
                        match vm.read_needle_bytes(vm.id(), local, crc32c) {
                            Ok(p) if p.len() == 8 => crate::versions::Event::Remove(u64::from_le_bytes(p[..8].try_into().unwrap())),
                            _ => continue,
                        }
                    } else if flags & AWAN_FLAG_TOMBSTONE != 0 {
                        crate::versions::Event::Put(crate::versions::Version { ts: n.created_at_ns, null, marker: true, vol_offset: offset, size: 0, etag_hi: 0, etag_lo: 0, part_count: 0, flags })
                    } else {
                        let (size, part_count) = if flags & AWAN_FLAG_MULTIPART != 0 {
                            match vm.read_needle_bytes(vm.id(), local, crc32c).ok().map(|b| decode_extents_manifest(&multipart.staging_dir, &b)) {
                                Some(Ok((total, extents))) => {
                                    let pc = extents.len() as u16;
                                    mp_extents.insert((b, offset), extents);
                                    (total, pc)
                                }
                                Some(Err(crate::multipart::ManifestError::NewerVersion { found, supported })) => {
                                    return Err(format!(
                                        "the multipart manifest of {} was written in format {} by a newer AwanFS; this version reads up to {}",
                                        key, found, supported
                                    ))
                                }
                                _ => continue,
                            }
                        } else if flags & crate::meta_index::FLAG_COMPRESSED != 0 {
                            match vm.payload_prefix(local, key.len(), crate::compress::PREFIX_LEN).ok().and_then(|p| crate::compress::logical_size(&p)) {
                                Some(n) => (n, 0),
                                None => continue,
                            }
                        } else {
                            (size, 0)
                        };
                        crate::versions::Event::Put(crate::versions::Version { ts: n.created_at_ns, null, marker: false, vol_offset: offset, size, etag_hi, etag_lo, part_count, flags })
                    };
                    version_store.apply(b, &key, ev);
                    match version_store.current(b, &key) {
                        Some(v) if !v.marker => {
                            let _ = engine.put_object(b, key.as_bytes(), v.vol_offset, v.size, v.etag_hi, v.etag_lo, v.part_count, v.flags);
                            if v.flags & AWAN_FLAG_MULTIPART != 0 {
                                if let Some(ext) = mp_extents.get(&(b, v.vol_offset)) {
                                    multipart.restore_completed(b, &key, ext.clone());
                                }
                            }
                        }
                        _ => {
                            let _ = engine.delete_object(b, key.as_bytes());
                        }
                    }
                    version_store.tidy(b, &key);
                    continue;
                }
                if flags & AWAN_FLAG_TOMBSTONE != 0 {
                    let _ = engine.delete_object(b_id as u32, key.as_bytes());
                } else if flags & AWAN_FLAG_MULTIPART != 0 {
                    let manifest = match vm.read_needle_bytes(vm.id(), local, crc32c) {
                        Ok(b) => match decode_extents_manifest(&multipart.staging_dir, &b) {
                            Ok(m) => Some(m),
                            Err(crate::multipart::ManifestError::NewerVersion { found, supported }) => {
                                return Err(format!(
                                    "the multipart manifest of {} was written in format {} by a newer AwanFS; this version reads up to {}",
                                    key, found, supported
                                ))
                            }
                            Err(crate::multipart::ManifestError::Malformed) => None,
                        },
                        Err(_) => None,
                    };
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
                    // A compressed needle's index row holds the object's size, not the stored one (#51).
                    let size = if flags & crate::meta_index::FLAG_COMPRESSED != 0 {
                        match vm.payload_prefix(local, key.len(), crate::compress::PREFIX_LEN).ok().and_then(|p| crate::compress::logical_size(&p)) {
                            Some(n) => n,
                            None => continue,
                        }
                    } else {
                        size
                    };
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

        Ok(Self {
            data_dir: Some(dir.to_path_buf()),
            total_volume_bytes: Arc::new(AtomicU64::new(initial_bytes)),
            tombstone_bytes: Arc::new(AtomicU64::new(0)),
            compaction_rate: Arc::new(AtomicU64::new(0)),
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
            not_ready: Arc::new(RwLock::new(None)),
            metrics: Arc::new(crate::metrics::Metrics::default()),
            access_log: Arc::new(RwLock::new(None)),
            audit_log: Arc::new(RwLock::new(None)),
            iam: Arc::new(RwLock::new(None)),
            versioning: Arc::new(RwLock::new(versioning_map)),
            versions: Arc::new(RwLock::new(version_store)),
        })
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

    /// Deletes one key the way DeleteObject does: under its key lock, the tombstone is logged
    /// and synced first, and only then is the index row removed. An error means the object is
    /// still there.
    /// Deletes a key: removes it, or in a versioned bucket writes a delete marker, whose
    /// version id it returns (#31).
    pub async fn delete_key(&self, b_id: u32, key: &str) -> Result<Option<String>, String> {
        let _key_guard = self.lock_key(b_id, key).await;
        self.hook("delete_start", key);
        if let Some((extra, ts)) = self.version_stamp(b_id, key) {
            let flags = AWAN_FLAG_TOMBSTONE | extra;
            let (vm, loc, g) = self.write_to_active_at(b_id, key, &[], flags, 0, 0, Some(ts)).map_err(|e| format!("could not record the delete: {:?}", e))?;
            self.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
            self.make_durable(&vm, loc.seq).await.map_err(|e| format!("could not sync the delete: {:?}", e))?;
            self.needle_crc_map.write().insert((b_id, g), loc.payload_crc32c);
            self.mvcc.tombstone(b_id, key);
            let v = crate::versions::Version { ts, null: flags & crate::meta_index::FLAG_NULL_ID != 0, marker: true, vol_offset: g, size: 0, etag_hi: 0, etag_lo: 0, part_count: 0, flags };
            self.record_version(b_id, key, crate::versions::Event::Put(v)).map_err(|c| format!("index error {} for the delete marker", c))?;
            return Ok(Some(v.id_string()));
        }
        // Log the tombstone first; only a logged delete removes the row, so a failed write is
        // an error and the object stays (verification/tla, DeleteLogsFirst).
        match self.write_to_active(b_id, key, &[], AWAN_FLAG_TOMBSTONE, 0, 0) {
            Ok((vm, loc, _)) => {
                self.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
                self.tombstone_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
                self.make_durable(&vm, loc.seq).await.map_err(|e| format!("could not sync the delete: {:?}", e))?;
            }
            Err(e) => return Err(format!("could not record the delete: {:?}", e)),
        }
        self.mvcc.tombstone(b_id, key);
        let old_info = {
            let mut engine = self.engine.write();
            let info = engine.get_object(b_id, key.as_bytes()).ok().map(|m| (m.vol_offset, m.size));
            let _ = engine.delete_object(b_id, key.as_bytes());
            info
        };
        if let Some((off, sz)) = old_info {
            if off > 0 {
                self.payload_cache.remove(b_id, off);
                self.needle_crc_map.write().remove(&(b_id, off));
                let raw_len = 64 + key.len() + sz as usize;
                let dead_bytes = raw_len.div_ceil(4096) as u64 * 4096;
                self.tombstone_bytes.fetch_add(dead_bytes, Ordering::Relaxed);
            }
        }
        Ok(None)
    }

    /// `DELETE ?versionId=`: removes one version for good (#31). Returns it, or `None` when the
    /// key has no such version.
    pub async fn delete_version(&self, b_id: u32, key: &str, id: u64) -> Result<Option<crate::versions::Version>, String> {
        let _key_guard = self.lock_key(b_id, key).await;
        let before = self.engine.read().get_object(b_id, key.as_bytes()).ok().map(|m| null_version_of(&m));
        let target = {
            let mut vs = self.versions.write();
            if !vs.has(b_id, key) {
                // The object of a key with no history is its null version.
                if id != 0 || before.is_none() {
                    return Ok(None);
                }
                vs.start(b_id, key, before);
            }
            vs.find(b_id, key, id)
        };
        let Some(target) = target else {
            self.versions.write().tidy(b_id, key);
            return Ok(None);
        };
        let flags = AWAN_FLAG_TOMBSTONE | crate::meta_index::FLAG_VERSION_DELETE;
        let (vm, loc, _) = self.write_to_active(b_id, key, &id.to_le_bytes(), flags, 0, 0).map_err(|e| format!("could not record the delete: {:?}", e))?;
        self.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
        self.tombstone_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
        self.make_durable(&vm, loc.seq).await.map_err(|e| format!("could not sync the delete: {:?}", e))?;
        self.record_version(b_id, key, crate::versions::Event::Remove(id)).map_err(|c| format!("index error {} after deleting a version", c))?;
        Ok(Some(target))
    }

    /// The flags and creation time a write of `key` takes in its bucket's versioning status;
    /// `None` when the bucket was never versioned (#31). Call under the key lock.
    pub fn version_stamp(&self, b_id: u32, key: &str) -> Option<(u16, u64)> {
        let status = self.versioning.read().get(&b_id).copied().unwrap_or_default();
        let extra = match status {
            crate::versions::Versioning::Unset => return None,
            crate::versions::Versioning::Enabled => crate::meta_index::FLAG_VERSIONED,
            crate::versions::Versioning::Suspended => crate::meta_index::FLAG_VERSIONED | crate::meta_index::FLAG_NULL_ID,
        };
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
        Some((extra, self.versions.read().next_ts(b_id, key, now)))
    }

    /// Records a durable version event: updates the history, drops the needles it made
    /// unreachable, and points the index row at the current version (#31).
    pub fn record_version(&self, b_id: u32, key: &str, ev: crate::versions::Event) -> Result<(), i32> {
        let before = self.engine.read().get_object(b_id, key.as_bytes()).ok().map(|m| null_version_of(&m));
        let (dead, current) = {
            let mut vs = self.versions.write();
            vs.start(b_id, key, before);
            let dead = vs.apply(b_id, key, ev);
            let current = vs.current(b_id, key);
            vs.tidy(b_id, key);
            (dead, current)
        };
        for v in dead {
            if v.vol_offset > 0 {
                self.payload_cache.remove(b_id, v.vol_offset);
                self.needle_crc_map.write().remove(&(b_id, v.vol_offset));
                let dead_bytes = (64 + key.len() as u64 + v.size).div_ceil(4096) * 4096;
                self.tombstone_bytes.fetch_add(dead_bytes, Ordering::Relaxed);
            }
        }
        match current {
            Some(v) if !v.marker => {
                self.engine.write().put_object(b_id, key.as_bytes(), v.vol_offset, v.size, v.etag_hi, v.etag_lo, v.part_count, v.flags)?;
                if v.flags & AWAN_FLAG_MULTIPART != 0 {
                    if let Some(ext) = self.manifest_extents(b_id, v.vol_offset) {
                        self.multipart.restore_completed(b_id, key, ext);
                    }
                }
            }
            _ => {
                let _ = self.engine.write().delete_object(b_id, key.as_bytes());
            }
        }
        Ok(())
    }

    /// The part list in the manifest needle at `vol_offset`.
    fn manifest_extents(&self, b_id: u32, vol_offset: u64) -> Option<Vec<PartExtent>> {
        let bytes = self.read_stored(b_id, vol_offset).ok()?;
        decode_extents_manifest(&self.multipart.staging_dir, &bytes).ok().map(|(_, e)| e)
    }

    /// The bytes of one version (#31).
    pub fn read_version_bytes(&self, b_id: u32, key: &str, v: &crate::versions::Version) -> Result<Bytes, VolumeError> {
        if v.flags & AWAN_FLAG_MULTIPART == 0 {
            return self.read_object_bytes(b_id, key, v.vol_offset, v.size, v.flags);
        }
        let extents = self.manifest_extents(b_id, v.vol_offset).ok_or(VolumeError::CorruptIndex)?;
        let mut out = Vec::with_capacity(v.size as usize);
        for ext in extents {
            let chunk = self.part_bytes(b_id, &ext)?;
            let n = (ext.part_size as usize).min(chunk.len());
            out.extend_from_slice(&chunk[..n]);
        }
        if out.len() as u64 != v.size {
            return Err(VolumeError::CorruptIndex);
        }
        Ok(Bytes::from(out))
    }

    /// Records a bucket's versioning status in the manifest and in memory (#31).
    pub fn set_versioning(&self, b_id: u32, status: crate::versions::Versioning) -> std::io::Result<()> {
        if let Some(ref d) = self.data_dir {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().create(true).append(true).open(d.join("buckets.manifest"))?;
            writeln!(f, "~{}:{}", b_id, status.as_str())?;
            f.sync_all()?;
        }
        self.versioning.write().insert(b_id, status);
        Ok(())
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

    /// (dead bytes, used bytes) of a volume: a needle is live when an index row points at it.
    /// Reads every needle header of the volume, so it costs about one read per needle.
    pub fn volume_usage(&self, vol_id: u32) -> Result<(u64, u64), VolumeError> {
        let vm = self.volumes.read().get(&vol_id).cloned().ok_or(VolumeError::CorruptIndex)?;
        let mut needles = Vec::new();
        vm.recover_needles(|b_id, key, offset, size, _, _, _, _| needles.push((b_id as u32, key, offset, size)))?;
        let engine = self.engine.read();
        let mut dead = 0;
        for (b_id, key, offset, size) in needles {
            let total = (64 + key.len() as u64 + size).div_ceil(crate::direct_io::SECTOR_SIZE as u64) * crate::direct_io::SECTOR_SIZE as u64;
            let g = global_offset(vol_id, offset);
            let live = engine.get_object(b_id, key.as_bytes()).map(|m| m.vol_offset == g).unwrap_or(false) || self.versions.read().holds_offset(b_id, &key, g);
            if !live {
                dead += total;
            }
        }
        Ok((dead, vm.offset().saturating_sub(crate::direct_io::SECTOR_SIZE as u64)))
    }

    /// One step of automatic compaction (#14, owner decision 15): compacts the oldest volume
    /// when at least `threshold` of its used bytes are dead. A lone active volume is sealed and
    /// compacted only once it holds at least `min_bytes`, so a small server does not roll over
    /// a fresh volume on every pass. Returns the report when it compacted.
    pub async fn auto_compact_once(&self, threshold: f64, min_bytes: u64) -> Result<Option<CompactionReport>, VolumeError> {
        if self.data_dir.is_none() {
            return Ok(None);
        }
        let active = self.volume_manager.read().clone().ok_or_else(no_volume_error)?;
        let oldest = match self.volumes.read().keys().next().copied() {
            Some(id) => id,
            None => return Ok(None),
        };
        if oldest == active.id() && active.offset() < min_bytes {
            return Ok(None);
        }
        let (dead, used) = self.volume_usage(oldest)?;
        if used == 0 || (dead as f64) < threshold * used as f64 {
            return Ok(None);
        }
        Ok(Some(self.perform_online_compaction().await?))
    }

    /// Runs `auto_compact_once` every `every` while the state is alive. The binary starts this
    /// unless `--auto-compaction off`; the library never starts it by itself.
    pub fn start_auto_compaction(self: &Arc<Self>, every: std::time::Duration, threshold: f64, min_bytes: u64) {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                let Some(state) = weak.upgrade() else { return };
                match state.auto_compact_once(threshold, min_bytes).await {
                    Ok(Some(r)) => eprintln!(
                        "auto-compaction: moved {} live needles, reclaimed {} dead ones",
                        r.live_needles_copied, r.dead_needles_reclaimed
                    ),
                    Ok(None) => {}
                    Err(e) => eprintln!("auto-compaction failed: {:?}", e),
                }
            }
        });
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
        old.scan_needles(|n| needles.push((n.bucket_id as u32, n.key, n.offset, n.payload_size, n.etag_hi, n.etag_lo, n.flags, n.payload_crc32c, n.created_at_ns)))?;

        let mut report = CompactionReport::default();
        let mut written: BTreeMap<u32, Arc<VolumeManager>> = BTreeMap::new();
        let mut throttle = crate::throttle::Throttle::new(self.compaction_rate.load(Ordering::Relaxed));
        for (b_id, key, local, size, etag_hi, etag_lo, flags, crc, created_at) in needles {
            let g_old = global_offset(old_id, local);
            // A delete marker still in a history is a version and is kept (#31); other
            // tombstones only matter until the needles they cover are gone, and those are
            // older, so already compacted.
            if flags & AWAN_FLAG_TOMBSTONE != 0 && (flags & crate::meta_index::FLAG_VERSIONED == 0 || !self.versions.read().holds_offset(b_id, &key, g_old)) {
                report.dead_needles_reclaimed += 1;
                continue;
            }
            let _guard = self.lock_key(b_id, &key).await;
            let current = self.engine.read().get_object(b_id, key.as_bytes()).map(|m| m.vol_offset == g_old).unwrap_or(false);
            let live = current || self.versions.read().holds_offset(b_id, &key, g_old);
            if !live {
                report.dead_needles_reclaimed += 1;
                continue;
            }
            self.hook("compact_copy", &key);
            let bytes = if size == 0 { Bytes::new() } else { old.read_needle_bytes(old_id, local, crc)? };
            let copied = bytes.len() as u64;
            // The creation time moves with the needle: it is a version's id (#31).
            let (vm, loc, g_new) = self.write_to_active_at(b_id, &key, &bytes, flags, etag_hi, etag_lo, Some(created_at))?;
            written.insert(vm.id(), Arc::clone(&vm));
            self.versions.write().move_offset(b_id, &key, g_old, g_new);
            if current {
                if let Err(code) = self.engine.write().update_object_offset(b_id, key.as_bytes(), g_new) {
                    return Err(VolumeError::Io(std::io::Error::new(std::io::ErrorKind::Other, format!("index error {} while moving {}", code, key))));
                }
            }
            {
                let mut crcs = self.needle_crc_map.write();
                crcs.remove(&(b_id, g_old));
                crcs.insert((b_id, g_new), loc.payload_crc32c);
            }
            self.payload_cache.remove(b_id, g_old);
            report.live_needles_copied += 1;
            report.new_locations.insert(key, loc);
            drop(_guard);
            // Let requests run between copies, and pace the copies (#28), with no key locked.
            tokio::task::yield_now().await;
            throttle.spend(copied).await;
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
        if flags & crate::meta_index::FLAG_COMPRESSED != 0 {
            return self.load_compressed(b_id, vol_offset, size);
        }
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

    /// Whether new objects in `bucket` are compressed: `[[buckets]]` in the config (#51).
    pub fn compresses(&self, bucket: &str) -> bool {
        self.iam.read().as_ref().map(|i| i.current().compresses(bucket)).unwrap_or(false)
    }

    /// `store_payload` for a whole object: compressed when its bucket asks for it and the
    /// bytes shrink (#51). Returns the offset and the flags the needle was written with.
    #[allow(clippy::too_many_arguments)]
    pub async fn store_object(&self, b_id: u32, bucket: &str, key: &str, data: &Bytes, flags: u16, etag_hi: u64, etag_lo: u64, created_at: Option<u64>) -> Result<(u64, u16), VolumeError> {
        let plain = flags & (AWAN_FLAG_MULTIPART | AWAN_FLAG_ZERO_BYTE | AWAN_FLAG_TOMBSTONE) == 0;
        if plain && self.compresses(bucket) {
            if let Some(enc) = crate::compress::encode(data) {
                let f = flags | crate::meta_index::FLAG_COMPRESSED;
                let off = self.store_payload_at(b_id, key, &Bytes::from(enc), f, etag_hi, etag_lo, created_at).await?;
                return Ok((off, f));
            }
        }
        Ok((self.store_payload_at(b_id, key, data, flags, etag_hi, etag_lo, created_at).await?, flags))
    }

    /// The payload bytes as stored, verified against their CRC.
    fn read_stored(&self, b_id: u32, vol_offset: u64) -> Result<Bytes, VolumeError> {
        if vol_offset == 0 {
            return Err(VolumeError::CorruptIndex);
        }
        let (vm, local) = self.volume_for(vol_offset)?;
        let crc = self.needle_crc_map.read().get(&(b_id, vol_offset)).copied().ok_or(VolumeError::CorruptIndex)?;
        vm.read_needle_bytes(vm.id(), local, crc)
    }

    /// Bytes `start..=end` of a compressed object, decoding only the blocks they overlap.
    pub fn load_compressed_range(&self, b_id: u32, vol_offset: u64, size: u64, start: u64, end: u64) -> Result<Bytes, VolumeError> {
        if let Some(cached) = self.payload_cache.get(b_id, vol_offset) {
            let e = ((end + 1) as usize).min(cached.len());
            return Ok(cached.slice((start as usize).min(e)..e));
        }
        let stored = self.read_stored(b_id, vol_offset)?;
        let (bytes, _) = crate::compress::decode_range(&stored, start, end + 1).map_err(decode_error)?;
        if bytes.len() as u64 != (end + 1).min(size).saturating_sub(start) {
            return Err(VolumeError::CorruptIndex);
        }
        Ok(Bytes::from(bytes))
    }

    /// A whole compressed object; cached as object bytes up to 4 MiB.
    pub fn load_compressed(&self, b_id: u32, vol_offset: u64, size: u64) -> Result<Bytes, VolumeError> {
        if let Some(cached) = self.payload_cache.get(b_id, vol_offset) {
            return Ok(cached);
        }
        let stored = self.read_stored(b_id, vol_offset)?;
        let bytes = Bytes::from(crate::compress::decode(&stored).map_err(decode_error)?);
        if bytes.len() as u64 != size {
            return Err(VolumeError::CorruptIndex);
        }
        if bytes.len() <= 4 * 1024 * 1024 {
            self.payload_cache.insert(b_id, vol_offset, bytes.clone());
        }
        Ok(bytes)
    }

    /// Writes the payload to the volume and waits until it is durable. An error means the
    /// write is not acknowledged.
    pub async fn store_payload(&self, b_id: u32, key: &str, data: &Bytes, flags: u16, etag_hi: u64, etag_lo: u64) -> Result<u64, VolumeError> {
        self.store_payload_at(b_id, key, data, flags, etag_hi, etag_lo, None).await
    }

    /// `store_payload` with a given needle creation time (a version id, #31).
    #[allow(clippy::too_many_arguments)]
    pub async fn store_payload_at(&self, b_id: u32, key: &str, data: &Bytes, flags: u16, etag_hi: u64, etag_lo: u64, created_at: Option<u64>) -> Result<u64, VolumeError> {
        let (vm, loc, offset) = tracing::info_span!("volume.write", bytes = data.len() as u64).in_scope(|| self.write_to_active_at(b_id, key, data, flags, etag_hi, etag_lo, created_at))?;
        self.make_durable(&vm, loc.seq).instrument(tracing::info_span!("volume.sync")).await?;
        self.needle_crc_map.write().insert((b_id, offset), loc.payload_crc32c);
        self.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
        // Cache hot payloads up to 4 MiB in dynamic auto-scaling LRU cache; the cache holds
        // object bytes, so a compressed payload is cached when it is first read. It holds the
        // request's buffer, which the per-object and total caps bound. Caching a view of the
        // volume mapping instead (#126, reverted) has to read the payload back, and volumes are
        // written with direct I/O: that faults every byte in from disk and took peak RSS over
        // the ratio workloads from 117.0 MiB to 2258.9 MiB.
        if !data.is_empty() && data.len() <= 4 * 1024 * 1024 && flags & crate::meta_index::FLAG_COMPRESSED == 0 {
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
        self.write_to_active_at(b_id, key, data, flags, etag_hi, etag_lo, None)
    }

    /// `write_to_active` with a given needle creation time (#31: version ids; compaction).
    #[allow(clippy::too_many_arguments)]
    pub fn write_to_active_at(
        &self,
        b_id: u32,
        key: &str,
        data: &[u8],
        flags: u16,
        etag_hi: u64,
        etag_lo: u64,
        created_at: Option<u64>,
    ) -> Result<(Arc<VolumeManager>, NeedleLocation, u64), VolumeError> {
        let mut attempts = 0;
        loop {
            let vm = self.volume_manager.read().clone().ok_or_else(no_volume_error)?;
            // An older AwanFS reads a format 1 volume and would serve compressed bytes as the
            // object: compressed needles go to a format 2 volume (decision 20, #51).
            if flags & FORMAT_2_FLAGS != 0 && vm.format() < 2 && attempts < 8 {
                attempts += 1;
                self.roll_volume(&vm)?;
                continue;
            }
            match vm.write_needle_at(b_id as u16, key, data, flags, etag_hi, etag_lo, created_at) {
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

    /// Starts a streamed needle on the active volume, rolling over when it is full (#36, #37).
    pub fn begin_needle_on_active(&self, b_id: u32, key: &str, len: u64, flags: u16) -> Result<crate::volume::NeedleWriter, VolumeError> {
        let mut attempts = 0;
        loop {
            let vm = self.volume_manager.read().clone().ok_or_else(no_volume_error)?;
            if flags & FORMAT_2_FLAGS != 0 && vm.format() < 2 && attempts < 8 {
                attempts += 1;
                self.roll_volume(&vm)?;
                continue;
            }
            match vm.begin_needle(b_id as u16, key, len, flags) {
                Ok(w) => return Ok(w),
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

/// Needle flags that only format 2 volumes may hold (FORMAT.md).
const FORMAT_2_FLAGS: u16 = crate::meta_index::FLAG_COMPRESSED | crate::meta_index::FLAG_VERSIONED | crate::meta_index::FLAG_NULL_ID | crate::meta_index::FLAG_VERSION_DELETE;

/// An index row as the null version of its key (#31).
fn null_version_of(m: &crate::meta_index::ObjectMeta) -> crate::versions::Version {
    crate::versions::Version {
        ts: 0,
        null: true,
        marker: false,
        vol_offset: m.vol_offset,
        size: m.size,
        etag_hi: m.etag_hi,
        etag_lo: m.etag_lo,
        part_count: unpack_part_count(m.bucket_flags),
        flags: unpack_flags(m.bucket_flags),
    }
}

fn version_etag(v: &crate::versions::Version) -> String {
    let hex = md5_to_etag_hex(v.etag_hi, v.etag_lo);
    if v.flags & AWAN_FLAG_MULTIPART != 0 {
        format!("\"{}-{}\"", hex, v.part_count)
    } else {
        format!("\"{}\"", hex)
    }
}

fn no_such_version(key: &str) -> Response<ResponseBody> {
    let xml = s3_error_xml("NoSuchVersion", "The specified version does not exist.", key, "req-version");
    Response::builder().status(StatusCode::NOT_FOUND).header(CONTENT_TYPE, "application/xml").body(full_body(Bytes::from(xml))).unwrap()
}

/// The response of a PUT that wrote a version (#31).
#[allow(clippy::too_many_arguments)]
fn finish_version_put(state: &S3ServiceState, b_id: u32, key: &str, ts: u64, flags: u16, vol_offset: u64, size: u64, etag_hi: u64, etag_lo: u64, part_count: u16) -> Result<String, Response<ResponseBody>> {
    let v = crate::versions::Version { ts, null: flags & crate::meta_index::FLAG_NULL_ID != 0, marker: false, vol_offset, size, etag_hi, etag_lo, part_count, flags };
    match state.record_version(b_id, key, crate::versions::Event::Put(v)) {
        Ok(()) => Ok(v.id_string()),
        Err(code) => Err(engine_error_response(code, key)),
    }
}

/// GET or HEAD of a key with a version history, or of `?versionId=` (#31).
/// The version `id` names (`None`: the current one), or `None` when there is no such version.
/// GET and CopyObject both look versions up here, so they cannot disagree (#139). A key with no
/// history has only its index row, which is the null version.
fn find_version(state: &S3ServiceState, b_id: u32, key: &str, id: Option<u64>) -> Option<crate::versions::Version> {
    let row = state.engine.read().get_object(b_id, key.as_bytes()).ok().map(|m| null_version_of(&m));
    let vs = state.versions.read();
    match id {
        None => vs.current(b_id, key),
        Some(i) if vs.has(b_id, key) => vs.find(b_id, key, i),
        Some(0) => row,
        Some(_) => None,
    }
}

fn version_get_response(state: &S3ServiceState, b_id: u32, key: &str, vid: Option<&str>, head: bool, range: Option<&str>) -> Response<ResponseBody> {
    let id = match vid {
        Some(s) => match crate::versions::parse_id(s) {
            Some(i) => Some(i),
            None => return no_such_version(key),
        },
        None => None,
    };
    let v = find_version(state, b_id, key, id);
    let Some(v) = v else {
        if id.is_some() {
            return no_such_version(key);
        }
        let xml = s3_error_xml("NoSuchKey", "The specified key does not exist.", key, "req-get-obj");
        return Response::builder().status(StatusCode::NOT_FOUND).header(CONTENT_TYPE, "application/xml").body(full_body(Bytes::from(xml))).unwrap();
    };
    if v.marker {
        let (status, code, msg) = if id.is_some() {
            (StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed", "The specified method is not allowed against a delete marker.")
        } else {
            (StatusCode::NOT_FOUND, "NoSuchKey", "The specified key does not exist.")
        };
        let body = if head { empty_body() } else { full_body(Bytes::from(s3_error_xml(code, msg, key, "req-get-obj"))) };
        return Response::builder()
            .status(status)
            .header(CONTENT_TYPE, "application/xml")
            .header("x-amz-delete-marker", "true")
            .header("x-amz-version-id", v.id_string())
            .body(body)
            .unwrap();
    }
    let bytes = if head {
        Bytes::new()
    } else {
        match state.read_version_bytes(b_id, key, &v) {
            Ok(b) => b,
            Err(e) => return internal_error_response(key, &format!("could not read the version: {:?}", e)),
        }
    };
    let builder = Response::builder()
        .header(CONTENT_TYPE, "application/octet-stream")
        .header(ETAG, version_etag(&v))
        .header(ACCEPT_RANGES, "bytes")
        .header("x-amz-version-id", v.id_string())
        .header(LAST_MODIFIED, "Tue, 10 Nov 2026 00:00:00 GMT");
    if let Some(h) = range {
        match parse_range_header(h, v.size) {
            Ok(Some(r)) => {
                let body = if head { empty_body() } else { full_body(bytes.slice(r.start as usize..(r.end + 1) as usize)) };
                return builder
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header(CONTENT_RANGE, format!("bytes {}-{}/{}", r.start, r.end, v.size))
                    .header(CONTENT_LENGTH, (r.end - r.start + 1).to_string())
                    .body(body)
                    .unwrap();
            }
            Err(()) => {
                return Response::builder()
                    .status(StatusCode::RANGE_NOT_SATISFIABLE)
                    .header(CONTENT_TYPE, "application/xml")
                    .header(CONTENT_RANGE, format!("bytes */{}", v.size))
                    .body(full_body(Bytes::from(s3_error_xml("InvalidRange", "The requested range cannot be satisfied.", key, "req-range-err"))))
                    .unwrap();
            }
            Ok(None) => {}
        }
    }
    let body = if head { empty_body() } else { full_body(bytes) };
    builder.status(StatusCode::OK).header(CONTENT_LENGTH, v.size.to_string()).body(body).unwrap()
}

/// ListObjectVersions over the index and the version histories (#31). Truncates at a key
/// boundary; a key with more versions than `max_keys` is listed whole.
fn list_versions_response(state: &S3ServiceState, b_id: u32, bucket: &str, prefix: &str, key_marker: &str, max_keys: usize, url: bool) -> Response<ResponseBody> {
    let rows = state.engine.read().list_keys(b_id, prefix.as_bytes(), key_marker.as_bytes(), max_keys + 1);
    let more_rows = rows.len() > max_keys;
    let last_row = rows.last().map(|(k, _)| String::from_utf8_lossy(k).into_owned());
    let mut keys: BTreeMap<String, Option<crate::meta_index::ObjectMeta>> = rows.into_iter().map(|(k, m)| (String::from_utf8_lossy(&k).into_owned(), Some(m))).collect();
    let histories: HashMap<String, Vec<crate::versions::Version>> = {
        let vs = state.versions.read();
        vs.keys_of(b_id, prefix)
            .into_iter()
            .filter(|k| k.as_str() > key_marker && (!more_rows || last_row.as_deref().map_or(true, |l| k.as_str() <= l)))
            .map(|k| {
                let list = vs.versions(b_id, &k).map(|l| l.to_vec()).unwrap_or_default();
                (k, list)
            })
            .collect()
    };
    for k in histories.keys() {
        keys.entry(k.clone()).or_insert(None);
    }
    let enc = |s: &str| if url { s3_url_encode(s) } else { s.to_string() };
    let mut entries = Vec::new();
    let mut next = None;
    let mut truncated = false;
    let total = keys.len();
    for (i, (k, row)) in keys.into_iter().enumerate() {
        let versions: Vec<crate::versions::Version> = match histories.get(&k) {
            Some(h) => h.clone(),
            None => row.iter().map(null_version_of).collect(),
        };
        if !entries.is_empty() && entries.len() + versions.len() > max_keys {
            truncated = true;
            break;
        }
        for (j, v) in versions.iter().enumerate() {
            entries.push(VersionListing {
                key: enc(&k),
                version_id: v.id_string(),
                latest: j == 0,
                marker: v.marker,
                last_modified: crate::versions::iso8601(v.ts),
                etag: version_etag(v).trim_matches('"').to_string(),
                size: v.size,
            });
        }
        next = Some(enc(&k));
        if entries.len() >= max_keys && (i + 1 < total || more_rows) {
            truncated = true;
            break;
        }
    }
    let xml = list_versions_xml(bucket, &enc(prefix), &enc(key_marker), max_keys as u32, truncated, if truncated { next.as_deref() } else { None }, url, &entries);
    Response::builder().status(StatusCode::OK).header(CONTENT_TYPE, "application/xml").body(full_body(Bytes::from(xml))).unwrap()
}

fn decode_error(e: crate::compress::CompressError) -> VolumeError {
    VolumeError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
}

/// The client's address, put into the request's extensions by the reactor.
#[derive(Clone, Copy, Debug)]
pub struct RemoteAddr(pub std::net::SocketAddr);

/// Reserved prefix for the server's own endpoints. `_` is not allowed in bucket names, so
/// nothing here can shadow a bucket (#44).
pub const ADMIN_PREFIX: &str = "/_awanfs/";

fn plain(status: StatusCode, text: &str) -> Response<ResponseBody> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(full_body(Bytes::from(format!("{}\n", text))))
        .unwrap()
}

impl S3ServiceState {
    /// `None` when the server can take writes, else the reason it cannot.
    pub fn readiness(&self) -> Option<String> {
        if let Some(r) = *self.not_ready.read() {
            return Some(r.to_string());
        }
        let vm = match self.volume_manager.read().clone() {
            Some(vm) => vm,
            None => return Some("no volume is open".to_string()),
        };
        // A full volume rolls over on the next write (#37); it is only "not ready" when even
        // the smallest needle cannot fit in a fresh volume of this span.
        if vm.max_span() < 2 * crate::direct_io::SECTOR_SIZE as u64 {
            return Some(format!("volume span {} leaves no room for a needle", vm.max_span()));
        }
        None
    }

    /// Syncs every open volume, whatever the durability mode: the last step of a graceful
    /// shutdown, so `fast` mode loses nothing on a clean stop (#49).
    pub fn sync_all_volumes(&self) -> Result<(), VolumeError> {
        let vols: Vec<_> = self.volumes.read().values().cloned().collect();
        for vm in vols {
            vm.sync_now()?;
        }
        if let Some(vm) = self.volume_manager.read().clone() {
            vm.sync_now()?;
        }
        Ok(())
    }

    /// Marks the server not ready (for example while recovering), or ready again with `None`.
    pub fn set_not_ready(&self, reason: Option<&'static str>) {
        *self.not_ready.write() = reason;
    }
}

/// `/_awanfs/health/live` and `/_awanfs/health/ready`, answered without authentication.
fn admin_response(state: &S3ServiceState, method: &Method, path: &str) -> Response<ResponseBody> {
    if method != Method::GET && method != Method::HEAD {
        return plain(StatusCode::METHOD_NOT_ALLOWED, "GET or HEAD");
    }
    match path {
        "/_awanfs/health/live" => plain(StatusCode::OK, "live"),
        "/_awanfs/health/ready" => match state.readiness() {
            None => plain(StatusCode::OK, "ready"),
            Some(reason) => plain(StatusCode::SERVICE_UNAVAILABLE, &format!("not ready: {}", reason)),
        },
        "/_awanfs/metrics" => Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")
            .body(full_body(Bytes::from(render_metrics(state))))
            .unwrap(),
        _ => plain(StatusCode::NOT_FOUND, "no such endpoint"),
    }
}

/// Everything `/_awanfs/metrics` reports, in Prometheus text format.
pub fn render_metrics(state: &S3ServiceState) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(8192);
    state.metrics.render(&mut out);
    let used: u64 = state.volumes.read().values().map(|v| v.offset()).sum();
    let (free, active_id) = match state.volume_manager.read().clone() {
        Some(vm) => (vm.max_span().saturating_sub(vm.offset()), vm.id()),
        None => (0, 0),
    };
    let (hits, misses) = state.payload_cache.hit_counts();
    let w = state.work.snapshot();
    let gauges: [(&str, &str, &str, u64); 14] = [
        ("awanfs_volume_used_bytes", "gauge", "Bytes written into all open volumes, including dead needles.", used),
        ("awanfs_volume_free_span_bytes", "gauge", "Span left in the active volume before it rolls over.", free),
        ("awanfs_volumes_open", "gauge", "Open volumes.", state.volumes.read().len() as u64),
        ("awanfs_active_volume_id", "gauge", "Id of the volume new writes go to.", active_id as u64),
        ("awanfs_payload_cache_hits_total", "counter", "Payload cache lookups that hit.", hits),
        ("awanfs_payload_cache_misses_total", "counter", "Payload cache lookups that missed.", misses),
        ("awanfs_payload_cache_bytes", "gauge", "Bytes held by the payload cache.", state.payload_cache.current_bytes() as u64),
        ("awanfs_disk_writes_total", "counter", "Disk write calls (work counters).", w.disk_writes),
        ("awanfs_disk_write_bytes_total", "counter", "Bytes passed to disk write calls.", w.disk_write_bytes),
        ("awanfs_disk_syncs_total", "counter", "fsync / FlushFileBuffers calls.", w.disk_syncs),
        ("awanfs_disk_reads_total", "counter", "Disk read calls (not mmap page faults).", w.disk_reads),
        ("awanfs_payload_copies_total", "counter", "Copies of payload bytes into server buffers.", w.payload_copies),
        ("awanfs_payload_copy_bytes_total", "counter", "Payload bytes copied.", w.payload_copy_bytes),
        ("awanfs_ready", "gauge", "1 when /_awanfs/health/ready answers 200.", state.readiness().is_none() as u64),
    ];
    for (name, kind, help, v) in gauges {
        let _ = writeln!(out, "# HELP {} {}\n# TYPE {} {}\n{} {}", name, help, name, kind, name, v);
    }
    out
}

/// The S3 operation a request is, for the metrics.
fn operation_name(method: &Method, target: &ParsedTarget, query: &str) -> &'static str {
    let has = |k: &str| query.split('&').any(|p| p == k || p.starts_with(&format!("{}=", k)));
    match target {
        ParsedTarget::Service => "ListBuckets",
        ParsedTarget::Bucket { .. } => match *method {
            Method::PUT if has("versioning") => "PutBucketVersioning",
            Method::GET if has("versioning") => "GetBucketVersioning",
            Method::GET if has("versions") => "ListObjectVersions",
            Method::PUT if has("lifecycle") => "PutBucketLifecycle",
            Method::GET if has("lifecycle") => "GetBucketLifecycle",
            Method::POST if has("delete") => "DeleteObjects",
            Method::PUT => "CreateBucket",
            Method::DELETE => "DeleteBucket",
            Method::HEAD => "HeadBucket",
            Method::GET => "ListObjects",
            _ => "Other",
        },
        ParsedTarget::Object { .. } => match *method {
            Method::POST if has("uploads") => "CreateMultipartUpload",
            Method::POST if has("uploadId") => "CompleteMultipartUpload",
            Method::PUT if has("uploadId") => "UploadPart",
            Method::DELETE if has("uploadId") => "AbortMultipartUpload",
            Method::PUT => "PutObject",
            Method::GET => "GetObject",
            Method::HEAD => "HeadObject",
            Method::DELETE => "DeleteObject",
            _ => "Other",
        },
    }
}

fn content_length(h: &hyper::HeaderMap) -> u64 {
    h.get(CONTENT_LENGTH).and_then(|v| v.to_str().ok()).and_then(|v| v.parse().ok()).unwrap_or(0)
}

pub async fn handle_s3_request(
    state: Arc<S3ServiceState>,
    req: Request<hyper::body::Incoming>,
) -> Result<Response<ResponseBody>, hyper::Error> {
    if req.uri().path().starts_with(ADMIN_PREFIX) {
        return Ok(admin_response(&state, req.method(), req.uri().path()));
    }
    let started = std::time::Instant::now();
    let host = req.headers().get(HOST).and_then(|v| v.to_str().ok());
    let mut op = operation_name(req.method(), &parse_s3_target(req.uri().path(), host), req.uri().query().unwrap_or(""));
    if op == "PutObject" && req.headers().contains_key("x-amz-copy-source") {
        op = "CopyObject";
    }
    let bytes_in = content_length(req.headers());
    let is_head = req.method() == Method::HEAD;
    let method = req.method().clone();
    let (bucket, key) = match parse_s3_target(req.uri().path(), host) {
        ParsedTarget::Service => (String::new(), String::new()),
        ParsedTarget::Bucket { bucket } => (bucket, String::new()),
        ParsedTarget::Object { bucket, key } => (bucket, key),
    };
    let request_id = crate::request_log::new_request_id();
    // One span per request (#43); exported only when an OTLP endpoint is configured.
    let traceparent = req.headers().get("traceparent").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    let span = tracing::info_span!(
        "s3.request",
        request_id = %request_id,
        method = %req.method(),
        operation = op,
        traceparent = %traceparent,
        status = tracing::field::Empty
    );
    let audit = state.audit_log.read().clone().map(|sink| {
        let h = req.headers();
        let get = |n: &str| h.get(n).and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
        let query = req.uri().query().unwrap_or("");
        let presigned = query.contains("X-Amz-Signature=");
        let signed = h.contains_key("authorization") || presigned;
        let remote = req.extensions().get::<RemoteAddr>().map(|r| r.0.ip().to_string()).unwrap_or_default();
        let uri = format!("{} {} {:?}", req.method(), req.uri(), req.version());
        (sink, crate::request_log::claimed_access_key(h.get("authorization").and_then(|v| v.to_str().ok()), query), remote, uri, get("user-agent"), get("host"), signed, presigned)
    });
    let (resp, error_code) = crate::request_log::with_request_id(request_id.clone(), handle_s3_inner(Arc::clone(&state), req)).instrument(span.clone()).await;
    let mut resp = resp?;
    if let Ok(v) = hyper::header::HeaderValue::from_str(&request_id) {
        resp.headers_mut().insert("x-amz-request-id", v);
    }
    let bytes_out = if is_head { 0 } else { content_length(resp.headers()) };
    span.record("status", resp.status().as_u16());
    let elapsed = started.elapsed();
    state.metrics.record(op, resp.status().as_u16(), elapsed.as_secs_f64(), bytes_in, bytes_out);
    let sink = state.access_log.read().clone();
    if let Some(sink) = sink {
        let rec = crate::request_log::RequestRecord {
            request_id: &request_id,
            method: method.as_str(),
            operation: op,
            bucket: &bucket,
            key: &key,
            status: resp.status().as_u16(),
            bytes_in,
            bytes_out,
            duration_us: elapsed.as_micros() as u64,
        };
        sink(&rec.to_json());
    }
    if let Some((sink, requester, remote, uri, ua, host_hdr, signed, presigned)) = audit {
        let op_name = format!(
            "REST.{}.{}",
            method.as_str(),
            match op {
                "PutObject" | "GetObject" | "HeadObject" | "DeleteObject" | "CopyObject" => "OBJECT",
                "UploadPart" => "PART",
                "CreateMultipartUpload" | "CompleteMultipartUpload" | "AbortMultipartUpload" => "UPLOAD",
                "ListObjects" => "BUCKET",
                "ListBuckets" => "SERVICE",
                "DeleteObjects" => "MULTI_OBJECT_DELETE",
                "PutBucketLifecycle" | "GetBucketLifecycle" => "LIFECYCLE",
                _ => "BUCKET",
            }
        );
        let line = crate::request_log::s3_access_log_line(
            &bucket,
            &remote,
            &requester,
            &request_id,
            &op_name,
            &key,
            &uri,
            resp.status().as_u16(),
            error_code.as_deref(),
            bytes_out,
            elapsed.as_millis() as u64,
            &ua,
            &host_hdr,
            if signed { "SigV4" } else { "" },
            if presigned { "QueryString" } else if signed { "AuthHeader" } else { "" },
        );
        sink(&line);
    }
    Ok(resp)
}

async fn handle_s3_inner(
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
    let iam = state.iam.read().as_ref().map(|r| r.current());
    let principal = match crate::auth::authenticate_with(
        &state.auth,
        iam.as_deref(),
        &state.sigv4_cache,
        req.method().as_str(),
        req.uri().path(),
        req.uri().query().unwrap_or(""),
        req.headers(),
        now_sec,
    ) {
        Ok(p) => p,
        Err(code) => {
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
    };
    // Policies (#53): a config user may only do what its policy allows; root and anonymous
    // (when allowed) are unrestricted.
    let user_may = |action: &str, resource: &str| match &principal {
        crate::iam::Principal::User(ak) => iam.as_ref().is_some_and(|i| i.allows(ak, action, resource)),
        _ => true,
    };
    {
        let (b, k) = match parse_s3_target(path, host_hdr) {
            ParsedTarget::Service => (None, None),
            ParsedTarget::Bucket { bucket } => (Some(bucket), None),
            ParsedTarget::Object { bucket, key } => (Some(bucket), Some(key)),
        };
        let copy_src = req.headers().get("x-amz-copy-source").and_then(|v| v.to_str().ok());
        for (action, resource) in crate::iam::requirements(method.as_str(), b.as_deref(), k.as_deref(), query, copy_src) {
            if !user_may(action, &resource) {
                let xml = s3_error_xml("AccessDenied", &format!("Access Denied: {} on {}", action, resource), path, "req-iam");
                return Ok(Response::builder()
                    .status(StatusCode::FORBIDDEN)
                    .header(CONTENT_TYPE, "application/xml")
                    .body(full_body(Bytes::from(xml)))
                    .unwrap());
            }
        }
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
                Method::PUT if query_map.contains_key("versioning") => {
                    let body_bytes = match read_body(req.into_body(), MAX_XML_BODY, path).await? {
                        Ok(b) => b,
                        Err(resp) => return Ok(resp),
                    };
                    let body = String::from_utf8_lossy(&body_bytes);
                    let status = body.find("<Status>").and_then(|s| body[s + 8..].split('<').next()).and_then(|t| crate::versions::Versioning::parse(t.trim()));
                    let Some(status) = status else {
                        return Ok(bad_request("MalformedXML", "The versioning configuration needs <Status>Enabled</Status> or <Status>Suspended</Status>.", &bucket));
                    };
                    let b_id = match state.engine.read().lookup_bucket(&bucket) {
                        Ok(id) => id,
                        Err(_) => {
                            let xml = s3_error_xml("NoSuchBucket", "The specified bucket does not exist", &bucket, "req-versioning");
                            return Ok(Response::builder().status(StatusCode::NOT_FOUND).header(CONTENT_TYPE, "application/xml").body(full_body(Bytes::from(xml))).unwrap());
                        }
                    };
                    if let Err(e) = state.set_versioning(b_id, status) {
                        return Ok(internal_error_response(&bucket, &format!("could not record the versioning status: {}", e)));
                    }
                    Ok(Response::builder().status(StatusCode::OK).header(CONTENT_LENGTH, "0").body(empty_body()).unwrap())
                }

                Method::GET if query_map.contains_key("versioning") => {
                    let b_id = match state.engine.read().lookup_bucket(&bucket) {
                        Ok(id) => id,
                        Err(_) => {
                            let xml = s3_error_xml("NoSuchBucket", "The specified bucket does not exist", &bucket, "req-versioning");
                            return Ok(Response::builder().status(StatusCode::NOT_FOUND).header(CONTENT_TYPE, "application/xml").body(full_body(Bytes::from(xml))).unwrap());
                        }
                    };
                    let status = state.versioning.read().get(&b_id).copied().unwrap_or_default();
                    let inner = if status == crate::versions::Versioning::Unset { String::new() } else { format!("<Status>{}</Status>", status.as_str()) };
                    let xml = format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{}</VersioningConfiguration>", inner);
                    Ok(Response::builder().status(StatusCode::OK).header(CONTENT_TYPE, "application/xml").body(full_body(Bytes::from(xml))).unwrap())
                }

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

                    if !state.versions.read().keys_of(b_id, "").is_empty() {
                        let xml = s3_error_xml("BucketNotEmpty", "The bucket you tried to delete is not empty", &bucket, "req-del-b");
                        return Ok(Response::builder().status(StatusCode::CONFLICT).header(CONTENT_TYPE, "application/xml").body(full_body(Bytes::from(xml))).unwrap());
                    }
                    match engine.delete_bucket(b_id) {
                        Ok(()) => {
                            state.versioning.write().remove(&b_id);
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

                    // ListObjects (V1), ListObjectsV2 and ListObjectVersions: prefix, delimiter,
                    // paging and encoding-type=url, over in-memory and spilled keys alike.
                    let b_id = match state.engine.read().lookup_bucket(&bucket) {
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
                    let q = |k: &str| query_map.get(k).map(|s| s.as_str()).unwrap_or("");
                    let versions = query_map.contains_key("versions");
                    let v2 = q("list-type") == "2";
                    let prefix = q("prefix");
                    let delimiter = q("delimiter");
                    let url_encode = q("encoding-type") == "url";
                    let max_keys = query_map.get("max-keys").and_then(|s| s.parse::<usize>().ok()).unwrap_or(1000).min(1000);
                    // Where to start, exclusive: V2's continuation token (hex of the cursor) wins
                    // over start-after; V1 uses marker; versions use key-marker.
                    let start: Vec<u8> = if versions {
                        q("key-marker").as_bytes().to_vec()
                    } else if v2 {
                        match query_map.get("continuation-token") {
                            Some(t) => match decode_hex_token(t) {
                                Some(c) => c,
                                None => {
                                    let xml = s3_error_xml("InvalidArgument", "The continuation token provided is incorrect", &bucket, "req-list-obj");
                                    return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .header(CONTENT_TYPE, "application/xml")
                                        .body(full_body(Bytes::from(xml)))
                                        .unwrap());
                                }
                            },
                            None => q("start-after").as_bytes().to_vec(),
                        }
                    } else {
                        q("marker").as_bytes().to_vec()
                    };
                    if versions {
                        return Ok(list_versions_response(&state, b_id, &bucket, prefix, q("key-marker"), max_keys, url_encode));
                    }
                    let page = list_page(&state, b_id, prefix.as_bytes(), delimiter.as_bytes(), &start, max_keys);
                    let enc = |s: &str| if url_encode { s3_url_encode(s) } else { s.to_string() };
                    let key_strings: Vec<String> = page.objects.iter().map(|(k, _)| enc(&String::from_utf8_lossy(k))).collect();
                    let etag_strings: Vec<String> = page.objects.iter().map(|(_, m)| md5_to_etag_hex(m.etag_hi, m.etag_lo)).collect();
                    let prefix_strings: Vec<String> = page.prefixes.iter().map(|p| enc(&String::from_utf8_lossy(p))).collect();
                    let contents: Vec<S3ObjectEntry> = page
                        .objects
                        .iter()
                        .enumerate()
                        .map(|(i, (_, m))| S3ObjectEntry { key: &key_strings[i], last_modified: "2026-11-10T00:00:00.000Z", etag: &etag_strings[i], size: m.size })
                        .collect();
                    let cps: Vec<&str> = prefix_strings.iter().map(|s| s.as_str()).collect();
                    let next = page.next.as_ref().map(|c| encode_hex_token(c));
                    let xml = if versions {
                        list_object_versions_xml(&bucket, prefix, q("key-marker"), max_keys as u32, page.truncated, &contents)
                    } else if v2 {
                        list_objects_v2_xml_full(&ListV2 {
                            bucket: &bucket,
                            prefix: &enc(prefix),
                            delimiter: &enc(delimiter),
                            start_after: &enc(q("start-after")),
                            continuation_token: query_map.get("continuation-token").map(|s| s.as_str()),
                            max_keys: max_keys as u32,
                            truncated: page.truncated,
                            next_token: next.as_deref(),
                            encoding_url: url_encode,
                            contents: &contents,
                            prefixes: &cps,
                        })
                    } else {
                        // V1: NextMarker is the last key or prefix returned, when truncated.
                        let next_marker = if page.truncated { page.last_name.as_ref().map(|n| enc(&String::from_utf8_lossy(n))) } else { None };
                        list_objects_v1_xml(&bucket, &enc(prefix), &enc(delimiter), &enc(q("marker")), max_keys as u32, page.truncated, next_marker.as_deref(), url_encode, &contents, &cps)
                    };
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

                    // Each <Object> has a <Key> and maybe a <VersionId> (#31). Keys are XML text:
                    // "&amp;" is "&" (it was used escaped before).
                    let text = |block: &str, tag: &str| -> Option<String> {
                        let open = format!("<{}>", tag);
                        let close = format!("</{}>", tag);
                        let s = block.find(&open)? + open.len();
                        let e = block[s..].find(&close)? + s;
                        let raw = &block[s..e];
                        Some(quick_xml::escape::unescape(raw).map(|k| k.into_owned()).unwrap_or_else(|_| raw.to_string()))
                    };
                    let mut keys_to_delete: Vec<String> = Vec::new();
                    let mut version_ids: Vec<Option<String>> = Vec::new();
                    let mut cursor = body_str.as_ref();
                    while let Some(start) = cursor.find("<Object>") {
                        let rest = &cursor[start + 8..];
                        let end = rest.find("</Object>").unwrap_or(rest.len());
                        let block = &rest[..end];
                        if let Some(k) = text(block, "Key") {
                            keys_to_delete.push(k);
                            version_ids.push(text(block, "VersionId"));
                        }
                        cursor = &rest[end..];
                    }

                    let looked_up = state.engine.read().lookup_bucket(&bucket);
                    let b_id = match looked_up {
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

                    // Each key goes through delete_key: tombstone logged and synced, under its key
                    // lock. Before, the rows were dropped with no tombstone, and every deleted
                    // object came back at the next restart.
                    let mut ok_keys: Vec<(&str, Option<&str>)> = Vec::new();
                    let mut failed: Vec<(String, &'static str, String)> = Vec::new();
                    for (k, vid) in keys_to_delete.iter().zip(version_ids.iter()) {
                        // Each key is checked against the signer's policy (#53).
                        if !user_may("s3:DeleteObject", &format!("arn:aws:s3:::{}/{}", bucket, k)) {
                            failed.push((k.clone(), "AccessDenied", "Access Denied".to_string()));
                            continue;
                        }
                        let result = match vid.as_deref() {
                            Some(v) => match crate::versions::parse_id(v) {
                                Some(id) => state.delete_version(b_id, k, id).await.map(|_| ()),
                                None => Err("NoSuchVersion".to_string()),
                            },
                            None => state.delete_key(b_id, k).await.map(|_| ()),
                        };
                        match result {
                            Ok(()) => ok_keys.push((k.as_str(), vid.as_deref())),
                            Err(e) if e == "NoSuchVersion" => failed.push((k.clone(), "NoSuchVersion", "The specified version does not exist.".to_string())),
                            Err(e) => failed.push((k.clone(), "InternalError", e)),
                        }
                    }
                    let deleted: Vec<DeleteItemResult> = ok_keys.into_iter().map(|(key, version_id)| DeleteItemResult { key, version_id }).collect();
                    let errors: Vec<DeleteErrorItem> = failed.iter().map(|(k, c, e)| DeleteErrorItem { key: k, code: c, message: e }).collect();
                    let xml = delete_objects_result_xml(&deleted, &errors);
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
                let upload_id = match state.multipart.create_upload(&bucket, b_id, &key) {
                    Ok(id) => id,
                    Err(e) => return Ok(internal_error_response(&key, &format!("could not start the upload: {}", e))),
                };
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

                let is_chunked = crate::auth::is_aws_chunked(req.headers());
                let signed_headers = req.headers().clone();
                // The length of what is stored: the decoded length for an aws-chunked body.
                let declared_len = signed_headers
                    .get(if is_chunked { "x-amz-decoded-content-length" } else { "content-length" })
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok());
                if let Some(len) = declared_len.filter(|&n| n > STREAM_PUT_MIN) {
                    // Large part: write it to its staged file as it arrives (#36).
                    if state.multipart.get_upload(upload_id).is_none() {
                        let xml = s3_error_xml("NoSuchUpload", "The specified upload does not exist.", &key, "req-up-part");
                        return Ok(Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .header(CONTENT_TYPE, "application/xml")
                            .body(full_body(Bytes::from(xml)))
                            .unwrap());
                    }
                    let mut writer = match state.multipart.begin_part(upload_id, part_num) {
                        Ok(w) => w,
                        Err(e) => return Ok(internal_error_response(&key, &format!("could not store the part: {}", e))),
                    };
                    let declared_sha = signed_headers
                        .get("x-amz-content-sha256")
                        .and_then(|v| v.to_str().ok())
                        .filter(|h| h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit()))
                        .map(|h| h.to_ascii_lowercase());
                    let mut sha = declared_sha.as_ref().map(|_| sha2::Sha256::new());
                    let mut md5 = Md5::new();
                    let mut body = req.into_body();
                    let mut verifier = crate::auth::ChunkVerifier::for_request_with(&state.auth, iam.as_deref(), &signed_headers);
                    let cache = &state.sigv4_cache;
                    let mut decoder = is_chunked.then(|| crate::aws_chunked::ChunkedDecoder::new(move |sha: &str, sig: Option<&str>| verifier.as_mut().map_or(true, |v| v.check_digest(cache, sha, sig))));
                    while let Some(frame) = body.frame().await {
                        if let Ok(data) = frame?.into_data() {
                            if let Some(h) = sha.as_mut() {
                                h.update(&data);
                            }
                            let mut sink = |d: &[u8]| -> Result<(), String> {
                                if writer.written() + d.len() as u64 > len {
                                    return Err("more bytes than the declared length".to_string());
                                }
                                md5.update(d);
                                writer.write(d).map_err(|e| e.to_string())
                            };
                            let fed = match decoder.as_mut() {
                                Some(dec) => dec.push(&data, &mut sink),
                                None => sink(&data).map_err(crate::aws_chunked::PushError::Sink),
                            };
                            match fed {
                                Ok(()) => {}
                                Err(crate::aws_chunked::PushError::Decode(e)) => {
                                    // Read the rest of the body (up to 64 MiB) before answering, so the client
                                    // gets the error instead of a reset connection (seen on Windows).
                                    drain_body(&mut body, 64 << 20).await;
                                    let status = if e == "SignatureDoesNotMatch" { StatusCode::FORBIDDEN } else { StatusCode::BAD_REQUEST };
                                    return Ok(Response::builder()
                                        .status(status)
                                        .header(CONTENT_TYPE, "application/xml")
                                        .body(full_body(Bytes::from(s3_error_xml(e, "Error decoding chunked body", &key, "req-up-part"))))
                                        .unwrap());
                                }
                                Err(crate::aws_chunked::PushError::Sink(e)) if e.starts_with("more bytes") => {
                                    return Ok(bad_request("IncompleteBody", "The body is longer than its declared length.", &key));
                                }
                                Err(crate::aws_chunked::PushError::Sink(e)) => {
                                    return Ok(internal_error_response(&key, &format!("could not store the part: {}", e)));
                                }
                            }
                        }
                    }
                    if decoder.as_ref().is_some_and(|d| d.finish().is_err()) || writer.written() != len {
                        return Ok(bad_request("IncompleteBody", "You did not provide the number of bytes specified by the Content-Length HTTP header.", &key));
                    }
                    if let (Some(h), Some(want)) = (sha, declared_sha) {
                        let got: String = h.finalize().iter().map(|b| format!("{:02x}", b)).collect();
                        if got != want {
                            return Ok(bad_request("XAmzContentSHA256Mismatch", "The provided 'x-amz-content-sha256' header does not match what was computed.", &key));
                        }
                    }
                    let raw_md5: [u8; 16] = md5.finalize().into();
                    let hi = u64::from_be_bytes(raw_md5[0..8].try_into().unwrap());
                    let lo = u64::from_be_bytes(raw_md5[8..16].try_into().unwrap());
                    let hex: String = raw_md5.iter().map(|b| format!("{:02x}", b)).collect();
                    let staged_path = match writer.finish() {
                        Ok(p) => p,
                        Err(e) => return Ok(internal_error_response(&key, &format!("could not store the part: {}", e))),
                    };
                    return Ok(match state.multipart.upload_part(upload_id, part_num, 0, len, raw_md5, hi, lo, hex.clone(), Some(staged_path)) {
                        Ok(_) => Response::builder()
                            .status(StatusCode::OK)
                            .header(ETAG, format!("\"{}\"", hex))
                            .header(CONTENT_LENGTH, "0")
                            .body(empty_body())
                            .unwrap(),
                        Err(err) => {
                            let status = if err == "NoSuchUpload" { StatusCode::NOT_FOUND } else { StatusCode::BAD_REQUEST };
                            Response::builder()
                                .status(status)
                                .header(CONTENT_TYPE, "application/xml")
                                .body(full_body(Bytes::from(s3_error_xml(err, "Failed to upload part", &key, "req-up-part"))))
                                .unwrap()
                        }
                    });
                }
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
                    let mut verifier = crate::auth::ChunkVerifier::for_request_with(&state.auth, iam.as_deref(), &signed_headers);
                    let cache = &state.sigv4_cache;
                    match crate::aws_chunked::decode_aws_chunked_checked(&raw_bytes, |sha, sig| verifier.as_mut().map_or(true, |v| v.check_digest(cache, sha, sig))) {
                        Ok(b) => b,
                        Err(e) => {
                            let xml = s3_error_xml(e, "Error decoding chunked body", &key, "req-up-part");
                            return Ok(Response::builder()
                                .status(if e == "SignatureDoesNotMatch" { StatusCode::FORBIDDEN } else { StatusCode::BAD_REQUEST })
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
                        let stamp = state.version_stamp(b_id, &key);
                        let mp_flags = AWAN_FLAG_ACTIVE | AWAN_FLAG_MULTIPART | stamp.map_or(0, |s| s.0);
                        let manifest_offset = match state.store_payload_at(
                            b_id,
                            &key,
                            &manifest,
                            mp_flags,
                            hi,
                            lo,
                            stamp.map(|s| s.1),
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
                        let mut version_id = None;
                        if let Some((_, ts)) = stamp {
                            match finish_version_put(&state, b_id, &key, ts, mp_flags, manifest_offset, total_size, hi, lo, extents.len() as u16) {
                                Ok(v) => version_id = Some(v),
                                Err(r) => return Ok(r),
                            }
                        } else {
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
                        }

                        let location = format!("http://127.0.0.1:9000/{}/{}", bucket, key);
                        let xml = complete_multipart_upload_result_xml(&bucket, &key, &composite_etag, &location);
                        let mut resp = Response::builder()
                            .status(StatusCode::OK)
                            .header(CONTENT_TYPE, "application/xml")
                            .header(ETAG, format!("\"{}\"", composite_etag));
                        if let Some(v) = version_id {
                            resp = resp.header("x-amz-version-id", v);
                        }
                        return Ok(resp.body(full_body(Bytes::from(xml))).unwrap());
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
                        // x-amz-copy-source is URL-encoded and may end in ?versionId=, which names the
                        // version to copy. It used to be cut off here and the current version copied
                        // with a 200 (#139).
                        let copy_source_raw = copy_source.to_str().unwrap_or("");
                        let (copy_path, copy_query) = copy_source_raw.split_once('?').unwrap_or((copy_source_raw, ""));
                        let src_vid: Option<String> = copy_query
                            .split('&')
                            .find_map(|kv| kv.strip_prefix("versionId="))
                            .map(percent_decode_path);
                        let copy_src_owned = percent_decode_path(copy_path);
                        let copy_src_str = copy_src_owned.trim_start_matches('/');
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
                        // A named version need not be the current one: the key may even be deleted.
                        let src_meta = if src_vid.is_some() {
                            None
                        } else {
                            match engine.get_object(src_b_id, src_key.as_bytes()) {
                                Ok(m) => Some(m),
                                Err(_) => {
                                    let xml = s3_error_xml("NoSuchKey", "Source object does not exist", src_key, "req-cp");
                                    return Ok(Response::builder()
                                        .status(StatusCode::NOT_FOUND)
                                        .header(CONTENT_TYPE, "application/xml")
                                        .body(full_body(Bytes::from(xml)))
                                        .unwrap());
                                }
                            }
                        };
                        (src_b_id, src_meta)
                        };

                        // The copy gets a needle of its own: it must survive a restart and an
                        // overwrite or delete of its source.
                        let read = match (&src_vid, &src_meta) {
                            (Some(vid), _) => {
                                // The same lookup as a versioned GET, so the two cannot disagree.
                                let Some(id) = crate::versions::parse_id(vid) else {
                                    return Ok(no_such_version(src_key));
                                };
                                let Some(v) = find_version(&state, src_b_id, src_key, Some(id)) else {
                                    return Ok(no_such_version(src_key));
                                };
                                if v.marker {
                                    let xml = s3_error_xml(
                                        "InvalidRequest",
                                        "The source of a copy request may not specifically refer to a delete marker by version id.",
                                        src_key,
                                        "req-cp",
                                    );
                                    return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .header(CONTENT_TYPE, "application/xml")
                                        .body(full_body(Bytes::from(xml)))
                                        .unwrap());
                                }
                                state.read_version_bytes(src_b_id, src_key, &v).map(|b| (b, v.size, v.etag_hi, v.etag_lo, Some(v.id_string())))
                            }
                            (None, Some(m)) => state
                                .read_object_bytes(src_b_id, src_key, m.vol_offset, m.size, unpack_flags(m.bucket_flags))
                                .map(|b| (b, m.size, m.etag_hi, m.etag_lo, None)),
                            (None, None) => unreachable!("src_meta is read whenever no version is named"),
                        };
                        let (bytes, src_size, src_etag_hi, src_etag_lo, copied_vid) = match read {
                            Ok(r) => r,
                            Err(e) => return Ok(internal_error_response(&key, &format!("could not read the copy source: {:?}", e))),
                        };
                        let flags = if bytes.is_empty() { AWAN_FLAG_ACTIVE | AWAN_FLAG_ZERO_BYTE } else { AWAN_FLAG_ACTIVE };
                        let _key_guard = state.lock_key(b_id, &key).await;
                        if let Err(code) = state.engine.read().has_room_for(b_id, key.as_bytes()) {
                            return Ok(engine_error_response(code, &key));
                        }
                        let old_info = state.engine.read().get_object(b_id, key.as_bytes()).ok().map(|m| m.vol_offset);
                        let stamp = state.version_stamp(b_id, &key);
                        let flags = flags | stamp.map_or(0, |s| s.0);
                        let (vol_offset, flags) = match state.store_object(b_id, &bucket, &key, &bytes, flags, src_etag_hi, src_etag_lo, stamp.map(|s| s.1)).await {
                            Ok(written) => written,
                            Err(e) => return Ok(internal_error_response(&key, &format!("could not store the copy: {:?}", e))),
                        };
                        if let Some((_, ts)) = stamp {
                            let vid = match finish_version_put(&state, b_id, &key, ts, flags, vol_offset, src_size, src_etag_hi, src_etag_lo, 0) {
                                Ok(v) => v,
                                Err(r) => return Ok(r),
                            };
                            let etag_hex = md5_to_etag_hex(src_etag_hi, src_etag_lo);
                            let resp = Response::builder()
                                .status(StatusCode::OK)
                                .header(CONTENT_TYPE, "application/xml")
                                .header(ETAG, format!("\"{}\"", etag_hex))
                                .header("x-amz-version-id", vid);
                            let resp = match &copied_vid {
                                Some(v) => resp.header("x-amz-copy-source-version-id", v.as_str()),
                                None => resp,
                            };
                            return Ok(resp
                                .body(full_body(Bytes::from(copy_object_result_xml(&etag_hex, "2026-11-10T00:00:00.000Z"))))
                                .unwrap());
                        }
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
                            src_size,
                            src_etag_hi,
                            src_etag_lo,
                            0,
                            flags,
                        ) {
                            return Ok(engine_error_response(code, &key));
                        }

                        let etag_hex = md5_to_etag_hex(src_etag_hi, src_etag_lo);
                        let xml = copy_object_result_xml(&etag_hex, "2026-11-10T00:00:00.000Z");
                        let resp = Response::builder()
                            .status(StatusCode::OK)
                            .header(CONTENT_TYPE, "application/xml")
                            .header(ETAG, format!("\"{}\"", etag_hex));
                        let resp = match &copied_vid {
                            Some(v) => resp.header("x-amz-copy-source-version-id", v.as_str()),
                            None => resp,
                        };
                        return Ok(resp.body(full_body(Bytes::from(xml))).unwrap());
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
                    let is_chunked = crate::auth::is_aws_chunked(req.headers());
                    let content_md5_hdr = req.headers().get("content-md5").and_then(|v| v.to_str().ok()).map(|s| s.trim().to_string());
                    let signed_headers = req.headers().clone();
                    let stream_len = if is_chunked {
                        signed_headers.get("x-amz-decoded-content-length").and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok())
                    } else {
                        expected_len.map(|n| n as u64)
                    };
                    if let Some(len) = stream_len.filter(|&n| n > STREAM_PUT_MIN) {
                        // Large body: write it to the volume as it arrives (#36). The key lock is
                        // held from the reservation to the index update, so log order stays
                        // index order for this key.
                        let flags = if key.ends_with('/') { AWAN_FLAG_ACTIVE | AWAN_FLAG_ZERO_BYTE } else { AWAN_FLAG_ACTIVE };
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
                        let old_info = state.engine.read().get_object(b_id, key.as_bytes()).ok().map(|m| (m.vol_offset, m.size));
                        let stamp = state.version_stamp(b_id, &key);
                        let flags = flags | stamp.map_or(0, |s| s.0);
                        let mut writer = match state.begin_needle_on_active(b_id, &key, len, flags) {
                            Ok(w) => w,
                            Err(e) => return Ok(internal_error_response(&key, &format!("could not store the object: {:?}", e))),
                        };
                        if let Some((_, ts)) = stamp {
                            writer.set_created_at(ts);
                        }
                        let declared_sha = signed_headers
                            .get("x-amz-content-sha256")
                            .and_then(|v| v.to_str().ok())
                            .filter(|h| h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit()))
                            .map(|h| h.to_ascii_lowercase());
                        let mut sha = declared_sha.as_ref().map(|_| sha2::Sha256::new());
                        let mut digest = crate::digest::StreamingDigest::new();
                        let mut body = req.into_body();
                        let mut verifier = crate::auth::ChunkVerifier::for_request_with(&state.auth, iam.as_deref(), &signed_headers);
                        let cache = &state.sigv4_cache;
                        let mut decoder = is_chunked.then(|| crate::aws_chunked::ChunkedDecoder::new(move |sha: &str, sig: Option<&str>| verifier.as_mut().map_or(true, |v| v.check_digest(cache, sha, sig))));
                        while let Some(frame) = body.frame().await {
                            if let Ok(data) = frame?.into_data() {
                                if let Some(h) = sha.as_mut() {
                                    h.update(&data);
                                }
                                let mut sink = |d: &[u8]| -> Result<(), String> {
                                    if writer.written() + d.len() as u64 > len {
                                        return Err("more bytes than the declared length".to_string());
                                    }
                                    digest.update(d);
                                    writer.write(d).map_err(|e| format!("{:?}", e))
                                };
                                let fed = match decoder.as_mut() {
                                    Some(dec) => dec.push(&data, &mut sink),
                                    None => sink(&data).map_err(crate::aws_chunked::PushError::Sink),
                                };
                                match fed {
                                    Ok(()) => {}
                                    Err(crate::aws_chunked::PushError::Decode(e)) => {
                                        // Read the rest of the body (up to 64 MiB) before answering, so the client
                                        // gets the error instead of a reset connection (seen on Windows).
                                        drain_body(&mut body, 64 << 20).await;
                                        let status = if e == "SignatureDoesNotMatch" { StatusCode::FORBIDDEN } else { StatusCode::BAD_REQUEST };
                                        return Ok(Response::builder()
                                            .status(status)
                                            .header(CONTENT_TYPE, "application/xml")
                                            .body(full_body(Bytes::from(s3_error_xml(e, "Error decoding chunked body", &key, "req-put-obj"))))
                                            .unwrap());
                                    }
                                    Err(crate::aws_chunked::PushError::Sink(e)) if e.starts_with("more bytes") => {
                                        return Ok(bad_request("IncompleteBody", "The body is longer than its declared length.", &key));
                                    }
                                    Err(crate::aws_chunked::PushError::Sink(e)) => {
                                        return Ok(internal_error_response(&key, &format!("could not store the object: {}", e)));
                                    }
                                }
                            }
                        }
                        if decoder.as_ref().is_some_and(|d| d.finish().is_err()) || writer.written() != len {
                            return Ok(bad_request("IncompleteBody", "You did not provide the number of bytes specified by the Content-Length HTTP header.", &key));
                        }
                        if let (Some(h), Some(want)) = (sha, declared_sha) {
                            let got: String = h.finalize().iter().map(|b| format!("{:02x}", b)).collect();
                            if got != want {
                                return Ok(bad_request("XAmzContentSHA256Mismatch", "The provided 'x-amz-content-sha256' header does not match what was computed.", &key));
                            }
                        }
                        let (etag_hi, etag_lo, etag_hex, raw_md5, _) = digest.finalize();
                        if let Some(ref content_md5) = content_md5_hdr {
                            if content_md5 != &crate::digest::base64_encode(&raw_md5) {
                                return Ok(bad_request("BadDigest", "The Content-MD5 you specified did not match what we received.", &key));
                            }
                        }
                        let vm = Arc::clone(writer.volume());
                        let loc = match writer.finish(etag_hi, etag_lo) {
                            Ok(l) => l,
                            Err(e) => return Ok(internal_error_response(&key, &format!("could not store the object: {:?}", e))),
                        };
                        if let Err(e) = state.make_durable(&vm, loc.seq).await {
                            return Ok(internal_error_response(&key, &format!("could not sync the object: {:?}", e)));
                        }
                        let vol_offset = global_offset(vm.id(), loc.offset);
                        state.needle_crc_map.write().insert((b_id, vol_offset), loc.payload_crc32c);
                        state.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
                        state.hook("put_stored", &key);
                        if let Some((_, ts)) = stamp {
                            return Ok(match finish_version_put(&state, b_id, &key, ts, flags, vol_offset, len, etag_hi, etag_lo, 0) {
                                Ok(vid) => put_ok_response(&etag_hex, Some(&vid)),
                                Err(r) => r,
                            });
                        }
                        return Ok(finish_put(&state, b_id, &key, old_info, vol_offset, len, etag_hi, etag_lo, flags, &etag_hex));
                    }
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
                        let mut verifier = crate::auth::ChunkVerifier::for_request_with(&state.auth, iam.as_deref(), &signed_headers);
                        let cache = &state.sigv4_cache;
                        match crate::aws_chunked::decode_aws_chunked_checked(&raw_bytes, |sha, sig| verifier.as_mut().map_or(true, |v| v.check_digest(cache, sha, sig))) {
                            Ok(b) => b,
                            Err(e) => {
                                let xml = s3_error_xml(e, "Error decoding chunked body", &key, "req-put-obj");
                                return Ok(Response::builder()
                                    .status(if e == "SignatureDoesNotMatch" { StatusCode::FORBIDDEN } else { StatusCode::BAD_REQUEST })
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
                    let stamp = state.version_stamp(b_id, &key);
                    let flags = flags | stamp.map_or(0, |s| s.0);
                    let (vol_offset, flags) = match state.store_object(b_id, &bucket, &key, &decoded_bytes, flags, etag_hi, etag_lo, stamp.map(|s| s.1)).await {
                        Ok(written) => written,
                        Err(e) => return Ok(internal_error_response(&key, &format!("could not store the object: {:?}", e))),
                    };
                    state.hook("put_stored", &key);
                    if let Some((_, ts)) = stamp {
                        return Ok(match finish_version_put(&state, b_id, &key, ts, flags, vol_offset, payload_len, etag_hi, etag_lo, 0) {
                            Ok(vid) => put_ok_response(&final_etag_hex, Some(&vid)),
                            Err(r) => r,
                        });
                    }
                    Ok(finish_put(&state, b_id, &key, old_info, vol_offset, payload_len, etag_hi, etag_lo, flags, &final_etag_hex))
                }

                Method::GET | Method::HEAD => {
                    // Versions (#31): `?versionId=`, or a key with a history (its current
                    // version may be a delete marker, and the answer carries the version id).
                    let vid = query_map.get("versionId").map(|s| s.as_str());
                    if vid.is_some() || state.versions.read().has(b_id, &key) {
                        let range = req.headers().get("Range").and_then(|v| v.to_str().ok());
                        return Ok(version_get_response(&state, b_id, &key, vid, method == Method::HEAD, range));
                    }
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
                    let is_compressed = unpack_flags(meta.bucket_flags) & crate::meta_index::FLAG_COMPRESSED != 0;

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
                                } else if is_compressed {
                                    match state.load_compressed_range(b_id, meta.vol_offset, meta.size, range.start, range.end) {
                                        Ok(b) => b,
                                        Err(e) => return Ok(internal_error_response(&key, &format!("could not read the object: {:?}", e))),
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
                                if is_compressed {
                                    state_for_read.load_compressed(b_id, meta.vol_offset, meta.size).unwrap_or_default()
                                } else {
                                    state_for_read.load_payload(b_id, meta.vol_offset, meta.size as usize).unwrap_or_default()
                                }
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
                    if let Some(vid) = query_map.get("versionId") {
                        let Some(id) = crate::versions::parse_id(vid) else {
                            return Ok(no_such_version(&key));
                        };
                        return Ok(match state.delete_version(b_id, &key, id).await {
                            Ok(removed) => {
                                let mut b = Response::builder().status(StatusCode::NO_CONTENT).header(CONTENT_LENGTH, "0").header("x-amz-version-id", vid.as_str());
                                if removed.is_some_and(|v| v.marker) {
                                    b = b.header("x-amz-delete-marker", "true");
                                }
                                b.body(empty_body()).unwrap()
                            }
                            Err(e) => internal_error_response(&key, &e),
                        });
                    }
                    let marker = match state.delete_key(b_id, &key).await {
                        Ok(m) => m,
                        Err(e) => return Ok(internal_error_response(&key, &e)),
                    };
                    let mut b = Response::builder().status(StatusCode::NO_CONTENT).header(CONTENT_LENGTH, "0");
                    if let Some(m) = marker {
                        b = b.header("x-amz-delete-marker", "true").header("x-amz-version-id", m);
                    }
                    Ok(b.body(empty_body()).unwrap())
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
