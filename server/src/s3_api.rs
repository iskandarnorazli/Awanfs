//! awan-storage/server/src/s3_api.rs
//! S3 REST Wire Protocol Dispatcher, Routing & RFC 9110 Range Engine
//! Reference: awan-storage/API_SPECIFICATION.md and awan-storage/HYBRID_POC_SPECIFICATION.md

use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE, HOST};
use hyper::{Method, Request, Response, StatusCode};
use md5::{Digest, Md5};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use crate::clock::{Clock, SystemClock};
use crate::scheduler::Scheduler;

use crate::cache::PayloadCache;
use crate::coalesce::SingleFlight;
use crate::compaction::CompactionReport;
use crate::ffi_bridge::*;
use crate::lifecycle::*;
use crate::multipart::*;
use crate::mvcc::MvccStore;
use crate::ops::object::null_version_of;
use crate::sigv4::*;
use crate::volume::{global_offset, split_offset, Durability, NeedleLocation, VolumeError, VolumeManager};
use crate::work::WorkCounters;
use crate::xml::*;
use tracing::Instrument;
use std::path::{Path, PathBuf};

/// A response body. It can fail: a large object is checksummed while it is sent, and a
/// mismatch at the end fails the body, which makes hyper drop the connection so the client
/// does not receive a complete response (#155, decision 46). Every other body never fails.
pub type ResponseBody = BoxBody<Bytes, std::io::Error>;

fn never_fails(never: std::convert::Infallible) -> std::io::Error {
    match never {}
}

pub(crate) fn full_body(bytes: Bytes) -> ResponseBody {
    Full::new(bytes).map_err(never_fails).boxed()
}

/// 500 InternalError: the server could not read or write the object's bytes.
pub(crate) fn internal_error_response(resource: &str, detail: &str) -> Response<ResponseBody> {
    let xml = s3_error_xml("InternalError", detail, resource, "req-internal");
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap()
}

/// Single S3 `503 SlowDown` response builder so every path carries `Retry-After`.
pub(crate) fn slow_down_with_req_id(resource: &str, msg: &str, req_id: &str) -> Response<ResponseBody> {
    let xml = s3_error_xml("SlowDown", msg, resource, req_id);
    Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .header(CONTENT_TYPE, "application/xml")
        .header("retry-after", crate::memory::RETRY_AFTER_SECS.to_string())
        .header("connection", "close")
        .body(full_body(Bytes::from(xml)))
        .unwrap()
}

/// The answer for an index error from `put_object`, `has_room_for`, or `CapacityReservation`.
pub(crate) fn engine_error_response(code: i32, resource: &str) -> Response<ResponseBody> {
    let (status, s3_code, msg) = match code {
        ERR_CAPACITY => return slow_down_with_req_id(resource, "The object index is full.", "req-index"),
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
pub(crate) const CHUNK_FRAMING_SLACK: u64 = 16 << 20;

pub(crate) fn body_limit_for(method: &Method, target: &ParsedTarget) -> u64 {
    match (method, target) {
        // PutObject and UploadPart; CompleteMultipartUpload and the other POSTs carry XML.
        (&Method::PUT, ParsedTarget::Object { .. }) => MAX_OBJECT_BODY,
        _ => MAX_XML_BODY,
    }
}

pub(crate) fn entity_too_large(resource: &str, limit: u64) -> Response<ResponseBody> {
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

/// Reads a request body of at most `limit` bytes, timing out if no frame arrives within
/// `idle_timeout`. `Ok(Err(response))` is the answer to send when the body is too large,
/// times out, or is cut short. The buffer starts at `capacity` bytes; a body that outgrows it
/// is counted in `work` (#454).
pub(crate) async fn read_body_sized<B>(
    mut body: B,
    limit: u64,
    idle_timeout: std::time::Duration,
    resource: &str,
    capacity: usize,
    work: Option<&WorkCounters>,
) -> Result<Result<Bytes, Response<ResponseBody>>, hyper::Error>
where
    B: hyper::body::Body<Data = Bytes, Error = hyper::Error> + Unpin,
{
    if body.size_hint().lower() > limit {
        return Ok(Err(entity_too_large(resource, limit)));
    }
    let mut buf = bytes::BytesMut::with_capacity(capacity);
    loop {
        match crate::body_timeout::next_frame_with_idle(&mut body, idle_timeout).await {
            Err(crate::body_timeout::BodyIdleTimeout) => {
                return Ok(Err(request_timeout_response(resource)));
            }
            Ok(None) => return Ok(Ok(buf.freeze())),
            Ok(Some(Err(e))) => return Err(e),
            Ok(Some(Ok(frame))) => {
                if let Ok(data) = frame.into_data() {
                    if (buf.len() as u64).saturating_add(data.len() as u64) > limit {
                        return Ok(Err(entity_too_large(resource, limit)));
                    }
                    if buf.is_empty() && body.is_end_stream() {
                        return Ok(Ok(data));
                    }
                    let cap = buf.capacity();
                    buf.extend_from_slice(&data);
                    if let Some(work) = work {
                        if cap != 0 && buf.capacity() != cap {
                            work.body_regrowth();
                        }
                    }
                }
            }
        }
    }
}

pub(crate) fn request_timeout_response(resource: &str) -> Response<ResponseBody> {
    let xml = s3_error_xml(
        "RequestTimeout",
        "Your socket connection to the server was not read from or written to within the timeout period. Idle connections will be closed.",
        resource,
        "req-timeout",
    );
    Response::builder()
        .status(StatusCode::REQUEST_TIMEOUT)
        .header(CONTENT_TYPE, "application/xml")
        .header("connection", "close")
        .body(full_body(Bytes::from(xml)))
        .unwrap()
}

pub(crate) fn slow_down_response(resource: &str, msg: &str) -> Response<ResponseBody> {
    slow_down_with_req_id(resource, msg, "req-mem")
}

/// Acquires against `budget` before buffering `body`, returning the `AdmissionGuard` and
/// `InflightGuard` alongside the collected bytes so the caller holds both until consumed or dropped.
pub(crate) async fn read_body_admitted<B>(
    budget: &crate::memory::BodyBudget,
    work: &Arc<WorkCounters>,
    body: B,
    limit: u64,
    estimated_bytes: Option<u64>,
    idle_timeout: std::time::Duration,
    resource: &str,
) -> Result<Result<(Bytes, (crate::memory::AdmissionGuard, crate::work::InflightGuard)), Response<ResponseBody>>, hyper::Error>
where
    B: hyper::body::Body<Data = Bytes, Error = hyper::Error> + Unpin,
{
    let hint = estimated_bytes
        .or_else(|| body.size_hint().exact())
        .unwrap_or(limit)
        .min(limit);
    let guard = match budget.acquire_async(hint).await {
        Ok(g) => g,
        Err(_) => {
            return Ok(Err(slow_down_response(
                resource,
                "Please reduce your request rate; in-flight body memory budget exhausted.",
            )));
        }
    };
    let inflight = work.hold_inflight(guard.bytes());
    // The buffer is the size the budget admitted when the length was declared, so a body
    // arriving in many frames is not moved to a larger buffer as it grows (#454).
    let capacity = estimated_bytes.map_or(0, |n| n.min(guard.bytes()) as usize);
    match read_body_sized(body, limit, idle_timeout, resource, capacity, Some(work.as_ref())).await? {
        Ok(bytes) => {
            let actual = if (bytes.len() as u64) != guard.bytes() {
                drop(inflight);
                work.hold_inflight(bytes.len() as u64)
            } else {
                inflight
            };
            Ok(Ok((bytes, (guard, actual))))
        }
        Err(resp) => Ok(Err(resp)),
    }
}

/// A response body sent as a list of chunks, one frame each: the parts of a multipart object
/// go out from their mmaps without being copied into one buffer (#36).
pub(crate) struct ChunksBody(pub(crate) std::collections::VecDeque<Bytes>);

impl hyper::body::Body for ChunksBody {
    type Data = Bytes;
    type Error = std::io::Error;

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

/// Objects above this size are checksummed while they are sent, not before, and a range of
/// one is not checksummed (#155, decision 46). It is the largest payload the cache holds.
pub const VERIFY_WHILE_SENDING_MIN: u64 = 4 * 1024 * 1024;

/// PUT bodies above this size are written to the volume as they arrive instead of being held
/// in memory (#36). Smaller ones are collected, and cached after the write.
pub const STREAM_PUT_MIN: u64 = 4 * 1024 * 1024;

/// Reads and drops what is left of a request body, at most `limit` bytes.
pub(crate) async fn drain_body<B>(body: &mut B, limit: u64)
where
    B: hyper::body::Body<Data = Bytes> + Unpin,
{
    let mut seen = 0u64;
    while seen < limit {
        match crate::body_timeout::next_frame_with_idle(body, crate::body_timeout::DEFAULT_BODY_IDLE_TIMEOUT).await {
            Ok(Some(Ok(f))) => seen += f.data_ref().map_or(0, |d| d.len() as u64),
            _ => break,
        }
    }
}

/// The end of a PutObject once the needle is durable: index row, bookkeeping, answer.
#[allow(clippy::too_many_arguments)]
/// Streamed PUTs in flight, by key (#154).
///
/// A streamed PUT reserves its needle before its body arrives and takes the key lock only to
/// commit. Recovery replays an unversioned key in offset order, so if any needle of that key
/// was appended while the body arrived, the streamed needle is no longer the last one in the
/// log and has to be written again at the end (`verification/tla/ObjectVersions.tla`,
/// `Relocate`). This records, for keys with a PUT in flight, the highest offset appended.
///
/// Order matters and is what makes the check sound: a PUT enters before it reserves its
/// offset, and every append notes itself after it reserved its own. So an append at a later
/// offset than the PUT's always finds the PUT's entry. Keys are tracked by a 64-bit hash: a
/// collision can only cause a copy that was not needed.
#[derive(Default)]
pub struct StreamsInFlight {
    count: std::sync::atomic::AtomicUsize,
    keys: parking_lot::Mutex<HashMap<(u32, u64), (u32, u64)>>,
}

impl StreamsInFlight {
    fn slot(b_id: u32, key: &str) -> (u32, u64) {
        (b_id, xxhash_rust::xxh3::xxh3_64(key.as_bytes()))
    }

    /// Starts tracking `key`. Call before reserving the needle; dropping the ticket stops.
    pub fn enter(self: &Arc<Self>, b_id: u32, key: &str) -> StreamTicket {
        let slot = Self::slot(b_id, key);
        self.count.fetch_add(1, Ordering::SeqCst);
        self.keys.lock().entry(slot).or_insert((0, 0)).0 += 1;
        StreamTicket { streams: Arc::clone(self), slot }
    }

    /// A needle of `key` was appended at global offset `g`. Call after the reservation.
    pub fn note_append(&self, b_id: u32, key: &str, g: u64) {
        if self.count.load(Ordering::SeqCst) == 0 {
            return;
        }
        if let Some(e) = self.keys.lock().get_mut(&Self::slot(b_id, key)) {
            e.1 = e.1.max(g);
        }
    }

    /// How many streamed PUTs are in flight.
    pub fn in_flight(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

/// One streamed PUT in flight; see `StreamsInFlight`.
pub struct StreamTicket {
    streams: Arc<StreamsInFlight>,
    slot: (u32, u64),
}

impl StreamTicket {
    /// Whether a needle of this key was appended after global offset `g`.
    pub fn appended_after(&self, g: u64) -> bool {
        self.streams.keys.lock().get(&self.slot).is_some_and(|e| e.1 > g)
    }
}

impl Drop for StreamTicket {
    fn drop(&mut self) {
        let mut keys = self.streams.keys.lock();
        if let Some(e) = keys.get_mut(&self.slot) {
            e.0 -= 1;
            if e.0 == 0 {
                keys.remove(&self.slot);
            }
        }
        drop(keys);
        self.streams.count.fetch_sub(1, Ordering::SeqCst);
    }
}

pub(crate) fn bad_request(code: &str, msg: &str, resource: &str) -> Response<ResponseBody> {
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(s3_error_xml(code, msg, resource, "req-put"))))
        .unwrap()
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

pub(crate) fn empty_body() -> ResponseBody {
    Empty::new().map_err(never_fails).boxed()
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

fn vhost_bucket(host_header: Option<&str>) -> Option<&str> {
    let host_no_port = host_header?.split(':').next()?;
    for base in &["s3.localhost", "localhost", "s3.internal", "s3.amazonaws.com", "127.0.0.1"] {
        if let Some(prefix) = host_no_port.strip_suffix(base).and_then(|p| p.strip_suffix('.')) {
            if !prefix.is_empty() && !prefix.contains('.') && !prefix.contains('/') {
                return Some(prefix);
            }
        }
    }
    None
}

fn split_path_style(trimmed_path: &str) -> (&str, Option<&str>) {
    let mut parts = trimmed_path.splitn(2, '/');
    let bucket = parts.next().unwrap_or("");
    // Path-style bucket URIs like `/bucket/` end in a slash with an empty tail; treat as bucket, not key `""`.
    let raw_key = parts.next().filter(|k| !k.is_empty());
    (bucket, raw_key)
}

pub fn parse_s3_target(uri_path: &str, host_header: Option<&str>) -> ParsedTarget {
    let raw_path = uri_path.split('?').next().unwrap_or(uri_path);
    let trimmed_path = raw_path.trim_start_matches('/');

    if let Some(bucket) = vhost_bucket(host_header) {
        let bucket = bucket.to_string();
        return if trimmed_path.is_empty() {
            ParsedTarget::Bucket { bucket }
        } else {
            ParsedTarget::Object {
                bucket,
                key: percent_decode_path(trimmed_path),
            }
        };
    }

    if trimmed_path.is_empty() {
        return ParsedTarget::Service;
    }

    let (bucket, raw_key) = split_path_style(trimmed_path);
    match raw_key {
        Some(key) => ParsedTarget::Object {
            bucket: bucket.to_string(),
            key: percent_decode_path(key),
        },
        None => ParsedTarget::Bucket {
            bucket: bucket.to_string(),
        },
    }
}

/// Decodes %XX in a URL path. Unlike query decoding, `+` stays `+`. S3 object keys are the
/// decoded path: a client sends "my%20file.txt" for the key "my file.txt". Keys used to be
/// stored as sent, encoded.
/// Returns the raw (still percent-encoded) object key slice from the request URI path, if any.
pub fn raw_s3_key<'a>(uri_path: &'a str, host_header: Option<&str>) -> Option<&'a str> {
    let raw_path = uri_path.split('?').next().unwrap_or(uri_path);
    let trimmed_path = raw_path.trim_start_matches('/');
    if vhost_bucket(host_header).is_some() {
        return if trimmed_path.is_empty() { None } else { Some(trimmed_path) };
    }
    if trimmed_path.is_empty() {
        return None;
    }
    split_path_style(trimmed_path).1
}

/// Decodes `%XX` in a URL path into raw bytes. Unlike query decoding, `+` stays `+`.
pub fn percent_decode_path_bytes(s: &str) -> Vec<u8> {
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
    out
}

pub fn percent_decode_path(s: &str) -> String {
    let out = percent_decode_path_bytes(s);
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
    if crate::names::validate_object_key(key).is_err() {
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
    pub payload_cache: Arc<PayloadCache>,
    /// Byte-budget admission controller for buffered in-flight request bodies (#128).
    pub body_budget: Arc<crate::memory::BodyBudget>,
    pub next_vol_offset: Arc<RwLock<u64>>,
    /// Disk writes, syncs, reads and payload copies, for the work-count tests.
    pub work: Arc<WorkCounters>,
    pub test_hook: Arc<RwLock<Option<TestHook>>>,
    /// Striped per-key locks: requests that change one key take its lock from before the
    /// volume write until the engine row is updated, so log order equals index order
    /// (`verification/tla/ObjectVersions.tla`, `UseKeyLock`). A streamed PUT takes it only to
    /// commit and uses `streams` to keep that order (#154).
    pub key_locks: Arc<Vec<tokio::sync::Mutex<()>>>,
    /// Large objects whose bytes failed their checksum while being sent (#155).
    pub read_checksum_failures: Arc<AtomicU64>,
    /// GETs of at least this many bytes are read with direct I/O; smaller ones through the
    /// page cache (owner decision 1C, 2026-11-15, #155). `AWAN_DIRECT_READ_MIN`.
    pub direct_read_min: AtomicU64,
    /// Streamed PUTs in flight, and what was appended for their keys meanwhile (#154).
    pub streams: Arc<StreamsInFlight>,
    /// Streamed PUTs written a second time because their key was written or deleted while
    /// their body arrived (#154).
    pub stream_relocations: Arc<AtomicU64>,
    /// Who may call; `AuthConfig::open()` by default, the binary sets it from flags and env.
    pub auth: crate::auth::AuthConfig,
    /// Default object ownership applied to buckets with no explicit ownership config (#122, D5).
    pub default_bucket_ownership: crate::authz::BucketOwnership,
    /// True when the server serves TLS (`--cert` / `--key`), reported in `aws:SecureTransport`.
    pub is_https: bool,
    /// Why the server is not ready yet (`None` when it is); see `/_awanfs/health/ready` (#44).
    pub not_ready: Arc<RwLock<Option<&'static str>>>,
    /// Request counts, latencies and bytes for `/_awanfs/metrics` (#42).
    pub metrics: Arc<crate::metrics::Metrics>,
    /// Where the per-request JSON log lines go; `None` logs nothing (#41).
    pub access_log: Arc<RwLock<Option<crate::request_log::LogSink>>>,
    /// Request-log lines dropped because the log writer could not keep up (#138).
    pub log_lines_dropped: Arc<AtomicU64>,
    /// Where S3-format server access log lines go; `None` (the default) writes none (#46).
    pub audit_log: Arc<RwLock<Option<crate::request_log::LogSink>>>,
    /// Users and policies from the config file (#53); `None` = only the root key pair.
    pub iam: Arc<RwLock<Option<Arc<crate::iam::ReloadingIam>>>>,
    /// Versioning status per bucket id (#31); absent = never set.
    pub versioning: Arc<RwLock<HashMap<u32, crate::versions::Versioning>>>,
    /// Version histories of the keys of versioned buckets (#31). Lock order: never take the
    /// engine lock while holding this one.
    /// Version histories of the keys of versioned buckets (#31). Lock order: never take the
    /// engine lock while holding this one.
    pub versions: Arc<RwLock<crate::versions::VersionStore>>,
    /// Single clock for the server (#158).
    pub clock: Arc<dyn Clock>,
    /// Time the server started (used as fallback CreationDate for legacy buckets).
    pub started_at_ns: u64,
    /// Bucket creation timestamps (ns since Unix epoch) from `buckets.manifest`.
    pub bucket_created_ns: Arc<RwLock<HashMap<u32, u64>>>,
    /// Background task scheduler (#158).
    pub scheduler: Arc<Scheduler>,
    /// Seconds per lifecycle day (default 86400; `--lifecycle-day-seconds`).
    pub lifecycle_day_seconds: Arc<AtomicU64>,
    /// Per-bucket configuration store backed by `buckets.manifest` (#149).
    pub bucket_configs: Arc<parking_lot::Mutex<crate::bucket_config::BucketConfigStore>>,
    /// Lock-free reader for `BucketConfig` snapshots (#149).
    pub bucket_config_reader: crate::bucket_config::BucketConfigReader,
    /// Metrics for bucket configuration writes and manifest health (#149).
    pub bucket_config_metrics: Arc<crate::bucket_config::ConfigMetrics>,
    /// Idle timeout while waiting for request headers (#187).
    pub header_idle_timeout_ms: Arc<AtomicU64>,
    /// Idle timeout between consecutive request body frames (#187).
    pub body_idle_timeout_ms: Arc<AtomicU64>,
    /// Configured S3 region (default `"us-east-1"`, `--region`) (#140).
    pub region: String,
    /// Side map for `0x200` attribute-update needles (`Decision 56`, `#203`).
    pub attr_updates: Arc<AttrUpdateStore>,
    /// `0x200` attribute-update needles ignored during recovery because their target was
    /// absent, deleted, or a delete marker.
    pub attr_updates_ignored: Arc<AtomicU64>,
    /// Background needle scrub state and last pass report (#155).
    pub scrub: Arc<crate::scrub::ScrubManager>,
    /// In-process commit event subscribers and per-bucket sequence numbers (#159).
    pub events: Arc<crate::events::CommitEventBus>,
    /// Allows emitting per-bucket usage gauges even above `PER_BUCKET_METRICS_CARDINALITY_CAP` (#159).
    pub per_bucket_metrics: Arc<AtomicBool>,
}

#[inline]
fn key_hash_of(key: &str) -> u64 {
    xxhash_rust::xxh3::xxh3_64(key.as_bytes())
}

#[derive(Default)]
struct AttrUpdateMapState {
    map: HashMap<(u32, u64, u64), Vec<crate::fsm::KeyAttrUpdate>>,
    total_updates: usize,
}

/// Side map from `(bucket_id, key_hash, target_version_id)` to the latest live `0x200`
/// attribute-update needle location per `AttrGroup`. Only updated objects hold entries.
#[derive(Default)]
pub struct AttrUpdateStore {
    inner: RwLock<AttrUpdateMapState>,
    entry_count: AtomicU64,
    estimated: std::sync::atomic::AtomicUsize,
}

impl AttrUpdateStore {
    /// Approximate heap bytes per updated object entry with one group update.
    pub const fn side_map_entry_bytes() -> usize {
        std::mem::size_of::<(u32, u64, u64)>()
            + std::mem::size_of::<Vec<crate::fsm::KeyAttrUpdate>>()
            + 8
            + std::mem::size_of::<crate::fsm::KeyAttrUpdate>()
    }

    fn recompute_locked(state: &AttrUpdateMapState) -> usize {
        if state.map.is_empty() {
            return 0;
        }
        let slot_bytes = std::mem::size_of::<(u32, u64, u64)>()
            + std::mem::size_of::<Vec<crate::fsm::KeyAttrUpdate>>()
            + 8;
        state.map.capacity() * slot_bytes
            + state.total_updates * std::mem::size_of::<crate::fsm::KeyAttrUpdate>()
    }

    fn finish_write(&self, state: &AttrUpdateMapState, cache: Option<&PayloadCache>) {
        let len = state.map.len() as u64;
        let bytes = Self::recompute_locked(state);
        self.entry_count.store(len, Ordering::Release);
        self.estimated.store(bytes, Ordering::Release);
        if let Some(c) = cache {
            c.set_reserved_bytes(bytes);
        }
    }

    pub fn has_updates(&self) -> bool {
        self.entry_count.load(Ordering::Acquire) > 0
    }

    pub fn entry_count(&self) -> usize {
        self.entry_count.load(Ordering::Acquire) as usize
    }

    pub fn estimated_bytes(&self) -> usize {
        self.estimated.load(Ordering::Acquire)
    }

    pub fn get(&self, b_id: u32, key: &str, target_version_id: u64) -> Vec<crate::fsm::KeyAttrUpdate> {
        if !self.has_updates() {
            return Vec::new();
        }
        let h = key_hash_of(key);
        self.inner
            .read()
            .map
            .get(&(b_id, h, target_version_id))
            .cloned()
            .unwrap_or_default()
    }

    pub fn max_ts(&self, b_id: u32, key: &str, target_version_id: u64) -> Option<u64> {
        if !self.has_updates() {
            return None;
        }
        let h = key_hash_of(key);
        self.inner
            .read()
            .map
            .get(&(b_id, h, target_version_id))
            .and_then(|v| v.iter().map(|u| u.created_at_ns).max())
    }

    pub fn set_for_target(
        &self,
        b_id: u32,
        key: &str,
        target_version_id: u64,
        updates: Vec<crate::fsm::KeyAttrUpdate>,
        cache: Option<&PayloadCache>,
    ) {
        let h = key_hash_of(key);
        let mut guard = self.inner.write();
        if let Some(old) = guard.map.remove(&(b_id, h, target_version_id)) {
            guard.total_updates = guard.total_updates.saturating_sub(old.len());
        }
        if !updates.is_empty() {
            guard.total_updates += updates.len();
            guard.map.insert((b_id, h, target_version_id), updates);
        }
        self.finish_write(&guard, cache);
    }

    pub fn remove_target(
        &self,
        b_id: u32,
        key: &str,
        target_version_id: u64,
        cache: Option<&PayloadCache>,
    ) -> Vec<crate::fsm::KeyAttrUpdate> {
        if !self.has_updates() {
            return Vec::new();
        }
        let h = key_hash_of(key);
        let mut guard = self.inner.write();
        let removed = guard.map.remove(&(b_id, h, target_version_id)).unwrap_or_default();
        if !removed.is_empty() {
            guard.total_updates = guard.total_updates.saturating_sub(removed.len());
            self.finish_write(&guard, cache);
        }
        removed
    }

    pub fn live_groups_at(
        &self,
        b_id: u32,
        key: &str,
        target_version_id: u64,
        vol_offset: u64,
    ) -> Vec<crate::attrs::AttrGroup> {
        if !self.has_updates() {
            return Vec::new();
        }
        let h = key_hash_of(key);
        self.inner
            .read()
            .map
            .get(&(b_id, h, target_version_id))
            .map(|list| {
                list.iter()
                    .filter(|u| u.vol_offset == vol_offset)
                    .map(|u| u.group)
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn move_offset(
        &self,
        b_id: u32,
        key: &str,
        target_version_id: u64,
        g_old: u64,
        g_new: u64,
        new_total_len: u64,
    ) -> usize {
        if !self.has_updates() {
            return 0;
        }
        let h = key_hash_of(key);
        let mut guard = self.inner.write();
        let Some(list) = guard.map.get_mut(&(b_id, h, target_version_id)) else {
            return 0;
        };
        let mut moved = 0usize;
        for u in list.iter_mut() {
            if u.vol_offset == g_old {
                u.vol_offset = g_new;
                u.total_len = new_total_len;
                moved += 1;
            }
        }
        moved
    }
}

/// Errors from `S3ServiceState::set_attribute_group`.
#[derive(Debug)]
pub enum SetAttrError {
    Refused {
        refusal: crate::fsm::Refusal,
        delete_marker: Option<(String, bool)>,
    },
    TooLarge,
    Internal {
        detail: String,
        after_record: bool,
    },
}

/// Result of writing a `0x200` attribute-update needle via `set_attribute_group`.
#[derive(Debug, Clone)]
pub struct SetAttrOutcome {
    pub version_id: Option<String>,
    pub vol_offset: u64,
    pub created_at_ns: u64,
}

/// Per-bucket live usage counters reported by `S3ServiceState::bucket_usage` (#159).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BucketUsageReport {
    pub current: crate::meta_index::BucketUsage,
    pub all_versions: crate::meta_index::BucketUsage,
}

/// Optional commit event metadata supplied on a versioned commit (#159).
#[derive(Clone, Debug)]
pub(crate) struct VersionCommitEventSpec {
    pub time: u64,
    pub version: Option<String>,
    pub kind: crate::events::CommitKind,
    pub size: u64,
    pub etag: String,
    pub acked: bool,
}

/// Maximum number of buckets for which `awanfs_bucket_*` gauges emit automatically (#159).
pub const PER_BUCKET_METRICS_CARDINALITY_CAP: usize = 1000;

/// What `S3ServiceState::try_with_data_dir_limits` keeps in memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenLimits {
    /// Objects kept in the in-memory table; the rest go to the spill index.
    pub max_objects: usize,
    /// The spill index's budget; `None` takes it from the memory budget.
    pub spill_cache_bytes: Option<usize>,
}

impl Default for OpenLimits {
    fn default() -> Self {
        OpenLimits { max_objects: 100_000, spill_cache_bytes: None }
    }
}

#[derive(Debug)]
pub enum DeleteError {
    PreconditionFailed(Option<crate::precondition::ObjectFacts>),
    AccessDenied,
    Internal(String),
}

impl S3ServiceState {
    pub fn new() -> Self {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let started_at_ns = clock.now_ns();
        let metrics = Arc::new(crate::metrics::Metrics::default());
        let scheduler = Arc::new(Scheduler::new(Arc::clone(&metrics)));
        let id = INSTANCE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let default_dir = std::env::temp_dir().join(format!(
            "s3_vol_{}_{}",
            std::process::id(),
            id
        ));
        let _ = std::fs::create_dir_all(&default_dir);
        let work = Arc::new(WorkCounters::default());
        let vol_mgr = VolumeManager::open(&default_dir, 1).ok().map(|mut vm| {
            vm.attach_work_counters(Arc::clone(&work));
            Arc::new(vm.with_clock(Arc::clone(&clock)))
        });
        let initial_bytes = if let Some(ref vm) = vol_mgr { vm.offset() } else { 0 };
        let bucket_store = crate::bucket_config::BucketConfigStore::in_memory(
            crate::bucket_config::ValidatorRegistry::with_default_validators(),
        );
        let bucket_config_reader = bucket_store.reader();
        let bucket_config_metrics = bucket_store.metrics();
        Self {
            data_dir: None,
            total_volume_bytes: Arc::new(AtomicU64::new(initial_bytes)),
            tombstone_bytes: Arc::new(AtomicU64::new(0)),
            compaction_rate: Arc::new(AtomicU64::new(0)),
            active_volume_id: Arc::new(AtomicU32::new(1)),
            engine: Arc::new(RwLock::new(AwanStorageEngine::new(100_000, 4_194_304, 65536))),
            multipart: Arc::new(
                MultipartManager::with_staging_dir(default_dir.join(".staging")).with_work_counters(Arc::clone(&work)).with_clock(Arc::clone(&clock)),
            ),
            sigv4_cache: Arc::new(SigV4KeyCache::new()),
            lifecycle: Arc::new(LifecycleManager::new()),
            single_flight: Arc::new(SingleFlight::new()),
            mvcc: Arc::new(MvccStore::new()),
            volumes: Arc::new(RwLock::new(vol_mgr.iter().map(|vm| (vm.id(), Arc::clone(vm))).collect())),
            roll_lock: Arc::new(parking_lot::Mutex::new(())),
            volume_manager: Arc::new(RwLock::new(vol_mgr)),
            payload_cache: Arc::new(PayloadCache::new(crate::memory::split_cache(crate::memory::effective().cache).1.min(usize::MAX as u64) as usize)),
            body_budget: Arc::new(crate::memory::BodyBudget::new(crate::memory::effective().bodies)),
            next_vol_offset: Arc::new(RwLock::new(4096)),
            work,
            test_hook: Arc::new(RwLock::new(None)),
            key_locks: Arc::new((0..256).map(|_| tokio::sync::Mutex::new(())).collect()),
            read_checksum_failures: Arc::new(AtomicU64::new(0)),
            direct_read_min: AtomicU64::new(crate::stream_read::direct_read_min_from_env()),
            streams: Arc::new(StreamsInFlight::default()),
            stream_relocations: Arc::new(AtomicU64::new(0)),
            auth: crate::auth::AuthConfig::open(),
            default_bucket_ownership: crate::authz::DEFAULT_BUCKET_OWNERSHIP,
            is_https: false,
            not_ready: Arc::new(RwLock::new(None)),
            metrics,
            access_log: Arc::new(RwLock::new(None)),
            log_lines_dropped: Arc::new(AtomicU64::new(0)),
            audit_log: Arc::new(RwLock::new(None)),
            iam: Arc::new(RwLock::new(None)),
            versioning: Arc::new(RwLock::new(HashMap::new())),
            versions: Arc::new(RwLock::new(crate::versions::VersionStore::default())),
            clock,
            started_at_ns,
            bucket_created_ns: Arc::new(RwLock::new(HashMap::new())),
            scheduler,
            lifecycle_day_seconds: Arc::new(AtomicU64::new(86_400)),
            bucket_configs: Arc::new(parking_lot::Mutex::new(bucket_store)),
            bucket_config_reader,
            bucket_config_metrics,
            header_idle_timeout_ms: Arc::new(AtomicU64::new(
                crate::body_timeout::DEFAULT_HEADER_IDLE_TIMEOUT.as_millis() as u64,
            )),
            body_idle_timeout_ms: Arc::new(AtomicU64::new(
                crate::body_timeout::DEFAULT_BODY_IDLE_TIMEOUT.as_millis() as u64,
            )),
            region: "us-east-1".to_string(),
            attr_updates: Arc::new(AttrUpdateStore::default()),
            attr_updates_ignored: Arc::new(AtomicU64::new(0)),
            scrub: Arc::new(crate::scrub::ScrubManager::new()),
            events: Arc::new(crate::events::CommitEventBus::new()),
            per_bucket_metrics: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Opens a data directory, panicking when it cannot be served; see `try_with_data_dir`.
    pub fn with_data_dir(dir: &Path) -> Self {
        Self::with_data_dir_and_clock(dir, Arc::new(SystemClock))
    }

    pub fn with_data_dir_and_clock(dir: &Path, clock: Arc<dyn Clock>) -> Self {
        Self::try_with_data_dir_and_clock(dir, clock).unwrap_or_else(|e| panic!("cannot open {}: {}", dir.display(), e))
    }

    /// Opens a data directory and replays it. Fails, instead of serving a subset, when a volume
    /// cannot be opened or was written in a newer on-disk format (#50).
    pub fn try_with_data_dir(dir: &Path) -> Result<Self, String> {
        Self::try_with_data_dir_and_clock(dir, Arc::new(SystemClock))
    }

    pub fn try_with_data_dir_and_clock(dir: &Path, clock: Arc<dyn Clock>) -> Result<Self, String> {
        Self::try_with_data_dir_limits(dir, clock, OpenLimits::default())
    }

    /// As `try_with_data_dir_and_clock`, with the in-memory row count and the spill budget set,
    /// so a test reaches the spill index and its compactions with a few thousand objects.
    pub fn try_with_data_dir_limits(dir: &Path, clock: Arc<dyn Clock>, limits: OpenLimits) -> Result<Self, String> {
        let started_at_ns = clock.now_ns();
        let metrics = Arc::new(crate::metrics::Metrics::default());
        let scheduler = Arc::new(Scheduler::new(Arc::clone(&metrics)));
        let _ = std::fs::create_dir_all(dir);
        let work = Arc::new(WorkCounters::default());
        // Every volume in id order; writes go to the last one (#37).
        let mut volumes: BTreeMap<u32, Arc<VolumeManager>> = BTreeMap::new();
        let mut ids = volume_ids(dir);
        if ids.is_empty() {
            ids.push(1);
        }
        for id in ids {
            // Every new volume grows as it is written (decision 52). A volume that is already a
            // 32 GiB sparse file, from before that decision, is recognised by its length and
            // stays sparse. `AWAN_SPARSE_VOLUME=1` still makes new ones sparse, for comparison.
            let sparse = std::env::var("AWAN_SPARSE_VOLUME").ok().as_deref() == Some("1");
            let mut vm = VolumeManager::open_mode_with_clock(dir, id, sparse, Arc::clone(&clock)).map_err(|e| format!("volume {}: {}", id, e))?;
            vm.attach_work_counters(Arc::clone(&work));
            volumes.insert(id, Arc::new(vm));
        }
        let vol_mgr = volumes.values().next_back().cloned();
        let multipart = Arc::new(
            MultipartManager::with_staging_dir(dir.join(".staging")).with_work_counters(Arc::clone(&work)).with_clock(Arc::clone(&clock)),
        );
        let mut engine = AwanStorageEngine::new(limits.max_objects, 4_194_304, 65536);
        // The redb spill index of servers before #26 stage 6: rebuilt from the volumes at every
        // boot, so nothing in it is needed (decision 59). The segments under meta/ replace it.
        let _ = std::fs::remove_file(dir.join("metadata.db"));
        let spill_cache_bytes = limits.spill_cache_bytes.unwrap_or_else(|| crate::memory::split_cache(crate::memory::effective().cache).0.min(usize::MAX as u64) as usize);
        let spill = crate::spill::SpillIndex::open_with_cache_size(dir, spill_cache_bytes).map_err(|e| format!("metadata segments: {}", e))?;
        // Rebuilt from the volume replay below, as before (#13); booting from the manifest's
        // checkpoint comes next in #26.
        spill.clear().map_err(|e| format!("metadata segments: {}", e))?;
        engine.attach_spill_index(spill);
        let mut initial_vol_offset = 4096;

        // 1. Replay buckets manifest
        let manifest_path = dir.join("buckets.manifest");
        let bucket_store = crate::bucket_config::BucketConfigStore::open(
            Some(manifest_path),
            crate::bucket_config::ValidatorRegistry::with_default_validators(),
        )?;
        let bucket_config_reader = bucket_store.reader();
        let bucket_config_metrics = bucket_store.metrics();
        let mut versioning_map: HashMap<u32, crate::versions::Versioning> = HashMap::new();
        let mut bucket_created_ns: HashMap<u32, u64> = HashMap::new();
        let mut version_store = crate::versions::VersionStore::default();
        for (&id, cfg) in bucket_store.snapshot().iter() {
            engine.restore_bucket_in(id, &cfg.namespace, &cfg.name);
            if cfg.versioning != crate::versions::Versioning::Unset {
                versioning_map.insert(id, cfg.versioning);
            }
            if let Some(ts) = cfg.created_ns {
                if ts != 0 {
                    bucket_created_ns.insert(id, ts);
                }
            }
        }

        // 2. Replay the needle volumes, oldest first: writes only move to a newer volume, so
        // volume order is log order. Versions are ordered by their ids instead (versions.rs).
        let mut mp_extents: HashMap<(u32, u64), Vec<PartExtent>> = HashMap::new();
        let mut pending_updates: HashMap<(u32, u64, u64), Vec<crate::fsm::KeyAttrUpdate>> = HashMap::new();
        let mut updated_keys: HashMap<(u32, u64), String> = HashMap::new();
        let mut unver_base_ns: HashMap<(u32, u64), u64> = HashMap::new();
        let mut ignored_attr_updates: u64 = 0;
        for vm in volumes.values() {
            initial_vol_offset = vm.offset().max(4096);
            let _records_guard = crate::memory::BootHeapGuard::enter(crate::memory::BootHeapPart::NeedleRecords);
            // Stream needles directly into replay in offset order so later writes win without
            // buffering a volume's records in memory (#323, Decision 7A).
            let res = vm.try_scan_needles(|n| -> Result<(), String> {
                let created_at_ms = n.created_at_ns / 1_000_000;
                let (b_id, key, local, size, total_len, etag_hi, etag_lo, flags, crc32c) = (
                    n.bucket_id,
                    n.key,
                    n.offset,
                    n.payload_size,
                    n.total_len,
                    n.etag_hi,
                    n.etag_lo,
                    n.flags,
                    n.payload_crc32c,
                );
                let offset = global_offset(vm.id(), local);
                let b = b_id as u32;
                let kh = key_hash_of(&key);

                // Collect 0x200 attribute-update needles without disturbing base object replay
                // and without keeping records for un-updated keys (#203, Decision 56).
                if flags & crate::meta_index::FLAG_ATTR_UPDATE != 0 {
                    let _other_guard = crate::memory::BootHeapGuard::enter(crate::memory::BootHeapPart::Other);
                    let payload_res = match n.attr_update_payload {
                        Some(p) => Ok(Bytes::from(p)),
                        None => vm.read_needle_bytes(vm.id(), local, crc32c),
                    };
                    match payload_res
                        .ok()
                        .and_then(|bytes| crate::attrs::decode_attr_update(&bytes).ok())
                    {
                        Some((target_vid, upd_attrs)) => {
                            let mut seen_groups = Vec::new();
                            for (tag, _) in upd_attrs.iter() {
                                let g = crate::attrs::AttrGroup::from_tag(tag);
                                if !seen_groups.contains(&g) {
                                    seen_groups.push(g);
                                }
                            }
                            if seen_groups.is_empty() {
                                ignored_attr_updates += 1;
                            } else {
                                updated_keys.entry((b, kh)).or_insert_with(|| key.clone());
                                let slot = pending_updates.entry((b, kh, target_vid)).or_default();
                                for group in seen_groups {
                                    slot.push(crate::fsm::KeyAttrUpdate {
                                        group,
                                        created_at_ns: n.created_at_ns,
                                        vol_offset: offset,
                                        total_len,
                                    });
                                }
                            }
                        }
                        None => {
                            ignored_attr_updates += 1;
                        }
                    }
                    return Ok(());
                }

                // Keys with a version history replay through the version store (#31).
                if flags & (crate::meta_index::FLAG_VERSIONED | crate::meta_index::FLAG_VERSION_DELETE) != 0 || version_store.has(b, &key) {
                    let _other_guard = crate::memory::BootHeapGuard::enter(crate::memory::BootHeapPart::Other);
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
                    // An unversioned record of a key that already has a history was stamped
                    // while the bucket was unversioned, so live it changed the row before the
                    // history began; it sits later in the log because a streamed PUT reserved
                    // first and committed versioned (#186). Apply it the way the live index
                    // did: a PUT becomes the null version at time 0 (`null_version_of`), a
                    // DELETE removes the null version. Its own creation time does not count.
                    let before_history = flags & (crate::meta_index::FLAG_VERSIONED | crate::meta_index::FLAG_VERSION_DELETE) == 0;
                    let ts = if before_history { 0 } else { n.created_at_ns };
                    // Last-Modified: the record's own time, in the row's milliseconds for a
                    // pre-history record, as `null_version_of` gives it live (#238).
                    let modified = if before_history { created_at_ms.saturating_mul(1_000_000) } else { n.created_at_ns };
                    let ev = if before_history && flags & AWAN_FLAG_TOMBSTONE != 0 {
                        if let Some(removed) = pending_updates.remove(&(b, kh, 0)) {
                            ignored_attr_updates += removed.len() as u64;
                        }
                        crate::versions::Event::Remove(0)
                    } else if flags & crate::meta_index::FLAG_VERSION_DELETE != 0 {
                        match vm.read_needle_bytes(vm.id(), local, crc32c) {
                            Ok(p) if p.len() == 8 => {
                                let del_id = u64::from_le_bytes(p[..8].try_into().unwrap());
                                if let Some(removed) = pending_updates.remove(&(b, kh, del_id)) {
                                    ignored_attr_updates += removed.len() as u64;
                                }
                                crate::versions::Event::Remove(del_id)
                            }
                            _ => return Ok(()),
                        }
                    } else if flags & AWAN_FLAG_TOMBSTONE != 0 {
                        if null {
                            if let Some(list) = pending_updates.get_mut(&(b, kh, 0)) {
                                let before_len = list.len();
                                list.retain(|u| u.created_at_ns > n.created_at_ns);
                                ignored_attr_updates += (before_len - list.len()) as u64;
                            }
                        }
                        crate::versions::Event::Put(crate::versions::Version { ts, modified, null, marker: true, vol_offset: offset, size: 0, etag_hi: 0, etag_lo: 0, part_count: 0, flags })
                    } else {
                        if before_history || null {
                            if let Some(list) = pending_updates.get_mut(&(b, kh, 0)) {
                                let before_len = list.len();
                                list.retain(|u| u.created_at_ns > n.created_at_ns);
                                ignored_attr_updates += (before_len - list.len()) as u64;
                            }
                        }
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
                                _ => return Ok(()),
                            }
                        } else if flags & crate::meta_index::FLAG_COMPRESSED != 0 {
                            match vm.payload_prefix(local, key.len(), crate::compress::PREFIX_LEN).ok().and_then(|p| crate::compress::logical_size(&p)) {
                                Some(n) => (n, 0),
                                None => return Ok(()),
                            }
                        } else {
                            (size, 0)
                        };
                        crate::versions::Event::Put(crate::versions::Version { ts, modified, null, marker: false, vol_offset: offset, size, etag_hi, etag_lo, part_count, flags })
                    };
                    version_store.apply(b, &key, ev);
                    match version_store.current(b, &key) {
                        Some(v) if !v.marker => {
                            let _ = engine.put_object_at(b, key.as_bytes(), v.vol_offset, v.size, v.etag_hi, v.etag_lo, v.part_count, v.flags, v.ts / 1_000_000);
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
                    return Ok(());
                }
                if flags & AWAN_FLAG_TOMBSTONE != 0 {
                    if let Some(removed) = pending_updates.remove(&(b, kh, 0)) {
                        ignored_attr_updates += removed.len() as u64;
                    }
                    let _ = unver_base_ns.remove(&(b, kh));
                    let next_state = match crate::fsm::key_step(
                        crate::fsm::KeyState::Absent,
                        crate::fsm::KeyRecord::Tombstone {
                            versioned: false,
                            is_null: false,
                            created_at_ns: n.created_at_ns,
                            vol_offset: offset,
                        },
                    ) {
                        Ok(s) => s,
                        Err(_) => crate::fsm::KeyState::Absent,
                    };
                    if matches!(next_state, crate::fsm::KeyState::Absent) {
                        let _ = engine.delete_object(b, key.as_bytes());
                    }
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
                        let _ = engine.put_object_at(
                            b,
                            key.as_bytes(),
                            offset,
                            total,
                            etag_hi,
                            etag_lo,
                            extents.len() as u16,
                            flags,
                            created_at_ms,
                        );
                        multipart.restore_completed(b, &key, extents);
                        if updated_keys.contains_key(&(b, kh)) {
                            unver_base_ns.insert((b, kh), n.created_at_ns);
                        }
                        if let Some(list) = pending_updates.get_mut(&(b, kh, 0)) {
                            let before_len = list.len();
                            list.retain(|u| u.created_at_ns > n.created_at_ns);
                            ignored_attr_updates += (before_len - list.len()) as u64;
                        }
                    }
                } else {
                    // A compressed needle's index row holds the object's size, not the stored one (#51).
                    let size = if flags & crate::meta_index::FLAG_COMPRESSED != 0 {
                        match vm.payload_prefix(local, key.len(), crate::compress::PREFIX_LEN).ok().and_then(|p| crate::compress::logical_size(&p)) {
                            Some(n) => n,
                            None => return Ok(()),
                        }
                    } else {
                        size
                    };
                    let _ = engine.put_object_at(
                        b,
                        key.as_bytes(),
                        offset,
                        size,
                        etag_hi,
                        etag_lo,
                        0,
                        flags,
                        created_at_ms,
                    );
                    if updated_keys.contains_key(&(b, kh)) {
                        unver_base_ns.insert((b, kh), n.created_at_ns);
                    }
                    if let Some(list) = pending_updates.get_mut(&(b, kh, 0)) {
                        let before_len = list.len();
                        list.retain(|u| u.created_at_ns > n.created_at_ns);
                        ignored_attr_updates += (before_len - list.len()) as u64;
                    }
                }
                Ok(())
            });
            match res {
                Err(e) => return Err(format!("scan volume {}: {:?}", vm.id(), e)),
                Ok(Err(e)) => return Err(e),
                Ok(Ok(())) => {}
            }
        }

        // Fold surviving 0x200 attribute updates in created_at_ns order through fsm::key_step (#203).
        let attr_updates = Arc::new(AttrUpdateStore::default());
        if !pending_updates.is_empty() {
            let _other_guard = crate::memory::BootHeapGuard::enter(crate::memory::BootHeapPart::Other);
            for ((b_id, kh, target_vid), mut updates) in pending_updates {
                let Some(key) = updated_keys.get(&(b_id, kh)) else {
                    ignored_attr_updates += updates.len() as u64;
                    continue;
                };
                updates.sort_by(|a, b| (a.created_at_ns, a.vol_offset).cmp(&(b.created_at_ns, b.vol_offset)));
                let mut key_state = if version_store.has(b_id, key) {
                    let entries = version_store
                        .versions(b_id, key)
                        .map(|list| {
                            list.iter()
                                .map(|v| {
                                    if v.marker {
                                        crate::fsm::KeyVersionEntry::DeleteMarker {
                                            version_id: v.id(),
                                            vol_offset: v.vol_offset,
                                        }
                                    } else {
                                        crate::fsm::KeyVersionEntry::Object(crate::fsm::KeyObject {
                                            version_id: v.id(),
                                            created_at_ns: v.ts,
                                            vol_offset: v.vol_offset,
                                            flags: v.flags,
                                            updates: Vec::new(),
                                        })
                                    }
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    crate::fsm::KeyState::Versioned { versions: entries }
                } else {
                    match engine.get_object(b_id, key.as_bytes()) {
                        Ok(meta) => {
                            let base_ns = unver_base_ns
                                .get(&(b_id, kh))
                                .copied()
                                .unwrap_or_else(|| meta.created_at.saturating_mul(1_000_000));
                            crate::fsm::KeyState::Unversioned(crate::fsm::KeyObject {
                                version_id: 0,
                                created_at_ns: base_ns,
                                vol_offset: meta.vol_offset,
                                flags: unpack_flags(meta.bucket_flags),
                                updates: Vec::new(),
                            })
                        }
                        Err(_) => crate::fsm::KeyState::Absent,
                    }
                };
                for u in updates {
                    let prev_ts = match &key_state {
                        crate::fsm::KeyState::Unversioned(obj) => Some(obj.created_at_ns),
                        crate::fsm::KeyState::Versioned { versions } => versions.iter().find_map(|e| match e {
                            crate::fsm::KeyVersionEntry::Object(o) if o.version_id == target_vid => {
                                Some(o.created_at_ns)
                            }
                            _ => None,
                        }),
                        crate::fsm::KeyState::Absent => None,
                    };
                    match crate::fsm::key_step(
                        key_state.clone(),
                        crate::fsm::KeyRecord::AttrUpdate {
                            target_version_id: target_vid,
                            group: u.group,
                            created_at_ns: u.created_at_ns,
                            vol_offset: u.vol_offset,
                            total_len: u.total_len,
                        },
                    ) {
                        Ok(next) => {
                            if prev_ts.is_some_and(|t| u.created_at_ns <= t) {
                                ignored_attr_updates += 1;
                            }
                            key_state = next;
                        }
                        Err(_) => {
                            ignored_attr_updates += 1;
                        }
                    }
                }
                let final_updates = match key_state {
                    crate::fsm::KeyState::Unversioned(obj) if target_vid == 0 => obj.updates,
                    crate::fsm::KeyState::Versioned { versions } => versions
                        .into_iter()
                        .find_map(|e| match e {
                            crate::fsm::KeyVersionEntry::Object(o) if o.version_id == target_vid => Some(o.updates),
                            _ => None,
                        })
                        .unwrap_or_default(),
                    _ => Vec::new(),
                };
                if !final_updates.is_empty() {
                    attr_updates.set_for_target(b_id, key, target_vid, final_updates, None);
                }
            }
        }

        let payload_cache = Arc::new(PayloadCache::new(crate::memory::split_cache(crate::memory::effective().cache).1.min(usize::MAX as u64) as usize));
        payload_cache.set_reserved_bytes(attr_updates.estimated_bytes());

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
            payload_cache,
            body_budget: Arc::new(crate::memory::BodyBudget::new(crate::memory::effective().bodies)),
            next_vol_offset: Arc::new(RwLock::new(initial_vol_offset)),
            work,
            test_hook: Arc::new(RwLock::new(None)),
            key_locks: Arc::new((0..256).map(|_| tokio::sync::Mutex::new(())).collect()),
            read_checksum_failures: Arc::new(AtomicU64::new(0)),
            direct_read_min: AtomicU64::new(crate::stream_read::direct_read_min_from_env()),
            streams: Arc::new(StreamsInFlight::default()),
            stream_relocations: Arc::new(AtomicU64::new(0)),
            auth: crate::auth::AuthConfig::open(),
            default_bucket_ownership: crate::authz::DEFAULT_BUCKET_OWNERSHIP,
            is_https: false,
            not_ready: Arc::new(RwLock::new(None)),
            metrics,
            access_log: Arc::new(RwLock::new(None)),
            log_lines_dropped: Arc::new(AtomicU64::new(0)),
            audit_log: Arc::new(RwLock::new(None)),
            iam: Arc::new(RwLock::new(None)),
            versioning: Arc::new(RwLock::new(versioning_map)),
            versions: Arc::new(RwLock::new(version_store)),
            clock,
            started_at_ns,
            bucket_created_ns: Arc::new(RwLock::new(bucket_created_ns)),
            scheduler,
            lifecycle_day_seconds: Arc::new(AtomicU64::new(86_400)),
            bucket_configs: Arc::new(parking_lot::Mutex::new(bucket_store)),
            bucket_config_reader,
            bucket_config_metrics,
            header_idle_timeout_ms: Arc::new(AtomicU64::new(crate::body_timeout::DEFAULT_HEADER_IDLE_TIMEOUT.as_millis() as u64)),
            body_idle_timeout_ms: Arc::new(AtomicU64::new(crate::body_timeout::DEFAULT_BODY_IDLE_TIMEOUT.as_millis() as u64)),
            region: "us-east-1".to_string(),
            attr_updates,
            attr_updates_ignored: Arc::new(AtomicU64::new(ignored_attr_updates)),
            scrub: Arc::new(crate::scrub::ScrubManager::new()),
            events: Arc::new(crate::events::CommitEventBus::new()),
            per_bucket_metrics: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn header_idle_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.header_idle_timeout_ms.load(Ordering::Relaxed))
    }

    pub fn set_header_idle_timeout(&self, d: std::time::Duration) {
        self.header_idle_timeout_ms
            .store(d.as_millis().max(1) as u64, Ordering::Relaxed);
    }

    pub fn body_idle_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.body_idle_timeout_ms.load(Ordering::Relaxed))
    }

    pub fn set_body_idle_timeout(&self, d: std::time::Duration) {
        self.body_idle_timeout_ms
            .store(d.as_millis().max(1) as u64, Ordering::Relaxed);
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
        let conds = crate::precondition::DeleteConditions::default();
        self.delete_key_conditional(b_id, key, &conds)
            .await
            .map_err(|e| match e {
                DeleteError::PreconditionFailed(_) => "PreconditionFailed".to_string(),
                DeleteError::AccessDenied => "AccessDenied".to_string(),
                DeleteError::Internal(s) => s,
            })
    }

    pub async fn delete_key_conditional(
        &self,
        b_id: u32,
        key: &str,
        conds: &crate::precondition::DeleteConditions,
    ) -> Result<Option<String>, DeleteError> {
        let _key_guard = self.lock_key(b_id, key).await;
        if !conds.is_empty() {
            let (target, facts_opt) = {
                let vs = self.versions.read();
                if vs.has(b_id, key) {
                    match vs.current(b_id, key) {
                        None => (crate::precondition::DeleteTarget::Absent, None),
                        Some(v) if v.marker => (
                            crate::precondition::DeleteTarget::DeleteMarker {
                                last_modified_secs: crate::ops::object::last_modified_secs_from_ns(v.modified),
                            },
                            None,
                        ),
                        Some(v) => {
                            let etag = crate::ops::object::version_etag(&v);
                            let lm = crate::ops::object::last_modified_secs_from_ns(v.modified);
                            (
                                crate::precondition::DeleteTarget::Object {
                                    etag: etag.clone(),
                                    last_modified_secs: lm,
                                    size: v.size,
                                },
                                Some(crate::precondition::ObjectFacts { etag, last_modified_secs: lm }),
                            )
                        }
                    }
                } else {
                    drop(vs);
                    match self.engine.read().get_object(b_id, key.as_bytes()) {
                        Ok(m) => {
                            let flags = unpack_flags(m.bucket_flags);
                            let is_mp = flags & AWAN_FLAG_MULTIPART != 0;
                            let etag = crate::ops::object::format_object_etag(
                                m.etag_hi,
                                m.etag_lo,
                                is_mp,
                                unpack_part_count(m.bucket_flags),
                            );
                            let lm = crate::ops::object::last_modified_secs_from_ms(m.created_at);
                            (
                                crate::precondition::DeleteTarget::Object {
                                    etag: etag.clone(),
                                    last_modified_secs: lm,
                                    size: m.size,
                                },
                                Some(crate::precondition::ObjectFacts { etag, last_modified_secs: lm }),
                            )
                        }
                        Err(_) => (crate::precondition::DeleteTarget::Absent, None),
                    }
                }
            };
            if crate::precondition::delete_precondition(conds, &target)
                == crate::precondition::PreconditionOutcome::PreconditionFailed
            {
                return Err(DeleteError::PreconditionFailed(facts_opt));
            }
            if matches!(target, crate::precondition::DeleteTarget::Absent) {
                return Ok(None);
            }
        }
        self.hook("delete_start", key);
        if self.version_stamp(b_id, key).is_some() {
            return self.write_delete_marker_locked(b_id, key).await.map(Some);
        }
        // Unversioned path (defense in depth: never delete a locked object even if unversioned).
        let unv_target = self
            .engine
            .read()
            .get_object(b_id, key.as_bytes())
            .ok()
            .map(|m| null_version_of(&m));
        if let Some(ref v) = unv_target {
            let lock = self.version_lock_for(b_id, key, v)?;
            let now_sec = self.clock.now_ns() / 1_000_000_000;
            if lock.may_delete(now_sec, false).is_err() {
                return Err(DeleteError::AccessDenied);
            }
        }
        self.expire_unversioned_locked(b_id, key).await.map(|_| None)
    }

    /// Writes a versioned delete marker under `key_locks` (#31, #118). Caller holds `lock_key`.
    pub(crate) async fn write_delete_marker_locked(&self, b_id: u32, key: &str) -> Result<String, DeleteError> {
        let Some((extra, ts)) = self.version_stamp(b_id, key) else {
            return Err(DeleteError::Internal("bucket is not versioned".to_string()));
        };
        let flags = AWAN_FLAG_TOMBSTONE | extra;
        // A marker whole in the volume is replayed by recovery: it takes effect here too,
        // and the request still answers an error (#184, owner decision 4B).
        let (_loc, g, late) = match self.write_to_active_at(b_id, key, &[], flags, 0, 0, Some(ts)) {
            Ok((vm, loc, g)) => {
                self.hook("delete_marker_written", key);
                self.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
                let late = self
                    .make_durable(&vm, loc.seq)
                    .await
                    .err()
                    .map(|e| format!("could not sync the delete: {:?}", e));
                (loc, g, late)
            }
            Err(VolumeError::AfterRecord { loc, cause, .. }) => {
                self.hook("delete_marker_written", key);
                self.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
                (
                    loc,
                    global_offset(loc.volume_id, loc.offset),
                    Some(format!("could not record the delete: {}", cause)),
                )
            }
            Err(e) => return Err(DeleteError::Internal(format!("could not record the delete: {:?}", e))),
        };
        self.mvcc.tombstone(b_id, key);
        let v = crate::versions::Version {
            ts,
            modified: ts,
            null: flags & crate::meta_index::FLAG_NULL_ID != 0,
            marker: true,
            vol_offset: g,
            size: 0,
            etag_hi: 0,
            etag_lo: 0,
            part_count: 0,
            flags,
        };
        let ev_spec = VersionCommitEventSpec {
            time: ts,
            version: Some(v.id_string()),
            kind: crate::events::CommitKind::DeleteMarker,
            size: 0,
            etag: String::new(),
            acked: late.is_none(),
        };
        self.record_version_commit(b_id, key, crate::versions::Event::Put(v), None, Some(ev_spec))
            .map_err(|c| DeleteError::Internal(format!("index error {} for the delete marker", c)))?;
        if let Some(e) = late {
            return Err(DeleteError::Internal(e));
        }
        Ok(v.id_string())
    }

    /// Writes an unversioned tombstone under `key_locks` (#184, #118). Returns previous object's payload size.
    pub(crate) async fn expire_unversioned_locked(&self, b_id: u32, key: &str) -> Result<u64, DeleteError> {
        let tomb_ts = self.unversioned_write_ts(b_id, key);
        let (tomb_g, late) = match self.write_to_active_at(b_id, key, &[], AWAN_FLAG_TOMBSTONE, 0, 0, Some(tomb_ts)) {
            Ok((vm, loc, g)) => {
                self.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
                self.tombstone_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
                let l = self
                    .make_durable(&vm, loc.seq)
                    .await
                    .err()
                    .map(|e| format!("could not sync the delete: {:?}", e));
                (g, l)
            }
            Err(VolumeError::AfterRecord { loc, cause, .. }) => {
                self.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
                self.tombstone_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
                (
                    global_offset(loc.volume_id, loc.offset),
                    Some(format!("could not record the delete: {}", cause)),
                )
            }
            Err(e) => return Err(DeleteError::Internal(format!("could not record the delete: {:?}", e))),
        };
        self.mvcc.tombstone(b_id, key);
        let old_info = {
            let mut engine = self.engine.write();
            let info = engine
                .get_object(b_id, key.as_bytes())
                .ok()
                .map(|m| (m.vol_offset, m.size, unpack_flags(m.bucket_flags), m.created_at));
            let prev_state = match info {
                Some((off, _, old_flags, created_ms)) => crate::fsm::KeyState::Unversioned(crate::fsm::KeyObject {
                    version_id: 0,
                    created_at_ns: created_ms.saturating_mul(1_000_000),
                    vol_offset: off,
                    flags: old_flags,
                    updates: Vec::new(),
                }),
                None => crate::fsm::KeyState::Absent,
            };
            let deleted = match crate::fsm::key_step(
                prev_state,
                crate::fsm::KeyRecord::Tombstone {
                    versioned: false,
                    is_null: false,
                    created_at_ns: tomb_ts,
                    vol_offset: tomb_g,
                },
            ) {
                Ok(crate::fsm::KeyState::Absent) | Err(_) => engine.delete_object(b_id, key.as_bytes()).is_ok(),
                Ok(_) => engine.delete_object(b_id, key.as_bytes()).is_ok(),
            };
            if deleted && late.is_none() && self.events.has_subscribers() {
                let bname = engine.buckets.get(&b_id).map(|b| b.name.as_str()).unwrap_or("");
                self.emit_commit_event_for(
                    b_id,
                    bname,
                    tomb_ts,
                    key,
                    None,
                    crate::events::CommitKind::Delete,
                    info.map_or(0, |x| x.1),
                    "",
                );
            }
            info
        };
        let retired_updates = self.attr_updates.remove_target(b_id, key, 0, Some(&self.payload_cache));
        self.add_dead_attr_update_bytes(&retired_updates);
        let expired_size = old_info.map_or(0, |x| x.1);
        if let Some((off, sz, old_flags, _)) = old_info {
            if off > 0 {
                self.payload_cache.remove(b_id, off);
                let dead_bytes = self.needle_total_len(off, old_flags, key.len(), sz);
                self.tombstone_bytes.fetch_add(dead_bytes, Ordering::Relaxed);
            }
        }
        match late {
            Some(e) => Err(DeleteError::Internal(e)),
            None => Ok(expired_size),
        }
    }

    /// Permanently removes one version (`FLAG_VERSION_DELETE` tombstone + `make_durable` + index remove).
    /// Caller holds `lock_key` and has already checked lock, lifecycle/precondition rules.
    pub(crate) async fn remove_version_commit_locked(
        &self,
        b_id: u32,
        key: &str,
        target: crate::versions::Version,
        uncovers_object: bool,
    ) -> Result<(), DeleteError> {
        let id = target.id();
        if uncovers_object {
            if let Err(code) = self.engine.read().has_room_for(b_id, key.as_bytes()) {
                return Err(DeleteError::Internal(format!("index error {} before deleting a version", code)));
            }
        }
        let flags = AWAN_FLAG_TOMBSTONE | crate::meta_index::FLAG_VERSION_DELETE;
        // A record whole in the volume is replayed by recovery: the version goes here too, and
        // the request still answers an error (#184, owner decision 4B).
        let late = match self.write_to_active(b_id, key, &id.to_le_bytes(), flags, 0, 0) {
            Ok((vm, loc, _)) => {
                self.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
                self.tombstone_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
                self.make_durable(&vm, loc.seq)
                    .await
                    .err()
                    .map(|e| format!("could not sync the delete: {:?}", e))
            }
            Err(VolumeError::AfterRecord { loc, cause, .. }) => {
                self.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
                self.tombstone_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
                Some(format!("could not record the delete: {}", cause))
            }
            Err(e) => return Err(DeleteError::Internal(format!("could not record the delete: {:?}", e))),
        };
        let ev_spec = VersionCommitEventSpec {
            time: self.clock.now_ns(),
            version: Some(target.id_string()),
            kind: crate::events::CommitKind::VersionDelete,
            size: target.size,
            etag: String::new(),
            acked: late.is_none(),
        };
        self.record_version_commit(b_id, key, crate::versions::Event::Remove(id), None, Some(ev_spec))
            .map_err(|c| DeleteError::Internal(format!("index error {} after deleting a version", c)))?;
        match late {
            Some(e) => Err(DeleteError::Internal(e)),
            None => Ok(()),
        }
    }

    /// `DELETE ?versionId=`: removes one version for good (#31). Returns it, or `None` when the
    /// key has no such version.
    pub async fn delete_version(&self, b_id: u32, key: &str, id: u64) -> Result<Option<crate::versions::Version>, String> {
        let conds = crate::precondition::DeleteConditions::default();
        self.delete_version_conditional(b_id, key, id, &conds, false)
            .await
            .map_err(|e| match e {
                DeleteError::PreconditionFailed(_) => "PreconditionFailed".to_string(),
                DeleteError::AccessDenied => "AccessDenied".to_string(),
                DeleteError::Internal(s) => s,
            })
    }

    pub async fn delete_version_conditional(
        &self,
        b_id: u32,
        key: &str,
        id: u64,
        conds: &crate::precondition::DeleteConditions,
        bypass_governance: bool,
    ) -> Result<Option<crate::versions::Version>, DeleteError> {
        let _key_guard = self.lock_key(b_id, key).await;
        let before = self.engine.read().get_object(b_id, key.as_bytes()).ok().map(|m| null_version_of(&m));
        let (target, uncovers_object, needs_start) = {
            let vs = self.versions.read();
            if !vs.has(b_id, key) {
                // The object of a key with no history is its null version.
                if id != 0 || before.is_none() {
                    return Ok(None);
                }
                (before, false, true)
            } else {
                let target = vs.find(b_id, key, id);
                let uncovers = vs
                    .versions(b_id, key)
                    .and_then(|l| l.iter().find(|x| x.id() != id))
                    .is_some_and(|next| !next.marker);
                (target, uncovers, false)
            }
        };
        let Some(target) = target else {
            return Ok(None);
        };
        if !target.marker {
            let lock = self.version_lock_for(b_id, key, &target)?;
            let now_sec = self.clock.now_ns() / 1_000_000_000;
            if lock.may_delete(now_sec, bypass_governance).is_err() {
                return Err(DeleteError::AccessDenied);
            }
        }
        if !conds.is_empty() {
            let (del_target, facts_opt) = if target.marker {
                (
                    crate::precondition::DeleteTarget::DeleteMarker {
                        last_modified_secs: crate::ops::object::last_modified_secs_from_ns(target.modified),
                    },
                    None,
                )
            } else {
                let etag = crate::ops::object::version_etag(&target);
                let lm = crate::ops::object::last_modified_secs_from_ns(target.modified);
                (
                    crate::precondition::DeleteTarget::Object {
                        etag: etag.clone(),
                        last_modified_secs: lm,
                        size: target.size,
                    },
                    Some(crate::precondition::ObjectFacts { etag, last_modified_secs: lm }),
                )
            };
            if crate::precondition::delete_precondition(conds, &del_target)
                == crate::precondition::PreconditionOutcome::PreconditionFailed
            {
                return Err(DeleteError::PreconditionFailed(facts_opt));
            }
        }
        if needs_start {
            self.versions.write().start(b_id, key, before);
        }
        self.remove_version_commit_locked(b_id, key, target, uncovers_object).await?;
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
        let now = self.clock.now_ns();
        let max_u = if extra & crate::meta_index::FLAG_NULL_ID != 0 {
            self.attr_updates.max_ts(b_id, key, 0).unwrap_or(0)
        } else {
            0
        };
        Some((
            extra,
            self.versions
                .read()
                .next_ts(b_id, key, now)
                .max(max_u.saturating_add(1)),
        ))
    }

    /// Records a durable version event: updates the history, drops the needles it made
    /// unreachable, and points the index row at the current version (#31).
    pub fn record_version(&self, b_id: u32, key: &str, ev: crate::versions::Event) -> Result<(), i32> {
        self.record_version_commit(b_id, key, ev, None, None)
    }

    /// Variant of `record_version` that checks capacity before mutating `versions` and
    /// consumes an upfront `CapacityReservation` atomically inside `engine.write()`.
    pub fn record_version_with_reservation(
        &self,
        b_id: u32,
        key: &str,
        ev: crate::versions::Event,
        reservation: Option<CapacityReservation>,
    ) -> Result<(), i32> {
        self.record_version_commit(b_id, key, ev, reservation, None)
    }

    /// Single versioned index commit path: mutates `versions` and `engine` under lock and,
    /// when `event` has `acked = true`, emits one `CommitEvent` before releasing locks (#159).
    pub(crate) fn record_version_commit(
        &self,
        b_id: u32,
        key: &str,
        ev: crate::versions::Event,
        mut reservation: Option<CapacityReservation>,
        event: Option<VersionCommitEventSpec>,
    ) -> Result<(), i32> {
        let mut restore_multipart = None;
        let dead = {
            let mut engine = self.engine.write();
            let before = engine.get_object(b_id, key.as_bytes()).ok().map(|m| null_version_of(&m));
            let mut vs = self.versions.write();
            let needs_put_row = match ev {
                crate::versions::Event::Put(v) => {
                    if let Some(cur) = vs.current(b_id, key) {
                        if cur.ts > v.ts { !cur.marker } else { !v.marker }
                    } else {
                        !v.marker
                    }
                }
                crate::versions::Event::Remove(id) => {
                    if let Some(list) = vs.versions(b_id, key) {
                        list.iter().find(|x| x.id() != id).is_some_and(|next| !next.marker)
                    } else {
                        id != 0 && before.is_some()
                    }
                }
            };
            if needs_put_row {
                let check = match reservation.as_ref() {
                    Some(r) if r.is_reserved_for(b_id, key.as_bytes()) => {
                        engine.has_room_with_reserved(b_id, key.as_bytes())
                    }
                    _ => engine.has_room_for(b_id, key.as_bytes()),
                };
                if let Err(code) = check {
                    return Err(code);
                }
            }
            vs.start(b_id, key, before);
            let dead = vs.apply(b_id, key, ev);
            let current = vs.current(b_id, key);
            vs.tidy(b_id, key);
            drop(vs);
            if let Some(ref mut r) = reservation {
                r.release_in(&mut engine);
            }
            match current {
                Some(v) if !v.marker => {
                    engine.put_object_at(
                        b_id,
                        key.as_bytes(),
                        v.vol_offset,
                        v.size,
                        v.etag_hi,
                        v.etag_lo,
                        v.part_count,
                        v.flags,
                        v.ts / 1_000_000,
                    )?;
                    if v.flags & AWAN_FLAG_MULTIPART != 0 {
                        restore_multipart = Some(v.vol_offset);
                    }
                }
                _ => {
                    let _ = engine.delete_object(b_id, key.as_bytes());
                }
            }
            if let Some(spec) = event {
                if spec.acked && self.events.has_subscribers() {
                    let bname = engine.buckets.get(&b_id).map(|b| b.name.as_str()).unwrap_or("");
                    self.emit_commit_event_for(
                        b_id,
                        bname,
                        spec.time,
                        key,
                        spec.version,
                        spec.kind,
                        spec.size,
                        &spec.etag,
                    );
                }
            }
            dead
        };
        if let Some(vol_offset) = restore_multipart {
            if let Some(ext) = self.manifest_extents(b_id, vol_offset) {
                self.multipart.restore_completed(b_id, key, ext);
            }
        }
        for v in dead {
            let retired = self.attr_updates.remove_target(b_id, key, v.id(), Some(&self.payload_cache));
            self.add_dead_attr_update_bytes(&retired);
            if v.vol_offset > 0 {
                self.payload_cache.remove(b_id, v.vol_offset);
                let dead_bytes = self.needle_total_len(v.vol_offset, v.flags, key.len(), v.size);
                self.tombstone_bytes.fetch_add(dead_bytes, Ordering::Relaxed);
            }
        }
        if let crate::versions::Event::Remove(id) = ev {
            let retired = self.attr_updates.remove_target(b_id, key, id, Some(&self.payload_cache));
            self.add_dead_attr_update_bytes(&retired);
        }
        Ok(())
    }

    /// The part list in the manifest needle at `vol_offset`.
    pub fn manifest_extents(&self, b_id: u32, vol_offset: u64) -> Option<Vec<PartExtent>> {
        let bytes = self.read_stored(b_id, vol_offset).ok()?;
        decode_extents_manifest(&self.multipart.staging_dir, &bytes).ok().map(|(_, e)| e)
    }

    /// Reads one multipart part without creating an mmap in `staged_mmaps` (#155).
    pub fn read_part_unmapped(&self, b_id: u32, key: &str, ext: &PartExtent) -> Result<Bytes, VolumeError> {
        if let Some(ref path) = ext.staged_path {
            let file = std::fs::File::open(path)?;
            let mut out = vec![0u8; ext.part_size as usize];
            if !out.is_empty() {
                self.work.disk_read();
                crate::direct_io::read_exact_at(&file, 0, &mut out)?;
            }
            return Ok(Bytes::from(out));
        }
        if ext.vol_offset == 0 {
            return Err(VolumeError::CorruptIndex);
        }
        let (vm, local) = self.volume_for(ext.vol_offset)?;
        match vm.read_needle_unmapped_verified(b_id, local) {
            Ok(bytes) => {
                if bytes.len() as u64 != ext.part_size {
                    return Err(VolumeError::CorruptIndex);
                }
                Ok(bytes)
            }
            Err(e) => {
                self.record_read_corruption(ext.vol_offset, key, &e);
                Err(e)
            }
        }
    }

    /// The bytes of one version (#31). Large multipart/single objects read without mapping (#155).
    pub fn read_version_bytes(&self, b_id: u32, key: &str, v: &crate::versions::Version) -> Result<Bytes, VolumeError> {
        if v.flags & AWAN_FLAG_MULTIPART == 0 {
            return self.read_object_bytes(b_id, key, v.vol_offset, v.size, v.flags);
        }
        let extents = self.manifest_extents(b_id, v.vol_offset).ok_or(VolumeError::CorruptIndex)?;
        let large = v.size > VERIFY_WHILE_SENDING_MIN;
        let mut out = Vec::with_capacity(v.size as usize);
        for ext in extents {
            let chunk = if large || ext.part_size > VERIFY_WHILE_SENDING_MIN {
                self.read_part_unmapped(b_id, key, &ext)?
            } else {
                self.part_bytes(b_id, &ext)?
            };
            let n = (ext.part_size as usize).min(chunk.len());
            out.extend_from_slice(&chunk[..n]);
        }
        if out.len() as u64 != v.size {
            return Err(VolumeError::CorruptIndex);
        }
        Ok(Bytes::from(out))
    }

    /// Enables or disables unbounded per-bucket usage Prometheus gauges (#159).
    pub fn set_per_bucket_metrics(&self, enabled: bool) {
        self.per_bucket_metrics.store(enabled, Ordering::Relaxed);
    }

    /// Returns current and all-version usage counters for bucket `b_id` (#159).
    pub fn bucket_usage(&self, b_id: u32) -> Option<BucketUsageReport> {
        let engine = self.engine.read();
        let current = engine.bucket_usage(b_id)?;
        let noncurrent = self.versions.read().noncurrent_usage(b_id);
        Some(BucketUsageReport {
            current,
            all_versions: crate::meta_index::BucketUsage {
                objects: current.objects + noncurrent.objects,
                bytes: current.bytes + noncurrent.bytes,
            },
        })
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn emit_commit_event_for(
        &self,
        b_id: u32,
        bucket: &str,
        time: u64,
        key: &str,
        version: Option<String>,
        kind: crate::events::CommitKind,
        size: u64,
        etag: &str,
    ) {
        if !self.events.has_subscribers() {
            return;
        }
        let principal = crate::request_log::current_principal().unwrap_or_default();
        let request_id = crate::request_log::current_request_id().unwrap_or_default();
        self.events.emit_with(b_id, |seq| crate::events::CommitEvent {
            seq,
            time,
            bucket: bucket.to_string(),
            key: key.to_string(),
            version,
            kind,
            size,
            etag: etag.to_string(),
            principal,
            request_id,
        });
    }

    /// Emits a commit event for a durable bucket-level configuration change (#159).
    pub(crate) fn emit_bucket_config_event(
        &self,
        b_id: u32,
        bucket: &str,
        key: &str,
        kind: crate::events::CommitKind,
        size: u64,
    ) {
        if !self.events.has_subscribers() {
            return;
        }
        let now = self.clock.now_ns();
        self.emit_commit_event_for(b_id, bucket, now, key, None, kind, size, "");
    }

    /// Single unversioned object commit point: updates the index under `engine.write()`,
    /// emits one commit event when `acked` is true, and retires superseded state (#154, #159).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn commit_unversioned_put(
        &self,
        b_id: u32,
        bucket: &str,
        key: &str,
        vol_offset: u64,
        size: u64,
        etag_hi: u64,
        etag_lo: u64,
        part_count: u16,
        flags: u16,
        created_ns: u64,
        old_info: Option<(u64, u64, u16)>,
        kind: crate::events::CommitKind,
        etag: &str,
        acked: bool,
        reservation: CapacityReservation,
    ) -> Result<(), i32> {
        {
            let mut engine = self.engine.write();
            let mut res = reservation;
            res.release_in(&mut engine);
            engine.put_object_at(
                b_id,
                key.as_bytes(),
                vol_offset,
                size,
                etag_hi,
                etag_lo,
                part_count,
                flags,
                created_ns / 1_000_000,
            )?;
            if acked && self.events.has_subscribers() {
                self.emit_commit_event_for(b_id, bucket, created_ns, key, None, kind, size, etag);
            }
        }
        self.retire_unversioned_overwrite(b_id, key, old_info, vol_offset, created_ns, flags);
        Ok(())
    }

    /// Returns the bucket's Object Lock configuration, if enabled (#120).
    pub fn bucket_object_lock(&self, b_id: u32) -> Option<crate::object_lock::LockConfiguration> {
        self.bucket_config_reader
            .load_bucket(b_id)
            .and_then(|cfg| cfg.object_lock())
    }

    /// Returns the bucket's parsed Lifecycle configuration, if set (#118).
    pub fn bucket_lifecycle(&self, b_id: u32) -> Option<Arc<crate::lifecycle_config::LifecycleConfig>> {
        self.bucket_config_reader
            .load_bucket(b_id)
            .and_then(|cfg| cfg.lifecycle())
    }

    /// Loads the current `Lock` (retention + legal hold) of a version, returning an error if attributes are corrupt.
    pub fn version_lock_for(
        &self,
        b_id: u32,
        key: &str,
        v: &crate::versions::Version,
    ) -> Result<crate::fsm::Lock, DeleteError> {
        if v.marker {
            return Ok(crate::fsm::Lock::default());
        }
        let view = self
            .object_view_for(b_id, key, v.id(), v.vol_offset, v.flags)
            .map_err(|e| DeleteError::Internal(format!("could not read version lock state: {:?}", e)))?;
        view.attrs
            .lock_state()
            .map_err(|e| DeleteError::Internal(format!("corrupt lock attribute: {:?}", e)))
    }

    /// Returns whether lifecycle expiration may remove the current (unversioned or head) object of `key`.
    pub fn may_lifecycle_delete_current(&self, b_id: u32, key: &str, now_sec: u64) -> bool {
        let head = {
            let vs = self.versions.read();
            if vs.has(b_id, key) {
                vs.current(b_id, key)
            } else {
                drop(vs);
                self.engine
                    .read()
                    .get_object(b_id, key.as_bytes())
                    .ok()
                    .map(|m| null_version_of(&m))
            }
        };
        let Some(v) = head else {
            return true;
        };
        if v.marker {
            return true;
        }
        match self.version_lock_for(b_id, key, &v) {
            Ok(lock) => lock.may_delete(now_sec, false).is_ok(),
            Err(_) => false,
        }
    }

    /// Records a bucket's versioning status in the manifest and in memory (#31).
    pub fn set_versioning(&self, b_id: u32, status: crate::versions::Versioning) -> std::io::Result<()> {
        let mut bc = self.bucket_configs.lock();
        bc.apply_versioning(b_id, status)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        self.versioning.write().insert(b_id, status);
        if self.events.has_subscribers() {
            let bucket = bc.load_bucket(b_id).map(|c| c.name.clone()).unwrap_or_default();
            self.emit_bucket_config_event(
                b_id,
                &bucket,
                "versioning",
                crate::events::CommitKind::BucketVersioning,
                status.as_str().len() as u64,
            );
        }
        Ok(())
    }

    /// Atomically checks Object Lock + current versioning, steps `fsm::versioning_step`, and persists (#120).
    pub fn try_set_versioning(
        &self,
        b_id: u32,
        bucket: &str,
        requested: crate::versions::Versioning,
    ) -> Result<crate::versions::Versioning, Result<crate::fsm::Refusal, std::io::Error>> {
        let mut bc = self.bucket_configs.lock();
        let has_object_lock = bc
            .load_bucket(b_id)
            .and_then(|c| c.object_lock())
            .is_some();
        let mut ver = self.versioning.write();
        let current = ver.get(&b_id).copied().unwrap_or_default();
        let next = crate::fsm::versioning_step(current, requested, has_object_lock).map_err(Ok)?;
        bc.apply_versioning(b_id, next)
            .map_err(|e| Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;
        ver.insert(b_id, next);
        drop(ver);
        drop(bc);
        self.emit_bucket_config_event(
            b_id,
            bucket,
            "versioning",
            crate::events::CommitKind::BucketVersioning,
            next.as_str().len() as u64,
        );
        Ok(next)
    }

    /// Atomically verifies `Versioning::Enabled` (if lock is not already enabled) and stores `ObjectLock` config (#120).
    pub fn enable_object_lock_config(
        &self,
        b_id: u32,
        bucket: &str,
        body: &[u8],
    ) -> Result<(), Result<crate::fsm::Refusal, crate::bucket_config::ConfigError>> {
        let mut bc = self.bucket_configs.lock();
        let already_enabled = bc
            .load_bucket(b_id)
            .and_then(|c| c.object_lock())
            .is_some();
        if !already_enabled {
            let ver = self.versioning.read().get(&b_id).copied().unwrap_or_default();
            if ver != crate::versions::Versioning::Enabled {
                return Err(Ok(crate::fsm::Refusal {
                    status: 409,
                    code: "InvalidBucketState",
                    why: "Versioning must be 'Enabled' on the bucket to apply a Object Lock configuration",
                }));
            }
        }
        bc.put_config(b_id, crate::bucket_config::ConfigKind::ObjectLock, body)
            .map_err(Err)?;
        drop(bc);
        self.emit_bucket_config_event(
            b_id,
            bucket,
            crate::bucket_config::ConfigKind::ObjectLock.as_str(),
            crate::events::CommitKind::BucketConfig,
            body.len() as u64,
        );
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

    /// (dead bytes, used bytes) of a volume: a needle is live when an index row or live
    /// `0x200` attribute update points at it.
    /// Reads every needle header of the volume, so it costs about one read per needle.
    pub fn volume_usage(&self, vol_id: u32) -> Result<(u64, u64), VolumeError> {
        let vm = self.volumes.read().get(&vol_id).cloned().ok_or(VolumeError::CorruptIndex)?;
        let mut needles = Vec::new();
        vm.scan_needles(|n| {
            needles.push((
                n.bucket_id as u32,
                n.key,
                n.offset,
                n.total_len,
                n.flags,
                n.payload_crc32c,
            ))
        })?;
        let engine = self.engine.read();
        let versions = self.versions.read();
        let mut dead = 0;
        for (b_id, key, offset, total, flags, crc) in needles {
            let g = global_offset(vol_id, offset);
            let live = if flags & crate::meta_index::FLAG_ATTR_UPDATE != 0 {
                match vm
                    .read_needle_bytes(vol_id, offset, crc)
                    .ok()
                    .and_then(|b| crate::attrs::decode_attr_update(&b).ok())
                {
                    Some((target_vid, _)) => !self.attr_updates.live_groups_at(b_id, &key, target_vid, g).is_empty(),
                    None => false,
                }
            } else {
                engine.get_object(b_id, key.as_bytes()).map(|m| m.vol_offset == g).unwrap_or(false)
                    || versions.holds_offset(b_id, &key, g)
            };
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
    /// Runs `auto_compact_once` every `every` on the background scheduler. The binary starts
    /// this unless `--auto-compaction off`; the library never starts it by itself.
    pub fn start_auto_compaction(self: &Arc<Self>, every: std::time::Duration, threshold: f64, min_bytes: u64) {
        let weak = Arc::downgrade(self);
        let rt = tokio::runtime::Handle::current();
        self.scheduler.register("auto_compaction", every, move |_budget| {
            let Some(state) = weak.upgrade() else { return Ok(false) };
            match rt.block_on(state.auto_compact_once(threshold, min_bytes)) {
                Ok(Some(r)) if r.busy => {
                    eprintln!("auto-compaction: the oldest volume still has a write in flight; next pass");
                    Ok(true)
                }
                Ok(Some(r)) => {
                    eprintln!(
                        "auto-compaction: moved {} live needles, reclaimed {} dead ones",
                        r.live_needles_copied, r.dead_needles_reclaimed
                    );
                    Ok(false)
                }
                Ok(None) => Ok(false),
                Err(e) => Err(format!("{:?}", e)),
            }
        });
    }

    /// Runs incomplete-multipart cleanup on the background scheduler.
    pub fn start_multipart_cleanup(self: &Arc<Self>, every: std::time::Duration, max_age_ms: u64) {
        let weak = Arc::downgrade(self);
        self.scheduler.register("multipart_cleanup", every, move |budget| {
            let Some(state) = weak.upgrade() else { return Ok(false) };
            let now_ms = state.clock.now_ns() / 1_000_000;
            let (_removed, more) = state.multipart.cleanup_expired(now_ms, max_age_ms, budget);
            Ok(more)
        });
    }

    /// Registers the periodic bucket lifecycle expiration sweep on the background scheduler (#118).
    pub fn start_lifecycle_sweep(self: &Arc<Self>, every: std::time::Duration) {
        let weak = Arc::downgrade(self);
        let rt = tokio::runtime::Handle::current();
        self.scheduler.register("lifecycle", every, move |budget| {
            let Some(state) = weak.upgrade() else { return Ok(false) };
            let now_ms = state.clock.now_ns() / 1_000_000;
            Ok(rt.block_on(crate::lifecycle::run_lifecycle_sweep_async(
                &state,
                now_ms,
                budget,
            )))
        });
    }

    /// Runs one complete background scrub pass across all open volumes (#155).
    pub fn scrub_pass(&self) -> crate::scrub::ScrubReport {
        let vols: Vec<Arc<VolumeManager>> = self.volumes.read().values().cloned().collect();
        let rate = self.scrub.rate_mib_per_sec();
        let stop = self.scheduler.shutdown_flag();
        self.scrub.run_pass(&vols, rate, &stop)
    }

    /// Registers the periodic background needle scrub on the scheduler (#155).
    /// `mib_per_sec == 0` disables background scrubbing.
    pub fn start_scrub(self: &Arc<Self>, every: std::time::Duration, mib_per_sec: u64) {
        self.scrub.set_rate_mib_per_sec(mib_per_sec);
        if mib_per_sec == 0 {
            return;
        }
        let weak = Arc::downgrade(self);
        self.scheduler.register("scrub", every, move |_budget| {
            let Some(state) = weak.upgrade() else { return Ok(false) };
            let _report = state.scrub_pass();
            Ok(false)
        });
    }

    /// Moves the live needles of the oldest sealed volume to the active volume and deletes
    /// the old volume (#14). Safe while requests run:
    /// - each live needle is copied under its key lock, and the index row moves only if it
    ///   still points at the old needle, so a concurrent PUT or DELETE wins and is never
    ///   followed in the log by an older copy;
    /// - the volume is sealed and listed only once no write is still landing in it (#167), so
    ///   every needle that will ever be in it is listed;
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
        // A write that reserved an offset in this volume and has not written its header would
        // not be listed, so not copied, and would land in the file removed below (#167;
        // verification/tla, compact_no_wait.cfg). Seal it first, so no new write lands in it,
        // then list it only when no write is in flight. A busy volume is left for the next pass.
        old.seal();
        if old.writers_in_flight() > 0 {
            return Ok(CompactionReport { busy: true, ..CompactionReport::default() });
        }
        let mut needles = Vec::new();
        old.scan_needles(|n| {
            needles.push((
                n.bucket_id as u32,
                n.key,
                n.offset,
                n.payload_size,
                n.etag_hi,
                n.etag_lo,
                n.flags,
                n.payload_crc32c,
                n.created_at_ns,
            ))
        })?;

        let mut report = CompactionReport::default();
        let mut written: BTreeMap<u32, Arc<VolumeManager>> = BTreeMap::new();
        let mut throttle = crate::throttle::Throttle::new(self.compaction_rate.load(Ordering::Relaxed));
        for (b_id, key, local, size, etag_hi, etag_lo, flags, crc, created_at) in needles {
            let g_old = global_offset(old_id, local);
            // Compacts live 0x200 attribute-update needles, keeping only the latest live group
            // entries whose target version still exists and dropping superseded/deleted ones (#203).
            if flags & crate::meta_index::FLAG_ATTR_UPDATE != 0 {
                let raw_bytes = old.read_needle_bytes(old_id, local, crc)?;
                let Ok((target_vid, upd_attrs)) = crate::attrs::decode_attr_update(&raw_bytes) else {
                    report.dead_needles_reclaimed += 1;
                    continue;
                };
                let _guard = self.lock_key(b_id, &key).await;
                let target_alive = {
                    let vs = self.versions.read();
                    if vs.has(b_id, &key) {
                        vs.find(b_id, &key, target_vid).is_some_and(|v| !v.marker)
                    } else {
                        target_vid == 0 && self.engine.read().get_object(b_id, key.as_bytes()).is_ok()
                    }
                };
                let live_groups = if target_alive {
                    self.attr_updates.live_groups_at(b_id, &key, target_vid, g_old)
                } else {
                    Vec::new()
                };
                if live_groups.is_empty() {
                    if !target_alive {
                        let _ = self.attr_updates.remove_target(b_id, &key, target_vid, Some(&self.payload_cache));
                    }
                    report.dead_needles_reclaimed += 1;
                    continue;
                }
                let mut filtered = crate::attrs::Attrs::new();
                for (tag, val) in upd_attrs.iter() {
                    if live_groups.contains(&crate::attrs::AttrGroup::from_tag(tag)) {
                        filtered.set(tag, val);
                    }
                }
                let copy_payload = crate::attrs::encode_attr_update(target_vid, &filtered)
                    .map_err(|_| VolumeError::CorruptIndex)?;
                let copied = copy_payload.len() as u64;
                let (vm, loc, g_new) = self.write_to_active_with(
                    b_id,
                    &key,
                    &[],
                    &copy_payload,
                    flags,
                    0,
                    0,
                    Some(created_at),
                )?;
                self.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
                written.insert(vm.id(), Arc::clone(&vm));
                self.attr_updates.move_offset(b_id, &key, target_vid, g_old, g_new, loc.total_len);
                report.live_needles_copied += 1;
                report.new_locations.insert(key, loc);
                drop(_guard);
                tokio::task::yield_now().await;
                throttle.spend(copied).await;
                continue;
            }
            // An unversioned tombstone only matters until the needles it covers are gone, and
            // those are older, so already compacted. A delete marker is a version (#31): whether
            // a history holds it is decided under the key lock below, like any needle, because
            // its DELETE writes it first and records it later, under that lock (#182 review).
            if flags & AWAN_FLAG_TOMBSTONE != 0 && flags & crate::meta_index::FLAG_VERSIONED == 0 {
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
            // The attributes block moves with the needle, unknown tags included (#150).
            let attrs = if flags & crate::meta_index::FLAG_ATTRS != 0 {
                old.read_attrs(local)?.encode().map_err(|_| VolumeError::CorruptIndex)?
            } else {
                Vec::new()
            };
            let (vm, loc, g_new) = self.write_to_active_with(b_id, &key, &attrs, &bytes, flags, etag_hi, etag_lo, Some(created_at))?;
            self.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
            written.insert(vm.id(), Arc::clone(&vm));
            self.versions.write().move_offset(b_id, &key, g_old, g_new);
            if current {
                if let Err(code) = self.engine.write().update_object_offset(b_id, key.as_bytes(), g_new) {
                    return Err(VolumeError::Io(std::io::Error::new(std::io::ErrorKind::Other, format!("index error {} while moving {}", code, key))));
                }
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
        let _ = self.total_volume_bytes.try_update(Ordering::Relaxed, Ordering::Relaxed, |t| Some(t.saturating_sub(reclaimed)));
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

    /// Records a read-path header or payload CRC32C failure in metrics, logs, and scrub (#155).
    pub fn record_read_corruption(&self, vol_offset: u64, key: &str, err: &VolumeError) {
        let is_checksum = matches!(
            err,
            VolumeError::CrcMismatch { .. }
                | VolumeError::HeaderCrcMismatch { .. }
                | VolumeError::InvalidMagic { .. }
        );
        if !is_checksum {
            return;
        }
        let (vol_id, local) = split_offset(vol_offset);
        self.read_checksum_failures.fetch_add(1, Ordering::SeqCst);
        tracing::error!(
            object = %key,
            volume_id = vol_id,
            offset = local,
            error = ?err,
            "checksum mismatch on read"
        );
        self.scrub.mark_bad(vol_id, local, key, format!("{:?}", err));
    }

    /// The bytes of an object, single or multipart, verified.
    pub fn read_object_bytes(&self, b_id: u32, key: &str, vol_offset: u64, size: u64, flags: u16) -> Result<Bytes, VolumeError> {
        if flags & crate::meta_index::FLAG_COMPRESSED != 0 {
            return self.load_compressed(b_id, vol_offset, size);
        }
        if flags & AWAN_FLAG_MULTIPART == 0 {
            if size > VERIFY_WHILE_SENDING_MIN {
                if vol_offset == 0 {
                    return Err(VolumeError::CorruptIndex);
                }
                let (vm, local) = self.volume_for(vol_offset)?;
                return match vm.read_needle_unmapped_verified(b_id, local) {
                    Ok(b) if b.len() as u64 == size => Ok(b),
                    Ok(_) => Err(VolumeError::CorruptIndex),
                    Err(e) => {
                        self.record_read_corruption(vol_offset, key, &e);
                        Err(e)
                    }
                };
            }
            return self.load_payload_for(b_id, key, vol_offset, size as usize);
        }
        let extents = self.multipart.get_completed_extents(b_id, key).ok_or(VolumeError::CorruptIndex)?;
        let large = size > VERIFY_WHILE_SENDING_MIN;
        let mut out = Vec::with_capacity(size as usize);
        for ext in extents {
            let chunk = if large || ext.part_size > VERIFY_WHILE_SENDING_MIN {
                self.read_part_unmapped(b_id, key, &ext)?
            } else {
                self.part_bytes(b_id, &ext)?
            };
            let n = (ext.part_size as usize).min(chunk.len());
            self.work.payload_copy(n as u64);
            out.extend_from_slice(&chunk[..n]);
        }
        if out.len() as u64 != size {
            return Err(VolumeError::CorruptIndex);
        }
        Ok(Bytes::from(out))
    }

    /// Reads an inclusive-exclusive byte range `[start, start + len)` of a single or multipart
    /// object using positioned reads (never mapping large parts or volumes, #155).
    #[allow(clippy::too_many_arguments)]
    pub fn read_object_range_unmapped(
        &self,
        b_id: u32,
        key: &str,
        vol_offset: u64,
        size: u64,
        flags: u16,
        start: u64,
        len: u64,
    ) -> Result<Bytes, VolumeError> {
        if start.checked_add(len).is_none_or(|end| end > size) {
            return Err(VolumeError::CorruptIndex);
        }
        if flags & crate::meta_index::FLAG_COMPRESSED != 0 {
            let full = self.load_compressed(b_id, vol_offset, size)?;
            return Ok(full.slice(start as usize..(start + len) as usize));
        }
        if flags & AWAN_FLAG_MULTIPART == 0 {
            if vol_offset == 0 {
                return Err(VolumeError::CorruptIndex);
            }
            let (vm, local) = self.volume_for(vol_offset)?;
            return match vm.read_needle_range_unmapped(b_id, local, start, len) {
                Ok(b) => Ok(b),
                Err(e) => {
                    self.record_read_corruption(vol_offset, key, &e);
                    Err(e)
                }
            };
        }
        let extents = self
            .manifest_extents(b_id, vol_offset)
            .or_else(|| self.multipart.get_completed_extents(b_id, key))
            .ok_or(VolumeError::CorruptIndex)?;
        let end = start + len;
        let mut out = Vec::with_capacity(len as usize);
        for ext in &extents {
            let p_start = ext.cumulative_end - ext.part_size;
            let (lo, hi) = (start.max(p_start), end.min(ext.cumulative_end));
            if lo >= hi {
                continue;
            }
            let in_part = lo - p_start;
            let take = (hi - lo) as usize;
            let mut buf = vec![0u8; take];
            match &ext.staged_path {
                Some(path) => {
                    let file = std::fs::File::open(path)?;
                    self.work.disk_read();
                    crate::direct_io::read_exact_at(&file, in_part, &mut buf)?;
                }
                None => {
                    let (vm, local) = self.volume_for(ext.vol_offset)?;
                    let slice = match vm.read_needle_range_unmapped(b_id, local, in_part, take as u64) {
                        Ok(b) => b,
                        Err(e) => {
                            self.record_read_corruption(ext.vol_offset, key, &e);
                            return Err(e);
                        }
                    };
                    buf.copy_from_slice(&slice);
                }
            }
            out.extend_from_slice(&buf);
        }
        if out.len() as u64 != len {
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

    /// Copies a large object (> 4 MiB, single or multipart) into a new needle on the active
    /// volume using 1 MiB positioned reads and writes, without mapping parts or buffering the
    /// whole payload on the heap (#155).
    #[allow(clippy::too_many_arguments)]
    pub async fn stream_copy_object(
        self: &Arc<Self>,
        dst_b_id: u32,
        dst_key: &str,
        dst_attrs: &[u8],
        dst_flags: u16,
        src_b_id: u32,
        src_key: &str,
        src_vol_offset: u64,
        src_size: u64,
        src_flags: u16,
        etag_hi: u64,
        etag_lo: u64,
        created_at: u64,
    ) -> Result<(u64, u16), VolumeError> {
        let eff_flags = if dst_attrs.is_empty() {
            dst_flags
        } else {
            dst_flags | crate::meta_index::FLAG_ATTRS
        };
        let state = Arc::clone(self);
        let dst_key_s = dst_key.to_string();
        let src_key_s = src_key.to_string();
        let attrs_vec = dst_attrs.to_vec();
        let res = tokio::task::spawn_blocking(move || -> Result<(Arc<VolumeManager>, NeedleLocation, u64), VolumeError> {
            let mut writer = state.begin_needle_on_active(dst_b_id, &dst_key_s, &attrs_vec, src_size, eff_flags)?;
            writer.set_created_at(created_at);

            let copy_res: Result<(), VolumeError> = if src_flags & AWAN_FLAG_MULTIPART == 0 {
                let (src_vm, src_local) = state.volume_for(src_vol_offset)?;
                let (_, stored_sz, expected_crc) = match src_vm.payload_extent_with_crc(src_b_id, src_local) {
                    Ok(x) => x,
                    Err(e) => {
                        state.record_read_corruption(src_vol_offset, &src_key_s, &e);
                        return Err(e);
                    }
                };
                if stored_sz != src_size {
                    return Err(VolumeError::CorruptIndex);
                }
                if let Err(e) = src_vm.copy_payload(src_local, expected_crc, |piece| writer.write(piece)) {
                    state.record_read_corruption(src_vol_offset, &src_key_s, &e);
                    Err(e)
                } else {
                    Ok(())
                }
            } else {
                let extents = state
                    .manifest_extents(src_b_id, src_vol_offset)
                    .or_else(|| state.multipart.get_completed_extents(src_b_id, &src_key_s))
                    .ok_or(VolumeError::CorruptIndex)?;
                let mut copied = 0u64;
                let mut buf = vec![0u8; crate::volume::STREAM_CHUNK];
                let mut res = Ok(());
                for ext in &extents {
                    if let Some(ref path) = ext.staged_path {
                        let file = match std::fs::File::open(path) {
                            Ok(f) => f,
                            Err(e) => {
                                res = Err(VolumeError::Io(e));
                                break;
                            }
                        };
                        let mut pos = 0u64;
                        while pos < ext.part_size {
                            let n = (crate::volume::STREAM_CHUNK as u64).min(ext.part_size - pos) as usize;
                            state.work.disk_read();
                            if let Err(e) = crate::direct_io::read_exact_at(&file, pos, &mut buf[..n]) {
                                res = Err(VolumeError::Io(e));
                                break;
                            }
                            if let Err(e) = writer.write(&buf[..n]) {
                                res = Err(e);
                                break;
                            }
                            pos += n as u64;
                            copied += n as u64;
                        }
                        if res.is_err() {
                            break;
                        }
                    } else {
                        let (part_vm, part_local) = match state.volume_for(ext.vol_offset) {
                            Ok(x) => x,
                            Err(e) => {
                                res = Err(e);
                                break;
                            }
                        };
                        let (_, stored_sz, expected_crc) = match part_vm.payload_extent_with_crc(src_b_id, part_local) {
                            Ok(x) => x,
                            Err(e) => {
                                state.record_read_corruption(ext.vol_offset, &src_key_s, &e);
                                res = Err(e);
                                break;
                            }
                        };
                        if stored_sz != ext.part_size {
                            res = Err(VolumeError::CorruptIndex);
                            break;
                        }
                        if let Err(e) = part_vm.copy_payload(part_local, expected_crc, |piece| writer.write(piece)) {
                            state.record_read_corruption(ext.vol_offset, &src_key_s, &e);
                            res = Err(e);
                            break;
                        }
                        copied += ext.part_size;
                    }
                }
                if res.is_ok() && copied != src_size {
                    res = Err(VolumeError::CorruptIndex);
                }
                res
            };

            copy_res?;
            let vm = Arc::clone(writer.volume());
            let g = global_offset(vm.id(), writer.offset());
            match writer.finish(etag_hi, etag_lo) {
                Ok(l) => {
                    state.streams.note_append(dst_b_id, &dst_key_s, g);
                    Ok((vm, l, g))
                }
                Err(VolumeError::AfterRecord { loc, flags, cause }) => {
                    state.streams.note_append(dst_b_id, &dst_key_s, g);
                    Err(VolumeError::AfterRecord { loc, flags, cause })
                }
                Err(e) => Err(e),
            }
        })
        .await
        .map_err(|e| VolumeError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;

        let (vm, loc, g) = match res {
            Ok(x) => x,
            Err(VolumeError::AfterRecord { loc, flags, cause }) => {
                self.note_record(dst_b_id, &loc);
                return Err(VolumeError::AfterRecord { loc, flags, cause });
            }
            Err(e) => return Err(e),
        };
        if let Err(e) = self.make_durable(&vm, loc.seq).await {
            self.note_record(dst_b_id, &loc);
            return Err(VolumeError::AfterRecord {
                loc,
                flags: eff_flags,
                cause: format!("could not sync the object: {:?}", e),
            });
        }
        self.note_record(dst_b_id, &loc);
        Ok((g, eff_flags))
    }

    /// `store_payload` for a whole object: compressed when its bucket asks for it and the
    /// bytes shrink (#51). Returns the offset and the flags the needle was written with.
    #[allow(clippy::too_many_arguments)]
    pub async fn store_object(&self, b_id: u32, bucket: &str, key: &str, attrs: &[u8], data: &Bytes, flags: u16, etag_hi: u64, etag_lo: u64, created_at: Option<u64>) -> Result<(u64, u16), VolumeError> {
        let flags = if attrs.is_empty() { flags } else { flags | crate::meta_index::FLAG_ATTRS };
        let plain = flags & (AWAN_FLAG_MULTIPART | AWAN_FLAG_ZERO_BYTE | AWAN_FLAG_TOMBSTONE) == 0;
        if plain && self.compresses(bucket) {
            if let Some(enc) = crate::compress::encode(data) {
                let f = flags | crate::meta_index::FLAG_COMPRESSED;
                let off = self.store_payload_with(b_id, key, attrs, &Bytes::from(enc), f, etag_hi, etag_lo, created_at).await?;
                return Ok((off, f));
            }
        }
        Ok((self.store_payload_with(b_id, key, attrs, data, flags, etag_hi, etag_lo, created_at).await?, flags))
    }

    /// The payload bytes as stored, verified against the CRC in its needle header.
    fn read_stored(&self, b_id: u32, vol_offset: u64) -> Result<Bytes, VolumeError> {
        if vol_offset == 0 {
            return Err(VolumeError::CorruptIndex);
        }
        let (vm, local) = self.volume_for(vol_offset)?;
        match vm.read_needle_bytes_verified(b_id, local) {
            Ok(b) => Ok(b),
            Err(e) => {
                self.record_read_corruption(vol_offset, &format!("bucket:{}/offset:{}", b_id, local), &e);
                Err(e)
            }
        }
    }

    /// Bytes `start..=end` of a compressed object, decoding only the blocks they overlap.
    pub fn load_compressed_range(&self, b_id: u32, vol_offset: u64, size: u64, start: u64, end: u64) -> Result<Bytes, VolumeError> {
        let (vol_id, local_off) = crate::split_offset(vol_offset);
        if self.scrub.is_bad(vol_id, local_off) {
            self.payload_cache.remove(b_id, vol_offset);
        } else if let Some(cached) = self.payload_cache.get(b_id, vol_offset) {
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
        let (vol_id, local_off) = crate::split_offset(vol_offset);
        if self.scrub.is_bad(vol_id, local_off) {
            self.payload_cache.remove(b_id, vol_offset);
        } else if let Some(cached) = self.payload_cache.get(b_id, vol_offset) {
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
        self.store_payload_with(b_id, key, &[], data, flags, etag_hi, etag_lo, created_at).await
    }

    /// `store_payload_at` with an attributes block (#150).
    #[allow(clippy::too_many_arguments)]
    pub async fn store_payload_with(&self, b_id: u32, key: &str, attrs: &[u8], data: &Bytes, flags: u16, etag_hi: u64, etag_lo: u64, created_at: Option<u64>) -> Result<u64, VolumeError> {
        let written = tracing::info_span!("volume.write", bytes = data.len() as u64).in_scope(|| self.write_to_active_with(b_id, key, attrs, data, flags, etag_hi, etag_lo, created_at));
        let (vm, loc, offset) = match written {
            Ok(w) => w,
            Err(VolumeError::AfterRecord { loc, flags, cause }) => {
                self.note_record(b_id, &loc);
                return Err(VolumeError::AfterRecord { loc, flags, cause });
            }
            Err(e) => return Err(e),
        };
        if let Err(e) = self.make_durable(&vm, loc.seq).instrument(tracing::info_span!("volume.sync")).await {
            // The needle is whole in the volume; only its sync failed (#184).
            self.note_record(b_id, &loc);
            return Err(VolumeError::AfterRecord { loc, flags, cause: format!("could not sync the object: {:?}", e) });
        }
        self.note_record(b_id, &loc);
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

    /// For a write that failed after its needle was whole in the volume (#184): the needle's
    /// global offset and flags. Recovery will replay that needle, so the caller indexes it as
    /// on success and still answers an error (owner decision 4B, 2026-11-16).
    pub fn late_record(e: &VolumeError) -> Option<(u64, u16)> {
        match e {
            VolumeError::AfterRecord { loc, flags, .. } => Some((global_offset(loc.volume_id, loc.offset), *flags)),
            _ => None,
        }
    }

    /// Bookkeeping for a needle now in the volume: the volume bytes it uses.
    pub fn note_record(&self, _b_id: u32, loc: &NeedleLocation) {
        self.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
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
        self.write_to_active_with(b_id, key, &[], data, flags, etag_hi, etag_lo, created_at)
    }

    /// `write_to_active_at` with an attributes block (`crate::attrs`, #150).
    #[allow(clippy::too_many_arguments)]
    pub fn write_to_active_with(
        &self,
        b_id: u32,
        key: &str,
        attrs: &[u8],
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
            // object: compressed needles go to a format 2 volume (decision 20, #51), and
            // needles with attributes to a format 3 one (#150).
            if vm.format() < needed_format(flags, attrs) && attempts < 8 {
                attempts += 1;
                self.roll_volume(&vm)?;
                continue;
            }
            match vm.write_needle_with_attrs(b_id as u16, key, attrs, data, flags, etag_hi, etag_lo, created_at) {
                Ok(loc) => {
                    let g = global_offset(vm.id(), loc.offset);
                    self.streams.note_append(b_id, key, g);
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
                Err(VolumeError::AfterRecord { loc, flags, cause }) => {
                    // The needle header is whole on disk (#184, #403), so recovery will
                    // replay it at `loc.offset`: note that exact global offset.
                    self.streams.note_append(b_id, key, global_offset(loc.volume_id, loc.offset));
                    return Err(VolumeError::AfterRecord { loc, flags, cause });
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Starts a streamed needle on the active volume, rolling over when it is full (#36, #37).
    /// Writes the needle at `from` again at the end of the log, in `STREAM_CHUNK` pieces, and
    /// returns the copy's place. The first needle stays in the log as dead space: recovery
    /// replays it and then whatever came after it, the copy last (#154).
    #[allow(clippy::too_many_arguments)]
    pub fn relocate_needle(
        &self,
        b_id: u32,
        key: &str,
        from_vm: &Arc<VolumeManager>,
        from: &NeedleLocation,
        flags: u16,
        etag_hi: u64,
        etag_lo: u64,
        created_at: Option<u64>,
    ) -> Result<(Arc<VolumeManager>, NeedleLocation, u64), VolumeError> {
        // Read in chunks, not through the mapping (#185); a CRC mismatch drops the writer
        // before its header is written, so the copy leaves only a gap. The copy carries the
        // same attributes block, byte for byte (#150).
        let attrs = from_vm.read_attrs(from.offset)?.encode().map_err(|_| VolumeError::CorruptIndex)?;
        let mut writer = self.begin_needle_on_active(b_id, key, &attrs, from.payload_size, flags)?;
        if let Some(ts) = created_at {
            writer.set_created_at(ts);
        }
        from_vm.copy_payload(from.offset, from.payload_crc32c, |piece| writer.write(piece))?;
        let vm = Arc::clone(writer.volume());
        let g = global_offset(vm.id(), writer.offset());
        let (loc, late) = match writer.finish(etag_hi, etag_lo) {
            Ok(loc) => (loc, None),
            // The copy is whole in the volume and is the newest record of the key; only its
            // journal append failed (#184, the model's RelocateLateFail).
            Err(VolumeError::AfterRecord { loc, flags, cause }) => (loc, Some(VolumeError::AfterRecord { loc, flags, cause })),
            Err(e) => return Err(e),
        };
        self.streams.note_append(b_id, key, g);
        self.total_volume_bytes.fetch_add(from.total_len, Ordering::Relaxed);
        self.tombstone_bytes.fetch_add(from.total_len, Ordering::Relaxed);
        self.stream_relocations.fetch_add(1, Ordering::SeqCst);
        match late {
            Some(e) => Err(e),
            None => Ok((vm, loc, g)),
        }
    }

    pub fn begin_needle_on_active(&self, b_id: u32, key: &str, attrs: &[u8], len: u64, flags: u16) -> Result<crate::volume::NeedleWriter, VolumeError> {
        let mut attempts = 0;
        loop {
            let vm = self.volume_manager.read().clone().ok_or_else(no_volume_error)?;
            if vm.format() < needed_format(flags, attrs) && attempts < 8 {
                attempts += 1;
                self.roll_volume(&vm)?;
                continue;
            }
            match vm.begin_needle_with_attrs(b_id as u16, key, attrs, len, flags) {
                // Not noted here: a reservation is not a record until its header is written.
                // The streamed PUT and the relocation note at commit, under the key lock
                // (#184, requirement 4).
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

    /// The attributes of the needle at global offset `vol_offset`: none, without a disk or
    /// mapping read, unless its flags say it has a block (#150).
    pub fn object_attrs(&self, vol_offset: u64, flags: u16) -> Result<crate::attrs::Attrs, VolumeError> {
        if flags & crate::meta_index::FLAG_ATTRS == 0 {
            return Ok(crate::attrs::Attrs::new());
        }
        let (vm, local) = self.volume_for(vol_offset)?;
        match vm.read_attrs(local) {
            Ok(a) => Ok(a),
            Err(e) => {
                self.record_read_corruption(vol_offset, &format!("offset:{}", local), &e);
                Err(e)
            }
        }
    }

    /// The `ObjectView` of an object, from its index row's offset and flags (base attributes only).
    pub fn object_view(&self, vol_offset: u64, flags: u16) -> Result<ObjectView, VolumeError> {
        Ok(ObjectView { attrs: self.object_attrs(vol_offset, flags)? })
    }

    /// Complete `ObjectView` for `(b_id, key, target_version_id)` merging base `0x400` attrs (if
    /// `FLAG_ATTRS`) with any surviving `0x200` attribute-update needles from the side map (#203).
    /// Objects with neither `FLAG_ATTRS` nor side-map entries do zero disk reads and zero locks.
    pub fn object_view_for(
        &self,
        b_id: u32,
        key: &str,
        target_version_id: u64,
        vol_offset: u64,
        flags: u16,
    ) -> Result<ObjectView, VolumeError> {
        if flags & crate::meta_index::FLAG_ATTRS == 0 && !self.attr_updates.has_updates() {
            return Ok(ObjectView {
                attrs: crate::attrs::Attrs::new(),
            });
        }
        let mut attrs = self.object_attrs(vol_offset, flags)?;
        let mut updates = self.attr_updates.get(b_id, key, target_version_id);
        if !updates.is_empty() {
            updates.sort_by(|a, b| (a.created_at_ns, a.vol_offset).cmp(&(b.created_at_ns, b.vol_offset)));
            let mut last_off = u64::MAX;
            for u in updates {
                if u.vol_offset == last_off {
                    continue;
                }
                last_off = u.vol_offset;
                let (vm, local) = self.volume_for(u.vol_offset)?;
                let payload = vm.read_needle_bytes_for_key(b_id, key_hash_of(key), local)?;
                let (upd_vid, upd_attrs) =
                    crate::attrs::decode_attr_update(&payload).map_err(|_| VolumeError::CorruptIndex)?;
                if upd_vid != target_version_id {
                    return Err(VolumeError::CorruptIndex);
                }
                attrs.merge_from(&upd_attrs);
            }
        }
        Ok(ObjectView { attrs })
    }

    /// Creation timestamp for an unversioned write or tombstone under per-key lock: guarantees a
    /// monotonically higher timestamp than any prior `0x200` update on that key (`Decision 56`).
    pub fn unversioned_write_ts(&self, b_id: u32, key: &str) -> u64 {
        let now = self.clock.now_ns();
        let max_u = self.attr_updates.max_ts(b_id, key, 0).unwrap_or(0);
        now.max(max_u.saturating_add(1))
    }

    /// Returns exact sector-rounded needle length, reading the needle header only when `FLAG_ATTRS`
    /// or `FLAG_COMPRESSED` is present so dead-byte accounting stays zero-read otherwise.
    pub fn needle_total_len(&self, vol_offset: u64, flags: u16, key_len: usize, logical_size: u64) -> u64 {
        if flags & (crate::meta_index::FLAG_ATTRS | crate::meta_index::FLAG_COMPRESSED) != 0 && vol_offset > 0 {
            if let Ok((vm, local)) = self.volume_for(vol_offset) {
                if let Ok(hdr) = vm.read_header(local) {
                    return hdr.total_len();
                }
            }
        }
        (64 + key_len as u64 + logical_size).div_ceil(crate::direct_io::SECTOR_SIZE as u64)
            * crate::direct_io::SECTOR_SIZE as u64
    }

    fn add_dead_attr_update_bytes(&self, updates: &[crate::fsm::KeyAttrUpdate]) {
        if updates.is_empty() {
            return;
        }
        let mut seen = Vec::with_capacity(updates.len());
        let mut bytes = 0u64;
        for u in updates {
            if !seen.contains(&u.vol_offset) {
                seen.push(u.vol_offset);
                bytes += u.total_len;
            }
        }
        if bytes > 0 {
            self.tombstone_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Drives an unversioned overwrite through `fsm::key_step`, retiring any `0x200` updates
    /// and accounting dead needle + update bytes in `tombstone_bytes`. Call under `lock_key`.
    #[allow(clippy::too_many_arguments)]
    pub fn retire_unversioned_overwrite(
        &self,
        b_id: u32,
        key: &str,
        old_info: Option<(u64, u64, u16)>,
        new_offset: u64,
        created_ns: u64,
        flags: u16,
    ) {
        let prev_state = match old_info {
            Some((off, _, old_flags)) => crate::fsm::KeyState::Unversioned(crate::fsm::KeyObject {
                version_id: 0,
                created_at_ns: 0,
                vol_offset: off,
                flags: old_flags,
                updates: self.attr_updates.get(b_id, key, 0),
            }),
            None => crate::fsm::KeyState::Absent,
        };
        let next_state = match crate::fsm::key_step(
            prev_state,
            crate::fsm::KeyRecord::Object {
                versioned: false,
                is_null: false,
                created_at_ns: created_ns,
                vol_offset: new_offset,
                flags,
            },
        ) {
            Ok(s) => s,
            Err(_) => crate::fsm::KeyState::Absent,
        };
        if let crate::fsm::KeyState::Unversioned(obj) = next_state {
            let retired = self.attr_updates.remove_target(b_id, key, obj.version_id, Some(&self.payload_cache));
            self.add_dead_attr_update_bytes(&retired);
        }
        if let Some((prev_off, prev_sz, old_flags)) = old_info {
            if prev_off > 0 && prev_off != new_offset {
                self.payload_cache.remove(b_id, prev_off);
                let dead_bytes = self.needle_total_len(prev_off, old_flags, key.len(), prev_sz);
                self.tombstone_bytes.fetch_add(dead_bytes, Ordering::Relaxed);
            }
        }
    }

    /// Writes a `0x200` attribute-update delta needle for `(b_id, key, vid)` without rewriting
    /// the object payload (`Decision 56`, `#203`). Acquires the per-key lock, checks existence and
    /// delete-marker refusal through `fsm::key_step`, stamps monotonic `created_at_ns`, appends
    /// the delta needle, and updates the side map and memory-budget cache accounting.
    pub async fn set_attribute_group(
        &self,
        b_id: u32,
        key: &str,
        vid: Option<&str>,
        update_attrs: crate::attrs::Attrs,
    ) -> Result<SetAttrOutcome, SetAttrError> {
        let _key_guard = self.lock_key(b_id, key).await;
        let (key_state, target_vid, resp_vid, base_upper_ns, dm_info, _) =
            self.resolve_attr_target_locked(b_id, key, vid)?;
        self.set_attribute_group_locked(
            b_id,
            key,
            key_state,
            target_vid,
            resp_vid,
            base_upper_ns,
            dm_info,
            update_attrs,
        )
        .await
    }

    /// Validates an object lock transition through `fsm::lock_step` under the per-key lock and
    /// atomically appends the corresponding `0x200` attribute-update needle (`#120`).
    pub async fn update_object_lock(
        &self,
        b_id: u32,
        key: &str,
        vid: Option<&str>,
        event: crate::fsm::LockEvent,
        retention: Option<&crate::object_lock::Retention>,
    ) -> Result<SetAttrOutcome, SetAttrError> {
        let group = match event {
            crate::fsm::LockEvent::PutRetention { .. } | crate::fsm::LockEvent::ClearRetention { .. } => {
                crate::attrs::AttrGroup::Retention
            }
            crate::fsm::LockEvent::PutLegalHold { .. } => crate::attrs::AttrGroup::LegalHold,
        };
        let _key_guard = self.lock_key(b_id, key).await;
        let (key_state, target_vid, resp_vid, base_upper_ns, dm_info, target_loc) =
            self.resolve_attr_target_locked(b_id, key, vid)?;

        // Refuse missing target or delete marker via `fsm::key_step` before loading view.
        if let Err(refusal) = crate::fsm::key_step(
            key_state.clone(),
            crate::fsm::KeyRecord::AttrUpdate {
                target_version_id: target_vid,
                group,
                created_at_ns: base_upper_ns.saturating_add(1),
                vol_offset: 1,
                total_len: 1,
            },
        ) {
            return Err(SetAttrError::Refused {
                refusal,
                delete_marker: dm_info,
            });
        }

        let Some((vol_offset, flags)) = target_loc else {
            return Err(SetAttrError::Refused {
                refusal: crate::fsm::NO_SUCH_KEY,
                delete_marker: dm_info,
            });
        };
        let view = self
            .object_view_for(b_id, key, target_vid, vol_offset, flags)
            .map_err(|e| SetAttrError::Internal {
                detail: format!("could not read the object's lock state: {:?}", e),
                after_record: false,
            })?;
        let current_lock = view.attrs.lock_state().map_err(|e| SetAttrError::Internal {
            detail: format!("corrupt lock attribute: {:?}", e),
            after_record: false,
        })?;
        let now_sec = self.clock.now_ns() / 1_000_000_000;
        if let Err(refusal) = crate::fsm::lock_step(current_lock, event, now_sec) {
            return Err(SetAttrError::Refused {
                refusal,
                delete_marker: dm_info,
            });
        }

        let mut update_attrs = crate::attrs::Attrs::new();
        match event {
            crate::fsm::LockEvent::PutRetention { .. } => update_attrs.set_retention(retention),
            crate::fsm::LockEvent::ClearRetention { .. } => update_attrs.set_retention(None),
            crate::fsm::LockEvent::PutLegalHold { on } => update_attrs.set_legal_hold(on),
        }
        self.set_attribute_group_locked(
            b_id,
            key,
            key_state,
            target_vid,
            resp_vid,
            base_upper_ns,
            dm_info,
            update_attrs,
        )
        .await
    }

    fn resolve_attr_target_locked(
        &self,
        b_id: u32,
        key: &str,
        vid: Option<&str>,
    ) -> Result<
        (
            crate::fsm::KeyState,
            u64,
            Option<String>,
            u64,
            Option<(String, bool)>,
            Option<(u64, u16)>,
        ),
        SetAttrError,
    > {
        if vid.is_some() || self.versions.read().has(b_id, key) {
            let parsed_vid = match vid {
                Some(s) => match crate::versions::parse_id(s) {
                    Some(id) => Some(id),
                    None => {
                        return Err(SetAttrError::Refused {
                            refusal: crate::fsm::NO_SUCH_VERSION,
                            delete_marker: None,
                        });
                    }
                },
                None => None,
            };
            let vs = self.versions.read();
            if !vs.has(b_id, key) {
                let meta_opt = self.engine.read().get_object(b_id, key.as_bytes()).ok();
                Ok(match (parsed_vid, meta_opt) {
                    (Some(0), Some(m)) | (None, Some(m)) => {
                        let updates = self.attr_updates.get(b_id, key, 0);
                        let upper = (m.created_at + 1).saturating_mul(1_000_000);
                        let flags = unpack_flags(m.bucket_flags);
                        let ks = crate::fsm::KeyState::Unversioned(crate::fsm::KeyObject {
                            version_id: 0,
                            created_at_ns: m.created_at.saturating_mul(1_000_000),
                            vol_offset: m.vol_offset,
                            flags,
                            updates,
                        });
                        (ks, 0u64, Some("null".to_string()), upper, None, Some((m.vol_offset, flags)))
                    }
                    (Some(req_vid), Some(m)) => {
                        let updates = self.attr_updates.get(b_id, key, 0);
                        let upper = (m.created_at + 1).saturating_mul(1_000_000);
                        let flags = unpack_flags(m.bucket_flags);
                        let ks = crate::fsm::KeyState::Unversioned(crate::fsm::KeyObject {
                            version_id: 0,
                            created_at_ns: m.created_at.saturating_mul(1_000_000),
                            vol_offset: m.vol_offset,
                            flags,
                            updates,
                        });
                        (ks, req_vid, None, upper, None, None)
                    }
                    (Some(req_vid), None) => (crate::fsm::KeyState::Absent, req_vid, None, 0, None, None),
                    (None, None) => (crate::fsm::KeyState::Absent, 0, None, 0, None, None),
                })
            } else {
                let list = vs.versions(b_id, key).unwrap_or(&[]);
                let entries: Vec<crate::fsm::KeyVersionEntry> = list
                    .iter()
                    .map(|v| {
                        if v.marker {
                            crate::fsm::KeyVersionEntry::DeleteMarker {
                                version_id: v.id(),
                                vol_offset: v.vol_offset,
                            }
                        } else {
                            crate::fsm::KeyVersionEntry::Object(crate::fsm::KeyObject {
                                version_id: v.id(),
                                created_at_ns: v.ts,
                                vol_offset: v.vol_offset,
                                flags: v.flags,
                                updates: self.attr_updates.get(b_id, key, v.id()),
                            })
                        }
                    })
                    .collect();
                let ks = crate::fsm::KeyState::Versioned { versions: entries };
                let target_v = match parsed_vid {
                    Some(id) => vs.find(b_id, key, id),
                    None => vs.current(b_id, key),
                };
                Ok(match target_v {
                    None => {
                        let tid = parsed_vid.unwrap_or(0);
                        (
                            if parsed_vid.is_some() { ks } else { crate::fsm::KeyState::Absent },
                            tid,
                            None,
                            0,
                            None,
                            None,
                        )
                    }
                    Some(v) if v.marker => (
                        ks,
                        v.id(),
                        Some(v.id_string()),
                        0,
                        Some((v.id_string(), parsed_vid.is_some())),
                        None,
                    ),
                    Some(v) => {
                        let upper = if v.ts == 0 {
                            ((v.modified / 1_000_000) + 1).saturating_mul(1_000_000)
                        } else {
                            v.ts
                        };
                        (ks, v.id(), Some(v.id_string()), upper, None, Some((v.vol_offset, v.flags)))
                    }
                })
            }
        } else {
            Ok(match self.engine.read().get_object(b_id, key.as_bytes()) {
                Ok(m) => {
                    let updates = self.attr_updates.get(b_id, key, 0);
                    let upper = (m.created_at + 1).saturating_mul(1_000_000);
                    let flags = unpack_flags(m.bucket_flags);
                    let ks = crate::fsm::KeyState::Unversioned(crate::fsm::KeyObject {
                        version_id: 0,
                        created_at_ns: m.created_at.saturating_mul(1_000_000),
                        vol_offset: m.vol_offset,
                        flags,
                        updates,
                    });
                    (ks, 0u64, None, upper, None, Some((m.vol_offset, flags)))
                }
                Err(_) => (crate::fsm::KeyState::Absent, 0u64, None, 0, None, None),
            })
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn set_attribute_group_locked(
        &self,
        b_id: u32,
        key: &str,
        mut key_state: crate::fsm::KeyState,
        target_vid: u64,
        resp_vid: Option<String>,
        base_upper_ns: u64,
        dm_info: Option<(String, bool)>,
        update_attrs: crate::attrs::Attrs,
    ) -> Result<SetAttrOutcome, SetAttrError> {
        let mut groups = Vec::new();
        for (tag, _) in update_attrs.iter() {
            let g = crate::attrs::AttrGroup::from_tag(tag);
            if !groups.contains(&g) {
                groups.push(g);
            }
        }
        if groups.is_empty() {
            return Err(SetAttrError::Refused {
                refusal: crate::fsm::INVALID_ARGUMENT,
                delete_marker: None,
            });
        }

        let existing_updates = self.attr_updates.get(b_id, key, target_vid);
        let max_u = existing_updates.iter().map(|u| u.created_at_ns).max().unwrap_or(0);
        let created_ns = self
            .clock
            .now_ns()
            .max(base_upper_ns.saturating_add(1))
            .max(max_u.saturating_add(1));

        // Refuse invalid targets before writing any record to disk.
        if let Err(refusal) = crate::fsm::key_step(
            key_state.clone(),
            crate::fsm::KeyRecord::AttrUpdate {
                target_version_id: target_vid,
                group: groups[0],
                created_at_ns: created_ns,
                vol_offset: 1,
                total_len: 1,
            },
        ) {
            return Err(SetAttrError::Refused {
                refusal,
                delete_marker: dm_info,
            });
        }

        let payload = crate::attrs::encode_attr_update(target_vid, &update_attrs)
            .map_err(|_| SetAttrError::TooLarge)?;
        let flags = AWAN_FLAG_ACTIVE | crate::meta_index::FLAG_ATTR_UPDATE;
        let (loc, g, late) = match self.write_to_active_with(
            b_id,
            key,
            &[],
            &payload,
            flags,
            0,
            0,
            Some(created_ns),
        ) {
            Ok((vm, loc, g)) => {
                self.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
                let l = self
                    .make_durable(&vm, loc.seq)
                    .await
                    .err()
                    .map(|e| format!("could not sync attribute update: {:?}", e));
                (loc, g, l)
            }
            Err(VolumeError::AfterRecord { loc, cause, .. }) => {
                self.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
                (
                    loc,
                    global_offset(loc.volume_id, loc.offset),
                    Some(format!("could not store attribute update: {}", cause)),
                )
            }
            Err(e) => {
                return Err(SetAttrError::Internal {
                    detail: format!("could not store attribute update: {:?}", e),
                    after_record: false,
                });
            }
        };

        for group in groups {
            match crate::fsm::key_step(
                key_state.clone(),
                crate::fsm::KeyRecord::AttrUpdate {
                    target_version_id: target_vid,
                    group,
                    created_at_ns: created_ns,
                    vol_offset: g,
                    total_len: loc.total_len,
                },
            ) {
                Ok(next) => key_state = next,
                Err(refusal) => {
                    return Err(SetAttrError::Refused {
                        refusal,
                        delete_marker: dm_info,
                    });
                }
            }
        }
        let new_updates = match key_state {
            crate::fsm::KeyState::Unversioned(obj) => obj.updates,
            crate::fsm::KeyState::Versioned { versions } => versions
                .into_iter()
                .find_map(|e| match e {
                    crate::fsm::KeyVersionEntry::Object(o) if o.version_id == target_vid => Some(o.updates),
                    _ => None,
                })
                .unwrap_or_default(),
            crate::fsm::KeyState::Absent => Vec::new(),
        };
        let superseded: Vec<_> = existing_updates
            .into_iter()
            .filter(|old| !new_updates.iter().any(|cur| cur.vol_offset == old.vol_offset))
            .collect();
        self.add_dead_attr_update_bytes(&superseded);
        self.attr_updates
            .set_for_target(b_id, key, target_vid, new_updates, Some(&self.payload_cache));

        if let Some(detail) = late {
            return Err(SetAttrError::Internal {
                detail,
                after_record: true,
            });
        }
        if self.events.has_subscribers() {
            let bname = self
                .engine
                .read()
                .buckets
                .get(&b_id)
                .map(|b| b.name.clone())
                .unwrap_or_default();
            self.emit_commit_event_for(
                b_id,
                &bname,
                created_ns,
                key,
                resp_vid.clone(),
                crate::events::CommitKind::AttrUpdate,
                payload.len() as u64,
                "",
            );
        }
        Ok(SetAttrOutcome {
            version_id: resp_vid,
            vol_offset: g,
            created_at_ns: created_ns,
        })
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

    /// The ids of the volumes this server has open, oldest first.
    pub fn volume_ids(&self) -> Vec<u32> {
        self.volumes.read().keys().copied().collect()
    }

    /// Writes reserved in volume `id` whose header is not written yet (#167); `None` when the
    /// volume does not exist. Tests wait on it instead of sleeping.
    pub fn volume_writers_in_flight(&self, id: u32) -> Option<usize> {
        self.volumes.read().get(&id).map(|v| v.writers_in_flight())
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
        self.load_payload_for(b_id, &format!("bucket:{}/offset:{}", b_id, vol_offset), vol_offset, expected_size)
    }

    /// Reads and verifies a payload, attributing any checksum mismatch to `key` in logs and scrub.
    pub fn load_payload_for(&self, b_id: u32, key: &str, vol_offset: u64, expected_size: usize) -> Result<Bytes, VolumeError> {
        if expected_size == 0 {
            return Ok(Bytes::new());
        }
        if vol_offset == 0 {
            return Err(VolumeError::CorruptIndex);
        }
        let (vol_id, local_off) = crate::split_offset(vol_offset);
        if self.scrub.is_bad(vol_id, local_off) {
            self.payload_cache.remove(b_id, vol_offset);
        } else if let Some(cached) = self.payload_cache.get(b_id, vol_offset) {
            return Ok(cached);
        }
        let (vm, local) = self.volume_for(vol_offset)?;
        let bytes = match vm.read_needle_bytes_verified(b_id, local) {
            Ok(b) => b,
            Err(e) => {
                self.record_read_corruption(vol_offset, key, &e);
                return Err(e);
            }
        };
        if bytes.len() != expected_size {
            return Err(VolumeError::CorruptIndex);
        }
        if bytes.len() <= 4 * 1024 * 1024 {
            self.payload_cache.insert(b_id, vol_offset, bytes.clone());
        }
        Ok(bytes)
    }

    /// A body that sends `len` bytes from payload offset `start` of a large object with
    /// positioned reads (`stream_read`, #155), checking the payload CRC when `verify` (a full
    /// GET). The header is read and checked against the index now, before any response
    /// header, so a mismatch is still a 500; no payload byte is read until the body is polled.
    #[allow(clippy::too_many_arguments)]
    pub fn large_payload_body(&self, b_id: u32, what: String, vol_offset: u64, size: u64, start: u64, len: u64, verify: bool) -> Result<crate::stream_read::PreadBody, VolumeError> {
        if vol_offset == 0 || start + len > size {
            return Err(VolumeError::CorruptIndex);
        }
        let (vm, local) = self.volume_for(vol_offset)?;
        let (payload_at, stored, expected_crc) = match vm.payload_extent_with_crc(b_id, local) {
            Ok(x) => x,
            Err(e) => {
                self.record_read_corruption(vol_offset, &what, &e);
                return Err(e);
            }
        };
        if stored != size {
            return Err(VolumeError::CorruptIndex);
        }
        let scrub = Arc::clone(&self.scrub);
        let vol_id = vm.id();
        let what_corrupt = what.clone();
        let on_corrupt: Arc<dyn Fn(&str) + Send + Sync> = Arc::new(move |err_msg: &str| {
            scrub.mark_bad(vol_id, local, &what_corrupt, err_msg.to_string());
        });
        let segment = crate::stream_read::Segment {
            source: crate::stream_read::Source::Volume {
                vm,
                direct: len >= self.direct_read_min.load(Ordering::Relaxed),
            },
            at: payload_at + start,
            len,
            verify: verify.then_some(expected_crc),
            transform: None,
        };
        Ok(crate::stream_read::PreadBody::from_job(crate::stream_read::Job {
            segments: vec![segment],
            what,
            failures: Arc::clone(&self.read_checksum_failures),
            work: Arc::clone(&self.work),
            body_budget: Some(Arc::clone(&self.body_budget)),
            on_corrupt: Some(on_corrupt),
        }))
    }

    /// A body that sends `len` bytes from offset `start` of a multipart object with positioned
    /// reads, part by part (#155 requirement 5, decision 53): a staged part from its file, a
    /// part stored as a needle from its volume. No part is mapped. Every part the range touches
    /// is opened and checked against its extent now, before any response header. When
    /// `verify`, a needle part sent whole is checked against its payload CRC as it is sent.
    #[allow(clippy::too_many_arguments)]
    pub fn multipart_body(&self, b_id: u32, what: String, extents: &[crate::multipart::PartExtent], start: u64, len: u64, verify: bool) -> Result<crate::stream_read::PreadBody, VolumeError> {
        use crate::stream_read::{Job, PreadBody, Segment, Source};
        let end = start + len;
        let mut segments = Vec::new();
        for ext in extents {
            let p_start = ext.cumulative_end - ext.part_size;
            let (lo, hi) = (start.max(p_start), end.min(ext.cumulative_end));
            if lo >= hi {
                continue;
            }
            let (in_part, seg_len) = (lo - p_start, hi - lo);
            let whole = in_part == 0 && seg_len == ext.part_size;
            let segment = match &ext.staged_path {
                Some(path) => {
                    let file = std::fs::File::open(path)?;
                    if file.metadata()?.len() < ext.part_size {
                        return Err(VolumeError::CorruptIndex);
                    }
                    Segment { source: Source::File(Arc::new(file)), at: in_part, len: seg_len, verify: None, transform: None }
                }
                None => {
                    let (vm, local) = self.volume_for(ext.vol_offset)?;
                    let (payload_at, stored, expected_crc) = match vm.payload_extent_with_crc(b_id, local) {
                        Ok(x) => x,
                        Err(e) => {
                            self.record_read_corruption(ext.vol_offset, &what, &e);
                            return Err(e);
                        }
                    };
                    if stored != ext.part_size {
                        return Err(VolumeError::CorruptIndex);
                    }
                    let check = (verify && whole).then_some(expected_crc);
                    let direct = seg_len >= self.direct_read_min.load(Ordering::Relaxed);
                    Segment { source: Source::Volume { vm, direct }, at: payload_at + in_part, len: seg_len, verify: check, transform: None }
                }
            };
            segments.push(segment);
        }
        if segments.iter().map(|s| s.len).sum::<u64>() != len {
            return Err(VolumeError::CorruptIndex);
        }
        Ok(PreadBody::from_job(Job {
            segments,
            what,
            failures: Arc::clone(&self.read_checksum_failures),
            work: Arc::clone(&self.work),
            body_budget: Some(Arc::clone(&self.body_budget)),
            on_corrupt: None,
        }))
    }

    /// A large object's payload from the mapping, unread and unchecked, with the checksum it
    /// must have. GET no longer sends large objects this way (`large_payload_body`, #155);
    /// the mapping is what faulted their pages into the RSS.
    pub fn load_payload_unverified(&self, b_id: u32, vol_offset: u64, expected_size: usize) -> Result<(Bytes, u32), VolumeError> {
        if vol_offset == 0 {
            return Err(VolumeError::CorruptIndex);
        }
        let (vm, local) = self.volume_for(vol_offset)?;
        let (header, bytes) = vm.needle_header_and_payload_unverified(local)?;
        if header.bucket_id != b_id as u16 || bytes.len() != expected_size {
            return Err(VolumeError::CorruptIndex);
        }
        Ok((bytes, header.payload_crc32c))
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
pub(crate) const FORMAT_2_FLAGS: u16 = crate::meta_index::FLAG_COMPRESSED | crate::meta_index::FLAG_VERSIONED | crate::meta_index::FLAG_NULL_ID | crate::meta_index::FLAG_VERSION_DELETE;

/// The oldest volume format that can hold a needle with these flags and attributes.
fn needed_format(flags: u16, attrs: &[u8]) -> u32 {
    if !attrs.is_empty()
        || flags & (crate::meta_index::FLAG_ATTRS | crate::meta_index::FLAG_ATTR_UPDATE) != 0
    {
        3
    } else if flags & FORMAT_2_FLAGS != 0 {
        2
    } else {
        1
    }
}

/// The attributes a PUT stores with its object: today its `Content-Type` (#150; the other
/// groups come with their issues). Empty when the request sends none, so the needle stays
/// a format 2 needle. A block that would not fit is 400 `MetadataTooLarge`.
/// What GET and HEAD say about an object besides its bytes, read once per request (#150).
pub struct ObjectView {
    pub attrs: crate::attrs::Attrs,
}

impl ObjectView {
    pub fn content_type(&self) -> &str {
        self.attrs.content_type().unwrap_or("application/octet-stream")
    }
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

/// The health answers say which profile is active (#358).
fn with_profile(mut r: Response<ResponseBody>) -> Response<ResponseBody> {
    r.headers_mut().insert("x-awanfs-profile", hyper::header::HeaderValue::from_static(crate::profile::active().as_str()));
    r
}

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
        "/_awanfs/health/live" => with_profile(plain(StatusCode::OK, "live")),
        "/_awanfs/health/ready" => with_profile(match state.readiness() {
            None => plain(StatusCode::OK, "ready"),
            Some(reason) => plain(StatusCode::SERVICE_UNAVAILABLE, &format!("not ready: {}", reason)),
        }),
        "/_awanfs/metrics" => Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")
            .body(full_body(Bytes::from(render_metrics(state))))
            .unwrap(),
        "/_awanfs/scrub" => {
            let body = serde_json::to_string_pretty(&state.scrub.report()).unwrap_or_else(|_| "{}".to_string());
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/json")
                .body(full_body(Bytes::from(body)))
                .unwrap()
        }
        _ => plain(StatusCode::NOT_FOUND, "no such endpoint"),
    }
}

/// Everything `/_awanfs/metrics` reports, in Prometheus text format.
pub fn render_metrics(state: &S3ServiceState) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(8192);
    let _ = writeln!(out, "# HELP awanfs_profile The startup profile (#358).\n# TYPE awanfs_profile gauge\nawanfs_profile{{profile=\"{}\"}} 1", crate::profile::active().as_str());
    state.metrics.render(&mut out);
    state.bucket_config_metrics.render(&mut out);
    state.events.render_metrics(&mut out);
    {
        let mut total_objects = 0u64;
        let mut total_bytes = 0u64;
        let mut usage_rows = Vec::new();
        {
            let engine = state.engine.read();
            let emit = engine.buckets.len() <= PER_BUCKET_METRICS_CARDINALITY_CAP
                || state.per_bucket_metrics.load(Ordering::Relaxed);
            for entry in engine.buckets.values() {
                total_objects += entry.current_objects;
                total_bytes += entry.current_bytes;
            }
            if emit {
                let versions = state.versions.read();
                for (&b_id, entry) in &engine.buckets {
                    let cur = crate::meta_index::BucketUsage {
                        objects: entry.current_objects,
                        bytes: entry.current_bytes,
                    };
                    let noncur = versions.noncurrent_usage(b_id);
                    let all_ver = crate::meta_index::BucketUsage {
                        objects: cur.objects + noncur.objects,
                        bytes: cur.bytes + noncur.bytes,
                    };
                    let labels = if entry.namespace.is_empty() {
                        format!("bucket=\"{}\"", entry.name)
                    } else {
                        let escaped_ns = entry
                            .namespace
                            .replace('\\', "\\\\")
                            .replace('"', "\\\"")
                            .replace('\n', "\\n");
                        format!("namespace=\"{}\",bucket=\"{}\"", escaped_ns, entry.name)
                    };
                    usage_rows.push((entry.namespace.clone(), entry.name.clone(), labels, cur, all_ver));
                }
            }
        }
        usage_rows.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
        let _ = writeln!(
            out,
            "# HELP awanfs_objects_total Current live objects across all buckets.\n# TYPE awanfs_objects_total gauge\nawanfs_objects_total {}",
            total_objects
        );
        let _ = writeln!(
            out,
            "# HELP awanfs_bytes_total Current live payload bytes across all buckets.\n# TYPE awanfs_bytes_total gauge\nawanfs_bytes_total {}",
            total_bytes
        );
        out.push_str("# HELP awanfs_bucket_objects Current live objects in the bucket.\n# TYPE awanfs_bucket_objects gauge\n");
        for (_, _, labels, cur, _) in &usage_rows {
            let _ = writeln!(out, "awanfs_bucket_objects{{{}}} {}", labels, cur.objects);
        }
        out.push_str("# HELP awanfs_bucket_bytes Current live payload bytes in the bucket.\n# TYPE awanfs_bucket_bytes gauge\n");
        for (_, _, labels, cur, _) in &usage_rows {
            let _ = writeln!(out, "awanfs_bucket_bytes{{{}}} {}", labels, cur.bytes);
        }
        out.push_str("# HELP awanfs_bucket_all_versions_objects Total live non-marker object versions in the bucket.\n# TYPE awanfs_bucket_all_versions_objects gauge\n");
        for (_, _, labels, _, all_ver) in &usage_rows {
            let _ = writeln!(out, "awanfs_bucket_all_versions_objects{{{}}} {}", labels, all_ver.objects);
        }
        out.push_str("# HELP awanfs_bucket_all_versions_bytes Total live non-marker object version bytes in the bucket.\n# TYPE awanfs_bucket_all_versions_bytes gauge\n");
        for (_, _, labels, _, all_ver) in &usage_rows {
            let _ = writeln!(out, "awanfs_bucket_all_versions_bytes{{{}}} {}", labels, all_ver.bytes);
        }
    }
    let used: u64 = state.volumes.read().values().map(|v| v.offset()).sum();
    let (free, active_id) = match state.volume_manager.read().clone() {
        Some(vm) => (vm.max_span().saturating_sub(vm.offset()), vm.id()),
        None => (0, 0),
    };
    let (hits, misses) = state.payload_cache.hit_counts();
    let w = state.work.snapshot();
    let budget = crate::memory::effective();
    let gauges: &[(&str, &str, &str, u64)] = &[
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
        ("awanfs_inflight_bytes", "gauge", "Bytes currently held in memory by in-flight requests.", state.work.inflight_bytes()),
        ("awanfs_peak_inflight_bytes", "gauge", "Peak bytes held in memory concurrently by in-flight requests.", state.work.peak_inflight_bytes()),
        ("awanfs_ready", "gauge", "1 when /_awanfs/health/ready answers 200.", state.readiness().is_none() as u64),
        ("awanfs_memory_budget_bytes", "gauge", "Memory the server may hold (--memory-budget).", budget.total),
        ("awanfs_memory_budget_cache_bytes", "gauge", "The budget's half for the payload cache.", budget.cache),
        ("awanfs_memory_budget_spill_bytes", "gauge", "The spill index's memtable and segment filters, carved out of the cache half.", crate::memory::split_cache(budget.cache).0),
        ("awanfs_memory_budget_bodies_bytes", "gauge", "The budget's half for in-flight request bodies.", budget.bodies),
        ("awanfs_memory_bodies_in_use_bytes", "gauge", "Bytes currently held by admitted in-flight request bodies.", state.body_budget.in_use()),
        ("awanfs_memory_bodies_rejected_total", "counter", "Requests rejected with 503 SlowDown because the body memory budget was exhausted.", state.body_budget.rejected()),
        (
            "awanfs_read_checksum_failures_total",
            "counter",
            "Reads that failed their header or payload checksum (decision 46).",
            state.read_checksum_failures.load(Ordering::Relaxed),
        ),
        (
            "awanfs_corrupt_needles_total",
            "counter",
            "Corrupt needles detected by background scrub or read verification (#155).",
            state.scrub.corrupt_needles_total(),
        ),
        (
            "awanfs_stream_relocations_total",
            "counter",
            "Streamed PUTs copied to the end of the log because their key was written meanwhile (decision 51).",
            state.stream_relocations.load(Ordering::Relaxed),
        ),
        (
            "awanfs_log_lines_dropped_total",
            "counter",
            "Request-log lines dropped because the log writer could not keep up.",
            state.log_lines_dropped.load(Ordering::Relaxed),
        ),
        (
            "awanfs_attr_updates_bytes",
            "gauge",
            "Estimated bytes held by the 0x200 attribute-update side map.",
            state.attr_updates.estimated_bytes() as u64,
        ),
        (
            "awanfs_attr_updates_ignored_total",
            "counter",
            "0x200 attribute-update needles ignored during recovery because their target was absent, deleted, or a delete marker.",
            state.attr_updates_ignored.load(Ordering::Relaxed),
        ),
        (
            "awanfs_lifecycle_objects_expired_total",
            "counter",
            "Objects, versions, and sole delete markers expired by the lifecycle sweep (#118).",
            state.lifecycle.objects_expired(),
        ),
        (
            "awanfs_lifecycle_bytes_expired_total",
            "counter",
            "Payload bytes expired by the lifecycle sweep (#118).",
            state.lifecycle.bytes_expired(),
        ),
        (
            "awanfs_lifecycle_uploads_aborted_total",
            "counter",
            "Incomplete multipart uploads aborted by the lifecycle sweep (#118).",
            state.lifecycle.uploads_aborted(),
        ),
    ];
    for (name, kind, help, v) in gauges {
        let _ = writeln!(out, "# HELP {} {}\n# TYPE {} {}\n{} {}", name, help, name, kind, name, v);
    }
    let _ = writeln!(
        out,
        "# HELP awanfs_scrub_pass_duration_seconds Duration of the last completed background scrub pass in seconds.\n# TYPE awanfs_scrub_pass_duration_seconds gauge\nawanfs_scrub_pass_duration_seconds {:.6}",
        state.scrub.last_pass_duration_seconds()
    );
    out
}

/// The S3 operation a request is, for the metrics.
fn operation_name(method: &Method, target: &ParsedTarget, query: &str, headers: &hyper::HeaderMap) -> &'static str {
    let (b, k) = match target {
        ParsedTarget::Service => (None, None),
        ParsedTarget::Bucket { bucket } => (Some(bucket.as_str()), None),
        ParsedTarget::Object { bucket, key } => (Some(bucket.as_str()), Some(key.as_str())),
    };
    match crate::ops::classify(method, b, k, query, headers) {
        Ok(op) => crate::ops::spec(op).name,
        Err(_) => "Other",
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
    let op = operation_name(req.method(), &parse_s3_target(req.uri().path(), host), req.uri().query().unwrap_or(""), req.headers());
    let bytes_in = content_length(req.headers());
    let is_head = req.method() == Method::HEAD;
    let method = req.method().clone();
    let cors_origin = req
        .headers()
        .get("origin")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let cors_req_method = req
        .headers()
        .get("access-control-request-method")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
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
        authz_reason = tracing::field::Empty,
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
    let (resp, error_code, authz_reason) = crate::request_log::with_request_id(request_id.clone(), handle_s3_inner(Arc::clone(&state), req)).instrument(span.clone()).await;
    let mut resp = resp?;
    if matches!(method, Method::GET | Method::PUT | Method::POST | Method::DELETE | Method::HEAD)
        && !bucket.is_empty()
    {
        if let Some(ref origin) = cors_origin {
            let b_id = state.engine.read().lookup_bucket(&bucket).ok();
            let cfg = b_id.and_then(|id| state.bucket_config_reader.load_bucket(id));
            if let Some(rules) = cfg
                .as_ref()
                .and_then(|c| c.get(crate::bucket_config::ConfigKind::Cors))
                .and_then(|p| p.parsed.as_ref())
                .and_then(|a| a.downcast_ref::<Vec<crate::cors::CorsRule>>())
            {
                let cors_method = cors_req_method.as_deref().unwrap_or(method.as_str());
                if let Some(answer) = crate::cors::match_actual(rules, origin, cors_method) {
                    crate::ops::cors::apply_cors_headers(resp.headers_mut(), &answer, false);
                }
            }
        }
    }
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
            authz_reason,
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
    let classified = match crate::ops::classify_request(req) {
        Ok(c) => c,
        Err(resp) => return Ok(resp),
    };
    // A form upload's key and credentials are in its body (#119): read the fields first.
    let classified = if classified.op == crate::ops::Operation::PostObject {
        match crate::ops::post_object::read_form(&state, classified).await {
            Ok(c) => c,
            Err(resp) => return Ok(resp),
        }
    } else {
        classified
    };
    let authenticated = match crate::ops::authenticate(&state, classified) {
        Ok(a) => a,
        Err(resp) => return Ok(resp),
    };
    let resolved = match crate::ops::resolve_bucket(&state, authenticated) {
        Ok(r) => r,
        Err(resp) => return Ok(resp),
    };
    let authorized = match crate::ops::authorize(&state, resolved) {
        Ok(a) => a,
        Err(resp) => return Ok(resp),
    };
    let validated = match crate::ops::validate(authorized) {
        Ok(v) => v,
        Err(resp) => return Ok(resp),
    };
    crate::ops::execute(state, validated).await
}
