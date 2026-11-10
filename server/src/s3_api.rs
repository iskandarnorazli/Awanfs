//! awan-storage/server/src/s3_api.rs
//! S3 REST Wire Protocol Dispatcher, Routing & RFC 9110 Range Engine
//! Reference: awan-storage/API_SPECIFICATION.md and awan-storage/HYBRID_POC_SPECIFICATION.md

use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, HOST, LAST_MODIFIED};
use hyper::{Method, Request, Response, StatusCode};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;


use crate::ffi_bridge::*;
use crate::xml::*;

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
        // Multi-range not required for S3 drop-in compatibility
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
                // Negative control: do not clamp
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

    // Check virtual-hosted-style: Host: {bucket}.s3.localhost:9000 or {bucket}.localhost:9000
    if let Some(host) = host_header {
        let host_no_port = host.split(':').next().unwrap_or(host);
        // Common domains to strip
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

    // Path-style: /{bucket}/{key...}
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

use md5::{Digest, Md5};

pub fn compute_md5(data: &[u8]) -> (u64, u64, String) {
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
    (hi, lo, hex)
}

pub struct S3ServiceState {
    pub engine: Arc<RwLock<AwanStorageEngine>>,
    /// Keyed by (bucket_id, vol_offset) -> raw payload bytes
    pub payload_store: Arc<RwLock<HashMap<(u32, u64), Bytes>>>,
    pub next_vol_offset: Arc<RwLock<u64>>,
}

impl S3ServiceState {
    pub fn new() -> Self {
        Self {
            engine: Arc::new(RwLock::new(AwanStorageEngine::new(100_000, 4_194_304, 65536))),
            payload_store: Arc::new(RwLock::new(HashMap::new())),
            next_vol_offset: Arc::new(RwLock::new(4096)),
        }
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

    let target = parse_s3_target(path, host_hdr);

    match target {
        ParsedTarget::Service => {
            if method == Method::GET {
                // ListBuckets
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
            // Bucket operations
            match method {
                Method::PUT => {
                    let mut engine = state.engine.write();
                    match engine.create_bucket(&bucket) {
                        Ok(_) => {
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
                        Ok(()) => Ok(Response::builder()
                            .status(StatusCode::NO_CONTENT)
                            .header(CONTENT_LENGTH, "0")
                            .body(empty_body())
                            .unwrap()),
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
                    if query.contains("location") {
                        let xml = location_constraint_xml("us-east-1");
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

                    let mut prefix = "";
                    let mut start_after = "";
                    let mut max_keys = 1000u32;
                    for q in query.split('&') {
                        if let Some((k, v)) = q.split_once('=') {
                            if k == "prefix" {
                                prefix = v;
                            } else if k == "start-after" {
                                start_after = v;
                            } else if k == "max-keys" {
                                if let Ok(n) = v.parse::<u32>() {
                                    max_keys = n;
                                }
                            }
                        }
                    }

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

                Method::POST if query.contains("delete") => {
                    // Batch DeleteObjects
                    let body_bytes = req.into_body().collect().await?.to_bytes();
                    let body_str = String::from_utf8_lossy(&body_bytes);

                    // Extract keys: <Key>...</Key>
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
                    for k in &keys_to_delete {
                        let _ = engine.delete_object(b_id, k.as_bytes());
                        deleted.push(DeleteItemResult {
                            key: k.as_str(),
                            version_id: None,
                        });
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
            // Object operations
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
                        let dst_b_id = match engine.lookup_bucket(&bucket) {
                            Ok(id) => id,
                            Err(_) => {
                                let xml = s3_error_xml("NoSuchBucket", "Target bucket does not exist", &bucket, "req-cp");
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

                        // Clone metadata pointing to same volume offset
                        let mut engine = state.engine.write();
                        engine.put_object(
                            dst_b_id,
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

                    // Standard PutObject: verify bucket exists first (for Expect: 100-continue validation)
                    let b_id = {
                        let engine = state.engine.read();
                        match engine.lookup_bucket(&bucket) {
                            Ok(id) => id,
                            Err(_) => {
                                let xml = s3_error_xml("NoSuchBucket", "The specified bucket does not exist", &bucket, "req-put-obj");
                                return Ok(Response::builder()
                                    .status(StatusCode::NOT_FOUND)
                                    .header(CONTENT_TYPE, "application/xml")
                                    .body(full_body(Bytes::from(xml)))
                                    .unwrap());
                            }
                        }
                    };

                    let body_bytes = req.into_body().collect().await?.to_bytes();
                    let payload_len = body_bytes.len() as u64;
                    let (etag_hi, etag_lo, etag_hex) = compute_md5(&body_bytes);

                    let vol_offset = state.allocate_offset(body_bytes.len());
                    state.payload_store.write().insert((b_id, vol_offset), body_bytes);

                    let mut engine = state.engine.write();
                    engine.put_object(
                        b_id,
                        key.as_bytes(),
                        vol_offset,
                        payload_len,
                        etag_hi,
                        etag_lo,
                        0,
                        AWAN_FLAG_ACTIVE,
                    ).unwrap();

                    let resp = Response::builder()
                        .status(StatusCode::OK)
                        .header(ETAG, format!("\"{}\"", etag_hex))
                        .header(CONTENT_LENGTH, "0")
                        .body(empty_body())
                        .unwrap();
                    Ok(resp)
                }

                Method::GET | Method::HEAD => {
                    let engine = state.engine.read();
                    let b_id = match engine.lookup_bucket(&bucket) {
                        Ok(id) => id,
                        Err(_) => {
                            let xml = s3_error_xml("NoSuchBucket", "The specified bucket does not exist", &bucket, "req-get-obj");
                            return Ok(Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .header(CONTENT_TYPE, "application/xml")
                                .body(full_body(Bytes::from(xml)))
                                .unwrap());
                        }
                    };

                    let meta = match engine.get_object(b_id, key.as_bytes()) {
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

                    let etag_hex = md5_to_etag_hex(meta.etag_hi, meta.etag_lo);
                    let etag_val = format!("\"{}\"", etag_hex);

                    // Conditional headers: If-Match
                    if let Some(if_match) = req.headers().get("If-Match").and_then(|v| v.to_str().ok()) {
                        let if_match = if_match.trim();
                        if if_match != "*" && if_match != etag_val && if_match != etag_hex {
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
                        if if_none_match == "*" || if_none_match == etag_val || if_none_match == etag_hex {
                            return Ok(Response::builder()
                                .status(StatusCode::NOT_MODIFIED)
                                .header(ETAG, etag_val)
                                .header(CONTENT_LENGTH, "0")
                                .body(empty_body())
                                .unwrap());
                        }
                    }

                    // Fetch payload
                    let payload = state
                        .payload_store
                        .read()
                        .get(&(b_id, meta.vol_offset))
                        .cloned()
                        .unwrap_or_else(|| Bytes::from(vec![0u8; meta.size as usize]));

                    // Check Range header
                    let range_hdr = req.headers().get("Range").and_then(|v| v.to_str().ok());
                    let range_res = range_hdr.map(|h| parse_range_header(h, meta.size));

                    let is_head = method == Method::HEAD;

                    if let Some(range_opt) = range_res {
                        match range_opt {
                            Ok(Some(range)) => {
                                let start = range.start as usize;
                                let end = range.end as usize;
                                let sub_bytes = if start < payload.len() {
                                    let actual_end = (end + 1).min(payload.len());
                                    payload.slice(start..actual_end)
                                } else {
                                    Bytes::new()
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
                                // 416 Range Not Satisfiable
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

                    // Full object GET/HEAD
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
                    let mut engine = state.engine.write();
                    if let Ok(b_id) = engine.lookup_bucket(&bucket) {
                        let _ = engine.delete_object(b_id, key.as_bytes());
                    }
                    // S3 spec: DeleteObject is idempotent 204 No Content even if key did not exist
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
