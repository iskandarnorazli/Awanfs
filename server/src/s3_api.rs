//! awan-storage/server/src/s3_api.rs
//! S3 REST Wire Protocol Dispatcher, Routing & RFC 9110 Range Engine
//! Reference: awan-storage/API_SPECIFICATION.md and awan-storage/HYBRID_POC_SPECIFICATION.md

use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, HOST, LAST_MODIFIED};
use hyper::{Method, Request, Response, StatusCode};
use md5::{Digest, Md5};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::aws_chunked::decode_aws_chunked;
use crate::ffi_bridge::*;
use crate::lifecycle::*;
use crate::multipart::*;
use crate::sigv4::*;
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

pub struct S3ServiceState {
    pub engine: Arc<RwLock<AwanStorageEngine>>,
    pub multipart: Arc<MultipartManager>,
    pub sigv4_cache: Arc<SigV4KeyCache>,
    pub lifecycle: Arc<LifecycleManager>,
    pub payload_store: Arc<RwLock<HashMap<(u32, u64), Bytes>>>,
    pub next_vol_offset: Arc<RwLock<u64>>,
}

impl S3ServiceState {
    pub fn new() -> Self {
        Self {
            engine: Arc::new(RwLock::new(AwanStorageEngine::new(100_000, 4_194_304, 65536))),
            multipart: Arc::new(MultipartManager::new()),
            sigv4_cache: Arc::new(SigV4KeyCache::new()),
            lifecycle: Arc::new(LifecycleManager::new()),
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

    let mut query_map = HashMap::new();
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            query_map.insert(k.to_string(), v.to_string());
        } else if !pair.is_empty() {
            query_map.insert(pair.to_string(), String::new());
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
                let vol_offset = state.allocate_offset(decoded_bytes.len());
                state.payload_store.write().insert((b_id, vol_offset), decoded_bytes.clone());

                match state.multipart.upload_part(
                    upload_id,
                    part_num,
                    vol_offset,
                    decoded_bytes.len() as u64,
                    raw_md5,
                    hi,
                    lo,
                    hex.clone(),
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

                    // Standard PutObject
                    let is_chunked = req.headers().get("x-amz-content-sha256").and_then(|v| v.to_str().ok()) == Some("STREAMING-AWS4-HMAC-SHA256-PAYLOAD");
                    let raw_bytes = req.into_body().collect().await?.to_bytes();
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
                    let (etag_hi, etag_lo, etag_hex, _) = compute_md5(&decoded_bytes);

                    let vol_offset = state.allocate_offset(decoded_bytes.len());
                    state.payload_store.write().insert((b_id, vol_offset), decoded_bytes);

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

                    let flags = unpack_flags(meta.bucket_flags);
                    let is_multipart = flags & AWAN_FLAG_MULTIPART != 0;

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

                    // Fetch payload (either single volume needle or multi-part extent stitched)
                    let payload = if is_multipart {
                        if let Some(extents) = state.multipart.get_completed_extents(b_id, &key) {
                            let pstore = state.payload_store.read();
                            let mut stitched = Vec::with_capacity(meta.size as usize);
                            for ext in extents {
                                if let Some(chunk) = pstore.get(&(b_id, ext.vol_offset)) {
                                    stitched.extend_from_slice(chunk);
                                }
                            }
                            Bytes::from(stitched)
                        } else {
                            Bytes::from(vec![0u8; meta.size as usize])
                        }
                    } else {
                        state
                            .payload_store
                            .read()
                            .get(&(b_id, meta.vol_offset))
                            .cloned()
                            .unwrap_or_else(|| Bytes::from(vec![0u8; meta.size as usize]))
                    };

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
                    if query_map.contains_key("lifecycle") {
                        state.lifecycle.delete_rules(&bucket);
                        return Ok(Response::builder()
                            .status(StatusCode::NO_CONTENT)
                            .header(CONTENT_LENGTH, "0")
                            .body(empty_body())
                            .unwrap());
                    }

                    let mut engine = state.engine.write();
                    let _ = engine.delete_object(b_id, key.as_bytes());
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
