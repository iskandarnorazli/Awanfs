use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, LAST_MODIFIED};
use hyper::{HeaderMap, Response, StatusCode};
use sha2::Digest;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::ffi_bridge::{
    unpack_flags, unpack_part_count, AWAN_FLAG_ACTIVE, AWAN_FLAG_MULTIPART, AWAN_FLAG_ZERO_BYTE,
};
use crate::iam::Iam;
use crate::precondition::{
    format_http_date, precondition, ObjectFacts, PreconditionOutcome, DEFAULT_LAST_MODIFIED_SECS,
};
use crate::s3_api::{
    bad_request, drain_body, empty_body, engine_error_response, full_body,
    internal_error_response, md5_to_etag_hex, parse_range_header, percent_decode_path, read_body_admitted,
    ChunksBody, ResponseBody, S3ServiceState, CHUNK_FRAMING_SLACK, FORMAT_2_FLAGS, MAX_OBJECT_BODY,
    STREAM_PUT_MIN, VERIFY_WHILE_SENDING_MIN,
};
use crate::volume::{global_offset, VolumeError};
use crate::xml::{copy_object_result_xml, s3_error_xml};

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
    created_at_ms: u64,
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
    if let Err(code) = state.engine.write().put_object_at(b_id, key.as_bytes(), vol_offset, payload_len, etag_hi, etag_lo, 0, flags, created_at_ms) {
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
    let mut b = Response::builder()
        .status(StatusCode::OK)
        .header(ETAG, format!("\"{}\"", etag_hex))
        .header(CONTENT_LENGTH, "0");
    if let Some(v) = version_id {
        b = b.header("x-amz-version-id", v);
    }
    b.body(empty_body()).unwrap()
}

fn request_attrs(headers: &HeaderMap, key: &str) -> Result<Vec<u8>, Response<ResponseBody>> {
    let mut attrs = crate::attrs::Attrs::new();
    if let Some(ct) = headers.get(CONTENT_TYPE) {
        attrs.set(crate::attrs::TAG_CONTENT_TYPE, ct.as_bytes());
    }
    attrs
        .encode()
        .map_err(|_| bad_request("MetadataTooLarge", "Your metadata headers exceed the maximum allowed metadata size.", key))
}

pub(crate) fn null_version_of(m: &crate::meta_index::ObjectMeta) -> crate::versions::Version {
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

pub(crate) fn version_etag(v: &crate::versions::Version) -> String {
    let hex = md5_to_etag_hex(v.etag_hi, v.etag_lo);
    if v.flags & AWAN_FLAG_MULTIPART != 0 {
        format!("\"{}-{}\"", hex, v.part_count)
    } else {
        format!("\"{}\"", hex)
    }
}

fn no_such_version(key: &str) -> Response<ResponseBody> {
    let xml = s3_error_xml("NoSuchVersion", "The specified version does not exist.", key, "req-version");
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap()
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn finish_version_put(
    state: &S3ServiceState,
    b_id: u32,
    key: &str,
    ts: u64,
    flags: u16,
    vol_offset: u64,
    size: u64,
    etag_hi: u64,
    etag_lo: u64,
    part_count: u16,
) -> Result<String, Response<ResponseBody>> {
    let v = crate::versions::Version {
        ts,
        null: flags & crate::meta_index::FLAG_NULL_ID != 0,
        marker: false,
        vol_offset,
        size,
        etag_hi,
        etag_lo,
        part_count,
        flags,
    };
    match state.record_version(b_id, key, crate::versions::Event::Put(v)) {
        Ok(()) => Ok(v.id_string()),
        Err(code) => Err(engine_error_response(code, key)),
    }
}

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

fn last_modified_secs_from_ms(ms: u64) -> u64 {
    if ms == 0 {
        DEFAULT_LAST_MODIFIED_SECS
    } else {
        ms / 1000
    }
}

fn last_modified_secs_from_ns(ns: u64) -> u64 {
    if ns == 0 {
        DEFAULT_LAST_MODIFIED_SECS
    } else {
        ns / 1_000_000_000
    }
}

fn format_object_etag(etag_hi: u64, etag_lo: u64, is_multipart: bool, part_count: u16) -> String {
    let hex = md5_to_etag_hex(etag_hi, etag_lo);
    if is_multipart {
        format!("\"{}-{}\"", hex, part_count)
    } else {
        format!("\"{}\"", hex)
    }
}

fn precondition_failure_response(
    outcome: PreconditionOutcome,
    facts: Option<&ObjectFacts>,
    key: &str,
    is_head: bool,
) -> Response<ResponseBody> {
    match outcome {
        PreconditionOutcome::Proceed => unreachable!("precondition_failure_response called with Proceed"),
        PreconditionOutcome::NotModified => {
            let mut b = Response::builder().status(StatusCode::NOT_MODIFIED).header(CONTENT_LENGTH, "0");
            if let Some(f) = facts {
                b = b.header(ETAG, &f.etag).header(LAST_MODIFIED, format_http_date(f.last_modified_secs));
            }
            b.body(empty_body()).unwrap()
        }
        PreconditionOutcome::PreconditionFailed => {
            let mut b = Response::builder().status(StatusCode::PRECONDITION_FAILED);
            if let Some(f) = facts {
                b = b.header(ETAG, &f.etag).header(LAST_MODIFIED, format_http_date(f.last_modified_secs));
            }
            if is_head {
                b.header(CONTENT_LENGTH, "0").body(empty_body()).unwrap()
            } else {
                let xml = s3_error_xml(
                    "PreconditionFailed",
                    "At least one of the pre-conditions you specified did not hold",
                    key,
                    "req-precond",
                );
                b.header(CONTENT_TYPE, "application/xml").body(full_body(Bytes::from(xml))).unwrap()
            }
        }
    }
}

fn version_get_response(
    state: &S3ServiceState,
    b_id: u32,
    key: &str,
    vid: Option<&str>,
    head: bool,
    range: Option<&str>,
    headers: &HeaderMap,
) -> Response<ResponseBody> {
    let id = match vid {
        Some(s) => match crate::versions::parse_id(s) {
            Some(i) => Some(i),
            None => return no_such_version(key),
        },
        None => None,
    };
    let v = find_version(state, b_id, key, id);
    let Some(v) = v else {
        if precondition(headers, None, false) == PreconditionOutcome::PreconditionFailed {
            return precondition_failure_response(PreconditionOutcome::PreconditionFailed, None, key, head);
        }
        if id.is_some() {
            return no_such_version(key);
        }
        let xml = s3_error_xml("NoSuchKey", "The specified key does not exist.", key, "req-get-obj");
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(CONTENT_TYPE, "application/xml")
            .body(full_body(Bytes::from(xml)))
            .unwrap();
    };
    if v.marker {
        if precondition(headers, None, false) == PreconditionOutcome::PreconditionFailed {
            return precondition_failure_response(PreconditionOutcome::PreconditionFailed, None, key, head);
        }
        let (status, code, msg) = if id.is_some() {
            (StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed", "The specified method is not allowed against a delete marker.")
        } else {
            (StatusCode::NOT_FOUND, "NoSuchKey", "The specified key does not exist.")
        };
        let body = if head {
            empty_body()
        } else {
            full_body(Bytes::from(s3_error_xml(code, msg, key, "req-get-obj")))
        };
        return Response::builder()
            .status(status)
            .header(CONTENT_TYPE, "application/xml")
            .header("x-amz-delete-marker", "true")
            .header("x-amz-version-id", v.id_string())
            .body(body)
            .unwrap();
    }
    let etag_val = version_etag(&v);
    let lm_secs = last_modified_secs_from_ns(v.ts);
    let facts = ObjectFacts {
        etag: etag_val.clone(),
        last_modified_secs: lm_secs,
    };
    match precondition(headers, Some(&facts), false) {
        PreconditionOutcome::Proceed => {}
        outcome => return precondition_failure_response(outcome, Some(&facts), key, head),
    }
    let bytes = if head {
        Bytes::new()
    } else {
        match state.read_version_bytes(b_id, key, &v) {
            Ok(b) => b,
            Err(e) => return internal_error_response(key, &format!("could not read the version: {:?}", e)),
        }
    };
    let view = match state.object_view(v.vol_offset, v.flags) {
        Ok(view) => view,
        Err(e) => return internal_error_response(key, &format!("could not read the version's attributes: {:?}", e)),
    };
    let builder = Response::builder()
        .header(CONTENT_TYPE, view.content_type())
        .header(ETAG, etag_val)
        .header(ACCEPT_RANGES, "bytes")
        .header("x-amz-version-id", v.id_string())
        .header(LAST_MODIFIED, crate::clock::format_http_date(v.ts));
    if let Some(h) = range {
        match parse_range_header(h, v.size) {
            Ok(Some(r)) => {
                let body = if head {
                    empty_body()
                } else {
                    full_body(bytes.slice(r.start as usize..(r.end + 1) as usize))
                };
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
                    .body(full_body(Bytes::from(s3_error_xml(
                        "InvalidRange",
                        "The requested range cannot be satisfied.",
                        key,
                        "req-range-err",
                    ))))
                    .unwrap();
            }
            Ok(None) => {}
        }
    }
    let body = if head { empty_body() } else { full_body(bytes) };
    builder.status(StatusCode::OK).header(CONTENT_LENGTH, v.size.to_string()).body(body).unwrap()
}

pub async fn copy_object(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let copy_source = headers.get("x-amz-copy-source").expect("CopyObject requires x-amz-copy-source");
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

    let mut src_view: Result<crate::attrs::Attrs, VolumeError> = Ok(crate::attrs::Attrs::new());
    let read = match (&src_vid, &src_meta) {
        (Some(vid), _) => {
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
            let src_facts = ObjectFacts {
                etag: version_etag(&v),
                last_modified_secs: last_modified_secs_from_ns(v.ts),
            };
            if precondition(headers, Some(&src_facts), true) != PreconditionOutcome::Proceed {
                return Ok(precondition_failure_response(PreconditionOutcome::PreconditionFailed, None, key, false));
            }
            src_view = state.object_attrs(v.vol_offset, v.flags);
            state
                .read_version_bytes(src_b_id, src_key, &v)
                .map(|b| (b, v.size, v.etag_hi, v.etag_lo, Some(v.id_string())))
        }
        (None, Some(m)) => {
            let flags = unpack_flags(m.bucket_flags);
            let is_mp = flags & AWAN_FLAG_MULTIPART != 0;
            let src_facts = ObjectFacts {
                etag: format_object_etag(m.etag_hi, m.etag_lo, is_mp, unpack_part_count(m.bucket_flags)),
                last_modified_secs: last_modified_secs_from_ms(m.created_at),
            };
            if precondition(headers, Some(&src_facts), true) != PreconditionOutcome::Proceed {
                return Ok(precondition_failure_response(PreconditionOutcome::PreconditionFailed, None, key, false));
            }
            src_view = state.object_attrs(m.vol_offset, flags);
            state
                .read_object_bytes(src_b_id, src_key, m.vol_offset, m.size, flags)
                .map(|b| (b, m.size, m.etag_hi, m.etag_lo, None))
        }
        (None, None) => unreachable!("src_meta is read whenever no version is named"),
    };
    let (bytes, src_size, src_etag_hi, src_etag_lo, copied_vid) = match read {
        Ok(r) => r,
        Err(e) => return Ok(internal_error_response(key, &format!("could not read the copy source: {:?}", e))),
    };
    let replace = headers
        .get("x-amz-metadata-directive")
        .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"REPLACE"));
    let copy_attrs = if replace {
        match request_attrs(headers, key) {
            Ok(a) => a,
            Err(resp) => return Ok(resp),
        }
    } else {
        match src_view.and_then(|a| a.encode().map_err(|_| VolumeError::CorruptIndex)) {
            Ok(a) => a,
            Err(e) => return Ok(internal_error_response(key, &format!("could not read the copy source's attributes: {:?}", e))),
        }
    };
    let flags = if bytes.is_empty() {
        AWAN_FLAG_ACTIVE | AWAN_FLAG_ZERO_BYTE
    } else {
        AWAN_FLAG_ACTIVE
    };
    let _key_guard = state.lock_key(b_id, key).await;
    if let Err(code) = state.engine.read().has_room_for(b_id, key.as_bytes()) {
        return Ok(engine_error_response(code, key));
    }
    let old_info = state.engine.read().get_object(b_id, key.as_bytes()).ok().map(|m| m.vol_offset);
    let stamp = state.version_stamp(b_id, key);
    let flags = flags | stamp.map_or(0, |s| s.0);
    let created_ns = stamp.map(|s| s.1).unwrap_or_else(|| state.clock.now_ns());
    let (vol_offset, flags) = match state
        .store_object(b_id, bucket, key, &copy_attrs, &bytes, flags, src_etag_hi, src_etag_lo, Some(created_ns))
        .await
    {
        Ok(written) => written,
        Err(e) => return Ok(internal_error_response(key, &format!("could not store the copy: {:?}", e))),
    };
    if let Some((_, ts)) = stamp {
        let vid = match finish_version_put(&state, b_id, key, ts, flags, vol_offset, src_size, src_etag_hi, src_etag_lo, 0) {
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
            .body(full_body(Bytes::from(copy_object_result_xml(&etag_hex, &crate::versions::iso8601(ts)))))
            .unwrap());
    }
    if let Some(prev_off) = old_info {
        if prev_off > 0 && prev_off != vol_offset {
            state.payload_cache.remove(b_id, prev_off);
            state.needle_crc_map.write().remove(&(b_id, prev_off));
        }
    }
    if let Err(code) = state.engine.write().put_object_at(
        b_id,
        key.as_bytes(),
        vol_offset,
        src_size,
        src_etag_hi,
        src_etag_lo,
        0,
        flags,
        created_ns / 1_000_000,
    ) {
        return Ok(engine_error_response(code, key));
    }

    let etag_hex = md5_to_etag_hex(src_etag_hi, src_etag_lo);
    let xml = copy_object_result_xml(&etag_hex, &crate::versions::iso8601(created_ns));
    let resp = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml")
        .header(ETAG, format!("\"{}\"", etag_hex));
    let resp = match &copied_vid {
        Some(v) => resp.header("x-amz-copy-source-version-id", v.as_str()),
        None => resp,
    };
    Ok(resp.body(full_body(Bytes::from(xml))).unwrap())
}

pub async fn put_object(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    key: &str,
    path: &str,
    headers: &HeaderMap,
    iam: Option<Arc<Iam>>,
    body: hyper::body::Incoming,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let if_none_match_star = headers
        .get("If-None-Match")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim() == "*")
        .unwrap_or(false);
    if if_none_match_star {
        let engine = state.engine.read();
        if engine.get_object(b_id, key.as_bytes()).is_ok() {
            let xml = s3_error_xml("PreconditionFailed", "At least one of the pre-conditions you specified did not hold.", key, "req-inm");
            return Ok(Response::builder()
                .status(StatusCode::PRECONDITION_FAILED)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap());
        }
    }

    let expected_len = headers.get(CONTENT_LENGTH).and_then(|v| v.to_str().ok()).and_then(|s| s.parse::<usize>().ok());
    let is_chunked = crate::auth::is_aws_chunked(headers);
    let content_md5_hdr = headers.get("content-md5").and_then(|v| v.to_str().ok()).map(|s| s.trim().to_string());
    let signed_headers = headers.clone();
    let put_attrs = match request_attrs(&signed_headers, key) {
        Ok(a) => a,
        Err(resp) => return Ok(resp),
    };
    let stream_len = if is_chunked {
        signed_headers
            .get("x-amz-decoded-content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
    } else {
        expected_len.map(|n| n as u64)
    };
    if let Some(len) = stream_len.filter(|&n| n > STREAM_PUT_MIN) {
        let base_flags = if key.ends_with('/') {
            AWAN_FLAG_ACTIVE | AWAN_FLAG_ZERO_BYTE
        } else {
            AWAN_FLAG_ACTIVE
        };
        if if_none_match_star && state.engine.read().get_object(b_id, key.as_bytes()).is_ok() {
            let xml = s3_error_xml("PreconditionFailed", "At least one of the pre-conditions you specified did not hold.", key, "req-inm");
            return Ok(Response::builder()
                .status(StatusCode::PRECONDITION_FAILED)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap());
        }
        if let Err(code) = state.engine.read().has_room_for(b_id, key.as_bytes()) {
            return Ok(engine_error_response(code, key));
        }
        let ticket = state.streams.enter(b_id, key);
        let begin_flags = base_flags | state.version_stamp(b_id, key).map_or(0, |s| s.0);
        let mut writer = match state.begin_needle_on_active(b_id, key, &put_attrs, len, begin_flags) {
            Ok(w) => w,
            Err(e) => return Ok(internal_error_response(key, &format!("could not store the object: {:?}", e))),
        };
        let declared_sha = signed_headers
            .get("x-amz-content-sha256")
            .and_then(|v| v.to_str().ok())
            .filter(|h| h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit()))
            .map(|h| h.to_ascii_lowercase());
        let mut sha = declared_sha.as_ref().map(|_| sha2::Sha256::new());
        let mut digest = crate::digest::StreamingDigest::new();
        let mut body = body;
        let mut verifier = crate::auth::ChunkVerifier::for_request_with(&state.auth, iam.as_deref(), &signed_headers);
        let cache = &state.sigv4_cache;
        let mut decoder = is_chunked.then(|| {
            crate::aws_chunked::ChunkedDecoder::new(move |sha: &str, sig: Option<&str>| {
                verifier.as_mut().map_or(true, |v| v.check_digest(cache, sha, sig))
            })
        });
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
                        drain_body(&mut body, 64 << 20).await;
                        let status = if e == "SignatureDoesNotMatch" {
                            StatusCode::FORBIDDEN
                        } else {
                            StatusCode::BAD_REQUEST
                        };
                        return Ok(Response::builder()
                            .status(status)
                            .header(CONTENT_TYPE, "application/xml")
                            .body(full_body(Bytes::from(s3_error_xml(e, "Error decoding chunked body", key, "req-put-obj"))))
                            .unwrap());
                    }
                    Err(crate::aws_chunked::PushError::Sink(e)) if e.starts_with("more bytes") => {
                        return Ok(bad_request("IncompleteBody", "The body is longer than its declared length.", key));
                    }
                    Err(crate::aws_chunked::PushError::Sink(e)) => {
                        return Ok(internal_error_response(key, &format!("could not store the object: {}", e)));
                    }
                }
            }
        }
        if decoder.as_ref().is_some_and(|d| d.finish().is_err()) || writer.written() != len {
            return Ok(bad_request(
                "IncompleteBody",
                "You did not provide the number of bytes specified by the Content-Length HTTP header.",
                key,
            ));
        }
        if let (Some(h), Some(want)) = (sha, declared_sha) {
            let got: String = h.finalize().iter().map(|b| format!("{:02x}", b)).collect();
            if got != want {
                return Ok(bad_request(
                    "XAmzContentSHA256Mismatch",
                    "The provided 'x-amz-content-sha256' header does not match what was computed.",
                    key,
                ));
            }
        }
        let (etag_hi, etag_lo, etag_hex, raw_md5, _) = digest.finalize();
        if let Some(ref content_md5) = content_md5_hdr {
            if content_md5 != &crate::digest::base64_encode(&raw_md5) {
                return Ok(bad_request("BadDigest", "The Content-MD5 you specified did not match what we received.", key));
            }
        }
        let _key_guard = state.lock_key(b_id, key).await;
        if if_none_match_star && state.engine.read().get_object(b_id, key.as_bytes()).is_ok() {
            return Ok(Response::builder()
                .status(StatusCode::PRECONDITION_FAILED)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(s3_error_xml(
                    "PreconditionFailed",
                    "At least one of the pre-conditions you specified did not hold.",
                    key,
                    "req-inm",
                ))))
                .unwrap());
        }
        if let Err(code) = state.engine.read().has_room_for(b_id, key.as_bytes()) {
            return Ok(engine_error_response(code, key));
        }
        let old_info = state.engine.read().get_object(b_id, key.as_bytes()).ok().map(|m| (m.vol_offset, m.size));
        let stamp = state.version_stamp(b_id, key);
        let attrs_flag = if put_attrs.is_empty() { 0 } else { crate::meta_index::FLAG_ATTRS };
        let flags = base_flags | attrs_flag | stamp.map_or(0, |s| s.0);
        if flags & FORMAT_2_FLAGS != 0 && writer.volume().format() < 2 {
            return Ok(internal_error_response(
                key,
                "the bucket's versioning changed while the object was uploaded; send it again",
            ));
        }
        writer.set_flags(flags);
        let created_ns = stamp.map(|s| s.1).unwrap_or_else(|| state.clock.now_ns());
        writer.set_created_at(created_ns);
        let first_vm = Arc::clone(writer.volume());
        let first_offset = global_offset(first_vm.id(), writer.offset());
        let first_loc = match writer.finish(etag_hi, etag_lo) {
            Ok(l) => l,
            Err(e) => {
                state.streams.note_append(b_id, key, u64::MAX);
                return Ok(internal_error_response(key, &format!("could not store the object: {:?}", e)));
            }
        };
        state.streams.note_append(b_id, key, first_offset);
        let (vm, loc, vol_offset) = if stamp.is_none() && ticket.appended_after(first_offset) {
            let moved = {
                let (st, k, fvm) = (Arc::clone(&state), key.to_string(), Arc::clone(&first_vm));
                tokio::task::spawn_blocking(move || st.relocate_needle(b_id, &k, &fvm, &first_loc, flags, etag_hi, etag_lo)).await
            };
            match moved {
                Ok(Ok(moved)) => moved,
                Ok(Err(e)) => return Ok(internal_error_response(key, &format!("could not store the object: {:?}", e))),
                Err(e) => return Ok(internal_error_response(key, &format!("could not store the object: {:?}", e))),
            }
        } else {
            (first_vm, first_loc, first_offset)
        };
        if let Err(e) = state.make_durable(&vm, loc.seq).await {
            return Ok(internal_error_response(key, &format!("could not sync the object: {:?}", e)));
        }
        drop(ticket);
        state.needle_crc_map.write().insert((b_id, vol_offset), loc.payload_crc32c);
        state.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
        state.hook("put_stored", key);
        if let Some((_, ts)) = stamp {
            return Ok(match finish_version_put(&state, b_id, key, ts, flags, vol_offset, len, etag_hi, etag_lo, 0) {
                Ok(vid) => put_ok_response(&etag_hex, Some(&vid)),
                Err(r) => r,
            });
        }
        return Ok(finish_put(&state, b_id, key, old_info, vol_offset, len, etag_hi, etag_lo, flags, &etag_hex, created_ns / 1_000_000));
    }
    let (raw_bytes, _body_guard) = match read_body_admitted(&state.body_budget, body, MAX_OBJECT_BODY + CHUNK_FRAMING_SLACK, expected_len.map(|n| n as u64), path).await? {
        Ok(b) => b,
        Err(resp) => return Ok(resp),
    };
    if !crate::auth::payload_hash_ok(&signed_headers, &raw_bytes) {
        let xml = s3_error_xml(
            "XAmzContentSHA256Mismatch",
            "The provided 'x-amz-content-sha256' header does not match what was computed.",
            key,
            "req-sha256",
        );
        return Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header(CONTENT_TYPE, "application/xml")
            .body(full_body(Bytes::from(xml)))
            .unwrap());
    }

    if let Some(exp) = expected_len {
        if raw_bytes.len() < exp {
            let xml = s3_error_xml(
                "IncompleteBody",
                "You did not provide the number of bytes specified by the Content-Length HTTP header.",
                key,
                "req-inc",
            );
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
        match crate::aws_chunked::decode_aws_chunked_checked(&raw_bytes, |sha, sig| {
            verifier.as_mut().map_or(true, |v| v.check_digest(cache, sha, sig))
        }) {
            Ok(b) => b,
            Err(e) => {
                let xml = s3_error_xml(e, "Error decoding chunked body", key, "req-put-obj");
                return Ok(Response::builder()
                    .status(if e == "SignatureDoesNotMatch" {
                        StatusCode::FORBIDDEN
                    } else {
                        StatusCode::BAD_REQUEST
                    })
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

    if let Some(ref content_md5) = content_md5_hdr {
        let expected_b64 = crate::digest::base64_encode(&raw_md5);
        if content_md5 != &expected_b64 {
            let xml = s3_error_xml("BadDigest", "The Content-MD5 you specified did not match what we received.", key, "req-bad-digest");
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

    let _key_guard = state.lock_key(b_id, key).await;
    if if_none_match_star && state.engine.read().get_object(b_id, key.as_bytes()).is_ok() {
        let xml = s3_error_xml("PreconditionFailed", "At least one of the pre-conditions you specified did not hold.", key, "req-inm");
        return Ok(Response::builder()
            .status(StatusCode::PRECONDITION_FAILED)
            .header(CONTENT_TYPE, "application/xml")
            .body(full_body(Bytes::from(xml)))
            .unwrap());
    }
    if let Err(code) = state.engine.read().has_room_for(b_id, key.as_bytes()) {
        return Ok(engine_error_response(code, key));
    }
    let old_info = {
        let engine = state.engine.read();
        engine.get_object(b_id, key.as_bytes()).ok().map(|m| (m.vol_offset, m.size))
    };

    let stamp = state.version_stamp(b_id, key);
    let flags = flags | stamp.map_or(0, |s| s.0);
    let created_ns = stamp.map(|s| s.1).unwrap_or_else(|| state.clock.now_ns());
    let (vol_offset, flags) = match state
        .store_object(b_id, bucket, key, &put_attrs, &decoded_bytes, flags, etag_hi, etag_lo, Some(created_ns))
        .await
    {
        Ok(written) => written,
        Err(e) => return Ok(internal_error_response(key, &format!("could not store the object: {:?}", e))),
    };
    state.hook("put_stored", key);
    if let Some((_, ts)) = stamp {
        return Ok(match finish_version_put(&state, b_id, key, ts, flags, vol_offset, payload_len, etag_hi, etag_lo, 0) {
            Ok(vid) => put_ok_response(&final_etag_hex, Some(&vid)),
            Err(r) => r,
        });
    }
    Ok(finish_put(&state, b_id, key, old_info, vol_offset, payload_len, etag_hi, etag_lo, flags, &final_etag_hex, created_ns / 1_000_000))
}

pub async fn get_or_head_object(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    key: &str,
    query_map: &HashMap<String, String>,
    headers: &HeaderMap,
    is_head: bool,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let vid = query_map.get("versionId").map(|s| s.as_str());
    if vid.is_some() || state.versions.read().has(b_id, key) {
        let range = headers.get("Range").and_then(|v| v.to_str().ok());
        return Ok(version_get_response(&state, b_id, key, vid, is_head, range, headers));
    }
    let (meta, is_multipart) = {
        let engine = state.engine.read();
        let m = match engine.get_object(b_id, key.as_bytes()) {
            Ok(m) => m,
            Err(_) => {
                if precondition(headers, None, false) == PreconditionOutcome::PreconditionFailed {
                    return Ok(precondition_failure_response(PreconditionOutcome::PreconditionFailed, None, key, is_head));
                }
                let xml = s3_error_xml("NoSuchKey", "The specified key does not exist.", key, "req-get-obj");
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
    let view = match state.object_view(meta.vol_offset, unpack_flags(meta.bucket_flags)) {
        Ok(v) => v,
        Err(e) => return Ok(internal_error_response(key, &format!("could not read the object's attributes: {:?}", e))),
    };
    let is_compressed = unpack_flags(meta.bucket_flags) & crate::meta_index::FLAG_COMPRESSED != 0;

    let etag_val = format_object_etag(meta.etag_hi, meta.etag_lo, is_multipart, unpack_part_count(meta.bucket_flags));
    let lm_secs = last_modified_secs_from_ms(meta.created_at);
    let lm_str = format_http_date(lm_secs);
    let facts = ObjectFacts {
        etag: etag_val.clone(),
        last_modified_secs: lm_secs,
    };
    match precondition(headers, Some(&facts), false) {
        PreconditionOutcome::Proceed => {}
        outcome => return Ok(precondition_failure_response(outcome, Some(&facts), key, is_head)),
    }

    let range_hdr = headers.get("Range").and_then(|v| v.to_str().ok());
    let range_res = range_hdr.map(|h| parse_range_header(h, meta.size));

    if let Some(range_opt) = range_res {
        match range_opt {
            Ok(Some(range)) => {
                if !is_head && !is_multipart && !is_compressed && meta.size > VERIFY_WHILE_SENDING_MIN {
                    let len = range.end - range.start + 1;
                    let body = match state.large_payload_body(
                        b_id,
                        format!("{}/{}", bucket, key),
                        meta.vol_offset,
                        meta.size,
                        range.start,
                        len,
                        false,
                    ) {
                        Ok(b) => b,
                        Err(e) => return Ok(internal_error_response(key, &format!("could not read the object: {:?}", e))),
                    };
                    return Ok(Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(CONTENT_TYPE, view.content_type())
                        .header(ETAG, etag_val)
                        .header(ACCEPT_RANGES, "bytes")
                        .header(CONTENT_RANGE, format!("bytes {}-{}/{}", range.start, range.end, meta.size))
                        .header(CONTENT_LENGTH, len.to_string())
                        .header(LAST_MODIFIED, crate::clock::format_http_date_ms(meta.created_at))
                        .body(body.boxed())
                        .unwrap());
                }
                let sub_bytes = if is_head {
                    Bytes::new()
                } else if is_multipart {
                    if let Some(extents) = state.multipart.get_completed_extents(b_id, key) {
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
                            let mut chunks = std::collections::VecDeque::new();
                            let mut p_start = 0u64;
                            for ext in extents {
                                let p_end = ext.cumulative_end;
                                if p_end > range.start && p_start <= range.end {
                                    let chunk = match state.part_bytes(b_id, &ext) {
                                        Ok(b) => b,
                                        Err(e) => return Ok(internal_error_response(key, &format!("could not read a part: {:?}", e))),
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
                                return Ok(internal_error_response(key, "a part of the object is missing"));
                            }
                            return Ok(Response::builder()
                                .status(StatusCode::PARTIAL_CONTENT)
                                .header(CONTENT_TYPE, view.content_type())
                                .header(ETAG, etag_val)
                                .header(ACCEPT_RANGES, "bytes")
                                .header(CONTENT_RANGE, format!("bytes {}-{}/{}", range.start, range.end, meta.size))
                                .header(CONTENT_LENGTH, (range.end - range.start + 1).to_string())
                                .header(LAST_MODIFIED, crate::clock::format_http_date_ms(meta.created_at))
                                .body(ChunksBody(chunks).boxed())
                                .unwrap());
                        }
                    } else {
                        return Ok(internal_error_response(key, "the multipart object's part list is missing"));
                    }
                } else if is_compressed {
                    match state.load_compressed_range(b_id, meta.vol_offset, meta.size, range.start, range.end) {
                        Ok(b) => b,
                        Err(e) => return Ok(internal_error_response(key, &format!("could not read the object: {:?}", e))),
                    }
                } else {
                    let loaded = if meta.size > VERIFY_WHILE_SENDING_MIN {
                        state.load_payload_unverified(b_id, meta.vol_offset, meta.size as usize).map(|(b, _)| b)
                    } else {
                        state.load_payload(b_id, meta.vol_offset, meta.size as usize)
                    };
                    let full = match loaded {
                        Ok(b) => b,
                        Err(e) => return Ok(internal_error_response(key, &format!("could not read the object: {:?}", e))),
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

                return Ok(Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header(CONTENT_TYPE, view.content_type())
                    .header(ETAG, etag_val)
                    .header(ACCEPT_RANGES, "bytes")
                    .header(CONTENT_RANGE, content_range)
                    .header(CONTENT_LENGTH, (range.end - range.start + 1).to_string())
                    .header(LAST_MODIFIED, crate::clock::format_http_date_ms(meta.created_at))
                    .body(body)
                    .unwrap());
            }
            Err(()) => {
                let xml = s3_error_xml("InvalidRange", "The requested range cannot be satisfied.", key, "req-range-err");
                let body = if is_head { empty_body() } else { full_body(Bytes::from(xml)) };
                return Ok(Response::builder()
                    .status(StatusCode::RANGE_NOT_SATISFIABLE)
                    .header(CONTENT_TYPE, "application/xml")
                    .header(CONTENT_RANGE, format!("bytes */{}", meta.size))
                    .body(body)
                    .unwrap());
            }
            Ok(None) => {}
        }
    }

    if is_multipart && !is_head {
        let extents = match state.multipart.get_completed_extents(b_id, key) {
            Some(e) => e,
            None => return Ok(internal_error_response(key, "the multipart object's part list is missing")),
        };
        let mut chunks = std::collections::VecDeque::with_capacity(extents.len());
        for ext in extents {
            let chunk = match state.part_bytes(b_id, &ext) {
                Ok(b) => b,
                Err(e) => return Ok(internal_error_response(key, &format!("could not read a part: {:?}", e))),
            };
            let n = (ext.part_size as usize).min(chunk.len());
            chunks.push_back(chunk.slice(..n));
        }
        if chunks.iter().map(|c| c.len() as u64).sum::<u64>() != meta.size {
            return Ok(internal_error_response(key, "a part of the object is missing"));
        }
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, view.content_type())
            .header(ETAG, etag_val)
            .header(ACCEPT_RANGES, "bytes")
            .header(CONTENT_LENGTH, meta.size.to_string())
            .header(LAST_MODIFIED, crate::clock::format_http_date_ms(meta.created_at))
            .body(ChunksBody(chunks).boxed())
            .unwrap());
    }

    if !is_head && !is_compressed && meta.size > VERIFY_WHILE_SENDING_MIN {
        let body = match state.large_payload_body(
            b_id,
            format!("{}/{}", bucket, key),
            meta.vol_offset,
            meta.size,
            0,
            meta.size,
            true,
        ) {
            Ok(b) => b,
            Err(e) => return Ok(internal_error_response(key, &format!("could not read the object: {:?}", e))),
        };
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, view.content_type())
            .header(ETAG, etag_val)
            .header(ACCEPT_RANGES, "bytes")
            .header(CONTENT_LENGTH, meta.size.to_string())
            .header(LAST_MODIFIED, crate::clock::format_http_date_ms(meta.created_at))
            .body(body.boxed())
            .unwrap());
    }

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
        return Ok(internal_error_response(key, "the object's bytes could not be read or failed verification"));
    }
    let body = if is_head { empty_body() } else { full_body(payload) };
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, view.content_type())
        .header(ETAG, etag_val)
        .header(ACCEPT_RANGES, "bytes")
        .header(CONTENT_LENGTH, meta.size.to_string())
        .header(LAST_MODIFIED, crate::clock::format_http_date_ms(meta.created_at))
        .body(body)
        .unwrap())
}

pub async fn delete_object(
    state: Arc<S3ServiceState>,
    b_id: u32,
    key: &str,
    query_map: &HashMap<String, String>,
) -> Result<Response<ResponseBody>, hyper::Error> {
    if let Some(vid) = query_map.get("versionId") {
        let Some(id) = crate::versions::parse_id(vid) else {
            return Ok(no_such_version(key));
        };
        return Ok(match state.delete_version(b_id, key, id).await {
            Ok(removed) => {
                let mut b = Response::builder()
                    .status(StatusCode::NO_CONTENT)
                    .header(CONTENT_LENGTH, "0")
                    .header("x-amz-version-id", vid.as_str());
                if removed.is_some_and(|v| v.marker) {
                    b = b.header("x-amz-delete-marker", "true");
                }
                b.body(empty_body()).unwrap()
            }
            Err(e) => internal_error_response(key, &e),
        });
    }
    let marker = match state.delete_key(b_id, key).await {
        Ok(m) => m,
        Err(e) => return Ok(internal_error_response(key, &e)),
    };
    let mut b = Response::builder().status(StatusCode::NO_CONTENT).header(CONTENT_LENGTH, "0");
    if let Some(m) = marker {
        b = b.header("x-amz-delete-marker", "true").header("x-amz-version-id", m);
    }
    Ok(b.body(empty_body()).unwrap())
}
