use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::header::{
    ACCEPT_RANGES, CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_LENGTH,
    CONTENT_RANGE, CONTENT_TYPE, ETAG, EXPIRES, LAST_MODIFIED,
};
use std::collections::BTreeMap;
use hyper::{HeaderMap, Response, StatusCode};
use sha2::Digest;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::ffi_bridge::{
    unpack_flags, unpack_part_count, CapacityReservation, AWAN_FLAG_ACTIVE, AWAN_FLAG_MULTIPART,
    AWAN_FLAG_ZERO_BYTE,
};
use crate::precondition::{
    format_http_date, parse_delete_conditions, precondition, write_precondition, ObjectFacts,
    PreconditionOutcome, WritePreconditionOutcome, DEFAULT_LAST_MODIFIED_SECS,
};
use crate::s3_api::{
    bad_request, drain_body, empty_body, engine_error_response, full_body,
    internal_error_response, md5_to_etag_hex, parse_range_header, percent_decode_path, read_body_admitted,
    request_timeout_response,
    ChunksBody, ResponseBody, S3ServiceState, CHUNK_FRAMING_SLACK, FORMAT_2_FLAGS, MAX_OBJECT_BODY,
    STREAM_PUT_MIN, VERIFY_WHILE_SENDING_MIN,
};
use crate::volume::{global_offset, VolumeError};
use crate::xml::{copy_object_result_xml, s3_error_xml};

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
    created_at_ms: u64,
    checksum: Option<(crate::checksum::ChecksumAlgorithm, &str)>,
    reservation: CapacityReservation,
) -> Response<ResponseBody> {
    if let Err(code) = reservation.commit_put_at(
        &state.engine,
        b_id,
        key.as_bytes(),
        vol_offset,
        payload_len,
        etag_hi,
        etag_lo,
        0,
        flags,
        created_at_ms,
    ) {
        return engine_error_response(code, key);
    }
    if let Some((prev_off, prev_sz)) = old_info {
        if prev_off > 0 && prev_off != vol_offset {
            state.payload_cache.remove(b_id, prev_off);
            let raw_len = 64 + key.len() + prev_sz as usize;
            let dead_bytes = raw_len.div_ceil(4096) as u64 * 4096;
            state.tombstone_bytes.fetch_add(dead_bytes, Ordering::Relaxed);
        }
    }
    let mut b = Response::builder()
        .status(StatusCode::OK)
        .header(ETAG, format!("\"{}\"", etag_hex))
        .header(CONTENT_LENGTH, "0");
    if let Some((alg, val)) = checksum {
        b = b.header(alg.header_name(), val);
    }
    b.body(empty_body()).unwrap()
}

/// A write whose needle is whole in the volume was indexed as on success (#184, owner decision
/// 4B): recovery will replay it, so a GET before and after a restart agree. The client is still
/// told the write failed, since a step after it did; S3 reads a 500 as "outcome unknown".
fn after_late_failure(key: &str, late: Option<VolumeError>, answer: Response<ResponseBody>) -> Response<ResponseBody> {
    match late {
        Some(e) => internal_error_response(key, &format!("could not store the object: {:?}", e)),
        None => answer,
    }
}

fn put_ok_response(etag_hex: &str, version_id: Option<&str>, checksum: Option<(crate::checksum::ChecksumAlgorithm, &str)>) -> Response<ResponseBody> {
    let mut b = Response::builder()
        .status(StatusCode::OK)
        .header(ETAG, format!("\"{}\"", etag_hex))
        .header(CONTENT_LENGTH, "0");
    if let Some(v) = version_id {
        b = b.header("x-amz-version-id", v);
    }
    if let Some((alg, val)) = checksum {
        b = b.header(alg.header_name(), val);
    }
    b.body(empty_body()).unwrap()
}

const MAX_USER_METADATA_BYTES: usize = 2048;

fn valid_header_str<'a>(val: &'a hyper::header::HeaderValue, key: &str) -> Result<&'a str, Response<ResponseBody>> {
    let bytes = val.as_bytes();
    if bytes.iter().any(|&b| b == b'\r' || b == b'\n') {
        return Err(bad_request("InvalidArgument", "Metadata header value must not contain CR or LF.", key));
    }
    val.to_str()
        .map_err(|_| bad_request("InvalidArgument", "Metadata header value must be valid UTF-8.", key))
}

/// A user metadata value as text: its bytes as UTF-8 when they are UTF-8, else one character
/// per byte (ISO-8859-1). ASCII is unchanged. CR and LF stay refused. botocore sends `\xe9` as
/// UTF-8 (0xC3 0xA9) and reads response headers as ISO-8859-1, so ceph's
/// `test_object_set_get_unicode_metadata` needs `header_bytes` to answer 0xE9.
fn meta_value_text(val: &hyper::header::HeaderValue, key: &str) -> Result<String, Response<ResponseBody>> {
    let bytes = val.as_bytes();
    if bytes.iter().any(|&b| b == b'\r' || b == b'\n') {
        return Err(bad_request("InvalidArgument", "Metadata header value must not contain CR or LF.", key));
    }
    Ok(match std::str::from_utf8(bytes) {
        Ok(text) => text.to_string(),
        Err(_) => bytes.iter().map(|&b| b as char).collect(),
    })
}

/// A metadata value's bytes on the wire: ISO-8859-1 when every character fits (one byte each,
/// how HTTP clients read header bytes), else UTF-8.
fn header_bytes(text: &str) -> hyper::header::HeaderValue {
    let bytes: Vec<u8> = if text.chars().all(|c| (c as u32) <= 0xFF) {
        text.chars().map(|c| c as u32 as u8).collect()
    } else {
        text.as_bytes().to_vec()
    };
    hyper::header::HeaderValue::from_bytes(&bytes).unwrap_or_else(|_| hyper::header::HeaderValue::from_static(""))
}

fn strip_aws_chunked(raw: &str) -> String {
    raw.split(',')
        .map(str::trim)
        .filter(|t| !t.is_empty() && !t.eq_ignore_ascii_case("aws-chunked"))
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn request_attrs_map(
    headers: &HeaderMap,
    key: &str,
    checksum: Option<(crate::checksum::ChecksumAlgorithm, &str, &str)>,
) -> Result<crate::attrs::Attrs, Response<ResponseBody>> {
    let mut attrs = crate::attrs::Attrs::new();
    if let Some(ct) = headers.get(CONTENT_TYPE) {
        attrs.set(crate::attrs::TAG_CONTENT_TYPE, valid_header_str(ct, key)?.as_bytes());
    }
    if let Some(tag_set) = crate::ops::tagging::parse_tagging_header_from(headers, key)? {
        attrs.set(crate::attrs::TAG_TAGGING, &crate::attrs::encode_tags(&tag_set));
    }
    if let Some((alg, cksum_type, val)) = checksum {
        attrs.set_checksum(alg, cksum_type, val);
    }
    if let Some(ce) = headers.get(CONTENT_ENCODING) {
        let raw_ce = valid_header_str(ce, key)?;
        let stripped = strip_aws_chunked(raw_ce);
        if !stripped.is_empty() {
            attrs.set(crate::attrs::TAG_CONTENT_ENCODING, stripped.as_bytes());
        }
    }
    for (hdr, tag) in [
        (CONTENT_DISPOSITION.as_str(), crate::attrs::TAG_CONTENT_DISPOSITION),
        (CONTENT_LANGUAGE.as_str(), crate::attrs::TAG_CONTENT_LANGUAGE),
        (CACHE_CONTROL.as_str(), crate::attrs::TAG_CACHE_CONTROL),
        (EXPIRES.as_str(), crate::attrs::TAG_EXPIRES),
        ("x-amz-website-redirect-location", crate::attrs::TAG_WEBSITE_REDIRECT),
    ] {
        if let Some(v) = headers.get(hdr) {
            attrs.set(tag, valid_header_str(v, key)?.as_bytes());
        }
    }

    let mut user_meta: BTreeMap<String, String> = BTreeMap::new();
    for (name, value) in headers.iter() {
        if let Some(suffix) = name.as_str().strip_prefix("x-amz-meta-") {
            if suffix.is_empty() {
                continue;
            }
            let val = meta_value_text(value, key)?;
            let lower = suffix.to_ascii_lowercase();
            user_meta
                .entry(lower)
                .and_modify(|existing| {
                    existing.push_str(", ");
                    existing.push_str(&val);
                })
                .or_insert(val);
        }
    }
    let total_user_meta: usize = user_meta.iter().map(|(k, v)| k.len() + v.len()).sum();
    if total_user_meta > MAX_USER_METADATA_BYTES {
        return Err(bad_request(
            "MetadataTooLarge",
            "Your metadata headers exceed the maximum allowed metadata size.",
            key,
        ));
    }
    attrs.set_user_meta(user_meta.iter().map(|(k, v)| (k.as_str(), v.as_str())));

    Ok(attrs)
}

pub(super) fn apply_owner_attr(attrs: &mut crate::attrs::Attrs, writer_account: &str, bucket_owner: &str) {
    let eff_bucket = if bucket_owner.is_empty() { crate::iam::ROOT_ACCOUNT } else { bucket_owner };
    let eff_writer = if writer_account.is_empty() { crate::iam::ROOT_ACCOUNT } else { writer_account };
    if eff_writer != eff_bucket {
        attrs.set_owner(eff_writer);
    } else {
        attrs.clear_owner();
    }
}

fn request_attrs_with_checksum(
    headers: &HeaderMap,
    key: &str,
    checksum: Option<(crate::checksum::ChecksumAlgorithm, &str, &str)>,
    writer_account: &str,
    bucket_owner: &str,
) -> Result<Vec<u8>, Response<ResponseBody>> {
    let mut attrs = request_attrs_map(headers, key, checksum)?;
    apply_owner_attr(&mut attrs, writer_account, bucket_owner);
    attrs
        .encode()
        .map_err(|_| bad_request("MetadataTooLarge", "Your metadata headers exceed the maximum allowed metadata size.", key))
}

fn with_tagging_count(b: hyper::http::response::Builder, count: usize) -> hyper::http::response::Builder {
    if count > 0 {
        b.header("x-amz-tagging-count", count.to_string())
    } else {
        b
    }
}

fn request_attrs(headers: &HeaderMap, key: &str) -> Result<Vec<u8>, Response<ResponseBody>> {
    request_attrs_with_checksum(headers, key, None, "", "")
}

pub(crate) fn checksum_mode_enabled(headers: &HeaderMap) -> bool {
    headers
        .get("x-amz-checksum-mode")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("ENABLED"))
}

pub(crate) fn with_checksum_headers(
    mut builder: hyper::http::response::Builder,
    attrs: &crate::attrs::Attrs,
    mode_enabled: bool,
) -> hyper::http::response::Builder {
    if mode_enabled {
        if let Some((alg, cksum_type, val)) = attrs.checksum() {
            builder = builder.header(alg.header_name(), val).header("x-amz-checksum-type", cksum_type);
        }
    }
    builder
}

fn with_part_checksum_headers(
    mut builder: hyper::http::response::Builder,
    attrs: &crate::attrs::Attrs,
    part_checksum: Option<&(crate::checksum::ChecksumAlgorithm, String)>,
) -> hyper::http::response::Builder {
    if let Some((alg, val)) = part_checksum {
        let cksum_type = attrs.checksum().map(|(_, ct, _)| ct).unwrap_or("COMPOSITE");
        builder = builder.header(alg.header_name(), val.as_str()).header("x-amz-checksum-type", cksum_type);
    }
    builder
}

#[derive(Default)]
struct ResponseOverrides<'a> {
    content_type: Option<&'a str>,
    content_language: Option<&'a str>,
    expires: Option<&'a str>,
    cache_control: Option<&'a str>,
    content_disposition: Option<&'a str>,
    content_encoding: Option<&'a str>,
}

impl<'a> ResponseOverrides<'a> {
    fn from_query(
        query_map: &'a HashMap<String, String>,
        key: &str,
        is_anon_with_auth: bool,
    ) -> Result<Self, Response<ResponseBody>> {
        let ov = Self {
            content_type: query_map.get("response-content-type").map(String::as_str),
            content_language: query_map.get("response-content-language").map(String::as_str),
            expires: query_map.get("response-expires").map(String::as_str),
            cache_control: query_map.get("response-cache-control").map(String::as_str),
            content_disposition: query_map.get("response-content-disposition").map(String::as_str),
            content_encoding: query_map.get("response-content-encoding").map(String::as_str),
        };
        let any = ov.content_type.is_some()
            || ov.content_language.is_some()
            || ov.expires.is_some()
            || ov.cache_control.is_some()
            || ov.content_disposition.is_some()
            || ov.content_encoding.is_some();
        if any && is_anon_with_auth {
            return Err(bad_request(
                "InvalidRequest",
                "Request specific response headers cannot be used for anonymous GET requests.",
                key,
            ));
        }
        for val in [
            ov.content_type,
            ov.content_language,
            ov.expires,
            ov.cache_control,
            ov.content_disposition,
            ov.content_encoding,
        ]
        .into_iter()
        .flatten()
        {
            if val.bytes().any(|b| b == b'\r' || b == b'\n') {
                return Err(bad_request("InvalidArgument", "Response override value must not contain CR or LF.", key));
            }
        }
        Ok(ov)
    }
}

trait MetadataHeadersExt {
    fn with_object_metadata(self, view: &crate::s3_api::ObjectView, ov: &ResponseOverrides<'_>) -> Self;
}

impl MetadataHeadersExt for hyper::http::response::Builder {
    fn with_object_metadata(mut self, view: &crate::s3_api::ObjectView, ov: &ResponseOverrides<'_>) -> Self {
        self = self.header(CONTENT_TYPE, ov.content_type.unwrap_or_else(|| view.content_type()));
        if let Some(v) = ov.content_encoding.or_else(|| view.attrs.content_encoding()) {
            self = self.header(CONTENT_ENCODING, v);
        }
        if let Some(v) = ov.content_disposition.or_else(|| view.attrs.content_disposition()) {
            self = self.header(CONTENT_DISPOSITION, v);
        }
        if let Some(v) = ov.content_language.or_else(|| view.attrs.content_language()) {
            self = self.header(CONTENT_LANGUAGE, v);
        }
        if let Some(v) = ov.cache_control.or_else(|| view.attrs.cache_control()) {
            self = self.header(CACHE_CONTROL, v);
        }
        if let Some(v) = ov.expires.or_else(|| view.attrs.expires()) {
            self = self.header(EXPIRES, v);
        }
        if let Some(v) = view.attrs.website_redirect() {
            self = self.header("x-amz-website-redirect-location", v);
        }
        for (k, v) in view.attrs.user_meta() {
            self = self.header(format!("x-amz-meta-{}", k), header_bytes(v));
        }
        self
    }
}

pub(crate) fn null_version_of(m: &crate::meta_index::ObjectMeta) -> crate::versions::Version {
    crate::versions::Version {
        ts: 0,
        modified: m.created_at.saturating_mul(1_000_000),
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

pub(crate) fn no_such_version(key: &str, head: bool) -> Response<ResponseBody> {
    let body = if head {
        empty_body()
    } else {
        let xml = s3_error_xml("NoSuchVersion", "The specified version does not exist.", key, "req-version");
        full_body(Bytes::from(xml))
    };
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(CONTENT_TYPE, "application/xml")
        .body(body)
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
    reservation: CapacityReservation,
) -> Result<String, Response<ResponseBody>> {
    let v = crate::versions::Version {
        ts,
        modified: ts,
        null: flags & crate::meta_index::FLAG_NULL_ID != 0,
        marker: false,
        vol_offset,
        size,
        etag_hi,
        etag_lo,
        part_count,
        flags,
    };
    match state.record_version_with_reservation(b_id, key, crate::versions::Event::Put(v), Some(reservation)) {
        Ok(()) => Ok(v.id_string()),
        Err(code) => Err(engine_error_response(code, key)),
    }
}

pub(crate) fn find_version(state: &S3ServiceState, b_id: u32, key: &str, id: Option<u64>) -> Option<crate::versions::Version> {
    let row = state.engine.read().get_object(b_id, key.as_bytes()).ok().map(|m| null_version_of(&m));
    let vs = state.versions.read();
    match id {
        None => vs.current(b_id, key),
        Some(i) if vs.has(b_id, key) => vs.find(b_id, key, i),
        Some(0) => row,
        Some(_) => None,
    }
}

pub(crate) fn last_modified_secs_from_ms(ms: u64) -> u64 {
    if ms == 0 {
        DEFAULT_LAST_MODIFIED_SECS
    } else {
        ms / 1000
    }
}

pub(crate) fn last_modified_secs_from_ns(ns: u64) -> u64 {
    if ns == 0 {
        DEFAULT_LAST_MODIFIED_SECS
    } else {
        ns / 1_000_000_000
    }
}

pub(crate) fn format_object_etag(etag_hi: u64, etag_lo: u64, is_multipart: bool, part_count: u16) -> String {
    let hex = md5_to_etag_hex(etag_hi, etag_lo);
    if is_multipart {
        format!("\"{}-{}\"", hex, part_count)
    } else {
        format!("\"{}\"", hex)
    }
}

pub(crate) fn live_object_facts(state: &S3ServiceState, b_id: u32, key: &str) -> Option<ObjectFacts> {
    let vs = state.versions.read();
    if vs.has(b_id, key) {
        let v = vs.current(b_id, key)?;
        if v.marker {
            return None;
        }
        return Some(ObjectFacts {
            etag: version_etag(&v),
            last_modified_secs: last_modified_secs_from_ns(v.modified),
        });
    }
    drop(vs);
    let m = state.engine.read().get_object(b_id, key.as_bytes()).ok()?;
    let flags = unpack_flags(m.bucket_flags);
    let is_mp = flags & AWAN_FLAG_MULTIPART != 0;
    Some(ObjectFacts {
        etag: format_object_etag(m.etag_hi, m.etag_lo, is_mp, unpack_part_count(m.bucket_flags)),
        last_modified_secs: last_modified_secs_from_ms(m.created_at),
    })
}

pub(crate) fn check_write_precondition(
    state: &S3ServiceState,
    b_id: u32,
    key: &str,
    headers: &HeaderMap,
) -> Option<Response<ResponseBody>> {
    let current = live_object_facts(state, b_id, key);
    match write_precondition(headers, current.as_ref()) {
        WritePreconditionOutcome::Proceed => None,
        WritePreconditionOutcome::PreconditionFailed => Some(precondition_failure_response(
            PreconditionOutcome::PreconditionFailed,
            current.as_ref(),
            key,
            false,
        )),
        WritePreconditionOutcome::NoSuchKey => {
            let xml = s3_error_xml("NoSuchKey", "The specified key does not exist.", key, "req-precond");
            Some(
                Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header(CONTENT_TYPE, "application/xml")
                    .body(full_body(Bytes::from(xml)))
                    .unwrap(),
            )
        }
    }
}

pub(super) fn precondition_failure_response(
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

#[allow(clippy::too_many_arguments)]
fn version_get_response(
    state: &S3ServiceState,
    b_id: u32,
    bucket: &str,
    key: &str,
    vid: Option<&str>,
    head: bool,
    range: Option<&str>,
    part_num: Option<u32>,
    headers: &HeaderMap,
    overrides: &ResponseOverrides<'_>,
) -> Response<ResponseBody> {
    let id = match vid {
        Some(s) => match crate::versions::parse_id(s) {
            Some(i) => Some(i),
            None => return no_such_version(key, head),
        },
        None => None,
    };
    let v = find_version(state, b_id, key, id);
    let Some(v) = v else {
        if precondition(headers, None, false) == PreconditionOutcome::PreconditionFailed {
            return precondition_failure_response(PreconditionOutcome::PreconditionFailed, None, key, head);
        }
        if id.is_some() {
            return no_such_version(key, head);
        }
        let body = if head {
            empty_body()
        } else {
            let xml = s3_error_xml("NoSuchKey", "The specified key does not exist.", key, "req-get-obj");
            full_body(Bytes::from(xml))
        };
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(CONTENT_TYPE, "application/xml")
            .header("x-amz-delete-marker", "false")
            .body(body)
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
    let lm_secs = last_modified_secs_from_ns(v.modified);
    let facts = ObjectFacts {
        etag: etag_val.clone(),
        last_modified_secs: lm_secs,
    };
    match precondition(headers, Some(&facts), false) {
        PreconditionOutcome::Proceed => {}
        outcome => return precondition_failure_response(outcome, Some(&facts), key, head),
    }
    let view = match state.object_view(v.vol_offset, v.flags) {
        Ok(view) => view,
        Err(e) => return internal_error_response(key, &format!("could not read the version's attributes: {:?}", e)),
    };
    if let Some(pn) = part_num {
        if v.flags & AWAN_FLAG_MULTIPART != 0 {
            let extents = match state
                .manifest_extents(b_id, v.vol_offset)
                .or_else(|| state.multipart.get_completed_extents(b_id, key))
            {
                Some(e) => e,
                None => return internal_error_response(key, "the multipart object's part list is missing"),
            };
            let parts_count = extents.len();
            if pn < 1 || (pn as usize) > parts_count {
                let body = if head {
                    empty_body()
                } else {
                    full_body(Bytes::from(s3_error_xml("InvalidPart", "The requested part number is not valid.", key, "req-get-part")))
                };
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .header(CONTENT_TYPE, "application/xml")
                    .body(body)
                    .unwrap();
            }
            let ext = &extents[(pn - 1) as usize];
            let body = if head {
                empty_body()
            } else if v.size > VERIFY_WHILE_SENDING_MIN {
                match state.multipart_body(
                    b_id,
                    format!("{}/{} part {}", bucket, key, pn),
                    &extents,
                    ext.cumulative_end - ext.part_size,
                    ext.part_size,
                    true,
                ) {
                    Ok(b) => b.boxed(),
                    Err(e) => return internal_error_response(key, &format!("could not read a part: {:?}", e)),
                }
            } else {
                let b = match state.part_bytes(b_id, ext) {
                    Ok(b) => b,
                    Err(e) => return internal_error_response(key, &format!("could not read a part: {:?}", e)),
                };
                full_body(b.slice(..(ext.part_size as usize).min(b.len())))
            };
            return with_part_checksum_headers(
                Response::builder()
                    .status(StatusCode::OK)
                    .header(CONTENT_TYPE, view.content_type())
                    .header(ETAG, etag_val)
                    .header(ACCEPT_RANGES, "bytes")
                    .header("x-amz-mp-parts-count", parts_count.to_string())
                    .header("x-amz-version-id", v.id_string())
                    .header(CONTENT_LENGTH, ext.part_size.to_string())
                    .header(LAST_MODIFIED, crate::clock::format_http_date(v.modified)),
                &view.attrs,
                ext.checksum.as_ref(),
            )
            .body(body)
            .unwrap();
        } else if pn > 1 {
            let body = if head {
                empty_body()
            } else {
                full_body(Bytes::from(s3_error_xml("InvalidPart", "The requested part number is not valid.", key, "req-get-part")))
            };
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header(CONTENT_TYPE, "application/xml")
                .body(body)
                .unwrap();
        }
    }
    let tag_count = view.attrs.tag_count();
    let cksum_mode = checksum_mode_enabled(headers);
    let is_compressed = v.flags & crate::meta_index::FLAG_COMPRESSED != 0;
    let is_multipart = v.flags & AWAN_FLAG_MULTIPART != 0;
    let builder = with_tagging_count(
        Response::builder()
            .with_object_metadata(&view, overrides)
            .header(ETAG, etag_val)
            .header(ACCEPT_RANGES, "bytes")
            .header("x-amz-version-id", v.id_string())
            .header(LAST_MODIFIED, crate::clock::format_http_date(v.modified)),
        tag_count,
    );
    if !head && !is_compressed && v.size > VERIFY_WHILE_SENDING_MIN {
        let what = format!("{}/{}", bucket, key);
        if let Some(h) = range {
            match parse_range_header(h, v.size) {
                Ok(Some(r)) => {
                    let len = r.end - r.start + 1;
                    let body = if is_multipart {
                        match state
                            .manifest_extents(b_id, v.vol_offset)
                            .or_else(|| state.multipart.get_completed_extents(b_id, key))
                        {
                            Some(extents) => state.multipart_body(b_id, what, &extents, r.start, len, false),
                            None => return internal_error_response(key, "the multipart object's part list is missing"),
                        }
                    } else {
                        state.large_payload_body(b_id, what, v.vol_offset, v.size, r.start, len, false)
                    };
                    let body = match body {
                        Ok(b) => b,
                        Err(e) => return internal_error_response(key, &format!("could not read the version: {:?}", e)),
                    };
                    return builder
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(CONTENT_RANGE, format!("bytes {}-{}/{}", r.start, r.end, v.size))
                        .header(CONTENT_LENGTH, len.to_string())
                        .body(body.boxed())
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
        let body = if is_multipart {
            match state
                .manifest_extents(b_id, v.vol_offset)
                .or_else(|| state.multipart.get_completed_extents(b_id, key))
            {
                Some(extents) => state.multipart_body(b_id, what, &extents, 0, v.size, true),
                None => return internal_error_response(key, "the multipart object's part list is missing"),
            }
        } else {
            state.large_payload_body(b_id, what, v.vol_offset, v.size, 0, v.size, true)
        };
        let body = match body {
            Ok(b) => b,
            Err(e) => return internal_error_response(key, &format!("could not read the version: {:?}", e)),
        };
        return with_checksum_headers(builder, &view.attrs, cksum_mode)
            .status(StatusCode::OK)
            .header(CONTENT_LENGTH, v.size.to_string())
            .body(body.boxed())
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
    with_checksum_headers(builder, &view.attrs, cksum_mode)
        .status(StatusCode::OK)
        .header(CONTENT_LENGTH, v.size.to_string())
        .body(body)
        .unwrap()
}

#[allow(clippy::too_many_arguments)]
pub async fn copy_object(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
    writer_account: &str,
    bucket_owner: &str,
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

    let src_view: Result<crate::attrs::Attrs, VolumeError>;
    let (src_vol_offset, src_flags, src_size, src_etag_hi, src_etag_lo, copied_vid, version_opt) = match (&src_vid, &src_meta) {
        (Some(vid), _) => {
            let Some(id) = crate::versions::parse_id(vid) else {
                return Ok(no_such_version(src_key, false));
            };
            let Some(v) = find_version(&state, src_b_id, src_key, Some(id)) else {
                return Ok(no_such_version(src_key, false));
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
                last_modified_secs: last_modified_secs_from_ns(v.modified),
            };
            if precondition(headers, Some(&src_facts), true) != PreconditionOutcome::Proceed {
                return Ok(precondition_failure_response(PreconditionOutcome::PreconditionFailed, None, key, false));
            }
            src_view = state.object_attrs(v.vol_offset, v.flags);
            let vid_s = v.id_string();
            (v.vol_offset, v.flags, v.size, v.etag_hi, v.etag_lo, Some(vid_s), Some(v))
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
            (m.vol_offset, flags, m.size, m.etag_hi, m.etag_lo, None, None)
        }
        (None, None) => unreachable!("src_meta is read whenever no version is named"),
    };
    let stream_copy = src_size > STREAM_PUT_MIN
        && (src_flags & crate::meta_index::FLAG_COMPRESSED == 0)
        && !state.compresses(bucket);
    let bytes = if stream_copy {
        Bytes::new()
    } else {
        let read = match version_opt {
            Some(v) => state.read_version_bytes(src_b_id, src_key, &v),
            None => state.read_object_bytes(src_b_id, src_key, src_vol_offset, src_size, src_flags),
        };
        match read {
            Ok(b) => b,
            Err(e) => return Ok(internal_error_response(key, &format!("could not read the copy source: {:?}", e))),
        }
    };
    let replace = match headers.get("x-amz-metadata-directive").and_then(|v| v.to_str().ok()) {
        None => false,
        Some(v) if v.eq_ignore_ascii_case("COPY") => false,
        Some(v) if v.eq_ignore_ascii_case("REPLACE") => true,
        Some(_) => return Ok(bad_request("InvalidArgument", "Unknown metadata directive.", key)),
    };
    let tagging_replace = match headers.get("x-amz-tagging-directive").and_then(|v| v.to_str().ok()) {
        None => false,
        Some(v) if v.eq_ignore_ascii_case("COPY") => false,
        Some(v) if v.eq_ignore_ascii_case("REPLACE") => true,
        Some(_) => return Ok(bad_request("InvalidArgument", "Unknown tagging directive.", key)),
    };
    if src_bucket == bucket && src_key == key && src_vid.is_none() && !replace && !tagging_replace {
        return Ok(bad_request(
            "InvalidRequest",
            "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.",
            key,
        ));
    }
    let mut copy_attrs_obj = if replace {
        let raw = match request_attrs(headers, key) {
            Ok(a) => a,
            Err(resp) => return Ok(resp),
        };
        crate::attrs::Attrs::decode(&raw).unwrap_or_default()
    } else {
        match src_view {
            Ok(ref a) => a.clone(),
            Err(e) => return Ok(internal_error_response(key, &format!("could not read the copy source's attributes: {:?}", e))),
        }
    };
    if tagging_replace {
        match crate::ops::tagging::parse_tagging_header_from(headers, key) {
            Ok(Some(tag_set)) => {
                copy_attrs_obj.set(crate::attrs::TAG_TAGGING, &crate::attrs::encode_tags(&tag_set));
            }
            Ok(None) => copy_attrs_obj.remove(crate::attrs::TAG_TAGGING),
            Err(resp) => return Ok(resp),
        }
    } else if replace {
        let src_attrs = match src_view {
            Ok(ref a) => a,
            Err(e) => return Ok(internal_error_response(key, &format!("could not read the copy source's attributes: {:?}", e))),
        };
        match src_attrs.get(crate::attrs::TAG_TAGGING) {
            Some(raw) => copy_attrs_obj.set(crate::attrs::TAG_TAGGING, raw),
            None => copy_attrs_obj.remove(crate::attrs::TAG_TAGGING),
        }
    }
    apply_owner_attr(&mut copy_attrs_obj, writer_account, bucket_owner);
    let copy_attrs = match copy_attrs_obj.encode() {
        Ok(a) => a,
        Err(_) => return Ok(bad_request("MetadataTooLarge", "Your metadata headers exceed the maximum allowed metadata size.", key)),
    };
    let flags = if src_size == 0 {
        AWAN_FLAG_ACTIVE | AWAN_FLAG_ZERO_BYTE
    } else {
        AWAN_FLAG_ACTIVE
    };
    let _key_guard = state.lock_key(b_id, key).await;
    if let Some(resp) = check_write_precondition(&state, b_id, key, headers) {
        return Ok(resp);
    }
    let reservation = match CapacityReservation::acquire(&state.engine, b_id, key.as_bytes()) {
        Ok(r) => r,
        Err(code) => return Ok(engine_error_response(code, key)),
    };
    let old_info = state.engine.read().get_object(b_id, key.as_bytes()).ok().map(|m| m.vol_offset);
    let stamp = state.version_stamp(b_id, key);
    let flags = flags | stamp.map_or(0, |s| s.0);
    let created_ns = stamp.map(|s| s.1).unwrap_or_else(|| state.clock.now_ns());
    let store_res = if stream_copy {
        state
            .stream_copy_object(
                b_id,
                key,
                &copy_attrs,
                flags,
                src_b_id,
                src_key,
                src_vol_offset,
                src_size,
                src_flags,
                src_etag_hi,
                src_etag_lo,
                created_ns,
            )
            .await
    } else {
        state
            .store_object(b_id, bucket, key, &copy_attrs, &bytes, flags, src_etag_hi, src_etag_lo, Some(created_ns))
            .await
    };
    let (vol_offset, flags, mut late) = match store_res {
        Ok((o, f)) => (o, f, None),
        // Indexed as on success, then answered 500 (#184, owner decision 4B).
        Err(e) => match S3ServiceState::late_record(&e) {
            Some((o, f)) => (o, f, Some(e)),
            None => return Ok(internal_error_response(key, &format!("could not store the copy: {:?}", e))),
        },
    };
    if let Some((_, ts)) = stamp {
        let vid = match finish_version_put(&state, b_id, key, ts, flags, vol_offset, src_size, src_etag_hi, src_etag_lo, 0, reservation) {
            Ok(v) => v,
            Err(r) => return Ok(r),
        };
        if let Some(e) = late.take() {
            return Ok(internal_error_response(key, &format!("could not store the copy: {:?}", e)));
        }
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
    if let Err(code) = reservation.commit_put_at(
        &state.engine,
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
    if let Some(prev_off) = old_info {
        if prev_off > 0 && prev_off != vol_offset {
            state.payload_cache.remove(b_id, prev_off);
        }
    }
    if let Some(e) = late {
        return Ok(internal_error_response(key, &format!("could not store the copy: {:?}", e)));
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

/// Bounded hash-and-write pipeline driver for streamed `PutObject` requests, driven by
/// `crate::fsm::pipeline_step`.
struct StreamPipeline {
    state: crate::fsm::PipelineState,
    tx: Option<tokio::sync::mpsc::Sender<crate::work::InflightBuf>>,
    recycle_rx: Option<tokio::sync::mpsc::Receiver<crate::work::InflightBuf>>,
    writer_task: Option<tokio::task::JoinHandle<Result<crate::volume::NeedleWriter, (&'static str, String)>>>,
    cur_buf: crate::work::InflightBuf,
    work: Arc<crate::work::WorkCounters>,
    failure: Option<Result<Response<ResponseBody>, hyper::Error>>,
}

impl StreamPipeline {
    fn new(
        tx: tokio::sync::mpsc::Sender<crate::work::InflightBuf>,
        recycle_rx: tokio::sync::mpsc::Receiver<crate::work::InflightBuf>,
        writer_task: tokio::task::JoinHandle<Result<crate::volume::NeedleWriter, (&'static str, String)>>,
        work: Arc<crate::work::WorkCounters>,
        cap: usize,
    ) -> Self {
        let cur_buf = crate::work::InflightBuf::with_capacity(&work, cap);
        Self {
            state: crate::fsm::PipelineState::Filling,
            tx: Some(tx),
            recycle_rx: Some(recycle_rx),
            writer_task: Some(writer_task),
            cur_buf,
            work,
            failure: None,
        }
    }

    fn release_buffers(&mut self) {
        drop(self.tx.take());
        drop(self.recycle_rx.take());
        self.cur_buf = crate::work::InflightBuf::default();
    }

    fn step(&mut self, event: crate::fsm::PipelineEvent) -> Result<crate::fsm::PipelineState, crate::fsm::Refusal> {
        match crate::fsm::pipeline_step(self.state, event) {
            Ok(next) => {
                self.state = next;
                if next == crate::fsm::PipelineState::Failed {
                    self.release_buffers();
                }
                Ok(next)
            }
            Err(r) => {
                self.state = crate::fsm::PipelineState::Failed;
                self.release_buffers();
                Err(r)
            }
        }
    }

    fn abort(
        &mut self,
        event: crate::fsm::PipelineEvent,
        key: &str,
        resp: Result<Response<ResponseBody>, hyper::Error>,
    ) {
        self.failure = Some(match self.step(event) {
            Ok(crate::fsm::PipelineState::Failed) => resp,
            Ok(_) => Ok(internal_error_response(key, "unexpected pipeline transition")),
            Err(r) => Ok(bad_request(r.code, r.why, key)),
        });
    }

    async fn join_writer(&mut self, key: &str) -> Option<crate::volume::NeedleWriter> {
        self.release_buffers();
        let res = match self.writer_task.take() {
            Some(t) => t.await,
            None => {
                self.abort(
                    crate::fsm::PipelineEvent::WriteError,
                    key,
                    Ok(internal_error_response(key, "writer task missing")),
                );
                return None;
            }
        };
        match res {
            Ok(Ok(w)) => Some(w),
            Ok(Err(("IncompleteBody", msg))) => {
                self.abort(
                    crate::fsm::PipelineEvent::LengthMismatch,
                    key,
                    Ok(bad_request("IncompleteBody", &msg, key)),
                );
                None
            }
            Ok(Err((_, msg))) => {
                self.abort(
                    crate::fsm::PipelineEvent::WriteError,
                    key,
                    Ok(internal_error_response(key, &msg)),
                );
                None
            }
            Err(e) => {
                self.abort(
                    crate::fsm::PipelineEvent::WriteError,
                    key,
                    Ok(internal_error_response(key, &format!("writer task failed: {:?}", e))),
                );
                None
            }
        }
    }

    async fn fail_from_writer(&mut self, key: &str) {
        if let Some(w) = self.join_writer(key).await {
            drop(w);
            self.abort(
                crate::fsm::PipelineEvent::WriteError,
                key,
                Ok(internal_error_response(key, "writer task exited unexpectedly")),
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn put_object(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    key: &str,
    path: &str,
    headers: &HeaderMap,
    writer_account: &str,
    bucket_owner: &str,
    mut chunk_verifier: Option<crate::auth::ChunkVerifier>,
    body: hyper::body::Incoming,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let expected_len = headers.get(CONTENT_LENGTH).and_then(|v| v.to_str().ok()).and_then(|s| s.parse::<usize>().ok());
    let is_chunked = crate::auth::is_aws_chunked(headers);
    let has_te_chunked = headers
        .get_all(hyper::header::TRANSFER_ENCODING)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .any(|v| v.split(',').any(|t| t.trim().eq_ignore_ascii_case("chunked")));
    if expected_len.is_none() && !is_chunked && !has_te_chunked {
        let xml = s3_error_xml(
            "MissingContentLength",
            "You must provide the Content-Length HTTP header.",
            key,
            "req-put-len",
        );
        return Ok(Response::builder()
            .status(StatusCode::LENGTH_REQUIRED)
            .header(CONTENT_TYPE, "application/xml")
            .body(full_body(Bytes::from(xml)))
            .unwrap());
    }
    let mut md5_values = headers.get_all("content-md5").iter();
    let content_md5_hdr = match md5_values.next() {
        Some(v) => {
            let valid = md5_values
                .next()
                .is_none()
                .then(|| v.to_str().ok().map(str::trim))
                .flatten()
                .filter(|s| crate::bucket_config::base64_decode(s).is_ok_and(|b| b.len() == 16));
            let Some(valid_md5) = valid else {
                let xml = s3_error_xml("InvalidDigest", "The Content-MD5 you specified was invalid.", key, "req-bad-digest");
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .header(CONTENT_TYPE, "application/xml")
                    .body(full_body(Bytes::from(xml)))
                    .unwrap());
            };
            Some(valid_md5.to_string())
        }
        None => None,
    };
    let signed_headers = headers.clone();
    let req_checksum = match crate::checksum::requested_checksum(&signed_headers) {
        Ok(c) => c,
        Err(r) => return Ok(bad_request(r.code, r.why, key)),
    };
    let put_attrs = match request_attrs_with_checksum(
        &signed_headers,
        key,
        req_checksum.as_ref().and_then(|(alg, exp)| exp.as_deref().map(|v| (*alg, "FULL_OBJECT", v))),
        writer_account,
        bucket_owner,
    ) {
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
        let mut reservation = match CapacityReservation::acquire(&state.engine, b_id, key.as_bytes()) {
            Ok(r) => r,
            Err(code) => return Ok(engine_error_response(code, key)),
        };
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
        let mut digest = Some(crate::digest::StreamingDigest::new_md5_only());
        let mut cksum_hasher = req_checksum.as_ref().map(|(alg, _)| crate::checksum::Checksum::new(*alg));
        let mut body = body;
        let cache = &state.sigv4_cache;
        let mut decoder = is_chunked.then(|| {
            let mut verifier = chunk_verifier.take();
            crate::aws_chunked::ChunkedDecoder::new(move |sha: &str, sig: Option<&str>| {
                verifier.as_mut().map_or(true, |v| v.check_digest(cache, sha, sig))
            })
        });
        let idle_timeout = state.body_idle_timeout();
        const COALESCE_CAP: usize = 1024 * 1024;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<crate::work::InflightBuf>(2);
        let (recycle_tx, recycle_rx) = tokio::sync::mpsc::channel::<crate::work::InflightBuf>(2);
        drop(recycle_tx.try_send(crate::work::InflightBuf::with_capacity(&state.work, COALESCE_CAP)));
        let writer_task = tokio::task::spawn_blocking(
            move || -> Result<crate::volume::NeedleWriter, (&'static str, String)> {
                while let Some(mut buf) = rx.blocking_recv() {
                    if writer.written() + buf.len() as u64 > len {
                        return Err((
                            "IncompleteBody",
                            "The body is longer than its declared length.".to_string(),
                        ));
                    }
                    if let Err(e) = writer.write(&buf) {
                        return Err(("InternalError", format!("could not store the object: {:?}", e)));
                    }
                    buf.clear();
                    drop(recycle_tx.try_send(buf));
                }
                Ok(writer)
            },
        );
        let mut pipe = StreamPipeline::new(tx, recycle_rx, writer_task, Arc::clone(&state.work), COALESCE_CAP);
        let mut total_fed: u64 = 0;
        let mut outcome: Option<(
            crate::volume::NeedleWriter,
            u64,
            u64,
            String,
            Option<(crate::checksum::ChecksumAlgorithm, String)>,
        )> = None;
        loop {
            match pipe.state {
                crate::fsm::PipelineState::Filling => {
                    let frame = match crate::body_timeout::next_frame_with_idle(&mut body, idle_timeout).await {
                        Ok(Some(Ok(f))) => f,
                        Ok(Some(Err(e))) => {
                            pipe.abort(crate::fsm::PipelineEvent::Disconnect, key, Err(e));
                            continue;
                        }
                        Ok(None) => {
                            if let Err(r) = pipe.step(crate::fsm::PipelineEvent::EndOfBody) {
                                pipe.failure = Some(Ok(bad_request(r.code, r.why, key)));
                            }
                            continue;
                        }
                        Err(crate::body_timeout::BodyIdleTimeout) => {
                            pipe.abort(crate::fsm::PipelineEvent::Disconnect, key, Ok(request_timeout_response(key)));
                            continue;
                        }
                    };
                    if let Ok(data) = frame.into_data() {
                        match pipe.step(crate::fsm::PipelineEvent::Frame) {
                            Ok(crate::fsm::PipelineState::Filling) => {}
                            Ok(_) => {
                                pipe.abort(
                                    crate::fsm::PipelineEvent::WriteError,
                                    key,
                                    Ok(internal_error_response(key, "unexpected pipeline state")),
                                );
                                continue;
                            }
                            Err(r) => {
                                pipe.failure = Some(Ok(bad_request(r.code, r.why, key)));
                                continue;
                            }
                        }
                        if let Some(h) = sha.as_mut() {
                            h.update(&data);
                        }
                        let mut ready_bufs: Vec<crate::work::InflightBuf> = Vec::new();
                        let cur_buf = &mut pipe.cur_buf;
                        let recycle_rx = &mut pipe.recycle_rx;
                        let work = &pipe.work;
                        let digest_ref = digest.as_mut().expect("digest present while filling");
                        let mut append_decoded = |mut d: &[u8]| -> Result<(), String> {
                            if total_fed + d.len() as u64 > len {
                                return Err("more bytes than the declared length".to_string());
                            }
                            total_fed += d.len() as u64;
                            digest_ref.update(d);
                            if let Some(h) = cksum_hasher.as_mut() {
                                h.update(d);
                            }
                            while !d.is_empty() {
                                let room = COALESCE_CAP - cur_buf.len();
                                let n = room.min(d.len());
                                cur_buf.extend_from_slice(&d[..n]);
                                d = &d[n..];
                                if cur_buf.len() == COALESCE_CAP {
                                    let next = recycle_rx
                                        .as_mut()
                                        .and_then(|rx| rx.try_recv().ok())
                                        .unwrap_or_else(|| crate::work::InflightBuf::with_capacity(work, COALESCE_CAP));
                                    ready_bufs.push(std::mem::replace(cur_buf, next));
                                }
                            }
                            Ok(())
                        };
                        let fed = match decoder.as_mut() {
                            Some(dec) => dec.push(&data, &mut append_decoded),
                            None => append_decoded(&data).map_err(crate::aws_chunked::PushError::Sink),
                        };
                        match fed {
                            Ok(()) => {}
                            Err(crate::aws_chunked::PushError::Decode(e)) => {
                                let status = if e == "SignatureDoesNotMatch" {
                                    StatusCode::FORBIDDEN
                                } else {
                                    StatusCode::BAD_REQUEST
                                };
                                let resp = Response::builder()
                                    .status(status)
                                    .header(CONTENT_TYPE, "application/xml")
                                    .body(full_body(Bytes::from(s3_error_xml(e, "Error decoding chunked body", key, "req-put-obj"))))
                                    .unwrap();
                                pipe.abort(crate::fsm::PipelineEvent::HashError, key, Ok(resp));
                                drain_body(&mut body, 64 << 20).await;
                                continue;
                            }
                            Err(crate::aws_chunked::PushError::Sink(e)) if e.starts_with("more bytes") => {
                                pipe.abort(
                                    crate::fsm::PipelineEvent::LengthMismatch,
                                    key,
                                    Ok(bad_request("IncompleteBody", "The body is longer than its declared length.", key)),
                                );
                                continue;
                            }
                            Err(crate::aws_chunked::PushError::Sink(e)) => {
                                pipe.abort(
                                    crate::fsm::PipelineEvent::WriteError,
                                    key,
                                    Ok(internal_error_response(key, &format!("could not store the object: {}", e))),
                                );
                                continue;
                            }
                        }
                        for full_buf in ready_bufs {
                            let send_err = match pipe.tx.as_ref() {
                                Some(sender) => sender.send(full_buf).await.is_err(),
                                None => true,
                            };
                            if send_err {
                                pipe.fail_from_writer(key).await;
                                break;
                            }
                        }
                    }
                }
                crate::fsm::PipelineState::Draining => {
                    if !pipe.cur_buf.is_empty() {
                        let tail = std::mem::take(&mut pipe.cur_buf);
                        let send_err = match pipe.tx.as_ref() {
                            Some(sender) => sender.send(tail).await.is_err(),
                            None => true,
                        };
                        if send_err {
                            pipe.fail_from_writer(key).await;
                            continue;
                        }
                    }
                    let Some(writer) = pipe.join_writer(key).await else {
                        continue;
                    };
                    if decoder.as_ref().is_some_and(|d| d.finish().is_err()) || writer.written() != len {
                        drop(writer);
                        pipe.abort(
                            crate::fsm::PipelineEvent::LengthMismatch,
                            key,
                            Ok(bad_request(
                                "IncompleteBody",
                                "You did not provide the number of bytes specified by the Content-Length HTTP header.",
                                key,
                            )),
                        );
                        continue;
                    }
                    if let (Some(h), Some(want)) = (sha.take(), declared_sha.as_ref()) {
                        let got: String = h.finalize().iter().map(|b| format!("{:02x}", b)).collect();
                        if &got != want {
                            drop(writer);
                            pipe.abort(
                                crate::fsm::PipelineEvent::HashError,
                                key,
                                Ok(bad_request(
                                    "XAmzContentSHA256Mismatch",
                                    "The provided 'x-amz-content-sha256' header does not match what was computed.",
                                    key,
                                )),
                            );
                            continue;
                        }
                    }
                    let (etag_hi, etag_lo, etag_hex, raw_md5, _) = digest.take().expect("digest present").finalize();
                    if let Some(ref content_md5) = content_md5_hdr {
                        if content_md5 != &crate::digest::base64_encode(&raw_md5) {
                            drop(writer);
                            pipe.abort(
                                crate::fsm::PipelineEvent::HashError,
                                key,
                                Ok(bad_request("BadDigest", "The Content-MD5 you specified did not match what we received.", key)),
                            );
                            continue;
                        }
                    }
                    let stream_cksum_out = match (req_checksum.as_ref(), cksum_hasher.take()) {
                        (Some((alg, expected_opt)), Some(h)) => {
                            let got = h.finish_base64();
                            if let Some(expected) = expected_opt {
                                if &got != expected {
                                    drop(writer);
                                    pipe.abort(
                                        crate::fsm::PipelineEvent::HashError,
                                        key,
                                        Ok(bad_request("BadDigest", "The checksum you specified did not match what we received.", key)),
                                    );
                                    continue;
                                }
                            }
                            Some((*alg, got))
                        }
                        _ => None,
                    };
                    match pipe.step(crate::fsm::PipelineEvent::Drained) {
                        Ok(crate::fsm::PipelineState::Done) => {
                            outcome = Some((writer, etag_hi, etag_lo, etag_hex, stream_cksum_out));
                        }
                        Ok(_) => {
                            drop(writer);
                            pipe.abort(
                                crate::fsm::PipelineEvent::WriteError,
                                key,
                                Ok(internal_error_response(key, "unexpected pipeline completion state")),
                            );
                        }
                        Err(r) => {
                            drop(writer);
                            pipe.failure = Some(Ok(bad_request(r.code, r.why, key)));
                        }
                    }
                }
                crate::fsm::PipelineState::Failed => {
                    pipe.release_buffers();
                    if let Some(t) = pipe.writer_task.take() {
                        if let Ok(Ok(w)) = t.await {
                            drop(w);
                        }
                    }
                    drop(ticket);
                    return pipe
                        .failure
                        .unwrap_or_else(|| Ok(internal_error_response(key, "streamed PUT pipeline failed")));
                }
                crate::fsm::PipelineState::Done => break,
            }
        }
        let (mut writer, etag_hi, etag_lo, etag_hex, stream_cksum_out) = outcome.expect("Done guarantees outcome");
        let _key_guard = state.lock_key(b_id, key).await;
        if let Some(resp) = check_write_precondition(&state, b_id, key, &signed_headers) {
            return Ok(resp);
        }
        if let Err(code) = reservation.ensure_room(&state.engine, b_id, key.as_bytes()) {
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
        // A failure after the header is on disk leaves a record recovery will replay, so the
        // live index follows what a restart would show (#184, owner decision 4B).
        let (first_loc, mut late) = match writer.finish(etag_hi, etag_lo) {
            Ok(l) => (l, None),
            Err(VolumeError::AfterRecord { loc, flags, cause }) => (loc, Some(VolumeError::AfterRecord { loc, flags, cause })),
            Err(e) => {
                state.streams.note_append(b_id, key, u64::MAX);
                return Ok(internal_error_response(key, &format!("could not store the object: {:?}", e)));
            }
        };
        state.streams.note_append(b_id, key, first_offset);
        let (vm, loc, vol_offset) = if stamp.is_none() && ticket.appended_after(first_offset) {
            if let Some(e) = late.take() {
                // A later record of the key wins at a restart, and the live index shows it.
                return Ok(internal_error_response(key, &format!("could not store the object: {:?}", e)));
            }
            let moved = {
                let (st, k, fvm) = (Arc::clone(&state), key.to_string(), Arc::clone(&first_vm));
                tokio::task::spawn_blocking(move || st.relocate_needle(b_id, &k, &fvm, &first_loc, flags, etag_hi, etag_lo)).await
            };
            match moved {
                Ok(Ok(moved)) => moved,
                Ok(Err(VolumeError::AfterRecord { loc, flags: f, cause })) => {
                    let g = global_offset(loc.volume_id, loc.offset);
                    late = Some(VolumeError::AfterRecord { loc, flags: f, cause });
                    (Arc::clone(&first_vm), loc, g)
                }
                Ok(Err(e)) => return Ok(internal_error_response(key, &format!("could not store the object: {:?}", e))),
                Err(e) => return Ok(internal_error_response(key, &format!("could not store the object: {:?}", e))),
            }
        } else {
            (first_vm, first_loc, first_offset)
        };
        if late.is_none() {
            if let Err(e) = state.make_durable(&vm, loc.seq).await {
                late = Some(VolumeError::AfterRecord { loc, flags, cause: format!("could not sync the object: {:?}", e) });
            }
        }
        drop(ticket);
        state.total_volume_bytes.fetch_add(loc.total_len, Ordering::Relaxed);
        state.hook("put_stored", key);
        let stream_cksum_ref = stream_cksum_out.as_ref().map(|(a, v)| (*a, v.as_str()));
        let answer = if let Some((extra, ts)) = stamp {
            match finish_version_put(&state, b_id, key, ts, flags, vol_offset, len, etag_hi, etag_lo, 0, reservation) {
                Ok(vid) => {
                    let resp_vid = (extra & crate::meta_index::FLAG_NULL_ID == 0).then_some(vid.as_str());
                    put_ok_response(&etag_hex, resp_vid, stream_cksum_ref)
                }
                Err(r) => r,
            }
        } else {
            finish_put(&state, b_id, key, old_info, vol_offset, len, etag_hi, etag_lo, flags, &etag_hex, created_ns / 1_000_000, stream_cksum_ref, reservation)
        };
        return Ok(after_late_failure(key, late, answer));
    }
    let (raw_bytes, _body_guard) = match read_body_admitted(&state.body_budget, &state.work, body, MAX_OBJECT_BODY + CHUNK_FRAMING_SLACK, expected_len.map(|n| n as u64), state.body_idle_timeout(), path).await? {
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
        let mut verifier = chunk_verifier.take();
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
    let mut digest = crate::digest::StreamingDigest::new_md5_only();
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
    if let Some(resp) = check_write_precondition(&state, b_id, key, &signed_headers) {
        return Ok(resp);
    }
    let reservation = match CapacityReservation::acquire(&state.engine, b_id, key.as_bytes()) {
        Ok(r) => r,
        Err(code) => return Ok(engine_error_response(code, key)),
    };
    let old_info = {
        let engine = state.engine.read();
        engine.get_object(b_id, key.as_bytes()).ok().map(|m| (m.vol_offset, m.size))
    };

    let buf_cksum_out = if let Some((alg, expected_opt)) = req_checksum {
        let mut h = crate::checksum::Checksum::new(alg);
        h.update(&decoded_bytes);
        let got = h.finish_base64();
        if let Some(expected) = expected_opt {
            if got != expected {
                return Ok(bad_request("BadDigest", "The checksum you specified did not match what we received.", key));
            }
        }
        Some((alg, got))
    } else {
        None
    };
    let buf_attrs = match request_attrs_with_checksum(
        &signed_headers,
        key,
        buf_cksum_out.as_ref().map(|(a, v)| (*a, "FULL_OBJECT", v.as_str())),
        writer_account,
        bucket_owner,
    ) {
        Ok(a) => a,
        Err(resp) => return Ok(resp),
    };
    let stamp = state.version_stamp(b_id, key);
    let flags = flags | stamp.map_or(0, |s| s.0);
    let created_ns = stamp.map(|s| s.1).unwrap_or_else(|| state.clock.now_ns());
    let (vol_offset, flags, late) = match state
        .store_object(b_id, bucket, key, &buf_attrs, &decoded_bytes, flags, etag_hi, etag_lo, Some(created_ns))
        .await
    {
        Ok((o, f)) => (o, f, None),
        Err(e) => match S3ServiceState::late_record(&e) {
            Some((o, f)) => (o, f, Some(e)),
            None => return Ok(internal_error_response(key, &format!("could not store the object: {:?}", e))),
        },
    };
    state.hook("put_stored", key);
    let buf_cksum_ref = buf_cksum_out.as_ref().map(|(a, v)| (*a, v.as_str()));
    let answer = if let Some((extra, ts)) = stamp {
        match finish_version_put(&state, b_id, key, ts, flags, vol_offset, payload_len, etag_hi, etag_lo, 0, reservation) {
            Ok(vid) => {
                let resp_vid = (extra & crate::meta_index::FLAG_NULL_ID == 0).then_some(vid.as_str());
                put_ok_response(&final_etag_hex, resp_vid, buf_cksum_ref)
            }
            Err(r) => r,
        }
    } else {
        finish_put(&state, b_id, key, old_info, vol_offset, payload_len, etag_hi, etag_lo, flags, &final_etag_hex, created_ns / 1_000_000, buf_cksum_ref, reservation)
    };
    Ok(after_late_failure(key, late, answer))
}

pub async fn get_or_head_object(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    key: &str,
    query_map: &HashMap<String, String>,
    headers: &HeaderMap,
    is_head: bool,
    is_anon_with_auth: bool,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let overrides = match ResponseOverrides::from_query(query_map, key, is_anon_with_auth) {
        Ok(ov) => ov,
        Err(resp) => return Ok(resp),
    };
    let part_num = match query_map.get("partNumber") {
        Some(s) => match s.parse::<u32>() {
            Ok(n) if (1..=10_000).contains(&n) => Some(n),
            _ => {
                let body = if is_head {
                    empty_body()
                } else {
                    full_body(Bytes::from(s3_error_xml("InvalidArgument", "Part number must be an integer between 1 and 10000, inclusive", key, "req-get-part")))
                };
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .header(CONTENT_TYPE, "application/xml")
                    .body(body)
                    .unwrap());
            }
        },
        None => None,
    };
    if part_num.is_some() && headers.contains_key("Range") {
        let body = if is_head {
            empty_body()
        } else {
            full_body(Bytes::from(s3_error_xml("InvalidRequest", "Cannot specify both Range header and partNumber query parameter", key, "req-get-part")))
        };
        return Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header(CONTENT_TYPE, "application/xml")
            .body(body)
            .unwrap());
    }
    let vid = query_map.get("versionId").map(|s| s.as_str());
    if vid.is_some() || state.versions.read().has(b_id, key) {
        let range = headers.get("Range").and_then(|v| v.to_str().ok());
        return Ok(version_get_response(&state, b_id, bucket, key, vid, is_head, range, part_num, headers, &overrides));
    }
    let (meta, is_multipart) = {
        let engine = state.engine.read();
        let m = match engine.get_object(b_id, key.as_bytes()) {
            Ok(m) => m,
            Err(_) => {
                if precondition(headers, None, false) == PreconditionOutcome::PreconditionFailed {
                    return Ok(precondition_failure_response(PreconditionOutcome::PreconditionFailed, None, key, is_head));
                }
                let body = if is_head {
                    empty_body()
                } else {
                    let xml = s3_error_xml("NoSuchKey", "The specified key does not exist.", key, "req-get-obj");
                    full_body(Bytes::from(xml))
                };
                return Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header(CONTENT_TYPE, "application/xml")
                    .header("x-amz-delete-marker", "false")
                    .body(body)
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
    let tag_count = view.attrs.tag_count();
    let is_compressed = unpack_flags(meta.bucket_flags) & crate::meta_index::FLAG_COMPRESSED != 0;
    let cksum_mode = checksum_mode_enabled(headers);

    let etag_val = format_object_etag(meta.etag_hi, meta.etag_lo, is_multipart, unpack_part_count(meta.bucket_flags));
    let lm_secs = last_modified_secs_from_ms(meta.created_at);
    let facts = ObjectFacts {
        etag: etag_val.clone(),
        last_modified_secs: lm_secs,
    };
    match precondition(headers, Some(&facts), false) {
        PreconditionOutcome::Proceed => {}
        outcome => return Ok(precondition_failure_response(outcome, Some(&facts), key, is_head)),
    }

    if let Some(pn) = part_num {
        if is_multipart {
            let extents = match state.multipart.get_completed_extents(b_id, key) {
                Some(e) => e,
                None => return Ok(internal_error_response(key, "the multipart object's part list is missing")),
            };
            let parts_count = extents.len();
            if (pn as usize) > parts_count {
                let body = if is_head {
                    empty_body()
                } else {
                    full_body(Bytes::from(s3_error_xml("InvalidPart", "The requested part number is not valid.", key, "req-get-part")))
                };
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .header(CONTENT_TYPE, "application/xml")
                    .body(body)
                    .unwrap());
            }
            let ext = &extents[(pn - 1) as usize];
            let body = if is_head {
                empty_body()
            } else if meta.size > VERIFY_WHILE_SENDING_MIN {
                // A part of a large object is read from its file, not its mapping (#155
                // requirement 5), whatever the part's own size.
                match state.multipart_body(b_id, format!("{}/{} part {}", bucket, key, pn), &extents, ext.cumulative_end - ext.part_size, ext.part_size, true) {
                    Ok(b) => b.boxed(),
                    Err(e) => return Ok(internal_error_response(key, &format!("could not read a part: {:?}", e))),
                }
            } else {
                let b = match state.part_bytes(b_id, ext) {
                    Ok(b) => b,
                    Err(e) => return Ok(internal_error_response(key, &format!("could not read a part: {:?}", e))),
                };
                full_body(b.slice(..(ext.part_size as usize).min(b.len())))
            };
            return Ok(with_part_checksum_headers(
                Response::builder()
                    .status(StatusCode::OK)
                    .header(CONTENT_TYPE, view.content_type())
                    .header(ETAG, etag_val)
                    .header(ACCEPT_RANGES, "bytes")
                    .header("x-amz-mp-parts-count", parts_count.to_string())
                    .header(CONTENT_LENGTH, ext.part_size.to_string())
                    .header(LAST_MODIFIED, crate::clock::format_http_date_ms(meta.created_at)),
                &view.attrs,
                ext.checksum.as_ref(),
            )
            .body(body)
            .unwrap());
        } else if pn > 1 {
            let body = if is_head {
                empty_body()
            } else {
                full_body(Bytes::from(s3_error_xml("InvalidPart", "The requested part number is not valid.", key, "req-get-part")))
            };
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header(CONTENT_TYPE, "application/xml")
                .body(body)
                .unwrap());
        }
    }

    let range_hdr = headers.get("Range").and_then(|v| v.to_str().ok());
    let range_res = range_hdr.map(|h| parse_range_header(h, meta.size));

    if let Some(range_opt) = range_res {
        match range_opt {
            Ok(Some(range)) => {
                if !is_head && !is_compressed && meta.size > VERIFY_WHILE_SENDING_MIN {
                    let len = range.end - range.start + 1;
                    let what = format!("{}/{}", bucket, key);
                    let body = if is_multipart {
                        // Parts read from their files, not their mappings (#155 requirement 5).
                        match state.multipart.get_completed_extents(b_id, key) {
                            Some(extents) => state.multipart_body(b_id, what, &extents, range.start, len, false),
                            None => return Ok(internal_error_response(key, "the multipart object's part list is missing")),
                        }
                    } else {
                        state.large_payload_body(b_id, what, meta.vol_offset, meta.size, range.start, len, false)
                    };
                    let body = match body {
                        Ok(b) => b,
                        Err(e) => return Ok(internal_error_response(key, &format!("could not read the object: {:?}", e))),
                    };
                    return Ok(with_tagging_count(Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .with_object_metadata(&view, &overrides)
                        .header(ETAG, etag_val)
                        .header(ACCEPT_RANGES, "bytes")
                        .header(CONTENT_RANGE, format!("bytes {}-{}/{}", range.start, range.end, meta.size))
                        .header(CONTENT_LENGTH, len.to_string())
                        .header(LAST_MODIFIED, crate::clock::format_http_date_ms(meta.created_at)), tag_count)
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
                            return Ok(with_tagging_count(Response::builder()
                                .status(StatusCode::PARTIAL_CONTENT)
                                .with_object_metadata(&view, &overrides)
                                .header(ETAG, etag_val)
                                .header(ACCEPT_RANGES, "bytes")
                                .header(CONTENT_RANGE, format!("bytes {}-{}/{}", range.start, range.end, meta.size))
                                .header(CONTENT_LENGTH, (range.end - range.start + 1).to_string())
                                .header(LAST_MODIFIED, crate::clock::format_http_date_ms(meta.created_at)), tag_count)
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
                        state.load_payload_for(b_id, &format!("{}/{}", bucket, key), meta.vol_offset, meta.size as usize)
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

                return Ok(with_tagging_count(Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .with_object_metadata(&view, &overrides)
                    .header(ETAG, etag_val)
                    .header(ACCEPT_RANGES, "bytes")
                    .header(CONTENT_RANGE, content_range)
                    .header(CONTENT_LENGTH, (range.end - range.start + 1).to_string())
                    .header(LAST_MODIFIED, crate::clock::format_http_date_ms(meta.created_at)), tag_count)
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
        if meta.size > VERIFY_WHILE_SENDING_MIN {
            // Parts read from their files, not their mappings (#155 requirement 5).
            let body = match state.multipart_body(b_id, format!("{}/{}", bucket, key), &extents, 0, meta.size, true) {
                Ok(b) => b,
                Err(e) => return Ok(internal_error_response(key, &format!("could not read a part: {:?}", e))),
            };
            return Ok(with_tagging_count(with_checksum_headers(Response::builder()
                .status(StatusCode::OK)
                .with_object_metadata(&view, &overrides)
                .header(ETAG, etag_val)
                .header(ACCEPT_RANGES, "bytes")
                .header(CONTENT_LENGTH, meta.size.to_string())
                .header(LAST_MODIFIED, crate::clock::format_http_date_ms(meta.created_at)), &view.attrs, cksum_mode), tag_count)
                .body(body.boxed())
                .unwrap());
        }
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
        return Ok(with_tagging_count(with_checksum_headers(Response::builder()
            .status(StatusCode::OK)
            .with_object_metadata(&view, &overrides)
            .header(ETAG, etag_val)
            .header(ACCEPT_RANGES, "bytes")
            .header(CONTENT_LENGTH, meta.size.to_string())
            .header(LAST_MODIFIED, crate::clock::format_http_date_ms(meta.created_at)), &view.attrs, cksum_mode), tag_count)
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
        return Ok(with_tagging_count(with_checksum_headers(Response::builder()
            .status(StatusCode::OK)
            .with_object_metadata(&view, &overrides)
            .header(ETAG, etag_val)
            .header(ACCEPT_RANGES, "bytes")
            .header(CONTENT_LENGTH, meta.size.to_string())
            .header(LAST_MODIFIED, crate::clock::format_http_date_ms(meta.created_at)), &view.attrs, cksum_mode), tag_count)
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
                    state_for_read
                        .load_payload_for(b_id, &format!("{}/{}", bucket, key), meta.vol_offset, meta.size as usize)
                        .unwrap_or_default()
                }
            })
            .await
    };

    if !is_head && payload.len() as u64 != meta.size {
        return Ok(internal_error_response(key, "the object's bytes could not be read or failed verification"));
    }
    let body = if is_head { empty_body() } else { full_body(payload) };
    Ok(with_tagging_count(with_checksum_headers(Response::builder()
        .status(StatusCode::OK)
        .with_object_metadata(&view, &overrides)
        .header(ETAG, etag_val)
        .header(ACCEPT_RANGES, "bytes")
        .header(CONTENT_LENGTH, meta.size.to_string())
        .header(LAST_MODIFIED, crate::clock::format_http_date_ms(meta.created_at)), &view.attrs, cksum_mode), tag_count)
        .body(body)
        .unwrap())
}

pub async fn delete_object(
    state: Arc<S3ServiceState>,
    b_id: u32,
    key: &str,
    query_map: &HashMap<String, String>,
    headers: &HeaderMap,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let conds = match parse_delete_conditions(headers) {
        Ok(c) => c,
        Err((code, msg)) => return Ok(bad_request(code, msg, key)),
    };
    if let Some(vid) = query_map.get("versionId") {
        let Some(id) = crate::versions::parse_id(vid) else {
            return Ok(no_such_version(key, false));
        };
        return Ok(match state.delete_version_conditional(b_id, key, id, &conds).await {
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
            Err(crate::s3_api::DeleteError::PreconditionFailed(facts)) => {
                precondition_failure_response(PreconditionOutcome::PreconditionFailed, facts.as_ref(), key, false)
            }
            Err(crate::s3_api::DeleteError::Internal(e)) => internal_error_response(key, &e),
        });
    }
    let marker = match state.delete_key_conditional(b_id, key, &conds).await {
        Ok(m) => m,
        Err(crate::s3_api::DeleteError::PreconditionFailed(facts)) => {
            return Ok(precondition_failure_response(
                PreconditionOutcome::PreconditionFailed,
                facts.as_ref(),
                key,
                false,
            ));
        }
        Err(crate::s3_api::DeleteError::Internal(e)) => return Ok(internal_error_response(key, &e)),
    };
    let mut b = Response::builder().status(StatusCode::NO_CONTENT).header(CONTENT_LENGTH, "0");
    if let Some(m) = marker {
        b = b.header("x-amz-delete-marker", "true").header("x-amz-version-id", m);
    }
    Ok(b.body(empty_body()).unwrap())
}


pub async fn get_object_attributes(
    state: Arc<S3ServiceState>,
    b_id: u32,
    _bucket: &str,
    key: &str,
    query_map: &HashMap<String, String>,
    headers: &HeaderMap,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let mut want_etag = false;
    let mut want_checksum = false;
    let mut want_parts = false;
    let mut want_storage_class = false;
    let mut want_size = false;
    let mut any_attr = false;

    for hdr_val in headers.get_all("x-amz-object-attributes") {
        let Ok(raw) = hdr_val.to_str() else {
            return Ok(bad_request("InvalidArgument", "Invalid x-amz-object-attributes header.", key));
        };
        for token in raw.split(',').map(|t| t.trim()).filter(|t| !t.is_empty()) {
            any_attr = true;
            match token {
                "ETag" => want_etag = true,
                "Checksum" => want_checksum = true,
                "ObjectParts" => want_parts = true,
                "StorageClass" => want_storage_class = true,
                "ObjectSize" => want_size = true,
                _ => return Ok(bad_request("InvalidArgument", "Unsupported x-amz-object-attributes value.", key)),
            }
        }
    }
    if !any_attr {
        return Ok(bad_request("InvalidArgument", "Missing required x-amz-object-attributes header.", key));
    }

    let max_parts: usize = match headers.get("x-amz-max-parts").and_then(|v| v.to_str().ok()) {
        Some(s) => match s.trim().parse::<usize>() {
            Ok(n) if n > 0 => n.min(1000),
            _ => return Ok(bad_request("InvalidArgument", "Invalid x-amz-max-parts header.", key)),
        },
        None => 1000,
    };
    let part_marker: usize = match headers.get("x-amz-part-number-marker").and_then(|v| v.to_str().ok()) {
        Some(s) => match s.trim().parse::<usize>() {
            Ok(n) => n,
            Err(_) => return Ok(bad_request("InvalidArgument", "Invalid x-amz-part-number-marker header.", key)),
        },
        None => 0,
    };

    let vid = query_map.get("versionId").map(|s| s.as_str());
    let (vol_offset, flags, size, etag_hi, etag_lo, part_count, last_mod_http, version_id_str) = if vid.is_some() || state.versions.read().has(b_id, key) {
        let id = match vid {
            Some(s) => match crate::versions::parse_id(s) {
                Some(i) => Some(i),
                None => return Ok(no_such_version(key, false)),
            },
            None => None,
        };
        let Some(v) = find_version(&state, b_id, key, id) else {
            if id.is_some() {
                return Ok(no_such_version(key, false));
            }
            let xml = s3_error_xml("NoSuchKey", "The specified key does not exist.", key, "req-get-attrs");
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap());
        };
        if v.marker {
            let (status, code, msg) = if id.is_some() {
                (StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed", "The specified method is not allowed against a delete marker.")
            } else {
                (StatusCode::NOT_FOUND, "NoSuchKey", "The specified key does not exist.")
            };
            let xml = s3_error_xml(code, msg, key, "req-get-attrs");
            return Ok(Response::builder()
                .status(status)
                .header(CONTENT_TYPE, "application/xml")
                .header("x-amz-delete-marker", "true")
                .header("x-amz-version-id", v.id_string())
                .body(full_body(Bytes::from(xml)))
                .unwrap());
        }
        (v.vol_offset, v.flags, v.size, v.etag_hi, v.etag_lo, v.part_count, crate::clock::format_http_date(v.modified), Some(v.id_string()))
    } else {
        let meta = {
            let engine = state.engine.read();
            match engine.get_object(b_id, key.as_bytes()) {
                Ok(m) => m,
                Err(_) => {
                    let xml = s3_error_xml("NoSuchKey", "The specified key does not exist.", key, "req-get-attrs");
                    return Ok(Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .header(CONTENT_TYPE, "application/xml")
                        .body(full_body(Bytes::from(xml)))
                        .unwrap());
                }
            }
        };
        (meta.vol_offset, unpack_flags(meta.bucket_flags), meta.size, meta.etag_hi, meta.etag_lo, unpack_part_count(meta.bucket_flags), crate::clock::format_http_date_ms(meta.created_at), None)
    };

    let is_multipart = flags & AWAN_FLAG_MULTIPART != 0;
    let view = match state.object_view(vol_offset, flags) {
        Ok(v) => v,
        Err(e) => return Ok(internal_error_response(key, &format!("could not read the object's attributes: {:?}", e))),
    };

    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<GetObjectAttributesResponse xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");
    if want_etag {
        let hex = md5_to_etag_hex(etag_hi, etag_lo);
        if is_multipart {
            xml.push_str(&format!("  <ETag>{}-{}</ETag>\n", hex, part_count));
        } else {
            xml.push_str(&format!("  <ETag>{}</ETag>\n", hex));
        }
    }
    if want_checksum {
        if let Some((alg, cksum_type, val)) = view.attrs.checksum() {
            xml.push_str("  <Checksum>\n");
            xml.push_str(&format!("    <{}>{}</{}>\n", alg.xml_name(), crate::xml::xml_escape(val), alg.xml_name()));
            xml.push_str(&format!("    <ChecksumType>{}</ChecksumType>\n", crate::xml::xml_escape(cksum_type)));
            xml.push_str("  </Checksum>\n");
        }
    }
    if want_parts && is_multipart {
        let part_sizes: Vec<u64> = if let Some(sizes) = view.attrs.part_sizes() {
            sizes
        } else if let Some(extents) = state.multipart.get_completed_extents(b_id, key) {
            extents.into_iter().map(|e| e.part_size).collect()
        } else {
            Vec::new()
        };
        let extents_opt = state.multipart.get_completed_extents(b_id, key);
        let total = part_sizes.len();
        let mut page: Vec<(usize, u64, Option<(crate::checksum::ChecksumAlgorithm, String)>)> = Vec::new();
        let mut has_more = false;
        for (idx, &psz) in part_sizes.iter().enumerate() {
            let p_num = idx + 1;
            if p_num <= part_marker {
                continue;
            }
            if page.len() < max_parts {
                let p_cksum = extents_opt
                    .as_ref()
                    .and_then(|exts| exts.get(idx))
                    .and_then(|e| e.checksum.clone());
                page.push((p_num, psz, p_cksum));
            } else {
                has_more = true;
                break;
            }
        }
        let next_marker = page.last().map(|(n, _, _)| *n).unwrap_or(part_marker);
        xml.push_str("  <ObjectParts>\n");
        xml.push_str(&format!("    <PartsCount>{}</PartsCount>\n", total));
        xml.push_str(&format!("    <PartNumberMarker>{}</PartNumberMarker>\n", part_marker));
        xml.push_str(&format!("    <NextPartNumberMarker>{}</NextPartNumberMarker>\n", next_marker));
        xml.push_str(&format!("    <MaxParts>{}</MaxParts>\n", max_parts));
        xml.push_str(&format!("    <IsTruncated>{}</IsTruncated>\n", has_more));
        for (p_num, p_size, p_cksum) in page {
            xml.push_str("    <Part>\n");
            xml.push_str(&format!("      <PartNumber>{}</PartNumber>\n", p_num));
            xml.push_str(&format!("      <Size>{}</Size>\n", p_size));
            if let Some((alg, val)) = p_cksum {
                xml.push_str(&format!("      <{}>{}</{}>\n", alg.xml_name(), crate::xml::xml_escape(&val), alg.xml_name()));
            }
            xml.push_str("    </Part>\n");
        }
        xml.push_str("  </ObjectParts>\n");
    }
    if want_storage_class {
        xml.push_str("  <StorageClass>STANDARD</StorageClass>\n");
    }
    if want_size {
        xml.push_str(&format!("  <ObjectSize>{}</ObjectSize>\n", size));
    }
    xml.push_str("</GetObjectAttributesResponse>");

    let mut b = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml")
        .header(LAST_MODIFIED, last_mod_http);
    if let Some(vid_str) = version_id_str {
        b = b.header("x-amz-version-id", vid_str);
    }
    Ok(b.body(full_body(Bytes::from(xml))).unwrap())
}