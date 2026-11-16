use bytes::Bytes;
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE};
use hyper::{Response, StatusCode};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::attrs::{encode_tags, TAG_TAGGING};
use crate::ffi_bridge::{unpack_flags, unpack_part_count, AWAN_FLAG_MULTIPART};
use crate::meta_index::FLAG_ATTRS;
use crate::ops::object::{find_version, no_such_version};
use crate::s3_api::{
    bad_request, empty_body, engine_error_response, full_body, internal_error_response,
    read_body_admitted, ResponseBody, S3ServiceState, MAX_XML_BODY,
};
use crate::tagging::{parse_tagging_xml, tagging_xml, Scope, TagSet};
use crate::volume::{split_offset, VolumeError};
use crate::xml::s3_error_xml;

struct TargetNeedle {
    vol_offset: u64,
    size: u64,
    etag_hi: u64,
    etag_lo: u64,
    part_count: u16,
    flags: u16,
    created_ms: u64,
    created_ns: Option<u64>,
    version: Option<crate::versions::Version>,
}

fn xml_error(status: StatusCode, code: &str, msg: &str, resource: &str) -> Response<ResponseBody> {
    let xml = s3_error_xml(code, msg, resource, "req-tagging");
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap()
}

fn resolve_target(
    state: &S3ServiceState,
    b_id: u32,
    key: &str,
    vid: Option<&str>,
) -> Result<TargetNeedle, Response<ResponseBody>> {
    if vid.is_some() || state.versions.read().has(b_id, key) {
        let id = match vid {
            Some(s) => match crate::versions::parse_id(s) {
                Some(i) => Some(i),
                None => return Err(no_such_version(key, false)),
            },
            None => None,
        };
        let Some(v) = find_version(state, b_id, key, id) else {
            if id.is_some() {
                return Err(no_such_version(key, false));
            }
            return Err(xml_error(
                StatusCode::NOT_FOUND,
                "NoSuchKey",
                "The specified key does not exist.",
                key,
            ));
        };
        if v.marker {
            let (status, code, msg) = if id.is_some() {
                (
                    StatusCode::METHOD_NOT_ALLOWED,
                    "MethodNotAllowed",
                    "The specified method is not allowed against a delete marker.",
                )
            } else {
                (
                    StatusCode::NOT_FOUND,
                    "NoSuchKey",
                    "The specified key does not exist.",
                )
            };
            let xml = s3_error_xml(code, msg, key, "req-tagging");
            return Err(Response::builder()
                .status(status)
                .header(CONTENT_TYPE, "application/xml")
                .header("x-amz-delete-marker", "true")
                .header("x-amz-version-id", v.id_string())
                .body(full_body(Bytes::from(xml)))
                .unwrap());
        }
        return Ok(TargetNeedle {
            vol_offset: v.vol_offset,
            size: v.size,
            etag_hi: v.etag_hi,
            etag_lo: v.etag_lo,
            part_count: v.part_count,
            flags: v.flags,
            created_ms: v.ts / 1_000_000,
            created_ns: (v.ts > 0).then_some(v.ts),
            version: Some(v),
        });
    }

    let meta = state.engine.read().get_object(b_id, key.as_bytes()).map_err(|_| {
        xml_error(
            StatusCode::NOT_FOUND,
            "NoSuchKey",
            "The specified key does not exist.",
            key,
        )
    })?;
    Ok(TargetNeedle {
        vol_offset: meta.vol_offset,
        size: meta.size,
        etag_hi: meta.etag_hi,
        etag_lo: meta.etag_lo,
        part_count: unpack_part_count(meta.bucket_flags),
        flags: unpack_flags(meta.bucket_flags),
        created_ms: meta.created_at,
        created_ns: (meta.created_at > 0).then_some(meta.created_at * 1_000_000),
        version: None,
    })
}

fn read_stored_needle_bytes(
    state: &S3ServiceState,
    b_id: u32,
    target: &TargetNeedle,
) -> Result<Bytes, VolumeError> {
    if target.size == 0 && target.flags & AWAN_FLAG_MULTIPART == 0 {
        return Ok(Bytes::new());
    }
    let (vol_id, local) = split_offset(target.vol_offset);
    let vm = state
        .volumes
        .read()
        .get(&vol_id)
        .cloned()
        .ok_or(VolumeError::CorruptIndex)?;
    let crc = state
        .needle_crc_map
        .read()
        .get(&(b_id, target.vol_offset))
        .copied()
        .ok_or(VolumeError::CorruptIndex)?;
    vm.read_needle_bytes(vol_id, local, crc)
}

fn retire_old_needle(state: &S3ServiceState, b_id: u32, key: &str, old_offset: u64, new_offset: u64, size: u64) {
    if old_offset > 0 && old_offset != new_offset {
        state.payload_cache.remove(b_id, old_offset);
        state.needle_crc_map.write().remove(&(b_id, old_offset));
        let raw_len = 64 + key.len() + size as usize;
        let dead_bytes = raw_len.div_ceil(4096) as u64 * 4096;
        state.tombstone_bytes.fetch_add(dead_bytes, Ordering::Relaxed);
    }
}

pub async fn get_object_tagging(
    state: Arc<S3ServiceState>,
    b_id: u32,
    key: &str,
    query_map: &HashMap<String, String>,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let vid = query_map.get("versionId").map(|s| s.as_str());
    let target = match resolve_target(&state, b_id, key, vid) {
        Ok(t) => t,
        Err(resp) => return Ok(resp),
    };
    let attrs = match state.object_attrs(target.vol_offset, target.flags) {
        Ok(a) => a,
        Err(e) => return Ok(internal_error_response(key, &format!("could not read the object's attributes: {:?}", e))),
    };
    let tag_set = match attrs.tag_set() {
        Ok(s) => s,
        Err(e) => return Ok(internal_error_response(key, &format!("could not decode the object's tags: {:?}", e))),
    };
    let xml = tagging_xml(&tag_set);
    let mut b = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml");
    if let Some(ref v) = target.version {
        b = b.header("x-amz-version-id", v.id_string());
    }
    Ok(b.body(full_body(Bytes::from(xml))).unwrap())
}

pub async fn put_object_tagging(
    state: Arc<S3ServiceState>,
    b_id: u32,
    key: &str,
    path: &str,
    query_map: &HashMap<String, String>,
    body: hyper::body::Incoming,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let (body_bytes, _body_guard) =
        match read_body_admitted(&state.body_budget, body, MAX_XML_BODY, None, state.body_idle_timeout(), path).await? {
            Ok(b) => b,
            Err(resp) => return Ok(resp),
        };
    let xml_str = match std::str::from_utf8(&body_bytes) {
        Ok(s) => s,
        Err(_) => return Ok(xml_error(StatusCode::BAD_REQUEST, "MalformedXML", "Invalid UTF-8 in XML body.", key)),
    };
    let tag_set = match parse_tagging_xml(xml_str, Scope::Object) {
        Ok(s) => s,
        Err(r) => {
            let status = StatusCode::from_u16(r.status).unwrap_or(StatusCode::BAD_REQUEST);
            return Ok(xml_error(status, r.code, r.why, key));
        }
    };
    let vid = query_map.get("versionId").map(|s| s.as_str());
    Ok(update_object_tags(&state, b_id, key, vid, Some(tag_set), StatusCode::OK).await)
}

pub async fn delete_object_tagging(
    state: Arc<S3ServiceState>,
    b_id: u32,
    key: &str,
    query_map: &HashMap<String, String>,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let vid = query_map.get("versionId").map(|s| s.as_str());
    Ok(update_object_tags(&state, b_id, key, vid, None, StatusCode::NO_CONTENT).await)
}

async fn update_object_tags(
    state: &S3ServiceState,
    b_id: u32,
    key: &str,
    vid: Option<&str>,
    new_tags: Option<TagSet>,
    ok_status: StatusCode,
) -> Response<ResponseBody> {
    let _key_guard = state.lock_key(b_id, key).await;
    let target = match resolve_target(state, b_id, key, vid) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let mut attrs = match state.object_attrs(target.vol_offset, target.flags) {
        Ok(a) => a,
        Err(e) => return internal_error_response(key, &format!("could not read the object's attributes: {:?}", e)),
    };

    let clearing = new_tags.as_ref().is_none_or(|s| s.tags.is_empty());
    if clearing && attrs.get(TAG_TAGGING).is_none() {
        let mut b = Response::builder().status(ok_status).header(CONTENT_LENGTH, "0");
        if let Some(ref v) = target.version {
            b = b.header("x-amz-version-id", v.id_string());
        }
        return b.body(empty_body()).unwrap();
    }

    match new_tags {
        Some(ref set) if !set.tags.is_empty() => attrs.set(TAG_TAGGING, &encode_tags(set)),
        _ => attrs.remove(TAG_TAGGING),
    }

    let new_attrs_bytes = match attrs.encode() {
        Ok(b) => b,
        Err(_) => return bad_request("MetadataTooLarge", "Your metadata headers exceed the maximum allowed metadata size.", key),
    };
    let new_flags = if new_attrs_bytes.is_empty() {
        target.flags & !FLAG_ATTRS
    } else {
        target.flags | FLAG_ATTRS
    };

    let stored_bytes = match read_stored_needle_bytes(state, b_id, &target) {
        Ok(b) => b,
        Err(e) => return internal_error_response(key, &format!("could not read the object's payload: {:?}", e)),
    };

    let new_offset = match state
        .store_payload_with(
            b_id,
            key,
            &new_attrs_bytes,
            &stored_bytes,
            new_flags,
            target.etag_hi,
            target.etag_lo,
            target.created_ns,
        )
        .await
    {
        Ok(off) => off,
        Err(e) => return internal_error_response(key, &format!("could not store updated object tags: {:?}", e)),
    };
    if new_flags & AWAN_FLAG_MULTIPART != 0 {
        state.payload_cache.remove(b_id, new_offset);
    }

    let version_id = if let Some(mut v) = target.version {
        let vid_str = v.id_string();
        v.vol_offset = new_offset;
        v.flags = new_flags;
        if let Err(code) = state.record_version(b_id, key, crate::versions::Event::Put(v)) {
            return engine_error_response(code, key);
        }
        retire_old_needle(state, b_id, key, target.vol_offset, new_offset, target.size);
        Some(vid_str)
    } else {
        retire_old_needle(state, b_id, key, target.vol_offset, new_offset, target.size);
        if let Err(code) = state.engine.write().put_object_at(
            b_id,
            key.as_bytes(),
            new_offset,
            target.size,
            target.etag_hi,
            target.etag_lo,
            target.part_count,
            new_flags,
            target.created_ms,
        ) {
            return engine_error_response(code, key);
        }
        None
    };

    let mut b = Response::builder().status(ok_status).header(CONTENT_LENGTH, "0");
    if let Some(vid_str) = version_id {
        b = b.header("x-amz-version-id", vid_str);
    }
    b.body(empty_body()).unwrap()
}
