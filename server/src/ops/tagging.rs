use bytes::Bytes;
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE};
use hyper::{Response, StatusCode};
use std::collections::HashMap;
use std::sync::Arc;

use crate::attrs::{encode_tags, Attrs, TAG_TAGGING};
use crate::bucket_config::ConfigKind;
use crate::ffi_bridge::unpack_flags;
use crate::ops::object::{find_version, no_such_version};
use crate::s3_api::{
    bad_request, empty_body, full_body, internal_error_response,
    read_body_admitted, ResponseBody, S3ServiceState, SetAttrError, MAX_XML_BODY,
};
use crate::tagging::{parse_tagging_xml, tagging_xml, Scope, TagSet};
use crate::xml::s3_error_xml;

struct TargetNeedle {
    vol_offset: u64,
    flags: u16,
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
            flags: v.flags,
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
        flags: unpack_flags(meta.bucket_flags),
        version: None,
    })
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
    let target_vid = target.version.as_ref().map_or(0, |v| v.id());
    let view = match state.object_view_for(b_id, key, target_vid, target.vol_offset, target.flags) {
        Ok(v) => v,
        Err(e) => return Ok(internal_error_response(key, &format!("could not read the object's attributes: {:?}", e))),
    };
    let mut tag_set = match view.attrs.tag_set() {
        Ok(s) => s,
        Err(e) => return Ok(internal_error_response(key, &format!("could not decode the object's tags: {:?}", e))),
    };
    tag_set.tags.sort_by(|a, b| a.key.cmp(&b.key));
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
        match read_body_admitted(&state.body_budget, &state.work, body, MAX_XML_BODY, None, state.body_idle_timeout(), path).await? {
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

pub async fn put_bucket_tagging(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    path: &str,
    body: hyper::body::Incoming,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let (body_bytes, _body_guard) =
        match read_body_admitted(&state.body_budget, &state.work, body, MAX_XML_BODY, None, state.body_idle_timeout(), path).await? {
            Ok(b) => b,
            Err(resp) => return Ok(resp),
        };
    if body_bytes.is_empty() {
        return Ok(xml_error(
            StatusCode::BAD_REQUEST,
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema",
            bucket,
        ));
    }
    let res = state
        .bucket_configs
        .lock()
        .put_config(b_id, ConfigKind::Tagging, &body_bytes);
    match res {
        Ok(()) => {
            state.emit_bucket_config_event(b_id, bucket, "tagging", crate::events::CommitKind::BucketConfig, body_bytes.len() as u64);
            Ok(Response::builder()
                .status(StatusCode::NO_CONTENT)
                .header(CONTENT_LENGTH, "0")
                .body(empty_body())
                .unwrap())
        }
        Err(e) => {
            let status = StatusCode::from_u16(e.status()).unwrap_or(StatusCode::BAD_REQUEST);
            Ok(xml_error(status, e.code(), &e.to_string(), bucket))
        }
    }
}

pub async fn get_bucket_tagging(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let cfg = state.bucket_config_reader.load_bucket(b_id);
    let parsed = cfg.as_ref().and_then(|c| c.get(ConfigKind::Tagging));
    match parsed {
        Some(p) => {
            let mut tag_set = match p.parsed.as_ref().and_then(|a| a.downcast_ref::<TagSet>()) {
                Some(ts) => ts.clone(),
                None => match std::str::from_utf8(&p.raw)
                    .ok()
                    .and_then(|s| parse_tagging_xml(s, Scope::Bucket).ok())
                {
                    Some(ts) => ts,
                    None => TagSet::default(),
                },
            };
            tag_set.tags.sort_by(|a, b| a.key.cmp(&b.key));
            let xml = tagging_xml(&tag_set);
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap())
        }
        None => Ok(xml_error(
            StatusCode::NOT_FOUND,
            "NoSuchTagSet",
            "The TagSet does not exist",
            bucket,
        )),
    }
}

pub async fn delete_bucket_tagging(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let res = state
        .bucket_configs
        .lock()
        .delete_config(b_id, ConfigKind::Tagging);
    match res {
        Ok(()) => {
            state.emit_bucket_config_event(b_id, bucket, "tagging", crate::events::CommitKind::BucketConfig, 0);
            Ok(Response::builder()
                .status(StatusCode::NO_CONTENT)
                .header(CONTENT_LENGTH, "0")
                .body(empty_body())
                .unwrap())
        }
        Err(e) => {
            let status = StatusCode::from_u16(e.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            Ok(xml_error(status, e.code(), &e.to_string(), bucket))
        }
    }
}

fn map_set_attr_error(key: &str, err: SetAttrError) -> Response<ResponseBody> {
    match err {
        SetAttrError::TooLarge => bad_request(
            "MetadataTooLarge",
            "Your metadata headers exceed the maximum allowed metadata size.",
            key,
        ),
        SetAttrError::Internal { detail, .. } => internal_error_response(key, &detail),
        SetAttrError::Refused {
            delete_marker: Some((vid_str, explicit_vid)),
            ..
        } => {
            let (status, code, msg) = if explicit_vid {
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
            Response::builder()
                .status(status)
                .header(CONTENT_TYPE, "application/xml")
                .header("x-amz-delete-marker", "true")
                .header("x-amz-version-id", vid_str)
                .body(full_body(Bytes::from(xml)))
                .unwrap()
        }
        SetAttrError::Refused { refusal, .. } => {
            let status = StatusCode::from_u16(refusal.status).unwrap_or(StatusCode::NOT_FOUND);
            let msg = match refusal.code {
                "NoSuchVersion" => "The specified version does not exist.",
                "NoSuchKey" => "The specified key does not exist.",
                _ => refusal.why,
            };
            xml_error(status, refusal.code, msg, key)
        }
    }
}

async fn update_object_tags(
    state: &S3ServiceState,
    b_id: u32,
    key: &str,
    vid: Option<&str>,
    new_tags: Option<TagSet>,
    ok_status: StatusCode,
) -> Response<ResponseBody> {
    let clearing = new_tags.as_ref().is_none_or(|s| s.tags.is_empty());
    let mut upd = Attrs::new();
    if let Some(ref set) = new_tags {
        if !set.tags.is_empty() {
            upd.set(TAG_TAGGING, &encode_tags(set));
            return match state.set_attribute_group(b_id, key, vid, upd).await {
                Ok(out) => {
                    let mut b = Response::builder().status(ok_status).header(CONTENT_LENGTH, "0");
                    if let Some(vid_str) = out.version_id {
                        b = b.header("x-amz-version-id", vid_str);
                    }
                    b.body(empty_body()).unwrap()
                }
                Err(e) => map_set_attr_error(key, e),
            };
        }
    }
    debug_assert!(clearing);
    let target = match resolve_target(state, b_id, key, vid) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let target_vid = target.version.as_ref().map_or(0, |v| v.id());
    let view = match state.object_view_for(b_id, key, target_vid, target.vol_offset, target.flags) {
        Ok(v) => v,
        Err(e) => return internal_error_response(key, &format!("could not read the object's attributes: {:?}", e)),
    };
    if view.attrs.tag_count() == 0 {
        let mut b = Response::builder().status(ok_status).header(CONTENT_LENGTH, "0");
        if let Some(ref v) = target.version {
            b = b.header("x-amz-version-id", v.id_string());
        }
        return b.body(empty_body()).unwrap();
    }
    upd.set(TAG_TAGGING, &encode_tags(&TagSet::default()));
    match state.set_attribute_group(b_id, key, vid, upd).await {
        Ok(out) => {
            let mut b = Response::builder().status(ok_status).header(CONTENT_LENGTH, "0");
            if let Some(vid_str) = out.version_id {
                b = b.header("x-amz-version-id", vid_str);
            }
            b.body(empty_body()).unwrap()
        }
        Err(e) => map_set_attr_error(key, e),
    }
}

pub(crate) fn parse_tagging_header_from(
    headers: &hyper::HeaderMap,
    key: &str,
) -> Result<Option<TagSet>, Response<ResponseBody>> {
    let Some(tagging) = headers.get("x-amz-tagging") else {
        return Ok(None);
    };
    let raw = tagging
        .to_str()
        .map_err(|_| bad_request("InvalidTag", "Invalid x-amz-tagging header.", key))?;
    let tag_set = crate::tagging::parse_tagging_header(raw)
        .map_err(|r| bad_request(r.code, r.why, key))?;
    Ok((!tag_set.tags.is_empty()).then_some(tag_set))
}
