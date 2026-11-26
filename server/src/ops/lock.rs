//! Object Lock HTTP handlers (`GET`/`PUT ?object-lock`, `?retention`, `?legal-hold`, #120).

use bytes::Bytes;
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE};
use hyper::{Response, StatusCode};
use std::collections::HashMap;
use std::sync::Arc;

use crate::bucket_config::ConfigKind;
use crate::fsm::LockEvent;
use crate::object_lock::{
    lock_configuration_xml, parse_legal_hold, parse_lock_configuration, parse_retention_or_clear,
    retention_xml, legal_hold_xml,
};
use crate::s3_api::{
    bad_request, empty_body, full_body, internal_error_response, read_body_admitted,
    ResponseBody, S3ServiceState, SetAttrError, MAX_XML_BODY,
};
use crate::versions::Versioning;
use crate::xml::s3_error_xml;

use super::object::{find_version, no_such_version};

fn xml_error(status: StatusCode, code: &str, message: &str, resource: &str) -> Response<ResponseBody> {
    let xml = s3_error_xml(code, message, resource, "req-lock");
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap()
}

fn refusal_response(r: crate::fsm::Refusal, resource: &str) -> Response<ResponseBody> {
    xml_error(
        StatusCode::from_u16(r.status).unwrap_or(StatusCode::BAD_REQUEST),
        r.code,
        r.why,
        resource,
    )
}

fn no_lock_config_bucket_error(resource: &str) -> Response<ResponseBody> {
    xml_error(
        StatusCode::BAD_REQUEST,
        "InvalidRequest",
        "Bucket is missing Object Lock Configuration",
        resource,
    )
}

fn resolve_target_version(
    state: &S3ServiceState,
    b_id: u32,
    key: &str,
    vid: Option<&str>,
) -> Result<(u64, u64, u16), Response<ResponseBody>> {
    let parsed_id = match vid {
        Some(s) => match crate::versions::parse_id(s) {
            Some(id) => Some(id),
            None => return Err(no_such_version(key, false)),
        },
        None => None,
    };
    let Some(v) = find_version(state, b_id, key, parsed_id) else {
        return Err(if parsed_id.is_some() {
            no_such_version(key, false)
        } else {
            xml_error(
                StatusCode::NOT_FOUND,
                "NoSuchKey",
                "The specified key does not exist.",
                key,
            )
        });
    };
    if v.marker {
        let (status, code, msg) = if parsed_id.is_some() {
            (
                StatusCode::METHOD_NOT_ALLOWED,
                "MethodNotAllowed",
                "The specified method is not allowed against a delete marker.",
            )
        } else {
            (StatusCode::NOT_FOUND, "NoSuchKey", "The specified key does not exist.")
        };
        let xml = s3_error_xml(code, msg, key, "req-lock");
        return Err(Response::builder()
            .status(status)
            .header(CONTENT_TYPE, "application/xml")
            .header("x-amz-delete-marker", "true")
            .header("x-amz-version-id", v.id_string())
            .body(full_body(Bytes::from(xml)))
            .unwrap());
    }
    Ok((v.id(), v.vol_offset, v.flags))
}

fn map_set_attr_error(err: SetAttrError, key: &str, has_vid: bool) -> Response<ResponseBody> {
    match err {
        SetAttrError::Refused {
            refusal,
            delete_marker: Some((dm_vid, is_explicit)),
        } => {
            let (status, code, msg) = if is_explicit && has_vid {
                (
                    StatusCode::METHOD_NOT_ALLOWED,
                    "MethodNotAllowed",
                    "The specified method is not allowed against a delete marker.",
                )
            } else {
                (StatusCode::NOT_FOUND, "NoSuchKey", "The specified key does not exist.")
            };
            let xml = s3_error_xml(code, msg, key, "req-lock");
            let mut b = Response::builder()
                .status(status)
                .header(CONTENT_TYPE, "application/xml")
                .header("x-amz-delete-marker", "true");
            if !dm_vid.is_empty() {
                b = b.header("x-amz-version-id", dm_vid);
            }
            let _ = refusal;
            b.body(full_body(Bytes::from(xml))).unwrap()
        }
        SetAttrError::Refused { refusal, delete_marker: None } => refusal_response(refusal, key),
        SetAttrError::TooLarge => bad_request(
            "MetadataTooLarge",
            "Your metadata headers exceed the maximum allowed size.",
            key,
        ),
        SetAttrError::Internal { detail, .. } => internal_error_response(key, &detail),
    }
}

pub async fn get_object_lock_configuration(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let Some(cfg) = state.bucket_object_lock(b_id) else {
        return Ok(xml_error(
            StatusCode::NOT_FOUND,
            "ObjectLockConfigurationNotFoundError",
            "Object Lock configuration does not exist for this bucket",
            bucket,
        ));
    };
    let xml = lock_configuration_xml(&cfg);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap())
}

pub async fn put_object_lock_configuration(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    path: &str,
    body: hyper::body::Incoming,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let (body_bytes, _body_guard) = match read_body_admitted(
        &state.body_budget,
        &state.work,
        body,
        MAX_XML_BODY,
        None,
        state.body_idle_timeout(),
        path,
    )
    .await?
    {
        Ok(b) => b,
        Err(resp) => return Ok(resp),
    };
    let Ok(body_str) = std::str::from_utf8(&body_bytes) else {
        return Ok(bad_request(
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema.",
            bucket,
        ));
    };
    if let Err(refusal) = parse_lock_configuration(body_str) {
        return Ok(refusal_response(refusal, bucket));
    }

    let already_enabled = state.bucket_object_lock(b_id).is_some();
    if !already_enabled {
        let ver = state.versioning.read().get(&b_id).copied().unwrap_or_default();
        if ver != Versioning::Enabled {
            return Ok(xml_error(
                StatusCode::CONFLICT,
                "InvalidBucketState",
                "Versioning must be 'Enabled' on the bucket to apply a Object Lock configuration",
                bucket,
            ));
        }
    }

    let res = state
        .bucket_configs
        .lock()
        .put_config(b_id, ConfigKind::ObjectLock, &body_bytes);
    match res {
        Ok(()) => {
            state.emit_bucket_config_event(
                b_id,
                bucket,
                ConfigKind::ObjectLock.as_str(),
                crate::events::CommitKind::BucketConfig,
                body_bytes.len() as u64,
            );
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_LENGTH, "0")
                .body(empty_body())
                .unwrap())
        }
        Err(e) => Ok(xml_error(
            StatusCode::from_u16(e.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            e.code(),
            &e.to_string(),
            bucket,
        )),
    }
}

pub async fn get_object_retention(
    state: Arc<S3ServiceState>,
    b_id: u32,
    key: &str,
    query_map: &HashMap<String, String>,
) -> Result<Response<ResponseBody>, hyper::Error> {
    if state.bucket_object_lock(b_id).is_none() {
        return Ok(no_lock_config_bucket_error(key));
    }
    let vid = query_map.get("versionId").map(String::as_str);
    let (target_vid, vol_offset, flags) = match resolve_target_version(&state, b_id, key, vid) {
        Ok(v) => v,
        Err(resp) => return Ok(resp),
    };
    let view = match state.object_view_for(b_id, key, target_vid, vol_offset, flags) {
        Ok(v) => v,
        Err(e) => {
            return Ok(internal_error_response(
                key,
                &format!("could not read the object's retention: {:?}", e),
            ))
        }
    };
    let ret = match view.attrs.retention() {
        Ok(Some(r)) => r,
        Ok(None) => {
            return Ok(xml_error(
                StatusCode::NOT_FOUND,
                "NoSuchObjectLockConfiguration",
                "The specified object does not have a ObjectLock configuration",
                key,
            ));
        }
        Err(e) => {
            return Ok(internal_error_response(
                key,
                &format!("corrupt retention attribute: {:?}", e),
            ))
        }
    };
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(retention_xml(&ret))))
        .unwrap())
}

pub async fn put_object_retention(
    state: Arc<S3ServiceState>,
    b_id: u32,
    key: &str,
    path: &str,
    query_map: &HashMap<String, String>,
    bypass_governance: bool,
    body: hyper::body::Incoming,
) -> Result<Response<ResponseBody>, hyper::Error> {
    if state.bucket_object_lock(b_id).is_none() {
        return Ok(no_lock_config_bucket_error(key));
    }
    let (body_bytes, _body_guard) = match read_body_admitted(
        &state.body_budget,
        &state.work,
        body,
        MAX_XML_BODY,
        None,
        state.body_idle_timeout(),
        path,
    )
    .await?
    {
        Ok(b) => b,
        Err(resp) => return Ok(resp),
    };
    let Ok(body_str) = std::str::from_utf8(&body_bytes) else {
        return Ok(bad_request(
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema.",
            key,
        ));
    };
    let retention_opt = match parse_retention_or_clear(body_str) {
        Ok(r) => r,
        Err(refusal) => return Ok(refusal_response(refusal, key)),
    };
    let event = match retention_opt {
        Some(ref r) => LockEvent::PutRetention {
            mode: r.mode,
            until: r.retain_until_epoch,
            bypass_governance,
        },
        None => LockEvent::ClearRetention { bypass_governance },
    };
    let vid = query_map.get("versionId").map(String::as_str);
    match state
        .update_object_lock(b_id, key, vid, event, retention_opt.as_ref())
        .await
    {
        Ok(_) => Ok(Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_LENGTH, "0")
            .body(empty_body())
            .unwrap()),
        Err(e) => Ok(map_set_attr_error(e, key, vid.is_some())),
    }
}

pub async fn get_object_legal_hold(
    state: Arc<S3ServiceState>,
    b_id: u32,
    key: &str,
    query_map: &HashMap<String, String>,
) -> Result<Response<ResponseBody>, hyper::Error> {
    if state.bucket_object_lock(b_id).is_none() {
        return Ok(no_lock_config_bucket_error(key));
    }
    let vid = query_map.get("versionId").map(String::as_str);
    let (target_vid, vol_offset, flags) = match resolve_target_version(&state, b_id, key, vid) {
        Ok(v) => v,
        Err(resp) => return Ok(resp),
    };
    let view = match state.object_view_for(b_id, key, target_vid, vol_offset, flags) {
        Ok(v) => v,
        Err(e) => {
            return Ok(internal_error_response(
                key,
                &format!("could not read the object's legal hold: {:?}", e),
            ))
        }
    };
    let on = match view.attrs.legal_hold() {
        Ok(Some(on)) => on,
        Ok(None) => {
            return Ok(xml_error(
                StatusCode::NOT_FOUND,
                "NoSuchObjectLockConfiguration",
                "The specified object does not have a ObjectLock configuration",
                key,
            ));
        }
        Err(e) => {
            return Ok(internal_error_response(
                key,
                &format!("corrupt legal hold attribute: {:?}", e),
            ))
        }
    };
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(legal_hold_xml(on))))
        .unwrap())
}

pub async fn put_object_legal_hold(
    state: Arc<S3ServiceState>,
    b_id: u32,
    key: &str,
    path: &str,
    query_map: &HashMap<String, String>,
    body: hyper::body::Incoming,
) -> Result<Response<ResponseBody>, hyper::Error> {
    if state.bucket_object_lock(b_id).is_none() {
        return Ok(no_lock_config_bucket_error(key));
    }
    let (body_bytes, _body_guard) = match read_body_admitted(
        &state.body_budget,
        &state.work,
        body,
        MAX_XML_BODY,
        None,
        state.body_idle_timeout(),
        path,
    )
    .await?
    {
        Ok(b) => b,
        Err(resp) => return Ok(resp),
    };
    let Ok(body_str) = std::str::from_utf8(&body_bytes) else {
        return Ok(bad_request(
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema.",
            key,
        ));
    };
    let on = match parse_legal_hold(body_str) {
        Ok(on) => on,
        Err(refusal) => return Ok(refusal_response(refusal, key)),
    };
    let vid = query_map.get("versionId").map(String::as_str);
    match state
        .update_object_lock(b_id, key, vid, LockEvent::PutLegalHold { on }, None)
        .await
    {
        Ok(_) => Ok(Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_LENGTH, "0")
            .body(empty_body())
            .unwrap()),
        Err(e) => Ok(map_set_attr_error(e, key, vid.is_some())),
    }
}
