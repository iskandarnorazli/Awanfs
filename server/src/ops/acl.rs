//! Bucket and object ACL HTTP handlers (`GET`/`PUT ?acl`) (#122).

use bytes::Bytes;
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE};
use hyper::{HeaderMap, Response, StatusCode};
use std::collections::HashMap;
use std::sync::Arc;

use super::object::{find_version, no_such_version};

fn no_such_key(key: &str) -> Response<ResponseBody> {
    let xml = s3_error_xml("NoSuchKey", "The specified key does not exist.", key, "req-acl");
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap()
}
use crate::acl::{
    acl_from_headers, has_grant_headers, owner_for_account, parse_acl_xml, render_acl_xml,
    AccessControlPolicy, BucketOwnership, CannedAcl,
};
use crate::attrs::Attrs;
use crate::bucket_config::ConfigKind;
use crate::ffi_bridge::unpack_flags;
use crate::iam::{Iam, ROOT_ACCOUNT};
use crate::s3_api::{
    bad_request, empty_body, full_body, internal_error_response, read_body_admitted, ResponseBody,
    S3ServiceState, MAX_XML_BODY,
};
use crate::xml::s3_error_xml;

fn forbidden_pab(resource: &str) -> Response<ResponseBody> {
    let xml = s3_error_xml("AccessDenied", "Access Denied", resource, "req-pab");
    Response::builder()
        .status(StatusCode::FORBIDDEN)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap()
}

pub async fn get_bucket_acl(
    state: Arc<S3ServiceState>,
    b_id: u32,
    iam: Option<&Iam>,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let cfg = state.bucket_config_reader.load_bucket(b_id);
    let owner_acct = cfg.as_deref().map_or(ROOT_ACCOUNT, |c| c.effective_owner());
    let owner = owner_for_account(owner_acct, iam);
    let acp = cfg
        .as_deref()
        .and_then(|c| c.acl())
        .map(|a| {
            AccessControlPolicy {
                owner: owner.clone(),
                grants: a.grants.clone(),
            }
        })
        .unwrap_or_else(|| CannedAcl::Private.expand(&owner, None));
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(render_acl_xml(&acp))))
        .unwrap())
}

pub async fn put_bucket_acl(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    path: &str,
    headers: &HeaderMap,
    iam: Option<&Iam>,
    body: hyper::body::Incoming,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let (body_bytes, _guard) = match read_body_admitted(
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
    let cfg = state.bucket_config_reader.load_bucket(b_id);
    let ownership = cfg
        .as_deref()
        .map_or(state.default_bucket_ownership, |c| c.ownership_or(state.default_bucket_ownership));
    if ownership == BucketOwnership::BucketOwnerEnforced {
        return Ok(bad_request(
            "AccessControlListNotSupported",
            "The bucket does not allow ACLs",
            bucket,
        ));
    }
    let owner_acct = cfg.as_deref().map_or(ROOT_ACCOUNT, |c| c.effective_owner());
    let owner = owner_for_account(owner_acct, iam);
    let has_canned = headers.contains_key("x-amz-acl");
    let has_grant_hdrs = has_grant_headers(headers);
    let has_body = !body_bytes.is_empty();
    if has_body && (has_canned || has_grant_hdrs) {
        return Ok(bad_request("UnexpectedContent", "This request does not support content", bucket));
    }
    let mut acp = if has_canned || has_grant_hdrs {
        match acl_from_headers(headers, &owner, None, iam) {
            Ok(Some(a)) => a,
            Ok(None) => CannedAcl::Private.expand(&owner, None),
            Err(r) => return Ok(bad_request(r.code, r.why, bucket)),
        }
    } else if has_body {
        let Ok(xml_str) = std::str::from_utf8(&body_bytes) else {
            return Ok(bad_request("MalformedACLError", "ACL XML is not valid UTF-8", bucket));
        };
        let mut parsed = match parse_acl_xml(xml_str, iam) {
            Ok(a) => a,
            Err(r) => return Ok(bad_request(r.code, r.why, bucket)),
        };
        parsed.owner = owner;
        parsed
    } else {
        CannedAcl::Private.expand(&owner, None)
    };
    acp.owner = owner_for_account(owner_acct, iam);

    let pab = cfg
        .as_deref()
        .map_or(Default::default(), |c| c.public_access_block());
    if pab.block_public_acls && acp.is_public() {
        return Ok(forbidden_pab(bucket));
    }

    let xml = render_acl_xml(&acp);
    if let Err(e) = state.bucket_configs.lock().put_config(b_id, ConfigKind::Acl, xml.as_bytes()) {
        return Ok(internal_error_response(bucket, &format!("config error: {:?}", e)));
    }
    state.emit_bucket_config_event(b_id, bucket, "acl", crate::events::CommitKind::BucketConfig, xml.len() as u64);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_LENGTH, "0")
        .body(empty_body())
        .unwrap())
}

pub async fn get_object_acl(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    key: &str,
    query_map: &HashMap<String, String>,
    iam: Option<&Iam>,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let req_vid = query_map.get("versionId").map(|s| s.as_str());
    let (target_vid, vol_offset, flags) = if let Some(v_str) = req_vid {
        let Some(v_id) = crate::versions::parse_id(v_str) else {
            return Ok(no_such_version(key, false));
        };
        let Some(v) = find_version(&state, b_id, key, Some(v_id)) else {
            return Ok(no_such_version(key, false));
        };
        if v.marker {
            return Ok(no_such_key(key));
        }
        (v.id(), v.vol_offset, v.flags)
    } else {
        let cur_v = state.versions.read().current(b_id, key);
        if let Some(v) = cur_v {
            if v.marker {
                return Ok(no_such_key(key));
            }
            (v.id(), v.vol_offset, v.flags)
        } else {
            let Ok(meta) = state.engine.read().get_object(b_id, key.as_bytes()) else {
                return Ok(no_such_key(key));
            };
            (0, meta.vol_offset, unpack_flags(meta.bucket_flags))
        }
    };

    let cfg = state.bucket_config_reader.load_bucket(b_id);
    let ownership = cfg
        .as_deref()
        .map_or(state.default_bucket_ownership, |c| c.ownership_or(state.default_bucket_ownership));
    let b_owner_acct = cfg.as_deref().map_or(ROOT_ACCOUNT, |c| c.effective_owner());
    let view = state
        .object_view_for(b_id, key, target_vid, vol_offset, flags)
        .ok();
    let eff_obj_acct = if ownership == BucketOwnership::BucketOwnerEnforced {
        b_owner_acct
    } else {
        view.as_ref().and_then(|v| v.attrs.owner()).unwrap_or(b_owner_acct)
    };
    let obj_owner = owner_for_account(eff_obj_acct, iam);

    let acp = if ownership == BucketOwnership::BucketOwnerEnforced {
        CannedAcl::Private.expand(&obj_owner, None)
    } else if let Some(xml) = view.as_ref().and_then(|v| v.attrs.acl_xml()) {
        match crate::acl::parse_acl_xml_structural(xml) {
            Ok(mut a) => {
                a.owner = obj_owner;
                a
            }
            Err(_) => CannedAcl::Private.expand(&obj_owner, None),
        }
    } else {
        CannedAcl::Private.expand(&obj_owner, None)
    };

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml");
    if target_vid != 0 {
        builder = builder.header("x-amz-version-id", crate::versions::id_string(target_vid));
    }
    let _ = bucket;
    Ok(builder.body(full_body(Bytes::from(render_acl_xml(&acp)))).unwrap())
}

pub async fn put_object_acl(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    key: &str,
    path: &str,
    query_map: &HashMap<String, String>,
    headers: &HeaderMap,
    iam: Option<&Iam>,
    body: hyper::body::Incoming,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let req_vid = query_map.get("versionId").map(|s| s.as_str());
    let (body_bytes, _guard) = match read_body_admitted(
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

    let cfg = state.bucket_config_reader.load_bucket(b_id);
    let ownership = cfg
        .as_deref()
        .map_or(state.default_bucket_ownership, |c| c.ownership_or(state.default_bucket_ownership));
    if ownership == BucketOwnership::BucketOwnerEnforced {
        return Ok(bad_request(
            "AccessControlListNotSupported",
            "The bucket does not allow ACLs",
            bucket,
        ));
    }

    // Lookup existing object/version to determine its true owner and existence.
    let (target_vid, vol_offset, flags) = if let Some(v_str) = req_vid {
        let Some(v_id) = crate::versions::parse_id(v_str) else {
            return Ok(no_such_version(key, false));
        };
        let Some(v) = find_version(&state, b_id, key, Some(v_id)) else {
            return Ok(no_such_version(key, false));
        };
        if v.marker {
            return Ok(no_such_key(key));
        }
        (v.id(), v.vol_offset, v.flags)
    } else {
        let cur_v = state.versions.read().current(b_id, key);
        if let Some(v) = cur_v {
            if v.marker {
                return Ok(no_such_key(key));
            }
            (v.id(), v.vol_offset, v.flags)
        } else {
            let Ok(meta) = state.engine.read().get_object(b_id, key.as_bytes()) else {
                return Ok(no_such_key(key));
            };
            (0, meta.vol_offset, unpack_flags(meta.bucket_flags))
        }
    };

    let b_owner_acct = cfg.as_deref().map_or(ROOT_ACCOUNT, |c| c.effective_owner());
    let b_owner = owner_for_account(b_owner_acct, iam);
    let view = state
        .object_view_for(b_id, key, target_vid, vol_offset, flags)
        .ok();
    let eff_obj_acct = view
        .as_ref()
        .and_then(|v| v.attrs.owner())
        .unwrap_or(b_owner_acct);
    let obj_owner = owner_for_account(eff_obj_acct, iam);

    let has_canned = headers.contains_key("x-amz-acl");
    let has_grant_hdrs = has_grant_headers(headers);
    let has_body = !body_bytes.is_empty();
    if has_body && (has_canned || has_grant_hdrs) {
        return Ok(bad_request("UnexpectedContent", "This request does not support content", key));
    }

    let mut acp = if has_canned || has_grant_hdrs {
        match acl_from_headers(headers, &obj_owner, Some(&b_owner), iam) {
            Ok(Some(a)) => a,
            Ok(None) => CannedAcl::Private.expand(&obj_owner, Some(&b_owner)),
            Err(r) => return Ok(bad_request(r.code, r.why, key)),
        }
    } else if has_body {
        let Ok(xml_str) = std::str::from_utf8(&body_bytes) else {
            return Ok(bad_request("MalformedACLError", "ACL XML is not valid UTF-8", key));
        };
        let mut parsed = match parse_acl_xml(xml_str, iam) {
            Ok(a) => a,
            Err(r) => return Ok(bad_request(r.code, r.why, key)),
        };
        parsed.owner = obj_owner;
        parsed
    } else {
        CannedAcl::Private.expand(&obj_owner, Some(&b_owner))
    };
    acp.owner = owner_for_account(eff_obj_acct, iam);

    let pab = cfg
        .as_deref()
        .map_or(Default::default(), |c| c.public_access_block());
    if pab.block_public_acls && acp.is_public() {
        return Ok(forbidden_pab(key));
    }

    let xml = render_acl_xml(&acp);
    let mut update = Attrs::new();
    update.set_acl_xml(&xml);
    match state.set_attribute_group(b_id, key, req_vid, update).await {
        Ok(outcome) => {
            let mut builder = Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_LENGTH, "0");
            if let Some(vid) = outcome.version_id {
                builder = builder.header("x-amz-version-id", vid);
            }
            Ok(builder.body(empty_body()).unwrap())
        }
        Err(crate::s3_api::SetAttrError::Refused { refusal, delete_marker }) => {
            let mut builder = Response::builder()
                .status(StatusCode::from_u16(refusal.status).unwrap_or(StatusCode::NOT_FOUND))
                .header(CONTENT_TYPE, "application/xml");
            if let Some((dm_vid, is_dm)) = delete_marker {
                builder = builder.header("x-amz-version-id", dm_vid);
                if is_dm {
                    builder = builder.header("x-amz-delete-marker", "true");
                }
            }
            let err_xml = s3_error_xml(refusal.code, refusal.why, key, "req-acl");
            Ok(builder.body(full_body(Bytes::from(err_xml))).unwrap())
        }
        Err(crate::s3_api::SetAttrError::TooLarge) => Ok(bad_request(
            "MetadataTooLarge",
            "ACL exceeds maximum metadata size.",
            key,
        )),
        Err(crate::s3_api::SetAttrError::Internal { detail, .. }) => Ok(internal_error_response(key, &detail)),
    }
}
