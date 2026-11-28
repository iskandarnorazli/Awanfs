//! Bucket policy (`?policy`, `?policyStatus`), Public Access Block (`?publicAccessBlock`), and
//! Ownership Controls (`?ownershipControls`) HTTP handlers (#122).

use bytes::Bytes;
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE};
use hyper::{Response, StatusCode};
use std::sync::Arc;

use crate::acl::{
    owner_for_account, parse_ownership_xml, parse_pab_xml, render_ownership_xml, render_pab_xml,
    BucketOwnership,
};
use crate::bucket_config::{ConfigError, ConfigKind};
use crate::iam::Iam;
use crate::policy::{parse_bucket_policy_for_bucket, policy_status_xml, MAX_POLICY_BYTES};
use crate::s3_api::{
    bad_request, empty_body, full_body, internal_error_response, read_body_admitted, ResponseBody,
    S3ServiceState, MAX_XML_BODY,
};
use crate::xml::s3_error_xml;

fn config_err_response(err: ConfigError, bucket: &str) -> Response<ResponseBody> {
    match err {
        ConfigError::Rejected { status, code, message } => {
            let xml = s3_error_xml(code, &message, bucket, "req-cfg");
            Response::builder()
                .status(StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_REQUEST))
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap()
        }
        ConfigError::TooLarge { max, .. } => bad_request(
            "InvalidRequest",
            &format!("Configuration exceeds {} byte limit", max),
            bucket,
        ),
        ConfigError::NoValidator(_) => internal_error_response(bucket, "missing config validator"),
        ConfigError::Fsm(r) => {
            let xml = s3_error_xml(r.code, r.why, bucket, "req-cfg");
            Response::builder()
                .status(StatusCode::from_u16(r.status).unwrap_or(StatusCode::NOT_FOUND))
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap()
        }
        ConfigError::Io(e) => internal_error_response(bucket, &format!("config io error: {}", e)),
    }
}

fn not_found_err(code: &str, message: &str, bucket: &str) -> Response<ResponseBody> {
    let xml = s3_error_xml(code, message, bucket, "req-cfg");
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap()
}

pub async fn get_bucket_policy(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let raw = state
        .bucket_config_reader
        .load_bucket(b_id)
        .and_then(|c| c.get(ConfigKind::Policy).map(|p| p.raw.clone()));
    match raw {
        Some(bytes) => Ok(Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .body(full_body(bytes))
            .unwrap()),
        None => Ok(not_found_err(
            "NoSuchBucketPolicy",
            "The bucket policy does not exist",
            bucket,
        )),
    }
}

pub async fn put_bucket_policy(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    path: &str,
    body: hyper::body::Incoming,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let (body_bytes, _guard) = match read_body_admitted(
        &state.body_budget,
        &state.work,
        body,
        MAX_POLICY_BYTES as u64,
        None,
        state.body_idle_timeout(),
        path,
    )
    .await?
    {
        Ok(b) => b,
        Err(resp) => return Ok(resp),
    };
    let Ok(text) = std::str::from_utf8(&body_bytes) else {
        return Ok(bad_request("MalformedPolicy", "This policy contains invalid Json", bucket));
    };
    let parsed = match parse_bucket_policy_for_bucket(text, Some(bucket)) {
        Ok(p) => p,
        Err(r) => return Ok(bad_request(r.code, r.why, bucket)),
    };
    let pab = state
        .bucket_config_reader
        .load_bucket(b_id)
        .map(|c| c.public_access_block())
        .unwrap_or_default();
    if parsed.is_public && pab.block_public_policy {
        let xml = s3_error_xml("AccessDenied", "Access Denied", bucket, "req-pab");
        return Ok(Response::builder()
            .status(StatusCode::FORBIDDEN)
            .header(CONTENT_TYPE, "application/xml")
            .body(full_body(Bytes::from(xml)))
            .unwrap());
    }
    if let Err(e) = state.bucket_configs.lock().put_config(b_id, ConfigKind::Policy, &body_bytes) {
        return Ok(config_err_response(e, bucket));
    }
    state.emit_bucket_config_event(b_id, bucket, "policy", crate::events::CommitKind::BucketConfig, body_bytes.len() as u64);
    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(CONTENT_LENGTH, "0")
        .body(empty_body())
        .unwrap())
}

pub async fn delete_bucket_policy(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
) -> Result<Response<ResponseBody>, hyper::Error> {
    if let Err(e) = state.bucket_configs.lock().delete_config(b_id, ConfigKind::Policy) {
        return Ok(config_err_response(e, bucket));
    }
    state.emit_bucket_config_event(b_id, bucket, "policy", crate::events::CommitKind::BucketConfig, 0);
    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(CONTENT_LENGTH, "0")
        .body(empty_body())
        .unwrap())
}

pub async fn get_bucket_policy_status(
    state: Arc<S3ServiceState>,
    b_id: u32,
    _bucket: &str,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let is_public = state.bucket_config_reader.load_bucket(b_id).is_some_and(|c| {
        let pab = c.public_access_block();
        let pol_pub = c.bucket_policy().is_some_and(|p| p.is_public);
        let acl_pub = c.ownership_or(state.default_bucket_ownership) != BucketOwnership::BucketOwnerEnforced
            && !pab.ignore_public_acls
            && c.acl().is_some_and(|a| a.is_public());
        pol_pub || acl_pub
    });
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(policy_status_xml(is_public))))
        .unwrap())
}

pub async fn get_bucket_ownership_controls(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let explicit = state
        .bucket_config_reader
        .load_bucket(b_id)
        .and_then(|c| c.explicit_ownership());
    match explicit {
        Some(own) => Ok(Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/xml")
            .body(full_body(Bytes::from(render_ownership_xml(own))))
            .unwrap()),
        None => Ok(not_found_err(
            "OwnershipControlsNotFoundError",
            "The bucket ownership controls were not found",
            bucket,
        )),
    }
}

pub async fn put_bucket_ownership_controls(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    path: &str,
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
    let Ok(text) = std::str::from_utf8(&body_bytes) else {
        return Ok(bad_request("MalformedXML", "OwnershipControls XML is not valid UTF-8", bucket));
    };
    let own = match parse_ownership_xml(text) {
        Ok(o) => o,
        Err(r) => return Ok(bad_request(r.code, r.why, bucket)),
    };
    if own == BucketOwnership::BucketOwnerEnforced {
        if let Some(cfg) = state.bucket_config_reader.load_bucket(b_id) {
            let owner = owner_for_account(cfg.effective_owner(), iam);
            if let Some(acp) = cfg.acl() {
                if !acp.is_private_bucket_acl(&owner.id) {
                    return Ok(bad_request(
                        "InvalidBucketAclWithObjectOwnership",
                        "Bucket cannot have ACLs set with ObjectOwnership's BucketOwnerEnforced setting.",
                        bucket,
                    ));
                }
            }
        }
    }
    let canonical = render_ownership_xml(own);
    if let Err(e) = state
        .bucket_configs
        .lock()
        .put_config(b_id, ConfigKind::Ownership, canonical.as_bytes())
    {
        return Ok(config_err_response(e, bucket));
    }
    state.emit_bucket_config_event(b_id, bucket, "policy", crate::events::CommitKind::BucketConfig, body_bytes.len() as u64);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_LENGTH, "0")
        .body(empty_body())
        .unwrap())
}

pub async fn delete_bucket_ownership_controls(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
) -> Result<Response<ResponseBody>, hyper::Error> {
    if let Err(e) = state.bucket_configs.lock().delete_config(b_id, ConfigKind::Ownership) {
        return Ok(config_err_response(e, bucket));
    }
    state.emit_bucket_config_event(b_id, bucket, "policy", crate::events::CommitKind::BucketConfig, 0);
    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(CONTENT_LENGTH, "0")
        .body(empty_body())
        .unwrap())
}

pub async fn get_public_access_block(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let raw = state
        .bucket_config_reader
        .load_bucket(b_id)
        .and_then(|c| c.get(ConfigKind::PublicAccessBlock).map(|p| p.raw.clone()));
    match raw {
        Some(_) => {
            let pab = state
                .bucket_config_reader
                .load_bucket(b_id)
                .map(|c| c.public_access_block())
                .unwrap_or_default();
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(render_pab_xml(pab))))
                .unwrap())
        }
        None => Ok(not_found_err(
            "NoSuchPublicAccessBlockConfiguration",
            "The public access block configuration was not found",
            bucket,
        )),
    }
}

pub async fn put_public_access_block(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    path: &str,
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
    let Ok(text) = std::str::from_utf8(&body_bytes) else {
        return Ok(bad_request("MalformedXML", "PublicAccessBlock XML is not valid UTF-8", bucket));
    };
    let pab = match parse_pab_xml(text) {
        Ok(p) => p,
        Err(r) => return Ok(bad_request(r.code, r.why, bucket)),
    };
    let canonical = render_pab_xml(pab);
    if let Err(e) = state
        .bucket_configs
        .lock()
        .put_config(b_id, ConfigKind::PublicAccessBlock, canonical.as_bytes())
    {
        return Ok(config_err_response(e, bucket));
    }
    state.emit_bucket_config_event(b_id, bucket, "policy", crate::events::CommitKind::BucketConfig, body_bytes.len() as u64);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_LENGTH, "0")
        .body(empty_body())
        .unwrap())
}

pub async fn delete_public_access_block(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
) -> Result<Response<ResponseBody>, hyper::Error> {
    if let Err(e) = state.bucket_configs.lock().delete_config(b_id, ConfigKind::PublicAccessBlock) {
        return Ok(config_err_response(e, bucket));
    }
    state.emit_bucket_config_event(b_id, bucket, "policy", crate::events::CommitKind::BucketConfig, 0);
    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(CONTENT_LENGTH, "0")
        .body(empty_body())
        .unwrap())
}
