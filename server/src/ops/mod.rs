pub mod bucket;
pub mod lifecycle;
pub mod multipart;
pub mod object;
pub mod service;

use bytes::Bytes;
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE, HOST};
use hyper::{HeaderMap, Method, Request, Response, StatusCode};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::iam::{Iam, Principal};
use crate::s3_api::{
    body_limit_for, empty_body, entity_too_large, full_body, parse_s3_target, percent_decode,
    percent_decode_path, validate_s3_key, ParsedTarget, ResponseBody, S3ServiceState,
};
use crate::xml::s3_error_xml;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operation {
    ListBuckets,
    CreateBucket,
    DeleteBucket,
    HeadBucket,
    ListObjects,
    ListObjectsV2,
    ListObjectVersions,
    GetBucketLocation,
    GetBucketVersioning,
    PutBucketVersioning,
    GetBucketLifecycle,
    PutBucketLifecycle,
    DeleteBucketLifecycle,
    ListMultipartUploads,
    DeleteObjects,
    PutObject,
    CopyObject,
    GetObject,
    HeadObject,
    DeleteObject,
    CreateMultipartUpload,
    UploadPart,
    CompleteMultipartUpload,
    AbortMultipartUpload,
    ListParts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetKind {
    Service,
    Bucket,
    Object,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclPermission {
    Read,
    Write,
    ReadAcp,
    WriteAcp,
    FullControl,
}

pub struct OpSpec {
    pub op: Operation,
    pub name: &'static str,
    pub iam_action: Option<&'static str>,
    pub acl_permission: AclPermission,
    pub target: TargetKind,
    pub requires_existing_bucket: bool,
}

pub const OPS: &[OpSpec] = &[
    OpSpec {
        op: Operation::ListBuckets,
        name: "ListBuckets",
        iam_action: Some("s3:ListAllMyBuckets"),
        acl_permission: AclPermission::Read,
        target: TargetKind::Service,
        requires_existing_bucket: false,
    },
    OpSpec {
        op: Operation::CreateBucket,
        name: "CreateBucket",
        iam_action: Some("s3:CreateBucket"),
        acl_permission: AclPermission::Write,
        target: TargetKind::Bucket,
        requires_existing_bucket: false,
    },
    OpSpec {
        op: Operation::DeleteBucket,
        name: "DeleteBucket",
        iam_action: Some("s3:DeleteBucket"),
        acl_permission: AclPermission::FullControl,
        target: TargetKind::Bucket,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::HeadBucket,
        name: "HeadBucket",
        iam_action: Some("s3:ListBucket"),
        acl_permission: AclPermission::Read,
        target: TargetKind::Bucket,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::ListObjects,
        name: "ListObjects",
        iam_action: Some("s3:ListBucket"),
        acl_permission: AclPermission::Read,
        target: TargetKind::Bucket,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::ListObjectsV2,
        name: "ListObjects",
        iam_action: Some("s3:ListBucket"),
        acl_permission: AclPermission::Read,
        target: TargetKind::Bucket,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::ListObjectVersions,
        name: "ListObjectVersions",
        iam_action: Some("s3:ListBucket"),
        acl_permission: AclPermission::Read,
        target: TargetKind::Bucket,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::GetBucketLocation,
        name: "ListObjects",
        iam_action: Some("s3:ListBucket"),
        acl_permission: AclPermission::Read,
        target: TargetKind::Bucket,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::GetBucketVersioning,
        name: "GetBucketVersioning",
        iam_action: Some("s3:ListBucket"),
        acl_permission: AclPermission::Read,
        target: TargetKind::Bucket,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::PutBucketVersioning,
        name: "PutBucketVersioning",
        iam_action: Some("s3:CreateBucket"),
        acl_permission: AclPermission::Write,
        target: TargetKind::Bucket,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::GetBucketLifecycle,
        name: "GetBucketLifecycle",
        iam_action: Some("s3:GetLifecycleConfiguration"),
        acl_permission: AclPermission::Read,
        target: TargetKind::Bucket,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::PutBucketLifecycle,
        name: "PutBucketLifecycle",
        iam_action: Some("s3:PutLifecycleConfiguration"),
        acl_permission: AclPermission::Write,
        target: TargetKind::Bucket,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::DeleteBucketLifecycle,
        name: "DeleteBucket",
        iam_action: Some("s3:PutLifecycleConfiguration"),
        acl_permission: AclPermission::Write,
        target: TargetKind::Bucket,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::ListMultipartUploads,
        name: "ListObjects",
        iam_action: Some("s3:ListBucketMultipartUploads"),
        acl_permission: AclPermission::Read,
        target: TargetKind::Bucket,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::DeleteObjects,
        name: "DeleteObjects",
        iam_action: None,
        acl_permission: AclPermission::Write,
        target: TargetKind::Bucket,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::PutObject,
        name: "PutObject",
        iam_action: Some("s3:PutObject"),
        acl_permission: AclPermission::Write,
        target: TargetKind::Object,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::CopyObject,
        name: "CopyObject",
        iam_action: Some("s3:PutObject"),
        acl_permission: AclPermission::Write,
        target: TargetKind::Object,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::GetObject,
        name: "GetObject",
        iam_action: Some("s3:GetObject"),
        acl_permission: AclPermission::Read,
        target: TargetKind::Object,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::HeadObject,
        name: "HeadObject",
        iam_action: Some("s3:GetObject"),
        acl_permission: AclPermission::Read,
        target: TargetKind::Object,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::DeleteObject,
        name: "DeleteObject",
        iam_action: Some("s3:DeleteObject"),
        acl_permission: AclPermission::Write,
        target: TargetKind::Object,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::CreateMultipartUpload,
        name: "CreateMultipartUpload",
        iam_action: Some("s3:PutObject"),
        acl_permission: AclPermission::Write,
        target: TargetKind::Object,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::UploadPart,
        name: "UploadPart",
        iam_action: Some("s3:PutObject"),
        acl_permission: AclPermission::Write,
        target: TargetKind::Object,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::CompleteMultipartUpload,
        name: "CompleteMultipartUpload",
        iam_action: Some("s3:PutObject"),
        acl_permission: AclPermission::Write,
        target: TargetKind::Object,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::AbortMultipartUpload,
        name: "AbortMultipartUpload",
        iam_action: Some("s3:AbortMultipartUpload"),
        acl_permission: AclPermission::Write,
        target: TargetKind::Object,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::ListParts,
        name: "GetObject",
        iam_action: Some("s3:ListMultipartUploadParts"),
        acl_permission: AclPermission::Read,
        target: TargetKind::Object,
        requires_existing_bucket: true,
    },
];

pub fn spec(op: Operation) -> &'static OpSpec {
    OPS.iter().find(|s| s.op == op).expect("every Operation variant has an OpSpec row")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClassifyRefusal {
    NotImplemented(&'static str),
    MethodNotAllowed(&'static str),
}

const BUILT_SUBRESOURCES: &[&str] = &[
    "versioning",
    "versions",
    "lifecycle",
    "location",
    "uploads",
    "delete",
    "uploadId",
    "partNumber",
    "versionId",
];

const UNBUILT_S3_SUBRESOURCES: &[&str] = &[
    "acl",
    "policy",
    "policyStatus",
    "cors",
    "encryption",
    "object-lock",
    "legal-hold",
    "retention",
    "tagging",
    "website",
    "notification",
    "replication",
    "accelerate",
    "requestPayment",
    "inventory",
    "analytics",
    "metrics",
    "intelligent-tiering",
    "select",
    "restore",
    "torrent",
    "ownershipControls",
    "publicAccessBlock",
    "logging",
    "attributes",
    "parts",
];

fn extract_subresources(query: &str) -> Result<(Vec<String>, bool), ClassifyRefusal> {
    let mut subs = Vec::new();
    let mut is_v2 = false;
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (raw_k, raw_v, has_eq) = match pair.split_once('=') {
            Some((k, v)) => (k, v, true),
            None => (pair, "", false),
        };
        let dec_k = percent_decode(raw_k);
        if dec_k == "list-type" && percent_decode(raw_v) == "2" {
            is_v2 = true;
        }
        if BUILT_SUBRESOURCES.contains(&dec_k.as_str()) {
            if !subs.contains(&dec_k) {
                subs.push(dec_k);
            }
        } else if UNBUILT_S3_SUBRESOURCES.contains(&dec_k.as_str()) || !has_eq {
            return Err(ClassifyRefusal::NotImplemented("The requested sub-resource is not implemented."));
        }
    }
    subs.sort();
    Ok((subs, is_v2))
}

pub fn classify(
    method: &Method,
    bucket: Option<&str>,
    key: Option<&str>,
    query: &str,
    headers: &HeaderMap,
) -> Result<Operation, ClassifyRefusal> {
    let (subs, is_v2) = extract_subresources(query)?;
    let subs_ref: Vec<&str> = subs.iter().map(|s| s.as_str()).collect();
    let has_copy = headers.contains_key("x-amz-copy-source");

    match (bucket, key) {
        (None, _) => {
            if !subs_ref.is_empty() {
                return Err(ClassifyRefusal::NotImplemented("The requested sub-resource is not implemented."));
            }
            match *method {
                Method::GET => Ok(Operation::ListBuckets),
                _ => Err(ClassifyRefusal::MethodNotAllowed("Method not allowed on service")),
            }
        }
        (Some(_), None) => match (method, subs_ref.as_slice()) {
            (&Method::PUT, ["versioning"]) => Ok(Operation::PutBucketVersioning),
            (&Method::GET, ["versioning"]) => Ok(Operation::GetBucketVersioning),
            (&Method::GET, ["versions"]) => Ok(Operation::ListObjectVersions),
            (&Method::PUT, ["lifecycle"]) => Ok(Operation::PutBucketLifecycle),
            (&Method::GET, ["lifecycle"]) => Ok(Operation::GetBucketLifecycle),
            (&Method::DELETE, ["lifecycle"]) => Ok(Operation::DeleteBucketLifecycle),
            (&Method::GET, ["location"]) => Ok(Operation::GetBucketLocation),
            (&Method::GET, ["uploads"]) => Ok(Operation::ListMultipartUploads),
            (&Method::POST, ["delete"]) => Ok(Operation::DeleteObjects),
            (_, s) if !s.is_empty() => Err(ClassifyRefusal::NotImplemented("The requested sub-resource is not implemented.")),
            (&Method::PUT, []) => Ok(Operation::CreateBucket),
            (&Method::DELETE, []) => Ok(Operation::DeleteBucket),
            (&Method::HEAD, []) => Ok(Operation::HeadBucket),
            (&Method::GET, []) => Ok(if is_v2 { Operation::ListObjectsV2 } else { Operation::ListObjects }),
            _ => Err(ClassifyRefusal::MethodNotAllowed("Method not allowed")),
        },
        (Some(_), Some(_)) => match (method, subs_ref.as_slice()) {
            (&Method::POST, ["uploads"]) => Ok(Operation::CreateMultipartUpload),
            (&Method::PUT, ["partNumber", "uploadId"]) => {
                if has_copy {
                    Err(ClassifyRefusal::NotImplemented(
                        "UploadPartCopy is not implemented: upload the part's bytes with UploadPart.",
                    ))
                } else {
                    Ok(Operation::UploadPart)
                }
            }
            (&Method::POST, ["uploadId"]) => Ok(Operation::CompleteMultipartUpload),
            (&Method::DELETE, ["uploadId"]) => Ok(Operation::AbortMultipartUpload),
            (&Method::GET, ["uploadId"]) => Ok(Operation::ListParts),
            (&Method::GET, ["versionId"]) | (&Method::GET, []) => Ok(Operation::GetObject),
            (&Method::HEAD, ["versionId"]) | (&Method::HEAD, []) => Ok(Operation::HeadObject),
            (&Method::DELETE, ["versionId"]) | (&Method::DELETE, []) => Ok(Operation::DeleteObject),
            (_, s) if !s.is_empty() => Err(ClassifyRefusal::NotImplemented("The requested sub-resource is not implemented.")),
            (&Method::PUT, []) => Ok(if has_copy { Operation::CopyObject } else { Operation::PutObject }),
            _ => Err(ClassifyRefusal::MethodNotAllowed("Method not allowed")),
        },
    }
}

pub fn iam_requirements(
    op: Operation,
    bucket: Option<&str>,
    key: Option<&str>,
    copy_source: Option<&str>,
) -> Vec<(&'static str, String)> {
    let s = spec(op);
    let Some(action) = s.iam_action else {
        return vec![];
    };
    let resource = match (s.target, bucket, key) {
        (TargetKind::Service, _, _) => "arn:aws:s3:::*".to_string(),
        (TargetKind::Bucket, Some(b), _) => format!("arn:aws:s3:::{}", b),
        (TargetKind::Object, Some(b), Some(k)) => format!("arn:aws:s3:::{}/{}", b, k),
        _ => return vec![],
    };
    let mut out = vec![(action, resource)];
    if op == Operation::CopyObject {
        if let Some(src) = copy_source {
            let src = percent_decode_path(src.split('?').next().unwrap_or(src));
            out.push(("s3:GetObject", format!("arn:aws:s3:::{}", src.trim_start_matches('/'))));
        }
    }
    out
}

pub struct Classified {
    pub op: Operation,
    pub bucket: Option<String>,
    pub key: Option<String>,
    pub query: String,
    pub query_map: HashMap<String, String>,
    pub parts: hyper::http::request::Parts,
    pub body: hyper::body::Incoming,
}

pub struct Authenticated {
    pub classified: Classified,
    pub principal: Principal,
    pub iam: Option<Arc<Iam>>,
}

pub struct Resolved {
    pub auth: Authenticated,
    pub b_id: Option<u32>,
}

pub struct Authorized {
    pub resolved: Resolved,
}

pub struct Validated {
    pub authorized: Authorized,
}

pub fn classify_request(req: Request<hyper::body::Incoming>) -> Result<Classified, Response<ResponseBody>> {
    let (parts, body) = req.into_parts();
    let path = parts.uri.path();
    let query = parts.uri.query().unwrap_or("").to_string();
    let host_hdr = parts.headers.get(HOST).and_then(|v| v.to_str().ok());
    let (bucket, key) = match parse_s3_target(path, host_hdr) {
        ParsedTarget::Service => (None, None),
        ParsedTarget::Bucket { bucket } => (Some(bucket), None),
        ParsedTarget::Object { bucket, key } => (Some(bucket), Some(key)),
    };

    let op = match classify(&parts.method, bucket.as_deref(), key.as_deref(), &query, &parts.headers) {
        Ok(op) => op,
        Err(ClassifyRefusal::NotImplemented(msg)) => {
            let resource = key.as_deref().or(bucket.as_deref()).unwrap_or("/");
            let xml = s3_error_xml("NotImplemented", msg, resource, "req-not-impl");
            return Err(Response::builder()
                .status(StatusCode::NOT_IMPLEMENTED)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap());
        }
        Err(ClassifyRefusal::MethodNotAllowed(msg)) => {
            let resource = key.as_deref().or(bucket.as_deref()).unwrap_or("/");
            let xml = s3_error_xml("MethodNotAllowed", msg, resource, "req-err");
            return Err(Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap());
        }
    };

    let mut query_map = HashMap::new();
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            let dec_k = percent_decode(k);
            let dec_v = percent_decode(v);
            query_map.insert(dec_k.to_lowercase(), dec_v.clone());
            query_map.insert(dec_k, dec_v);
        } else if !pair.is_empty() {
            let dec_k = percent_decode(pair);
            query_map.insert(dec_k.to_lowercase(), String::new());
            query_map.insert(dec_k, String::new());
        }
    }

    Ok(Classified {
        op,
        bucket,
        key,
        query,
        query_map,
        parts,
        body,
    })
}

pub fn authenticate(state: &S3ServiceState, c: Classified) -> Result<Authenticated, Response<ResponseBody>> {
    let now_sec = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    let iam = state.iam.read().as_ref().map(|r| r.current());
    let path = c.parts.uri.path();
    let principal = match crate::auth::authenticate_with(
        &state.auth,
        iam.as_deref(),
        &state.sigv4_cache,
        c.parts.method.as_str(),
        path,
        &c.query,
        &c.parts.headers,
        now_sec,
    ) {
        Ok(p) => p,
        Err(code) => {
            let status = match code {
                "AuthorizationHeaderMalformed" | "AuthorizationQueryParametersError" => StatusCode::BAD_REQUEST,
                _ => StatusCode::FORBIDDEN,
            };
            let xml = s3_error_xml(code, "Authentication failed", path, "req-auth");
            return Err(Response::builder()
                .status(status)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap());
        }
    };
    Ok(Authenticated { classified: c, principal, iam })
}

pub fn resolve_bucket(state: &S3ServiceState, a: Authenticated) -> Result<Resolved, Response<ResponseBody>> {
    let s = spec(a.classified.op);
    if !s.requires_existing_bucket {
        return Ok(Resolved { auth: a, b_id: None });
    }
    let bucket = a.classified.bucket.as_deref().expect("bucket op requires bucket");
    let b_id = match state.engine.read().lookup_bucket(bucket) {
        Ok(id) => id,
        Err(_) => {
            if a.classified.op == Operation::HeadBucket {
                return Err(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header(CONTENT_LENGTH, "0")
                    .body(empty_body())
                    .unwrap());
            }
            let xml = s3_error_xml("NoSuchBucket", "The specified bucket does not exist", bucket, "req-bucket");
            return Err(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap());
        }
    };
    Ok(Resolved { auth: a, b_id: Some(b_id) })
}

pub fn authorize(r: Resolved) -> Result<Authorized, Response<ResponseBody>> {
    let c = &r.auth.classified;
    let copy_src = c.parts.headers.get("x-amz-copy-source").and_then(|v| v.to_str().ok());
    for (action, resource) in iam_requirements(c.op, c.bucket.as_deref(), c.key.as_deref(), copy_src) {
        let allowed = match &r.auth.principal {
            Principal::User(ak) => r.auth.iam.as_ref().is_some_and(|i| i.allows(ak, action, &resource)),
            _ => true,
        };
        if !allowed {
            let xml = s3_error_xml(
                "AccessDenied",
                &format!("Access Denied: {} on {}", action, resource),
                c.parts.uri.path(),
                "req-iam",
            );
            return Err(Response::builder()
                .status(StatusCode::FORBIDDEN)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap());
        }
    }
    Ok(Authorized { resolved: r })
}

pub fn validate(a: Authorized) -> Result<Validated, Response<ResponseBody>> {
    let c = &a.resolved.auth.classified;
    if let Some(ref key) = c.key {
        if let Err(err_code) = validate_s3_key(key) {
            let xml = s3_error_xml(err_code.as_str(), err_code.description(), key, "req-val-key");
            return Err(Response::builder()
                .status(err_code.status_code())
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap());
        }
    }

    let target = match (&c.bucket, &c.key) {
        (None, _) => ParsedTarget::Service,
        (Some(b), None) => ParsedTarget::Bucket { bucket: b.clone() },
        (Some(b), Some(k)) => ParsedTarget::Object {
            bucket: b.clone(),
            key: k.clone(),
        },
    };
    let body_limit = body_limit_for(&c.parts.method, &target);
    let declared = c
        .parts
        .headers
        .get("x-amz-decoded-content-length")
        .or_else(|| c.parts.headers.get(CONTENT_LENGTH))
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok());
    if declared.is_some_and(|n| n > body_limit) {
        return Err(entity_too_large(c.parts.uri.path(), body_limit));
    }

    Ok(Validated { authorized: a })
}

pub async fn execute(state: Arc<S3ServiceState>, v: Validated) -> Result<Response<ResponseBody>, hyper::Error> {
    let resolved = v.authorized.resolved;
    let b_id = resolved.b_id;
    let auth = resolved.auth;
    let principal = auth.principal;
    let iam = auth.iam;
    let c = auth.classified;
    let path = c.parts.uri.path().to_string();
    let bucket = c.bucket.as_deref().unwrap_or("");
    let key = c.key.as_deref().unwrap_or("");

    match c.op {
        Operation::ListBuckets => service::list_buckets(state).await,
        Operation::CreateBucket => bucket::create_bucket(state, bucket).await,
        Operation::DeleteBucket => bucket::delete_bucket(state, b_id.unwrap(), bucket).await,
        Operation::HeadBucket => bucket::head_bucket().await,
        Operation::ListObjects => bucket::list_objects(state, b_id.unwrap(), bucket, &c.query_map, false).await,
        Operation::ListObjectsV2 => bucket::list_objects(state, b_id.unwrap(), bucket, &c.query_map, true).await,
        Operation::ListObjectVersions => bucket::list_object_versions(state, b_id.unwrap(), bucket, &c.query_map).await,
        Operation::GetBucketLocation => bucket::get_bucket_location().await,
        Operation::GetBucketVersioning => bucket::get_bucket_versioning(state, b_id.unwrap()).await,
        Operation::PutBucketVersioning => bucket::put_bucket_versioning(state, b_id.unwrap(), bucket, &path, c.body).await,
        Operation::GetBucketLifecycle => lifecycle::get_bucket_lifecycle(state, bucket).await,
        Operation::PutBucketLifecycle => lifecycle::put_bucket_lifecycle(state, bucket, &path, c.body).await,
        Operation::DeleteBucketLifecycle => lifecycle::delete_bucket_lifecycle(state, bucket).await,
        Operation::ListMultipartUploads => multipart::list_multipart_uploads(state, bucket).await,
        Operation::DeleteObjects => {
            bucket::delete_objects(state, b_id.unwrap(), bucket, &path, &principal, iam.as_deref(), c.body).await
        }
        Operation::PutObject => {
            object::put_object(state, b_id.unwrap(), bucket, key, &path, &c.parts.headers, iam, c.body).await
        }
        Operation::CopyObject => object::copy_object(state, b_id.unwrap(), bucket, key, &c.parts.headers).await,
        Operation::GetObject => {
            object::get_or_head_object(state, b_id.unwrap(), bucket, key, &c.query_map, &c.parts.headers, false).await
        }
        Operation::HeadObject => {
            object::get_or_head_object(state, b_id.unwrap(), bucket, key, &c.query_map, &c.parts.headers, true).await
        }
        Operation::DeleteObject => object::delete_object(state, b_id.unwrap(), key, &c.query_map).await,
        Operation::CreateMultipartUpload => multipart::create_multipart_upload(state, b_id.unwrap(), bucket, key).await,
        Operation::UploadPart => {
            multipart::upload_part(state, key, &path, &c.query_map, &c.parts.headers, iam, c.body).await
        }
        Operation::CompleteMultipartUpload => {
            multipart::complete_multipart_upload(state, b_id.unwrap(), bucket, key, &path, &c.query_map, c.body).await
        }
        Operation::AbortMultipartUpload => multipart::abort_multipart_upload(state, b_id.unwrap(), key, &c.query_map).await,
        Operation::ListParts => multipart::list_parts(state, bucket, key, &c.query_map).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn table_cases() -> Vec<(Operation, Method, Option<&'static str>, Option<&'static str>, &'static str, Option<&'static str>)> {
        vec![
            (Operation::ListBuckets, Method::GET, None, None, "", None),
            (Operation::CreateBucket, Method::PUT, Some("bkt"), None, "", None),
            (Operation::DeleteBucket, Method::DELETE, Some("bkt"), None, "", None),
            (Operation::HeadBucket, Method::HEAD, Some("bkt"), None, "", None),
            (Operation::ListObjects, Method::GET, Some("bkt"), None, "", None),
            (Operation::ListObjectsV2, Method::GET, Some("bkt"), None, "list-type=2", None),
            (Operation::ListObjectVersions, Method::GET, Some("bkt"), None, "versions", None),
            (Operation::GetBucketLocation, Method::GET, Some("bkt"), None, "location", None),
            (Operation::GetBucketVersioning, Method::GET, Some("bkt"), None, "versioning", None),
            (Operation::PutBucketVersioning, Method::PUT, Some("bkt"), None, "versioning", None),
            (Operation::GetBucketLifecycle, Method::GET, Some("bkt"), None, "lifecycle", None),
            (Operation::PutBucketLifecycle, Method::PUT, Some("bkt"), None, "lifecycle", None),
            (Operation::DeleteBucketLifecycle, Method::DELETE, Some("bkt"), None, "lifecycle", None),
            (Operation::ListMultipartUploads, Method::GET, Some("bkt"), None, "uploads", None),
            (Operation::DeleteObjects, Method::POST, Some("bkt"), None, "delete", None),
            (Operation::PutObject, Method::PUT, Some("bkt"), Some("obj"), "", None),
            (Operation::CopyObject, Method::PUT, Some("bkt"), Some("obj"), "", Some("/src/key")),
            (Operation::GetObject, Method::GET, Some("bkt"), Some("obj"), "", None),
            (Operation::HeadObject, Method::HEAD, Some("bkt"), Some("obj"), "", None),
            (Operation::DeleteObject, Method::DELETE, Some("bkt"), Some("obj"), "", None),
            (Operation::CreateMultipartUpload, Method::POST, Some("bkt"), Some("obj"), "uploads", None),
            (Operation::UploadPart, Method::PUT, Some("bkt"), Some("obj"), "partNumber=1&uploadId=u1", None),
            (Operation::CompleteMultipartUpload, Method::POST, Some("bkt"), Some("obj"), "uploadId=u1", None),
            (Operation::AbortMultipartUpload, Method::DELETE, Some("bkt"), Some("obj"), "uploadId=u1", None),
            (Operation::ListParts, Method::GET, Some("bkt"), Some("obj"), "uploadId=u1", None),
        ]
    }

    #[test]
    fn classification_table_covers_every_operation() {
        let cases = table_cases();
        let mut seen = HashSet::new();
        for (expected, method, b, k, q, copy) in &cases {
            let mut hdrs = HeaderMap::new();
            if let Some(c) = copy {
                hdrs.insert("x-amz-copy-source", c.parse().unwrap());
            }
            let got = classify(method, *b, *k, q, &hdrs).unwrap();
            assert_eq!(got, *expected, "for {:?} {:?} {:?} ?{}", method, b, k, q);
            seen.insert(got);
        }
        assert_eq!(seen.len(), OPS.len(), "every OpSpec row must be covered by the table test");
        for s in OPS {
            assert!(seen.contains(&s.op), "missing {:?}", s.op);
        }
    }

    #[test]
    fn iam_requirements_parity_with_old_function() {
        for (_, method, b, k, q, copy) in table_cases() {
            let mut hdrs = HeaderMap::new();
            if let Some(c) = copy {
                hdrs.insert("x-amz-copy-source", c.parse().unwrap());
            }
            let op = classify(&method, b, k, q, &hdrs).unwrap();
            let new_reqs = iam_requirements(op, b, k, copy);
            let old_reqs = crate::iam::requirements(method.as_str(), b, k, q, copy);
            assert_eq!(new_reqs, old_reqs, "parity mismatch for {:?} ({} ?{})", op, method, q);
        }
    }

    #[test]
    fn unknown_and_unbuilt_subresources_return_not_implemented() {
        let hdrs = HeaderMap::new();
        for (method, b, k, q) in [
            (Method::GET, None, None, "fooBar"),
            (Method::GET, Some("bkt"), None, "fooBar"),
            (Method::PUT, Some("bkt"), None, "fooBar"),
            (Method::GET, Some("bkt"), Some("obj"), "fooBar"),
            (Method::PUT, Some("bkt"), Some("obj"), "fooBar"),
            (Method::GET, Some("bkt"), None, "acl"),
            (Method::PUT, Some("bkt"), None, "policy"),
            (Method::GET, Some("bkt"), None, "website"),
            (Method::GET, Some("bkt"), None, "notification"),
            (Method::GET, Some("bkt"), None, "replication"),
            (Method::GET, Some("bkt"), None, "accelerate"),
            (Method::GET, Some("bkt"), None, "requestPayment"),
            (Method::GET, Some("bkt"), None, "inventory"),
            (Method::GET, Some("bkt"), None, "analytics"),
            (Method::GET, Some("bkt"), None, "metrics"),
            (Method::GET, Some("bkt"), None, "intelligent-tiering"),
            (Method::POST, Some("bkt"), Some("obj"), "select"),
            (Method::POST, Some("bkt"), Some("obj"), "restore"),
            (Method::GET, Some("bkt"), Some("obj"), "torrent"),
            (Method::PUT, Some("bkt"), Some("obj"), "uploadId=u1"),
            (Method::PUT, Some("bkt"), Some("obj"), "partNumber=1"),
            (Method::PUT, Some("bkt"), Some("obj"), "versionId=123"),
        ] {
            let res = classify(&method, b, k, q, &hdrs);
            assert!(
                matches!(res, Err(ClassifyRefusal::NotImplemented(_))),
                "expected 501 for {} {:?}/{:?}?{}, got {:?}",
                method,
                b,
                k,
                q,
                res
            );
        }

        // UploadPartCopy
        let mut copy_hdrs = HeaderMap::new();
        copy_hdrs.insert("x-amz-copy-source", "/b/s".parse().unwrap());
        assert!(matches!(
            classify(&Method::PUT, Some("bkt"), Some("obj"), "partNumber=1&uploadId=u1", &copy_hdrs),
            Err(ClassifyRefusal::NotImplemented(_))
        ));
    }

    #[test]
    fn classify_never_panics_on_arbitrary_inputs() {
        let methods = [Method::GET, Method::PUT, Method::POST, Method::DELETE, Method::HEAD, Method::PATCH, Method::OPTIONS];
        let buckets = [None, Some(""), Some("bkt"), Some("a/b")];
        let keys = [None, Some(""), Some("k"), Some("a/b/c")];
        let queries = ["", "fooBar", "&&=", "=%ZZ", "uploadId=1&partNumber=2", "list-type=2&prefix=a%20b", "acl&versioning"];
        for m in &methods {
            for b in &buckets {
                for k in &keys {
                    for q in &queries {
                        let _ = classify(m, *b, *k, q, &HeaderMap::new());
                    }
                }
            }
        }
    }
}
