pub mod bucket;
pub mod cors;
pub mod lifecycle;
pub mod lock;
pub mod multipart;
pub mod object;
pub mod post_object;
pub mod service;
pub mod tagging;

use bytes::Bytes;
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE, HOST};
use hyper::{HeaderMap, Method, Request, Response, StatusCode};
use std::collections::HashMap;
use std::sync::Arc;


use crate::authz::{
    AclGrants, AuthzContext, BucketOwnership, BucketPolicyDecision, BucketTarget, Decision,
    ObjectTarget, PolicyMatch, PublicAccessBlock, DEFAULT_BUCKET_OWNERSHIP,
};
use crate::bucket_config::BucketConfig;
use crate::ffi_bridge::unpack_flags;
use crate::fsm::Refusal;
use crate::iam::{Iam, Principal, ROOT_ACCOUNT};
use crate::meta_index::FLAG_ATTRS;
use crate::s3_api::{
    body_limit_for, empty_body, entity_too_large, full_body, parse_s3_target, percent_decode,
    percent_decode_path, percent_decode_path_bytes, raw_s3_key, validate_s3_key, ParsedTarget, ResponseBody, S3ServiceState,
};
use crate::xml::s3_error_xml;
use std::marker::PhantomData;

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
    GetBucketCors,
    PutBucketCors,
    DeleteBucketCors,
    GetBucketTagging,
    PutBucketTagging,
    DeleteBucketTagging,
    CorsPreflight,
    ListMultipartUploads,
    DeleteObjects,
    PutObject,
    PostObject,
    CopyObject,
    GetObject,
    GetObjectAttributes,
    HeadObject,
    DeleteObject,
    CreateMultipartUpload,
    UploadPart,
    UploadPartCopy,
    CompleteMultipartUpload,
    AbortMultipartUpload,
    ListParts,
    GetObjectTagging,
    PutObjectTagging,
    DeleteObjectTagging,
    GetObjectLockConfiguration,
    PutObjectLockConfiguration,
    GetObjectRetention,
    PutObjectRetention,
    GetObjectLegalHold,
    PutObjectLegalHold,
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
        op: Operation::GetBucketCors,
        name: "GetBucketCors",
        iam_action: Some("s3:ListBucket"),
        acl_permission: AclPermission::Read,
        target: TargetKind::Bucket,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::PutBucketCors,
        name: "PutBucketCors",
        iam_action: Some("s3:CreateBucket"),
        acl_permission: AclPermission::Write,
        target: TargetKind::Bucket,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::DeleteBucketCors,
        name: "DeleteBucketCors",
        iam_action: Some("s3:DeleteBucket"),
        acl_permission: AclPermission::Write,
        target: TargetKind::Bucket,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::GetBucketTagging,
        name: "GetBucketTagging",
        iam_action: Some("s3:ListBucket"),
        acl_permission: AclPermission::Read,
        target: TargetKind::Bucket,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::PutBucketTagging,
        name: "PutBucketTagging",
        iam_action: Some("s3:CreateBucket"),
        acl_permission: AclPermission::Write,
        target: TargetKind::Bucket,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::DeleteBucketTagging,
        name: "DeleteBucketTagging",
        iam_action: Some("s3:DeleteBucket"),
        acl_permission: AclPermission::Write,
        target: TargetKind::Bucket,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::CorsPreflight,
        name: "CorsPreflight",
        iam_action: None,
        acl_permission: AclPermission::Read,
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
    // A browser form upload (#119): a PutObject whose key and credentials are in the form.
    OpSpec {
        op: Operation::PostObject,
        name: "PostObject",
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
        op: Operation::GetObjectAttributes,
        name: "GetObjectAttributes",
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
        op: Operation::UploadPartCopy,
        name: "UploadPartCopy",
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
    OpSpec {
        op: Operation::GetObjectTagging,
        name: "GetObjectTagging",
        iam_action: Some("s3:GetObjectTagging"),
        acl_permission: AclPermission::Read,
        target: TargetKind::Object,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::PutObjectTagging,
        name: "PutObjectTagging",
        iam_action: Some("s3:PutObjectTagging"),
        acl_permission: AclPermission::Write,
        target: TargetKind::Object,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::DeleteObjectTagging,
        name: "DeleteObjectTagging",
        iam_action: Some("s3:DeleteObjectTagging"),
        acl_permission: AclPermission::Write,
        target: TargetKind::Object,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::GetObjectLockConfiguration,
        name: "GetObjectLockConfiguration",
        iam_action: Some("s3:GetBucketObjectLockConfiguration"),
        acl_permission: AclPermission::Read,
        target: TargetKind::Bucket,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::PutObjectLockConfiguration,
        name: "PutObjectLockConfiguration",
        iam_action: Some("s3:PutBucketObjectLockConfiguration"),
        acl_permission: AclPermission::Write,
        target: TargetKind::Bucket,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::GetObjectRetention,
        name: "GetObjectRetention",
        iam_action: Some("s3:GetObjectRetention"),
        acl_permission: AclPermission::Read,
        target: TargetKind::Object,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::PutObjectRetention,
        name: "PutObjectRetention",
        iam_action: Some("s3:PutObjectRetention"),
        acl_permission: AclPermission::Write,
        target: TargetKind::Object,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::GetObjectLegalHold,
        name: "GetObjectLegalHold",
        iam_action: Some("s3:GetObjectLegalHold"),
        acl_permission: AclPermission::Read,
        target: TargetKind::Object,
        requires_existing_bucket: true,
    },
    OpSpec {
        op: Operation::PutObjectLegalHold,
        name: "PutObjectLegalHold",
        iam_action: Some("s3:PutObjectLegalHold"),
        acl_permission: AclPermission::Write,
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
    "cors",
    "location",
    "uploads",
    "delete",
    "uploadId",
    "partNumber",
    "versionId",
    "tagging",
    "attributes",
    "object-lock",
    "retention",
    "legal-hold",
];

const UNBUILT_S3_SUBRESOURCES: &[&str] = &[
    "acl",
    "policy",
    "policyStatus",
    "encryption",
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
            (&Method::PUT, ["cors"]) => Ok(Operation::PutBucketCors),
            (&Method::GET, ["cors"]) => Ok(Operation::GetBucketCors),
            (&Method::DELETE, ["cors"]) => Ok(Operation::DeleteBucketCors),
            (&Method::PUT, ["tagging"]) => Ok(Operation::PutBucketTagging),
            (&Method::GET, ["tagging"]) => Ok(Operation::GetBucketTagging),
            (&Method::DELETE, ["tagging"]) => Ok(Operation::DeleteBucketTagging),
            (&Method::GET, ["object-lock"]) => Ok(Operation::GetObjectLockConfiguration),
            (&Method::PUT, ["object-lock"]) => Ok(Operation::PutObjectLockConfiguration),
            (&Method::OPTIONS, []) => Ok(Operation::CorsPreflight),
            (&Method::GET, ["location"]) => Ok(Operation::GetBucketLocation),
            (&Method::GET, ["uploads"]) => Ok(Operation::ListMultipartUploads),
            (&Method::POST, ["delete"]) => Ok(Operation::DeleteObjects),
            (&Method::POST, []) if post_object::is_form_upload(headers) => Ok(Operation::PostObject),
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
                    Ok(Operation::UploadPartCopy)
                } else {
                    Ok(Operation::UploadPart)
                }
            }
            (&Method::POST, ["uploadId"]) => Ok(Operation::CompleteMultipartUpload),
            (&Method::DELETE, ["uploadId"]) => Ok(Operation::AbortMultipartUpload),
            (&Method::GET, ["uploadId"]) => Ok(Operation::ListParts),
            (&Method::GET, ["tagging"]) | (&Method::GET, ["tagging", "versionId"]) => Ok(Operation::GetObjectTagging),
            (&Method::PUT, ["tagging"]) | (&Method::PUT, ["tagging", "versionId"]) => Ok(Operation::PutObjectTagging),
            (&Method::DELETE, ["tagging"]) | (&Method::DELETE, ["tagging", "versionId"]) => Ok(Operation::DeleteObjectTagging),
            (&Method::GET, ["attributes"]) | (&Method::GET, ["attributes", "versionId"]) => {
                Ok(Operation::GetObjectAttributes)
            }
            (&Method::GET, ["retention"]) | (&Method::GET, ["retention", "versionId"]) => {
                Ok(Operation::GetObjectRetention)
            }
            (&Method::PUT, ["retention"]) | (&Method::PUT, ["retention", "versionId"]) => {
                Ok(Operation::PutObjectRetention)
            }
            (&Method::GET, ["legal-hold"]) | (&Method::GET, ["legal-hold", "versionId"]) => {
                Ok(Operation::GetObjectLegalHold)
            }
            (&Method::PUT, ["legal-hold"]) | (&Method::PUT, ["legal-hold", "versionId"]) => {
                Ok(Operation::PutObjectLegalHold)
            }
                        (&Method::GET, ["versionId"])
            | (&Method::GET, ["partNumber"])
            | (&Method::GET, ["partNumber", "versionId"])
            | (&Method::GET, []) => Ok(Operation::GetObject),
            (&Method::HEAD, ["versionId"])
            | (&Method::HEAD, ["partNumber"])
            | (&Method::HEAD, ["partNumber", "versionId"])
            | (&Method::HEAD, []) => Ok(Operation::HeadObject),
            (&Method::DELETE, ["versionId"]) | (&Method::DELETE, []) => Ok(Operation::DeleteObject),
            (_, s) if !s.is_empty() => Err(ClassifyRefusal::NotImplemented("The requested sub-resource is not implemented.")),
            (&Method::PUT, []) => Ok(if has_copy { Operation::CopyObject } else { Operation::PutObject }),
            (&Method::OPTIONS, []) => Ok(Operation::CorsPreflight),
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
    if matches!(op, Operation::CopyObject | Operation::UploadPartCopy) {
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
    /// PostObject only: the form read by `post_object::read_form` (#119).
    pub form: Option<Box<post_object::PostForm>>,
}

pub struct Authenticated {
    pub classified: Classified,
    pub principal: Principal,
    pub iam: Option<Arc<Iam>>,
}

pub struct CopySourceResolved {
    pub bucket: String,
    pub key: String,
    pub bucket_cfg: Option<Arc<BucketConfig>>,
    pub object_owner: Option<String>,
}

pub struct Resolved {
    pub auth: Authenticated,
    pub b_id: Option<u32>,
    pub bucket_cfg: Option<Arc<BucketConfig>>,
    pub object_owner: Option<String>,
    pub copy_source: Option<CopySourceResolved>,
}

pub struct Authorized {
    pub resolved: Resolved,
    pub bypass_governance: bool,
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
        form: None,
    })
}

pub fn authenticate(state: &S3ServiceState, c: Classified) -> Result<Authenticated, Response<ResponseBody>> {
    let now_sec = state.clock.now_ns() / 1_000_000_000;
    let iam = state.iam.read().as_ref().map(|r| r.current());
    if c.op == Operation::CorsPreflight {
        return Ok(Authenticated { classified: c, principal: Principal::Anonymous, iam });
    }
    if c.op == Operation::PostObject {
        return authenticate_form(state, c);
    }
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
    let principal_label = match &principal {
        Principal::Root => ROOT_ACCOUNT.to_string(),
        Principal::User { account, name, .. } => {
            if account == name {
                name.clone()
            } else {
                format!("{}:{}", account, name)
            }
        }
        Principal::Anonymous => "anonymous".to_string(),
    };
    crate::request_log::note_principal(principal_label);
    Ok(Authenticated { classified: c, principal, iam })
}

/// The authenticate stage for a form: the form's own signature stands in for the
/// Authorization header. A signed form's policy is checked here too, once its signature is
/// known to be good, so nothing the policy refuses reaches storage.
fn authenticate_form(state: &S3ServiceState, mut c: Classified) -> Result<Authenticated, Response<ResponseBody>> {
    let path = c.parts.uri.path().to_string();
    let iam = state.iam.read().as_ref().map(|r| r.current());
    let now = state.clock.now_ns() / 1_000_000_000;
    let form = c.form.as_mut().expect("read_form ran for PostObject");
    let refuse = |r: Refusal| post_object::refusal_response(r, &path);
    let signature = crate::post_object::read_signature(&form.fields).map_err(refuse)?;
    let no_keys = state.auth.credentials.is_none() && iam.as_deref().is_none_or(|i| i.is_empty());
    let principal = match &signature {
        None if state.auth.allow_anonymous => Principal::Anonymous,
        None => return Err(post_object::refusal_response(Refusal { code: "AccessDenied", status: 403, why: "Access Denied" }, &path)),
        Some(_) if no_keys && state.auth.allow_anonymous => Principal::Anonymous,
        Some(_) if no_keys => return Err(refuse(crate::post_object::UNKNOWN_KEY)),
        Some(sig) => {
            let (principal, secret) =
                crate::auth::secret_for(&state.auth, iam.as_deref(), &sig.access_key).ok_or_else(|| refuse(crate::post_object::UNKNOWN_KEY))?;
            let want = crate::post_object::expected_signature(&state.sigv4_cache, secret, sig);
            if !crate::auth::constant_time_eq(&want, &sig.signature) {
                return Err(refuse(crate::post_object::BAD_SIGNATURE));
            }
            principal
        }
    };
    if signature.is_some() {
        let bucket = c.bucket.as_deref().unwrap_or("");
        let key = c.key.as_deref().unwrap_or("");
        let policy = crate::post_object::parse_policy(form.fields.get("policy").unwrap_or("")).map_err(refuse)?;
        crate::post_object::check_policy(&policy, &form.fields, bucket, key, now).map_err(refuse)?;
        form.policy = Some(policy);
    }
    crate::request_log::note_principal(match &principal {
        Principal::Anonymous => "anonymous".to_string(),
        Principal::Root => crate::iam::ROOT_ACCOUNT.to_string(),
        Principal::User { account, name, .. } if account == name => name.clone(),
        Principal::User { account, name, .. } => format!("{}:{}", account, name),
    });
    Ok(Authenticated { classified: c, principal, iam })
}

/// Parsed `x-amz-copy-source` header components (`bucket`, `key`, optional `versionId`) (#152, #445).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedCopySource {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
}

/// Parses an `x-amz-copy-source` header value into bucket, key, and optional `versionId`.
pub fn parse_copy_source(raw: &str) -> Option<ParsedCopySource> {
    let (copy_path, copy_query) = raw.split_once('?').unwrap_or((raw, ""));
    let version_id = copy_query
        .split('&')
        .find_map(|kv| kv.strip_prefix("versionId="))
        .map(percent_decode_path);
    let decoded = percent_decode_path(copy_path);
    let trimmed = decoded.trim_start_matches('/');
    let (bucket, key) = trimmed.split_once('/')?;
    if bucket.is_empty() || key.is_empty() {
        return None;
    }
    Some(ParsedCopySource {
        bucket: bucket.to_string(),
        key: key.to_string(),
        version_id,
    })
}

fn lookup_object_owner(
    state: &S3ServiceState,
    b_id: u32,
    key: &str,
    vid: Option<&str>,
) -> Option<String> {
    let (target_vid, vol_offset, flags) = if vid.is_some() || state.versions.read().has(b_id, key) {
        let id = vid.and_then(crate::versions::parse_id);
        if vid.is_some() && id.is_none() {
            return None;
        }
        let v = object::find_version(state, b_id, key, id)?;
        (v.id(), v.vol_offset, v.flags)
    } else {
        let m = state.engine.read().get_object(b_id, key.as_bytes()).ok()?;
        (0, m.vol_offset, unpack_flags(m.bucket_flags))
    };
    if flags & FLAG_ATTRS == 0 && !state.attr_updates.has_updates() {
        return None;
    }
    state
        .object_view_for(b_id, key, target_vid, vol_offset, flags)
        .ok()
        .and_then(|v| v.attrs.owner().map(str::to_string))
}

fn bucket_not_found_response(op: Operation, bucket: &str) -> Response<ResponseBody> {
    if op == Operation::HeadBucket {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(CONTENT_LENGTH, "0")
            .body(empty_body())
            .unwrap();
    }
    let xml = s3_error_xml("NoSuchBucket", "The specified bucket does not exist", bucket, "req-bucket");
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap()
}

pub fn resolve_bucket(state: &S3ServiceState, a: Authenticated) -> Result<Resolved, Response<ResponseBody>> {
    let s = spec(a.classified.op);
    if !s.requires_existing_bucket {
        return Ok(Resolved {
            auth: a,
            b_id: None,
            bucket_cfg: None,
            object_owner: None,
            copy_source: None,
        });
    }
    let bucket = a.classified.bucket.as_deref().expect("bucket op requires bucket");
    let b_id = match state.engine.read().lookup_bucket(bucket) {
        Ok(id) => id,
        Err(_) => return Err(bucket_not_found_response(a.classified.op, bucket)),
    };
    let bucket_cfg = state.bucket_config_reader.load_bucket(b_id);
    let ownership = bucket_cfg
        .as_ref()
        .map_or(DEFAULT_BUCKET_OWNERSHIP, |c| c.ownership());
    let object_owner = if ownership != BucketOwnership::BucketOwnerEnforced
        && s.target == TargetKind::Object
        && matches!(
            a.classified.op,
            Operation::GetObject
                | Operation::HeadObject
                | Operation::GetObjectAttributes
                | Operation::DeleteObject
                | Operation::GetObjectTagging
                | Operation::PutObjectTagging
                | Operation::DeleteObjectTagging
                | Operation::GetObjectRetention
                | Operation::PutObjectRetention
                | Operation::GetObjectLegalHold
                | Operation::PutObjectLegalHold
        ) {
        let vid = a.classified.query_map.get("versionId").map(String::as_str);
        a.classified
            .key
            .as_deref()
            .and_then(|k| lookup_object_owner(state, b_id, k, vid))
    } else {
        None
    };

    let copy_source = if matches!(
        a.classified.op,
        Operation::CopyObject | Operation::UploadPartCopy
    ) {
        if let Some(src) = a
            .classified
            .parts
            .headers
            .get("x-amz-copy-source")
            .and_then(|v| v.to_str().ok())
            .and_then(parse_copy_source)
        {
            let src_b_id = match state.engine.read().lookup_bucket(&src.bucket) {
                Ok(id) => id,
                Err(_) => {
                    let xml = s3_error_xml(
                        "NoSuchBucket",
                        "Source bucket does not exist",
                        &src.bucket,
                        "req-cp",
                    );
                    return Err(Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .header(CONTENT_TYPE, "application/xml")
                        .body(full_body(Bytes::from(xml)))
                        .unwrap());
                }
            };
            let src_cfg = state.bucket_config_reader.load_bucket(src_b_id);
            let src_ownership = src_cfg
                .as_ref()
                .map_or(DEFAULT_BUCKET_OWNERSHIP, |c| c.ownership());
            let src_obj_owner = if src_ownership != BucketOwnership::BucketOwnerEnforced {
                lookup_object_owner(state, src_b_id, &src.key, src.version_id.as_deref())
            } else {
                None
            };
            Some(CopySourceResolved {
                bucket: src.bucket,
                key: src.key,
                bucket_cfg: src_cfg,
                object_owner: src_obj_owner,
            })
        } else {
            None
        }
    } else {
        None
    };

    Ok(Resolved {
        auth: a,
        b_id: Some(b_id),
        bucket_cfg,
        object_owner,
        copy_source,
    })
}

fn record_authz_reason(reason: &'static str) {
    crate::request_log::note_authz_reason(reason);
    tracing::Span::current().record("authz_reason", reason);
}

fn identity_policy_for(
    principal: &Principal,
    iam: Option<&Iam>,
    op: Operation,
    bucket: Option<&str>,
    key: Option<&str>,
) -> PolicyMatch {
    let Principal::User { name, .. } = principal else {
        return PolicyMatch::NoOpinion;
    };
    let Some(iam) = iam else {
        return PolicyMatch::NoOpinion;
    };
    if op == Operation::DeleteObjects {
        return match bucket {
            Some(b) => iam.may_delete_in_bucket(name, b),
            None => PolicyMatch::NoOpinion,
        };
    }
    let s = spec(op);
    let Some(action) = s.iam_action else {
        return PolicyMatch::NoOpinion;
    };
    let resource = match (s.target, bucket, key) {
        (TargetKind::Service, _, _) => "arn:aws:s3:::*".to_string(),
        (TargetKind::Bucket, Some(b), _) => format!("arn:aws:s3:::{}", b),
        (TargetKind::Object, Some(b), Some(k)) => format!("arn:aws:s3:::{}/{}", b, k),
        _ => return PolicyMatch::NoOpinion,
    };
    iam.evaluate_user(name, action, &resource)
}

fn bucket_target_for<'a>(
    name: &'a str,
    cfg: Option<&'a BucketConfig>,
    empty_acl: &'a AclGrants,
) -> BucketTarget<'a> {
    match cfg {
        Some(c) => BucketTarget {
            name,
            owner_account: c.effective_owner(),
            ownership: c.ownership(),
            public_access_block: c.public_access_block(),
            policy: c.policy_decision(),
            acl: empty_acl,
        },
        None => BucketTarget {
            name,
            owner_account: ROOT_ACCOUNT,
            ownership: DEFAULT_BUCKET_OWNERSHIP,
            public_access_block: PublicAccessBlock::default(),
            policy: BucketPolicyDecision::none(),
            acl: empty_acl,
        },
    }
}

fn access_denied_response(path: &str) -> Response<ResponseBody> {
    let xml = s3_error_xml("AccessDenied", "Access Denied", path, "req-authz");
    Response::builder()
        .status(StatusCode::FORBIDDEN)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap()
}

pub fn authorize(state: &S3ServiceState, r: Resolved) -> Result<Authorized, Response<ResponseBody>> {
    let c = &r.auth.classified;
    if c.op == Operation::CorsPreflight {
        record_authz_reason("cors-preflight");
        return Ok(Authorized {
            resolved: r,
            bypass_governance: false,
        });
    }
    let wants_bypass = matches!(c.op, Operation::PutObjectRetention | Operation::DeleteObject)
        && crate::object_lock::parse_bypass_governance_header(&c.parts.headers);
    if state.auth.allow_anonymous
        && matches!(r.auth.principal, Principal::Anonymous)
        && r.auth.iam.as_deref().is_none_or(|i| i.is_empty())
    {
        record_authz_reason("allow-anonymous");
        return Ok(Authorized {
            resolved: r,
            bypass_governance: wants_bypass,
        });
    }

    let empty_acl = AclGrants::default();
    let bucket_target = if r.b_id.is_some() {
        c.bucket
            .as_deref()
            .map(|b| bucket_target_for(b, r.bucket_cfg.as_deref(), &empty_acl))
    } else {
        None
    };
    let object_target = if spec(c.op).target == TargetKind::Object {
        c.key.as_deref().map(|k| ObjectTarget {
            key: k,
            owner_account: r
                .object_owner
                .as_deref()
                .or_else(|| bucket_target.as_ref().map(|b| b.owner_account))
                .unwrap_or(ROOT_ACCOUNT),
            acl: &empty_acl,
        })
    } else {
        None
    };
    let id_pol = identity_policy_for(
        &r.auth.principal,
        r.auth.iam.as_deref(),
        c.op,
        c.bucket.as_deref(),
        c.key.as_deref(),
    );
    let ctx = AuthzContext {
        identity_policy: id_pol,
        _marker: PhantomData,
    };
    let decision = crate::authz::authorize(
        &r.auth.principal,
        c.op,
        bucket_target.as_ref(),
        object_target.as_ref(),
        &ctx,
    );
    record_authz_reason(decision.reason().as_str());
    if matches!(decision, Decision::Deny { .. }) {
        return Err(access_denied_response(c.parts.uri.path()));
    }

    if let Some(ref src) = r.copy_source {
        let src_bucket = bucket_target_for(&src.bucket, src.bucket_cfg.as_deref(), &empty_acl);
        let src_object = ObjectTarget {
            key: &src.key,
            owner_account: src.object_owner.as_deref().unwrap_or(src_bucket.owner_account),
            acl: &empty_acl,
        };
        let src_pol = identity_policy_for(
            &r.auth.principal,
            r.auth.iam.as_deref(),
            Operation::GetObject,
            Some(&src.bucket),
            Some(&src.key),
        );
        let src_ctx = AuthzContext {
            identity_policy: src_pol,
            _marker: PhantomData,
        };
        let src_dec = crate::authz::authorize(
            &r.auth.principal,
            Operation::GetObject,
            Some(&src_bucket),
            Some(&src_object),
            &src_ctx,
        );
        if matches!(src_dec, Decision::Deny { .. }) {
            record_authz_reason(src_dec.reason().as_str());
            return Err(access_denied_response(c.parts.uri.path()));
        }
    }

    let bypass_governance = if wants_bypass {
        let bypass_pol = match (&r.auth.principal, r.auth.iam.as_deref(), c.bucket.as_deref(), c.key.as_deref()) {
            (Principal::User { name, .. }, Some(iam), Some(b), Some(k)) => {
                iam.evaluate_user(name, "s3:BypassGovernanceRetention", &format!("arn:aws:s3:::{}/{}", b, k))
            }
            _ => PolicyMatch::NoOpinion,
        };
        let bypass_ctx = AuthzContext {
            identity_policy: bypass_pol,
            _marker: PhantomData,
        };
        crate::authz::authorize_bypass_governance(
            &r.auth.principal,
            bucket_target.as_ref(),
            object_target.as_ref(),
            &bypass_ctx,
        )
        .is_allowed()
    } else {
        false
    };

    Ok(Authorized {
        resolved: r,
        bypass_governance,
    })
}

fn authorize_key_action(
    state: &S3ServiceState,
    principal: &Principal,
    iam: Option<&Iam>,
    b_id: u32,
    bucket_cfg: Option<&BucketConfig>,
    bucket: &str,
    key: &str,
    bypass: bool,
) -> bool {
    if state.auth.allow_anonymous
        && matches!(principal, Principal::Anonymous)
        && iam.is_none_or(|i| i.is_empty())
    {
        return true;
    }
    let empty_acl = AclGrants::default();
    let b_target = bucket_target_for(bucket, bucket_cfg, &empty_acl);
    let obj_owner = if b_target.ownership != BucketOwnership::BucketOwnerEnforced {
        lookup_object_owner(state, b_id, key, None)
    } else {
        None
    };
    let o_target = ObjectTarget {
        key,
        owner_account: obj_owner.as_deref().unwrap_or(b_target.owner_account),
        acl: &empty_acl,
    };
    if bypass {
        let id_pol = match (principal, iam) {
            (Principal::User { name, .. }, Some(iam)) => iam.evaluate_user(
                name,
                "s3:BypassGovernanceRetention",
                &format!("arn:aws:s3:::{}/{}", bucket, key),
            ),
            _ => PolicyMatch::NoOpinion,
        };
        let ctx = AuthzContext {
            identity_policy: id_pol,
            _marker: PhantomData,
        };
        crate::authz::authorize_bypass_governance(principal, Some(&b_target), Some(&o_target), &ctx).is_allowed()
    } else {
        let id_pol = identity_policy_for(principal, iam, Operation::DeleteObject, Some(bucket), Some(key));
        let ctx = AuthzContext {
            identity_policy: id_pol,
            _marker: PhantomData,
        };
        crate::authz::authorize(principal, Operation::DeleteObject, Some(&b_target), Some(&o_target), &ctx).is_allowed()
    }
}

fn authorize_delete_key(
    state: &S3ServiceState,
    principal: &Principal,
    iam: Option<&Iam>,
    b_id: u32,
    bucket_cfg: Option<&BucketConfig>,
    bucket: &str,
    key: &str,
) -> bool {
    authorize_key_action(state, principal, iam, b_id, bucket_cfg, bucket, key, false)
}

fn authorize_bypass_key(
    state: &S3ServiceState,
    principal: &Principal,
    iam: Option<&Iam>,
    b_id: u32,
    bucket_cfg: Option<&BucketConfig>,
    bucket: &str,
    key: &str,
) -> bool {
    authorize_key_action(state, principal, iam, b_id, bucket_cfg, bucket, key, true)
}

pub fn validate(a: Authorized) -> Result<Validated, Response<ResponseBody>> {
    let c = &a.resolved.auth.classified;
    if let Some(ref key) = c.key {
        let host_hdr = c.parts.headers.get(HOST).and_then(|v| v.to_str().ok());
        let invalid_utf8 = raw_s3_key(c.parts.uri.path(), host_hdr)
            .is_some_and(|raw_key| std::str::from_utf8(&percent_decode_path_bytes(raw_key)).is_err());
        let has_c1_or_del = key.chars().any(|ch| matches!(ch, '\u{007f}' | '\u{0080}'..='\u{009f}'));
        if invalid_utf8 || has_c1_or_del {
            let xml = s3_error_xml("InvalidURI", "Couldn't parse the specified URI.", c.parts.uri.path(), "req-val-uri");
            return Err(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap());
        }
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
    // A form carries the file and at most 2 MiB of fields (100 of 20 KiB) around it.
    let body_limit = if c.op == Operation::PostObject {
        crate::s3_api::MAX_OBJECT_BODY + crate::s3_api::MAX_XML_BODY
    } else {
        body_limit_for(&c.parts.method, &target)
    };
    // A form's size is its Content-Length; `x-amz-decoded-content-length` is for aws-chunked
    // PUT bodies and must not let a form past the limit.
    let decoded = if c.op == Operation::PostObject { None } else { c.parts.headers.get("x-amz-decoded-content-length") };
    let declared = decoded
        .or_else(|| c.parts.headers.get(CONTENT_LENGTH))
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok());
    if declared.is_some_and(|n| n > body_limit) {
        return Err(entity_too_large(c.parts.uri.path(), body_limit));
    }
    Ok(Validated { authorized: a })
}

pub async fn execute(state: Arc<S3ServiceState>, v: Validated) -> Result<Response<ResponseBody>, hyper::Error> {
    let bypass_governance = v.authorized.bypass_governance;
    let resolved = v.authorized.resolved;
    let b_id = resolved.b_id;
    let bucket_cfg = resolved.bucket_cfg;
    let bucket_owner = bucket_cfg
        .as_deref()
        .map(|cfg| cfg.effective_owner().to_string())
        .unwrap_or_else(|| ROOT_ACCOUNT.to_string());
    let auth = resolved.auth;
    let principal = auth.principal;
    let iam = auth.iam;
    let c = auth.classified;
    let path = c.parts.uri.path().to_string();
    let bucket = c.bucket.as_deref().unwrap_or("");
    let key = c.key.as_deref().unwrap_or("");
    let writer_account = principal.account().unwrap_or("");

    match c.op {
        Operation::ListBuckets => service::list_buckets(state, &c.query_map, principal.account()).await,
        Operation::CreateBucket => bucket::create_bucket(state, bucket, &path, &c.parts.headers, writer_account, c.body).await,
        Operation::DeleteBucket => bucket::delete_bucket(state, b_id.unwrap(), bucket).await,
        Operation::HeadBucket => bucket::head_bucket().await,
        Operation::ListObjects => bucket::list_objects(state, b_id.unwrap(), bucket, &c.query_map, false).await,
        Operation::ListObjectsV2 => bucket::list_objects(state, b_id.unwrap(), bucket, &c.query_map, true).await,
        Operation::ListObjectVersions => bucket::list_object_versions(state, b_id.unwrap(), bucket, &c.query_map).await,
        Operation::GetBucketLocation => bucket::get_bucket_location(state).await,
        Operation::GetBucketVersioning => bucket::get_bucket_versioning(state, b_id.unwrap()).await,
        Operation::PutBucketVersioning => bucket::put_bucket_versioning(state, b_id.unwrap(), bucket, &path, c.body).await,
        Operation::GetBucketLifecycle => lifecycle::get_bucket_lifecycle(state, b_id.unwrap(), bucket).await,
        Operation::PutBucketLifecycle => lifecycle::put_bucket_lifecycle(state, b_id.unwrap(), bucket, &path, c.body).await,
        Operation::DeleteBucketLifecycle => lifecycle::delete_bucket_lifecycle(state, b_id.unwrap(), bucket).await,
        Operation::GetBucketCors => cors::get_bucket_cors(state, b_id.unwrap(), bucket).await,
        Operation::PutBucketCors => cors::put_bucket_cors(state, b_id.unwrap(), bucket, &path, c.body).await,
        Operation::DeleteBucketCors => cors::delete_bucket_cors(state, b_id.unwrap(), bucket).await,
        Operation::GetBucketTagging => tagging::get_bucket_tagging(state, b_id.unwrap(), bucket).await,
        Operation::PutBucketTagging => tagging::put_bucket_tagging(state, b_id.unwrap(), bucket, &path, c.body).await,
        Operation::DeleteBucketTagging => tagging::delete_bucket_tagging(state, b_id.unwrap(), bucket).await,
        Operation::CorsPreflight => cors::preflight(state, b_id.unwrap(), &path, &c.parts.headers).await,
        Operation::ListMultipartUploads => multipart::list_multipart_uploads(state, bucket, &c.query_map).await,
        Operation::DeleteObjects => {
            let bid = b_id.unwrap();
            let st_for_del = Arc::clone(&state);
            let p_for_del = principal.clone();
            let iam_for_del = iam.clone();
            let cfg_for_del = bucket_cfg.clone();
            let b_name = bucket.to_string();
            let wants_bypass = crate::object_lock::parse_bypass_governance_header(&c.parts.headers);
            let st_for_byp = Arc::clone(&state);
            let p_for_byp = principal.clone();
            let iam_for_byp = iam.clone();
            let cfg_for_byp = bucket_cfg.clone();
            let b_name_byp = bucket.to_string();
            bucket::delete_objects(
                state,
                bid,
                bucket,
                &path,
                move |k| {
                    authorize_delete_key(
                        &st_for_del,
                        &p_for_del,
                        iam_for_del.as_deref(),
                        bid,
                        cfg_for_del.as_deref(),
                        &b_name,
                        k,
                    )
                },
                move |k| {
                    wants_bypass
                        && authorize_bypass_key(
                            &st_for_byp,
                            &p_for_byp,
                            iam_for_byp.as_deref(),
                            bid,
                            cfg_for_byp.as_deref(),
                            &b_name_byp,
                            k,
                        )
                },
                c.body,
            )
            .await
        }
        Operation::PutObject => {
            let verifier = crate::auth::ChunkVerifier::for_request_with(&state.auth, iam.as_deref(), &c.parts.headers);
            object::put_object(state, b_id.unwrap(), bucket, key, &path, &c.parts.headers, writer_account, &bucket_owner, verifier, c.body).await
        }
        Operation::PostObject => {
            post_object::execute(state, b_id.unwrap(), bucket, key, &path, &c.parts.headers, writer_account, &bucket_owner, c.form, c.body).await
        }
        Operation::CopyObject => object::copy_object(state, b_id.unwrap(), bucket, key, &c.parts.headers, writer_account, &bucket_owner).await,
        Operation::GetObject => {
            let auth_enabled = state.auth.credentials.is_some() || iam.as_deref().is_some_and(|i| !i.is_empty());
            let is_anon_with_auth = auth_enabled && matches!(principal, Principal::Anonymous);
            object::get_or_head_object(state, b_id.unwrap(), bucket, key, &c.query_map, &c.parts.headers, false, is_anon_with_auth).await
        }
        Operation::GetObjectAttributes => {
            object::get_object_attributes(state, b_id.unwrap(), bucket, key, &c.query_map, &c.parts.headers).await
        }
        Operation::HeadObject => {
            let auth_enabled = state.auth.credentials.is_some() || iam.as_deref().is_some_and(|i| !i.is_empty());
            let is_anon_with_auth = auth_enabled && matches!(principal, Principal::Anonymous);
            object::get_or_head_object(state, b_id.unwrap(), bucket, key, &c.query_map, &c.parts.headers, true, is_anon_with_auth).await
        }
        Operation::DeleteObject => object::delete_object(state, b_id.unwrap(), key, &c.query_map, &c.parts.headers, bypass_governance).await,
        Operation::CreateMultipartUpload => {
            multipart::create_multipart_upload(state, b_id.unwrap(), bucket, key, &c.parts.headers, writer_account, &bucket_owner).await
        }
        Operation::UploadPart => {
            let verifier = crate::auth::ChunkVerifier::for_request_with(&state.auth, iam.as_deref(), &c.parts.headers);
            multipart::upload_part(state, key, &path, &c.query_map, &c.parts.headers, verifier, c.body).await
        }
        Operation::UploadPartCopy => {
            multipart::upload_part_copy(state, key, &c.query_map, &c.parts.headers).await
        }
        Operation::CompleteMultipartUpload => {
            multipart::complete_multipart_upload(state, b_id.unwrap(), bucket, key, &path, &c.query_map, &c.parts.headers, c.body).await
        }
        Operation::AbortMultipartUpload => multipart::abort_multipart_upload(state, b_id.unwrap(), key, &c.query_map).await,
        Operation::ListParts => multipart::list_parts(state, bucket, key, &c.query_map).await,
        Operation::GetObjectTagging => tagging::get_object_tagging(state, b_id.unwrap(), key, &c.query_map).await,
        Operation::PutObjectTagging => tagging::put_object_tagging(state, b_id.unwrap(), key, &path, &c.query_map, c.body).await,
        Operation::DeleteObjectTagging => tagging::delete_object_tagging(state, b_id.unwrap(), key, &c.query_map).await,
        Operation::GetObjectLockConfiguration => lock::get_object_lock_configuration(state, b_id.unwrap(), bucket).await,
        Operation::PutObjectLockConfiguration => lock::put_object_lock_configuration(state, b_id.unwrap(), bucket, &path, c.body).await,
        Operation::GetObjectRetention => lock::get_object_retention(state, b_id.unwrap(), key, &c.query_map).await,
        Operation::PutObjectRetention => lock::put_object_retention(state, b_id.unwrap(), key, &path, &c.query_map, bypass_governance, c.body).await,
        Operation::GetObjectLegalHold => lock::get_object_legal_hold(state, b_id.unwrap(), key, &c.query_map).await,
        Operation::PutObjectLegalHold => lock::put_object_legal_hold(state, b_id.unwrap(), key, &path, &c.query_map, c.body).await,
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
            (Operation::GetBucketCors, Method::GET, Some("bkt"), None, "cors", None),
            (Operation::PutBucketCors, Method::PUT, Some("bkt"), None, "cors", None),
            (Operation::DeleteBucketCors, Method::DELETE, Some("bkt"), None, "cors", None),
            (Operation::GetBucketTagging, Method::GET, Some("bkt"), None, "tagging", None),
            (Operation::PutBucketTagging, Method::PUT, Some("bkt"), None, "tagging", None),
            (Operation::DeleteBucketTagging, Method::DELETE, Some("bkt"), None, "tagging", None),
            (Operation::CorsPreflight, Method::OPTIONS, Some("bkt"), None, "", None),
            (Operation::ListMultipartUploads, Method::GET, Some("bkt"), None, "uploads", None),
            (Operation::DeleteObjects, Method::POST, Some("bkt"), None, "delete", None),
            (Operation::PutObject, Method::PUT, Some("bkt"), Some("obj"), "", None),
            (Operation::PostObject, Method::POST, Some("bkt"), None, "", None),
            (Operation::CopyObject, Method::PUT, Some("bkt"), Some("obj"), "", Some("/src/key")),
            (Operation::GetObject, Method::GET, Some("bkt"), Some("obj"), "", None),
            (Operation::GetObjectAttributes, Method::GET, Some("bkt"), Some("obj"), "attributes", None),
            (Operation::HeadObject, Method::HEAD, Some("bkt"), Some("obj"), "", None),
            (Operation::DeleteObject, Method::DELETE, Some("bkt"), Some("obj"), "", None),
            (Operation::CreateMultipartUpload, Method::POST, Some("bkt"), Some("obj"), "uploads", None),
            (Operation::UploadPart, Method::PUT, Some("bkt"), Some("obj"), "partNumber=1&uploadId=u1", None),
            (Operation::UploadPartCopy, Method::PUT, Some("bkt"), Some("obj"), "partNumber=1&uploadId=u1", Some("/src/key")),
            (Operation::CompleteMultipartUpload, Method::POST, Some("bkt"), Some("obj"), "uploadId=u1", None),
            (Operation::AbortMultipartUpload, Method::DELETE, Some("bkt"), Some("obj"), "uploadId=u1", None),
            (Operation::ListParts, Method::GET, Some("bkt"), Some("obj"), "uploadId=u1", None),
            (Operation::GetObjectTagging, Method::GET, Some("bkt"), Some("obj"), "tagging", None),
            (Operation::PutObjectTagging, Method::PUT, Some("bkt"), Some("obj"), "tagging", None),
            (Operation::DeleteObjectTagging, Method::DELETE, Some("bkt"), Some("obj"), "tagging", None),
            (Operation::GetObjectLockConfiguration, Method::GET, Some("bkt"), None, "object-lock", None),
            (Operation::PutObjectLockConfiguration, Method::PUT, Some("bkt"), None, "object-lock", None),
            (Operation::GetObjectRetention, Method::GET, Some("bkt"), Some("obj"), "retention", None),
            (Operation::PutObjectRetention, Method::PUT, Some("bkt"), Some("obj"), "retention", None),
            (Operation::GetObjectLegalHold, Method::GET, Some("bkt"), Some("obj"), "legal-hold", None),
            (Operation::PutObjectLegalHold, Method::PUT, Some("bkt"), Some("obj"), "legal-hold", None),
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
            if *expected == Operation::PostObject {
                hdrs.insert(CONTENT_TYPE, "multipart/form-data; boundary=x".parse().unwrap());
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
            if method == Method::POST && b.is_some() && k.is_none() && q.is_empty() {
                // PostObject (#119): its key is in the form, so the old path-based function has no row.
                continue;
            }
            let op = classify(&method, b, k, q, &hdrs).unwrap();
            if op == Operation::CorsPreflight {
                continue;
            }
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
        assert_eq!(
            classify(&Method::PUT, Some("bkt"), Some("obj"), "partNumber=1&uploadId=u1", &copy_hdrs).unwrap(),
            Operation::UploadPartCopy
        );
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
