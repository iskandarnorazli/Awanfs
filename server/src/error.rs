//! awan-storage/server/src/error.rs
//! Standard S3 XML Error Codes & HTTP Status Mappings (22 Codes)
//! Reference: awan-storage/EDGE_CASES_AND_CONTRACTS.md Section 3

use hyper::StatusCode;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum S3ErrorCode {
    AccessDenied,
    BucketAlreadyExists,
    BucketAlreadyOwnedByYou,
    BucketNotEmpty,
    EntityTooSmall,
    IncompleteBody,
    InternalError,
    InvalidArgument,
    InvalidBucketName,
    InvalidDigest,
    InvalidObjectName,
    InvalidPart,
    InvalidPartOrder,
    InvalidRange,
    KeyTooLongError,
    MalformedXML,
    MethodNotAllowed,
    NoSuchBucket,
    NoSuchKey,
    NoSuchLifecycleConfiguration,
    NoSuchUpload,
    PreconditionFailed,
    RequestTimeTooSkewed,
    SignatureDoesNotMatch,
}

impl S3ErrorCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AccessDenied => "AccessDenied",
            Self::BucketAlreadyExists => "BucketAlreadyExists",
            Self::BucketAlreadyOwnedByYou => "BucketAlreadyOwnedByYou",
            Self::BucketNotEmpty => "BucketNotEmpty",
            Self::EntityTooSmall => "EntityTooSmall",
            Self::IncompleteBody => "IncompleteBody",
            Self::InternalError => "InternalError",
            Self::InvalidArgument => "InvalidArgument",
            Self::InvalidBucketName => "InvalidBucketName",
            Self::InvalidDigest => "InvalidDigest",
            Self::InvalidObjectName => "InvalidObjectName",
            Self::InvalidPart => "InvalidPart",
            Self::InvalidPartOrder => "InvalidPartOrder",
            Self::InvalidRange => "InvalidRange",
            Self::KeyTooLongError => "KeyTooLongError",
            Self::MalformedXML => "MalformedXML",
            Self::MethodNotAllowed => "MethodNotAllowed",
            Self::NoSuchBucket => "NoSuchBucket",
            Self::NoSuchKey => "NoSuchKey",
            Self::NoSuchLifecycleConfiguration => "NoSuchLifecycleConfiguration",
            Self::NoSuchUpload => "NoSuchUpload",
            Self::PreconditionFailed => "PreconditionFailed",
            Self::RequestTimeTooSkewed => "RequestTimeTooSkewed",
            Self::SignatureDoesNotMatch => "SignatureDoesNotMatch",
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::NoSuchKey | Self::NoSuchBucket | Self::NoSuchLifecycleConfiguration | Self::NoSuchUpload => {
                StatusCode::NOT_FOUND
            }
            Self::BucketNotEmpty | Self::BucketAlreadyExists | Self::BucketAlreadyOwnedByYou => {
                StatusCode::CONFLICT
            }
            Self::PreconditionFailed => StatusCode::PRECONDITION_FAILED,
            Self::InvalidRange => StatusCode::RANGE_NOT_SATISFIABLE,
            Self::AccessDenied | Self::SignatureDoesNotMatch | Self::RequestTimeTooSkewed => {
                StatusCode::FORBIDDEN
            }
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST,
        }
    }

    pub fn description(&self) -> &'static str {
        match self {
            Self::NoSuchKey => "The specified key does not exist.",
            Self::NoSuchBucket => "The specified bucket does not exist.",
            Self::BucketNotEmpty => "The bucket you tried to delete is not empty.",
            Self::EntityTooSmall => "Your proposed upload is smaller than the minimum allowed object size.",
            Self::IncompleteBody => "You did not provide the number of bytes specified by the Content-Length HTTP header.",
            Self::InvalidObjectName => "The specified object name is not valid.",
            Self::KeyTooLongError => "Your key is too long.",
            Self::InvalidRange => "The requested range cannot be satisfied.",
            Self::InvalidPartOrder => "The list of parts was not in ascending order.",
            Self::InvalidPart => "One or more of the specified parts could not be found or part ETag did not match.",
            Self::PreconditionFailed => "At least one of the pre-conditions you specified did not hold.",
            Self::RequestTimeTooSkewed => "The difference between the request time and the server's time is too large.",
            Self::SignatureDoesNotMatch => "The request signature we calculated does not match the signature you provided.",
            Self::NoSuchLifecycleConfiguration => "The lifecycle configuration does not exist.",
            Self::NoSuchUpload => "The specified multipart upload does not exist.",
            _ => "An error occurred.",
        }
    }
}
