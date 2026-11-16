use bytes::Bytes;
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE};
use hyper::{Response, StatusCode};
use std::sync::Arc;

use crate::lifecycle::{lifecycle_config_xml, parse_lifecycle_xml};
use crate::s3_api::{empty_body, full_body, read_body_admitted, ResponseBody, S3ServiceState, MAX_XML_BODY};
use crate::xml::s3_error_xml;

pub async fn put_bucket_lifecycle(
    state: Arc<S3ServiceState>,
    bucket: &str,
    path: &str,
    body: hyper::body::Incoming,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let (body_bytes, _body_guard) = match read_body_admitted(&state.body_budget, body, MAX_XML_BODY, None, path).await? {
        Ok(b) => b,
        Err(resp) => return Ok(resp),
    };
    let body_str = String::from_utf8_lossy(&body_bytes);
    match parse_lifecycle_xml(&body_str) {
        Ok(rules) => {
            state.lifecycle.set_rules(bucket, rules);
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_LENGTH, "0")
                .body(empty_body())
                .unwrap())
        }
        Err(_) => {
            let xml = s3_error_xml("MalformedXML", "The XML provided was not well-formed", bucket, "req-lc");
            Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap())
        }
    }
}

pub async fn get_bucket_lifecycle(
    state: Arc<S3ServiceState>,
    bucket: &str,
) -> Result<Response<ResponseBody>, hyper::Error> {
    match state.lifecycle.get_rules(bucket) {
        Some(rules) => {
            let xml = lifecycle_config_xml(&rules);
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap())
        }
        None => {
            let xml = s3_error_xml("NoSuchLifecycleConfiguration", "The lifecycle configuration does not exist", bucket, "req-lc");
            Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap())
        }
    }
}

pub async fn delete_bucket_lifecycle(
    state: Arc<S3ServiceState>,
    bucket: &str,
) -> Result<Response<ResponseBody>, hyper::Error> {
    state.lifecycle.delete_rules(bucket);
    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(CONTENT_LENGTH, "0")
        .body(empty_body())
        .unwrap())
}
