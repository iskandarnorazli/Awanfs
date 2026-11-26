use bytes::Bytes;
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE};
use hyper::{Response, StatusCode};
use std::sync::Arc;

use crate::bucket_config::ConfigKind;
use crate::lifecycle_config::lifecycle_config_xml;
use crate::s3_api::{
    empty_body, full_body, read_body_admitted, ResponseBody, S3ServiceState, MAX_XML_BODY,
};
use crate::xml::s3_error_xml;

pub async fn put_bucket_lifecycle(
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
    if body_bytes.is_empty() {
        let xml = s3_error_xml(
            "MalformedXML",
            "LifecycleConfiguration must contain at least one Rule",
            bucket,
            "req-lc",
        );
        return Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header(CONTENT_TYPE, "application/xml")
            .body(full_body(Bytes::from(xml)))
            .unwrap());
    }
    let res = {
        let mut bc = state.bucket_configs.lock();
        let res = bc.put_config(b_id, ConfigKind::Lifecycle, &body_bytes);
        if res.is_ok() {
            state.emit_bucket_config_event(
                b_id,
                bucket,
                ConfigKind::Lifecycle.as_str(),
                crate::events::CommitKind::BucketConfig,
                body_bytes.len() as u64,
            );
        }
        res
    };
    match res {
        Ok(()) => Ok(Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_LENGTH, "0")
            .body(empty_body())
            .unwrap()),
        Err(e) => {
            let status = StatusCode::from_u16(e.status()).unwrap_or(StatusCode::BAD_REQUEST);
            let xml = s3_error_xml(e.code(), &e.to_string(), bucket, "req-lc");
            Ok(Response::builder()
                .status(status)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap())
        }
    }
}

pub async fn get_bucket_lifecycle(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
) -> Result<Response<ResponseBody>, hyper::Error> {
    match state.bucket_lifecycle(b_id) {
        Some(cfg) => {
            let xml = lifecycle_config_xml(&cfg);
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap())
        }
        None => {
            let xml = s3_error_xml(
                "NoSuchLifecycleConfiguration",
                "The lifecycle configuration does not exist",
                bucket,
                "req-lc",
            );
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
    b_id: u32,
    bucket: &str,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let had_config = state.bucket_lifecycle(b_id).is_some();
    let res = state
        .bucket_configs
        .lock()
        .delete_config(b_id, ConfigKind::Lifecycle);
    match res {
        Ok(()) => {
            if had_config {
                state.emit_bucket_config_event(
                    b_id,
                    bucket,
                    ConfigKind::Lifecycle.as_str(),
                    crate::events::CommitKind::BucketConfig,
                    0,
                );
            }
            Ok(Response::builder()
                .status(StatusCode::NO_CONTENT)
                .header(CONTENT_LENGTH, "0")
                .body(empty_body())
                .unwrap())
        }
        Err(e) => {
            let status = StatusCode::from_u16(e.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            let xml = s3_error_xml(e.code(), &e.to_string(), bucket, "req-lc");
            Ok(Response::builder()
                .status(status)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap())
        }
    }
}
