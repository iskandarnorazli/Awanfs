use bytes::Bytes;
use hyper::header::{HeaderName, HeaderValue, CONTENT_LENGTH, CONTENT_TYPE, VARY};
use hyper::{HeaderMap, Response, StatusCode};
use std::sync::Arc;

use crate::bucket_config::ConfigKind;
use crate::cors::{cors_xml, match_preflight, CorsAnswer, CorsRule};
use crate::s3_api::{
    empty_body, full_body, read_body_admitted, ResponseBody, S3ServiceState, MAX_XML_BODY,
};
use crate::xml::s3_error_xml;

const HDR_ALLOW_ORIGIN: HeaderName = HeaderName::from_static("access-control-allow-origin");
const HDR_ALLOW_METHODS: HeaderName = HeaderName::from_static("access-control-allow-methods");
const HDR_ALLOW_HEADERS: HeaderName = HeaderName::from_static("access-control-allow-headers");
const HDR_MAX_AGE: HeaderName = HeaderName::from_static("access-control-max-age");
const HDR_EXPOSE_HEADERS: HeaderName = HeaderName::from_static("access-control-expose-headers");

fn xml_error(status: StatusCode, code: &str, msg: &str, resource: &str) -> Response<ResponseBody> {
    let xml = s3_error_xml(code, msg, resource, "req-cors");
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap()
}

fn insert_header(headers: &mut HeaderMap, name: HeaderName, val: &str) {
    if let Ok(hv) = HeaderValue::from_str(val) {
        headers.insert(name, hv);
    }
}

/// Writes CORS response headers from a matched `CorsAnswer`.
pub fn apply_cors_headers(headers: &mut HeaderMap, answer: &CorsAnswer, include_allow_headers: bool) {
    insert_header(headers, HDR_ALLOW_ORIGIN, &answer.allow_origin);
    if !answer.allow_methods.is_empty() {
        let methods: Vec<&str> = answer.allow_methods.iter().map(|m| m.as_str()).collect();
        insert_header(headers, HDR_ALLOW_METHODS, &methods.join(", "));
    }
    if include_allow_headers && !answer.allow_headers.is_empty() {
        insert_header(headers, HDR_ALLOW_HEADERS, &answer.allow_headers.join(", "));
    }
    if let Some(max_age) = answer.max_age_seconds {
        insert_header(headers, HDR_MAX_AGE, &max_age.to_string());
    }
    if !answer.expose_headers.is_empty() {
        insert_header(headers, HDR_EXPOSE_HEADERS, &answer.expose_headers.join(", "));
    }
    if answer.vary_origin {
        headers.insert(VARY, HeaderValue::from_static("Origin"));
    }
}

pub async fn put_bucket_cors(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    path: &str,
    body: hyper::body::Incoming,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let (body_bytes, _body_guard) =
        match read_body_admitted(&state.body_budget, body, MAX_XML_BODY, None, state.body_idle_timeout(), path).await? {
            Ok(b) => b,
            Err(resp) => return Ok(resp),
        };
    if body_bytes.is_empty() {
        return Ok(xml_error(
            StatusCode::BAD_REQUEST,
            "MalformedXML",
            "CORSConfiguration must contain at least one CORSRule",
            bucket,
        ));
    }
    let res = state
        .bucket_configs
        .lock()
        .put_config(b_id, ConfigKind::Cors, &body_bytes);
    match res {
        Ok(()) => Ok(Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_LENGTH, "0")
            .body(empty_body())
            .unwrap()),
        Err(e) => {
            let status = StatusCode::from_u16(e.status()).unwrap_or(StatusCode::BAD_REQUEST);
            Ok(xml_error(status, e.code(), &e.to_string(), bucket))
        }
    }
}

pub async fn get_bucket_cors(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let cfg = state.bucket_config_reader.load_bucket(b_id);
    let parsed = cfg.as_ref().and_then(|c| c.get(ConfigKind::Cors));
    match parsed {
        Some(p) => {
            let body = match p.parsed.as_ref().and_then(|a| a.downcast_ref::<Vec<CorsRule>>()) {
                Some(rules) => Bytes::from(cors_xml(rules)),
                None => p.raw.clone(),
            };
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(body))
                .unwrap())
        }
        None => Ok(xml_error(
            StatusCode::NOT_FOUND,
            "NoSuchCORSConfiguration",
            "The CORS configuration does not exist",
            bucket,
        )),
    }
}

pub async fn delete_bucket_cors(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let res = state
        .bucket_configs
        .lock()
        .delete_config(b_id, ConfigKind::Cors);
    match res {
        Ok(()) => Ok(Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header(CONTENT_LENGTH, "0")
            .body(empty_body())
            .unwrap()),
        Err(e) => {
            let status = StatusCode::from_u16(e.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            Ok(xml_error(status, e.code(), &e.to_string(), bucket))
        }
    }
}

pub async fn preflight(
    state: Arc<S3ServiceState>,
    b_id: u32,
    path: &str,
    headers: &HeaderMap,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let origin = headers
        .get("origin")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let req_method = headers
        .get("access-control-request-method")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let (Some(origin), Some(req_method)) = (origin, req_method) else {
        return Ok(xml_error(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "Insufficient information. Origin and Access-Control-Request-Method headers are needed.",
            path,
        ));
    };

    let req_headers: Vec<&str> = headers
        .get("access-control-request-headers")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').map(str::trim).filter(|h| !h.is_empty()).collect())
        .unwrap_or_default();

    let cfg = state.bucket_config_reader.load_bucket(b_id);
    let answer = cfg
        .as_ref()
        .and_then(|c| c.get(ConfigKind::Cors))
        .and_then(|p| p.parsed.as_ref())
        .and_then(|a| a.downcast_ref::<Vec<CorsRule>>())
        .and_then(|rules| match_preflight(rules, origin, req_method, &req_headers));

    match answer {
        Some(ans) => {
            let mut resp = Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_LENGTH, "0")
                .body(empty_body())
                .unwrap();
            apply_cors_headers(resp.headers_mut(), &ans, true);
            Ok(resp)
        }
        None => Ok(xml_error(
            StatusCode::FORBIDDEN,
            "AccessDenied",
            "CORSResponse: This CORS request is not allowed.",
            path,
        )),
    }
}
