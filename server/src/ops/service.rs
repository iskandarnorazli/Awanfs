use bytes::Bytes;
use hyper::header::CONTENT_TYPE;
use hyper::{Response, StatusCode};
use std::sync::Arc;

use crate::s3_api::{full_body, ResponseBody, S3ServiceState};
use crate::xml::list_buckets_xml;

pub async fn list_buckets(state: Arc<S3ServiceState>) -> Result<Response<ResponseBody>, hyper::Error> {
    let engine = state.engine.read();
    let mut b_list = Vec::new();
    for (name, _) in &engine.bucket_names {
        b_list.push((name.as_str(), "2026-11-10T00:00:00.000Z"));
    }
    b_list.sort_by_key(|&(n, _)| n);
    let xml = list_buckets_xml(&b_list, "req-list-buckets");
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap())
}
