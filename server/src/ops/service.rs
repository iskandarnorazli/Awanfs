use bytes::Bytes;
use hyper::header::CONTENT_TYPE;
use hyper::{Response, StatusCode};
use std::sync::Arc;

use crate::s3_api::{full_body, ResponseBody, S3ServiceState};
use crate::xml::list_buckets_xml;

pub async fn list_buckets(state: Arc<S3ServiceState>) -> Result<Response<ResponseBody>, hyper::Error> {
    let engine = state.engine.read();
    let created_map = state.bucket_created_ns.read();
    let mut entries: Vec<(&str, String)> = Vec::new();
    for (name, &b_id) in &engine.bucket_names {
        let ts = created_map.get(&b_id).copied().filter(|&t| t != 0).unwrap_or(state.started_at_ns);
        entries.push((name.as_str(), crate::versions::iso8601(ts)));
    }
    entries.sort_by_key(|(n, _)| *n);
    let b_list: Vec<(&str, &str)> = entries.iter().map(|(n, d)| (*n, d.as_str())).collect();
    let xml = list_buckets_xml(&b_list, "req-list-buckets");
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap())
}
