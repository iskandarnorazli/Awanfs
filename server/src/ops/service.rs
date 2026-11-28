use bytes::Bytes;
use hyper::header::CONTENT_TYPE;
use hyper::{Response, StatusCode};
use std::collections::HashMap;
use std::sync::Arc;

use super::bucket::{decode_hex_token, encode_hex_token};
use crate::s3_api::{bad_request, full_body, ResponseBody, S3ServiceState};
use crate::xml::list_buckets_xml;

pub async fn list_buckets(
    state: Arc<S3ServiceState>,
    query_map: &HashMap<String, String>,
    caller_account: Option<&str>,
    list_all_open_anon: bool,
    iam: Option<&crate::iam::Iam>,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let max_buckets = match query_map.get("max-buckets") {
        Some(s) => match s.parse::<i32>() {
            // 1 to 10000 as S3 allows; above it is refused, not capped (#297).
            Ok(n) if (1..=10_000).contains(&n) => n as usize,
            _ => {
                return Ok(bad_request(
                    "InvalidArgument",
                    "Argument max-buckets must be an integer between 1 and 10000",
                    "/",
                ));
            }
        },
        None => 10_000,
    };

    let after = match query_map.get("continuation-token") {
        Some(t) => match decode_hex_token(t).and_then(|b| String::from_utf8(b).ok()) {
            Some(s) => Some(s),
            None => {
                return Ok(bad_request(
                    "InvalidArgument",
                    "The continuation token provided is incorrect",
                    "/",
                ));
            }
        },
        None => None,
    };

    let prefix = query_map.get("prefix").map(|s| s.as_str());

    let engine = state.engine.read();
    let created_map = state.bucket_created_ns.read();
    let cfg_snap = state.bucket_config_reader.snapshot();
    let mut entries: Vec<(String, String)> = Vec::new();
    if caller_account.is_some() || list_all_open_anon {
        if let Some(ns_map) = engine.bucket_names.get("") {
            for (name, &b_id) in ns_map {
                if let Some(acct) = caller_account {
                    let owner = cfg_snap
                        .get(&b_id)
                        .map(|c| c.effective_owner())
                        .unwrap_or("root");
                    if owner != acct {
                        continue;
                    }
                }
                if let Some(pfx) = prefix {
                    if !name.starts_with(pfx) {
                        continue;
                    }
                }
                if let Some(ref marker) = after {
                    if name.as_str() <= marker.as_str() {
                        continue;
                    }
                }
                let ts = created_map.get(&b_id).copied().filter(|&t| t != 0).unwrap_or(state.started_at_ns);
                entries.push((name.clone(), crate::versions::iso8601(ts)));
            }
        }
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    let next_token = if entries.len() > max_buckets {
        entries.truncate(max_buckets);
        entries.last().map(|(n, _)| encode_hex_token(n.as_bytes()))
    } else {
        None
    };

    let owner = crate::acl::owner_for_account(caller_account.unwrap_or(""), iam);
    let b_list: Vec<(&str, &str)> = entries.iter().map(|(n, d)| (n.as_str(), d.as_str())).collect();
    let xml = list_buckets_xml(&b_list, prefix, next_token.as_deref(), &owner.id, &owner.display_name, "req-list-buckets");
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap())
}
