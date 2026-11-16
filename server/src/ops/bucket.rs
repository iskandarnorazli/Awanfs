use bytes::Bytes;
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE};
use hyper::{Response, StatusCode};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::ffi_bridge::{AWAN_ERR_BUCKET_NOT_EMPTY, AWAN_ERR_INVALID_ARG};
use crate::iam::{Iam, Principal};
use crate::s3_api::{
    bad_request, empty_body, full_body, internal_error_response, md5_to_etag_hex, read_body_admitted,
    s3_url_encode, ResponseBody, S3ServiceState, MAX_XML_BODY,
};
use crate::xml::{
    delete_objects_result_xml, list_objects_v1_xml, list_objects_v2_xml_full,
    list_versions_xml, location_constraint_xml, s3_error_xml, DeleteErrorItem, DeleteItemResult,
    ListV2, ListVersionsParams, S3ObjectEntry, VersionListing,
};

use super::object::{null_version_of, version_etag};

/// One page of a listing.
struct ListPage {
    objects: Vec<(Vec<u8>, crate::meta_index::ObjectMeta)>,
    prefixes: Vec<Vec<u8>>,
    truncated: bool,
    /// Where the next page starts (exclusive), when truncated.
    next: Option<Vec<u8>>,
    /// The last key or common prefix on this page.
    last_name: Option<Vec<u8>>,
}

/// Lists up to `max` keys and common prefixes after `start`. With a delimiter, keys sharing
/// the part of the key up to the first delimiter after `prefix` collapse into one common prefix,
/// and the scan jumps past them (prefix + 0xFF, a byte no UTF-8 key contains).
fn list_page(state: &S3ServiceState, b_id: u32, prefix: &[u8], delimiter: &[u8], start: &[u8], max: usize) -> ListPage {
    let mut page = ListPage { objects: Vec::new(), prefixes: Vec::new(), truncated: false, next: None, last_name: None };
    if max == 0 {
        return page;
    }
    let mut cursor = start.to_vec();
    'outer: loop {
        let batch = state.engine.read().list_keys(b_id, prefix, &cursor, 1000);
        if batch.is_empty() {
            break;
        }
        for (k, meta) in batch {
            let group = if delimiter.is_empty() {
                None
            } else {
                k[prefix.len()..].windows(delimiter.len()).position(|w| w == delimiter).map(|p| k[..prefix.len() + p + delimiter.len()].to_vec())
            };
            if page.objects.len() + page.prefixes.len() >= max {
                page.truncated = true;
                page.next = Some(cursor.clone());
                break 'outer;
            }
            match group {
                Some(cp) => {
                    let mut past = cp.clone();
                    past.push(0xFF);
                    if cp.as_slice() <= start {
                        // The marker was this common prefix (V1 NextMarker): already returned.
                        cursor = past;
                        continue 'outer;
                    }
                    page.last_name = Some(cp.clone());
                    if page.prefixes.last() != Some(&cp) {
                        page.prefixes.push(cp);
                    }
                    cursor = past;
                    continue 'outer;
                }
                None => {
                    cursor = k.clone();
                    page.last_name = Some(k.clone());
                    page.objects.push((k, meta));
                }
            }
        }
    }
    page
}

fn encode_hex_token(b: &[u8]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

fn decode_hex_token(t: &str) -> Option<Vec<u8>> {
    if t.len() % 2 != 0 {
        return None;
    }
    (0..t.len()).step_by(2).map(|i| u8::from_str_radix(t.get(i..i + 2)?, 16).ok()).collect()
}

pub async fn create_bucket(
    state: Arc<S3ServiceState>,
    bucket: &str,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let mut engine = state.engine.write();
    match engine.create_bucket(bucket) {
        Ok(id) => {
            let created_ns = state.clock.now_ns();
            state.bucket_created_ns.write().insert(id, created_ns);
            let _ = state.bucket_configs.lock().apply_bucket_create(id, bucket, created_ns, "", "");
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_LENGTH, "0")
                .body(empty_body())
                .unwrap())
        }
        Err(AWAN_ERR_INVALID_ARG) => {
            let xml = s3_error_xml("InvalidBucketName", "The specified bucket is not valid.", bucket, "req-b-err");
            Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap())
        }
        Err(_) => {
            let xml = s3_error_xml("InternalError", "Failed to create bucket", bucket, "req-b-err");
            Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap())
        }
    }
}

pub async fn head_bucket() -> Result<Response<ResponseBody>, hyper::Error> {
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_LENGTH, "0")
        .body(empty_body())
        .unwrap())
}

pub async fn delete_bucket(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let mut engine = state.engine.write();
    if !state.versions.read().keys_of(b_id, "").is_empty() {
        let xml = s3_error_xml("BucketNotEmpty", "The bucket you tried to delete is not empty", bucket, "req-del-b");
        return Ok(Response::builder()
            .status(StatusCode::CONFLICT)
            .header(CONTENT_TYPE, "application/xml")
            .body(full_body(Bytes::from(xml)))
            .unwrap());
    }
    match engine.delete_bucket(b_id) {
        Ok(()) => {
            state.versioning.write().remove(&b_id);
            state.bucket_created_ns.write().remove(&b_id);
            let _ = state.bucket_configs.lock().apply_bucket_delete(b_id, bucket, true);
            Ok(Response::builder()
                .status(StatusCode::NO_CONTENT)
                .header(CONTENT_LENGTH, "0")
                .body(empty_body())
                .unwrap())
        }
        Err(AWAN_ERR_BUCKET_NOT_EMPTY) => {
            let xml = s3_error_xml("BucketNotEmpty", "The bucket you tried to delete is not empty", bucket, "req-del-b");
            Ok(Response::builder()
                .status(StatusCode::CONFLICT)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap())
        }
        Err(_) => {
            let xml = s3_error_xml("InternalError", "Bucket delete error", bucket, "req-del-b");
            Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap())
        }
    }
}

pub async fn get_bucket_location() -> Result<Response<ResponseBody>, hyper::Error> {
    let xml = location_constraint_xml("us-east-1");
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap())
}

pub async fn get_bucket_versioning(
    state: Arc<S3ServiceState>,
    b_id: u32,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let status = state.versioning.read().get(&b_id).copied().unwrap_or_default();
    let inner = if status == crate::versions::Versioning::Unset {
        String::new()
    } else {
        format!("<Status>{}</Status>", status.as_str())
    };
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{}</VersioningConfiguration>",
        inner
    );
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap())
}

fn refusal_response(r: crate::fsm::Refusal, resource: &str) -> Response<ResponseBody> {
    let status = StatusCode::from_u16(r.status).unwrap_or(StatusCode::BAD_REQUEST);
    let xml = s3_error_xml(r.code, r.why, resource, "req-xml");
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap()
}

pub async fn put_bucket_versioning(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    path: &str,
    body: hyper::body::Incoming,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let (body_bytes, _body_guard) = match read_body_admitted(&state.body_budget, body, MAX_XML_BODY, None, state.body_idle_timeout(), path).await? {
        Ok(b) => b,
        Err(resp) => return Ok(resp),
    };
    let Ok(body_str) = std::str::from_utf8(&body_bytes) else {
        return Ok(bad_request(
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema.",
            bucket,
        ));
    };
    let req = match crate::request_xml::parse_versioning(body_str) {
        Ok(r) => r,
        Err(refusal) => return Ok(refusal_response(refusal, bucket)),
    };
    if let Err(e) = state.set_versioning(b_id, req.status) {
        return Ok(internal_error_response(bucket, &format!("could not record the versioning status: {}", e)));
    }
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_LENGTH, "0")
        .body(empty_body())
        .unwrap())
}

pub async fn list_objects(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    query_map: &HashMap<String, String>,
    v2: bool,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let q = |k: &str| query_map.get(k).map(|s| s.as_str()).unwrap_or("");
    let prefix = q("prefix");
    let delimiter = q("delimiter");
    if let Some(et) = query_map.get("encoding-type") {
        if !et.is_empty() && et != "url" {
            let xml = s3_error_xml("InvalidArgument", "Invalid Encoding Method specified in Request", bucket, "req-list-obj");
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap());
        }
    }
    let url_encode = q("encoding-type") == "url";
    let fetch_owner = v2 && q("fetch-owner").eq_ignore_ascii_case("true");
    let max_keys = match query_map.get("max-keys") {
        Some(s) => match s.parse::<i32>() {
            Ok(n) if n >= 0 => (n as usize).min(1000),
            _ => {
                let xml = s3_error_xml(
                    "InvalidArgument",
                    "Argument max-keys must be an integer between 0 and 2147483647",
                    bucket,
                    "req-list-obj",
                );
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .header(CONTENT_TYPE, "application/xml")
                    .body(full_body(Bytes::from(xml)))
                    .unwrap());
            }
        },
        None => 1000,
    };
    let start: Vec<u8> = if v2 {
        match query_map.get("continuation-token") {
            Some(t) => match decode_hex_token(t) {
                Some(c) => c,
                None => {
                    let xml = s3_error_xml("InvalidArgument", "The continuation token provided is incorrect", bucket, "req-list-obj");
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header(CONTENT_TYPE, "application/xml")
                        .body(full_body(Bytes::from(xml)))
                        .unwrap());
                }
            },
            None => q("start-after").as_bytes().to_vec(),
        }
    } else {
        q("marker").as_bytes().to_vec()
    };
    let page = list_page(&state, b_id, prefix.as_bytes(), delimiter.as_bytes(), &start, max_keys);
    let enc = |s: &str| if url_encode { s3_url_encode(s) } else { s.to_string() };
    let key_strings: Vec<String> = page.objects.iter().map(|(k, _)| enc(&String::from_utf8_lossy(k))).collect();
    let etag_strings: Vec<String> = page.objects.iter().map(|(_, m)| md5_to_etag_hex(m.etag_hi, m.etag_lo)).collect();
    let prefix_strings: Vec<String> = page.prefixes.iter().map(|p| enc(&String::from_utf8_lossy(p))).collect();
    let lm_strings: Vec<String> = page.objects.iter().map(|(_, m)| crate::versions::iso8601(m.created_at.saturating_mul(1_000_000))).collect();
    let contents: Vec<S3ObjectEntry> = page
        .objects
        .iter()
        .enumerate()
        .map(|(i, (_, m))| S3ObjectEntry {
            key: &key_strings[i],
            last_modified: &lm_strings[i],
            etag: &etag_strings[i],
            size: m.size,
        })
        .collect();
    let cps: Vec<&str> = prefix_strings.iter().map(|s| s.as_str()).collect();
    let next = page.next.as_ref().map(|c| encode_hex_token(c));
    let xml = if v2 {
        list_objects_v2_xml_full(&ListV2 {
            bucket,
            prefix: &enc(prefix),
            delimiter: &enc(delimiter),
            start_after: &enc(q("start-after")),
            continuation_token: query_map.get("continuation-token").map(|s| s.as_str()),
            max_keys: max_keys as u32,
            truncated: page.truncated,
            next_token: next.as_deref(),
            encoding_url: url_encode,
            fetch_owner,
            contents: &contents,
            prefixes: &cps,
        })
    } else {
        let next_marker = if page.truncated {
            page.last_name.as_ref().map(|n| enc(&String::from_utf8_lossy(n)))
        } else {
            None
        };
        list_objects_v1_xml(
            bucket,
            &enc(prefix),
            &enc(delimiter),
            &enc(q("marker")),
            max_keys as u32,
            page.truncated,
            next_marker.as_deref(),
            url_encode,
            &contents,
            &cps,
        )
    };
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap())
}

pub async fn list_object_versions(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    query_map: &HashMap<String, String>,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let q = |k: &str| query_map.get(k).map(|s| s.as_str()).unwrap_or("");
    let prefix = q("prefix");
    let delimiter = q("delimiter");
    let key_marker = q("key-marker");
    let version_id_marker = q("version-id-marker");

    let url = match query_map.get("encoding-type").map(|s| s.as_str()) {
        None | Some("") => false,
        Some("url") => true,
        Some(_) => {
            return Ok(bad_request(
                "InvalidArgument",
                "Invalid Encoding Method specified in Request",
                bucket,
            ));
        }
    };

    let max_keys = match query_map.get("max-keys") {
        None => 1000usize,
        Some(s) => match s.parse::<usize>() {
            Ok(n) => n.min(1000),
            Err(_) => {
                return Ok(bad_request(
                    "InvalidArgument",
                    "Argument maxKeys must be an integer between 0 and 2147483647",
                    bucket,
                ));
            }
        },
    };

    if !version_id_marker.is_empty() && key_marker.is_empty() {
        return Ok(bad_request(
            "InvalidArgument",
            "A version-id marker cannot be specified without a key marker.",
            bucket,
        ));
    }

    let enc = |s: &str| if url { s3_url_encode(s) } else { s.to_string() };

    if max_keys == 0 {
        let xml = list_versions_xml(&ListVersionsParams {
            bucket,
            prefix: &enc(prefix),
            delimiter: &enc(delimiter),
            key_marker: &enc(key_marker),
            version_id_marker,
            max_keys: 0,
            truncated: false,
            next_key_marker: None,
            next_version_id_marker: None,
            url,
            entries: &[],
            common_prefixes: &[],
        });
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/xml")
            .body(full_body(Bytes::from(xml)))
            .unwrap());
    }

    let mut keys: BTreeMap<String, Option<crate::meta_index::ObjectMeta>> = BTreeMap::new();
    if !version_id_marker.is_empty() {
        if let Ok(m) = state.engine.read().get_object(b_id, key_marker.as_bytes()) {
            if key_marker.starts_with(prefix) {
                keys.insert(key_marker.to_string(), Some(m));
            }
        }
    }
    let mut cursor = key_marker.as_bytes().to_vec();
    loop {
        let batch = state.engine.read().list_keys(b_id, prefix.as_bytes(), &cursor, 1000);
        if batch.is_empty() {
            break;
        }
        for (k, m) in batch {
            cursor = k.clone();
            keys.insert(String::from_utf8_lossy(&k).into_owned(), Some(m));
        }
    }

    let histories: HashMap<String, Vec<crate::versions::Version>> = {
        let vs = state.versions.read();
        vs.keys_of(b_id, prefix)
            .into_iter()
            .filter(|k| {
                if version_id_marker.is_empty() {
                    k.as_str() > key_marker
                } else {
                    k.as_str() >= key_marker
                }
            })
            .map(|k| {
                let list = vs.versions(b_id, &k).map(|l| l.to_vec()).unwrap_or_default();
                (k, list)
            })
            .collect()
    };
    for k in histories.keys() {
        keys.entry(k.clone()).or_insert(None);
    }

    let mut entries: Vec<VersionListing> = Vec::new();
    let mut common_prefixes: Vec<String> = Vec::new();
    let mut last_cp_raw: Option<String> = None;
    let mut last_key_raw: Option<String> = None;
    let mut last_vid: Option<String> = None;
    let mut truncated = false;

    'outer: for (k, row) in keys {
        if !delimiter.is_empty() {
            if let Some(pos) = k[prefix.len()..].find(delimiter) {
                let cp = &k[..prefix.len() + pos + delimiter.len()];
                if !key_marker.is_empty() && cp <= key_marker {
                    continue;
                }
                if last_cp_raw.as_deref() == Some(cp) {
                    continue;
                }
                if entries.len() + common_prefixes.len() >= max_keys {
                    truncated = true;
                    break 'outer;
                }
                last_cp_raw = Some(cp.to_string());
                last_key_raw = Some(cp.to_string());
                last_vid = None;
                common_prefixes.push(enc(cp));
                continue;
            }
        }

        let versions: Vec<crate::versions::Version> = match histories.get(&k) {
            Some(h) => h.clone(),
            None => row.iter().map(null_version_of).collect(),
        };

        let start_idx = if k == key_marker && !version_id_marker.is_empty() {
            if let Some(pos) = versions.iter().position(|v| v.id_string() == version_id_marker) {
                pos + 1
            } else if let Some(target_id) = crate::versions::parse_id(version_id_marker) {
                if target_id == 0 {
                    versions.len()
                } else {
                    versions
                        .iter()
                        .position(|v| !v.null && v.ts < target_id || v.null)
                        .unwrap_or(versions.len())
                }
            } else {
                0
            }
        } else {
            0
        };

        for (orig_idx, v) in versions.iter().enumerate().skip(start_idx) {
            if entries.len() + common_prefixes.len() >= max_keys {
                truncated = true;
                break 'outer;
            }
            let vid = v.id_string();
            entries.push(VersionListing {
                key: enc(&k),
                version_id: vid.clone(),
                latest: orig_idx == 0,
                marker: v.marker,
                last_modified: crate::versions::iso8601(v.ts),
                etag: version_etag(v).trim_matches('"').to_string(),
                size: v.size,
            });
            last_key_raw = Some(k.clone());
            last_vid = Some(vid);
        }
    }

    let next_key_marker = if truncated { last_key_raw.as_deref().map(&enc) } else { None };
    let next_version_id_marker = if truncated { last_vid.as_deref() } else { None };
    let cp_refs: Vec<&str> = common_prefixes.iter().map(|s| s.as_str()).collect();
    let xml = list_versions_xml(&ListVersionsParams {
        bucket,
        prefix: &enc(prefix),
        delimiter: &enc(delimiter),
        key_marker: &enc(key_marker),
        version_id_marker,
        max_keys: max_keys as u32,
        truncated,
        next_key_marker: next_key_marker.as_deref(),
        next_version_id_marker,
        url,
        entries: &entries,
        common_prefixes: &cp_refs,
    });
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap())
}

struct DeletedOutcome<'a> {
    key: &'a str,
    version_id: Option<&'a str>,
    delete_marker: bool,
    delete_marker_version_id: Option<String>,
}

pub async fn delete_objects(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    path: &str,
    principal: &Principal,
    iam: Option<&Iam>,
    body: hyper::body::Incoming,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let (body_bytes, _body_guard) = match read_body_admitted(&state.body_budget, body, MAX_XML_BODY, None, state.body_idle_timeout(), path).await? {
        Ok(b) => b,
        Err(resp) => return Ok(resp),
    };
    let Ok(body_str) = std::str::from_utf8(&body_bytes) else {
        return Ok(bad_request(
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema.",
            bucket,
        ));
    };
    let req = match crate::request_xml::parse_delete_objects(body_str) {
        Ok(r) => r,
        Err(refusal) => return Ok(refusal_response(refusal, bucket)),
    };

    let user_may = |action: &str, resource: &str| match principal {
        Principal::User(ak) => iam.is_some_and(|i| i.allows(ak, action, resource)),
        _ => true,
    };

    let mut ok_keys: Vec<DeletedOutcome<'_>> = Vec::new();
    let mut failed: Vec<(String, &'static str, String)> = Vec::new();
    for obj in &req.objects {
        let k = &obj.key;
        let vid = obj.version_id.as_deref();
        if !user_may("s3:DeleteObject", &format!("arn:aws:s3:::{}/{}", bucket, k)) {
            failed.push((k.clone(), "AccessDenied", "Access Denied".to_string()));
            continue;
        }
        match vid {
            Some(v) => match crate::versions::parse_id(v) {
                Some(id) => match state.delete_version(b_id, k, id).await {
                    Ok(removed) => {
                        let is_dm = removed.is_some_and(|ver| ver.marker);
                        ok_keys.push(DeletedOutcome {
                            key: k.as_str(),
                            version_id: Some(v),
                            delete_marker: is_dm,
                            delete_marker_version_id: if is_dm { Some(v.to_string()) } else { None },
                        });
                    }
                    Err(e) => failed.push((k.clone(), "InternalError", e)),
                },
                None => failed.push((
                    k.clone(),
                    "NoSuchVersion",
                    "The specified version does not exist.".to_string(),
                )),
            },
            None => match state.delete_key(b_id, k).await {
                Ok(dm_vid) => {
                    let is_dm = dm_vid.is_some();
                    ok_keys.push(DeletedOutcome {
                        key: k.as_str(),
                        version_id: None,
                        delete_marker: is_dm,
                        delete_marker_version_id: dm_vid,
                    });
                }
                Err(e) => failed.push((k.clone(), "InternalError", e)),
            },
        }
    }
    let deleted: Vec<DeleteItemResult> = if req.quiet {
        Vec::new()
    } else {
        ok_keys
            .iter()
            .map(|o| DeleteItemResult {
                key: o.key,
                version_id: o.version_id,
                delete_marker: o.delete_marker,
                delete_marker_version_id: o.delete_marker_version_id.as_deref(),
            })
            .collect()
    };
    let errors: Vec<DeleteErrorItem> = failed.iter().map(|(k, c, e)| DeleteErrorItem { key: k, code: c, message: e }).collect();
    let xml = delete_objects_result_xml(&deleted, &errors);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap())
}
