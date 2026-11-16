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
    delete_objects_result_xml, list_object_versions_xml, list_objects_v1_xml,
    list_objects_v2_xml_full, list_versions_xml, location_constraint_xml, s3_error_xml,
    DeleteErrorItem, DeleteItemResult, ListV2, S3ObjectEntry, VersionListing,
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
                    page.prefixes.push(cp);
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
            if let Some(ref d) = state.data_dir {
                let manifest = d.join("buckets.manifest");
                if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(&manifest) {
                    use std::io::Write;
                    let _ = writeln!(f, "+{}:{}", id, bucket);
                }
            }
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
            if let Some(ref d) = state.data_dir {
                let manifest = d.join("buckets.manifest");
                if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(&manifest) {
                    use std::io::Write;
                    let _ = writeln!(f, "-{}:{}", b_id, bucket);
                }
            }
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

pub async fn put_bucket_versioning(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    path: &str,
    body: hyper::body::Incoming,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let (body_bytes, _body_guard) = match read_body_admitted(&state.body_budget, body, MAX_XML_BODY, None, path).await? {
        Ok(b) => b,
        Err(resp) => return Ok(resp),
    };
    let body = String::from_utf8_lossy(&body_bytes);
    let status = body
        .find("<Status>")
        .and_then(|s| body[s + 8..].split('<').next())
        .and_then(|t| crate::versions::Versioning::parse(t.trim()));
    let Some(status) = status else {
        return Ok(bad_request(
            "MalformedXML",
            "The versioning configuration needs <Status>Enabled</Status> or <Status>Suspended</Status>.",
            bucket,
        ));
    };
    if let Err(e) = state.set_versioning(b_id, status) {
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
    let url_encode = q("encoding-type") == "url";
    let max_keys = query_map.get("max-keys").and_then(|s| s.parse::<usize>().ok()).unwrap_or(1000).min(1000);
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
    let contents: Vec<S3ObjectEntry> = page
        .objects
        .iter()
        .enumerate()
        .map(|(i, (_, m))| S3ObjectEntry {
            key: &key_strings[i],
            last_modified: "2026-11-10T00:00:00.000Z",
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
    let key_marker = q("key-marker");
    let url = q("encoding-type") == "url";
    let max_keys = query_map.get("max-keys").and_then(|s| s.parse::<usize>().ok()).unwrap_or(1000).min(1000);

    let rows = state.engine.read().list_keys(b_id, prefix.as_bytes(), key_marker.as_bytes(), max_keys + 1);
    let more_rows = rows.len() > max_keys;
    let last_row = rows.last().map(|(k, _)| String::from_utf8_lossy(k).into_owned());
    let mut keys: BTreeMap<String, Option<crate::meta_index::ObjectMeta>> =
        rows.into_iter().map(|(k, m)| (String::from_utf8_lossy(&k).into_owned(), Some(m))).collect();
    let histories: HashMap<String, Vec<crate::versions::Version>> = {
        let vs = state.versions.read();
        vs.keys_of(b_id, prefix)
            .into_iter()
            .filter(|k| k.as_str() > key_marker && (!more_rows || last_row.as_deref().map_or(true, |l| k.as_str() <= l)))
            .map(|k| {
                let list = vs.versions(b_id, &k).map(|l| l.to_vec()).unwrap_or_default();
                (k, list)
            })
            .collect()
    };
    for k in histories.keys() {
        keys.entry(k.clone()).or_insert(None);
    }
    let enc = |s: &str| if url { s3_url_encode(s) } else { s.to_string() };
    let mut entries = Vec::new();
    let mut next = None;
    let mut truncated = false;
    let total = keys.len();
    for (i, (k, row)) in keys.into_iter().enumerate() {
        let versions: Vec<crate::versions::Version> = match histories.get(&k) {
            Some(h) => h.clone(),
            None => row.iter().map(null_version_of).collect(),
        };
        if !entries.is_empty() && entries.len() + versions.len() > max_keys {
            truncated = true;
            break;
        }
        for (j, v) in versions.iter().enumerate() {
            entries.push(VersionListing {
                key: enc(&k),
                version_id: v.id_string(),
                latest: j == 0,
                marker: v.marker,
                last_modified: crate::versions::iso8601(v.ts),
                etag: version_etag(v).trim_matches('"').to_string(),
                size: v.size,
            });
        }
        next = Some(enc(&k));
        if entries.len() >= max_keys && (i + 1 < total || more_rows) {
            truncated = true;
            break;
        }
    }
    let xml = list_versions_xml(
        bucket,
        &enc(prefix),
        &enc(key_marker),
        max_keys as u32,
        truncated,
        if truncated { next.as_deref() } else { None },
        url,
        &entries,
    );
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap())
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
    let (body_bytes, _body_guard) = match read_body_admitted(&state.body_budget, body, MAX_XML_BODY, None, path).await? {
        Ok(b) => b,
        Err(resp) => return Ok(resp),
    };
    let body_str = String::from_utf8_lossy(&body_bytes);

    let text = |block: &str, tag: &str| -> Option<String> {
        let open = format!("<{}>", tag);
        let close = format!("</{}>", tag);
        let s = block.find(&open)? + open.len();
        let e = block[s..].find(&close)? + s;
        let raw = &block[s..e];
        Some(quick_xml::escape::unescape(raw).map(|k| k.into_owned()).unwrap_or_else(|_| raw.to_string()))
    };
    let mut keys_to_delete: Vec<String> = Vec::new();
    let mut version_ids: Vec<Option<String>> = Vec::new();
    let mut cursor = body_str.as_ref();
    while let Some(start) = cursor.find("<Object>") {
        let rest = &cursor[start + 8..];
        let end = rest.find("</Object>").unwrap_or(rest.len());
        let block = &rest[..end];
        if let Some(k) = text(block, "Key") {
            keys_to_delete.push(k);
            version_ids.push(text(block, "VersionId"));
        }
        cursor = &rest[end..];
    }

    let user_may = |action: &str, resource: &str| match principal {
        Principal::User(ak) => iam.is_some_and(|i| i.allows(ak, action, resource)),
        _ => true,
    };

    let mut ok_keys: Vec<(&str, Option<&str>)> = Vec::new();
    let mut failed: Vec<(String, &'static str, String)> = Vec::new();
    for (k, vid) in keys_to_delete.iter().zip(version_ids.iter()) {
        if !user_may("s3:DeleteObject", &format!("arn:aws:s3:::{}/{}", bucket, k)) {
            failed.push((k.clone(), "AccessDenied", "Access Denied".to_string()));
            continue;
        }
        let result = match vid.as_deref() {
            Some(v) => match crate::versions::parse_id(v) {
                Some(id) => state.delete_version(b_id, k, id).await.map(|_| ()),
                None => Err("NoSuchVersion".to_string()),
            },
            None => state.delete_key(b_id, k).await.map(|_| ()),
        };
        match result {
            Ok(()) => ok_keys.push((k.as_str(), vid.as_deref())),
            Err(e) if e == "NoSuchVersion" => failed.push((k.clone(), "NoSuchVersion", "The specified version does not exist.".to_string())),
            Err(e) => failed.push((k.clone(), "InternalError", e)),
        }
    }
    let deleted: Vec<DeleteItemResult> = ok_keys.into_iter().map(|(key, version_id)| DeleteItemResult { key, version_id }).collect();
    let errors: Vec<DeleteErrorItem> = failed.iter().map(|(k, c, e)| DeleteErrorItem { key: k, code: c, message: e }).collect();
    let xml = delete_objects_result_xml(&deleted, &errors);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap())
}
