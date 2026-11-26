//! Shared oracle checks for Issue #271 fuzz targets (`checksum_headers`,
//! `list_buckets_query`, `raw_s3_key`), called by both `fuzz_targets/*.rs` and unit tests.

use http_body_util::BodyExt;
use hyper::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use hyper::{Method, StatusCode};
use s3_server::ops::{classify, ClassifyRefusal, Operation, OPS};
use s3_server::bucket_config::{
    base64_decode as bucket_base64_decode, base64_encode as bucket_base64_encode,
};
use s3_server::checksum::{base64_decode, requested_checksum, ChecksumAlgorithm};
use s3_server::ops::service::list_buckets;
use s3_server::{
    base64_encode, parse_s3_target, percent_decode, percent_decode_path_bytes, raw_s3_key,
    ParsedTarget, S3ServiceState,
};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

fn hex_decode_strict(t: &str) -> Option<Vec<u8>> {
    if t.len() % 2 != 0 || !t.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    (0..t.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&t[i..i + 2], 16).ok())
        .collect()
}

fn check_single_checksum_header(header_name: &'static str, expected_alg: ChecksumAlgorithm, val: &[u8]) {
    let Ok(hv) = HeaderValue::from_bytes(val) else {
        return;
    };
    let mut headers = HeaderMap::new();
    headers.insert(header_name, hv);
    if let Ok(Some((alg, Some(decoded_str)))) = requested_checksum(&headers) {
        assert_eq!(alg, expected_alg);
        let decoded = base64_decode(&decoded_str).expect("requested_checksum accepted invalid base64");
        let expected_len = match alg {
            ChecksumAlgorithm::Crc32 | ChecksumAlgorithm::Crc32c => 4,
            ChecksumAlgorithm::Crc64Nvme => 8,
            ChecksumAlgorithm::Sha1 => 20,
            ChecksumAlgorithm::Sha256 => 32,
        };
        assert_eq!(decoded.len(), expected_len);
        assert_eq!(
            base64_encode(&decoded),
            decoded_str,
            "checksum header {:?} decoded and re-encoded does not match input {:?}",
            header_name,
            decoded_str
        );
    }
}

/// Oracle for `checksum_headers`:
/// - `requested_checksum`, `checksum::base64_decode`, and `bucket_config::base64_decode` never panic on arbitrary bytes.
/// - Decoding valid base64 through either decoder and encoding it again gives the input.
/// - Valid 8-byte (CRC64NVME), 16-byte (Content-MD5), and 32-byte (SHA256) base64 digests round-trip.
pub fn check_checksum_headers(data: &[u8]) {
    if let Ok(s) = std::str::from_utf8(data) {
        if let Some(raw) = base64_decode(s) {
            assert_eq!(
                base64_encode(&raw),
                s,
                "checksum::base64_decode accepted non-canonical input {:?}",
                s
            );
        }
        if let Ok(raw) = bucket_base64_decode(s) {
            assert_eq!(
                base64_encode(&raw),
                s,
                "bucket_config::base64_decode accepted non-canonical input {:?}",
                s
            );
            assert_eq!(
                bucket_base64_encode(&raw),
                s,
                "bucket_config::base64_encode round-trip mismatch for {:?}",
                s
            );
        }
    }

    check_single_checksum_header("x-amz-checksum-crc64nvme", ChecksumAlgorithm::Crc64Nvme, data);
    check_single_checksum_header("x-amz-checksum-sha256", ChecksumAlgorithm::Sha256, data);

    // Combined header fuzzing (newline-separated crc64nvme, sha256, sdk-algorithm, trailer).
    let text = String::from_utf8_lossy(data);
    let mut lines = text.split('\n');
    let mut combined = HeaderMap::new();
    for name in [
        "x-amz-checksum-crc64nvme",
        "x-amz-checksum-sha256",
        "x-amz-sdk-checksum-algorithm",
        "x-amz-trailer",
    ] {
        if let Some(line) = lines.next() {
            if !line.is_empty() {
                if let Ok(hv) = HeaderValue::from_str(line) {
                    combined.insert(name, hv);
                }
            }
        }
    }
    if let Ok(Some((_alg, Some(val)))) = requested_checksum(&combined) {
        let decoded = base64_decode(&val).expect("requested_checksum accepted invalid base64");
        assert_eq!(
            base64_encode(&decoded),
            val,
            "combined checksum header value {:?} did not round-trip",
            val
        );
    }

    // Valid digest round-trip for CRC64NVME (8 bytes) and SHA256 (32 bytes).
    let mut d8 = [0u8; 8];
    for (i, &b) in data.iter().take(8).enumerate() {
        d8[i] = b;
    }
    let b64_8 = base64_encode(&d8);
    let mut h8 = HeaderMap::new();
    h8.insert("x-amz-checksum-crc64nvme", HeaderValue::from_str(&b64_8).unwrap());
    h8.insert("x-amz-sdk-checksum-algorithm", HeaderValue::from_static("CRC64NVME"));
    assert_eq!(
        requested_checksum(&h8),
        Ok(Some((ChecksumAlgorithm::Crc64Nvme, Some(b64_8.clone()))))
    );
    assert_eq!(base64_decode(&b64_8).as_deref(), Some(&d8[..]));

    let mut d32 = [0u8; 32];
    if !data.is_empty() {
        for (i, slot) in d32.iter_mut().enumerate() {
            *slot = data[i % data.len()];
        }
    }
    let b64_32 = base64_encode(&d32);
    let mut h32 = HeaderMap::new();
    h32.insert("x-amz-checksum-sha256", HeaderValue::from_str(&b64_32).unwrap());
    h32.insert("x-amz-sdk-checksum-algorithm", HeaderValue::from_static("SHA256"));
    assert_eq!(
        requested_checksum(&h32),
        Ok(Some((ChecksumAlgorithm::Sha256, Some(b64_32.clone()))))
    );
    assert_eq!(base64_decode(&b64_32).as_deref(), Some(&d32[..]));

    let mut d16 = [0u8; 16];
    if !data.is_empty() {
        for (i, slot) in d16.iter_mut().enumerate() {
            *slot = data[i % data.len()];
        }
    }
    let b64_16 = base64_encode(&d16);
    assert_eq!(bucket_base64_encode(&d16), b64_16);
    assert_eq!(bucket_base64_decode(&b64_16).as_deref(), Ok(&d16[..]));
}

fn list_buckets_runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("create tokio runtime")
    })
}

fn list_buckets_state() -> &'static Arc<S3ServiceState> {
    static STATE: OnceLock<Arc<S3ServiceState>> = OnceLock::new();
    STATE.get_or_init(|| {
        let state = Arc::new(S3ServiceState::new());
        {
            let mut engine = state.engine.write();
            for name in ["bkt-a", "bkt-b", "other-c"] {
                engine.create_bucket(name).expect("create seed bucket");
            }
        }
        state
    })
}

fn call_list_buckets(query_map: &HashMap<String, String>) -> (StatusCode, String) {
    let rt = list_buckets_runtime();
    let state = Arc::clone(list_buckets_state());
    rt.block_on(async move {
        let resp = list_buckets(state, query_map, None)
            .await
            .expect("list_buckets never returns hyper::Error");
        let status = resp.status();
        let collected = resp.into_body().collect().await.expect("collect body");
        let xml = String::from_utf8_lossy(&collected.to_bytes()).into_owned();
        (status, xml)
    })
}

fn extract_tag<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    xml.split(&open)
        .skip(1)
        .filter_map(|p| p.split(&close).next())
        .collect()
}

fn assert_list_buckets_query_map(query_map: &HashMap<String, String>) {
    let (status, xml) = call_list_buckets(query_map);
    let (status2, xml2) = call_list_buckets(query_map);
    assert_eq!(status, status2, "non-deterministic status for {:?}", query_map);
    assert_eq!(xml, xml2, "non-deterministic body for {:?}", query_map);

    if let Some(mb) = query_map.get("max-buckets") {
        let valid_mb = mb
            .parse::<i64>()
            .ok()
            .filter(|&n| (1..=10_000).contains(&n));
        if valid_mb.is_none() {
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "max-buckets={:?} outside 1..=10000 must return 400 error, never clamp (got {} with body:\n{})",
                mb,
                status,
                xml
            );
            assert!(
                xml.contains("<Code>InvalidArgument</Code>"),
                "expected InvalidArgument for max-buckets={:?}, got:\n{}",
                mb,
                xml
            );
            return;
        }
    }

    if let Some(tok) = query_map.get("continuation-token") {
        let valid_tok = hex_decode_strict(tok).and_then(|b| String::from_utf8(b).ok()).is_some();
        if !valid_tok {
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "invalid continuation-token={:?} must return 400 InvalidArgument (got {} with body:\n{})",
                tok,
                status,
                xml
            );
            assert!(xml.contains("<Code>InvalidArgument</Code>"));
            return;
        }
    }

    assert_eq!(status, StatusCode::OK, "valid query {:?} failed: {}", query_map, xml);
    if let Some(mb) = query_map.get("max-buckets") {
        let limit = mb.parse::<usize>().unwrap();
        let names = extract_tag(&xml, "Name");
        assert!(names.len() <= limit, "returned {} > max-buckets={}", names.len(), limit);
    }

    // If truncated, following the returned ContinuationToken must succeed deterministically.
    if let Some(&next_tok) = extract_tag(&xml, "ContinuationToken").first() {
        let mut next_q = query_map.clone();
        next_q.insert("continuation-token".to_string(), next_tok.to_string());
        let (next_status, next_xml) = call_list_buckets(&next_q);
        assert_eq!(
            next_status,
            StatusCode::OK,
            "returned continuation token {:?} was rejected: {}",
            next_tok,
            next_xml
        );
        let prev_names = extract_tag(&xml, "Name");
        let next_names = extract_tag(&next_xml, "Name");
        if let (Some(&last_prev), Some(&first_next)) = (prev_names.last(), next_names.first()) {
            assert!(
                last_prev < first_next,
                "pagination order broken: {:?} before {:?}",
                last_prev,
                first_next
            );
        }
    }
}

/// Oracle for `list_buckets_query`:
/// - `list_buckets` never panics on arbitrary `max-buckets`, `continuation-token`, `prefix`.
/// - `max-buckets` outside `1..=10000` is always a 400 `InvalidArgument` error, never clamped.
/// - Valid `continuation-token` values parse and page deterministically.
pub fn check_list_buckets_query(data: &[u8]) {
    let text = String::from_utf8_lossy(data);
    let mut query_map = HashMap::new();
    for pair in text.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            let dec_k = percent_decode(k);
            let dec_v = percent_decode(v);
            query_map.insert(dec_k.to_lowercase(), dec_v.clone());
            query_map.insert(dec_k, dec_v);
        }
    }
    assert_list_buckets_query_map(&query_map);

    if !text.contains('=') {
        let mut mb_only = HashMap::new();
        mb_only.insert("max-buckets".to_string(), text.as_ref().to_string());
        assert_list_buckets_query_map(&mb_only);

        let mut tok_only = HashMap::new();
        tok_only.insert("continuation-token".to_string(), text.as_ref().to_string());
        assert_list_buckets_query_map(&tok_only);
    }
}

fn percent_encode_bytes(bytes: &[u8], keep_slash: bool) -> String {
    let mut out = String::with_capacity(bytes.len() * 3);
    for &b in bytes {
        let unreserved = b.is_ascii_alphanumeric()
            || matches!(b, b'-' | b'_' | b'.' | b'~')
            || (keep_slash && b == b'/');
        if unreserved {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

/// Oracle for `raw_s3_key`:
/// - `raw_s3_key` and `percent_decode_path_bytes` never panic on arbitrary bytes or invalid UTF-8 / `%`.
/// - Percent-encoding arbitrary bytes and calling `percent_decode_path_bytes` (directly and via `raw_s3_key`)
///   returns the exact original bytes.
pub fn check_raw_s3_key(data: &[u8]) {
    let text = String::from_utf8_lossy(data);
    let direct = percent_decode_path_bytes(&text);
    assert!(direct.len() <= text.len());

    let mut parts = text.splitn(2, '\n');
    let uri_path = parts.next().unwrap_or("");
    let host_header = parts.next();

    let raw = raw_s3_key(uri_path, host_header);
    if let Some(raw_key) = raw {
        let decoded = percent_decode_path_bytes(raw_key);
        assert!(decoded.len() <= raw_key.len());
        match parse_s3_target(uri_path, host_header) {
            ParsedTarget::Object { key, .. } => {
                assert_eq!(String::from_utf8_lossy(&decoded).as_ref(), key.as_str());
            }
            other => panic!("raw_s3_key returned Some({:?}) but parse_s3_target returned {:?}", raw_key, other),
        }
    } else {
        assert!(!matches!(parse_s3_target(uri_path, host_header), ParsedTarget::Object { .. }));
    }

    // Round-trip arbitrary raw bytes through percent encoding + percent_decode_path_bytes + raw_s3_key.
    let encoded_all = percent_encode_bytes(data, false);
    assert_eq!(percent_decode_path_bytes(&encoded_all), data);

    // An empty key leaves `/mybucket/`: a single trailing slash names the bucket, not an object
    // with an empty key (#260, PR #284), so no key comes back.
    if data.is_empty() {
        assert_eq!(raw_s3_key("/mybucket/", None), None);
        assert_eq!(raw_s3_key("/mybucket/?partNumber=1&ignored=%20", None), None);
        assert!(matches!(parse_s3_target("/mybucket/", None), ParsedTarget::Bucket { .. }));
    } else {
        let path_uri = format!("/mybucket/{}", encoded_all);
        let extracted = raw_s3_key(&path_uri, None).expect("path-style URI must yield raw_s3_key");
        assert_eq!(percent_decode_path_bytes(extracted), data);

        let path_uri_with_query = format!("/mybucket/{}?partNumber=1&ignored=%20", encoded_all);
        let extracted_q = raw_s3_key(&path_uri_with_query, None).expect("query-bearing path must yield raw_s3_key");
        assert_eq!(percent_decode_path_bytes(extracted_q), data);

        let encoded_with_slash = percent_encode_bytes(data, true);
        let path_slash_uri = format!("/mybucket/{}", encoded_with_slash);
        let extracted_slash = raw_s3_key(&path_slash_uri, None).expect("path-style URI with literal slashes must yield raw_s3_key");
        assert_eq!(percent_decode_path_bytes(extracted_slash), data);
    }

    if !data.is_empty() {
        let vhost_uri = format!("/{}", encoded_all);
        let extracted_vh = raw_s3_key(&vhost_uri, Some("mybucket.s3.amazonaws.com"))
            .expect("vhost URI must yield raw_s3_key");
        assert_eq!(percent_decode_path_bytes(extracted_vh), data);
    }
}

fn canonical_operation_cases() -> &'static [(
    Operation,
    Method,
    Option<&'static str>,
    Option<&'static str>,
    &'static str,
    Option<&'static str>,
)] {
    &[
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

fn reorder_query(query: &str, sort_pairs: bool) -> String {
    let mut parts: Vec<&str> = query.split('&').filter(|p| !p.is_empty()).collect();
    if sort_pairs {
        parts.sort_unstable();
    } else {
        parts.reverse();
    }
    parts.join("&")
}

fn check_classify_one(method: &Method, bucket: Option<&str>, key: Option<&str>, query: &str, hdrs: &HeaderMap) {
    let r1 = classify(method, bucket, key, query, hdrs);
    let rev_q = reorder_query(query, false);
    let sort_q = reorder_query(query, true);
    let r2 = classify(method, bucket, key, &rev_q, hdrs);
    let r3 = classify(method, bucket, key, &sort_q, hdrs);
    assert_eq!(
        r1, r2,
        "classify depended on query parameter order for {} {:?}/{:?} ?{:?} vs ?{:?}",
        method, bucket, key, query, rev_q
    );
    assert_eq!(
        r1, r3,
        "classify depended on query parameter sort order for {} {:?}/{:?} ?{:?} vs ?{:?}",
        method, bucket, key, query, sort_q
    );
}

/// Oracle for `classify` (#148, #448):
/// - Every input yields an `Operation` or `ClassifyRefusal` without panicking.
/// - Reordering/sorting query parameters produces the identical classification.
/// - Every `Operation` in `OPS` round-trips from its canonical request.
pub fn check_classify(data: &[u8]) {
    static CANONICAL_OK: OnceLock<()> = OnceLock::new();
    CANONICAL_OK.get_or_init(|| {
        let mut seen = Vec::new();
        for (expected, method, bucket, key, query, copy) in canonical_operation_cases() {
            let mut hdrs = HeaderMap::new();
            if let Some(c) = copy {
                hdrs.insert("x-amz-copy-source", HeaderValue::from_static(c));
            }
            if *expected == Operation::PostObject {
                hdrs.insert(CONTENT_TYPE, HeaderValue::from_static("multipart/form-data; boundary=x"));
            }
            let got = classify(method, *bucket, *key, query, &hdrs)
                .unwrap_or_else(|e| panic!("canonical {:?} refused: {:?}", expected, e));
            assert_eq!(got, *expected, "canonical mismatch for {:?}", expected);
            check_classify_one(method, *bucket, *key, query, &hdrs);
            if !seen.contains(&got) {
                seen.push(got);
            }
        }
        assert_eq!(seen.len(), OPS.len(), "every Operation variant in OPS must be covered");
    });

    let text = String::from_utf8_lossy(data);
    // Support structured multi-field seeds (`METHOD\nbucket\nkey\nheaders\nquery`) AND raw query strings.
    let raw_query = if let Some(rest) = text.strip_prefix("REQ:") {
        let mut lines = rest.splitn(5, '\n');
        let m_str = lines.next().unwrap_or("GET");
        let b_str = lines.next().unwrap_or("");
        let k_str = lines.next().unwrap_or("");
        let h_flags = lines.next().unwrap_or("");
        let q = lines.next().unwrap_or("");
        let method = Method::from_bytes(m_str.as_bytes()).unwrap_or(Method::GET);
        let bucket = (!b_str.is_empty()).then_some(b_str);
        let key = (!k_str.is_empty()).then_some(k_str);
        let mut hdrs = HeaderMap::new();
        if h_flags.contains('C') {
            hdrs.insert("x-amz-copy-source", HeaderValue::from_static("/src/key"));
        }
        if h_flags.contains('F') {
            hdrs.insert(CONTENT_TYPE, HeaderValue::from_static("multipart/form-data; boundary=x"));
        }
        check_classify_one(&method, bucket, key, q, &hdrs);
        q
    } else {
        text.as_ref()
    };

    let methods = [Method::GET, Method::PUT, Method::POST, Method::DELETE, Method::HEAD, Method::OPTIONS];
    let selector = data.first().copied().unwrap_or(0) as usize;
    let bucket_opts = [None, Some("bkt"), Some("")];
    let key_opts = [None, Some("obj"), Some("")];
    let b = bucket_opts[(selector >> 3) % bucket_opts.len()];
    let k = key_opts[(selector >> 5) % key_opts.len()];
    let mut hdrs = HeaderMap::new();
    if selector & 0x40 != 0 {
        hdrs.insert("x-amz-copy-source", HeaderValue::from_static("/src/k"));
    }
    if selector & 0x80 != 0 {
        hdrs.insert(CONTENT_TYPE, HeaderValue::from_static("multipart/form-data; boundary=x"));
    }
    for m in &methods {
        check_classify_one(m, b, k, raw_query, &hdrs);
    }
    // Also exercise explicit arbitrary-order queries with PUT object + headers so sub-resource order bugs fail fast.
    let mut empty_hdrs = HeaderMap::new();
    check_classify_one(&Method::PUT, Some("bkt"), Some("obj"), raw_query, &empty_hdrs);
    empty_hdrs.insert("x-amz-copy-source", HeaderValue::from_static("/src/k"));
    check_classify_one(&Method::PUT, Some("bkt"), Some("obj"), raw_query, &empty_hdrs);
    let _: Result<Operation, ClassifyRefusal> = classify(&Method::GET, Some("bkt"), None, raw_query, &HeaderMap::new());
}

#[cfg(test)]
mod tests {
    use super::{check_checksum_headers, check_classify, check_list_buckets_query, check_raw_s3_key};

    #[test]
    fn checksum_headers_canonical_vectors_and_invalid_inputs() {
        for sample in [
            b"Qeh8oXvGiSo=".as_slice(),
            b"arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=".as_slice(),
            b"rosUhgp5mIg=".as_slice(),
            b"FeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiU=".as_slice(),
            b"1B2M2Y8AsgTpgAmY7PhCfg==".as_slice(),
            b"XUFAKrxLKna5cZ2REBfFkg==".as_slice(),
            b"bad!".as_slice(),
            b"AAAA".as_slice(),
            b"".as_slice(),
            &[0xFF, 0x00, b'\n', b'A'],
        ] {
            check_checksum_headers(sample);
        }
    }

    #[test]
    fn checksum_headers_rejects_non_canonical_pad_bits_round_trip() {
        check_checksum_headers(b"Qeh8oXvGiSp=");
        check_checksum_headers(b"arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+1=");
        check_checksum_headers(b"rpIg5gm=");
        check_checksum_headers(b"1B2M2Y8AsgTpgAmY7PhCfh==");
        check_checksum_headers(b"XUFAKrxLKna5cZ2REBfFkh==");
    }

    #[test]
    fn checksum_headers_corpus_seeds_hold_oracle() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/corpus/checksum_headers");
        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .expect("read checksum_headers corpus dir")
            .map(|e| e.expect("corpus dir entry").path())
            .collect();
        entries.sort();
        assert!(!entries.is_empty());
        for path in entries {
            let bytes = std::fs::read(&path).expect("read corpus seed");
            check_checksum_headers(&bytes);
        }
    }

    #[test]
    fn list_buckets_query_valid_and_invalid_bounds() {
        for sample in [
            b"max-buckets=1".as_slice(),
            b"max-buckets=10000".as_slice(),
            b"max-buckets=0".as_slice(),
            b"max-buckets=-1".as_slice(),
            b"max-buckets=abc".as_slice(),
            b"max-buckets=".as_slice(),
            b"continuation-token=zzz".as_slice(),
            b"continuation-token=626b742d61".as_slice(),
            b"max-buckets=1&continuation-token=626b742d61".as_slice(),
            b"".as_slice(),
        ] {
            check_list_buckets_query(sample);
        }
    }

    #[test]
    fn list_buckets_query_rejects_max_buckets_above_10000_never_clamps() {
        check_list_buckets_query(b"max-buckets=10001");
        check_list_buckets_query(b"max-buckets=2147483647");
    }

    #[test]
    fn list_buckets_query_rejects_non_hex_continuation_token_plus_prefix() {
        check_list_buckets_query(b"continuation-token=%2Ba");
    }

    #[test]
    fn raw_s3_key_percent_round_trip_and_unreadable_bytes() {
        for sample in [
            b"/uri-bkt/%AE%8A-".as_slice(),
            b"/uri-bkt/%C2%AE%C2%8A-".as_slice(),
            b"/uri-bkt/caf%C3%A9-%C2%AE".as_slice(),
            b"/bkt/a+b%20c%25%2F?partNumber=1".as_slice(),
            b"/%AE%8A-\nmybkt.s3.amazonaws.com".as_slice(),
            b"/bkt/%".as_slice(),
            b"/bkt/".as_slice(),
            b"/bkt".as_slice(),
            b"".as_slice(),
            &[0x00, 0x20, 0x25, 0x2B, 0x2F, 0x3F, 0xAE, 0x8A, 0xFF],
        ] {
            check_raw_s3_key(sample);
        }

        // Deterministic 10,000-case sweep over raw byte inputs, percent escapes, and vhost lines.
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let interesting: &[u8] = b"/%0123456789ABCDEFabcdef+?\n.localhosts3amazonawscom\xae\x8a\xff\x00 ";
        for i in 0..10_000 {
            let len = (next() % 96) as usize;
            let mut buf = Vec::with_capacity(len);
            for _ in 0..len {
                if i % 2 == 0 {
                    buf.push((next() & 0xff) as u8);
                } else {
                    buf.push(interesting[(next() as usize) % interesting.len()]);
                }
            }
            check_raw_s3_key(&buf);
        }
    }

    #[test]
    fn classify_canonical_operations_and_corpus_seeds() {
        for sample in [
            b"acl&versioning".as_slice(),
            b"=%ZZ".as_slice(),
            b"uploadId=1&partNumber=0".as_slice(),
            b"list-type=3".as_slice(),
            b"REQ:PUT\nbkt\nobj\n\nuploadId=1&partNumber=0".as_slice(),
        ] {
            check_classify(sample);
        }

        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/corpus/classify");
        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .expect("read classify corpus dir")
            .map(|e| e.expect("corpus dir entry").path())
            .collect();
        entries.sort();
        assert!(entries.len() >= 48, "expected canonical + regression seeds, got {}", entries.len());
        for path in entries {
            let bytes = std::fs::read(&path).expect("read corpus seed");
            check_classify(&bytes);
        }
    }
}
