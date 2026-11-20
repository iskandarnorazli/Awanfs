//! Shared oracle checks for Issue #271 fuzz targets (`checksum_headers`,
//! `list_buckets_query`, `raw_s3_key`), called by both `fuzz_targets/*.rs` and unit tests.

use http_body_util::BodyExt;
use hyper::header::{HeaderMap, HeaderValue};
use hyper::StatusCode;
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
/// - `requested_checksum` and `base64_decode` never panic on arbitrary bytes.
/// - Decoding a valid base64 checksum and encoding it again gives the input.
/// - Valid 8-byte (CRC64NVME) and 32-byte (SHA256) base64 digests round-trip through `requested_checksum`.
pub fn check_checksum_headers(data: &[u8]) {
    if let Ok(s) = std::str::from_utf8(data) {
        if let Some(raw) = base64_decode(s) {
            assert_eq!(
                base64_encode(&raw),
                s,
                "base64_decode accepted non-canonical input {:?}",
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
        let resp = list_buckets(state, query_map)
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

#[cfg(test)]
mod tests {
    use super::{check_checksum_headers, check_list_buckets_query, check_raw_s3_key};

    #[test]
    fn checksum_headers_canonical_vectors_and_invalid_inputs() {
        for sample in [
            b"Qeh8oXvGiSo=".as_slice(),
            b"arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=".as_slice(),
            b"rosUhgp5mIg=".as_slice(),
            b"FeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiU=".as_slice(),
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
}
