//! Differential property tests and edge-rule tests comparing AwanFS's SigV4
//! verification (`server/src/auth.rs`, plus end-to-end HTTP checks) against
//! AWS's official Rust signer (`aws-sigv4`).
//!
//! Existing edge-rule coverage in `server/tests/` before this file (#279):
//! 1. 15-minute clock-skew limit:
//!    - `auth_test.rs::unknown_access_key_and_skewed_clock_are_refused` checks `now() + 3600`
//!      (1 hour, not the exact 900 s / 901 s boundary).
//!    - `multipart_sigv4_test.rs::test_sigv4_presigned_url_and_skew_checks` checks `now - 1000`
//!      against legacy `validate_sigv4_auth`, not `auth::authenticate`.
//! 2. Presigned expiry, including `X-Amz-Expires` above 7 days refused:
//!    - `auth_test.rs::aws_documented_presigned_url_is_accepted_until_it_expires` checks
//!      `X-Amz-Expires=86400` at `now + 60` (`Ok`) and `now + 86_401` (`AccessDenied`).
//!      The 7-day cap (`604_800` vs `604_801`) had no acceptance/refusal test.
//! 3. `x-amz-content-sha256` not matching the body:
//!    - `auth_test.rs::a_body_must_match_its_declared_sha256` (`payload_hash_ok`),
//!      `auth_test.rs::a_put_whose_body_does_not_match_its_hash_is_refused` (buffered PUT -> 400),
//!      `streaming_put_test.rs::a_streamed_body_with_a_wrong_hash_or_md5_is_refused_and_not_stored` (>4 MiB PUT -> 400).
//! 4. `UNSIGNED-PAYLOAD`:
//!    - `auth_test.rs::a_body_must_match_its_declared_sha256` checks `payload_hash_ok` only;
//!      no test verified `aws-sigv4`-signed `UNSIGNED-PAYLOAD` header requests end to end.
//! 5. Streaming with trailing checksums (`STREAMING-UNSIGNED-PAYLOAD-TRAILER`,
//!    `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`):
//!    - `chunk_signature_test.rs::an_unsigned_trailer_body_is_decoded_not_stored_with_its_framing`
//!      covers `STREAMING-UNSIGNED-PAYLOAD-TRAILER` anonymously.
//!    - `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` (signed streaming with trailing checksums)
//!      had no test.

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    sign, PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest,
    SignatureLocation, SigningSettings, UriPathNormalizationMode,
};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use hyper::header::{HeaderMap, HeaderName, HeaderValue};
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, RngAlgorithm, TestRng, TestRunner};
use reqwest::StatusCode;
use s3_server::{
    authenticate, payload_hash_ok, sha256_hex, start_s3_reactor, AuthConfig, S3ServiceState,
    SigV4KeyCache,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const AK: &str = "AKIAIOSFODNN7EXAMPLE";
const SK: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";
const SERVICE: &str = "s3";
// Fixed signing timestamp: 2026-11-19T00:00:00Z
const SIGN_EPOCH: u64 = 1_795_046_400;

fn auth_cfg() -> AuthConfig {
    AuthConfig {
        credentials: Some((AK.to_string(), SK.to_string())),
        allow_anonymous: false,
    }
}

fn aws_identity() -> Identity {
    Credentials::new(AK, SK, None, None, "sigv4-diff-test").into()
}

fn s3_signing_settings(location: SignatureLocation, expires: Option<u64>) -> SigningSettings {
    let mut settings = SigningSettings::default();
    settings.percent_encoding_mode = PercentEncodingMode::Single;
    settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
    settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
    settings.signature_location = location;
    settings.expires_in = expires.map(Duration::from_secs);
    settings
}

fn uri_encode_segment(s: &str, encode_plus: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric()
            || matches!(b, b'-' | b'_' | b'.' | b'~')
            || (!encode_plus && b == b'+')
        {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

fn encode_s3_key_for_uri(raw_key: &str, encode_plus: bool) -> String {
    raw_key
        .split('/')
        .map(|seg| uri_encode_segment(seg, encode_plus))
        .collect::<Vec<_>>()
        .join("/")
}

fn encode_query_component(s: &str, use_plus_for_space: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b == b' ' && use_plus_for_space {
            out.push('+');
        } else if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

#[derive(Clone, Debug)]
enum QuerySpec {
    KeyValue {
        key: String,
        val: String,
        plus_space: bool,
    },
    EmptyVal {
        key: String,
        with_eq: bool,
    },
}

#[derive(Clone, Debug)]
struct GeneratedRequest {
    method: &'static str,
    bucket: String,
    key: String,
    encode_plus_in_path: bool,
    queries: Vec<QuerySpec>,
    extra_headers: Vec<(String, String)>,
    body: Vec<u8>,
    unsigned_payload: bool,
    presigned_expires: Option<u64>,
}

fn arb_bucket() -> impl Strategy<Value = String> {
    prop::string::string_regex("[a-z0-9][a-z0-9-]{1,20}[a-z0-9]").unwrap()
}

fn arb_key() -> impl Strategy<Value = String> {
    let seg_char = prop_oneof![
        Just('a'),
        Just('Z'),
        Just('0'),
        Just(' '),
        Just('+'),
        Just('%'),
        Just('é'),
        Just('ሴ'),
        Just('🦀'),
        Just('.'),
        Just('-'),
        Just('_'),
        Just('~'),
    ];
    let normal_seg = prop::collection::vec(seg_char, 1..=10)
        .prop_map(|cs| cs.into_iter().collect::<String>());
    let segment = prop_oneof![
        4 => normal_seg,
        1 => Just(String::new()),
        1 => Just(".".to_string()),
        1 => Just("..".to_string()),
    ];
    prop::collection::vec(segment, 1..=4).prop_map(|segs| segs.join("/"))
}

fn arb_query_token() -> impl Strategy<Value = String> {
    let ch = prop_oneof![
        Just('k'),
        Just('v'),
        Just('1'),
        Just(' '),
        Just('+'),
        Just('%'),
        Just('='),
        Just('&'),
        Just('é'),
        Just('ሴ'),
        Just('-'),
        Just('_'),
        Just('.'),
        Just('~'),
    ];
    prop::collection::vec(ch, 1..=8).prop_map(|cs| cs.into_iter().collect::<String>())
}

fn arb_queries() -> impl Strategy<Value = Vec<QuerySpec>> {
    let item = prop_oneof![
        (arb_query_token(), arb_query_token(), any::<bool>()).prop_map(|(k, v, plus_space)| {
            QuerySpec::KeyValue {
                key: format!("q-{}", k),
                val: v,
                plus_space,
            }
        }),
        // Repeated key and empty key
        (arb_query_token(), any::<bool>()).prop_map(|(v, plus_space)| QuerySpec::KeyValue {
            key: "dup".to_string(),
            val: v,
            plus_space,
        }),
        (arb_query_token(), any::<bool>()).prop_map(|(v, plus_space)| QuerySpec::KeyValue {
            key: "".to_string(),
            val: v,
            plus_space,
        }),
        (arb_query_token(), any::<bool>()).prop_map(|(k, with_eq)| QuerySpec::EmptyVal {
            key: format!("q-{}", k),
            with_eq,
        }),
    ];
    prop::collection::vec(item, 0..=5)
}

fn arb_extra_headers() -> impl Strategy<Value = Vec<(String, String)>> {
    let header_names = prop_oneof![
        Just("X-Amz-Meta-Foo".to_string()),
        Just("x-amz-meta-foo".to_string()),
        Just("X-AMZ-META-BAR".to_string()),
        Just("X-Custom-One".to_string()),
        Just("x-custom-two".to_string()),
        Just("Content-Type".to_string()),
        Just("Cache-Control".to_string()),
    ];
    let val_piece = prop_oneof![
        Just("alpha"),
        Just("BETA"),
        Just("123"),
        Just(" "),
        Just("  "),
        Just("\t"),
        Just("a,b"),
        Just("quoted \"x\""),
    ];
    let header_val = prop::collection::vec(val_piece, 1..=4)
        .prop_map(|parts| parts.concat())
        .prop_filter("non-empty header value", |s| !s.trim().is_empty());
    prop::collection::vec((header_names, header_val), 0..=6)
}

fn arb_request() -> impl Strategy<Value = GeneratedRequest> {
    let methods = prop_oneof![
        Just("GET"),
        Just("PUT"),
        Just("HEAD"),
        Just("DELETE"),
        Just("POST"),
    ];
    (
        methods,
        arb_bucket(),
        arb_key(),
        any::<bool>(),
        arb_queries(),
        arb_extra_headers(),
        prop::collection::vec(any::<u8>(), 0..=128),
        any::<bool>(),
        prop_oneof![Just(None), (1u64..=604_800).prop_map(Some)],
    )
        .prop_map(
            |(
                method,
                bucket,
                key,
                encode_plus_in_path,
                queries,
                extra_headers,
                body,
                unsigned_payload,
                presigned_expires,
            )| GeneratedRequest {
                method,
                bucket,
                key,
                encode_plus_in_path,
                queries,
                extra_headers,
                body,
                unsigned_payload,
                presigned_expires,
            },
        )
}

fn build_raw_query(queries: &[QuerySpec]) -> String {
    queries
        .iter()
        .map(|q| match q {
            QuerySpec::KeyValue {
                key,
                val,
                plus_space,
            } => format!(
                "{}={}",
                encode_query_component(key, false),
                encode_query_component(val, *plus_space)
            ),
            QuerySpec::EmptyVal { key, with_eq } => {
                let ek = encode_query_component(key, false);
                if *with_eq {
                    format!("{}=", ek)
                } else {
                    ek
                }
            }
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn sign_with_aws(
    req: &GeneratedRequest,
    sign_epoch: u64,
) -> (String, String, HeaderMap) {
    let encoded_key = encode_s3_key_for_uri(&req.key, req.encode_plus_in_path);
    let raw_path = format!("/{}/{}", req.bucket, encoded_key);
    let raw_query = build_raw_query(&req.queries);
    let uri = if raw_query.is_empty() {
        format!("https://example.s3.amazonaws.com{}", raw_path)
    } else {
        format!("https://example.s3.amazonaws.com{}?{}", raw_path, raw_query)
    };

    let identity = aws_identity();
    let (location, expires) = match req.presigned_expires {
        Some(exp) => (SignatureLocation::QueryParams, Some(exp)),
        None => (SignatureLocation::Headers, None),
    };
    let settings = s3_signing_settings(location, expires);
    let sign_time = UNIX_EPOCH + Duration::from_secs(sign_epoch);
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region(REGION)
        .name(SERVICE)
        .time(sign_time)
        .settings(settings)
        .build()
        .unwrap()
        .into();

    let signable_body = if req.presigned_expires.is_some() || req.unsigned_payload {
        SignableBody::UnsignedPayload
    } else {
        SignableBody::Bytes(&req.body)
    };

    let header_pairs: Vec<(&str, &str)> = req
        .extra_headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    let signable = SignableRequest::new(
        req.method,
        uri.as_str(),
        header_pairs.into_iter(),
        signable_body,
    )
    .unwrap();

    let (instructions, _sig) = sign(signable, &params).unwrap().into_parts();

    let mut http_req = http::Request::builder()
        .method(req.method)
        .uri(&uri)
        .body(())
        .unwrap();
    for (k, v) in &req.extra_headers {
        let name = HeaderName::from_bytes(k.as_bytes()).unwrap();
        let val = HeaderValue::from_str(v).unwrap();
        http_req.headers_mut().append(name, val);
    }
    instructions.apply_to_request_http1x(&mut http_req);
    if !http_req.headers().contains_key("host") {
        http_req
            .headers_mut()
            .insert("host", HeaderValue::from_static("example.s3.amazonaws.com"));
    }

    let final_path = http_req.uri().path().to_string();
    let final_query = http_req.uri().query().unwrap_or("").to_string();

    let mut hyper_headers = HeaderMap::new();
    for (k, v) in http_req.headers().iter() {
        let hk = hyper::header::HeaderName::from_bytes(k.as_str().as_bytes()).unwrap();
        let hv = hyper::header::HeaderValue::from_bytes(v.as_bytes()).unwrap();
        hyper_headers.append(hk, hv);
    }
    (final_path, final_query, hyper_headers)
}

fn flip_one_byte_in_body(body: &[u8]) -> Vec<u8> {
    let mut out = body.to_vec();
    if out.is_empty() {
        out.push(0x42);
    } else {
        out[0] ^= 0x01;
    }
    out
}

fn replace_query_param(query: &str, name: &str, new_val: &str) -> String {
    query
        .split('&')
        .map(|pair| {
            if let Some((k, _)) = pair.split_once('=') {
                if k == name {
                    return format!("{}={}", k, new_val);
                }
            }
            pair.to_string()
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn amz_date_from_epoch(epoch: u64) -> String {
    let clf = s3_server::request_log::clf_time(epoch);
    let months = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let m = months.iter().position(|x| *x == &clf[3..6]).unwrap() + 1;
    format!(
        "{}{:02}{}T{}{}{}Z",
        &clf[7..11],
        m,
        &clf[0..2],
        &clf[12..14],
        &clf[15..17],
        &clf[18..20]
    )
}

#[test]
fn differential_proptest_accepts_aws_sigv4_and_refuses_mutations() {
    let cfg = auth_cfg();
    let cache = SigV4KeyCache::new();
    let config = ProptestConfig {
        cases: 256,
        max_shrink_iters: 1024,
        ..ProptestConfig::default()
    };
    let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &[0x27u8; 32]);
    let mut runner = TestRunner::new_with_rng(config, rng);

    let res = runner.run(&arb_request(), |req| {
        let (path, query, headers) = sign_with_aws(&req, SIGN_EPOCH);

        // 1. Valid request signed by aws-sigv4 must be accepted.
        let got = authenticate(&cfg, &cache, req.method, &path, &query, &headers, SIGN_EPOCH);
        prop_assert_eq!(got, Ok(()), "AwanFS refused valid aws-sigv4 request: path={:?} query={:?} headers={:?}", path, query, headers);

        if req.presigned_expires.is_none() && !req.unsigned_payload {
            prop_assert!(payload_hash_ok(&headers, &req.body));
            // Mutate body -> payload_hash_ok must refuse (XAmzContentSHA256Mismatch on server).
            let tampered_body = flip_one_byte_in_body(&req.body);
            prop_assert!(!payload_hash_ok(&headers, &tampered_body));
        }

        // 2. Mutate a signed header value -> SignatureDoesNotMatch.
        let mut h_tampered = headers.clone();
        h_tampered.insert("host", HeaderValue::from_static("tampered.s3.amazonaws.com"));
        prop_assert_eq!(
            authenticate(&cfg, &cache, req.method, &path, &query, &h_tampered, SIGN_EPOCH),
            Err("SignatureDoesNotMatch")
        );

        // 3. Mutate path -> SignatureDoesNotMatch.
        let bad_path = format!("{}-x", path);
        prop_assert_eq!(
            authenticate(&cfg, &cache, req.method, &bad_path, &query, &headers, SIGN_EPOCH),
            Err("SignatureDoesNotMatch")
        );

        // 4. Mutate a query value -> SignatureDoesNotMatch.
        let bad_query = if req.presigned_expires.is_some() {
            let exp = req.presigned_expires.unwrap();
            let new_exp = if exp == 1 { 2 } else { exp - 1 };
            replace_query_param(&query, "X-Amz-Expires", &new_exp.to_string())
        } else if query.is_empty() {
            "tampered=1".to_string()
        } else {
            format!("{}&tampered=1", query)
        };
        prop_assert_eq!(
            authenticate(&cfg, &cache, req.method, &path, &bad_query, &headers, SIGN_EPOCH),
            Err("SignatureDoesNotMatch")
        );

        // 5. Mutate date by +1s (within 900s skew and expiry) -> SignatureDoesNotMatch.
        let bumped_date = amz_date_from_epoch(SIGN_EPOCH + 1);
        if req.presigned_expires.is_some() {
            let bad_date_query = replace_query_param(&query, "X-Amz-Date", &bumped_date);
            prop_assert_eq!(
                authenticate(&cfg, &cache, req.method, &path, &bad_date_query, &headers, SIGN_EPOCH),
                Err("SignatureDoesNotMatch")
            );
        } else {
            let mut h_date = headers.clone();
            h_date.insert("x-amz-date", HeaderValue::from_str(&bumped_date).unwrap());
            prop_assert_eq!(
                authenticate(&cfg, &cache, req.method, &path, &query, &h_date, SIGN_EPOCH),
                Err("SignatureDoesNotMatch")
            );
        }

        // 6. Server clock skew / presigned expiry codes.
        if let Some(exp) = req.presigned_expires {
            prop_assert_eq!(
                authenticate(&cfg, &cache, req.method, &path, &query, &headers, SIGN_EPOCH + exp + 1),
                Err("AccessDenied")
            );
            prop_assert_eq!(
                authenticate(&cfg, &cache, req.method, &path, &query, &headers, SIGN_EPOCH - 901),
                Err("RequestTimeTooSkewed")
            );
        } else {
            prop_assert_eq!(
                authenticate(&cfg, &cache, req.method, &path, &query, &headers, SIGN_EPOCH + 901),
                Err("RequestTimeTooSkewed")
            );
            prop_assert_eq!(
                authenticate(&cfg, &cache, req.method, &path, &query, &headers, SIGN_EPOCH - 901),
                Err("RequestTimeTooSkewed")
            );
        }

        Ok(())
    });

    if let Err(e) = res {
        panic!("differential proptest failed: {:?}", e);
    }
}

async fn spawn_server(auth: AuthConfig) -> (Arc<S3ServiceState>, String, s3_server::ServerHandle) {
    let mut state = S3ServiceState::new();
    state.auth = auth;
    let state = Arc::new(state);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    (state, url, handle)
}

fn sign_live_http(
    method: &str,
    url: &str,
    extra_headers: &[(&str, &str)],
    body: SignableBody<'_>,
    location: SignatureLocation,
    expires: Option<u64>,
    now: SystemTime,
) -> (String, HeaderMap) {
    let identity = aws_identity();
    let settings = s3_signing_settings(location, expires);
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region(REGION)
        .name(SERVICE)
        .time(now)
        .settings(settings)
        .build()
        .unwrap()
        .into();

    let signable = SignableRequest::new(
        method,
        url,
        extra_headers.iter().copied(),
        body,
    )
    .unwrap();
    let (instructions, _sig) = sign(signable, &params).unwrap().into_parts();

    let mut req = http::Request::builder()
        .method(method)
        .uri(url)
        .body(())
        .unwrap();
    for (k, v) in extra_headers {
        req.headers_mut().append(
            HeaderName::from_bytes(k.as_bytes()).unwrap(),
            HeaderValue::from_str(v).unwrap(),
        );
    }
    instructions.apply_to_request_http1x(&mut req);

    let mut hyper_headers = HeaderMap::new();
    for (k, v) in req.headers() {
        hyper_headers.append(
            hyper::header::HeaderName::from_bytes(k.as_str().as_bytes()).unwrap(),
            hyper::header::HeaderValue::from_bytes(v.as_bytes()).unwrap(),
        );
    }
    (req.uri().to_string(), hyper_headers)
}

fn to_reqwest_headers(hm: &HeaderMap) -> reqwest::header::HeaderMap {
    let mut out = reqwest::header::HeaderMap::new();
    for (k, v) in hm.iter() {
        out.append(
            reqwest::header::HeaderName::from_bytes(k.as_str().as_bytes()).unwrap(),
            reqwest::header::HeaderValue::from_bytes(v.as_bytes()).unwrap(),
        );
    }
    out
}

#[tokio::test]
async fn differential_http_error_codes_match_aws_s3() {
    let (state, base_url, _handle) = spawn_server(auth_cfg()).await;
    let client = reqwest::Client::new();
    let now = SystemTime::now();

    // Create bucket using aws-sigv4.
    let bucket_url = format!("{}/diff-http-bkt", base_url);
    let (signed_url, hdrs) = sign_live_http(
        "PUT",
        &bucket_url,
        &[],
        SignableBody::Bytes(&[]),
        SignatureLocation::Headers,
        None,
        now,
    );
    let res = client
        .put(&signed_url)
        .headers(to_reqwest_headers(&hdrs))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // PUT object signed with SHA-256 of body, then send tampered body -> 400 XAmzContentSHA256Mismatch.
    let obj_url = format!("{}/diff-http-bkt/item%20one%2Btwo.txt", base_url);
    let body = b"authentic-payload";
    let (signed_url, hdrs) = sign_live_http(
        "PUT",
        &obj_url,
        &[],
        SignableBody::Bytes(body),
        SignatureLocation::Headers,
        None,
        now,
    );
    let mut bad_body = body.to_vec();
    bad_body[0] ^= 0xff;
    let res = client
        .put(&signed_url)
        .headers(to_reqwest_headers(&hdrs))
        .body(bad_body)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let xml = res.text().await.unwrap();
    assert!(xml.contains("<Code>XAmzContentSHA256Mismatch</Code>"), "{}", xml);

    // Valid PUT succeeds.
    let res = client
        .put(&signed_url)
        .headers(to_reqwest_headers(&hdrs))
        .body(body.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // Tampered header value -> 403 SignatureDoesNotMatch.
    let (signed_url, mut hdrs) = sign_live_http(
        "GET",
        &obj_url,
        &[("x-amz-meta-tag", "orig")],
        SignableBody::Bytes(&[]),
        SignatureLocation::Headers,
        None,
        now,
    );
    hdrs.insert("x-amz-meta-tag", HeaderValue::from_static("tampered"));
    let res = client
        .get(&signed_url)
        .headers(to_reqwest_headers(&hdrs))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    let xml = res.text().await.unwrap();
    assert!(xml.contains("<Code>SignatureDoesNotMatch</Code>"), "{}", xml);

    // Valid header containing HTAB ('\t') signed by aws-sigv4 must also be accepted over HTTP.
    let (tab_url, tab_hdrs) = sign_live_http(
        "GET",
        &obj_url,
        &[("x-amz-meta-tab", "a\tb")],
        SignableBody::Bytes(&[]),
        SignatureLocation::Headers,
        None,
        now,
    );
    let res = client
        .get(&tab_url)
        .headers(to_reqwest_headers(&tab_hdrs))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK, "HTAB header rejected: {}", res.text().await.unwrap());

    // Skewed request (> 15 min) -> 403 RequestTimeTooSkewed.
    let skewed_time = now - Duration::from_secs(905);
    let (signed_url, hdrs) = sign_live_http(
        "GET",
        &obj_url,
        &[],
        SignableBody::Bytes(&[]),
        SignatureLocation::Headers,
        None,
        skewed_time,
    );
    let res = client
        .get(&signed_url)
        .headers(to_reqwest_headers(&hdrs))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    let xml = res.text().await.unwrap();
    assert!(xml.contains("<Code>RequestTimeTooSkewed</Code>"), "{}", xml);

    // Expired presigned URL -> 403 AccessDenied.
    let expired_time = now - Duration::from_secs(120);
    let (presigned_url, _) = sign_live_http(
        "GET",
        &obj_url,
        &[],
        SignableBody::UnsignedPayload,
        SignatureLocation::QueryParams,
        Some(60),
        expired_time,
    );
    let res = client.get(&presigned_url).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    let xml = res.text().await.unwrap();
    assert!(xml.contains("<Code>AccessDenied</Code>"), "{}", xml);

    drop(state);
}

// =========================================================================
// Edge rules (Requirement 3): one dedicated test per rule
// =========================================================================

#[test]
fn edge_1_clock_skew_15_minute_limit_exact_boundary() {
    let cfg = auth_cfg();
    let cache = SigV4KeyCache::new();
    let req = GeneratedRequest {
        method: "GET",
        bucket: "skew-bkt".into(),
        key: "obj".into(),
        encode_plus_in_path: true,
        queries: vec![],
        extra_headers: vec![],
        body: vec![],
        unsigned_payload: false,
        presigned_expires: None,
    };
    let (path, query, headers) = sign_with_aws(&req, SIGN_EPOCH);

    assert_eq!(authenticate(&cfg, &cache, "GET", &path, &query, &headers, SIGN_EPOCH), Ok(()));
    assert_eq!(authenticate(&cfg, &cache, "GET", &path, &query, &headers, SIGN_EPOCH + 900), Ok(()));
    assert_eq!(authenticate(&cfg, &cache, "GET", &path, &query, &headers, SIGN_EPOCH - 900), Ok(()));
    assert_eq!(
        authenticate(&cfg, &cache, "GET", &path, &query, &headers, SIGN_EPOCH + 901),
        Err("RequestTimeTooSkewed")
    );
    assert_eq!(
        authenticate(&cfg, &cache, "GET", &path, &query, &headers, SIGN_EPOCH - 901),
        Err("RequestTimeTooSkewed")
    );
}

#[test]
fn edge_2_presigned_expiry_and_seven_day_maximum() {
    let cfg = auth_cfg();
    let cache = SigV4KeyCache::new();
    let seven_days = 7 * 24 * 60 * 60;
    let req_7d = GeneratedRequest {
        method: "GET",
        bucket: "exp-bkt".into(),
        key: "obj".into(),
        encode_plus_in_path: true,
        queries: vec![],
        extra_headers: vec![],
        body: vec![],
        unsigned_payload: true,
        presigned_expires: Some(seven_days),
    };
    let (path, query, headers) = sign_with_aws(&req_7d, SIGN_EPOCH);
    assert_eq!(
        authenticate(&cfg, &cache, "GET", &path, &query, &headers, SIGN_EPOCH + seven_days),
        Ok(())
    );
    assert_eq!(
        authenticate(&cfg, &cache, "GET", &path, &query, &headers, SIGN_EPOCH + seven_days + 1),
        Err("AccessDenied")
    );

    // X-Amz-Expires > 7 days (604801) signed with aws-sigv4 must be refused.
    let req_over_7d = GeneratedRequest {
        presigned_expires: Some(seven_days + 1),
        ..req_7d
    };
    let (path2, query2, headers2) = sign_with_aws(&req_over_7d, SIGN_EPOCH);
    assert_eq!(
        authenticate(&cfg, &cache, "GET", &path2, &query2, &headers2, SIGN_EPOCH),
        Err("AuthorizationQueryParametersError")
    );
}

#[tokio::test]
async fn edge_3_content_sha256_mismatch_refused_with_aws_sigv4() {
    let (_state, base_url, _handle) = spawn_server(auth_cfg()).await;
    let client = reqwest::Client::new();
    let now = SystemTime::now();

    let (b_url, b_hdrs) = sign_live_http(
        "PUT",
        &format!("{}/sha-bkt", base_url),
        &[],
        SignableBody::Bytes(&[]),
        SignatureLocation::Headers,
        None,
        now,
    );
    assert_eq!(client.put(&b_url).headers(to_reqwest_headers(&b_hdrs)).send().await.unwrap().status(), StatusCode::OK);

    let (o_url, o_hdrs) = sign_live_http(
        "PUT",
        &format!("{}/sha-bkt/obj", base_url),
        &[],
        SignableBody::Bytes(b"signed-bytes"),
        SignatureLocation::Headers,
        None,
        now,
    );
    let res = client
        .put(&o_url)
        .headers(to_reqwest_headers(&o_hdrs))
        .body("tampered-bytes")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = res.text().await.unwrap();
    assert!(body.contains("<Code>XAmzContentSHA256Mismatch</Code>"), "{}", body);
}

#[tokio::test]
async fn edge_4_unsigned_payload_accepted_and_stored_verbatim() {
    let (_state, base_url, _handle) = spawn_server(auth_cfg()).await;
    let client = reqwest::Client::new();
    let now = SystemTime::now();

    let (b_url, b_hdrs) = sign_live_http(
        "PUT",
        &format!("{}/uns-bkt", base_url),
        &[],
        SignableBody::UnsignedPayload,
        SignatureLocation::Headers,
        None,
        now,
    );
    assert_eq!(client.put(&b_url).headers(to_reqwest_headers(&b_hdrs)).send().await.unwrap().status(), StatusCode::OK);

    let payload = b"arbitrary unsigned payload bytes";
    let (o_url, o_hdrs) = sign_live_http(
        "PUT",
        &format!("{}/uns-bkt/obj", base_url),
        &[],
        SignableBody::UnsignedPayload,
        SignatureLocation::Headers,
        None,
        now,
    );
    assert_eq!(o_hdrs.get("x-amz-content-sha256").unwrap(), "UNSIGNED-PAYLOAD");
    let res = client
        .put(&o_url)
        .headers(to_reqwest_headers(&o_hdrs))
        .body(payload.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let (g_url, g_hdrs) = sign_live_http(
        "GET",
        &format!("{}/uns-bkt/obj", base_url),
        &[],
        SignableBody::UnsignedPayload,
        SignatureLocation::Headers,
        None,
        now,
    );
    let got = client
        .get(&g_url)
        .headers(to_reqwest_headers(&g_hdrs))
        .send()
        .await
        .unwrap();
    assert_eq!(got.status(), StatusCode::OK);
    assert_eq!(got.bytes().await.unwrap().as_ref(), payload);
}

#[tokio::test]
async fn edge_5_streaming_with_trailing_checksums_unsigned_and_signed() {
    let (_state, base_url, _handle) = spawn_server(auth_cfg()).await;
    let client = reqwest::Client::new();
    let now = SystemTime::now();

    let (b_url, b_hdrs) = sign_live_http(
        "PUT",
        &format!("{}/trl-bkt", base_url),
        &[],
        SignableBody::Bytes(&[]),
        SignatureLocation::Headers,
        None,
        now,
    );
    assert_eq!(client.put(&b_url).headers(to_reqwest_headers(&b_hdrs)).send().await.unwrap().status(), StatusCode::OK);

    // 5a. STREAMING-UNSIGNED-PAYLOAD-TRAILER signed by aws-sigv4
    let raw_payload = b"hello trailer";
    let crc = crc32fast::hash(raw_payload);
    let crc_b64 = {
        use std::io::Write;
        let _ = std::io::sink().write(&crc.to_be_bytes());
        // Base64 of 4-byte CRC32: compute with simple helper
        base64_encode(&crc.to_be_bytes())
    };
    let unsigned_framed = format!(
        "{:x}\r\nhello trailer\r\n0\r\nx-amz-checksum-crc32:{}\r\n\r\n",
        raw_payload.len(),
        crc_b64
    );
    let len_str = raw_payload.len().to_string();
    let (u_url, u_hdrs) = sign_live_http(
        "PUT",
        &format!("{}/trl-bkt/unsigned", base_url),
        &[
            ("content-encoding", "aws-chunked"),
            ("x-amz-decoded-content-length", &len_str),
            ("x-amz-trailer", "x-amz-checksum-crc32"),
        ],
        SignableBody::StreamingUnsignedPayloadTrailer,
        SignatureLocation::Headers,
        None,
        now,
    );
    assert_eq!(
        u_hdrs.get("x-amz-content-sha256").unwrap(),
        "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
    );
    let res = client
        .put(&u_url)
        .headers(to_reqwest_headers(&u_hdrs))
        .body(unsigned_framed)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK, "{}", res.text().await.unwrap());

    // 5b. STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER signed by aws-sigv4 (seed + chunk sigs + trailer)
    let (s_url, s_hdrs) = sign_live_http(
        "PUT",
        &format!("{}/trl-bkt/signed", base_url),
        &[
            ("content-encoding", "aws-chunked"),
            ("x-amz-decoded-content-length", &len_str),
            ("x-amz-trailer", "x-amz-checksum-crc32"),
        ],
        SignableBody::StreamingSignedPayloadTrailer,
        SignatureLocation::Headers,
        None,
        now,
    );
    assert_eq!(
        s_hdrs.get("x-amz-content-sha256").unwrap(),
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
    );
    let auth_val = s_hdrs.get("authorization").unwrap().to_str().unwrap();
    let seed_sig = auth_val.split("Signature=").nth(1).unwrap().to_string();
    let amz_date = s_hdrs.get("x-amz-date").unwrap().to_str().unwrap();
    let date8 = &amz_date[..8];
    let scope = format!("{}/{}/{}/aws4_request", date8, REGION, SERVICE);
    let cache = SigV4KeyCache::new();
    let empty_sha = sha256_hex(b"");
    let chunk1_sts = format!(
        "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
        amz_date,
        scope,
        seed_sig,
        empty_sha,
        sha256_hex(raw_payload)
    );
    let chunk1_sig = s3_server::compute_sigv4_signature(&cache, SK, date8, REGION, SERVICE, &chunk1_sts);
    let final_sts = format!(
        "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
        amz_date, scope, chunk1_sig, empty_sha, empty_sha
    );
    let final_sig = s3_server::compute_sigv4_signature(&cache, SK, date8, REGION, SERVICE, &final_sts);
    let trailer_line = format!("x-amz-checksum-crc32:{}\n", crc_b64);
    let trailer_sts = format!(
        "AWS4-HMAC-SHA256-TRAILER\n{}\n{}\n{}\n{}",
        amz_date,
        scope,
        final_sig,
        sha256_hex(trailer_line.as_bytes())
    );
    let trailer_sig = s3_server::compute_sigv4_signature(&cache, SK, date8, REGION, SERVICE, &trailer_sts);
    let signed_framed = format!(
        "{:x};chunk-signature={}\r\nhello trailer\r\n0;chunk-signature={}\r\nx-amz-checksum-crc32:{}\r\nx-amz-trailer-signature:{}\r\n\r\n",
        raw_payload.len(),
        chunk1_sig,
        final_sig,
        crc_b64,
        trailer_sig
    );
    let res = client
        .put(&s_url)
        .headers(to_reqwest_headers(&s_hdrs))
        .body(signed_framed)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK, "{}", res.text().await.unwrap());

    // Tampered chunk in STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER is refused with 403 SignatureDoesNotMatch.
    let (bad_url, bad_hdrs) = sign_live_http(
        "PUT",
        &format!("{}/trl-bkt/signed-bad", base_url),
        &[
            ("content-encoding", "aws-chunked"),
            ("x-amz-decoded-content-length", &len_str),
            ("x-amz-trailer", "x-amz-checksum-crc32"),
        ],
        SignableBody::StreamingSignedPayloadTrailer,
        SignatureLocation::Headers,
        None,
        now,
    );
    let bad_auth = bad_hdrs.get("authorization").unwrap().to_str().unwrap();
    let bad_seed = bad_auth.split("Signature=").nth(1).unwrap();
    let bad_date = bad_hdrs.get("x-amz-date").unwrap().to_str().unwrap();
    let bad_date8 = &bad_date[..8];
    let bad_scope = format!("{}/{}/{}/aws4_request", bad_date8, REGION, SERVICE);
    let c1_sig = s3_server::compute_sigv4_signature(
        &cache,
        SK,
        bad_date8,
        REGION,
        SERVICE,
        &format!("AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}", bad_date, bad_scope, bad_seed, empty_sha, sha256_hex(raw_payload)),
    );
    let fin_sig = s3_server::compute_sigv4_signature(
        &cache,
        SK,
        bad_date8,
        REGION,
        SERVICE,
        &format!("AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}", bad_date, bad_scope, c1_sig, empty_sha, empty_sha),
    );
    let bad_framed = format!(
        "{:x};chunk-signature={}\r\nHello trailer\r\n0;chunk-signature={}\r\nx-amz-checksum-crc32:{}\r\n\r\n",
        raw_payload.len(),
        c1_sig,
        fin_sig,
        crc_b64
    );
    let res = client
        .put(&bad_url)
        .headers(to_reqwest_headers(&bad_hdrs))
        .body(bad_framed)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    assert!(res.text().await.unwrap().contains("<Code>SignatureDoesNotMatch</Code>"));

    // Verify both stored objects read back cleanly without chunk framing or trailers.
    for key in ["unsigned", "signed"] {
        let (g_url, g_hdrs) = sign_live_http(
            "GET",
            &format!("{}/trl-bkt/{}", base_url, key),
            &[],
            SignableBody::Bytes(&[]),
            SignatureLocation::Headers,
            None,
            now,
        );
        let got = client
            .get(&g_url)
            .headers(to_reqwest_headers(&g_hdrs))
            .send()
            .await
            .unwrap();
        assert_eq!(got.status(), StatusCode::OK);
        assert_eq!(got.bytes().await.unwrap().as_ref(), raw_payload);
    }
}

fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        out.push(TABLE[(b0 >> 2) as usize] as char);
        out.push(TABLE[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        out.push(if chunk.len() > 1 {
            TABLE[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[(b2 & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}
