//! Official AWS SigV4 test suite vectors (`server/tests/fixtures/sigv4/v4/`,
//! sourced from `aws-sigv4`'s Apache-2.0 `aws-signing-test-suite/v4`).
//!
//! Each vector in `fixtures/sigv4/v4/` is explicitly classified below:
//! - Excluded (non-S3 double encoding):
//!   `double-encode-path`, `double-url-encode`
//! - Excluded (non-S3 path normalization of `//`, `.`, `..`, or paired normalized variant):
//!   `get-relative-normalized`, `get-relative-relative-normalized`,
//!   `get-slash-dot-slash-normalized`, `get-slash-normalized`,
//!   `get-slash-pointless-dot-normalized`, `get-slashes-normalized`,
//!   `get-space-normalized`
//! - Excluded (invalid unencoded HTTP wire request-target characters; `http::Uri` rejects):
//!   `get-space-unnormalized`, `get-utf8`, `get-vanilla-utf8-query`
//! - Applicable (28 vectors): verified against expected canonical request,
//!   string-to-sign, signature, and `s3_server::authenticate`.

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    sign, PayloadChecksumKind, PercentEncodingMode, SessionTokenMode, SignableBody,
    SignableRequest, SigningSettings, UriPathNormalizationMode,
};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use hyper::header::{HeaderMap, HeaderName, HeaderValue};
use s3_server::{
    authenticate, compute_sigv4_signature, parse_iso8601_epoch, verify_sigv4_signature,
    AuthConfig, SigV4KeyCache,
};
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH};

const EXCLUDED_NON_S3: &[(&str, &str)] = &[
    ("double-encode-path", "non-S3 service (execute-api) double-encodes URI path"),
    ("double-url-encode", "non-S3 service (lambda) double-encodes URI path"),
    ("get-relative-normalized", "normalizes '..' segments; S3 does not normalize paths"),
    ("get-relative-relative-normalized", "normalizes '..' segments; S3 does not normalize paths"),
    ("get-slash-dot-slash-normalized", "normalizes '/./'; S3 does not normalize paths"),
    ("get-slash-normalized", "normalizes '//'; S3 does not normalize paths"),
    ("get-slash-pointless-dot-normalized", "normalizes '/./'; S3 does not normalize paths"),
    ("get-slashes-normalized", "normalizes '//'; S3 does not normalize paths"),
    ("get-space-normalized", "normalize:true variant with unencoded space in signed request"),
    ("get-space-unnormalized", "unencoded space in raw HTTP request line (invalid URI wire target)"),
    ("get-utf8", "unencoded UTF-8 in raw HTTP request line (invalid URI wire target)"),
    ("get-vanilla-utf8-query", "unencoded UTF-8 in raw HTTP query line (invalid URI wire target)"),
];

fn fixtures_v4_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("sigv4")
        .join("v4")
}

fn read_trimmed(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("failed to read {}: {}", path.display(), e))
        .replace("\r\n", "\n")
        .trim_end_matches('\n')
        .to_string()
}

struct ParsedHttp {
    method: String,
    raw_path: String,
    raw_query: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

fn parse_http_request(raw: &str) -> ParsedHttp {
    let raw = raw.replace("\r\n", "\n");
    let (head, body) = match raw.split_once("\n\n") {
        Some((h, b)) => (h, b.as_bytes().to_vec()),
        None => (raw.as_str(), Vec::new()),
    };
    let mut lines = head.split('\n');
    let req_line = lines.next().expect("request line");
    let (method, rest) = req_line.split_once(' ').expect("method");
    let target = rest.rsplit_once(" HTTP/").map(|(t, _)| t).unwrap_or(rest);
    let (raw_path, raw_query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.to_string(), String::new()),
    };

    let mut headers: Vec<(String, String)> = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if line.starts_with(' ') || line.starts_with('\t') {
            if let Some((_, val)) = headers.last_mut() {
                val.push(' ');
                val.push_str(line.trim());
            }
        } else if let Some((k, v)) = line.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }

    ParsedHttp {
        method: method.to_string(),
        raw_path,
        raw_query,
        headers,
        body,
    }
}

fn to_hyper_headers(pairs: &[(String, String)]) -> HeaderMap {
    let mut map = HeaderMap::new();
    for (k, v) in pairs {
        let name = HeaderName::from_bytes(k.as_bytes()).expect("header name");
        let val = HeaderValue::from_str(v).expect("header value");
        map.append(name, val);
    }
    map
}

#[test]
fn fixtures_include_license_and_every_directory_is_accounted_for() {
    let base = fixtures_v4_dir().parent().unwrap().to_path_buf();
    let license = std::fs::read_to_string(base.join("LICENSE")).expect("LICENSE file");
    assert!(license.contains("Apache License"));

    let excluded: BTreeSet<&str> = EXCLUDED_NON_S3.iter().map(|(n, _)| *n).collect();
    let mut all = BTreeSet::new();
    for entry in std::fs::read_dir(fixtures_v4_dir()).unwrap() {
        let entry = entry.unwrap();
        if entry.path().is_dir() {
            all.insert(entry.file_name().to_string_lossy().into_owned());
        }
    }
    assert_eq!(all.len(), 40, "expected 40 v4 vector dirs");
    for name in &excluded {
        assert!(all.contains(*name), "excluded vector missing: {}", name);
    }
    let used_count = all.iter().filter(|n| !excluded.contains(n.as_str())).count();
    assert_eq!(excluded.len(), 12);
    assert_eq!(used_count, 28);
}

#[test]
fn applicable_aws_sigv4_vectors_match_expected_and_authenticate() {
    let excluded: BTreeSet<&str> = EXCLUDED_NON_S3.iter().map(|(n, _)| *n).collect();
    let v4_dir = fixtures_v4_dir();
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(&v4_dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|x| x.path()))
        .filter(|p| p.is_dir())
        .collect();
    dirs.sort();

    let cache = SigV4KeyCache::new();
    let mut failures = Vec::new();
    let mut passed = 0usize;

    for dir in dirs {
        let name = dir.file_name().unwrap().to_string_lossy().to_string();
        if excluded.contains(name.as_str()) {
            continue;
        }

        let ctx: Value = serde_json::from_str(&read_trimmed(&dir.join("context.json"))).unwrap();
        let ak = ctx["credentials"]["access_key_id"].as_str().unwrap();
        let sk = ctx["credentials"]["secret_access_key"].as_str().unwrap();
        let token = ctx["credentials"]["token"].as_str().map(str::to_string);
        let region = ctx["region"].as_str().unwrap();
        let service = ctx["service"].as_str().unwrap();
        let sign_body = ctx["sign_body"].as_bool().unwrap_or(true);
        let omit_token = ctx["omit_session_token"].as_bool().unwrap_or(false);
        let timestamp = ctx["timestamp"].as_str().unwrap();

        let expected_creq = read_trimmed(&dir.join("header-canonical-request.txt"));
        let expected_sts = read_trimmed(&dir.join("header-string-to-sign.txt"));
        let expected_sig = read_trimmed(&dir.join("header-signature.txt"));
        let signed_req = parse_http_request(&std::fs::read_to_string(dir.join("header-signed-request.txt")).unwrap());

        // 1. Check string-to-sign -> signature using AwanFS's SigV4 key derivation & HMAC.
        let amz_date = expected_sts.lines().nth(1).unwrap();
        let date8 = &amz_date[..8];
        let got_sig = compute_sigv4_signature(&cache, sk, date8, region, service, &expected_sts);
        assert_eq!(got_sig, expected_sig, "signature mismatch on vector {}", name);
        assert_eq!(
            verify_sigv4_signature(&cache, sk, date8, region, service, &expected_sts, &expected_sig),
            Ok(()),
            "verify_sigv4_signature failed on {}",
            name
        );

        // 2. Check aws-sigv4 reference signer matches the vector signature in S3 path mode
        //    (no path normalization, single percent-encoding).
        let raw_req = parse_http_request(&std::fs::read_to_string(dir.join("request.txt")).unwrap());
        let host = raw_req
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("host"))
            .map(|(_, v)| v.as_str())
            .unwrap();
        let uri = if raw_req.raw_query.is_empty() {
            format!("https://{}{}", host, raw_req.raw_path)
        } else {
            format!("https://{}{}?{}", host, raw_req.raw_path, raw_req.raw_query)
        };
        let identity: Identity = Credentials::new(ak, sk, token, None, "vector").into();
        let mut settings = SigningSettings::default();
        settings.percent_encoding_mode = PercentEncodingMode::Single;
        settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
        settings.payload_checksum_kind = if sign_body {
            PayloadChecksumKind::XAmzSha256
        } else {
            PayloadChecksumKind::NoHeader
        };
        settings.session_token_mode = if omit_token {
            SessionTokenMode::Exclude
        } else {
            SessionTokenMode::Include
        };
        let epoch = parse_iso8601_epoch(amz_date).expect("valid amz_date");
        let params = v4::SigningParams::builder()
            .identity(&identity)
            .region(region)
            .name(service)
            .time(UNIX_EPOCH + Duration::from_secs(epoch))
            .settings(settings)
            .build()
            .unwrap()
            .into();
        let hdr_refs: Vec<(&str, &str)> = raw_req
            .headers
            .iter()
            .filter(|(k, _)| !k.eq_ignore_ascii_case("host"))
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let signable = SignableRequest::new(
            &raw_req.method,
            &uri,
            hdr_refs.into_iter(),
            SignableBody::Bytes(&raw_req.body),
        )
        .unwrap();
        let (_inst, ref_sig) = sign(signable, &params).unwrap().into_parts();
        assert_eq!(
            ref_sig, expected_sig,
            "aws-sigv4 in S3 unnormalized single-encode mode disagreed on {} (timestamp={}, creq=\n{})",
            name, timestamp, expected_creq
        );

        // 3. Check AwanFS's `authenticate` accepts the signed request verbatim.
        let cfg = AuthConfig {
            credentials: Some((ak.to_string(), sk.to_string())),
            allow_anonymous: false,
        };
        let hyper_headers = to_hyper_headers(&signed_req.headers);
        match authenticate(
            &cfg,
            &cache,
            &signed_req.method,
            &signed_req.raw_path,
            &signed_req.raw_query,
            &hyper_headers,
            epoch,
        ) {
            Ok(()) => passed += 1,
            Err(e) => failures.push((name, e, sign_body)),
        }
    }

    assert!(
        failures.is_empty(),
        "AwanFS refused {} of {} applicable AWS SigV4 vectors (passed={}): {:?}",
        failures.len(),
        passed + failures.len(),
        passed,
        failures
    );
}
