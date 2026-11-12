//! Request authentication: AWS Signature Version 4, in the Authorization header or as a
//! presigned URL, checked against the configured access key (owner decision 2A, #35).
//!
//! Signed `aws-chunked` bodies (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER]`): the seed
//! signature over the headers is checked by `authenticate`, and every chunk's signature, chained
//! from it, by `ChunkVerifier` (owner decision 14). The trailer signature of the `-TRAILER` form
//! is not checked; AwanFS does not use the trailing checksum it protects.

use crate::iam::{Iam, Principal};
use crate::sigv4::{compute_sigv4_signature, parse_iso8601_epoch, SigV4KeyCache};
use hyper::HeaderMap;
use sha2::{Digest, Sha256};

/// Who may call the server.
#[derive(Clone, Debug)]
pub struct AuthConfig {
    /// Access key and secret; `None` means no signed request can be accepted.
    pub credentials: Option<(String, String)>,
    /// Unsigned requests are allowed (and signed ones are not checked when there are no keys).
    pub allow_anonymous: bool,
}

impl AuthConfig {
    /// The library default, used by tests and embedders: anyone may call. The `s3-server`
    /// binary never uses it unless `--allow-anonymous` is given.
    pub fn open() -> Self {
        AuthConfig { credentials: None, allow_anonymous: true }
    }

    /// Keys from `AWANFS_ACCESS_KEY` and `AWANFS_SECRET_KEY`.
    pub fn from_env(allow_anonymous: bool) -> Self {
        let ak = std::env::var("AWANFS_ACCESS_KEY").ok().filter(|s| !s.is_empty());
        let sk = std::env::var("AWANFS_SECRET_KEY").ok().filter(|s| !s.is_empty());
        AuthConfig { credentials: ak.zip(sk), allow_anonymous }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

pub fn sha256_hex(data: &[u8]) -> String {
    hex(&Sha256::digest(data))
}

/// RFC 3986 encoding as SigV4 wants it: unreserved characters stay, everything else is %XX.
fn uri_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (crate::s3_api::hex_digit(bytes[i + 1]), crate::s3_api::hex_digit(bytes[i + 2])) {
                out.push(h << 4 | l);
                i += 3;
                continue;
            }
        }
        out.push(if bytes[i] == b'+' { b' ' } else { bytes[i] });
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn query_pairs(raw_query: &str) -> Vec<(String, String)> {
    raw_query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (percent_decode(k), percent_decode(v)),
            None => (percent_decode(p), String::new()),
        })
        .collect()
}

fn canonical_query(pairs: &[(String, String)], skip: Option<&str>) -> String {
    let mut enc: Vec<(String, String)> = pairs
        .iter()
        .filter(|(k, _)| Some(k.as_str()) != skip)
        .map(|(k, v)| (uri_encode(k), uri_encode(v)))
        .collect();
    enc.sort();
    enc.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join("&")
}

fn canonical_headers(headers: &HeaderMap, signed: &[&str]) -> Result<String, &'static str> {
    let mut out = String::new();
    for name in signed {
        let values: Vec<String> = headers
            .get_all(*name)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .map(|v| v.split_whitespace().collect::<Vec<_>>().join(" "))
            .collect();
        if values.is_empty() {
            return Err("AuthorizationHeaderMalformed");
        }
        out.push_str(name);
        out.push(':');
        out.push_str(&values.join(","));
        out.push('\n');
    }
    Ok(out)
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

struct Parsed<'a> {
    access_key: &'a str,
    date8: &'a str,
    region: &'a str,
    service: &'a str,
    signed_headers: Vec<&'a str>,
    signature: &'a str,
}

fn parse_credential(cred: &str) -> Result<(&str, &str, &str, &str), &'static str> {
    let parts: Vec<&str> = cred.split('/').collect();
    if parts.len() != 5 || parts[4] != "aws4_request" {
        return Err("AuthorizationHeaderMalformed");
    }
    Ok((parts[0], parts[1], parts[2], parts[3]))
}

/// Checks one request. `Err` carries the S3 error code to answer with.
#[allow(clippy::too_many_arguments)]
pub fn authenticate(
    cfg: &AuthConfig,
    cache: &SigV4KeyCache,
    method: &str,
    raw_path: &str,
    raw_query: &str,
    headers: &HeaderMap,
    now_sec: u64,
) -> Result<(), &'static str> {
    authenticate_with(cfg, None, cache, method, raw_path, raw_query, headers, now_sec).map(|_| ())
}

/// The secret for an access key, and who it belongs to: the root pair, or a config user.
fn secret_for<'a>(cfg: &'a AuthConfig, iam: Option<&'a Iam>, access_key: &str) -> Option<(Principal, &'a str)> {
    if let Some((a, s)) = &cfg.credentials {
        if a == access_key {
            return Some((Principal::Root, s.as_str()));
        }
    }
    iam.and_then(|i| i.user(access_key)).map(|u| (Principal::User(access_key.to_string()), u.secret.as_str()))
}

/// `authenticate` with the config file's users too (#53); returns who signed the request.
#[allow(clippy::too_many_arguments)]
pub fn authenticate_with(
    cfg: &AuthConfig,
    iam: Option<&Iam>,
    cache: &SigV4KeyCache,
    method: &str,
    raw_path: &str,
    raw_query: &str,
    headers: &HeaderMap,
    now_sec: u64,
) -> Result<Principal, &'static str> {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());
    let pairs = query_pairs(raw_query);
    let q = |name: &str| pairs.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str());
    let presigned = q("X-Amz-Algorithm").is_some();

    if auth_header.is_none() && !presigned {
        return if cfg.allow_anonymous { Ok(Principal::Anonymous) } else { Err("AccessDenied") };
    }
    if cfg.credentials.is_none() && iam.is_none_or(|i| i.is_empty()) {
        return if cfg.allow_anonymous { Ok(Principal::Anonymous) } else { Err("InvalidAccessKeyId") };
    }

    let (p, amz_date, payload_hash, skip) = if let Some(h) = auth_header {
        let rest = h.strip_prefix("AWS4-HMAC-SHA256").ok_or("AuthorizationHeaderMalformed")?.trim();
        let mut cred = None;
        let mut signed = None;
        let mut sig = None;
        for part in rest.split(',') {
            let part = part.trim();
            if let Some(v) = part.strip_prefix("Credential=") {
                cred = Some(v);
            } else if let Some(v) = part.strip_prefix("SignedHeaders=") {
                signed = Some(v);
            } else if let Some(v) = part.strip_prefix("Signature=") {
                sig = Some(v);
            }
        }
        let (access_key, date8, region, service) = parse_credential(cred.ok_or("AuthorizationHeaderMalformed")?)?;
        let amz_date = headers
            .get("x-amz-date")
            .and_then(|v| v.to_str().ok())
            .ok_or("AuthorizationHeaderMalformed")?
            .to_string();
        let req_epoch = parse_iso8601_epoch(&amz_date).ok_or("AuthorizationHeaderMalformed")?;
        if req_epoch.abs_diff(now_sec) > 900 {
            return Err("RequestTimeTooSkewed");
        }
        let payload_hash = headers
            .get("x-amz-content-sha256")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("UNSIGNED-PAYLOAD")
            .to_string();
        let parsed = Parsed {
            access_key,
            date8,
            region,
            service,
            signed_headers: signed.ok_or("AuthorizationHeaderMalformed")?.split(';').collect(),
            signature: sig.ok_or("AuthorizationHeaderMalformed")?,
        };
        (parsed, amz_date, payload_hash, None)
    } else {
        if q("X-Amz-Algorithm") != Some("AWS4-HMAC-SHA256") {
            return Err("AuthorizationQueryParametersError");
        }
        let (access_key, date8, region, service) = parse_credential(q("X-Amz-Credential").ok_or("AuthorizationQueryParametersError")?)?;
        let amz_date = q("X-Amz-Date").ok_or("AuthorizationQueryParametersError")?.to_string();
        let req_epoch = parse_iso8601_epoch(&amz_date).ok_or("AuthorizationQueryParametersError")?;
        let expires: u64 = q("X-Amz-Expires").and_then(|v| v.parse().ok()).ok_or("AuthorizationQueryParametersError")?;
        if expires > 604_800 {
            return Err("AuthorizationQueryParametersError");
        }
        if now_sec > req_epoch + expires {
            return Err("AccessDenied");
        }
        if req_epoch > now_sec + 900 {
            return Err("RequestTimeTooSkewed");
        }
        let parsed = Parsed {
            access_key,
            date8,
            region,
            service,
            signed_headers: q("X-Amz-SignedHeaders").ok_or("AuthorizationQueryParametersError")?.split(';').collect(),
            signature: q("X-Amz-Signature").ok_or("AuthorizationQueryParametersError")?,
        };
        (parsed, amz_date, "UNSIGNED-PAYLOAD".to_string(), Some("X-Amz-Signature"))
    };

    let (principal, secret) = secret_for(cfg, iam, p.access_key).ok_or("InvalidAccessKeyId")?;
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method,
        if raw_path.is_empty() { "/" } else { raw_path },
        canonical_query(&pairs, skip),
        canonical_headers(headers, &p.signed_headers)?,
        p.signed_headers.join(";"),
        payload_hash
    );
    let scope = format!("{}/{}/{}/aws4_request", p.date8, p.region, p.service);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date,
        scope,
        sha256_hex(canonical_request.as_bytes())
    );
    let expected = compute_sigv4_signature(cache, secret, p.date8, p.region, p.service, &string_to_sign);
    if constant_time_eq(&expected, &p.signature.to_ascii_lowercase()) {
        Ok(principal)
    } else {
        Err("SignatureDoesNotMatch")
    }
}

/// For a signed request whose `x-amz-content-sha256` is a hex digest: does the body match it?
/// Unsigned and streaming payloads have nothing to compare and pass.
pub fn payload_hash_ok(headers: &HeaderMap, body: &[u8]) -> bool {
    match headers.get("x-amz-content-sha256").and_then(|v| v.to_str().ok()) {
        Some(h) if h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit()) => sha256_hex(body).eq_ignore_ascii_case(h),
        _ => true,
    }
}

/// Hex SHA-256 of the empty string, part of every chunk's string to sign.
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Checks the chain of chunk signatures of one signed `aws-chunked` body.
pub struct ChunkVerifier {
    secret: String,
    date8: String,
    region: String,
    service: String,
    amz_date: String,
    previous: String,
}

impl ChunkVerifier {
    /// For a request `authenticate` accepted: `Some` when its body is a signed `aws-chunked`
    /// stream and keys are configured, else `None` (nothing to check).
    pub fn for_request(cfg: &AuthConfig, headers: &HeaderMap) -> Option<Self> {
        Self::for_request_with(cfg, None, headers)
    }

    /// `for_request`, looking the signer's secret up among the config users too.
    pub fn for_request_with(cfg: &AuthConfig, iam: Option<&Iam>, headers: &HeaderMap) -> Option<Self> {
        let content = headers.get("x-amz-content-sha256")?.to_str().ok()?;
        if !content.starts_with("STREAMING-AWS4-HMAC-SHA256-PAYLOAD") {
            return None;
        }
        let auth = headers.get("authorization")?.to_str().ok()?.strip_prefix("AWS4-HMAC-SHA256")?.trim();
        let mut cred = None;
        let mut sig = None;
        for part in auth.split(',') {
            let part = part.trim();
            if let Some(v) = part.strip_prefix("Credential=") {
                cred = Some(v);
            } else if let Some(v) = part.strip_prefix("Signature=") {
                sig = Some(v);
            }
        }
        let (access_key, date8, region, service) = parse_credential(cred?).ok()?;
        let (_, secret) = secret_for(cfg, iam, access_key)?;
        Some(ChunkVerifier {
            secret: secret.to_string(),
            date8: date8.to_string(),
            region: region.to_string(),
            service: service.to_string(),
            amz_date: headers.get("x-amz-date")?.to_str().ok()?.to_string(),
            previous: sig?.to_ascii_lowercase(),
        })
    }

    /// Checks the next chunk: its signature must be the HMAC of
    /// `AWS4-HMAC-SHA256-PAYLOAD\n<date>\n<scope>\n<previous signature>\n<sha256("")>\n<sha256(data)>`.
    pub fn check(&mut self, cache: &SigV4KeyCache, data: &[u8], signature: Option<&str>) -> bool {
        let Some(signature) = signature else { return false };
        let sts = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}/{}/{}/aws4_request\n{}\n{}\n{}",
            self.amz_date,
            self.date8,
            self.region,
            self.service,
            self.previous,
            EMPTY_SHA256,
            sha256_hex(data)
        );
        let expected = compute_sigv4_signature(cache, &self.secret, &self.date8, &self.region, &self.service, &sts);
        if constant_time_eq(&expected, &signature.to_ascii_lowercase()) {
            self.previous = expected;
            true
        } else {
            false
        }
    }
}

/// Is this body `aws-chunked` framed? Any `STREAMING-*` content hash, or
/// `Content-Encoding: aws-chunked`.
pub fn is_aws_chunked(headers: &HeaderMap) -> bool {
    headers.get("x-amz-content-sha256").and_then(|v| v.to_str().ok()).is_some_and(|v| v.starts_with("STREAMING-"))
        || headers
            .get("content-encoding")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.split(',').any(|e| e.trim().eq_ignore_ascii_case("aws-chunked")))
}
