//! SigV4 authentication (#35). Vectors from the AWS S3 documentation, "Examples: Signature
//! Calculations in AWS Signature Version 4" (GET object with a Range header, and the presigned
//! URL example), plus the server's behaviour with and without credentials.

use hyper::header::{HeaderMap, HeaderValue};
use reqwest::StatusCode;
use s3_server::{authenticate, parse_iso8601_epoch, start_s3_reactor, AuthConfig, S3ServiceState, SigV4KeyCache};
use std::net::SocketAddr;
use std::sync::Arc;

const AK: &str = "AKIAIOSFODNN7EXAMPLE";
const SK: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

fn keys(secret: &str) -> AuthConfig {
    AuthConfig { credentials: Some((AK.to_string(), secret.to_string())), allow_anonymous: false }
}

fn get_object_headers(signature: &str) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert("host", HeaderValue::from_static("examplebucket.s3.amazonaws.com"));
    h.insert("range", HeaderValue::from_static("bytes=0-9"));
    h.insert("x-amz-content-sha256", HeaderValue::from_static("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"));
    h.insert("x-amz-date", HeaderValue::from_static("20130524T000000Z"));
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={}/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature={}",
        AK, signature
    );
    h.insert("authorization", HeaderValue::from_str(&auth).unwrap());
    h
}

const GOOD: &str = "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41";

fn now() -> u64 {
    parse_iso8601_epoch("20130524T000000Z").unwrap()
}

#[test]
fn aws_documented_get_object_signature_is_accepted() {
    let r = authenticate(&keys(SK), &SigV4KeyCache::new(), "GET", "/test.txt", "", &get_object_headers(GOOD), now());
    assert_eq!(r, Ok(()));
}

#[test]
fn a_changed_signature_or_secret_is_refused() {
    let bad = format!("{}0", &GOOD[..63]);
    let r = authenticate(&keys(SK), &SigV4KeyCache::new(), "GET", "/test.txt", "", &get_object_headers(&bad), now());
    assert_eq!(r, Err("SignatureDoesNotMatch"));
    let r = authenticate(&keys("not-the-secret"), &SigV4KeyCache::new(), "GET", "/test.txt", "", &get_object_headers(GOOD), now());
    assert_eq!(r, Err("SignatureDoesNotMatch"));
    // The same signature on another path or method is refused too.
    let r = authenticate(&keys(SK), &SigV4KeyCache::new(), "DELETE", "/test.txt", "", &get_object_headers(GOOD), now());
    assert_eq!(r, Err("SignatureDoesNotMatch"));
}

#[test]
fn unknown_access_key_and_skewed_clock_are_refused() {
    let other = AuthConfig { credentials: Some(("SOMEONEELSE".into(), SK.into())), allow_anonymous: false };
    let r = authenticate(&other, &SigV4KeyCache::new(), "GET", "/test.txt", "", &get_object_headers(GOOD), now());
    assert_eq!(r, Err("InvalidAccessKeyId"));
    let r = authenticate(&keys(SK), &SigV4KeyCache::new(), "GET", "/test.txt", "", &get_object_headers(GOOD), now() + 3600);
    assert_eq!(r, Err("RequestTimeTooSkewed"));
}

const PRESIGNED_QUERY: &str = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404";

#[test]
fn aws_documented_presigned_url_is_accepted_until_it_expires() {
    let mut h = HeaderMap::new();
    h.insert("host", HeaderValue::from_static("examplebucket.s3.amazonaws.com"));
    let cache = SigV4KeyCache::new();
    assert_eq!(authenticate(&keys(SK), &cache, "GET", "/test.txt", PRESIGNED_QUERY, &h, now() + 60), Ok(()));
    assert_eq!(authenticate(&keys(SK), &cache, "GET", "/test.txt", PRESIGNED_QUERY, &h, now() + 86_401), Err("AccessDenied"));
    let tampered = PRESIGNED_QUERY.replace("X-Amz-Expires=86400", "X-Amz-Expires=86399");
    assert_eq!(authenticate(&keys(SK), &cache, "GET", "/test.txt", &tampered, &h, now() + 60), Err("SignatureDoesNotMatch"));
}

async fn server(auth: AuthConfig) -> (String, s3_server::ServerHandle) {
    let mut state = S3ServiceState::new();
    state.auth = auth;
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::new(state)).await.unwrap();
    (format!("http://{}", handle.local_addr), handle)
}

#[tokio::test]
async fn unsigned_requests_need_the_anonymous_switch() {
    let c = reqwest::Client::new();
    let (url, _h) = server(keys(SK)).await;
    let res = c.put(format!("{}/locked-bkt", url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    assert!(res.text().await.unwrap().contains("AccessDenied"));

    let res = c
        .put(format!("{}/locked-bkt", url))
        .header("authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20261112/us-east-1/s3/aws4_request,SignedHeaders=host,Signature=00")
        .header("x-amz-date", "20261112T000000Z")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN, "a made-up signature must not get in");

    let (url, _h2) = server(AuthConfig::open()).await;
    assert_eq!(c.put(format!("{}/open-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
}

#[test]
fn the_binary_refuses_to_start_without_credentials_or_the_anonymous_switch() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_s3-server"))
        .args(["--port", "0"])
        .env_remove("AWANFS_ACCESS_KEY")
        .env_remove("AWANFS_SECRET_KEY")
        .env_remove("AWANFS_ALLOW_ANONYMOUS")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("--allow-anonymous"));
}

#[test]
fn a_body_must_match_its_declared_sha256() {
    let mut h = HeaderMap::new();
    h.insert("x-amz-content-sha256", HeaderValue::from_str(&s3_server::sha256_hex(b"hello")).unwrap());
    assert!(s3_server::payload_hash_ok(&h, b"hello"));
    assert!(!s3_server::payload_hash_ok(&h, b"hellO"));
    h.insert("x-amz-content-sha256", HeaderValue::from_static("UNSIGNED-PAYLOAD"));
    assert!(s3_server::payload_hash_ok(&h, b"anything"));
}

#[tokio::test]
async fn a_put_whose_body_does_not_match_its_hash_is_refused() {
    let c = reqwest::Client::new();
    let (url, _h) = server(AuthConfig::open()).await;
    assert_eq!(c.put(format!("{}/hash-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let res = c
        .put(format!("{}/hash-bkt/k", url))
        .header("x-amz-content-sha256", s3_server::sha256_hex(b"declared"))
        .body("sent")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert!(res.text().await.unwrap().contains("XAmzContentSHA256Mismatch"));
    assert_eq!(c.get(format!("{}/hash-bkt/k", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND, "nothing stored");
}

/// A SigV4 signature over `canonical_value` for one signed `x-amz-meta-meta1` header, computed
/// here with hmac and sha2 rather than the server's signer.
fn sign_meta(secret: &str, canonical_value: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    let mac = |key: &[u8], data: &[u8]| {
        let mut m = Hmac::<Sha256>::new_from_slice(key).unwrap();
        m.update(data);
        m.finalize().into_bytes().to_vec()
    };
    let hexs = |b: &[u8]| b.iter().map(|x| format!("{:02x}", x)).collect::<String>();
    let payload = "UNSIGNED-PAYLOAD";
    let canonical = format!(
        "PUT\n/bucket/foo\n\nhost:examplebucket.s3.amazonaws.com\nx-amz-content-sha256:{}\nx-amz-date:20130524T000000Z\nx-amz-meta-meta1:{}\n\nhost;x-amz-content-sha256;x-amz-date;x-amz-meta-meta1\n{}",
        payload, canonical_value, payload
    );
    let sts = format!(
        "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n{}",
        hexs(&Sha256::digest(canonical.as_bytes()))
    );
    let k = mac(format!("AWS4{}", secret).as_bytes(), b"20130524");
    let k = mac(&k, b"us-east-1");
    let k = mac(&k, b"s3");
    let k = mac(&k, b"aws4_request");
    hexs(&mac(&k, sts.as_bytes()))
}

fn meta_put_headers(wire_value: &[u8], signature: &str) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert("host", HeaderValue::from_static("examplebucket.s3.amazonaws.com"));
    h.insert("x-amz-content-sha256", HeaderValue::from_static("UNSIGNED-PAYLOAD"));
    h.insert("x-amz-date", HeaderValue::from_static("20130524T000000Z"));
    h.insert("x-amz-meta-meta1", HeaderValue::from_bytes(wire_value).unwrap());
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={}/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-meta-meta1,Signature={}",
        AK, signature
    );
    h.insert("authorization", HeaderValue::from_str(&auth).unwrap());
    h
}

/// ceph's `test_object_set_get_unicode_metadata`: botocore signs the text `Hello World\xe9` (the
/// canonical request is hashed as UTF-8), and Python's http.client sends it as Latin-1, one byte
/// 0xE9. A signed header value that is not ASCII used to vanish from the canonical request, so
/// the request was refused as AuthorizationHeaderMalformed.
#[test]
fn a_signed_header_sent_as_latin1_bytes_verifies_against_its_text() {
    let sig = sign_meta(SK, "Hello World\u{e9}");
    let h = meta_put_headers(b"Hello World\xe9", &sig);
    assert_eq!(authenticate(&keys(SK), &SigV4KeyCache::new(), "PUT", "/bucket/foo", "", &h, now()), Ok(()));
}

/// A client that sends the UTF-8 bytes it signed (0xC3 0xA9 for `\xe9`) verifies too.
#[test]
fn a_signed_header_sent_as_utf8_bytes_verifies() {
    let sig = sign_meta(SK, "Hello World\u{e9}");
    let h = meta_put_headers("Hello World\u{e9}".as_bytes(), &sig);
    assert_eq!(authenticate(&keys(SK), &SigV4KeyCache::new(), "PUT", "/bucket/foo", "", &h, now()), Ok(()));
}

/// Reading the bytes two ways never accepts a signature over other bytes, or another secret.
#[test]
fn a_non_ascii_header_still_needs_the_right_value_and_secret() {
    let cache = SigV4KeyCache::new();
    let h = meta_put_headers(b"Hello World\xe9", &sign_meta(SK, "Hello World\u{e8}"));
    assert_eq!(authenticate(&keys(SK), &cache, "PUT", "/bucket/foo", "", &h, now()), Err("SignatureDoesNotMatch"));
    let h = meta_put_headers(b"Hello World\xe9", &sign_meta("another-secret", "Hello World\u{e9}"));
    assert_eq!(authenticate(&keys(SK), &cache, "PUT", "/bucket/foo", "", &h, now()), Err("SignatureDoesNotMatch"));
}

/// #457: a SigV4 signature over GET /bucket/k whose request time is `sts_time`, with
/// `date_line` (the canonical `date` or `x-amz-date` header) signed. Computed here with hmac
/// and sha2 rather than the server's signer.
fn sign_get(secret: &str, signed: &str, date_line: &str, sts_time: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    let mac = |key: &[u8], data: &[u8]| {
        let mut m = Hmac::<Sha256>::new_from_slice(key).unwrap();
        m.update(data);
        m.finalize().into_bytes().to_vec()
    };
    let hexs = |b: &[u8]| b.iter().map(|x| format!("{:02x}", x)).collect::<String>();
    let canonical = format!(
        "GET\n/bucket/k\n\n{}\nhost:examplebucket.s3.amazonaws.com\nx-amz-content-sha256:UNSIGNED-PAYLOAD\n\n{}\nUNSIGNED-PAYLOAD",
        date_line, signed
    );
    let sts = format!(
        "AWS4-HMAC-SHA256\n{}\n20130524/us-east-1/s3/aws4_request\n{}",
        sts_time,
        hexs(&Sha256::digest(canonical.as_bytes()))
    );
    let k = mac(format!("AWS4{}", secret).as_bytes(), b"20130524");
    let k = mac(&k, b"us-east-1");
    let k = mac(&k, b"s3");
    let k = mac(&k, b"aws4_request");
    hexs(&mac(&k, sts.as_bytes()))
}

fn date_get_headers(date: Option<&str>, amz_date: Option<&str>, signed: &str, signature: &str) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert("host", HeaderValue::from_static("examplebucket.s3.amazonaws.com"));
    h.insert("x-amz-content-sha256", HeaderValue::from_static("UNSIGNED-PAYLOAD"));
    if let Some(d) = date {
        h.insert("date", HeaderValue::from_str(d).unwrap());
    }
    if let Some(d) = amz_date {
        h.insert("x-amz-date", HeaderValue::from_str(d).unwrap());
    }
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={}/20130524/us-east-1/s3/aws4_request,SignedHeaders={},Signature={}",
        AK, signed, signature
    );
    h.insert("authorization", HeaderValue::from_str(&auth).unwrap());
    h
}

const HTTP_DATE: &str = "Fri, 24 May 2013 00:00:00 GMT";
const DATE_SIGNED: &str = "date;host;x-amz-content-sha256";

#[test]
fn a_request_signed_with_only_the_date_header_is_accepted() {
    let sig = sign_get(SK, DATE_SIGNED, &format!("date:{}", HTTP_DATE), "20130524T000000Z");
    let h = date_get_headers(Some(HTTP_DATE), None, DATE_SIGNED, &sig);
    assert_eq!(authenticate(&keys(SK), &SigV4KeyCache::new(), "GET", "/bucket/k", "", &h, now()), Ok(()));
    // The Date header is covered by the signature.
    let h = date_get_headers(Some("Fri, 24 May 2013 00:00:01 GMT"), None, DATE_SIGNED, &sig);
    assert_eq!(authenticate(&keys(SK), &SigV4KeyCache::new(), "GET", "/bucket/k", "", &h, now()), Err("SignatureDoesNotMatch"));
}

#[test]
fn a_date_header_an_hour_away_is_too_skewed() {
    let sig = sign_get(SK, DATE_SIGNED, &format!("date:{}", HTTP_DATE), "20130524T000000Z");
    let h = date_get_headers(Some(HTTP_DATE), None, DATE_SIGNED, &sig);
    assert_eq!(authenticate(&keys(SK), &SigV4KeyCache::new(), "GET", "/bucket/k", "", &h, now() + 3600), Err("RequestTimeTooSkewed"));
}

#[test]
fn an_unreadable_date_without_x_amz_date_is_malformed() {
    let h = date_get_headers(Some("yesterday at noon"), None, DATE_SIGNED, &"0".repeat(64));
    assert_eq!(authenticate(&keys(SK), &SigV4KeyCache::new(), "GET", "/bucket/k", "", &h, now()), Err("AuthorizationHeaderMalformed"));
    let h = date_get_headers(None, None, "host;x-amz-content-sha256", &"0".repeat(64));
    assert_eq!(authenticate(&keys(SK), &SigV4KeyCache::new(), "GET", "/bucket/k", "", &h, now()), Err("AuthorizationHeaderMalformed"));
}

#[test]
fn x_amz_date_wins_over_the_date_header() {
    let signed = "host;x-amz-content-sha256;x-amz-date";
    let canonical_headers = "host:examplebucket.s3.amazonaws.com\nx-amz-content-sha256:UNSIGNED-PAYLOAD\nx-amz-date:20130524T000000Z";
    // sign_get puts its date line first; here the canonical headers are host, sha256, x-amz-date.
    let sig = {
        use hmac::{Hmac, Mac};
        use sha2::{Digest, Sha256};
        let mac = |key: &[u8], data: &[u8]| {
            let mut m = Hmac::<Sha256>::new_from_slice(key).unwrap();
            m.update(data);
            m.finalize().into_bytes().to_vec()
        };
        let hexs = |b: &[u8]| b.iter().map(|x| format!("{:02x}", x)).collect::<String>();
        let canonical = format!("GET\n/bucket/k\n\n{}\n\n{}\nUNSIGNED-PAYLOAD", canonical_headers, signed);
        let sts = format!("AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n{}", hexs(&Sha256::digest(canonical.as_bytes())));
        let k = mac(format!("AWS4{}", SK).as_bytes(), b"20130524");
        let k = mac(&k, b"us-east-1");
        let k = mac(&k, b"s3");
        let k = mac(&k, b"aws4_request");
        hexs(&mac(&k, sts.as_bytes()))
    };
    // An unsigned Date an hour later: if it were read, the request would be too skewed.
    let h = date_get_headers(Some("Fri, 24 May 2013 01:00:00 GMT"), Some("20130524T000000Z"), signed, &sig);
    assert_eq!(authenticate(&keys(SK), &SigV4KeyCache::new(), "GET", "/bucket/k", "", &h, now()), Ok(()));
}
