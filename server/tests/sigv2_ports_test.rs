//! SigV4 ports of the ceph s3-tests `test_headers::*_aws2` tests that have no other AwanFS test
//! (#433). The suite signs these with Signature Version 2, which AwanFS does not accept
//! (`CONFORMANCE_DESIGN.md` section 9). Each port sends the same change to the request (a bad
//! Authorization header, no User-Agent, a bad date), signed with SigV4, and asserts the status
//! and error code AwanFS answers. `tests/s3tests/PORTED.md` maps each suite test to its port.
//!
//! The signer here is written from the AWS SigV4 description, not taken from the server, so
//! the User-Agent ports check a request the server did not sign itself.

use hmac::{Hmac, Mac};
use reqwest::StatusCode;
use s3_server::{start_s3_reactor, AuthConfig, S3ServiceState};
use sha2::{Digest, Sha256};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const AK: &str = "AKIAIOSFODNN7EXAMPLE";
const SK: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";
const EMPTY: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

async fn server() -> (String, String, s3_server::ServerHandle) {
    let mut state = S3ServiceState::new();
    state.auth = AuthConfig { credentials: Some((AK.to_string(), SK.to_string())), allow_anonymous: false };
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::new(state)).await.unwrap();
    let host = handle.local_addr.to_string();
    (format!("http://{}", host), host, handle)
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

fn sha256(b: &[u8]) -> String {
    hex(&Sha256::digest(b))
}

fn hmac(key: &[u8], msg: &str) -> Vec<u8> {
    let mut m = Hmac::<Sha256>::new_from_slice(key).unwrap();
    m.update(msg.as_bytes());
    m.finalize().into_bytes().to_vec()
}

/// `YYYYMMDDTHHMMSSZ` for a Unix time (days to civil date, Howard Hinnant's algorithm).
fn amz_date(epoch: u64) -> String {
    let days = (epoch / 86_400) as i64;
    let rem = epoch % 86_400;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    format!("{:04}{:02}{:02}T{:02}{:02}{:02}Z", y, m, d, rem / 3600, rem % 3600 / 60, rem % 60)
}

fn now_amz_date() -> String {
    amz_date(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs())
}

/// The Authorization header for `PUT path` with `host;x-amz-content-sha256;x-amz-date` signed.
fn authorization(host: &str, path: &str, date: &str, payload_sha: &str) -> String {
    let canonical = format!(
        "PUT\n{}\n\nhost:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n\nhost;x-amz-content-sha256;x-amz-date\n{}",
        path, host, payload_sha, date, payload_sha
    );
    let scope = format!("{}/{}/s3/aws4_request", &date[..8], REGION);
    let to_sign = format!("AWS4-HMAC-SHA256\n{}\n{}\n{}", date, scope, sha256(canonical.as_bytes()));
    let k = hmac(format!("AWS4{}", SK).as_bytes(), &date[..8]);
    let k = hmac(&k, REGION);
    let k = hmac(&k, "s3");
    let k = hmac(&k, "aws4_request");
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{},SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature={}",
        AK,
        scope,
        hex(&hmac(&k, &to_sign))
    )
}

/// What to do to a signed `PUT path` before it is sent.
enum Change<'a> {
    None,
    /// Send this Authorization header instead of the signed one.
    Authorization(&'a str),
    /// Send this User-Agent (not signed, as in the suite).
    UserAgent(&'a str),
    /// Sign with this `x-amz-date` value (a well-formed one is signed as is; a malformed one
    /// is sent with a signature made for the current time).
    Date(&'a str),
    /// Send no `x-amz-date` header (and no `Date` header).
    NoDate,
}

async fn put(url: &str, host: &str, path: &str, body: &'static [u8], change: Change<'_>) -> (StatusCode, String) {
    let payload = if body.is_empty() { EMPTY.to_string() } else { sha256(body) };
    let date = match change {
        Change::Date(d) if d.len() == 16 => d.to_string(),
        _ => now_amz_date(),
    };
    let auth = authorization(host, path, &date, &payload);
    let mut req = reqwest::Client::new().put(format!("{}{}", url, path)).header("x-amz-content-sha256", &payload).body(body);
    req = match change {
        Change::Authorization(a) => req.header("authorization", a).header("x-amz-date", &date),
        Change::NoDate => req.header("authorization", &auth),
        Change::Date(d) => req.header("authorization", &auth).header("x-amz-date", d),
        Change::UserAgent(ua) => req.header("authorization", &auth).header("x-amz-date", &date).header("user-agent", ua),
        Change::None => req.header("authorization", &auth).header("x-amz-date", &date),
    };
    let res = req.send().await.unwrap();
    let status = res.status();
    (status, res.text().await.unwrap())
}

fn code(xml: &str) -> &str {
    xml.split("<Code>").nth(1).and_then(|s| s.split("</Code>").next()).unwrap_or("")
}

async fn with_bucket(name: &str) -> (String, String, s3_server::ServerHandle) {
    let (url, host, h) = server().await;
    let (st, body) = put(&url, &host, &format!("/{}", name), b"", Change::None).await;
    assert_eq!(st, StatusCode::OK, "a signed CreateBucket must work: {}", body);
    (url, host, h)
}

async fn assert_refused(url: &str, host: &str, path: &str, change: Change<'_>, status: StatusCode, want: &str) {
    let body: &'static [u8] = if path.matches('/').count() > 1 { b"bar" } else { b"" };
    let (st, xml) = put(url, host, path, body, change).await;
    assert_eq!((st, code(&xml)), (status, want), "{}", xml);
}

// --- Authorization header that is not SigV4 at all ---
// The suite sends `AWS HAHAHA` and expects 400 InvalidArgument. The SigV4 form of an
// unreadable header is 400 AuthorizationHeaderMalformed.

#[tokio::test]
async fn test_object_create_bad_authorization_invalid_aws2() {
    let (url, host, _h) = with_bucket("port-auth-obj").await;
    assert_refused(&url, &host, "/port-auth-obj/foo", Change::Authorization("AWS4-HMAC-SHA256 HAHAHA"), StatusCode::BAD_REQUEST, "AuthorizationHeaderMalformed").await;
    assert_refused(&url, &host, "/port-auth-obj/foo", Change::Authorization("AWS HAHAHA"), StatusCode::BAD_REQUEST, "AuthorizationHeaderMalformed").await;
}

#[tokio::test]
async fn test_bucket_create_bad_authorization_invalid_aws2() {
    let (url, host, _h) = server().await;
    assert_refused(&url, &host, "/port-auth-bkt", Change::Authorization("AWS4-HMAC-SHA256 HAHAHA"), StatusCode::BAD_REQUEST, "AuthorizationHeaderMalformed").await;
    assert_refused(&url, &host, "/port-auth-bkt", Change::Authorization("AWS HAHAHA"), StatusCode::BAD_REQUEST, "AuthorizationHeaderMalformed").await;
}

// --- User-Agent: empty or absent, the request still succeeds ---

#[tokio::test]
async fn test_object_create_bad_ua_empty_aws2() {
    let (url, host, _h) = with_bucket("port-ua-obj").await;
    let (st, body) = put(&url, &host, "/port-ua-obj/foo", b"bar", Change::UserAgent("")).await;
    assert_eq!(st, StatusCode::OK, "{}", body);
}

#[tokio::test]
async fn test_object_create_bad_ua_none_aws2() {
    // reqwest sends no User-Agent unless one is set.
    let (url, host, _h) = with_bucket("port-noua-obj").await;
    let (st, body) = put(&url, &host, "/port-noua-obj/foo", b"bar", Change::None).await;
    assert_eq!(st, StatusCode::OK, "{}", body);
}

#[tokio::test]
async fn test_bucket_create_bad_ua_empty_aws2() {
    let (url, host, _h) = server().await;
    let (st, body) = put(&url, &host, "/port-ua-bkt", b"", Change::UserAgent("")).await;
    assert_eq!(st, StatusCode::OK, "{}", body);
}

#[tokio::test]
async fn test_bucket_create_bad_ua_none_aws2() {
    let (url, host, _h) = server().await;
    let (st, body) = put(&url, &host, "/port-noua-bkt", b"", Change::None).await;
    assert_eq!(st, StatusCode::OK, "{}", body);
}

// --- x-amz-date that is unreadable, empty, absent or out of range ---
// The suite (SigV2, against RGW) expects 403 AccessDenied for an unreadable, empty, absent or
// pre-1970 date. AwanFS answers 400 AuthorizationHeaderMalformed, as MinIO answers 400;
// AWS's documentation names no status for it (#433). A date in 9999 is a readable date far
// from now: 403 RequestTimeTooSkewed, as the suite expects.

#[tokio::test]
async fn test_object_create_bad_date_invalid_aws2() {
    let (url, host, _h) = with_bucket("port-date-obj1").await;
    assert_refused(&url, &host, "/port-date-obj1/foo", Change::Date("Bad Date"), StatusCode::BAD_REQUEST, "AuthorizationHeaderMalformed").await;
}

#[tokio::test]
async fn test_object_create_bad_date_empty_aws2() {
    let (url, host, _h) = with_bucket("port-date-obj2").await;
    assert_refused(&url, &host, "/port-date-obj2/foo", Change::Date(""), StatusCode::BAD_REQUEST, "AuthorizationHeaderMalformed").await;
}

#[tokio::test]
async fn test_object_create_bad_date_none_aws2() {
    let (url, host, _h) = with_bucket("port-date-obj3").await;
    assert_refused(&url, &host, "/port-date-obj3/foo", Change::NoDate, StatusCode::BAD_REQUEST, "AuthorizationHeaderMalformed").await;
}

#[tokio::test]
async fn test_object_create_bad_date_before_epoch_aws2() {
    let (url, host, _h) = with_bucket("port-date-obj4").await;
    assert_refused(&url, &host, "/port-date-obj4/foo", Change::Date("19500707T215304Z"), StatusCode::BAD_REQUEST, "AuthorizationHeaderMalformed").await;
}

#[tokio::test]
async fn test_object_create_bad_date_after_end_aws2() {
    let (url, host, _h) = with_bucket("port-date-obj5").await;
    assert_refused(&url, &host, "/port-date-obj5/foo", Change::Date("99990707T215304Z"), StatusCode::FORBIDDEN, "RequestTimeTooSkewed").await;
}

#[tokio::test]
async fn test_bucket_create_bad_date_invalid_aws2() {
    let (url, host, _h) = server().await;
    assert_refused(&url, &host, "/port-date-bkt1", Change::Date("Bad Date"), StatusCode::BAD_REQUEST, "AuthorizationHeaderMalformed").await;
}

#[tokio::test]
async fn test_bucket_create_bad_date_empty_aws2() {
    let (url, host, _h) = server().await;
    assert_refused(&url, &host, "/port-date-bkt2", Change::Date(""), StatusCode::BAD_REQUEST, "AuthorizationHeaderMalformed").await;
}

#[tokio::test]
async fn test_bucket_create_bad_date_none_aws2() {
    let (url, host, _h) = server().await;
    assert_refused(&url, &host, "/port-date-bkt3", Change::NoDate, StatusCode::BAD_REQUEST, "AuthorizationHeaderMalformed").await;
}

#[tokio::test]
async fn test_bucket_create_bad_date_before_epoch_aws2() {
    let (url, host, _h) = server().await;
    assert_refused(&url, &host, "/port-date-bkt4", Change::Date("19500707T215304Z"), StatusCode::BAD_REQUEST, "AuthorizationHeaderMalformed").await;
}

#[test]
fn the_test_signer_formats_dates_like_aws() {
    assert_eq!(amz_date(1_369_353_600), "20130524T000000Z");
    assert_eq!(amz_date(0), "19700101T000000Z");
    assert_eq!(amz_date(951_782_400), "20000229T000000Z");
}
