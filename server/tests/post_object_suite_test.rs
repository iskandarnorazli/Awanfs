//! POST object (#119): AwanFS's own SigV4 version of each ceph s3-tests `test_post_object_*`
//! function (owner decision, #119 issuecomment-7191269534). The suite signs its forms with
//! SigV2, which AwanFS does not accept (CONFORMANCE_DESIGN.md section 9), so each test below
//! sends the same form with a SigV4 signature (the policy also covers the x-amz-algorithm,
//! x-amz-credential and x-amz-date fields, as AWS requires) and expects the suite's answer.
//! `tests/s3tests/PORTED.md` maps each suite test to its port here.
//!
//! The five tests that use a `public-read-write` bucket in the suite (canned ACLs are #122)
//! run against a server started with anonymous access instead.

use reqwest::StatusCode;
use s3_server::bucket_config::base64_encode;
use s3_server::request_log::clf_time;
use s3_server::{compute_sigv4_signature, sha256_hex, start_s3_reactor, AuthConfig, S3ServiceState, ServerHandle, SigV4KeyCache};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const AK: &str = "AKPOSTTEST000";
const SK: &str = "post-object-secret";
const BOUNDARY: &str = "awanfs0post0boundary";

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

/// `(yyyymmdd, HHMMSS)` of a Unix time.
fn ymd_hms(secs: u64) -> (String, String) {
    let t = clf_time(secs);
    let months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let m = months.iter().position(|x| *x == &t[3..6]).unwrap() + 1;
    (format!("{}{:02}{}", &t[7..11], m, &t[0..2]), format!("{}{}{}", &t[12..14], &t[15..17], &t[18..20]))
}

fn amz_date(secs: u64) -> String {
    let (d, t) = ymd_hms(secs);
    format!("{}T{}Z", d, t)
}

/// The suite's `expires.strftime("%Y-%m-%dT%H:%M:%SZ")`.
fn expiration(secs: u64) -> String {
    let (d, t) = ymd_hms(secs);
    format!("{}-{}-{}T{}:{}:{}Z", &d[0..4], &d[4..6], &d[6..8], &t[0..2], &t[2..4], &t[4..6])
}

struct Srv {
    host: String,
    _h: ServerHandle,
}

async fn server(auth: AuthConfig) -> Srv {
    let mut state = S3ServiceState::new();
    state.auth = auth;
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let h = start_s3_reactor(addr, Arc::new(state)).await.unwrap();
    Srv { host: h.local_addr.to_string(), _h: h }
}

async fn signed_server() -> Srv {
    server(AuthConfig { credentials: Some((AK.into(), SK.into())), allow_anonymous: false }).await
}

async fn anonymous_server() -> Srv {
    server(AuthConfig::open()).await
}

/// A SigV4 header-signed request, for the bucket set-up and the read-backs.
fn signed(c: &reqwest::Client, method: reqwest::Method, host: &str, path: &str, query: &str, body: &[u8]) -> reqwest::RequestBuilder {
    let date = amz_date(now());
    let hash = sha256_hex(body);
    let canonical = format!(
        "{}\n{}\n{}\nhost:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n\nhost;x-amz-content-sha256;x-amz-date\n{}",
        method.as_str(),
        path,
        query,
        host,
        hash,
        date,
        hash
    );
    let scope = format!("{}/us-east-1/s3/aws4_request", &date[..8]);
    let sts = format!("AWS4-HMAC-SHA256\n{}\n{}\n{}", date, scope, sha256_hex(canonical.as_bytes()));
    let sig = compute_sigv4_signature(&SigV4KeyCache::new(), SK, &date[..8], "us-east-1", "s3", &sts);
    let url = if query.is_empty() { format!("http://{}{}", host, path) } else { format!("http://{}{}?{}", host, path, query) };
    c.request(method, url)
        .header("x-amz-date", date)
        .header("x-amz-content-sha256", hash)
        .header(
            "authorization",
            format!("AWS4-HMAC-SHA256 Credential={}/{},SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature={}", AK, scope, sig),
        )
        .body(body.to_vec())
}

/// The form's SigV4 fields and the conditions that cover them.
struct Signer {
    ak: String,
    sk: String,
    date: String,
}

impl Signer {
    fn new() -> Self {
        Signer { ak: AK.into(), sk: SK.into(), date: amz_date(now()) }
    }

    fn credential(&self) -> String {
        format!("{}/{}/us-east-1/s3/aws4_request", self.ak, &self.date[..8])
    }

    fn conditions(&self) -> String {
        format!(
            r#"{{"x-amz-algorithm": "AWS4-HMAC-SHA256"}}, {{"x-amz-credential": "{}"}}, {{"x-amz-date": "{}"}}"#,
            self.credential(),
            self.date
        )
    }

    fn signature(&self, policy_b64: &str) -> String {
        compute_sigv4_signature(&SigV4KeyCache::new(), &self.sk, &self.date[..8], "us-east-1", "s3", policy_b64)
    }

    /// The signature fields for a policy, in the suite's place for them (after `key`).
    fn fields(&self, policy_b64: &str) -> Vec<(String, String)> {
        vec![
            ("x-amz-algorithm".into(), "AWS4-HMAC-SHA256".into()),
            ("x-amz-credential".into(), self.credential()),
            ("x-amz-date".into(), self.date.clone()),
            ("x-amz-signature".into(), self.signature(policy_b64)),
            ("policy".into(), policy_b64.into()),
        ]
    }
}

fn b64(json: &str) -> String {
    base64_encode(json.as_bytes())
}

/// The suite's usual policy document, with the SigV4 conditions added.
fn policy(s: &Signer, bucket: &str, extra: &str, exp_secs: i64) -> String {
    let exp = expiration((now() as i64 + exp_secs) as u64);
    let extra = if extra.is_empty() { String::new() } else { format!(", {}", extra) };
    b64(&format!(
        r#"{{"expiration": "{}", "conditions": [{{"bucket": "{}"}}, {}{}]}}"#,
        exp,
        bucket,
        s.conditions(),
        extra
    ))
}

const STD: &str = r#"["starts-with", "$key", "foo"], {"acl": "private"}, ["starts-with", "$Content-Type", "text/plain"], ["content-length-range", 0, 1024]"#;

fn field(k: &str, v: &str) -> (String, String) {
    (k.to_string(), v.to_string())
}

/// `multipart/form-data` as Python's `requests` builds it: each field, then the file part
/// (`filename="file"` when no name is given, as `requests` does for a bare string).
fn form_body(fields: &[(String, String)], filename: Option<&str>, file: &[u8]) -> Vec<u8> {
    let mut b = Vec::new();
    for (k, v) in fields {
        b.extend_from_slice(format!("--{}\r\nContent-Disposition: form-data; name=\"{}\"\r\n\r\n{}\r\n", BOUNDARY, k, v).as_bytes());
    }
    b.extend_from_slice(
        format!(
            "--{}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{}\"\r\n\r\n",
            BOUNDARY,
            filename.unwrap_or("file")
        )
        .as_bytes(),
    );
    b.extend_from_slice(file);
    b.extend_from_slice(format!("\r\n--{}--\r\n", BOUNDARY).as_bytes());
    b
}

async fn post(c: &reqwest::Client, host: &str, bucket: &str, body: Vec<u8>) -> reqwest::Response {
    c.post(format!("http://{}/{}", host, bucket))
        .header("content-type", format!("multipart/form-data; boundary={}", BOUNDARY))
        .body(body)
        .send()
        .await
        .unwrap()
}

async fn make_bucket(c: &reqwest::Client, srv: &Srv, bucket: &str) {
    let r = signed(c, reqwest::Method::PUT, &srv.host, &format!("/{}", bucket), "", b"").send().await.unwrap();
    assert_eq!(r.status(), StatusCode::OK, "create {}", bucket);
}

async fn get_body(c: &reqwest::Client, srv: &Srv, bucket: &str, key_path: &str) -> (StatusCode, Vec<u8>) {
    let r = signed(c, reqwest::Method::GET, &srv.host, &format!("/{}/{}", bucket, key_path), "", b"").send().await.unwrap();
    (r.status(), r.bytes().await.unwrap().to_vec())
}

/// The suite's standard signed form for `foo.txt`, with `extra` fields before the file.
fn std_form(s: &Signer, policy_b64: &str, key: &str, extra: &[(String, String)]) -> Vec<(String, String)> {
    let mut f = vec![field("key", key)];
    f.extend(s.fields(policy_b64));
    f.push(field("acl", "private"));
    f.push(field("Content-Type", "text/plain"));
    f.extend(extra.iter().cloned());
    f
}

// ---- The suite's tests, one port each ------------------------------------------------------

#[tokio::test]
async fn test_post_object_anonymous_request() {
    let srv = anonymous_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-anon").await;
    let fields = vec![field("key", "foo.txt"), field("acl", "public-read"), field("Content-Type", "text/plain")];
    assert_eq!(post(&c, &srv.host, "post-anon", form_body(&fields, None, b"bar")).await.status(), StatusCode::NO_CONTENT);
    assert_eq!(get_body(&c, &srv, "post-anon", "foo.txt").await, (StatusCode::OK, b"bar".to_vec()));
}

// covers: PostObject
#[tokio::test]
async fn test_post_object_authenticated_request() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-auth").await;
    let s = Signer::new();
    let p = policy(&s, "post-auth", STD, 6000);
    let r = post(&c, &srv.host, "post-auth", form_body(&std_form(&s, &p, "foo.txt", &[]), None, b"bar")).await;
    assert_eq!(r.status(), StatusCode::NO_CONTENT);
    assert_eq!(get_body(&c, &srv, "post-auth", "foo.txt").await, (StatusCode::OK, b"bar".to_vec()));
}

#[tokio::test]
async fn test_post_object_authenticated_no_content_type() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-noct").await;
    let s = Signer::new();
    let p = policy(&s, "post-noct", r#"["starts-with", "$key", "foo"], {"acl": "private"}, ["content-length-range", 0, 1024]"#, 6000);
    let mut fields = vec![field("key", "foo.txt")];
    fields.extend(s.fields(&p));
    fields.push(field("acl", "private"));
    assert_eq!(post(&c, &srv.host, "post-noct", form_body(&fields, None, b"bar")).await.status(), StatusCode::NO_CONTENT);
    assert_eq!(get_body(&c, &srv, "post-noct", "foo.txt").await, (StatusCode::OK, b"bar".to_vec()));
}

#[tokio::test]
async fn test_post_object_authenticated_request_bad_access_key() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-badak").await;
    let s = Signer { ak: "foo".into(), ..Signer::new() };
    let p = policy(&s, "post-badak", STD, 6000);
    let r = post(&c, &srv.host, "post-badak", form_body(&std_form(&s, &p, "foo.txt", &[]), None, b"bar")).await;
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
    assert!(r.text().await.unwrap().contains("InvalidAccessKeyId"));
    assert_eq!(get_body(&c, &srv, "post-badak", "foo.txt").await.0, StatusCode::NOT_FOUND, "nothing stored");
}

#[tokio::test]
async fn test_post_object_set_success_code() {
    let srv = anonymous_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-201").await;
    let fields = vec![
        field("key", "foo.txt"),
        field("acl", "public-read"),
        field("success_action_status", "201"),
        field("Content-Type", "text/plain"),
    ];
    let r = post(&c, &srv.host, "post-201", form_body(&fields, None, b"bar")).await;
    assert_eq!(r.status(), StatusCode::CREATED);
    let xml = r.text().await.unwrap();
    assert!(xml.contains("<Key>foo.txt</Key>"), "{}", xml);
    assert!(xml.contains("<Bucket>post-201</Bucket>"), "{}", xml);
}

#[tokio::test]
async fn test_post_object_set_invalid_success_code() {
    let srv = anonymous_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-404").await;
    let fields = vec![
        field("key", "foo.txt"),
        field("acl", "public-read"),
        field("success_action_status", "404"),
        field("Content-Type", "text/plain"),
    ];
    let r = post(&c, &srv.host, "post-404", form_body(&fields, None, b"bar")).await;
    assert_eq!(r.status(), StatusCode::NO_CONTENT);
    assert_eq!(r.text().await.unwrap(), "");
}

#[tokio::test]
async fn test_post_object_upload_larger_than_chunk() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-3mib").await;
    let s = Signer::new();
    let p = policy(
        &s,
        "post-3mib",
        r#"["starts-with", "$key", "foo"], {"acl": "private"}, ["starts-with", "$Content-Type", "text/plain"], ["content-length-range", 0, 5242880]"#,
        6000,
    );
    let foo = "foo".repeat(1024 * 1024).into_bytes();
    let r = post(&c, &srv.host, "post-3mib", form_body(&std_form(&s, &p, "foo.txt", &[]), None, &foo)).await;
    assert_eq!(r.status(), StatusCode::NO_CONTENT);
    assert_eq!(get_body(&c, &srv, "post-3mib", "foo.txt").await, (StatusCode::OK, foo));
}

#[tokio::test]
async fn test_post_object_set_key_from_filename() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-fname").await;
    let s = Signer::new();
    let p = policy(&s, "post-fname", STD, 6000);
    let r = post(&c, &srv.host, "post-fname", form_body(&std_form(&s, &p, "${filename}", &[]), Some("foo.txt"), b"bar")).await;
    assert_eq!(r.status(), StatusCode::NO_CONTENT);
    assert_eq!(get_body(&c, &srv, "post-fname", "foo.txt").await, (StatusCode::OK, b"bar".to_vec()));
}

#[tokio::test]
async fn test_post_object_ignored_header() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-ignore").await;
    let s = Signer::new();
    let p = policy(&s, "post-ignore", STD, 6000);
    let form = std_form(&s, &p, "foo.txt", &[field("x-ignore-foo", "bar")]);
    assert_eq!(post(&c, &srv.host, "post-ignore", form_body(&form, None, b"bar")).await.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn test_post_object_case_insensitive_condition_fields() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-case").await;
    let s = Signer::new();
    let exp = expiration(now() + 6000);
    let p = b64(&format!(
        r#"{{"expiration": "{}", "conditions": [{{"bUcKeT": "post-case"}}, ["StArTs-WiTh", "$KeY", "foo"], {{"AcL": "private"}}, ["StArTs-WiTh", "$CoNtEnT-TyPe", "text/plain"], ["content-length-range", 0, 1024], {}]}}"#,
        exp,
        s.conditions()
    ));
    let mut fields = vec![field("kEy", "foo.txt")];
    for (k, v) in s.fields(&p) {
        fields.push(if k == "policy" { field("pOLICy", &v) } else { (k, v) });
    }
    fields.push(field("aCl", "private"));
    fields.push(field("Content-Type", "text/plain"));
    assert_eq!(post(&c, &srv.host, "post-case", form_body(&fields, None, b"bar")).await.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn test_post_object_escaped_field_values() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-esc").await;
    let s = Signer::new();
    let p = policy(
        &s,
        "post-esc",
        r#"["starts-with", "$key", "\\$foo"], {"acl": "private"}, ["starts-with", "$Content-Type", "text/plain"], ["content-length-range", 0, 1024]"#,
        6000,
    );
    let r = post(&c, &srv.host, "post-esc", form_body(&std_form(&s, &p, r"\$foo.txt", &[]), None, b"bar")).await;
    assert_eq!(r.status(), StatusCode::NO_CONTENT);
    assert_eq!(get_body(&c, &srv, "post-esc", "%5C%24foo.txt").await, (StatusCode::OK, b"bar".to_vec()));
}

#[tokio::test]
async fn test_post_object_success_redirect_action() {
    let srv = signed_server().await;
    let c = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();
    make_bucket(&c, &srv, "post-redir").await;
    let redirect_url = format!("http://{}/post-redir", srv.host);
    let s = Signer::new();
    let p = policy(&s, "post-redir", &format!(r#"{}, ["eq", "$success_action_redirect", "{}"]"#, STD, redirect_url), 6000);
    let form = std_form(&s, &p, "foo.txt", &[field("success_action_redirect", &redirect_url)]);
    let r = post(&c, &srv.host, "post-redir", form_body(&form, None, b"bar")).await;
    assert_eq!(r.status(), StatusCode::SEE_OTHER);
    let location = r.headers().get("location").unwrap().to_str().unwrap().to_string();
    let head = signed(&c, reqwest::Method::HEAD, &srv.host, "/post-redir/foo.txt", "", b"").send().await.unwrap();
    let etag = head.headers().get("etag").unwrap().to_str().unwrap().trim_matches('"').to_string();
    assert_eq!(location, format!("{}?bucket=post-redir&key=foo.txt&etag=%22{}%22", redirect_url, etag));
}

#[tokio::test]
async fn test_post_object_invalid_signature() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-badsig").await;
    let s = Signer::new();
    let p = policy(&s, "post-badsig", STD, 6000);
    let mut form = std_form(&s, &p, "foo.txt", &[]);
    for (k, v) in form.iter_mut() {
        if k.as_str() == "x-amz-signature" {
            *v = v.chars().rev().collect();
        }
    }
    let r = post(&c, &srv.host, "post-badsig", form_body(&form, None, b"bar")).await;
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
    assert!(r.text().await.unwrap().contains("SignatureDoesNotMatch"));
}

#[tokio::test]
async fn test_post_object_invalid_access_key() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-revak").await;
    let s = Signer { ak: AK.chars().rev().collect(), ..Signer::new() };
    let p = policy(&s, "post-revak", STD, 6000);
    let r = post(&c, &srv.host, "post-revak", form_body(&std_form(&s, &p, "foo.txt", &[]), None, b"bar")).await;
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_post_object_invalid_date_format() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-baddate").await;
    let s = Signer::new();
    // Python's str(datetime): "2026-11-26 13:00:00.123456+00:00".
    let p = b64(&format!(
        r#"{{"expiration": "2099-11-26 13:00:00.123456+00:00", "conditions": [{{"bucket": "post-baddate"}}, {}, {}]}}"#,
        s.conditions(),
        STD
    ));
    let r = post(&c, &srv.host, "post-baddate", form_body(&std_form(&s, &p, "foo.txt", &[]), None, b"bar")).await;
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_post_object_no_key_specified() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-nokey").await;
    let s = Signer::new();
    let p = policy(&s, "post-nokey", r#"{"acl": "private"}, ["starts-with", "$Content-Type", "text/plain"], ["content-length-range", 0, 1024]"#, 6000);
    let mut fields = s.fields(&p);
    fields.push(field("acl", "private"));
    fields.push(field("Content-Type", "text/plain"));
    assert_eq!(post(&c, &srv.host, "post-nokey", form_body(&fields, None, b"bar")).await.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_post_object_missing_signature() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-nosig").await;
    let s = Signer::new();
    let p = policy(&s, "post-nosig", STD, 6000);
    let form: Vec<_> = std_form(&s, &p, "foo.txt", &[]).into_iter().filter(|(k, _)| k != "x-amz-signature").collect();
    assert_eq!(post(&c, &srv.host, "post-nosig", form_body(&form, None, b"bar")).await.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_post_object_missing_policy_condition() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-nobcond").await;
    let s = Signer::new();
    let p = b64(&format!(r#"{{"expiration": "{}", "conditions": [{}, {}]}}"#, expiration(now() + 6000), s.conditions(), STD));
    let r = post(&c, &srv.host, "post-nobcond", form_body(&std_form(&s, &p, "foo.txt", &[]), None, b"bar")).await;
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
    assert_eq!(get_body(&c, &srv, "post-nobcond", "foo.txt").await.0, StatusCode::NOT_FOUND, "refused before storing");
}

#[tokio::test]
async fn test_post_object_user_specified_header() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-meta").await;
    let s = Signer::new();
    let p = policy(&s, "post-meta", &format!(r#"{}, ["starts-with", "$x-amz-meta-foo", "bar"]"#, STD), 6000);
    let r = post(&c, &srv.host, "post-meta", form_body(&std_form(&s, &p, "foo.txt", &[field("x-amz-meta-foo", "barclamp")]), None, b"bar")).await;
    assert_eq!(r.status(), StatusCode::NO_CONTENT);
    let got = signed(&c, reqwest::Method::GET, &srv.host, "/post-meta/foo.txt", "", b"").send().await.unwrap();
    assert_eq!(got.headers().get("x-amz-meta-foo").unwrap(), "barclamp");
    assert_eq!(got.headers().get("content-type").unwrap(), "text/plain");
}

#[tokio::test]
async fn test_post_object_request_missing_policy_specified_field() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-missf").await;
    let s = Signer::new();
    let p = policy(&s, "post-missf", &format!(r#"{}, ["starts-with", "$x-amz-meta-foo", "bar"]"#, STD), 6000);
    let r = post(&c, &srv.host, "post-missf", form_body(&std_form(&s, &p, "foo.txt", &[]), None, b"bar")).await;
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_post_object_condition_is_case_sensitive() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-condcase").await;
    let s = Signer::new();
    let p = b64(&format!(
        r#"{{"expiration": "{}", "CONDITIONS": [{{"bucket": "post-condcase"}}, {}, {}]}}"#,
        expiration(now() + 6000),
        s.conditions(),
        STD
    ));
    let r = post(&c, &srv.host, "post-condcase", form_body(&std_form(&s, &p, "foo.txt", &[]), None, b"bar")).await;
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_post_object_expires_is_case_sensitive() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-expcase").await;
    let s = Signer::new();
    let p = b64(&format!(
        r#"{{"EXPIRATION": "{}", "conditions": [{{"bucket": "post-expcase"}}, {}, {}]}}"#,
        expiration(now() + 6000),
        s.conditions(),
        STD
    ));
    let r = post(&c, &srv.host, "post-expcase", form_body(&std_form(&s, &p, "foo.txt", &[]), None, b"bar")).await;
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_post_object_expired_policy() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-expired").await;
    let s = Signer::new();
    let p = policy(&s, "post-expired", STD, -6000);
    let r = post(&c, &srv.host, "post-expired", form_body(&std_form(&s, &p, "foo.txt", &[]), None, b"bar")).await;
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
    assert_eq!(get_body(&c, &srv, "post-expired", "foo.txt").await.0, StatusCode::NOT_FOUND, "refused before storing");
}

#[tokio::test]
async fn test_post_object_wrong_bucket() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-right").await;
    make_bucket(&c, &srv, "post-wrong").await;
    let s = Signer::new();
    let p = policy(&s, "post-right", STD, 6000);
    let mut form = vec![field("key", "${filename}"), field("bucket", "post-right")];
    form.extend(s.fields(&p));
    form.push(field("acl", "private"));
    form.push(field("Content-Type", "text/plain"));
    let r = post(&c, &srv.host, "post-wrong", form_body(&form, Some("foo.txt"), b"bar")).await;
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
    assert_eq!(get_body(&c, &srv, "post-wrong", "foo.txt").await.0, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_post_object_invalid_request_field_value() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-badval").await;
    let s = Signer::new();
    let p = policy(&s, "post-badval", &format!(r#"{}, ["eq", "$x-amz-meta-foo", ""]"#, STD), 6000);
    let r = post(&c, &srv.host, "post-badval", form_body(&std_form(&s, &p, "foo.txt", &[field("x-amz-meta-foo", "barclamp")]), None, b"bar")).await;
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_post_object_missing_expires_condition() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-noexp").await;
    let s = Signer::new();
    let p = b64(&format!(r#"{{"conditions": [{{"bucket": "post-noexp"}}, {}, {}]}}"#, s.conditions(), STD));
    let r = post(&c, &srv.host, "post-noexp", form_body(&std_form(&s, &p, "foo.txt", &[]), None, b"bar")).await;
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_post_object_missing_conditions_list() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-noconds").await;
    let s = Signer::new();
    let p = b64(&format!(r#"{{"expiration": "{}"}}"#, expiration(now() + 6000)));
    let r = post(&c, &srv.host, "post-noconds", form_body(&std_form(&s, &p, "foo.txt", &[]), None, b"bar")).await;
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
}

/// The suite's size tests differ only in the range and its expected answer.
async fn size_case(bucket: &str, range: &str, file: &[u8], want: StatusCode) {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, bucket).await;
    let s = Signer::new();
    let p = policy(
        &s,
        bucket,
        &format!(r#"["starts-with", "$key", "foo"], {{"acl": "private"}}, ["starts-with", "$Content-Type", "text/plain"], {}"#, range),
        6000,
    );
    let r = post(&c, &srv.host, bucket, form_body(&std_form(&s, &p, "foo.txt", &[]), None, file)).await;
    assert_eq!(r.status(), want, "{} with {} bytes", range, file.len());
    if want != StatusCode::NO_CONTENT {
        assert_eq!(get_body(&c, &srv, bucket, "foo.txt").await.0, StatusCode::NOT_FOUND, "a refused upload leaves no object");
    }
}

#[tokio::test]
async fn test_post_object_upload_size_limit_exceeded() {
    size_case("post-toobig", r#"["content-length-range", 0, 0]"#, b"bar", StatusCode::BAD_REQUEST).await;
}

#[tokio::test]
async fn test_post_object_missing_content_length_argument() {
    size_case("post-range1", r#"["content-length-range", 0]"#, b"bar", StatusCode::BAD_REQUEST).await;
}

#[tokio::test]
async fn test_post_object_invalid_content_length_argument() {
    size_case("post-rangeneg", r#"["content-length-range", -1, 0]"#, b"bar", StatusCode::BAD_REQUEST).await;
}

#[tokio::test]
async fn test_post_object_upload_size_below_minimum() {
    size_case("post-toosmall", r#"["content-length-range", 512, 1000]"#, b"bar", StatusCode::BAD_REQUEST).await;
}

#[tokio::test]
async fn test_post_object_upload_size_rgw_chunk_size_bug() {
    // 4 MiB + 200 bytes inside [4 MiB, 12 MiB]: above 4 MiB, so the file streams.
    let mib4 = 4 * 1024 * 1024;
    let file = vec![b'x'; mib4 + 200];
    size_case("post-rgwbug", &format!(r#"["content-length-range", {}, {}]"#, mib4, mib4 * 3), &file, StatusCode::NO_CONTENT).await;
}

#[tokio::test]
async fn test_post_object_empty_conditions() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-emptyc").await;
    let s = Signer::new();
    let p = b64(&format!(r#"{{"expiration": "{}", "conditions": [{{}}]}}"#, expiration(now() + 6000)));
    let r = post(&c, &srv.host, "post-emptyc", form_body(&std_form(&s, &p, "foo.txt", &[]), None, b"bar")).await;
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
}

const TAGS: &str = "<Tagging><TagSet><Tag><Key>0</Key><Value>0</Value></Tag><Tag><Key>1</Key><Value>1</Value></Tag></TagSet></Tagging>";

#[tokio::test]
async fn test_post_object_tags_anonymous_request() {
    let srv = anonymous_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-tagsanon").await;
    let fields = vec![
        field("key", "foo.txt"),
        field("acl", "public-read"),
        field("Content-Type", "text/plain"),
        field("tagging", TAGS),
    ];
    assert_eq!(post(&c, &srv.host, "post-tagsanon", form_body(&fields, None, b"bar")).await.status(), StatusCode::NO_CONTENT);
    assert_eq!(get_body(&c, &srv, "post-tagsanon", "foo.txt").await, (StatusCode::OK, b"bar".to_vec()));
    let tags = c.get(format!("http://{}/post-tagsanon/foo.txt?tagging", srv.host)).send().await.unwrap().text().await.unwrap();
    for (k, v) in [("0", "0"), ("1", "1")] {
        assert!(tags.contains(&format!("<Key>{}</Key>", k)) && tags.contains(&format!("<Value>{}</Value>", v)), "{}", tags);
    }
}

#[tokio::test]
async fn test_post_object_tags_authenticated_request() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-tagsauth").await;
    let s = Signer::new();
    let p = policy(&s, "post-tagsauth", &format!(r#"{}, ["starts-with", "$tagging", ""]"#, STD), 6000);
    let r = post(&c, &srv.host, "post-tagsauth", form_body(&std_form(&s, &p, "foo.txt", &[field("tagging", TAGS)]), None, b"bar")).await;
    assert_eq!(r.status(), StatusCode::NO_CONTENT);
    assert_eq!(get_body(&c, &srv, "post-tagsauth", "foo.txt").await, (StatusCode::OK, b"bar".to_vec()));
    let tags = signed(&c, reqwest::Method::GET, &srv.host, "/post-tagsauth/foo.txt", "tagging=", b"").send().await.unwrap().text().await.unwrap();
    assert!(tags.contains("<Key>1</Key>"), "{}", tags);
}

#[tokio::test]
async fn test_post_object_upload_checksum() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-cksum").await;
    let s = Signer::new();
    let p = policy(
        &s,
        "post-cksum",
        r#"["starts-with", "$key", "foo_cksum_test"], {"acl": "private"}, ["starts-with", "$Content-Type", "text/plain"], ["content-length-range", 0, 5242880]"#,
        6000,
    );
    let file = vec![b'x'; 2 * 1024 * 1024];
    let good = std_form(&s, &p, "foo_cksum_test.txt", &[field("x-amz-checksum-sha256", "aTL9MeXa9HObn6eP93eygxsJlcwdCwCTysgGAZAgE7w=")]);
    assert_eq!(post(&c, &srv.host, "post-cksum", form_body(&good, None, &file)).await.status(), StatusCode::NO_CONTENT);
    let bad = std_form(&s, &p, "foo_cksum_test.txt", &[field("x-amz-checksum-sha256", "sailorjerry")]);
    assert_eq!(post(&c, &srv.host, "post-cksum", form_body(&bad, None, &file)).await.status(), StatusCode::BAD_REQUEST);
}

// ---- The issue's invariants, beyond the suite ----------------------------------------------

/// A form signed the SigV2 way is refused, and nothing is stored.
#[tokio::test]
async fn a_sigv2_signed_form_is_refused() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-sigv2").await;
    let fields = vec![
        field("key", "foo.txt"),
        field("AWSAccessKeyId", AK),
        field("acl", "private"),
        field("signature", "c2lnbmF0dXJl"),
        field("policy", &b64(r#"{"expiration": "2099-01-01T00:00:00Z", "conditions": [{"bucket": "post-sigv2"}]}"#)),
    ];
    let r = post(&c, &srv.host, "post-sigv2", form_body(&fields, None, b"bar")).await;
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
    assert_eq!(get_body(&c, &srv, "post-sigv2", "foo.txt").await.0, StatusCode::NOT_FOUND);
}

/// An unsigned form is refused when the server does not allow anonymous requests.
#[tokio::test]
async fn an_unsigned_form_needs_anonymous_access() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-unsigned").await;
    let fields = vec![field("key", "foo.txt")];
    assert_eq!(post(&c, &srv.host, "post-unsigned", form_body(&fields, None, b"bar")).await.status(), StatusCode::FORBIDDEN);
    assert_eq!(get_body(&c, &srv, "post-unsigned", "foo.txt").await.0, StatusCode::NOT_FOUND);
}

/// A streamed file (above 4 MiB) whose body does not end where Content-Length says is
/// refused, and no object is left: here an epilogue after the closing delimiter makes the
/// file shorter than its predicted size.
#[tokio::test]
async fn a_streamed_file_that_does_not_end_at_its_predicted_size_leaves_no_object() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-epilogue").await;
    let s = Signer::new();
    let p = policy(&s, "post-epilogue", r#"["starts-with", "$key", "foo"], {"acl": "private"}, ["starts-with", "$Content-Type", "text/plain"]"#, 6000);
    let mut body = form_body(&std_form(&s, &p, "foo.txt", &[]), None, &vec![b'y'; 5 * 1024 * 1024]);
    body.extend_from_slice(b"an epilogue the server does not expect");
    let r = post(&c, &srv.host, "post-epilogue", body).await;
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
    assert_eq!(get_body(&c, &srv, "post-epilogue", "foo.txt").await.0, StatusCode::NOT_FOUND);
}

/// A streamed file larger than the policy allows is refused before it is stored. The server
/// answers before reading the rest of the body, so the client may see the connection close
/// instead of the 400; either way no object is left.
#[tokio::test]
async fn a_streamed_file_above_the_range_is_refused_before_storing() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-bigstream").await;
    let s = Signer::new();
    let mib5 = 5 * 1024 * 1024;
    let p = policy(
        &s,
        "post-bigstream",
        &format!(r#"["starts-with", "$key", "foo"], {{"acl": "private"}}, ["starts-with", "$Content-Type", "text/plain"], ["content-length-range", 0, {}]"#, mib5),
        6000,
    );
    let body = form_body(&std_form(&s, &p, "foo.txt", &[]), None, &vec![b'z'; mib5 + 1]);
    let sent = c
        .post(format!("http://{}/post-bigstream", srv.host))
        .header("content-type", format!("multipart/form-data; boundary={}", BOUNDARY))
        .body(body)
        .send()
        .await;
    if let Ok(r) = sent {
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);
        assert!(r.text().await.unwrap_or_default().contains("EntityTooLarge"));
    }
    assert_eq!(get_body(&c, &srv, "post-bigstream", "foo.txt").await.0, StatusCode::NOT_FOUND);
}

/// A form whose file part is followed by another field is malformed and stores nothing.
#[tokio::test]
async fn a_field_after_the_file_is_malformed() {
    let srv = anonymous_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-after").await;
    let mut body = Vec::new();
    body.extend_from_slice(format!("--{b}\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\nfoo.txt\r\n", b = BOUNDARY).as_bytes());
    body.extend_from_slice(format!("--{b}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"f\"\r\n\r\nbar\r\n", b = BOUNDARY).as_bytes());
    body.extend_from_slice(format!("--{b}\r\nContent-Disposition: form-data; name=\"acl\"\r\n\r\nprivate\r\n--{b}--\r\n", b = BOUNDARY).as_bytes());
    let r = post(&c, &srv.host, "post-after", body).await;
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
    assert_eq!(get_body(&c, &srv, "post-after", "foo.txt").await.0, StatusCode::NOT_FOUND);
}

/// A POST to a bucket that is not a form is still not an operation.
#[tokio::test]
async fn a_post_to_a_bucket_without_a_form_is_method_not_allowed() {
    let srv = anonymous_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-notform").await;
    let r = c.post(format!("http://{}/post-notform", srv.host)).body("x").send().await.unwrap();
    assert_eq!(r.status(), StatusCode::METHOD_NOT_ALLOWED);
}

// ---- Bounds found by the review on #119 (issuecomment after 7194136864) --------------------

/// Sends a form's head and `body` with a declared Content-Length, and returns the status the
/// server answers with, without sending the rest of what was declared.
async fn raw_post(host: &str, bucket: &str, declared: u64, extra: &str, body: &[u8], trickle: Option<&[u8]>) -> u16 {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(host).await.unwrap();
    let head = format!(
        "POST /{} HTTP/1.1\r\nHost: {}\r\nContent-Type: multipart/form-data; boundary={}\r\nContent-Length: {}\r\n{}\r\n",
        bucket, host, BOUNDARY, declared, extra
    );
    s.write_all(head.as_bytes()).await.unwrap();
    s.write_all(body).await.unwrap();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    let mut extra_bytes = trickle.unwrap_or(&[]).iter();
    loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let text = String::from_utf8_lossy(&buf[..pos]).to_string();
            return text.split_whitespace().nth(1).unwrap().parse().unwrap();
        }
        assert!(tokio::time::Instant::now() < deadline, "no answer within 20 s");
        if let Some(b) = extra_bytes.next() {
            let _ = s.write_all(&[*b]).await;
        }
        match tokio::time::timeout(std::time::Duration::from_millis(100), s.read(&mut chunk)).await {
            Ok(Ok(0)) => panic!("connection closed without an answer"),
            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
            Ok(Err(e)) => panic!("read failed: {}", e),
            Err(_) => {}
        }
    }
}

/// A whole form that arrives in one frame with a much larger Content-Length (the rest would be
/// an epilogue) is charged for the bytes it holds, not the declared size, so it does not hold
/// the body budget: it is stored, and the next form goes through.
#[tokio::test]
async fn a_form_shorter_than_its_content_length_does_not_hold_the_budget() {
    let srv = anonymous_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-short").await;
    let body = form_body(&[field("key", "foo.txt")], None, b"bar");
    let status = raw_post(&srv.host, "post-short", body.len() as u64 + (1 << 20), "", &body, None).await;
    assert_eq!(status, 204);
    assert_eq!(get_body(&c, &srv, "post-short", "foo.txt").await, (StatusCode::OK, b"bar".to_vec()));
    let ok = post(&c, &srv.host, "post-short", form_body(&[field("key", "next.txt")], None, b"bar")).await;
    assert_eq!(ok.status(), StatusCode::NO_CONTENT);
}

/// Ruby's multipart-body gem (Mint's aws-sdk-ruby presignedPost) ends the body with
/// `--<boundary>--` and no CRLF. A file up to 4 MiB sent that way is stored byte for byte.
#[tokio::test]
async fn a_form_without_a_crlf_after_its_closing_delimiter_is_stored() {
    let srv = signed_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-nocrlf").await;
    let s = Signer::new();
    let p = policy(&s, "post-nocrlf", STD, 6000);
    let mut body = form_body(&std_form(&s, &p, "foo.txt", &[]), None, b"bar");
    assert!(body.ends_with(b"--\r\n"));
    body.truncate(body.len() - 2);
    let r = post(&c, &srv.host, "post-nocrlf", body).await;
    assert_eq!(r.status(), StatusCode::NO_CONTENT);
    assert_eq!(get_body(&c, &srv, "post-nocrlf", "foo.txt").await, (StatusCode::OK, b"bar".to_vec()));
}

/// A form whose file would be larger than 5 GiB is refused before the file is read, whether
/// or not it also sends `x-amz-decoded-content-length` (which validation used to trust).
#[tokio::test]
async fn a_form_with_a_file_above_5_gib_is_refused_before_reading_it() {
    let srv = anonymous_server().await;
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-huge").await;
    let mut head = Vec::new();
    head.extend_from_slice(format!("--{b}\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\nfoo.txt\r\n", b = BOUNDARY).as_bytes());
    head.extend_from_slice(format!("--{b}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"f\"\r\n\r\nxxxx", b = BOUNDARY).as_bytes());
    let gib5 = 5u64 << 30;
    assert_eq!(raw_post(&srv.host, "post-huge", gib5 + (8 << 20), "", &head, None).await, 400, "file above 5 GiB");
    assert_eq!(
        raw_post(&srv.host, "post-huge", 6 << 30, "x-amz-decoded-content-length: 1\r\n", &head, None).await,
        400,
        "a decoded length does not get a form past the limit"
    );
    assert_eq!(get_body(&c, &srv, "post-huge", "foo.txt").await.0, StatusCode::NOT_FOUND);
}

/// The fields are read before authentication, so their whole read has a deadline (three idle
/// timeouts): a client that sends a byte just often enough to stay under the idle timeout gets
/// 408 instead of holding the connection.
#[tokio::test]
async fn a_form_that_trickles_its_fields_gets_408() {
    let mut state = S3ServiceState::new();
    state.auth = AuthConfig::open();
    state.set_body_idle_timeout(std::time::Duration::from_millis(300));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let h = start_s3_reactor(addr, Arc::new(state)).await.unwrap();
    let srv = Srv { host: h.local_addr.to_string(), _h: h };
    let c = reqwest::Client::new();
    make_bucket(&c, &srv, "post-slow").await;
    let start = format!("--{b}\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\n", b = BOUNDARY);
    let slow = vec![b'k'; 200];
    assert_eq!(raw_post(&srv.host, "post-slow", 1 << 20, "", start.as_bytes(), Some(&slow)).await, 408);
}
