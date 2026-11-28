//! #148: One operation table, unknown sub-resources answer 501 NotImplemented,
//! and the request pipeline enforces the error precedence table.

use reqwest::{Method, StatusCode};
use s3_server::iam::ReloadingIam;
use s3_server::request_log::clf_time;
use s3_server::{compute_sigv4_signature, sha256_hex, start_s3_reactor, AuthConfig, S3ServiceState, SigV4KeyCache};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn between<'a>(s: &'a str, open: &str, close: &str) -> Option<&'a str> {
    s.split(open).nth(1).and_then(|r| r.split(close).next())
}

fn amz_date(secs: u64) -> String {
    let t = clf_time(secs);
    let months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let m = months.iter().position(|x| *x == &t[3..6]).unwrap() + 1;
    format!("{}{:02}{}T{}{}{}Z", &t[7..11], m, &t[0..2], &t[12..14], &t[15..17], &t[18..20])
}

fn signed(c: &reqwest::Client, ak: &str, sk: &str, method: Method, host: &str, path_and_query: &str, body: &[u8]) -> reqwest::RequestBuilder {
    let (path, query) = path_and_query.split_once('?').unwrap_or((path_and_query, ""));
    let mut q: Vec<(String, String)> = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (k.to_string(), v.to_string()),
            None => (p.to_string(), String::new()),
        })
        .collect();
    q.sort();
    let canonical_query = q.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join("&");
    let date = amz_date(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs());
    let hash = sha256_hex(body);
    let canonical = format!(
        "{}\n{}\n{}\nhost:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n\nhost;x-amz-content-sha256;x-amz-date\n{}",
        method.as_str(), path, canonical_query, host, hash, date, hash
    );
    let scope = format!("{}/us-east-1/s3/aws4_request", &date[..8]);
    let sts = format!("AWS4-HMAC-SHA256\n{}\n{}\n{}", date, scope, sha256_hex(canonical.as_bytes()));
    let sig = compute_sigv4_signature(&SigV4KeyCache::new(), sk, &date[..8], "us-east-1", "s3", &sts);
    c.request(method, format!("http://{}{}", host, path_and_query))
        .header("x-amz-date", date)
        .header("x-amz-content-sha256", hash)
        .header("authorization", format!("AWS4-HMAC-SHA256 Credential={}/{},SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature={}", ak, scope, sig))
        .body(body.to_vec())
}

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_optbl_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_config(dir: &Path, text: &str) -> PathBuf {
    let p = dir.join("awanfs.toml");
    std::fs::write(&p, text).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    p
}

#[tokio::test]
async fn unknown_subresources_answer_501_and_store_nothing() {
    let dir = temp_dir("501");
    let st = Arc::new(S3ServiceState::with_data_dir(&dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, st).await.unwrap();
    let base = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();

    assert_eq!(c.put(format!("{}/bkt", base)).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{}/bkt/obj", base)).body("hello").send().await.unwrap().status(), StatusCode::OK);

    // Unknown and unimplemented sub-resources on service, bucket, and object must answer 501 NotImplemented.
    for (method, path) in [
        (Method::GET, "/?fooBar"),
        (Method::GET, "/bkt?fooBar"),
        (Method::PUT, "/bkt?fooBar"),
        (Method::DELETE, "/bkt?fooBar"),
        (Method::GET, "/bkt/obj?fooBar"),
        (Method::PUT, "/bkt/new-obj?fooBar"),
        (Method::DELETE, "/bkt/obj?fooBar"),
        (Method::GET, "/bkt?website"),
        (Method::GET, "/bkt?notification"),
        (Method::GET, "/bkt?replication"),
        (Method::GET, "/bkt?accelerate"),
        (Method::GET, "/bkt?requestPayment"),
        (Method::GET, "/bkt?inventory"),
        (Method::GET, "/bkt?analytics"),
        (Method::GET, "/bkt?metrics"),
        (Method::GET, "/bkt?intelligent-tiering"),
        (Method::POST, "/bkt/obj?select"),
        (Method::POST, "/bkt/obj?restore"),
        (Method::GET, "/bkt/obj?torrent"),
    ] {
        let res = c.request(method.clone(), format!("{}{}", base, path)).body("payload").send().await.unwrap();
        let status = res.status();
        let body = res.text().await.unwrap();
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{} {}: {}", method, path, body);
        assert_eq!(between(&body, "<Code>", "</Code>"), Some("NotImplemented"), "{} {}: {}", method, path, body);
    }

    // PUT /bkt/new-obj?fooBar must not have fallen through to PutObject.
    assert_eq!(c.get(format!("{}/bkt/new-obj", base)).send().await.unwrap().status(), StatusCode::NOT_FOUND);

    // Regular query parameters (non-sub-resources) are ignored as S3 does.
    let res = c.get(format!("{}/bkt/obj?customParam=123", base)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.text().await.unwrap(), "hello");

    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn error_precedence_follows_pipeline_order() {
    let dir = temp_dir("prec");
    let cfg = r#"
[[users]]
name = "admin"
access_key = "AKADMIN"
secret_key = "admin-secret"
policy = '{"Statement": {"Effect": "Allow", "Action": "s3:*", "Resource": "*"}}'

[[users]]
name = "reader"
access_key = "AKREADER"
secret_key = "reader-secret"
policy = '{"Statement": {"Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket"], "Resource": "*"}}'
"#;
    let iam = ReloadingIam::open(&write_config(&dir, cfg)).unwrap();
    let mut st = S3ServiceState::with_data_dir(&dir);
    st.auth = AuthConfig { credentials: None, allow_anonymous: false };
    *st.iam.write() = Some(iam);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::new(st)).await.unwrap();
    let host = handle.local_addr.to_string();
    let c = reqwest::Client::new();

    // Create /existing-bkt as admin.
    assert_eq!(signed(&c, "AKADMIN", "admin-secret", Method::PUT, &host, "/existing-bkt", b"").send().await.unwrap().status(), StatusCode::OK);

    // 1 (Classify) beats 2 (Authenticate): bad signature + unknown sub-resource -> 501 NotImplemented.
    let res = signed(&c, "AKREADER", "wrong-secret", Method::GET, &host, "/existing-bkt?fooBar", b"").send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED);
    assert!(res.text().await.unwrap().contains("NotImplemented"));

    // 2 (Authenticate) beats 3 (Resolve bucket): bad signature + missing bucket -> 403 SignatureDoesNotMatch.
    let res = signed(&c, "AKREADER", "wrong-secret", Method::GET, &host, "/missing-bkt/k", b"").send().await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    assert!(res.text().await.unwrap().contains("SignatureDoesNotMatch"));

    // 3 (Resolve bucket) beats 4 (Authorize): unauthorized reader + missing bucket -> 404 NoSuchBucket.
    let res = signed(&c, "AKREADER", "reader-secret", Method::PUT, &host, "/missing-bkt/k", b"x").send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    assert!(res.text().await.unwrap().contains("NoSuchBucket"));

    // CreateBucket does not resolve an existing bucket, so unauthorized reader -> 403 AccessDenied.
    let res = signed(&c, "AKREADER", "reader-secret", Method::PUT, &host, "/missing-bkt", b"").send().await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    assert!(res.text().await.unwrap().contains("AccessDenied"));

    // 4 (Authorize) beats 5 (Validate): unauthorized reader + oversized key (> 1024 bytes) -> 403 AccessDenied, not 400 KeyTooLongError.
    let long_key = format!("/existing-bkt/{}", "a".repeat(1100));
    let res = signed(&c, "AKREADER", "reader-secret", Method::PUT, &host, &long_key, b"x").send().await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    assert!(res.text().await.unwrap().contains("AccessDenied"));

    // 5 (Validate) beats 6 (Execute): authorized admin + oversized key -> 400 KeyTooLongError.
    let res = signed(&c, "AKADMIN", "admin-secret", Method::PUT, &host, &long_key, b"x").send().await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert!(res.text().await.unwrap().contains("KeyTooLongError"));

    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
