//! #53: several access keys, each with a policy. Gate: an admin, a read-only user and a
//! bucket-scoped user; out-of-scope requests are 403 AccessDenied; a wrong secret is 403
//! SignatureDoesNotMatch; the file reloads.

use reqwest::{Method, StatusCode};
use s3_server::iam::{glob_match, Iam, ReloadingIam};
use s3_server::request_log::clf_time;
use s3_server::{compute_sigv4_signature, sha256_hex, start_s3_reactor, AuthConfig, S3ServiceState, SigV4KeyCache};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn amz_date(secs: u64) -> String {
    let t = clf_time(secs);
    let months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let m = months.iter().position(|x| *x == &t[3..6]).unwrap() + 1;
    format!("{}{:02}{}T{}{}{}Z", &t[7..11], m, &t[0..2], &t[12..14], &t[15..17], &t[18..20])
}

/// A SigV4-signed request (header form), signed in the test with the library's primitives.
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

const CONFIG: &str = r#"
[[users]]
name = "admin"
access_key = "AKADMIN"
secret_key = "admin-secret"
policy = '{"Statement": {"Effect": "Allow", "Action": "s3:*", "Resource": "*"}}'

[[users]]
name = "reader"
access_key = "AKREADER"
secret_key = "reader-secret"
policy = '''{"Statement": [{"Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket", "s3:ListAllMyBuckets"], "Resource": "*"}]}'''

[[users]]
name = "scoped"
access_key = "AKSCOPED"
secret_key = "scoped-secret"
policy = '''{"Statement": [
  {"Effect": "Allow", "Action": "s3:*", "Resource": ["arn:aws:s3:::team-a", "arn:aws:s3:::team-a/*"]},
  {"Effect": "Deny", "Action": "s3:DeleteObject", "Resource": "arn:aws:s3:::team-a/keep/*"}
]}'''
"#;

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

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_iam_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test]
async fn admin_reader_and_scoped_users_get_what_their_policies_say() {
    let dir = temp_dir("gate");
    let iam = ReloadingIam::open(&write_config(&dir, CONFIG)).unwrap();
    let mut st = S3ServiceState::new();
    st.auth = AuthConfig { credentials: None, allow_anonymous: false };
    *st.iam.write() = Some(Arc::clone(&iam));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::new(st)).await.unwrap();
    let host = handle.local_addr.to_string();
    let c = reqwest::Client::new();
    let admin = |m: Method, p: &str, b: &[u8]| signed(&c, "AKADMIN", "admin-secret", m, &host, p, b);
    let reader = |m: Method, p: &str, b: &[u8]| signed(&c, "AKREADER", "reader-secret", m, &host, p, b);
    let scoped = |m: Method, p: &str, b: &[u8]| signed(&c, "AKSCOPED", "scoped-secret", m, &host, p, b);

    for b in ["/team-a", "/team-b"] {
        assert_eq!(admin(Method::PUT, b, b"").send().await.unwrap().status(), StatusCode::OK, "admin creates {}", b);
    }
    assert_eq!(admin(Method::PUT, "/team-b/doc", b"b-data").send().await.unwrap().status(), StatusCode::OK);

    // Read-only: reads work, writes and deletes are AccessDenied.
    assert_eq!(reader(Method::GET, "/team-b/doc", b"").send().await.unwrap().text().await.unwrap(), "b-data");
    for (m, p) in [(Method::PUT, "/team-b/new"), (Method::DELETE, "/team-b/doc"), (Method::PUT, "/team-c")] {
        let res = reader(m.clone(), p, b"x").send().await.unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN, "reader {} {}", m, p);
        assert!(res.text().await.unwrap().contains("AccessDenied"));
    }
    assert_eq!(admin(Method::GET, "/team-b/doc", b"").send().await.unwrap().status(), StatusCode::OK, "reader's DELETE did nothing");

    // Bucket-scoped: everything in team-a, nothing elsewhere, and its Deny wins.
    assert_eq!(scoped(Method::PUT, "/team-a/x", b"a-data").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(scoped(Method::GET, "/team-a/x", b"").send().await.unwrap().text().await.unwrap(), "a-data");
    assert_eq!(scoped(Method::GET, "/team-b/doc", b"").send().await.unwrap().status(), StatusCode::FORBIDDEN);
    assert_eq!(scoped(Method::PUT, "/team-b/y", b"y").send().await.unwrap().status(), StatusCode::FORBIDDEN);
    assert_eq!(scoped(Method::PUT, "/team-a/keep/z", b"z").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(scoped(Method::DELETE, "/team-a/keep/z", b"").send().await.unwrap().status(), StatusCode::FORBIDDEN, "explicit Deny");
    // CopyObject needs GetObject on the source too.
    let res = scoped(Method::PUT, "/team-a/copy", b"").header("x-amz-copy-source", "/team-b/doc").send().await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN, "copying from a bucket it cannot read");
    // DeleteObjects is checked per key.
    let body = b"<Delete><Object><Key>x</Key></Object><Object><Key>keep/z</Key></Object></Delete>";
    let xml = scoped(Method::POST, "/team-a?delete", body).send().await.unwrap().text().await.unwrap();
    assert!(xml.contains("<Deleted><Key>x</Key>") || xml.contains("<Key>x</Key>"), "{}", xml);
    assert!(xml.contains("AccessDenied"), "keep/z is refused inside the batch: {}", xml);

    // Admin can do everything.
    assert_eq!(admin(Method::DELETE, "/team-b/doc", b"").send().await.unwrap().status(), StatusCode::NO_CONTENT);

    // Wrong secret, unknown key.
    let res = signed(&c, "AKREADER", "not-the-secret", Method::GET, &host, "/team-a/x", b"").send().await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    assert!(res.text().await.unwrap().contains("SignatureDoesNotMatch"));
    let res = signed(&c, "AKNOBODY", "x", Method::GET, &host, "/team-a/x", b"").send().await.unwrap();
    assert!(res.text().await.unwrap().contains("InvalidAccessKeyId"));

    // Reload: the reader is removed; a broken file keeps the current users.
    let without_reader = CONFIG.split("[[users]]").filter(|u| !u.contains("AKREADER")).collect::<Vec<_>>().join("[[users]]");
    write_config(&dir, &without_reader);
    iam.reload().unwrap();
    let res = reader(Method::GET, "/team-a/x", b"").send().await.unwrap();
    assert!(res.text().await.unwrap().contains("InvalidAccessKeyId"), "the reader is gone after the reload");
    write_config(&dir, "this is not toml [[[");
    assert!(iam.reload().is_err());
    assert_eq!(scoped(Method::GET, "/team-a/x", b"").send().await.unwrap().status(), StatusCode::OK, "users kept after a failed reload");
}

#[test]
fn unsupported_policy_parts_are_refused_instead_of_ignored() {
    for bad in [
        r#"{"Statement": {"Effect": "Allow", "Action": "s3:*", "Resource": "*", "Condition": {"IpAddress": {}}}}"#,
        r#"{"Statement": {"Effect": "Allow", "NotAction": "s3:DeleteObject", "Resource": "*"}}"#,
        r#"{"Statement": {"Effect": "Maybe", "Action": "s3:*", "Resource": "*"}}"#,
        r#"not json"#,
    ] {
        let cfg = format!("[[users]]\nname = \"u\"\naccess_key = \"AK\"\nsecret_key = \"s\"\npolicy = '{}'\n", bad);
        assert!(Iam::parse(&cfg).is_err(), "accepted: {}", bad);
    }
    let dup = "[[users]]\nname=\"a\"\naccess_key=\"AK\"\nsecret_key=\"s\"\npolicy='{\"Statement\":[]}'\n[[users]]\nname=\"b\"\naccess_key=\"AK\"\nsecret_key=\"t\"\npolicy='{\"Statement\":[]}'\n";
    assert!(Iam::parse(dup).is_err(), "one access key twice");
}

#[test]
fn glob_matching() {
    assert!(glob_match("s3:*", "s3:GetObject"));
    assert!(glob_match("s3:Get*", "s3:GetObject"));
    assert!(!glob_match("s3:Get*", "s3:PutObject"));
    assert!(glob_match("arn:aws:s3:::b/*", "arn:aws:s3:::b/a/c"));
    assert!(!glob_match("arn:aws:s3:::b/*", "arn:aws:s3:::b"));
    assert!(glob_match("arn:aws:s3:::b?", "arn:aws:s3:::b1"));
    assert!(glob_match("*", ""));
    assert!(!glob_match("a*b", "a"));
}

#[cfg(unix)]
#[test]
fn a_config_readable_by_others_is_refused() {
    use std::os::unix::fs::PermissionsExt;
    let dir = temp_dir("perm");
    let p = write_config(&dir, CONFIG);
    std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
    let err = Iam::load(&p).err().expect("refused");
    assert!(err.contains("chmod 600"), "{}", err);
}
