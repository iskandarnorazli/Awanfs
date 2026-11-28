//! Owner-approved ported Ceph s3-tests for #122 (`issuecomment-7216749965` Options 2A and 3A-ii).
//!
//! Covers:
//! - `test_bucket_policy_set_condition_operator_end_with_IfExists` (2A): tests `StringLikeIfExists` +
//!   `aws:Referer` against non-owner/anonymous callers (preserving `Principal::Root` Step 3 invariants).
//! - The 4 out-of-range `X-Amz-Expires` presigned URL tests (3A-ii): AwanFS returns AWS-compliant
//!   `400 AuthorizationQueryParametersError` rather than Ceph RGW's `403`.

use reqwest::Method;
use reqwest::StatusCode;
use s3_server::iam::ReloadingIam;
use s3_server::request_log::clf_time;
use s3_server::{
    compute_sigv4_signature, sha256_hex, start_s3_reactor, AuthConfig, S3ServiceState,
    SigV4KeyCache,
};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const AK_ROOT: &str = "AKROOTPORTED1220000";
const SK_ROOT: &str = "root-secret-ported-122-000000000000";
const AK_ALT: &str = "AKALTPORTED12200000";
const SK_ALT: &str = "alt-secret-ported-122-0000000000000";

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

fn presigned_url(ak: &str, sk: &str, method: &str, host: &str, path: &str, amz_date_str: &str, expires_raw: &str) -> String {
    let date8 = &amz_date_str[..8];
    let scope = format!("{}/us-east-1/s3/aws4_request", date8);
    let cred = format!("{}/{}", ak, scope).replace('/', "%2F");
    let query = format!(
        "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={}&X-Amz-Date={}&X-Amz-Expires={}&X-Amz-SignedHeaders=host",
        cred, amz_date_str, expires_raw
    );
    let canonical = format!(
        "{}\n{}\n{}\nhost:{}\n\nhost\nUNSIGNED-PAYLOAD",
        method, path, query, host
    );
    let sts = format!("AWS4-HMAC-SHA256\n{}\n{}\n{}", amz_date_str, scope, sha256_hex(canonical.as_bytes()));
    let sig = compute_sigv4_signature(&SigV4KeyCache::new(), sk, date8, "us-east-1", "s3", &sts);
    format!("http://{}{}?{}&X-Amz-Signature={}", host, path, query, sig)
}

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_port122_{}_{}_{}", name, std::process::id(), nanos));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn spawn_server(dir: &std::path::Path) -> (String, s3_server::ServerHandle) {
    let cfg = format!(
        "[[users]]\nname = \"alt\"\naccount = \"alt\"\nid = \"alt-id\"\ndisplay_name = \"alt\"\nemail = \"alt@example.com\"\naccess_key = \"{}\"\nsecret_key = \"{}\"\npolicy = '{{\"Statement\": {{\"Effect\": \"Allow\", \"Action\": \"s3:*\", \"Resource\": \"*\"}}}}'\n",
        AK_ALT, SK_ALT
    );
    let p = dir.join("users.toml");
    std::fs::write(&p, cfg).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let iam = ReloadingIam::open(&p).unwrap();
    let mut st = S3ServiceState::with_data_dir(dir);
    st.auth = AuthConfig {
        credentials: Some((AK_ROOT.to_string(), SK_ROOT.to_string())),
        allow_anonymous: false,
    };
    *st.iam.write() = Some(iam);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::new(st)).await.unwrap();
    (handle.local_addr.to_string(), handle)
}

#[tokio::test]
async fn test_bucket_policy_set_condition_operator_end_with_if_exists() {
    let dir = temp_dir("if_exists");
    let (host, _h) = spawn_server(&dir).await;
    let c = reqwest::Client::new();

    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/ifexists-bkt", b"").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/ifexists-bkt/obj", b"hello").send().await.unwrap().status(), StatusCode::OK);

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::ifexists-bkt/*",
            "Condition": {
                "StringLikeIfExists": {
                    "aws:Referer": "http://www.example.com/*"
                }
            }
        }]
    })
    .to_string();
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/ifexists-bkt?policy", policy.as_bytes()).send().await.unwrap().status(), StatusCode::NO_CONTENT);

    // Matching Referer -> 200 for anonymous and non-owner.
    assert_eq!(c.get(format!("http://{}/ifexists-bkt/obj", host)).header("referer", "http://www.example.com/page").send().await.unwrap().status(), StatusCode::OK);
    // Missing Referer -> 200 because of IfExists.
    assert_eq!(c.get(format!("http://{}/ifexists-bkt/obj", host)).send().await.unwrap().status(), StatusCode::OK);
    // Mismatched Referer -> 403 for anonymous and alt, while owner Root still allowed (Step 3).
    assert_eq!(c.get(format!("http://{}/ifexists-bkt/obj", host)).header("referer", "http://evil.example.com/page").send().await.unwrap().status(), StatusCode::FORBIDDEN);
    assert_eq!(signed(&c, AK_ALT, SK_ALT, Method::GET, &host, "/ifexists-bkt/obj", b"").header("referer", "http://evil.example.com/page").send().await.unwrap().status(), StatusCode::FORBIDDEN);
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::GET, &host, "/ifexists-bkt/obj", b"").header("referer", "http://evil.example.com/page").send().await.unwrap().status(), StatusCode::OK);
}

#[tokio::test]
async fn test_object_raw_get_x_amz_expires_out_range_and_put_expired_port() {
    let dir = temp_dir("expires");
    let (host, _h) = spawn_server(&dir).await;
    let c = reqwest::Client::new();

    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/exp-bkt", b"").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/exp-bkt/obj", b"val").send().await.unwrap().status(), StatusCode::OK);

    let now_sec = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let date_now = amz_date(now_sec);

    // 1. X-Amz-Expires=0 -> 400 AuthorizationQueryParametersError
    let u_zero = presigned_url(AK_ROOT, SK_ROOT, "GET", &host, "/exp-bkt/obj", &date_now, "0");
    let r_zero = c.get(u_zero).send().await.unwrap();
    assert_eq!(r_zero.status(), StatusCode::BAD_REQUEST);
    assert!(r_zero.text().await.unwrap().contains("AuthorizationQueryParametersError"));

    // 2. X-Amz-Expires=609901 (> 604800) -> 400 AuthorizationQueryParametersError
    let u_max = presigned_url(AK_ROOT, SK_ROOT, "GET", &host, "/exp-bkt/obj", &date_now, "609901");
    let r_max = c.get(u_max).send().await.unwrap();
    assert_eq!(r_max.status(), StatusCode::BAD_REQUEST);
    assert!(r_max.text().await.unwrap().contains("AuthorizationQueryParametersError"));

    // 3. X-Amz-Expires=-7 (negative) -> 400 AuthorizationQueryParametersError
    let u_neg = presigned_url(AK_ROOT, SK_ROOT, "GET", &host, "/exp-bkt/obj", &date_now, "-7");
    let r_neg = c.get(u_neg).send().await.unwrap();
    assert_eq!(r_neg.status(), StatusCode::BAD_REQUEST);
    assert!(r_neg.text().await.unwrap().contains("AuthorizationQueryParametersError"));

    // 4. PUT presigned X-Amz-Expires=-1000 -> 400 AuthorizationQueryParametersError
    let u_put = presigned_url(AK_ROOT, SK_ROOT, "PUT", &host, "/exp-bkt/obj2", &date_now, "-1000");
    let r_put = c.put(u_put).body("abc").send().await.unwrap();
    assert_eq!(r_put.status(), StatusCode::BAD_REQUEST);
    assert!(r_put.text().await.unwrap().contains("AuthorizationQueryParametersError"));
}
