//! #122: Bucket policy, ACLs, ownership controls, public access block, and anonymous access tests (TDD).

use reqwest::{Method, StatusCode};
use s3_server::authz::BucketOwnership;
use s3_server::iam::ReloadingIam;
use s3_server::request_log::clf_time;
use s3_server::{
    compute_sigv4_signature, sha256_hex, start_s3_reactor, AuthConfig, S3ServiceState,
    SigV4KeyCache,
};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const AK_ROOT: &str = "AKROOT122TEST000000";
const SK_ROOT: &str = "root-secret-122-test-000000000000000";
const AK_ALT: &str = "AKALT122TEST0000000";
const SK_ALT: &str = "alt-secret-122-test-0000000000000000";
const AK_OTHER: &str = "AKOTHER122TEST00000";
const SK_OTHER: &str = "other-secret-122-test-00000000000000";

fn amz_date(secs: u64) -> String {
    let t = clf_time(secs);
    let months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let m = months.iter().position(|x| *x == &t[3..6]).unwrap() + 1;
    format!("{}{:02}{}T{}{}{}Z", &t[7..11], m, &t[0..2], &t[12..14], &t[15..17], &t[18..20])
}

fn signed_headers(
    c: &reqwest::Client,
    ak: &str,
    sk: &str,
    method: Method,
    host: &str,
    path_and_query: &str,
    extra: &[(&str, &str)],
    body: &[u8],
) -> reqwest::RequestBuilder {
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
    let canonical_query = q
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&");
    let date = amz_date(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs());
    let hash = sha256_hex(body);
    let mut hdrs: Vec<(String, String)> = vec![
        ("host".to_string(), host.to_string()),
        ("x-amz-content-sha256".to_string(), hash.clone()),
        ("x-amz-date".to_string(), date.clone()),
    ];
    for (k, v) in extra {
        hdrs.push((k.to_ascii_lowercase(), v.trim().to_string()));
    }
    hdrs.sort_by(|a, b| a.0.cmp(&b.0));
    let canonical_headers = hdrs
        .iter()
        .map(|(k, v)| format!("{}:{}\n", k, v))
        .collect::<String>();
    let signed_names = hdrs
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method.as_str(),
        path,
        canonical_query,
        canonical_headers,
        signed_names,
        hash
    );
    let scope = format!("{}/us-east-1/s3/aws4_request", &date[..8]);
    let sts = format!("AWS4-HMAC-SHA256\n{}\n{}\n{}", date, scope, sha256_hex(canonical.as_bytes()));
    let sig = compute_sigv4_signature(&SigV4KeyCache::new(), sk, &date[..8], "us-east-1", "s3", &sts);
    let mut req = c
        .request(method, format!("http://{}{}", host, path_and_query))
        .header("x-amz-date", date)
        .header("x-amz-content-sha256", hash)
        .header(
            "authorization",
            format!(
                "AWS4-HMAC-SHA256 Credential={}/{},SignedHeaders={},Signature={}",
                ak, scope, signed_names, sig
            ),
        );
    for (k, v) in extra {
        req = req.header(*k, *v);
    }
    req.body(body.to_vec())
}

fn signed(
    c: &reqwest::Client,
    ak: &str,
    sk: &str,
    method: Method,
    host: &str,
    path_and_query: &str,
    body: &[u8],
) -> reqwest::RequestBuilder {
    signed_headers(c, ak, sk, method, host, path_and_query, &[], body)
}

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_acc_{}_{}_{}", name, std::process::id(), nanos));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_users_toml(dir: &Path) -> PathBuf {
    let cfg = format!(
        r#"
[[users]]
name = "alt"
account = "alt"
id = "alt-id"
display_name = "alt-display"
email = "alt@example.com"
access_key = "{ak_alt}"
secret_key = "{sk_alt}"
policy = '{{"Statement": {{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}}}}'

[[users]]
name = "other"
account = "other"
id = "other-id"
display_name = "other-display"
email = "other@example.com"
access_key = "{ak_other}"
secret_key = "{sk_other}"
policy = '{{"Statement": {{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}}}}'
"#,
        ak_alt = AK_ALT,
        sk_alt = SK_ALT,
        ak_other = AK_OTHER,
        sk_other = SK_OTHER,
    );
    let p = dir.join("users.toml");
    std::fs::write(&p, cfg).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    p
}

async fn spawn_test_server(
    dir: &Path,
    default_ownership: BucketOwnership,
) -> (String, Arc<S3ServiceState>, s3_server::ServerHandle) {
    let iam = ReloadingIam::open(&write_users_toml(dir)).unwrap();
    let mut st = S3ServiceState::with_data_dir(dir);
    st.auth = AuthConfig {
        credentials: Some((AK_ROOT.to_string(), SK_ROOT.to_string())),
        allow_anonymous: false,
    };
    st.default_bucket_ownership = default_ownership;
    *st.iam.write() = Some(iam);
    let st = Arc::new(st);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&st)).await.unwrap();
    (handle.local_addr.to_string(), st, handle)
}

// covers: PutBucketPolicy, GetBucketPolicy, DeleteBucketPolicy, GetBucketPolicyStatus
#[tokio::test]
async fn bucket_policy_crud_exact_bytes_and_validation() {
    let dir = temp_dir("policy_crud");
    let (host, _st, _h) = spawn_test_server(&dir, BucketOwnership::BucketOwnerEnforced).await;
    let c = reqwest::Client::new();

    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/pol-bkt", b"").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/other-bkt", b"").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/pol-bkt/a", b"hello").send().await.unwrap().status(), StatusCode::OK);

    // Unset GET ?policy -> 404 NoSuchBucketPolicy; GET ?policyStatus -> 200 IsPublic false.
    let res = signed(&c, AK_ROOT, SK_ROOT, Method::GET, &host, "/pol-bkt?policy", b"").send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    assert!(res.text().await.unwrap().contains("NoSuchBucketPolicy"));

    let st_res = signed(&c, AK_ROOT, SK_ROOT, Method::GET, &host, "/pol-bkt?policyStatus", b"").send().await.unwrap();
    assert_eq!(st_res.status(), StatusCode::OK);
    assert!(st_res.text().await.unwrap().contains("<IsPublic>false</IsPublic>"));

    // Unimplemented operator / condition key / Allow+NotPrincipal / specific other bucket -> 400 MalformedPolicy.
    for bad in [
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::pol-bkt/*","Condition":{"FutureOperator":{"aws:SourceIp":"1.2.3.4"}}}]}"#,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::pol-bkt/*","Condition":{"StringEquals":{"aws:UnknownKey":"x"}}}]}"#,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","NotPrincipal":{"AWS":"alt-id"},"Action":"s3:ListBucket","Resource":"arn:aws:s3:::pol-bkt"}]}"#,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::other-bkt/*"}]}"#,
    ] {
        let r = signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/pol-bkt?policy", bad.as_bytes()).send().await.unwrap();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST, "expected 400 for {}", bad);
        assert!(r.text().await.unwrap().contains("MalformedPolicy"));
    }

    // Valid policy exact byte-for-byte preservation + cross-account allow + wildcard arn:aws:s3:::* allowed.
    let policy = "{\n  \"Version\": \"2012-10-17\",\n  \"Statement\": [{\n    \"Sid\": \"allow-read\",\n    \"Effect\": \"Allow\",\n    \"Principal\": {\"AWS\": \"*\"},\n    \"Action\": \"s3:GetObject\",\n    \"Resource\": \"arn:aws:s3:::pol-bkt/*\"\n  }]\n}";
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/pol-bkt?policy", policy.as_bytes()).send().await.unwrap().status(), StatusCode::NO_CONTENT);
    let got = signed(&c, AK_ROOT, SK_ROOT, Method::GET, &host, "/pol-bkt?policy", b"").send().await.unwrap();
    assert_eq!(got.status(), StatusCode::OK);
    assert_eq!(got.text().await.unwrap(), policy);

    let st_pub = signed(&c, AK_ROOT, SK_ROOT, Method::GET, &host, "/pol-bkt?policyStatus", b"").send().await.unwrap();
    assert_eq!(st_pub.status(), StatusCode::OK);
    assert!(st_pub.text().await.unwrap().contains("<IsPublic>true</IsPublic>"));

    // Anonymous and alt can GET object; delete removes policy -> 403 again.
    let anon = c.get(format!("http://{}/pol-bkt/a", host)).send().await.unwrap();
    assert_eq!(anon.status(), StatusCode::OK);
    assert_eq!(anon.text().await.unwrap(), "hello");
    assert_eq!(signed(&c, AK_ALT, SK_ALT, Method::GET, &host, "/pol-bkt/a", b"").send().await.unwrap().status(), StatusCode::OK);

    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::DELETE, &host, "/pol-bkt?policy", b"").send().await.unwrap().status(), StatusCode::NO_CONTENT);
    assert_eq!(c.get(format!("http://{}/pol-bkt/a", host)).send().await.unwrap().status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn policy_conditions_operators_and_keys() {
    let dir = temp_dir("policy_conds");
    let (host, _st, _h) = spawn_test_server(&dir, BucketOwnership::ObjectWriter).await;
    let c = reqwest::Client::new();

    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/cond-bkt", b"").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(signed_headers(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/cond-bkt/pub", &[("x-amz-tagging", "security=public")], b"pubdata").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(signed_headers(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/cond-bkt/priv", &[("x-amz-tagging", "security=private")], b"privdata").send().await.unwrap().status(), StatusCode::OK);

    // 1. ExistingObjectTag + RequestObjectTag + Deny s3:x-amz-acl=public* + s3:prefix.
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::cond-bkt/*",
                "Condition": { "StringEquals": { "s3:ExistingObjectTag/security": "public" } }
            },
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::cond-bkt",
                "Condition": { "StringLike": { "s3:prefix": "pub*" } }
            },
            {
                "Effect": "Allow",
                "Principal": { "AWS": "alt-id" },
                "Action": "s3:PutObject",
                "Resource": "arn:aws:s3:::cond-bkt/*",
                "Condition": { "StringEquals": { "s3:RequestObjectTag/security": "public" } }
            },
            {
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": "arn:aws:s3:::cond-bkt/*",
                "Condition": { "StringLike": { "s3:x-amz-acl": "public*" } }
            }
        ]
    })
    .to_string();
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/cond-bkt?policy", policy.as_bytes()).send().await.unwrap().status(), StatusCode::NO_CONTENT);

    assert_eq!(signed(&c, AK_ALT, SK_ALT, Method::GET, &host, "/cond-bkt/pub", b"").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(signed(&c, AK_ALT, SK_ALT, Method::GET, &host, "/cond-bkt/priv", b"").send().await.unwrap().status(), StatusCode::FORBIDDEN);
    assert_eq!(signed(&c, AK_ALT, SK_ALT, Method::GET, &host, "/cond-bkt?prefix=pub", b"").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(signed(&c, AK_ALT, SK_ALT, Method::GET, &host, "/cond-bkt?prefix=priv", b"").send().await.unwrap().status(), StatusCode::FORBIDDEN);
    // HeadObject 404 when missing matches s3:prefix policy vs 403 otherwise.
    assert_eq!(signed(&c, AK_ALT, SK_ALT, Method::HEAD, &host, "/cond-bkt/pub-missing", b"").send().await.unwrap().status(), StatusCode::NOT_FOUND);
    assert_eq!(signed(&c, AK_ALT, SK_ALT, Method::HEAD, &host, "/cond-bkt/priv-missing", b"").send().await.unwrap().status(), StatusCode::FORBIDDEN);
    // PutObject with RequestObjectTag=public allowed unless x-amz-acl=public-read (Explicit Deny wins).
    assert_eq!(
        signed_headers(&c, AK_ALT, SK_ALT, Method::PUT, &host, "/cond-bkt/new", &[("x-amz-tagging", "security=public")], b"ok").send().await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        signed_headers(&c, AK_ALT, SK_ALT, Method::PUT, &host, "/cond-bkt/new2", &[("x-amz-tagging", "security=public"), ("x-amz-acl", "public-read")], b"no").send().await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
}

// covers: PutBucketAcl, GetBucketAcl, PutObjectAcl, GetObjectAcl
#[tokio::test]
async fn acl_canned_headers_xml_email_and_mtime() {
    let dir = temp_dir("acl_flow");
    let (host, _st, _h) = spawn_test_server(&dir, BucketOwnership::ObjectWriter).await;
    let c = reqwest::Client::new();

    assert_eq!(
        signed_headers(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/acl-bkt", &[("x-amz-acl", "public-read")], b"")
            .send().await.unwrap().status(),
        StatusCode::OK
    );
    // Recreating with different ACL -> 409 BucketAlreadyExists; same ACL -> 200 OK.
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/acl-bkt", b"").send().await.unwrap().status(), StatusCode::CONFLICT);
    assert_eq!(
        signed_headers(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/acl-bkt", &[("x-amz-acl", "public-read")], b"")
            .send().await.unwrap().status(),
        StatusCode::OK
    );

    // Anonymous can list public-read bucket, gets 403 on private object.
    assert_eq!(c.get(format!("http://{}/acl-bkt", host)).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/acl-bkt/obj", b"val").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.get(format!("http://{}/acl-bkt/obj", host)).send().await.unwrap().status(), StatusCode::FORBIDDEN);

    let head_before = signed(&c, AK_ROOT, SK_ROOT, Method::HEAD, &host, "/acl-bkt/obj", b"").send().await.unwrap();
    let mtime_before = head_before.headers().get("last-modified").unwrap().to_str().unwrap().to_string();

    // Grant READ by email -> resolved to alt CanonicalUser, preserves mtime.
    let acl_xml = r#"<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>root</ID><DisplayName>root</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="AmazonCustomerByEmail"><EmailAddress>alt@example.com</EmailAddress></Grantee><Permission>READ</Permission></Grant></AccessControlList></AccessControlPolicy>"#;
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/acl-bkt/obj?acl", acl_xml.as_bytes()).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(signed(&c, AK_ALT, SK_ALT, Method::GET, &host, "/acl-bkt/obj", b"").send().await.unwrap().status(), StatusCode::OK);
    let got_acl = signed(&c, AK_ROOT, SK_ROOT, Method::GET, &host, "/acl-bkt/obj?acl", b"").send().await.unwrap().text().await.unwrap();
    assert!(got_acl.contains("<ID>alt-id</ID>"));

    let head_after = signed(&c, AK_ROOT, SK_ROOT, Method::HEAD, &host, "/acl-bkt/obj", b"").send().await.unwrap();
    let mtime_after = head_after.headers().get("last-modified").unwrap().to_str().unwrap().to_string();
    assert_eq!(mtime_before, mtime_after, "PutObjectAcl must not alter LastModified");

    // Unknown email -> 400 UnresolvableGrantByEmailAddress; unknown CanonicalUser -> 400 InvalidArgument.
    let bad_email = acl_xml.replace("alt@example.com", "nobody@invalid.example");
    let r1 = signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/acl-bkt/obj?acl", bad_email.as_bytes()).send().await.unwrap();
    assert_eq!(r1.status(), StatusCode::BAD_REQUEST);
    assert!(r1.text().await.unwrap().contains("UnresolvableGrantByEmailAddress"));

    let bad_id = r#"<AccessControlPolicy><Owner><ID>root</ID></Owner><AccessControlList><Grant><Grantee xsi:type="CanonicalUser"><ID>no-such-id</ID></Grantee><Permission>READ</Permission></Grant></AccessControlList></AccessControlPolicy>"#;
    let r2 = signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/acl-bkt?acl", bad_id.as_bytes()).send().await.unwrap();
    assert_eq!(r2.status(), StatusCode::BAD_REQUEST);
    assert!(r2.text().await.unwrap().contains("InvalidArgument"));
}

// covers: PutBucketOwnershipControls, GetBucketOwnershipControls, DeleteBucketOwnershipControls, PutPublicAccessBlock, GetPublicAccessBlock, DeletePublicAccessBlock
#[tokio::test]
async fn ownership_controls_pab_expected_owner_anonymous_and_100_continue() {
    let dir = temp_dir("own_pab");
    let (host, _st, _h) = spawn_test_server(&dir, BucketOwnership::BucketOwnerEnforced).await;
    let c = reqwest::Client::new();

    // Anonymous ListBuckets returns 200 with empty <Buckets>.
    let anon_lb = c.get(format!("http://{}/", host)).send().await.unwrap();
    assert_eq!(anon_lb.status(), StatusCode::OK);
    let anon_lb_body = anon_lb.text().await.unwrap();
    assert!(!anon_lb_body.contains("<Bucket>"));

    // Anonymous missing bucket returns 404 NoSuchBucket before authz.
    let missing = c.get(format!("http://{}/no-such-bkt-122/k", host)).send().await.unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);

    // Default BucketOwnerEnforced bucket: GET ?ownershipControls is 404 OwnershipControlsNotFoundError.
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/own-bkt", b"").send().await.unwrap().status(), StatusCode::OK);
    let go = signed(&c, AK_ROOT, SK_ROOT, Method::GET, &host, "/own-bkt?ownershipControls", b"").send().await.unwrap();
    assert_eq!(go.status(), StatusCode::NOT_FOUND);
    assert!(go.text().await.unwrap().contains("OwnershipControlsNotFoundError"));

    // PutBucketAcl refused under BucketOwnerEnforced with 400 AccessControlListNotSupported.
    let acl_err = signed_headers(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/own-bkt?acl", &[("x-amz-acl", "public-read")], b"")
        .send().await.unwrap();
    assert_eq!(acl_err.status(), StatusCode::BAD_REQUEST);
    assert!(acl_err.text().await.unwrap().contains("AccessControlListNotSupported"));

    // Switch to ObjectWriter -> ACLs work; putting BucketOwnerEnforced while public-read -> 400 InvalidBucketAclWithObjectOwnership.
    let ow_xml = r#"<OwnershipControls><Rule><ObjectOwnership>ObjectWriter</ObjectOwnership></Rule></OwnershipControls>"#;
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/own-bkt?ownershipControls", ow_xml.as_bytes()).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(signed_headers(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/own-bkt?acl", &[("x-amz-acl", "public-read-write")], b"").send().await.unwrap().status(), StatusCode::OK);
    let enforced_xml = r#"<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>"#;
    let inv = signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/own-bkt?ownershipControls", enforced_xml.as_bytes()).send().await.unwrap();
    assert_eq!(inv.status(), StatusCode::BAD_REQUEST);
    assert!(inv.text().await.unwrap().contains("InvalidBucketAclWithObjectOwnership"));

    // Expect: 100-continue returns 403 on private bucket, 100 on public-read-write bucket.
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/priv-100", b"").send().await.unwrap().status(), StatusCode::OK);
    async fn send_100(host: &str, bkt: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut s = tokio::net::TcpStream::connect(host).await.unwrap();
        let req = format!("PUT /{}/k HTTP/1.1\r\nHost: {}\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n", bkt, host);
        s.write_all(req.as_bytes()).await.unwrap();
        let mut buf = [0u8; 256];
        let n = tokio::time::timeout(std::time::Duration::from_secs(3), s.read(&mut buf))
            .await
            .expect("100-continue response timeout")
            .unwrap();
        String::from_utf8_lossy(&buf[..n]).to_string()
    }
    let r_priv = send_100(&host, "priv-100").await;
    assert!(r_priv.starts_with("HTTP/1.1 403"), "priv-100 got: {:?}", r_priv);
    let r_own = send_100(&host, "own-bkt").await;
    assert!(r_own.starts_with("HTTP/1.1 100"), "own-bkt got: {:?}", r_own);

    // ExpectedBucketOwner mismatch -> 403 AccessDenied even when anonymous allowed.
    let exp = c.get(format!("http://{}/own-bkt", host)).header("x-amz-expected-bucket-owner", "wrong-owner").send().await.unwrap();
    assert_eq!(exp.status(), StatusCode::FORBIDDEN);

    // PublicAccessBlock: IgnorePublicAcls & BlockPublicAcls & BlockPublicPolicy.
    let pab_xml = r#"<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>true</RestrictPublicBuckets></PublicAccessBlockConfiguration>"#;
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/own-bkt?publicAccessBlock", pab_xml.as_bytes()).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::GET, &host, "/own-bkt?publicAccessBlock", b"").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.get(format!("http://{}/own-bkt", host)).send().await.unwrap().status(), StatusCode::FORBIDDEN);
    assert_eq!(signed_headers(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/own-bkt?acl", &[("x-amz-acl", "public-read")], b"").send().await.unwrap().status(), StatusCode::FORBIDDEN);
    let blk_pol = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::own-bkt/*"}]}"#;
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/own-bkt?policy", blk_pol.as_bytes()).send().await.unwrap().status(), StatusCode::FORBIDDEN);
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::DELETE, &host, "/own-bkt?publicAccessBlock", b"").send().await.unwrap().status(), StatusCode::NO_CONTENT);
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::DELETE, &host, "/own-bkt?ownershipControls", b"").send().await.unwrap().status(), StatusCode::NO_CONTENT);
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::GET, &host, "/own-bkt?ownershipControls", b"").send().await.unwrap().status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn policy_and_acl_live_update_and_restart_round_trip() {
    let dir = temp_dir("restart_rt");
    let raw_policy = "{\n  \"Version\": \"2012-10-17\",\n  \"Statement\": [{\n    \"Effect\": \"Allow\",\n    \"Principal\": \"*\",\n    \"Action\": \"s3:GetObject\",\n    \"Resource\": \"arn:aws:s3:::rt-bkt/*\"\n  }]\n}";
    {
        let (host, _st, handle) = spawn_test_server(&dir, BucketOwnership::ObjectWriter).await;
        let c = reqwest::Client::new();
        assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/rt-bkt", b"").send().await.unwrap().status(), StatusCode::OK);
        assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/rt-bkt/k", b"data").send().await.unwrap().status(), StatusCode::OK);
        // Immediate live change with no stale cache.
        assert_eq!(c.get(format!("http://{}/rt-bkt/k", host)).send().await.unwrap().status(), StatusCode::FORBIDDEN);
        assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/rt-bkt?policy", raw_policy.as_bytes()).send().await.unwrap().status(), StatusCode::NO_CONTENT);
        assert_eq!(c.get(format!("http://{}/rt-bkt/k", host)).send().await.unwrap().status(), StatusCode::OK);
        assert_eq!(signed_headers(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/rt-bkt/k?acl", &[("x-amz-acl", "public-read")], b"").send().await.unwrap().status(), StatusCode::OK);
        handle.shutdown();
    }

    let (host, _st, handle) = spawn_test_server(&dir, BucketOwnership::ObjectWriter).await;
    let c = reqwest::Client::new();
    let got_pol = signed(&c, AK_ROOT, SK_ROOT, Method::GET, &host, "/rt-bkt?policy", b"").send().await.unwrap();
    assert_eq!(got_pol.status(), StatusCode::OK);
    assert_eq!(got_pol.text().await.unwrap(), raw_policy);
    let got_acl = signed(&c, AK_ROOT, SK_ROOT, Method::GET, &host, "/rt-bkt/k?acl", b"").send().await.unwrap().text().await.unwrap();
    assert!(got_acl.contains("AllUsers"));
    handle.shutdown();
}

#[test]
fn cli_default_bucket_ownership_flag_accepts_valid_and_rejects_invalid() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_awanfs"))
        .args(["--port", "0", "--allow-anonymous", "--default-bucket-ownership", "InvalidMode"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("default-bucket-ownership"));
}

#[tokio::test]
async fn adversarial_security_regressions() {
    use s3_server::iam::Principal;
    use s3_server::policy::{parse_bucket_policy, PolicyRequestContext};

    // HIGH #4: leading multibyte Unicode whitespace in Date condition must not panic, and RFC3339 parses cleanly.
    let ws_pol = parse_bucket_policy(
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*","Condition":{"DateGreaterThan":{"aws:CurrentTime":"  \u20032020-01-01T00:00:00Z"}}}]}"#,
    )
    .unwrap();
    let d_ws = ws_pol.evaluate(
        &Principal::Anonymous,
        &["s3:GetObject"],
        "arn:aws:s3:::b/k",
        &PolicyRequestContext { now_epoch_secs: 1_800_000_000, ..Default::default() },
    );
    assert!(d_ws.allow);

    // HIGH #5: 0.0.0.0/0, ::/0, and IpAddressIfExists stay is_public = true for BlockPublicPolicy.
    let pol_0000 = parse_bucket_policy(
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*","Condition":{"IpAddress":{"aws:SourceIp":"0.0.0.0/0"}}}]}"#,
    )
    .unwrap();
    assert!(pol_0000.is_public);
    let pol_if_exists = parse_bucket_policy(
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*","Condition":{"IpAddressIfExists":{"aws:SourceIp":"10.0.0.1"}}}]}"#,
    )
    .unwrap();
    assert!(pol_if_exists.is_public);

    // MEDIUM #8: ForAnyValue:<NegatedOp> Deny triggers when any tag mismatches.
    let neg_pol = parse_bucket_policy(
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutObjectTagging","Resource":"arn:aws:s3:::b/*","Condition":{"ForAnyValue:StringNotEquals":{"s3:RequestObjectTagKeys":"approved"}}}]}"#,
    )
    .unwrap();
    let req_tags = vec![
        ("approved".to_string(), "1".to_string()),
        ("evil".to_string(), "2".to_string()),
    ];
    let d_neg = neg_pol.evaluate(
        &Principal::Anonymous,
        &["s3:PutObjectTagging"],
        "arn:aws:s3:::b/k",
        &PolicyRequestContext { request_tags: &req_tags, ..Default::default() },
    );
    assert!(d_neg.explicit_deny);

    // MEDIUM #10: User with name == "root" in non-root account does not match Principal "root".
    let root_pol = parse_bucket_policy(
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}"#,
    )
    .unwrap();
    let impostor = Principal::User {
        account: "impostor".to_string(),
        name: "root".to_string(),
        id: "impostor-id".to_string(),
    };
    assert!(!root_pol.evaluate(&impostor, &["s3:GetObject"], "arn:aws:s3:::b/k", &PolicyRequestContext::default()).allow);

    // HTTP server security regressions (HIGH #1, HIGH #2, HIGH #3).
    let dir = temp_dir("adv_sec");
    let iam = ReloadingIam::open(&write_users_toml(&dir)).unwrap();
    let mut st = S3ServiceState::with_data_dir(&dir);
    st.auth = AuthConfig {
        credentials: Some((AK_ROOT.to_string(), SK_ROOT.to_string())),
        allow_anonymous: true,
    };
    st.default_bucket_ownership = BucketOwnership::ObjectWriter;
    *st.iam.write() = Some(iam);
    let st = Arc::new(st);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let h = start_s3_reactor(addr, Arc::clone(&st)).await.unwrap();
    let host = h.local_addr.to_string();
    let c = reqwest::Client::new();

    // HIGH #3: --allow-anonymous with credentials must NOT bypass authz on private buckets.
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/sec-priv", b"").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/sec-priv/k", b"v").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.get(format!("http://{}/sec-priv/k", host)).send().await.unwrap().status(), StatusCode::FORBIDDEN);

    // HIGH #1: Governance bypass cannot be granted by DeleteObject policy or WRITE ACL when BypassGovernanceRetention is denied.
    assert_eq!(
        signed_headers(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/sec-lock", &[("x-amz-bucket-object-lock-enabled", "true"), ("x-amz-acl", "public-read-write")], b"")
            .send().await.unwrap().status(),
        StatusCode::OK
    );
    let put_lock = signed_headers(
        &c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/sec-lock/obj",
        &[
            ("content-md5", "TO8vMKx9M0GdAMHZOgkAlQ=="),
            ("x-amz-object-lock-mode", "GOVERNANCE"),
            ("x-amz-object-lock-retain-until-date", "2099-01-01T00:00:00Z"),
        ],
        b"locked",
    ).send().await.unwrap();
    assert_eq!(put_lock.status(), StatusCode::OK);
    let vid = put_lock.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();
    let lock_pol = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"alt-id"},"Action":"s3:DeleteObject","Resource":"arn:aws:s3:::sec-lock/*"},{"Effect":"Deny","Principal":{"AWS":"alt-id"},"Action":"s3:BypassGovernanceRetention","Resource":"arn:aws:s3:::sec-lock/*"}]}"#;
    assert_eq!(signed(&c, AK_ROOT, SK_ROOT, Method::PUT, &host, "/sec-lock?policy", lock_pol.as_bytes()).send().await.unwrap().status(), StatusCode::NO_CONTENT);
    let alt_del = signed_headers(
        &c, AK_ALT, SK_ALT, Method::DELETE, &host, &format!("/sec-lock/obj?versionId={}", vid),
        &[("x-amz-bypass-governance-retention", "true")],
        b"",
    ).send().await.unwrap();
    assert_eq!(alt_del.status(), StatusCode::FORBIDDEN);

    // HIGH #2: Cross-bucket/key multipart uploadId returns 404 NoSuchUpload.
    let init = signed(&c, AK_ROOT, SK_ROOT, Method::POST, &host, "/sec-priv/m?uploads", b"").send().await.unwrap();
    assert_eq!(init.status(), StatusCode::OK);
    let init_xml = init.text().await.unwrap();
    let u_s = init_xml.find("<UploadId>").unwrap() + "<UploadId>".len();
    let u_e = init_xml[u_s..].find("</UploadId>").unwrap() + u_s;
    let uid = &init_xml[u_s..u_e];
    assert_eq!(signed(&c, AK_ALT, SK_ALT, Method::PUT, &host, "/alt-bkt", b"").send().await.unwrap().status(), StatusCode::OK);
    let hijack_up = signed(&c, AK_ALT, SK_ALT, Method::PUT, &host, &format!("/alt-bkt/other?partNumber=1&uploadId={}", uid), b"p1").send().await.unwrap();
    assert_eq!(hijack_up.status(), StatusCode::NOT_FOUND);
    let hijack_abort = signed(&c, AK_ALT, SK_ALT, Method::DELETE, &host, &format!("/alt-bkt/other?uploadId={}", uid), b"").send().await.unwrap();
    assert_eq!(hijack_abort.status(), StatusCode::NOT_FOUND);

    h.shutdown();
}
