//! Regression tests for MinIO `warp` / `minio-go` HTTP request shapes (#260).
//!
//! `minio-go` sends path-style bucket requests with a trailing slash (e.g.
//! `GET /warp-benchmark-bucket/?location=`, `PUT /warp-benchmark-bucket/`).
//! Those must route as bucket operations rather than object operations with an
//! empty key, while Decision 61's unbuilt security sub-resources (such as
//! `GET /warp-benchmark-bucket/?object-lock=`) continue returning 501.

use reqwest::{Method, StatusCode};
use s3_server::request_log::clf_time;
use s3_server::{
    compute_sigv4_signature, parse_s3_target, raw_s3_key, sha256_hex, start_s3_reactor,
    AuthConfig, ParsedTarget, S3ServiceState, SigV4KeyCache,
};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_warp_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn amz_date(secs: u64) -> String {
    let t = clf_time(secs);
    let months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let m = months.iter().position(|x| *x == &t[3..6]).unwrap() + 1;
    format!("{}{:02}{}T{}{}{}Z", &t[7..11], m, &t[0..2], &t[12..14], &t[15..17], &t[18..20])
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
        method.as_str(),
        path,
        canonical_query,
        host,
        hash,
        date,
        hash
    );
    let scope = format!("{}/us-east-1/s3/aws4_request", &date[..8]);
    let sts = format!("AWS4-HMAC-SHA256\n{}\n{}\n{}", date, scope, sha256_hex(canonical.as_bytes()));
    let sig = compute_sigv4_signature(&SigV4KeyCache::new(), sk, &date[..8], "us-east-1", "s3", &sts);
    c.request(method, format!("http://{}{}", host, path_and_query))
        .header("x-amz-date", date)
        .header("x-amz-content-sha256", hash)
        .header(
            "authorization",
            format!(
                "AWS4-HMAC-SHA256 Credential={}/{},SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature={}",
                ak, scope, sig
            ),
        )
        .body(body.to_vec())
}

#[test]
fn trailing_slash_parse_s3_target_distinguishes_bucket_from_slash_key() {
    assert_eq!(
        parse_s3_target("/warp-benchmark-bucket/", Some("127.0.0.1:9000")),
        ParsedTarget::Bucket { bucket: "warp-benchmark-bucket".into() }
    );
    assert_eq!(
        parse_s3_target("/warp-benchmark-bucket", Some("127.0.0.1:9000")),
        ParsedTarget::Bucket { bucket: "warp-benchmark-bucket".into() }
    );
    assert_eq!(
        parse_s3_target("/warp-benchmark-bucket//", Some("127.0.0.1:9000")),
        ParsedTarget::Object { bucket: "warp-benchmark-bucket".into(), key: "/".into() }
    );
    assert_eq!(
        parse_s3_target("/warp-benchmark-bucket//obj", Some("127.0.0.1:9000")),
        ParsedTarget::Object { bucket: "warp-benchmark-bucket".into(), key: "/obj".into() }
    );
    assert_eq!(raw_s3_key("/warp-benchmark-bucket/", Some("127.0.0.1:9000")), None);
    assert_eq!(raw_s3_key("/warp-benchmark-bucket//", Some("127.0.0.1:9000")), Some("/"));
}

#[tokio::test]
async fn warp_prepare_and_cleanup_trailing_slash_requests_succeed_without_weakening_decision_61() {
    let dir = temp_dir("prepare");
    let mut st = S3ServiceState::with_data_dir(&dir);
    st.auth = AuthConfig {
        credentials: Some(("warpkey".into(), "warpsecret".into())),
        allow_anonymous: false,
    };
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::new(st)).await.unwrap();
    let host = handle.local_addr.to_string();
    let c = reqwest::Client::new();

    // 1. Exact prepare probe warp sends before the bucket exists: GET /<bucket>/?location= -> 404 NoSuchBucket.
    let res = signed(&c, "warpkey", "warpsecret", Method::GET, &host, "/warp-benchmark-bucket/?location=", b"")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let body = res.text().await.unwrap();
    assert!(body.contains("<Code>NoSuchBucket</Code>"), "expected NoSuchBucket, got: {}", body);

    // 2. PUT /<bucket>/ (trailing slash) creates the bucket.
    let res = signed(&c, "warpkey", "warpsecret", Method::PUT, &host, "/warp-benchmark-bucket/", b"")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 3. HEAD /<bucket>/ succeeds.
    let res = signed(&c, "warpkey", "warpsecret", Method::HEAD, &host, "/warp-benchmark-bucket/", b"")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 4. GET /<bucket>/?location= after CreateBucket returns 200 LocationConstraint.
    let res = signed(&c, "warpkey", "warpsecret", Method::GET, &host, "/warp-benchmark-bucket/?location=", b"")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.text().await.unwrap();
    assert!(body.contains("<LocationConstraint"), "expected LocationConstraint XML, got: {}", body);

    // 5. GET /<bucket>/?versioning= returns 200 VersioningConfiguration.
    let res = signed(&c, "warpkey", "warpsecret", Method::GET, &host, "/warp-benchmark-bucket/?versioning=", b"")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.text().await.unwrap();
    assert!(body.contains("<VersioningConfiguration"), "expected VersioningConfiguration XML, got: {}", body);

    // 6. ListObjectsV2 with trailing slash and warp's query parameters returns 200.
    let res = signed(
        &c,
        "warpkey",
        "warpsecret",
        Method::GET,
        &host,
        "/warp-benchmark-bucket/?delimiter=&encoding-type=url&fetch-owner=true&list-type=2&prefix=",
        b"",
    )
    .send()
    .await
    .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.text().await.unwrap();
    assert!(body.contains("<ListBucketResult"), "expected ListBucketResult XML, got: {}", body);

    // 7. PutObject + batch DeleteObjects (POST /<bucket>/?delete=) succeeds.
    let res = signed(&c, "warpkey", "warpsecret", Method::PUT, &host, "/warp-benchmark-bucket/prefix/k1", b"payload")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let del_xml = b"<Delete><Object><Key>prefix/k1</Key></Object></Delete>";
    let res = signed(&c, "warpkey", "warpsecret", Method::POST, &host, "/warp-benchmark-bucket/?delete=", del_xml)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.text().await.unwrap();
    assert!(body.contains("<DeleteResult"), "expected DeleteResult XML, got: {}", body);

    // 8. Decision 61 guard: unimplemented security sub-resources still answer 501 NotImplemented.
    for sub in ["object-lock=", "policy=", "encryption=", "acl="] {
        let path_q = format!("/warp-benchmark-bucket/?{}", sub);
        let res = signed(&c, "warpkey", "warpsecret", Method::GET, &host, &path_q, b"")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED, "expected 501 for {}", path_q);
        let body = res.text().await.unwrap();
        assert!(body.contains("<Code>NotImplemented</Code>"), "expected NotImplemented for {}, got: {}", path_q, body);
    }

    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
