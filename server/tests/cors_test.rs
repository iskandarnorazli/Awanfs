//! S3 Bucket CORS Configuration and Preflight Tests (#116).

use reqwest::header::HeaderMap;
use reqwest::{Method, StatusCode};
use s3_server::{start_s3_reactor, AuthConfig, S3ServiceState};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_cors_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn spawn_server() -> (String, Arc<S3ServiceState>, s3_server::ServerHandle) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let state = Arc::new(S3ServiceState::new());
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.expect("start reactor");
    let base_url = format!("http://{}", handle.local_addr);
    (base_url, state, handle)
}

fn hdr(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

async fn cors_request_and_check(
    client: &reqwest::Client,
    method: Method,
    url: &str,
    headers: &[(&str, &str)],
    expect_status: StatusCode,
    expect_allow_origin: Option<&str>,
    expect_allow_methods: Option<&str>,
) -> HeaderMap {
    let mut req = client.request(method, url);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let res = req.send().await.unwrap();
    let status = res.status();
    let resp_headers = res.headers().clone();
    assert_eq!(status, expect_status, "unexpected status for {}", url);
    assert_eq!(
        hdr(&resp_headers, "access-control-allow-origin").as_deref(),
        expect_allow_origin,
        "access-control-allow-origin mismatch for {}",
        url
    );
    assert_eq!(
        hdr(&resp_headers, "access-control-allow-methods").as_deref(),
        expect_allow_methods,
        "access-control-allow-methods mismatch for {}",
        url
    );
    resp_headers
}

// covers: GetBucketCors, PutBucketCors, DeleteBucketCors
#[tokio::test]
async fn test_set_cors() {
    let (base_url, _state, _handle) = spawn_server().await;
    let client = reqwest::Client::new();

    // Non-existent bucket -> 404 NoSuchBucket
    let res = client.get(format!("{}/no-such-bkt?cors", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    assert!(res.text().await.unwrap().contains("NoSuchBucket"));

    let res = client.delete(format!("{}/no-such-bkt?cors", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    // Create bucket
    assert_eq!(client.put(format!("{}/cors-bkt", base_url)).send().await.unwrap().status(), StatusCode::OK);

    // GET ?cors before setting -> 404 NoSuchCORSConfiguration
    let res = client.get(format!("{}/cors-bkt?cors", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    assert!(res.text().await.unwrap().contains("NoSuchCORSConfiguration"));

    // DELETE ?cors when none is set -> 204 NoContent
    let res = client.delete(format!("{}/cors-bkt?cors", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    // PUT ?cors with empty or invalid XML -> 400 MalformedXML
    let res = client.put(format!("{}/cors-bkt?cors", base_url)).body("").send().await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let res = client.put(format!("{}/cors-bkt?cors", base_url)).body("<CORSConfiguration/>").send().await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    // PUT ?cors valid
    let xml = r#"<CORSConfiguration>
        <CORSRule>
            <AllowedMethod>GET</AllowedMethod>
            <AllowedMethod>PUT</AllowedMethod>
            <AllowedOrigin>*.get</AllowedOrigin>
            <AllowedOrigin>*.put</AllowedOrigin>
        </CORSRule>
    </CORSConfiguration>"#;
    let res = client.put(format!("{}/cors-bkt?cors", base_url)).body(xml).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // GET ?cors -> 200 OK with matching rules
    let res = client.get(format!("{}/cors-bkt?cors", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.text().await.unwrap();
    assert!(body.contains("<AllowedMethod>GET</AllowedMethod>"));
    assert!(body.contains("<AllowedMethod>PUT</AllowedMethod>"));
    assert!(body.contains("<AllowedOrigin>*.get</AllowedOrigin>"));
    assert!(body.contains("<AllowedOrigin>*.put</AllowedOrigin>"));

    // DELETE ?cors -> 204 NoContent
    let res = client.delete(format!("{}/cors-bkt?cors", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    // GET ?cors after delete -> 404
    let res = client.get(format!("{}/cors-bkt?cors", base_url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

// covers: CorsPreflight
#[tokio::test]
async fn test_cors_origin_response() {
    let (base_url, _state, _handle) = spawn_server().await;
    let client = reqwest::Client::new();

    assert_eq!(client.put(format!("{}/cors-orig", base_url)).send().await.unwrap().status(), StatusCode::OK);

    let xml = r#"<CORSConfiguration>
        <CORSRule>
            <AllowedMethod>GET</AllowedMethod>
            <AllowedOrigin>*suffix</AllowedOrigin>
        </CORSRule>
        <CORSRule>
            <AllowedMethod>GET</AllowedMethod>
            <AllowedOrigin>start*end</AllowedOrigin>
        </CORSRule>
        <CORSRule>
            <AllowedMethod>GET</AllowedMethod>
            <AllowedOrigin>prefix*</AllowedOrigin>
        </CORSRule>
        <CORSRule>
            <AllowedMethod>PUT</AllowedMethod>
            <AllowedOrigin>*.put</AllowedOrigin>
        </CORSRule>
    </CORSConfiguration>"#;
    assert_eq!(
        client.put(format!("{}/cors-orig?cors", base_url)).body(xml).send().await.unwrap().status(),
        StatusCode::OK
    );

    let url = format!("{}/cors-orig", base_url);
    let obj_url = format!("{}/cors-orig/bar", base_url);

    // Actual GET requests on bucket
    cors_request_and_check(&client, Method::GET, &url, &[], StatusCode::OK, None, None).await;
    let h = cors_request_and_check(&client, Method::GET, &url, &[("Origin", "foo.suffix")], StatusCode::OK, Some("foo.suffix"), Some("GET")).await;
    assert_eq!(hdr(&h, "vary").as_deref(), Some("Origin"));

    cors_request_and_check(&client, Method::GET, &url, &[("Origin", "foo.bar")], StatusCode::OK, None, None).await;
    cors_request_and_check(&client, Method::GET, &url, &[("Origin", "foo.suffix.get")], StatusCode::OK, None, None).await;
    cors_request_and_check(&client, Method::GET, &url, &[("Origin", "startend")], StatusCode::OK, Some("startend"), Some("GET")).await;
    cors_request_and_check(&client, Method::GET, &url, &[("Origin", "start1end")], StatusCode::OK, Some("start1end"), Some("GET")).await;
    cors_request_and_check(&client, Method::GET, &url, &[("Origin", "start12end")], StatusCode::OK, Some("start12end"), Some("GET")).await;
    cors_request_and_check(&client, Method::GET, &url, &[("Origin", "0start12end")], StatusCode::OK, None, None).await;
    cors_request_and_check(&client, Method::GET, &url, &[("Origin", "prefix")], StatusCode::OK, Some("prefix"), Some("GET")).await;
    cors_request_and_check(&client, Method::GET, &url, &[("Origin", "prefix.suffix")], StatusCode::OK, Some("prefix.suffix"), Some("GET")).await;
    cors_request_and_check(&client, Method::GET, &url, &[("Origin", "bla.prefix")], StatusCode::OK, None, None).await;

    // Actual GET on missing object (404) still carries CORS headers
    cors_request_and_check(&client, Method::GET, &obj_url, &[("Origin", "foo.suffix")], StatusCode::NOT_FOUND, Some("foo.suffix"), Some("GET")).await;

    // Actual PUT with Access-Control-Request-Method override (from test_s3.py)
    cors_request_and_check(
        &client,
        Method::PUT,
        &obj_url,
        &[("Origin", "foo.suffix"), ("Access-Control-Request-Method", "GET"), ("content-length", "0")],
        StatusCode::OK,
        Some("foo.suffix"),
        Some("GET"),
    ).await;
    cors_request_and_check(
        &client,
        Method::PUT,
        &obj_url,
        &[("Origin", "foo.suffix"), ("Access-Control-Request-Method", "PUT"), ("content-length", "0")],
        StatusCode::OK,
        None,
        None,
    ).await;
    cors_request_and_check(
        &client,
        Method::PUT,
        &obj_url,
        &[("Origin", "foo.suffix"), ("content-length", "0")],
        StatusCode::OK,
        None,
        None,
    ).await;
    cors_request_and_check(
        &client,
        Method::PUT,
        &obj_url,
        &[("Origin", "foo.put"), ("content-length", "0")],
        StatusCode::OK,
        Some("foo.put"),
        Some("PUT"),
    ).await;

    // OPTIONS preflight checks
    cors_request_and_check(&client, Method::OPTIONS, &url, &[], StatusCode::BAD_REQUEST, None, None).await;
    cors_request_and_check(&client, Method::OPTIONS, &url, &[("Origin", "foo.suffix")], StatusCode::BAD_REQUEST, None, None).await;
    cors_request_and_check(&client, Method::OPTIONS, &url, &[("Origin", "bla")], StatusCode::BAD_REQUEST, None, None).await;

    cors_request_and_check(
        &client,
        Method::OPTIONS,
        &obj_url,
        &[("Origin", "foo.suffix"), ("Access-Control-Request-Method", "GET"), ("content-length", "0")],
        StatusCode::OK,
        Some("foo.suffix"),
        Some("GET"),
    ).await;
    cors_request_and_check(&client, Method::OPTIONS, &url, &[("Origin", "foo.bar"), ("Access-Control-Request-Method", "GET")], StatusCode::FORBIDDEN, None, None).await;
    cors_request_and_check(&client, Method::OPTIONS, &url, &[("Origin", "foo.suffix.get"), ("Access-Control-Request-Method", "GET")], StatusCode::FORBIDDEN, None, None).await;
    cors_request_and_check(&client, Method::OPTIONS, &url, &[("Origin", "startend"), ("Access-Control-Request-Method", "GET")], StatusCode::OK, Some("startend"), Some("GET")).await;
    cors_request_and_check(&client, Method::OPTIONS, &url, &[("Origin", "start1end"), ("Access-Control-Request-Method", "GET")], StatusCode::OK, Some("start1end"), Some("GET")).await;
    cors_request_and_check(&client, Method::OPTIONS, &url, &[("Origin", "start12end"), ("Access-Control-Request-Method", "GET")], StatusCode::OK, Some("start12end"), Some("GET")).await;
    cors_request_and_check(&client, Method::OPTIONS, &url, &[("Origin", "0start12end"), ("Access-Control-Request-Method", "GET")], StatusCode::FORBIDDEN, None, None).await;
    cors_request_and_check(&client, Method::OPTIONS, &url, &[("Origin", "prefix"), ("Access-Control-Request-Method", "GET")], StatusCode::OK, Some("prefix"), Some("GET")).await;
    cors_request_and_check(&client, Method::OPTIONS, &url, &[("Origin", "prefix.suffix"), ("Access-Control-Request-Method", "GET")], StatusCode::OK, Some("prefix.suffix"), Some("GET")).await;
    cors_request_and_check(&client, Method::OPTIONS, &url, &[("Origin", "bla.prefix"), ("Access-Control-Request-Method", "GET")], StatusCode::FORBIDDEN, None, None).await;
    cors_request_and_check(&client, Method::OPTIONS, &url, &[("Origin", "foo.put"), ("Access-Control-Request-Method", "GET")], StatusCode::FORBIDDEN, None, None).await;
    cors_request_and_check(&client, Method::OPTIONS, &url, &[("Origin", "foo.put"), ("Access-Control-Request-Method", "PUT")], StatusCode::OK, Some("foo.put"), Some("PUT")).await;
}

#[tokio::test]
async fn test_cors_origin_wildcard() {
    let (base_url, _state, _handle) = spawn_server().await;
    let client = reqwest::Client::new();

    assert_eq!(client.put(format!("{}/cors-star", base_url)).send().await.unwrap().status(), StatusCode::OK);
    let xml = r#"<CORSConfiguration>
        <CORSRule>
            <AllowedMethod>GET</AllowedMethod>
            <AllowedOrigin>*</AllowedOrigin>
        </CORSRule>
    </CORSConfiguration>"#;
    assert_eq!(client.put(format!("{}/cors-star?cors", base_url)).body(xml).send().await.unwrap().status(), StatusCode::OK);

    let url = format!("{}/cors-star", base_url);
    cors_request_and_check(&client, Method::GET, &url, &[], StatusCode::OK, None, None).await;
    let h = cors_request_and_check(&client, Method::GET, &url, &[("Origin", "example.origin")], StatusCode::OK, Some("*"), Some("GET")).await;
    assert_eq!(hdr(&h, "vary"), None);
}

#[tokio::test]
async fn test_cors_header_option_and_expose() {
    let (base_url, _state, _handle) = spawn_server().await;
    let client = reqwest::Client::new();

    assert_eq!(client.put(format!("{}/cors-hdrs", base_url)).send().await.unwrap().status(), StatusCode::OK);
    let xml = r#"<CORSConfiguration>
        <CORSRule>
            <AllowedMethod>GET</AllowedMethod>
            <AllowedOrigin>*</AllowedOrigin>
            <ExposeHeader>x-amz-meta-header1</ExposeHeader>
        </CORSRule>
    </CORSConfiguration>"#;
    assert_eq!(client.put(format!("{}/cors-hdrs?cors", base_url)).body(xml).send().await.unwrap().status(), StatusCode::OK);

    let obj_url = format!("{}/cors-hdrs/bar", base_url);
    cors_request_and_check(
        &client,
        Method::OPTIONS,
        &obj_url,
        &[
            ("Origin", "example.origin"),
            ("Access-Control-Request-Headers", "x-amz-meta-header2"),
            ("Access-Control-Request-Method", "GET"),
        ],
        StatusCode::FORBIDDEN,
        None,
        None,
    ).await;

    // Now configure AllowedHeader + MaxAgeSeconds + ExposeHeader
    let xml2 = r#"<CORSConfiguration>
        <CORSRule>
            <AllowedMethod>GET</AllowedMethod>
            <AllowedOrigin>https://app.example</AllowedOrigin>
            <AllowedHeader>x-amz-meta-*</AllowedHeader>
            <MaxAgeSeconds>3600</MaxAgeSeconds>
            <ExposeHeader>x-amz-meta-header1</ExposeHeader>
        </CORSRule>
    </CORSConfiguration>"#;
    assert_eq!(client.put(format!("{}/cors-hdrs?cors", base_url)).body(xml2).send().await.unwrap().status(), StatusCode::OK);

    let h = cors_request_and_check(
        &client,
        Method::OPTIONS,
        &obj_url,
        &[
            ("Origin", "https://app.example"),
            ("Access-Control-Request-Headers", "x-amz-meta-header2, x-amz-meta-other"),
            ("Access-Control-Request-Method", "GET"),
        ],
        StatusCode::OK,
        Some("https://app.example"),
        Some("GET"),
    ).await;
    assert_eq!(hdr(&h, "access-control-allow-headers").as_deref(), Some("x-amz-meta-header2, x-amz-meta-other"));
    assert_eq!(hdr(&h, "access-control-max-age").as_deref(), Some("3600"));
    assert_eq!(hdr(&h, "access-control-expose-headers").as_deref(), Some("x-amz-meta-header1"));
    assert_eq!(hdr(&h, "vary").as_deref(), Some("Origin"));

    // Actual request also gets Expose-Headers
    let h_get = cors_request_and_check(
        &client,
        Method::GET,
        &format!("{}/cors-hdrs", base_url),
        &[("Origin", "https://app.example")],
        StatusCode::OK,
        Some("https://app.example"),
        Some("GET"),
    ).await;
    assert_eq!(hdr(&h_get, "access-control-expose-headers").as_deref(), Some("x-amz-meta-header1"));
}

#[tokio::test]
async fn test_cors_presigned_and_unauthenticated_options() {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut state = S3ServiceState::new();
    state.auth = AuthConfig {
        allow_anonymous: false,
        credentials: Some(("AKIAIOSFODNN7EXAMPLE".to_string(), "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string())),
    };
    let state = Arc::new(state);
    // Create bucket and set CORS directly on state (since auth is enabled for HTTP)
    state.engine.write().create_bucket("cors-auth").unwrap();
    let b_id = state.engine.read().lookup_bucket("cors-auth").unwrap();
    {
        let mut store = state.bucket_configs.lock();
        store.apply_bucket_create(b_id, "cors-auth", 0, "", "").unwrap();
        let xml = b"<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>example</AllowedOrigin></CORSRule></CORSConfiguration>";
        store.put_config(b_id, s3_server::bucket_config::ConfigKind::Cors, xml).unwrap();
    }

    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.expect("start reactor");
    let base_url = format!("http://{}", handle.local_addr);
    let client = reqwest::Client::new();

    // Even with allow_anonymous=false and presigned query params for GET, OPTIONS preflight works unauthenticated!
    let presigned_url = format!("{}/cors-auth/foo?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=deadbeef", base_url);
    cors_request_and_check(&client, Method::OPTIONS, &presigned_url, &[], StatusCode::BAD_REQUEST, None, None).await;
    cors_request_and_check(
        &client,
        Method::OPTIONS,
        &presigned_url,
        &[("Origin", "example"), ("Access-Control-Request-Method", "GET")],
        StatusCode::OK,
        Some("example"),
        Some("GET"),
    ).await;
}

#[tokio::test]
async fn test_cors_restart_persistence() {
    let dir = temp_dir("restart");
    let xml = r#"<CORSConfiguration>
        <CORSRule>
            <AllowedMethod>GET</AllowedMethod>
            <AllowedOrigin>https://persist.example</AllowedOrigin>
        </CORSRule>
    </CORSConfiguration>"#;

    // 1. Start server, create bucket, PUT ?cors
    {
        let state = Arc::new(S3ServiceState::with_data_dir(&dir));
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let handle = start_s3_reactor(addr, state).await.unwrap();
        let base_url = format!("http://{}", handle.local_addr);
        let client = reqwest::Client::new();

        assert_eq!(client.put(format!("{}/persist-bkt", base_url)).send().await.unwrap().status(), StatusCode::OK);
        assert_eq!(
            client.put(format!("{}/persist-bkt?cors", base_url)).body(xml).send().await.unwrap().status(),
            StatusCode::OK
        );
    }

    // 2. Restart from same data dir -> CORS config replayed from buckets.manifest
    {
        let state = Arc::new(S3ServiceState::with_data_dir(&dir));
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let handle = start_s3_reactor(addr, state).await.unwrap();
        let base_url = format!("http://{}", handle.local_addr);
        let client = reqwest::Client::new();

        let res = client.get(format!("{}/persist-bkt?cors", base_url)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert!(res.text().await.unwrap().contains("https://persist.example"));

        cors_request_and_check(
            &client,
            Method::OPTIONS,
            &format!("{}/persist-bkt", base_url),
            &[("Origin", "https://persist.example"), ("Access-Control-Request-Method", "GET")],
            StatusCode::OK,
            Some("https://persist.example"),
            Some("GET"),
        ).await;

        assert_eq!(client.delete(format!("{}/persist-bkt?cors", base_url)).send().await.unwrap().status(), StatusCode::NO_CONTENT);
    }

    // 3. Restart again -> CORS deletion persisted
    {
        let state = Arc::new(S3ServiceState::with_data_dir(&dir));
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let handle = start_s3_reactor(addr, state).await.unwrap();
        let base_url = format!("http://{}", handle.local_addr);
        let client = reqwest::Client::new();

        let res = client.get(format!("{}/persist-bkt?cors", base_url)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        cors_request_and_check(
            &client,
            Method::OPTIONS,
            &format!("{}/persist-bkt", base_url),
            &[("Origin", "https://persist.example"), ("Access-Control-Request-Method", "GET")],
            StatusCode::FORBIDDEN,
            None,
            None,
        ).await;
    }

    let _ = std::fs::remove_dir_all(&dir);
}
