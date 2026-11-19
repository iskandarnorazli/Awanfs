//! S3 response details tests (#140):
//! - Bucket name validation at CreateBucket (400 InvalidBucketName)
//! - Object key >1024 UTF-8 bytes rejection (400 KeyTooLongError)
//! - Region config, GetBucketLocation, and CreateBucket LocationConstraint
//! - Expect: 100-continue handling (valid and rejected-before-body)
//! - BucketAlreadyOwnedByYou, BucketAlreadyExists, NoSuchBucket, BucketNotEmpty

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, AuthConfig, S3ServiceState};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn spawn_with_region(region: &str, auth: AuthConfig) -> (String, SocketAddr, Arc<S3ServiceState>, s3_server::ServerHandle) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut state = S3ServiceState::new();
    state.region = region.to_string();
    state.auth = auth;
    let state = Arc::new(state);
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.expect("start reactor");
    let local_addr = handle.local_addr;
    let base_url = format!("http://{}", local_addr);
    (base_url, local_addr, state, handle)
}

#[tokio::test]
async fn create_bucket_validates_bucket_names() {
    let (base_url, _addr, _state, handle) = spawn_with_region("us-east-1", AuthConfig::open()).await;
    let client = reqwest::Client::new();

    let bad_names = [
        "_mybucket",
        "192.168.5.123",
        "foo_bar",
        "foo-",
        "foo..bar",
        "foo.-bar",
        "foo-.bar",
        "ab",
        "FooBar",
    ];
    for name in bad_names {
        let resp = client.put(format!("{}/{}", base_url, name)).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "expected 400 for bucket {:?}", name);
        let body = resp.text().await.unwrap();
        assert!(
            body.contains("<Code>InvalidBucketName</Code>"),
            "expected InvalidBucketName for {:?}, got: {}",
            name,
            body
        );
    }

    let resp = client.put(format!("{}/valid-bucket-123", base_url)).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    handle.shutdown();
}

#[tokio::test]
async fn object_key_over_1024_utf8_bytes_rejected() {
    let (base_url, _addr, _state, handle) = spawn_with_region("us-east-1", AuthConfig::open()).await;
    let client = reqwest::Client::new();

    let resp = client.put(format!("{}/key-limit-bucket", base_url)).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 512 * 2-byte UTF-8 char = 1024 bytes -> allowed
    let ok_key = "\u{00e9}".repeat(512);
    assert_eq!(ok_key.len(), 1024);
    let resp = client
        .put(format!("{}/key-limit-bucket/{}", base_url, ok_key))
        .body("ok")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 513 * 2-byte UTF-8 char = 1026 bytes -> 400 KeyTooLongError
    let too_long_key = "\u{00e9}".repeat(513);
    assert_eq!(too_long_key.len(), 1026);
    for method in [reqwest::Method::PUT, reqwest::Method::GET, reqwest::Method::HEAD, reqwest::Method::DELETE] {
        let is_head = method == reqwest::Method::HEAD;
        let resp = client
            .request(method.clone(), format!("{}/key-limit-bucket/{}", base_url, too_long_key))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "expected 400 on {} for >1024 byte key", method);
        if !is_head {
            let body = resp.text().await.unwrap();
            assert!(
                body.contains("<Code>KeyTooLongError</Code>"),
                "expected KeyTooLongError on {}, got: {}",
                method,
                body
            );
        }
    }

    handle.shutdown();
}

#[tokio::test]
async fn get_bucket_location_and_create_bucket_constraint() {
    // 1. Default us-east-1 returns empty LocationConstraint
    let (base_url, _addr, _state, handle) = spawn_with_region("us-east-1", AuthConfig::open()).await;
    let client = reqwest::Client::new();

    let resp = client.put(format!("{}/loc-default-bkt", base_url)).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = client.get(format!("{}/loc-default-bkt?location", base_url)).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></LocationConstraint>")
            || body.contains("<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>"),
        "expected empty LocationConstraint for us-east-1, got: {}",
        body
    );
    handle.shutdown();

    // 2. Custom region returns that region string, and CreateBucket validates LocationConstraint
    let (base_url, _addr, _state, handle) = spawn_with_region("us-west-2", AuthConfig::open()).await;
    let client = reqwest::Client::new();

    // Mismatched LocationConstraint -> 400 InvalidLocationConstraint
    let bad_xml = r#"<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>eu-west-1</LocationConstraint></CreateBucketConfiguration>"#;
    let resp = client
        .put(format!("{}/loc-mismatch-bkt", base_url))
        .body(bad_xml)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = resp.text().await.unwrap();
    assert!(body.contains("<Code>InvalidLocationConstraint</Code>"), "got: {}", body);

    // Matching LocationConstraint -> 200 OK
    let ok_xml = r#"<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>us-west-2</LocationConstraint></CreateBucketConfiguration>"#;
    let resp = client
        .put(format!("{}/loc-west-bkt", base_url))
        .body(ok_xml)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = client.get(format!("{}/loc-west-bkt?location", base_url)).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">us-west-2</LocationConstraint>"),
        "expected us-west-2 LocationConstraint, got: {}",
        body
    );

    handle.shutdown();
}

#[tokio::test]
async fn bucket_already_exists_no_such_bucket_and_not_empty() {
    // Same owner in non-us-east-1 -> 409 BucketAlreadyOwnedByYou
    let (base_url, _addr, _state, handle) = spawn_with_region("us-west-2", AuthConfig::open()).await;
    let client = reqwest::Client::new();

    let resp = client.put(format!("{}/owned-bkt", base_url)).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = client.put(format!("{}/owned-bkt", base_url)).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let body = resp.text().await.unwrap();
    assert!(body.contains("<Code>BucketAlreadyOwnedByYou</Code>"), "got: {}", body);

    // Missing bucket -> 404 NoSuchBucket
    let resp = client.get(format!("{}/missing-bkt/obj", base_url)).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = resp.text().await.unwrap();
    assert!(body.contains("<Code>NoSuchBucket</Code>"), "got: {}", body);

    // Non-empty bucket delete -> 409 BucketNotEmpty
    let resp = client.put(format!("{}/owned-bkt/key1", base_url)).body("data").send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = client.delete(format!("{}/owned-bkt", base_url)).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let body = resp.text().await.unwrap();
    assert!(body.contains("<Code>BucketNotEmpty</Code>"), "got: {}", body);

    handle.shutdown();
}

#[tokio::test]
async fn expect_100_continue_valid_and_rejected_before_body() {
    let (_base_url, addr, state, handle) = spawn_with_region("us-east-1", AuthConfig::open()).await;
    state.engine.write().create_bucket("cont-bkt").unwrap();

    // 1. Valid PUT with Expect: 100-continue -> receives 100 Continue, then sends body, gets 200 OK
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req_headers = format!(
        "PUT /cont-bkt/hello.txt HTTP/1.1\r\nHost: {}\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n",
        addr
    );
    stream.write_all(req_headers.as_bytes()).await.unwrap();

    let mut buf = [0u8; 1024];
    let n = tokio::time::timeout(std::time::Duration::from_secs(3), stream.read(&mut buf))
        .await
        .expect("timed out waiting for 100 Continue")
        .unwrap();
    let head = String::from_utf8_lossy(&buf[..n]);
    assert!(head.starts_with("HTTP/1.1 100 Continue"), "expected 100 Continue, got: {:?}", head);

    stream.write_all(b"hello").await.unwrap();
    let n2 = tokio::time::timeout(std::time::Duration::from_secs(3), stream.read(&mut buf))
        .await
        .expect("timed out waiting for 200 OK")
        .unwrap();
    let resp = String::from_utf8_lossy(&buf[..n2]);
    assert!(resp.starts_with("HTTP/1.1 200 OK"), "expected 200 OK, got: {:?}", resp);

    // 2. Missing bucket with Expect: 100-continue -> immediate 404 NoSuchBucket without 100 Continue
    let mut stream2 = TcpStream::connect(addr).await.unwrap();
    let bad_req = format!(
        "PUT /no-such-bkt/hello.txt HTTP/1.1\r\nHost: {}\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n",
        addr
    );
    stream2.write_all(bad_req.as_bytes()).await.unwrap();
    let n3 = tokio::time::timeout(std::time::Duration::from_secs(3), stream2.read(&mut buf))
        .await
        .expect("timed out waiting for 404")
        .unwrap();
    let resp3 = String::from_utf8_lossy(&buf[..n3]);
    assert!(resp3.starts_with("HTTP/1.1 404"), "expected 404 without 100 Continue, got: {:?}", resp3);
    assert!(resp3.contains("NoSuchBucket"), "got: {:?}", resp3);

    // 3. Invalid Content-MD5 header with Expect: 100-continue -> immediate 400 InvalidDigest without 100 Continue
    let mut stream3 = TcpStream::connect(addr).await.unwrap();
    let bad_hdr_req = format!(
        "PUT /cont-bkt/badmd5.txt HTTP/1.1\r\nHost: {}\r\nContent-Length: 5\r\nContent-MD5: not-valid-base64!!\r\nExpect: 100-continue\r\n\r\n",
        addr
    );
    stream3.write_all(bad_hdr_req.as_bytes()).await.unwrap();
    let n4 = tokio::time::timeout(std::time::Duration::from_secs(3), stream3.read(&mut buf))
        .await
        .expect("timed out waiting for 400")
        .unwrap();
    let resp4 = String::from_utf8_lossy(&buf[..n4]);
    assert!(resp4.starts_with("HTTP/1.1 400"), "expected 400 without 100 Continue, got: {:?}", resp4);
    assert!(resp4.contains("InvalidDigest"), "got: {:?}", resp4);

    handle.shutdown();
}

/// An `Expect` value other than `100-continue` is not refused: ceph s3-tests
/// `test_{bucket,object}_create_bad_expect_{mismatch,empty}` send `Expect: 200` and an empty
/// `Expect` and expect the request to succeed. Run 43845822284 found these four tests, listed
/// in passing.txt, answered 417 ExpectationFailed (#140's commit d788491 added the refusal;
/// the issue asked only that a refused request skip `100 Continue`).
#[tokio::test]
async fn other_expect_values_are_not_refused() {
    let (base_url, _addr, _state, handle) = spawn_with_region("us-east-1", AuthConfig::open()).await;
    let c = reqwest::Client::new();
    for (i, expect) in ["200", ""].iter().enumerate() {
        let bucket = format!("expect-bkt-{}", i);
        let res = c.put(format!("{}/{}", base_url, bucket)).header("Expect", *expect).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "CreateBucket with Expect {:?}: {}", expect, res.text().await.unwrap());
        let res = c
            .put(format!("{}/{}/k", base_url, bucket))
            .header("Expect", *expect)
            .body("bar")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK, "PutObject with Expect {:?}: {}", expect, res.text().await.unwrap());
        let got = c.get(format!("{}/{}/k", base_url, bucket)).send().await.unwrap().text().await.unwrap();
        assert_eq!(got, "bar");
    }
    handle.shutdown();
}


/// #160: PUT object without Content-Length (and without chunked transfer) returns 411 MissingContentLength.
#[tokio::test]
async fn put_object_without_content_length_returns_411() {
    let (base_url, addr, state, handle) = spawn_with_region("us-east-1", AuthConfig::open()).await;
    state.engine.write().create_bucket("len-bkt").unwrap();

    // 1. PUT without Content-Length and without Transfer-Encoding -> 411 MissingContentLength
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!("PUT /len-bkt/no-len HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", addr);
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw_resp = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(3), stream.read_to_end(&mut raw_resp))
        .await
        .expect("timed out waiting for 411")
        .unwrap();
    let resp_str = String::from_utf8_lossy(&raw_resp);
    assert!(resp_str.starts_with("HTTP/1.1 411"), "expected 411 Length Required, got: {:?}", resp_str);
    assert!(resp_str.contains("<Code>MissingContentLength</Code>"), "got: {:?}", resp_str);

    // Object must not have been created.
    let c = reqwest::Client::new();
    let get_missing = c.get(format!("{}/len-bkt/no-len", base_url)).send().await.unwrap();
    assert_eq!(get_missing.status(), StatusCode::NOT_FOUND);

    // 2. PUT with Content-Length: 0 -> 200 OK
    let mut stream_zero = TcpStream::connect(addr).await.unwrap();
    let req_zero = format!(
        "PUT /len-bkt/zero HTTP/1.1\r\nHost: {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        addr
    );
    stream_zero.write_all(req_zero.as_bytes()).await.unwrap();
    let mut resp_zero = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(3), stream_zero.read_to_end(&mut resp_zero))
        .await
        .expect("timed out waiting for 200 on Content-Length: 0")
        .unwrap();
    let resp_zero_str = String::from_utf8_lossy(&resp_zero);
    assert!(resp_zero_str.starts_with("HTTP/1.1 200 OK"), "expected 200 OK, got: {:?}", resp_zero_str);

    // 3. PUT with Transfer-Encoding: chunked (no Content-Length) -> 200 OK
    let mut stream_chunked = TcpStream::connect(addr).await.unwrap();
    let req_chunked = format!(
        "PUT /len-bkt/chunked HTTP/1.1\r\nHost: {}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n3\r\nfoo\r\n0\r\n\r\n",
        addr
    );
    stream_chunked.write_all(req_chunked.as_bytes()).await.unwrap();
    let mut resp_chunked = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(3), stream_chunked.read_to_end(&mut resp_chunked))
        .await
        .expect("timed out waiting for 200 on Transfer-Encoding: chunked")
        .unwrap();
    let resp_chunked_str = String::from_utf8_lossy(&resp_chunked);
    assert!(resp_chunked_str.starts_with("HTTP/1.1 200 OK"), "expected 200 OK, got: {:?}", resp_chunked_str);
    let got = c.get(format!("{}/len-bkt/chunked", base_url)).send().await.unwrap().text().await.unwrap();
    assert_eq!(got, "foo");

    handle.shutdown();
}
