//! #152: Object metadata: Content-Type, standard headers, user metadata (`x-amz-meta-*`),
//! CopyObject directives (COPY/REPLACE, copy-to-self 400), GET/HEAD response overrides
//! (and anonymous refusal when auth is enabled), and persistence across compaction & restart.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, AuthConfig, S3ServiceState, STREAM_PUT_MIN};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_meta_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn start(dir: &Path) -> (Arc<S3ServiceState>, String, s3_server::ServerHandle) {
    let state = Arc::new(S3ServiceState::with_data_dir(dir));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    (state, url, handle)
}

async fn start_with_auth(dir: &Path, auth: AuthConfig) -> (Arc<S3ServiceState>, String, s3_server::ServerHandle) {
    let mut state = S3ServiceState::with_data_dir(dir);
    state.auth = auth;
    let state = Arc::new(state);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    (state, url, handle)
}

fn pattern(len: usize, seed: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + seed * 7 + 13) % 251) as u8).collect()
}

#[tokio::test]
async fn all_standard_metadata_headers_round_trip_on_get_and_head() {
    let dir = temp_dir("std");
    let c = reqwest::Client::new();
    let (_state, url, _h) = start(&dir).await;
    assert_eq!(c.put(format!("{}/abkt", url)).send().await.unwrap().status(), StatusCode::OK);

    let body = b"hello metadata".to_vec();
    let chunked_body = b"e\r\nhello metadata\r\n0\r\n\r\n".to_vec();
    let res = c
        .put(format!("{}/abkt/obj", url))
        .header("content-type", "text/plain; charset=utf-8")
        .header("content-encoding", "gzip, aws-chunked")
        .header("x-amz-decoded-content-length", "14")
        .header("content-disposition", "attachment; filename=\"a.txt\"")
        .header("content-language", "en-US")
        .header("cache-control", "max-age=3600")
        .header("expires", "Wed, 21 Oct 2026 07:28:00 GMT")
        .header("x-amz-website-redirect-location", "/other/path")
        .body(chunked_body)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    for head in [false, true] {
        let res = if head {
            c.head(format!("{}/abkt/obj", url)).send().await.unwrap()
        } else {
            c.get(format!("{}/abkt/obj", url)).send().await.unwrap()
        };
        assert_eq!(res.status(), StatusCode::OK);
        let h = res.headers();
        assert_eq!(h.get("content-type").unwrap().to_str().unwrap(), "text/plain; charset=utf-8");
        assert_eq!(h.get("content-encoding").unwrap().to_str().unwrap(), "gzip");
        assert_eq!(h.get("content-disposition").unwrap().to_str().unwrap(), "attachment; filename=\"a.txt\"");
        assert_eq!(h.get("content-language").unwrap().to_str().unwrap(), "en-US");
        assert_eq!(h.get("cache-control").unwrap().to_str().unwrap(), "max-age=3600");
        assert_eq!(h.get("expires").unwrap().to_str().unwrap(), "Wed, 21 Oct 2026 07:28:00 GMT");
        assert_eq!(h.get("x-amz-website-redirect-location").unwrap().to_str().unwrap(), "/other/path");
        if !head {
            assert_eq!(res.bytes().await.unwrap().as_ref(), &body[..]);
        }
    }

    // aws-chunked stripping edge cases
    for (sent, want) in [
        ("aws-chunked, gzip", Some("gzip")),
        ("deflate, gzip", Some("deflate, gzip")),
        ("aws-chunked", None),
        ("aws-chunked, aws-chunked", None),
    ] {
        let mut req = c.put(format!("{}/abkt/enc", url)).header("content-encoding", sent);
        if sent.contains("aws-chunked") {
            req = req
                .header("x-amz-decoded-content-length", "1")
                .body("1\r\nx\r\n0\r\n\r\n");
        } else {
            req = req.body("x");
        }
        assert_eq!(req.send().await.unwrap().status(), StatusCode::OK);
        let res = c.head(format!("{}/abkt/enc", url)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers().get("content-encoding").and_then(|v| v.to_str().ok()), want, "sent={}", sent);
    }
}

#[tokio::test]
async fn user_metadata_lower_cased_exact_and_empty_values() {
    let dir = temp_dir("usermeta");
    let c = reqwest::Client::new();
    let (_state, url, _h) = start(&dir).await;
    assert_eq!(c.put(format!("{}/abkt", url)).send().await.unwrap().status(), StatusCode::OK);

    let res = c
        .put(format!("{}/abkt/item", url))
        .header("x-amz-meta-FooBar", "ExactValue")
        .header("x-amz-meta-empty", "")
        .body("payload")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    for head in [false, true] {
        let res = if head {
            c.head(format!("{}/abkt/item", url)).send().await.unwrap()
        } else {
            c.get(format!("{}/abkt/item", url)).send().await.unwrap()
        };
        assert_eq!(res.status(), StatusCode::OK);
        let h = res.headers();
        assert_eq!(h.get("x-amz-meta-foobar").unwrap().to_str().unwrap(), "ExactValue");
        assert_eq!(h.get("x-amz-meta-empty").unwrap().to_str().unwrap(), "");
    }

    // Overwriting without metadata clears previous user metadata.
    assert_eq!(
        c.put(format!("{}/abkt/item", url)).body("payload2").send().await.unwrap().status(),
        StatusCode::OK
    );
    let res = c.head(format!("{}/abkt/item", url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(res.headers().get("x-amz-meta-foobar").is_none());
    assert!(res.headers().get("x-amz-meta-empty").is_none());
}

#[tokio::test]
async fn user_metadata_rejects_crlf_and_over_2kib_metadata_too_large() {
    let dir = temp_dir("limits");
    let c = reqwest::Client::new();
    let (_state, url, handle) = start(&dir).await;
    assert_eq!(c.put(format!("{}/abkt", url)).send().await.unwrap().status(), StatusCode::OK);

    // Exactly 2048 bytes ("k" = 1 byte + value = 2047 bytes) succeeds.
    let exact_val = "a".repeat(2047);
    let res = c
        .put(format!("{}/abkt/ok2k", url))
        .header("x-amz-meta-k", exact_val)
        .body("x")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 2049 bytes ("k" = 1 byte + value = 2048 bytes) -> 400 MetadataTooLarge.
    let too_big_val = "a".repeat(2048);
    let res = c
        .put(format!("{}/abkt/too_big", url))
        .header("x-amz-meta-k", too_big_val)
        .body("x")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = res.text().await.unwrap();
    assert!(body.contains("<Code>MetadataTooLarge</Code>"), "body={}", body);

    // Raw TCP request with CR/LF inside a header value is rejected with 400.
    let mut stream = tokio::net::TcpStream::connect(handle.local_addr).await.unwrap();
    let raw_req = format!(
        "PUT /abkt/badcrlf HTTP/1.1\r\nHost: {}\r\nContent-Length: 1\r\nx-amz-meta-a: bad\nval\r\nConnection: close\r\n\r\nx",
        handle.local_addr
    );
    stream.write_all(raw_req.as_bytes()).await.unwrap();
    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await.unwrap();
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(resp_str.starts_with("HTTP/1.1 400 "), "resp={}", resp_str);
}

#[tokio::test]
async fn copy_object_directives_and_copy_to_self() {
    let dir = temp_dir("copy");
    let c = reqwest::Client::new();
    let (_state, url, _h) = start(&dir).await;
    assert_eq!(c.put(format!("{}/abkt", url)).send().await.unwrap().status(), StatusCode::OK);

    let body = pattern(4096, 9);
    assert_eq!(
        c.put(format!("{}/abkt/src", url))
            .header("content-type", "audio/ogg")
            .header("content-encoding", "gzip")
            .header("content-disposition", "inline")
            .header("content-language", "fr")
            .header("cache-control", "no-store")
            .header("expires", "Thu, 01 Dec 2026 16:00:00 GMT")
            .header("x-amz-website-redirect-location", "/redirect")
            .header("x-amz-meta-key1", "val1")
            .body(body.clone())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // Default directive (COPY) preserves all source attributes.
    assert_eq!(
        c.put(format!("{}/abkt/dst_copy", url))
            .header("x-amz-copy-source", "/abkt/src")
            .header("content-type", "application/json")
            .header("x-amz-meta-key2", "ignored")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let res = c.get(format!("{}/abkt/dst_copy", url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    {
        let h = res.headers();
        assert_eq!(h.get("content-type").unwrap().to_str().unwrap(), "audio/ogg");
        assert_eq!(h.get("content-encoding").unwrap().to_str().unwrap(), "gzip");
        assert_eq!(h.get("content-disposition").unwrap().to_str().unwrap(), "inline");
        assert_eq!(h.get("content-language").unwrap().to_str().unwrap(), "fr");
        assert_eq!(h.get("cache-control").unwrap().to_str().unwrap(), "no-store");
        assert_eq!(h.get("expires").unwrap().to_str().unwrap(), "Thu, 01 Dec 2026 16:00:00 GMT");
        assert_eq!(h.get("x-amz-website-redirect-location").unwrap().to_str().unwrap(), "/redirect");
        assert_eq!(h.get("x-amz-meta-key1").unwrap().to_str().unwrap(), "val1");
        assert!(h.get("x-amz-meta-key2").is_none());
    }
    assert_eq!(res.bytes().await.unwrap().as_ref(), &body[..]);

    // REPLACE uses request attributes and drops old ones.
    assert_eq!(
        c.put(format!("{}/abkt/dst_replace", url))
            .header("x-amz-copy-source", "/abkt/src")
            .header("x-amz-metadata-directive", "REPLACE")
            .header("content-type", "audio/mpeg")
            .header("x-amz-meta-key3", "val3")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let res = c.head(format!("{}/abkt/dst_replace", url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    {
        let h = res.headers();
        assert_eq!(h.get("content-type").unwrap().to_str().unwrap(), "audio/mpeg");
        assert!(h.get("content-encoding").is_none());
        assert!(h.get("x-amz-meta-key1").is_none());
        assert_eq!(h.get("x-amz-meta-key3").unwrap().to_str().unwrap(), "val3");
    }

    // Copy to itself without REPLACE -> 400 InvalidRequest.
    let res = c
        .put(format!("{}/abkt/src", url))
        .header("x-amz-copy-source", "/abkt/src")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let err_xml = res.text().await.unwrap();
    assert!(err_xml.contains("<Code>InvalidRequest</Code>"), "xml={}", err_xml);

    // Copy to itself with REPLACE -> 200 OK and updates metadata.
    let res = c
        .put(format!("{}/abkt/src", url))
        .header("x-amz-copy-source", "/abkt/src")
        .header("x-amz-metadata-directive", "REPLACE")
        .header("content-type", "text/csv")
        .header("x-amz-meta-updated", "yes")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let res = c.get(format!("{}/abkt/src", url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers().get("content-type").unwrap().to_str().unwrap(), "text/csv");
    assert_eq!(res.headers().get("x-amz-meta-updated").unwrap().to_str().unwrap(), "yes");
    assert!(res.headers().get("x-amz-meta-key1").is_none());
    assert_eq!(res.bytes().await.unwrap().as_ref(), &body[..]);
}

#[tokio::test]
async fn response_overrides_on_get_and_head_and_anon_refusal() {
    let dir = temp_dir("overrides");
    let c = reqwest::Client::new();
    let (_state, url, handle) = start(&dir).await;
    assert_eq!(c.put(format!("{}/abkt", url)).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(
        c.put(format!("{}/abkt/doc", url))
            .header("content-type", "text/plain")
            .body("hello")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    let qs = "response-content-type=foo%2Fbar&response-content-language=esperanto&response-expires=123&response-cache-control=no-cache&response-content-disposition=bla&response-content-encoding=aaa";
    for head in [false, true] {
        let u = format!("{}/abkt/doc?{}", url, qs);
        let res = if head { c.head(&u).send().await.unwrap() } else { c.get(&u).send().await.unwrap() };
        assert_eq!(res.status(), StatusCode::OK);
        let h = res.headers();
        assert_eq!(h.get("content-type").unwrap().to_str().unwrap(), "foo/bar");
        assert_eq!(h.get("content-language").unwrap().to_str().unwrap(), "esperanto");
        assert_eq!(h.get("expires").unwrap().to_str().unwrap(), "123");
        assert_eq!(h.get("cache-control").unwrap().to_str().unwrap(), "no-cache");
        assert_eq!(h.get("content-disposition").unwrap().to_str().unwrap(), "bla");
        assert_eq!(h.get("content-encoding").unwrap().to_str().unwrap(), "aaa");
    }
    handle.shutdown();

    // With auth enabled and allow_anonymous = true, anonymous requests with response-* overrides are refused (400 InvalidRequest).
    let auth = AuthConfig {
        credentials: Some(("AKIAIOSFODNN7EXAMPLE".into(), "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into())),
        allow_anonymous: true,
    };
    let (_state2, url2, _h2) = start_with_auth(&dir, auth).await;
    let res = c
        .get(format!("{}/abkt/doc?response-content-type=text/html", url2))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let xml = res.text().await.unwrap();
    assert!(xml.contains("<Code>InvalidRequest</Code>"), "xml={}", xml);
}

#[tokio::test]
async fn metadata_survives_compaction_and_restart() {
    let dir = temp_dir("persist");
    let c = reqwest::Client::new();
    let (state, url, handle) = start(&dir).await;
    assert_eq!(c.put(format!("{}/abkt", url)).send().await.unwrap().status(), StatusCode::OK);

    let small = pattern(2048, 11);
    let big = pattern(STREAM_PUT_MIN as usize + 128 * 1024, 12);
    for (k, body) in [("small", &small), ("big", &big)] {
        assert_eq!(
            c.put(format!("{}/abkt/{}", url, k))
                .header("content-type", "application/octet-stream")
                .header("content-encoding", "br")
                .header("cache-control", "public, max-age=60")
                .header("x-amz-meta-owner", "alice")
                .body(body.clone())
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }

    let report = state.perform_online_compaction().await.unwrap();
    assert!(report.live_needles_copied >= 2, "{:?}", report);
    state.payload_cache.clear();

    for (k, body) in [("small", &small), ("big", &big)] {
        let res = c.get(format!("{}/abkt/{}", url, k)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers().get("content-encoding").unwrap().to_str().unwrap(), "br");
        assert_eq!(res.headers().get("cache-control").unwrap().to_str().unwrap(), "public, max-age=60");
        assert_eq!(res.headers().get("x-amz-meta-owner").unwrap().to_str().unwrap(), "alice");
        assert_eq!(res.bytes().await.unwrap().as_ref(), &body[..]);
    }
    handle.shutdown();
    drop(state);

    let (_state, url, _h) = start(&dir).await;
    for (k, body) in [("small", &small), ("big", &big)] {
        let res = c.head(format!("{}/abkt/{}", url, k)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers().get("content-encoding").unwrap().to_str().unwrap(), "br");
        assert_eq!(res.headers().get("cache-control").unwrap().to_str().unwrap(), "public, max-age=60");
        assert_eq!(res.headers().get("x-amz-meta-owner").unwrap().to_str().unwrap(), "alice");
        let res = c.get(format!("{}/abkt/{}", url, k)).send().await.unwrap();
        assert_eq!(res.bytes().await.unwrap().as_ref(), &body[..]);
    }
}
