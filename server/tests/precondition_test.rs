//! End-to-end tests for conditional GET, HEAD, and CopyObject (#181).

use reqwest::header::{HeaderMap, HeaderValue, ETAG, IF_MATCH, IF_MODIFIED_SINCE, IF_NONE_MATCH, IF_UNMODIFIED_SINCE, LAST_MODIFIED};
use reqwest::StatusCode;
use s3_server::precondition::{format_http_date, parse_http_date};
use s3_server::{start_s3_reactor, S3ServiceState, WorkSnapshot};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_precond_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct Server {
    dir: PathBuf,
    state: Arc<S3ServiceState>,
    url: String,
    client: reqwest::Client,
    handle: Option<s3_server::ServerHandle>,
}

impl Server {
    async fn start(name: &str) -> Self {
        let dir = temp_dir(name);
        let state = Arc::new(S3ServiceState::with_data_dir(&dir));
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let handle = start_s3_reactor(addr, Arc::clone(&state)).await.expect("start reactor");
        let s = Server {
            dir,
            url: format!("http://{}", handle.local_addr),
            state,
            client: reqwest::Client::new(),
            handle: Some(handle),
        };
        let res = s.client.put(format!("{}/bkt", s.url)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        s
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(h) = self.handle.take() {
            h.shutdown();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[derive(Clone, Copy, Debug)]
enum Hdr {
    Absent,
    True,
    False,
}

#[derive(Clone, Copy, Debug)]
enum Obj {
    Exists,
    DeleteMarker,
    Missing,
}

fn expected_status(im: Hdr, ius: Hdr, inm: Hdr, ims: Hdr, obj: Obj) -> StatusCode {
    let exists = matches!(obj, Obj::Exists);
    match im {
        Hdr::True => {
            if !exists {
                return StatusCode::PRECONDITION_FAILED;
            }
        }
        Hdr::False => return StatusCode::PRECONDITION_FAILED,
        Hdr::Absent => {
            if exists && matches!(ius, Hdr::False) {
                return StatusCode::PRECONDITION_FAILED;
            }
        }
    }
    match inm {
        Hdr::True => {
            if exists {
                StatusCode::NOT_MODIFIED
            } else {
                StatusCode::NOT_FOUND
            }
        }
        Hdr::False => {
            if exists {
                StatusCode::OK
            } else {
                StatusCode::NOT_FOUND
            }
        }
        Hdr::Absent => {
            if exists && matches!(ims, Hdr::False) {
                StatusCode::NOT_MODIFIED
            } else if exists {
                StatusCode::OK
            } else {
                StatusCode::NOT_FOUND
            }
        }
    }
}

#[tokio::test]
async fn get_and_head_exhaustive_table_and_headers() {
    let s = Server::start("table").await;

    // 1. Existing object
    let body = b"conditional-payload-data";
    let put = s.client.put(format!("{}/bkt/exists", s.url)).body(&body[..]).send().await.unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    let head = s.client.head(format!("{}/bkt/exists", s.url)).send().await.unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    let etag = head.headers().get(ETAG).unwrap().to_str().unwrap().to_string();
    let lm_str = head.headers().get(LAST_MODIFIED).unwrap().to_str().unwrap().to_string();
    let lm_secs = parse_http_date(&lm_str).expect("valid Last-Modified HTTP date");

    // 2. Delete marker (versioned bucket)
    let v_put = s
        .client
        .put(format!("{}/bkt?versioning", s.url))
        .body("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>")
        .send()
        .await
        .unwrap();
    assert_eq!(v_put.status(), StatusCode::OK);
    let _ = s.client.put(format!("{}/bkt/deleted", s.url)).body("gone").send().await.unwrap();
    let del = s.client.delete(format!("{}/bkt/deleted", s.url)).send().await.unwrap();
    assert_eq!(del.status(), StatusCode::NO_CONTENT);

    let all_hdr = [Hdr::Absent, Hdr::True, Hdr::False];
    let all_obj = [Obj::Exists, Obj::DeleteMarker, Obj::Missing];
    let mut checked = 0usize;

    for &is_head in &[false, true] {
        for &obj in &all_obj {
            let key = match obj {
                Obj::Exists => "exists",
                Obj::DeleteMarker => "deleted",
                Obj::Missing => "missing",
            };
            for &im in &all_hdr {
                for &ius in &all_hdr {
                    for &inm in &all_hdr {
                        for &ims in &all_hdr {
                            let mut hdrs = HeaderMap::new();
                            match im {
                                Hdr::Absent => {}
                                Hdr::True => {
                                    hdrs.insert(IF_MATCH, HeaderValue::from_str(&etag).unwrap());
                                }
                                Hdr::False => {
                                    hdrs.insert(IF_MATCH, HeaderValue::from_static("\"00000000000000000000000000000000\""));
                                }
                            }
                            match ius {
                                Hdr::Absent => {}
                                Hdr::True => {
                                    hdrs.insert(IF_UNMODIFIED_SINCE, HeaderValue::from_str(&format_http_date(lm_secs)).unwrap());
                                }
                                Hdr::False => {
                                    hdrs.insert(IF_UNMODIFIED_SINCE, HeaderValue::from_str(&format_http_date(lm_secs - 10)).unwrap());
                                }
                            }
                            match inm {
                                Hdr::Absent => {}
                                Hdr::True => {
                                    let val = if matches!(obj, Obj::Exists) { etag.as_str() } else { "*" };
                                    hdrs.insert(IF_NONE_MATCH, HeaderValue::from_str(val).unwrap());
                                }
                                Hdr::False => {
                                    hdrs.insert(IF_NONE_MATCH, HeaderValue::from_static("\"00000000000000000000000000000000\""));
                                }
                            }
                            match ims {
                                Hdr::Absent => {}
                                Hdr::True => {
                                    hdrs.insert(IF_MODIFIED_SINCE, HeaderValue::from_str(&format_http_date(lm_secs - 10)).unwrap());
                                }
                                Hdr::False => {
                                    hdrs.insert(IF_MODIFIED_SINCE, HeaderValue::from_str(&format_http_date(lm_secs)).unwrap());
                                }
                            }

                            let req = if is_head {
                                s.client.head(format!("{}/bkt/{}", s.url, key))
                            } else {
                                s.client.get(format!("{}/bkt/{}", s.url, key))
                            };
                            let resp = req.headers(hdrs).send().await.unwrap();
                            let want = expected_status(im, ius, inm, ims, obj);
                            assert_eq!(
                                resp.status(),
                                want,
                                "head={is_head} obj={obj:?} im={im:?} ius={ius:?} inm={inm:?} ims={ims:?}"
                            );
                            if matches!(obj, Obj::Exists) && (want == StatusCode::NOT_MODIFIED || want == StatusCode::PRECONDITION_FAILED) {
                                assert_eq!(resp.headers().get(ETAG).and_then(|v| v.to_str().ok()), Some(etag.as_str()));
                                assert!(resp.headers().get(LAST_MODIFIED).is_some(), "304/412 must carry Last-Modified");
                            }
                            let resp_bytes = resp.bytes().await.unwrap();
                            if want == StatusCode::NOT_MODIFIED || is_head {
                                assert!(resp_bytes.is_empty(), "304 and HEAD must have empty body");
                            } else if want == StatusCode::PRECONDITION_FAILED {
                                let text = String::from_utf8_lossy(&resp_bytes);
                                assert!(text.contains("<Code>PreconditionFailed</Code>"), "GET 412 body: {text}");
                            }
                            checked += 1;
                        }
                    }
                }
            }
        }
    }
    assert_eq!(checked, 2 * 3 * 81);
}

#[tokio::test]
async fn conditional_304_and_412_read_zero_payload_bytes() {
    let s = Server::start("work").await;
    let payload = vec![42u8; 64 * 1024];
    let put = s.client.put(format!("{}/bkt/obj", s.url)).body(payload).send().await.unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    let etag = put.headers().get(ETAG).unwrap().to_str().unwrap().to_string();

    s.state.payload_cache.clear();
    let before = s.state.work.snapshot();
    let res_304 = s
        .client
        .get(format!("{}/bkt/obj", s.url))
        .header(IF_NONE_MATCH, &etag)
        .send()
        .await
        .unwrap();
    assert_eq!(res_304.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(s.state.work.snapshot().since(&before), WorkSnapshot::default(), "304 reads zero payload bytes");

    let before = s.state.work.snapshot();
    let res_412 = s
        .client
        .get(format!("{}/bkt/obj", s.url))
        .header(IF_MATCH, "\"wrong\"")
        .send()
        .await
        .unwrap();
    assert_eq!(res_412.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(s.state.work.snapshot().since(&before), WorkSnapshot::default(), "412 reads zero payload bytes");
}

#[tokio::test]
async fn copy_object_conditional_headers() {
    let s = Server::start("copy").await;
    let put = s.client.put(format!("{}/bkt/src", s.url)).body("copy-src").send().await.unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    let etag = put.headers().get(ETAG).unwrap().to_str().unwrap().to_string();
    let head = s.client.head(format!("{}/bkt/src", s.url)).send().await.unwrap();
    let lm_secs = parse_http_date(head.headers().get(LAST_MODIFIED).unwrap().to_str().unwrap()).unwrap();

    // If-Match good -> 200
    let ok = s
        .client
        .put(format!("{}/bkt/dst1", s.url))
        .header("x-amz-copy-source", "/bkt/src")
        .header("x-amz-copy-source-if-match", &etag)
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), StatusCode::OK);

    // If-Match bad -> 412 PreconditionFailed
    let bad = s
        .client
        .put(format!("{}/bkt/dst2", s.url))
        .header("x-amz-copy-source", "/bkt/src")
        .header("x-amz-copy-source-if-match", "\"nope\"")
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), StatusCode::PRECONDITION_FAILED);
    assert!(bad.text().await.unwrap().contains("<Code>PreconditionFailed</Code>"));

    // If-None-Match matching -> 412 PreconditionFailed (S3 copy-source semantics)
    let inm_match = s
        .client
        .put(format!("{}/bkt/dst3", s.url))
        .header("x-amz-copy-source", "/bkt/src")
        .header("x-amz-copy-source-if-none-match", &etag)
        .send()
        .await
        .unwrap();
    assert_eq!(inm_match.status(), StatusCode::PRECONDITION_FAILED);

    // If-None-Match not matching -> 200
    let inm_ok = s
        .client
        .put(format!("{}/bkt/dst4", s.url))
        .header("x-amz-copy-source", "/bkt/src")
        .header("x-amz-copy-source-if-none-match", "\"different\"")
        .send()
        .await
        .unwrap();
    assert_eq!(inm_ok.status(), StatusCode::OK);

    // If-Modified-Since equal -> 412; older -> 200
    let ims_fail = s
        .client
        .put(format!("{}/bkt/dst5", s.url))
        .header("x-amz-copy-source", "/bkt/src")
        .header("x-amz-copy-source-if-modified-since", format_http_date(lm_secs))
        .send()
        .await
        .unwrap();
    assert_eq!(ims_fail.status(), StatusCode::PRECONDITION_FAILED);

    let ims_ok = s
        .client
        .put(format!("{}/bkt/dst6", s.url))
        .header("x-amz-copy-source", "/bkt/src")
        .header("x-amz-copy-source-if-modified-since", format_http_date(lm_secs - 1))
        .send()
        .await
        .unwrap();
    assert_eq!(ims_ok.status(), StatusCode::OK);

    // If-Unmodified-Since older -> 412; equal -> 200
    let ius_fail = s
        .client
        .put(format!("{}/bkt/dst7", s.url))
        .header("x-amz-copy-source", "/bkt/src")
        .header("x-amz-copy-source-if-unmodified-since", format_http_date(lm_secs - 1))
        .send()
        .await
        .unwrap();
    assert_eq!(ius_fail.status(), StatusCode::PRECONDITION_FAILED);

    let ius_ok = s
        .client
        .put(format!("{}/bkt/dst8", s.url))
        .header("x-amz-copy-source", "/bkt/src")
        .header("x-amz-copy-source-if-unmodified-since", format_http_date(lm_secs))
        .send()
        .await
        .unwrap();
    assert_eq!(ius_ok.status(), StatusCode::OK);
}

#[tokio::test]
async fn versioned_get_and_head_respect_preconditions() {
    let s = Server::start("ver").await;
    assert_eq!(
        s.client
            .put(format!("{}/bkt?versioning", s.url))
            .body("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    let r1 = s.client.put(format!("{}/bkt/k", s.url)).body("v1").send().await.unwrap();
    assert_eq!(r1.status(), StatusCode::OK);
    let vid1 = r1.headers().get("x-amz-version-id").unwrap().to_str().unwrap().to_string();
    let etag1 = r1.headers().get(ETAG).unwrap().to_str().unwrap().to_string();

    let r2 = s.client.put(format!("{}/bkt/k", s.url)).body("v2-longer").send().await.unwrap();
    assert_eq!(r2.status(), StatusCode::OK);
    let etag2 = r2.headers().get(ETAG).unwrap().to_str().unwrap().to_string();

    // Latest version matches etag2 -> 304 on If-None-Match: etag2, 412 on If-Match: etag1
    let g_304 = s
        .client
        .get(format!("{}/bkt/k", s.url))
        .header(IF_NONE_MATCH, &etag2)
        .send()
        .await
        .unwrap();
    assert_eq!(g_304.status(), StatusCode::NOT_MODIFIED);

    let g_412 = s
        .client
        .get(format!("{}/bkt/k", s.url))
        .header(IF_MATCH, &etag1)
        .send()
        .await
        .unwrap();
    assert_eq!(g_412.status(), StatusCode::PRECONDITION_FAILED);

    // Explicit ?versionId=vid1 matches etag1 -> 200 on If-Match: etag1, 304 on If-None-Match: etag1
    let gv_200 = s
        .client
        .get(format!("{}/bkt/k?versionId={}", s.url, vid1))
        .header(IF_MATCH, &etag1)
        .send()
        .await
        .unwrap();
    assert_eq!(gv_200.status(), StatusCode::OK);
    assert_eq!(gv_200.text().await.unwrap(), "v1");

    let hv_304 = s
        .client
        .head(format!("{}/bkt/k?versionId={}", s.url, vid1))
        .header(IF_NONE_MATCH, &etag1)
        .send()
        .await
        .unwrap();
    assert_eq!(hv_304.status(), StatusCode::NOT_MODIFIED);
}
