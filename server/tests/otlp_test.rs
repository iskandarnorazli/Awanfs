//! #43: with an OTLP endpoint, a PUT is exported as a request span (with the request ID from
//! x-amz-request-id) and child spans for the volume write and sync; a traceparent header
//! continues the caller's trace. The collector is an in-process HTTP stub.

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

async fn collector() -> (String, Arc<Mutex<Vec<String>>>) {
    let bodies: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let store = Arc::clone(&bodies);
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let store = Arc::clone(&store);
            tokio::spawn(async move {
                let svc = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                    let store = Arc::clone(&store);
                    async move {
                        let path = req.uri().path().to_string();
                        let body = req.into_body().collect().await.unwrap().to_bytes();
                        if path == "/v1/traces" {
                            store.lock().unwrap().push(String::from_utf8_lossy(&body).into_owned());
                        }
                        Ok::<_, std::convert::Infallible>(hyper::Response::new(Full::new(Bytes::from_static(b"{}"))))
                    }
                });
                let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new()).serve_connection(TokioIo::new(stream), svc).await;
            });
        }
    });
    (format!("http://{}", addr), bodies)
}

/// The value of `"field":"..."` in a span object of the JSON text.
fn spans(all: &str) -> Vec<String> {
    all.split("{\"traceId\"").skip(1).map(|s| format!("{{\"traceId\"{}", s)).collect()
}

fn field<'a>(span: &'a str, name: &str) -> Option<&'a str> {
    let pat = format!("\"{}\":\"", name);
    let i = span.find(&pat)? + pat.len();
    Some(&span[i..i + span[i..].find('"')?])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_put_exports_a_request_span_with_write_and_sync_children() {
    let (endpoint, bodies) = collector().await;
    s3_server::trace::init(&endpoint, "awanfs-test").unwrap();
    let dir = std::env::temp_dir().join(format!("awanfs_otlp_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::new(S3ServiceState::with_data_dir(&dir))).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/ot-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    let res = c
        .put(format!("{}/ot-bkt/k", url))
        .header("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        .body("traced")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let request_id = res.headers().get("x-amz-request-id").unwrap().to_str().unwrap().to_string();

    // The exporter sends once a second; wait for the request's spans to arrive.
    let mut found = None;
    for _ in 0..100 {
        let all = bodies.lock().unwrap().join("\n");
        let list = spans(&all);
        if let Some(req) = list.iter().find(|s| s.contains(&request_id)) {
            let id = field(req, "spanId").unwrap().to_string();
            let children: Vec<String> = list.iter().filter(|s| field(s, "parentSpanId") == Some(id.as_str())).map(|s| field(s, "name").unwrap().to_string()).collect();
            if children.contains(&"volume.write".to_string()) && children.contains(&"volume.sync".to_string()) {
                found = Some((req.clone(), children));
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (req, children) = found.expect("request span with volume.write and volume.sync children");
    assert_eq!(field(&req, "name"), Some("s3.request"));
    assert_eq!(field(&req, "traceId"), Some("4bf92f3577b34da6a3ce929d0e0e4736"), "the caller's trace is continued");
    assert_eq!(field(&req, "parentSpanId"), Some("00f067aa0ba902b7"));
    assert!(req.contains("\"key\":\"operation\",\"value\":{\"stringValue\":\"PutObject\"}"), "{}", req);
    assert!(req.contains("\"key\":\"status\",\"value\":{\"stringValue\":\"200\"}"), "{}", req);
    assert!(children.len() >= 2, "{:?}", children);
}

#[test]
fn traceparent_parsing() {
    let (t, s) = s3_server::trace::parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01").unwrap();
    assert_eq!(t[0], 0x4b);
    assert_eq!(s[7], 0xb7);
    assert!(s3_server::trace::parse_traceparent("00-00000000000000000000000000000000-00f067aa0ba902b7-01").is_none());
    assert!(s3_server::trace::parse_traceparent("garbage").is_none());
}
