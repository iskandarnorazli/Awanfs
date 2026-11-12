//! #41: every response carries a unique x-amz-request-id, error bodies carry the same ID,
//! and each request writes one JSON log line with it.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn request_ids_are_unique_and_match_the_error_body_and_the_log_line() {
    let state = Arc::new(S3ServiceState::new());
    let lines: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let lines = Arc::clone(&lines);
        let sink: s3_server::request_log::LogSink = Arc::new(move |l: &str| lines.lock().unwrap().push(l.to_string()));
        *state.access_log.write() = Some(sink);
    }
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();

    let mut ids = Vec::new();
    for _ in 0..2 {
        let res = c.get(format!("{}/no-such-bucket/k", url)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let id = res.headers().get("x-amz-request-id").expect("x-amz-request-id").to_str().unwrap().to_string();
        let body = res.text().await.unwrap();
        assert!(body.contains(&format!("<RequestId>{}</RequestId>", id)), "error body carries the request ID {}: {}", id, body);
        ids.push(id);
    }
    assert_ne!(ids[0], ids[1]);

    assert_eq!(c.put(format!("{}/log-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    lines.lock().unwrap().clear();
    let res = c.put(format!("{}/log-bkt/a%20key", url)).body("hello").send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let id = res.headers().get("x-amz-request-id").unwrap().to_str().unwrap().to_string();
    let got = lines.lock().unwrap().clone();
    assert_eq!(got.len(), 1, "one line per request: {:?}", got);
    let l = &got[0];
    assert!(l.starts_with('{') && l.ends_with('}'), "{}", l);
    for want in [
        format!("\"request_id\":\"{}\"", id),
        "\"status\":200".to_string(),
        "\"method\":\"PUT\"".to_string(),
        "\"operation\":\"PutObject\"".to_string(),
        "\"bucket\":\"log-bkt\"".to_string(),
        "\"bytes_in\":5".to_string(),
    ] {
        assert!(l.contains(&want), "{} in {}", want, l);
    }
}
