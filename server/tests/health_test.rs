//! #44: /_awanfs/health/live and /_awanfs/health/ready, without authentication. The prefix
//! starts with `_`, which bucket names cannot contain, so no bucket is shadowed.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, AuthConfig, S3ServiceState};
use std::net::SocketAddr;
use std::sync::Arc;

async fn server(state: Arc<S3ServiceState>) -> (String, s3_server::ServerHandle) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, state).await.unwrap();
    (format!("http://{}", handle.local_addr), handle)
}

#[tokio::test]
async fn live_and_ready_answer_without_credentials() {
    let mut st = S3ServiceState::new();
    st.auth = AuthConfig { credentials: Some(("AK".into(), "SK".into())), allow_anonymous: false };
    let state = Arc::new(st);
    let (url, _h) = server(Arc::clone(&state)).await;
    let c = reqwest::Client::new();
    assert_eq!(c.get(format!("{}/", url)).send().await.unwrap().status(), StatusCode::FORBIDDEN, "S3 still needs a signature");
    assert_eq!(c.get(format!("{}/_awanfs/health/live", url)).send().await.unwrap().status(), StatusCode::OK);
    let res = c.get(format!("{}/_awanfs/health/ready", url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.text().await.unwrap().trim(), "ready");
    assert_eq!(c.get(format!("{}/_awanfs/nothing", url)).send().await.unwrap().status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn ready_is_503_while_recovering_and_when_no_needle_can_fit() {
    let state = Arc::new(S3ServiceState::new());
    let (url, _h) = server(Arc::clone(&state)).await;
    let c = reqwest::Client::new();

    state.set_not_ready(Some("recovering"));
    let res = c.get(format!("{}/_awanfs/health/ready", url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(res.text().await.unwrap().contains("recovering"));
    assert_eq!(c.get(format!("{}/_awanfs/health/live", url)).send().await.unwrap().status(), StatusCode::OK, "live stays 200");
    state.set_not_ready(None);
    assert_eq!(c.get(format!("{}/_awanfs/health/ready", url)).send().await.unwrap().status(), StatusCode::OK);

    state.set_volume_span(4096);
    let res = c.get(format!("{}/_awanfs/health/ready", url)).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE, "a span with no room for a needle");
    assert!(res.text().await.unwrap().contains("span"));
}

#[tokio::test]
async fn buckets_named_health_still_work() {
    let state = Arc::new(S3ServiceState::new());
    let (url, _h) = server(state).await;
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/health", url)).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{}/health/live", url)).body("object").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.get(format!("{}/health/live", url)).send().await.unwrap().text().await.unwrap(), "object");
}
