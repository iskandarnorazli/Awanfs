//! Static website hosting is not built (#364): `?website` on a bucket answers 501
//! NotImplemented for PUT, GET and DELETE. This pins today's answer, so the `static-website`
//! row in `features.toml` has a server test until the feature is built.

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, AuthConfig, S3ServiceState};
use std::net::SocketAddr;
use std::sync::Arc;

const WEBSITE_XML: &str = "<WebsiteConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><IndexDocument><Suffix>index.html</Suffix></IndexDocument></WebsiteConfiguration>";

#[tokio::test]
async fn bucket_website_put_get_and_delete_answer_not_implemented() {
    let mut state = S3ServiceState::new();
    state.auth = AuthConfig::open();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::new(state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/site-bkt", url)).send().await.unwrap().status(), StatusCode::OK);

    for (method, body) in [
        (reqwest::Method::PUT, WEBSITE_XML),
        (reqwest::Method::GET, ""),
        (reqwest::Method::DELETE, ""),
    ] {
        let res = c.request(method.clone(), format!("{}/site-bkt?website", url)).body(body).send().await.unwrap();
        let status = res.status();
        let text = res.text().await.unwrap();
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{} ?website: {}", method, text);
        assert!(text.contains("<Code>NotImplemented</Code>"), "{} ?website: {}", method, text);
    }

    // The bucket itself is untouched: it still lists.
    assert_eq!(c.get(format!("{}/site-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
}
