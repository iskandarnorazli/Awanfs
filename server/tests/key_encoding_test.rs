//! Object keys are the percent-decoded URL path, as in S3: "my%20file.txt" is the key
//! "my file.txt", "%2B" and "+" are both "+", and CopyObject's x-amz-copy-source is decoded the
//! same way. Keys were stored as sent, so listings showed "my%20file.txt".

use reqwest::StatusCode;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::sync::Arc;

#[tokio::test]
async fn keys_are_the_decoded_path() {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::new(S3ServiceState::new())).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("{}/enc-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{}/enc-bkt/my%20file.txt", url)).body("one").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{}/enc-bkt/a+b", url)).body("two").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{}/enc-bkt/caf%C3%A9", url)).body("three").send().await.unwrap().status(), StatusCode::OK);

    let list = c.get(format!("{}/enc-bkt?list-type=2", url)).send().await.unwrap().text().await.unwrap();
    for k in ["<Key>my file.txt</Key>", "<Key>a+b</Key>", "<Key>café</Key>"] {
        assert!(list.contains(k), "{} in {}", k, list);
    }
    assert!(!list.contains("%20"), "{}", list);
    assert_eq!(c.get(format!("{}/enc-bkt/a%2Bb", url)).send().await.unwrap().text().await.unwrap(), "two", "%2B and + name the same key");

    let res = c.put(format!("{}/enc-bkt/copy", url)).header("x-amz-copy-source", "/enc-bkt/my%20file.txt").send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "copy source is decoded");
    assert_eq!(c.get(format!("{}/enc-bkt/copy", url)).send().await.unwrap().text().await.unwrap(), "one");
}
