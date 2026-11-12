//! #30: HTTPS with rustls. A client that trusts the certificate can PUT and GET; one that does
//! not fails the handshake; plain HTTP still works without a certificate; a new certificate
//! is picked up by reload without a restart.

use reqwest::{Certificate, StatusCode};
use s3_server::tls::{server_config, ReloadingCert};
use s3_server::{start_s3_reactor, start_s3_reactor_with, S3ServiceState};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls").join(name)
}

fn client_trusting(cert: &str) -> reqwest::Client {
    let pem = std::fs::read(fixture(cert)).unwrap();
    reqwest::Client::builder().add_root_certificate(Certificate::from_pem(&pem).unwrap()).build().unwrap()
}

#[tokio::test]
async fn https_put_and_get_with_a_trusted_certificate_and_refused_otherwise() {
    let rc = ReloadingCert::open(&fixture("cert.pem"), &fixture("key.pem")).unwrap();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor_with(addr, Arc::new(S3ServiceState::new()), Some(server_config(Arc::clone(&rc)))).await.unwrap();
    let url = format!("https://localhost:{}", handle.local_addr.port());

    let c = client_trusting("cert.pem");
    assert_eq!(c.put(format!("{}/tls-bkt", url)).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.put(format!("{}/tls-bkt/k", url)).body("over tls").send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(c.get(format!("{}/tls-bkt/k", url)).send().await.unwrap().text().await.unwrap(), "over tls");

    let untrusting = reqwest::Client::new();
    assert!(untrusting.get(format!("{}/tls-bkt/k", url)).send().await.is_err(), "a client that does not trust the certificate");
    let other = client_trusting("cert2.pem");
    assert!(other.get(format!("{}/tls-bkt/k", url)).send().await.is_err(), "trusting a different certificate");
    let plain = reqwest::Client::new().get(format!("http://127.0.0.1:{}/tls-bkt/k", handle.local_addr.port())).send().await;
    assert!(plain.map(|r| !r.status().is_success()).unwrap_or(true), "plain HTTP on the TLS port gets no object");

    // Swap in the second certificate: new connections present it.
    let dir = std::env::temp_dir().join(format!("awanfs_tls_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (cp, kp) = (dir.join("cert.pem"), dir.join("key.pem"));
    std::fs::copy(fixture("cert.pem"), &cp).unwrap();
    std::fs::copy(fixture("key.pem"), &kp).unwrap();
    let rc2 = ReloadingCert::open(&cp, &kp).unwrap();
    let handle2 = start_s3_reactor_with(addr, Arc::new(S3ServiceState::new()), Some(server_config(Arc::clone(&rc2)))).await.unwrap();
    let url2 = format!("https://localhost:{}", handle2.local_addr.port());
    assert!(client_trusting("cert.pem").get(format!("{}/", url2)).send().await.is_ok());
    std::fs::copy(fixture("cert2.pem"), &cp).unwrap();
    std::fs::copy(fixture("key2.pem"), &kp).unwrap();
    assert!(rc2.reload().unwrap());
    assert!(client_trusting("cert2.pem").get(format!("{}/", url2)).send().await.is_ok(), "the reloaded certificate is presented");
    assert!(client_trusting("cert.pem").get(format!("{}/", url2)).send().await.is_err(), "the old one is not");

    // A broken file keeps the current certificate.
    std::fs::write(&cp, b"not a certificate").unwrap();
    assert!(rc2.reload().is_err());
    assert!(client_trusting("cert2.pem").get(format!("{}/", url2)).send().await.is_ok());
}

#[tokio::test]
async fn plain_http_still_works_without_a_certificate() {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::new(S3ServiceState::new())).await.unwrap();
    let c = reqwest::Client::new();
    assert_eq!(c.put(format!("http://{}/plain-bkt", handle.local_addr)).send().await.unwrap().status(), StatusCode::OK);
}

#[test]
fn cert_without_key_stops_the_binary() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_s3-server"))
        .args(["--port", "0", "--allow-anonymous", "--cert", fixture("cert.pem").to_str().unwrap()])
        .env_remove("AWANFS_TLS_KEY")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
}
