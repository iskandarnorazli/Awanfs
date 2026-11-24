//! #128: body memory admission controller (`BodyBudget` / `AdmissionGuard`) and server enforcement.

use reqwest::StatusCode;
use s3_server::memory::{BodyBudget, BodyBudgetExhausted};
use s3_server::{start_s3_reactor, S3ServiceState};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn send_head_and_read(addr: SocketAddr, head: &str) -> String {
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(head.as_bytes()).await.unwrap();
    let mut buf = vec![0u8; 4096];
    let n = tokio::time::timeout(Duration::from_secs(5), s.read(&mut buf))
        .await
        .expect("server answered")
        .unwrap();
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

fn parse_metrics(text: &str) -> HashMap<String, f64> {
    let mut out = HashMap::new();
    for line in text.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.rsplit_once(' ') {
            if let Ok(n) = v.parse::<f64>() {
                out.insert(k.to_string(), n);
            }
        }
    }
    out
}

async fn scrape_metrics(client: &reqwest::Client, url: &str) -> HashMap<String, f64> {
    let text = client
        .get(format!("{}/_awanfs/metrics", url))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    parse_metrics(&text)
}

#[test]
fn body_budget_try_acquire_and_drop_releases() {
    let budget = BodyBudget::new(1024);
    assert_eq!(budget.limit(), 1024);
    assert_eq!(budget.in_use(), 0);

    let g1 = budget.try_acquire(600).expect("600 fits in 1024");
    assert_eq!(budget.in_use(), 600);
    assert!(budget.try_acquire(500).is_none(), "600 + 500 > 1024");
    assert_eq!(budget.in_use(), 600);

    let g2 = budget.try_acquire(424).expect("600 + 424 == 1024");
    assert_eq!(budget.in_use(), 1024);

    drop(g1);
    assert_eq!(budget.in_use(), 424);
    drop(g2);
    assert_eq!(budget.in_use(), 0);
}

#[test]
fn body_budget_clamps_oversized_request_so_it_can_run_alone() {
    let budget = BodyBudget::new(6 << 20);
    let g = budget.try_acquire(8 << 20).expect("oversized request runs alone");
    assert_eq!(budget.in_use(), 6 << 20);
    assert!(budget.try_acquire(1).is_none(), "nothing else fits while oversized runs");
    drop(g);
    assert_eq!(budget.in_use(), 0);
}

#[tokio::test]
async fn body_budget_acquire_async_waits_and_rejects_on_timeout() {
    let budget = BodyBudget::with_wait_timeout(1024, Duration::from_millis(30));
    let g1 = budget.try_acquire(1024).unwrap();

    let err = budget.acquire_async(256).await.unwrap_err();
    assert_eq!(err, BodyBudgetExhausted);
    assert_eq!(budget.rejected(), 1);
    assert_eq!(budget.in_use(), 1024);

    let b2 = budget.clone();
    let releaser = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(g1);
    });

    let g2 = b2
        .acquire_with_timeout(256, Duration::from_secs(2))
        .await
        .expect("waiter succeeds once g1 drops");
    assert_eq!(budget.in_use(), 256);
    drop(g2);
    assert_eq!(budget.in_use(), 0);
    releaser.await.unwrap();
}

#[tokio::test]
async fn server_bounds_concurrent_puts_and_returns_503_slowdown_when_exhausted() {
    let mut st = S3ServiceState::new();
    st.body_budget = Arc::new(BodyBudget::with_wait_timeout(4 << 20, Duration::from_millis(30)));
    let state = Arc::new(st);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let client = reqwest::Client::new();

    assert_eq!(
        client.put(format!("{}/admit", url)).send().await.unwrap().status(),
        StatusCode::OK
    );

    // Hold 3 MiB in flight by opening a PUT and sending only the headers + partial body.
    let mut slow = TcpStream::connect(handle.local_addr).await.unwrap();
    let req_head = format!(
        "PUT /admit/slow HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\n\r\n",
        handle.local_addr,
        3 << 20
    );
    slow.write_all(req_head.as_bytes()).await.unwrap();
    slow.write_all(&[b'a'; 1024]).await.unwrap();
    slow.flush().await.unwrap();

    // Wait until the server has admitted the 3 MiB PUT.
    let mut admitted = false;
    for _ in 0..50 {
        let m = scrape_metrics(&client, &url).await;
        if m.get("awanfs_memory_bodies_in_use_bytes").copied() == Some((3 << 20) as f64) {
            admitted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(admitted, "stalled 3 MiB PUT was not admitted");

    // A 2 MiB PUT exceeds the 4 MiB bodies budget (3 + 2 > 4) and must return 503 SlowDown.
    let resp = send_head_and_read(
        handle.local_addr,
        &format!(
            "PUT /admit/too-much HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\n\r\n",
            handle.local_addr,
            2 << 20
        ),
    )
    .await;
    assert!(resp.starts_with("HTTP/1.1 503"), "expected 503, got: {}", resp);
    assert!(resp.contains("<Code>SlowDown</Code>"), "expected SlowDown XML, got: {}", resp);
    assert!(
        resp.lines().any(|l| l.trim_end_matches('\r').eq_ignore_ascii_case("retry-after: 1")),
        "expected Retry-After: 1 header, got: {}",
        resp
    );

    // Rejected over-budget PUT must not write or index the object.
    assert_eq!(
        client.get(format!("{}/admit/too-much", url)).send().await.unwrap().status(),
        StatusCode::NOT_FOUND
    );

    let m = scrape_metrics(&client, &url).await;
    assert!(m.contains_key("awanfs_memory_bodies_rejected_total"));
    assert_eq!(m["awanfs_memory_bodies_rejected_total"], 1.0);
    assert_eq!(m["awanfs_memory_bodies_in_use_bytes"], (3 << 20) as f64);

    // A 512 KiB PUT still fits (3 MiB + 512 KiB <= 4 MiB) and succeeds.
    let small = client
        .put(format!("{}/admit/fits", url))
        .body(vec![b'c'; 512 << 10])
        .send()
        .await
        .unwrap();
    assert_eq!(small.status(), StatusCode::OK);

    // Disconnect the slow client mid-body; the Drop guard must release its 3 MiB.
    drop(slow);

    let mut released = false;
    for _ in 0..50 {
        let m = scrape_metrics(&client, &url).await;
        if m.get("awanfs_memory_bodies_in_use_bytes").copied() == Some(0.0) {
            released = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(released, "bodies in use did not return to 0 after client disconnect");

    // Now the 2 MiB PUT succeeds.
    let ok = client
        .put(format!("{}/admit/now-fits", url))
        .body(vec![b'b'; 2 << 20])
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), StatusCode::OK);
    let m = scrape_metrics(&client, &url).await;
    assert_eq!(m["awanfs_memory_bodies_in_use_bytes"], 0.0);

    // Non-S3 readiness probe stays 503 without Retry-After when not ready.
    state.set_not_ready(Some("draining"));
    let ready = client.get(format!("{}/_awanfs/health/ready", url)).send().await.unwrap();
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(ready.headers().get("retry-after").is_none());

    handle.shutdown();
}

#[tokio::test]
async fn server_admits_upload_part_and_xml_bodies_and_releases_on_error() {
    let mut st = S3ServiceState::new();
    st.body_budget = Arc::new(BodyBudget::with_wait_timeout(2 << 20, Duration::from_millis(30)));
    let state = Arc::new(st);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let client = reqwest::Client::new();

    assert_eq!(
        client.put(format!("{}/admit2", url)).send().await.unwrap().status(),
        StatusCode::OK
    );

    // Initiate multipart upload.
    let init = client
        .post(format!("{}/admit2/obj?uploads", url))
        .send()
        .await
        .unwrap();
    assert_eq!(init.status(), StatusCode::OK);
    let init_xml = init.text().await.unwrap();
    let s = init_xml.find("<UploadId>").unwrap() + "<UploadId>".len();
    let e = init_xml[s..].find("</UploadId>").unwrap() + s;
    let upload_id = init_xml[s..e].to_string();

    // Hold 1.5 MiB via a stalled DeleteObjects XML POST.
    let mut slow_xml = TcpStream::connect(handle.local_addr).await.unwrap();
    let req_head = format!(
        "POST /admit2?delete HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\n\r\n",
        handle.local_addr,
        1536 << 10
    );
    slow_xml.write_all(req_head.as_bytes()).await.unwrap();
    slow_xml.write_all(b"<Delete>").await.unwrap();
    slow_xml.flush().await.unwrap();

    let mut admitted = false;
    for _ in 0..50 {
        let m = scrape_metrics(&client, &url).await;
        if m.get("awanfs_memory_bodies_in_use_bytes").copied() == Some((1536 << 10) as f64) {
            admitted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(admitted, "stalled XML body was not admitted");

    // A 1 MiB UploadPart exceeds 2 MiB (1.5 + 1 > 2) and must return 503 SlowDown.
    let part_rej = send_head_and_read(
        handle.local_addr,
        &format!(
            "PUT /admit2/obj?partNumber=1&uploadId={} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\n\r\n",
            upload_id,
            handle.local_addr,
            1 << 20
        ),
    )
    .await;
    assert!(part_rej.starts_with("HTTP/1.1 503"), "expected 503, got: {}", part_rej);
    assert!(part_rej.contains("<Code>SlowDown</Code>"), "expected SlowDown XML, got: {}", part_rej);
    assert!(
        part_rej.lines().any(|l| l.trim_end_matches('\r').eq_ignore_ascii_case("retry-after: 1")),
        "expected Retry-After: 1 header on UploadPart 503, got: {}",
        part_rej
    );

    // Drop stalled XML client and verify release.
    drop(slow_xml);
    let mut released = false;
    for _ in 0..50 {
        let m = scrape_metrics(&client, &url).await;
        if m.get("awanfs_memory_bodies_in_use_bytes").copied() == Some(0.0) {
            released = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(released, "XML body permit was not released on disconnect");

    // Error path: UploadPart to nonexistent uploadId releases its permit.
    let err_part = client
        .put(format!("{}/admit2/obj?partNumber=1&uploadId=missing-id", url))
        .body(vec![b'p'; 64 << 10])
        .send()
        .await
        .unwrap();
    assert_eq!(err_part.status(), StatusCode::NOT_FOUND);
    let m = scrape_metrics(&client, &url).await;
    assert_eq!(m["awanfs_memory_bodies_in_use_bytes"], 0.0);

    // Error path: PutBucketLifecycle with malformed XML releases its permit.
    let err_lc = client
        .put(format!("{}/admit2?lifecycle", url))
        .body("<not-valid-lifecycle/>")
        .send()
        .await
        .unwrap();
    assert_eq!(err_lc.status(), StatusCode::BAD_REQUEST);
    let m = scrape_metrics(&client, &url).await;
    assert_eq!(m["awanfs_memory_bodies_in_use_bytes"], 0.0);

    // Valid UploadPart succeeds and releases its permit.
    let ok_part = client
        .put(format!("{}/admit2/obj?partNumber=1&uploadId={}", url, upload_id))
        .body(vec![b'p'; 1 << 20])
        .send()
        .await
        .unwrap();
    assert_eq!(ok_part.status(), StatusCode::OK);
    let m = scrape_metrics(&client, &url).await;
    assert_eq!(m["awanfs_memory_bodies_in_use_bytes"], 0.0);

    handle.shutdown();
}
