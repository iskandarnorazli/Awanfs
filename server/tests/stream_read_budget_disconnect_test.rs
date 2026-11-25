//! #155 Step 4: Client disconnect releases read buffers and body_budget immediately.
//! Own test binary because `readers_in_flight()` is process-wide.

use reqwest::StatusCode;
use s3_server::memory::BodyBudget;
use s3_server::stream_read::readers_in_flight;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MIB: usize = 1024 * 1024;

#[tokio::test]
async fn client_disconnect_releases_read_budget_and_reader_immediately() {
    let dir = std::env::temp_dir().join(format!("awanfs_srb_disc_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let mut state = S3ServiceState::with_data_dir(&dir);
    let tight_budget = Arc::new(BodyBudget::new(8 * MIB as u64));
    state.body_budget = Arc::clone(&tight_budget);
    let state = Arc::new(state);
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&state)).await.unwrap();
    let url = format!("http://{}", handle.local_addr);
    let c = reqwest::Client::new();

    assert_eq!(
        c.put(format!("{}/dbkt", url)).send().await.unwrap().status(),
        StatusCode::OK
    );
    let data: Vec<u8> = (0..10 * MIB + 333)
        .map(|i| ((i * 131 + 13) % 251) as u8)
        .collect();
    assert_eq!(
        c.put(format!("{}/dbkt/big", url))
            .body(data)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    let host = url.trim_start_matches("http://").to_string();
    let mut s = tokio::net::TcpStream::connect(&host).await.unwrap();
    s.write_all(format!("GET /dbkt/big HTTP/1.1\r\nHost: {}\r\n\r\n", host).as_bytes())
        .await
        .unwrap();
    let mut hdr = vec![0u8; 4096];
    let n = s.read(&mut hdr).await.unwrap();
    assert!(n > 0);
    for _ in 0..100 {
        if readers_in_flight() >= 1 && tight_budget.in_use() > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(tight_budget.in_use() > 0);
    drop(s);
    for _ in 0..300 {
        if readers_in_flight() == 0 && tight_budget.in_use() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(readers_in_flight(), 0);
    assert_eq!(tight_budget.in_use(), 0);

    handle.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
