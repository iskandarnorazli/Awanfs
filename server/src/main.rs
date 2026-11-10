//! awan-storage/server/src/main.rs
//! Awan Storage S3 Server binary entry point

use awan_storage_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut port = 9000u16;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--port" || arg == "-p" {
            if let Some(p_str) = args.next() {
                if let Ok(p) = p_str.parse::<u16>() {
                    port = p;
                }
            }
        }
    }

    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;
    let state = Arc::new(S3ServiceState::new());

    println!("Awan Storage S3 Server listening on http://{}", addr);
    let handle = start_s3_reactor(addr, state).await?;

    tokio::signal::ctrl_c().await?;
    println!("Shutdown signal received, exiting...");
    handle.shutdown();
    Ok(())
}
