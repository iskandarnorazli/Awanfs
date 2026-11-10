//! awan-storage/server/src/main.rs
//! S3 Storage Server binary entry point

use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut port: u16 = std::env::var("S3_PORT")
        .or_else(|_| std::env::var("AWAN_S3_PORT"))
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9000);
    let mut data_dir: Option<PathBuf> = std::env::var("S3_DATA_DIR")
        .or_else(|_| std::env::var("AWAN_S3_DATA_DIR"))
        .ok()
        .map(PathBuf::from);

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--port" || arg == "-p" {
            if let Some(p_str) = args.next() {
                if let Ok(p) = p_str.parse::<u16>() {
                    port = p;
                }
            }
        } else if arg == "--data-dir" || arg == "-d" {
            if let Some(d_str) = args.next() {
                data_dir = Some(PathBuf::from(d_str));
            }
        }
    }

    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;
    let state = match data_dir {
        Some(ref dir) => Arc::new(S3ServiceState::with_data_dir(dir)),
        None => Arc::new(S3ServiceState::new()),
    };

    println!("S3 Storage Server listening on http://{}", addr);
    let handle = start_s3_reactor(addr, state).await?;

    tokio::signal::ctrl_c().await?;
    println!("Shutdown signal received, exiting...");
    handle.shutdown();
    Ok(())
}
