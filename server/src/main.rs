//! awan-storage/server/src/main.rs
//! S3 Storage Server binary entry point

use s3_server::{start_s3_reactor, AuthConfig, Durability, S3ServiceState};
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

    let mut listen = std::env::var("AWANFS_LISTEN").unwrap_or_else(|_| "127.0.0.1".to_string());
    let mut allow_anonymous = std::env::var("AWANFS_ALLOW_ANONYMOUS").ok().as_deref() == Some("1");
    let mut durability = std::env::var("AWANFS_DURABILITY").unwrap_or_else(|_| "group".to_string());

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--port" || arg == "-p" {
            if let Some(p_str) = args.next() {
                if let Ok(p) = p_str.parse::<u16>() {
                    port = p;
                }
            }
        } else if arg == "--listen" {
            if let Some(a) = args.next() {
                listen = a;
            }
        } else if arg == "--durability" {
            if let Some(d) = args.next() {
                durability = d;
            }
        } else if arg == "--allow-anonymous" {
            allow_anonymous = true;
        } else if arg == "--data-dir" || arg == "-d" {
            if let Some(d_str) = args.next() {
                data_dir = Some(PathBuf::from(d_str));
            }
        }
    }

    let auth = AuthConfig::from_env(allow_anonymous);
    if auth.credentials.is_none() && !auth.allow_anonymous {
        eprintln!("No credentials: set AWANFS_ACCESS_KEY and AWANFS_SECRET_KEY, or pass --allow-anonymous to let anyone who can reach the port read, write and delete.");
        std::process::exit(2);
    }
    let durability = match Durability::parse(&durability) {
        Some(d) => d,
        None => {
            eprintln!("--durability must be group (default), fast or strict, not {:?}", durability);
            std::process::exit(2);
        }
    };
    let addr: SocketAddr = format!("{}:{}", listen, port).parse()?;
    let mut state = match data_dir {
        Some(ref dir) => S3ServiceState::with_data_dir(dir),
        None => S3ServiceState::new(),
    };
    state.auth = auth;
    state.set_durability(durability);
    // One JSON line per request on stderr; AWANFS_LOG=off turns it off.
    if std::env::var("AWANFS_LOG").ok().as_deref() != Some("off") {
        let sink: s3_server::request_log::LogSink = Arc::new(|line: &str| eprintln!("{}", line));
        *state.access_log.write() = Some(sink);
    }
    let state = Arc::new(state);
    let state_for_stop = Arc::clone(&state);

    println!("S3 Storage Server listening on http://{}", addr);
    let handle = start_s3_reactor(addr, state).await?;

    wait_for_stop_signal().await;
    eprintln!("stop signal received: finishing requests in flight");
    // Readiness goes false first, so a load balancer stops sending new requests.
    state_for_stop.set_not_ready(Some("shutting down"));
    let finished = handle.shutdown_gracefully(std::time::Duration::from_secs(30)).await;
    if !finished {
        eprintln!("some connections were still open after 30 s");
    }
    if let Err(e) = state_for_stop.sync_all_volumes() {
        eprintln!("final sync failed: {:?}", e);
        std::process::exit(1);
    }
    eprintln!("stopped");
    Ok(())
}

/// Ctrl-C everywhere; SIGTERM on Unix; Ctrl-Break, console close and system shutdown on Windows.
async fn wait_for_stop_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(windows)]
    {
        use tokio::signal::windows;
        let mut brk = windows::ctrl_break().expect("Ctrl-Break handler");
        let mut close = windows::ctrl_close().expect("console close handler");
        let mut shut = windows::ctrl_shutdown().expect("shutdown handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = brk.recv() => {}
            _ = close.recv() => {}
            _ = shut.recv() => {}
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
