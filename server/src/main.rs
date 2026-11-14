//! awan-storage/server/src/main.rs
//! S3 Storage Server binary entry point

use s3_server::{start_s3_reactor_with, AuthConfig, Durability, S3ServiceState};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

/// glibc raises its mmap threshold after a large block is freed, so later 1 MiB buffers come from
/// the heap and stay there once freed. Under 64 clients writing then reading 1 MiB objects that
/// kept about 140 MiB: peak RSS 539.8 MiB by default, 291.2 MiB with the threshold fixed, at the
/// same throughput (#127, run 43535764903). Owner decision 2026-11-14: fix it in the process, at
/// glibc's own starting value, unless the operator set MALLOC_MMAP_THRESHOLD_.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn fix_mmap_threshold() -> &'static str {
    if std::env::var_os("MALLOC_MMAP_THRESHOLD_").is_some() {
        return "glibc mmap threshold left as MALLOC_MMAP_THRESHOLD_ sets it";
    }
    // SAFETY: mallopt only sets an allocator parameter; it is called once, first thing in main.
    if unsafe { libc::mallopt(libc::M_MMAP_THRESHOLD, 128 * 1024) } == 1 {
        "glibc mmap threshold fixed at 128 KiB, so freed large buffers go back to the OS"
    } else {
        "glibc mmap threshold could not be fixed: freed large buffers may stay in the heap"
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn fix_mmap_threshold() -> &'static str {
    "the platform's allocator, unchanged"
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let allocator = fix_mmap_threshold();
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
    let mut access_log: Option<String> = None;
    let mut config_path: Option<PathBuf> = None;
    let mut tls_cert: Option<PathBuf> = std::env::var("AWANFS_TLS_CERT").ok().filter(|s| !s.is_empty()).map(PathBuf::from);
    let mut tls_key: Option<PathBuf> = std::env::var("AWANFS_TLS_KEY").ok().filter(|s| !s.is_empty()).map(PathBuf::from);
    let mut http_port: Option<u16> = None;
    let mut compaction_mb: Option<u64> = std::env::var("AWANFS_COMPACTION_RATE_MB").ok().and_then(|v| v.parse().ok());
    let mut otlp: Option<String> = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok().filter(|s| !s.is_empty());
    let mut auto_compaction = std::env::var("AWANFS_AUTO_COMPACTION").ok().as_deref() != Some("off");
    let mut durability = std::env::var("AWANFS_DURABILITY").unwrap_or_else(|_| "group".to_string());
    let mut memory_budget: Option<String> = None;

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
        } else if arg == "--cert" {
            tls_cert = args.next().map(PathBuf::from);
        } else if arg == "--key" {
            tls_key = args.next().map(PathBuf::from);
        } else if arg == "--http-port" {
            http_port = args.next().and_then(|p| p.parse().ok());
        } else if arg == "--compaction-rate" {
            compaction_mb = args.next().and_then(|v| v.parse::<u64>().ok());
        } else if arg == "--otlp-endpoint" {
            otlp = args.next();
        } else if arg == "--auto-compaction" {
            auto_compaction = args.next().as_deref() != Some("off");
        } else if arg == "--access-log" {
            access_log = args.next();
        } else if arg == "--config" {
            config_path = args.next().map(PathBuf::from);
        } else if arg == "--memory-budget" {
            memory_budget = args.next();
        } else if arg == "--allow-anonymous" {
            allow_anonymous = true;
        } else if arg == "--data-dir" || arg == "-d" {
            if let Some(d_str) = args.next() {
                data_dir = Some(PathBuf::from(d_str));
            }
        }
    }

    // One budget for the cache and for in-flight bodies (#127). Set before the server is
    // built, because the cache is sized from it.
    let budget = match s3_server::memory::resolve(
        memory_budget.as_deref(),
        std::env::var("AWANFS_MEMORY_BUDGET").ok().as_deref(),
        s3_server::memory::detect(),
    ) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("{}", e);
            std::process::exit(2);
        }
    };
    if let Err(in_force) = s3_server::memory::configure(budget) {
        eprintln!("memory budget was already set to {} bytes", in_force.total);
    }
    let mib = |n: u64| n / (1 << 20);
    println!(
        "Memory budget {} MiB ({}): payload cache {} MiB, in-flight request bodies {} MiB",
        mib(budget.total),
        budget.source.as_str(),
        mib(budget.cache),
        mib(budget.bodies)
    );
    println!("Allocator: {}", allocator);

    let auth = AuthConfig::from_env(allow_anonymous);
    // Users and policies (#53): --config <file> or AWANFS_CONFIG.
    let iam = match config_path.or_else(|| std::env::var("AWANFS_CONFIG").ok().filter(|s| !s.is_empty()).map(PathBuf::from)) {
        Some(p) => match s3_server::iam::ReloadingIam::open(&p) {
            Ok(r) => Some(r),
            Err(e) => {
                eprintln!("cannot load the config: {}", e);
                std::process::exit(2);
            }
        },
        None => None,
    };
    let has_users = iam.as_ref().is_some_and(|r| !r.current().is_empty());
    if auth.credentials.is_none() && !has_users && !auth.allow_anonymous {
        eprintln!("No credentials: set AWANFS_ACCESS_KEY and AWANFS_SECRET_KEY, give users in --config, or pass --allow-anonymous to let anyone who can reach the port read, write and delete.");
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
        Some(ref dir) => match S3ServiceState::try_with_data_dir(dir) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("cannot serve {}: {}", dir.display(), e);
                std::process::exit(2);
            }
        },
        None => S3ServiceState::new(),
    };
    state.auth = auth;
    if let Some(r) = iam {
        r.watch(std::time::Duration::from_secs(60));
        *state.iam.write() = Some(r);
    }
    state.set_durability(durability);
    // Compaction copies at most this many MB/s (#28, decision 24); 0 = no limit.
    state.compaction_rate.store(compaction_mb.unwrap_or(50) * 1_000_000, std::sync::atomic::Ordering::Relaxed);
    // Traces (#43): off unless an OTLP endpoint is given.
    if let Some(ref endpoint) = otlp {
        if let Err(e) = s3_server::trace::init(endpoint, "awanfs") {
            eprintln!("cannot start OTLP export: {}", e);
            std::process::exit(2);
        }
    }
    // S3 server access log (#46): --access-log <file> or -, or AWANFS_ACCESS_LOG; off by default.
    if let Some(target) = access_log.or_else(|| std::env::var("AWANFS_ACCESS_LOG").ok()).filter(|t| !t.is_empty()) {
        let sink: s3_server::request_log::LogSink = if target == "-" {
            Arc::new(|line: &str| println!("{}", line))
        } else {
            let file = std::fs::OpenOptions::new().create(true).append(true).open(&target).unwrap_or_else(|e| {
                eprintln!("cannot open the access log {}: {}", target, e);
                std::process::exit(2);
            });
            let file = std::sync::Mutex::new(file);
            Arc::new(move |line: &str| {
                use std::io::Write;
                if let Ok(mut f) = file.lock() {
                    let _ = writeln!(f, "{}", line);
                }
            })
        };
        *state.audit_log.write() = Some(sink);
    }
    // One JSON line per request on stderr; AWANFS_LOG=off turns it off.
    if std::env::var("AWANFS_LOG").ok().as_deref() != Some("off") {
        let sink: s3_server::request_log::LogSink = Arc::new(|line: &str| eprintln!("{}", line));
        *state.access_log.write() = Some(sink);
    }
    let state = Arc::new(state);
    let state_for_stop = Arc::clone(&state);
    // Automatic compaction (owner decision 15): on unless --auto-compaction off.
    if auto_compaction {
        state.start_auto_compaction(std::time::Duration::from_secs(600), 0.20, 1 << 30);
    }

    // TLS (#30, owner decision 18): with --cert and --key the port serves HTTPS only; plain
    // HTTP needs --http-port.
    let tls = match (&tls_cert, &tls_key) {
        (Some(c), Some(k)) => match s3_server::tls::ReloadingCert::open(c, k) {
            Ok(rc) => {
                rc.watch(std::time::Duration::from_secs(60));
                Some(s3_server::tls::server_config(rc))
            }
            Err(e) => {
                eprintln!("cannot load the TLS certificate: {}", e);
                std::process::exit(2);
            }
        },
        (None, None) => None,
        _ => {
            eprintln!("--cert and --key go together");
            std::process::exit(2);
        }
    };
    let https = tls.is_some();
    let handle = start_s3_reactor_with(addr, Arc::clone(&state), tls).await?;
    let plain = match (https, http_port) {
        (true, Some(p)) => Some(start_s3_reactor_with(format!("{}:{}", listen, p).parse()?, Arc::clone(&state), None).await?),
        _ => None,
    };
    println!("S3 Storage Server listening on {}://{}", if https { "https" } else { "http" }, addr);

    wait_for_stop_signal().await;
    eprintln!("stop signal received: finishing requests in flight");
    // Readiness goes false first, so a load balancer stops sending new requests.
    state_for_stop.set_not_ready(Some("shutting down"));
    if let Some(p) = plain {
        let _ = p.shutdown_gracefully(std::time::Duration::from_secs(30)).await;
    }
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
