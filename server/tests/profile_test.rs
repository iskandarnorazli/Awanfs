//! #358: `--profile production` refuses to start an unsafe server and starts a safe one, and the
//! health endpoint says which profile is active. Runs the binary.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const SECRET: &str = "Zq7v1Xb9Lw2Rk8Ty4Hn6Pm3Jc5Gd0Fs2Ae7Ui9Oo";

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls").join(name)
}

fn server(args: &[&str]) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_awanfs"));
    c.args(args)
        .env_remove("AWANFS_ALLOW_ANONYMOUS")
        .env_remove("AWANFS_PROFILE")
        .env_remove("AWANFS_TLS_CERT")
        .env_remove("AWANFS_TLS_KEY")
        .env_remove("AWANFS_CONFIG")
        .env_remove("AWANFS_DURABILITY")
        .env("AWANFS_ACCESS_KEY", "profile-test-access")
        .env("AWANFS_SECRET_KEY", SECRET);
    c
}

/// Starts the server and returns it with the address it printed; stderr goes to a file.
fn start(args: &[&str], name: &str) -> (Child, String) {
    let log = std::env::temp_dir().join(format!("awanfs_profile_{}_{}.log", name, std::process::id()));
    let mut child = server(args)
        .stdout(Stdio::piped())
        .stderr(std::fs::File::create(&log).unwrap())
        .spawn()
        .unwrap();
    let mut out = BufReader::new(child.stdout.take().unwrap());
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut line = String::new();
    while Instant::now() < deadline {
        line.clear();
        if out.read_line(&mut line).unwrap() == 0 {
            break;
        }
        if let Some(at) = line.split("listening on ").nth(1) {
            let addr = at.split_whitespace().next().unwrap_or("").to_string();
            // Keep reading stdout so the server never blocks on a full pipe.
            std::thread::spawn(move || {
                let mut sink = String::new();
                while out.read_line(&mut sink).map(|n| n > 0).unwrap_or(false) {
                    sink.clear();
                }
            });
            return (child, addr);
        }
    }
    let _ = child.kill();
    panic!("the server did not start: {}", std::fs::read_to_string(&log).unwrap_or_default());
}

/// Runs the server to its exit, or kills it after 60 s and fails: a refused start must exit.
fn refused(args: &[&str]) -> (Option<i32>, String) {
    let log = std::env::temp_dir().join(format!("awanfs_profile_refused_{}_{}.log", std::process::id(), args.len()));
    let mut child = server(args).stdout(Stdio::null()).stderr(std::fs::File::create(&log).unwrap()).spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return (status.code(), std::fs::read_to_string(&log).unwrap_or_default());
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("the server started instead of refusing: {:?}", args);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn production_refuses_anonymous_access_and_names_why() {
    let dir = std::env::temp_dir().join(format!("awanfs_profile_anon_{}", std::process::id()));
    let (code, err) = refused(&["--port", "0", "--profile", "production", "--allow-anonymous", "--data-dir", dir.to_str().unwrap()]);
    assert_eq!(code, Some(2));
    assert!(err.contains("anonymous access is on"), "{}", err);
    assert!(err.contains("TLS is off"), "every reason is listed: {}", err);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn an_unknown_profile_stops_the_binary() {
    let (code, err) = refused(&["--port", "0", "--profile", "prod"]);
    assert_eq!(code, Some(2));
    assert!(err.contains("--profile"), "{}", err);
}

#[tokio::test]
async fn a_safe_production_server_starts_and_says_so_and_default_says_default() {
    let dir = std::env::temp_dir().join(format!("awanfs_profile_ok_{}", std::process::id()));
    let (cert, key) = (fixture("cert.pem"), fixture("key.pem"));
    let (mut child, addr) = start(
        &[
            "--port", "0", "--listen", "127.0.0.1", "--profile", "production",
            "--cert", cert.to_str().unwrap(), "--key", key.to_str().unwrap(),
            "--data-dir", dir.to_str().unwrap(),
        ],
        "ok",
    );
    assert!(addr.starts_with("https://"), "{}", addr);
    let port = addr.rsplit(':').next().unwrap();
    let pem = std::fs::read(&cert).unwrap();
    let c = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(&pem).unwrap())
        .build()
        .unwrap();
    let res = c.get(format!("https://localhost:{}/_awanfs/health/live", port)).send().await.unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    assert_eq!(res.headers().get("x-awanfs-profile").and_then(|v| v.to_str().ok()), Some("production"));
    let metrics = c.get(format!("https://localhost:{}/_awanfs/metrics", port)).send().await.unwrap().text().await.unwrap();
    assert!(metrics.contains("awanfs_profile{profile=\"production\"} 1"), "{}", metrics);
    let _ = child.kill();
    let _ = child.wait();

    let (mut child, addr) = start(&["--port", "0", "--allow-anonymous"], "default");
    let res = reqwest::get(format!("{}/_awanfs/health/live", addr)).await.unwrap();
    assert_eq!(res.headers().get("x-awanfs-profile").and_then(|v| v.to_str().ok()), Some("default"));
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&dir);
}
