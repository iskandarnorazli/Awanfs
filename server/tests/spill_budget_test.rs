//! #206: The redb spill cache comes out of `--memory-budget`'s cache half.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_spill_budget_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start(dir: &Path, extra: &[&str]) -> (Server, String, String) {
    let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_s3-server"));
    cmd.args(["--port", &port.to_string(), "--data-dir", dir.to_str().unwrap(), "--allow-anonymous"])
        .args(extra)
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut child = cmd.spawn().expect("spawn s3-server");
    let mut out = child.stdout.take().expect("piped stdout");
    let url = format!("http://127.0.0.1:{}", port);
    let server = Server(child);

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut buf = [0u8; 4096];
        let mut seen = String::new();
        while let Ok(n) = out.read(&mut buf) {
            if n == 0 {
                break;
            }
            seen.push_str(&String::from_utf8_lossy(&buf[..n]));
            let _ = tx.send(seen.clone());
        }
    });

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut text = String::new();
    while Instant::now() < deadline {
        while let Ok(t) = rx.try_recv() {
            text = t;
        }
        if http_request("GET", &format!("{}/_awanfs/health/live", url), &[]).is_some() {
            while let Ok(t) = rx.try_recv() {
                text = t;
            }
            return (server, url, text);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("s3-server did not answer on {}. stdout so far:\n{}", url, text);
}

fn http_request(method: &str, url: &str, body: &[u8]) -> Option<String> {
    let rest = url.strip_prefix("http://")?;
    let (host, path) = rest.split_once('/').map(|(h, p)| (h, format!("/{}", p))).unwrap_or((rest, "/".into()));
    let mut s = TcpStream::connect(host).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(10))).ok()?;
    write!(
        s,
        "{} {} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        method,
        path,
        host,
        body.len()
    )
    .ok()?;
    if !body.is_empty() {
        s.write_all(body).ok()?;
    }
    let mut resp = Vec::new();
    s.read_to_end(&mut resp).ok()?;
    let text = String::from_utf8_lossy(&resp).to_string();
    if text.starts_with("HTTP/1.1 2") {
        Some(text)
    } else {
        None
    }
}

fn gauge(metrics: &str, name: &str) -> u64 {
    metrics
        .lines()
        .find(|l| l.starts_with(&format!("{} ", name)))
        .unwrap_or_else(|| panic!("no {} in:\n{}", name, metrics))
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap()
}

#[test]
fn spill_cache_is_bounded_by_memory_budget_and_deducted_from_payload_cache() {
    let dir = temp_dir("32mib");
    let (_s, url, _startup) = start(&dir, &["--memory-budget", "32MiB"]);

    let m = http_request("GET", &format!("{}/_awanfs/metrics", url), &[]).expect("GET /_awanfs/metrics");
    let cache_half = gauge(&m, "awanfs_memory_budget_cache_bytes");
    assert_eq!(cache_half, 16 << 20);

    let spill = gauge(&m, "awanfs_memory_budget_spill_bytes");
    assert!(spill <= 4 << 20, "expected spill <= 4 MiB, got {}", spill);
    assert_eq!(spill, 4 << 20, "expected spill == 4 MiB for 32 MiB budget, got {}", spill);

    let payload_bound = cache_half - spill;
    assert_eq!(payload_bound, 12 << 20);

    // Put and read 13 x 1 MiB objects (13 MiB total > 12 MiB payload_bound, < 16 MiB cache_half).
    http_request("PUT", &format!("{}/spill-bkt", url), &[]).expect("create bucket");
    let one_mib = vec![b'a'; 1 << 20];
    for i in 0..13 {
        let obj_url = format!("{}/spill-bkt/obj-{:02}", url, i);
        http_request("PUT", &obj_url, &one_mib).expect("PUT object");
        http_request("GET", &obj_url, &[]).expect("GET object");
    }

    let m2 = http_request("GET", &format!("{}/_awanfs/metrics", url), &[]).expect("GET /_awanfs/metrics");
    let cached_bytes = gauge(&m2, "awanfs_payload_cache_bytes");
    assert!(
        cached_bytes <= payload_bound,
        "payload cache {} exceeded bound {} (cache_half {} - spill {})",
        cached_bytes,
        payload_bound,
        cache_half,
        spill
    );

    let _ = std::fs::remove_dir_all(&dir);
}
