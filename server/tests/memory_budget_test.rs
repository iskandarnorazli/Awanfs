//! #127 (decision 36): one `--memory-budget` bounds the payload cache and the bytes in-flight
//! request bodies may hold. Gate: the flag reaches the cache and the metrics, the environment
//! variable does the same, a budget that cannot work is refused with a non-zero exit, and the
//! default on this machine comes from the container's limit or its RAM.

use std::io::Read;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_budget_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Kills the server when dropped, also when an assertion fails.
struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Starts the binary with `extra` arguments and `env`, and returns it with its base URL once it
/// answers, plus everything it wrote to stdout by then.
fn start(dir: &Path, extra: &[&str], env: &[(&str, &str)]) -> (Server, String, String) {
    let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_s3-server"));
    cmd.args(["--port", &port.to_string(), "--data-dir", dir.to_str().unwrap(), "--allow-anonymous"])
        .args(extra)
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    for (k, v) in env {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn().expect("spawn s3-server");
    let mut out = child.stdout.take().expect("piped stdout");
    let url = format!("http://127.0.0.1:{}", port);
    let server = Server(child);

    // Read the startup lines on a thread: the pipe must be drained or the server blocks on it.
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
        if ureq_get(&format!("{}/_awanfs/health/live", url)).is_some() {
            while let Ok(t) = rx.try_recv() {
                text = t;
            }
            return (server, url, text);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("s3-server did not answer on {}. stdout so far:\n{}", url, text);
}

/// A GET with the standard library, so this test needs no async runtime.
fn ureq_get(url: &str) -> Option<String> {
    use std::io::Write;
    let rest = url.strip_prefix("http://")?;
    let (host, path) = rest.split_once('/').map(|(h, p)| (h, format!("/{}", p))).unwrap_or((rest, "/".into()));
    let mut s = std::net::TcpStream::connect(host).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(10))).ok()?;
    write!(s, "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", path, host).ok()?;
    let mut body = Vec::new();
    s.read_to_end(&mut body).ok()?;
    let text = String::from_utf8_lossy(&body).to_string();
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
fn the_flag_sets_the_budget_the_cache_and_the_metrics() {
    let dir = temp_dir("flag");
    let (_s, url, startup) = start(&dir, &["--memory-budget", "256MiB"], &[]);
    assert!(
        startup.contains("Memory budget 256 MiB (flag): payload cache 128 MiB, in-flight request bodies 128 MiB"),
        "startup said:\n{}",
        startup
    );
    let m = ureq_get(&format!("{}/_awanfs/metrics", url)).expect("GET /_awanfs/metrics");
    assert_eq!(gauge(&m, "awanfs_memory_budget_bytes"), 256 << 20);
    assert_eq!(gauge(&m, "awanfs_memory_budget_cache_bytes"), 128 << 20);
    assert_eq!(gauge(&m, "awanfs_memory_budget_bodies_bytes"), 128 << 20);
    assert_eq!(
        gauge(&m, "awanfs_memory_budget_cache_bytes") + gauge(&m, "awanfs_memory_budget_bodies_bytes"),
        gauge(&m, "awanfs_memory_budget_bytes"),
        "the halves add up"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_environment_sets_it_and_the_flag_wins_over_the_environment() {
    let dir = temp_dir("env");
    let (_s, url, startup) = start(&dir, &[], &[("AWANFS_MEMORY_BUDGET", "512MiB")]);
    assert!(startup.contains("Memory budget 512 MiB (env)"), "startup said:\n{}", startup);
    let m = ureq_get(&format!("{}/_awanfs/metrics", url)).expect("GET /_awanfs/metrics");
    assert_eq!(gauge(&m, "awanfs_memory_budget_bytes"), 512 << 20);
    drop(_s);

    let dir2 = temp_dir("both");
    let (_s2, _url2, startup2) = start(&dir2, &["--memory-budget", "256MiB"], &[("AWANFS_MEMORY_BUDGET", "1G")]);
    assert!(startup2.contains("Memory budget 256 MiB (flag)"), "startup said:\n{}", startup2);
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&dir2);
}

#[test]
fn without_a_setting_the_budget_comes_from_this_machine_and_stays_inside_its_bounds() {
    let dir = temp_dir("default");
    let (_s, url, startup) = start(&dir, &[], &[]);
    assert!(startup.contains("Memory budget "), "startup said:\n{}", startup);
    let m = ureq_get(&format!("{}/_awanfs/metrics", url)).expect("GET /_awanfs/metrics");
    let total = gauge(&m, "awanfs_memory_budget_bytes");
    assert!(total >= 16 << 20, "a budget of {} bytes is unusable", total);
    assert!(total <= s3_server::memory::MAX_DEFAULT_BUDGET, "{} is over the default cap", total);
    // On a machine or container where nothing is detected the budget is the 2 GiB fallback,
    // whose cache half is the 1 GiB cap that #127 replaces.
    if startup.contains("(fallback)") {
        assert_eq!(gauge(&m, "awanfs_memory_budget_cache_bytes"), 1 << 30);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_budget_that_cannot_work_stops_the_server_with_a_message() {
    let dir = temp_dir("bad");
    for bad in ["plenty", "4MiB", "0"] {
        let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let mut child = Command::new(env!("CARGO_BIN_EXE_s3-server"))
            .args(["--port", &port.to_string(), "--data-dir", dir.to_str().unwrap(), "--allow-anonymous"])
            .args(["--memory-budget", bad])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn s3-server");
        // It must exit on its own. Waiting forever here would hang CI, so poll and kill.
        let deadline = Instant::now() + Duration::from_secs(20);
        let status = loop {
            match child.try_wait().expect("try_wait") {
                Some(st) => break Some(st),
                None if Instant::now() >= deadline => {
                    let _ = child.kill();
                    let _ = child.wait();
                    break None;
                }
                None => std::thread::sleep(Duration::from_millis(50)),
            }
        };
        let status = status.unwrap_or_else(|| panic!("--memory-budget {} kept the server running", bad));
        assert!(!status.success(), "--memory-budget {} exited 0", bad);
        let mut err = String::new();
        let _ = child.stderr.take().expect("piped stderr").read_to_string(&mut err);
        assert!(err.contains("--memory-budget"), "--memory-budget {} said: {:?}", bad, err);
    }
    let _ = std::fs::remove_dir_all(&dir);
}
