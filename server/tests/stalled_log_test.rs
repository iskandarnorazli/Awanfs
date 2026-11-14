//! #138 (owner decision, 2026-11-14): request handling never waits on log output.
//!
//! The server writes one JSON line per request to stderr. That write used to happen inline in
//! the request handler with eprintln!, so when nothing read stderr, the pipe filled and every
//! request blocked behind it: the server kept its port and answered nothing. It stopped after
//! 271 requests on a 64 KiB Linux pipe and 16 on a 4 KiB Windows one (#135, run 43450981256).
//!
//! Now lines go through a bounded queue to a background writer, and a full queue drops the line
//! and counts it in `awanfs_log_lines_dropped_total`.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// One request on its own connection; the status code, or None on a timeout or a refused
/// connection.
fn request(port: u16, method: &str, path: &str) -> Option<(u16, String)> {
    let mut s = TcpStream::connect(("127.0.0.1", port)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(10))).ok()?;
    s.set_write_timeout(Some(Duration::from_secs(10))).ok()?;
    write!(s, "{} {} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", method, path).ok()?;
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).ok()?;
    let text = String::from_utf8_lossy(&buf).to_string();
    let code = text.get(9..12)?.parse().ok()?;
    Some((code, text))
}

#[test]
fn a_server_whose_stderr_nobody_reads_keeps_answering() {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_stalledlog_{}_{}", std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    // stderr is a pipe this test never reads: what a stalled log collector looks like.
    let child = Command::new(env!("CARGO_BIN_EXE_s3-server"))
        .args(["--port", &port.to_string(), "--data-dir", dir.to_str().unwrap(), "--allow-anonymous"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn s3-server");
    let _server = Server(child);

    let deadline = Instant::now() + Duration::from_secs(30);
    while request(port, "GET", "/_awanfs/health/live").map(|r| r.0) != Some(200) {
        assert!(Instant::now() < deadline, "the server did not start");
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(request(port, "PUT", "/logbkt").map(|r| r.0), Some(200), "create the bucket");

    // Each answered request writes a line of about 200 bytes: 2,000 of them is several times
    // what a 64 KiB pipe holds, and over a hundred times a 4 KiB one.
    for i in 0..2000 {
        match request(port, "HEAD", "/logbkt") {
            Some((200, _)) => {}
            other => panic!("request {} of 2000 got {:?}: the server stopped answering behind its log", i, other.map(|r| r.0)),
        }
    }

    let (code, metrics) = request(port, "GET", "/_awanfs/metrics").expect("metrics");
    assert_eq!(code, 200);
    let dropped: u64 = metrics
        .lines()
        .find(|l| l.starts_with("awanfs_log_lines_dropped_total "))
        .unwrap_or_else(|| panic!("no awanfs_log_lines_dropped_total in:\n{}", metrics))
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    assert!(dropped > 0, "the lines that did not fit are counted, got {}", dropped);
    let _ = std::fs::remove_dir_all(&dir);
}
