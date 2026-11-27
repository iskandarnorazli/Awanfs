//! #486 (owner decision 2026-11-27): the server sweeps for expired objects every 1/24 of a
//! lifecycle day (`--lifecycle-day-seconds`), between 500 ms and 1 hour: hourly by default. It
//! printed nothing and swept every 60 s before; one pass over 1M objects cost 2.8 s of CPU
//! (`measurements/2026-11-27-lifecycle-sweep-cost.md`). These tests start the binary and read
//! the period it prints.

use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_sweep_{}_{}_{}", name, std::process::id(), nanos));
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

fn command(name: &str, extra: &[&str], env: &[(&str, &str)]) -> Command {
    let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let dir = temp_dir(name);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_s3-server"));
    cmd.args(["--port", &port.to_string(), "--data-dir", dir.to_str().unwrap(), "--allow-anonymous"])
        .args(extra)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd
}

/// The `Lifecycle sweep every ...` line the server prints, or `None` if its stdout ended first or no
/// such line came within 30 s (a running server never closes its stdout, so the read is on a
/// thread and the server is killed when this returns).
fn sweep_line(mut cmd: Command) -> Option<String> {
    let mut child = Server(cmd.spawn().expect("spawn s3-server"));
    let out = child.0.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(out).lines() {
            let Ok(line) = line else { break };
            if line.starts_with("Lifecycle sweep every") {
                let _ = tx.send(line);
                return;
            }
        }
    });
    let found = rx.recv_timeout(std::time::Duration::from_secs(30)).ok();
    drop(child);
    found
}

#[test]
fn the_server_sweeps_hourly_by_default() {
    assert_eq!(sweep_line(command("default", &[], &[])).as_deref(), Some("Lifecycle sweep every 3600s"));
}

#[test]
fn the_period_follows_the_lifecycle_day() {
    assert_eq!(
        sweep_line(command("day-2400", &["--lifecycle-day-seconds", "2400"], &[])).as_deref(),
        Some("Lifecycle sweep every 100s")
    );
    // The suite's 2-second day keeps its short period.
    assert_eq!(
        sweep_line(command("day-2", &["--lifecycle-day-seconds", "2"], &[])).as_deref(),
        Some("Lifecycle sweep every 500ms")
    );
}
