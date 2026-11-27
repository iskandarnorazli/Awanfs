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

struct Server {
    child: Child,
    dir: PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn command(name: &str, extra: &[&str], env: &[(&str, &str)]) -> (Command, PathBuf) {
    let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let dir = temp_dir(name);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_awanfs"));
    cmd.args(["--port", &port.to_string(), "--data-dir", dir.to_str().unwrap(), "--allow-anonymous"])
        .args(extra)
        .env("AWANFS_LOG", "off")
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    for (k, v) in env {
        cmd.env(k, v);
    }
    (cmd, dir)
}

fn raw_command(name: &str, args_list: &[&str]) -> (Command, PathBuf) {
    let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let dir = temp_dir(name);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_awanfs"));
    cmd.args(["--port", &port.to_string(), "--data-dir", dir.to_str().unwrap()])
        .args(args_list)
        .env("AWANFS_LOG", "off")
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    (cmd, dir)
}

fn startup_lines(spec: (Command, PathBuf), prefix: &str) -> Vec<String> {
    let (mut cmd, dir) = spec;
    let mut child = Server {
        child: cmd.spawn().expect("spawn awanfs"),
        dir,
    };
    let out = child.child.stdout.take().unwrap();
    let prefix = prefix.to_string();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut lines = Vec::new();
        for line in BufReader::new(out).lines() {
            let Ok(line) = line else { break };
            let done = line.starts_with(&prefix);
            lines.push(line);
            if done {
                let _ = tx.send(lines);
                return;
            }
        }
        let _ = tx.send(lines);
    });
    let found = rx.recv_timeout(std::time::Duration::from_secs(30)).unwrap_or_default();
    drop(child);
    found
}

/// The `Lifecycle sweep every ...` line the server prints, or `None` if its stdout ended first or no
/// such line came within 30 s (a running server never closes its stdout, so the read is on a
/// thread and the server is killed when this returns).
fn sweep_line(spec: (Command, PathBuf)) -> Option<String> {
    startup_lines(spec, "Lifecycle sweep every")
        .into_iter()
        .find(|l| l.starts_with("Lifecycle sweep every"))
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

#[test]
fn lifecycle_day_seconds_does_not_consume_following_flag() {
    assert_eq!(
        sweep_line(raw_command(
            "before-allow-anon",
            &["--lifecycle-day-seconds", "2400", "--allow-anonymous"],
        ))
        .as_deref(),
        Some("Lifecycle sweep every 100s")
    );
}

#[test]
fn lifecycle_day_seconds_does_not_clear_prior_memory_budget() {
    let lines = startup_lines(
        raw_command(
            "preserve-budget",
            &["--memory-budget", "256MiB", "--lifecycle-day-seconds", "2400", "--allow-anonymous"],
        ),
        "Lifecycle sweep every",
    );
    assert!(
        lines.iter().any(|l| l.starts_with("Memory budget 256 MiB (flag)")),
        "expected Memory budget 256 MiB (flag) in startup lines: {:?}",
        lines
    );
    assert!(
        lines.iter().any(|l| l == "Lifecycle sweep every 100s"),
        "expected Lifecycle sweep every 100s in startup lines: {:?}",
        lines
    );
}
