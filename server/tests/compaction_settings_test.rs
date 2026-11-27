//! #509 (follow-up from #494): pin `--compaction-rate` / `AWANFS_COMPACTION_RATE_MB` and
//! `--auto-compaction` / `AWANFS_AUTO_COMPACTION` directly on the `s3-server` binary.

use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_compaction_cfg_{}_{}_{}", name, std::process::id(), nanos));
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
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_s3-server"));
    cmd.args(["--port", &port.to_string(), "--data-dir", dir.to_str().unwrap(), "--allow-anonymous"])
        .args(extra)
        .env("AWANFS_LOG", "off")
        .env_remove("AWANFS_COMPACTION_RATE_MB")
        .env_remove("AWANFS_AUTO_COMPACTION")
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    for (k, v) in env {
        cmd.env(k, v);
    }
    (cmd, dir)
}

/// Returns the `Compaction: ...` startup line printed by `s3-server`, or `None` if stdout ended or timed out.
fn compaction_line(spec: (Command, PathBuf)) -> Option<String> {
    let (mut cmd, dir) = spec;
    let mut child = Server {
        child: cmd.spawn().expect("spawn s3-server"),
        dir,
    };
    let out = child.child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(out).lines() {
            let Ok(line) = line else { break };
            if line.starts_with("Compaction: ") {
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
fn compaction_defaults_are_auto_on_and_50_mb_per_sec() {
    assert_eq!(
        compaction_line(command("default", &[], &[])).as_deref(),
        Some("Compaction: auto=on rate=50 MB/s")
    );
}

#[test]
fn compaction_rate_flag_and_env_override_default() {
    assert_eq!(
        compaction_line(command("env-120", &[], &[("AWANFS_COMPACTION_RATE_MB", "120")])).as_deref(),
        Some("Compaction: auto=on rate=120 MB/s")
    );
    assert_eq!(
        compaction_line(command("flag-0", &["--compaction-rate", "0"], &[])).as_deref(),
        Some("Compaction: auto=on rate=0 MB/s")
    );
    assert_eq!(
        compaction_line(command(
            "flag-over-env",
            &["--compaction-rate", "25"],
            &[("AWANFS_COMPACTION_RATE_MB", "120")],
        ))
        .as_deref(),
        Some("Compaction: auto=on rate=25 MB/s")
    );
}

#[test]
fn auto_compaction_flag_and_env_toggle_background_task() {
    assert_eq!(
        compaction_line(command("env-off", &[], &[("AWANFS_AUTO_COMPACTION", "off")])).as_deref(),
        Some("Compaction: auto=off rate=50 MB/s")
    );
    assert_eq!(
        compaction_line(command("flag-off", &["--auto-compaction", "off"], &[])).as_deref(),
        Some("Compaction: auto=off rate=50 MB/s")
    );
    assert_eq!(
        compaction_line(command(
            "flag-over-env-on",
            &["--auto-compaction", "on"],
            &[("AWANFS_AUTO_COMPACTION", "off")],
        ))
        .as_deref(),
        Some("Compaction: auto=on rate=50 MB/s")
    );
}
