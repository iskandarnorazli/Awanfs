//! #130 (owner decision, 2026-11-13): `--allow-anonymous` together with users in `--config` is
//! refused at startup.
//!
//! `user_may` checks policies only for `Principal::User`; every other principal is allowed. With
//! `--allow-anonymous` alone that is what the flag promises, and the startup message says so.
//! With restricted users in a config it is a trap: an unsigned request would get more access
//! than any user the operator restricted. The server exits 2 and says which flag to drop. The
//! policy model for public access is #122 (decision 39), not this guard.

use std::io::Read;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CONFIG: &str = r#"
[[users]]
name = "scoped"
access_key = "AKSCOPED"
secret_key = "scoped-secret"
policy = '''{"Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::team-a/*"}]}'''
"#;

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_anon_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_config(dir: &Path) -> PathBuf {
    let p = dir.join("awanfs.toml");
    std::fs::write(&p, CONFIG).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    p
}

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Runs the binary with `extra` and waits, bounded, for it to exit on its own.
/// `(exit code, stderr)`, or `None` when it was still running at the deadline.
fn run_until_exit(dir: &Path, extra: &[&str], env: &[(&str, &str)]) -> Option<(i32, String)> {
    let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_s3-server"));
    cmd.args(["--port", &port.to_string(), "--data-dir", dir.to_str().unwrap()])
        .args(extra)
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    for (k, v) in env {
        cmd.env(k, v);
    }
    // A server that starts stays up forever, so poll and kill instead of waiting.
    let mut child = Server(cmd.spawn().expect("spawn s3-server"));
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        match child.0.try_wait().expect("try_wait") {
            Some(st) => break Some(st),
            None if Instant::now() >= deadline => break None,
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    };
    // A server that is still running holds its stderr open, so reading it to the end would wait
    // for an exit that only Drop brings: the first version of this helper deadlocked here and
    // both Test jobs hit their 30-minute timeout. Kill it first, then read what it wrote.
    if status.is_none() {
        let _ = child.0.kill();
        let _ = child.0.wait();
    }
    let mut err = String::new();
    if let Some(mut pipe) = child.0.stderr.take() {
        let _ = pipe.read_to_string(&mut err);
    }
    status.map(|st| (st.code().unwrap_or(-1), err))
}

#[test]
fn users_in_the_config_with_allow_anonymous_is_refused() {
    let dir = temp_dir("both");
    let cfg = write_config(&temp_dir("both_cfg"));
    let cfg = cfg.to_str().unwrap().to_string();

    let (code, err) = run_until_exit(&dir, &["--config", &cfg, "--allow-anonymous"], &[])
        .expect("the server must exit, not serve");
    assert_eq!(code, 2, "exit code; stderr was: {}", err);
    assert!(err.contains("--allow-anonymous"), "the message names the flag: {}", err);
    assert!(err.contains("--config"), "and the other one: {}", err);

    // The same through the environment, which is how a container sets it.
    let (code, err) = run_until_exit(&dir, &[], &[("AWANFS_CONFIG", &cfg), ("AWANFS_ALLOW_ANONYMOUS", "1")])
        .expect("the server must exit, not serve");
    assert_eq!(code, 2, "exit code from the environment; stderr was: {}", err);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn either_one_on_its_own_still_starts() {
    let dir = temp_dir("alone");
    let cfg = write_config(&temp_dir("alone_cfg"));
    let cfg = cfg.to_str().unwrap().to_string();

    assert!(
        run_until_exit(&dir, &["--config", &cfg], &[]).is_none(),
        "users in --config, no --allow-anonymous: the server serves"
    );
    assert!(
        run_until_exit(&dir, &["--allow-anonymous"], &[]).is_none(),
        "--allow-anonymous with no users: the server serves, as the flag says"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_config_with_no_users_is_not_the_combination() {
    let dir = temp_dir("buckets");
    let cfg_dir = temp_dir("buckets_cfg");
    let p = cfg_dir.join("awanfs.toml");
    std::fs::write(&p, "[[buckets]]\nname = \"logs\"\ncompression = \"zstd\"\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    assert!(
        run_until_exit(&dir, &["--config", p.to_str().unwrap(), "--allow-anonymous"], &[]).is_none(),
        "a config that only sets bucket compression restricts nobody, so there is nothing to bypass"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
