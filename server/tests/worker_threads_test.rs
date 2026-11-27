//! #456: the server starts as many tokio workers as there are cores the process may run on,
//! and `--worker-threads` (or `AWANFS_WORKER_THREADS`) overrides it. The binary prints
//! `Worker threads: <n> (<source>)` at startup; these tests start it and read that line.

use std::io::{BufRead, BufReader, Read};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_workers_{}_{}_{}", name, std::process::id(), nanos));
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
        .env_remove("AWANFS_WORKER_THREADS")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd
}

/// The `Worker threads: ...` line the server prints, or `None` if its stdout ended first or no
/// such line came within 30 s (a running server never closes its stdout, so the read is on a
/// thread and the server is killed when this returns).
fn worker_line(mut cmd: Command) -> Option<String> {
    let mut child = Server(cmd.spawn().expect("spawn s3-server"));
    let out = child.0.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(out).lines() {
            let Ok(line) = line else { break };
            if line.starts_with("Worker threads:") {
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
fn the_flag_and_the_environment_set_the_count() {
    assert_eq!(
        worker_line(command("flag", &["--worker-threads", "3"], &[])).as_deref(),
        Some("Worker threads: 3 (--worker-threads)")
    );
    assert_eq!(
        worker_line(command("env", &[], &[("AWANFS_WORKER_THREADS", "5")])).as_deref(),
        Some("Worker threads: 5 (AWANFS_WORKER_THREADS)")
    );
    assert_eq!(
        worker_line(command("both", &["--worker-threads", "2"], &[("AWANFS_WORKER_THREADS", "5")])).as_deref(),
        Some("Worker threads: 2 (--worker-threads)")
    );
}

#[test]
fn an_invalid_count_stops_the_server_with_its_source() {
    for (extra, env, named) in [
        (&["--worker-threads", "0"][..], &[][..], "--worker-threads"),
        (&["--worker-threads", "many"][..], &[][..], "--worker-threads"),
        (&[][..], &[("AWANFS_WORKER_THREADS", "-3")][..], "AWANFS_WORKER_THREADS"),
    ] {
        let mut child = Server(command("bad", extra, env).spawn().expect("spawn s3-server"));
        let status = child.0.wait().expect("wait");
        let mut err = String::new();
        child.0.stderr.take().unwrap().read_to_string(&mut err).unwrap();
        assert_eq!(status.code(), Some(2), "{:?} {:?}: {}", extra, env, err);
        assert!(err.contains(&format!("{} must be a whole number", named)), "{}", err);
    }
}

/// Pins this test's thread (Linux) or process (Windows) to the first two CPUs; the server it
/// starts inherits that. `false` when the machine has fewer than two.
#[cfg(target_os = "linux")]
fn pin_to_two_cores() -> bool {
    // SAFETY: the set is a zeroed local, filled with CPU_SET, and applied to this thread only.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) != 0 || libc::CPU_COUNT(&set) < 2 {
            return false;
        }
        let mut two: libc::cpu_set_t = std::mem::zeroed();
        let mut picked = 0;
        for cpu in 0..libc::CPU_SETSIZE as usize {
            if picked < 2 && libc::CPU_ISSET(cpu, &set) {
                libc::CPU_SET(cpu, &mut two);
                picked += 1;
            }
        }
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &two) == 0
    }
}

#[cfg(windows)]
fn pin_to_two_cores() -> bool {
    #[link(name = "kernel32")]
    extern "system" {
        fn GetCurrentProcess() -> isize;
        fn GetProcessAffinityMask(process: isize, process_mask: *mut usize, system_mask: *mut usize) -> i32;
        fn SetProcessAffinityMask(process: isize, mask: usize) -> i32;
    }
    let (mut mask, mut system) = (0usize, 0usize);
    // SAFETY: a pseudo-handle for this process and pointers to live locals.
    unsafe {
        if GetProcessAffinityMask(GetCurrentProcess(), &mut mask, &mut system) == 0 || mask.count_ones() < 2 {
            return false;
        }
        let low = mask & mask.wrapping_neg();
        let rest = mask & !low;
        let two = low | (rest & rest.wrapping_neg());
        SetProcessAffinityMask(GetCurrentProcess(), two) != 0
    }
}

#[cfg(any(target_os = "linux", windows))]
#[test]
fn a_server_started_on_two_cores_runs_two_workers() {
    if !pin_to_two_cores() {
        eprintln!("skipped: fewer than two CPUs to pin to");
        return;
    }
    let want = if cfg!(windows) {
        "Worker threads: 2 (process affinity)"
    } else {
        // Linux: available_parallelism already follows the affinity (#456).
        "Worker threads: 2 (available cores)"
    };
    assert_eq!(worker_line(command("pinned", &[], &[])).as_deref(), Some(want));
}
