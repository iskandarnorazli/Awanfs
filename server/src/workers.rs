//! How many tokio worker threads the server starts (#456).
//!
//! Tokio starts one worker per core that `std::thread::available_parallelism` reports. On Linux
//! that already follows the process's affinity (and cgroup CPU quota), so it is kept. On
//! Windows it counts every logical processor, so a server started on 1 of 16 cores still ran 16
//! workers, and P99 at PUT 4 KiB on 1 core was 33.85 ms instead of 10.59 ms
//! (`measurements/2026-11-27-group-commit-window.md`). There the count comes from the process
//! affinity mask.

/// Largest count `--worker-threads` accepts.
pub const MAX_WORKER_THREADS: usize = 1024;

/// The worker count and where it came from: `--worker-threads`, else `AWANFS_WORKER_THREADS`,
/// else the cores in the process affinity (Windows), else `available`. An empty environment
/// value is ignored; a value that is not a whole number from 1 to `MAX_WORKER_THREADS` is an
/// error naming where it came from.
pub fn pick_worker_threads(
    flag: Option<&str>,
    env: Option<&str>,
    affinity: Option<usize>,
    available: usize,
) -> Result<(usize, &'static str), String> {
    let parse = |v: &str, what: &str| -> Result<usize, String> {
        match v.trim().parse::<usize>() {
            Ok(n) if (1..=MAX_WORKER_THREADS).contains(&n) => Ok(n),
            _ => Err(format!("{} must be a whole number from 1 to {}, not {:?}", what, MAX_WORKER_THREADS, v)),
        }
    };
    if let Some(v) = flag {
        return parse(v, "--worker-threads").map(|n| (n, "--worker-threads"));
    }
    if let Some(v) = env.filter(|v| !v.trim().is_empty()) {
        return parse(v, "AWANFS_WORKER_THREADS").map(|n| (n, "AWANFS_WORKER_THREADS"));
    }
    if let Some(n) = affinity.filter(|&n| n > 0) {
        return Ok((n.min(MAX_WORKER_THREADS), "process affinity"));
    }
    Ok((available.clamp(1, MAX_WORKER_THREADS), "available cores"))
}

/// The value after `--worker-threads` in `args` (the first one); `Some("")` when it is last.
pub fn worker_threads_flag<I: IntoIterator<Item = String>>(args: I) -> Option<String> {
    let mut it = args.into_iter();
    while let Some(a) = it.next() {
        if a == "--worker-threads" {
            return Some(it.next().unwrap_or_default());
        }
    }
    None
}

/// Cores in the process affinity mask, where `available_parallelism` does not follow it.
#[cfg(windows)]
pub fn affinity_cores() -> Option<usize> {
    #[link(name = "kernel32")]
    extern "system" {
        fn GetCurrentProcess() -> isize;
        fn GetProcessAffinityMask(process: isize, process_mask: *mut usize, system_mask: *mut usize) -> i32;
    }
    let (mut process_mask, mut system_mask) = (0usize, 0usize);
    // SAFETY: GetCurrentProcess returns a pseudo-handle that needs no closing; both out
    // pointers are to live locals.
    let ok = unsafe { GetProcessAffinityMask(GetCurrentProcess(), &mut process_mask, &mut system_mask) };
    (ok != 0 && process_mask != 0).then(|| process_mask.count_ones() as usize)
}

/// Cores in the process affinity mask, where `available_parallelism` does not follow it.
#[cfg(not(windows))]
pub fn affinity_cores() -> Option<usize> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_flag_wins_then_the_environment_then_affinity_then_available() {
        assert_eq!(pick_worker_threads(Some("3"), Some("5"), Some(2), 16), Ok((3, "--worker-threads")));
        assert_eq!(pick_worker_threads(None, Some("5"), Some(2), 16), Ok((5, "AWANFS_WORKER_THREADS")));
        assert_eq!(pick_worker_threads(None, None, Some(2), 16), Ok((2, "process affinity")));
        assert_eq!(pick_worker_threads(None, Some("  "), Some(2), 16), Ok((2, "process affinity")));
        assert_eq!(pick_worker_threads(None, None, None, 16), Ok((16, "available cores")));
        assert_eq!(pick_worker_threads(None, None, Some(0), 4), Ok((4, "available cores")));
    }

    #[test]
    fn invalid_values_are_refused_with_their_source() {
        for bad in ["0", "-1", "two", "1.5", "", "1025"] {
            let e = pick_worker_threads(Some(bad), None, Some(2), 16).unwrap_err();
            assert!(e.starts_with("--worker-threads must be"), "{}", e);
        }
        let e = pick_worker_threads(None, Some("x"), None, 16).unwrap_err();
        assert!(e.starts_with("AWANFS_WORKER_THREADS must be"), "{}", e);
        assert_eq!(pick_worker_threads(Some(" 1024 "), None, None, 1), Ok((1024, "--worker-threads")));
    }

    #[test]
    fn the_flag_value_is_read_from_the_arguments() {
        let args = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(worker_threads_flag(args(&["--port", "9000", "--worker-threads", "2"])), Some("2".to_string()));
        assert_eq!(worker_threads_flag(args(&["--worker-threads"])), Some(String::new()));
        assert_eq!(worker_threads_flag(args(&["--port", "9000"])), None);
    }
}
