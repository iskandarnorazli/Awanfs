//! One setting for how much memory the server may hold: `--memory-budget` (#127, decision 36).
//!
//! The budget is split in half: one half bounds the payload cache (`cache.rs`), the other is
//! what in-flight request bodies may hold (#128 enforces that half). Before this there was a
//! fixed 1 GiB cache cap and nothing at all on bodies.
//!
//! Without the flag the budget comes from the container's limit, then the machine's RAM:
//! a quarter of it, never below 128 MiB and never above 2 GiB, so the default cache stays at
//! the 1 GiB it was. Detection reads files, so it works on Linux and in containers; elsewhere
//! the budget is the 2 GiB fallback until someone passes the flag. `GET /_awanfs/metrics` reports
//! which it was.

use std::path::Path;

/// A quarter of the machine or the container.
pub const BUDGET_PERCENT: u64 = 25;
/// The default budget never goes below this, unless the detected limit itself is smaller.
pub const MIN_DEFAULT_BUDGET: u64 = 128 << 20;
/// The default budget never goes above this, so the default cache stays at 1 GiB.
pub const MAX_DEFAULT_BUDGET: u64 = 2 << 30;
/// A cgroup limit at or above this means "no limit" (cgroup v1 writes i64::MAX rounded down).
const NO_LIMIT: u64 = 1 << 60;

/// Where the budget came from. Reported by `GET /_awanfs/metrics` so an operator can see whether
/// detection worked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Flag,
    Env,
    CgroupV2,
    CgroupV1,
    MemTotal,
    /// Nothing was detected: the 2 GiB default, which keeps the cache at its old 1 GiB.
    Fallback,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Source::Flag => "flag",
            Source::Env => "env",
            Source::CgroupV2 => "cgroup2",
            Source::CgroupV1 => "cgroup1",
            Source::MemTotal => "memtotal",
            Source::Fallback => "fallback",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    /// Everything the server may hold, bytes.
    pub total: u64,
    /// The payload cache's half.
    pub cache: u64,
    /// The half in-flight request bodies may hold (#128).
    pub bodies: u64,
    pub source: Source,
}

impl Budget {
    fn split(total: u64, source: Source) -> Self {
        let cache = total / 2;
        Budget { total, cache, bodies: total - cache, source }
    }
}

/// `"256MiB"`, `"2G"`, `"1073741824"`. Decimal suffixes are the powers of two they are usually
/// meant as, the way `docker --memory` reads them. Zero and a budget the split cannot halve
/// into anything useful are refused by `resolve`.
pub fn parse_size(s: &str) -> Option<u64> {
    let t = s.trim();
    if t.is_empty() {
        return None;
    }
    let digits_end = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    if digits_end == 0 {
        return None;
    }
    let n: u64 = t[..digits_end].parse().ok()?;
    let shift = match t[digits_end..].trim().to_ascii_lowercase().as_str() {
        "" | "b" => 0,
        "k" | "kb" | "kib" => 10,
        "m" | "mb" | "mib" => 20,
        "g" | "gb" | "gib" => 30,
        "t" | "tb" | "tib" => 40,
        _ => return None,
    };
    n.checked_mul(1u64 << shift)
}

/// The first limit of the three files that names one. `cgroup2` is `memory.max`, `cgroup1` is
/// `memory.limit_in_bytes`, `meminfo` is `/proc/meminfo`. Paths are arguments so the tests can
/// use fixtures instead of the machine they run on.
pub fn detect_limit(cgroup2: &Path, cgroup1: &Path, meminfo: &Path) -> Option<(u64, Source)> {
    if let Some(n) = read_number(cgroup2) {
        if n > 0 && n < NO_LIMIT {
            return Some((n, Source::CgroupV2));
        }
    }
    if let Some(n) = read_number(cgroup1) {
        if n > 0 && n < NO_LIMIT {
            return Some((n, Source::CgroupV1));
        }
    }
    read_mem_total(meminfo).map(|n| (n, Source::MemTotal))
}

/// The limit of the machine this is running on.
pub fn detect() -> Option<(u64, Source)> {
    detect_limit(
        Path::new("/sys/fs/cgroup/memory.max"),
        Path::new("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
        Path::new("/proc/meminfo"),
    )
}

fn read_number(p: &Path) -> Option<u64> {
    std::fs::read_to_string(p).ok()?.trim().parse().ok()
}

/// `MemTotal:       16318488 kB`
fn read_mem_total(p: &Path) -> Option<u64> {
    let text = std::fs::read_to_string(p).ok()?;
    let line = text.lines().find(|l| l.starts_with("MemTotal:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    kb.checked_mul(1024)
}

/// A quarter of the detected limit, held between 128 MiB and 2 GiB. A limit smaller than
/// 256 MiB gets half of itself rather than a floor it cannot afford.
pub fn default_budget(limit: Option<(u64, Source)>) -> Budget {
    let Some((limit, source)) = limit else {
        return Budget::split(MAX_DEFAULT_BUDGET, Source::Fallback);
    };
    let floor = MIN_DEFAULT_BUDGET.min(limit / 2);
    let total = (limit * BUDGET_PERCENT / 100).clamp(floor, MAX_DEFAULT_BUDGET);
    Budget::split(total.max(1), source)
}

/// The budget this run uses: `--memory-budget`, then `AWANFS_MEMORY_BUDGET`, then detection.
/// `Err` carries what to print and exit on.
pub fn resolve(flag: Option<&str>, env: Option<&str>, limit: Option<(u64, Source)>) -> Result<Budget, String> {
    for (given, source) in [(flag, Source::Flag), (env, Source::Env)] {
        let Some(text) = given.map(str::trim).filter(|t| !t.is_empty()) else { continue };
        let n = parse_size(text).ok_or_else(|| {
            format!("--memory-budget {:?}: give bytes or a size like 256MiB, 2G", text)
        })?;
        if n < 16 << 20 {
            return Err(format!("--memory-budget {:?} is {} bytes: the smallest budget is 16MiB", text, n));
        }
        return Ok(Budget::split(n, source));
    }
    Ok(default_budget(limit))
}

/// The budget this process uses. `main` sets it from the flag before the server is built;
/// everything else reads it. One process, one budget, so the cache and the body limit cannot
/// disagree about what they are halves of.
static CONFIGURED: std::sync::OnceLock<Budget> = std::sync::OnceLock::new();

/// Sets the budget for this process. `Err` means it was already set or read, and carries the
/// budget in force. `main` calls this before building the server.
pub fn configure(b: Budget) -> Result<(), Budget> {
    CONFIGURED.set(b).map_err(|_| effective())
}

/// The budget in force: what `configure` was given, or `AWANFS_MEMORY_BUDGET` and detection.
pub fn effective() -> Budget {
    *CONFIGURED.get_or_init(|| {
        let env = std::env::var("AWANFS_MEMORY_BUDGET").ok();
        resolve(None, env.as_deref(), detect()).unwrap_or_else(|_| default_budget(detect()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn fixture_dir(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let d = std::env::temp_dir().join(format!("awanfs_budget_{}_{}_{}", name, std::process::id(), nanos));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn write(dir: &Path, name: &str, body: &str) -> PathBuf {
        let p = dir.join(name);
        std::fs::write(&p, body).unwrap();
        p
    }

    #[test]
    fn sizes_parse_with_and_without_a_suffix() {
        assert_eq!(parse_size("1073741824"), Some(1 << 30));
        assert_eq!(parse_size("256MiB"), Some(256 << 20));
        assert_eq!(parse_size(" 256 mb "), Some(256 << 20));
        assert_eq!(parse_size("2G"), Some(2 << 30));
        assert_eq!(parse_size("4k"), Some(4096));
        assert_eq!(parse_size("1T"), Some(1 << 40));
        for bad in ["", "   ", "MiB", "12Q", "-1", "1.5G", "0x10", "99999999999999999999"] {
            assert_eq!(parse_size(bad), None, "{:?}", bad);
        }
        assert_eq!(parse_size("0"), Some(0), "zero parses; resolve refuses it");
    }

    #[test]
    fn the_cgroup_limit_wins_over_the_machines_memory() {
        let d = fixture_dir("cg2");
        let c2 = write(&d, "memory.max", "536870912\n");
        let c1 = write(&d, "limit_in_bytes", "268435456\n");
        let mi = write(&d, "meminfo", "MemTotal:       16318488 kB\nMemFree: 1 kB\n");
        assert_eq!(detect_limit(&c2, &c1, &mi), Some((512 << 20, Source::CgroupV2)));
        // v1 is read when v2 is absent or says there is no limit.
        assert_eq!(detect_limit(&d.join("absent"), &c1, &mi), Some((256 << 20, Source::CgroupV1)));
        let unlimited = write(&d, "max", "max\n");
        assert_eq!(detect_limit(&unlimited, &d.join("absent"), &mi), Some((16318488 * 1024, Source::MemTotal)));
        let v1_unlimited = write(&d, "v1max", "9223372036854771712\n");
        assert_eq!(detect_limit(&d.join("absent"), &v1_unlimited, &mi), Some((16318488 * 1024, Source::MemTotal)));
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn nothing_detected_is_the_fallback_that_keeps_todays_cache() {
        let d = fixture_dir("none");
        let missing = d.join("absent");
        assert_eq!(detect_limit(&missing, &missing, &missing), None);
        let b = default_budget(None);
        assert_eq!((b.total, b.cache, b.source), (2 << 30, 1 << 30, Source::Fallback));
        assert_eq!(b.cache, 1024 * 1024 * 1024, "the cache cap before #127");
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn the_default_is_a_quarter_held_between_the_floor_and_the_cap() {
        // 16 GiB machine: a quarter is 4 GiB, held at the 2 GiB cap, so the cache stays at 1 GiB.
        let b = default_budget(Some((16 << 30, Source::MemTotal)));
        assert_eq!((b.total, b.cache, b.bodies), (2 << 30, 1 << 30, 1 << 30));
        // 2 GiB container: a quarter is 512 MiB, inside both bounds.
        let b = default_budget(Some((2 << 30, Source::CgroupV2)));
        assert_eq!((b.total, b.cache, b.source), (512 << 20, 256 << 20, Source::CgroupV2));
        // 256 MiB container: a quarter is 64 MiB, lifted to the 128 MiB floor.
        let b = default_budget(Some((256 << 20, Source::CgroupV2)));
        assert_eq!((b.total, b.cache), (128 << 20, 64 << 20));
        // 64 MiB container: the floor is more than it has, so it gets half of itself.
        let b = default_budget(Some((64 << 20, Source::CgroupV2)));
        assert_eq!((b.total, b.cache), (32 << 20, 16 << 20));
        // A budget is never zero, whatever the limit says.
        assert!(default_budget(Some((1, Source::CgroupV2))).total >= 1);
    }

    #[test]
    fn the_flag_wins_over_the_environment_and_both_over_detection() {
        let limit = Some((16 << 30, Source::MemTotal));
        let b = resolve(Some("256MiB"), Some("1G"), limit).unwrap();
        assert_eq!((b.total, b.cache, b.bodies, b.source), (256 << 20, 128 << 20, 128 << 20, Source::Flag));
        let b = resolve(None, Some("1G"), limit).unwrap();
        assert_eq!((b.total, b.source), (1 << 30, Source::Env));
        let b = resolve(None, Some("  "), limit).unwrap();
        assert_eq!(b.source, Source::MemTotal, "an empty setting is not a setting");
        assert_eq!(resolve(None, None, limit).unwrap().source, Source::MemTotal);
    }

    #[test]
    fn a_budget_that_cannot_work_is_refused_with_what_to_type() {
        let err = resolve(Some("plenty"), None, None).unwrap_err();
        assert!(err.contains("256MiB"), "{}", err);
        let err = resolve(Some("4MiB"), None, None).unwrap_err();
        assert!(err.contains("16MiB"), "{}", err);
        assert!(resolve(Some("0"), None, None).is_err());
        assert!(resolve(Some("16MiB"), None, None).is_ok(), "the smallest budget is allowed");
    }

    #[test]
    fn the_halves_always_add_up_to_the_budget() {
        for total in [16u64 << 20, 1 << 30, (3 << 30) + 1, u32::MAX as u64 + 7] {
            let b = Budget::split(total, Source::Flag);
            assert_eq!(b.cache + b.bodies, total, "total {}", total);
            assert!(b.bodies >= b.cache, "the odd byte goes to bodies, total {}", total);
        }
    }
}
