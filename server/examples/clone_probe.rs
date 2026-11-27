//! Compaction copy vs `FICLONERANGE` reflink measurement probe (#104).
//!
//! Throwaway measurement binary (no server code changes): populates only the 50% live
//! 4 KiB-aligned extents of a sparse 32 GiB source file on XFS (`-m reflink=1`), copies the
//! live extents once via read+write and once via `ioctl(FICLONERANGE)` per live extent across
//! three extent mixes matching `benchmarks/compaction_cost.py`, and reports median seconds,
//! bytes written, and copy/clone ratios.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mix {
    Small,
    Large,
    Mixed,
}

impl Mix {
    fn parse(s: &str) -> Result<Self, String> {
        match s.trim() {
            "small" => Ok(Mix::Small),
            "large" => Ok(Mix::Large),
            "mixed" => Ok(Mix::Mixed),
            other => Err(format!("unknown mix '{}'; expected small, large, or mixed", other)),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Mix::Small => "small",
            Mix::Large => "large",
            Mix::Mixed => "mixed",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Extent {
    src_offset: u64,
    dest_offset: u64,
    len: u64,
}

const ALIGN: u64 = 4096;
const SMALL_EXTENT: u64 = 4096;
const LARGE_EXTENT: u64 = 64 * 1024 * 1024;
const MIB: f64 = (1024 * 1024) as f64;
const GIB: f64 = (1024 * 1024 * 1024) as f64;

/// Builds sorted, 4 KiB-aligned live extents totaling 50% of `total_bytes` (aligned down to 4 KiB).
/// Dead intervals are left sparse in the source file; `dest_offset` packs live extents contiguously.
fn build_extents(mix: Mix, total_bytes: u64) -> Result<Vec<Extent>, String> {
    if total_bytes < 2 * ALIGN {
        return Err(format!("total_bytes ({}) must be at least {}", total_bytes, 2 * ALIGN));
    }
    let aligned_total = (total_bytes / ALIGN) * ALIGN;
    let mut extents = Vec::new();
    let mut dest_off = 0u64;

    match mix {
        Mix::Small => {
            let stride = 2 * SMALL_EXTENT;
            let count = aligned_total / stride;
            extents.reserve(count as usize);
            for i in 0..count {
                extents.push(Extent {
                    src_offset: i * stride,
                    dest_offset: dest_off,
                    len: SMALL_EXTENT,
                });
                dest_off += SMALL_EXTENT;
            }
        }
        Mix::Large => {
            let unit = LARGE_EXTENT.min((aligned_total / 2 / ALIGN).max(1) * ALIGN);
            let stride = 2 * unit;
            let count = aligned_total / stride;
            extents.reserve(count as usize);
            for i in 0..count {
                extents.push(Extent {
                    src_offset: i * stride,
                    dest_offset: dest_off,
                    len: unit,
                });
                dest_off += unit;
            }
        }
        Mix::Mixed => {
            // Mirrors `benchmarks/compaction_cost.py` (`balanced`: 80% large 64 MiB payload,
            // 20% small 4 KiB payload), interleaved across the file with equal dead gaps.
            let target_live = (aligned_total / 2 / ALIGN) * ALIGN;
            let large_budget = (target_live * 4 / 5 / LARGE_EXTENT) * LARGE_EXTENT;
            let large_count = large_budget / LARGE_EXTENT;
            let small_budget = target_live - large_count * LARGE_EXTENT;
            let small_count = small_budget / SMALL_EXTENT;

            if large_count == 0 {
                return build_extents(Mix::Small, total_bytes);
            }
            let small_per_large = small_count / large_count;
            let small_rem = small_count % large_count;
            extents.reserve((large_count + small_count) as usize);

            let mut src_off = 0u64;
            for i in 0..large_count {
                // One live 64 MiB extent followed by an equal 64 MiB dead hole.
                extents.push(Extent {
                    src_offset: src_off,
                    dest_offset: dest_off,
                    len: LARGE_EXTENT,
                });
                dest_off += LARGE_EXTENT;
                src_off += 2 * LARGE_EXTENT;

                let n_small = small_per_large + u64::from(i < small_rem);
                for _ in 0..n_small {
                    extents.push(Extent {
                        src_offset: src_off,
                        dest_offset: dest_off,
                        len: SMALL_EXTENT,
                    });
                    dest_off += SMALL_EXTENT;
                    src_off += 2 * SMALL_EXTENT;
                }
            }
        }
    }

    if extents.is_empty() {
        return Err(format!("mix {} produced zero extents for {} bytes", mix.as_str(), total_bytes));
    }
    Ok(extents)
}

fn median_f64(vals: &[f64]) -> f64 {
    let mut s = vals.to_vec();
    s.sort_by(|a, b| a.total_cmp(b));
    let n = s.len();
    if n % 2 == 1 {
        s[n / 2]
    } else {
        (s[n / 2 - 1] + s[n / 2]) / 2.0
    }
}

fn median_u64(vals: &[u64]) -> u64 {
    let mut s = vals.to_vec();
    s.sort_unstable();
    let n = s.len();
    if n % 2 == 1 {
        s[n / 2]
    } else {
        s[n / 2 - 1].saturating_add(s[n / 2]) / 2
    }
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_proc_io_write_bytes(text: &str) -> Option<u64> {
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("write_bytes:") {
            return rest.trim().parse::<u64>().ok();
        }
    }
    None
}

/// Parses Linux `/sys/block/<dev>/stat` field 7 (1-based; 0-based index 6) = sectors written.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_block_stat_sectors(text: &str) -> Option<u64> {
    let fields: Vec<&str> = text.split_whitespace().collect();
    fields.get(6)?.parse::<u64>().ok()
}

#[derive(Clone, Debug)]
struct RunSample {
    copy_secs: f64,
    clone_secs: f64,
    copy_proc_bytes: u64,
    clone_proc_bytes: u64,
    copy_dev_bytes: Option<u64>,
    clone_dev_bytes: Option<u64>,
}

#[derive(Clone, Debug)]
struct MixReport {
    mix: Mix,
    extent_count: usize,
    live_bytes: u64,
    source_allocated_bytes: u64,
    samples: Vec<RunSample>,
}

impl MixReport {
    fn copy_bytes_med(&self) -> u64 {
        let dev: Vec<u64> = self.samples.iter().filter_map(|s| s.copy_dev_bytes).collect();
        if dev.len() == self.samples.len() && dev.iter().any(|&v| v > 0) {
            median_u64(&dev)
        } else {
            let proc: Vec<u64> = self.samples.iter().map(|s| s.copy_proc_bytes).collect();
            median_u64(&proc)
        }
    }

    fn clone_bytes_med(&self) -> u64 {
        let dev: Vec<u64> = self.samples.iter().filter_map(|s| s.clone_dev_bytes).collect();
        let copy_dev_nonzero = self.samples.iter().filter_map(|s| s.copy_dev_bytes).any(|v| v > 0);
        if dev.len() == self.samples.len() && copy_dev_nonzero {
            median_u64(&dev)
        } else {
            let proc: Vec<u64> = self.samples.iter().map(|s| s.clone_proc_bytes).collect();
            median_u64(&proc)
        }
    }

    fn copy_secs_med(&self) -> f64 {
        let v: Vec<f64> = self.samples.iter().map(|s| s.copy_secs).collect();
        median_f64(&v)
    }

    fn clone_secs_med(&self) -> f64 {
        let v: Vec<f64> = self.samples.iter().map(|s| s.clone_secs).collect();
        median_f64(&v)
    }

    fn time_ratio(&self) -> f64 {
        let cl = self.clone_secs_med();
        if cl > 0.0 { self.copy_secs_med() / cl } else { f64::INFINITY }
    }

    fn bytes_ratio(&self) -> f64 {
        let cl = self.clone_bytes_med();
        if cl > 0 { self.copy_bytes_med() as f64 / cl as f64 } else { f64::INFINITY }
    }
}

fn format_ratio(r: f64) -> String {
    if r.is_infinite() {
        "inf".to_string()
    } else if r >= 100.0 {
        format!("{:.0}x", r)
    } else {
        format!("{:.2}x", r)
    }
}

fn format_mib(b: u64) -> String {
    format!("{:.2} MiB ({})", b as f64 / MIB, b)
}

fn format_report(total_bytes: u64, runs: usize, reports: &[MixReport]) -> String {
    let mut out = String::new();
    out.push_str("# Compaction Copy vs FICLONERANGE Probe (#104)\n\n");
    out.push_str(&format!(
        "- **Source logical size:** {:.2} GiB ({} bytes), 50% live, 4 KiB-aligned extents, dead half sparse\n",
        total_bytes as f64 / GIB,
        total_bytes
    ));
    out.push_str(&format!("- **Runs per mix:** {} (medians reported)\n\n", runs));
    out.push_str(
        "| mix | extents | live payload | source alloc | copy s | clone s | time ratio | copy bytes written | clone bytes written | bytes ratio |\n",
    );
    out.push_str("|---|---|---|---|---|---|---|---|---|---|\n");

    let mut any_ge_2x = false;
    let mut any_mid = false;
    for r in reports {
        let t_ratio = r.time_ratio();
        let b_ratio = r.bytes_ratio();
        if t_ratio >= 2.0 || b_ratio >= 2.0 {
            any_ge_2x = true;
        }
        if (1.5..2.0).contains(&t_ratio) || (1.5..2.0).contains(&b_ratio) {
            any_mid = true;
        }
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {:.3} | {:.3} | {} | {} | {} | {} |\n",
            r.mix.as_str(),
            r.extent_count,
            format_mib(r.live_bytes),
            format_mib(r.source_allocated_bytes),
            r.copy_secs_med(),
            r.clone_secs_med(),
            format_ratio(t_ratio),
            format_mib(r.copy_bytes_med()),
            format_mib(r.clone_bytes_med()),
            format_ratio(b_ratio),
        ));
    }

    out.push_str("\n## Per-run samples\n\n");
    out.push_str("| mix | run | copy s | clone s | copy block B | clone block B | copy proc B | clone proc B |\n");
    out.push_str("|---|---|---|---|---|---|---|---|\n");
    for r in reports {
        for (idx, s) in r.samples.iter().enumerate() {
            out.push_str(&format!(
                "| `{}` | {} | {:.3} | {:.3} | {} | {} | {} | {} |\n",
                r.mix.as_str(),
                idx + 1,
                s.copy_secs,
                s.clone_secs,
                s.copy_dev_bytes.map_or_else(|| "n/a".to_string(), |v| v.to_string()),
                s.clone_dev_bytes.map_or_else(|| "n/a".to_string(), |v| v.to_string()),
                s.copy_proc_bytes,
                s.clone_proc_bytes,
            ));
        }
    }

    out.push('\n');
    if any_mid && !any_ge_2x {
        out.push_str("Note: at least one mix is between 1.5x and 2x (stop and report per #104).\n");
    }
    if any_ge_2x {
        out.push_str("≥ 2x\n");
    } else {
        out.push_str("< 2x\n");
    }
    out
}

#[cfg(not(target_os = "linux"))]
fn main() {
    let _ = (Mix::parse, build_extents, format_report);
    println!("not supported");
}

#[cfg(target_os = "linux")]
mod linux_probe {
    use super::*;
    use std::fs::{File, OpenOptions};
    use std::io::{BufWriter, Write};
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::{FileExt, MetadataExt};
    use std::path::{Path, PathBuf};
    use std::time::Instant;

    #[repr(C)]
    struct FileCloneRange {
        src_fd: i64,
        src_offset: u64,
        src_length: u64,
        dest_offset: u64,
    }

    const _: () = assert!(std::mem::size_of::<FileCloneRange>() == 32);
    const FICLONERANGE: libc::Ioctl = 0x4020_940d as libc::Ioctl;

    fn read_proc_write_bytes() -> u64 {
        let Ok(txt) = std::fs::read_to_string("/proc/self/io") else {
            return 0;
        };
        parse_proc_io_write_bytes(&txt).unwrap_or(0)
    }

    fn read_block_bytes(path: Option<&Path>) -> Option<u64> {
        let p = path?;
        let txt = std::fs::read_to_string(p).ok()?;
        Some(parse_block_stat_sectors(&txt)?.saturating_mul(512))
    }

    fn sync_and_drop_caches(enabled: bool) {
        unsafe { libc::sync() };
        if enabled {
            let _ = std::fs::write("/proc/sys/vm/drop_caches", b"3\n");
        }
    }

    fn populate_source(path: &Path, total_bytes: u64, extents: &[Extent]) -> Result<u64, String> {
        let _ = std::fs::remove_file(path);
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| format!("create {}: {}", path.display(), e))?;
        f.set_len(total_bytes)
            .map_err(|e| format!("set_len {}: {}", path.display(), e))?;

        let mut buf = vec![0x5Au8; LARGE_EXTENT.min(4 * 1024 * 1024) as usize];
        for (i, ext) in extents.iter().enumerate() {
            let mut off = 0u64;
            while off < ext.len {
                let take = (buf.len() as u64).min(ext.len - off) as usize;
                let tag = (ext.src_offset + off).to_le_bytes();
                buf[0..8].copy_from_slice(&tag);
                f.write_all_at(&buf[..take], ext.src_offset + off)
                    .map_err(|e| format!("write source at {}: {}", ext.src_offset + off, e))?;
                off += take as u64;
            }
            if (i + 1) % 500_000 == 0 {
                eprintln!("  populated {}/{} extents...", i + 1, extents.len());
            }
        }
        f.sync_all().map_err(|e| format!("sync source: {}", e))?;
        let blocks = f.metadata().map_err(|e| format!("stat source: {}", e))?.blocks();
        Ok(blocks.saturating_mul(512))
    }

    fn verify_spot_checks(src: &File, dst_path: &Path, extents: &[Extent]) -> Result<(), String> {
        let dst = File::open(dst_path).map_err(|e| format!("open verify {}: {}", dst_path.display(), e))?;
        let expected_len: u64 = extents.iter().map(|e| e.len).sum();
        let actual_len = dst.metadata().map_err(|e| format!("stat dst: {}", e))?.len();
        if actual_len != expected_len {
            return Err(format!("dst len {} != expected {}", actual_len, expected_len));
        }
        let mut sbuf = [0u8; 4096];
        let mut dbuf = [0u8; 4096];
        for idx in [0, extents.len() / 2, extents.len() - 1] {
            let e = extents[idx];
            src.read_exact_at(&mut sbuf, e.src_offset)
                .map_err(|err| format!("verify src read {}: {}", e.src_offset, err))?;
            dst.read_exact_at(&mut dbuf, e.dest_offset)
                .map_err(|err| format!("verify dst read {}: {}", e.dest_offset, err))?;
            if sbuf != dbuf {
                return Err(format!("content mismatch at extent {} (src={} dst={})", idx, e.src_offset, e.dest_offset));
            }
        }
        Ok(())
    }

    fn run_copy_pass(
        src: &File,
        dst_path: &Path,
        extents: &[Extent],
        drop_caches: bool,
        block_stat: Option<&Path>,
    ) -> Result<(f64, u64, Option<u64>), String> {
        let _ = std::fs::remove_file(dst_path);
        sync_and_drop_caches(drop_caches);
        let p0 = read_proc_write_bytes();
        let d0 = read_block_bytes(block_stat);

        let t0 = Instant::now();
        let dst_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dst_path)
            .map_err(|e| format!("create dst_copy: {}", e))?;
        let mut writer = BufWriter::with_capacity(4 * 1024 * 1024, dst_file);
        let mut buf = vec![0u8; 4 * 1024 * 1024];
        for (i, ext) in extents.iter().enumerate() {
            let mut off = 0u64;
            while off < ext.len {
                let take = (buf.len() as u64).min(ext.len - off) as usize;
                src.read_exact_at(&mut buf[..take], ext.src_offset + off)
                    .map_err(|e| format!("read src at {}: {}", ext.src_offset + off, e))?;
                writer
                    .write_all(&buf[..take])
                    .map_err(|e| format!("write dst_copy: {}", e))?;
                off += take as u64;
            }
            if (i + 1) % 500_000 == 0 {
                eprintln!("  copied {}/{} extents...", i + 1, extents.len());
            }
        }
        let dst_file = writer.into_inner().map_err(|e| format!("flush dst_copy: {}", e))?;
        dst_file.sync_all().map_err(|e| format!("sync dst_copy: {}", e))?;
        let elapsed = t0.elapsed().as_secs_f64();

        unsafe { libc::sync() };
        let p1 = read_proc_write_bytes();
        let d1 = read_block_bytes(block_stat);
        drop(dst_file);
        verify_spot_checks(src, dst_path, extents)?;
        std::fs::remove_file(dst_path).map_err(|e| format!("remove dst_copy: {}", e))?;
        Ok((elapsed, p1.saturating_sub(p0), d0.zip(d1).map(|(a, b)| b.saturating_sub(a))))
    }

    fn run_clone_pass(
        src: &File,
        dst_path: &Path,
        extents: &[Extent],
        drop_caches: bool,
        block_stat: Option<&Path>,
    ) -> Result<(f64, u64, Option<u64>), String> {
        let _ = std::fs::remove_file(dst_path);
        sync_and_drop_caches(drop_caches);
        let p0 = read_proc_write_bytes();
        let d0 = read_block_bytes(block_stat);

        let t0 = Instant::now();
        let dst_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dst_path)
            .map_err(|e| format!("create dst_clone: {}", e))?;
        let src_fd = src.as_raw_fd() as i64;
        let dst_fd = dst_file.as_raw_fd();
        for (i, ext) in extents.iter().enumerate() {
            let range = FileCloneRange {
                src_fd,
                src_offset: ext.src_offset,
                src_length: ext.len,
                dest_offset: ext.dest_offset,
            };
            let ret = unsafe { libc::ioctl(dst_fd, FICLONERANGE, &range as *const FileCloneRange) };
            if ret != 0 {
                let err = std::io::Error::last_os_error();
                return Err(format!(
                    "FICLONERANGE failed at extent {} (src={} len={} dst={}): {}",
                    i, ext.src_offset, ext.len, ext.dest_offset, err
                ));
            }
            if (i + 1) % 500_000 == 0 {
                eprintln!("  cloned {}/{} extents...", i + 1, extents.len());
            }
        }
        dst_file.sync_all().map_err(|e| format!("sync dst_clone: {}", e))?;
        let elapsed = t0.elapsed().as_secs_f64();

        unsafe { libc::sync() };
        let p1 = read_proc_write_bytes();
        let d1 = read_block_bytes(block_stat);
        drop(dst_file);
        verify_spot_checks(src, dst_path, extents)?;
        std::fs::remove_file(dst_path).map_err(|e| format!("remove dst_clone: {}", e))?;
        Ok((elapsed, p1.saturating_sub(p0), d0.zip(d1).map(|(a, b)| b.saturating_sub(a))))
    }

    pub fn run() -> Result<(), String> {
        let mut dir: Option<PathBuf> = None;
        let mut total_bytes: u64 = 32 * 1024 * 1024 * 1024;
        let mut runs: usize = 3;
        let mut mixes: Vec<Mix> = vec![Mix::Small, Mix::Large, Mix::Mixed];
        let mut drop_caches = false;
        let mut block_stat: Option<PathBuf> = None;
        let mut output: Option<PathBuf> = None;

        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--dir" => dir = Some(PathBuf::from(args.next().ok_or("missing --dir value")?)),
                "--total-bytes" => {
                    total_bytes = args
                        .next()
                        .ok_or("missing --total-bytes value")?
                        .parse::<u64>()
                        .map_err(|e| format!("invalid --total-bytes: {}", e))?;
                }
                "--runs" => {
                    runs = args
                        .next()
                        .ok_or("missing --runs value")?
                        .parse::<usize>()
                        .map_err(|e| format!("invalid --runs: {}", e))?;
                }
                "--mixes" => {
                    let raw = args.next().ok_or("missing --mixes value")?;
                    mixes = raw
                        .split(',')
                        .filter(|s| !s.trim().is_empty())
                        .map(Mix::parse)
                        .collect::<Result<Vec<_>, _>>()?;
                }
                "--drop-caches" => drop_caches = true,
                "--block-stat" => block_stat = Some(PathBuf::from(args.next().ok_or("missing --block-stat value")?)),
                "--output" => output = Some(PathBuf::from(args.next().ok_or("missing --output value")?)),
                other => return Err(format!("unknown argument: {}", other)),
            }
        }
        let dir = dir.ok_or("--dir <path> is required")?;
        if runs == 0 {
            return Err("--runs must be >= 1".to_string());
        }
        if mixes.is_empty() {
            return Err("--mixes must not be empty".to_string());
        }

        let src_path = dir.join("probe_source.dat");
        let dst_copy = dir.join("probe_copy.dat");
        let dst_clone = dir.join("probe_clone.dat");

        let mut reports = Vec::with_capacity(mixes.len());
        for mix in mixes {
            let extents = build_extents(mix, total_bytes)?;
            let live_bytes: u64 = extents.iter().map(|e| e.len).sum();
            eprintln!(
                "=== mix={} extents={} live={} MiB ===",
                mix.as_str(),
                extents.len(),
                live_bytes / (1024 * 1024)
            );
            let source_allocated_bytes = populate_source(&src_path, total_bytes, &extents)?;
            let src_file = File::open(&src_path).map_err(|e| format!("open source: {}", e))?;

            let mut samples = Vec::with_capacity(runs);
            for r in 0..runs {
                eprintln!("  run {}/{}: copy...", r + 1, runs);
                let (copy_secs, copy_proc_bytes, copy_dev_bytes) =
                    run_copy_pass(&src_file, &dst_copy, &extents, drop_caches, block_stat.as_deref())?;
                eprintln!(
                    "  run {}/{}: copy {:.3}s dev={:?}B proc={}B; clone...",
                    r + 1,
                    runs,
                    copy_secs,
                    copy_dev_bytes,
                    copy_proc_bytes
                );
                let (clone_secs, clone_proc_bytes, clone_dev_bytes) =
                    run_clone_pass(&src_file, &dst_clone, &extents, drop_caches, block_stat.as_deref())?;
                eprintln!(
                    "  run {}/{}: clone {:.3}s dev={:?}B proc={}B",
                    r + 1,
                    runs,
                    clone_secs,
                    clone_dev_bytes,
                    clone_proc_bytes
                );
                samples.push(RunSample {
                    copy_secs,
                    clone_secs,
                    copy_proc_bytes,
                    clone_proc_bytes,
                    copy_dev_bytes,
                    clone_dev_bytes,
                });
            }
            drop(src_file);
            let _ = std::fs::remove_file(&src_path);
            reports.push(MixReport {
                mix,
                extent_count: extents.len(),
                live_bytes,
                source_allocated_bytes,
                samples,
            });
        }

        let md = format_report(total_bytes, runs, &reports);
        print!("{}", md);
        if let Some(out_path) = output {
            if let Some(parent) = out_path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            std::fs::write(&out_path, md.as_bytes()).map_err(|e| format!("write {}: {}", out_path.display(), e))?;
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn main() {
    if let Err(e) = linux_probe::run() {
        eprintln!("clone_probe error: {}", e);
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_valid_extents(extents: &[Extent], total_bytes: u64, expected_live: u64) {
        assert!(!extents.is_empty());
        let mut prev_src_end = 0u64;
        let mut expected_dest = 0u64;
        for ext in extents {
            assert_eq!(ext.src_offset % ALIGN, 0, "src_offset unaligned: {}", ext.src_offset);
            assert_eq!(ext.dest_offset % ALIGN, 0, "dest_offset unaligned: {}", ext.dest_offset);
            assert_eq!(ext.len % ALIGN, 0, "len unaligned: {}", ext.len);
            assert!(ext.len > 0);
            assert!(ext.src_offset >= prev_src_end, "extents overlap or unsorted");
            assert_eq!(ext.dest_offset, expected_dest, "dest offsets must pack contiguously");
            prev_src_end = ext.src_offset + ext.len;
            expected_dest += ext.len;
        }
        assert!(prev_src_end <= total_bytes, "extent end {} > total {}", prev_src_end, total_bytes);
        assert_eq!(expected_dest, expected_live, "live bytes mismatch");
    }

    #[test]
    fn small_large_and_mixed_32gib_extents_are_aligned_and_50_percent_live() {
        assert!(build_extents(Mix::Small, 4096).is_err());
        let gib32 = 32 * 1024 * 1024 * 1024u64;
        let gib16 = gib32 / 2;

        let small = build_extents(Mix::Small, gib32).unwrap();
        assert_eq!(small.len(), 4_194_304);
        assert!(small.iter().all(|e| e.len == SMALL_EXTENT));
        assert_valid_extents(&small, gib32, gib16);

        let large = build_extents(Mix::Large, gib32).unwrap();
        assert_eq!(large.len(), 256);
        assert!(large.iter().all(|e| e.len == LARGE_EXTENT));
        assert_valid_extents(&large, gib32, gib16);

        let mixed = build_extents(Mix::Mixed, gib32).unwrap();
        let n_large = mixed.iter().filter(|e| e.len == LARGE_EXTENT).count();
        let n_small = mixed.iter().filter(|e| e.len == SMALL_EXTENT).count();
        assert_eq!(n_large, 204); // 80% of 16 GiB = 12.8 GiB -> 204 * 64 MiB
        assert!(n_small > 800_000);
        assert_eq!(n_large + n_small, mixed.len());
        assert_valid_extents(&mixed, gib32, gib16);
    }

    #[test]
    fn medians_parsers_and_report_ratios_work() {
        assert_eq!(median_u64(&[30, 10, 20]), 20);
        assert!((median_f64(&[3.0, 1.0, 2.0]) - 2.0).abs() < 1e-9);
        assert_eq!(parse_proc_io_write_bytes("rchar: 1\nwrite_bytes: 17179869184\n"), Some(17_179_869_184));
        assert_eq!(parse_block_stat_sectors("10 0 80 5 100 0 33554432 20 0 10 30"), Some(33_554_432));

        let rep = MixReport {
            mix: Mix::Large,
            extent_count: 256,
            live_bytes: 16 * 1024 * 1024 * 1024,
            source_allocated_bytes: 16 * 1024 * 1024 * 1024,
            samples: vec![
                RunSample {
                    copy_secs: 20.0,
                    clone_secs: 1.0,
                    copy_proc_bytes: 16 * 1024 * 1024 * 1024,
                    clone_proc_bytes: 65536,
                    copy_dev_bytes: Some(16 * 1024 * 1024 * 1024),
                    clone_dev_bytes: Some(1024 * 1024),
                },
                RunSample {
                    copy_secs: 22.0,
                    clone_secs: 1.1,
                    copy_proc_bytes: 16 * 1024 * 1024 * 1024,
                    clone_proc_bytes: 65536,
                    copy_dev_bytes: Some(16 * 1024 * 1024 * 1024),
                    clone_dev_bytes: Some(1024 * 1024),
                },
                RunSample {
                    copy_secs: 24.0,
                    clone_secs: 1.2,
                    copy_proc_bytes: 16 * 1024 * 1024 * 1024,
                    clone_proc_bytes: 65536,
                    copy_dev_bytes: Some(16 * 1024 * 1024 * 1024),
                    clone_dev_bytes: Some(1024 * 1024),
                },
            ],
        };
        let txt = format_report(32 * 1024 * 1024 * 1024, 3, &[rep]);
        assert!(txt.contains("20.00x"));
        assert!(txt.contains("≥ 2x"));
    }
}
