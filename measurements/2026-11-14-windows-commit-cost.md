Command: `cargo test --locked --release --manifest-path server/Cargo.toml --test commit_cost_probe -- --ignored --nocapture` and `python -u benchmarks/put_path_probe.py --server-bin server/target/release/s3-server` (`.github/workflows/probe-sync.yml`)

Run on GitHub Actions: https://github.com/iskandarnorazli/Awanfs/actions/runs/43599299278 (PR #171). Both tools put their data under the runner's temp directory (D:\a\_temp on Windows). Copied from the job logs.

# Where a 64 KiB PUT's time goes on the Windows runner (2026-11-14)

## At the volume layer (no HTTP, no async runtime), 200 commits, one writer

windows-latest:

| volume | write ms (median) | volume + journal flush ms (median) | read back ms (median) |
|---|---|---|---|
| growing, never read | 0.045 | 0.585 | not measured |
| growing, read after each write | 0.069 | 0.751 | 0.126 |
| sparse, never read | 0.051 | 7.750 | not measured |
| sparse, read after each write | 0.075 | 7.728 | 0.026 |

ubuntu-latest:

| volume | write ms (median) | volume + journal flush ms (median) | read back ms (median) |
|---|---|---|---|
| growing, never read | 0.031 | 0.263 | not measured |
| growing, read after each write | 0.033 | 0.267 | 0.023 |
| sparse, never read | 0.033 | 0.273 | not measured |
| sparse, read after each write | 0.031 | 0.251 | 0.007 |

## Through the whole server, windows-latest: ops/s (P50, P90 ms)

| server started with | 1 client | 8 clients | 64 clients |
|---|---|---|---|
| default | 116 (8.54, 8.84) | 295 (26.64, 34.95) | 404 (152.20, 219.02) |
| no sync (`AWANFS_DURABILITY=fast`) | 2024 (0.45, 0.54) | 3118 (1.75, 3.18) | 3500 (13.35, 30.01) |

(Run 43598896601. The same on ubuntu-latest, run 43599299278: default 947, 2431, 2905 ops/s; no sync 3437, 6902, 6789.)

## What this shows

- **On Windows, flushing a sparse volume file takes 7.75 ms; flushing one that grows takes
  0.59 ms.** On Linux the two are the same (0.27 ms). The server's volumes are sparse 32 GiB
  files, so every commit on Windows pays the 7.75 ms, on any disk: with one client a PUT takes
  8.5 ms, and 0.45 ms without the commit.
- The disk matters as well (C: against D:, `2026-11-14-sync-probe.md`), and so does the write
  that waits for a flush of its own handle (PR #172), but this is the largest single cost on
  the fast disk and the only one a faster disk does not remove.
- A growing volume costs something on reads: 0.126 ms to read a needle back against 0.026 ms,
  because the mapping is redone as the file grows.
- `AWAN_SPARSE_VOLUME=1` made no difference through the server ("sparse volume" rows equal
  "default" in run 43598412603), which is how the default was found to be sparse already.

Not yet measured: the server with growing volumes on Windows, end to end. That is the next
experiment, and whether to change the volume layout on Windows is a decision after it.
