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

## The experiment: the first volume grows on Windows (branch `try/windows-growing-volume`)

One line changed: a new data directory's first volume is opened growing, not sparse, on
Windows. Data on D:, as above.

Through the server alone (run 43599934385), ops/s (P50, P90 ms):

| | 1 client | 8 clients | 64 clients |
|---|---|---|---|
| sparse first volume (today) | 116 (8.54, 8.84) | 295 (26.64, 34.95) | 404 (152.20, 219.02) |
| growing first volume | 2212 (0.43, 0.49) | 3625 (1.39, 4.33) | 3633 (17.13, 25.30) |

Full matrix on windows-latest, 64 clients (run 43600434246), next to the run that started
this (43579954027: sparse, data on C:) and the one with only the disk changed (43597407482:
sparse, data on D:). AwanFS ops/s, with MinIO and RustFS from the last run:

| workload | sparse, C: | sparse, D: | growing, D: | MinIO | RustFS |
|---|---|---|---|---|---|
| 64 KiB PUT | 118 | 416 | 3685 | 772 | 189 |
| 64 KiB GET | 2842 | 3074 | 4808 | 3342 | 4009 |
| 64 KiB mixed | 532 | 788 | 4487 | 2656 | 848 |
| 1 MiB PUT | 74 | 179 | 243 | 214 | 65 |
| 1 MiB GET | 2156 | not copied | 3119 | 1917 | 1192 |
| mixed sweep, 8 clients | 336 | 374 | 4804 | 2979 | 668 |
| mixed sweep, 128 clients | 441 | not copied | 4377 | 1394 | 801 |

With the first volume growing, AwanFS is ahead of both on every row of that run, and GETs did
not get slower. MinIO's PUT rows on Windows are not durable writes (its fdatasync is a no-op
there); AwanFS's are.

Not measured: a volume larger than these short runs fill (the mapping is redone as the file
grows, and the cost of that at tens of GiB is unknown), and PR #172 together with this.
