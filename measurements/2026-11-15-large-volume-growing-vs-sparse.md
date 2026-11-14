Command: `python -u benchmarks/large_volume_probe.py --server-bin server/target/release/s3-server --fill-gib 10` (`.github/workflows/probe-sync.yml`)

Run on GitHub Actions: https://github.com/iskandarnorazli/Awanfs/actions/runs/43632200501 (PR #173). Each layout twice, in opposite orders. Copied from the job logs.

# A 10 GiB volume: growing against sparse (2026-11-15)

## ubuntu-latest

Machine: runnervmccnms (Linux-7.0.0-1019-azure-x86_64-with-glibc2.43, 4 CPUs). Data under /home/runner/work/_temp. Each server first stores 10 GiB of 4 MiB objects; the rows after that are 64 KiB objects at 64 clients.

| peak RSS, MiB | 1133.8 |
| | growing (default), round 1 | sparse (AWAN_SPARSE_VOLUME=1), round 1 | sparse (AWAN_SPARSE_VOLUME=1), round 2 | growing (default), round 2 |
| volume on disk after the fill, GiB | 10.0 | 10.0 | 10.0 | 10.0 |
| fill, MiB/s (8 clients) | 245 | 197 | 271 | 215 |
| PUT: ops/s (P50, P99 ms) | 2878 (20.63, 46.00) | 2897 (20.41, 44.47) | 3899 (17.16, 31.59) | 2899 (20.40, 46.61) |
| GET: ops/s (P50, P99 ms) | 14011 (4.17, 13.05) | 13666 (4.23, 13.50) | 14218 (3.93, 12.93) | 14036 (4.02, 13.12) |
| mixed (80% GET): ops/s (P50, P99 ms) | 11089 (5.03, 17.26) | 10708 (5.17, 18.25) | 11283 (4.97, 16.91) | 10695 (4.99, 17.65) |
| GET straight after its PUT, 1 client: P50, P99 ms | 0.18, 0.23 | 0.16, 0.24 | 0.17, 0.21 | 0.15, 0.22 |
| peak RSS, MiB | 1120.1 | 1112.7 | 1120.1 | 1133.8 |

## windows-latest

Machine: runnervmm7ud1 (Windows-2025Server-10.0.26100-SP0, 4 CPUs). Data under D:\a\_temp. Each server first stores 10 GiB of 4 MiB objects; the rows after that are 64 KiB objects at 64 clients.

| peak RSS, MiB | 1120.7 |
| | growing (default), round 1 | sparse (AWAN_SPARSE_VOLUME=1), round 1 | sparse (AWAN_SPARSE_VOLUME=1), round 2 | growing (default), round 2 |
| volume on disk after the fill, GiB | 10.0 | 32.0 | 32.0 | 10.0 |
| fill, MiB/s (8 clients) | 386 | 92 | 92 | 387 |
| PUT: ops/s (P50, P99 ms) | 3371 (17.08, 48.54) | 84 (584.74, 1736.56) | 79 (617.71, 1849.45) | 3244 (17.71, 49.66) |
| GET: ops/s (P50, P99 ms) | 2891 (20.98, 45.44) | 2898 (21.27, 45.41) | 2915 (21.17, 44.99) | 2914 (21.09, 44.95) |
| mixed (80% GET): ops/s (P50, P99 ms) | 2862 (20.62, 51.33) | 419 (18.67, 1161.10) | 379 (18.09, 1220.41) | 2903 (20.27, 50.63) |
| GET straight after its PUT, 1 client: P50, P99 ms | 14.86, 30.17 | 12.10, 26.39 | 12.25, 26.11 | 14.94, 29.70 |
| peak RSS, MiB | 1120.0 | 1111.6 | 1112.5 | 1120.7 |

## What this shows

- **Linux: no difference.** GET, mixed and a GET straight after its PUT are the same either
  way. PUT was 2878 and 2899 ops/s growing against 2897 and 3899 sparse: one sparse round was
  higher and the other level, as in the first run (43628997538: 1821 against 2595), so the
  PUT phase on these runners varies by more than the layouts differ.
- **Windows: growing is about 40 times faster for writes on a filled volume.** PUT 3371 and
  3244 ops/s against 84 and 79; mixed 2862 and 2903 against 419 and 379; the fill itself 386
  MiB/s against 92. GETs are the same. A sparse volume also shows as 32 GiB on disk.
- **Windows: a read that redoes the mapping costs about 2.8 ms more at 10 GiB.** A GET
  straight after its PUT, one client: 14.9 ms growing against 12.1 to 12.3 ms sparse. On a
  small volume the difference was 0.1 ms (`2026-11-14-windows-commit-cost.md`), so the cost
  grows with the volume. It did not show in throughput here (mixed load matched GET-only),
  but it is the stall the sparse design was meant to avoid, and it will be larger at 32 GiB.
  Growing the file in steps and mapping the whole step would redo the mapping once per step
  instead of once per new read.
- The 12 ms itself, on both layouts, is mostly this probe opening a new connection for each
  request on Windows; it is not the server's read time.
