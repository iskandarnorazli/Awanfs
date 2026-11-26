Command: `gh workflow run bench-full.yml --ref experiment/362-small-put-strict` (run [45562061552](https://github.com/iskandarnorazli/Awanfs/actions/runs/45562061552), commit `b85edb4`), running `python benchmarks/small_put_latency.py --server-bin server/target/release/s3-server --durabilities group,strict --runs 3 --duration 2s` on `ubuntu-latest` and `windows-latest`

# Small-PUT P50/P99 Latency: `group` vs `strict` Experiment (#362, 2026-11-25)

One-time experiment authorized by Owner Decision Option C ([issue #362 comment](https://github.com/iskandarnorazli/Awanfs/issues/362#issuecomment-7073550567), 2026-11-22): measure `--durability=strict` small-PUT P50/P99 latency once into `measurements/` while keeping nightly workflows (`bench-full.yml`) at the default `--durability=group` only.

Measured on GitHub-hosted 4-vCPU runners in run [45562061552](https://github.com/iskandarnorazli/Awanfs/actions/runs/45562061552) (`ubuntu-latest` job [139027883206](https://github.com/iskandarnorazli/Awanfs/actions/runs/45562061552/job/139027883206), `windows-latest` job [139027882914](https://github.com/iskandarnorazli/Awanfs/actions/runs/45562061552/job/139027882914)), 3 alternating rounds of 2.0 s per workload phase (medians reported).

## Key observations (measured)

1. **Single-client (`c=1`) latency is parity between `group` and `strict`.** On Linux, 4 KiB PUT P50 is `0.458 ms` (`458.4 us`) for `group` and `0.451 ms` (`450.6 us`) for `strict` (P99 `0.657 ms` vs `0.601 ms`). On Windows (`D:\a\_temp`), 4 KiB PUT P50 is `0.939 ms` (`938.6 us`) for `group` and `0.944 ms` (`943.7 us`) for `strict` (P99 `2.069 ms` vs `3.019 ms`). With one client in flight, there are no concurrent PUTs to coalesce.
2. **Concurrent (`c=16`) group commit separates `group` from `strict`.** On Windows, 4 KiB PUT at `c=16` drops from `33.599 ms` P50 (`92.967 ms` P99, `441.0 ops/s`) under `strict` to `4.722 ms` P50 (`10.611 ms` P99, `3,168.7 ops/s`) under `group`, a `7.1x` lower P50 and `8.8x` lower P99. On Linux, 64 KiB PUT at `c=16` is `6.371 ms` P50 (`10.695 ms` P99) under `group` vs `8.734 ms` P50 (`15.059 ms` P99) under `strict`.
3. **Conditional PUTs (`If-None-Match: *` and `If-Match`) add negligible latency.** On Linux at `c=1`, `4 KiB PUT (If-None-Match: *)` is `0.475 ms` P50 / `0.654 ms` P99 and `4 KiB PUT (If-Match)` is `0.458 ms` P50 / `0.627 ms` P99 under `group`, matching unconditional 4 KiB PUT (`0.458 ms` P50 / `0.657 ms` P99). At `c=16`, both conditional modes stay at `1.742 ms` / `1.710 ms` P50 on Linux and `4.786 ms` / `4.806 ms` P50 on Windows.

## Linux (`ubuntu-latest`, job 139027883206)

- **Machine:** runnervm0hi8g (Linux 7.0.0-1020-azure (x86_64), 4 CPUs, 15.6 GB RAM, CPU: AMD EPYC 9V45 96-Core Processor)
- **Command on runner:** `python benchmarks/small_put_latency.py --server-bin server/target/release/s3-server --durabilities group,strict --runs 3 --duration 2s --data-dir /home/runner/work/_temp --output ci_small_put_latency.md --minio-bin .tmp/minio/minio --mc-bin .tmp/minio/mc --rustfs-bin .tmp/rustfs/rustfs`
- **Durability:** AwanFS (`--durability=group`, default): shared group commit sync before 200 OK (#29). AwanFS (`--durability=strict`): per-write `fsync`/`FlushFileBuffers` before 200 OK. MinIO: syncs each PUT (`O_DSYNC`). RustFS (release defaults): durable on Linux (3.09 sync calls/PUT, `measurements/2026-11-21-durability-probe.md`).

| Workload | Size | Concurrency | Condition | Target | ops/s | MiB/s | P50 (ms) | P50 (us) | P99 (ms) | P99 (us) |
|---|---|---|---|---|---|---|---|---|---|---|
| 4 KiB PUT | 4 KiB | 1 | none | AwanFS (--durability=group, default) | 2052.8 | 8.02 | 0.458 | 458.4 | 0.657 | 656.7 |
| 4 KiB PUT | 4 KiB | 1 | none | AwanFS (--durability=strict) | 2127.2 | 8.31 | 0.451 | 450.6 | 0.601 | 601.3 |
| 4 KiB PUT | 4 KiB | 1 | none | MinIO | 631.8 | 2.47 | 1.508 | 1507.5 | 5.045 | 5044.9 |
| 4 KiB PUT | 4 KiB | 1 | none | RustFS | 588.8 | 2.30 | 1.473 | 1472.6 | 5.526 | 5525.9 |
| 4 KiB PUT | 4 KiB | 16 | none | AwanFS (--durability=group, default) | 8429.3 | 32.93 | 1.719 | 1718.8 | 4.060 | 4059.9 |
| 4 KiB PUT | 4 KiB | 16 | none | AwanFS (--durability=strict) | 7572.7 | 29.58 | 1.854 | 1853.9 | 6.842 | 6841.9 |
| 4 KiB PUT | 4 KiB | 16 | none | MinIO | 2012.6 | 7.86 | 6.968 | 6967.8 | 32.120 | 32119.9 |
| 4 KiB PUT | 4 KiB | 16 | none | RustFS | 1955.4 | 7.64 | 8.050 | 8050.5 | 15.480 | 15479.5 |
| 16 KiB PUT | 16 KiB | 1 | none | AwanFS (--durability=group, default) | 1869.5 | 29.21 | 0.519 | 519.3 | 0.697 | 696.6 |
| 16 KiB PUT | 16 KiB | 1 | none | AwanFS (--durability=strict) | 1890.7 | 29.54 | 0.511 | 510.8 | 0.744 | 744.2 |
| 16 KiB PUT | 16 KiB | 1 | none | MinIO | 624.1 | 9.75 | 1.556 | 1556.1 | 4.683 | 4683.2 |
| 16 KiB PUT | 16 KiB | 1 | none | RustFS | 574.8 | 8.98 | 1.578 | 1578.3 | 6.174 | 6174.5 |
| 16 KiB PUT | 16 KiB | 16 | none | AwanFS (--durability=group, default) | 7167.8 | 112.00 | 1.977 | 1976.7 | 6.526 | 6526.2 |
| 16 KiB PUT | 16 KiB | 16 | none | AwanFS (--durability=strict) | 4579.2 | 71.55 | 2.480 | 2480.2 | 8.927 | 8927.4 |
| 16 KiB PUT | 16 KiB | 16 | none | MinIO | 1814.2 | 28.35 | 8.011 | 8011.0 | 24.192 | 24192.2 |
| 16 KiB PUT | 16 KiB | 16 | none | RustFS | 1848.0 | 28.88 | 8.550 | 8550.0 | 17.311 | 17310.8 |
| 64 KiB PUT | 64 KiB | 1 | none | AwanFS (--durability=group, default) | 1386.0 | 86.62 | 0.664 | 664.5 | 1.150 | 1150.0 |
| 64 KiB PUT | 64 KiB | 1 | none | AwanFS (--durability=strict) | 1473.4 | 92.09 | 0.656 | 655.8 | 0.849 | 848.7 |
| 64 KiB PUT | 64 KiB | 1 | none | MinIO | 550.8 | 34.42 | 1.671 | 1671.4 | 5.881 | 5880.9 |
| 64 KiB PUT | 64 KiB | 1 | none | RustFS | 550.0 | 34.37 | 1.724 | 1724.2 | 5.082 | 5082.2 |
| 64 KiB PUT | 64 KiB | 16 | none | AwanFS (--durability=group, default) | 2675.7 | 167.23 | 6.371 | 6371.5 | 10.695 | 10694.8 |
| 64 KiB PUT | 64 KiB | 16 | none | AwanFS (--durability=strict) | 2139.5 | 133.72 | 8.734 | 8734.1 | 15.059 | 15059.4 |
| 64 KiB PUT | 64 KiB | 16 | none | MinIO | 920.7 | 57.55 | 15.471 | 15471.2 | 49.675 | 49675.4 |
| 64 KiB PUT | 64 KiB | 16 | none | RustFS | 1254.3 | 78.39 | 12.491 | 12490.7 | 23.312 | 23312.2 |
| 4 KiB PUT (If-None-Match: *) | 4 KiB | 1 | If-None-Match: * | AwanFS (--durability=group, default) | 2039.8 | 7.97 | 0.475 | 475.4 | 0.654 | 653.5 |
| 4 KiB PUT (If-None-Match: *) | 4 KiB | 1 | If-None-Match: * | AwanFS (--durability=strict) | 2028.0 | 7.92 | 0.468 | 467.8 | 0.656 | 656.3 |
| 4 KiB PUT (If-None-Match: *) | 4 KiB | 1 | If-None-Match: * | MinIO | 952.5 | 3.72 | 1.002 | 1001.9 | 1.363 | 1363.0 |
| 4 KiB PUT (If-None-Match: *) | 4 KiB | 1 | If-None-Match: * | RustFS | 771.7 | 3.01 | 1.269 | 1269.5 | 1.878 | 1877.9 |
| 4 KiB PUT (If-None-Match: *) | 4 KiB | 16 | If-None-Match: * | AwanFS (--durability=group, default) | 8578.6 | 33.51 | 1.742 | 1741.6 | 3.852 | 3852.4 |
| 4 KiB PUT (If-None-Match: *) | 4 KiB | 16 | If-None-Match: * | AwanFS (--durability=strict) | 7619.9 | 29.77 | 1.907 | 1906.6 | 4.744 | 4744.3 |
| 4 KiB PUT (If-None-Match: *) | 4 KiB | 16 | If-None-Match: * | MinIO | 3197.4 | 12.49 | 4.483 | 4482.6 | 13.469 | 13469.5 |
| 4 KiB PUT (If-None-Match: *) | 4 KiB | 16 | If-None-Match: * | RustFS | 2954.3 | 11.54 | 5.131 | 5131.2 | 11.942 | 11942.0 |
| 4 KiB PUT (If-Match) | 4 KiB | 1 | If-Match | AwanFS (--durability=group, default) | 2048.4 | 8.00 | 0.458 | 457.6 | 0.627 | 626.8 |
| 4 KiB PUT (If-Match) | 4 KiB | 1 | If-Match | AwanFS (--durability=strict) | 2078.0 | 8.12 | 0.462 | 461.9 | 0.629 | 628.7 |
| 4 KiB PUT (If-Match) | 4 KiB | 1 | If-Match | MinIO | 615.1 | 2.40 | 1.525 | 1525.4 | 5.408 | 5408.2 |
| 4 KiB PUT (If-Match) | 4 KiB | 1 | If-Match | RustFS | 562.6 | 2.20 | 1.642 | 1641.8 | 5.353 | 5353.4 |
| 4 KiB PUT (If-Match) | 4 KiB | 16 | If-Match | AwanFS (--durability=group, default) | 8632.4 | 33.72 | 1.710 | 1710.5 | 3.681 | 3681.4 |
| 4 KiB PUT (If-Match) | 4 KiB | 16 | If-Match | AwanFS (--durability=strict) | 7810.5 | 30.51 | 1.895 | 1894.6 | 4.638 | 4638.2 |
| 4 KiB PUT (If-Match) | 4 KiB | 16 | If-Match | MinIO | 1865.0 | 7.29 | 7.655 | 7655.0 | 25.454 | 25454.4 |
| 4 KiB PUT (If-Match) | 4 KiB | 16 | If-Match | RustFS | 1973.3 | 7.71 | 7.827 | 7827.4 | 15.790 | 15790.2 |

## Windows (`windows-latest`, job 139027882914)

- **Machine:** runnervmbpeip (Windows 2025Server (AMD64), 4 CPUs, 16.0 GB RAM, CPU: AMD64 Family 25 Model 1 Stepping 1, AuthenticAMD)
- **Command on runner:** `python benchmarks/small_put_latency.py --server-bin server/target/release/s3-server.exe --durabilities group,strict --runs 3 --duration 2s --data-dir D:\a\_temp --output ci_small_put_latency.md --minio-bin .tmp/minio/minio.exe --mc-bin .tmp/minio/mc.exe --rustfs-bin .tmp/rustfs/rustfs.exe`
- **Durability:** AwanFS (`--durability=group`, default): shared group commit sync before 200 OK (#29). AwanFS (`--durability=strict`): per-write `fsync`/`FlushFileBuffers` before 200 OK. MinIO does not flush these PUTs on Windows (its `fdatasync` is a no-op there), so its PUT rows on this machine are not durable writes and are not like for like (`AGENTS.md`). RustFS Windows sync count is not probed.

| Workload | Size | Concurrency | Condition | Target | ops/s | MiB/s | P50 (ms) | P50 (us) | P99 (ms) | P99 (us) |
|---|---|---|---|---|---|---|---|---|---|---|
| 4 KiB PUT | 4 KiB | 1 | none | AwanFS (--durability=group, default) | 977.6 | 3.82 | 0.939 | 938.6 | 2.069 | 2068.6 |
| 4 KiB PUT | 4 KiB | 1 | none | AwanFS (--durability=strict) | 949.9 | 3.71 | 0.944 | 943.7 | 3.019 | 3018.8 |
| 4 KiB PUT | 4 KiB | 1 | none | MinIO | 429.9 | 1.68 | 2.241 | 2241.3 | 2.857 | 2856.8 |
| 4 KiB PUT | 4 KiB | 1 | none | RustFS | 212.2 | 0.83 | 4.415 | 4414.6 | 8.388 | 8387.6 |
| 4 KiB PUT | 4 KiB | 16 | none | AwanFS (--durability=group, default) | 3168.7 | 12.38 | 4.722 | 4722.3 | 10.611 | 10611.1 |
| 4 KiB PUT | 4 KiB | 16 | none | AwanFS (--durability=strict) | 441.0 | 1.72 | 33.599 | 33599.0 | 92.967 | 92967.4 |
| 4 KiB PUT | 4 KiB | 16 | none | MinIO | 767.6 | 3.00 | 7.384 | 7383.8 | 214.476 | 214476.1 |
| 4 KiB PUT | 4 KiB | 16 | none | RustFS | 69.3 | 0.27 | 235.963 | 235963.1 | 265.675 | 265675.4 |
| 16 KiB PUT | 16 KiB | 1 | none | AwanFS (--durability=group, default) | 936.4 | 14.63 | 0.974 | 973.6 | 2.590 | 2590.0 |
| 16 KiB PUT | 16 KiB | 1 | none | AwanFS (--durability=strict) | 920.1 | 14.38 | 0.984 | 984.4 | 3.073 | 3073.2 |
| 16 KiB PUT | 16 KiB | 1 | none | MinIO | 409.8 | 6.40 | 2.309 | 2308.6 | 3.549 | 3548.7 |
| 16 KiB PUT | 16 KiB | 1 | none | RustFS | 203.8 | 3.19 | 4.635 | 4635.0 | 9.811 | 9810.5 |
| 16 KiB PUT | 16 KiB | 16 | none | AwanFS (--durability=group, default) | 3165.5 | 49.46 | 4.748 | 4747.9 | 10.707 | 10706.6 |
| 16 KiB PUT | 16 KiB | 16 | none | AwanFS (--durability=strict) | 462.7 | 7.23 | 32.843 | 32843.3 | 91.945 | 91945.5 |
| 16 KiB PUT | 16 KiB | 16 | none | MinIO | 745.7 | 11.65 | 7.396 | 7396.3 | 209.702 | 209701.7 |
| 16 KiB PUT | 16 KiB | 16 | none | RustFS | 74.6 | 1.17 | 217.649 | 217648.6 | 256.853 | 256853.4 |
| 64 KiB PUT | 64 KiB | 1 | none | AwanFS (--durability=group, default) | 791.6 | 49.48 | 1.166 | 1165.8 | 3.404 | 3403.6 |
| 64 KiB PUT | 64 KiB | 1 | none | AwanFS (--durability=strict) | 780.1 | 48.76 | 1.163 | 1162.8 | 3.506 | 3506.2 |
| 64 KiB PUT | 64 KiB | 1 | none | MinIO | 382.8 | 23.92 | 2.476 | 2476.0 | 4.497 | 4497.2 |
| 64 KiB PUT | 64 KiB | 1 | none | RustFS | 195.2 | 12.20 | 4.840 | 4840.1 | 8.838 | 8837.6 |
| 64 KiB PUT | 64 KiB | 16 | none | AwanFS (--durability=group, default) | 2927.1 | 182.95 | 5.158 | 5158.5 | 11.080 | 11080.5 |
| 64 KiB PUT | 64 KiB | 16 | none | AwanFS (--durability=strict) | 538.2 | 33.64 | 29.963 | 29962.7 | 84.009 | 84009.2 |
| 64 KiB PUT | 64 KiB | 16 | none | MinIO | 731.5 | 45.72 | 8.099 | 8099.1 | 207.248 | 207248.0 |
| 64 KiB PUT | 64 KiB | 16 | none | RustFS | 68.8 | 4.30 | 236.709 | 236708.8 | 267.902 | 267902.1 |
| 4 KiB PUT (If-None-Match: *) | 4 KiB | 1 | If-None-Match: * | AwanFS (--durability=group, default) | 961.5 | 3.76 | 0.940 | 940.2 | 2.496 | 2495.6 |
| 4 KiB PUT (If-None-Match: *) | 4 KiB | 1 | If-None-Match: * | AwanFS (--durability=strict) | 978.4 | 3.82 | 0.928 | 928.3 | 2.704 | 2704.2 |
| 4 KiB PUT (If-None-Match: *) | 4 KiB | 1 | If-None-Match: * | MinIO | 548.8 | 2.14 | 1.744 | 1743.9 | 2.649 | 2649.4 |
| 4 KiB PUT (If-None-Match: *) | 4 KiB | 1 | If-None-Match: * | RustFS | 312.0 | 1.22 | 3.057 | 3057.3 | 5.842 | 5842.1 |
| 4 KiB PUT (If-None-Match: *) | 4 KiB | 16 | If-None-Match: * | AwanFS (--durability=group, default) | 3105.9 | 12.13 | 4.786 | 4786.2 | 10.905 | 10904.6 |
| 4 KiB PUT (If-None-Match: *) | 4 KiB | 16 | If-None-Match: * | AwanFS (--durability=strict) | 453.8 | 1.77 | 32.123 | 32123.2 | 92.407 | 92407.2 |
| 4 KiB PUT (If-None-Match: *) | 4 KiB | 16 | If-None-Match: * | MinIO | 1291.4 | 5.04 | 10.085 | 10085.0 | 39.405 | 39405.3 |
| 4 KiB PUT (If-None-Match: *) | 4 KiB | 16 | If-None-Match: * | RustFS | 175.2 | 0.68 | 96.169 | 96169.5 | 159.245 | 159244.7 |
| 4 KiB PUT (If-Match) | 4 KiB | 1 | If-Match | AwanFS (--durability=group, default) | 989.7 | 3.87 | 0.921 | 920.8 | 2.491 | 2491.1 |
| 4 KiB PUT (If-Match) | 4 KiB | 1 | If-Match | AwanFS (--durability=strict) | 988.0 | 3.86 | 0.934 | 934.4 | 2.832 | 2831.6 |
| 4 KiB PUT (If-Match) | 4 KiB | 1 | If-Match | MinIO | 400.3 | 1.56 | 2.407 | 2407.3 | 3.183 | 3182.8 |
| 4 KiB PUT (If-Match) | 4 KiB | 1 | If-Match | RustFS | 186.1 | 0.73 | 5.069 | 5069.2 | 8.540 | 8540.3 |
| 4 KiB PUT (If-Match) | 4 KiB | 16 | If-Match | AwanFS (--durability=group, default) | 3078.2 | 12.02 | 4.806 | 4806.0 | 10.849 | 10849.3 |
| 4 KiB PUT (If-Match) | 4 KiB | 16 | If-Match | AwanFS (--durability=strict) | 566.1 | 2.21 | 30.314 | 30314.2 | 86.180 | 86179.6 |
| 4 KiB PUT (If-Match) | 4 KiB | 16 | If-Match | MinIO | 751.1 | 2.93 | 7.307 | 7306.9 | 212.846 | 212845.5 |
| 4 KiB PUT (If-Match) | 4 KiB | 16 | If-Match | RustFS | 74.3 | 0.29 | 212.595 | 212594.7 | 252.956 | 252955.7 |
