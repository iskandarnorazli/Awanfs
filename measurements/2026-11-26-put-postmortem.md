`bench-scaling.yml` run 45810996859 (job "Core scaling (self-hosted)" 139878544823 on LAPTOP-6RBIQUM5, branch `try/430-postmortem` at `2b73ca4`, not merged), which ran `python benchmarks/core_scaling.py --runs 2 --duration 5 --client-procs 8 --data-dir <runner temp> --output <runner temp>/core_scaling/report.md --compare-fast --cores 2,4,8,12 --matrix --pattern seq-write,seq-read --sizes 4KiB,64KiB,256KiB,1MiB,4MiB --concurrencies 16,64,128,256 --seed 42 --servers AwanFS --fast-every-concurrency`

# AwanFS PUT post mortem: size, concurrency, group vs fast, cores (#430)

One experiment run, AwanFS only, 240 points, each the median of 2 rounds of 5 s. The sync counters come from `/_awanfs/metrics`, read before and after each phase (#431). The report's own title lists five servers; only AwanFS ran (`--servers AwanFS`). The report below is the job's output, cut to its tables; the charts and the per-thread and client CPU sections are left out.

## What it shows

**1. PUT 4 KiB with group commit is capped by how many PUTs share a sync, not by a thread or by cores.** Throughput is syncs per second times PUTs per sync. At 8 cores (from the "work per phase" table):

| 8 cores, group | c=16 | c=64 | c=128 | c=256 |
|---|---|---|---|---|
| ops/s | 6,255 | 11,211 | 12,313 | 12,784 |
| syncs/s | 1,346.7 | 709.4 | 450.4 | 423.6 |
| PUTs per sync | 4.6 | 15.8 | 27.3 | 30.2 |

At 2 cores it stays between 4,876 and 6,216 ops/s at every concurrency. With `--durability fast` (about 1.5 syncs/s, the background flusher) it reaches 16,232 at 8 cores, c=64. More cores help only when enough PUTs are in flight to fill a sync. The levers are batching (wait a little to gather more PUTs into one sync, at a cost in latency) or a faster sync; both are design decisions (#436).

**2. Large PUTs were 2 to 3 times slower than in the two earlier runs, in fast mode too.** PUT 1 MiB at 8 cores: group 103 to 227 ops/s, fast 150 to 440 ops/s. Run 45808274063 measured 516 (4 cores) and 525 (8 cores) with group commit. Fast mode's disk rate here is 132.5 to 371.2 MiB/s at 8 cores. Since fast mode slowed as well, the likely cause is the laptop's SSD: the sweep writes tens of GB without a pause, which can exhaust the drive's write cache. That is inferred; this run did not measure the disk. It also makes the first run's 308 ops/s for PUT 1 MiB at 8 cores (run 45790294615) most likely the same disk variance. Large-object numbers from this laptop need a disk check (`sync_probe` and a sequential write rate) before and after the sweep.

**3. GET levels off at the same byte rate for every large size.** GET 1 MiB holds about 2,000 to 2,200 ops/s and GET 4 MiB about 490 to 550 ops/s from 4 cores up, both about 2.0 to 2.2 GB/s. The server uses 2.4 to 3.2 cores, and the client is not flagged as saturated at 4 and 8 cores. A cap in bytes points to the loopback connection or the client reading the bodies, not the server; also inferred.

## Not measured

- The disk's own write rate before and after the sweep.
- `TOKIO_WORKER_THREADS` set to the pinned core count (#430 step 2).
- Other servers: this run is AwanFS only.

## Report (job output)

# Pattern & Concurrency Scaling Benchmark: AwanFS vs MinIO vs RustFS vs Garage vs SeaweedFS (2026-11-26 13:21 UTC)

- **Machine:** LAPTOP-6RBIQUM5 (Windows-11-10.0.26300-SP0, 16 logical cores, 31.4 GB RAM, CPU: AMD64 Family 25 Model 80 Stepping 0, AuthenticAMD)
- **Methodology:** `patterns = seq-write, seq-read`, `sizes = 4 KiB, 64 KiB, 256 KiB, 1 MiB, 4 MiB`, `cores = 2, 4, 8, 12`, `concurrencies = 16, 64, 128, 256`, `client_procs = 8`, 2 alternating runs per point (median reported), 5.0s per phase, seed = `42`.
- **Cache & Prefill:** AwanFS at its default `--memory-budget` (a quarter of RAM, held between 128 MiB and 2 GiB; half of it is the cache). Read prefill size = 48 MiB (12,288 x 4 KiB objects + 768 x 64 KiB objects + 192 x 256 KiB objects + 1 x 48 MiB large object for range GETs), identical deterministic bytes for every server, written before timing.
- **Core pinning:** Load generator is pinned to remaining cores (`N..15`) at all tested core counts (never shares cores with the server).
- **Durability:** AwanFS group commit (#29). MinIO does not flush these PUTs on Windows (its fdatasync is a no-op there), so its PUT and mixed rows on this machine are not durable writes and are not like for like. RustFS release defaults (durable, 3.09 syncs/PUT; measurements/2026-11-21-durability-probe.md, #334). Garage `metadata_fsync=false, data_fsync=false`. SeaweedFS `fsync` unset (`false`) by default.

## Durability Settings

| Server | Documented Single-Node Default | PUT waits for sync? | Notes |
|---|---|---|---|
| AwanFS | --durability=group (group commit, #29) | Yes | Concurrent PUTs share one fsync before 200 OK. |
| MinIO | fdatasync_unsupported.go (no-op on Windows) | No (for > 4 KiB) | Only files <= 4 KiB open write-through on Windows; 64 KiB and 1 MiB PUTs do not wait for disk sync. |
| RustFS | release defaults (durable) | Yes | Measured on Linux (measurements/2026-11-21-durability-probe.md, #334): 3.09 sync calls/PUT (618 syncs / 200 PUTs). |
| Garage | metadata_fsync = false, data_fsync = false (LMDB) | No | Default single-node config skips fsync on metadata and data blocks (cluster mode relies on replication); client uses AWS SigV4 (UNSIGNED-PAYLOAD). |
| SeaweedFS | weed mini (filer.conf fsync unset / false) | No | Default storage rule skips fsync on volume .dat/.idx and filer metadata unless fsync = true is configured. |

## All Measured Points

| Server | Pattern | Size | Cores | Concurrency | ops/s | MiB/s | P50 (ms) | P99 (ms) | CPU % |
|---|---|---|---|---|---|---|---|---|---|
| AwanFS | seq-write | 4 KiB | 2 | 16 | 4,876 | 19.05 | 3.09 | 6.18 | 148.9% |
| AwanFS | seq-write | 4 KiB | 2 | 64 | 6,216 | 24.28 | 9.07 | 26.05 | 182.5% |
| AwanFS | seq-write | 4 KiB | 2 | 128 | 5,967 | 23.31 | 19.97 | 46.55 | 185.5% |
| AwanFS | seq-write | 4 KiB | 2 | 256 | 5,845 | 22.83 | 38.64 | 100.90 | 184.0% |
| AwanFS | seq-write | 4 KiB | 4 | 16 | 5,241 | 20.47 | 2.50 | 8.81 | 177.2% |
| AwanFS | seq-write | 4 KiB | 4 | 64 | 8,461 | 33.05 | 6.35 | 18.90 | 266.7% |
| AwanFS | seq-write | 4 KiB | 4 | 128 | 10,025 | 39.16 | 11.32 | 33.48 | 313.9% |
| AwanFS | seq-write | 4 KiB | 4 | 256 | 10,609 | 41.44 | 21.61 | 51.34 | 351.5% |
| AwanFS | seq-write | 4 KiB | 8 | 16 | 6,255 | 24.43 | 2.34 | 4.74 | 226.4% |
| AwanFS | seq-write | 4 KiB | 8 | 64 | 11,211 | 43.79 | 5.13 | 12.71 | 412.9% |
| AwanFS | seq-write | 4 KiB | 8 | 128 | 12,313 | 48.10 | 8.95 | 25.83 | 415.4% |
| AwanFS | seq-write | 4 KiB | 8 | 256 | 12,784 | 49.94 | 17.43 | 46.62 | 448.2% |
| AwanFS | seq-write | 4 KiB | 12 | 16 | 5,425 | 21.19 | 2.30 | 16.73 | 200.1% |
| AwanFS | seq-write | 4 KiB | 12 | 64 | 11,217 | 43.82 | 4.92 | 14.33 | 438.6% |
| AwanFS | seq-write | 4 KiB | 12 | 128 | 10,548 | 41.20 | 7.68 | 70.09 | 397.3% |
| AwanFS | seq-write | 4 KiB | 12 | 256 | 8,948 | 34.95 | 18.94 | 174.78 | 327.1% |
| AwanFS | seq-write | 64 KiB | 2 | 16 | 1,874 | 117.15 | 7.88 | 18.29 | 177.4% |
| AwanFS | seq-write | 64 KiB | 2 | 64 | 1,220 | 76.22 | 50.48 | 100.38 | 156.2% |
| AwanFS | seq-write | 64 KiB | 2 | 128 | 1,325 | 82.82 | 90.46 | 207.78 | 163.7% |
| AwanFS | seq-write | 64 KiB | 2 | 256 | 1,394 | 87.12 | 159.71 | 842.15 | 161.1% |
| AwanFS | seq-write | 64 KiB | 4 | 16 | 2,070 | 129.39 | 7.06 | 17.73 | 228.7% |
| AwanFS | seq-write | 64 KiB | 4 | 64 | 1,264 | 78.98 | 48.15 | 100.30 | 164.4% |
| AwanFS | seq-write | 64 KiB | 4 | 128 | 1,394 | 87.09 | 86.10 | 196.05 | 169.9% |
| AwanFS | seq-write | 64 KiB | 4 | 256 | 1,475 | 92.21 | 154.46 | 733.84 | 182.0% |
| AwanFS | seq-write | 64 KiB | 8 | 16 | 2,044 | 127.76 | 6.87 | 19.61 | 243.1% |
| AwanFS | seq-write | 64 KiB | 8 | 64 | 1,272 | 79.48 | 48.26 | 95.33 | 168.6% |
| AwanFS | seq-write | 64 KiB | 8 | 128 | 1,390 | 86.88 | 86.81 | 197.33 | 178.6% |
| AwanFS | seq-write | 64 KiB | 8 | 256 | 1,478 | 92.36 | 149.56 | 849.85 | 188.3% |
| AwanFS | seq-write | 64 KiB | 12 | 16 | 1,955 | 122.21 | 7.10 | 22.51 | 213.1% |
| AwanFS | seq-write | 64 KiB | 12 | 64 | 1,365 | 85.34 | 45.18 | 89.51 | 174.8% |
| AwanFS | seq-write | 64 KiB | 12 | 128 | 1,533 | 95.78 | 79.12 | 172.98 | 187.0% |
| AwanFS | seq-write | 64 KiB | 12 | 256 | 1,689 | 105.57 | 138.95 | 368.71 | 199.5% |
| AwanFS | seq-write | 256 KiB | 2 | 16 | 615 | 153.72 | 22.89 | 63.32 | 141.7% |
| AwanFS | seq-write | 256 KiB | 2 | 64 | 754 | 188.53 | 77.52 | 201.44 | 137.9% |
| AwanFS | seq-write | 256 KiB | 2 | 128 | 822 | 205.60 | 152.42 | 425.29 | 138.7% |
| AwanFS | seq-write | 256 KiB | 2 | 256 | 975 | 243.76 | 240.64 | 977.99 | 156.3% |
| AwanFS | seq-write | 256 KiB | 4 | 16 | 718 | 179.38 | 20.38 | 52.28 | 192.2% |
| AwanFS | seq-write | 256 KiB | 4 | 64 | 1,205 | 301.26 | 44.21 | 165.50 | 233.2% |
| AwanFS | seq-write | 256 KiB | 4 | 128 | 690 | 172.43 | 173.93 | 411.21 | 114.6% |
| AwanFS | seq-write | 256 KiB | 4 | 256 | 1,138 | 284.47 | 156.26 | 1000.84 | 195.1% |
| AwanFS | seq-write | 256 KiB | 8 | 16 | 485 | 121.21 | 32.61 | 77.44 | 123.2% |
| AwanFS | seq-write | 256 KiB | 8 | 64 | 1,136 | 283.94 | 51.19 | 140.49 | 235.5% |
| AwanFS | seq-write | 256 KiB | 8 | 128 | 679 | 169.65 | 174.92 | 392.57 | 123.8% |
| AwanFS | seq-write | 256 KiB | 8 | 256 | 1,169 | 292.34 | 157.06 | 848.13 | 219.0% |
| AwanFS | seq-write | 256 KiB | 12 | 16 | 695 | 173.82 | 21.51 | 59.78 | 181.7% |
| AwanFS | seq-write | 256 KiB | 12 | 64 | 1,413 | 353.27 | 42.64 | 104.70 | 288.8% |
| AwanFS | seq-write | 256 KiB | 12 | 128 | 887 | 221.63 | 122.98 | 455.63 | 153.1% |
| AwanFS | seq-write | 256 KiB | 12 | 256 | 845 | 211.33 | 272.88 | 1082.59 | 148.6% |
| AwanFS | seq-write | 1 MiB | 2 | 16 | 187 | 187.32 | 80.95 | 193.19 | 117.7% |
| AwanFS | seq-write | 1 MiB | 2 | 64 | 146 | 145.70 | 430.30 | 877.00 | 89.5% |
| AwanFS | seq-write | 1 MiB | 2 | 128 | 101 | 100.51 | 1306.58 | 3318.84 | 61.6% |
| AwanFS | seq-write | 1 MiB | 2 | 256 | 92 | 92.08 | 3297.30 | 5911.56 | 57.0% |
| AwanFS | seq-write | 1 MiB | 4 | 16 | 146 | 146.22 | 105.89 | 206.60 | 93.3% |
| AwanFS | seq-write | 1 MiB | 4 | 64 | 167 | 166.61 | 262.76 | 1561.86 | 109.9% |
| AwanFS | seq-write | 1 MiB | 4 | 128 | 95 | 94.85 | 1464.16 | 3593.06 | 62.6% |
| AwanFS | seq-write | 1 MiB | 4 | 256 | 128 | 128.18 | 2782.26 | 6096.48 | 88.6% |
| AwanFS | seq-write | 1 MiB | 8 | 16 | 139 | 139.32 | 111.47 | 213.03 | 100.2% |
| AwanFS | seq-write | 1 MiB | 8 | 64 | 227 | 227.49 | 142.54 | 1239.75 | 192.8% |
| AwanFS | seq-write | 1 MiB | 8 | 128 | 103 | 103.07 | 1447.88 | 3091.04 | 88.3% |
| AwanFS | seq-write | 1 MiB | 8 | 256 | 134 | 134.23 | 3058.38 | 5128.51 | 113.0% |
| AwanFS | seq-write | 1 MiB | 12 | 16 | 216 | 216.11 | 68.50 | 178.66 | 178.6% |
| AwanFS | seq-write | 1 MiB | 12 | 64 | 212 | 212.17 | 275.26 | 827.33 | 186.3% |
| AwanFS | seq-write | 1 MiB | 12 | 128 | 139 | 138.98 | 789.85 | 2029.46 | 118.0% |
| AwanFS | seq-write | 1 MiB | 12 | 256 | 173 | 173.39 | 1404.02 | 3622.16 | 149.0% |
| AwanFS | seq-write | 4 MiB | 2 | 16 | 20 | 80.57 | 1190.80 | 1499.70 | 39.6% |
| AwanFS | seq-write | 4 MiB | 2 | 64 | 31 | 123.87 | 4912.48 | 7036.34 | 70.4% |
| AwanFS | seq-write | 4 MiB | 2 | 128 | 39 | 156.48 | 2901.68 | 5701.16 | 86.2% |
| AwanFS | seq-write | 4 MiB | 2 | 256 | 63 | 251.24 | 3510.31 | 8245.06 | 141.4% |
| AwanFS | seq-write | 4 MiB | 4 | 16 | 91 | 362.29 | 137.41 | 591.30 | 215.9% |
| AwanFS | seq-write | 4 MiB | 4 | 64 | 33 | 131.73 | 1740.02 | 3141.29 | 73.4% |
| AwanFS | seq-write | 4 MiB | 4 | 128 | 38 | 151.60 | 2824.19 | 5007.89 | 88.7% |
| AwanFS | seq-write | 4 MiB | 4 | 256 | 54 | 214.73 | 4915.96 | 6732.11 | 129.5% |
| AwanFS | seq-write | 4 MiB | 8 | 16 | 20 | 79.23 | 2727.06 | 2888.33 | 56.9% |
| AwanFS | seq-write | 4 MiB | 8 | 64 | 18 | 73.53 | 5366.06 | 8503.19 | 48.6% |
| AwanFS | seq-write | 4 MiB | 8 | 128 | 21 | 84.56 | 8601.11 | 10051.94 | 53.1% |
| AwanFS | seq-write | 4 MiB | 8 | 256 | 93 | 373.78 | 3409.31 | 7073.69 | 306.0% |
| AwanFS | seq-write | 4 MiB | 12 | 16 | 33 | 130.78 | 496.47 | 909.85 | 102.1% |
| AwanFS | seq-write | 4 MiB | 12 | 64 | 38 | 152.34 | 3763.35 | 4914.71 | 105.6% |
| AwanFS | seq-write | 4 MiB | 12 | 128 | 49 | 196.13 | 1543.98 | 5917.28 | 148.4% |
| AwanFS | seq-write | 4 MiB | 12 | 256 | 40 | 158.15 | 7216.58 | 7831.02 | 126.8% |
| AwanFS | seq-read | 4 KiB | 2 | 16 | 15,431 | 60.28 | 0.50 | 10.23 | 189.9% |
| AwanFS | seq-read | 4 KiB | 2 | 64 | 17,347 | 67.76 | 2.97 | 14.24 | 189.8% |
| AwanFS | seq-read | 4 KiB | 2 | 128 | 16,070 | 62.77 | 7.27 | 20.46 | 189.9% |
| AwanFS | seq-read | 4 KiB | 2 | 256 | 15,666 | 61.20 | 14.83 | 35.57 | 192.5% |
| AwanFS | seq-read | 4 KiB | 4 | 16 | 22,808 | 89.09 | 0.60 | 1.64 | 331.2% |
| AwanFS | seq-read | 4 KiB | 4 | 64 | 23,836 | 93.11 | 1.84 | 11.13 | 338.0% |
| AwanFS | seq-read | 4 KiB | 4 | 128 | 22,181 | 86.64 | 4.30 | 25.08 | 313.8% |
| AwanFS | seq-read | 4 KiB | 4 | 256 | 22,288 | 87.06 | 5.90 | 68.76 | 324.9% |
| AwanFS | seq-read | 4 KiB | 8 | 16 | 22,243 | 86.89 | 0.62 | 1.31 | 387.7% |
| AwanFS | seq-read | 4 KiB | 8 | 64 | 22,969 | 89.72 | 0.76 | 18.19 | 383.6% |
| AwanFS | seq-read | 4 KiB | 8 | 128 | 22,789 | 89.02 | 0.92 | 82.37 | 386.5% |
| AwanFS | seq-read | 4 KiB | 8 | 256 | 22,163 | 86.57 | 3.01 | 143.75 | 395.8% |
| AwanFS | seq-read | 4 KiB | 12 | 16 | 13,395 | 52.32 | 0.44 | 7.59 | 210.8% |
| AwanFS | seq-read | 4 KiB | 12 | 64 | 12,924 | 50.48 | 0.52 | 109.85 | 188.2% |
| AwanFS | seq-read | 4 KiB | 12 | 128 | 12,643 | 49.39 | 0.54 | 183.81 | 192.6% |
| AwanFS | seq-read | 4 KiB | 12 | 256 | 12,497 | 48.82 | 0.57 | 375.36 | 181.6% |
| AwanFS | seq-read | 64 KiB | 2 | 16 | 12,901 | 806.32 | 0.64 | 11.32 | 189.6% |
| AwanFS | seq-read | 64 KiB | 2 | 64 | 13,858 | 866.14 | 3.83 | 16.85 | 186.3% |
| AwanFS | seq-read | 64 KiB | 2 | 128 | 13,026 | 814.14 | 9.06 | 24.26 | 189.7% |
| AwanFS | seq-read | 64 KiB | 2 | 256 | 12,592 | 786.98 | 18.76 | 43.88 | 194.1% |
| AwanFS | seq-read | 64 KiB | 4 | 16 | 19,548 | 1221.74 | 0.72 | 1.90 | 349.8% |
| AwanFS | seq-read | 64 KiB | 4 | 64 | 19,056 | 1191.00 | 2.47 | 14.13 | 336.1% |
| AwanFS | seq-read | 64 KiB | 4 | 128 | 18,541 | 1158.84 | 5.09 | 32.07 | 323.9% |
| AwanFS | seq-read | 64 KiB | 4 | 256 | 17,437 | 1089.84 | 12.38 | 67.90 | 310.6% |
| AwanFS | seq-read | 64 KiB | 8 | 16 | 19,080 | 1192.52 | 0.72 | 1.79 | 390.7% |
| AwanFS | seq-read | 64 KiB | 8 | 64 | 19,127 | 1195.41 | 0.83 | 40.01 | 378.4% |
| AwanFS | seq-read | 64 KiB | 8 | 128 | 19,112 | 1194.48 | 0.79 | 116.07 | 407.9% |
| AwanFS | seq-read | 64 KiB | 8 | 256 | 18,418 | 1151.13 | 1.55 | 235.99 | 389.8% |
| AwanFS | seq-read | 64 KiB | 12 | 16 | 11,663 | 728.95 | 0.48 | 8.53 | 210.3% |
| AwanFS | seq-read | 64 KiB | 12 | 64 | 11,170 | 698.11 | 0.57 | 132.83 | 196.8% |
| AwanFS | seq-read | 64 KiB | 12 | 128 | 10,617 | 663.58 | 0.62 | 219.94 | 195.1% |
| AwanFS | seq-read | 64 KiB | 12 | 256 | 10,496 | 655.99 | 0.67 | 394.12 | 188.2% |
| AwanFS | seq-read | 256 KiB | 2 | 16 | 8,683 | 2170.77 | 1.00 | 14.88 | 188.1% |
| AwanFS | seq-read | 256 KiB | 2 | 64 | 8,440 | 2109.93 | 6.47 | 26.04 | 187.7% |
| AwanFS | seq-read | 256 KiB | 2 | 128 | 8,492 | 2122.90 | 14.09 | 36.15 | 190.3% |
| AwanFS | seq-read | 256 KiB | 2 | 256 | 8,381 | 2095.24 | 28.13 | 64.74 | 194.2% |
| AwanFS | seq-read | 256 KiB | 4 | 16 | 12,766 | 3191.39 | 0.98 | 4.51 | 366.8% |
| AwanFS | seq-read | 256 KiB | 4 | 64 | 12,337 | 3084.22 | 4.76 | 11.93 | 359.2% |
| AwanFS | seq-read | 256 KiB | 4 | 128 | 12,054 | 3013.48 | 10.08 | 22.07 | 349.7% |
| AwanFS | seq-read | 256 KiB | 4 | 256 | 11,475 | 2868.64 | 20.57 | 43.80 | 348.7% |
| AwanFS | seq-read | 256 KiB | 8 | 16 | 12,706 | 3176.54 | 1.07 | 3.30 | 443.1% |
| AwanFS | seq-read | 256 KiB | 8 | 64 | 11,655 | 2913.72 | 2.02 | 75.47 | 447.5% |
| AwanFS | seq-read | 256 KiB | 8 | 128 | 10,839 | 2709.75 | 5.20 | 133.66 | 437.2% |
| AwanFS | seq-read | 256 KiB | 8 | 256 | 10,579 | 2644.73 | 3.97 | 411.11 | 442.9% |
| AwanFS | seq-read | 256 KiB | 12 | 16 | 9,053 | 2263.33 | 0.68 | 12.13 | 246.9% |
| AwanFS | seq-read | 256 KiB | 12 | 64 | 8,725 | 2181.14 | 0.81 | 178.05 | 249.6% |
| AwanFS | seq-read | 256 KiB | 12 | 128 | 8,394 | 2098.56 | 0.88 | 253.45 | 234.8% |
| AwanFS | seq-read | 256 KiB | 12 | 256 | 8,265 | 2066.23 | 0.93 | 539.07 | 228.8% |
| AwanFS | seq-read | 1 MiB | 2 | 16 | 1,899 | 1899.47 | 6.92 | 31.51 | 184.8% |
| AwanFS | seq-read | 1 MiB | 2 | 64 | 1,924 | 1924.03 | 31.33 | 73.21 | 185.4% |
| AwanFS | seq-read | 1 MiB | 2 | 128 | 1,861 | 1860.82 | 65.33 | 150.75 | 186.8% |
| AwanFS | seq-read | 1 MiB | 2 | 256 | 1,853 | 1852.55 | 121.56 | 621.84 | 189.8% |
| AwanFS | seq-read | 1 MiB | 4 | 16 | 2,174 | 2174.44 | 6.99 | 18.51 | 279.3% |
| AwanFS | seq-read | 1 MiB | 4 | 64 | 2,213 | 2212.66 | 28.08 | 80.21 | 280.3% |
| AwanFS | seq-read | 1 MiB | 4 | 128 | 2,173 | 2173.25 | 57.23 | 168.68 | 279.1% |
| AwanFS | seq-read | 1 MiB | 4 | 256 | 2,062 | 2062.38 | 119.16 | 563.87 | 265.7% |
| AwanFS | seq-read | 1 MiB | 8 | 16 | 2,218 | 2218.00 | 6.78 | 15.27 | 315.1% |
| AwanFS | seq-read | 1 MiB | 8 | 64 | 2,216 | 2216.49 | 28.06 | 58.19 | 316.6% |
| AwanFS | seq-read | 1 MiB | 8 | 128 | 2,074 | 2073.59 | 60.68 | 124.48 | 295.1% |
| AwanFS | seq-read | 1 MiB | 8 | 256 | 2,001 | 2000.89 | 123.55 | 275.49 | 267.9% |
| AwanFS | seq-read | 1 MiB | 12 | 16 | 2,091 | 2090.96 | 6.05 | 55.73 | 281.9% |
| AwanFS | seq-read | 1 MiB | 12 | 64 | 1,991 | 1991.16 | 30.83 | 68.38 | 281.0% |
| AwanFS | seq-read | 1 MiB | 12 | 128 | 2,046 | 2046.23 | 60.46 | 124.45 | 272.0% |
| AwanFS | seq-read | 1 MiB | 12 | 256 | 1,986 | 1986.34 | 124.56 | 262.49 | 269.2% |
| AwanFS | seq-read | 4 MiB | 2 | 16 | 522 | 2087.15 | 27.56 | 84.89 | 188.8% |
| AwanFS | seq-read | 4 MiB | 2 | 64 | 498 | 1993.01 | 119.25 | 289.19 | 187.6% |
| AwanFS | seq-read | 4 MiB | 2 | 128 | 484 | 1935.71 | 238.49 | 646.71 | 189.6% |
| AwanFS | seq-read | 4 MiB | 2 | 256 | 464 | 1854.16 | 452.31 | 1548.50 | 189.6% |
| AwanFS | seq-read | 4 MiB | 4 | 16 | 550 | 2201.95 | 27.75 | 57.31 | 239.4% |
| AwanFS | seq-read | 4 MiB | 4 | 64 | 552 | 2206.82 | 112.93 | 190.86 | 236.8% |
| AwanFS | seq-read | 4 MiB | 4 | 128 | 547 | 2189.08 | 227.75 | 360.48 | 244.8% |
| AwanFS | seq-read | 4 MiB | 4 | 256 | 522 | 2089.29 | 471.85 | 1046.02 | 237.0% |
| AwanFS | seq-read | 4 MiB | 8 | 16 | 536 | 2142.33 | 28.96 | 43.89 | 250.2% |
| AwanFS | seq-read | 4 MiB | 8 | 64 | 539 | 2154.97 | 115.82 | 167.76 | 249.2% |
| AwanFS | seq-read | 4 MiB | 8 | 128 | 506 | 2023.21 | 244.44 | 361.54 | 253.2% |
| AwanFS | seq-read | 4 MiB | 8 | 256 | 506 | 2025.50 | 491.70 | 1013.32 | 249.6% |
| AwanFS | seq-read | 4 MiB | 12 | 16 | 545 | 2178.78 | 28.42 | 43.47 | 247.2% |
| AwanFS | seq-read | 4 MiB | 12 | 64 | 539 | 2157.58 | 115.78 | 169.98 | 250.6% |
| AwanFS | seq-read | 4 MiB | 12 | 128 | 514 | 2055.66 | 240.84 | 359.97 | 249.2% |
| AwanFS | seq-read | 4 MiB | 12 | 256 | 492 | 1966.68 | 501.24 | 712.36 | 248.1% |
| AwanFS (--durability fast) | seq-write | 4 KiB | 2 | 16 | 6,135 | 23.96 | 0.52 | 22.71 | 130.9% |
| AwanFS (--durability fast) | seq-write | 4 KiB | 2 | 64 | 9,044 | 35.33 | 5.77 | 25.70 | 185.6% |
| AwanFS (--durability fast) | seq-write | 4 KiB | 2 | 128 | 8,933 | 34.89 | 12.83 | 51.35 | 186.6% |
| AwanFS (--durability fast) | seq-write | 4 KiB | 2 | 256 | 8,900 | 34.77 | 24.48 | 92.20 | 186.6% |
| AwanFS (--durability fast) | seq-write | 4 KiB | 4 | 16 | 9,606 | 37.52 | 0.65 | 6.41 | 254.8% |
| AwanFS (--durability fast) | seq-write | 4 KiB | 4 | 64 | 14,154 | 55.29 | 3.86 | 10.45 | 343.4% |
| AwanFS (--durability fast) | seq-write | 4 KiB | 4 | 128 | 13,103 | 51.18 | 7.97 | 23.12 | 320.2% |
| AwanFS (--durability fast) | seq-write | 4 KiB | 4 | 256 | 13,962 | 54.54 | 14.95 | 92.04 | 346.6% |
| AwanFS (--durability fast) | seq-write | 4 KiB | 8 | 16 | 8,753 | 34.19 | 0.79 | 2.69 | 253.4% |
| AwanFS (--durability fast) | seq-write | 4 KiB | 8 | 64 | 16,232 | 63.41 | 3.43 | 8.11 | 417.0% |
| AwanFS (--durability fast) | seq-write | 4 KiB | 8 | 128 | 16,110 | 62.93 | 6.98 | 18.22 | 430.8% |
| AwanFS (--durability fast) | seq-write | 4 KiB | 8 | 256 | 16,132 | 63.02 | 12.91 | 97.59 | 422.5% |
| AwanFS (--durability fast) | seq-write | 4 KiB | 12 | 16 | 6,754 | 26.38 | 0.69 | 6.49 | 182.9% |
| AwanFS (--durability fast) | seq-write | 4 KiB | 12 | 64 | 11,505 | 44.94 | 0.75 | 61.74 | 320.9% |
| AwanFS (--durability fast) | seq-write | 4 KiB | 12 | 128 | 11,702 | 45.71 | 0.75 | 158.45 | 337.4% |
| AwanFS (--durability fast) | seq-write | 4 KiB | 12 | 256 | 11,147 | 43.54 | 6.51 | 266.73 | 323.7% |
| AwanFS (--durability fast) | seq-write | 64 KiB | 2 | 16 | 2,239 | 139.97 | 5.61 | 28.99 | 172.0% |
| AwanFS (--durability fast) | seq-write | 64 KiB | 2 | 64 | 1,262 | 78.89 | 48.39 | 108.93 | 142.4% |
| AwanFS (--durability fast) | seq-write | 64 KiB | 2 | 128 | 1,359 | 84.92 | 86.69 | 243.30 | 146.4% |
| AwanFS (--durability fast) | seq-write | 64 KiB | 2 | 256 | 1,358 | 84.87 | 134.76 | 911.15 | 134.1% |
| AwanFS (--durability fast) | seq-write | 64 KiB | 4 | 16 | 2,444 | 152.76 | 5.90 | 19.50 | 206.0% |
| AwanFS (--durability fast) | seq-write | 64 KiB | 4 | 64 | 1,297 | 81.04 | 47.47 | 100.22 | 152.0% |
| AwanFS (--durability fast) | seq-write | 64 KiB | 4 | 128 | 1,451 | 90.66 | 81.85 | 210.46 | 152.2% |
| AwanFS (--durability fast) | seq-write | 64 KiB | 4 | 256 | 1,424 | 89.00 | 131.66 | 1012.05 | 145.9% |
| AwanFS (--durability fast) | seq-write | 64 KiB | 8 | 16 | 2,466 | 154.15 | 5.58 | 20.38 | 231.2% |
| AwanFS (--durability fast) | seq-write | 64 KiB | 8 | 64 | 1,288 | 80.49 | 47.64 | 102.79 | 149.7% |
| AwanFS (--durability fast) | seq-write | 64 KiB | 8 | 128 | 1,433 | 89.55 | 83.49 | 217.68 | 153.9% |
| AwanFS (--durability fast) | seq-write | 64 KiB | 8 | 256 | 1,531 | 95.70 | 129.85 | 752.76 | 160.6% |
| AwanFS (--durability fast) | seq-write | 64 KiB | 12 | 16 | 2,521 | 157.55 | 5.68 | 18.97 | 214.8% |
| AwanFS (--durability fast) | seq-write | 64 KiB | 12 | 64 | 1,246 | 77.88 | 49.11 | 103.47 | 150.6% |
| AwanFS (--durability fast) | seq-write | 64 KiB | 12 | 128 | 1,380 | 86.26 | 85.35 | 223.16 | 154.5% |
| AwanFS (--durability fast) | seq-write | 64 KiB | 12 | 256 | 1,470 | 91.86 | 140.40 | 549.64 | 162.3% |
| AwanFS (--durability fast) | seq-write | 256 KiB | 2 | 16 | 623 | 155.64 | 21.08 | 54.07 | 139.3% |
| AwanFS (--durability fast) | seq-write | 256 KiB | 2 | 64 | 1,048 | 262.12 | 55.18 | 182.11 | 169.6% |
| AwanFS (--durability fast) | seq-write | 256 KiB | 2 | 128 | 917 | 229.21 | 89.51 | 1289.71 | 129.3% |
| AwanFS (--durability fast) | seq-write | 256 KiB | 2 | 256 | 894 | 223.56 | 146.03 | 2278.05 | 122.5% |
| AwanFS (--durability fast) | seq-write | 256 KiB | 4 | 16 | 884 | 220.97 | 17.05 | 40.26 | 189.6% |
| AwanFS (--durability fast) | seq-write | 256 KiB | 4 | 64 | 1,561 | 390.33 | 33.70 | 229.16 | 238.5% |
| AwanFS (--durability fast) | seq-write | 256 KiB | 4 | 128 | 1,190 | 297.53 | 53.21 | 2068.07 | 176.6% |
| AwanFS (--durability fast) | seq-write | 256 KiB | 4 | 256 | 827 | 206.82 | 79.19 | 3539.66 | 121.8% |
| AwanFS (--durability fast) | seq-write | 256 KiB | 8 | 16 | 666 | 166.41 | 17.01 | 788.86 | 142.5% |
| AwanFS (--durability fast) | seq-write | 256 KiB | 8 | 64 | 1,543 | 385.80 | 36.64 | 157.80 | 257.7% |
| AwanFS (--durability fast) | seq-write | 256 KiB | 8 | 128 | 1,456 | 363.98 | 52.38 | 1851.99 | 221.6% |
| AwanFS (--durability fast) | seq-write | 256 KiB | 8 | 256 | 2,181 | 545.25 | 71.90 | 712.57 | 344.5% |
| AwanFS (--durability fast) | seq-write | 256 KiB | 12 | 16 | 850 | 212.62 | 17.56 | 44.08 | 186.2% |
| AwanFS (--durability fast) | seq-write | 256 KiB | 12 | 64 | 1,653 | 413.37 | 33.79 | 155.49 | 269.6% |
| AwanFS (--durability fast) | seq-write | 256 KiB | 12 | 128 | 943 | 235.82 | 49.43 | 2802.68 | 152.6% |
| AwanFS (--durability fast) | seq-write | 256 KiB | 12 | 256 | 1,149 | 287.15 | 76.62 | 3435.97 | 181.4% |
| AwanFS (--durability fast) | seq-write | 1 MiB | 2 | 16 | 226 | 225.85 | 46.34 | 1337.60 | 131.3% |
| AwanFS (--durability fast) | seq-write | 1 MiB | 2 | 64 | 295 | 295.25 | 194.68 | 500.47 | 180.6% |
| AwanFS (--durability fast) | seq-write | 1 MiB | 2 | 128 | 197 | 196.67 | 469.16 | 2585.30 | 122.5% |
| AwanFS (--durability fast) | seq-write | 1 MiB | 2 | 256 | 157 | 157.21 | 1089.34 | 5445.83 | 100.6% |
| AwanFS (--durability fast) | seq-write | 1 MiB | 4 | 16 | 174 | 173.58 | 30.01 | 2195.17 | 119.8% |
| AwanFS (--durability fast) | seq-write | 1 MiB | 4 | 64 | 141 | 140.77 | 152.41 | 2907.07 | 99.3% |
| AwanFS (--durability fast) | seq-write | 1 MiB | 4 | 128 | 186 | 186.27 | 306.83 | 3640.41 | 133.1% |
| AwanFS (--durability fast) | seq-write | 1 MiB | 4 | 256 | 130 | 129.52 | 891.72 | 6957.93 | 89.5% |
| AwanFS (--durability fast) | seq-write | 1 MiB | 8 | 16 | 440 | 440.47 | 20.01 | 339.41 | 401.8% |
| AwanFS (--durability fast) | seq-write | 1 MiB | 8 | 64 | 209 | 209.26 | 86.32 | 3967.23 | 199.5% |
| AwanFS (--durability fast) | seq-write | 1 MiB | 8 | 128 | 158 | 158.40 | 195.01 | 5258.87 | 143.7% |
| AwanFS (--durability fast) | seq-write | 1 MiB | 8 | 256 | 150 | 149.57 | 537.94 | 7422.91 | 136.8% |
| AwanFS (--durability fast) | seq-write | 1 MiB | 12 | 16 | 213 | 213.17 | 19.34 | 4024.02 | 202.9% |
| AwanFS (--durability fast) | seq-write | 1 MiB | 12 | 64 | 173 | 172.89 | 83.92 | 3487.97 | 191.9% |
| AwanFS (--durability fast) | seq-write | 1 MiB | 12 | 128 | 214 | 213.67 | 175.35 | 4190.80 | 206.7% |
| AwanFS (--durability fast) | seq-write | 1 MiB | 12 | 256 | 158 | 158.07 | 415.81 | 6385.65 | 156.8% |
| AwanFS (--durability fast) | seq-write | 4 MiB | 2 | 16 | 33 | 132.46 | 241.71 | 2519.18 | 68.2% |
| AwanFS (--durability fast) | seq-write | 4 MiB | 2 | 64 | 35 | 140.96 | 1207.50 | 4783.55 | 74.3% |
| AwanFS (--durability fast) | seq-write | 4 MiB | 2 | 128 | 31 | 125.74 | 2790.96 | 8178.99 | 65.5% |
| AwanFS (--durability fast) | seq-write | 4 MiB | 2 | 256 | 43 | 172.86 | 4793.05 | 8893.59 | 91.3% |
| AwanFS (--durability fast) | seq-write | 4 MiB | 4 | 16 | 24 | 94.13 | 477.22 | 4378.84 | 54.0% |
| AwanFS (--durability fast) | seq-write | 4 MiB | 4 | 64 | 25 | 100.69 | 2146.35 | 6789.46 | 57.0% |
| AwanFS (--durability fast) | seq-write | 4 MiB | 4 | 128 | 86 | 343.17 | 1340.20 | 4734.90 | 223.1% |
| AwanFS (--durability fast) | seq-write | 4 MiB | 4 | 256 | 42 | 166.93 | 3855.30 | 10180.60 | 104.3% |
| AwanFS (--durability fast) | seq-write | 4 MiB | 8 | 16 | 26 | 103.66 | 255.55 | 3158.17 | 61.0% |
| AwanFS (--durability fast) | seq-write | 4 MiB | 8 | 64 | 34 | 136.50 | 1118.65 | 5082.22 | 91.6% |
| AwanFS (--durability fast) | seq-write | 4 MiB | 8 | 128 | 37 | 148.22 | 3447.10 | 6406.49 | 93.3% |
| AwanFS (--durability fast) | seq-write | 4 MiB | 8 | 256 | 30 | 120.10 | 5673.54 | 12637.04 | 76.2% |
| AwanFS (--durability fast) | seq-write | 4 MiB | 12 | 16 | 22 | 87.30 | 1127.44 | 3317.16 | 50.6% |
| AwanFS (--durability fast) | seq-write | 4 MiB | 12 | 64 | 24 | 96.47 | 1847.41 | 7364.98 | 62.3% |
| AwanFS (--durability fast) | seq-write | 4 MiB | 12 | 128 | 92 | 366.78 | 891.85 | 4079.65 | 342.5% |
| AwanFS (--durability fast) | seq-write | 4 MiB | 12 | 256 | 44 | 175.86 | 2786.52 | 11592.78 | 134.7% |

## AwanFS work per phase (`/_awanfs/metrics`, before and after each phase)

Syncs and disk bytes are the server's own counters; rates use the phase's wall time. PUTs per sync is ops/s over syncs/s.

| Durability | Pattern | Size | Cores | Concurrency | ops/s | syncs/s | PUTs per sync | MiB per sync | disk MiB/s | peak in-flight MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| group | seq-write | 4 KiB | 2 | 16 | 4,876 | 1080.8 | 4.5 | 0.03 | 32.0 | 4.0 |
| group | seq-write | 4 KiB | 2 | 64 | 6,216 | 834.0 | 7.5 | 0.05 | 40.4 | 4.0 |
| group | seq-write | 4 KiB | 2 | 128 | 5,967 | 900.1 | 6.6 | 0.04 | 38.7 | 4.0 |
| group | seq-write | 4 KiB | 2 | 256 | 5,845 | 921.2 | 6.3 | 0.04 | 38.1 | 4.0 |
| group | seq-write | 4 KiB | 4 | 16 | 5,241 | 1128.7 | 4.6 | 0.03 | 33.9 | 4.0 |
| group | seq-write | 4 KiB | 4 | 64 | 8,461 | 571.6 | 14.8 | 0.09 | 53.2 | 4.0 |
| group | seq-write | 4 KiB | 4 | 128 | 10,025 | 553.0 | 18.1 | 0.12 | 64.8 | 4.0 |
| group | seq-write | 4 KiB | 4 | 256 | 10,609 | 622.9 | 17.0 | 0.11 | 68.4 | 4.0 |
| group | seq-write | 4 KiB | 8 | 16 | 6,255 | 1346.7 | 4.6 | 0.03 | 40.6 | 4.0 |
| group | seq-write | 4 KiB | 8 | 64 | 11,211 | 709.4 | 15.8 | 0.10 | 71.3 | 4.0 |
| group | seq-write | 4 KiB | 8 | 128 | 12,313 | 450.4 | 27.3 | 0.17 | 77.2 | 4.0 |
| group | seq-write | 4 KiB | 8 | 256 | 12,784 | 423.6 | 30.2 | 0.19 | 82.2 | 4.0 |
| group | seq-write | 4 KiB | 12 | 16 | 5,425 | 1112.4 | 4.9 | 0.03 | 33.3 | 4.0 |
| group | seq-write | 4 KiB | 12 | 64 | 11,217 | 798.4 | 14.0 | 0.09 | 69.5 | 4.0 |
| group | seq-write | 4 KiB | 12 | 128 | 10,548 | 608.9 | 17.4 | 0.11 | 64.7 | 4.0 |
| group | seq-write | 4 KiB | 12 | 256 | 8,948 | 418.6 | 27.0 | 0.17 | 54.8 | 4.0 |
| group | seq-write | 64 KiB | 2 | 16 | 1,874 | 562.5 | 3.3 | 0.19 | 104.6 | 4.0 |
| group | seq-write | 64 KiB | 2 | 64 | 1,220 | 568.5 | 2.1 | 0.12 | 67.8 | 4.0 |
| group | seq-write | 64 KiB | 2 | 128 | 1,325 | 641.1 | 2.1 | 0.12 | 73.9 | 8.0 |
| group | seq-write | 64 KiB | 2 | 256 | 1,394 | 557.7 | 2.7 | 0.15 | 78.1 | 15.5 |
| group | seq-write | 64 KiB | 4 | 16 | 2,070 | 876.0 | 2.4 | 0.13 | 113.8 | 4.0 |
| group | seq-write | 64 KiB | 4 | 64 | 1,264 | 547.6 | 2.3 | 0.12 | 68.3 | 4.0 |
| group | seq-write | 64 KiB | 4 | 128 | 1,394 | 651.8 | 2.1 | 0.12 | 76.6 | 7.1 |
| group | seq-write | 64 KiB | 4 | 256 | 1,475 | 732.0 | 2.0 | 0.11 | 81.1 | 11.7 |
| group | seq-write | 64 KiB | 8 | 16 | 2,044 | 928.4 | 2.2 | 0.12 | 112.6 | 4.0 |
| group | seq-write | 64 KiB | 8 | 64 | 1,272 | 575.6 | 2.2 | 0.12 | 70.1 | 4.0 |
| group | seq-write | 64 KiB | 8 | 128 | 1,390 | 665.1 | 2.1 | 0.12 | 76.6 | 7.3 |
| group | seq-write | 64 KiB | 8 | 256 | 1,478 | 747.0 | 2.0 | 0.11 | 81.3 | 12.6 |
| group | seq-write | 64 KiB | 12 | 16 | 1,955 | 826.0 | 2.4 | 0.12 | 102.4 | 4.0 |
| group | seq-write | 64 KiB | 12 | 64 | 1,365 | 595.4 | 2.3 | 0.12 | 71.6 | 4.0 |
| group | seq-write | 64 KiB | 12 | 128 | 1,533 | 723.9 | 2.1 | 0.11 | 80.6 | 7.1 |
| group | seq-write | 64 KiB | 12 | 256 | 1,689 | 859.5 | 2.0 | 0.10 | 87.9 | 13.5 |
| group | seq-write | 256 KiB | 2 | 16 | 615 | 343.8 | 1.8 | 0.38 | 130.7 | 15.5 |
| group | seq-write | 256 KiB | 2 | 64 | 754 | 273.0 | 2.9 | 0.62 | 161.0 | 16.0 |
| group | seq-write | 256 KiB | 2 | 128 | 822 | 254.7 | 7.7 | 1.64 | 175.4 | 31.9 |
| group | seq-write | 256 KiB | 2 | 256 | 975 | 309.1 | 3.5 | 0.76 | 209.1 | 57.1 |
| group | seq-write | 256 KiB | 4 | 16 | 718 | 534.0 | 1.4 | 0.29 | 150.8 | 11.7 |
| group | seq-write | 256 KiB | 4 | 64 | 1,205 | 572.6 | 2.1 | 0.45 | 254.4 | 16.0 |
| group | seq-write | 256 KiB | 4 | 128 | 690 | 74.5 | 10.6 | 2.24 | 145.9 | 32.0 |
| group | seq-write | 256 KiB | 4 | 256 | 1,138 | 237.4 | 4.8 | 1.02 | 240.7 | 64.0 |
| group | seq-write | 256 KiB | 8 | 16 | 485 | 190.5 | 2.6 | 0.54 | 100.9 | 12.6 |
| group | seq-write | 256 KiB | 8 | 64 | 1,136 | 650.1 | 1.7 | 0.37 | 239.4 | 16.0 |
| group | seq-write | 256 KiB | 8 | 128 | 679 | 50.6 | 13.4 | 2.84 | 143.5 | 32.0 |
| group | seq-write | 256 KiB | 8 | 256 | 1,169 | 311.6 | 3.8 | 0.79 | 247.2 | 64.0 |
| group | seq-write | 256 KiB | 12 | 16 | 695 | 474.2 | 1.6 | 0.32 | 138.3 | 13.5 |
| group | seq-write | 256 KiB | 12 | 64 | 1,413 | 723.0 | 1.9 | 0.39 | 281.5 | 16.0 |
| group | seq-write | 256 KiB | 12 | 128 | 887 | 195.5 | 8.6 | 1.73 | 178.7 | 32.0 |
| group | seq-write | 256 KiB | 12 | 256 | 845 | 157.5 | 13.1 | 2.65 | 170.7 | 64.0 |
| group | seq-write | 1 MiB | 2 | 16 | 187 | 107.5 | 1.8 | 1.48 | 157.9 | 57.1 |
| group | seq-write | 1 MiB | 2 | 64 | 146 | 47.6 | 3.3 | 2.78 | 123.6 | 64.0 |
| group | seq-write | 1 MiB | 2 | 128 | 101 | 13.6 | 7.9 | 6.88 | 87.0 | 128.0 |
| group | seq-write | 1 MiB | 2 | 256 | 92 | 10.4 | 10.6 | 9.39 | 80.5 | 256.0 |
| group | seq-write | 1 MiB | 4 | 16 | 146 | 42.6 | 3.4 | 2.86 | 122.0 | 64.0 |
| group | seq-write | 1 MiB | 4 | 64 | 167 | 43.0 | 6.1 | 5.15 | 141.5 | 64.0 |
| group | seq-write | 1 MiB | 4 | 128 | 95 | 7.5 | 12.5 | 10.77 | 80.8 | 128.0 |
| group | seq-write | 1 MiB | 4 | 256 | 128 | 25.5 | 9.3 | 8.29 | 108.9 | 255.0 |
| group | seq-write | 1 MiB | 8 | 16 | 139 | 38.5 | 3.6 | 3.02 | 116.4 | 64.0 |
| group | seq-write | 1 MiB | 8 | 64 | 227 | 47.1 | 4.8 | 4.02 | 190.6 | 64.0 |
| group | seq-write | 1 MiB | 8 | 128 | 103 | 7.9 | 12.8 | 10.98 | 87.4 | 128.0 |
| group | seq-write | 1 MiB | 8 | 256 | 134 | 19.0 | 10.4 | 9.08 | 113.4 | 256.0 |
| group | seq-write | 1 MiB | 12 | 16 | 216 | 74.5 | 3.2 | 2.52 | 171.5 | 64.0 |
| group | seq-write | 1 MiB | 12 | 64 | 212 | 37.6 | 6.0 | 4.83 | 169.5 | 64.0 |
| group | seq-write | 1 MiB | 12 | 128 | 139 | 10.1 | 13.7 | 11.02 | 111.6 | 128.0 |
| group | seq-write | 1 MiB | 12 | 256 | 173 | 23.6 | 10.7 | 8.97 | 143.8 | 256.0 |
| group | seq-write | 4 MiB | 2 | 16 | 20 | 7.4 | 2.8 | 9.54 | 68.6 | 256.0 |
| group | seq-write | 4 MiB | 2 | 64 | 31 | 14.7 | 3.2 | 11.51 | 105.5 | 256.0 |
| group | seq-write | 4 MiB | 2 | 128 | 39 | 18.6 | 2.1 | 7.54 | 140.2 | 512.0 |
| group | seq-write | 4 MiB | 2 | 256 | 63 | 50.4 | 1.5 | 5.33 | 224.7 | 1022.0 |
| group | seq-write | 4 MiB | 4 | 16 | 91 | 65.6 | 1.4 | 4.72 | 303.0 | 255.0 |
| group | seq-write | 4 MiB | 4 | 64 | 33 | 7.6 | 4.3 | 14.78 | 112.5 | 256.0 |
| group | seq-write | 4 MiB | 4 | 128 | 38 | 14.7 | 3.3 | 11.60 | 135.0 | 512.0 |
| group | seq-write | 4 MiB | 4 | 256 | 54 | 23.1 | 2.3 | 8.05 | 186.1 | 1024.0 |
| group | seq-write | 4 MiB | 8 | 16 | 20 | 7.1 | 3.2 | 10.53 | 65.9 | 256.0 |
| group | seq-write | 4 MiB | 8 | 64 | 18 | 4.4 | 3.9 | 14.09 | 64.1 | 256.0 |
| group | seq-write | 4 MiB | 8 | 128 | 21 | 6.2 | 3.1 | 11.27 | 75.5 | 512.0 |
| group | seq-write | 4 MiB | 8 | 256 | 93 | 49.3 | 2.4 | 8.62 | 325.1 | 1024.0 |
| group | seq-write | 4 MiB | 12 | 16 | 33 | 12.0 | 2.7 | 8.67 | 103.6 | 256.0 |
| group | seq-write | 4 MiB | 12 | 64 | 38 | 15.3 | 3.5 | 11.45 | 123.9 | 256.0 |
| group | seq-write | 4 MiB | 12 | 128 | 49 | 22.7 | 2.2 | 7.58 | 168.3 | 512.0 |
| group | seq-write | 4 MiB | 12 | 256 | 40 | 19.4 | 2.0 | 7.01 | 135.7 | 1024.0 |
| group | seq-read | 4 KiB | 2 | 16 | 15,431 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 4 KiB | 2 | 64 | 17,347 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 4 KiB | 2 | 128 | 16,070 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 4 KiB | 2 | 256 | 15,666 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 4 KiB | 4 | 16 | 22,808 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 4 KiB | 4 | 64 | 23,836 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 4 KiB | 4 | 128 | 22,181 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 4 KiB | 4 | 256 | 22,288 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 4 KiB | 8 | 16 | 22,243 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 4 KiB | 8 | 64 | 22,969 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 4 KiB | 8 | 128 | 22,789 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 4 KiB | 8 | 256 | 22,163 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 4 KiB | 12 | 16 | 13,395 | 0.3 | 22137.0 | 0.00 | 0.0 | 1022.0 |
| group | seq-read | 4 KiB | 12 | 64 | 12,924 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 4 KiB | 12 | 128 | 12,643 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 4 KiB | 12 | 256 | 12,497 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 64 KiB | 2 | 16 | 12,901 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 64 KiB | 2 | 64 | 13,858 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 64 KiB | 2 | 128 | 13,026 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 64 KiB | 2 | 256 | 12,592 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 64 KiB | 4 | 16 | 19,548 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 64 KiB | 4 | 64 | 19,056 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 64 KiB | 4 | 128 | 18,541 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 64 KiB | 4 | 256 | 17,437 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 64 KiB | 8 | 16 | 19,080 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 64 KiB | 8 | 64 | 19,127 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 64 KiB | 8 | 128 | 19,112 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 64 KiB | 8 | 256 | 18,418 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 64 KiB | 12 | 16 | 11,663 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 64 KiB | 12 | 64 | 11,170 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 64 KiB | 12 | 128 | 10,617 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 64 KiB | 12 | 256 | 10,496 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 256 KiB | 2 | 16 | 8,683 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 256 KiB | 2 | 64 | 8,440 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 256 KiB | 2 | 128 | 8,492 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 256 KiB | 2 | 256 | 8,381 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 256 KiB | 4 | 16 | 12,766 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 256 KiB | 4 | 64 | 12,337 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 256 KiB | 4 | 128 | 12,054 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 256 KiB | 4 | 256 | 11,475 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 256 KiB | 8 | 16 | 12,706 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 256 KiB | 8 | 64 | 11,655 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 256 KiB | 8 | 128 | 10,839 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 256 KiB | 8 | 256 | 10,579 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 256 KiB | 12 | 16 | 9,053 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 256 KiB | 12 | 64 | 8,725 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 256 KiB | 12 | 128 | 8,394 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 256 KiB | 12 | 256 | 8,265 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 1 MiB | 2 | 16 | 1,899 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 1 MiB | 2 | 64 | 1,924 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 1 MiB | 2 | 128 | 1,861 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 1 MiB | 2 | 256 | 1,853 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 1 MiB | 4 | 16 | 2,174 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 1 MiB | 4 | 64 | 2,213 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 1 MiB | 4 | 128 | 2,173 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 1 MiB | 4 | 256 | 2,062 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 1 MiB | 8 | 16 | 2,218 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 1 MiB | 8 | 64 | 2,216 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 1 MiB | 8 | 128 | 2,074 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 1 MiB | 8 | 256 | 2,001 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 1 MiB | 12 | 16 | 2,091 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 1 MiB | 12 | 64 | 1,991 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 1 MiB | 12 | 128 | 2,046 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 1 MiB | 12 | 256 | 1,986 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 4 MiB | 2 | 16 | 522 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 4 MiB | 2 | 64 | 498 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 4 MiB | 2 | 128 | 484 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 4 MiB | 2 | 256 | 464 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 4 MiB | 4 | 16 | 550 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 4 MiB | 4 | 64 | 552 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 4 MiB | 4 | 128 | 547 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 4 MiB | 4 | 256 | 522 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 4 MiB | 8 | 16 | 536 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 4 MiB | 8 | 64 | 539 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 4 MiB | 8 | 128 | 506 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 4 MiB | 8 | 256 | 506 | 0.0 | no syncs | no syncs | 0.0 | 1024.0 |
| group | seq-read | 4 MiB | 12 | 16 | 545 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 4 MiB | 12 | 64 | 539 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 4 MiB | 12 | 128 | 514 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| group | seq-read | 4 MiB | 12 | 256 | 492 | 0.0 | no syncs | no syncs | 0.0 | 1022.0 |
| fast | seq-write | 4 KiB | 2 | 16 | 6,135 | 1.0 | 6247.8 | 40.33 | 39.6 | 0.1 |
| fast | seq-write | 4 KiB | 2 | 64 | 9,044 | 1.6 | 5531.9 | 35.78 | 58.5 | 0.1 |
| fast | seq-write | 4 KiB | 2 | 128 | 8,933 | 1.8 | 5067.4 | 32.63 | 57.5 | 0.1 |
| fast | seq-write | 4 KiB | 2 | 256 | 8,900 | 1.8 | 5010.6 | 32.41 | 57.5 | 0.1 |
| fast | seq-write | 4 KiB | 4 | 16 | 9,606 | 1.0 | 9232.9 | 58.93 | 61.4 | 0.1 |
| fast | seq-write | 4 KiB | 4 | 64 | 14,154 | 1.6 | 8680.3 | 53.61 | 87.4 | 0.1 |
| fast | seq-write | 4 KiB | 4 | 128 | 13,103 | 1.5 | 8997.9 | 55.68 | 81.0 | 0.1 |
| fast | seq-write | 4 KiB | 4 | 256 | 13,962 | 1.9 | 7417.3 | 46.43 | 87.4 | 0.2 |
| fast | seq-write | 4 KiB | 8 | 16 | 8,753 | 0.8 | 11488.9 | 73.00 | 55.7 | 0.1 |
| fast | seq-write | 4 KiB | 8 | 64 | 16,232 | 1.7 | 9495.7 | 59.35 | 101.4 | 0.1 |
| fast | seq-write | 4 KiB | 8 | 128 | 16,110 | 1.7 | 9343.9 | 58.92 | 101.5 | 0.1 |
| fast | seq-write | 4 KiB | 8 | 256 | 16,132 | 1.4 | 11544.8 | 72.33 | 101.1 | 0.2 |
| fast | seq-write | 4 KiB | 12 | 16 | 6,754 | 0.8 | 10491.0 | 64.50 | 41.4 | 0.1 |
| fast | seq-write | 4 KiB | 12 | 64 | 11,505 | 1.7 | 7055.2 | 43.57 | 71.1 | 0.1 |
| fast | seq-write | 4 KiB | 12 | 128 | 11,702 | 0.9 | 13011.9 | 80.23 | 72.2 | 0.2 |
| fast | seq-write | 4 KiB | 12 | 256 | 11,147 | 1.3 | 8467.6 | 52.01 | 68.5 | 0.4 |
| fast | seq-write | 64 KiB | 2 | 16 | 2,239 | 1.6 | 1363.0 | 75.07 | 123.3 | 1.0 |
| fast | seq-write | 64 KiB | 2 | 64 | 1,262 | 1.8 | 706.6 | 39.12 | 69.8 | 2.8 |
| fast | seq-write | 64 KiB | 2 | 128 | 1,359 | 1.9 | 700.6 | 38.57 | 74.8 | 6.6 |
| fast | seq-write | 64 KiB | 2 | 256 | 1,358 | 1.4 | 961.5 | 53.40 | 75.4 | 7.0 |
| fast | seq-write | 64 KiB | 4 | 16 | 2,444 | 1.5 | 1689.8 | 92.33 | 133.6 | 1.0 |
| fast | seq-write | 64 KiB | 4 | 64 | 1,297 | 2.0 | 664.3 | 36.50 | 71.2 | 3.3 |
| fast | seq-write | 64 KiB | 4 | 128 | 1,451 | 1.9 | 747.0 | 41.29 | 80.2 | 6.8 |
| fast | seq-write | 64 KiB | 4 | 256 | 1,424 | 1.6 | 914.0 | 50.57 | 78.8 | 7.5 |
| fast | seq-write | 64 KiB | 8 | 16 | 2,466 | 2.0 | 1249.2 | 68.91 | 136.1 | 1.0 |
| fast | seq-write | 64 KiB | 8 | 64 | 1,288 | 2.0 | 654.0 | 36.27 | 71.4 | 3.2 |
| fast | seq-write | 64 KiB | 8 | 128 | 1,433 | 1.6 | 877.6 | 48.82 | 79.7 | 4.8 |
| fast | seq-write | 64 KiB | 8 | 256 | 1,531 | 1.8 | 877.5 | 48.82 | 85.2 | 7.3 |
| fast | seq-write | 64 KiB | 12 | 16 | 2,521 | 1.9 | 1347.3 | 70.64 | 132.2 | 1.0 |
| fast | seq-write | 64 KiB | 12 | 64 | 1,246 | 1.7 | 733.2 | 38.58 | 65.6 | 2.6 |
| fast | seq-write | 64 KiB | 12 | 128 | 1,380 | 1.8 | 749.4 | 39.35 | 72.5 | 4.9 |
| fast | seq-write | 64 KiB | 12 | 256 | 1,470 | 2.0 | 750.2 | 39.63 | 77.6 | 9.7 |
| fast | seq-write | 256 KiB | 2 | 16 | 623 | 1.3 | 483.2 | 102.19 | 131.6 | 7.0 |
| fast | seq-write | 256 KiB | 2 | 64 | 1,048 | 1.5 | 719.8 | 152.72 | 222.5 | 14.0 |
| fast | seq-write | 256 KiB | 2 | 128 | 917 | 1.1 | 849.7 | 179.32 | 193.4 | 30.2 |
| fast | seq-write | 256 KiB | 2 | 256 | 894 | 0.9 | 958.7 | 204.57 | 189.9 | 33.5 |
| fast | seq-write | 256 KiB | 4 | 16 | 884 | 1.6 | 535.9 | 113.31 | 186.9 | 7.5 |
| fast | seq-write | 256 KiB | 4 | 64 | 1,561 | 1.5 | 1081.0 | 226.58 | 327.2 | 13.1 |
| fast | seq-write | 256 KiB | 4 | 128 | 1,190 | 0.9 | 1390.0 | 296.34 | 254.9 | 30.2 |
| fast | seq-write | 256 KiB | 4 | 256 | 827 | 0.6 | 1426.3 | 305.80 | 176.8 | 34.0 |
| fast | seq-write | 256 KiB | 8 | 16 | 666 | 1.4 | 488.1 | 105.41 | 142.7 | 7.3 |
| fast | seq-write | 256 KiB | 8 | 64 | 1,543 | 1.5 | 1073.5 | 228.17 | 328.1 | 14.5 |
| fast | seq-write | 256 KiB | 8 | 128 | 1,456 | 1.2 | 1140.2 | 244.31 | 311.1 | 29.0 |
| fast | seq-write | 256 KiB | 8 | 256 | 2,181 | 1.3 | 1682.4 | 356.41 | 462.0 | 30.4 |
| fast | seq-write | 256 KiB | 12 | 16 | 850 | 1.6 | 573.2 | 114.73 | 170.3 | 9.7 |
| fast | seq-write | 256 KiB | 12 | 64 | 1,653 | 1.5 | 1069.5 | 214.58 | 331.7 | 12.4 |
| fast | seq-write | 256 KiB | 12 | 128 | 943 | 0.9 | 1064.3 | 222.34 | 197.2 | 23.5 |
| fast | seq-write | 256 KiB | 12 | 256 | 1,149 | 0.8 | 1360.0 | 279.42 | 235.3 | 47.8 |
| fast | seq-write | 1 MiB | 2 | 16 | 226 | 1.0 | 226.4 | 193.08 | 192.6 | 33.5 |
| fast | seq-write | 1 MiB | 2 | 64 | 295 | 1.3 | 233.7 | 196.51 | 248.3 | 63.0 |
| fast | seq-write | 1 MiB | 2 | 128 | 197 | 1.1 | 174.1 | 151.34 | 169.4 | 127.5 |
| fast | seq-write | 1 MiB | 2 | 256 | 157 | 0.9 | 177.8 | 155.48 | 137.2 | 242.5 |
| fast | seq-write | 1 MiB | 4 | 16 | 174 | 0.7 | 226.5 | 194.38 | 149.0 | 34.0 |
| fast | seq-write | 1 MiB | 4 | 64 | 141 | 0.7 | 201.8 | 179.11 | 124.9 | 64.0 |
| fast | seq-write | 1 MiB | 4 | 128 | 186 | 0.7 | 274.5 | 237.72 | 161.5 | 128.0 |
| fast | seq-write | 1 MiB | 4 | 256 | 130 | 0.5 | 257.1 | 224.12 | 112.3 | 230.0 |
| fast | seq-write | 1 MiB | 8 | 16 | 440 | 1.1 | 426.3 | 358.57 | 371.2 | 30.4 |
| fast | seq-write | 1 MiB | 8 | 64 | 209 | 0.6 | 297.1 | 258.21 | 179.8 | 64.0 |
| fast | seq-write | 1 MiB | 8 | 128 | 158 | 0.5 | 301.6 | 270.09 | 142.0 | 127.5 |
| fast | seq-write | 1 MiB | 8 | 256 | 150 | 0.5 | 325.4 | 288.12 | 132.5 | 218.0 |
| fast | seq-write | 1 MiB | 12 | 16 | 213 | 0.5 | 415.3 | 346.35 | 175.5 | 47.8 |
| fast | seq-write | 1 MiB | 12 | 64 | 173 | 0.5 | 324.8 | 274.61 | 146.5 | 60.5 |
| fast | seq-write | 1 MiB | 12 | 128 | 214 | 0.6 | 375.2 | 304.06 | 172.9 | 119.5 |
| fast | seq-write | 1 MiB | 12 | 256 | 158 | 0.5 | 327.3 | 286.03 | 138.2 | 239.0 |
| fast | seq-write | 4 MiB | 2 | 16 | 33 | 0.8 | 44.9 | 151.31 | 111.0 | 242.5 |
| fast | seq-write | 4 MiB | 2 | 64 | 35 | 0.6 | 58.7 | 203.03 | 121.9 | 256.0 |
| fast | seq-write | 4 MiB | 2 | 128 | 31 | 0.5 | 64.8 | 231.56 | 112.6 | 512.0 |
| fast | seq-write | 4 MiB | 2 | 256 | 43 | 0.6 | 72.2 | 258.25 | 154.5 | 1024.0 |
| fast | seq-write | 4 MiB | 4 | 16 | 24 | 0.5 | 43.3 | 152.15 | 83.1 | 230.0 |
| fast | seq-write | 4 MiB | 4 | 64 | 25 | 0.5 | 46.7 | 165.49 | 89.0 | 256.0 |
| fast | seq-write | 4 MiB | 4 | 128 | 86 | 1.0 | 85.7 | 296.29 | 292.9 | 512.0 |
| fast | seq-write | 4 MiB | 4 | 256 | 42 | 0.5 | 79.0 | 284.94 | 150.5 | 1012.0 |
| fast | seq-write | 4 MiB | 8 | 16 | 26 | 0.5 | 49.3 | 170.17 | 89.0 | 218.0 |
| fast | seq-write | 4 MiB | 8 | 64 | 34 | 0.5 | 69.6 | 244.24 | 119.7 | 256.0 |
| fast | seq-write | 4 MiB | 8 | 128 | 37 | 0.5 | 72.4 | 251.75 | 128.9 | 494.0 |
| fast | seq-write | 4 MiB | 8 | 256 | 30 | 0.4 | 69.0 | 255.58 | 111.2 | 1008.0 |
| fast | seq-write | 4 MiB | 12 | 16 | 22 | 0.5 | 41.8 | 140.80 | 73.6 | 239.0 |
| fast | seq-write | 4 MiB | 12 | 64 | 24 | 0.5 | 45.7 | 159.82 | 84.5 | 252.0 |
| fast | seq-write | 4 MiB | 12 | 128 | 92 | 0.9 | 93.6 | 305.46 | 299.3 | 510.0 |
| fast | seq-write | 4 MiB | 12 | 256 | 44 | 0.5 | 86.6 | 309.22 | 157.1 | 976.0 |
