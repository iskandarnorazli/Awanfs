`gh workflow run bench-scaling.yml --ref issue/262-put-core-cap -f cores=1,2,4,8 -f matrix=false -f include_self_hosted=true` (run 44646106203, job "Core scaling (self-hosted)" on LAPTOP-6RBIQUM5), which ran `python -u benchmarks/core_scaling.py --runs 2 --duration 2 --data-dir $RUNNER_TEMP --output report.md --cores 1,2,4,8`

# Is PUT capped near one core? (#262)

**Answer: not by the server in this run. The client caps it, so the server's core cap is not measured here.** Two things show it. AwanFS's PUT with `--durability fast` (no flush) is as flat as durable PUT, about 4,800 ops/s from 1 to 8 server cores. GET 4 KiB, which takes no write lock and no flush, is just as flat at about 5,000 ops/s. Meanwhile the server is never more than about 1.4 cores busy (GET: about 0.5), and the per-thread table spreads the work over the tokio workers, with no single thread holding it. P50 latency doubles each time concurrency doubles (2.97, 5.97, 11.95, 21.98 ms at 16, 32, 64, 128 clients) while throughput stays flat: a single serial stage outside the server. RustFS GET bunches up at the same rate (4,276 to 4,474 ops/s).

The likeliest serial stage is the Python load generator (`benchmarks/core_scaling.py`: `http.client` connections driven by threads in one process, so one interpreter lock). That is inferred, not measured. The client ran on its own cores (1 to 15, server on the first N), so this is not cores shared with the server. Next measurement, before any server change: the same matrix with a load generator that is not one Python process (warp, or several client processes), to see where the server stops.

The report as the run printed it:

# Core Scaling Benchmark: AwanFS vs MinIO vs RustFS (2026-11-20 04:02 UTC)

- **Machine:** LAPTOP-6RBIQUM5 (Windows-11-10.0.26300-SP0, 16 logical cores, 31.4 GB RAM, CPU: AMD64 Family 25 Model 80 Stepping 0, AuthenticAMD)
- **Methodology:** `cores = 1, 2, 4, 8`, `concurrency = 16 � N`, 2 alternating runs per point (median reported), 2.0s per phase.
- **Core pinning:** Load generator is pinned to remaining cores (`N..15`) at all tested core counts (never shares cores with the server).
- **Durability:** AwanFS group commit (#29). MinIO does not flush these PUTs on Windows (its fdatasync is a no-op there), so its PUT and mixed rows on this machine are not durable writes and are not like for like. Disk `sync_probe` durable append ceiling (`MAX` line on PUT charts): 4 KiB = 1,120 ops/s; 64 KiB = 945 ops/s.

## Throughput and Latency (`ops/s (P50, P99 ms)`)

| Workload | Size | Server | 1 core | 2 cores | 4 cores | 8 cores | Scaling (8/1) |
|---|---|---|---|---|---|---|---|
| PUT 4 KiB | 4 KiB | AwanFS | 3,456 ops/s (4.01, 15.81 ms) | 4,320 ops/s (6.76, 19.75 ms) | 3,818 ops/s (12.99, 80.58 ms) | 4,164 ops/s (23.62, 98.65 ms) | 1.20x |
| PUT 4 KiB | 4 KiB | AwanFS (--durability fast) | 4,817 ops/s (2.97, 12.26 ms) | 4,799 ops/s (5.97, 18.56 ms) | 4,039 ops/s (11.95, 77.04 ms) | 4,207 ops/s (21.98, 99.52 ms) | 0.87x |
| PUT 4 KiB | 4 KiB | MinIO | 115 ops/s (127.55, 391.26 ms) | 89 ops/s (363.07, 663.50 ms) | 116 ops/s (519.32, 1054.10 ms) | 158 ops/s (845.25, 1309.21 ms) | 1.37x |
| PUT 4 KiB | 4 KiB | RustFS | 101 ops/s (156.05, 260.67 ms) | 138 ops/s (224.43, 310.68 ms) | 168 ops/s (372.98, 514.41 ms) | 175 ops/s (704.12, 1009.93 ms) | 1.74x |
| PUT 64 KiB | 64 KiB | AwanFS | 1,901 ops/s (7.49, 24.07 ms) | 3,312 ops/s (8.88, 22.91 ms) | 3,910 ops/s (14.78, 38.89 ms) | 4,016 ops/s (26.63, 72.73 ms) | 2.11x |
| PUT 64 KiB | 64 KiB | MinIO | 83 ops/s (184.10, 456.78 ms) | 90 ops/s (333.76, 692.61 ms) | 123 ops/s (565.59, 804.97 ms) | 138 ops/s (953.02, 1325.51 ms) | 1.67x |
| PUT 64 KiB | 64 KiB | RustFS | 80 ops/s (182.20, 375.04 ms) | 127 ops/s (247.52, 305.33 ms) | 148 ops/s (431.17, 559.25 ms) | 142 ops/s (880.06, 1257.43 ms) | 1.79x |
| GET 4 KiB | 4 KiB | AwanFS | 4,868 ops/s (2.85, 12.49 ms) | 5,266 ops/s (5.16, 18.47 ms) | 4,852 ops/s (10.79, 37.22 ms) | 5,086 ops/s (19.03, 68.58 ms) | 1.04x |
| GET 4 KiB | 4 KiB | MinIO | 1,434 ops/s (10.53, 24.83 ms) | 2,387 ops/s (12.13, 34.38 ms) | 3,552 ops/s (15.58, 47.18 ms) | 3,352 ops/s (27.34, 100.88 ms) | 2.34x |
| GET 4 KiB | 4 KiB | RustFS | 612 ops/s (2.39, 51.45 ms) | 4,474 ops/s (6.10, 23.25 ms) | 4,276 ops/s (12.71, 40.40 ms) | 4,318 ops/s (21.72, 80.16 ms) | 7.06x |
| GET 64 KiB | 64 KiB | AwanFS | 3,836 ops/s (3.77, 14.08 ms) | 4,258 ops/s (6.74, 20.25 ms) | 3,971 ops/s (14.24, 38.79 ms) | 4,032 ops/s (26.88, 70.26 ms) | 1.05x |
| GET 64 KiB | 64 KiB | MinIO | 1,312 ops/s (11.26, 29.60 ms) | 2,056 ops/s (13.77, 39.88 ms) | 2,750 ops/s (20.84, 55.10 ms) | 2,764 ops/s (37.89, 99.20 ms) | 2.11x |
| GET 64 KiB | 64 KiB | RustFS | 2,378 ops/s (3.57, 45.77 ms) | 3,665 ops/s (7.75, 24.44 ms) | 3,574 ops/s (16.20, 42.05 ms) | 3,500 ops/s (30.42, 78.30 ms) | 1.47x |

## Server CPU Usage and Efficiency (`CPU %` where 100% = 1 core busy; `ops/s per busy core`)

| Workload | Size | Server | 1 core | 2 cores | 4 cores | 8 cores |
|---|---|---|---|---|---|---|
| PUT 4 KiB | 4 KiB | AwanFS | 68.1% (5,076 ops/s/core) | 134.1% (3,227 ops/s/core) | 114.5% (3,352 ops/s/core) | 137.4% (3,032 ops/s/core) |
| PUT 4 KiB | 4 KiB | AwanFS (--durability fast) | 63.1% (7,640 ops/s/core) | 88.9% (5,402 ops/s/core) | 81.8% (4,996 ops/s/core) | 84.3% (5,017 ops/s/core) |
| PUT 4 KiB | 4 KiB | MinIO | 95.3% (121 ops/s/core) | 132.0% (68 ops/s/core) | 198.2% (58 ops/s/core) | 292.3% (54 ops/s/core) |
| PUT 4 KiB | 4 KiB | RustFS | 97.1% (104 ops/s/core) | 165.0% (83 ops/s/core) | 231.4% (73 ops/s/core) | 245.7% (71 ops/s/core) |
| PUT 64 KiB | 64 KiB | AwanFS | 88.4% (2,151 ops/s/core) | 168.1% (1,970 ops/s/core) | 220.8% (1,771 ops/s/core) | 264.5% (1,519 ops/s/core) |
| PUT 64 KiB | 64 KiB | MinIO | 91.3% (91 ops/s/core) | 130.0% (69 ops/s/core) | 218.8% (56 ops/s/core) | 266.4% (52 ops/s/core) |
| PUT 64 KiB | 64 KiB | RustFS | 97.7% (81 ops/s/core) | 162.6% (78 ops/s/core) | 209.0% (71 ops/s/core) | 222.1% (64 ops/s/core) |
| GET 4 KiB | 4 KiB | AwanFS | 46.2% (10,791 ops/s/core) | 53.3% (9,890 ops/s/core) | 52.6% (9,225 ops/s/core) | 56.9% (8,939 ops/s/core) |
| GET 4 KiB | 4 KiB | MinIO | 79.9% (1,797 ops/s/core) | 162.8% (1,467 ops/s/core) | 247.1% (1,438 ops/s/core) | 284.9% (1,177 ops/s/core) |
| GET 4 KiB | 4 KiB | RustFS | 93.4% (655 ops/s/core) | 118.8% (3,767 ops/s/core) | 136.5% (3,141 ops/s/core) | 166.5% (2,599 ops/s/core) |
| GET 64 KiB | 64 KiB | AwanFS | 41.6% (9,222 ops/s/core) | 58.7% (7,319 ops/s/core) | 58.3% (6,816 ops/s/core) | 53.4% (7,570 ops/s/core) |
| GET 64 KiB | 64 KiB | MinIO | 82.9% (1,582 ops/s/core) | 163.9% (1,254 ops/s/core) | 240.2% (1,145 ops/s/core) | 270.2% (1,023 ops/s/core) |
| GET 64 KiB | 64 KiB | RustFS | 97.5% (2,443 ops/s/core) | 98.8% (3,717 ops/s/core) | 121.0% (2,958 ops/s/core) | 150.3% (2,329 ops/s/core) |

## Per-Thread CPU Breakdown (AwanFS PUT 4 KiB)

| Durability | Cores | Thread ID | Thread Name | User (s) | System (s) | Total CPU (s) | Share | Concurrency | Wall (s) |
|---|---|---|---|---|---|---|---|---|---|
| group | 4 | 9580 | tokio-rt-worker | 0.109 | 0.172 | 0.281 | 9.8% | 64 | 2.24 |
| group | 4 | 30144 | tokio-rt-worker | 0.141 | 0.125 | 0.266 | 9.2% | 64 | 2.24 |
| group | 4 | 28816 | tokio-rt-worker | 0.094 | 0.141 | 0.234 | 8.2% | 64 | 2.24 |
| group | 4 | 29896 | awanfs-log | 0.078 | 0.156 | 0.234 | 8.2% | 64 | 2.24 |
| group | 4 | 21072 | tokio-rt-worker | 0.094 | 0.125 | 0.219 | 7.6% | 64 | 2.24 |
| group | 4 | 26552 | tokio-rt-worker | 0.141 | 0.078 | 0.219 | 7.6% | 64 | 2.24 |
| group | 4 | 2852 | tokio-rt-worker | 0.078 | 0.109 | 0.188 | 6.5% | 64 | 2.24 |
| group | 4 | 21340 | tokio-rt-worker | 0.047 | 0.062 | 0.109 | 3.8% | 64 | 2.24 |
| group | 4 | 26412 | tokio-rt-worker | 0.078 | 0.016 | 0.094 | 3.3% | 64 | 2.24 |
| group | 4 | 24872 | tokio-rt-worker | 0.047 | 0.031 | 0.078 | 2.7% | 64 | 2.24 |
| group | 4 | 27272 | tokio-rt-worker | 0.031 | 0.047 | 0.078 | 2.7% | 64 | 2.24 |
| group | 4 | 18420 | tokio-rt-worker | 0.062 | 0.000 | 0.062 | 2.2% | 64 | 2.24 |
| group | 4 | 26028 | tokio-rt-worker | 0.031 | 0.016 | 0.047 | 1.6% | 64 | 2.24 |
| group | 4 | 27356 | tokio-rt-worker | 0.031 | 0.016 | 0.047 | 1.6% | 64 | 2.24 |
| group | 4 | 28688 | tokio-rt-worker | 0.000 | 0.047 | 0.047 | 1.6% | 64 | 2.24 |
| group | 4 | 29108 | tokio-rt-worker | 0.000 | 0.047 | 0.047 | 1.6% | 64 | 2.24 |
| group | 4 | 3252 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 1.1% | 64 | 2.24 |
| group | 4 | 3260 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 1.1% | 64 | 2.24 |
| group | 4 | 8560 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 1.1% | 64 | 2.24 |
| group | 4 | 9244 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 1.1% | 64 | 2.24 |
| group | 4 | 17252 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 1.1% | 64 | 2.24 |
| group | 4 | 18300 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 1.1% | 64 | 2.24 |
| group | 4 | 23304 | tokio-rt-worker | 0.031 | 0.000 | 0.031 | 1.1% | 64 | 2.24 |
| group | 4 | 25776 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 1.1% | 64 | 2.24 |
| group | 4 | 26100 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 1.1% | 64 | 2.24 |
| group | 4 | 27404 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 1.1% | 64 | 2.24 |
| group | 4 | 27908 | tokio-rt-worker | 0.031 | 0.000 | 0.031 | 1.1% | 64 | 2.24 |
| group | 4 | 29620 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 1.1% | 64 | 2.24 |
| group | 4 | 6728 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.5% | 64 | 2.24 |
| group | 4 | 7664 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.5% | 64 | 2.24 |
| group | 4 | 7772 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.5% | 64 | 2.24 |
| group | 4 | 10284 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.5% | 64 | 2.24 |
| group | 4 | 11052 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.5% | 64 | 2.24 |
| group | 4 | 11108 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.5% | 64 | 2.24 |
| group | 4 | 15524 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.5% | 64 | 2.24 |
| group | 4 | 18592 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.5% | 64 | 2.24 |
| group | 4 | 23724 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.5% | 64 | 2.24 |
| group | 4 | 24432 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.5% | 64 | 2.24 |
| group | 4 | 27652 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.5% | 64 | 2.24 |
| group | 4 | 29360 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.5% | 64 | 2.24 |
| group | 4 | 29468 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.5% | 64 | 2.24 |
| group | 4 | 29536 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.5% | 64 | 2.24 |
| group | 4 | 29612 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.5% | 64 | 2.24 |
| group | 4 | 29884 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.5% | 64 | 2.24 |
| group | 4 | 2808 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 4 | 5124 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 4 | 10016 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 4 | 10728 | main | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 4 | 11296 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 4 | 14928 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 4 | 15308 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 4 | 17180 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 4 | 18472 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 4 | 18524 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 4 | 19008 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 4 | 23744 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 4 | 24592 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 4 | 24976 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 4 | 25684 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 4 | 25880 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 4 | 26200 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 4 | 26368 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 4 | 26612 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 4 | 27376 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 4 | 28232 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 4 | 28728 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 4 | 28992 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.24 |
| group | 8 | 9712 | tokio-rt-worker | 0.234 | 0.172 | 0.406 | 9.6% | 128 | 3.00 |
| group | 8 | 17740 | tokio-rt-worker | 0.234 | 0.172 | 0.406 | 9.6% | 128 | 3.00 |
| group | 8 | 22588 | tokio-rt-worker | 0.172 | 0.188 | 0.359 | 8.5% | 128 | 3.00 |
| group | 8 | 25064 | tokio-rt-worker | 0.219 | 0.141 | 0.359 | 8.5% | 128 | 3.00 |
| group | 8 | 20288 | tokio-rt-worker | 0.172 | 0.156 | 0.328 | 7.7% | 128 | 3.00 |
| group | 8 | 24244 | tokio-rt-worker | 0.141 | 0.156 | 0.297 | 7.0% | 128 | 3.00 |
| group | 8 | 29036 | tokio-rt-worker | 0.156 | 0.141 | 0.297 | 7.0% | 128 | 3.00 |
| group | 8 | 6052 | awanfs-log | 0.031 | 0.219 | 0.250 | 5.9% | 128 | 3.00 |
| group | 8 | 26424 | tokio-rt-worker | 0.125 | 0.078 | 0.203 | 4.8% | 128 | 3.00 |
| group | 8 | 15852 | tokio-rt-worker | 0.062 | 0.125 | 0.188 | 4.4% | 128 | 3.00 |
| group | 8 | 1796 | tokio-rt-worker | 0.047 | 0.094 | 0.141 | 3.3% | 128 | 3.00 |
| group | 8 | 23744 | tokio-rt-worker | 0.094 | 0.016 | 0.109 | 2.6% | 128 | 3.00 |
| group | 8 | 27996 | tokio-rt-worker | 0.047 | 0.062 | 0.109 | 2.6% | 128 | 3.00 |
| group | 8 | 14516 | tokio-rt-worker | 0.031 | 0.031 | 0.062 | 1.5% | 128 | 3.00 |
| group | 8 | 26912 | tokio-rt-worker | 0.031 | 0.031 | 0.062 | 1.5% | 128 | 3.00 |
| group | 8 | 7680 | tokio-rt-worker | 0.016 | 0.031 | 0.047 | 1.1% | 128 | 3.00 |
| group | 8 | 16964 | tokio-rt-worker | 0.031 | 0.016 | 0.047 | 1.1% | 128 | 3.00 |
| group | 8 | 2604 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.7% | 128 | 3.00 |
| group | 8 | 2800 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.7% | 128 | 3.00 |
| group | 8 | 4088 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.7% | 128 | 3.00 |
| group | 8 | 6772 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.7% | 128 | 3.00 |
| group | 8 | 11288 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.7% | 128 | 3.00 |
| group | 8 | 11552 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.7% | 128 | 3.00 |
| group | 8 | 29400 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.7% | 128 | 3.00 |
| group | 8 | 29520 | tokio-rt-worker | 0.031 | 0.000 | 0.031 | 0.7% | 128 | 3.00 |
| group | 8 | 2324 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.4% | 128 | 3.00 |
| group | 8 | 2476 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.4% | 128 | 3.00 |
| group | 8 | 5304 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.4% | 128 | 3.00 |
| group | 8 | 5860 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.4% | 128 | 3.00 |
| group | 8 | 6352 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.4% | 128 | 3.00 |
| group | 8 | 9312 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.4% | 128 | 3.00 |
| group | 8 | 11072 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.4% | 128 | 3.00 |
| group | 8 | 12364 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.4% | 128 | 3.00 |
| group | 8 | 12700 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.4% | 128 | 3.00 |
| group | 8 | 22864 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.4% | 128 | 3.00 |
| group | 8 | 25072 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.4% | 128 | 3.00 |
| group | 8 | 26568 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.4% | 128 | 3.00 |
| group | 8 | 26672 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.4% | 128 | 3.00 |
| group | 8 | 27240 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.4% | 128 | 3.00 |
| group | 8 | 27536 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.4% | 128 | 3.00 |
| group | 8 | 28460 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.4% | 128 | 3.00 |
| group | 8 | 28488 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.4% | 128 | 3.00 |
| group | 8 | 28808 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.4% | 128 | 3.00 |
| group | 8 | 29272 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.4% | 128 | 3.00 |
| group | 8 | 29476 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.4% | 128 | 3.00 |
| group | 8 | 30440 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.4% | 128 | 3.00 |
| group | 8 | 720 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 1776 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 2484 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 4760 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 6836 | main | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 7352 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 8548 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 8668 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 9644 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 10000 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 13224 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 15344 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 15448 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 17952 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 20864 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 22516 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 24064 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 24600 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 26272 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 27432 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 28064 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 28940 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 28964 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 29384 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| group | 8 | 29812 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 3.00 |
| fast | 4 | 22232 | tokio-rt-worker | 0.078 | 0.172 | 0.250 | 15.0% | 64 | 2.29 |
| fast | 4 | 27084 | tokio-rt-worker | 0.078 | 0.172 | 0.250 | 15.0% | 64 | 2.29 |
| fast | 4 | 3776 | awanfs-log | 0.078 | 0.141 | 0.219 | 13.1% | 64 | 2.29 |
| fast | 4 | 27512 | tokio-rt-worker | 0.109 | 0.078 | 0.188 | 11.2% | 64 | 2.29 |
| fast | 4 | 29024 | tokio-rt-worker | 0.078 | 0.094 | 0.172 | 10.3% | 64 | 2.29 |
| fast | 4 | 26100 | tokio-rt-worker | 0.094 | 0.062 | 0.156 | 9.3% | 64 | 2.29 |
| fast | 4 | 2704 | tokio-rt-worker | 0.047 | 0.078 | 0.125 | 7.5% | 64 | 2.29 |
| fast | 4 | 29060 | tokio-rt-worker | 0.047 | 0.047 | 0.094 | 5.6% | 64 | 2.29 |
| fast | 4 | 11204 | tokio-rt-worker | 0.031 | 0.047 | 0.078 | 4.7% | 64 | 2.29 |
| fast | 4 | 7324 | tokio-rt-worker | 0.031 | 0.016 | 0.047 | 2.8% | 64 | 2.29 |
| fast | 4 | 11104 | tokio-rt-worker | 0.000 | 0.047 | 0.047 | 2.8% | 64 | 2.29 |
| fast | 4 | 26204 | tokio-rt-worker | 0.031 | 0.016 | 0.047 | 2.8% | 64 | 2.29 |
| fast | 4 | 2684 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.29 |
| fast | 4 | 3532 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.29 |
| fast | 4 | 15064 | main | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.29 |
| fast | 4 | 20796 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.29 |
| fast | 4 | 21284 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.29 |
| fast | 4 | 26684 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.29 |
| fast | 4 | 26884 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.29 |
| fast | 4 | 28932 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.29 |
| fast | 4 | 29404 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.29 |
| fast | 4 | 30212 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 2.29 |
| fast | 8 | 26208 | awanfs-log | 0.141 | 0.312 | 0.453 | 16.6% | 128 | 2.96 |
| fast | 8 | 12364 | tokio-rt-worker | 0.203 | 0.219 | 0.422 | 15.4% | 128 | 2.96 |
| fast | 8 | 26412 | tokio-rt-worker | 0.219 | 0.078 | 0.297 | 10.9% | 128 | 2.96 |
| fast | 8 | 5444 | tokio-rt-worker | 0.109 | 0.156 | 0.266 | 9.7% | 128 | 2.96 |
| fast | 8 | 30048 | tokio-rt-worker | 0.109 | 0.125 | 0.234 | 8.6% | 128 | 2.96 |
| fast | 8 | 24028 | tokio-rt-worker | 0.094 | 0.109 | 0.203 | 7.4% | 128 | 2.96 |
| fast | 8 | 10016 | tokio-rt-worker | 0.078 | 0.094 | 0.172 | 6.3% | 128 | 2.96 |
| fast | 8 | 23224 | tokio-rt-worker | 0.047 | 0.125 | 0.172 | 6.3% | 128 | 2.96 |
| fast | 8 | 4976 | tokio-rt-worker | 0.062 | 0.078 | 0.141 | 5.1% | 128 | 2.96 |
| fast | 8 | 19784 | tokio-rt-worker | 0.047 | 0.047 | 0.094 | 3.4% | 128 | 2.96 |
| fast | 8 | 21160 | tokio-rt-worker | 0.031 | 0.062 | 0.094 | 3.4% | 128 | 2.96 |
| fast | 8 | 1776 | tokio-rt-worker | 0.016 | 0.047 | 0.062 | 2.3% | 128 | 2.96 |
| fast | 8 | 29384 | tokio-rt-worker | 0.031 | 0.031 | 0.062 | 2.3% | 128 | 2.96 |
| fast | 8 | 8604 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 1.1% | 128 | 2.96 |
| fast | 8 | 15064 | (unnamed) | 0.000 | 0.031 | 0.031 | 1.1% | 128 | 2.96 |
| fast | 8 | 3124 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 2.96 |
| fast | 8 | 7224 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 2.96 |
| fast | 8 | 8548 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 2.96 |
| fast | 8 | 11312 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 2.96 |
| fast | 8 | 16644 | main | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 2.96 |
| fast | 8 | 23776 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 2.96 |
| fast | 8 | 27240 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 2.96 |

## Charts

- `report_put_4k.png`
- `report_put_4k.svg`
- `report_put_64k.png`
- `report_put_64k.svg`
- `report_get_4k.png`
- `report_get_4k.svg`
- `report_get_64k.png`
- `report_get_64k.svg`
- `report_ops_per_busy_core.png`
- `report_ops_per_busy_core.svg`

