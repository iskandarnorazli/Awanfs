`gh workflow run bench-scaling.yml --ref issue/427-selfhosted-compare-fast -f cores=1,2,4,8 -f matrix=false -f duration=5 -f runs=3 -f include_self_hosted=true` (run 45790294615, job "Core scaling (self-hosted)" 139807301780 on LAPTOP-6RBIQUM5), which ran `python benchmarks/core_scaling.py --runs 3 --duration 5 --client-procs 4 --data-dir <runner temp> --output <runner temp>/core_scaling/report.md --compare-fast --cores 1,2,4,8`

# Is PUT capped near one core? (#262, second run)

**Answer: no. With the client in 4 processes on its own cores, AwanFS's PUT 4 KiB uses 2.9 server cores at 4 and 3.8 at 8, and no thread holds it: the busiest thread, `awanfs-log` (the group-commit sync), is 28% of one core at 4 cores and 26% at 8.**

What changed since the first run (`measurements/2026-11-20-put-core-cap.md`, run 44646106203): that run drove the load from one Python process, and every server was flat at about 4,000 to 5,000 ops/s. This run uses `--client-procs 4`. AwanFS's PUT 4 KiB now goes from 1,688 to 9,569 ops/s from 1 to 8 cores (5.67x), and server CPU from 86% to 379%.

Where the 8-core point stops: the report flags the client as saturated there (`yes (saturated client procs)`, 402% of its 8 cores, with 4 processes), while the server uses 3.8 of its 8 cores. So the 8-core PUT number is the client's limit, not the server's. At 4 cores neither side is flagged; the server is at 293% of 4 cores. `--durability fast` (no sync) at 4 cores gives 12,672 ops/s against 8,734 with group commit: the sync wait costs about 31% there, and the sync thread's own CPU is small (1.41 s of 14.9 s).

Two things seen, not explained, and not measured further here:
- The server starts 16 tokio workers whatever its core affinity (the laptop has 16 logical cores): the `fast` rows show 16 `tokio-rt-worker` threads at both 4 and 8 cores. The `group` rows show 88 and 122 threads with that name, which includes the blocking pool. A guess: 16 workers on 1 pinned core may be why 1 core (1,688 ops/s) scales by more than 2x to 2 cores (5,747).
- PUT 1 MiB with group commit drops from 499 ops/s at 4 cores to 308 at 8 (P99 1,573 ms), while `--durability fast` rises from 473 to 590. One run of 3 rounds.

Garage has no Windows binary (`fetch_garage.py` exits 1, the job continues), so its rows are not measured. MinIO's GET 64 KiB at 2 cores is not measured (its warm-up PUT got HTTP 403).

- **Machine:** LAPTOP-6RBIQUM5 (Windows-11-10.0.26300-SP0, 16 logical cores, 31.4 GB RAM, CPU: AMD64 Family 25 Model 80 Stepping 0, AuthenticAMD)
- **Methodology:** `cores = 1, 2, 4, 8`, `concurrency = 16 × N`, `client_procs = 4`, 3 alternating runs per point (median reported), 5.0s per phase.
- **Core pinning:** Load generator is pinned to remaining cores (`N..15`) at all tested core counts (never shares cores with the server).
- **Durability:** AwanFS group commit (#29). MinIO does not flush these PUTs on Windows (its fdatasync is a no-op there), so its PUT and mixed rows on this machine are not durable writes and are not like for like. RustFS release defaults (durable, 3.09 syncs/PUT; measurements/2026-11-21-durability-probe.md, #334). Garage `metadata_fsync=false, data_fsync=false`. SeaweedFS `fsync` unset (`false`) by default. Disk `sync_probe` durable append ceiling (`MAX` line on PUT charts): 4 KiB = 904 ops/s; 64 KiB = 833 ops/s.

## PUT 4 KiB: throughput, server CPU and client CPU

| Workload | Size | Server | 1 core | 2 cores | 4 cores | 8 cores | Scaling (8/1) |
|---|---|---|---|---|---|---|---|
| PUT 4 KiB | 4 KiB | AwanFS | 1,688 ops/s (4.47, 67.63 ms) | 5,747 ops/s (4.95, 12.93 ms) | 8,734 ops/s (6.13, 15.72 ms) | 9,569 ops/s (10.44, 39.43 ms) | 5.67x |
| PUT 4 KiB | 4 KiB | AwanFS (--durability fast) | 7,551 ops/s (0.52, 32.16 ms) | 9,283 ops/s (1.84, 25.61 ms) | 12,672 ops/s (4.17, 12.92 ms) | 12,242 ops/s (8.17, 41.65 ms) | 1.62x |
| PUT 4 KiB | 4 KiB | MinIO | 89 ops/s (171.20, 460.78 ms) | 131 ops/s (208.06, 627.38 ms) | 190 ops/s (285.10, 901.05 ms) | 264 ops/s (239.16, 1685.87 ms) | 2.97x |
| PUT 4 KiB | 4 KiB | RustFS | 81 ops/s (193.83, 467.22 ms) | 145 ops/s (247.40, 391.32 ms) | 207 ops/s (367.22, 501.11 ms) | 273 ops/s (328.23, 1119.09 ms) | 3.37x |
| PUT 4 KiB | 4 KiB | SeaweedFS | 897 ops/s (16.05, 45.48 ms) | 1,410 ops/s (21.32, 51.79 ms) | 2,611 ops/s (22.98, 52.86 ms) | 4,513 ops/s (26.81, 55.97 ms) | 5.03x |

| Workload | Size | Server | 1 core | 2 cores | 4 cores | 8 cores |
|---|---|---|---|---|---|---|
| PUT 4 KiB | 4 KiB | AwanFS | 86.1% (1,960 ops/s/core) | 175.4% (3,299 ops/s/core) | 293.0% (2,835 ops/s/core) | 379.1% (2,574 ops/s/core) |
| PUT 4 KiB | 4 KiB | AwanFS (--durability fast) | 93.2% (7,670 ops/s/core) | 188.0% (4,966 ops/s/core) | 343.8% (3,685 ops/s/core) | 397.8% (3,115 ops/s/core) |
| PUT 4 KiB | 4 KiB | MinIO | 91.0% (97 ops/s/core) | 141.7% (92 ops/s/core) | 229.8% (83 ops/s/core) | 303.9% (86 ops/s/core) |
| PUT 4 KiB | 4 KiB | RustFS | 92.4% (88 ops/s/core) | 159.4% (91 ops/s/core) | 235.5% (88 ops/s/core) | 284.8% (96 ops/s/core) |
| PUT 4 KiB | 4 KiB | SeaweedFS | 97.1% (919 ops/s/core) | 195.7% (726 ops/s/core) | 383.9% (680 ops/s/core) | 755.1% (599 ops/s/core) |

Client CPU (`CPU % / client cores (share, saturated?)`):

| Workload | Size | Server | 1 core | 2 cores | 4 cores | 8 cores |
|---|---|---|---|---|---|---|
| PUT 4 KiB | 4 KiB | AwanFS | 57.1% / 15 cores (4%, no) | 165.8% / 14 cores (12%, no) | 300.7% / 12 cores (25%, no) | 402.2% / 8 cores (50%, yes (saturated client procs)) |
| PUT 4 KiB | 4 KiB | AwanFS (--durability fast) | 157.7% / 15 cores (11%, no) | 228.9% / 14 cores (16%, no) | 462.8% / 12 cores (39%, no (server also >=85%)) | 457.0% / 8 cores (57%, yes (saturated client procs)) |
| PUT 4 KiB | 4 KiB | MinIO | 5.2% / 15 cores (0%, no) | 7.6% / 14 cores (1%, no) | 11.5% / 12 cores (1%, no) | 17.9% / 8 cores (2%, no) |
| PUT 4 KiB | 4 KiB | RustFS | 5.1% / 15 cores (0%, no) | 8.0% / 14 cores (1%, no) | 12.6% / 12 cores (1%, no) | 15.4% / 8 cores (2%, no) |
| PUT 4 KiB | 4 KiB | SeaweedFS | 29.7% / 15 cores (2%, no) | 41.7% / 14 cores (3%, no) | 87.1% / 12 cores (7%, no) | 174.5% / 8 cores (22%, no) |

## Per-thread CPU, AwanFS PUT 4 KiB, summed from the table below

| Durability | Cores | Threads | Named `tokio-rt-worker` | Threads above 0.3 s | Server CPU (s) | Wall (s) | Cores busy | Busiest thread | Its CPU (s) | Share of one core |
|---|---|---|---|---|---|---|---|---|---|---|
| group | 4 | 93 | 88 | 17 | 14.88 | 5.08 | 2.93 | awanfs-log | 1.406 | 28% |
| group | 8 | 127 | 122 | 17 | 19.23 | 5.07 | 3.79 | awanfs-log | 1.312 | 26% |
| fast | 4 | 22 | 16 | 18 | 17.47 | 5.08 | 3.44 | awanfs-log | 1.734 | 34% |
| fast | 8 | 22 | 16 | 18 | 20.37 | 5.08 | 4.01 | awanfs-log | 2.094 | 41% |

## All workloads, as the run printed them

| Workload | Size | Server | 1 core | 2 cores | 4 cores | 8 cores | Scaling (8/1) |
|---|---|---|---|---|---|---|---|
| PUT 4 KiB | 4 KiB | AwanFS | 1,688 ops/s (4.47, 67.63 ms) | 5,747 ops/s (4.95, 12.93 ms) | 8,734 ops/s (6.13, 15.72 ms) | 9,569 ops/s (10.44, 39.43 ms) | 5.67x |
| PUT 4 KiB | 4 KiB | AwanFS (--durability fast) | 7,551 ops/s (0.52, 32.16 ms) | 9,283 ops/s (1.84, 25.61 ms) | 12,672 ops/s (4.17, 12.92 ms) | 12,242 ops/s (8.17, 41.65 ms) | 1.62x |
| PUT 4 KiB | 4 KiB | MinIO | 89 ops/s (171.20, 460.78 ms) | 131 ops/s (208.06, 627.38 ms) | 190 ops/s (285.10, 901.05 ms) | 264 ops/s (239.16, 1685.87 ms) | 2.97x |
| PUT 4 KiB | 4 KiB | RustFS | 81 ops/s (193.83, 467.22 ms) | 145 ops/s (247.40, 391.32 ms) | 207 ops/s (367.22, 501.11 ms) | 273 ops/s (328.23, 1119.09 ms) | 3.37x |
| PUT 4 KiB | 4 KiB | Garage | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | - |
| PUT 4 KiB | 4 KiB | SeaweedFS | 897 ops/s (16.05, 45.48 ms) | 1,410 ops/s (21.32, 51.79 ms) | 2,611 ops/s (22.98, 52.86 ms) | 4,513 ops/s (26.81, 55.97 ms) | 5.03x |
| PUT 64 KiB | 64 KiB | AwanFS | 1,294 ops/s (7.29, 49.26 ms) | 3,341 ops/s (8.45, 24.99 ms) | 5,316 ops/s (10.14, 47.00 ms) | 6,715 ops/s (17.78, 40.72 ms) | 5.19x |
| PUT 64 KiB | 64 KiB | AwanFS (--durability fast) | 2,691 ops/s (0.61, 108.70 ms) | 4,497 ops/s (4.15, 37.31 ms) | 6,269 ops/s (7.12, 20.62 ms) | 4,157 ops/s (12.15, 343.73 ms) | 1.55x |
| PUT 64 KiB | 64 KiB | MinIO | 69 ops/s (209.39, 516.28 ms) | 90 ops/s (342.28, 693.13 ms) | 121 ops/s (600.15, 1023.41 ms) | 133 ops/s (954.57, 1462.14 ms) | 1.94x |
| PUT 64 KiB | 64 KiB | RustFS | 68 ops/s (221.26, 446.50 ms) | 114 ops/s (271.70, 386.28 ms) | 145 ops/s (425.45, 605.61 ms) | 159 ops/s (776.10, 1185.46 ms) | 2.34x |
| PUT 64 KiB | 64 KiB | Garage | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | - |
| PUT 64 KiB | 64 KiB | SeaweedFS | 631 ops/s (23.37, 59.12 ms) | 998 ops/s (30.31, 63.12 ms) | 1,847 ops/s (32.72, 68.23 ms) | 2,570 ops/s (40.80, 91.34 ms) | 4.08x |
| GET 4 KiB | 4 KiB | AwanFS | 10,778 ops/s (0.69, 21.76 ms) | 15,958 ops/s (1.59, 8.56 ms) | 15,581 ops/s (3.63, 11.11 ms) | 14,950 ops/s (6.63, 31.69 ms) | 1.39x |
| GET 4 KiB | 4 KiB | MinIO | 1,140 ops/s (12.57, 31.75 ms) | 2,133 ops/s (13.78, 38.55 ms) | 4,030 ops/s (15.73, 32.13 ms) | 6,266 ops/s (19.37, 35.67 ms) | 5.49x |
| GET 4 KiB | 4 KiB | RustFS | 3,366 ops/s (1.04, 71.50 ms) | 6,232 ops/s (3.64, 24.98 ms) | 10,917 ops/s (5.09, 23.72 ms) | 11,741 ops/s (8.66, 37.47 ms) | 3.49x |
| GET 4 KiB | 4 KiB | Garage | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | - |
| GET 4 KiB | 4 KiB | SeaweedFS | 2,142 ops/s (7.00, 14.22 ms) | 3,196 ops/s (9.60, 17.97 ms) | 5,766 ops/s (10.70, 20.01 ms) | 9,287 ops/s (12.98, 28.15 ms) | 4.33x |
| GET 64 KiB | 64 KiB | AwanFS | 8,929 ops/s (0.60, 34.35 ms) | 12,446 ops/s (2.13, 10.17 ms) | 12,269 ops/s (4.83, 11.81 ms) | 11,938 ops/s (9.17, 34.18 ms) | 1.34x |
| GET 64 KiB | 64 KiB | MinIO | 1,151 ops/s (12.27, 32.03 ms) | not measured (warm-up PUT HTTP 403) | 3,741 ops/s (16.79, 37.81 ms) | 5,685 ops/s (20.76, 41.46 ms) | 4.94x |
| GET 64 KiB | 64 KiB | RustFS | 2,765 ops/s (1.05, 82.07 ms) | 5,797 ops/s (3.91, 27.31 ms) | 9,419 ops/s (5.78, 31.93 ms) | 9,388 ops/s (11.25, 41.04 ms) | 3.40x |
| GET 64 KiB | 64 KiB | Garage | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | - |
| GET 64 KiB | 64 KiB | SeaweedFS | 1,763 ops/s (8.38, 17.92 ms) | 2,641 ops/s (11.64, 24.93 ms) | 4,705 ops/s (13.04, 27.32 ms) | 7,011 ops/s (16.88, 46.56 ms) | 3.98x |
| PUT 1 MiB | 1024 KiB | AwanFS | 178 ops/s (83.20, 245.14 ms) | 303 ops/s (95.96, 247.32 ms) | 499 ops/s (122.89, 233.06 ms) | 308 ops/s (241.35, 1573.31 ms) | 1.73x |
| PUT 1 MiB | 1024 KiB | AwanFS (--durability fast) | 205 ops/s (74.58, 217.26 ms) | 318 ops/s (93.13, 244.79 ms) | 473 ops/s (111.78, 484.58 ms) | 590 ops/s (149.97, 833.56 ms) | 2.88x |
| PUT 1 MiB | 1024 KiB | MinIO | 45 ops/s (341.45, 805.96 ms) | 69 ops/s (421.80, 988.22 ms) | 88 ops/s (692.48, 1191.63 ms) | 95 ops/s (1335.37, 2132.24 ms) | 2.11x |
| PUT 1 MiB | 1024 KiB | RustFS | 28 ops/s (553.45, 755.58 ms) | 53 ops/s (605.26, 754.12 ms) | 75 ops/s (839.99, 1039.03 ms) | 90 ops/s (1407.51, 1718.23 ms) | 3.16x |
| PUT 1 MiB | 1024 KiB | Garage | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | - |
| PUT 1 MiB | 1024 KiB | SeaweedFS | 104 ops/s (148.93, 221.88 ms) | 178 ops/s (162.75, 395.46 ms) | 307 ops/s (171.47, 599.70 ms) | 503 ops/s (228.77, 643.35 ms) | 4.86x |
| GET 1 MiB | 1024 KiB | AwanFS | 2,007 ops/s (2.78, 88.71 ms) | 2,819 ops/s (8.60, 40.14 ms) | 2,508 ops/s (22.51, 74.33 ms) | 2,569 ops/s (33.36, 266.69 ms) | 1.28x |
| GET 1 MiB | 1024 KiB | MinIO | 508 ops/s (30.40, 71.93 ms) | 802 ops/s (37.59, 86.12 ms) | 1,328 ops/s (45.78, 87.84 ms) | 1,511 ops/s (79.67, 207.50 ms) | 2.97x |
| GET 1 MiB | 1024 KiB | RustFS | 305 ops/s (48.92, 138.15 ms) | 493 ops/s (60.03, 146.74 ms) | 747 ops/s (81.38, 175.25 ms) | 866 ops/s (141.49, 311.44 ms) | 2.84x |
| GET 1 MiB | 1024 KiB | Garage | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | - |
| GET 1 MiB | 1024 KiB | SeaweedFS | 509 ops/s (30.60, 48.71 ms) | 795 ops/s (39.14, 70.91 ms) | 1,175 ops/s (32.37, 279.81 ms) | 1,219 ops/s (67.00, 506.51 ms) | 2.40x |

| Workload | Size | Server | 1 core | 2 cores | 4 cores | 8 cores |
|---|---|---|---|---|---|---|
| PUT 4 KiB | 4 KiB | AwanFS | 86.1% (1,960 ops/s/core) | 175.4% (3,299 ops/s/core) | 293.0% (2,835 ops/s/core) | 379.1% (2,574 ops/s/core) |
| PUT 4 KiB | 4 KiB | AwanFS (--durability fast) | 93.2% (7,670 ops/s/core) | 188.0% (4,966 ops/s/core) | 343.8% (3,685 ops/s/core) | 397.8% (3,115 ops/s/core) |
| PUT 4 KiB | 4 KiB | MinIO | 91.0% (97 ops/s/core) | 141.7% (92 ops/s/core) | 229.8% (83 ops/s/core) | 303.9% (86 ops/s/core) |
| PUT 4 KiB | 4 KiB | RustFS | 92.4% (88 ops/s/core) | 159.4% (91 ops/s/core) | 235.5% (88 ops/s/core) | 284.8% (96 ops/s/core) |
| PUT 4 KiB | 4 KiB | Garage | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) |
| PUT 4 KiB | 4 KiB | SeaweedFS | 97.1% (919 ops/s/core) | 195.7% (726 ops/s/core) | 383.9% (680 ops/s/core) | 755.1% (599 ops/s/core) |
| PUT 64 KiB | 64 KiB | AwanFS | 93.2% (1,417 ops/s/core) | 180.2% (1,854 ops/s/core) | 313.5% (1,696 ops/s/core) | 452.3% (1,494 ops/s/core) |
| PUT 64 KiB | 64 KiB | AwanFS (--durability fast) | 92.0% (2,924 ops/s/core) | 180.5% (2,465 ops/s/core) | 291.1% (2,154 ops/s/core) | 221.5% (1,877 ops/s/core) |
| PUT 64 KiB | 64 KiB | MinIO | 91.0% (77 ops/s/core) | 130.6% (69 ops/s/core) | 215.1% (58 ops/s/core) | 270.4% (50 ops/s/core) |
| PUT 64 KiB | 64 KiB | RustFS | 93.4% (73 ops/s/core) | 160.2% (71 ops/s/core) | 216.7% (66 ops/s/core) | 249.9% (64 ops/s/core) |
| PUT 64 KiB | 64 KiB | Garage | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) |
| PUT 64 KiB | 64 KiB | SeaweedFS | 93.6% (672 ops/s/core) | 193.6% (515 ops/s/core) | 386.7% (478 ops/s/core) | 647.0% (396 ops/s/core) |
| GET 4 KiB | 4 KiB | AwanFS | 94.4% (11,374 ops/s/core) | 186.8% (8,563 ops/s/core) | 220.6% (7,062 ops/s/core) | 241.1% (6,277 ops/s/core) |
| GET 4 KiB | 4 KiB | MinIO | 76.1% (1,459 ops/s/core) | 146.7% (1,445 ops/s/core) | 303.4% (1,347 ops/s/core) | 566.6% (1,113 ops/s/core) |
| GET 4 KiB | 4 KiB | RustFS | 97.5% (3,452 ops/s/core) | 192.0% (3,287 ops/s/core) | 380.0% (2,870 ops/s/core) | 478.5% (2,421 ops/s/core) |
| GET 4 KiB | 4 KiB | Garage | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) |
| GET 4 KiB | 4 KiB | SeaweedFS | 94.0% (2,334 ops/s/core) | 193.3% (1,653 ops/s/core) | 391.4% (1,485 ops/s/core) | 744.2% (1,246 ops/s/core) |
| GET 64 KiB | 64 KiB | AwanFS | 98.1% (9,103 ops/s/core) | 188.3% (6,603 ops/s/core) | 231.3% (5,263 ops/s/core) | 224.0% (5,251 ops/s/core) |
| GET 64 KiB | 64 KiB | MinIO | 74.9% (1,536 ops/s/core) | not measured (warm-up PUT HTTP 403) | 325.8% (1,160 ops/s/core) | 587.3% (968 ops/s/core) |
| GET 64 KiB | 64 KiB | RustFS | 91.9% (3,008 ops/s/core) | 192.9% (2,992 ops/s/core) | 372.7% (2,517 ops/s/core) | 442.0% (2,077 ops/s/core) |
| GET 64 KiB | 64 KiB | Garage | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) |
| GET 64 KiB | 64 KiB | SeaweedFS | 96.1% (1,880 ops/s/core) | 196.4% (1,336 ops/s/core) | 390.0% (1,206 ops/s/core) | 742.0% (945 ops/s/core) |
| PUT 1 MiB | 1024 KiB | AwanFS | 91.7% (190 ops/s/core) | 189.0% (160 ops/s/core) | 351.7% (141 ops/s/core) | 260.7% (121 ops/s/core) |
| PUT 1 MiB | 1024 KiB | AwanFS (--durability fast) | 91.5% (224 ops/s/core) | 182.2% (173 ops/s/core) | 316.6% (149 ops/s/core) | 505.9% (117 ops/s/core) |
| PUT 1 MiB | 1024 KiB | MinIO | 91.6% (49 ops/s/core) | 166.9% (41 ops/s/core) | 256.6% (34 ops/s/core) | 348.1% (27 ops/s/core) |
| PUT 1 MiB | 1024 KiB | RustFS | 93.8% (30 ops/s/core) | 173.3% (30 ops/s/core) | 246.1% (30 ops/s/core) | 317.8% (27 ops/s/core) |
| PUT 1 MiB | 1024 KiB | Garage | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) |
| PUT 1 MiB | 1024 KiB | SeaweedFS | 97.3% (106 ops/s/core) | 196.7% (91 ops/s/core) | 370.3% (83 ops/s/core) | 755.2% (67 ops/s/core) |
| GET 1 MiB | 1024 KiB | AwanFS | 94.9% (2,115 ops/s/core) | 182.9% (1,541 ops/s/core) | 269.3% (930 ops/s/core) | 253.3% (1,015 ops/s/core) |
| GET 1 MiB | 1024 KiB | MinIO | 86.3% (592 ops/s/core) | 171.3% (461 ops/s/core) | 349.1% (379 ops/s/core) | 614.6% (246 ops/s/core) |
| GET 1 MiB | 1024 KiB | RustFS | 97.0% (315 ops/s/core) | 193.0% (257 ops/s/core) | 386.8% (193 ops/s/core) | 744.0% (116 ops/s/core) |
| GET 1 MiB | 1024 KiB | Garage | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) | not measured (garage binary not found (upstream releases Linux binaries only)) |
| GET 1 MiB | 1024 KiB | SeaweedFS | 92.4% (552 ops/s/core) | 193.2% (411 ops/s/core) | 376.5% (308 ops/s/core) | 673.1% (175 ops/s/core) |

## Per-thread CPU, as the run printed it

| Durability | Cores | Thread ID | Thread Name | User (s) | System (s) | Total CPU (s) | Share | Concurrency | Wall (s) |
|---|---|---|---|---|---|---|---|---|---|
| group | 4 | 29700 | awanfs-log | 0.312 | 1.094 | 1.406 | 9.5% | 64 | 5.08 |
| group | 4 | 30444 | tokio-rt-worker | 0.594 | 0.375 | 0.969 | 6.5% | 64 | 5.08 |
| group | 4 | 32308 | tokio-rt-worker | 0.453 | 0.422 | 0.875 | 5.9% | 64 | 5.08 |
| group | 4 | 30196 | tokio-rt-worker | 0.406 | 0.344 | 0.750 | 5.0% | 64 | 5.08 |
| group | 4 | 30640 | tokio-rt-worker | 0.328 | 0.422 | 0.750 | 5.0% | 64 | 5.08 |
| group | 4 | 15540 | tokio-rt-worker | 0.297 | 0.438 | 0.734 | 4.9% | 64 | 5.08 |
| group | 4 | 29780 | tokio-rt-worker | 0.359 | 0.359 | 0.719 | 4.8% | 64 | 5.08 |
| group | 4 | 18104 | tokio-rt-worker | 0.219 | 0.469 | 0.688 | 4.6% | 64 | 5.08 |
| group | 4 | 5064 | tokio-rt-worker | 0.406 | 0.266 | 0.672 | 4.5% | 64 | 5.08 |
| group | 4 | 31692 | tokio-rt-worker | 0.328 | 0.344 | 0.672 | 4.5% | 64 | 5.08 |
| group | 4 | 8732 | tokio-rt-worker | 0.250 | 0.406 | 0.656 | 4.4% | 64 | 5.08 |
| group | 4 | 24556 | tokio-rt-worker | 0.359 | 0.266 | 0.625 | 4.2% | 64 | 5.08 |
| group | 4 | 34572 | tokio-rt-worker | 0.312 | 0.312 | 0.625 | 4.2% | 64 | 5.08 |
| group | 4 | 432 | tokio-rt-worker | 0.297 | 0.312 | 0.609 | 4.1% | 64 | 5.08 |
| group | 4 | 34676 | tokio-rt-worker | 0.219 | 0.391 | 0.609 | 4.1% | 64 | 5.08 |
| group | 4 | 23920 | tokio-rt-worker | 0.266 | 0.266 | 0.531 | 3.6% | 64 | 5.08 |
| group | 4 | 23960 | tokio-rt-worker | 0.312 | 0.219 | 0.531 | 3.6% | 64 | 5.08 |
| group | 4 | 31200 | tokio-rt-worker | 0.016 | 0.094 | 0.109 | 0.7% | 64 | 5.08 |
| group | 4 | 16888 | tokio-rt-worker | 0.031 | 0.047 | 0.078 | 0.5% | 64 | 5.08 |
| group | 4 | 25436 | tokio-rt-worker | 0.031 | 0.047 | 0.078 | 0.5% | 64 | 5.08 |
| group | 4 | 28664 | tokio-rt-worker | 0.016 | 0.062 | 0.078 | 0.5% | 64 | 5.08 |
| group | 4 | 30556 | tokio-rt-worker | 0.047 | 0.031 | 0.078 | 0.5% | 64 | 5.08 |
| group | 4 | 31648 | tokio-rt-worker | 0.000 | 0.078 | 0.078 | 0.5% | 64 | 5.08 |
| group | 4 | 31872 | tokio-rt-worker | 0.047 | 0.031 | 0.078 | 0.5% | 64 | 5.08 |
| group | 4 | 34980 | tokio-rt-worker | 0.000 | 0.078 | 0.078 | 0.5% | 64 | 5.08 |
| group | 4 | 7244 | tokio-rt-worker | 0.031 | 0.031 | 0.062 | 0.4% | 64 | 5.08 |
| group | 4 | 18196 | tokio-rt-worker | 0.016 | 0.047 | 0.062 | 0.4% | 64 | 5.08 |
| group | 4 | 24140 | tokio-rt-worker | 0.031 | 0.031 | 0.062 | 0.4% | 64 | 5.08 |
| group | 4 | 26156 | tokio-rt-worker | 0.016 | 0.047 | 0.062 | 0.4% | 64 | 5.08 |
| group | 4 | 27052 | tokio-rt-worker | 0.031 | 0.031 | 0.062 | 0.4% | 64 | 5.08 |
| group | 4 | 30760 | tokio-rt-worker | 0.016 | 0.047 | 0.062 | 0.4% | 64 | 5.08 |
| group | 4 | 10452 | tokio-rt-worker | 0.031 | 0.016 | 0.047 | 0.3% | 64 | 5.08 |
| group | 4 | 14688 | tokio-rt-worker | 0.016 | 0.031 | 0.047 | 0.3% | 64 | 5.08 |
| group | 4 | 14848 | tokio-rt-worker | 0.000 | 0.047 | 0.047 | 0.3% | 64 | 5.08 |
| group | 4 | 19184 | tokio-rt-worker | 0.000 | 0.047 | 0.047 | 0.3% | 64 | 5.08 |
| group | 4 | 21396 | tokio-rt-worker | 0.031 | 0.016 | 0.047 | 0.3% | 64 | 5.08 |
| group | 4 | 25124 | tokio-rt-worker | 0.031 | 0.016 | 0.047 | 0.3% | 64 | 5.08 |
| group | 4 | 29484 | tokio-rt-worker | 0.000 | 0.047 | 0.047 | 0.3% | 64 | 5.08 |
| group | 4 | 30064 | tokio-rt-worker | 0.031 | 0.016 | 0.047 | 0.3% | 64 | 5.08 |
| group | 4 | 31000 | tokio-rt-worker | 0.031 | 0.016 | 0.047 | 0.3% | 64 | 5.08 |
| group | 4 | 31752 | tokio-rt-worker | 0.016 | 0.031 | 0.047 | 0.3% | 64 | 5.08 |
| group | 4 | 34064 | tokio-rt-worker | 0.016 | 0.031 | 0.047 | 0.3% | 64 | 5.08 |
| group | 4 | 34460 | tokio-rt-worker | 0.000 | 0.047 | 0.047 | 0.3% | 64 | 5.08 |
| group | 4 | 34712 | tokio-rt-worker | 0.016 | 0.031 | 0.047 | 0.3% | 64 | 5.08 |
| group | 4 | 35324 | tokio-rt-worker | 0.016 | 0.031 | 0.047 | 0.3% | 64 | 5.08 |
| group | 4 | 780 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.2% | 64 | 5.08 |
| group | 4 | 24176 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 64 | 5.08 |
| group | 4 | 27316 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 64 | 5.08 |
| group | 4 | 28920 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.2% | 64 | 5.08 |
| group | 4 | 31212 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.2% | 64 | 5.08 |
| group | 4 | 31400 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.2% | 64 | 5.08 |
| group | 4 | 31460 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.2% | 64 | 5.08 |
| group | 4 | 31576 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 64 | 5.08 |
| group | 4 | 31724 | tokio-rt-worker | 0.031 | 0.000 | 0.031 | 0.2% | 64 | 5.08 |
| group | 4 | 32984 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.2% | 64 | 5.08 |
| group | 4 | 33576 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 64 | 5.08 |
| group | 4 | 34152 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 64 | 5.08 |
| group | 4 | 34996 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.2% | 64 | 5.08 |
| group | 4 | 35056 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.2% | 64 | 5.08 |
| group | 4 | 35368 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 64 | 5.08 |
| group | 4 | 6060 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.1% | 64 | 5.08 |
| group | 4 | 6876 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 64 | 5.08 |
| group | 4 | 8092 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 64 | 5.08 |
| group | 4 | 10108 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 64 | 5.08 |
| group | 4 | 12708 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 64 | 5.08 |
| group | 4 | 16100 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.1% | 64 | 5.08 |
| group | 4 | 17212 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 64 | 5.08 |
| group | 4 | 17496 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.1% | 64 | 5.08 |
| group | 4 | 24268 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.1% | 64 | 5.08 |
| group | 4 | 25744 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.1% | 64 | 5.08 |
| group | 4 | 26128 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 64 | 5.08 |
| group | 4 | 31536 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 64 | 5.08 |
| group | 4 | 32632 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.1% | 64 | 5.08 |
| group | 4 | 33480 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.1% | 64 | 5.08 |
| group | 4 | 33612 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.1% | 64 | 5.08 |
| group | 4 | 33620 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.1% | 64 | 5.08 |
| group | 4 | 34804 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 64 | 5.08 |
| group | 4 | 35196 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 64 | 5.08 |
| group | 4 | 35692 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.1% | 64 | 5.08 |
| group | 4 | 13692 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 5.08 |
| group | 4 | 14560 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 5.08 |
| group | 4 | 24800 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 5.08 |
| group | 4 | 25108 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 5.08 |
| group | 4 | 27136 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 5.08 |
| group | 4 | 28428 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 5.08 |
| group | 4 | 29044 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 5.08 |
| group | 4 | 31448 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 5.08 |
| group | 4 | 31620 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 5.08 |
| group | 4 | 32592 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 5.08 |
| group | 4 | 33868 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 5.08 |
| group | 4 | 34020 | main | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 5.08 |
| group | 4 | 34428 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 5.08 |
| group | 4 | 34924 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 5.08 |
| group | 8 | 31020 | awanfs-log | 0.172 | 1.141 | 1.312 | 6.8% | 128 | 5.07 |
| group | 8 | 29020 | tokio-rt-worker | 0.656 | 0.438 | 1.094 | 5.7% | 128 | 5.07 |
| group | 8 | 32804 | tokio-rt-worker | 0.594 | 0.453 | 1.047 | 5.4% | 128 | 5.07 |
| group | 8 | 34828 | tokio-rt-worker | 0.469 | 0.562 | 1.031 | 5.4% | 128 | 5.07 |
| group | 8 | 31624 | tokio-rt-worker | 0.562 | 0.438 | 1.000 | 5.2% | 128 | 5.07 |
| group | 8 | 32640 | tokio-rt-worker | 0.484 | 0.500 | 0.984 | 5.1% | 128 | 5.07 |
| group | 8 | 16240 | tokio-rt-worker | 0.375 | 0.578 | 0.953 | 5.0% | 128 | 5.07 |
| group | 8 | 31380 | tokio-rt-worker | 0.391 | 0.547 | 0.938 | 4.9% | 128 | 5.07 |
| group | 8 | 25928 | tokio-rt-worker | 0.406 | 0.500 | 0.906 | 4.7% | 128 | 5.07 |
| group | 8 | 404 | tokio-rt-worker | 0.375 | 0.500 | 0.875 | 4.5% | 128 | 5.07 |
| group | 8 | 16348 | tokio-rt-worker | 0.469 | 0.406 | 0.875 | 4.5% | 128 | 5.07 |
| group | 8 | 33304 | tokio-rt-worker | 0.438 | 0.422 | 0.859 | 4.5% | 128 | 5.07 |
| group | 8 | 27500 | tokio-rt-worker | 0.375 | 0.469 | 0.844 | 4.4% | 128 | 5.07 |
| group | 8 | 30140 | tokio-rt-worker | 0.438 | 0.406 | 0.844 | 4.4% | 128 | 5.07 |
| group | 8 | 34372 | tokio-rt-worker | 0.531 | 0.281 | 0.812 | 4.2% | 128 | 5.07 |
| group | 8 | 34820 | tokio-rt-worker | 0.453 | 0.328 | 0.781 | 4.1% | 128 | 5.07 |
| group | 8 | 27432 | tokio-rt-worker | 0.344 | 0.344 | 0.688 | 3.6% | 128 | 5.07 |
| group | 8 | 30236 | tokio-rt-worker | 0.047 | 0.062 | 0.109 | 0.6% | 128 | 5.07 |
| group | 8 | 34928 | tokio-rt-worker | 0.047 | 0.062 | 0.109 | 0.6% | 128 | 5.07 |
| group | 8 | 31816 | tokio-rt-worker | 0.016 | 0.078 | 0.094 | 0.5% | 128 | 5.07 |
| group | 8 | 3008 | tokio-rt-worker | 0.000 | 0.078 | 0.078 | 0.4% | 128 | 5.07 |
| group | 8 | 22668 | tokio-rt-worker | 0.000 | 0.078 | 0.078 | 0.4% | 128 | 5.07 |
| group | 8 | 27820 | tokio-rt-worker | 0.031 | 0.047 | 0.078 | 0.4% | 128 | 5.07 |
| group | 8 | 33280 | tokio-rt-worker | 0.031 | 0.047 | 0.078 | 0.4% | 128 | 5.07 |
| group | 8 | 34852 | tokio-rt-worker | 0.031 | 0.047 | 0.078 | 0.4% | 128 | 5.07 |
| group | 8 | 10108 | tokio-rt-worker | 0.047 | 0.016 | 0.062 | 0.3% | 128 | 5.07 |
| group | 8 | 14688 | tokio-rt-worker | 0.016 | 0.047 | 0.062 | 0.3% | 128 | 5.07 |
| group | 8 | 17548 | tokio-rt-worker | 0.031 | 0.031 | 0.062 | 0.3% | 128 | 5.07 |
| group | 8 | 29700 | tokio-rt-worker | 0.000 | 0.062 | 0.062 | 0.3% | 128 | 5.07 |
| group | 8 | 33488 | tokio-rt-worker | 0.047 | 0.016 | 0.062 | 0.3% | 128 | 5.07 |
| group | 8 | 34612 | tokio-rt-worker | 0.031 | 0.031 | 0.062 | 0.3% | 128 | 5.07 |
| group | 8 | 8568 | tokio-rt-worker | 0.047 | 0.000 | 0.047 | 0.2% | 128 | 5.07 |
| group | 8 | 11880 | tokio-rt-worker | 0.016 | 0.031 | 0.047 | 0.2% | 128 | 5.07 |
| group | 8 | 24352 | tokio-rt-worker | 0.031 | 0.016 | 0.047 | 0.2% | 128 | 5.07 |
| group | 8 | 24808 | tokio-rt-worker | 0.016 | 0.031 | 0.047 | 0.2% | 128 | 5.07 |
| group | 8 | 27972 | tokio-rt-worker | 0.031 | 0.016 | 0.047 | 0.2% | 128 | 5.07 |
| group | 8 | 30528 | tokio-rt-worker | 0.031 | 0.016 | 0.047 | 0.2% | 128 | 5.07 |
| group | 8 | 31584 | tokio-rt-worker | 0.031 | 0.016 | 0.047 | 0.2% | 128 | 5.07 |
| group | 8 | 32148 | tokio-rt-worker | 0.031 | 0.016 | 0.047 | 0.2% | 128 | 5.07 |
| group | 8 | 33788 | tokio-rt-worker | 0.031 | 0.016 | 0.047 | 0.2% | 128 | 5.07 |
| group | 8 | 34408 | tokio-rt-worker | 0.016 | 0.031 | 0.047 | 0.2% | 128 | 5.07 |
| group | 8 | 34740 | tokio-rt-worker | 0.031 | 0.016 | 0.047 | 0.2% | 128 | 5.07 |
| group | 8 | 34916 | tokio-rt-worker | 0.047 | 0.000 | 0.047 | 0.2% | 128 | 5.07 |
| group | 8 | 34980 | tokio-rt-worker | 0.031 | 0.016 | 0.047 | 0.2% | 128 | 5.07 |
| group | 8 | 35124 | tokio-rt-worker | 0.031 | 0.016 | 0.047 | 0.2% | 128 | 5.07 |
| group | 8 | 35324 | tokio-rt-worker | 0.031 | 0.016 | 0.047 | 0.2% | 128 | 5.07 |
| group | 8 | 35724 | tokio-rt-worker | 0.000 | 0.047 | 0.047 | 0.2% | 128 | 5.07 |
| group | 8 | 1956 | tokio-rt-worker | 0.031 | 0.000 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 2448 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 3048 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 5168 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 5608 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 7436 | tokio-rt-worker | 0.031 | 0.000 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 16712 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 17080 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 17604 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 19512 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 20356 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 25128 | tokio-rt-worker | 0.031 | 0.000 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 25404 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 28156 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 28416 | tokio-rt-worker | 0.031 | 0.000 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 28552 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 28568 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 29028 | tokio-rt-worker | 0.031 | 0.000 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 29096 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 29176 | tokio-rt-worker | 0.031 | 0.000 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 29308 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 29928 | tokio-rt-worker | 0.031 | 0.000 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 30020 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 31088 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 31368 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 32212 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 32272 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 32464 | tokio-rt-worker | 0.031 | 0.000 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 32648 | tokio-rt-worker | 0.031 | 0.000 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 32924 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 33436 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 34064 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 34112 | tokio-rt-worker | 0.000 | 0.031 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 34660 | tokio-rt-worker | 0.031 | 0.000 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 34800 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 34896 | tokio-rt-worker | 0.016 | 0.016 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 35772 | tokio-rt-worker | 0.031 | 0.000 | 0.031 | 0.2% | 128 | 5.07 |
| group | 8 | 736 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 7652 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 12164 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 15268 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 16168 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 17084 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 18836 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 20964 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 27104 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 28592 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 29508 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 30044 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 30348 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 30840 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 31472 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 32052 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 32076 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 32332 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 33072 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 33140 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 33648 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 34016 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 34996 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 35292 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 35392 | tokio-rt-worker | 0.016 | 0.000 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 35536 | tokio-rt-worker | 0.000 | 0.016 | 0.016 | 0.1% | 128 | 5.07 |
| group | 8 | 5596 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 5.07 |
| group | 8 | 17480 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 5.07 |
| group | 8 | 22920 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 5.07 |
| group | 8 | 24184 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 5.07 |
| group | 8 | 24668 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 5.07 |
| group | 8 | 25640 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 5.07 |
| group | 8 | 25812 | main | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 5.07 |
| group | 8 | 26216 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 5.07 |
| group | 8 | 26516 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 5.07 |
| group | 8 | 30080 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 5.07 |
| group | 8 | 31048 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 5.07 |
| group | 8 | 31536 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 5.07 |
| group | 8 | 32040 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 5.07 |
| group | 8 | 32964 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 5.07 |
| group | 8 | 33652 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 5.07 |
| group | 8 | 33956 | tokio-rt-worker | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 5.07 |
| group | 8 | 35228 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 5.07 |
| fast | 4 | 21808 | awanfs-log | 0.234 | 1.500 | 1.734 | 9.9% | 64 | 5.08 |
| fast | 4 | 26516 | tokio-rt-worker | 0.734 | 0.516 | 1.250 | 7.2% | 64 | 5.08 |
| fast | 4 | 33304 | tokio-rt-worker | 0.578 | 0.531 | 1.109 | 6.4% | 64 | 5.08 |
| fast | 4 | 27432 | tokio-rt-worker | 0.516 | 0.531 | 1.047 | 6.0% | 64 | 5.08 |
| fast | 4 | 31380 | tokio-rt-worker | 0.516 | 0.531 | 1.047 | 6.0% | 64 | 5.08 |
| fast | 4 | 25640 | tokio-rt-worker | 0.500 | 0.531 | 1.031 | 5.9% | 64 | 5.08 |
| fast | 4 | 34828 | tokio-rt-worker | 0.422 | 0.609 | 1.031 | 5.9% | 64 | 5.08 |
| fast | 4 | 12996 | tokio-rt-worker | 0.469 | 0.547 | 1.016 | 5.8% | 64 | 5.08 |
| fast | 4 | 31624 | tokio-rt-worker | 0.422 | 0.531 | 0.953 | 5.5% | 64 | 5.08 |
| fast | 4 | 30140 | tokio-rt-worker | 0.453 | 0.484 | 0.938 | 5.4% | 64 | 5.08 |
| fast | 4 | 34372 | tokio-rt-worker | 0.484 | 0.453 | 0.938 | 5.4% | 64 | 5.08 |
| fast | 4 | 16348 | tokio-rt-worker | 0.359 | 0.562 | 0.922 | 5.3% | 64 | 5.08 |
| fast | 4 | 35228 | tokio-rt-worker | 0.344 | 0.547 | 0.891 | 5.1% | 64 | 5.08 |
| fast | 4 | 16240 | tokio-rt-worker | 0.453 | 0.391 | 0.844 | 4.8% | 64 | 5.08 |
| fast | 4 | 404 | tokio-rt-worker | 0.250 | 0.531 | 0.781 | 4.5% | 64 | 5.08 |
| fast | 4 | 25812 | tokio-rt-worker | 0.328 | 0.438 | 0.766 | 4.4% | 64 | 5.08 |
| fast | 4 | 25928 | tokio-rt-worker | 0.266 | 0.484 | 0.750 | 4.3% | 64 | 5.08 |
| fast | 4 | 32804 | (unnamed) | 0.016 | 0.406 | 0.422 | 2.4% | 64 | 5.08 |
| fast | 4 | 6224 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 5.08 |
| fast | 4 | 29852 | main | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 5.08 |
| fast | 4 | 31272 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 5.08 |
| fast | 4 | 31708 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 64 | 5.08 |
| fast | 8 | 34360 | awanfs-log | 0.453 | 1.641 | 2.094 | 10.3% | 128 | 5.08 |
| fast | 8 | 20964 | tokio-rt-worker | 0.484 | 0.844 | 1.328 | 6.5% | 128 | 5.08 |
| fast | 8 | 32360 | tokio-rt-worker | 0.578 | 0.656 | 1.234 | 6.1% | 128 | 5.08 |
| fast | 8 | 23412 | tokio-rt-worker | 0.547 | 0.625 | 1.172 | 5.8% | 128 | 5.08 |
| fast | 8 | 8300 | tokio-rt-worker | 0.531 | 0.625 | 1.156 | 5.7% | 128 | 5.08 |
| fast | 8 | 35536 | tokio-rt-worker | 0.578 | 0.578 | 1.156 | 5.7% | 128 | 5.08 |
| fast | 8 | 35692 | tokio-rt-worker | 0.484 | 0.672 | 1.156 | 5.7% | 128 | 5.08 |
| fast | 8 | 30344 | tokio-rt-worker | 0.438 | 0.703 | 1.141 | 5.6% | 128 | 5.08 |
| fast | 8 | 19132 | tokio-rt-worker | 0.578 | 0.531 | 1.109 | 5.4% | 128 | 5.08 |
| fast | 8 | 28488 | tokio-rt-worker | 0.516 | 0.594 | 1.109 | 5.4% | 128 | 5.08 |
| fast | 8 | 32140 | tokio-rt-worker | 0.516 | 0.594 | 1.109 | 5.4% | 128 | 5.08 |
| fast | 8 | 29316 | tokio-rt-worker | 0.562 | 0.500 | 1.062 | 5.2% | 128 | 5.08 |
| fast | 8 | 34320 | tokio-rt-worker | 0.547 | 0.516 | 1.062 | 5.2% | 128 | 5.08 |
| fast | 8 | 26208 | tokio-rt-worker | 0.516 | 0.531 | 1.047 | 5.1% | 128 | 5.08 |
| fast | 8 | 35136 | tokio-rt-worker | 0.500 | 0.531 | 1.031 | 5.1% | 128 | 5.08 |
| fast | 8 | 24328 | tokio-rt-worker | 0.531 | 0.484 | 1.016 | 5.0% | 128 | 5.08 |
| fast | 8 | 35164 | tokio-rt-worker | 0.438 | 0.547 | 0.984 | 4.8% | 128 | 5.08 |
| fast | 8 | 34972 | (unnamed) | 0.016 | 0.391 | 0.406 | 2.0% | 128 | 5.08 |
| fast | 8 | 17604 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 5.08 |
| fast | 8 | 25724 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 5.08 |
| fast | 8 | 25816 | (unnamed) | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 5.08 |
| fast | 8 | 35272 | main | 0.000 | 0.000 | 0.000 | 0.0% | 128 | 5.08 |
