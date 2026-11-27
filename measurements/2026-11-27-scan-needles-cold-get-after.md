Command: `gh workflow run bench-scaling.yml --ref try/512-cold-get-after` (run 45967560937, commit 9bcb3f2; baseline before fix: run 45962606934 in `measurements/2026-11-27-cold-get-1mib-probe.md`; runner command: `python benchmarks/core_scaling.py --cold-get --no-fetch-minio --compare-fast --sizes 1MiB,8MiB --servers AwanFS --cold-set 1GiB --cold-concurrency 16 --cold-cores 2 --data-dir /mnt/bench --output /home/runner/work/_temp/core_scaling/cold_get.md`)

# Restart scan (`scan_needles`) before and after fix (#512)

Fix in #512: call `window.cap_next_refill(SKIP_REFILL_BYTES)` in `VolumeManager::try_scan_needles` after advancing past a needle or gap larger than `SCAN_BATCH_BYTES` (`1 MiB`), matching `open_with` (#412).

## Before vs After (`3. after_restart_before_get` and Cold GET)

| Size | Metric | Before (`run 45962606934`) | After (`run 45967560937`) | Change |
|---|---|---|---|---|
| 1 MiB | Restart `Cached` delta (`2. after_drop` -> `3. after_restart_before_get`) | `+1,032.9 MiB` (`338.3` -> `1,371.2 MiB`) | `+70.5 MiB` (`326.5` -> `397.0 MiB`) | **-962.4 MiB (14.7x less)** |
| 1 MiB | Restart `/proc/<pid>/io` `read_bytes` | `1,033.7 MiB` (`1,083,920,384` B) | `71.0 MiB` (`74,420,224` B) | **14.6x less** |
| 1 MiB | Restart `/proc/<pid>/io` `rchar` | `1,089.1 MiB` (`1,141,961,714` B) | `130.0 MiB` (`136,311,794` B) | **8.4x less** |
| 1 MiB | `read_bytes` added during Cold GET pass (`3 -> 4`) | `+0.6 MiB` (served from page cache) | `+963.4 MiB` (true disk read) | Reads payloads only on GET |
| 1 MiB | Cold GET throughput (P50 / P99 ms) | `2,161.3 MiB/s` (`7.13 / 12.39`) | `465.3 MiB/s` (`34.48 / 66.40`) | True cold disk speed (no restart pre-warm) |
| 8 MiB | Restart `Cached` delta (`2. after_drop` -> `3. after_restart_before_get`) | `+168.7 MiB` (`338.1` -> `506.8 MiB`) | `+14.4 MiB` (`326.5` -> `340.9 MiB`) | **11.7x less** |
| 8 MiB | Restart `/proc/<pid>/io` `read_bytes` | `169.5 MiB` (`177,754,112` B) | `15.0 MiB` (`15,716,352` B) | **11.3x less** |
| 8 MiB | Restart `/proc/<pid>/io` `rchar` | `137.0 MiB` (`143,679,730` B) | `18.0 MiB` (`18,833,650` B) | **7.6x less** |
| 8 MiB | Cold GET throughput (P50 / P99 ms) | `435.2 MiB/s` (`283.83 / 477.48`) | `446.0 MiB/s` (`296.35 / 361.57`) | +2.5% |

## After-fix run (`45967560937`, 2026-11-27 08:42 UTC)

- **Machine:** runnervm3lb7s (Linux-7.0.0-1020-azure-x86_64-with-glibc2.43, 4 logical cores, 15.6 GB RAM, CPU: AMD EPYC 9V45 96-Core Processor)
- **Method:** per size, 1024 MiB of objects is written with 16 connections, the server is stopped, `sync` and `echo 3 > /proc/sys/vm/drop_caches` drop the page cache, and the server restarts on the same data before reading every object once cold and once warm. Server pinned to 2 cores, client to [2, 3] (shares cores: False).

### Cold and warm GET throughput

| Server | Size | Objects | PUT (writing the set) | Cold GET | Cold P50 / P99 ms | Warm GET | Warm P50 / P99 ms |
|---|---|---|---|---|---|---|---|
| AwanFS | 1 MiB | 1024 | 191 ops/s, 190.6 MiB/s | 465 ops/s, 465.3 MiB/s | 34.48 / 66.40 | 2,641 ops/s, 2,641.2 MiB/s | 5.93 / 9.56 |
| AwanFS | 8 MiB | 128 | 25 ops/s, 196.9 MiB/s | 56 ops/s, 446.0 MiB/s | 296.35 / 361.57 | 380 ops/s, 3,039.5 MiB/s | 31.00 / 138.05 |

### Per-step counters (`AwanFS`)

| Size | Step | `/proc/meminfo` Cached | `/proc/meminfo` Dirty | `/proc/<pid>/io` read_bytes | `/proc/<pid>/io` rchar | `/proc/<pid>/io` syscr | `awanfs_disk_reads_total` | `awanfs_payload_cache_hits_total` | `awanfs_payload_cache_misses_total` | `awanfs_payload_cache_bytes` | `awanfs_volume_used_bytes` | `awanfs_payload_copies_total` | `awanfs_payload_copy_bytes_total` |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 MiB | 1. after_write | 3,053.4 MiB (3,201,765,376) | 170.2 MiB (178,470,912) | 0.0 MiB (0) | 0.0 MiB (19,403) | 39 | 0 | 0 | 0 | 1,006,632,960 | 1,077,940,224 | 1,024 | 1,073,741,824 |
| 1 MiB | 2. after_drop | 326.5 MiB (342,372,352) | 0.1 MiB (118,784) | - | - | - | - | - | - | - | - | - | - |
| 1 MiB | 3. after_restart_before_get | 397.0 MiB (416,288,768) | 0.2 MiB (249,856) | 71.0 MiB (74,420,224) | 130.0 MiB (136,311,794) | 2,089 | 1,024 | 0 | 0 | 0 | 1,077,940,224 | 0 | 0 |
| 1 MiB | 4. after_cold_pass | 1,362.5 MiB (1,428,664,320) | 0.5 MiB (528,384) | 1,034.4 MiB (1,084,698,624) | 130.0 MiB (136,311,794) | 2,089 | 1,024 | 0 | 1,024 | 1,006,632,960 | 1,077,940,224 | 0 | 0 |
| 8 MiB | 1. after_write | 1,359.5 MiB (1,425,514,496) | 0.9 MiB (966,656) | 0.0 MiB (4,096) | 0.0 MiB (19,403) | 39 | 0 | 0 | 0 | 0 | 1,074,270,208 | 128 | 1,073,741,824 |
| 8 MiB | 2. after_drop | 326.5 MiB (342,405,120) | 0.2 MiB (180,224) | - | - | - | - | - | - | - | - | - | - |
| 8 MiB | 3. after_restart_before_get | 340.9 MiB (357,466,112) | 0.2 MiB (233,472) | 15.0 MiB (15,716,352) | 18.0 MiB (18,833,650) | 297 | 128 | 0 | 0 | 0 | 1,074,270,208 | 0 | 0 |
| 8 MiB | 4. after_cold_pass | 1,357.0 MiB (1,422,876,672) | 0.3 MiB (266,240) | 1,031.0 MiB (1,081,085,952) | 1,042.5 MiB (1,093,099,774) | 1,452 | 1,280 | 0 | 0 | 0 | 1,074,270,208 | 0 | 0 |

### Restart log tails

- 1 MiB (`scaling_awanfs_45531.log.restart`):
  `Compaction: auto=on rate=50 MB/s | Lifecycle sweep every 3600s | S3 Storage Server listening on http://127.0.0.1:45531 (profile default)`
- 8 MiB (`scaling_awanfs_45873.log.restart`):
  `Compaction: auto=on rate=50 MB/s | Lifecycle sweep every 3600s | S3 Storage Server listening on http://127.0.0.1:45873 (profile default)`
