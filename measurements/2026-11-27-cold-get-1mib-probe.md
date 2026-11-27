Command: `gh workflow run bench-scaling.yml --ref try/460-cold-get-probe` (run 45962606934, commit a4aab0a; invoked `python benchmarks/core_scaling.py --cold-get --no-fetch-minio --compare-fast --sizes 1MiB,8MiB --servers AwanFS --cold-set 1GiB --cold-concurrency 16 --cold-cores 2 --data-dir /mnt/bench --output /home/runner/work/_temp/core_scaling/cold_get.md`)

# Cold GET probe (#460, 2026-11-27 08:04 UTC)

- **Machine:** runnervm0hi8g (Linux-7.0.0-1020-azure-x86_64-with-glibc2.43, 4 logical cores, 15.6 GB RAM, CPU: AMD EPYC 9V74 80-Core Processor)
- **Method:** per size, 1024 MiB of objects is written with 16 connections, the server is stopped, `sync` and `echo 3 > /proc/sys/vm/drop_caches` drop the page cache, and the server restarts on the same data before reading every object once cold and once warm. Server pinned to 2 cores, client to [2, 3] (shares cores: False).

## Cold and warm GET throughput

| Server | Size | Objects | PUT (writing the set) | Cold GET | Cold P50 / P99 ms | Warm GET | Warm P50 / P99 ms |
|---|---|---|---|---|---|---|---|
| AwanFS | 1 MiB | 1024 | 157 ops/s, 157.4 MiB/s | 2,161 ops/s, 2,161.3 MiB/s | 7.13 / 12.39 | 1,963 ops/s, 1,963.4 MiB/s | 7.93 / 13.45 |
| AwanFS | 8 MiB | 128 | 24 ops/s, 193.4 MiB/s | 54 ops/s, 435.2 MiB/s | 283.83 / 477.48 | 285 ops/s, 2,276.7 MiB/s | 43.66 / 165.53 |

## Per-step counters (`AwanFS`)

| Size | Step | `/proc/meminfo` Cached | `/proc/meminfo` Dirty | `/proc/<pid>/io` read_bytes | `/proc/<pid>/io` rchar | `/proc/<pid>/io` syscr | `awanfs_disk_reads_total` | `awanfs_payload_cache_hits_total` | `awanfs_payload_cache_misses_total` | `awanfs_payload_cache_bytes` | `awanfs_volume_used_bytes` | `awanfs_payload_copies_total` | `awanfs_payload_copy_bytes_total` |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 MiB | 1. after_write | 3,069.5 MiB (3,218,608,128) | 2.6 MiB (2,736,128) | 0.0 MiB (0) | 0.0 MiB (19,403) | 39 | 0 | 0 | 0 | 1,006,632,960 | 1,077,940,224 | 1,024 | 1,073,741,824 |
| 1 MiB | 2. after_drop | 338.3 MiB (354,721,792) | 0.2 MiB (176,128) | - | - | - | - | - | - | - | - | - | - |
| 1 MiB | 3. after_restart_before_get | 1,371.2 MiB (1,437,822,976) | 0.2 MiB (237,568) | 1,033.7 MiB (1,083,920,384) | 1,089.1 MiB (1,141,961,714) | 2,089 | 1,024 | 0 | 0 | 0 | 1,077,940,224 | 0 | 0 |
| 1 MiB | 4. after_cold_pass | 1,373.9 MiB (1,440,661,504) | 0.4 MiB (425,984) | 1,034.3 MiB (1,084,534,784) | 1,089.1 MiB (1,141,961,714) | 2,089 | 1,024 | 0 | 1,024 | 1,006,632,960 | 1,077,940,224 | 0 | 0 |
| 8 MiB | 1. after_write | 1,371.1 MiB (1,437,659,136) | 0.9 MiB (958,464) | 0.0 MiB (4,096) | 0.0 MiB (19,403) | 39 | 0 | 0 | 0 | 0 | 1,074,270,208 | 128 | 1,073,741,824 |
| 8 MiB | 2. after_drop | 338.1 MiB (354,525,184) | 0.2 MiB (184,320) | - | - | - | - | - | - | - | - | - | - |
| 8 MiB | 3. after_restart_before_get | 506.8 MiB (531,374,080) | 0.2 MiB (249,856) | 169.5 MiB (177,754,112) | 137.0 MiB (143,679,730) | 297 | 128 | 0 | 0 | 0 | 1,074,270,208 | 0 | 0 |
| 8 MiB | 4. after_cold_pass | 1,368.3 MiB (1,434,816,512) | 0.3 MiB (282,624) | 1,031.0 MiB (1,081,032,704) | 1,161.5 MiB (1,217,945,858) | 1,453 | 1,280 | 0 | 0 | 0 | 1,074,270,208 | 0 | 0 |

### 1 MiB restart log tail (`scaling_awanfs_45883.log.restart`)

```
Worker threads: 2 (available cores) | Lifecycle sweep every 3600s | S3 Storage Server listening on http://127.0.0.1:45883 (profile default)
```

### 8 MiB restart log tail (`scaling_awanfs_55395.log.restart`)

```
Worker threads: 2 (available cores) | Lifecycle sweep every 3600s | S3 Storage Server listening on http://127.0.0.1:55395 (profile default)
```

## Conclusion

The harness is not at fault: **AwanFS itself read all 1 GiB of 1 MiB object payloads into the OS page cache during restart, before a single GET ran.**

- At `3. after_restart_before_get` for 1 MiB objects (`1,024` objects, `1,077,940,224` B volume):
  - `/proc/meminfo` `Cached` jumped by `+1,032.9 MiB` (`338.3` -> `1,371.2 MiB`).
  - `/proc/<pid>/io` `read_bytes` reached `1,033.7 MiB` (`1,083,920,384` B) across `2,089` `syscr` calls (`rchar = 1,141,961,714` B = `1,089.1 MiB`), and `awanfs_disk_reads_total = 1,024` (note `open_with` passes `work: None` while `scan_needles` passes `work: Some(&self.work)`).
  - Then `4. after_cold_pass` added only `+0.6 MiB` `read_bytes` (`syscr` stayed `2,089`, `0` additional read syscalls), so every 1 MiB "cold" GET hit warm Linux page cache at `2,161.3 MiB/s`.
- At 8 MiB (`128` objects, `1,074,270,208` B volume), restart still read `169.5 MiB` (`rchar = 137.0 MiB`, `awanfs_disk_reads_total = 128`, ~1 MiB per needle), leaving the remaining `~861 MiB` outside page cache so `4. after_cold_pass` read `1,031.0 MiB` from disk at `435.2 MiB/s`.
- Why: `VolumeManager::open_with` (`server/src/volume.rs:1028`, #412) calls `window.cap_next_refill(SKIP_REFILL_BYTES)` after skipping a large journaled needle, whereas `VolumeManager::scan_needles` (`server/src/volume.rs:1930-2027`) creates an `AlignedScanWindow` (`SCAN_BATCH_BYTES = 1 MiB`) and never caps its refill after advancing past a needle with `total_len > SCAN_BATCH_BYTES`, causing one `1 MiB` aligned read per needle (~65 MiB in `open_with` + `1,024 * 1 MiB = 1,024 MiB` in `scan_needles` = `~1,089 MiB` `rchar`).

Follow-up bug filed in #512.
