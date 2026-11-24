Command: `gh workflow run bench-boot.yml --ref issue/324-boot-windows-scan -f objects=10000000 -f size=64` (run 45406079452, commit ba9b777)

# Windows and Linux boot split and 10M replay after 1 MiB batched volume scans (#324)

Batching `VolumeManager::scan_needles` and `VolumeManager::open_with` into 1 MiB sequential aligned reads (`SCAN_BATCH_BYTES = 1 << 20`, Owner Decision 7A) reduced `windows-latest` 10M boot time from `3,391.760` s to `260.546` s (`13.02x` faster) and brought Windows boot to `1.38x` of `ubuntu-latest` (`260.546` s against `188.898` s, meeting the `<= 2x` gate). Linux warm boot improved from `254.904` s to `188.898` s (`<= 254.9` s gate), with `0` of `1,000` sampled objects wrong on both platforms.

## 10M objects (41,637,609,768 bytes across 2 volumes)

Comparison between baseline run [44664190258](https://github.com/iskandarnorazli/Awanfs/actions/runs/44664190258) (`measurements/2026-11-20-boot-full-replay.md`) and post-fix run [45406079452](https://github.com/iskandarnorazli/Awanfs/actions/runs/45406079452) on GitHub-hosted 4-vCPU runners (`ubuntu-latest` job [138487065041](https://github.com/iskandarnorazli/Awanfs/actions/runs/45406079452/job/138487065041), `windows-latest` job [138487065643](https://github.com/iskandarnorazli/Awanfs/actions/runs/45406079452/job/138487065643)).

| 10M objects of 64 bytes | `ubuntu-latest` before | `ubuntu-latest` after | `windows-latest` before | `windows-latest` after |
|---|---|---|---|---|
| `fill`, s | 199.579 | 185.305 | 116.386 | 115.034 |
| `fill`, objects/s | 50,105 | 53,965 | 85,921 | 86,931 |
| `open_secs` (`VolumeManager::open`), s | not measured | 79.734 | not measured | 101.226 |
| `scan` (`scan_needles`), s | not measured | 80.480 | not measured | 100.396 |
| `scan` needles/s | not measured | 124,255 | not measured | 99,606 |
| `scan` `read_calls` (`WorkCounters::disk_read`) | 10,000,000 (inferred: 1/needle) | 39,063 | 10,000,000 (inferred: 1/needle) | 39,063 |
| `boot_cold`, s (after `drop_caches`) | 256.257 | 197.390 | not measured (no cache drop on Windows) | not measured (no cache drop on Windows) |
| `boot` (warm), s | 254.904 | 188.898 | 3,391.760 | 260.546 |
| `boot`, objects/s | 39,231 | 52,939 | 2,948 | 38,381 |
| `apply` (`boot - open_secs - scan`), s | not measured | 28.684 | not measured | 58.924 |
| `apply` share of warm `boot` | not measured | 15.2% | not measured | 22.6% |
| `boot` peak RSS, MiB | 1,594.766 | 1,594.812 | 1,244.559 | 1,382.082 |
| `boot` peak heap, MiB | not measured | 1,349.260 | not measured | 1,349.272 |
| `boot` retained heap, MiB | not measured | 576.053 | not measured | 576.065 |
| sampled objects wrong | 0 of 1,000 | 0 of 1,000 | 0 of 1,000 | 0 of 1,000 |

## 1M objects scan and apply split (4,215,516,840 bytes in 1 volume)

Comparison between baseline run [44990360137](https://github.com/iskandarnorazli/Awanfs/actions/runs/44990360137) (`measurements/2026-11-22-boot-scan.md`) and PR run [45406074833](https://github.com/iskandarnorazli/Awanfs/actions/runs/45406074833) on commit `ba9b777`.

| 1M objects of 64 bytes | `ubuntu-latest` before | `ubuntu-latest` after | `windows-latest` before | `windows-latest` after |
|---|---|---|---|---|
| `open_secs` (`VolumeManager::open`), s | not measured | 0.508 | not measured | 0.981 |
| `scan` (`scan_needles`), s | 0.870 | 0.499 | 2.234 | 1.000 |
| `scan` `read_calls` | 1,000,000 | 3,907 | 1,000,000 | 3,907 |
| `boot_cold`, s | 12.481 | 14.695 | not measured | not measured |
| `boot` (warm), s | 3.424 | 2.874 | 6.210 | 4.183 |
| `apply` (`boot - open_secs - scan`), s | 2.554 (`boot - scan`) | 1.867 | 3.976 (`boot - scan`) | 2.202 |
| sampled objects wrong | 0 of 1,000 | 0 of 1,000 | 0 of 1,000 | 0 of 1,000 |

## Benchmark checklist notes (explain the number)

- **Read calls reduction (measured):** `scan_needles` `read_calls` dropped from `1,000,000` to `3,907` at 1M (`256x` reduction) and to `39,063` across `41.64 GB` at 10M.
- **Why 10M saw 13x speedup while 1M saw 1.5x (measured):** At 1M (`4.22 GB`), the volume fit inside runner RAM, so OS caching hid individual 4 KiB syscall cost (`6.210` s). At 10M (`41.64 GB`), volumes exceeded runner RAM; unbatched 4 KiB reads in both `VolumeManager::open_with` and `VolumeManager::scan_needles` stalled on Windows disk I/O (`3,391.760` s). With 1 MiB sequential reads, `open` streams at `411 MB/s` (`101.226` s) and `scan_needles` streams at `415 MB/s` (`100.396` s) on Windows.
- **Current limiter (measured from split):** Sequential disk read bandwidth across the two full-volume passes (`open_secs + scan` = `201.622` s on Windows, `160.214` s on Linux) accounts for `77.4%` of Windows 10M boot and `84.8%` of Linux 10M boot. The `apply` phase takes `58.924` s on Windows (`22.6%`) and `28.684` s on Linux (`15.2%`), so `SpillIndex` sync deferral is not needed to meet the `2x` gate.
- **Run count and validity:** One 10M dispatch (`45406079452`) and one 1M PR run (`45406074833`) after the fix, compared against the prior single 10M (`44664190258`) and 1M (`44990360137`) runs on GitHub-hosted runners. Correctness verified with `0` of `1,000` sampled objects wrong on every run.

## Verbatim `boot.jsonl` output

10M dispatch run `45406079452`:

```json
ubuntu-latest
{"phase":"fill","objects":10000000,"size":64,"secs":185.305,"objects_per_s":53965,"volumes":2,"bytes_on_disk":41637609768}
{"phase":"scan","volumes":2,"open_secs":79.734,"secs":80.480,"needles":10000000,"needles_per_s":124255,"read_calls":39063}
{"phase":"boot_cold","objects":10000000,"secs":197.390,"objects_per_s":50661,"peak_rss_mib":1594.594,"peak_heap_mib":1349.260,"heap_after_mib":576.053,"checked":1000,"wrong":0}
{"phase":"boot","objects":10000000,"secs":188.898,"objects_per_s":52939,"peak_rss_mib":1594.812,"peak_heap_mib":1349.260,"heap_after_mib":576.053,"checked":1000,"wrong":0}
windows-latest
{"phase":"fill","objects":10000000,"size":64,"secs":115.034,"objects_per_s":86931,"volumes":2,"bytes_on_disk":41637609768}
{"phase":"scan","volumes":2,"open_secs":101.226,"secs":100.396,"needles":10000000,"needles_per_s":99606,"read_calls":39063}
{"phase":"boot","objects":10000000,"secs":260.546,"objects_per_s":38381,"peak_rss_mib":1382.082,"peak_heap_mib":1349.272,"heap_after_mib":576.065,"checked":1000,"wrong":0}
```

1M PR run `45406074833`:

```json
ubuntu-latest
{"phase":"fill","objects":1000000,"size":64,"secs":23.363,"objects_per_s":42802,"volumes":1,"bytes_on_disk":4215516840}
{"phase":"scan","volumes":1,"open_secs":0.508,"secs":0.499,"needles":1000000,"needles_per_s":2004470,"read_calls":3907}
{"phase":"boot_cold","objects":1000000,"secs":14.695,"objects_per_s":68053,"peak_rss_mib":241.934,"peak_heap_mib":197.362,"heap_after_mib":125.317,"checked":1000,"wrong":0}
{"phase":"boot","objects":1000000,"secs":2.874,"objects_per_s":347945,"peak_rss_mib":241.934,"peak_heap_mib":197.362,"heap_after_mib":125.317,"checked":1000,"wrong":0}
windows-latest
{"phase":"fill","objects":1000000,"size":64,"secs":16.280,"objects_per_s":61423,"volumes":1,"bytes_on_disk":4215516840}
{"phase":"scan","volumes":1,"open_secs":0.981,"secs":1.000,"needles":1000000,"needles_per_s":999841,"read_calls":3907}
{"phase":"boot","objects":1000000,"secs":4.183,"objects_per_s":239036,"peak_rss_mib":203.367,"peak_heap_mib":197.374,"heap_after_mib":125.329,"checked":1000,"wrong":0}
```
