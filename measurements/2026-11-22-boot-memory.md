Command: `gh workflow run bench-boot.yml --ref issue/328-boot-memory -f objects=1000000` (run 44990465043), `gh workflow run bench-boot.yml --ref issue/328-boot-memory -f objects=2000000` (run 44990466962), `gh workflow run bench-boot.yml --ref issue/328-boot-memory -f objects=5000000` (run 44990469289), `gh workflow run bench-boot.yml --ref issue/328-boot-memory -f objects=10000000` (run 44990471523) on commit 5896ab9

# Boot memory against the object count (1M to 10M, #328)

Measurement for #323 step 1 (#328): how boot peak heap, retained heap after boot (`heap_after_mib`), and peak RSS scale across 1M, 2M, 5M, and 10M 64-byte objects on GitHub-hosted 4-vCPU runners (`ubuntu-latest` and `windows-latest`). `peak_heap_mib` and `heap_after_mib` come from `Counting` (`GlobalAlloc` in `server/examples/boot_bench.rs`), reset right before `S3ServiceState::try_with_data_dir` and read right after it returns. All runs verified 1,000 sampled objects with 0 wrong.

## Measured table (warm `boot` phase)

| Objects | Platform | Run / Job | `peak_heap_mib` | `heap_after_mib` | `peak_rss_mib` | `secs` | Cold `secs` (`peak_rss_mib`) | Volumes |
|---|---|---|---|---|---|---|---|---|
| 1,000,000 | Linux | [44990465043](https://github.com/iskandarnorazli/Awanfs/actions/runs/44990465043/job/137110172129) | 197.362 | 125.317 | 242.004 | 3.373 | 12.248 (242.043) | 1 |
| 1,000,000 | Windows | [44990465043](https://github.com/iskandarnorazli/Awanfs/actions/runs/44990465043/job/137110171971) | 197.374 | 125.329 | 203.512 | 6.223 | not measured (no cache drop on Windows) | 1 |
| 2,000,000 | Linux | [44990466962](https://github.com/iskandarnorazli/Awanfs/actions/runs/44990466962/job/137110177893) | 365.224 | 186.660 | 426.684 | 8.946 | 26.140 (426.508) | 1 |
| 2,000,000 | Windows | [44990466962](https://github.com/iskandarnorazli/Awanfs/actions/runs/44990466962/job/137110177665) | 365.236 | 186.672 | 370.781 | 13.774 | not measured (no cache drop on Windows) | 1 |
| 5,000,000 | Linux | [44990469289](https://github.com/iskandarnorazli/Awanfs/actions/runs/44990469289/job/137110185461) | 991.348 | 320.062 | 905.277 | 85.952 | 91.413 (905.340) | 1 |
| 5,000,000 | Windows | [44990469289](https://github.com/iskandarnorazli/Awanfs/actions/runs/44990469289/job/137110185352) | 991.360 | 320.074 | 734.227 | 539.987 | not measured (no cache drop on Windows) | 1 |
| 10,000,000 | Linux | [44990471523](https://github.com/iskandarnorazli/Awanfs/actions/runs/44990471523/job/137110193529) | 1,349.260 | 576.053 | 1,594.859 | 258.955 | 261.241 (1,594.727) | 2 |
| 10,000,000 | Windows | [44990471523](https://github.com/iskandarnorazli/Awanfs/actions/runs/44990471523/job/137110193319) | 1,349.272 | 576.065 | 1,258.590 | 3,532.387 | not measured (no cache drop on Windows) | 2 |

## Slope in bytes per object

Computed from the MiB figures above (`1 MiB = 1,048,576 bytes`):

| Metric | Platform | `(10M - 1M) / 9,000,000` (bytes/object) | OLS slope across 1M, 2M, 5M, 10M (bytes/object) | OLS intercept (MiB) | OLS `R^2` |
|---|---|---|---|---|---|
| Peak heap (`peak_heap_mib`) | Linux & Windows | 134.21 | 135.09 | 146.054 (Linux) / 146.066 (Windows) | 0.9365 |
| Retained heap (`heap_after_mib`) | Linux & Windows | 52.51 | 51.85 | 79.497 (Linux) / 79.509 (Windows) | 0.9989 |
| Peak RSS (`peak_rss_mib`) | Linux | 157.62 | 156.44 | 120.821 | 0.9976 |
| Peak RSS (`peak_rss_mib`) | Windows | 122.93 | 120.91 | 122.889 | 0.9954 |

Piecewise heap slopes (identical to 2 decimals on both platforms):

- `1M -> 2M`: peak heap `176.02` B/object, retained heap `64.32` B/object.
- `2M -> 5M`: peak heap `218.85` B/object, retained heap `46.63` B/object.
- `5M -> 10M`: peak heap `75.06` B/object, retained heap `53.69` B/object.

## Raw JSON lines

```text
# 1M (run 44990465043)
ubuntu-latest:
{"phase":"fill","objects":1000000,"size":64,"secs":12.347,"objects_per_s":80992,"volumes":1,"bytes_on_disk":4215516840}
{"phase":"boot_cold","objects":1000000,"secs":12.248,"objects_per_s":81647,"peak_rss_mib":242.043,"peak_heap_mib":197.362,"heap_after_mib":125.317,"checked":1000,"wrong":0}
{"phase":"boot","objects":1000000,"secs":3.373,"objects_per_s":296434,"peak_rss_mib":242.004,"peak_heap_mib":197.362,"heap_after_mib":125.317,"checked":1000,"wrong":0}
windows-latest:
{"phase":"fill","objects":1000000,"size":64,"secs":14.774,"objects_per_s":67684,"volumes":1,"bytes_on_disk":4215516840}
{"phase":"boot","objects":1000000,"secs":6.223,"objects_per_s":160702,"peak_rss_mib":203.512,"peak_heap_mib":197.374,"heap_after_mib":125.329,"checked":1000,"wrong":0}

# 2M (run 44990466962)
ubuntu-latest:
{"phase":"fill","objects":2000000,"size":64,"secs":22.768,"objects_per_s":87842,"volumes":1,"bytes_on_disk":8284048680}
{"phase":"boot_cold","objects":2000000,"secs":26.140,"objects_per_s":76510,"peak_rss_mib":426.508,"peak_heap_mib":365.224,"heap_after_mib":186.660,"checked":1000,"wrong":0}
{"phase":"boot","objects":2000000,"secs":8.946,"objects_per_s":223573,"peak_rss_mib":426.684,"peak_heap_mib":365.224,"heap_after_mib":186.660,"checked":1000,"wrong":0}
windows-latest:
{"phase":"fill","objects":2000000,"size":64,"secs":25.466,"objects_per_s":78535,"volumes":1,"bytes_on_disk":8284048680}
{"phase":"boot","objects":2000000,"secs":13.774,"objects_per_s":145199,"peak_rss_mib":370.781,"peak_heap_mib":365.236,"heap_after_mib":186.672,"checked":1000,"wrong":0}

# 5M (run 44990469289)
ubuntu-latest:
{"phase":"fill","objects":5000000,"size":64,"secs":92.958,"objects_per_s":53788,"volumes":1,"bytes_on_disk":20758079656}
{"phase":"boot_cold","objects":5000000,"secs":91.413,"objects_per_s":54697,"peak_rss_mib":905.340,"peak_heap_mib":991.348,"heap_after_mib":320.062,"checked":1000,"wrong":0}
{"phase":"boot","objects":5000000,"secs":85.952,"objects_per_s":58172,"peak_rss_mib":905.277,"peak_heap_mib":991.348,"heap_after_mib":320.062,"checked":1000,"wrong":0}
windows-latest:
{"phase":"fill","objects":5000000,"size":64,"secs":75.855,"objects_per_s":65915,"volumes":1,"bytes_on_disk":20758079656}
{"phase":"boot","objects":5000000,"secs":539.987,"objects_per_s":9259,"peak_rss_mib":734.227,"peak_heap_mib":991.360,"heap_after_mib":320.074,"checked":1000,"wrong":0}

# 10M (run 44990471523)
ubuntu-latest:
{"phase":"fill","objects":10000000,"size":64,"secs":204.843,"objects_per_s":48818,"volumes":2,"bytes_on_disk":41637609768}
{"phase":"boot_cold","objects":10000000,"secs":261.241,"objects_per_s":38279,"peak_rss_mib":1594.727,"peak_heap_mib":1349.260,"heap_after_mib":576.053,"checked":1000,"wrong":0}
{"phase":"boot","objects":10000000,"secs":258.955,"objects_per_s":38617,"peak_rss_mib":1594.859,"peak_heap_mib":1349.260,"heap_after_mib":576.053,"checked":1000,"wrong":0}
windows-latest:
{"phase":"fill","objects":10000000,"size":64,"secs":116.339,"objects_per_s":85956,"volumes":2,"bytes_on_disk":41637609768}
{"phase":"boot","objects":10000000,"secs":3532.387,"objects_per_s":2831,"peak_rss_mib":1258.590,"peak_heap_mib":1349.272,"heap_after_mib":576.065,"checked":1000,"wrong":0}
```

## What the numbers say

- **Identical heap across OSes (measured):** `peak_heap_mib` and `heap_after_mib` match within `0.012 MiB` between Linux and Windows at every count, while Linux `peak_rss_mib` runs `38` to `336 MiB` higher than Windows `peak_rss_mib` because OS allocators, uninitialized spare vector capacity, and resident page accounting differ.
- **Transient replay peak exceeds retained heap (measured):** At 10M objects, `peak_heap_mib` is `1,349.26 MiB` while `heap_after_mib` drops to `576.05 MiB` (`773.21 MiB` freed once replay finishes).
- **Retained heap grows linearly at `~52 B/object` (measured, `R^2 = 0.9989`):** `heap_after_mib` adds `450.74 MiB` across `9M` objects (`52.51 B/object` end-to-end, `51.85 B/object` OLS) on top of a `~79.5 MiB` fixed base. That retained per-object cost matches `needle_crc_map` plus `SpillIndex` segment fences and filters (`server/src/s3_api.rs`, inferred by part).
- **Why peak heap steps down from `218.85` (`2M -> 5M`) to `75.06` B/object (`5M -> 10M`) (inferred from code):** `try_with_data_dir_limits` (`server/src/s3_api.rs`) scans one volume at a time into `events: Vec<ScannedNeedle>` (`server/src/volume.rs`), then drops `events` after applying it. `5M` objects sit in 1 volume where `Vec` capacity doubles past `2^22` (`4,194,304`) to `2^23` (`8,388,608`), leaving unused allocated capacity that explains why `5M` `peak_heap_mib` (`991.35 MiB`) exceeds Linux `peak_rss_mib` (`905.28 MiB`). At `10M` objects (`41.64 GB` > `32 GiB` `MAX_VOLUME_SPAN`), data spans 2 volumes, so `events` still caps at one volume while `needle_crc_map` and `SpillIndex` keep growing across both.
