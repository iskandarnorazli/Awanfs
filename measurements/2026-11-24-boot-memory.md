`$bin = "server/target/release/examples/boot_bench.exe"; & $bin fill --dir $d --objects 1000000 --size 64; & $bin boot --dir $d --objects 1000000 --size 64` (Windows local Step 1 baseline on commit 2f56dab; CI 10M `bench-boot.yml` runs recorded below once completed)

# Boot memory breakdown by part (#323)

Measurement for Issue #323 (Owner Decision 7A): boot peak heap and retained heap broken down by part (`needle_records`, `checksum_map`, `hot_table`, `spill_index`, `other`) using the `Counting` `GlobalAlloc` in `server/examples/boot_bench.rs`.

## Before (local Windows, 4-vCPU / C: drive, `boot_bench` Step 1 instrumentation on `2f56dab`)

| Objects | `secs` | `peak_rss_mib` | `peak_heap_mib` | `heap_after_mib` | `needle_records` peak (after) | `checksum_map` peak (after) | `hot_table` peak (after) | `spill_index` peak (after) | `other` peak (after) | Wrong |
|---|---|---|---|---|---|---|---|---|---|---|
| 250,000 | 0.773 | 85.664 | 79.341 | 79.341 | 23.292 (0.000) | 18.750 (12.500) | 25.871 (25.871) | 24.063 (4.426) | 36.556 (36.543) | 0 of 1,000 |
| 500,000 | 1.670 | 133.691 | 125.699 | 94.704 | 45.583 (0.000) | 37.500 (25.000) | 25.871 (25.871) | 24.518 (4.866) | 38.983 (38.966) | 0 of 1,000 |
| 1,000,000 | 3.828 | 204.148 | 197.415 | 125.370 | 90.166 (0.000) | 75.000 (50.000) | 25.871 (25.871) | 25.407 (5.679) | 57.878 (43.819) | 0 of 1,000 |

Raw JSON lines (before):

```text
{"phase":"fill","objects":250000,"size":64,"secs":3.339,"objects_per_s":74864,"volumes":1,"bytes_on_disk":1231226824}
{"phase":"boot","objects":250000,"secs":0.773,"objects_per_s":323316,"peak_rss_mib":85.664,"peak_heap_mib":79.341,"heap_after_mib":79.341,"heap_parts_mib":{"needle_records":23.292,"checksum_map":18.750,"hot_table":25.871,"spill_index":24.063,"other":36.556},"heap_parts_after_mib":{"needle_records":0.000,"checksum_map":12.500,"hot_table":25.871,"spill_index":4.426,"other":36.543},"checked":1000,"wrong":0}
{"phase":"fill","objects":500000,"size":64,"secs":6.580,"objects_per_s":75983,"volumes":1,"bytes_on_disk":2315468648}
{"phase":"boot","objects":500000,"secs":1.670,"objects_per_s":299370,"peak_rss_mib":133.691,"peak_heap_mib":125.699,"heap_after_mib":94.704,"heap_parts_mib":{"needle_records":45.583,"checksum_map":37.500,"hot_table":25.871,"spill_index":24.518,"other":38.983},"heap_parts_after_mib":{"needle_records":0.000,"checksum_map":25.000,"hot_table":25.871,"spill_index":4.866,"other":38.966},"checked":1000,"wrong":0}
{"phase":"fill","objects":1000000,"size":64,"secs":12.182,"objects_per_s":82091,"volumes":1,"bytes_on_disk":4215516840}
{"phase":"boot","objects":1000000,"secs":3.828,"objects_per_s":261242,"peak_rss_mib":204.148,"peak_heap_mib":197.415,"heap_after_mib":125.370,"heap_parts_mib":{"needle_records":90.166,"checksum_map":75.000,"hot_table":25.871,"spill_index":25.407,"other":57.878},"heap_parts_after_mib":{"needle_records":0.000,"checksum_map":50.000,"hot_table":25.871,"spill_index":5.679,"other":43.819},"checked":1000,"wrong":0}
```

## What Step 1 showed

1. `needle_records` (`events: Vec<ScannedNeedle>`) scales linearly at ~90 MiB per 1M needles per volume (467 to 773 MiB transient peak at 5M needles per volume).
2. `checksum_map` (`needle_crc_map`) retains 50 MiB at 1M objects (75 MiB peak during `HashMap` resize), scaling to 384 MiB retained / 576 MiB peak at 10M objects.
3. `hot_table` stays flat at 25.871 MiB (capped at `OpenLimits::default().max_objects = 100,000`).
4. `spill_index` peaks at 25.407 MiB during memtable flushes and retains 5.679 MiB at 1M objects (~1.8 B/object growth for segment fences and Bloom filters).
5. `other` holds ~34 MiB fixed (`PayloadCache::new` `LruCache` entry table) plus `VolumeManager::open_with` journal buffer (`idx_bytes`, 40 MiB transient at 1M) and `VolumeManager::indexed` (`BTreeSet<u64>`, ~9.7 MiB retained at 1M, scaling to ~97 MiB retained plus ~200 MiB transient at 10M).

## After (local Windows, 4-vCPU / C: drive, `boot_bench` after streaming `scan_needles`, removing `needle_crc_map`, and streaming `.idx` journal)

| Objects | `secs` | `peak_rss_mib` | `peak_heap_mib` | `heap_after_mib` | `needle_records` peak (after) | `checksum_map` peak (after) | `hot_table` peak (after) | `spill_index` peak (after) | `other` peak (after) | Wrong |
|---|---|---|---|---|---|---|---|---|---|---|
| 250,000 | 1.221 | 59.738 | 64.412 | 64.412 | 1.000 (0.000) | 0.000 (0.000) | 25.871 (25.871) | 24.063 (4.426) | 34.127 (34.115) | 0 of 1,000 |
| 500,000 | 1.657 | 60.465 | 64.848 | 64.848 | 1.000 (0.000) | 0.000 (0.000) | 25.871 (25.871) | 24.518 (4.866) | 34.127 (34.111) | 0 of 1,000 |
| 1,000,000 | 3.871 | 61.344 | 65.659 | 65.659 | 1.000 (0.000) | 0.000 (0.000) | 25.871 (25.871) | 25.407 (5.679) | 34.127 (34.108) | 0 of 1,000 |

Raw JSON lines (after, local Windows):

```text
{"phase":"fill","objects":250000,"size":64,"secs":3.763,"objects_per_s":66441,"volumes":1,"bytes_on_disk":1231226824}
{"phase":"boot","objects":250000,"secs":1.221,"objects_per_s":204689,"peak_rss_mib":59.738,"peak_heap_mib":64.412,"heap_after_mib":64.412,"heap_parts_mib":{"needle_records":1.000,"checksum_map":0.000,"hot_table":25.871,"spill_index":24.063,"other":34.127},"heap_parts_after_mib":{"needle_records":0.000,"checksum_map":0.000,"hot_table":25.871,"spill_index":4.426,"other":34.115},"checked":1000,"wrong":0}
{"phase":"fill","objects":500000,"size":64,"secs":7.826,"objects_per_s":63890,"volumes":1,"bytes_on_disk":2315468648}
{"phase":"boot","objects":500000,"secs":1.657,"objects_per_s":301771,"peak_rss_mib":60.465,"peak_heap_mib":64.848,"heap_after_mib":64.848,"heap_parts_mib":{"needle_records":1.000,"checksum_map":0.000,"hot_table":25.871,"spill_index":24.518,"other":34.127},"heap_parts_after_mib":{"needle_records":0.000,"checksum_map":0.000,"hot_table":25.871,"spill_index":4.866,"other":34.111},"checked":1000,"wrong":0}
{"phase":"fill","objects":1000000,"size":64,"secs":15.643,"objects_per_s":63926,"volumes":1,"bytes_on_disk":4215516840}
{"phase":"boot","objects":1000000,"secs":3.871,"objects_per_s":258315,"peak_rss_mib":61.344,"peak_heap_mib":65.659,"heap_after_mib":65.659,"heap_parts_mib":{"needle_records":1.000,"checksum_map":0.000,"hot_table":25.871,"spill_index":25.407,"other":34.127},"heap_parts_after_mib":{"needle_records":0.000,"checksum_map":0.000,"hot_table":25.871,"spill_index":5.679,"other":34.108},"checked":1000,"wrong":0}
```

At 1,000,000 objects locally on Windows, `peak_rss_mib` dropped from `204.148 MiB` to `61.344 MiB` (`-69.9%`) and `peak_heap_mib` dropped from `197.415 MiB` to `65.659 MiB` (`-66.7%`). `needle_records` stays flat at `1.000 MiB` (`AlignedScanWindow`), `checksum_map` is `0.000 MiB`, `hot_table` stays flat at `25.871 MiB`, and `other` stays flat at `34.127 MiB`.

## CI 10M `bench-boot.yml` (Linux and Windows)

Pending PR dispatch.
