`bench-boot.yml` on PR #319 (run 44664190258, `boot_bench fill --objects 10000000 --size 64`, then `boot_bench boot` in a fresh process; on Linux once after dropping the page cache, then again)

# Boot from a full replay of the volumes, 10M objects (#26)

Owner decision 2026-11-20 on #26, option 3: boot keeps its full replay, and this measures it. GitHub-hosted runners, 4 vCPUs. `fill` writes through `S3ServiceState::store_object` with durability fast (setup only) and syncs at the end. `boot` times `S3ServiceState::try_with_data_dir`: bucket manifest, every volume scanned, the hot table and the spill index rebuilt. Then 1,000 sampled objects are checked.

| 10M objects of 64 bytes | Linux ([job](https://github.com/iskandarnorazli/Awanfs/actions/runs/44664190258/job/136049383171)) | Windows ([job](https://github.com/iskandarnorazli/Awanfs/actions/runs/44664190258/job/136049382923)) |
|---|---|---|
| fill, s | 199.579 | 116.386 |
| fill, objects per second | 50,105 | 85,921 |
| volumes | 2 | 2 |
| bytes on disk | 41,637,609,768 | 41,637,609,768 |
| boot after dropping the page cache, s | 256.257 | not measured (no cache drop on Windows) |
| boot, s | 254.904 | 3,391.760 |
| boot, objects per second | 39,231 | 2,948 |
| peak RSS while booting, MiB | 1,594.766 (cold: 1,594.660) | 1,244.559 |
| sampled objects wrong after boot | 0 of 1,000 | 0 of 1,000 |

The lines as the run printed them:

```
ubuntu-latest
{"phase":"fill","objects":10000000,"size":64,"secs":199.579,"objects_per_s":50105,"volumes":2,"bytes_on_disk":41637609768}
{"phase":"boot_cold","objects":10000000,"secs":256.257,"objects_per_s":39023,"peak_rss_mib":1594.660,"checked":1000,"wrong":0}
{"phase":"boot","objects":10000000,"secs":254.904,"objects_per_s":39231,"peak_rss_mib":1594.766,"checked":1000,"wrong":0}
windows-latest
{"phase":"fill","objects":10000000,"size":64,"secs":116.386,"objects_per_s":85921,"volumes":2,"bytes_on_disk":41637609768}
{"phase":"boot","objects":10000000,"secs":3391.760,"objects_per_s":2948,"peak_rss_mib":1244.559,"checked":1000,"wrong":0}
```

## What the numbers say, with where each comes from

- **Disk:** 41,637,609,768 bytes for 10M objects is 4,164 bytes per object. A needle is sector aligned (`FORMAT.md`, "Needle (sector aligned, from offset 4096)"), so a 64-byte object takes one 4,096-byte sector, about 65 times its size.
- **Boot time:** `VolumeManager::scan_needles` reads each needle's header with one aligned 4,096-byte read (`server/src/volume.rs`, `read_aligned_at`). That is 10M reads. On Windows each one is unbuffered, and boot runs at 2,948 objects per second against 39,231 on Linux (inferred cause; not profiled).
- **Boot memory:** for each volume, boot first collects every needle record, key included, into a vector, and then applies them in log order (`server/src/s3_api.rs`, "Collect the log first"). With 5M needles per volume that vector dominates the 1.6 GiB peak (inferred; not measured by part). The checksum map adds one entry per needle, about 190 MB at 10M by estimate.

Each is filed as its own issue; checkpoint boot itself is part of #322.
