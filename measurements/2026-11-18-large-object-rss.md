gh workflow run bench-selfhosted.yml --ref issue/133-large-object-rss -f runs=3 -f duration=30

# Large-Object Peak RSS & In-Flight Request Bytes (#133)

- **GitHub Actions Run:** `44257320568` (https://github.com/iskandarnorazli/Awanfs/actions/runs/44257320568)
- **Commit / Branch:** `9454e62` (`issue/133-large-object-rss`)
- **Inner Matrix Command:** `python benchmarks/run_warp_benchmark.py --sizes 64KiB,1MiB,1GiB --concurrent 64 --duration 30s --runs 3 --rss-ceiling-mib 2500.0 --output selfhosted_full.md`
- **Machine:** `LAPTOP-6RBIQUM5`, `AMD64 Family 25 Model 80 Stepping 0, AuthenticAMD`, 16 logical CPUs, 31.4 GB host RAM, `Windows 11 (AMD64)`
- **Affinity Pinning:** Server Cores 0..3 (Affinity Mask 0x0F); Client Cores 4..7 (Affinity Mask 0xF0)
- **Rounds per Server:** 3 (values are medians; Peak RSS shows median and min..max spread)
- **Binaries:** `s3-server` `--release`; MinIO `minio.exe RELEASE.2025-09-07T16-13-09Z` (`DEVELOPMENT.GOGET`)
- **Durability per PUT:** `s3-server` runs with its default, group commit (#29): a 200 means the bytes are on stable storage, and concurrent PUTs share one sync. MinIO does not flush these PUTs on Windows (its fdatasync is a no-op there), so its PUT and mixed rows on this machine are not durable writes and are not like for like.
- **`_rss_ceiling_source`:** Run `44257320568` (3 rounds on `LAPTOP-6RBIQUM5`) measured AwanFS overall median peak RSS at `1210.5 MiB` (spread `1192.8..1217.2` on `get_10g`); ceiling set to `2500.0 MiB` (`~= 2.07x`).

## Peak RSS per Workload (3-Round Median and min..max Spread)

| Workload | Phase | AwanFS Peak RSS (MB) | MinIO Peak RSS (MB) | AwanFS Throughput (MiB/s) | MinIO Throughput (MiB/s) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **64 KiB PUT (Single Object)** | `put_64k` | **110.5 MB (109.9..110.5)** | 548.1 MB (528.6..562.4) | 222.9 | 8.2 |
| **64 KiB GET (Single Object)** | `get_64k` | **110.5 MB (109.9..110.5)** | 549.0 MB (544.8..562.4) | 323.9 | 216.2 |
| **64 KiB Mixed (80% GET / 20% PUT)** | `mixed_64k` | **297.4 MB (295.7..297.7)** | 946.0 MB (926.6..965.4) | 273.0 | 36.7 |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | `range_get_4k` | **110.5 MB (109.9..110.5)** | 554.8 MB (551.6..562.4) | 21.0 | 14.0 |
| **1 MiB PUT (Single Object)** | `put_1m` | **297.4 MB (295.7..297.7)** | 600.3 MB (594.4..610.3) | 192.6 | 111.0 |
| **1 MiB GET (Single Object)** | `get_1m` | **297.4 MB (295.7..297.7)** | 973.8 MB (965.4..982.1) | 1980.7 | 1309.3 |
| **1 MiB Mixed (80% GET / 20% PUT)** | `mixed_1m` | **297.7 MB (297.4..299.1)** | 994.6 MB (937.1..1019.8) | 721.8 | 479.7 |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | `mpu_1m` | **297.4 MB (295.7..297.7)** | 965.4 MB (918.2..982.1) | 151.0 | N/A (S3 >=5MiB spec limit) |
| **1 GiB Multipart Upload (16x64MiB Parts + Complete)** | `mpu_put_1g` | **297.7 MB (297.4..299.1)** | 994.6 MB (937.1..1019.8) | 142.9 | 1011.8 |
| **1 GiB Multipart Range GET (16x64MiB Parallel Ranges)** | `mpu_get_1g` | **1166.3 MB (1153.5..1176.2)** | 994.6 MB (937.1..1019.8) | 967.3 | 1164.1 |
| **10 GiB Volume PUT (160x64MiB Streamed Parts)** | `put_10g` | **1180.0 MB (1170.2..1186.9)** | 994.6 MB (937.1..1019.8) | 0.0 (did not finish: 8 workers did not finish their request) | 215.9 |
| **10 GiB Volume GET (160x64MiB Parallel Ranges)** | `get_10g` | **1210.5 MB (1192.8..1217.2)** | 994.6 MB (937.1..1019.8) | 725.1 | 1227.5 |

## Peak In-Flight Request Bytes (AwanFS)

| Phase | Peak In-Flight (MB) | Peak RSS (MB) |
| :--- | :--- | :--- |
| `put_64k` | 4.00 | 110.5 MB (109.9..110.5) |
| `get_64k` | 4.00 | 110.5 MB (109.9..110.5) |
| `range_get_4k` | 4.00 | 110.5 MB (109.9..110.5) |
| `put_1m` | 32.00 | 297.4 MB (295.7..297.7) |
| `get_1m` | 32.00 | 297.4 MB (295.7..297.7) |
| `mpu_1m` | 32.00 | 297.4 MB (295.7..297.7) |
| `mixed_64k` | 32.00 | 297.4 MB (295.7..297.7) |
| `mixed_1m` | 32.00 | 297.7 MB (297.4..299.1) |
| `mpu_put_1g` | 32.00 | 297.7 MB (297.4..299.1) |
| `mpu_get_1g` | 32.00 | 1166.3 MB (1153.5..1176.2) |
| `put_10g` | 32.00 | 1180.0 MB (1170.2..1186.9) |
| `get_10g` | 32.00 | 1210.5 MB (1192.8..1217.2) |

## Peak RSS Ceiling Check

| Server | Median Peak RSS (MiB) | Ceiling (MiB) | Verdict |
| :--- | :--- | :--- | :--- |
| AwanFS | 1210.5 | 2500.0 | ok |
| MinIO | 994.6 | - | - |

## Notes from the Run

- `_rss_ceiling_source`: Run `44257320568` (`1210.5 MiB` AwanFS median peak RSS -> ceiling `2500.0 MiB` ~= `2.07x`).
- In `selfhosted_full.md`, `10 GiB Volume PUT (160x64MiB Streamed Parts)` for AwanFS recorded `did not finish: 8 workers did not finish their request`, while still recording Peak RSS `1180.0 MB (1170.2..1186.9)` and Peak In-Flight `32.00 MB`.
- In `selfhosted_ratio.md` from the same run (`44257320568`), AwanFS small-object peak RSS was `120.2` MiB (ceiling `250.0` MiB, `ok`) vs MinIO `650.1` MiB.
