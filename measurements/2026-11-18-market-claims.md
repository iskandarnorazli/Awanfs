Command: `python benchmarks/market_claims.py --build --profile full --runs 3 --duration 30 --output C:\Users\iskan\Documents\Dev\Awanfs\actions-runner\_work\_temp/selfhosted_market_claims.md`

Run on GitHub Actions: https://github.com/iskandarnorazli/Awanfs/actions/runs/44312933524 (run ID `44312933524`, `LAPTOP-6RBIQUM5`, branch `issue/58-market-claims-bench` at `4911cc3`, 2026-11-18).

# Market-Claims S3 Benchmark Report (2026-11-18 15:28 UTC)

## 1. Hardware, Pinning & Execution Context
- **Profile:** `full`
- **CPU Model:** AMD64 Family 25 Model 80 Stepping 0, AuthenticAMD
- **Logical Cores:** 16
- **Host RAM:** 31.4 GB
- **Operating System:** Windows 11 (AMD64)
- **Server Affinity Pin:** Cores 0..3 (Affinity Mask 0x0F)
- **Client Affinity Pin:** Cores 4..7 (Affinity Mask 0xF0)
- **Runs / Phase Duration:** 3 rounds (alternating server order), 30.0s per phase
- **MinIO Setup:** 4-directory erasure set (`Measured Live (minio.exe version DEVELOPMENT.GOGET (commit-id=DEVELOPMENT.GOGET))`)
- **RustFS Setup:** `Measured Live (rustfs 1.0.5)`
- **MinIO `warp` Client:** Measured (warp.exe version (dev) - (dev))
- **Cold-Read Setup (`--cache-bytes 0`):** `s3-server` exposes `--memory-budget` (minimum `16MiB`, yielding a 6 MiB payload cache) rather than `--cache-bytes 0`; cold-read phases start AwanFS with `--memory-budget 16MiB` and sweep a unique-key working set larger than the cache.

## 2. Durability & Setup Table per Server

| Server / Mode | Invocation / Setup | What a PUT Waits For | Durable on Power Cut? |
| :--- | :--- | :--- | :--- |
| AwanFS (--durability=group, default) | `s3-server --durability group` | Shared group commit (`fsync`/`FlushFileBuffers` of volume + journal before 200) | Yes (shared group commit sync) |
| AwanFS (--durability=strict) | `s3-server --durability strict` | Per-write `fsync`/`FlushFileBuffers` of volume + journal before 200 (no batching) | Yes (per-write sync) |
| AwanFS (--durability=none/fast, NO-SYNC) | `s3-server --durability fast` | Acknowledged in memory before flush; background thread syncs every ~1s | No (~1s background flush window) |
| MinIO (Measured Live (minio.exe version DEVELOPMENT.GOGET (commit-id=DEVELOPMENT.GOGET))) | `minio server dir1..dir4` (4-dir erasure) | MinIO does not flush these PUTs on Windows (its fdatasync is a no-op there), so its PUT and mixed rows on this machine are not durable writes and are not like for like. | No (>4 KiB on Windows) |
| RustFS (Measured Live (rustfs 1.0.5)) | `rustfs server <data_dir>` | Single-drive release default (sync not independently probed here) | Unverified here |

## 3. Throughput, Latency & Resource Spread (`median (min..max)`)

| Workload | Server / Mode | Client | Throughput MiB/s (min..max) | Ops/sec (min..max) | P50 ms (min..max) | P99 ms (min..max) | Peak RSS MB (min..max) | Disk Used MiB | Server CPU s |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **64 KiB PUT (c=C)** | AwanFS (--durability=group, default) | http | 203.8 (156.9..205.2) | 3260.4 (2511.1..3282.5) | 16.80 (16.74..23.31) | 56.10 (53.79..64.96) | 110.5 (109.7..111.1) | 6824.3 (5282.2..6824.5) | 56.81 (41.08..58.38) |
| **64 KiB PUT (c=C)** | AwanFS (--durability=strict) | http | 102.7 (102.1..105.9) | 1643.7 (1633.7..1693.8) | 33.51 (33.18..33.54) | 100.72 (95.39..103.36) | 106.0 (104.2..107.0) | 3483.2 (3483.1..3483.6) | 39.20 (38.62..40.36) |
| **64 KiB PUT (c=C)** | AwanFS (--durability=none/fast, NO-SYNC) | http | 242.7 (148.7..248.4) | 3883.6 (2379.6..3974.4) | 13.40 (13.29..13.40) | 48.86 (46.54..54.94) | 95.0 (94.9..95.2) | 8110.0 (5025.2..8110.1) | 55.39 (32.84..56.16) |
| **64 KiB PUT (c=C)** | MinIO (4-dir erasure) | http | 2.7 (2.6..2.8) | 43.6 (41.1..44.7) | 1457.45 (1425.43..1562.46) | 2138.01 (2069.52..2271.67) | 609.1 (589.4..616.3) | 235.8 (224.3..239.4) | 100.06 (99.89..103.30) |
| **64 KiB PUT (c=C)** | RustFS | http | 8.3 (8.3..8.4) | 132.6 (132.5..135.2) | 468.73 (458.81..473.94) | 600.22 (592.99..1572.58) | 109.6 (108.6..109.7) | 32.3 (32.3..32.4) | 63.70 (60.30..64.08) |
| **64 KiB PUT (c=C)** | AwanFS (--durability=group, default) | `warp` | not measured (Error preparing server: The requested sub-resource is not implemented.) | - | - | - | - | - | - |
| **64 KiB PUT (c=C)** | AwanFS (--durability=strict) | `warp` | not measured (Error preparing server: The requested sub-resource is not implemented.) | - | - | - | - | - | - |
| **64 KiB PUT (c=C)** | AwanFS (--durability=none/fast, NO-SYNC) | `warp` | not measured (Error preparing server: The requested sub-resource is not implemented.) | - | - | - | - | - | - |
| **64 KiB PUT (c=C)** | MinIO (4-dir erasure) | `warp` | 10.3 (9.6..10.3) | 164.3 (154.0..164.4) | 379.91 (377.28..402.67) | 873.33 (854.18..953.86) | 1097.9 (1071.6..1178.0) | - | - |
| **64 KiB PUT (c=C)** | RustFS | `warp` | 17.3 (17.0..18.1) | 277.3 (272.5..289.8) | 226.71 (215.45..227.32) | 303.20 (297.37..324.52) | 183.9 (182.9..184.4) | - | - |
| **64 KiB GET (default cache)** | AwanFS (--durability=group, default) | http | 305.8 (305.3..307.5) | 4892.1 (4885.6..4919.5) | 12.39 (12.26..12.40) | 31.03 (30.84..31.27) | 110.5 (109.7..111.1) | 6858.0 (5316.2..6858.2) | 23.48 (23.12..24.94) |
| **64 KiB GET (default cache)** | AwanFS (--durability=strict) | http | 294.2 (283.5..299.7) | 4707.1 (4535.7..4794.6) | 12.69 (12.68..13.13) | 33.82 (31.63..34.68) | 106.0 (104.2..107.0) | 3515.7 (3514.9..3516.2) | 23.91 (22.81..23.94) |
| **64 KiB GET (default cache)** | AwanFS (--durability=none/fast, NO-SYNC) | http | 300.2 (299.9..302.0) | 4803.3 (4797.7..4831.8) | 12.56 (12.52..12.67) | 31.73 (31.64..32.02) | 95.0 (94.9..95.2) | 8143.3 (5058.3..8143.4) | 23.33 (22.36..24.03) |
| **64 KiB GET (default cache)** | MinIO (4-dir erasure) | http | 92.3 (77.7..93.4) | 1476.9 (1243.1..1494.3) | 40.37 (40.01..47.53) | 95.40 (93.70..128.76) | 676.1 (649.4..678.0) | 300.6 (289.1..304.3) | 82.58 (76.39..82.98) |
| **64 KiB GET (default cache)** | RustFS | http | 265.8 (261.5..271.2) | 4252.4 (4183.4..4339.7) | 14.12 (13.87..14.27) | 36.59 (36.13..37.33) | 159.0 (159.0..159.8) | 32.6 (32.6..32.6) | 48.16 (47.77..48.72) |
| **64 KiB GET (default cache)** | AwanFS (--durability=group, default) | `warp` | not measured (Error preparing server: The requested sub-resource is not implemented.) | - | - | - | - | - | - |
| **64 KiB GET (default cache)** | AwanFS (--durability=strict) | `warp` | not measured (Error preparing server: The requested sub-resource is not implemented.) | - | - | - | - | - | - |
| **64 KiB GET (default cache)** | AwanFS (--durability=none/fast, NO-SYNC) | `warp` | not measured (Error preparing server: The requested sub-resource is not implemented.) | - | - | - | - | - | - |
| **64 KiB GET (default cache)** | MinIO (4-dir erasure) | `warp` | 88.7 (84.1..90.1) | 1420.0 (1345.7..1442.3) | 43.82 (42.54..45.95) | 92.33 (91.89..100.03) | 1097.9 (1071.6..1178.0) | - | - |
| **64 KiB GET (default cache)** | RustFS | `warp` | 545.6 (526.3..547.2) | 8729.4 (8421.0..8754.7) | 6.58 (6.54..6.71) | 19.54 (19.49..21.74) | 183.9 (182.9..184.4) | - | - |
| **64 KiB Cold GET (--cache-bytes 0 / >cache set)** | AwanFS (--durability=group, default) | http | 294.9 (281.9..298.9) | 4718.6 (4510.6..4782.1) | 12.72 (12.64..13.14) | 32.85 (31.64..36.07) | 60.1 (59.7..60.2) | 68.3 (66.8..68.7) | 25.17 (23.53..25.89) |
| **64 KiB Cold GET (--cache-bytes 0 / >cache set)** | AwanFS (--durability=strict) | http | 285.9 (285.4..295.2) | 4573.9 (4566.2..4724.0) | 13.05 (12.77..13.20) | 33.20 (32.74..35.52) | 59.8 (59.7..60.4) | 67.3 (67.2..68.3) | 25.34 (24.56..27.14) |
| **64 KiB Cold GET (--cache-bytes 0 / >cache set)** | AwanFS (--durability=none/fast, NO-SYNC) | http | 293.3 (292.5..299.8) | 4692.2 (4680.3..4796.5) | 12.91 (12.68..12.94) | 32.41 (31.49..32.44) | 60.1 (59.9..60.2) | 67.8 (67.7..68.5) | 25.12 (24.66..25.73) |
| **64 KiB Cold GET (--cache-bytes 0 / >cache set)** | MinIO (4-dir erasure) | http | 86.7 (85.2..91.3) | 1387.3 (1363.1..1460.5) | 43.85 (41.90..44.14) | 100.10 (94.31..111.98) | 624.8 (608.7..637.0) | 64.9 (64.9..64.9) | 81.25 (77.95..81.98) |
| **64 KiB Cold GET (--cache-bytes 0 / >cache set)** | RustFS | http | 262.1 (252.3..266.8) | 4194.2 (4037.1..4269.0) | 14.29 (14.06..14.79) | 37.02 (36.38..38.97) | 152.8 (152.7..153.1) | 32.6 (32.6..32.6) | 49.33 (48.91..49.59) |
| **64 KiB Mixed 80/20** | AwanFS (--durability=group, default) | http | 269.2 (265.4..270.7) | 4306.5 (4247.1..4331.6) | 13.46 (13.27..13.57) | 37.08 (36.48..37.30) | 110.5 (109.7..111.1) | 8680.7 (7139.3..8680.9) | 39.61 (38.27..39.70) |
| **64 KiB Mixed 80/20** | AwanFS (--durability=strict) | http | 263.3 (252.8..265.5) | 4212.4 (4045.4..4248.0) | 13.75 (13.75..14.15) | 39.56 (37.56..41.64) | 106.0 (104.2..107.0) | 5337.9 (5335.9..5338.6) | 40.25 (39.59..41.39) |
| **64 KiB Mixed 80/20** | AwanFS (--durability=none/fast, NO-SYNC) | http | 288.9 (286.1..294.8) | 4622.3 (4578.0..4717.6) | 12.70 (12.46..12.77) | 34.27 (33.37..34.48) | 95.0 (94.9..95.2) | 10224.4 (6883.1..10225.1) | 32.94 (31.88..34.28) |
| **64 KiB Mixed 80/20** | MinIO (4-dir erasure) | http | 12.2 (10.5..12.5) | 195.9 (168.4..200.2) | 20.37 (19.16..31.45) | 1949.51 (1938.23..2379.98) | 732.7 (728.1..754.0) | 520.4 (483.8..520.8) | 102.22 (96.94..103.66) |
| **64 KiB Mixed 80/20** | RustFS | http | 36.8 (36.4..38.2) | 588.1 (581.9..611.3) | 32.88 (31.73..34.82) | 526.88 (491.51..543.88) | 166.0 (164.2..166.8) | 32.9 (32.9..32.9) | 69.72 (68.30..69.73) |
| **64 KiB Mixed 80/20** | AwanFS (--durability=group, default) | `warp` | not measured (Error preparing server: The requested sub-resource is not implemented.) | - | - | - | - | - | - |
| **64 KiB Mixed 80/20** | AwanFS (--durability=strict) | `warp` | not measured (Error preparing server: The requested sub-resource is not implemented.) | - | - | - | - | - | - |
| **64 KiB Mixed 80/20** | AwanFS (--durability=none/fast, NO-SYNC) | `warp` | not measured (Error preparing server: The requested sub-resource is not implemented.) | - | - | - | - | - | - |
| **64 KiB Mixed 80/20** | MinIO (4-dir erasure) | `warp` | 34.0 (31.6..34.5) | 543.8 (505.5..551.2) | 93.84 (92.64..103.23) | 372.17 (371.04..382.58) | 1097.9 (1071.6..1178.0) | - | - |
| **64 KiB Mixed 80/20** | RustFS | `warp` | 64.1 (62.7..65.0) | 1024.9 (1003.2..1039.9) | 56.47 (55.99..57.04) | 220.60 (220.01..226.10) | 303.9 (303.0..320.1) | - | - |
| **64 KiB Range GET (4 KiB)** | AwanFS (--durability=group, default) | http | 19.9 (19.8..20.0) | 5104.4 (5066.3..5130.1) | 11.58 (11.54..11.61) | 33.73 (33.57..34.00) | 110.5 (109.7..111.1) | 8715.7 (7174.4..8716.0) | 21.02 (20.92..21.50) |
| **64 KiB Range GET (4 KiB)** | AwanFS (--durability=strict) | http | 19.4 (18.5..19.4) | 4962.6 (4748.1..4972.7) | 11.94 (11.86..12.17) | 35.74 (34.59..38.78) | 106.0 (104.2..107.0) | 5372.1 (5368.8..5372.9) | 20.31 (19.81..21.47) |
| **64 KiB Range GET (4 KiB)** | AwanFS (--durability=none/fast, NO-SYNC) | http | 19.7 (19.5..19.9) | 5037.6 (4999.0..5082.5) | 11.78 (11.58..11.83) | 33.98 (33.78..34.34) | 95.0 (94.9..95.2) | 10258.9 (6917.8..10260.1) | 20.59 (20.47..21.73) |
| **64 KiB Range GET (4 KiB)** | MinIO (4-dir erasure) | http | 6.1 (5.8..6.2) | 1559.2 (1488.1..1575.6) | 37.78 (37.10..40.10) | 91.15 (90.52..97.72) | 732.7 (728.2..755.2) | 585.3 (548.7..585.7) | 83.08 (82.14..83.47) |
| **64 KiB Range GET (4 KiB)** | RustFS | http | 16.8 (16.7..17.1) | 4305.7 (4268.4..4369.0) | 13.64 (13.47..13.79) | 40.11 (39.31..40.67) | 166.0 (164.2..166.8) | 33.2 (33.2..33.2) | 61.47 (61.33..61.59) |
| **1 MiB PUT** | AwanFS (--durability=group, default) | http | 200.1 (170.4..223.7) | 200.1 (170.4..223.7) | 95.68 (77.62..166.79) | 518.40 (496.11..533.36) | 297.0 (289.6..298.9) | 14861.7 (12551.8..15629.6) | 40.08 (33.98..45.77) |
| **1 MiB PUT** | AwanFS (--durability=strict) | http | 254.6 (84.5..274.3) | 254.6 (84.5..274.3) | 91.66 (84.25..106.83) | 362.43 (341.61..2292.56) | 288.8 (284.2..289.1) | 13054.2 (8185.5..13823.1) | 50.12 (16.33..56.27) |
| **1 MiB PUT** | AwanFS (--durability=none/fast, NO-SYNC) | http | 201.0 (192.6..208.7) | 201.0 (192.6..208.7) | 71.78 (70.91..73.61) | 1928.75 (1836.64..2026.41) | 276.7 (276.7..278.5) | 16660.6 (13063.4..16661.8) | 41.09 (37.97..41.19) |
| **1 MiB PUT** | MinIO (4-dir erasure) | http | 30.0 (26.9..30.2) | 30.0 (26.9..30.2) | 1057.51 (1055.16..1172.72) | 1535.17 (1482.05..1809.62) | 770.4 (755.2..805.8) | 2656.8 (2462.2..2687.3) | 102.09 (99.36..103.20) |
| **1 MiB PUT** | RustFS | http | 67.5 (65.2..67.6) | 67.5 (65.2..67.6) | 471.44 (470.34..488.16) | 565.88 (550.36..597.92) | 166.0 (164.2..166.8) | 2197.4 (2126.4..2202.4) | 72.00 (71.25..73.19) |
| **1 MiB GET (default cache)** | AwanFS (--durability=group, default) | http | 1664.0 (1605.4..1805.7) | 1664.0 (1605.4..1805.7) | 18.56 (17.16..19.13) | 35.74 (32.99..38.91) | 297.0 (289.6..298.9) | 14873.2 (12562.9..15642.1) | 26.34 (25.27..30.19) |
| **1 MiB GET (default cache)** | AwanFS (--durability=strict) | http | 1718.3 (1647.9..1755.2) | 1718.3 (1647.9..1755.2) | 17.71 (17.48..18.78) | 37.43 (35.94..39.00) | 288.8 (284.2..289.1) | 13322.3 (8197.4..13834.5) | 29.16 (24.67..31.66) |
| **1 MiB GET (default cache)** | AwanFS (--durability=none/fast, NO-SYNC) | http | 1706.6 (1644.6..1766.7) | 1706.6 (1644.6..1766.7) | 18.19 (17.52..18.84) | 34.69 (34.48..36.07) | 276.7 (276.7..278.5) | 16672.0 (13075.6..16929.6) | 26.02 (25.89..30.80) |
| **1 MiB GET (default cache)** | MinIO (4-dir erasure) | http | 861.0 (775.3..864.8) | 861.0 (775.3..864.8) | 35.17 (34.92..39.13) | 81.79 (80.81..87.21) | 1097.9 (1071.6..1178.0) | 2913.1 (2718.4..2943.5) | 92.27 (89.44..92.45) |
| **1 MiB GET (default cache)** | RustFS | http | 735.4 (689.3..738.5) | 735.4 (689.3..738.5) | 42.47 (42.07..45.14) | 79.38 (77.59..82.67) | 166.0 (164.2..166.8) | 2325.7 (2254.7..2330.7) | 114.66 (114.48..117.92) |
| **1 MiB Cold GET (--cache-bytes 0 / >cache set)** | AwanFS (--durability=group, default) | http | 1404.6 (1370.9..1448.4) | 1404.6 (1370.9..1448.4) | 21.42 (21.29..21.77) | 52.00 (43.88..56.17) | 166.1 (165.8..166.4) | 239.4 (237.8..240.2) | 65.36 (62.48..67.72) |
| **1 MiB Cold GET (--cache-bytes 0 / >cache set)** | AwanFS (--durability=strict) | http | 1473.9 (1429.8..1486.3) | 1473.9 (1429.8..1486.3) | 21.03 (20.90..21.28) | 40.47 (39.66..44.85) | 162.9 (162.8..163.0) | 238.9 (238.6..240.1) | 67.75 (67.61..68.70) |
| **1 MiB Cold GET (--cache-bytes 0 / >cache set)** | AwanFS (--durability=none/fast, NO-SYNC) | http | 1473.2 (1434.7..1477.7) | 1473.2 (1434.7..1477.7) | 21.04 (20.94..21.18) | 40.67 (40.34..46.32) | 164.3 (163.3..164.4) | 239.5 (239.1..240.2) | 67.94 (66.41..68.86) |
| **1 MiB Cold GET (--cache-bytes 0 / >cache set)** | MinIO (4-dir erasure) | http | 815.7 (813.8..824.7) | 815.7 (813.8..824.7) | 36.84 (36.49..37.03) | 85.37 (84.90..86.30) | 879.7 (873.6..898.7) | 321.1 (321.1..321.1) | 91.41 (89.72..92.97) |
| **1 MiB Cold GET (--cache-bytes 0 / >cache set)** | RustFS | http | 731.8 (701.0..737.9) | 731.8 (701.0..737.9) | 42.54 (42.24..44.49) | 77.24 (77.18..82.07) | 167.9 (156.2..172.0) | 160.7 (160.7..160.7) | 117.20 (116.33..117.52) |
| **LIST 1,000 keys (ListObjectsV2)** | AwanFS (--durability=group, default) | http | 141.5 (134.4..143.4) | 633.4 (601.5..642.0) | 25.26 (24.92..26.31) | 42.65 (41.61..50.57) | 297.0 (289.6..298.9) | 14877.4 (12567.0..15646.4) | 118.08 (115.56..118.41) |
| **LIST 1,000 keys (ListObjectsV2)** | AwanFS (--durability=strict) | http | 135.8 (130.7..139.7) | 607.7 (585.0..625.3) | 25.94 (25.47..27.09) | 51.22 (46.20..52.61) | 288.8 (284.2..289.1) | 13326.4 (8201.3..13838.7) | 114.22 (112.95..118.19) |
| **LIST 1,000 keys (ListObjectsV2)** | AwanFS (--durability=none/fast, NO-SYNC) | http | 138.6 (137.8..141.1) | 620.4 (616.7..631.7) | 25.46 (25.31..25.92) | 44.77 (43.00..49.51) | 276.7 (276.7..278.5) | 16676.1 (13079.8..16933.8) | 118.55 (118.03..118.81) |
| **LIST 1,000 keys (ListObjectsV2)** | MinIO (4-dir erasure) | http | 0.5 (0.4..0.5) | 2.2 (2.1..2.3) | 6777.80 (6660.20..7051.05) | 8953.29 (8182.37..9387.84) | 1097.9 (1071.6..1178.0) | 2235.3 (1971.9..2354.3) | 85.75 (85.52..86.95) |
| **LIST 1,000 keys (ListObjectsV2)** | RustFS | http | 1.4 (1.4..1.4) | 7.2 (7.1..7.3) | 2157.41 (2151.90..2255.15) | 2872.12 (2856.10..2907.37) | 166.0 (164.2..166.8) | 2326.4 (2255.4..2331.4) | 121.03 (120.72..121.70) |
| **LIST 1,000 keys (ListObjectsV2)** | AwanFS (--durability=group, default) | `warp` | not measured (Error preparing server: The requested sub-resource is not implemented.) | - | - | - | - | - | - |
| **LIST 1,000 keys (ListObjectsV2)** | AwanFS (--durability=strict) | `warp` | not measured (Error preparing server: The requested sub-resource is not implemented.) | - | - | - | - | - | - |
| **LIST 1,000 keys (ListObjectsV2)** | AwanFS (--durability=none/fast, NO-SYNC) | `warp` | not measured (Error preparing server: The requested sub-resource is not implemented.) | - | - | - | - | - | - |
| **LIST 1,000 keys (ListObjectsV2)** | MinIO (4-dir erasure) | `warp` | 0.0 (0.0..0.0) | 34.5 (33.9..34.5) | 445.39 (444.10..459.57) | 817.49 (817.15..822.32) | 1097.9 (1071.6..1178.0) | - | - |
| **LIST 1,000 keys (ListObjectsV2)** | RustFS | `warp` | 0.0 (0.0..0.0) | 92.7 (90.4..92.8) | 166.66 (166.60..171.31) | 314.10 (308.13..317.81) | 304.1 (303.3..320.1) | - | - |
| **HEAD 64 KiB object** | AwanFS (--durability=group, default) | http | 0.0 (0.0..0.0) | 5321.2 (5052.1..5323.9) | 11.19 (11.14..11.45) | 33.04 (32.74..36.19) | 297.0 (289.6..298.9) | 14914.0 (12601.7..15682.9) | 20.72 (19.89..22.20) |
| **HEAD 64 KiB object** | AwanFS (--durability=strict) | http | 0.0 (0.0..0.0) | 5244.9 (5185.6..5256.7) | 11.26 (11.14..11.26) | 33.70 (33.47..34.70) | 288.8 (284.2..289.1) | 13362.0 (8237.4..13874.9) | 20.89 (20.36..21.38) |
| **HEAD 64 KiB object** | AwanFS (--durability=none/fast, NO-SYNC) | http | 0.0 (0.0..0.0) | 5292.2 (5264.6..5334.3) | 11.08 (11.06..11.17) | 33.18 (32.88..33.32) | 276.7 (276.7..278.5) | 16712.4 (13115.9..16970.5) | 21.14 (20.33..21.38) |
| **HEAD 64 KiB object** | MinIO (4-dir erasure) | http | 0.0 (0.0..0.0) | 1397.9 (1329.9..1426.7) | 43.63 (43.29..46.11) | 90.42 (83.41..91.34) | 1097.9 (1071.6..1178.0) | 339.1 (339.1..340.5) | 80.00 (79.44..80.97) |
| **HEAD 64 KiB object** | RustFS | http | 0.0 (0.0..0.0) | 4843.8 (4767.8..4862.0) | 12.14 (12.10..12.30) | 36.50 (36.07..37.21) | 166.0 (164.2..166.8) | 2343.0 (2271.9..2348.0) | 40.47 (39.91..41.19) |
| **DELETE 64 KiB object** | AwanFS (--durability=group, default) | http | 0.0 (0.0..0.0) | 4150.7 (4019.8..4362.8) | 9.47 (9.17..9.70) | 33.80 (32.12..34.90) | 397.5 (390.7..397.7) | 22651.2 (20081.8..23420.7) | 109.86 (107.34..112.00) |
| **DELETE 64 KiB object** | AwanFS (--durability=strict) | http | 0.0 (0.0..0.0) | 1646.1 (1587.7..1695.5) | 31.01 (30.29..31.32) | 105.84 (105.14..134.10) | 317.3 (316.3..318.0) | 16716.4 (11848.9..17486.9) | 72.73 (69.48..74.94) |
| **DELETE 64 KiB object** | AwanFS (--durability=none/fast, NO-SYNC) | http | 0.0 (0.0..0.0) | 4239.0 (4080.3..4245.4) | 6.69 (6.61..6.71) | 31.86 (31.06..34.93) | 403.2 (402.2..406.9) | 25996.6 (22401.8..26256.4) | 111.62 (110.16..112.31) |
| **DELETE 64 KiB object** | MinIO (4-dir erasure) | http | 0.0 (0.0..0.0) | 156.1 (154.5..163.1) | 341.89 (321.82..350.28) | 1155.30 (1121.58..1234.81) | 1097.9 (1071.6..1178.0) | 987.8 (987.7..987.8) | 204.94 (202.23..214.56) |
| **DELETE 64 KiB object** | RustFS | http | 0.0 (0.0..0.0) | 148.7 (148.3..151.3) | 418.14 (413.26..418.67) | 613.01 (593.86..632.13) | 167.0 (166.0..167.2) | 169.9 (169.5..170.3) | 165.19 (164.58..165.27) |

## 4. Cold-Read Payload Cache Verification (`/_awanfs/metrics`)

| Workload | Server / Mode | Round | Cache Hits (delta) | Cache Misses (delta) |
| :--- | :--- | :--- | :--- | :--- |
| 64 KiB Cold GET (--cache-bytes 0 / >cache set) | AwanFS (--durability=group, default) | run 1 | 1 | 136767 |
| 64 KiB Cold GET (--cache-bytes 0 / >cache set) | AwanFS (--durability=group, default) | run 2 | 11 | 145005 |
| 64 KiB Cold GET (--cache-bytes 0 / >cache set) | AwanFS (--durability=group, default) | run 3 | 14 | 143028 |
| 64 KiB Cold GET (--cache-bytes 0 / >cache set) | AwanFS (--durability=strict) | run 1 | 2 | 138548 |
| 64 KiB Cold GET (--cache-bytes 0 / >cache set) | AwanFS (--durability=strict) | run 2 | 17 | 138641 |
| 64 KiB Cold GET (--cache-bytes 0 / >cache set) | AwanFS (--durability=strict) | run 3 | 16 | 143343 |
| 64 KiB Cold GET (--cache-bytes 0 / >cache set) | AwanFS (--durability=none/fast, NO-SYNC) | run 1 | 11 | 140658 |
| 64 KiB Cold GET (--cache-bytes 0 / >cache set) | AwanFS (--durability=none/fast, NO-SYNC) | run 2 | 21 | 140996 |
| 64 KiB Cold GET (--cache-bytes 0 / >cache set) | AwanFS (--durability=none/fast, NO-SYNC) | run 3 | 16 | 144102 |
| 1 MiB Cold GET (--cache-bytes 0 / >cache set) | AwanFS (--durability=group, default) | run 1 | 0 | 41190 |
| 1 MiB Cold GET (--cache-bytes 0 / >cache set) | AwanFS (--durability=group, default) | run 2 | 0 | 43504 |
| 1 MiB Cold GET (--cache-bytes 0 / >cache set) | AwanFS (--durability=group, default) | run 3 | 0 | 42201 |
| 1 MiB Cold GET (--cache-bytes 0 / >cache set) | AwanFS (--durability=strict) | run 1 | 0 | 43004 |
| 1 MiB Cold GET (--cache-bytes 0 / >cache set) | AwanFS (--durability=strict) | run 2 | 0 | 44273 |
| 1 MiB Cold GET (--cache-bytes 0 / >cache set) | AwanFS (--durability=strict) | run 3 | 0 | 44716 |
| 1 MiB Cold GET (--cache-bytes 0 / >cache set) | AwanFS (--durability=none/fast, NO-SYNC) | run 1 | 0 | 43083 |
| 1 MiB Cold GET (--cache-bytes 0 / >cache set) | AwanFS (--durability=none/fast, NO-SYNC) | run 2 | 0 | 44369 |
| 1 MiB Cold GET (--cache-bytes 0 / >cache set) | AwanFS (--durability=none/fast, NO-SYNC) | run 3 | 0 | 44245 |
