`python benchmarks/s3_warp/run_warp_benchmark.py --sizes 64KiB,1MiB,1GiB,10GiB --concurrent 64 --duration 2 --output docs/measurements/s3_warp_minio_vs_awan.md`

# Head-to-Head S3 Benchmark Report: S3 Storage Server vs MinIO & RustFS

## 1. Physical Hardware & Execution Context
- **CPU Model:** AMD64 Family 25 Model 80 Stepping 0, AuthenticAMD
- **Logical Cores:** 16
- **Host RAM:** 32.0 GB
- **Operating System:** Windows 11 (AMD64)
- **Server Affinity Pin:** Cores 0..3 (Affinity Mask 0x0F)
- **Client Affinity Pin:** Cores 4..7 (Affinity Mask 0xF0)
- **Live MinIO Binary Status:** Measured Live (minio.exe RELEASE.2025-09-07T16-13-09Z, Go 1.24.6 windows/amd64)
- **Live RustFS Binary Status:** Measured Live (rustfs.exe v1.0.5, Apache 2.0 Rust x86_64)

## 2. Live Measured HTTP/1.1 Keep-Alive Throughput & Latency Matrix (64 KiB – 10 GiB)

| Workload | Target Engine | Throughput (MiB/s) | Operations/sec | P50 (ms) | P90 (ms) | P99 (ms) | P99.9 (ms) | Peak RSS (MB) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **64 KiB PUT (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **366.4** | **5862.6** | **9.83** | **17.67** | **26.56** | **33.01** | **41.0 MB** |
| **64 KiB PUT (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 8.3 | 132.8 | 435.65 | 754.23 | 815.92 | 822.34 | 428.8 MB |
| **64 KiB PUT (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 8.0 | 128.3 | 450.54 | 759.77 | 855.77 | 865.33 | 107.7 MB |
| **64 KiB GET (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **324.6** | **5193.7** | **11.16** | **19.86** | **29.74** | **38.90** | **41.0 MB** |
| **64 KiB GET (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 210.2 | 3363.2 | 17.11 | 28.92 | 43.18 | 59.29 | 534.5 MB |
| **64 KiB GET (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 277.9 | 4446.0 | 13.33 | 23.24 | 35.06 | 46.31 | 158.0 MB |
| **64 KiB Mixed (80% GET / 20% PUT)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **311.5** | **4983.5** | **11.29** | **20.97** | **32.01** | **42.67** | **97.5 MB** |
| **64 KiB Mixed (80% GET / 20% PUT)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 41.7 | 667.0 | 13.58 | 403.67 | 724.47 | 785.03 | 978.2 MB |
| **64 KiB Mixed (80% GET / 20% PUT)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 31.0 | 496.5 | 23.38 | 497.76 | 824.95 | 853.41 | 177.9 MB |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **21.9** | **5599.9** | **9.66** | **19.81** | **31.53** | **42.62** | **41.0 MB** |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 14.5 | 3716.1 | 15.57 | 28.06 | 44.22 | 93.40 | 549.3 MB |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 17.0 | 4347.8 | 13.50 | 25.42 | 37.91 | 48.25 | 158.1 MB |
| **1 MiB PUT (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **797.3** | **797.3** | **39.21** | **55.04** | **68.31** | **78.42** | **77.1 MB** |
| **1 MiB PUT (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 126.1 | 126.1 | 214.30 | 369.27 | 538.89 | 622.03 | 551.4 MB |
| **1 MiB PUT (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 20.4 | 20.4 | 1532.74 | 1972.68 | 2071.21 | 2153.84 | 158.1 MB |
| **1 MiB GET (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **1174.4** | **1174.4** | **26.75** | **34.24** | **40.40** | **45.25** | **77.1 MB** |
| **1 MiB GET (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 1371.8 | 1371.8 | 21.62 | 37.22 | 58.39 | 116.72 | 957.2 MB |
| **1 MiB GET (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 751.1 | 751.1 | 41.12 | 57.01 | 74.42 | 84.56 | 158.1 MB |
| **1 MiB Mixed (80% GET / 20% PUT)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **203.8** | **203.8** | **26.90** | **35.46** | **43.58** | **52.02** | **97.5 MB** |
| **1 MiB Mixed (80% GET / 20% PUT)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 450.1 | 450.1 | 28.93 | 221.50 | 500.10 | 625.52 | 978.2 MB |
| **1 MiB Mixed (80% GET / 20% PUT)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 143.6 | 143.6 | 44.51 | 807.81 | 1669.24 | 1694.87 | 177.9 MB |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **756.2** | **756.2** | **20.70** | **27.14** | **33.19** | **36.41** | **97.5 MB** |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | N/A (S3 $\ge$5MiB spec limit) | N/A | N/A | N/A | N/A | N/A | 957.2 MB |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | N/A (S3 $\ge$5MiB spec limit) | N/A | N/A | N/A | N/A | N/A | 158.1 MB |
| **1 GiB Multipart Upload (16x64MiB Parts + Complete)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **44.1** | **0.7** | **11612.33** | **17196.35** | **18672.16** | **18672.16** | **1154.0 MB** |
| **1 GiB Multipart Upload (16x64MiB Parts + Complete)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 684.1 | 10.7 | 644.21 | 769.19 | 845.99 | 845.99 | 978.2 MB |
| **1 GiB Multipart Upload (16x64MiB Parts + Complete)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 455.8 | 7.1 | 1007.94 | 1068.40 | 1075.01 | 1075.01 | 255.7 MB |
| **1 GiB Multipart Range GET (16x64MiB Parallel Ranges)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **1263.8** | **19.7** | **346.96** | **432.86** | **482.36** | **482.36** | **1154.0 MB** |
| **1 GiB Multipart Range GET (16x64MiB Parallel Ranges)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 943.7 | 14.7 | 473.76 | 609.53 | 641.35 | 641.35 | 978.2 MB |
| **1 GiB Multipart Range GET (16x64MiB Parallel Ranges)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 884.1 | 13.8 | 520.62 | 592.88 | 885.04 | 885.04 | 255.7 MB |
| **10 GiB Volume PUT (160x64MiB Streamed Parts)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **769.7** | **12.0** | **644.48** | **739.25** | **997.87** | **1177.10** | **1197.5 MB** |
| **10 GiB Volume PUT (160x64MiB Streamed Parts)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 154.5 | 2.4 | 1328.91 | 5818.41 | 20607.01 | 21830.98 | 978.2 MB |
| **10 GiB Volume PUT (160x64MiB Streamed Parts)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 262.1 | 4.2 | 1101.28 | 3297.26 | 6528.34 | 6937.70 | 255.7 MB |
| **10 GiB Volume GET (160x64MiB Parallel Ranges)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **1145.0** | **17.9** | **445.19** | **471.87** | **493.61** | **497.31** | **1197.5 MB** |
| **10 GiB Volume GET (160x64MiB Parallel Ranges)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 1151.2 | 18.0 | 433.24 | 527.25 | 680.84 | 857.86 | 978.2 MB |
| **10 GiB Volume GET (160x64MiB Parallel Ranges)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 843.1 | 13.2 | 599.87 | 683.32 | 786.47 | 808.50 | 255.7 MB |

## 3. Concurrency Scaling Sweep (64 KiB Mixed 80% GET / 20% PUT: C in {8, 32, 64, 128})

| Concurrency (Workers) | Target Engine | Throughput (MiB/s) | Operations/sec | P50 (ms) | P90 (ms) | P99 (ms) | P99.9 (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **C = 8** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **366.0** | **5855.3** | **1.27** | **2.14** | **3.14** | **5.21** |
| **C = 8** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 59.8 | 957.4 | 2.23 | 25.53 | 71.34 | 133.95 |
| **C = 8** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 38.8 | 621.2 | 2.25 | 54.95 | 67.33 | 71.64 |
| **C = 32** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **327.1** | **5234.3** | **5.50** | **10.09** | **14.90** | **19.69** |
| **C = 32** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 39.9 | 638.7 | 7.27 | 202.29 | 437.52 | 480.92 |
| **C = 32** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 37.3 | 596.9 | 11.33 | 227.71 | 271.66 | 286.07 |
| **C = 64** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **314.2** | **5026.5** | **11.00** | **20.55** | **30.38** | **40.43** |
| **C = 64** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 35.3 | 564.5 | 15.14 | 420.06 | 825.11 | 848.05 |
| **C = 64** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 43.2 | 691.6 | 30.48 | 379.95 | 418.47 | 437.95 |
| **C = 128** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **309.0** | **4944.5** | **19.90** | **39.94** | **62.34** | **82.67** |
| **C = 128** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 38.8 | 620.3 | 27.25 | 794.79 | 1312.31 | 1373.62 |
| **C = 128** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 40.9 | 653.9 | 91.66 | 684.10 | 968.86 | 1026.70 |

## 4. Open-Source S3 Alternatives Reference Comparison (`COMPETITIVE_LANDSCAPE.md`)

> **Provenance Note (Audit Rule 3):** Official standalone MinIO (`RELEASE.2025-09-07T16-13-09Z`, Go 1.24.6 windows/amd64) and RustFS (`v1.0.5`, Apache 2.0 Rust windows/x86_64) binaries were executed live on local NVMe pinned to Cores 0..3 with anonymous public access policy enabled via `mc.exe`, receiving identical persistent HTTP/1.1 keep-alive requests from client workers pinned to Cores 4..7. Peak Working Set RSS was measured live using Windows OS API `GetProcessMemoryInfo`. Note that for 1 MiB multipart upload with 256 KiB parts, both MinIO and RustFS strictly reject parts smaller than 5 MiB with HTTP 400 `EntityTooSmall` per AWS S3 RFC specification, whereas `s3-server` supports sub-5 MiB parts.

| Dimension | `s3-server` (Measured Local) | MinIO (Go, Measured Local) | RustFS (Rust, Measured Local) | SeaweedFS (Go, Published Ref) | Garage (Rust, Published Ref) | Ceph RGW (C++, Published Ref) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **64 KiB PUT Throughput** | **366.4 MiB/s (5863 ops/s)** | 8.3 MiB/s (133 ops/s) | 8.0 MiB/s (128 ops/s) | ~250–450 MiB/s (Haystack append) | ~40–110 MiB/s (LMDB/Sled) | ~60–180 MiB/s (BlueStore WAL) |
| **64 KiB GET Throughput** | **324.6 MiB/s (5194 ops/s)** | 210.2 MiB/s (3363 ops/s) | 277.9 MiB/s (4446 ops/s) | ~400–650 MiB/s (Haystack O(1)) | ~80–180 MiB/s (LMDB/Sled) | ~120–300 MiB/s (BlueStore) |
| **1 MiB PUT Throughput** | **797.3 MiB/s (797 ops/s)** | 126.1 MiB/s (126 ops/s) | 20.4 MiB/s (20 ops/s) | ~600–1,400 MiB/s | ~200–500 MiB/s | ~400–1,000 MiB/s |
| **1 MiB / 10 GiB GET Throughput** | **1174.4 – 1145.0 MiB/s** | 1371.8 – 1151.2 MiB/s | 751.1 – 843.1 MiB/s | ~900–1,900 MiB/s | ~350–700 MiB/s | ~700–1,500 MiB/s |
| **Server Peak RSS (64K–10G)** | **1197.5 MB (Bounded 16 MiB Slab Cache)** | 978.2 MB (Go GC heap) | 255.7 MB (Rust jemalloc) | ~180–450 MB (Go GC heap) | ~60–150 MB (Rust) | ~1,024–4,096 MB (OSD+RGW) |
| **Metadata Layout** | **64B `ObjectMeta` Columnar SoA** | Per-object `xl.meta` file | Multi-volume FS Metadata | Needle Index map in RAM | Sled / SQLite / LMDB B-Tree | RocksDB LSM inside BlueStore |
| **Small-File Disk Layout** | **4096B WORM Needle Volume** | 2+ files/inodes per object | Multi-volume File Store | Haystack Volume (O(1) seek) | Content-addressed blocks | 4 KiB min alloc in BlueStore |
| **License** | **Permissive (Workspace / MIT / Apache-2.0)** | AGPLv3 (Active Community OSS) | Apache 2.0 (Active Rust OSS) | Apache 2.0 | AGPLv3 | LGPLv2.1 /v3 |
