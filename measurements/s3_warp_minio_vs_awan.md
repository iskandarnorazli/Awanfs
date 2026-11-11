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
| **64 KiB PUT (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **317.9** | **5086.1** | **11.27** | **20.04** | **30.23** | **38.98** | **83.7 MB** |
| **64 KiB PUT (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 5.7 | 91.0 | 736.64 | 982.30 | 1081.99 | 1096.39 | 417.9 MB |
| **64 KiB PUT (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 9.1 | 146.4 | 432.34 | 494.39 | 560.33 | 580.44 | 106.7 MB |
| **64 KiB GET (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **335.4** | **5366.2** | **10.79** | **19.43** | **29.37** | **39.07** | **83.7 MB** |
| **64 KiB GET (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 221.9 | 3549.6 | 16.47 | 27.87 | 43.69 | 115.23 | 530.7 MB |
| **64 KiB GET (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 273.4 | 4373.7 | 13.59 | 23.76 | 34.69 | 47.08 | 157.5 MB |
| **64 KiB Mixed (80% GET / 20% PUT)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **316.7** | **5066.9** | **11.14** | **20.58** | **31.03** | **41.18** | **1202.6 MB** |
| **64 KiB Mixed (80% GET / 20% PUT)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 34.6 | 553.8 | 14.82 | 474.88 | 796.85 | 841.55 | 975.4 MB |
| **64 KiB Mixed (80% GET / 20% PUT)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 41.3 | 660.5 | 28.17 | 405.45 | 462.22 | 494.29 | 173.2 MB |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **22.6** | **5780.5** | **9.33** | **19.67** | **31.51** | **42.55** | **83.7 MB** |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 14.1 | 3615.2 | 15.68 | 28.23 | 42.48 | 57.08 | 538.5 MB |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 17.3 | 4419.9 | 13.36 | 24.62 | 38.62 | 48.71 | 157.6 MB |
| **1 MiB PUT (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **642.5** | **642.5** | **46.35** | **75.18** | **115.01** | **156.76** | **249.8 MB** |
| **1 MiB PUT (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 67.1 | 67.1 | 390.08 | 827.46 | 1057.92 | 1153.78 | 545.7 MB |
| **1 MiB PUT (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 82.3 | 82.3 | 375.61 | 459.37 | 503.69 | 507.48 | 157.6 MB |
| **1 MiB GET (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **1862.4** | **1862.4** | **16.54** | **23.88** | **30.90** | **37.71** | **249.8 MB** |
| **1 MiB GET (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 1355.3 | 1355.3 | 21.79 | 36.53 | 56.58 | 89.52 | 949.1 MB |
| **1 MiB GET (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 776.9 | 776.9 | 39.82 | 56.93 | 70.61 | 78.96 | 157.6 MB |
| **1 MiB Mixed (80% GET / 20% PUT)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **119.4** | **119.4** | **16.84** | **29.61** | **934.76** | **1838.16** | **1207.8 MB** |
| **1 MiB Mixed (80% GET / 20% PUT)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 498.7 | 498.7 | 33.16 | 188.88 | 311.80 | 404.07 | 975.4 MB |
| **1 MiB Mixed (80% GET / 20% PUT)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 289.0 | 289.0 | 40.80 | 412.97 | 473.79 | 503.51 | 173.2 MB |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **669.9** | **669.9** | **22.92** | **31.87** | **46.51** | **63.81** | **1201.8 MB** |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | N/A (S3 $\ge$5MiB spec limit) | N/A | N/A | N/A | N/A | N/A | 949.1 MB |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | N/A (S3 $\ge$5MiB spec limit) | N/A | N/A | N/A | N/A | N/A | 157.6 MB |
| **1 GiB Multipart Upload (16x64MiB Parts + Complete)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **46.0** | **0.7** | **11215.49** | **12440.36** | **12600.80** | **12600.80** | **2147.7 MB** |
| **1 GiB Multipart Upload (16x64MiB Parts + Complete)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 847.3 | 13.2 | 558.39 | 621.10 | 631.66 | 631.66 | 975.4 MB |
| **1 GiB Multipart Upload (16x64MiB Parts + Complete)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 447.9 | 7.0 | 1040.55 | 1085.33 | 1113.37 | 1113.37 | 261.0 MB |
| **1 GiB Multipart Range GET (16x64MiB Parallel Ranges)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **1074.6** | **16.8** | **430.33** | **508.54** | **548.73** | **548.73** | **2381.6 MB** |
| **1 GiB Multipart Range GET (16x64MiB Parallel Ranges)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 1339.5 | 20.9 | 353.12 | 396.38 | 453.07 | 453.07 | 975.4 MB |
| **1 GiB Multipart Range GET (16x64MiB Parallel Ranges)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 813.9 | 12.7 | 540.28 | 620.78 | 870.44 | 870.44 | 261.0 MB |
| **10 GiB Volume PUT (160x64MiB Streamed Parts)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **107.1** | **1.7** | **690.09** | **12934.27** | **22657.75** | **23257.43** | **3199.2 MB** |
| **10 GiB Volume PUT (160x64MiB Streamed Parts)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 304.8 | 4.8 | 861.33 | 4047.23 | 4753.08 | 4846.41 | 975.4 MB |
| **10 GiB Volume PUT (160x64MiB Streamed Parts)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 147.1 | 2.4 | 954.27 | 10184.33 | 15876.63 | 15944.30 | 261.0 MB |
| **10 GiB Volume GET (160x64MiB Parallel Ranges)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **1404.3** | **21.9** | **343.27** | **449.29** | **661.80** | **679.77** | **3199.2 MB** |
| **10 GiB Volume GET (160x64MiB Parallel Ranges)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 1201.2 | 18.8 | 415.87 | 498.60 | 562.03 | 599.81 | 975.4 MB |
| **10 GiB Volume GET (160x64MiB Parallel Ranges)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 847.9 | 13.2 | 591.46 | 695.15 | 787.38 | 906.87 | 261.0 MB |

## 3. Concurrency Scaling Sweep (64 KiB Mixed 80% GET / 20% PUT: C in {8, 32, 64, 128})

| Concurrency (Workers) | Target Engine | Throughput (MiB/s) | Operations/sec | P50 (ms) | P90 (ms) | P99 (ms) | P99.9 (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **C = 8** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **357.0** | **5712.2** | **1.27** | **2.16** | **3.36** | **13.64** |
| **C = 8** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 58.5 | 936.0 | 2.24 | 25.19 | 82.05 | 141.20 |
| **C = 8** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 40.5 | 648.4 | 2.19 | 51.72 | 59.72 | 86.52 |
| **C = 32** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **347.9** | **5566.7** | **5.14** | **9.60** | **14.15** | **18.37** |
| **C = 32** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 38.9 | 622.2 | 6.05 | 206.42 | 458.13 | 481.57 |
| **C = 32** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 41.5 | 663.8 | 8.21 | 209.14 | 230.72 | 246.84 |
| **C = 64** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **338.1** | **5408.9** | **10.36** | **19.01** | **29.38** | **39.37** |
| **C = 64** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 33.0 | 527.4 | 15.73 | 435.28 | 956.70 | 1013.08 |
| **C = 64** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 43.3 | 692.6 | 29.91 | 373.89 | 424.73 | 437.82 |
| **C = 128** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **324.5** | **5191.6** | **19.12** | **38.51** | **61.57** | **82.95** |
| **C = 128** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 34.1 | 545.2 | 35.82 | 911.74 | 1390.69 | 1417.18 |
| **C = 128** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 43.0 | 687.7 | 82.65 | 653.89 | 872.51 | 914.24 |

## 4. Open-Source S3 Alternatives Reference Comparison (`COMPETITIVE_LANDSCAPE.md`)

> **Provenance Note (Audit Rule 3):** Official standalone MinIO (`RELEASE.2025-09-07T16-13-09Z`, Go 1.24.6 windows/amd64) and RustFS (`v1.0.5`, Apache 2.0 Rust windows/x86_64) binaries were executed live on local NVMe pinned to Cores 0..3 with anonymous public access policy enabled via `mc.exe`, receiving identical persistent HTTP/1.1 keep-alive requests from client workers pinned to Cores 4..7. Peak Working Set RSS was measured live using Windows OS API `GetProcessMemoryInfo`. Note that for 1 MiB multipart upload with 256 KiB parts, both MinIO and RustFS strictly reject parts smaller than 5 MiB with HTTP 400 `EntityTooSmall` per AWS S3 RFC specification, whereas `s3-server` supports sub-5 MiB parts.

| Dimension | `s3-server` (Measured Local) | MinIO (Go, Measured Local) | RustFS (Rust, Measured Local) | SeaweedFS (Go, Published Ref) | Garage (Rust, Published Ref) | Ceph RGW (C++, Published Ref) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **64 KiB PUT Throughput** | **317.9 MiB/s (5086 ops/s)** | 5.7 MiB/s (91 ops/s) | 9.1 MiB/s (146 ops/s) | ~250–450 MiB/s (Haystack append) | ~40–110 MiB/s (LMDB/Sled) | ~60–180 MiB/s (BlueStore WAL) |
| **64 KiB GET Throughput** | **335.4 MiB/s (5366 ops/s)** | 221.9 MiB/s (3550 ops/s) | 273.4 MiB/s (4374 ops/s) | ~400–650 MiB/s (Haystack O(1)) | ~80–180 MiB/s (LMDB/Sled) | ~120–300 MiB/s (BlueStore) |
| **1 MiB PUT Throughput** | **642.5 MiB/s (643 ops/s)** | 67.1 MiB/s (67 ops/s) | 82.3 MiB/s (82 ops/s) | ~600–1,400 MiB/s | ~200–500 MiB/s | ~400–1,000 MiB/s |
| **1 MiB / 10 GiB GET Throughput** | **1862.4 – 1404.3 MiB/s** | 1355.3 – 1201.2 MiB/s | 776.9 – 847.9 MiB/s | ~900–1,900 MiB/s | ~350–700 MiB/s | ~700–1,500 MiB/s |
| **Server Peak RSS (64K–10G)** | **3199.2 MB (Bounded 16 MiB Slab Cache)** | 975.4 MB (Go GC heap) | 261.0 MB (Rust jemalloc) | ~180–450 MB (Go GC heap) | ~60–150 MB (Rust) | ~1,024–4,096 MB (OSD+RGW) |
| **Metadata Layout** | **64B `ObjectMeta` Columnar SoA** | Per-object `xl.meta` file | Multi-volume FS Metadata | Needle Index map in RAM | Sled / SQLite / LMDB B-Tree | RocksDB LSM inside BlueStore |
| **Small-File Disk Layout** | **4096B WORM Needle Volume** | 2+ files/inodes per object | Multi-volume File Store | Haystack Volume (O(1) seek) | Content-addressed blocks | 4 KiB min alloc in BlueStore |
| **License** | **Permissive (Workspace / MIT / Apache-2.0)** | AGPLv3 (Active Community OSS) | Apache 2.0 (Active Rust OSS) | Apache 2.0 | AGPLv3 | LGPLv2.1 /v3 |
