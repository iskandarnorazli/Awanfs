`python benchmarks/s3_warp/run_warp_benchmark.py --sizes 64KiB,1MiB,1GiB,10GiB --concurrent 64 --duration 3s --output docs/measurements/s3_warp_minio_vs_awan.md`

# Head-to-Head S3 Benchmark Report: S3 Storage Server vs S3 Baselines

## 1. Physical Hardware & Execution Context
- **CPU Model:** AMD64 Family 25 Model 80 Stepping 0, AuthenticAMD
- **Logical Cores:** 16
- **Host RAM:** 32.0 GB
- **Operating System:** Windows 11 (AMD64)
- **Server Affinity Pin:** Cores 0..3 (Affinity Mask 0x0F)
- **Client Affinity Pin:** Cores 4..7 (Affinity Mask 0xF0)
- **Live MinIO Binary Status:** not measured (upstream dl.min.io/server/minio/release/windows-amd64/minio.exe returned HTTP 410 Gone — OSS project archived)

## 2. Live Measured HTTP/1.1 Keep-Alive Throughput & Latency Matrix (64 KiB – 10 GiB)

| Workload | Target Engine | Throughput (MiB/s) | Operations/sec | P50 (ms) | P90 (ms) | P99 (ms) | P99.9 (ms) | Peak RSS (MB) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **64 KiB PUT (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **363.2** | **5811.2** | **10.12** | **17.68** | **26.01** | **32.69** | **42.8 MB** |
| **64 KiB PUT (Single Object)** | File-per-Object + `.meta` Sidecar Baseline (Local Measured) | 60.1 | 961.4 | 62.18 | 76.28 | 169.62 | 173.10 | 798.6 MB |
| **64 KiB GET (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **365.8** | **5852.4** | **9.86** | **17.57** | **26.19** | **33.82** | **42.8 MB** |
| **64 KiB GET (Single Object)** | File-per-Object + `.meta` Sidecar Baseline (Local Measured) | 127.0 | 2031.7 | 28.50 | 40.85 | 59.27 | 286.26 | 798.6 MB |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **24.8** | **6359.5** | **8.96** | **17.40** | **28.21** | **36.18** | **42.8 MB** |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | File-per-Object + `.meta` Sidecar Baseline (Local Measured) | 8.6 | 2192.0 | 26.65 | 37.79 | 54.60 | 338.27 | 798.6 MB |
| **1 MiB PUT (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **773.2** | **773.2** | **37.25** | **65.08** | **106.37** | **157.74** | **72.8 MB** |
| **1 MiB PUT (Single Object)** | File-per-Object + `.meta` Sidecar Baseline (Local Measured) | 841.7 | 841.7 | 35.73 | 40.37 | 121.03 | 123.95 | 798.6 MB |
| **1 MiB GET (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **1285.1** | **1285.1** | **24.50** | **31.47** | **38.12** | **43.12** | **72.8 MB** |
| **1 MiB GET (Single Object)** | File-per-Object + `.meta` Sidecar Baseline (Local Measured) | 1265.2 | 1265.2 | 24.27 | 32.53 | 43.22 | 105.66 | 798.6 MB |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **837.5** | **837.5** | **18.48** | **24.65** | **30.91** | **36.01** | **97.8 MB** |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | File-per-Object + `.meta` Sidecar Baseline (Local Measured) | 227.1 | 227.1 | 63.33 | 71.16 | 295.08 | 298.37 | 798.6 MB |
| **1 GiB Multipart Upload (16x64MiB Parts + Complete)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **595.2** | **9.3** | **627.19** | **1076.72** | **1165.58** | **1165.58** | **1194.6 MB** |
| **1 GiB Multipart Upload (16x64MiB Parts + Complete)** | File-per-Object + `.meta` Sidecar Baseline (Local Measured) | 52.5 | 0.8 | 11791.30 | 14875.64 | 19363.50 | 19363.50 | 798.6 MB |
| **1 GiB Multipart Range GET (16x64MiB Parallel Ranges)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **1160.8** | **18.1** | **375.96** | **451.14** | **494.03** | **494.03** | **1194.6 MB** |
| **1 GiB Multipart Range GET (16x64MiB Parallel Ranges)** | File-per-Object + `.meta` Sidecar Baseline (Local Measured) | 1168.7 | 18.3 | 343.17 | 497.17 | 539.39 | 539.39 | 1025.2 MB |
| **10 GiB Volume PUT (160x64MiB Streamed Parts)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **783.9** | **12.2** | **626.68** | **681.71** | **1028.02** | **1185.05** | **1194.6 MB** |
| **10 GiB Volume PUT (160x64MiB Streamed Parts)** | File-per-Object + `.meta` Sidecar Baseline (Local Measured) | 662.3 | 10.3 | 299.91 | 2061.82 | 3243.87 | 4129.05 | 1025.2 MB |
| **10 GiB Volume GET (160x64MiB Parallel Ranges)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **1155.3** | **18.1** | **428.38** | **478.10** | **584.79** | **601.95** | **1194.6 MB** |
| **10 GiB Volume GET (160x64MiB Parallel Ranges)** | File-per-Object + `.meta` Sidecar Baseline (Local Measured) | 1262.2 | 19.7 | 393.37 | 470.39 | 521.48 | 698.79 | 1153.5 MB |

## 3. Open-Source S3 Alternatives Reference Comparison (`COMPETITIVE_LANDSCAPE.md`)

> **Provenance Note (Audit Rule 3):** Live `minio.exe` binary download from `dl.min.io` returned `HTTP 410 Gone` because the upstream MinIO open-source repository was archived. Above table reports live OS-measured numbers on this machine for `s3-server --release` (with `GetProcessMemoryInfo` Peak Working Set RSS and persistent HTTP/1.1 keep-alive connections) alongside the local file-per-object + `.meta` sidecar HTTP baseline server. Below table compares `s3-server` against published architectural characteristics of open-source S3 alternatives.

| Dimension | `s3-server` (Measured Local) | MinIO (Go, Published Ref) | SeaweedFS (Go, Published Ref) | Garage (Rust, Published Ref) | Ceph RGW (C++, Published Ref) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **64 KiB PUT Throughput** | **363.2 MiB/s (5811 ops/s)** | ~80–220 MiB/s (`xl.meta` + data fs) | ~250–450 MiB/s (Haystack append) | ~40–110 MiB/s (LMDB/Sled) | ~60–180 MiB/s (BlueStore WAL) |
| **64 KiB GET Throughput** | **365.8 MiB/s (5852 ops/s)** | ~150–350 MiB/s (`xl.meta` fs) | ~400–650 MiB/s (Haystack O(1)) | ~80–180 MiB/s (LMDB/Sled) | ~120–300 MiB/s (BlueStore) |
| **1 MiB PUT Throughput** | **773.2 MiB/s (773 ops/s)** | ~500–1,200 MiB/s | ~600–1,400 MiB/s | ~200–500 MiB/s | ~400–1,000 MiB/s |
| **1 MiB / 10 GiB GET Throughput** | **1285.1 – 1155.3 MiB/s** | ~800–1,800 MiB/s | ~900–1,900 MiB/s | ~350–700 MiB/s | ~700–1,500 MiB/s |
| **Server Peak RSS (64K–10G)** | **1194.6 MB (Bounded 16 MiB Slab Cache)** | ~250–600 MB (Go GC heap) | ~180–450 MB (Go GC heap) | ~60–150 MB (Rust) | ~1,024–4,096 MB (OSD+RGW) |
| **Metadata Layout** | **64B `ObjectMeta` Columnar SoA** | Per-object `xl.meta` file | Needle Index map in RAM | Sled / SQLite / LMDB B-Tree | RocksDB LSM inside BlueStore |
| **Small-File Disk Layout** | **4096B WORM Needle Volume** | 2+ files/inodes per object | Haystack Volume (O(1) seek) | Content-addressed blocks | 4 KiB min alloc in BlueStore |
| **License** | **Permissive (Workspace / MIT / Apache-2.0)** | AGPLv3 (Archived OSS) | Apache 2.0 | AGPLv3 | LGPLv2.1 /v3 |
