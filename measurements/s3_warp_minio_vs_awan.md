`python benchmarks/s3_warp/run_warp_benchmark.py --sizes 64KiB,1MiB,1GiB,10GiB --concurrent 64 --duration 3s --output docs/measurements/s3_warp_minio_vs_awan.md`

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
| **64 KiB PUT (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **331.7** | **5307.7** | **11.01** | **19.11** | **27.45** | **35.31** | **39.6 MB** |
| **64 KiB PUT (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 8.4 | 134.0 | 522.74 | 769.37 | 938.92 | 962.95 | 442.5 MB |
| **64 KiB PUT (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 8.9 | 142.0 | 442.54 | 518.52 | 603.18 | 630.05 | 106.8 MB |
| **64 KiB GET (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **320.3** | **5125.1** | **11.31** | **20.34** | **30.78** | **39.05** | **39.6 MB** |
| **64 KiB GET (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 213.5 | 3416.4 | 16.98 | 29.40 | 42.10 | 67.36 | 562.0 MB |
| **64 KiB GET (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 273.7 | 4378.6 | 13.58 | 24.16 | 34.40 | 44.43 | 157.5 MB |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **20.6** | **5261.1** | **10.37** | **21.59** | **33.90** | **44.74** | **39.6 MB** |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 12.1 | 3104.2 | 16.23 | 30.08 | 154.16 | 306.45 | 567.7 MB |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 16.8 | 4303.4 | 13.58 | 26.37 | 39.84 | 49.51 | 157.6 MB |
| **1 MiB PUT (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **765.6** | **765.6** | **40.14** | **58.40** | **71.91** | **82.89** | **78.3 MB** |
| **1 MiB PUT (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 112.9 | 112.9 | 255.25 | 419.12 | 507.71 | 625.48 | 602.8 MB |
| **1 MiB PUT (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 84.4 | 84.4 | 375.56 | 409.02 | 465.08 | 478.33 | 157.6 MB |
| **1 MiB GET (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **361.7** | **361.7** | **25.55** | **33.39** | **40.10** | **46.51** | **78.3 MB** |
| **1 MiB GET (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 1327.2 | 1327.2 | 22.39 | 38.01 | 57.05 | 96.71 | 950.5 MB |
| **1 MiB GET (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 769.4 | 769.4 | 40.28 | 56.62 | 73.50 | 91.24 | 157.6 MB |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **774.1** | **774.1** | **20.03** | **26.52** | **33.54** | **41.30** | **96.7 MB** |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | N/A (S3 $\ge$5MiB spec limit) | N/A | N/A | N/A | N/A | N/A | 950.5 MB |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | N/A (S3 $\ge$5MiB spec limit) | N/A | N/A | N/A | N/A | N/A | 157.6 MB |
| **1 GiB Multipart Upload (16x64MiB Parts + Complete)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **616.3** | **9.6** | **636.89** | **1083.52** | **1151.83** | **1151.83** | **1156.2 MB** |
| **1 GiB Multipart Upload (16x64MiB Parts + Complete)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 880.6 | 13.8 | 481.53 | 683.16 | 704.26 | 704.26 | 951.8 MB |
| **1 GiB Multipart Upload (16x64MiB Parts + Complete)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 386.9 | 6.0 | 1440.89 | 1532.83 | 1557.21 | 1557.21 | 157.6 MB |
| **1 GiB Multipart Range GET (16x64MiB Parallel Ranges)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **1190.7** | **18.6** | **350.04** | **485.67** | **504.17** | **504.17** | **1156.2 MB** |
| **1 GiB Multipart Range GET (16x64MiB Parallel Ranges)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 1291.9 | 20.2 | 336.77 | 435.52 | 439.46 | 439.46 | 951.8 MB |
| **1 GiB Multipart Range GET (16x64MiB Parallel Ranges)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 937.5 | 14.6 | 510.87 | 567.69 | 608.29 | 608.29 | 195.3 MB |
| **10 GiB Volume PUT (160x64MiB Streamed Parts)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **770.2** | **12.0** | **631.43** | **719.79** | **1022.08** | **1170.64** | **1163.1 MB** |
| **10 GiB Volume PUT (160x64MiB Streamed Parts)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 193.3 | 3.0 | 577.69 | 6604.64 | 21707.19 | 24159.04 | 959.6 MB |
| **10 GiB Volume PUT (160x64MiB Streamed Parts)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 211.0 | 3.4 | 1793.43 | 3491.94 | 9588.33 | 11325.48 | 195.3 MB |
| **10 GiB Volume GET (160x64MiB Parallel Ranges)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **1165.0** | **18.2** | **438.48** | **461.77** | **477.60** | **484.37** | **1163.1 MB** |
| **10 GiB Volume GET (160x64MiB Parallel Ranges)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 1216.4 | 19.0 | 411.45 | 522.01 | 608.61 | 656.52 | 959.6 MB |
| **10 GiB Volume GET (160x64MiB Parallel Ranges)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 897.3 | 14.0 | 558.48 | 682.25 | 796.80 | 887.38 | 202.9 MB |

## 3. Open-Source S3 Alternatives Reference Comparison (`COMPETITIVE_LANDSCAPE.md`)

> **Provenance Note (Audit Rule 3):** Official standalone MinIO (`RELEASE.2025-09-07T16-13-09Z`, Go 1.24.6 windows/amd64) and RustFS (`v1.0.5`, Apache 2.0 Rust windows/x86_64) binaries were executed live on local NVMe pinned to Cores 0..3 with anonymous public access policy enabled via `mc.exe`, receiving identical persistent HTTP/1.1 keep-alive requests from client workers pinned to Cores 4..7. Peak Working Set RSS was measured live using Windows OS API `GetProcessMemoryInfo`. Note that for 1 MiB multipart upload with 256 KiB parts, both MinIO and RustFS strictly reject parts smaller than 5 MiB with HTTP 400 `EntityTooSmall` per AWS S3 RFC specification, whereas `s3-server` supports sub-5 MiB parts.

| Dimension | `s3-server` (Measured Local) | MinIO (Go, Measured Local) | RustFS (Rust, Measured Local) | SeaweedFS (Go, Published Ref) | Garage (Rust, Published Ref) | Ceph RGW (C++, Published Ref) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **64 KiB PUT Throughput** | **331.7 MiB/s (5308 ops/s)** | 8.4 MiB/s (134 ops/s) | 8.9 MiB/s (142 ops/s) | ~250–450 MiB/s (Haystack append) | ~40–110 MiB/s (LMDB/Sled) | ~60–180 MiB/s (BlueStore WAL) |
| **64 KiB GET Throughput** | **320.3 MiB/s (5125 ops/s)** | 213.5 MiB/s (3416 ops/s) | 273.7 MiB/s (4379 ops/s) | ~400–650 MiB/s (Haystack O(1)) | ~80–180 MiB/s (LMDB/Sled) | ~120–300 MiB/s (BlueStore) |
| **1 MiB PUT Throughput** | **765.6 MiB/s (766 ops/s)** | 112.9 MiB/s (113 ops/s) | 84.4 MiB/s (84 ops/s) | ~600–1,400 MiB/s | ~200–500 MiB/s | ~400–1,000 MiB/s |
| **1 MiB / 10 GiB GET Throughput** | **361.7 – 1165.0 MiB/s** | 1327.2 – 1216.4 MiB/s | 769.4 – 897.3 MiB/s | ~900–1,900 MiB/s | ~350–700 MiB/s | ~700–1,500 MiB/s |
| **Server Peak RSS (64K–10G)** | **1163.1 MB (Bounded 16 MiB Slab Cache)** | 959.6 MB (Go GC heap) | 202.9 MB (Rust jemalloc) | ~180–450 MB (Go GC heap) | ~60–150 MB (Rust) | ~1,024–4,096 MB (OSD+RGW) |
| **Metadata Layout** | **64B `ObjectMeta` Columnar SoA** | Per-object `xl.meta` file | Multi-volume FS Metadata | Needle Index map in RAM | Sled / SQLite / LMDB B-Tree | RocksDB LSM inside BlueStore |
| **Small-File Disk Layout** | **4096B WORM Needle Volume** | 2+ files/inodes per object | Multi-volume File Store | Haystack Volume (O(1) seek) | Content-addressed blocks | 4 KiB min alloc in BlueStore |
| **License** | **Permissive (Workspace / MIT / Apache-2.0)** | AGPLv3 (Active Community OSS) | Apache 2.0 (Active Rust OSS) | Apache 2.0 | AGPLv3 | LGPLv2.1 /v3 |
