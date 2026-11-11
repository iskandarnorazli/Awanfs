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
| **64 KiB PUT (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **338.5** | **5415.8** | **10.82** | **18.86** | **28.23** | **37.22** | **40.3 MB** |
| **64 KiB PUT (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 8.9 | 142.0 | 463.54 | 629.06 | 781.05 | 791.75 | 433.0 MB |
| **64 KiB PUT (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 6.2 | 99.8 | 636.54 | 805.14 | 880.20 | 908.13 | 109.4 MB |
| **64 KiB GET (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **317.4** | **5077.8** | **11.42** | **20.43** | **31.17** | **42.19** | **40.3 MB** |
| **64 KiB GET (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 219.8 | 3517.4 | 16.60 | 27.95 | 39.81 | 58.54 | 534.6 MB |
| **64 KiB GET (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 283.6 | 4538.1 | 13.27 | 23.24 | 33.25 | 44.41 | 157.6 MB |
| **64 KiB Mixed (80% GET / 20% PUT)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **318.1** | **5089.4** | **11.06** | **20.50** | **31.59** | **43.64** | **97.4 MB** |
| **64 KiB Mixed (80% GET / 20% PUT)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 36.3 | 581.0 | 12.96 | 498.57 | 691.29 | 746.68 | 927.7 MB |
| **64 KiB Mixed (80% GET / 20% PUT)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 40.5 | 647.9 | 35.21 | 405.36 | 447.65 | 468.15 | 177.1 MB |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **21.2** | **5439.7** | **10.19** | **20.35** | **32.79** | **43.44** | **40.3 MB** |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 14.0 | 3586.2 | 16.29 | 28.60 | 43.78 | 119.30 | 548.3 MB |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 18.1 | 4629.4 | 12.74 | 24.01 | 36.41 | 47.99 | 157.7 MB |
| **1 MiB PUT (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **750.4** | **750.4** | **41.15** | **59.19** | **74.84** | **88.74** | **75.4 MB** |
| **1 MiB PUT (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 129.1 | 129.1 | 220.60 | 419.08 | 645.72 | 700.71 | 595.2 MB |
| **1 MiB PUT (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 78.6 | 78.6 | 372.00 | 607.44 | 702.22 | 706.28 | 157.7 MB |
| **1 MiB GET (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **1025.2** | **1025.2** | **30.70** | **40.72** | **50.88** | **62.23** | **75.4 MB** |
| **1 MiB GET (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 1248.2 | 1248.2 | 23.85 | 39.50 | 58.86 | 79.90 | 927.7 MB |
| **1 MiB GET (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 788.6 | 788.6 | 39.30 | 55.90 | 72.47 | 96.33 | 157.7 MB |
| **1 MiB Mixed (80% GET / 20% PUT)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **297.5** | **297.5** | **27.14** | **35.77** | **44.55** | **54.41** | **97.4 MB** |
| **1 MiB Mixed (80% GET / 20% PUT)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 477.7 | 477.7 | 33.11 | 197.78 | 398.00 | 484.32 | 927.7 MB |
| **1 MiB Mixed (80% GET / 20% PUT)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 310.3 | 310.3 | 32.34 | 398.30 | 467.28 | 487.81 | 177.1 MB |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **726.8** | **726.8** | **21.37** | **28.12** | **35.44** | **39.56** | **97.4 MB** |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | N/A (S3 $\ge$5MiB spec limit) | N/A | N/A | N/A | N/A | N/A | 927.7 MB |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | N/A (S3 $\ge$5MiB spec limit) | N/A | N/A | N/A | N/A | N/A | 157.7 MB |
| **1 GiB Multipart Upload (16x64MiB Parts + Complete)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **557.3** | **8.7** | **693.60** | **1182.69** | **1267.69** | **1267.69** | **1129.2 MB** |
| **1 GiB Multipart Upload (16x64MiB Parts + Complete)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 767.3 | 12.0 | 575.68 | 732.81 | 1001.62 | 1001.62 | 927.7 MB |
| **1 GiB Multipart Upload (16x64MiB Parts + Complete)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 315.4 | 4.9 | 1101.73 | 1930.78 | 1973.33 | 1973.33 | 245.3 MB |
| **1 GiB Multipart Range GET (16x64MiB Parallel Ranges)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **1026.2** | **16.0** | **429.08** | **498.62** | **558.16** | **558.16** | **1129.2 MB** |
| **1 GiB Multipart Range GET (16x64MiB Parallel Ranges)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 997.0 | 15.6 | 448.74 | 594.36 | 628.85 | 628.85 | 927.7 MB |
| **1 GiB Multipart Range GET (16x64MiB Parallel Ranges)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 852.1 | 13.3 | 510.84 | 661.61 | 909.86 | 909.86 | 245.3 MB |
| **10 GiB Volume PUT (160x64MiB Streamed Parts)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **454.6** | **7.1** | **668.81** | **2213.24** | **2678.72** | **2829.95** | **1172.6 MB** |
| **10 GiB Volume PUT (160x64MiB Streamed Parts)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 85.8 | 1.3 | 739.98 | 21216.34 | 26541.78 | 28093.25 | 927.7 MB |
| **10 GiB Volume PUT (160x64MiB Streamed Parts)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 312.9 | 4.9 | 1576.16 | 2825.32 | 3280.00 | 4292.43 | 245.3 MB |
| **10 GiB Volume GET (160x64MiB Parallel Ranges)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **1152.1** | **18.0** | **440.24** | **472.01** | **489.81** | **503.63** | **1172.6 MB** |
| **10 GiB Volume GET (160x64MiB Parallel Ranges)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 1195.1 | 18.7 | 420.91 | 510.60 | 591.11 | 692.44 | 927.7 MB |
| **10 GiB Volume GET (160x64MiB Parallel Ranges)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 856.4 | 13.4 | 584.06 | 708.30 | 827.89 | 915.85 | 245.3 MB |

## 3. Concurrency Scaling Sweep (64 KiB Mixed 80% GET / 20% PUT: C in {8, 32, 64, 128})

| Concurrency (Workers) | Target Engine | Throughput (MiB/s) | Operations/sec | P50 (ms) | P90 (ms) | P99 (ms) | P99.9 (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **C = 8** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **366.8** | **5869.1** | **1.25** | **2.15** | **3.25** | **4.96** |
| **C = 8** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 57.9 | 926.5 | 2.28 | 26.76 | 71.78 | 139.05 |
| **C = 8** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 42.4 | 678.4 | 2.16 | 50.60 | 57.70 | 63.44 |
| **C = 32** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **340.5** | **5447.4** | **5.28** | **9.59** | **14.96** | **19.44** |
| **C = 32** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 41.8 | 669.5 | 7.02 | 154.73 | 379.68 | 425.81 |
| **C = 32** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 41.9 | 670.1 | 10.25 | 204.18 | 244.55 | 253.31 |
| **C = 64** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **309.1** | **4945.6** | **11.29** | **21.09** | **31.65** | **40.81** |
| **C = 64** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 40.1 | 641.2 | 17.83 | 366.54 | 639.04 | 742.73 |
| **C = 64** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 44.0 | 704.4 | 30.64 | 363.45 | 426.83 | 442.85 |
| **C = 128** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **305.9** | **4894.9** | **20.12** | **40.55** | **63.28** | **84.58** |
| **C = 128** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 41.2 | 658.9 | 33.58 | 596.11 | 1228.46 | 1341.48 |
| **C = 128** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 38.5 | 615.9 | 90.93 | 721.14 | 1035.96 | 1087.71 |

## 4. Open-Source S3 Alternatives Reference Comparison (`COMPETITIVE_LANDSCAPE.md`)

> **Provenance Note (Audit Rule 3):** Official standalone MinIO (`RELEASE.2025-09-07T16-13-09Z`, Go 1.24.6 windows/amd64) and RustFS (`v1.0.5`, Apache 2.0 Rust windows/x86_64) binaries were executed live on local NVMe pinned to Cores 0..3 with anonymous public access policy enabled via `mc.exe`, receiving identical persistent HTTP/1.1 keep-alive requests from client workers pinned to Cores 4..7. Peak Working Set RSS was measured live using Windows OS API `GetProcessMemoryInfo`. Note that for 1 MiB multipart upload with 256 KiB parts, both MinIO and RustFS strictly reject parts smaller than 5 MiB with HTTP 400 `EntityTooSmall` per AWS S3 RFC specification, whereas `s3-server` supports sub-5 MiB parts.

| Dimension | `s3-server` (Measured Local) | MinIO (Go, Measured Local) | RustFS (Rust, Measured Local) | SeaweedFS (Go, Published Ref) | Garage (Rust, Published Ref) | Ceph RGW (C++, Published Ref) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **64 KiB PUT Throughput** | **338.5 MiB/s (5416 ops/s)** | 8.9 MiB/s (142 ops/s) | 6.2 MiB/s (100 ops/s) | ~250–450 MiB/s (Haystack append) | ~40–110 MiB/s (LMDB/Sled) | ~60–180 MiB/s (BlueStore WAL) |
| **64 KiB GET Throughput** | **317.4 MiB/s (5078 ops/s)** | 219.8 MiB/s (3517 ops/s) | 283.6 MiB/s (4538 ops/s) | ~400–650 MiB/s (Haystack O(1)) | ~80–180 MiB/s (LMDB/Sled) | ~120–300 MiB/s (BlueStore) |
| **1 MiB PUT Throughput** | **750.4 MiB/s (750 ops/s)** | 129.1 MiB/s (129 ops/s) | 78.6 MiB/s (79 ops/s) | ~600–1,400 MiB/s | ~200–500 MiB/s | ~400–1,000 MiB/s |
| **1 MiB / 10 GiB GET Throughput** | **1025.2 – 1152.1 MiB/s** | 1248.2 – 1195.1 MiB/s | 788.6 – 856.4 MiB/s | ~900–1,900 MiB/s | ~350–700 MiB/s | ~700–1,500 MiB/s |
| **Server Peak RSS (64K–10G)** | **1172.6 MB (Bounded 16 MiB Slab Cache)** | 927.7 MB (Go GC heap) | 245.3 MB (Rust jemalloc) | ~180–450 MB (Go GC heap) | ~60–150 MB (Rust) | ~1,024–4,096 MB (OSD+RGW) |
| **Metadata Layout** | **64B `ObjectMeta` Columnar SoA** | Per-object `xl.meta` file | Multi-volume FS Metadata | Needle Index map in RAM | Sled / SQLite / LMDB B-Tree | RocksDB LSM inside BlueStore |
| **Small-File Disk Layout** | **4096B WORM Needle Volume** | 2+ files/inodes per object | Multi-volume File Store | Haystack Volume (O(1) seek) | Content-addressed blocks | 4 KiB min alloc in BlueStore |
| **License** | **Permissive (Workspace / MIT / Apache-2.0)** | AGPLv3 (Active Community OSS) | Apache 2.0 (Active Rust OSS) | Apache 2.0 | AGPLv3 | LGPLv2.1 /v3 |
