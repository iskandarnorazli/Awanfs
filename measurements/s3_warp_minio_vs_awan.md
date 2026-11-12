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
- **Durability per PUT:** `s3-server` acknowledges a PUT before it is flushed to disk (no fsync on the write path; group commit is #490). MinIO and RustFS fsync each PUT, so PUT and mixed rows compare a non-durable write with durable ones.

## 2. Live Measured HTTP/1.1 Keep-Alive Throughput & Latency Matrix (64 KiB – 10 GiB)

| Workload | Target Engine | Throughput (MiB/s) | Operations/sec | P50 (ms) | P90 (ms) | P99 (ms) | P99.9 (ms) | Peak RSS (MB) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **64 KiB PUT (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **349.4** | **5590.1** | **10.06** | **18.89** | **31.89** | **40.78** | **100.6 MB** |
| **64 KiB PUT (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 8.5 | 135.5 | 505.72 | 732.39 | 864.74 | 877.58 | 420.0 MB |
| **64 KiB PUT (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 9.4 | 149.9 | 416.51 | 514.23 | 600.80 | 627.42 | 105.7 MB |
| **64 KiB GET (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **339.1** | **5424.8** | **10.57** | **19.02** | **29.42** | **36.93** | **100.6 MB** |
| **64 KiB GET (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 225.6 | 3610.1 | 15.87 | 27.38 | 41.22 | 53.29 | 530.9 MB |
| **64 KiB GET (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 256.4 | 4102.1 | 14.52 | 24.68 | 37.47 | 47.03 | 156.9 MB |
| **64 KiB Mixed (80% GET / 20% PUT)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **310.1** | **4962.1** | **10.69** | **21.91** | **41.07** | **74.21** | **245.6 MB** |
| **64 KiB Mixed (80% GET / 20% PUT)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 37.9 | 606.2 | 16.07 | 383.23 | 714.03 | 801.16 | 936.4 MB |
| **64 KiB Mixed (80% GET / 20% PUT)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 40.0 | 640.5 | 31.82 | 423.23 | 472.16 | 482.41 | 172.5 MB |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **23.1** | **5907.1** | **9.13** | **19.16** | **29.96** | **39.66** | **100.6 MB** |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 14.1 | 3612.4 | 15.85 | 28.57 | 41.90 | 66.29 | 550.0 MB |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 17.1 | 4376.5 | 13.31 | 24.99 | 39.78 | 54.93 | 157.0 MB |
| **1 MiB PUT (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **737.3** | **737.3** | **40.64** | **64.00** | **93.31** | **126.00** | **245.6 MB** |
| **1 MiB PUT (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 126.3 | 126.3 | 227.82 | 377.29 | 510.44 | 554.83 | 568.8 MB |
| **1 MiB PUT (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 65.0 | 65.0 | 508.43 | 599.85 | 627.85 | 627.86 | 157.0 MB |
| **1 MiB GET (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **2165.6** | **2165.6** | **14.12** | **21.00** | **27.48** | **33.60** | **245.6 MB** |
| **1 MiB GET (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 1307.7 | 1307.7 | 22.45 | 38.22 | 55.63 | 125.04 | 936.4 MB |
| **1 MiB GET (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 773.7 | 773.7 | 39.99 | 57.56 | 73.70 | 83.05 | 157.0 MB |
| **1 MiB Mixed (80% GET / 20% PUT)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **1734.5** | **1734.5** | **17.29** | **26.27** | **41.82** | **66.97** | **245.6 MB** |
| **1 MiB Mixed (80% GET / 20% PUT)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 481.0 | 481.0 | 35.48 | 161.00 | 500.92 | 608.41 | 936.4 MB |
| **1 MiB Mixed (80% GET / 20% PUT)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 305.4 | 305.4 | 37.05 | 403.58 | 457.28 | 467.77 | 172.5 MB |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **188.1** | **188.1** | **77.30** | **129.38** | **180.01** | **203.72** | **245.6 MB** |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | N/A (S3 $\ge$5MiB spec limit) | N/A | N/A | N/A | N/A | N/A | 936.4 MB |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | N/A (S3 $\ge$5MiB spec limit) | N/A | N/A | N/A | N/A | N/A | 157.0 MB |
| **1 GiB Multipart Upload (16x64MiB Parts + Complete)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **628.2** | **9.8** | **841.92** | **914.61** | **916.68** | **916.68** | **660.3 MB** |
| **1 GiB Multipart Upload (16x64MiB Parts + Complete)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 848.1 | 13.3 | 497.89 | 654.07 | 679.27 | 679.27 | 936.4 MB |
| **1 GiB Multipart Upload (16x64MiB Parts + Complete)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 453.9 | 7.1 | 1021.27 | 1136.78 | 1141.16 | 1141.16 | 266.8 MB |
| **1 GiB Multipart Range GET (16x64MiB Parallel Ranges)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **1119.3** | **17.5** | **401.12** | **456.02** | **506.68** | **506.68** | **1135.2 MB** |
| **1 GiB Multipart Range GET (16x64MiB Parallel Ranges)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 1252.3 | 19.6 | 375.43 | 435.29 | 449.46 | 449.46 | 936.4 MB |
| **1 GiB Multipart Range GET (16x64MiB Parallel Ranges)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 879.1 | 13.7 | 508.63 | 594.63 | 855.12 | 855.12 | 266.8 MB |
| **10 GiB Volume PUT (160x64MiB Streamed Parts)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **140.3** | **2.2** | **1875.89** | **12159.81** | **19866.48** | **20554.22** | **2281.4 MB** |
| **10 GiB Volume PUT (160x64MiB Streamed Parts)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 154.9 | 2.4 | 930.07 | 8853.66 | 21516.72 | 21936.33 | 936.4 MB |
| **10 GiB Volume PUT (160x64MiB Streamed Parts)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 253.2 | 4.1 | 880.80 | 6432.06 | 6906.43 | 6961.65 | 266.8 MB |
| **10 GiB Volume GET (160x64MiB Parallel Ranges)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **1412.7** | **22.1** | **354.72** | **434.58** | **508.42** | **546.97** | **2281.4 MB** |
| **10 GiB Volume GET (160x64MiB Parallel Ranges)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 1149.2 | 18.0 | 430.59 | 552.03 | 653.67 | 781.12 | 936.4 MB |
| **10 GiB Volume GET (160x64MiB Parallel Ranges)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 834.8 | 13.0 | 602.91 | 683.69 | 736.30 | 801.23 | 266.8 MB |

## 3. Concurrency Scaling Sweep (64 KiB Mixed 80% GET / 20% PUT: C in {8, 32, 64, 128})

| Concurrency (Workers) | Target Engine | Throughput (MiB/s) | Operations/sec | P50 (ms) | P90 (ms) | P99 (ms) | P99.9 (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **C = 8** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **337.3** | **5397.3** | **1.31** | **2.26** | **3.53** | **5.34** |
| **C = 8** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 59.6 | 953.4 | 2.40 | 26.57 | 67.80 | 87.81 |
| **C = 8** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 41.8 | 669.2 | 2.13 | 51.40 | 62.47 | 70.30 |
| **C = 32** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **322.9** | **5165.8** | **5.50** | **10.25** | **16.84** | **31.54** |
| **C = 32** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 39.6 | 632.9 | 9.07 | 156.34 | 421.79 | 473.88 |
| **C = 32** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 41.0 | 656.4 | 9.27 | 211.06 | 229.85 | 236.96 |
| **C = 64** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **325.1** | **5201.9** | **10.62** | **20.41** | **31.07** | **39.60** |
| **C = 64** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 47.6 | 762.1 | 17.49 | 272.51 | 645.00 | 673.38 |
| **C = 64** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 41.2 | 658.8 | 33.40 | 388.77 | 467.76 | 497.75 |
| **C = 128** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **324.1** | **5185.8** | **19.18** | **38.38** | **60.03** | **78.86** |
| **C = 128** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 37.8 | 604.3 | 42.83 | 816.70 | 1239.43 | 1442.95 |
| **C = 128** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 39.4 | 631.1 | 81.25 | 687.60 | 958.97 | 980.89 |

## 4. Open-Source S3 Alternatives Reference Comparison (`COMPETITIVE_LANDSCAPE.md`)

> **Provenance Note (Audit Rule 3):** Official standalone MinIO (`RELEASE.2025-09-07T16-13-09Z`, Go 1.24.6 windows/amd64) and RustFS (`v1.0.5`, Apache 2.0 Rust windows/x86_64) binaries were executed live on local NVMe pinned to Cores 0..3 with anonymous public access policy enabled via `mc.exe`, receiving identical persistent HTTP/1.1 keep-alive requests from client workers pinned to Cores 4..7. Peak Working Set RSS was measured live using Windows OS API `GetProcessMemoryInfo`. Note that for 1 MiB multipart upload with 256 KiB parts, both MinIO and RustFS strictly reject parts smaller than 5 MiB with HTTP 400 `EntityTooSmall` per AWS S3 RFC specification, whereas `s3-server` accepts them, which does not conform to S3. Reference figures for SeaweedFS, Garage and Ceph RGW are not measured here and are left out.

| Dimension | `s3-server` (Measured Local) | MinIO (Go, Measured Local) | RustFS (Rust, Measured Local) |
| :--- | :--- | :--- | :--- |
| **64 KiB PUT Throughput** | **349.4 MiB/s (5590 ops/s)** | 8.5 MiB/s (136 ops/s) | 9.4 MiB/s (150 ops/s) |
| **64 KiB GET Throughput** | **339.1 MiB/s (5425 ops/s)** | 225.6 MiB/s (3610 ops/s) | 256.4 MiB/s (4102 ops/s) |
| **1 MiB PUT Throughput** | **737.3 MiB/s (737 ops/s)** | 126.3 MiB/s (126 ops/s) | 65.0 MiB/s (65 ops/s) |
| **1 MiB / 10 GiB GET Throughput** | **2165.6 – 1412.7 MiB/s** | 1307.7 – 1149.2 MiB/s | 773.7 – 834.8 MiB/s |
| **Server Peak RSS (64K–10G)** | **2281.4 MB (Bounded 16 MiB Slab Cache)** | 936.4 MB (Go GC heap) | 266.8 MB (Rust jemalloc) |
| **Metadata Layout** | **64B `ObjectMeta` Columnar SoA** | Per-object `xl.meta` file | Multi-volume FS Metadata |
| **Small-File Disk Layout** | **4096B WORM Needle Volume** | 2+ files/inodes per object | Multi-volume File Store |
| **License** | **Permissive (Workspace / MIT / Apache-2.0)** | AGPLv3 (Active Community OSS) | Apache 2.0 (Active Rust OSS) |
