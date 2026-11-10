`python benchmarks/s3_warp/run_warp_benchmark.py --sizes 64KiB,1MiB,1GiB,10GiB --concurrent 64 --duration 3s --output docs/measurements/s3_warp_minio_vs_awan.md`

# Head-to-Head S3 Benchmark Report: Awan Storage vs S3 Baselines (#466)

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
| **64 KiB PUT (Single Object)** | Awan Storage Server (`awan-storage` `--release`) | **374.0** | **5984.3** | **9.82** | **17.13** | **24.90** | **31.96** | **41.4 MB** |
| **64 KiB PUT (Single Object)** | File-per-Object + `.meta` Sidecar Baseline (Local Measured) | 63.0 | 1007.2 | 62.70 | 68.92 | 124.04 | 135.37 | 798.3 MB |
| **64 KiB GET (Single Object)** | Awan Storage Server (`awan-storage` `--release`) | **378.8** | **6061.3** | **9.49** | **17.29** | **26.52** | **34.20** | **41.4 MB** |
| **64 KiB GET (Single Object)** | File-per-Object + `.meta` Sidecar Baseline (Local Measured) | 123.1 | 1969.6 | 29.21 | 43.17 | 74.21 | 323.62 | 798.3 MB |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | Awan Storage Server (`awan-storage` `--release`) | **25.7** | **6585.2** | **8.32** | **17.55** | **27.68** | **36.33** | **41.4 MB** |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | File-per-Object + `.meta` Sidecar Baseline (Local Measured) | 8.0 | 2057.4 | 27.96 | 41.16 | 68.06 | 395.67 | 798.3 MB |
| **1 MiB PUT (Single Object)** | Awan Storage Server (`awan-storage` `--release`) | **794.4** | **794.4** | **37.21** | **61.91** | **91.58** | **114.35** | **82.9 MB** |
| **1 MiB PUT (Single Object)** | File-per-Object + `.meta` Sidecar Baseline (Local Measured) | 864.8 | 864.8 | 36.01 | 40.66 | 59.46 | 67.11 | 798.3 MB |
| **1 MiB GET (Single Object)** | Awan Storage Server (`awan-storage` `--release`) | **1273.8** | **1273.8** | **24.59** | **31.82** | **40.55** | **48.73** | **82.9 MB** |
| **1 MiB GET (Single Object)** | File-per-Object + `.meta` Sidecar Baseline (Local Measured) | 1232.2 | 1232.2 | 24.94 | 34.06 | 44.11 | 94.36 | 798.3 MB |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | Awan Storage Server (`awan-storage` `--release`) | **826.8** | **826.8** | **18.79** | **24.64** | **31.75** | **37.94** | **98.7 MB** |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | File-per-Object + `.meta` Sidecar Baseline (Local Measured) | 244.0 | 244.0 | 64.47 | 71.80 | 90.89 | 91.95 | 798.3 MB |
| **1 GiB Multipart Upload (16x64MiB Parts + Complete)** | Awan Storage Server (`awan-storage` `--release`) | **595.5** | **9.3** | **648.62** | **1034.92** | **1117.45** | **1117.45** | **1185.0 MB** |
| **1 GiB Multipart Upload (16x64MiB Parts + Complete)** | File-per-Object + `.meta` Sidecar Baseline (Local Measured) | 1582.6 | 24.7 | 277.59 | 415.67 | 423.16 | 423.16 | 798.3 MB |
| **1 GiB Multipart Range GET (16x64MiB Parallel Ranges)** | Awan Storage Server (`awan-storage` `--release`) | **1203.8** | **18.8** | **362.86** | **496.83** | **520.50** | **520.50** | **1185.0 MB** |
| **1 GiB Multipart Range GET (16x64MiB Parallel Ranges)** | File-per-Object + `.meta` Sidecar Baseline (Local Measured) | 1198.1 | 18.7 | 360.03 | 482.72 | 511.48 | 511.48 | 992.3 MB |
| **10 GiB Volume PUT (160x64MiB Streamed Parts)** | Awan Storage Server (`awan-storage` `--release`) | **806.1** | **12.6** | **607.53** | **684.92** | **923.00** | **1074.07** | **1193.0 MB** |
| **10 GiB Volume PUT (160x64MiB Streamed Parts)** | File-per-Object + `.meta` Sidecar Baseline (Local Measured) | 428.0 | 6.7 | 477.96 | 2419.96 | 4186.85 | 5146.06 | 992.3 MB |
| **10 GiB Volume GET (160x64MiB Parallel Ranges)** | Awan Storage Server (`awan-storage` `--release`) | **1244.1** | **19.4** | **408.28** | **432.61** | **447.76** | **458.06** | **1193.0 MB** |
| **10 GiB Volume GET (160x64MiB Parallel Ranges)** | File-per-Object + `.meta` Sidecar Baseline (Local Measured) | 1277.0 | 20.0 | 394.22 | 462.72 | 528.95 | 562.98 | 1152.7 MB |

## 3. Open-Source S3 Alternatives Reference Comparison (`COMPETITIVE_LANDSCAPE.md`)

> **Provenance Note (Audit Rule 3):** Live `minio.exe` binary download from `dl.min.io` returned `HTTP 410 Gone` because the upstream MinIO open-source repository was archived. Above table reports live OS-measured numbers on this machine for `awan-storage-server --release` (with `GetProcessMemoryInfo` Peak Working Set RSS and persistent HTTP/1.1 keep-alive connections) alongside the local file-per-object + `.meta` sidecar HTTP baseline server. Below table compares `awan-storage` against published architectural characteristics of open-source S3 alternatives.

| Dimension | `awan-storage` (Measured Local) | MinIO (Go, Published Ref) | SeaweedFS (Go, Published Ref) | Garage (Rust, Published Ref) | Ceph RGW (C++, Published Ref) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **64 KiB PUT Throughput** | **374.0 MiB/s (5984 ops/s)** | ~80–220 MiB/s (`xl.meta` + data fs) | ~250–450 MiB/s (Haystack append) | ~40–110 MiB/s (LMDB/Sled) | ~60–180 MiB/s (BlueStore WAL) |
| **64 KiB GET Throughput** | **378.8 MiB/s (6061 ops/s)** | ~150–350 MiB/s (`xl.meta` fs) | ~400–650 MiB/s (Haystack O(1)) | ~80–180 MiB/s (LMDB/Sled) | ~120–300 MiB/s (BlueStore) |
| **1 MiB PUT Throughput** | **794.4 MiB/s (794 ops/s)** | ~500–1,200 MiB/s | ~600–1,400 MiB/s | ~200–500 MiB/s | ~400–1,000 MiB/s |
| **1 MiB / 10 GiB GET Throughput** | **1273.8 – 1244.1 MiB/s** | ~800–1,800 MiB/s | ~900–1,900 MiB/s | ~350–700 MiB/s | ~700–1,500 MiB/s |
| **Server Peak RSS (64K–10G)** | **1193.0 MB (Bounded 16 MiB Slab Cache)** | ~250–600 MB (Go GC heap) | ~180–450 MB (Go GC heap) | ~60–150 MB (Rust) | ~1,024–4,096 MB (OSD+RGW) |
| **Metadata Layout** | **64B `@flat ObjectMeta` SoA** | Per-object `xl.meta` file | Needle Index map in RAM | Sled / SQLite / LMDB B-Tree | RocksDB LSM inside BlueStore |
| **Small-File Disk Layout** | **4096B WORM Needle Volume** | 2+ files/inodes per object | Haystack Volume (O(1) seek) | Content-addressed blocks | 4 KiB min alloc in BlueStore |
| **License** | **Permissive (Workspace)** | AGPLv3 (Archived OSS) | Apache 2.0 | AGPLv3 | LGPLv2.1 /v3 |
