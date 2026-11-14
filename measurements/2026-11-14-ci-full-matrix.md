Command: `python -u benchmarks/run_warp_benchmark.py --sizes 64KiB,1MiB --concurrent 64 --duration 2 --output ci_full_matrix.md` (`.github/workflows/bench-full.yml`)

Run on GitHub Actions: https://github.com/iskandarnorazli/Awanfs/actions/runs/43537255399 (scheduled, main at a7dc98b, 2026-11-14). Both jobs finished: ubuntu-latest in 7 min 48 s (job 132269140223), windows-latest in 16 min 10 s (job 132269140005). This is the first completed full matrix on hosted runners (#135).

# CI full matrix: AwanFS, MinIO and RustFS (2026-11-14)

Tables below are copied from each job log. They are this run's output, not edited.

Read them with these limits:

- a7dc98b is before PR #134 (glibc mmap threshold, decision 42), so Linux AwanFS RSS here is with the default allocator.
- The report's durability line in this run said AwanFS PUTs are not synced. That was stale: the benchmark starts `s3-server` with its default, group commit (#29), so a 200 means the bytes are on stable storage. Fixed in the report generator on 2026-11-14. The line is left out of the copy below.
- Phases are 2 s on shared 4-CPU VMs, one run each. Use `measurements/ratio_*.md` (5 runs, medians) for ratios to MinIO.
- Phases 7 to 10 (1 GiB and 10 GiB) do not run with `--sizes 64KiB,1MiB`. The log carries markers [1/10] to [6/10], the two mixed phases and the four sweep phases for each server, each with its warm-up time.
- MinIO is built from source at RELEASE.2025-09-07T16-13-09Z, so it reports itself as DEVELOPMENT.GOGET.

## ubuntu-latest

- **CPU Model:** AMD EPYC 9V74 80-Core Processor
- **Logical Cores:** 4
- **Host RAM:** 15.6 GB
- **Operating System:** Linux 7.0.0-1017-azure (x86_64)

### Live Measured HTTP/1.1 Keep-Alive Throughput & Latency Matrix (64 KiB – 10 GiB)

| Workload | Target Engine | Throughput (MiB/s) | Operations/sec | P50 (ms) | P90 (ms) | P99 (ms) | P99.9 (ms) | Peak RSS (MB) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **64 KiB PUT (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **253.1** | **4049.5** | **13.38** | **27.48** | **43.32** | **57.11** | **129.2 MB** |
| **64 KiB PUT (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 80.1 | 1281.2 | 45.49 | 79.22 | 115.15 | 131.05 | 483.2 MB |
| **64 KiB PUT (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 95.3 | 1525.5 | 41.27 | 48.95 | 56.45 | 62.76 | 195.8 MB |
| **64 KiB GET (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **309.7** | **4954.8** | **11.46** | **20.34** | **29.66** | **37.57** | **138.9 MB** |
| **64 KiB GET (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 185.3 | 2965.5 | 18.44 | 36.15 | 56.79 | 74.16 | 483.8 MB |
| **64 KiB GET (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 239.9 | 3838.7 | 14.69 | 27.40 | 42.45 | 54.37 | 236.9 MB |
| **64 KiB Mixed (80% GET / 20% PUT)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **282.2** | **4515.4** | **12.83** | **23.08** | **34.23** | **44.05** | **310.5 MB** |
| **64 KiB Mixed (80% GET / 20% PUT)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 165.6 | 2650.1 | 20.76 | 40.70 | 62.83 | 81.53 | 845.7 MB |
| **64 KiB Mixed (80% GET / 20% PUT)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 179.8 | 2876.2 | 18.19 | 41.71 | 65.91 | 85.93 | 287.4 MB |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **24.5** | **6273.0** | **7.92** | **18.66** | **30.62** | **41.89** | **138.9 MB** |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 12.7 | 3239.1 | 15.74 | 35.30 | 59.14 | 82.93 | 483.8 MB |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 16.6 | 4238.7 | 11.50 | 27.91 | 50.14 | 67.40 | 237.5 MB |
| **1 MiB PUT (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **434.0** | **434.0** | **79.29** | **117.96** | **180.64** | **229.57** | **298.6 MB** |
| **1 MiB PUT (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 373.1 | 373.1 | 84.34 | 130.66 | 169.37 | 187.23 | 483.8 MB |
| **1 MiB PUT (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 357.1 | 357.1 | 87.69 | 122.73 | 162.60 | 174.96 | 275.1 MB |
| **1 MiB GET (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **3347.4** | **3347.4** | **8.76** | **15.81** | **23.68** | **31.44** | **310.5 MB** |
| **1 MiB GET (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 1823.2 | 1823.2 | 15.73 | 29.08 | 43.79 | 54.57 | 845.7 MB |
| **1 MiB GET (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 1136.8 | 1136.8 | 26.95 | 38.64 | 51.27 | 58.25 | 275.1 MB |
| **1 MiB Mixed (80% GET / 20% PUT)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **2117.6** | **2117.6** | **11.47** | **26.05** | **78.73** | **111.00** | **310.5 MB** |
| **1 MiB Mixed (80% GET / 20% PUT)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 1401.4 | 1401.4 | 19.55 | 40.87 | 67.03 | 87.90 | 901.8 MB |
| **1 MiB Mixed (80% GET / 20% PUT)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 953.2 | 953.2 | 24.95 | 70.69 | 105.54 | 137.33 | 320.0 MB |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **363.7** | **363.7** | **44.07** | **61.10** | **77.24** | **83.46** | **310.5 MB** |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | N/A (S3 $\ge$5MiB spec limit) | N/A | N/A | N/A | N/A | N/A | 845.7 MB |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | N/A (S3 $\ge$5MiB spec limit) | N/A | N/A | N/A | N/A | N/A | 275.1 MB |

### Concurrency Scaling Sweep (64 KiB Mixed 80% GET / 20% PUT: C in {8, 32, 64, 128})

| Concurrency (Workers) | Target Engine | Throughput (MiB/s) | Operations/sec | P50 (ms) | P90 (ms) | P99 (ms) | P99.9 (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **C = 8** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **263.2** | **4210.9** | **1.79** | **2.88** | **4.17** | **5.46** |
| **C = 8** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 154.0 | 2464.6 | 2.87 | 5.63 | 9.49 | 13.53 |
| **C = 8** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 182.4 | 2919.2 | 2.21 | 5.47 | 8.16 | 10.51 |
| **C = 32** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **284.1** | **4544.9** | **6.50** | **11.28** | **16.30** | **21.45** |
| **C = 32** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 155.9 | 2493.9 | 11.03 | 22.18 | 40.56 | 65.61 |
| **C = 32** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 192.7 | 3082.9 | 9.30 | 17.91 | 27.33 | 34.24 |
| **C = 64** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **281.3** | **4500.6** | **12.65** | **22.87** | **34.64** | **42.46** |
| **C = 64** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 166.7 | 2666.6 | 19.91 | 40.72 | 65.51 | 85.55 |
| **C = 64** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 186.3 | 2980.7 | 18.24 | 36.59 | 56.57 | 74.93 |
| **C = 128** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **273.0** | **4368.3** | **23.30** | **45.14** | **68.40** | **85.83** |
| **C = 128** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 161.2 | 2578.7 | 36.34 | 82.13 | 137.59 | 177.61 |
| **C = 128** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 183.1 | 2929.0 | 32.84 | 69.56 | 110.04 | 145.47 |


## windows-latest

- **CPU Model:** Intel64 Family 6 Model 207 Stepping 2, GenuineIntel
- **Logical Cores:** 4
- **Host RAM:** 16.0 GB
- **Operating System:** Windows 2025Server (AMD64)

### Live Measured HTTP/1.1 Keep-Alive Throughput & Latency Matrix (64 KiB – 10 GiB)

| Workload | Target Engine | Throughput (MiB/s) | Operations/sec | P50 (ms) | P90 (ms) | P99 (ms) | P99.9 (ms) | Peak RSS (MB) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **64 KiB PUT (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **7.7** | **122.4** | **491.08** | **668.22** | **793.62** | **857.61** | **68.6 MB** |
| **64 KiB PUT (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 10.6 | 169.1 | 263.18 | 717.68 | 1199.92 | 1211.13 | 425.3 MB |
| **64 KiB PUT (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 2.9 | 45.8 | 1232.94 | 1380.25 | 2224.81 | 2327.71 | 106.1 MB |
| **64 KiB GET (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **249.6** | **3993.0** | **14.29** | **24.10** | **33.55** | **43.78** | **68.6 MB** |
| **64 KiB GET (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 165.8 | 2652.9 | 20.88 | 36.56 | 57.37 | 81.99 | 548.0 MB |
| **64 KiB GET (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 208.9 | 3342.5 | 15.79 | 31.89 | 55.20 | 85.69 | 149.4 MB |
| **64 KiB Mixed (80% GET / 20% PUT)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **28.5** | **456.0** | **63.45** | **323.43** | **631.08** | **794.95** | **222.1 MB** |
| **64 KiB Mixed (80% GET / 20% PUT)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 40.3 | 644.1 | 11.33 | 213.24 | 1303.63 | 1525.76 | 1012.5 MB |
| **64 KiB Mixed (80% GET / 20% PUT)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 13.9 | 222.3 | 10.57 | 1205.06 | 1765.57 | 2059.34 | 171.5 MB |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **19.2** | **4926.7** | **10.26** | **22.08** | **37.00** | **53.12** | **68.6 MB** |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 11.1 | 2832.7 | 16.83 | 32.41 | 56.19 | 92.68 | 623.1 MB |
| **64 KiB Range GET (bytes=0-4095, 206 Partial)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 14.7 | 3761.1 | 12.47 | 31.77 | 62.03 | 97.25 | 149.4 MB |
| **1 MiB PUT (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **61.0** | **61.0** | **500.08** | **644.38** | **799.16** | **844.43** | **222.1 MB** |
| **1 MiB PUT (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 29.1 | 29.1 | 1006.41 | 1813.53 | 1980.32 | 2102.05 | 623.1 MB |
| **1 MiB PUT (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 25.5 | 25.5 | 1177.38 | 1380.29 | 1670.34 | 1670.60 | 149.4 MB |
| **1 MiB GET (Single Object)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **2018.7** | **2018.7** | **10.51** | **30.37** | **88.45** | **160.45** | **222.1 MB** |
| **1 MiB GET (Single Object)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 1066.9 | 1066.9 | 25.17 | 44.81 | 79.56 | 104.76 | 1011.0 MB |
| **1 MiB GET (Single Object)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 620.5 | 620.5 | 46.01 | 68.26 | 88.68 | 107.48 | 149.4 MB |
| **1 MiB Mixed (80% GET / 20% PUT)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **272.2** | **272.2** | **72.81** | **303.73** | **516.98** | **600.07** | **222.1 MB** |
| **1 MiB Mixed (80% GET / 20% PUT)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 133.2 | 133.2 | 6.07 | 1042.72 | 1692.50 | 2108.60 | 1012.5 MB |
| **1 MiB Mixed (80% GET / 20% PUT)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 126.9 | 126.9 | 37.93 | 1028.64 | 1539.03 | 1648.29 | 171.5 MB |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **16.0** | **16.0** | **997.31** | **1168.80** | **1211.73** | **1211.73** | **222.1 MB** |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | N/A (S3 $\ge$5MiB spec limit) | N/A | N/A | N/A | N/A | N/A | 1011.0 MB |
| **1 MiB Multipart Upload (4x256KiB Parts + Complete)** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | N/A (S3 $\ge$5MiB spec limit) | N/A | N/A | N/A | N/A | N/A | 149.4 MB |

### Concurrency Scaling Sweep (64 KiB Mixed 80% GET / 20% PUT: C in {8, 32, 64, 128})

| Concurrency (Workers) | Target Engine | Throughput (MiB/s) | Operations/sec | P50 (ms) | P90 (ms) | P99 (ms) | P99.9 (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **C = 8** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **17.5** | **279.8** | **0.94** | **141.87** | **160.40** | **206.78** |
| **C = 8** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 48.0 | 767.5 | 1.75 | 11.96 | 63.32 | 1016.76 |
| **C = 8** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 13.7 | 218.6 | 1.04 | 172.76 | 215.32 | 302.61 |
| **C = 32** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **24.4** | **390.5** | **49.42** | **225.40** | **355.38** | **395.17** |
| **C = 32** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 62.6 | 1002.1 | 4.82 | 82.83 | 464.65 | 555.70 |
| **C = 32** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 12.8 | 205.0 | 4.14 | 638.31 | 909.25 | 917.64 |
| **C = 64** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **25.7** | **411.3** | **59.29** | **386.26** | **650.63** | **706.61** |
| **C = 64** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 27.7 | 443.5 | 8.78 | 253.77 | 1717.14 | 1778.46 |
| **C = 64** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 12.2 | 196.0 | 10.05 | 1161.68 | 2132.56 | 2141.90 |
| **C = 128** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **25.6** | **410.1** | **49.54** | **1015.62** | **1654.77** | **1740.78** |
| **C = 128** | MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z) | 45.3 | 725.2 | 32.66 | 339.32 | 1738.55 | 1868.43 |
| **C = 128** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | 14.4 | 230.1 | 23.23 | 2370.51 | 2442.67 | 2451.48 |

