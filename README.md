# S3 Storage Server (`s3-server`)

> **High-Performance, S3-Compatible Object Storage Daemon in Pure Rust**  
> Engineered for microsecond-latency compute, AI agent sandboxes, and high-throughput checkpointing.  
> Zero garbage collection pauses. Columnar Struct-of-Arrays (SoA) metadata indexing. 4096B WORM needle volume storage.

---

## 1. Executive Summary

`s3-server` is a standalone, drop-in AWS S3-compatible object storage server implemented 100% in pure Rust. It combines:

1. **64-Byte Columnar Struct-of-Arrays (`ObjectMeta`) Metadata Index:**
   - 1,000,000 objects indexed in ~40 MB of RAM.
   - O(1) hash lookup via `xxh3` hash slots and arena-backed string deduplication.
   - BTree-backed lexicographical prefix and range scans (`ListObjectsV2`).

2. **4096-Byte Sector-Aligned WORM Needle Volume Storage:**
   - Sequential append-only write path eliminating file-system inode exhaustion.
   - CRC32C Castagnoli checksum verification on every needle header.
   - Background copy-on-write compaction to reclaim tombstoned storage.

3. **High-Concurrency Async Reactor:**
   - Powered by Tokio, Hyper 1.0, and HTTP/1.1 persistent keep-alive connections.
   - Real streaming RFC 9110 Range GETs (`bytes=start-end`) and S3 Multipart Uploads (`?uploads`, `?partNumber=N`, `CompleteMultipartUpload`).
   - Single-flight read coalescing barrier protecting disk storage from thundering-herd traffic.

---

## 2. Live Measured Benchmark Results

Measured on AMD Ryzen 7 5800H (16 Cores, 32 GB RAM, Windows 11) using persistent HTTP/1.1 keep-alive connections (`python awan-storage/benchmarks/run_warp_benchmark.py`):

| Workload | `s3-server` (Rust `--release`) | File-per-Object S3 Baseline | Advantage / Speedup |
| :--- | :---: | :---: | :---: |
| **64 KiB PUT (Single Object)** | **363.2 MiB/s (5,811 ops/s)** | 60.1 MiB/s (961 ops/s) | **6.0x faster** (P50 = 10.1 ms) |
| **64 KiB GET (Single Object)** | **365.8 MiB/s (5,852 ops/s)** | 127.0 MiB/s (2,031 ops/s) | **2.9x faster** (P50 = 9.8 ms) |
| **64 KiB Range GET (bytes=0-4095)** | **24.8 MiB/s (6,359 ops/s)** | 8.6 MiB/s (2,192 ops/s) | **2.9x faster** (P50 = 8.9 ms) |
| **1 MiB PUT (Single Object)** | **773.2 MiB/s (773 ops/s)** | 841.7 MiB/s | Low latency (P50 = 37.2 ms) |
| **1 MiB GET (Single Object)** | **1,285.1 MiB/s (1,285 ops/s)** | 1,265.2 MiB/s | Wire-rate saturating NVMe read |
| **1 MiB Multipart Upload (4x256 KiB)** | **837.5 MiB/s (837 ops/s)** | 227.1 MiB/s | **3.7x faster** (P50 = 18.5 ms) |
| **1 GiB Multipart Upload (16x64 MiB)** | **595.2 MiB/s** | 52.5 MiB/s | **11.3x faster** |
| **1 GiB Parallel Range GET (16x64 MiB)**| **1,160.8 MiB/s** | 1,168.7 MiB/s | Full bandwidth streaming |
| **10 GiB Volume PUT (160x64 MiB)** | **783.9 MiB/s** | 662.3 MiB/s | Sustained disk write |
| **10 GiB Volume GET (160x64 MiB)** | **1,155.3 MiB/s** | 1,262.2 MiB/s | Sustained NVMe read |
| **Peak Server RAM (Working Set)** | **Bounded ~1,194 MB** | ~1,153 MB | Bounded slab buffer cache |

---

## 3. Quickstart

### Build from Source
```bash
cd awan-storage/server
cargo build --release
```

### Run Server
```bash
# Start listening on port 9000 with custom data directory
./target/release/s3-server --port 9000 --data-dir ./s3_data
```

### Run Tests & Verification
```bash
# Run unit tests, property tests (500 cases), and integration suite
cargo test
```

### Run Benchmark Suite
```bash
python awan-storage/benchmarks/run_warp_benchmark.py --sizes 64KiB,1MiB,1GiB,10GiB --concurrent 64 --duration 3s --output awan-storage/measurements/s3_warp_minio_vs_awan.md
```

---

## 4. Architecture Overview

- **`server/src/meta_index.rs`**: Columnar Struct-of-Arrays (SoA) table storing 64-byte packed metadata records (`key_hash`, `bucket_flags`, `vol_offset`, `size`, `etag_hi`, `etag_lo`, `created_at`, `key_slice`).
- **`server/src/volume.rs`**: 4096B sector-aligned WORM needle volume manager with direct file I/O and CRC32C payload verification.
- **`server/src/s3_api.rs`**: S3 REST API router supporting Virtual-Host and Path-Style addressing, bucket operations, object CRUD, range queries, and batch deletions.
- **`server/src/multipart.rs`**: High-concurrency S3 Multipart Upload manager tracking part staging, extent concatenation, and composite ETag generation.
- **`server/src/sigv4.rs`**: AWS Signature Version 4 HMAC-SHA256 signature calculator and presigned URL validator.
- **`server/src/direct_io.rs`**: Sector-aligned buffer allocations (`AlignedBuffer`) for kernel zero-copy bypass.
- **`server/src/compaction.rs`**: Automated volume garbage collection copying live needles into fresh volume files.
- **`server/src/mvcc.rs`**: Multi-version concurrency control providing read-isolation for in-flight requests while new versions are written.

---

## 5. Standalone Binary Distribution

Pre-compiled standalone binaries with SHA-256 integrity verification:
- `dist/s3-server-x86_64-windows.exe` (Windows x86_64 Release Binary)
- `dist/SHA256SUMS.txt` (SHA-256 Checksum Manifest)

The entire `awan-storage/` folder can be copied or shared standalone as a complete, independent Rust project.
