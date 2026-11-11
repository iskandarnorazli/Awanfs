# S3 Storage Server (`s3-server`)

> **High-Performance, S3-Compatible Object Storage Daemon in Pure Rust**  
> Engineered for microsecond-latency compute, AI agent sandboxes, and high-throughput checkpointing.  
> Zero garbage collection pauses. Columnar Struct-of-Arrays (SoA) metadata indexing. 32 GiB sparse virtual volume pre-allocation. 4096B WORM needle storage with direct memory-mapped zero-copy slicing.

---

## 1. Executive Summary

`s3-server` is a standalone, drop-in AWS S3-compatible object storage server implemented 100% in pure Rust. It combines:

1. **64-Byte Columnar Struct-of-Arrays (`ObjectMeta`) Metadata Index:**
   - 1,000,000 objects indexed in ~40 MB of RAM.
   - $O(1)$ hash lookup via `xxh3` hash slots and arena-backed string deduplication.
   - BTree-backed lexicographical prefix and range scans (`ListObjectsV2`).

2. **32 GiB Sparse Virtual Volume Pre-Allocation (`FSCTL_SET_SPARSE`):**
   - Volume files are initialized as 32 GiB sparse files (`FSCTL_SET_SPARSE` on Windows NTFS, native sparse holes on Unix) without allocating physical disk clusters upfront.
   - Mapped once into a 32 GiB virtual address space at server startup via `memmap2::Mmap`, completely eliminating runtime `remap()` latency stalls and lock contention.

3. **4096-Byte Sector-Aligned WORM Needle Volume Storage:**
   - Sequential append-only write path eliminating filesystem inode exhaustion.
   - Castagnoli CRC32C checksum verification on every needle header for silent bitrot scrubbing.
   - Background copy-on-write compaction to reclaim tombstoned storage.

4. **Staged Parallel Part Files for Multipart Uploads:**
   - Multipart parts stream in parallel to independent file handles under `.staging/<upload-id>/part_<num>.dat`.
   - Completely eliminates Windows kernel `FO_SYNCHRONOUS_IO` lock contention on shared volume handles during high-concurrency uploads.
   - Atomic volume append upon `CompleteMultipartUpload` and recursive cleanup on `AbortMultipartUpload`.

5. **Streaming Hardware-Accelerated MD5 & CRC32C Dual Checksum Pipeline:**
   - `StreamingDigest` dual-hasher pipeline processes incoming TCP frames in real time as the HTTP body streams.
   - Automatic CPU runtime detection (SSE4.2, AVX2, ARMv8 CRC) with a 256-entry software Castagnoli lookup table fallback, ensuring 100% bit-identical digests on hypervisors or non-AVX machines.
   - Full `Content-MD5` header verification and composite multipart ETag calculation (`"<md5>-<parts>"`).

6. **Zero-Copy Direct Extent Slicing for Range GETs:**
   - Memory-mapped volume extents and staged multipart parts are cached and sliced directly using `Bytes::from_owner(MmapOwner(mmap)).slice(...)`.
   - Bypasses intermediate heap allocations and kernel read syscalls during RFC 9110 Byte-Range GETs (`206 Partial Content`).

7. **High-Concurrency Async Reactor:**
   - Powered by Tokio, Hyper 1.0, and HTTP/1.1 persistent keep-alive connections.
   - Single-flight read coalescing barrier protecting disk storage from thundering-herd traffic.
   - Auto-scaling bounded LRU slab cache (16 MiB) ensuring constant bounded memory usage.

---

## 2. Live Measured 3-Way Benchmark Results (Awan vs MinIO vs RustFS)

Measured live on physical AMD64 NVMe storage (16 logical cores, 32 GB RAM, Windows 11) using persistent HTTP/1.1 keep-alive connections across 64 concurrent client workers (`python benchmarks/s3_warp/run_warp_benchmark.py`):

| Workload | `s3-server` (`--release`) | MinIO Reference (`RELEASE.2025-09-07`) | RustFS Reference (`v1.0.5`) | Speedup vs MinIO |
| :--- | :---: | :---: | :---: | :---: |
| **64 KiB PUT (Single Object)** | **349.4 MiB/s (5,590 ops/s)** | 8.5 MiB/s (136 ops/s) | 9.4 MiB/s (150 ops/s) | **41.1x faster** (P50 = 10.1 ms) |
| **64 KiB GET (Single Object)** | **339.1 MiB/s (5,425 ops/s)** | 225.6 MiB/s (3,610 ops/s) | 256.4 MiB/s (4,102 ops/s) | **1.50x faster** (P50 = 10.6 ms) |
| **64 KiB Mixed (80% GET / 20% PUT)** | **310.1 MiB/s (4,962 ops/s)** | 37.9 MiB/s (606 ops/s) | 40.0 MiB/s (641 ops/s) | **8.18x faster** (P99 = 41.1 ms) |
| **64 KiB Range GET (bytes=0-4095)** | **23.1 MiB/s (5,907 ops/s)** | 14.1 MiB/s (3,612 ops/s) | 17.1 MiB/s (4,377 ops/s) | **1.64x faster** (P50 = 9.1 ms) |
| **1 MiB PUT (Single Object)** | **737.3 MiB/s (737 ops/s)** | 126.3 MiB/s (126 ops/s) | 65.0 MiB/s (65 ops/s) | **5.84x faster** (P50 = 40.6 ms) |
| **1 MiB GET (Single Object)** | **2,165.6 MiB/s** | 1,307.7 MiB/s | 773.7 MiB/s | **1.65x faster** (P50 = 14.1 ms) |
| **1 MiB Mixed (80% GET / 20% PUT)** | **1,734.5 MiB/s** | 481.0 MiB/s | 305.4 MiB/s | **3.61x faster** (P50 = 17.3 ms) |
| **10 GiB Volume Parallel GET** | **1,412.7 MiB/s** | 1,149.2 MiB/s | 834.8 MiB/s | **1.23x faster** (P50 = 354.7 ms) |
| **Concurrency Scaling (C = 128)** | **324.1 MiB/s** (P99 = 60.0 ms) | 37.8 MiB/s (P99 = 1,239.4 ms) | 39.4 MiB/s (P99 = 959.0 ms) | **8.57x faster (20x lower P99)** |
| **Server Peak RAM (Working Set)** | **Bounded ~245 MB (64K-1M)** | ~936 MB (Go GC Heap) | ~172 MB (Rust jemalloc) | **3.8x lower RAM vs MinIO** |

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

### Run Pure Rust Test Suite (58 Tests)
```bash
cd awan-storage/server
cargo test
```

### Run Live Head-to-Head Benchmark Suite
```bash
python benchmarks/s3_warp/run_warp_benchmark.py --sizes 64KiB,1MiB,1GiB,10GiB --concurrent 64 --duration 2 --output docs/measurements/s3_warp_minio_vs_awan.md
```

---

## 4. Architecture Overview & Module Index

- **`server/src/meta_index.rs`**: Columnar Struct-of-Arrays (SoA) table storing 64-byte packed metadata records (`key_hash`, `bucket_flags`, `vol_offset`, `size`, `etag_hi`, `etag_lo`, `created_at`, `key_slice`) with open-addressing lookup and contiguous key arena.
- **`server/src/volume.rs`**: 4096B sector-aligned WORM needle volume manager with 32 GiB sparse virtual pre-allocation (`open_sparse`), crash recovery scanner, and needle verification.
- **`server/src/direct_io.rs`**: Sector-aligned buffer allocations (`AlignedBuffer`), Windows `FSCTL_SET_SPARSE` direct controls, and lock-free positional I/O (`seek_read` / `seek_write`).
- **`server/src/multipart.rs`**: High-concurrency S3 Multipart Upload manager tracking parallel staged part files (`.staging/<upload-id>/part_<num>.dat`), memory-mapped extent caches, and zero-copy part concatenation.
- **`server/src/digest.rs`**: Real-time dual-hasher pipeline concurrently calculating MD5 and Castagnoli CRC32C over streaming TCP frames with AVX2/SSE4.2 detection and software table fallback.
- **`server/src/s3_api.rs`**: S3 REST API router supporting Virtual-Host and Path-Style routing, bucket operations, object CRUD (PUT, GET, HEAD, DELETE, batch POST delete), RFC 9110 Range GETs, and atomic conditional mutexes (`If-None-Match: *`).
- **`server/src/sigv4.rs`**: AWS Signature Version 4 HMAC-SHA256 signature calculator, chunked streaming decoder, and presigned URL validator.
- **`server/src/compaction.rs`**: Automated volume garbage collection copying live needles into fresh volume files to reclaim tombstoned storage.
- **`server/src/mvcc.rs`**: Multi-version concurrency control providing read-isolation for in-flight requests while new versions are written.

---

## 5. Standalone Binary Distribution

Pre-compiled standalone binaries with SHA-256 integrity verification:
- `dist/s3-server-x86_64-windows.exe` (Windows x86_64 Release Binary)
- `dist/SHA256SUMS.txt` (SHA-256 Checksum Manifest)

The entire `awan-storage/` folder can be copied or shared standalone as a complete, independent Rust project.
