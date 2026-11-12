# AwanFS (`s3-server`)

Split from `awan-lang/awan-storage` on 2026-11-12, with its git history. Measurement files keep the commands they were produced with, which use the old `benchmarks/s3_warp/` path.

> **High-Performance, S3-Compatible Object Storage Daemon in Pure Rust**  
> Engineered for microsecond-latency compute, AI agent sandboxes, and high-throughput checkpointing.  
> Zero garbage collection pauses. Columnar Struct-of-Arrays (SoA) metadata indexing. 32 GiB sparse virtual volume pre-allocation. 4096B WORM needle storage with direct memory-mapped zero-copy slicing.

---

## Status (2026-11-12): not production-ready yet

Read this before you run it anywhere that matters. Plan of record: the Phase 1-4 milestones and [`ROADMAP_AND_DEPENDENCIES.md`](ROADMAP_AND_DEPENDENCIES.md); decisions in [`ARCHITECTURE.md`](ARCHITECTURE.md#0-decisions-2026-11-12).

- **One access key, no TLS.** Requests are checked with SigV4 against one key pair and the server listens on 127.0.0.1 by default (#35). There are no per-user policies (#53) and no TLS (#30): put a TLS proxy in front before opening it to a network.
- **Writes are synced before the 200 by default** (`group` durability, #29). The measurements below were taken before that change, when a PUT was acknowledged without a sync; they will be re-run.
- **Volumes of 32 GiB, rolled over when full** (#37). `S3ServiceState::compact_oldest_volume` moves the live objects of the oldest volume into the active one and deletes it, safely while requests run (#14). Nothing calls it on a schedule yet, so space from overwrites and deletes is only reclaimed when it is called.
- A PUT body above 4 MiB with a `Content-Length` is written to disk as it arrives, in 1 MiB pieces, and so is an UploadPart above 4 MiB, into its staged part file. Smaller bodies and `aws-chunked` bodies are still held in memory before they are written. Declared bodies above the S3 limit of 5 GiB get 400 `EntityTooLarge` before they are read; XML bodies are capped at 16 MiB. Multipart GETs stream part by part (#36).

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
   - A compactor exists (`compaction.rs`) but is not wired into the server and is not yet safe to run (#14).

4. **Staged Parallel Part Files for Multipart Uploads:**
   - Multipart parts stream in parallel to independent file handles under `.staging/<upload-id>/part_<num>.dat`.
   - Completely eliminates Windows kernel `FO_SYNCHRONOUS_IO` lock contention on shared volume handles during high-concurrency uploads.
   - Parts stay in their staged files; `CompleteMultipartUpload` writes the part list to the volume as a manifest needle, so recovery rebuilds the object. `AbortMultipartUpload` removes the staged files.

5. **Streaming Hardware-Accelerated MD5 & CRC32C Dual Checksum Pipeline:**
   - `StreamingDigest` computes MD5 and CRC32C in one pass. The PUT path collects the whole body first, so the digest is not yet computed while the body streams in (#36).
   - Automatic CPU runtime detection (SSE4.2, AVX2, ARMv8 CRC) with a 256-entry software Castagnoli lookup table fallback, ensuring 100% bit-identical digests on hypervisors or non-AVX machines.
   - Full `Content-MD5` header verification and composite multipart ETag calculation (`"<md5>-<parts>"`).

6. **Zero-Copy Direct Extent Slicing for Range GETs:**
   - Memory-mapped volume extents and staged multipart parts are cached and sliced directly using `Bytes::from_owner(MmapOwner(mmap)).slice(...)`.
   - Bypasses intermediate heap allocations and kernel read syscalls during RFC 9110 Byte-Range GETs (`206 Partial Content`).

7. **High-Concurrency Async Reactor:**
   - Powered by Tokio, Hyper 1.0, and HTTP/1.1 persistent keep-alive connections.
   - Single-flight read coalescing barrier protecting disk storage from thundering-herd traffic.
   - Byte-bounded LRU payload cache (1 GiB default) for objects up to 4 MiB.

---

## 2. Live Measured 3-Way Benchmark Results (Awan vs MinIO vs RustFS)

> **Provisional.** These are real measurements from one laptop run, but they do not yet support a general "faster than MinIO" claim: PUT rows compare an unsynced AwanFS write with MinIO's synced one, the data set fits in memory, each phase ran 2 s once, and MinIO and RustFS ran in single-drive default setups. #58 tracks the benchmark that would support or refute the claim. The nightly CI runs (`.github/workflows/bench-full.yml`, `bench-ratio.yml`) track regressions.

Measured live on physical AMD64 NVMe storage (16 logical cores, 32 GB RAM, Windows 11) using persistent HTTP/1.1 keep-alive connections across 64 concurrent client workers (`python benchmarks/run_warp_benchmark.py`):

| Workload | `s3-server` (`--release`) | MinIO Reference (`RELEASE.2025-09-07`) | RustFS Reference (`v1.0.5`) | Speedup vs MinIO (provisional) |
| :--- | :---: | :---: | :---: | :---: |
| **64 KiB PUT (Single Object)** | **349.4 MiB/s (5,590 ops/s)** | 8.5 MiB/s (136 ops/s) | 9.4 MiB/s (150 ops/s) | **41.1x faster** (P50 = 10.1 ms) (not durable, see note) |
| **64 KiB GET (Single Object)** | **339.1 MiB/s (5,425 ops/s)** | 225.6 MiB/s (3,610 ops/s) | 256.4 MiB/s (4,102 ops/s) | **1.50x faster** (P50 = 10.6 ms) |
| **64 KiB Mixed (80% GET / 20% PUT)** | **310.1 MiB/s (4,962 ops/s)** | 37.9 MiB/s (606 ops/s) | 40.0 MiB/s (641 ops/s) | **8.18x faster** (P99 = 41.1 ms) (not durable, see note) |
| **64 KiB Range GET (bytes=0-4095)** | **23.1 MiB/s (5,907 ops/s)** | 14.1 MiB/s (3,612 ops/s) | 17.1 MiB/s (4,377 ops/s) | **1.64x faster** (P50 = 9.1 ms) |
| **1 MiB PUT (Single Object)** | **737.3 MiB/s (737 ops/s)** | 126.3 MiB/s (126 ops/s) | 65.0 MiB/s (65 ops/s) | **5.84x faster** (P50 = 40.6 ms) (not durable, see note) |
| **1 MiB GET (Single Object)** | **2,165.6 MiB/s** | 1,307.7 MiB/s | 773.7 MiB/s | **1.65x faster** (P50 = 14.1 ms) |
| **1 MiB Mixed (80% GET / 20% PUT)** | **1,734.5 MiB/s** | 481.0 MiB/s | 305.4 MiB/s | **3.61x faster** (P50 = 17.3 ms) (not durable, see note) |
| **1 GiB Multipart Upload (16 x 64 MiB parts)** | 628.2 MiB/s | **848.1 MiB/s** | 453.9 MiB/s | **0.74x (slower)** (P50 = 841.9 ms) |
| **1 GiB Multipart Range GET (16 parallel ranges)** | 1,119.3 MiB/s | **1,252.3 MiB/s** | 879.1 MiB/s | **0.89x (slower)** (P50 = 401.1 ms) |
| **10 GiB Volume PUT (160 x 64 MiB parts)** | 140.3 MiB/s | 154.9 MiB/s | **253.2 MiB/s** | **0.91x (slower)** (P50 = 1,875.9 ms) |
| **10 GiB Volume Parallel GET** | **1,412.7 MiB/s** | 1,149.2 MiB/s | 834.8 MiB/s | **1.23x faster** (P50 = 354.7 ms) |
| **Concurrency Scaling (C = 128)** | **324.1 MiB/s** (P99 = 60.0 ms) | 37.8 MiB/s (P99 = 1,239.4 ms) | 39.4 MiB/s (P99 = 959.0 ms) | **8.57x faster (20x lower P99)** (not durable, see note) |
| **Server Peak RAM (Working Set)** | **Bounded ~245 MB (64K-1M)** | ~936 MB (Go GC Heap) | ~172 MB (Rust jemalloc) | **3.8x lower RAM vs MinIO** |

The table lists every workload in the report, including the three where `s3-server` is slower.  **Durability:** when this report was made, `s3-server` acknowledged a PUT before it was flushed to disk, while MinIO and RustFS fsync each PUT; `group` durability (#29) is now the default. Rows marked "not durable" compare a non-durable write with durable ones, so their speedups are not like for like. Each workload ran once for 2 seconds on one Windows laptop; the full report with its command is [`measurements/s3_warp_minio_vs_awan.md`](measurements/s3_warp_minio_vs_awan.md).

---

## 3. Quickstart

### Build from Source
```bash
cd server
cargo build --release
```

### Run Server
```bash
# Port 9000, data in ./s3_data, requests must carry a SigV4 signature made with these keys
AWANFS_ACCESS_KEY=mykey AWANFS_SECRET_KEY=mysecret \
  ./target/release/s3-server --port 9000 --data-dir ./s3_data
```
- The server listens on `127.0.0.1` only. `--listen 0.0.0.0` (or `AWANFS_LISTEN`) opens it to the network; there is no TLS yet (#30), so put it behind a TLS proxy first.
- Requests are checked with AWS Signature Version 4, in the `Authorization` header or as a presigned URL, against `AWANFS_ACCESS_KEY`/`AWANFS_SECRET_KEY`. A body whose SHA-256 differs from `x-amz-content-sha256` is refused. One key pair for now; IAM-style users and policies are #53.
- `--durability group|fast|strict` (or `AWANFS_DURABILITY`) sets when a write is acknowledged. After a power cut:
  - `group` (default): every acknowledged PUT, DELETE and multipart completion is kept. Writers wait for a shared sync of the volume and its journal; concurrent writers share one.
  - `fast`: writes are acknowledged before they reach the disk and a background thread syncs once a second, so about the last second of acknowledged writes can be lost. For caches and data you can fetch again.
  - `strict`: each write is synced on its own before the answer. Slowest; no batching.
  A killed process (no power cut) loses nothing acknowledged in any mode.
- `--allow-anonymous` (or `AWANFS_ALLOW_ANONYMOUS=1`) also accepts unsigned requests, for local tests and benchmarks. Without keys and without that flag the server refuses to start (exit code 2).

### Run the Test Suite
```bash
cargo test --locked --manifest-path server/Cargo.toml
```
CI runs it on ubuntu-latest and windows-latest. `work_count_test.rs` pins the disk writes, syncs, reads and copies of each request type; the nightly MinIO ratio check is `benchmarks/ratio_check.py`.

### Benchmark on Your Machine
```bash
python benchmarks/bench.py                 # AwanFS vs MinIO, report in measurements/
python benchmarks/bench.py --awanfs-only   # no MinIO needed
```
See [`benchmarks/README.md`](benchmarks/README.md) for what it measures and how to read it.

### Run Live Head-to-Head Benchmark Suite
```bash
python benchmarks/run_warp_benchmark.py --sizes 64KiB,1MiB,1GiB,10GiB --concurrent 64 --duration 2 --output measurements/s3_warp_minio_vs_awan.md
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

The repository can be copied or shared standalone as a complete, independent Rust project.
