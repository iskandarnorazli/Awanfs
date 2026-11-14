# AwanFS (`s3-server`)

Split from `awan-lang/awan-storage` on 2026-11-12, with its git history. Measurement files keep the commands they were produced with, which use the old `benchmarks/s3_warp/` path.

> **High-Performance, S3-Compatible Object Storage Daemon in Pure Rust**  
> Engineered for microsecond-latency compute, AI agent sandboxes, and high-throughput checkpointing.  
> Zero garbage collection pauses. Columnar Struct-of-Arrays (SoA) metadata indexing. 32 GiB sparse virtual volume pre-allocation. 4096B WORM needle storage with direct memory-mapped zero-copy slicing.

---

## Status (2026-11-13): not production-ready yet

Read this before you run it anywhere that matters. Plan of record: the Phase 1-4 milestones and [`ROADMAP_AND_DEPENDENCIES.md`](ROADMAP_AND_DEPENDENCIES.md); decisions in [`ARCHITECTURE.md`](ARCHITECTURE.md#0-decisions-2026-11-12); on-disk format in [`FORMAT.md`](FORMAT.md).

- **Access:** SigV4 with a root key from the environment, plus users with IAM-style JSON policies in a TOML file (`--config`, #53). The server listens on 127.0.0.1 by default. HTTPS with `--cert`/`--key`, reloaded when the files change (#30).
- **Durability:** writes are synced before the 200 by default (`group` durability, #29). The benchmark numbers below were taken before that change and will be re-run.
- **Volumes** of 32 GiB, rolled over when full (#37), compacted in the background when at least 20 % is dead (#14), at most 50 MB/s (`--compaction-rate`, #28).
- **Bodies:** a PUT or UploadPart above 4 MiB streams to disk, `aws-chunked` included (#36); bodies above 5 GiB get 400 `EntityTooLarge` before they are read. Multipart GETs stream part by part.
- **Observability:** request logs with request IDs; OTLP/HTTP traces with `--otlp-endpoint` (#43).
- **Versioning** per bucket (#31) and opt-in **zstd compression** per bucket in the config file (#51), both format 2 (`FORMAT.md`).
- **Conformance:** the ceph s3-tests suite passes 246 of 838 tests (94 skipped), checked nightly; the tracker is #123. The plan to pass the rest is [`CONFORMANCE_DESIGN.md`](CONFORMANCE_DESIGN.md), and [`tests/s3tests/CONFORMANCE_MAP.md`](tests/s3tests/CONFORMANCE_MAP.md) gives every test its area and owner.
- **Not yet:** replication and erasure coding across nodes, bucket policies and ACLs, object lock, server-side encryption, tagging, CORS (all in #123's area issues).

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
   - Byte-bounded LRU payload cache for objects up to 4 MiB, sized from `--memory-budget` (half of it; 1 GiB when nothing is detected).

---

## 2. Benchmarks

### Nightly ratio to MinIO (both servers sync every PUT)

GitHub-hosted ubuntu-latest, 4 CPUs, 2026-11-13: 5 runs per server in alternating order, medians of 3 s phases ([`measurements/ratio_2026-11-13.md`](measurements/ratio_2026-11-13.md), command inside).

| Workload | Clients | AwanFS MiB/s | MinIO MiB/s | AwanFS / MinIO |
|---|---|---|---|---|
| PUT 64 KiB | 16 | 156.3 | 49.7 | 3.15x |
| GET 64 KiB | 16 | 418.6 | 229.2 | 1.83x |
| Range GET 4 KiB | 16 | 33.7 | 17.9 | 1.88x |
| Mixed 64 KiB | 16 | 294.7 | 138.1 | 2.13x |
| PUT 1 MiB | 8 | 169.0 | 155.7 | 1.08x |
| GET 1 MiB | 8 | 4,086.9 | 2,402.6 | 1.70x |

Peak RSS over the six workloads: AwanFS 117 MiB, MinIO 560 MiB (both include reclaimable pages of mapped files). One shared 4-CPU VM and short phases: these are ratios on identical hardware, not absolute numbers (#58).

The nightly full matrix adds RustFS, latency percentiles and a concurrency sweep, on both Linux and Windows runners, one 2 s run per phase ([`measurements/2026-11-14-ci-full-matrix.md`](measurements/2026-11-14-ci-full-matrix.md)).

### Earlier laptop run (2026-11-11, before durable PUTs)

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
- The server listens on `127.0.0.1` only. `--listen 0.0.0.0` (or `AWANFS_LISTEN`) opens it to the network.
- HTTPS: `--cert cert.pem --key key.pem` (or `AWANFS_TLS_CERT`/`AWANFS_TLS_KEY`) serves TLS with HTTP/1.1 and HTTP/2 on the port, and no plain HTTP unless `--http-port <port>` adds a plain listener. A renewed certificate is picked up within 60 s of the files changing, or at once on SIGHUP (Unix); a broken file keeps the old certificate.
- Requests are checked with AWS Signature Version 4, in the `Authorization` header or as a presigned URL, against `AWANFS_ACCESS_KEY`/`AWANFS_SECRET_KEY`. A body whose SHA-256 differs from `x-amz-content-sha256` is refused. More users with policies: `--config` below.
- `--durability group|fast|strict` (or `AWANFS_DURABILITY`) sets when a write is acknowledged. After a power cut:
  - `group` (default): every acknowledged PUT, DELETE and multipart completion is kept. Writers wait for a shared sync of the volume and its journal; concurrent writers share one.
  - `fast`: writes are acknowledged before they reach the disk and a background thread syncs once a second, so about the last second of acknowledged writes can be lost. For caches and data you can fetch again.
  - `strict`: each write is synced on its own before the answer. Slowest; no batching.
  A killed process (no power cut) loses nothing acknowledged in any mode.
- Health probes, without authentication: `GET /_awanfs/health/live` (200 while the process serves) and `GET /_awanfs/health/ready` (200, or 503 with the reason). `_` cannot appear in a bucket name, so these paths never shadow a bucket.
- Prometheus metrics, without authentication: `GET /_awanfs/metrics` (requests and latency per S3 operation, bytes in and out, volume use, payload cache hits, the disk and copy counters from `server/src/work.rs`, and `awanfs_log_lines_dropped_total`: the per-request log on stderr is written by a background thread, so a stderr nobody reads costs log lines, counted there, and never stalls requests).
- Dashboards and alerts: `deploy/grafana/awanfs-dashboard.json` (Grafana) and `deploy/prometheus/alerts.yml` (Prometheus rules), both built on `/_awanfs/metrics`.
- Access log (off by default): `--access-log <file>` or `--access-log -` for stdout (or `AWANFS_ACCESS_LOG`) writes one line per request in the Amazon S3 server access log format: requester access key, operation, bucket, key, status, error code, bytes, time, request ID.
- Traces (off by default): `--otlp-endpoint http://collector:4318` (or `OTEL_EXPORTER_OTLP_ENDPOINT`) exports a span per request, with child spans for volume writes and syncs, over OTLP/HTTP JSON to Tempo, Jaeger or an OpenTelemetry Collector. A W3C `traceparent` header continues the caller's trace.
- Stopping: Ctrl-C or SIGTERM (Ctrl-Break, console close or system shutdown on Windows) marks the server not ready, stops accepting connections, lets requests in flight finish (up to 30 s), syncs every volume and exits 0.
- Several users with policies: `--config awanfs.toml` (or `AWANFS_CONFIG`). The file holds secrets, so on Unix the server refuses it unless only its owner can read it (`chmod 600`). Each user has an AWS-style policy (Effect Allow/Deny, Action and Resource with `*` and `?`; Condition, Principal and NotAction are refused rather than ignored). Explicit Deny wins. The file reloads within 60 s of a change, or on SIGHUP. The `AWANFS_ACCESS_KEY` pair, if set, keeps full access.
  ```toml
  [[users]]
  name = "reader"
  access_key = "AKREADER"
  secret_key = "change-me"
  policy = '''{"Statement": [{"Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket"],
                              "Resource": ["arn:aws:s3:::photos", "arn:aws:s3:::photos/*"]}]}'''
  ```
- `--allow-anonymous` (or `AWANFS_ALLOW_ANONYMOUS=1`) also accepts unsigned requests, for local tests and benchmarks. Without keys and without that flag the server refuses to start (exit code 2). It is refused together with users in `--config` (also exit code 2): unsigned requests are not checked against any policy, so anyone reaching the port would get more access than the users you restricted (#130).
- `--memory-budget 256MiB` (or `AWANFS_MEMORY_BUDGET`) caps what the server holds in memory: half bounds the payload cache, half is for in-flight request bodies. Without it the budget is a quarter of the container's `memory.max` or the machine's `MemTotal`, held between 128 MiB and 2 GiB; where neither is readable it is 2 GiB, which leaves the cache at the 1 GiB it had before. The startup line and `awanfs_memory_budget_bytes` in `GET /_awanfs/metrics` say which it was. On Linux with glibc the server also fixes the allocator's mmap threshold at 128 KiB, so freed large buffers go back to the OS instead of staying in the heap (set `MALLOC_MMAP_THRESHOLD_` to choose your own); the startup line says so.

### Docker and systemd
```bash
docker build -t awanfs .
docker run -p 9000:9000 -v awanfs-data:/data -e AWANFS_ACCESS_KEY=mykey -e AWANFS_SECRET_KEY=mysecret awanfs
```
`deploy/systemd/awanfs.service` runs the binary as a hardened service with the keys in `/etc/awanfs/awanfs.env`. Both stop with SIGTERM, which finishes requests in flight and syncs before exit.

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

---

## License

Apache License 2.0: see [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE). Security reports: [`SECURITY.md`](SECURITY.md). Contributing: [`CONTRIBUTING.md`](CONTRIBUTING.md).
