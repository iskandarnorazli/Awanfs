# AwanFS (`s3-server`)

Split from `awan-lang/awan-storage` on 2026-11-12, with its git history. Measurement files keep the commands they were produced with, which use the old `benchmarks/s3_warp/` path.

> **High-Performance, S3-Compatible Object Storage Daemon in Pure Rust**  
> Engineered for microsecond-latency compute, AI agent sandboxes, and high-throughput checkpointing.  
> Zero garbage collection pauses. Columnar Struct-of-Arrays (SoA) metadata indexing. Append-only volumes of up to 32 GiB. 4096B WORM needle storage with direct memory-mapped zero-copy slicing.

---

## Status (2026-11-13): not production-ready yet

Read this before you run it anywhere that matters. Plan of record: the Phase 1-4 milestones and [`ROADMAP_AND_DEPENDENCIES.md`](ROADMAP_AND_DEPENDENCIES.md); decisions in [`ARCHITECTURE.md`](ARCHITECTURE.md#0-decisions-2026-11-12); on-disk format in [`FORMAT.md`](FORMAT.md).

- **Access:** SigV4 with a root key from the environment, plus users with IAM-style JSON policies in a TOML file (`--config`, #53). The server listens on 127.0.0.1 by default. HTTPS with `--cert`/`--key`, reloaded when the files change (#30).
- **Durability:** writes are synced before the 200 by default (`group` durability, #29). The 16-core laptop benchmarks below were re-run with `group`, `strict`, and `fast` durability ([`measurements/2026-11-18-market-claims.md`](measurements/2026-11-18-market-claims.md)).
- **Volumes** of 32 GiB, rolled over when full (#37), compacted in the background when at least 20 % is dead (#14), at most 50 MB/s (`--compaction-rate`, #28).
- **Bodies:** a PUT or UploadPart above 4 MiB streams to disk, `aws-chunked` included (#36); bodies above 5 GiB get 400 `EntityTooLarge` before they are read. Multipart GETs stream part by part.
- **Observability:** request logs with request IDs; OTLP/HTTP traces with `--otlp-endpoint` (#43).
- **Versioning** per bucket (#31) and opt-in **zstd compression** per bucket in the config file (#51), both format 2 (`FORMAT.md`).
- **Conformance:** the ceph s3-tests suite passes 335 of 886 tests (94 skipped; main at 38660ee, 2026-11-19), checked nightly and on every pull request that changes server code; the tracker is #123. The plan to pass the rest is [`CONFORMANCE_DESIGN.md`](CONFORMANCE_DESIGN.md), and [`tests/s3tests/CONFORMANCE_MAP.md`](tests/s3tests/CONFORMANCE_MAP.md) gives every test its area and owner.
- **Not yet:** replication and erasure coding across nodes, bucket policies and ACLs, object lock, server-side encryption, tagging, CORS (all in #123's area issues).

## 1. Executive Summary

`s3-server` is a standalone, drop-in AWS S3-compatible object storage server implemented 100% in pure Rust. It combines:

1. **64-Byte Columnar Struct-of-Arrays (`ObjectMeta`) Metadata Index:**
   - 1,000,000 objects indexed in ~40 MB of RAM.
   - $O(1)$ hash lookup via `xxh3` hash slots and arena-backed string deduplication.
   - BTree-backed lexicographical prefix and range scans (`ListObjectsV2`).

2. **Append-only volumes that grow as they are written:**
   - A volume file holds up to 32 GiB and grows with each write; a new one is started when it is full. The same layout on every platform (decision 52).
   - Reads go through a memory mapping of the volume, redone when a read reaches past it. The first volume used to be a 32 GiB sparse file mapped once; flushing a sparse file cost 7.75 ms per commit on Windows, so it was dropped. Data directories that already have a sparse volume keep it.

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

### 16-Core Laptop Benchmark (2026-11-18, durable writes & 3-run spread)

Measured live on `LAPTOP-6RBIQUM5` (AMD64, 16 logical cores, 31.4 GB RAM, Windows 11; server pinned to cores `0..3` / `0x0F`, client pinned to cores `4..7` / `0xF0`), 3 alternating rounds of 30.0s per phase, reporting `median (min..max)` ([`measurements/2026-11-18-market-claims.md`](measurements/2026-11-18-market-claims.md), GitHub Actions run `44312933524`).

- **Durability & competitor setups:** AwanFS ran in `--durability=group` (default, shared group commit sync), `--durability=strict` (per-write sync), and `--durability=fast` (NO-SYNC, ~1s background flush). MinIO (`RELEASE.2025-09-07T16-13-09Z`) ran as a 4-directory erasure set (`minio server dir1..dir4`); on Windows its `fdatasync` is a no-op for objects >4 KiB, so its PUT and mixed rows there are not durable writes and are not like for like. RustFS (`1.0.5`) ran in its single-drive release default (sync not independently probed).
- **MinIO `warp` client:** measured on MinIO and RustFS (`warp.exe`); `not measured` on AwanFS in the 2026-11-18 run (`Error preparing server: The requested sub-resource is not implemented.`, pending an 8+ core self-hosted runner rerun after #260).
- **Cold GET working set (warm page cache):** the 2026-11-18 Cold GET rows ran with `--cold-set-mib 32` (`32 MiB` dataset) and `--memory-budget 16MiB` (6 MiB AwanFS payload cache) on `31.4 GB` host RAM, so they measure AwanFS payload-cache misses from **warm page cache** rather than a dataset exceeding host RAM (`>31.4 GB` cold disk reads: `not measured`).

| Workload (http client) | AwanFS `group` (default, durable) | AwanFS `strict` (per-write sync) | AwanFS `fast` (NO-SYNC) | MinIO (4-dir erasure) | RustFS (`1.0.5`) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **64 KiB PUT (c=64)** | 203.8 (156.9..205.2) MiB/s<br>3260.4 (2511.1..3282.5) ops/s | 102.7 (102.1..105.9) MiB/s<br>1643.7 (1633.7..1693.8) ops/s | 242.7 (148.7..248.4) MiB/s<br>3883.6 (2379.6..3974.4) ops/s | 2.7 (2.6..2.8) MiB/s<br>43.6 (41.1..44.7) ops/s<br>(`warp`: 10.3 (9.6..10.3) MiB/s) | 8.3 (8.3..8.4) MiB/s<br>132.6 (132.5..135.2) ops/s<br>(`warp`: 17.3 (17.0..18.1) MiB/s) |
| **64 KiB GET (default cache)** | 305.8 (305.3..307.5) MiB/s<br>4892.1 (4885.6..4919.5) ops/s | 294.2 (283.5..299.7) MiB/s<br>4707.1 (4535.7..4794.6) ops/s | 300.2 (299.9..302.0) MiB/s<br>4803.3 (4797.7..4831.8) ops/s | 92.3 (77.7..93.4) MiB/s<br>1476.9 (1243.1..1494.3) ops/s<br>(`warp`: 88.7 (84.1..90.1) MiB/s) | 265.8 (261.5..271.2) MiB/s<br>4252.4 (4183.4..4339.7) ops/s<br>(`warp`: 545.6 (526.3..547.2) MiB/s) |
| **64 KiB Cold GET (`16MiB` budget, >6 MiB cache, `32 MiB` dataset, warm page cache)** | 294.9 (281.9..298.9) MiB/s<br>4718.6 (4510.6..4782.1) ops/s | 285.9 (285.4..295.2) MiB/s<br>4573.9 (4566.2..4724.0) ops/s | 293.3 (292.5..299.8) MiB/s<br>4692.2 (4680.3..4796.5) ops/s | 86.7 (85.2..91.3) MiB/s<br>1387.3 (1363.1..1460.5) ops/s | 262.1 (252.3..266.8) MiB/s<br>4194.2 (4037.1..4269.0) ops/s |
| **64 KiB Mixed 80/20** | 269.2 (265.4..270.7) MiB/s<br>4306.5 (4247.1..4331.6) ops/s | 263.3 (252.8..265.5) MiB/s<br>4212.4 (4045.4..4248.0) ops/s | 288.9 (286.1..294.8) MiB/s<br>4622.3 (4578.0..4717.6) ops/s | 12.2 (10.5..12.5) MiB/s<br>195.9 (168.4..200.2) ops/s<br>(`warp`: 34.0 (31.6..34.5) MiB/s) | 36.8 (36.4..38.2) MiB/s<br>588.1 (581.9..611.3) ops/s<br>(`warp`: 64.1 (62.7..65.0) MiB/s) |
| **64 KiB Range GET (4 KiB)** | 19.9 (19.8..20.0) MiB/s<br>5104.4 (5066.3..5130.1) ops/s | 19.4 (18.5..19.4) MiB/s<br>4962.6 (4748.1..4972.7) ops/s | 19.7 (19.5..19.9) MiB/s<br>5037.6 (4999.0..5082.5) ops/s | 6.1 (5.8..6.2) MiB/s<br>1559.2 (1488.1..1575.6) ops/s | 16.8 (16.7..17.1) MiB/s<br>4305.7 (4268.4..4369.0) ops/s |
| **1 MiB PUT** | 200.1 (170.4..223.7) MiB/s | 254.6 (84.5..274.3) MiB/s | 201.0 (192.6..208.7) MiB/s | 30.0 (26.9..30.2) MiB/s | 67.5 (65.2..67.6) MiB/s |
| **1 MiB GET (default cache)** | 1664.0 (1605.4..1805.7) MiB/s | 1718.3 (1647.9..1755.2) MiB/s | 1706.6 (1644.6..1766.7) MiB/s | 861.0 (775.3..864.8) MiB/s | 735.4 (689.3..738.5) MiB/s |
| **1 MiB Cold GET (`16MiB` budget, >6 MiB cache, `32 MiB` dataset, warm page cache)** | 1404.6 (1370.9..1448.4) MiB/s | 1473.9 (1429.8..1486.3) MiB/s | 1473.2 (1434.7..1477.7) MiB/s | 815.7 (813.8..824.7) MiB/s | 731.8 (701.0..737.9) MiB/s |
| **LIST 1,000 keys (`ListObjectsV2`)** | 141.5 (134.4..143.4) MiB/s<br>633.4 (601.5..642.0) ops/s | 135.8 (130.7..139.7) MiB/s<br>607.7 (585.0..625.3) ops/s | 138.6 (137.8..141.1) MiB/s<br>620.4 (616.7..631.7) ops/s | 0.5 (0.4..0.5) MiB/s<br>2.2 (2.1..2.3) ops/s<br>(`warp`: 34.5 (33.9..34.5) ops/s) | 1.4 (1.4..1.4) MiB/s<br>7.2 (7.1..7.3) ops/s<br>(`warp`: 92.7 (90.4..92.8) ops/s) |
| **HEAD 64 KiB object** | 5321.2 (5052.1..5323.9) ops/s | 5244.9 (5185.6..5256.7) ops/s | 5292.2 (5264.6..5334.3) ops/s | 1397.9 (1329.9..1426.7) ops/s | 4843.8 (4767.8..4862.0) ops/s |
| **DELETE 64 KiB object** | 4150.7 (4019.8..4362.8) ops/s | 1646.1 (1587.7..1695.5) ops/s | 4239.0 (4080.3..4245.4) ops/s | 156.1 (154.5..163.1) ops/s | 148.7 (148.3..151.3) ops/s |
| **Peak RSS (64K PUT / Cold 64K / after DELETE)** | 110.5 (109.7..111.1) MB / 60.1 (59.7..60.2) MB / 397.5 (390.7..397.7) MB | 106.0 (104.2..107.0) MB / 59.8 (59.7..60.4) MB / 317.3 (316.3..318.0) MB | 95.0 (94.9..95.2) MB / 60.1 (59.9..60.2) MB / 403.2 (402.2..406.9) MB | 609.1 (589.4..616.3) MB / 624.8 (608.7..637.0) MB / 1097.9 (1071.6..1178.0) MB | 109.6 (108.6..109.7) MB / 152.8 (152.7..153.1) MB / 167.0 (166.0..167.2) MB |

Not measured in [`measurements/2026-11-18-market-claims.md`](measurements/2026-11-18-market-claims.md): `warp` against AwanFS (`not measured`, pending an 8+ core self-hosted runner rerun after #260), Cold GET dataset exceeding host RAM (`>31.4 GB`: `not measured`; the `32 MiB` Cold GET rows above are warm page cache), 1 MiB Mixed (`not measured`), 1 GiB / 10 GiB multipart (`not measured`), `C=128` concurrency sweep (`not measured`), `--profile community` 800 KiB compressible documents (`not measured`), and Garage / SeaweedFS / HS5 (`not measured`). See the full report (including P50/P99 latency spreads, disk used, server CPU time, and `/_awanfs/metrics` cold-read cache hit/miss verification) in [`measurements/2026-11-18-market-claims.md`](measurements/2026-11-18-market-claims.md).

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
- Access log (off by default): `--access-log <file>` or `--access-log -` for stdout (or `AWANFS_ACCESS_LOG`) writes one line per request in the Amazon S3 server access log format: requester access key, operation, bucket, key, status, error code, bytes, time, request ID. It is written by a background thread through a bounded queue, so a slow or full log target never stalls requests: lines that do not fit are dropped and counted in `awanfs_log_lines_dropped_total`. It is a log, not an audit record.
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
- `--body-idle-timeout 20s` sets how long a request may go without sending the next body frame before the server closes it with `408 RequestTimeout` (default `20s`; accepts `s` or `ms`). Request headers have a 20 s idle timeout.
- `--memory-budget 256MiB` (or `AWANFS_MEMORY_BUDGET`) caps what the server holds in memory: half bounds the payload cache, half is for in-flight request bodies. A request whose body is read into memory (a PUT or UploadPart that is not streamed, or an XML body) is admitted only when its bytes fit in that half; otherwise it waits up to 100 ms and is then refused with 503 `SlowDown`. `awanfs_memory_bodies_in_use_bytes` and `awanfs_memory_bodies_rejected_total` show it. Without it the budget is a quarter of the container's `memory.max` or the machine's `MemTotal`, held between 128 MiB and 2 GiB; where neither is readable it is 2 GiB, which leaves the cache at the 1 GiB it had before. The startup line and `awanfs_memory_budget_bytes` in `GET /_awanfs/metrics` say which it was. On Linux with glibc the server also fixes the allocator's mmap threshold at 128 KiB, so freed large buffers go back to the OS instead of staying in the heap (set `MALLOC_MMAP_THRESHOLD_` to choose your own); the startup line says so.

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
- **`server/src/volume.rs`**: 4096B sector-aligned WORM needle volume manager (growing volumes; `open_sparse` for volumes made sparse before decision 52), crash recovery scanner, and needle verification.
- **`server/src/direct_io.rs`**: Sector-aligned buffer allocations (`AlignedBuffer`), Windows `FSCTL_SET_SPARSE` direct controls, and lock-free positional I/O (`seek_read` / `seek_write`).
- **`server/src/multipart.rs`**: High-concurrency S3 Multipart Upload manager tracking parallel staged part files (`.staging/<upload-id>/part_<num>.dat`), memory-mapped extent caches, and zero-copy part concatenation.
- **`server/src/digest.rs`**: Real-time dual-hasher pipeline concurrently calculating MD5 and Castagnoli CRC32C over streaming TCP frames with AVX2/SSE4.2 detection and software table fallback.
- **`server/src/s3_api.rs`**: S3 REST API router supporting Virtual-Host and Path-Style routing, bucket operations, object CRUD (PUT, GET, HEAD, DELETE, batch POST delete), RFC 9110 Range GETs, and commit-time key-locked conditional writes and deletes (`If-Match`, `If-None-Match`, last-modified, size).
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
