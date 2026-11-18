gh workflow run bench-full.yml --ref main

# Multipart Upload Bottleneck & Optimization (#57)

## Baseline CI (`bench-full.yml`, run `44255781454`, `main`)

Baseline `bench-full.yml` ran with `--sizes 64KiB,1MiB --concurrent 64 --duration 2` (it did not include `1GiB` because `run_warp_benchmark.py` previously coupled `1GiB` and `10GiB`):

| Platform | Workload | AwanFS MiB/s | AwanFS P50 | Notes |
|---|---|---|---|---|
| `ubuntu-latest` | `1 MiB PUT (Single Object)` | `197.6` | `160.27 ms` | Baseline `44255781454` |
| `ubuntu-latest` | `1 MiB Multipart Upload (4x256KiB Parts + Complete)` | `200.0` | `83.27 ms` | Baseline `44255781454` |
| `windows-latest` | `1 MiB PUT (Single Object)` | `432.5` | `124.22 ms` | Baseline `44255781454` |
| `windows-latest` | `1 MiB Multipart Upload (4x256KiB Parts + Complete)` | `54.1` | `288.36 ms` | Baseline `44255781454` (MinIO Windows rows are not durable writes: `fdatasync` is a no-op) |

## Root Cause

1. **Streamed `UploadPart` (`> 4 MiB`) blocked Tokio async worker threads and serialized disk I/O with HTTP reads (`server/src/ops/multipart.rs`, `server/src/multipart.rs`):**
   - Each incoming body frame was written synchronously via `PartWriter::write_chunk` -> `File::write_all`, `PartWriter::finish` -> `File::sync_all`, and `std::fs::rename` directly on Tokio's async worker threads, stalling the reactor under concurrent part uploads (`16 x 64 MiB` at `c=8`).
   - `PartWriter::finish` called `File::sync_all()` instead of `File::sync_data()` (`fdatasync` on Linux, matching `VolumeManager::sync_files`), adding full metadata sync latency per part.
2. **Buffered `UploadPart` (`<= 4 MiB`, including `4 x 256 KiB` 1 MiB multipart) also called blocking file create/write/`sync_all`/rename inline on Tokio worker threads.**
3. **Metadata map cloning & directory syscalls on every `UploadPart` / `CompleteMultipartUpload`:**
   - `MultipartManager::get_upload` cloned the entire `MultipartUpload` and its `BTreeMap<u32, StagedPart>` in both `upload_part` and `complete_multipart_upload` just to read checksum config (`upload_checksum_info` replaces those clones).
   - `MultipartManager::begin_part` unconditionally called `create_dir_all` on every part instead of falling back to `create_dir_all` only on `ErrorKind::NotFound`.
4. **Benchmark coverage (`benchmarks/run_warp_benchmark.py`, `.github/workflows/bench-full.yml`):**
   - `run_warp_benchmark.py` coupled `"1GiB"` with `"10GiB"` (`if "1GiB" in args.sizes or "10GiB" in args.sizes:`), preventing `bench-full.yml` from running the `1 GiB` multipart phase without triggering the 10 GiB volume phases. Decoupled `1GiB` and `10GiB` via exact token matching (`size_set`) and enabled `1GiB` in `.github/workflows/bench-full.yml` (`--sizes 64KiB,1MiB,1GiB`).

## Local Verification (`LAPTOP-6RBIQUM5`, Windows-11, 16 CPUs)

Command: `python -u bench_mpu.py <s3-server.exe> --repo=.` (back-to-back comparison of `server/target/release/s3-server.exe` on `main` vs `server/target-57/release/s3-server.exe` with pipelined `UploadPart` + `sync_data()`):

| Server | Round | `1GiB MPU (16x64MiB c=8)` PUT | P50 | Peak RSS | `1GiB MPU` Range GET | `1x64MiB c=1` UploadPart | `1MiB MPU (4x256KiB c=16)` |
|---|---|---|---|---|---|---|---|
| AwanFS (`main`, `target/release`) | 1 (earlier) | `851.8 MiB/s` | `565.6 ms` | `36.0 MB` | `1371.9 MiB/s` | `277.41 ms` (`230.7 MiB/s`) | `145.4 MiB/s` |
| AwanFS (`main`, `target/release`) | 2 (earlier) | `777.6 MiB/s` | `587.9 ms` | `30.9 MB` | `1418.7 MiB/s` | `254.74 ms` (`251.2 MiB/s`) | `133.6 MiB/s` |
| AwanFS (`main`, `target/release`) | back-to-back | `43.4 MiB/s` | `14522.7 ms` | `34.6 MB` | `1167.2 MiB/s` | `288.95 ms` (`221.5 MiB/s`) | `151.9 MiB/s` |
| AwanFS (`issue/57`, `target-57`) | back-to-back | `838.7 MiB/s` | `583.0 ms` | `44.2 MB` | `1189.8 MiB/s` | `255.23 ms` (`250.8 MiB/s`) | `142.6 MiB/s` |
| MinIO (`minio.exe`, Windows non-durable) | 1 (earlier) | `1174.7 MiB/s` | `371.2 ms` | `289.7 MB` | `1082.1 MiB/s` | `170.05 ms` (`376.4 MiB/s`) | N/A (`< 5 MiB` spec limit) |
| MinIO (`minio.exe`, Windows non-durable) | 2 (earlier) | `1239.7 MiB/s` | `382.6 ms` | `292.1 MB` | `1122.6 MiB/s` | — | N/A |

*(Note: MinIO Windows rows are not durable writes because MinIO's `fdatasync` is a no-op on Windows. Full `ubuntu-latest` and `windows-latest` CI ratios are produced on the PR's `Nightly full benchmark matrix` run.)*
