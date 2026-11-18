Command: `python benchmarks/run_warp_benchmark.py --sizes 64KiB,1MiB,1GiB --concurrent 64 --duration 2 --output ci_full_matrix.md` (`.github/workflows/bench-full.yml`, baseline run `44255781454` vs PR #259 run `44261880474`)

# Multipart Upload Bottleneck & Optimization (#57)

## Baseline vs PR #259 CI Full Matrix

Baseline `bench-full.yml` (run `44255781454`, `main`) ran with `--sizes 64KiB,1MiB --concurrent 64 --duration 2` (it did not measure `1GiB` because `run_warp_benchmark.py` previously coupled `1GiB` and `10GiB`; for historical `1GiB` context, issue #57's 2-CPU run `43164218840` measured `67.2 MiB/s` / `9,060 ms` P50 on `ubuntu-latest` vs MinIO `124.2 MiB/s`, and `68.0 MiB/s` / `8,796 ms` P50 on `windows-latest` vs MinIO `212.2 MiB/s`).

PR #259 (`a2b1cd3`, run `44261880474`) enabled `--sizes 64KiB,1MiB,1GiB` on both `ubuntu-latest` and `windows-latest` (4-CPU Azure runners):

### Baseline CI (`44255781454`, `main`, `--sizes 64KiB,1MiB`)

| Platform | Workload | AwanFS MiB/s | AwanFS P50 | MinIO MiB/s (P50) | RustFS MiB/s (P50) |
|---|---|---|---|---|---|
| `ubuntu-latest` | `1 MiB PUT (Single Object)` | `197.6` | `159.05 ms` | `176.5` (`183.72 ms`) | `156.3` (`189.37 ms`) |
| `ubuntu-latest` | `1 MiB Multipart Upload (4x256KiB Parts + Complete)` | `200.0` | `79.92 ms` | `N/A (S3 >=5MiB spec limit)` | `N/A (S3 >=5MiB spec limit)` |
| `ubuntu-latest` | `1 GiB Multipart Upload (16x64MiB Parts + Complete)` | not measured (`--sizes 64KiB,1MiB`) | — | — | — |
| `windows-latest` | `1 MiB PUT (Single Object)` | `432.5` | `61.26 ms` | `89.6` (`322.29 ms`) | `62.3` (`477.57 ms`) |
| `windows-latest` | `1 MiB Multipart Upload (4x256KiB Parts + Complete)` | `54.1` | `288.36 ms` | `N/A (S3 >=5MiB spec limit)` | `N/A (S3 >=5MiB spec limit)` |
| `windows-latest` | `1 GiB Multipart Upload (16x64MiB Parts + Complete)` | not measured (`--sizes 64KiB,1MiB`) | — | — | — |

*(Note: MinIO's Windows rows are not durable writes because MinIO's `fdatasync` is a no-op on Windows per `AGENTS.md`.)*

### PR #259 CI (`44261880474`, `issue/57-multipart-speed` at `a2b1cd3`, `--sizes 64KiB,1MiB,1GiB`)

| Platform | Workload | AwanFS MiB/s | AwanFS P50 | AwanFS Peak RSS | MinIO MiB/s (P50, Peak RSS) | RustFS MiB/s (P50, Peak RSS) | AwanFS vs MinIO / RustFS |
|---|---|---|---|---|---|---|---|
| `ubuntu-latest` | `1 MiB PUT (Single Object)` | `163.0` | `182.98 ms` | `259.9 MB` | `168.6` (`185.52 ms`, `518.7 MB`) | `161.4` (`199.80 ms`, `284.9 MB`) | `0.97x` MinIO / `1.01x` RustFS |
| `ubuntu-latest` | `1 MiB Multipart Upload (4x256KiB Parts + Complete)` | `191.6` | `83.34 ms` | `259.9 MB` | `N/A (S3 >=5MiB spec limit)` | `N/A (S3 >=5MiB spec limit)` | — |
| `ubuntu-latest` | `1 GiB Multipart Upload (16x64MiB Parts + Complete)` | `188.0` | `2,335.97 ms` | `270.2 MB` | `188.8` (`2,409.40 ms`, `801.3 MB`) | `179.6` (`2,515.53 ms`, `456.5 MB`) | `0.996x` MinIO throughput (`1.03x` faster P50) / `1.05x` RustFS |
| `ubuntu-latest` | `1 GiB Multipart Range GET (16x64MiB Parallel Ranges)` | `1,332.9` | `302.14 ms` | `1,181.3 MB` | `1,241.7` (`328.14 ms`, `801.3 MB`) | `1,088.7` (`431.52 ms`, `456.5 MB`) | `1.07x` MinIO / `1.22x` RustFS |
| `windows-latest` | `1 MiB PUT (Single Object)` | `234.5` | `133.79 ms` | `260.8 MB` | `77.5` (`295.26 ms`, `729.1 MB`) | `63.4` (`505.28 ms`, `149.4 MB`) | `3.03x` MinIO / `3.70x` RustFS |
| `windows-latest` | `1 MiB Multipart Upload (4x256KiB Parts + Complete)` | `41.1` | `378.42 ms` | `260.8 MB` | `N/A (S3 >=5MiB spec limit)` | `N/A (S3 >=5MiB spec limit)` | — |
| `windows-latest` | `1 GiB Multipart Upload (16x64MiB Parts + Complete)` | `223.4` | `2,129.26 ms` | `266.1 MB` | `1,052.4` (`387.74 ms`, `993.3 MB` — **non-durable**) | `173.5` (`2,895.49 ms`, `283.7 MB`) | `0.21x` MinIO (MinIO Windows writes are non-durable: `fdatasync` is a no-op) / `1.29x` RustFS |
| `windows-latest` | `1 GiB Multipart Range GET (16x64MiB Parallel Ranges)` | `2,229.1` | `196.36 ms` | `1,142.7 MB` | `2,041.6` (`212.66 ms`, `993.3 MB`) | `1,337.0` (`330.92 ms`, `283.7 MB`) | `1.09x` MinIO / `1.67x` RustFS |

## Root Cause

1. **Sync worker blocking on Tokio async threads (`server/src/ops/multipart.rs`, `server/src/multipart.rs`):**
   - Streamed `UploadPart` (`> 4 MiB`) wrote each incoming body frame synchronously via `PartWriter::write_chunk` -> `File::write_all`, `PartWriter::finish` -> `File::sync_all`, and `std::fs::rename` directly on Tokio's async worker threads, stalling the reactor and serializing disk I/O with HTTP reads under concurrent `16 x 64 MiB` part uploads. Offloaded to a pipelined `spawn_blocking` writer driven by `fsm::step_part_pipeline`.
   - Buffered `UploadPart` (`<= 4 MiB`, including `4 x 256 KiB` 1 MiB multipart) also called blocking file create/write/sync/rename inline on Tokio worker threads; offloaded to `spawn_blocking`.
2. **`sync_all` -> `sync_data` (`server/src/multipart.rs`):**
   - `PartWriter::finish` called `File::sync_all()` instead of `File::sync_data()` (`fdatasync` on Linux, matching `VolumeManager::sync_files`), adding full metadata sync latency per part.
3. **Metadata lock cloning (`server/src/multipart.rs`, `server/src/ops/multipart.rs`):**
   - `MultipartManager::get_upload` cloned the entire `MultipartUpload` and its `BTreeMap<u32, StagedPart>` under lock in both `upload_part` and `complete_multipart_upload` just to read checksum config (`upload_checksum_info` replaces those clones).
4. **Redundant `create_dir_all` syscalls (`server/src/multipart.rs`):**
   - `MultipartManager::begin_part` unconditionally called `create_dir_all` on every part instead of falling back to `create_dir_all` only on `ErrorKind::NotFound`.
5. **Benchmark coverage (`benchmarks/run_warp_benchmark.py`, `.github/workflows/bench-full.yml`):**
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

*(Note: MinIO Windows rows are not durable writes because MinIO's `fdatasync` is a no-op on Windows.)*
