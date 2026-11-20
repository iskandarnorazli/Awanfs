Command: `python benchmarks/run_warp_benchmark.py --sizes 64KiB,1MiB,1GiB --concurrent 64 --duration 2 --output ci_full_matrix.md` (`.github/workflows/bench-full.yml`, `main` + 1GiB baseline run `44462407485` vs PR #259 runs `44261880474` and `44283095192`)

# Multipart Upload Bottleneck & Optimization (#57)

## Baseline (`main` + 1GiB Bench Enablement) vs PR #259 CI Full Matrix

True before-and-after comparison at `1 GiB` using the exact same `.github/workflows/bench-full.yml` workload (`--sizes 64KiB,1MiB,1GiB --concurrent 64 --duration 2`) on 4-CPU GitHub-hosted runners (`ubuntu-latest` and `windows-latest`):
- **Before (`main` @ `2e36237` + 1GiB bench change on `try/57-main-1gib-baseline` @ `1322e57`, zero server changes, run `44462407485`):** includes Windows Defender exclusion and all prior `main` fixes (#171, #172).
- **After (PR #259 `issue/57-multipart-speed`, runs `44261880474` on `a2b1cd3` and `44283095192` on `645269f`):** adds `spawn_blocking` pipelined `UploadPart`, `sync_data()`, `upload_checksum_info` (no `BTreeMap` clone per part), and `NotFound`-only `create_dir_all` fallback.

### Side-by-Side 1 GiB Multipart Comparison (Before vs After, Both OSes)

| Runner | Run | Commit / State | AwanFS MiB/s (P50, Peak RSS) | MinIO MiB/s (P50, Peak RSS) | RustFS MiB/s (P50, Peak RSS) | Owner Decision 2026-11-19 Check |
|---|---|---|---|---|---|---|
| `ubuntu-latest` | `44462407485` (Before) | `try/57-main-1gib-baseline` (`1322e57`, `main` server) | `189.9` (`2,237.88 ms`, `273.0 MB`) | `186.0` (`2,601.54 ms`, `854.1 MB`) | `185.0` (`2,224.02 ms`, `424.5 MB`) | `1.02x` MinIO / `1.03x` RustFS |
| `ubuntu-latest` | `44261880474` (After, PR #259 run 1) | `issue/57-multipart-speed` (`a2b1cd3`) | `188.0` (`2,335.97 ms`, `270.2 MB`) | `188.8` (`2,409.40 ms`, `801.3 MB`) | `179.6` (`2,515.53 ms`, `456.5 MB`) | `0.996x` MinIO (`1.03x` faster P50), within MinIO spread `[186.0, 194.7]` |
| `ubuntu-latest` | `44283095192` (After, PR #259 run 2) | `issue/57-multipart-speed` (`645269f`, identical server) | `200.5` (`2,366.69 ms`, `267.3 MB`) | `194.7` (`2,330.43 ms`, `802.5 MB`) | `188.0` (`2,628.62 ms`, `531.3 MB`) | `1.03x` MinIO / `1.07x` RustFS |
| `windows-latest` | `44462407485` (Before) | `try/57-main-1gib-baseline` (`1322e57`, `main` server + Defender excl.) | `423.4` (`992.84 ms`, `264.8 MB`) | `1,268.0` (`314.44 ms`, `1,045.7 MB` — **non-durable**) | `312.4` (`1,545.45 ms`, `248.0 MB`) | `1.36x` RustFS (durable); MinIO Windows writes are non-durable (`fdatasync` no-op) |
| `windows-latest` | `44261880474` (After, PR #259 run 1) | `issue/57-multipart-speed` (`a2b1cd3`, pre-Defender excl.) | `223.4` (`2,129.26 ms`, `266.1 MB`) | `1,052.4` (`387.74 ms`, `993.3 MB` — **non-durable**) | `173.5` (`2,895.49 ms`, `283.7 MB`) | `1.29x` RustFS (durable); `0.21x` MinIO (**non-durable**) |
| `windows-latest` | `44283095192` (After, PR #259 run 2) | `issue/57-multipart-speed` (`645269f`, pre-Defender excl.) | `393.9` (`1,218.43 ms`, `268.8 MB`) | `1,092.4` (`361.06 ms`, `1,005.3 MB` — **non-durable**) | `326.0` (`1,420.96 ms`, `318.5 MB`) | `1.21x` RustFS (durable); `0.36x` MinIO (**non-durable**) |

**Run-to-run spread summary (Owner Decision 2026-11-19 Option A):**
- **`ubuntu-latest`:** Across the two PR #259 runs (`44261880474` and `44283095192`), MinIO measured `188.8` and `194.7 MiB/s` (and `186.0 MiB/s` in `44462407485`, overall MinIO spread `186.0 - 194.7 MiB/s`), while AwanFS measured `188.0` and `200.5 MiB/s` (`0.996x` to `1.03x` MinIO, within/above MinIO's run-to-run spread and `1.05x - 1.07x` RustFS) with ~3x lower Peak RSS (`267.3 - 270.2 MB` vs MinIO `801.3 - 802.5 MB`).
- **`windows-latest`:** MinIO does not flush PUTs on Windows (its `fdatasync` is a no-op there), so MinIO's Windows rows (`1,052.4 - 1,268.0 MiB/s`) are non-durable and excluded per `AGENTS.md` and Owner Decision 2026-11-19. Against durable RustFS, AwanFS measured `223.4` vs `173.5 MiB/s` (`1.29x`) in `44261880474` and `393.9` vs `326.0 MiB/s` (`1.21x`) in `44283095192`.

### Full Workload Breakdown: `main` + 1GiB Baseline (`44462407485`)

| Platform | Workload | AwanFS MiB/s | AwanFS P50 | AwanFS Peak RSS | MinIO MiB/s (P50, Peak RSS) | RustFS MiB/s (P50, Peak RSS) |
|---|---|---|---|---|---|---|
| `ubuntu-latest` | `1 MiB PUT (Single Object)` | `188.2` | `167.18 ms` | `271.3 MB` | `170.4` (`191.12 ms`, `436.3 MB`) | `155.5` (`199.64 ms`, `266.7 MB`) |
| `ubuntu-latest` | `1 MiB Multipart Upload (4x256KiB Parts + Complete)` | `182.9` | `86.63 ms` | `271.3 MB` | `N/A (S3 >=5MiB spec limit)` | `N/A (S3 >=5MiB spec limit)` |
| `ubuntu-latest` | `1 GiB Multipart Upload (16x64MiB Parts + Complete)` | `189.9` | `2,237.88 ms` | `273.0 MB` | `186.0` (`2,601.54 ms`, `854.1 MB`) | `185.0` (`2,224.02 ms`, `424.5 MB`) |
| `ubuntu-latest` | `1 GiB Multipart Range GET (16x64MiB Parallel Ranges)` | `1,144.0` | `400.48 ms` | `1,200.8 MB` | `1,005.1` (`450.53 ms`, `854.1 MB`) | `831.6` (`512.15 ms`, `424.5 MB`) |
| `windows-latest` | `1 MiB PUT (Single Object)` | `429.0` | `70.48 ms` | `264.8 MB` | `93.6` (`328.11 ms`, `596.8 MB`) | `65.4` (`471.51 ms`, `151.2 MB`) |
| `windows-latest` | `1 MiB Multipart Upload (4x256KiB Parts + Complete)` | `57.2` | `271.87 ms` | `264.8 MB` | `N/A (S3 >=5MiB spec limit)` | `N/A (S3 >=5MiB spec limit)` |
| `windows-latest` | `1 GiB Multipart Upload (16x64MiB Parts + Complete)` | `423.4` | `992.84 ms` | `264.8 MB` | `1,268.0` (`314.44 ms`, `1,045.7 MB` — **non-durable**) | `312.4` (`1,545.45 ms`, `248.0 MB`) |
| `windows-latest` | `1 GiB Multipart Range GET (16x64MiB Parallel Ranges)` | `3,326.7` | `130.44 ms` | `1,142.9 MB` | `2,963.3` (`145.34 ms`, `1,045.7 MB`) | `1,861.4` (`263.50 ms`, `248.0 MB`) |

*(Note: MinIO's Windows rows are not durable writes because MinIO's `fdatasync` is a no-op on Windows per `AGENTS.md`. Earlier `main` run `44255781454` ran `--sizes 64KiB,1MiB` only; historical 2-CPU run `43164218840` in #57 was `67.2 MiB/s` on `ubuntu-latest` and `68.0 MiB/s` on `windows-latest`.)*

### Full Workload Breakdown: PR #259 CI (`44261880474`, `--sizes 64KiB,1MiB,1GiB`)

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
   - Streamed `UploadPart` (`> 4 MiB`) wrote each incoming body frame synchronously via `PartWriter::write_chunk` -> `File::write_all`, `PartWriter::finish` -> `File::sync_all`, and `std::fs::rename` directly on Tokio's async worker threads, stalling the reactor and serializing disk I/O with HTTP reads under concurrent `16 x 64 MiB` part uploads. Offloaded to a pipelined `spawn_blocking` writer driven by `fsm::part_pipeline_step`.
   - Buffered `UploadPart` (`<= 4 MiB`, including `4 x 256 KiB` 1 MiB multipart) also called blocking file create/write/sync/rename inline on Tokio worker threads; offloaded to `spawn_blocking`.
2. **`sync_all` -> `sync_data` (`server/src/multipart.rs`):**
   - `PartWriter::finish` called `File::sync_all()` instead of `File::sync_data()` (`fdatasync` on Linux, matching `VolumeManager::sync_files`), adding full metadata sync latency per part.
3. **Metadata lock cloning (`server/src/multipart.rs`, `server/src/ops/multipart.rs`):**
   - `MultipartManager::get_upload` cloned the entire `MultipartUpload` and its `BTreeMap<u32, StagedPart>` under lock in both `upload_part` and `complete_multipart_upload` just to read checksum config and tags (`upload_checksum_info` and `upload_complete_info` replace those clones).
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
