gh workflow run bench-large.yml --ref main -f sizes=16MiB,64MiB,256MiB -f runs=3

# Large PUT with one client: hash and write overlap (#170)

## Step 1: Hash throughput & bottleneck analysis

Measured locally (`cargo test --release --locked --manifest-path server/Cargo.toml --lib checksum::tests::throughput_64mib -- --ignored --nocapture`, 64 MiB buffer):

| Algorithm | Throughput | Time (64 MiB) |
|---|---|---|
| CRC32 | 12132.7 MiB/s | 5.28 ms |
| CRC32C | 16985.1 MiB/s | 3.77 ms |
| CRC64NVME | 1707.3 MiB/s | 37.49 ms |
| SHA1 | 1515.8 MiB/s | 42.22 ms |
| SHA256 | 1491.2 MiB/s | 42.92 ms |

On streamed `PutObject` (`> STREAM_PUT_MIN`), `ops/object.rs` previously hashed every incoming chunk with both MD5 and CRC32C via `Digest::new()`, even though `NeedleWriter` (`server/src/volume.rs`) already computes `loc.payload_crc32c` while writing the needle. Commit `1233d58` (`Digest::new_md5_only()`) removes the duplicate CRC32C pass in `ops/object.rs` while keeping `loc.payload_crc32c` intact in `NeedleWriter`.

## Step 2: Benchmark results on `main` (post-#216 merge)

| Workflow | Run | Metric | Result | Notes |
|---|---|---|---|---|
| `bench-large.yml` (`large_object_check.py`) | `43904980885` | PUT 1 client, 16 MiB | `1.24x` vs MinIO | Faster than MinIO |
| `bench-large.yml` (`large_object_check.py`) | `43904980885` | PUT 1 client, 64 MiB | `1.00x` (~215 MiB/s) | Inconclusive: runner disk ceiling (~215 MiB/s) masks hash/write overlap |
| `bench-large.yml` (`large_object_check.py`) | `43904980885` | PUT 1 client, 256 MiB | `1.00x` (~215 MiB/s) | Inconclusive: runner disk ceiling (~215 MiB/s) masks hash/write overlap |
| `bench-large.yml` (`large_object_check.py`) | `43904980885` | `small_busy` P99 during large PUTs | `0.5 ms`, `0.4 ms` | Under `2 ms` gate |
| `bench-ratio.yml` | `43904970500` | 6 rows + RSS budget | All 6 rows ok; `394.3 MiB` RSS | Under `456.0 MiB` ceiling |

## Step 3: the final pipeline, PR #252 at 24ce6a8 (thin LTO dropped)

`gh workflow run bench-large.yml --ref issue/170-large-put-speed -f sizes=16MiB,64MiB,256MiB -f runs=3` (run [44465505395](https://github.com/iskandarnorazli/Awanfs/actions/runs/44465505395), `ubuntu-latest`), which ran `python benchmarks/large_object_check.py --server-bin server/target/release/s3-server --minio-bin .tmp/minio/minio --mc-bin .tmp/minio/mc --sizes 16MiB,64MiB,256MiB --runs 3 --report large_report.md`. The tables as the run printed them (headings one level down):

### 16MiB objects

| metric | AwanFS | MinIO | AwanFS better by |
|---|---|---|---|
| PUT, 1 client, MiB/s | 137.0 | 172.1 | 0.80x |
| PUT, 4 clients, MiB/s | 189.9 | 196.7 | 0.97x |
| first GET, 1 client, MiB/s | 3591.0 | 1909.7 | 1.88x |
| first GET, ms to headers (median) | 0.3 | 1.2 | 4.23x |
| GET again, 4 clients, MiB/s | 5520.0 | 4446.8 | 1.24x |
| 1 MiB range, P50 ms | 1.6 | 3.2 | 1.96x |
| 1 MiB range, P99 ms | 3.3 | 7.8 | 2.32x |
| multipart upload, 16 MiB parts x4, MiB/s | 130.9 | 136.2 | 0.96x |
| GET of the multipart object, MiB/s | 1921.9 | 1920.1 | 1.00x |
| 4 KiB GET P99 ms, server idle | 0.1 | 0.5 | 3.50x |
| 4 KiB GET P99 ms, during 4 large PUTs | 0.5 | 1.4 | 2.56x |

### 64MiB objects

| metric | AwanFS | MinIO | AwanFS better by |
|---|---|---|---|
| PUT, 1 client, MiB/s | 139.2 | 148.3 | 0.94x |
| PUT, 4 clients, MiB/s | 183.7 | 180.6 | 1.02x |
| first GET, 1 client, MiB/s | 4296.4 | 2116.6 | 2.03x |
| first GET, ms to headers (median) | 0.4 | 1.4 | 3.67x |
| GET again, 4 clients, MiB/s | 5943.2 | 4580.2 | 1.30x |
| 1 MiB range, P50 ms | 1.8 | 3.0 | 1.68x |
| 1 MiB range, P99 ms | 3.6 | 5.3 | 1.47x |
| multipart upload, 16 MiB parts x4, MiB/s | 184.8 | 171.8 | 1.08x |
| GET of the multipart object, MiB/s | 2216.9 | 2108.4 | 1.05x |
| 4 KiB GET P99 ms, server idle | 0.1 | 0.5 | 3.75x |
| 4 KiB GET P99 ms, during 4 large PUTs | 0.3 | 0.9 | 2.89x |

### 256MiB objects

| metric | AwanFS | MinIO | AwanFS better by |
|---|---|---|---|
| PUT, 1 client, MiB/s | 145.3 | 151.5 | 0.96x |
| PUT, 4 clients, MiB/s | 174.2 | 175.6 | 0.99x |
| first GET, 1 client, MiB/s | 4229.3 | 2224.6 | 1.90x |
| first GET, ms to headers (median) | 0.5 | 1.4 | 2.56x |
| GET again, 4 clients, MiB/s | 6114.4 | 5040.0 | 1.21x |
| 1 MiB range, P50 ms | 1.7 | 3.1 | 1.84x |
| 1 MiB range, P99 ms | 3.3 | 9.6 | 2.87x |
| multipart upload, 16 MiB parts x4, MiB/s | 191.1 | 173.9 | 1.10x |
| GET of the multipart object, MiB/s | 3377.8 | 2246.8 | 1.50x |
| 4 KiB GET P99 ms, server idle | 0.1 | 0.5 | 3.74x |
| 4 KiB GET P99 ms, during 4 large PUTs | 0.2 | 2.2 | 11.81x |

### Memory over the whole run (highest of the runs)

| server | peak RSS MiB | peak heap (RssAnon) MiB | peak file pages (RssFile) MiB |
|---|---|---|---|
| awanfs | 386.1 | 40.2 | 345.5 |
| minio | 368.8 | 312.1 | 57.6 |

##[group]Run set +e
set +e

Owner decision 2026-11-19 on #170 ([comment](https://github.com/iskandarnorazli/Awanfs/issues/170#issuecomment-6998467791)): the partial gain is accepted; the remaining 1-client PUT gap is recorded as the CI runner's disk ceiling.
