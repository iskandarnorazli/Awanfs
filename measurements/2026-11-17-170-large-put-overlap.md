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
