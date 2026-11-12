# Benchmarks

Three ways to measure AwanFS, from cheapest to most detailed. Numbers from one machine are not comparable with another's; the AwanFS/MinIO ratio is.

## 1. Work counts (every PR, no timing)

```bash
cargo test --locked --manifest-path server/Cargo.toml --test work_count_test
```

Exact disk writes, syncs, reads and payload copies per request type. A change that adds work fails here on any machine.

## 2. Throughput on your machine, against MinIO

```bash
python benchmarks/bench.py                 # build, fetch MinIO, run, save a report
python benchmarks/bench.py --awanfs-only   # AwanFS only, no MinIO needed
python benchmarks/bench.py --runs 3 --duration 2
```

Needs Python 3 and a Rust toolchain. For the MinIO comparison it also needs MinIO and `mc` in `.tmp/minio/`; `bench.py` gets the pinned release (`RELEASE.2025-09-07T16-13-09Z`, the one CI uses) by copying it out of the quay.io image when Docker is available on Linux, or by building it from source when `git` and `go` are installed. Or put the binaries there yourself.

What it runs: six workloads (64 KiB PUT, GET, 4 KiB range GET, 80/20 mixed; 1 MiB PUT and GET) against AwanFS and MinIO one after the other, alternating which goes first, for `--runs` rounds of `--duration` seconds each. The report goes to `measurements/bench_<date>_<host>.md` and starts with its command; it lists the median MiB/s of each server, their ratio, and the verdict against `benchmarks/ratio_baseline.json`.

Read the results with these in mind:
- The baseline was recorded on a 2-CPU GitHub runner. On a very different machine, "faster" or "REGRESSION" is a hint; compare your ratio with your own earlier runs.
- PUT rows compare a non-durable AwanFS write with MinIO's synced one until group commit (#29) lands.
- The client is Python; on small machines it competes with the servers for CPU. Close other work while it runs.
- If you commit a report, keep it as written: its numbers are that command's output.

## 3. Nightly ratio in CI

`.github/workflows/bench-ratio.yml` runs the same check every night at 19:17 UTC and on demand (Actions tab, "Nightly MinIO ratio", "Run workflow"). The report is in the run's summary and artifacts; a regression opens or comments on a `perf-regression` issue.

## 4. Full matrix (64 KiB to 10 GiB, MinIO and RustFS)

```bash
python benchmarks/run_warp_benchmark.py --sizes 64KiB,1MiB,1GiB,10GiB --concurrent 64 --duration 2
```

The original Windows-oriented harness: it pins the server to cores 0-3 and the client to cores 4-7 (needs 8 or more cores), looks for MinIO, mc and RustFS in `.tmp/`, and writes `measurements/s3_warp_minio_vs_awan.md`. The 1 GiB and 10 GiB phases need about 25 GB of free disk.
