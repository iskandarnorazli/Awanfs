Command: `python3 benchmarks/durability_probe.py --server-bin server/target/release/s3-server --minio-bin .tmp/minio/minio --mc-bin .tmp/minio/mc --rustfs-bin .tmp/rustfs/rustfs --data-dir /home/runner/work/_temp --run-id 44916961459 --output /home/runner/work/_temp/durability_probe.md`

Run ID: `44916961459`

# Server Durability Probe (2026-11-21 15:03 UTC)

- **Machine:** ip-10-0-138-212 (Linux-7.0.0-1016-aws-x86_64-with-glibc2.43, 4 CPUs, 15.3 GB RAM, CPU: Intel(R) Xeon(R) 6975P-C)
- **Sync trace passes:** 200 PUTs of 64 KiB, 16 concurrent, under `strace -f -c -e trace=fsync,fdatasync,sync_file_range,syncfs,msync` and `strace -f -e trace=openat` (`O_DSYNC` or `O_SYNC`).
- **Throughput pass:** separate un-straced pass, 64 KiB PUTs, 64 workers, 10 s, same Python `http.client` client for every row.
- **Rule:** at least one sync call or synchronous open per PUT (`>= 1.00`) is `durable`, otherwise `not durable`.
- **Client ceiling:** the Python client caps near 5,000 requests a second (#262).

| Server | Sync calls / PUT | Sync opens / PUT | Durability | 64 KiB PUT ops/s |
|---|---|---|---|---|
| AwanFS (--durability group) | 0.83 | 0.00 | not durable | 3,374.3 |
| MinIO | 1.00 | 0.00 | durable | 980.2 |
| RustFS (release defaults) | 3.09 | 0.00 | durable | 984.2 |

## Raw Trace Counts

| Server | Binary | PUTs traced | Sync syscalls | Sync opens (`O_DSYNC`/`O_SYNC`) |
|---|---|---|---|---|
| AwanFS (--durability group) | Memory budget 2048 MiB (memtotal): payload cache 1024 MiB, in-flight request bodies 1024 MiB | 200 | 166 | 0 |
| MinIO | minio version DEVELOPMENT.GOGET (commit-id=DEVELOPMENT.GOGET) | 200 | 200 | 0 |
| RustFS (release defaults) | rustfs 1.0.5 | 200 | 618 | 0 |

## Observation on group commit and RustFS

- **RustFS (`1.0.5` release defaults):** made 618 sync syscalls across 200 PUTs (3.09 sync calls per PUT, 0 synchronous opens) and achieved 984.2 PUT ops/s at 64 workers (matching MinIO's 980.2 ops/s). RustFS's release default syncs every PUT (`durable`).
- **AwanFS (`--durability group`):** made 166 sync syscalls across 200 concurrent PUTs (`c=16`), i.e. 83 volume+journal `fdatasync` pairs shared across ~2.4 PUTs per batch. Under the probe's literal `>= 1.00` per-PUT rule at `c=16`, `0.83 < 1.00` prints `not durable` in the table even though every PUT waited for a shared group-commit sync before `200 OK` (and `group` achieved 3,374.3 ops/s at 64 workers vs MinIO 980.2 ops/s and RustFS 984.2 ops/s).
