Command: `python benchmarks/large_object_check.py --server-bin server/target/release/s3-server --minio-bin .tmp/minio/minio --mc-bin .tmp/minio/mc --sizes 16MiB,64MiB,256MiB --runs 3 --report large_report.md`

Run on GitHub Actions: https://github.com/iskandarnorazli/Awanfs/actions/runs/43572993562 (workflow_dispatch on PR #169, the commit before the test-data fix; the server code is the same), ubuntu-latest.

# Large objects: AwanFS next to MinIO (2026-11-14 07:59 UTC)

Machine: runnervm8axu0 (Linux-7.0.0-1017-azure-x86_64-with-glibc2.39, 4 CPUs). 3 run(s) per server, order alternating; medians. The client is a Python process on the same machine. "AwanFS better by" is AwanFS/MinIO for MiB/s and MinIO/AwanFS for ms, so above 1 is always AwanFS ahead.

## 16MiB objects

| metric | AwanFS | MinIO | AwanFS better by |
|---|---|---|---|
| PUT, 1 client, MiB/s | 139.2 | 151.4 | 0.92x |
| PUT, 4 clients, MiB/s | 192.3 | 204.4 | 0.94x |
| first GET, 1 client, MiB/s | 3627.9 | 1795.2 | 2.02x |
| first GET, ms to headers (median) | 0.4 | 1.2 | 3.32x |
| GET again, 4 clients, MiB/s | 7253.0 | 5126.2 | 1.41x |
| 1 MiB range, P50 ms | 1.5 | 3.3 | 2.17x |
| 1 MiB range, P99 ms | 3.1 | 9.3 | 3.06x |
| multipart upload, 16 MiB parts x4, MiB/s | 130.5 | 140.9 | 0.93x |
| GET of the multipart object, MiB/s | 2303.5 | 2059.2 | 1.12x |
| 4 KiB GET P99 ms, server idle | 0.2 | 0.5 | 2.83x |
| 4 KiB GET P99 ms, during 4 large PUTs | 0.5 | 1.6 | 3.27x |

## 64MiB objects

| metric | AwanFS | MinIO | AwanFS better by |
|---|---|---|---|
| PUT, 1 client, MiB/s | 138.1 | 152.1 | 0.91x |
| PUT, 4 clients, MiB/s | 181.3 | 188.7 | 0.96x |
| first GET, 1 client, MiB/s | 3574.7 | 2001.7 | 1.79x |
| first GET, ms to headers (median) | 0.4 | 1.0 | 2.24x |
| GET again, 4 clients, MiB/s | 7525.8 | 5710.4 | 1.32x |
| 1 MiB range, P50 ms | 1.6 | 2.5 | 1.61x |
| 1 MiB range, P99 ms | 3.2 | 4.4 | 1.40x |
| multipart upload, 16 MiB parts x4, MiB/s | 179.1 | 177.7 | 1.01x |
| GET of the multipart object, MiB/s | 2517.1 | 2368.6 | 1.06x |
| 4 KiB GET P99 ms, server idle | 0.2 | 0.5 | 2.71x |
| 4 KiB GET P99 ms, during 4 large PUTs | 0.2 | 0.8 | 3.49x |

## 256MiB objects

| metric | AwanFS | MinIO | AwanFS better by |
|---|---|---|---|
| PUT, 1 client, MiB/s | 138.2 | 154.5 | 0.89x |
| PUT, 4 clients, MiB/s | 176.8 | 179.2 | 0.99x |
| first GET, 1 client, MiB/s | 3626.1 | 2093.4 | 1.73x |
| first GET, ms to headers (median) | 0.4 | 1.1 | 2.48x |
| GET again, 4 clients, MiB/s | 7815.8 | 5840.6 | 1.34x |
| 1 MiB range, P50 ms | 1.6 | 2.7 | 1.68x |
| 1 MiB range, P99 ms | 2.8 | 5.7 | 2.04x |
| multipart upload, 16 MiB parts x4, MiB/s | 194.6 | 185.9 | 1.05x |
| GET of the multipart object, MiB/s | 2870.0 | 2497.0 | 1.15x |
| 4 KiB GET P99 ms, server idle | 0.2 | 0.5 | 2.70x |
| 4 KiB GET P99 ms, during 4 large PUTs | 0.3 | 2.0 | 7.04x |

## Memory over the whole run (highest of the runs)

| server | peak RSS MiB | peak heap (RssAnon) MiB | peak file pages (RssFile) MiB |
|---|---|---|---|
| awanfs | 2654.6 | 11.7 | 2643.3 |
| minio | 366.6 | 309.3 | 57.3 |

## Against the baseline (`2026-11-14-large-objects.md`, run 43544670813, before #155 step 1)

| AwanFS | 16 MiB | 64 MiB | 256 MiB |
|---|---|---|---|
| first GET, ms to headers: before | 1.5 | 5.2 | 20.7 |
| first GET, ms to headers: now | 0.4 | 0.4 | 0.4 |
| 1 MiB range, P50 ms: before | 3.2 | 8.2 | 29.6 |
| 1 MiB range, P50 ms: now | 1.5 | 1.6 | 1.6 |

The two runs are on different runners of the same class, so compare shapes, and each run's
AwanFS column with its own MinIO column. Time to headers and range latency no longer grow
with object size. Every GET row is ahead of MinIO in this run.

Peak RSS is 2654.6 MiB, 2643.3 MiB of it file pages (baseline 4452.6 and 4441.2): a range no
longer faults the whole object in, but full GETs still read through the mapping. Taking large
reads out of the mapping is step two of #155.

Single-client PUT is 0.89x to 0.92x of MinIO here, and 0.75x to 0.81x in run 43572749471 on a
runner with a faster disk; at 4 clients the two are level. This is not from #155 or #154
(neither touches an uncontended PUT's work): it is the first runs on disks fast enough to
show it. A streamed PUT hashes and writes on one thread.
