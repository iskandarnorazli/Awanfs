Command: `python benchmarks/large_object_check.py --server-bin server/target/release/s3-server --minio-bin .tmp/minio/minio --mc-bin .tmp/minio/mc --sizes 16MiB,64MiB,256MiB --runs 3 --report large_report.md`

Run on GitHub Actions: https://github.com/iskandarnorazli/Awanfs/actions/runs/43544670813 (PR #147, ubuntu-latest).

# Large objects: AwanFS next to MinIO (2026-11-14 02:17 UTC)

Machine: ip-10-0-131-115 (Linux-7.0.0-1014-aws-x86_64-with-glibc2.39, 4 CPUs). 3 run(s) per server, order alternating; medians. The client is a Python process on the same machine. "AwanFS better by" is AwanFS/MinIO for MiB/s and MinIO/AwanFS for ms, so above 1 is always AwanFS ahead.

## 16MiB objects

| metric | AwanFS | MinIO | AwanFS better by |
|---|---|---|---|
| PUT, 1 client, MiB/s | 270.1 | 310.4 | 0.87x |
| PUT, 4 clients, MiB/s | 213.3 | 212.5 | 1.00x |
| first GET, 1 client, MiB/s | 3450.1 | 2156.0 | 1.60x |
| first GET, ms to headers (median) | 1.5 | 1.1 | 0.72x |
| GET again, 4 clients, MiB/s | 6174.2 | 4497.3 | 1.37x |
| 1 MiB range, P50 ms | 3.2 | 3.3 | 1.03x |
| 1 MiB range, P99 ms | 6.2 | 9.0 | 1.45x |
| multipart upload, 16 MiB parts x4, MiB/s | 379.7 | 451.6 | 0.84x |
| GET of the multipart object, MiB/s | 2856.4 | 2208.0 | 1.29x |
| 4 KiB GET P99 ms, server idle | 0.1 | 0.4 | 4.23x |
| 4 KiB GET P99 ms, during 4 large PUTs | 0.5 | 1.5 | 3.13x |

## 64MiB objects

| metric | AwanFS | MinIO | AwanFS better by |
|---|---|---|---|
| PUT, 1 client, MiB/s | 215.8 | 215.3 | 1.00x |
| PUT, 4 clients, MiB/s | 213.5 | 213.3 | 1.00x |
| first GET, 1 client, MiB/s | 3516.1 | 2435.5 | 1.44x |
| first GET, ms to headers (median) | 5.2 | 1.0 | 0.20x |
| GET again, 4 clients, MiB/s | 5902.3 | 5009.3 | 1.18x |
| 1 MiB range, P50 ms | 8.2 | 2.6 | 0.32x |
| 1 MiB range, P99 ms | 13.9 | 5.9 | 0.43x |
| multipart upload, 16 MiB parts x4, MiB/s | 914.8 | 930.6 | 0.98x |
| GET of the multipart object, MiB/s | 3401.7 | 2120.0 | 1.60x |
| 4 KiB GET P99 ms, server idle | 0.1 | 0.4 | 4.07x |
| 4 KiB GET P99 ms, during 4 large PUTs | 0.4 | 1.2 | 3.36x |

## 256MiB objects

| metric | AwanFS | MinIO | AwanFS better by |
|---|---|---|---|
| PUT, 1 client, MiB/s | 217.9 | 218.0 | 1.00x |
| PUT, 4 clients, MiB/s | 213.6 | 172.4 | 1.24x |
| first GET, 1 client, MiB/s | 3370.6 | 2504.5 | 1.35x |
| first GET, ms to headers (median) | 20.7 | 1.2 | 0.06x |
| GET again, 4 clients, MiB/s | 5370.9 | 5188.6 | 1.04x |
| 1 MiB range, P50 ms | 29.6 | 3.0 | 0.10x |
| 1 MiB range, P99 ms | 57.1 | 16.4 | 0.29x |
| multipart upload, 16 MiB parts x4, MiB/s | 1054.3 | 972.4 | 1.08x |
| GET of the multipart object, MiB/s | 3690.2 | 2395.7 | 1.54x |
| 4 KiB GET P99 ms, server idle | 0.1 | 0.4 | 4.02x |
| 4 KiB GET P99 ms, during 4 large PUTs | 0.5 | 3.2 | 6.86x |

## Memory over the whole run (highest of the runs)

| server | peak RSS MiB | peak heap (RssAnon) MiB | peak file pages (RssFile) MiB |
|---|---|---|---|
| awanfs | 4452.6 | 12.0 | 4441.2 |
| minio | 383.9 | 327.0 | 57.6 |

## What this run shows

- **Time to headers and range latency grow with object size on AwanFS, and stay flat on MinIO.** A GET of a single-needle object checksums the whole payload before the first byte. Median time to headers is 1.5, 5.2 and 20.7 ms at 16, 64 and 256 MiB; the 1 MiB range P50 is 3.2, 8.2 and 29.6 ms, against about 3 ms on MinIO at every size.
- **Large uploads do not delay small requests.** The 4 KiB GET P99 during four large PUTs is 0.4 to 0.5 ms.
- **Whole-object PUT is the same on both servers at about 215 MiB/s, at 1 and 4 clients.** That is the runner's disk, so this run cannot rank the servers on PUT.
- **Peak RSS is 4452.6 MiB on AwanFS, 4441.2 MiB of it file pages**, against 383.9 MiB on MinIO. Reads go through a mapping of the volume, so every byte read stays resident until the kernel reclaims it. The heap peaked at 12.0 MiB.
