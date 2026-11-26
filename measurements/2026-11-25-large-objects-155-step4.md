gh workflow run bench-large.yml --ref issue/155-scrub-and-positioned-reads

# Large objects and background scrub (#155, step four)

Measured on `issue/155-scrub-and-positioned-reads` at commit `d171b5e` through `.github/workflows/bench-large.yml` (run `45575537280`) and `.github/workflows/bench-ratio.yml` (run `45575543162`), GitHub `ubuntu-latest` (`runnervm0hi8g`, Linux 7.0.0-1020-azure, 4 CPUs). Default settings (`--scrub-mib-per-sec 10`, 1 MiB chunks, read-ahead 1, `group` durability). "AwanFS better by" is `AwanFS/MinIO` for MiB/s and `MinIO/AwanFS` for ms, so above `1.0x` means AwanFS is ahead.

## Large objects: AwanFS next to MinIO (`bench-large.yml`, run `45575537280`)

Command run inside workflow:
`python benchmarks/large_object_check.py --server-bin server/target/release/s3-server --minio-bin .tmp/minio/minio --mc-bin .tmp/minio/mc --sizes 16MiB,64MiB,256MiB --runs 3 --report large_report.md`

### 16 MiB objects

| metric | AwanFS | MinIO | AwanFS better by |
|---|---|---|---|
| PUT, 1 client, MiB/s | 148.2 | 178.8 | 0.83x |
| PUT, 4 clients, MiB/s | 188.5 | 201.1 | 0.94x |
| first GET, 1 client, MiB/s | 5355.8 | 2282.0 | 2.35x |
| first GET, ms to headers (median) | 0.2 | 1.2 | 4.81x |
| GET again, 4 clients, MiB/s | 8214.4 | 6222.0 | 1.32x |
| 1 MiB range, P50 ms | 1.1 | 2.6 | 2.40x |
| 1 MiB range, P99 ms | 2.7 | 6.7 | 2.53x |
| multipart upload, 16 MiB parts x4, MiB/s | 145.3 | 145.7 | 1.00x |
| GET of the multipart object, MiB/s | 2881.5 | 2272.6 | 1.27x |
| 4 KiB GET P99 ms, server idle | 0.1 | 0.3 | 3.29x |
| 4 KiB GET P99 ms, during 4 large PUTs | 0.3 | 0.8 | 2.72x |

### 64 MiB objects

| metric | AwanFS | MinIO | AwanFS better by |
|---|---|---|---|
| PUT, 1 client, MiB/s | 144.2 | 151.4 | 0.95x |
| PUT, 4 clients, MiB/s | 183.3 | 184.3 | 0.99x |
| first GET, 1 client, MiB/s | 5591.4 | 2637.4 | 2.12x |
| first GET, ms to headers (median) | 0.4 | 1.1 | 2.49x |
| GET again, 4 clients, MiB/s | 8951.2 | 6755.8 | 1.32x |
| 1 MiB range, P50 ms | 1.2 | 2.1 | 1.74x |
| 1 MiB range, P99 ms | 2.1 | 4.2 | 2.00x |
| multipart upload, 16 MiB parts x4, MiB/s | 192.7 | 182.1 | 1.06x |
| GET of the multipart object, MiB/s | 3915.6 | 2414.9 | 1.62x |
| 4 KiB GET P99 ms, server idle | 0.1 | 0.4 | 4.06x |
| 4 KiB GET P99 ms, during 4 large PUTs | 0.1 | 0.6 | 4.89x |

### 256 MiB objects

| metric | AwanFS | MinIO | AwanFS better by |
|---|---|---|---|
| PUT, 1 client, MiB/s | 141.9 | 155.9 | 0.91x |
| PUT, 4 clients, MiB/s | 175.3 | 178.8 | 0.98x |
| first GET, 1 client, MiB/s | 5011.6 | 2817.7 | 1.78x |
| first GET, ms to headers (median) | 0.5 | 1.1 | 2.23x |
| GET again, 4 clients, MiB/s | 8952.9 | 7213.4 | 1.24x |
| 1 MiB range, P50 ms | 1.2 | 1.8 | 1.54x |
| 1 MiB range, P99 ms | 2.3 | 3.7 | 1.56x |
| multipart upload, 16 MiB parts x4, MiB/s | 187.9 | 186.5 | 1.01x |
| GET of the multipart object, MiB/s | 3780.3 | 2751.8 | 1.37x |
| 4 KiB GET P99 ms, server idle | 0.1 | 0.3 | 3.33x |
| 4 KiB GET P99 ms, during 4 large PUTs | 0.1 | 0.8 | 7.25x |

### Memory over the whole `bench-large.yml` run (highest of the runs, note for #133)

| server | peak RSS MiB | peak heap (RssAnon) MiB | peak file pages (RssFile) MiB |
|---|---|---|---|
| awanfs | 50.4 | 39.2 | 9.2 |
| minio | 373.4 | 315.6 | 57.8 |

For #133: eliminating the remaining large-object and multipart mapping paths drops `bench-large.yml` peak RSS from `4452.6 MiB` (4441.2 MiB file pages, baseline run `43544670813`) and `370.5 MiB` (Step 2, run `43736773027`) down to `50.4 MiB` total RSS (`39.2 MiB` heap, `9.2 MiB` `RssFile`), well below the `512 MiB` `RssFile` gate and `7.4x` lower than MinIO (`373.4 MiB`).

Relocation of a 64 MiB streamed PUT under `--memory-budget 32MiB` (`relocation_rss.py` in run `45575537280`): peak RSS `13.2 MiB` (`6.8 MiB` `RssAnon`, `6.8 MiB` `RssFile`) against a `48.0 MiB` ceiling (`ok`).

## Ratio and budget gates (`bench-ratio.yml`, run `45575543162`)

Command run inside workflow:
`python benchmarks/ratio_check.py --server-bin server/target/release/s3-server --minio-bin .tmp/minio/minio --mc-bin .tmp/minio/mc --runs 5 --duration 3 --report ratio_report.md`

| workload | AwanFS MiB/s (median) | MinIO MiB/s (median) | ratio | baseline | verdict |
|---|---|---|---|---|---|
| `put_64k` | 236.49 | 84.47 | 2.80 | 3.94 | ok |
| `get_64k` | 354.94 | 228.41 | 1.55 | 1.64 | ok |
| `range_get_4k` | 22.98 | 14.42 | 1.59 | 2.10 | ok |
| `mixed_64k` | 314.74 | 189.08 | 1.66 | 2.15 | ok |
| `put_1m` | 418.15 | 368.44 | 1.13 | 1.16 | ok |
| `get_1m` | 3689.80 | 2015.78 | 1.83 | 2.74 | ok |

- `ratio_check.py` peak RSS: AwanFS `103.7 MiB` (`ok`, ceiling `250.0 MiB`) vs MinIO `598.0 MiB`.
- `budget_rss.py` (`--budget 256MiB --concurrency 64 --size 1MiB --duration 10.0 --overhead 200.0`): peak RSS `391.2 MiB` (`ok`, ceiling `456.0 MiB`; `125.9 MiB` `RssAnon`, `265.4 MiB` `RssFile`).
- `budget_rss.py` (`--budget 256MiB --concurrency 64 --size 4KiB --duration 20.0 --overhead 200.0 --unique-keys`): peak RSS `194.6 MiB` (`ok`, ceiling `456.0 MiB`).
