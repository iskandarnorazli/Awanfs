`gh api -X POST repos/iskandarnorazli/Awanfs/actions/workflows/bench-scaling.yml/dispatches -f ref=try/hs5-experiment -f inputs[cores]=1,2,4 -f inputs[duration]=5 -f inputs[runs]=3 -F inputs[matrix]=true -f inputs[patterns]=seq-write,seq-read -f inputs[sizes]=4KiB,64KiB,1MiB -f inputs[concurrencies]=64 -f inputs[client_procs]=8` (run 45872695021, job "Core scaling (ubuntu-latest)" 140091889726, branch `try/hs5-experiment` at `b246b49`, not merged: `main` at `de1959c` plus HS5 in `core_scaling.py`), which ran `python benchmarks/core_scaling.py --cores 1,2,4 --runs 3 --duration 5 --client-procs 8 --data-dir <runner temp> --output <runner temp>/core_scaling/report.md --compare-fast --matrix --pattern seq-write,seq-read --sizes 4KiB,64KiB,1MiB --concurrencies 64 --seed 42 --servers AwanFS,HS5,Garage,SeaweedFS,MinIO,RustFS`

# AwanFS against HS5 and four others, once, on the hosted Linux runner (#452)

An experiment (owner decision on [#452](https://github.com/iskandarnorazli/Awanfs/issues/452#issuecomment-7199926741)): run once, on an unmerged branch. HS5 release 1.5.0 (`hs5-amd64-musl`, sha256 `94510190e7c8af34ea973e28fcfd3dc9bafa5700972d20d2417eb7cea30ccee1`) ran at its defaults: `hs5 run` with no `--wal-mode` and no `--manual-commit`, so a PUT is persisted before it is answered, and commits wait up to 10 ms (`commit_max_defer_ms`) to batch. The other servers ran as `bench-scaling.yml` runs them. Each point is the median of 3 rounds of 5 s at 64 requests in flight.

The runner has 4 logical cores, so at 4 server cores the client shares them; the 2-core column is the clean comparison.

## Results at 2 server cores (ops/s, from the table below)

| | AwanFS | AwanFS fast | HS5 | Garage | SeaweedFS | MinIO | RustFS |
|---|---|---|---|---|---|---|---|
| PUT 4 KiB | 8,623 | 8,361 | 3,246 | 965 | 2,083 | 1,562 | 1,570 |
| PUT 64 KiB | 3,338 | 4,327 | 2,318 | 586 | 1,185 | 803 | 1,011 |
| PUT 1 MiB | 400 | 479 | 397 | 192 | 189 | 322 | 251 |
| GET 4 KiB | 8,120 | | 7,059 | 1,537 | 3,951 | 4,949 | 6,830 |
| GET 64 KiB | 7,037 | | 6,285 | 1,154 | 3,334 | 4,304 | 6,662 |
| GET 1 MiB | 3,795 | | 2,912 | 325 | 822 | 1,414 | 550 |

- **PUT:** AwanFS is ahead of HS5 by 2.7x at 4 KiB and 1.4x at 64 KiB, and level at 1 MiB (400 against 397). Both sync before answering.
- **GET:** AwanFS is ahead by 1.15x at 4 KiB, 1.12x at 64 KiB and 1.3x at 1 MiB. These are warm reads: each GET phase reads objects the run just wrote.
- **Not like for like:** Garage and SeaweedFS do not sync their writes at these defaults (see Durability Settings below).
- **PUT 1 MiB differs from the last run.** Here AwanFS is level with HS5 and ahead of Garage (192). In run 45808274063 (`d7344fc`, the tool's core mode rather than `--matrix`) it was 193 against Garage's 279 at 2 cores. Between the two runs, both the code (#441, #435) and the workload changed, so which one explains it is not measured.

## Report (job output)

## Durability Settings

| Server | Documented Single-Node Default | PUT waits for sync? | Notes |
|---|---|---|---|
| AwanFS | --durability=group (group commit, #29) | Yes | Concurrent PUTs share one fsync before 200 OK. |
| MinIO | O_DSYNC / fdatasync | Yes | Syncs each PUT (O_DSYNC / fdatasync) before returning 200 OK. |
| RustFS | release defaults (durable) | Yes | Measured on Linux (measurements/2026-11-21-durability-probe.md, #334): 3.09 sync calls/PUT (618 syncs / 200 PUTs). |
| Garage | metadata_fsync = false, data_fsync = false (LMDB) | No | Default single-node config skips fsync on metadata and data blocks (cluster mode relies on replication); client uses AWS SigV4 (UNSIGNED-PAYLOAD). |
| SeaweedFS | weed mini (filer.conf fsync unset / false) | No | Default storage rule skips fsync on volume .dat/.idx and filer metadata unless fsync = true is configured. |

## All Measured Points

| Server | Pattern | Size | Cores | Concurrency | ops/s | MiB/s | P50 (ms) | P99 (ms) | CPU % |
|---|---|---|---|---|---|---|---|---|---|
| AwanFS | seq-write | 4 KiB | 1 | 64 | 4,648 | 18.16 | 13.10 | 20.81 | 95.5% |
| AwanFS | seq-write | 4 KiB | 2 | 64 | 8,623 | 33.68 | 6.88 | 15.51 | 169.0% |
| AwanFS | seq-write | 4 KiB | 4 | 64 | 7,912 | 30.91 | 7.67 | 15.86 | 158.2% |
| AwanFS | seq-write | 64 KiB | 1 | 64 | 1,870 | 116.85 | 31.94 | 54.24 | 96.9% |
| AwanFS | seq-write | 64 KiB | 2 | 64 | 3,338 | 208.60 | 18.03 | 38.59 | 184.7% |
| AwanFS | seq-write | 64 KiB | 4 | 64 | 3,949 | 246.80 | 15.41 | 35.39 | 221.2% |
| AwanFS | seq-write | 1 MiB | 1 | 64 | 224 | 224.28 | 270.03 | 431.79 | 97.6% |
| AwanFS | seq-write | 1 MiB | 2 | 64 | 400 | 399.84 | 150.83 | 312.82 | 179.0% |
| AwanFS | seq-write | 1 MiB | 4 | 64 | 408 | 407.71 | 149.12 | 352.12 | 189.2% |
| AwanFS | seq-read | 4 KiB | 1 | 64 | 9,984 | 39.00 | 6.45 | 9.25 | 97.9% |
| AwanFS | seq-read | 4 KiB | 2 | 64 | 8,120 | 31.72 | 7.41 | 20.80 | 79.1% |
| AwanFS | seq-read | 4 KiB | 4 | 64 | 11,857 | 46.32 | 5.10 | 11.54 | 95.5% |
| AwanFS | seq-read | 64 KiB | 1 | 64 | 8,382 | 523.87 | 7.68 | 9.07 | 99.1% |
| AwanFS | seq-read | 64 KiB | 2 | 64 | 7,037 | 439.80 | 8.53 | 23.94 | 79.2% |
| AwanFS | seq-read | 64 KiB | 4 | 64 | 10,185 | 636.58 | 6.06 | 12.35 | 104.8% |
| AwanFS | seq-read | 1 MiB | 1 | 64 | 2,339 | 2338.77 | 27.23 | 30.99 | 99.3% |
| AwanFS | seq-read | 1 MiB | 2 | 64 | 3,795 | 3794.74 | 16.16 | 34.20 | 188.0% |
| AwanFS | seq-read | 1 MiB | 4 | 64 | 3,749 | 3748.89 | 16.85 | 33.04 | 193.4% |
| AwanFS (--durability fast) | seq-write | 4 KiB | 1 | 64 | 6,415 | 25.06 | 9.73 | 16.67 | 99.2% |
| AwanFS (--durability fast) | seq-write | 4 KiB | 2 | 64 | 8,361 | 32.66 | 7.21 | 19.94 | 135.0% |
| AwanFS (--durability fast) | seq-write | 4 KiB | 4 | 64 | 9,843 | 38.45 | 6.24 | 14.31 | 145.8% |
| AwanFS (--durability fast) | seq-write | 64 KiB | 1 | 64 | 2,649 | 165.55 | 23.38 | 39.54 | 98.7% |
| AwanFS (--durability fast) | seq-write | 64 KiB | 2 | 64 | 4,327 | 270.44 | 14.53 | 31.35 | 191.6% |
| AwanFS (--durability fast) | seq-write | 64 KiB | 4 | 64 | 4,494 | 280.88 | 13.59 | 34.93 | 211.4% |
| AwanFS (--durability fast) | seq-write | 1 MiB | 1 | 64 | 258 | 257.99 | 236.80 | 327.85 | 97.9% |
| AwanFS (--durability fast) | seq-write | 1 MiB | 2 | 64 | 479 | 478.86 | 130.90 | 243.54 | 194.0% |
| AwanFS (--durability fast) | seq-write | 1 MiB | 4 | 64 | 722 | 722.16 | 82.85 | 197.57 | 298.0% |
| HS5 | seq-write | 4 KiB | 1 | 64 | 1,792 | 7.00 | 34.97 | 46.60 | 56.9% |
| HS5 | seq-write | 4 KiB | 2 | 64 | 3,246 | 12.68 | 19.19 | 25.36 | 94.2% |
| HS5 | seq-write | 4 KiB | 4 | 64 | 4,417 | 17.26 | 14.01 | 19.75 | 111.2% |
| HS5 | seq-write | 64 KiB | 1 | 64 | 1,232 | 77.00 | 51.35 | 62.78 | 63.4% |
| HS5 | seq-write | 64 KiB | 2 | 64 | 2,318 | 144.85 | 26.84 | 34.16 | 108.3% |
| HS5 | seq-write | 64 KiB | 4 | 64 | 3,334 | 208.39 | 18.73 | 25.19 | 142.0% |
| HS5 | seq-write | 1 MiB | 1 | 64 | 365 | 364.79 | 168.23 | 332.42 | 85.6% |
| HS5 | seq-write | 1 MiB | 2 | 64 | 397 | 396.82 | 164.60 | 197.35 | 104.7% |
| HS5 | seq-write | 1 MiB | 4 | 64 | 403 | 402.67 | 156.09 | 303.41 | 109.3% |
| HS5 | seq-read | 4 KiB | 1 | 64 | 7,774 | 30.37 | 8.12 | 9.26 | 98.9% |
| HS5 | seq-read | 4 KiB | 2 | 64 | 7,059 | 27.58 | 8.48 | 24.54 | 103.8% |
| HS5 | seq-read | 4 KiB | 4 | 64 | 9,988 | 39.02 | 6.13 | 13.45 | 119.6% |
| HS5 | seq-read | 64 KiB | 1 | 64 | 6,354 | 397.10 | 9.97 | 10.71 | 99.3% |
| HS5 | seq-read | 64 KiB | 2 | 64 | 6,285 | 392.79 | 9.58 | 26.11 | 110.6% |
| HS5 | seq-read | 64 KiB | 4 | 64 | 8,497 | 531.04 | 7.27 | 15.50 | 126.4% |
| HS5 | seq-read | 1 MiB | 1 | 64 | 1,861 | 1861.18 | 33.91 | 37.15 | 99.6% |
| HS5 | seq-read | 1 MiB | 2 | 64 | 2,912 | 2912.23 | 20.92 | 38.41 | 193.9% |
| HS5 | seq-read | 1 MiB | 4 | 64 | 3,130 | 3130.47 | 19.52 | 37.25 | 215.3% |
| Garage | seq-write | 4 KiB | 1 | 64 | 977 | 3.82 | 65.13 | 85.21 | 99.4% |
| Garage | seq-write | 4 KiB | 2 | 64 | 965 | 3.77 | 65.56 | 84.12 | 150.9% |
| Garage | seq-write | 4 KiB | 4 | 64 | 1,167 | 4.56 | 54.31 | 69.25 | 194.5% |
| Garage | seq-write | 64 KiB | 1 | 64 | 550 | 34.36 | 114.96 | 161.63 | 99.6% |
| Garage | seq-write | 64 KiB | 2 | 64 | 586 | 36.65 | 107.67 | 143.74 | 157.3% |
| Garage | seq-write | 64 KiB | 4 | 64 | 810 | 50.60 | 78.47 | 115.50 | 221.8% |
| Garage | seq-write | 1 MiB | 1 | 64 | 130 | 129.86 | 485.42 | 913.23 | 99.5% |
| Garage | seq-write | 1 MiB | 2 | 64 | 192 | 191.74 | 328.60 | 566.80 | 189.6% |
| Garage | seq-write | 1 MiB | 4 | 64 | 274 | 274.35 | 229.41 | 397.55 | 286.7% |
| Garage | seq-read | 4 KiB | 1 | 64 | 1,533 | 5.99 | 40.99 | 45.96 | 93.0% |
| Garage | seq-read | 4 KiB | 2 | 64 | 1,537 | 6.00 | 40.99 | 43.49 | 131.1% |
| Garage | seq-read | 4 KiB | 4 | 64 | 1,532 | 5.98 | 41.05 | 43.43 | 152.6% |
| Garage | seq-read | 64 KiB | 1 | 64 | 982 | 61.37 | 63.90 | 94.74 | 99.4% |
| Garage | seq-read | 64 KiB | 2 | 64 | 1,154 | 72.15 | 54.93 | 63.68 | 181.9% |
| Garage | seq-read | 64 KiB | 4 | 64 | 1,289 | 80.56 | 49.24 | 60.48 | 253.8% |
| Garage | seq-read | 1 MiB | 1 | 64 | 240 | 240.18 | 254.86 | 407.36 | 99.5% |
| Garage | seq-read | 1 MiB | 2 | 64 | 325 | 325.36 | 194.10 | 227.33 | 183.1% |
| Garage | seq-read | 1 MiB | 4 | 64 | 428 | 428.23 | 147.50 | 178.34 | 270.4% |
| SeaweedFS | seq-write | 4 KiB | 1 | 64 | 1,518 | 5.93 | 40.59 | 75.09 | 99.3% |
| SeaweedFS | seq-write | 4 KiB | 2 | 64 | 2,083 | 8.14 | 29.81 | 52.95 | 197.6% |
| SeaweedFS | seq-write | 4 KiB | 4 | 64 | 3,061 | 11.96 | 20.31 | 35.08 | 306.9% |
| SeaweedFS | seq-write | 64 KiB | 1 | 64 | 784 | 48.98 | 78.56 | 155.15 | 99.5% |
| SeaweedFS | seq-write | 64 KiB | 2 | 64 | 1,185 | 74.07 | 52.06 | 91.41 | 197.8% |
| SeaweedFS | seq-write | 64 KiB | 4 | 64 | 1,862 | 116.37 | 32.61 | 67.96 | 332.4% |
| SeaweedFS | seq-write | 1 MiB | 1 | 64 | 115 | 114.56 | 546.12 | 720.01 | 99.5% |
| SeaweedFS | seq-write | 1 MiB | 2 | 64 | 189 | 188.88 | 272.22 | 1238.76 | 198.6% |
| SeaweedFS | seq-write | 1 MiB | 4 | 64 | 348 | 348.18 | 148.54 | 635.56 | 366.4% |
| SeaweedFS | seq-read | 4 KiB | 1 | 64 | 2,699 | 10.54 | 23.18 | 44.13 | 99.3% |
| SeaweedFS | seq-read | 4 KiB | 2 | 64 | 3,951 | 15.43 | 15.60 | 30.20 | 196.7% |
| SeaweedFS | seq-read | 4 KiB | 4 | 64 | 4,620 | 18.05 | 13.27 | 27.28 | 257.7% |
| SeaweedFS | seq-read | 64 KiB | 1 | 64 | 2,291 | 143.17 | 26.70 | 53.40 | 99.3% |
| SeaweedFS | seq-read | 64 KiB | 2 | 64 | 3,334 | 208.38 | 18.25 | 37.66 | 197.0% |
| SeaweedFS | seq-read | 64 KiB | 4 | 64 | 3,994 | 249.63 | 15.25 | 31.10 | 255.8% |
| SeaweedFS | seq-read | 1 MiB | 1 | 64 | 548 | 548.46 | 112.35 | 157.26 | 99.6% |
| SeaweedFS | seq-read | 1 MiB | 2 | 64 | 822 | 822.23 | 75.88 | 114.74 | 198.2% |
| SeaweedFS | seq-read | 1 MiB | 4 | 64 | 1,138 | 1137.97 | 54.95 | 91.76 | 294.5% |
| MinIO | seq-write | 4 KiB | 1 | 64 | 866 | 3.38 | 73.37 | 569.36 | 62.2% |
| MinIO | seq-write | 4 KiB | 2 | 64 | 1,562 | 6.10 | 36.28 | 93.93 | 151.2% |
| MinIO | seq-write | 4 KiB | 4 | 64 | 2,183 | 8.53 | 27.90 | 75.00 | 232.6% |
| MinIO | seq-write | 64 KiB | 1 | 64 | 478 | 29.87 | 126.87 | 1051.29 | 59.3% |
| MinIO | seq-write | 64 KiB | 2 | 64 | 803 | 50.16 | 51.82 | 1105.48 | 132.3% |
| MinIO | seq-write | 64 KiB | 4 | 64 | 1,295 | 80.94 | 40.34 | 276.26 | 223.5% |
| MinIO | seq-write | 1 MiB | 1 | 64 | 201 | 201.02 | 320.18 | 505.30 | 69.7% |
| MinIO | seq-write | 1 MiB | 2 | 64 | 322 | 321.70 | 184.98 | 473.06 | 131.9% |
| MinIO | seq-write | 1 MiB | 4 | 64 | 331 | 331.10 | 188.35 | 377.41 | 151.4% |
| MinIO | seq-read | 4 KiB | 1 | 64 | 3,017 | 11.79 | 20.77 | 41.33 | 100.0% |
| MinIO | seq-read | 4 KiB | 2 | 64 | 4,949 | 19.33 | 11.60 | 32.21 | 193.1% |
| MinIO | seq-read | 4 KiB | 4 | 64 | 5,123 | 20.01 | 10.58 | 38.17 | 223.1% |
| MinIO | seq-read | 64 KiB | 1 | 64 | 2,643 | 165.21 | 23.89 | 49.15 | 99.3% |
| MinIO | seq-read | 64 KiB | 2 | 64 | 4,304 | 268.99 | 13.62 | 35.11 | 196.6% |
| MinIO | seq-read | 64 KiB | 4 | 64 | 4,506 | 281.63 | 12.39 | 41.76 | 230.4% |
| MinIO | seq-read | 1 MiB | 1 | 64 | 923 | 922.95 | 7.95 | 487.26 | 99.7% |
| MinIO | seq-read | 1 MiB | 2 | 64 | 1,414 | 1414.17 | 19.66 | 195.09 | 198.1% |
| MinIO | seq-read | 1 MiB | 4 | 64 | 1,829 | 1828.78 | 34.12 | 90.77 | 273.3% |
| RustFS | seq-write | 4 KiB | 1 | 64 | 1,053 | 4.12 | 59.99 | 74.96 | 95.2% |
| RustFS | seq-write | 4 KiB | 2 | 64 | 1,570 | 6.13 | 39.93 | 48.86 | 178.8% |
| RustFS | seq-write | 4 KiB | 4 | 64 | 1,991 | 7.78 | 31.45 | 43.69 | 220.4% |
| RustFS | seq-write | 64 KiB | 1 | 64 | 667 | 41.67 | 94.33 | 136.80 | 91.8% |
| RustFS | seq-write | 64 KiB | 2 | 64 | 1,011 | 63.19 | 62.43 | 76.97 | 169.9% |
| RustFS | seq-write | 64 KiB | 4 | 64 | 1,371 | 85.67 | 46.11 | 65.65 | 222.6% |
| RustFS | seq-write | 1 MiB | 1 | 64 | 158 | 157.93 | 395.55 | 632.88 | 99.2% |
| RustFS | seq-write | 1 MiB | 2 | 64 | 251 | 250.64 | 241.98 | 424.51 | 196.4% |
| RustFS | seq-write | 1 MiB | 4 | 64 | 315 | 315.16 | 202.22 | 307.31 | 244.4% |
| RustFS | seq-read | 4 KiB | 1 | 64 | 3,661 | 14.30 | 16.82 | 32.80 | 99.5% |
| RustFS | seq-read | 4 KiB | 2 | 64 | 6,830 | 26.68 | 8.88 | 17.59 | 192.4% |
| RustFS | seq-read | 4 KiB | 4 | 64 | 6,833 | 26.69 | 9.17 | 16.23 | 211.1% |
| RustFS | seq-read | 64 KiB | 1 | 64 | 4,262 | 266.39 | 13.59 | 33.12 | 99.5% |
| RustFS | seq-read | 64 KiB | 2 | 64 | 6,662 | 416.39 | 8.72 | 25.32 | 158.2% |
| RustFS | seq-read | 64 KiB | 4 | 64 | 7,234 | 452.10 | 8.62 | 16.95 | 177.6% |
| RustFS | seq-read | 1 MiB | 1 | 64 | 342 | 341.95 | 183.18 | 304.97 | 99.7% |
| RustFS | seq-read | 1 MiB | 2 | 64 | 550 | 550.01 | 115.93 | 190.76 | 198.6% |
| RustFS | seq-read | 1 MiB | 4 | 64 | 900 | 900.37 | 69.94 | 116.65 | 340.4% |

## Client CPU Usage (`CPU %` where 100% = 1 core busy, against client cores given)

Client cap check: 12 of 117 measured phases were client-capped: AwanFS seq-write 4 KiB 2c c=64 (yes (saturated client cores)), AwanFS seq-read 4 KiB 2c c=64 (yes (saturated client cores)), AwanFS seq-read 4 KiB 4c c=64 (yes (shared cores, client-heavy)), AwanFS seq-read 64 KiB 2c c=64 (yes (saturated client cores)), AwanFS seq-read 64 KiB 4c c=64 (yes (shared cores, client-heavy)), AwanFS (--durability fast) seq-write 4 KiB 2c c=64 (yes (saturated client cores)), AwanFS (--durability fast) seq-write 4 KiB 4c c=64 (yes (shared cores, client-heavy)), HS5 seq-read 4 KiB 2c c=64 (yes (saturated client cores)) (+4 more).

| Server | Pattern | Size | Cores | Concurrency | Client Procs | Client Cores | Client CPU % | Client Core Util | Client-Capped? |
|---|---|---|---|---|---|---|---|---|---|
| AwanFS | seq-write | 4 KiB | 1 | 64 | 8 | 3 | 118.9% | 39.6% | no |
| AwanFS | seq-write | 4 KiB | 2 | 64 | 8 | 2 | 198.2% | 99.1% | yes (saturated client cores) |
| AwanFS | seq-write | 4 KiB | 4 | 64 | 8 | 4 (shared) | 201.1% | 50.3% | no |
| AwanFS | seq-write | 64 KiB | 1 | 64 | 8 | 3 | 52.4% | 17.5% | no |
| AwanFS | seq-write | 64 KiB | 2 | 64 | 8 | 2 | 89.8% | 44.9% | no |
| AwanFS | seq-write | 64 KiB | 4 | 64 | 8 | 4 (shared) | 112.5% | 28.1% | no |
| AwanFS | seq-write | 1 MiB | 1 | 64 | 8 | 3 | 15.3% | 5.1% | no |
| AwanFS | seq-write | 1 MiB | 2 | 64 | 8 | 2 | 25.8% | 12.9% | no |
| AwanFS | seq-write | 1 MiB | 4 | 64 | 8 | 4 (shared) | 28.7% | 7.2% | no |
| AwanFS | seq-read | 4 KiB | 1 | 64 | 8 | 3 | 240.1% | 80.0% | no (server also >=85%) |
| AwanFS | seq-read | 4 KiB | 2 | 64 | 8 | 2 | 199.8% | 99.9% | yes (saturated client cores) |
| AwanFS | seq-read | 4 KiB | 4 | 64 | 8 | 4 (shared) | 289.4% | 72.4% | yes (shared cores, client-heavy) |
| AwanFS | seq-read | 64 KiB | 1 | 64 | 8 | 3 | 225.2% | 75.1% | no |
| AwanFS | seq-read | 64 KiB | 2 | 64 | 8 | 2 | 200.1% | 100.0% | yes (saturated client cores) |
| AwanFS | seq-read | 64 KiB | 4 | 64 | 8 | 4 (shared) | 285.7% | 71.4% | yes (shared cores, client-heavy) |
| AwanFS | seq-read | 1 MiB | 1 | 64 | 8 | 3 | 109.0% | 36.3% | no |
| AwanFS | seq-read | 1 MiB | 2 | 64 | 8 | 2 | 190.5% | 95.3% | no (server also >=85%) |
| AwanFS | seq-read | 1 MiB | 4 | 64 | 8 | 4 (shared) | 187.9% | 47.0% | no |
| AwanFS (--durability fast) | seq-write | 4 KiB | 1 | 64 | 8 | 3 | 157.3% | 52.4% | no |
| AwanFS (--durability fast) | seq-write | 4 KiB | 2 | 64 | 8 | 2 | 199.2% | 99.6% | yes (saturated client cores) |
| AwanFS (--durability fast) | seq-write | 4 KiB | 4 | 64 | 8 | 4 (shared) | 237.9% | 59.5% | yes (shared cores, client-heavy) |
| AwanFS (--durability fast) | seq-write | 64 KiB | 1 | 64 | 8 | 3 | 60.3% | 20.1% | no |
| AwanFS (--durability fast) | seq-write | 64 KiB | 2 | 64 | 8 | 2 | 104.6% | 52.3% | no |
| AwanFS (--durability fast) | seq-write | 64 KiB | 4 | 64 | 8 | 4 (shared) | 118.4% | 29.6% | no |
| AwanFS (--durability fast) | seq-write | 1 MiB | 1 | 64 | 8 | 3 | 16.2% | 5.4% | no |
| AwanFS (--durability fast) | seq-write | 1 MiB | 2 | 64 | 8 | 2 | 28.7% | 14.3% | no |
| AwanFS (--durability fast) | seq-write | 1 MiB | 4 | 64 | 8 | 4 (shared) | 47.6% | 11.9% | no |
| HS5 | seq-write | 4 KiB | 1 | 64 | 8 | 3 | 50.2% | 16.7% | no |
| HS5 | seq-write | 4 KiB | 2 | 64 | 8 | 2 | 90.4% | 45.2% | no |
| HS5 | seq-write | 4 KiB | 4 | 64 | 8 | 4 (shared) | 131.8% | 32.9% | no |
| HS5 | seq-write | 64 KiB | 1 | 64 | 8 | 3 | 39.0% | 13.0% | no |
| HS5 | seq-write | 64 KiB | 2 | 64 | 8 | 2 | 67.5% | 33.8% | no |
| HS5 | seq-write | 64 KiB | 4 | 64 | 8 | 4 (shared) | 108.7% | 27.2% | no |
| HS5 | seq-write | 1 MiB | 1 | 64 | 8 | 3 | 24.5% | 8.2% | no |
| HS5 | seq-write | 1 MiB | 2 | 64 | 8 | 2 | 25.2% | 12.6% | no |
| HS5 | seq-write | 1 MiB | 4 | 64 | 8 | 4 (shared) | 28.5% | 7.1% | no |
| HS5 | seq-read | 4 KiB | 1 | 64 | 8 | 3 | 211.2% | 70.4% | no |
| HS5 | seq-read | 4 KiB | 2 | 64 | 8 | 2 | 199.6% | 99.8% | yes (saturated client cores) |
| HS5 | seq-read | 4 KiB | 4 | 64 | 8 | 4 (shared) | 276.8% | 69.2% | yes (shared cores, client-heavy) |
| HS5 | seq-read | 64 KiB | 1 | 64 | 8 | 3 | 199.8% | 66.6% | no |
| HS5 | seq-read | 64 KiB | 2 | 64 | 8 | 2 | 199.6% | 99.8% | yes (saturated client cores) |
| HS5 | seq-read | 64 KiB | 4 | 64 | 8 | 4 (shared) | 269.5% | 67.4% | yes (shared cores, client-heavy) |
| HS5 | seq-read | 1 MiB | 1 | 64 | 8 | 3 | 98.2% | 32.7% | no |
| HS5 | seq-read | 1 MiB | 2 | 64 | 8 | 2 | 163.6% | 81.8% | no (server also >=85%) |
| HS5 | seq-read | 1 MiB | 4 | 64 | 8 | 4 (shared) | 182.0% | 45.5% | no |
| Garage | seq-write | 4 KiB | 1 | 64 | 8 | 3 | 31.4% | 10.5% | no |
| Garage | seq-write | 4 KiB | 2 | 64 | 8 | 2 | 28.3% | 14.2% | no |
| Garage | seq-write | 4 KiB | 4 | 64 | 8 | 4 (shared) | 44.5% | 11.1% | no |
| Garage | seq-write | 64 KiB | 1 | 64 | 8 | 3 | 19.5% | 6.5% | no |
| Garage | seq-write | 64 KiB | 2 | 64 | 8 | 2 | 19.4% | 9.7% | no |
| Garage | seq-write | 64 KiB | 4 | 64 | 8 | 4 (shared) | 34.2% | 8.6% | no |
| Garage | seq-write | 1 MiB | 1 | 64 | 8 | 3 | 9.8% | 3.3% | no |
| Garage | seq-write | 1 MiB | 2 | 64 | 8 | 2 | 13.5% | 6.8% | no |
| Garage | seq-write | 1 MiB | 4 | 64 | 8 | 4 (shared) | 22.0% | 5.5% | no |
| Garage | seq-read | 4 KiB | 1 | 64 | 8 | 3 | 45.9% | 15.3% | no |
| Garage | seq-read | 4 KiB | 2 | 64 | 8 | 2 | 46.0% | 23.0% | no |
| Garage | seq-read | 4 KiB | 4 | 64 | 8 | 4 (shared) | 55.6% | 13.9% | no |
| Garage | seq-read | 64 KiB | 1 | 64 | 8 | 3 | 32.8% | 10.9% | no |
| Garage | seq-read | 64 KiB | 2 | 64 | 8 | 2 | 35.5% | 17.8% | no |
| Garage | seq-read | 64 KiB | 4 | 64 | 8 | 4 (shared) | 53.0% | 13.3% | no |
| Garage | seq-read | 1 MiB | 1 | 64 | 8 | 3 | 23.5% | 7.8% | no |
| Garage | seq-read | 1 MiB | 2 | 64 | 8 | 2 | 30.4% | 15.2% | no |
| Garage | seq-read | 1 MiB | 4 | 64 | 8 | 4 (shared) | 46.1% | 11.5% | no |
| SeaweedFS | seq-write | 4 KiB | 1 | 64 | 8 | 3 | 44.9% | 15.0% | no |
| SeaweedFS | seq-write | 4 KiB | 2 | 64 | 8 | 2 | 55.8% | 27.9% | no |
| SeaweedFS | seq-write | 4 KiB | 4 | 64 | 8 | 4 (shared) | 87.4% | 21.9% | no |
| SeaweedFS | seq-write | 64 KiB | 1 | 64 | 8 | 3 | 27.9% | 9.3% | no |
| SeaweedFS | seq-write | 64 KiB | 2 | 64 | 8 | 2 | 36.4% | 18.2% | no |
| SeaweedFS | seq-write | 64 KiB | 4 | 64 | 8 | 4 (shared) | 61.4% | 15.4% | no |
| SeaweedFS | seq-write | 1 MiB | 1 | 64 | 8 | 3 | 9.3% | 3.1% | no |
| SeaweedFS | seq-write | 1 MiB | 2 | 64 | 8 | 2 | 13.9% | 7.0% | no |
| SeaweedFS | seq-write | 1 MiB | 4 | 64 | 8 | 4 (shared) | 24.3% | 6.1% | no |
| SeaweedFS | seq-read | 4 KiB | 1 | 64 | 8 | 3 | 74.8% | 24.9% | no |
| SeaweedFS | seq-read | 4 KiB | 2 | 64 | 8 | 2 | 101.7% | 50.8% | no |
| SeaweedFS | seq-read | 4 KiB | 4 | 64 | 8 | 4 (shared) | 134.2% | 33.5% | no |
| SeaweedFS | seq-read | 64 KiB | 1 | 64 | 8 | 3 | 71.8% | 23.9% | no |
| SeaweedFS | seq-read | 64 KiB | 2 | 64 | 8 | 2 | 98.6% | 49.3% | no |
| SeaweedFS | seq-read | 64 KiB | 4 | 64 | 8 | 4 (shared) | 131.8% | 32.9% | no |
| SeaweedFS | seq-read | 1 MiB | 1 | 64 | 8 | 3 | 39.0% | 13.0% | no |
| SeaweedFS | seq-read | 1 MiB | 2 | 64 | 8 | 2 | 52.9% | 26.5% | no |
| SeaweedFS | seq-read | 1 MiB | 4 | 64 | 8 | 4 (shared) | 83.0% | 20.8% | no |
| MinIO | seq-write | 4 KiB | 1 | 64 | 8 | 3 | 26.7% | 8.9% | no |
| MinIO | seq-write | 4 KiB | 2 | 64 | 8 | 2 | 44.8% | 22.4% | no |
| MinIO | seq-write | 4 KiB | 4 | 64 | 8 | 4 (shared) | 76.9% | 19.2% | no |
| MinIO | seq-write | 64 KiB | 1 | 64 | 8 | 3 | 17.9% | 6.0% | no |
| MinIO | seq-write | 64 KiB | 2 | 64 | 8 | 2 | 27.4% | 13.7% | no |
| MinIO | seq-write | 64 KiB | 4 | 64 | 8 | 4 (shared) | 51.4% | 12.8% | no |
| MinIO | seq-write | 1 MiB | 1 | 64 | 8 | 3 | 14.1% | 4.7% | no |
| MinIO | seq-write | 1 MiB | 2 | 64 | 8 | 2 | 22.4% | 11.2% | no |
| MinIO | seq-write | 1 MiB | 4 | 64 | 8 | 4 (shared) | 27.3% | 6.8% | no |
| MinIO | seq-read | 4 KiB | 1 | 64 | 8 | 3 | 85.2% | 28.4% | no |
| MinIO | seq-read | 4 KiB | 2 | 64 | 8 | 2 | 143.7% | 71.8% | no |
| MinIO | seq-read | 4 KiB | 4 | 64 | 8 | 4 (shared) | 165.4% | 41.4% | no |
| MinIO | seq-read | 64 KiB | 1 | 64 | 8 | 3 | 83.1% | 27.7% | no |
| MinIO | seq-read | 64 KiB | 2 | 64 | 8 | 2 | 135.9% | 68.0% | no |
| MinIO | seq-read | 64 KiB | 4 | 64 | 8 | 4 (shared) | 159.9% | 40.0% | no |
| MinIO | seq-read | 1 MiB | 1 | 64 | 8 | 3 | 48.8% | 16.3% | no |
| MinIO | seq-read | 1 MiB | 2 | 64 | 8 | 2 | 79.1% | 39.5% | no |
| MinIO | seq-read | 1 MiB | 4 | 64 | 8 | 4 (shared) | 115.4% | 28.8% | no |
| RustFS | seq-write | 4 KiB | 1 | 64 | 8 | 3 | 31.5% | 10.5% | no |
| RustFS | seq-write | 4 KiB | 2 | 64 | 8 | 2 | 41.1% | 20.5% | no |
| RustFS | seq-write | 4 KiB | 4 | 64 | 8 | 4 (shared) | 65.2% | 16.3% | no |
| RustFS | seq-write | 64 KiB | 1 | 64 | 8 | 3 | 23.2% | 7.7% | no |
| RustFS | seq-write | 64 KiB | 2 | 64 | 8 | 2 | 32.0% | 16.0% | no |
| RustFS | seq-write | 64 KiB | 4 | 64 | 8 | 4 (shared) | 50.6% | 12.6% | no |
| RustFS | seq-write | 1 MiB | 1 | 64 | 8 | 3 | 11.4% | 3.8% | no |
| RustFS | seq-write | 1 MiB | 2 | 64 | 8 | 2 | 16.3% | 8.2% | no |
| RustFS | seq-write | 1 MiB | 4 | 64 | 8 | 4 (shared) | 21.7% | 5.4% | no |
| RustFS | seq-read | 4 KiB | 1 | 64 | 8 | 3 | 91.1% | 30.4% | no |
| RustFS | seq-read | 4 KiB | 2 | 64 | 8 | 2 | 170.8% | 85.4% | no (server also >=85%) |
| RustFS | seq-read | 4 KiB | 4 | 64 | 8 | 4 (shared) | 185.6% | 46.4% | no |
| RustFS | seq-read | 64 KiB | 1 | 64 | 8 | 3 | 113.0% | 37.7% | no |
| RustFS | seq-read | 64 KiB | 2 | 64 | 8 | 2 | 196.2% | 98.1% | yes (saturated client cores) |
| RustFS | seq-read | 64 KiB | 4 | 64 | 8 | 4 (shared) | 218.0% | 54.5% | no |
| RustFS | seq-read | 1 MiB | 1 | 64 | 8 | 3 | 22.4% | 7.5% | no |
| RustFS | seq-read | 1 MiB | 2 | 64 | 8 | 2 | 32.5% | 16.3% | no |
| RustFS | seq-read | 1 MiB | 4 | 64 | 8 | 4 (shared) | 55.1% | 13.8% | no |

## AwanFS work per phase (`/_awanfs/metrics`, before and after each phase)

Syncs and disk bytes are the server's own counters; rates use the phase's wall time. PUTs per sync is ops/s over syncs/s.

| Durability | Pattern | Size | Cores | Concurrency | ops/s | syncs/s | PUTs per sync | MiB per sync | disk MiB/s | peak in-flight MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| group | seq-write | 4 KiB | 1 | 64 | 4,648 | 2312.2 | 2.0 | 0.01 | 31.8 | 3.0 |
| group | seq-write | 4 KiB | 2 | 64 | 8,623 | 1759.0 | 4.9 | 0.03 | 54.8 | 3.0 |
| group | seq-write | 4 KiB | 4 | 64 | 7,912 | 1313.0 | 6.0 | 0.04 | 54.6 | 3.0 |
| group | seq-write | 64 KiB | 1 | 64 | 1,870 | 1487.2 | 1.3 | 0.07 | 108.5 | 3.9 |
| group | seq-write | 64 KiB | 2 | 64 | 3,338 | 1266.0 | 2.6 | 0.14 | 180.7 | 4.0 |
| group | seq-write | 64 KiB | 4 | 64 | 3,949 | 1423.2 | 2.8 | 0.16 | 233.4 | 4.0 |
| group | seq-write | 1 MiB | 1 | 64 | 224 | 390.3 | 0.6 | 0.51 | 197.1 | 64.0 |
| group | seq-write | 1 MiB | 2 | 64 | 400 | 169.4 | 2.4 | 1.93 | 327.4 | 64.0 |
| group | seq-write | 1 MiB | 4 | 64 | 408 | 62.9 | 6.4 | 5.72 | 365.6 | 64.0 |
| group | seq-read | 4 KiB | 1 | 64 | 9,984 | 0.0 | no syncs | no syncs | 0.0 | 64.0 |
| group | seq-read | 4 KiB | 2 | 64 | 8,120 | 0.0 | no syncs | no syncs | 0.0 | 64.0 |
| group | seq-read | 4 KiB | 4 | 64 | 11,857 | 0.0 | no syncs | no syncs | 0.0 | 64.0 |
| group | seq-read | 64 KiB | 1 | 64 | 8,382 | 0.0 | no syncs | no syncs | 0.0 | 64.0 |
| group | seq-read | 64 KiB | 2 | 64 | 7,037 | 0.0 | no syncs | no syncs | 0.0 | 64.0 |
| group | seq-read | 64 KiB | 4 | 64 | 10,185 | 0.0 | no syncs | no syncs | 0.0 | 64.0 |
| group | seq-read | 1 MiB | 1 | 64 | 2,339 | 0.0 | no syncs | no syncs | 0.0 | 64.0 |
| group | seq-read | 1 MiB | 2 | 64 | 3,795 | 0.0 | no syncs | no syncs | 0.0 | 64.0 |
| group | seq-read | 1 MiB | 4 | 64 | 3,749 | 0.0 | no syncs | no syncs | 0.0 | 64.0 |
| fast | seq-write | 4 KiB | 1 | 64 | 6,415 | 1.7 | 3718.7 | 25.36 | 43.7 | 0.0 |
| fast | seq-write | 4 KiB | 2 | 64 | 8,361 | 1.6 | 5302.5 | 33.57 | 52.8 | 0.0 |
| fast | seq-write | 4 KiB | 4 | 64 | 9,843 | 1.7 | 5635.7 | 38.98 | 68.1 | 0.0 |
| fast | seq-write | 64 KiB | 1 | 64 | 2,649 | 1.7 | 1531.8 | 88.96 | 153.8 | 3.1 |
| fast | seq-write | 64 KiB | 2 | 64 | 4,327 | 1.3 | 3419.1 | 182.68 | 231.2 | 1.5 |
| fast | seq-write | 64 KiB | 4 | 64 | 4,494 | 1.4 | 3199.9 | 188.97 | 265.1 | 3.4 |
| fast | seq-write | 1 MiB | 1 | 64 | 258 | 1.3 | 192.4 | 169.53 | 227.3 | 64.0 |
| fast | seq-write | 1 MiB | 2 | 64 | 479 | 0.9 | 512.2 | 415.62 | 389.4 | 63.0 |
| fast | seq-write | 1 MiB | 4 | 64 | 722 | 0.3 | 2098.6 | 1867.27 | 642.6 | 64.0 |

