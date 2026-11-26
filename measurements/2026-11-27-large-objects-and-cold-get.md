`gh api -X POST repos/iskandarnorazli/Awanfs/actions/workflows/bench-scaling.yml/dispatches -f ref=try/cold-large -f inputs[cores]=2,4 -f inputs[duration]=5 -f inputs[runs]=2 -F inputs[matrix]=true -f inputs[patterns]=seq-write,seq-read -f inputs[sizes]=1MiB,8MiB,16MiB -f inputs[concurrencies]=16,64 -f inputs[client_procs]=8` (run 45875072516, job "Core scaling (ubuntu-latest)" 140099986822, branch `try/cold-large` at `6fe8c50`, not merged: `main` at `de1959c` plus HS5 and the cold GET mode in `core_scaling.py`), which ran `python benchmarks/core_scaling.py --cores 2,4 --runs 2 --duration 5 --client-procs 8 --data-dir /mnt/bench --output <runner temp>/core_scaling/report.md --compare-fast --matrix --pattern seq-write,seq-read --sizes 1MiB,8MiB,16MiB --concurrencies 16,64 --seed 42 --servers AwanFS,HS5,Garage,SeaweedFS,MinIO,RustFS`, then `python benchmarks/core_scaling.py --cold-get --sizes 1MiB,8MiB,16MiB --servers AwanFS,HS5,Garage,SeaweedFS,MinIO,RustFS --cold-set 1GiB --cold-concurrency 16 --cold-cores 2 --compare-fast --data-dir /mnt/bench --output <runner temp>/core_scaling/cold_get.md`

# Large objects (1, 8, 16 MiB) and cold GET, once, on the hosted Linux runner (#452)

An experiment (owner decision on [#452](https://github.com/iskandarnorazli/Awanfs/issues/452#issuecomment-7200191144)): run once, on an unmerged branch. HS5 1.5.0 and the other servers at their defaults, as in `measurements/2026-11-27-hs5-experiment.md`. Data on `/mnt/bench`, which is on the same disk as `/` on this runner (`df`: `/dev/root`, 145G, 86G free). Each matrix point is the median of 2 rounds of 5 s. The runner has 4 logical cores, so at 4 server cores the client shares them.

## Why AwanFS's PUT falls with size

| PUT, 2 server cores, c=16 | AwanFS | AwanFS fast | HS5 | MinIO | RustFS | Garage | SeaweedFS |
|---|---|---|---|---|---|---|---|
| 1 MiB, ops/s (server CPU) | 308 (196%) | 322 (193%) | 401 (103%) | 366 (136%) | 266 (196%) | 198 (190%) | 188 (195%) |
| 8 MiB | 50 (154%) | 62 (183%) | 51 (93%) | 49 (112%) | 47 (195%) | 27 (193%) | 24 (198%) |
| 16 MiB | 25 (147%) | 33 (185%) | 25 (94%) | 25 (108%) | 24 (188%) | 14 (192%) | 13 (199%) |

- **At 8 and 16 MiB the durable servers meet at the disk:** AwanFS, HS5, MinIO and RustFS all write about 400 MiB/s. The cold reads below show the same ceiling.
- **At 1 MiB, on 2 cores, AwanFS runs out of CPU.** It uses 196% of 2 cores for 308 ops/s, about 6.4 ms of CPU per MiB. HS5 needs 103% for 401 ops/s, about 2.6 ms per MiB.
  - `--durability fast` does not change it (322 ops/s, 193%), so waiting for syncs is not the cost.
  - Group commit already covers about 1.4 PUTs per sync at 1 MiB, computed from the counters below (308 PUTs over 427.2 syncs/s, which count the volume and journal syncs separately).
- **At 4 cores AwanFS has spare CPU,** and it is level with HS5 at 1 MiB (403 against 407).

So the cost is CPU spent per request on each body. Hashing or copying is inferred, not measured: this run did not sample threads on Linux.

## Cold and warm GET

AwanFS's cold 1 MiB row is not valid. It read 1,480 MiB/s, three times what every server read cold from this disk (about 430 to 460 MiB/s), and close to its own warm rate. So its 1 MiB objects were most likely in memory before the cold pass; the cause is not measured. RustFS answered HTTP 503 after its restart, so its GETs are not measured. At 8 and 16 MiB, AwanFS, HS5, MinIO and SeaweedFS read cold at the disk's rate (about 430 to 460 MiB/s). Garage read about 300 MiB/s, both cold and warm.

- **Machine:** runnervm0hi8g (Linux-7.0.0-1020-azure-x86_64-with-glibc2.43, 4 logical cores, 15.6 GB RAM, CPU: AMD EPYC 7763 64-Core Processor)
- **Method:** per server and size, 1024 MiB of objects is written with 16 connections (the PUT column times that write). The server is stopped, `sync` and `echo 3 > /proc/sys/vm/drop_caches` empty the page cache, and the server is started again on the same data, so its own caches are empty too. Each object is then read once (cold: from disk), then once more (warm: from the page cache, or the server's own cache). Server pinned to 2 cores, client to [2, 3] (shares cores: False). Every GET checks the body length.

| Server | Size | Objects | PUT (writing the set) | Cold GET | Cold P50 / P99 ms | Warm GET | Warm P50 / P99 ms |
|---|---|---|---|---|---|---|---|
| AwanFS | 1 MiB | 1024 | 309 ops/s, 308.9 MiB/s | 1,480 ops/s, 1,479.9 MiB/s | 10.56 / 18.43 | 1,597 ops/s, 1,596.6 MiB/s | 9.91 / 15.36 |
| HS5 | 1 MiB | 1024 | 419 ops/s, 419.3 MiB/s | 443 ops/s, 443.0 MiB/s | 44.62 / 63.30 | 2,846 ops/s, 2,846.2 MiB/s | 5.04 / 12.34 |
| Garage | 1 MiB | 1024 | 199 ops/s, 199.2 MiB/s | 287 ops/s, 286.7 MiB/s | 55.04 / 88.55 | 289 ops/s, 289.5 MiB/s | 54.82 / 88.24 |
| SeaweedFS | 1 MiB | 1024 | 191 ops/s, 191.0 MiB/s | 439 ops/s, 439.2 MiB/s | 40.05 / 68.81 | 1,217 ops/s, 1,217.3 MiB/s | 12.66 / 22.99 |
| MinIO | 1 MiB | 1024 | 383 ops/s, 383.5 MiB/s | 429 ops/s, 429.4 MiB/s | 40.08 / 72.31 | 1,351 ops/s, 1,350.9 MiB/s | 10.24 / 34.06 |
| RustFS | 1 MiB | 1024 | 268 ops/s, 268.4 MiB/s | not measured (GET cold/1048576/001021: HTTP 503, 45 bytes) |  | not measured (GET cold/1048576/001022: HTTP 503, 45 bytes) |  |
| AwanFS | 8 MiB | 128 | 53 ops/s, 422.3 MiB/s | 57 ops/s, 458.6 MiB/s | 265.31 / 374.33 | 215 ops/s, 1,723.2 MiB/s | 60.86 / 243.02 |
| HS5 | 8 MiB | 128 | 54 ops/s, 434.5 MiB/s | 54 ops/s, 432.2 MiB/s | 296.35 / 507.56 | 230 ops/s, 1,840.7 MiB/s | 53.23 / 175.71 |
| Garage | 8 MiB | 128 | 27 ops/s, 218.1 MiB/s | 38 ops/s, 303.0 MiB/s | 417.03 / 478.37 | 38 ops/s, 305.2 MiB/s | 410.22 / 498.93 |
| SeaweedFS | 8 MiB | 128 | 24 ops/s, 193.2 MiB/s | 54 ops/s, 433.7 MiB/s | 299.20 / 442.26 | 170 ops/s, 1,363.0 MiB/s | 88.59 / 167.14 |
| MinIO | 8 MiB | 128 | 53 ops/s, 421.0 MiB/s | 55 ops/s, 440.3 MiB/s | 300.41 / 450.58 | 226 ops/s, 1,805.1 MiB/s | 59.50 / 181.46 |
| RustFS | 8 MiB | 128 | 48 ops/s, 387.6 MiB/s | not measured (GET cold/8388608/000126: HTTP 503, 45 bytes) |  | not measured (GET cold/8388608/000127: HTTP 503, 45 bytes) |  |
| AwanFS | 16 MiB | 64 | 26 ops/s, 414.9 MiB/s | 27 ops/s, 435.2 MiB/s | 589.60 / 737.36 | 106 ops/s, 1,701.0 MiB/s | 117.71 / 470.39 |
| HS5 | 16 MiB | 64 | 25 ops/s, 403.4 MiB/s | 27 ops/s, 433.8 MiB/s | 591.27 / 1042.95 | 104 ops/s, 1,664.3 MiB/s | 134.49 / 339.54 |
| Garage | 16 MiB | 64 | 14 ops/s, 220.5 MiB/s | 19 ops/s, 308.9 MiB/s | 813.95 / 912.17 | 20 ops/s, 312.8 MiB/s | 797.97 / 890.42 |
| SeaweedFS | 16 MiB | 64 | 12 ops/s, 185.0 MiB/s | 27 ops/s, 433.9 MiB/s | 546.44 / 848.77 | 86 ops/s, 1,378.5 MiB/s | 154.76 / 371.13 |
| MinIO | 16 MiB | 64 | 26 ops/s, 411.6 MiB/s | 27 ops/s, 435.8 MiB/s | 595.95 / 801.39 | 106 ops/s, 1,697.1 MiB/s | 125.89 / 340.37 |
| RustFS | 16 MiB | 64 | 24 ops/s, 387.7 MiB/s | not measured (GET cold/16777216/000048: HTTP 503, 45 bytes) |  | not measured (GET cold/16777216/000063: HTTP 503, 45 bytes) |  |

##[group]Run mkdir -p "$RUNNER_TEMP/core_scaling"
[36;1mmkdir -p "$RUNNER_TEMP/core_scaling"[0m

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
| AwanFS | seq-write | 1 MiB | 2 | 16 | 308 | 307.70 | 49.68 | 89.65 | 196.1% |
| AwanFS | seq-write | 1 MiB | 2 | 64 | 307 | 307.02 | 206.11 | 321.54 | 194.6% |
| AwanFS | seq-write | 1 MiB | 4 | 16 | 403 | 403.13 | 40.97 | 68.49 | 264.4% |
| AwanFS | seq-write | 1 MiB | 4 | 64 | 403 | 402.99 | 149.10 | 340.34 | 266.8% |
| AwanFS | seq-write | 8 MiB | 2 | 16 | 50 | 403.45 | 303.78 | 575.11 | 154.1% |
| AwanFS | seq-write | 8 MiB | 2 | 64 | 48 | 382.99 | 1255.09 | 2438.77 | 149.0% |
| AwanFS | seq-write | 8 MiB | 4 | 16 | 51 | 407.75 | 304.54 | 556.00 | 159.4% |
| AwanFS | seq-write | 8 MiB | 4 | 64 | 50 | 402.56 | 1109.11 | 2644.53 | 162.2% |
| AwanFS | seq-write | 16 MiB | 2 | 16 | 25 | 400.56 | 675.03 | 1020.88 | 146.6% |
| AwanFS | seq-write | 16 MiB | 2 | 64 | 21 | 340.28 | 3170.66 | 3848.87 | 124.1% |
| AwanFS | seq-write | 16 MiB | 4 | 16 | 25 | 406.93 | 646.35 | 974.59 | 152.7% |
| AwanFS | seq-write | 16 MiB | 4 | 64 | 25 | 393.89 | 2393.10 | 4916.00 | 149.5% |
| AwanFS | seq-read | 1 MiB | 2 | 16 | 3,631 | 3631.27 | 4.22 | 10.74 | 187.1% |
| AwanFS | seq-read | 1 MiB | 2 | 64 | 3,704 | 3703.54 | 16.79 | 32.84 | 190.3% |
| AwanFS | seq-read | 1 MiB | 4 | 16 | 3,348 | 3348.24 | 4.49 | 10.46 | 187.3% |
| AwanFS | seq-read | 1 MiB | 4 | 64 | 3,486 | 3485.66 | 18.09 | 36.02 | 196.9% |
| AwanFS | seq-read | 8 MiB | 2 | 16 | 390 | 3117.62 | 37.19 | 109.15 | 183.7% |
| AwanFS | seq-read | 8 MiB | 2 | 64 | 346 | 2769.31 | 152.86 | 593.10 | 166.0% |
| AwanFS | seq-read | 8 MiB | 4 | 16 | 417 | 3335.29 | 36.63 | 75.85 | 215.9% |
| AwanFS | seq-read | 8 MiB | 4 | 64 | 380 | 3039.72 | 156.07 | 385.96 | 201.6% |
| AwanFS | seq-read | 16 MiB | 2 | 16 | 200 | 3195.28 | 71.26 | 211.54 | 178.6% |
| AwanFS | seq-read | 16 MiB | 2 | 64 | 166 | 2651.78 | 313.68 | 1144.73 | 146.6% |
| AwanFS | seq-read | 16 MiB | 4 | 16 | 216 | 3453.96 | 69.16 | 154.68 | 215.1% |
| AwanFS | seq-read | 16 MiB | 4 | 64 | 191 | 3050.57 | 302.19 | 730.23 | 186.9% |
| AwanFS (--durability fast) | seq-write | 1 MiB | 2 | 16 | 322 | 322.49 | 49.59 | 92.78 | 193.1% |
| AwanFS (--durability fast) | seq-write | 1 MiB | 2 | 64 | 321 | 321.17 | 203.35 | 368.00 | 193.2% |
| AwanFS (--durability fast) | seq-write | 1 MiB | 4 | 64 | 443 | 442.70 | 124.98 | 718.55 | 277.3% |
| AwanFS (--durability fast) | seq-write | 8 MiB | 2 | 16 | 62 | 496.14 | 245.28 | 737.90 | 182.7% |
| AwanFS (--durability fast) | seq-write | 8 MiB | 2 | 64 | 64 | 513.05 | 1001.57 | 1411.36 | 194.3% |
| AwanFS (--durability fast) | seq-write | 8 MiB | 4 | 64 | 81 | 644.11 | 588.90 | 2425.32 | 256.2% |
| AwanFS (--durability fast) | seq-write | 16 MiB | 2 | 16 | 33 | 520.63 | 473.47 | 1077.80 | 185.3% |
| AwanFS (--durability fast) | seq-write | 16 MiB | 2 | 64 | 33 | 529.20 | 1862.41 | 2637.40 | 194.4% |
| AwanFS (--durability fast) | seq-write | 16 MiB | 4 | 64 | 28 | 450.20 | 1982.23 | 4028.49 | 175.6% |
| HS5 | seq-write | 1 MiB | 2 | 16 | 401 | 400.73 | 41.00 | 76.34 | 102.6% |
| HS5 | seq-write | 1 MiB | 2 | 64 | 356 | 356.26 | 168.15 | 266.64 | 102.5% |
| HS5 | seq-write | 1 MiB | 4 | 16 | 407 | 407.35 | 43.00 | 85.31 | 107.9% |
| HS5 | seq-write | 1 MiB | 4 | 64 | 404 | 403.97 | 156.22 | 306.80 | 120.8% |
| HS5 | seq-write | 8 MiB | 2 | 16 | 51 | 405.40 | 307.59 | 456.29 | 93.3% |
| HS5 | seq-write | 8 MiB | 2 | 64 | 50 | 403.60 | 1268.03 | 1521.96 | 107.4% |
| HS5 | seq-write | 8 MiB | 4 | 16 | 51 | 407.49 | 304.18 | 522.47 | 93.7% |
| HS5 | seq-write | 8 MiB | 4 | 64 | 50 | 400.66 | 1257.84 | 1677.52 | 110.7% |
| HS5 | seq-write | 16 MiB | 2 | 16 | 25 | 397.73 | 642.89 | 868.89 | 93.9% |
| HS5 | seq-write | 16 MiB | 2 | 64 | 24 | 390.31 | 2603.14 | 3044.26 | 99.3% |
| HS5 | seq-write | 16 MiB | 4 | 16 | 25 | 403.41 | 614.41 | 1012.78 | 95.8% |
| HS5 | seq-write | 16 MiB | 4 | 64 | 25 | 396.43 | 2365.79 | 3157.63 | 103.6% |
| HS5 | seq-read | 1 MiB | 2 | 16 | 3,028 | 3027.58 | 5.04 | 8.10 | 195.7% |
| HS5 | seq-read | 1 MiB | 2 | 64 | 2,923 | 2922.93 | 20.84 | 33.51 | 194.4% |
| HS5 | seq-read | 1 MiB | 4 | 16 | 3,179 | 3178.85 | 4.82 | 9.93 | 209.2% |
| HS5 | seq-read | 1 MiB | 4 | 64 | 3,144 | 3144.02 | 19.38 | 37.85 | 212.5% |
| HS5 | seq-read | 8 MiB | 2 | 16 | 335 | 2678.55 | 44.60 | 112.32 | 185.7% |
| HS5 | seq-read | 8 MiB | 2 | 64 | 281 | 2248.37 | 202.96 | 619.50 | 168.2% |
| HS5 | seq-read | 8 MiB | 4 | 16 | 388 | 3101.50 | 39.08 | 81.40 | 213.6% |
| HS5 | seq-read | 8 MiB | 4 | 64 | 333 | 2662.49 | 180.43 | 405.70 | 197.4% |
| HS5 | seq-read | 16 MiB | 2 | 16 | 169 | 2702.12 | 86.73 | 221.55 | 179.8% |
| HS5 | seq-read | 16 MiB | 2 | 64 | 129 | 2063.48 | 421.86 | 1242.91 | 153.7% |
| HS5 | seq-read | 16 MiB | 4 | 16 | 202 | 3231.61 | 75.55 | 158.22 | 217.6% |
| HS5 | seq-read | 16 MiB | 4 | 64 | 172 | 2747.79 | 356.11 | 797.32 | 205.2% |
| Garage | seq-write | 1 MiB | 2 | 16 | 198 | 198.48 | 79.25 | 125.93 | 189.9% |
| Garage | seq-write | 1 MiB | 2 | 64 | 191 | 190.64 | 329.86 | 590.23 | 190.2% |
| Garage | seq-write | 1 MiB | 4 | 16 | 290 | 289.57 | 54.23 | 84.54 | 287.2% |
| Garage | seq-write | 1 MiB | 4 | 64 | 281 | 281.29 | 223.91 | 385.33 | 287.4% |
| Garage | seq-write | 8 MiB | 2 | 16 | 27 | 217.27 | 568.85 | 914.26 | 192.9% |
| Garage | seq-write | 8 MiB | 2 | 64 | 26 | 211.09 | 2365.44 | 3941.86 | 190.9% |
| Garage | seq-write | 8 MiB | 4 | 16 | 38 | 306.90 | 404.47 | 680.17 | 278.2% |
| Garage | seq-write | 8 MiB | 4 | 64 | 41 | 325.28 | 1537.73 | 2675.44 | 293.2% |
| Garage | seq-write | 16 MiB | 2 | 16 | 14 | 216.27 | 1110.16 | 1766.82 | 192.3% |
| Garage | seq-write | 16 MiB | 2 | 64 | 13 | 213.27 | 4170.02 | 6299.87 | 191.8% |
| Garage | seq-write | 16 MiB | 4 | 16 | 20 | 324.25 | 749.86 | 1331.57 | 293.8% |
| Garage | seq-write | 16 MiB | 4 | 64 | 21 | 328.16 | 2829.12 | 4690.50 | 295.2% |
| Garage | seq-read | 1 MiB | 2 | 16 | 301 | 300.50 | 52.24 | 82.80 | 181.8% |
| Garage | seq-read | 1 MiB | 2 | 64 | 303 | 303.19 | 208.63 | 246.61 | 182.1% |
| Garage | seq-read | 1 MiB | 4 | 16 | 415 | 415.48 | 37.73 | 58.89 | 268.2% |
| Garage | seq-read | 1 MiB | 4 | 64 | 418 | 418.31 | 151.10 | 183.08 | 269.4% |
| Garage | seq-read | 8 MiB | 2 | 16 | 41 | 325.14 | 386.81 | 464.64 | 182.1% |
| Garage | seq-read | 8 MiB | 2 | 64 | 41 | 325.86 | 1540.01 | 1632.20 | 181.9% |
| Garage | seq-read | 8 MiB | 4 | 16 | 56 | 447.82 | 280.74 | 331.16 | 267.0% |
| Garage | seq-read | 8 MiB | 4 | 64 | 54 | 434.93 | 1146.91 | 1269.91 | 253.6% |
| Garage | seq-read | 16 MiB | 2 | 16 | 20 | 325.59 | 772.60 | 875.84 | 181.3% |
| Garage | seq-read | 16 MiB | 2 | 64 | 20 | 320.22 | 3091.78 | 3253.65 | 178.7% |
| Garage | seq-read | 16 MiB | 4 | 16 | 28 | 446.56 | 562.20 | 649.61 | 262.5% |
| Garage | seq-read | 16 MiB | 4 | 64 | 26 | 411.34 | 2418.88 | 2617.46 | 237.8% |
| SeaweedFS | seq-write | 1 MiB | 2 | 16 | 188 | 188.15 | 78.86 | 180.78 | 195.3% |
| SeaweedFS | seq-write | 1 MiB | 2 | 64 | 183 | 183.26 | 276.98 | 1265.82 | 197.3% |
| SeaweedFS | seq-write | 1 MiB | 4 | 16 | 354 | 353.68 | 39.66 | 115.18 | 371.2% |
| SeaweedFS | seq-write | 1 MiB | 4 | 64 | 288 | 287.76 | 149.93 | 1259.27 | 309.3% |
| SeaweedFS | seq-write | 8 MiB | 2 | 16 | 24 | 193.38 | 611.93 | 1660.70 | 197.6% |
| SeaweedFS | seq-write | 8 MiB | 2 | 64 | 22 | 173.37 | 2353.78 | 5754.49 | 191.1% |
| SeaweedFS | seq-write | 8 MiB | 4 | 16 | 41 | 331.62 | 341.64 | 1118.95 | 337.6% |
| SeaweedFS | seq-write | 8 MiB | 4 | 64 | 43 | 343.26 | 1415.84 | 3362.22 | 363.2% |
| SeaweedFS | seq-write | 16 MiB | 2 | 16 | 13 | 206.16 | 1115.02 | 2583.24 | 198.6% |
| SeaweedFS | seq-write | 16 MiB | 2 | 64 | 11 | 170.38 | 5922.40 | 6243.54 | 195.9% |
| SeaweedFS | seq-write | 16 MiB | 4 | 16 | 20 | 318.18 | 694.62 | 1873.55 | 310.6% |
| SeaweedFS | seq-write | 16 MiB | 4 | 64 | 20 | 321.47 | 2997.29 | 4933.25 | 359.4% |
| SeaweedFS | seq-read | 1 MiB | 2 | 16 | 806 | 806.12 | 18.87 | 66.97 | 190.9% |
| SeaweedFS | seq-read | 1 MiB | 2 | 64 | 788 | 788.09 | 77.62 | 157.27 | 191.8% |
| SeaweedFS | seq-read | 1 MiB | 4 | 16 | 1,127 | 1126.93 | 12.58 | 84.18 | 276.6% |
| SeaweedFS | seq-read | 1 MiB | 4 | 64 | 1,141 | 1140.66 | 52.82 | 122.09 | 287.6% |
| SeaweedFS | seq-read | 8 MiB | 2 | 16 | 231 | 1847.42 | 67.66 | 131.97 | 192.3% |
| SeaweedFS | seq-read | 8 MiB | 2 | 64 | 286 | 2284.21 | 196.72 | 640.67 | 177.0% |
| SeaweedFS | seq-read | 8 MiB | 4 | 16 | 279 | 2230.38 | 54.34 | 119.15 | 253.2% |
| SeaweedFS | seq-read | 8 MiB | 4 | 64 | 324 | 2594.57 | 180.80 | 489.36 | 214.3% |
| SeaweedFS | seq-read | 16 MiB | 2 | 16 | 131 | 2103.72 | 116.02 | 227.97 | 190.7% |
| SeaweedFS | seq-read | 16 MiB | 2 | 64 | 153 | 2447.88 | 344.93 | 1123.24 | 157.2% |
| SeaweedFS | seq-read | 16 MiB | 4 | 16 | 161 | 2574.92 | 94.82 | 190.22 | 252.7% |
| SeaweedFS | seq-read | 16 MiB | 4 | 64 | 180 | 2884.11 | 323.78 | 797.23 | 190.7% |
| MinIO | seq-write | 1 MiB | 2 | 16 | 366 | 366.21 | 43.03 | 92.88 | 135.6% |
| MinIO | seq-write | 1 MiB | 2 | 64 | 339 | 339.13 | 180.46 | 400.21 | 129.1% |
| MinIO | seq-write | 1 MiB | 4 | 16 | 375 | 375.30 | 44.66 | 94.68 | 149.1% |
| MinIO | seq-write | 1 MiB | 4 | 64 | 355 | 354.99 | 181.64 | 359.00 | 156.0% |
| MinIO | seq-write | 8 MiB | 2 | 16 | 49 | 392.76 | 337.89 | 578.47 | 112.0% |
| MinIO | seq-write | 8 MiB | 2 | 64 | 49 | 390.44 | 1420.95 | 2516.87 | 109.5% |
| MinIO | seq-write | 8 MiB | 4 | 16 | 50 | 401.56 | 340.20 | 464.23 | 116.0% |
| MinIO | seq-write | 8 MiB | 4 | 64 | 49 | 395.34 | 1428.05 | 1649.92 | 113.6% |
| MinIO | seq-write | 16 MiB | 2 | 16 | 25 | 398.45 | 675.14 | 1181.94 | 108.1% |
| MinIO | seq-write | 16 MiB | 2 | 64 | 24 | 383.70 | 2621.33 | 4710.87 | 104.8% |
| MinIO | seq-write | 16 MiB | 4 | 16 | 25 | 401.48 | 665.68 | 893.94 | 110.2% |
| MinIO | seq-write | 16 MiB | 4 | 64 | 23 | 375.06 | 2878.35 | 3544.37 | 105.0% |
| MinIO | seq-read | 1 MiB | 2 | 16 | 1,363 | 1363.21 | 4.54 | 79.51 | 197.1% |
| MinIO | seq-read | 1 MiB | 2 | 64 | 1,418 | 1418.46 | 18.80 | 250.00 | 197.9% |
| MinIO | seq-read | 1 MiB | 4 | 16 | 1,834 | 1834.36 | 7.41 | 27.06 | 273.4% |
| MinIO | seq-read | 1 MiB | 4 | 64 | 1,717 | 1717.09 | 35.30 | 93.63 | 261.5% |
| MinIO | seq-read | 8 MiB | 2 | 16 | 311 | 2491.01 | 48.40 | 115.77 | 189.5% |
| MinIO | seq-read | 8 MiB | 2 | 64 | 285 | 2279.76 | 198.38 | 626.82 | 176.1% |
| MinIO | seq-read | 8 MiB | 4 | 16 | 344 | 2753.82 | 43.65 | 101.11 | 228.2% |
| MinIO | seq-read | 8 MiB | 4 | 64 | 328 | 2620.57 | 180.25 | 470.83 | 219.2% |
| MinIO | seq-read | 16 MiB | 2 | 16 | 166 | 2660.51 | 86.53 | 219.26 | 184.9% |
| MinIO | seq-read | 16 MiB | 2 | 64 | 144 | 2298.27 | 347.87 | 1222.52 | 155.6% |
| MinIO | seq-read | 16 MiB | 4 | 16 | 191 | 3058.70 | 78.82 | 172.46 | 235.7% |
| MinIO | seq-read | 16 MiB | 4 | 64 | 163 | 2613.60 | 356.61 | 913.93 | 198.9% |
| RustFS | seq-write | 1 MiB | 2 | 16 | 266 | 265.62 | 58.40 | 101.71 | 195.5% |
| RustFS | seq-write | 1 MiB | 2 | 64 | 245 | 245.05 | 249.02 | 463.80 | 193.4% |
| RustFS | seq-write | 1 MiB | 4 | 16 | 352 | 352.00 | 45.28 | 77.53 | 260.6% |
| RustFS | seq-write | 1 MiB | 4 | 64 | 336 | 335.67 | 187.99 | 296.79 | 259.7% |
| RustFS | seq-write | 8 MiB | 2 | 16 | 47 | 375.06 | 324.79 | 537.01 | 194.6% |
| RustFS | seq-write | 8 MiB | 2 | 64 | 46 | 368.07 | 1218.62 | 2755.66 | 194.8% |
| RustFS | seq-write | 8 MiB | 4 | 16 | 50 | 396.79 | 316.32 | 503.57 | 203.3% |
| RustFS | seq-write | 8 MiB | 4 | 64 | 49 | 389.82 | 1294.48 | 2129.09 | 202.2% |
| RustFS | seq-write | 16 MiB | 2 | 16 | 24 | 382.93 | 643.01 | 1075.53 | 187.6% |
| RustFS | seq-write | 16 MiB | 2 | 64 | 21 | 335.80 | 2600.95 | 5440.36 | 175.2% |
| RustFS | seq-write | 16 MiB | 4 | 16 | 25 | 397.24 | 645.99 | 999.46 | 192.8% |
| RustFS | seq-write | 16 MiB | 4 | 64 | 24 | 387.29 | 2408.60 | 4256.46 | 195.7% |
| RustFS | seq-read | 1 MiB | 2 | 16 | 532 | 531.53 | 28.89 | 53.92 | 198.0% |
| RustFS | seq-read | 1 MiB | 2 | 64 | 532 | 532.17 | 117.96 | 194.85 | 198.3% |
| RustFS | seq-read | 1 MiB | 4 | 16 | 870 | 870.33 | 17.56 | 32.63 | 342.4% |
| RustFS | seq-read | 1 MiB | 4 | 64 | 877 | 877.21 | 71.69 | 121.01 | 341.2% |
| RustFS | seq-read | 8 MiB | 2 | 16 | 87 | 699.75 | 175.10 | 297.81 | 198.5% |
| RustFS | seq-read | 8 MiB | 2 | 64 | 88 | 704.98 | 694.74 | 1147.14 | 198.5% |
| RustFS | seq-read | 8 MiB | 4 | 16 | 142 | 1136.48 | 108.35 | 199.41 | 335.1% |
| RustFS | seq-read | 8 MiB | 4 | 64 | 131 | 1045.40 | 453.16 | 964.61 | 303.1% |
| RustFS | seq-read | 16 MiB | 2 | 16 | 51 | 811.40 | 300.66 | 509.53 | 197.7% |
| RustFS | seq-read | 16 MiB | 2 | 64 | 51 | 821.10 | 1180.28 | 1872.38 | 198.6% |
| RustFS | seq-read | 16 MiB | 4 | 16 | 77 | 1229.62 | 198.16 | 357.55 | 310.9% |
| RustFS | seq-read | 16 MiB | 4 | 64 | 71 | 1135.31 | 835.44 | 1388.54 | 282.3% |

## Client CPU Usage (`CPU %` where 100% = 1 core busy, against client cores given)

Client cap check: 5 of 153 measured phases were client-capped: AwanFS seq-read 8 MiB 2c c=64 (yes (saturated client cores)), AwanFS seq-read 16 MiB 2c c=64 (yes (saturated client cores)), HS5 seq-read 16 MiB 2c c=64 (yes (saturated client cores)), SeaweedFS seq-read 16 MiB 2c c=64 (yes (saturated client cores)), MinIO seq-read 16 MiB 2c c=64 (yes (saturated client cores)).

| Server | Pattern | Size | Cores | Concurrency | Client Procs | Client Cores | Client CPU % | Client Core Util | Client-Capped? |
|---|---|---|---|---|---|---|---|---|---|
| AwanFS | seq-write | 1 MiB | 2 | 16 | 8 | 2 | 17.9% | 9.0% | no |
| AwanFS | seq-write | 1 MiB | 2 | 64 | 8 | 2 | 18.2% | 9.1% | no |
| AwanFS | seq-write | 1 MiB | 4 | 16 | 8 | 4 (shared) | 27.3% | 6.8% | no |
| AwanFS | seq-write | 1 MiB | 4 | 64 | 8 | 4 (shared) | 26.8% | 6.7% | no |
| AwanFS | seq-write | 8 MiB | 2 | 16 | 8 | 2 | 11.5% | 5.8% | no |
| AwanFS | seq-write | 8 MiB | 2 | 64 | 8 | 2 | 11.2% | 5.6% | no |
| AwanFS | seq-write | 8 MiB | 4 | 16 | 8 | 4 (shared) | 12.1% | 3.0% | no |
| AwanFS | seq-write | 8 MiB | 4 | 64 | 8 | 4 (shared) | 12.1% | 3.0% | no |
| AwanFS | seq-write | 16 MiB | 2 | 16 | 8 | 2 | 9.8% | 4.9% | no |
| AwanFS | seq-write | 16 MiB | 2 | 64 | 8 | 2 | 8.4% | 4.2% | no |
| AwanFS | seq-write | 16 MiB | 4 | 16 | 8 | 4 (shared) | 10.2% | 2.6% | no |
| AwanFS | seq-write | 16 MiB | 4 | 64 | 8 | 4 (shared) | 10.2% | 2.6% | no |
| AwanFS | seq-read | 1 MiB | 2 | 16 | 8 | 2 | 182.0% | 91.0% | no (server also >=85%) |
| AwanFS | seq-read | 1 MiB | 2 | 64 | 8 | 2 | 188.0% | 94.0% | no (server also >=85%) |
| AwanFS | seq-read | 1 MiB | 4 | 16 | 8 | 4 (shared) | 170.0% | 42.5% | no |
| AwanFS | seq-read | 1 MiB | 4 | 64 | 8 | 4 (shared) | 181.0% | 45.2% | no |
| AwanFS | seq-read | 8 MiB | 2 | 16 | 8 | 2 | 175.3% | 87.7% | no (server also >=85%) |
| AwanFS | seq-read | 8 MiB | 2 | 64 | 8 | 2 | 179.8% | 89.9% | yes (saturated client cores) |
| AwanFS | seq-read | 8 MiB | 4 | 16 | 8 | 4 (shared) | 174.1% | 43.5% | no |
| AwanFS | seq-read | 8 MiB | 4 | 64 | 8 | 4 (shared) | 183.4% | 45.8% | no |
| AwanFS | seq-read | 16 MiB | 2 | 16 | 8 | 2 | 173.2% | 86.6% | no (server also >=85%) |
| AwanFS | seq-read | 16 MiB | 2 | 64 | 8 | 2 | 186.6% | 93.3% | yes (saturated client cores) |
| AwanFS | seq-read | 16 MiB | 4 | 16 | 8 | 4 (shared) | 176.4% | 44.1% | no |
| AwanFS | seq-read | 16 MiB | 4 | 64 | 8 | 4 (shared) | 199.8% | 50.0% | no |
| AwanFS (--durability fast) | seq-write | 1 MiB | 2 | 16 | 8 | 2 | 19.3% | 9.6% | no |
| AwanFS (--durability fast) | seq-write | 1 MiB | 2 | 64 | 8 | 2 | 19.4% | 9.7% | no |
| AwanFS (--durability fast) | seq-write | 1 MiB | 4 | 64 | 8 | 4 (shared) | 29.2% | 7.3% | no |
| AwanFS (--durability fast) | seq-write | 8 MiB | 2 | 16 | 8 | 2 | 12.9% | 6.5% | no |
| AwanFS (--durability fast) | seq-write | 8 MiB | 2 | 64 | 8 | 2 | 13.9% | 7.0% | no |
| AwanFS (--durability fast) | seq-write | 8 MiB | 4 | 64 | 8 | 4 (shared) | 17.5% | 4.4% | no |
| AwanFS (--durability fast) | seq-write | 16 MiB | 2 | 16 | 8 | 2 | 12.3% | 6.1% | no |
| AwanFS (--durability fast) | seq-write | 16 MiB | 2 | 64 | 8 | 2 | 12.6% | 6.3% | no |
| AwanFS (--durability fast) | seq-write | 16 MiB | 4 | 64 | 8 | 4 (shared) | 11.1% | 2.8% | no |
| HS5 | seq-write | 1 MiB | 2 | 16 | 8 | 2 | 27.4% | 13.7% | no |
| HS5 | seq-write | 1 MiB | 2 | 64 | 8 | 2 | 23.9% | 12.0% | no |
| HS5 | seq-write | 1 MiB | 4 | 16 | 8 | 4 (shared) | 30.0% | 7.5% | no |
| HS5 | seq-write | 1 MiB | 4 | 64 | 8 | 4 (shared) | 29.0% | 7.3% | no |
| HS5 | seq-write | 8 MiB | 2 | 16 | 8 | 2 | 14.3% | 7.1% | no |
| HS5 | seq-write | 8 MiB | 2 | 64 | 8 | 2 | 13.3% | 6.7% | no |
| HS5 | seq-write | 8 MiB | 4 | 16 | 8 | 4 (shared) | 15.5% | 3.9% | no |
| HS5 | seq-write | 8 MiB | 4 | 64 | 8 | 4 (shared) | 13.2% | 3.3% | no |
| HS5 | seq-write | 16 MiB | 2 | 16 | 8 | 2 | 12.8% | 6.4% | no |
| HS5 | seq-write | 16 MiB | 2 | 64 | 8 | 2 | 11.2% | 5.6% | no |
| HS5 | seq-write | 16 MiB | 4 | 16 | 8 | 4 (shared) | 13.6% | 3.4% | no |
| HS5 | seq-write | 16 MiB | 4 | 64 | 8 | 4 (shared) | 11.2% | 2.8% | no |
| HS5 | seq-read | 1 MiB | 2 | 16 | 8 | 2 | 163.5% | 81.7% | no (server also >=85%) |
| HS5 | seq-read | 1 MiB | 2 | 64 | 8 | 2 | 163.2% | 81.6% | no (server also >=85%) |
| HS5 | seq-read | 1 MiB | 4 | 16 | 8 | 4 (shared) | 181.8% | 45.4% | no |
| HS5 | seq-read | 1 MiB | 4 | 64 | 8 | 4 (shared) | 184.0% | 46.0% | no |
| HS5 | seq-read | 8 MiB | 2 | 16 | 8 | 2 | 158.8% | 79.4% | no |
| HS5 | seq-read | 8 MiB | 2 | 64 | 8 | 2 | 154.8% | 77.4% | no |
| HS5 | seq-read | 8 MiB | 4 | 16 | 8 | 4 (shared) | 182.4% | 45.6% | no |
| HS5 | seq-read | 8 MiB | 4 | 64 | 8 | 4 (shared) | 181.9% | 45.5% | no |
| HS5 | seq-read | 16 MiB | 2 | 16 | 8 | 2 | 154.8% | 77.4% | no |
| HS5 | seq-read | 16 MiB | 2 | 64 | 8 | 2 | 161.0% | 80.5% | yes (saturated client cores) |
| HS5 | seq-read | 16 MiB | 4 | 16 | 8 | 4 (shared) | 178.4% | 44.6% | no |
| HS5 | seq-read | 16 MiB | 4 | 64 | 8 | 4 (shared) | 190.1% | 47.5% | no |
| Garage | seq-write | 1 MiB | 2 | 16 | 8 | 2 | 14.1% | 7.1% | no |
| Garage | seq-write | 1 MiB | 2 | 64 | 8 | 2 | 13.4% | 6.7% | no |
| Garage | seq-write | 1 MiB | 4 | 16 | 8 | 4 (shared) | 22.1% | 5.5% | no |
| Garage | seq-write | 1 MiB | 4 | 64 | 8 | 4 (shared) | 22.5% | 5.6% | no |
| Garage | seq-write | 8 MiB | 2 | 16 | 8 | 2 | 7.2% | 3.6% | no |
| Garage | seq-write | 8 MiB | 2 | 64 | 8 | 2 | 7.1% | 3.6% | no |
| Garage | seq-write | 8 MiB | 4 | 16 | 8 | 4 (shared) | 9.7% | 2.4% | no |
| Garage | seq-write | 8 MiB | 4 | 64 | 8 | 4 (shared) | 10.5% | 2.6% | no |
| Garage | seq-write | 16 MiB | 2 | 16 | 8 | 2 | 5.9% | 2.9% | no |
| Garage | seq-write | 16 MiB | 2 | 64 | 8 | 2 | 6.0% | 3.0% | no |
| Garage | seq-write | 16 MiB | 4 | 16 | 8 | 4 (shared) | 8.8% | 2.2% | no |
| Garage | seq-write | 16 MiB | 4 | 64 | 8 | 4 (shared) | 8.7% | 2.2% | no |
| Garage | seq-read | 1 MiB | 2 | 16 | 8 | 2 | 27.0% | 13.5% | no |
| Garage | seq-read | 1 MiB | 2 | 64 | 8 | 2 | 29.5% | 14.7% | no |
| Garage | seq-read | 1 MiB | 4 | 16 | 8 | 4 (shared) | 44.3% | 11.1% | no |
| Garage | seq-read | 1 MiB | 4 | 64 | 8 | 4 (shared) | 45.8% | 11.5% | no |
| Garage | seq-read | 8 MiB | 2 | 16 | 8 | 2 | 32.2% | 16.1% | no |
| Garage | seq-read | 8 MiB | 2 | 64 | 8 | 2 | 50.4% | 25.2% | no |
| Garage | seq-read | 8 MiB | 4 | 16 | 8 | 4 (shared) | 46.3% | 11.6% | no |
| Garage | seq-read | 8 MiB | 4 | 64 | 8 | 4 (shared) | 65.6% | 16.4% | no |
| Garage | seq-read | 16 MiB | 2 | 16 | 8 | 2 | 36.9% | 18.4% | no |
| Garage | seq-read | 16 MiB | 2 | 64 | 8 | 2 | 55.4% | 27.7% | no |
| Garage | seq-read | 16 MiB | 4 | 16 | 8 | 4 (shared) | 52.8% | 13.2% | no |
| Garage | seq-read | 16 MiB | 4 | 64 | 8 | 4 (shared) | 72.6% | 18.1% | no |
| SeaweedFS | seq-write | 1 MiB | 2 | 16 | 8 | 2 | 13.2% | 6.6% | no |
| SeaweedFS | seq-write | 1 MiB | 2 | 64 | 8 | 2 | 13.4% | 6.7% | no |
| SeaweedFS | seq-write | 1 MiB | 4 | 16 | 8 | 4 (shared) | 24.7% | 6.2% | no |
| SeaweedFS | seq-write | 1 MiB | 4 | 64 | 8 | 4 (shared) | 21.1% | 5.3% | no |
| SeaweedFS | seq-write | 8 MiB | 2 | 16 | 8 | 2 | 7.2% | 3.6% | no |
| SeaweedFS | seq-write | 8 MiB | 2 | 64 | 8 | 2 | 5.9% | 2.9% | no |
| SeaweedFS | seq-write | 8 MiB | 4 | 16 | 8 | 4 (shared) | 12.6% | 3.1% | no |
| SeaweedFS | seq-write | 8 MiB | 4 | 64 | 8 | 4 (shared) | 11.5% | 2.9% | no |
| SeaweedFS | seq-write | 16 MiB | 2 | 16 | 8 | 2 | 6.4% | 3.2% | no |
| SeaweedFS | seq-write | 16 MiB | 2 | 64 | 8 | 2 | 5.4% | 2.7% | no |
| SeaweedFS | seq-write | 16 MiB | 4 | 16 | 8 | 4 (shared) | 10.4% | 2.6% | no |
| SeaweedFS | seq-write | 16 MiB | 4 | 64 | 8 | 4 (shared) | 8.8% | 2.2% | no |
| SeaweedFS | seq-read | 1 MiB | 2 | 16 | 8 | 2 | 50.3% | 25.1% | no |
| SeaweedFS | seq-read | 1 MiB | 2 | 64 | 8 | 2 | 51.9% | 26.0% | no |
| SeaweedFS | seq-read | 1 MiB | 4 | 16 | 8 | 4 (shared) | 75.7% | 18.9% | no |
| SeaweedFS | seq-read | 1 MiB | 4 | 64 | 8 | 4 (shared) | 82.1% | 20.5% | no |
| SeaweedFS | seq-read | 8 MiB | 2 | 16 | 8 | 2 | 101.0% | 50.5% | no |
| SeaweedFS | seq-read | 8 MiB | 2 | 64 | 8 | 2 | 152.7% | 76.4% | no |
| SeaweedFS | seq-read | 8 MiB | 4 | 16 | 8 | 4 (shared) | 119.5% | 29.9% | no |
| SeaweedFS | seq-read | 8 MiB | 4 | 64 | 8 | 4 (shared) | 166.8% | 41.7% | no |
| SeaweedFS | seq-read | 16 MiB | 2 | 16 | 8 | 2 | 115.8% | 57.9% | no |
| SeaweedFS | seq-read | 16 MiB | 2 | 64 | 8 | 2 | 180.9% | 90.4% | yes (saturated client cores) |
| SeaweedFS | seq-read | 16 MiB | 4 | 16 | 8 | 4 (shared) | 133.4% | 33.3% | no |
| SeaweedFS | seq-read | 16 MiB | 4 | 64 | 8 | 4 (shared) | 198.6% | 49.7% | no |
| MinIO | seq-write | 1 MiB | 2 | 16 | 8 | 2 | 25.4% | 12.7% | no |
| MinIO | seq-write | 1 MiB | 2 | 64 | 8 | 2 | 24.0% | 12.0% | no |
| MinIO | seq-write | 1 MiB | 4 | 16 | 8 | 4 (shared) | 29.7% | 7.4% | no |
| MinIO | seq-write | 1 MiB | 4 | 64 | 8 | 4 (shared) | 28.3% | 7.1% | no |
| MinIO | seq-write | 8 MiB | 2 | 16 | 8 | 2 | 12.7% | 6.4% | no |
| MinIO | seq-write | 8 MiB | 2 | 64 | 8 | 2 | 12.3% | 6.1% | no |
| MinIO | seq-write | 8 MiB | 4 | 16 | 8 | 4 (shared) | 13.6% | 3.4% | no |
| MinIO | seq-write | 8 MiB | 4 | 64 | 8 | 4 (shared) | 13.1% | 3.3% | no |
| MinIO | seq-write | 16 MiB | 2 | 16 | 8 | 2 | 10.4% | 5.2% | no |
| MinIO | seq-write | 16 MiB | 2 | 64 | 8 | 2 | 10.5% | 5.2% | no |
| MinIO | seq-write | 16 MiB | 4 | 16 | 8 | 4 (shared) | 11.4% | 2.9% | no |
| MinIO | seq-write | 16 MiB | 4 | 64 | 8 | 4 (shared) | 10.8% | 2.7% | no |
| MinIO | seq-read | 1 MiB | 2 | 16 | 8 | 2 | 76.3% | 38.2% | no |
| MinIO | seq-read | 1 MiB | 2 | 64 | 8 | 2 | 81.3% | 40.7% | no |
| MinIO | seq-read | 1 MiB | 4 | 16 | 8 | 4 (shared) | 115.0% | 28.7% | no |
| MinIO | seq-read | 1 MiB | 4 | 64 | 8 | 4 (shared) | 111.0% | 27.8% | no |
| MinIO | seq-read | 8 MiB | 2 | 16 | 8 | 2 | 150.6% | 75.3% | no |
| MinIO | seq-read | 8 MiB | 2 | 64 | 8 | 2 | 151.9% | 76.0% | no |
| MinIO | seq-read | 8 MiB | 4 | 16 | 8 | 4 (shared) | 158.7% | 39.7% | no |
| MinIO | seq-read | 8 MiB | 4 | 64 | 8 | 4 (shared) | 168.7% | 42.2% | no |
| MinIO | seq-read | 16 MiB | 2 | 16 | 8 | 2 | 144.9% | 72.5% | no |
| MinIO | seq-read | 16 MiB | 2 | 64 | 8 | 2 | 171.2% | 85.6% | yes (saturated client cores) |
| MinIO | seq-read | 16 MiB | 4 | 16 | 8 | 4 (shared) | 155.2% | 38.8% | no |
| MinIO | seq-read | 16 MiB | 4 | 64 | 8 | 4 (shared) | 188.6% | 47.1% | no |
| RustFS | seq-write | 1 MiB | 2 | 16 | 8 | 2 | 16.6% | 8.3% | no |
| RustFS | seq-write | 1 MiB | 2 | 64 | 8 | 2 | 15.5% | 7.7% | no |
| RustFS | seq-write | 1 MiB | 4 | 16 | 8 | 4 (shared) | 24.7% | 6.2% | no |
| RustFS | seq-write | 1 MiB | 4 | 64 | 8 | 4 (shared) | 22.6% | 5.7% | no |
| RustFS | seq-write | 8 MiB | 2 | 16 | 8 | 2 | 11.4% | 5.7% | no |
| RustFS | seq-write | 8 MiB | 2 | 64 | 8 | 2 | 11.0% | 5.5% | no |
| RustFS | seq-write | 8 MiB | 4 | 16 | 8 | 4 (shared) | 13.4% | 3.4% | no |
| RustFS | seq-write | 8 MiB | 4 | 64 | 8 | 4 (shared) | 11.8% | 2.9% | no |
| RustFS | seq-write | 16 MiB | 2 | 16 | 8 | 2 | 11.0% | 5.5% | no |
| RustFS | seq-write | 16 MiB | 2 | 64 | 8 | 2 | 9.7% | 4.9% | no |
| RustFS | seq-write | 16 MiB | 4 | 16 | 8 | 4 (shared) | 12.0% | 3.0% | no |
| RustFS | seq-write | 16 MiB | 4 | 64 | 8 | 4 (shared) | 10.4% | 2.6% | no |
| RustFS | seq-read | 1 MiB | 2 | 16 | 8 | 2 | 31.1% | 15.5% | no |
| RustFS | seq-read | 1 MiB | 2 | 64 | 8 | 2 | 33.3% | 16.6% | no |
| RustFS | seq-read | 1 MiB | 4 | 16 | 8 | 4 (shared) | 52.1% | 13.0% | no |
| RustFS | seq-read | 1 MiB | 4 | 64 | 8 | 4 (shared) | 54.5% | 13.6% | no |
| RustFS | seq-read | 8 MiB | 2 | 16 | 8 | 2 | 43.5% | 21.7% | no |
| RustFS | seq-read | 8 MiB | 2 | 64 | 8 | 2 | 66.4% | 33.2% | no |
| RustFS | seq-read | 8 MiB | 4 | 16 | 8 | 4 (shared) | 60.4% | 15.1% | no |
| RustFS | seq-read | 8 MiB | 4 | 64 | 8 | 4 (shared) | 83.6% | 20.9% | no |
| RustFS | seq-read | 16 MiB | 2 | 16 | 8 | 2 | 53.7% | 26.8% | no |
| RustFS | seq-read | 16 MiB | 2 | 64 | 8 | 2 | 86.5% | 43.2% | no |
| RustFS | seq-read | 16 MiB | 4 | 16 | 8 | 4 (shared) | 67.1% | 16.8% | no |
| RustFS | seq-read | 16 MiB | 4 | 64 | 8 | 4 (shared) | 107.0% | 26.8% | no |

## AwanFS work per phase (`/_awanfs/metrics`, before and after each phase)

Syncs and disk bytes are the server's own counters; rates use the phase's wall time. PUTs per sync is ops/s over syncs/s.

| Durability | Pattern | Size | Cores | Concurrency | ops/s | syncs/s | PUTs per sync | MiB per sync | disk MiB/s | peak in-flight MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| group | seq-write | 1 MiB | 2 | 16 | 308 | 427.2 | 0.7 | 0.58 | 248.0 | 16.0 |
| group | seq-write | 1 MiB | 2 | 64 | 307 | 452.9 | 0.7 | 0.55 | 248.9 | 64.0 |
| group | seq-write | 1 MiB | 4 | 16 | 403 | 276.7 | 1.5 | 1.30 | 359.4 | 16.0 |
| group | seq-write | 1 MiB | 4 | 64 | 403 | 131.2 | 3.1 | 2.76 | 358.2 | 64.0 |
| group | seq-write | 8 MiB | 2 | 16 | 50 | 21.0 | 2.4 | 15.28 | 319.7 | 64.0 |
| group | seq-write | 8 MiB | 2 | 64 | 48 | 6.5 | 7.5 | 48.25 | 309.2 | 192.2 |
| group | seq-write | 8 MiB | 4 | 16 | 51 | 17.7 | 2.9 | 20.20 | 357.5 | 64.0 |
| group | seq-write | 8 MiB | 4 | 64 | 50 | 6.8 | 7.5 | 53.03 | 357.9 | 192.8 |
| group | seq-write | 16 MiB | 2 | 16 | 25 | 10.9 | 2.3 | 28.41 | 310.1 | 192.2 |
| group | seq-write | 16 MiB | 2 | 64 | 21 | 4.3 | 5.0 | 65.26 | 277.7 | 199.7 |
| group | seq-write | 16 MiB | 4 | 16 | 25 | 8.0 | 3.2 | 44.17 | 353.2 | 192.8 |
| group | seq-write | 16 MiB | 4 | 64 | 25 | 4.1 | 6.0 | 86.55 | 353.2 | 231.2 |
| group | seq-read | 1 MiB | 2 | 16 | 3,631 | 0.0 | no syncs | no syncs | 0.0 | 199.7 |
| group | seq-read | 1 MiB | 2 | 64 | 3,704 | 0.0 | no syncs | no syncs | 0.0 | 199.7 |
| group | seq-read | 1 MiB | 4 | 16 | 3,348 | 0.0 | no syncs | no syncs | 0.0 | 231.2 |
| group | seq-read | 1 MiB | 4 | 64 | 3,486 | 0.0 | no syncs | no syncs | 0.0 | 231.2 |
| group | seq-read | 8 MiB | 2 | 16 | 390 | 0.0 | no syncs | no syncs | 0.0 | 199.7 |
| group | seq-read | 8 MiB | 2 | 64 | 346 | 0.0 | no syncs | no syncs | 0.0 | 199.7 |
| group | seq-read | 8 MiB | 4 | 16 | 417 | 0.0 | no syncs | no syncs | 0.0 | 231.2 |
| group | seq-read | 8 MiB | 4 | 64 | 380 | 0.0 | no syncs | no syncs | 0.0 | 231.2 |
| group | seq-read | 16 MiB | 2 | 16 | 200 | 0.0 | no syncs | no syncs | 0.0 | 199.7 |
| group | seq-read | 16 MiB | 2 | 64 | 166 | 0.0 | no syncs | no syncs | 0.0 | 199.7 |
| group | seq-read | 16 MiB | 4 | 16 | 216 | 0.0 | no syncs | no syncs | 0.0 | 231.2 |
| group | seq-read | 16 MiB | 4 | 64 | 191 | 0.0 | no syncs | no syncs | 0.0 | 231.2 |
| fast | seq-write | 1 MiB | 2 | 16 | 322 | 0.6 | 507.9 | 411.35 | 261.2 | 16.0 |
| fast | seq-write | 1 MiB | 2 | 64 | 321 | 1.2 | 258.2 | 210.00 | 261.2 | 62.0 |
| fast | seq-write | 1 MiB | 4 | 64 | 443 | 0.7 | 640.0 | 571.72 | 395.5 | 64.0 |
| fast | seq-write | 8 MiB | 2 | 16 | 62 | 0.5 | 118.9 | 661.32 | 348.9 | 76.5 |
| fast | seq-write | 8 MiB | 2 | 64 | 64 | 0.6 | 112.4 | 585.29 | 331.8 | 192.2 |
| fast | seq-write | 8 MiB | 4 | 64 | 81 | 0.3 | 279.6 | 2006.98 | 577.7 | 237.2 |
| fast | seq-write | 16 MiB | 2 | 16 | 33 | 0.7 | 45.8 | 564.14 | 400.5 | 192.2 |
| fast | seq-write | 16 MiB | 2 | 64 | 33 | 0.4 | 112.8 | 1260.31 | 385.8 | 367.2 |
| fast | seq-write | 16 MiB | 4 | 64 | 28 | 0.0 | no syncs | no syncs | 383.1 | 369.8 |

