`python -u benchmarks/hot_set_scale.py --server server/target/release/s3-server --client server/target/release/examples/hot_set_bench --objects 1000000 --window 50000 --data-dir $RUNNER_TEMP/hotset --results hot_set.jsonl --report hot_set.md` (bench-hotset.yml, run 43776732811, commit 42560b0), then `python benchmarks/hot_set_scale.py --report-only` on each job's JSON lines

# Today's server past its 100,000-object hot set, to 1M objects (#210)

Run [43776732811](https://github.com/iskandarnorazli/Awanfs/actions/runs/43776732811), GitHub-hosted runners. Jobs: Linux `ubuntu-latest` 133004582134, Windows `windows-latest` 133004581887.

Settings:
- the server from `main` at 32e6fef with `--memory-budget 2GiB --auto-compaction off --allow-anonymous`;
- 1 KiB objects, new keys only;
- 64 PUT threads, with 8 GET threads during each window;
- a burst of 20,000 GETs from 64 threads after each window;
- peak RSS from VmHWM on Linux and the peak working set on Windows.

Every GET answered 200 with the bytes that were PUT, after the restart too: 0 wrong on both platforms.

The tables are the report script's output on the JSON lines in each job's log (the artifact download was refused from this session). The run's wrapper printed the restart only in its report text ("answered after 311.2 s", "271.7 s"), so that record was rebuilt from it. The wrapper now prints every record.

## Linux (ubuntu-latest, job 133004582134)

Hot set: the first 100,000 objects are kept in memory; later ones go to the redb spill index.

| objects after the window | PUT ops/s | PUT P50 / P99, ms | GET P99 during PUTs, ms | GET ops/s after | GET P99 after, ms | peak RSS so far, MiB |
|---|---|---|---|---|---|---|
| 50,000 | 17,248 | 3.57 / 7.33 | not measured | 54,149 | 2.91 | 427.3 |
| 100,000 | 15,516 | 3.96 / 8.75 | 4.37 | 54,648 | 2.66 | 805.6 |
| 150,000 (past the hot set) | 4,737 | 10.64 / 93.70 | 19.00 | 50,712 | 3.25 | 1,261.6 |
| 200,000 (past the hot set) | 2,826 | 10.77 / 691.58 | 13.21 | 51,270 | 2.97 | 1,641.4 |
| 250,000 (past the hot set) | 2,349 | 11.10 / 1,023.82 | 32.42 | 50,296 | 3.18 | 2,030.2 |
| 300,000 (past the hot set) | 2,279 | 11.07 / 1,082.17 | 40.73 | 48,690 | 3.04 | 2,415.3 |
| 350,000 (past the hot set) | 2,149 | 11.18 / 1,094.21 | 49.68 | 44,193 | 4.71 | 2,803.3 |
| 400,000 (past the hot set) | 2,241 | 11.26 / 1,078.13 | 19.98 | 46,441 | 2.82 | 3,180.6 |
| 450,000 (past the hot set) | 2,206 | 11.21 / 1,123.21 | 15.63 | 43,335 | 3.57 | 3,562.7 |
| 500,000 (past the hot set) | 2,192 | 11.21 / 1,117.14 | 72.58 | 45,950 | 2.96 | 3,956.2 |
| 550,000 (past the hot set) | 2,159 | 11.18 / 1,138.57 | 79.18 | 45,818 | 3.06 | 4,339.2 |
| 600,000 (past the hot set) | 2,012 | 11.21 / 1,180.88 | 78.01 | 38,516 | 3.58 | 4,724.6 |
| 650,000 (past the hot set) | 2,101 | 11.24 / 1,183.80 | 64.93 | 45,689 | 2.97 | 5,101.4 |
| 700,000 (past the hot set) | 2,093 | 11.40 / 1,175.36 | 15.86 | 44,525 | 3.10 | 5,483.5 |
| 750,000 (past the hot set) | 2,032 | 11.29 / 1,225.75 | 74.31 | 44,410 | 2.99 | 5,866.2 |
| 800,000 (past the hot set) | 2,037 | 11.28 / 1,229.46 | 41.70 | 45,707 | 3.04 | 6,247.6 |
| 850,000 (past the hot set) | 1,921 | 11.32 / 1,224.51 | 63.22 | 43,757 | 3.40 | 6,630.9 |
| 900,000 (past the hot set) | 1,999 | 11.38 / 1,266.19 | 83.08 | 38,932 | 8.18 | 7,008.4 |
| 950,000 (past the hot set) | 1,954 | 11.38 / 1,295.55 | 92.65 | 44,281 | 2.95 | 7,414.9 |
| 1,000,000 (past the hot set) | 1,952 | 11.40 / 1,286.41 | 79.92 | 43,578 | 3.18 | 7,795.1 |

Restart with 1,000,000 objects: answered after 311.2 s (peak RSS during the boot 322.9 MiB).
After the restart: 20,000 GETs at 31,007 ops/s, P99 4.99 ms.
Wrong answers: 0.


## Windows (windows-latest, job 133004581887)

Hot set: the first 100,000 objects are kept in memory; later ones go to the redb spill index.

| objects after the window | PUT ops/s | PUT P50 / P99, ms | GET P99 during PUTs, ms | GET ops/s after | GET P99 after, ms | peak RSS so far, MiB |
|---|---|---|---|---|---|---|
| 50,000 | 14,935 | 4.08 / 8.43 | not measured | 44,284 | 3.24 | 393.6 |
| 100,000 | 13,868 | 4.41 / 8.94 | 8.02 | 44,243 | 3.05 | 748.6 |
| 150,000 (past the hot set) | 2,290 | 12.38 / 397.20 | 9.38 | 40,988 | 3.53 | 1,208.8 |
| 200,000 (past the hot set) | 1,963 | 12.43 / 668.91 | 11.65 | 41,490 | 3.37 | 1,586.3 |
| 250,000 (past the hot set) | 2,509 | 12.73 / 431.84 | 16.92 | 40,912 | 3.30 | 1,958.3 |
| 300,000 (past the hot set) | 2,462 | 12.90 / 617.91 | 17.70 | 39,332 | 3.51 | 2,325.5 |
| 350,000 (past the hot set) | 1,723 | 13.16 / 792.33 | 12.33 | 41,799 | 3.25 | 2,697.2 |
| 400,000 (past the hot set) | 1,756 | 14.03 / 535.23 | 10.93 | 38,054 | 3.61 | 3,064.7 |
| 450,000 (past the hot set) | 2,310 | 14.19 / 501.93 | 20.94 | 17,393 | 12.23 | 3,433.4 |
| 500,000 (past the hot set) | 2,372 | 13.85 / 423.86 | 19.72 | 30,191 | 5.16 | 3,818.5 |
| 550,000 (past the hot set) | 2,347 | 13.45 / 595.07 | 17.93 | 33,509 | 4.39 | 4,187.8 |
| 600,000 (past the hot set) | 2,285 | 13.48 / 347.52 | 17.60 | 36,069 | 3.85 | 4,556.9 |
| 650,000 (past the hot set) | 2,340 | 13.38 / 443.54 | 18.09 | 36,844 | 3.71 | 4,653.2 |
| 700,000 (past the hot set) | 2,312 | 13.37 / 370.40 | 18.01 | 36,116 | 3.82 | 4,692.1 |
| 750,000 (past the hot set) | 2,182 | 13.63 / 359.42 | 17.69 | 34,832 | 3.92 | 4,692.1 |
| 800,000 (past the hot set) | 1,726 | 14.10 / 837.04 | 12.09 | 24,317 | 5.80 | 4,692.1 |
| 850,000 (past the hot set) | 1,806 | 13.87 / 795.43 | 16.07 | 35,426 | 4.01 | 4,692.1 |
| 900,000 (past the hot set) | 1,892 | 14.01 / 710.37 | 13.31 | 30,971 | 4.77 | 4,988.8 |
| 950,000 (past the hot set) | 2,067 | 15.53 / 396.22 | 23.29 | 27,876 | 6.24 | 5,399.6 |
| 1,000,000 (past the hot set) | 2,152 | 15.24 / 348.02 | 22.87 | 24,438 | 5.91 | 5,544.4 |

Restart with 1,000,000 objects: answered after 271.7 s (peak RSS during the boot 340.9 MiB).
After the restart: 20,000 GETs at 29,628 ops/s, P99 4.74 ms.
Wrong answers: 0.


## What it says

- **PUTs fall off a cliff at the hot set.** On Linux, PUTs ran at 15,516 to 17,248 per second up to 100,000 objects and 1,921 to 2,826 per second from 200,000 on: 6x to 8x fewer. On Windows they fell from 13,868 to 14,935 per second to 1,723 to 2,509. PUT P99 rose from 7 to 9 ms to 0.35 to 1.30 s. The rate after the cliff matches #207's redb rate of one transaction per key (2,389 and 1,700 per second at 10M keys).
- **GETs wait behind those PUTs.** GET P99 while PUTs ran went from 4.4 ms (Linux, 100,000 objects) to 13 to 93 ms. A burst of GETs alone, after the PUTs, was about the same before and after the hot set: 43,000 to 55,000 per second on Linux.
- **A restart with 1M objects takes 311 s on Linux and 272 s on Windows** before the server answers.
- **Peak RSS grows by about 7.9 KiB per object on Linux from the first window,** before any key is spilled, to 7,795 MiB at 1M objects, with `--memory-budget 2GiB`. On Windows it grows by about 5.5 KiB per object, to 5,544 MiB. This is not redb: it starts at 50,000 objects. After the restart it is 323 MiB (Linux) and 341 MiB (Windows). The cause is not measured; #211 has the code that matches it.

## Predictions made before the run (#210, 2026-11-16), against the result

| | predicted | measured (Linux / Windows) |
|---|---|---|
| PUT ops/s up to 100,000 objects | like the ratio runs | 15,516 to 17,248 / 13,868 to 14,935 |
| PUT ops/s past the hot set | at most about 2,400 / 1,700 | 1,921 to 2,826 / 1,723 to 2,509 (from 200,000 on; 4,737 / 2,290 in the window that crosses it) |
| GET P99 during PUTs | rises | 4.37 ms to 13 to 93 ms / 8.0 ms to 9.4 to 23.3 ms |
| boot with 1M objects | about 6 min / 9 min | 5.2 min / 4.5 min: Windows faster than predicted |
| peak RSS | not predicted | 7,795 / 5,544 MiB at 1M objects: not foreseen |
