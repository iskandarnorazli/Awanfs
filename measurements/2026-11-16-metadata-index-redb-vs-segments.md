`gh workflow run bench-metaindex.yml --ref measure/207-metadata-index` (push trigger; run 43739116538, commit d524300: KEYS=10000000 GETS=1000000 SCANS=1000), then `python benchmarks/meta_index_report.py results.jsonl` on each job's JSON lines

# Metadata index at 10M keys: redb against a segment prototype (#207, decision 59)

Run [43739116538](https://github.com/iskandarnorazli/Awanfs/actions/runs/43739116538), GitHub-hosted runners, one thread.
Jobs: Linux `ubuntu-latest` 132888145071, Windows `windows-latest` 132888144859.
The numbers below are the report script's output on each job's JSON lines (read from the job logs: the artifact download was refused from this session).

- **redb** is `spill::SpillIndex` unchanged, as the server uses it: one write transaction per put, one durable commit in 4096, a 64 MiB cache.
- **segments** is the prototype in `server/examples/meta_index_bench.rs`: 1M-row memtable, about 10 sorted runs, 4 KiB blocks, fence keys and a blocked bloom filter in heap, positioned reads, no compaction.
- Both stores gave the same answers to every query (hash `47cff400f22342b1` on both platforms), with 0 wrong answers.
- Read calls are counted by the OS: `/proc/self/io` `syscr` on Linux and `GetProcessIoCounters` on Windows. Peak RSS is `VmHWM` on Linux and the peak working set on Windows.

## Linux (ubuntu-latest, job 132888145071)

| | redb (AwanFS's spill index) | segments (prototype) |
|---|---|---|
| inserts per second | 2,389 | 521,155 |
| build time, s | 4,185 | 19.188 |
| bytes on disk | 2,156,408,832 | 1,189,120,118 |
| peak RSS while building, MiB | 65.742 | 378 |
| open time, s | 0.001 | 0.018 |
| peak RSS while querying, MiB | 79.312 | 57.086 |
| read calls per get | 1.376 | 1.051 |
| get P50, us | 3.000 | 2.170 |
| get P99, us | 5.440 | 4.550 |
| read calls per absent get | 1.555 | 0.112 |
| absent get P50, us | 3.460 | 0.280 |
| absent get P99, us | 5.150 | 4.020 |
| prefix scan of up to 1000 keys, ms | 0.083 | 0.113 |
| open time after dropping the page cache, s | 0.161 | 0.047 |
| get P50 after dropping the page cache, us | 4.040 | 2.960 |

## Windows (windows-latest, job 132888144859)

| | redb (AwanFS's spill index) | segments (prototype) |
|---|---|---|
| inserts per second | 1,700 | 434,793 |
| build time, s | 5,881 | 22.999 |
| bytes on disk | 2,156,408,832 | 1,189,120,118 |
| peak RSS while building, MiB | 86.855 | 467 |
| open time, s | 0.026 | 0.042 |
| peak RSS while querying, MiB | 94.727 | 55.914 |
| read calls per get | 1.376 | 1.051 |
| get P50, us | 9.900 | 7.700 |
| get P99, us | 32.000 | 28.900 |
| read calls per absent get | 1.555 | 0.112 |
| absent get P50, us | 10.100 | 0.600 |
| absent get P99, us | 32.300 | 15.100 |
| prefix scan of up to 1000 keys, ms | 0.222 | 0.252 |

Cold open on Windows: not measured (the workflow drops the page cache only on Linux).

## What it says

- **Boot.** The server clears redb at every boot and rebuilds it with the same one-transaction-per-key puts (`meta_index.rs` `put_object`, `s3_api.rs` `SpillIndex::open` then `clear`). Its boot cost is therefore its build time: 70 minutes on Linux and 98 minutes on Windows for 10M keys. The segments open from their files in 0.018 to 0.047 s.
- **Segments win:** inserts (218x on Linux, 256x on Windows), disk (55% of redb's), present gets (fewer reads, and P50 and P99 lower on both platforms, cold and warm), absent gets (14x fewer reads, P50 12x lower on Linux and 17x lower on Windows), and memory while querying (28% less on Linux, 41% less on Windows).
- **redb wins:** memory while building (5.7x less on Linux, 5.4x less on Windows) and warm prefix scans (1.36x faster on Linux, 1.14x on Windows). On Linux after dropping the page cache the segments' scan is faster (0.102 against 0.127 ms; the `query_cold` line).
- **Both losses come from the prototype's shortcuts.** The build memory is the 1M-row memtable and a whole segment buffered before it is written. The scan cost is the merge across 10 uncompacted runs.
- **Fairness.** redb is measured as AwanFS uses it. Batching many puts into one redb transaction would raise its insert rate; by how much is not measured.
- **Single thread.** Concurrent writers are not measured. redb takes one writer at a time.

## Predictions made before the run (2026-11-16), against the result

| | predicted | measured (Linux / Windows) |
|---|---|---|
| redb inserts per second | 10K to 100K | 2,389 / 1,700: wrong, 4x to 60x lower |
| segment inserts per second | about 300K | 521K / 435K |
| redb disk | 1.5 to 3 GB | 2.16 GB |
| segment disk | about 1.2 GB | 1.19 GB |
| present get reads | about 1 each, a tie | 1.376 against 1.051: segments better than a tie |
| absent get reads | about 1 against 0.1 | 1.555 against 0.112 |
| scans | redb several times faster | redb 1.14x to 1.36x faster warm; segments faster cold: wrong, much closer |
| RSS querying | 70-80 against 40-50 MiB | 79-95 against 56-57 MiB |
| RSS building | about 100 against about 350 MiB | 66-87 against 378-467 MiB |
