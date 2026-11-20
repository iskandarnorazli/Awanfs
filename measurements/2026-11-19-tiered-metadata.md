`gh workflow run bench-metaindex.yml --ref issue/26-switch -f stores="redb prod"` (run 44550797419, commit ee54fca: KEYS=10000000 GETS=1000000 SCANS=1000), then `python benchmarks/meta_index_report.py results.jsonl` on each job's JSON lines

# Metadata index at 10M keys: redb against the production segments (#26 stage 6, decision 59)

Run [44550797419](https://github.com/iskandarnorazli/Awanfs/actions/runs/44550797419), GitHub-hosted
runners (`ubuntu-latest` and `windows-latest`, shared 4-vCPU VMs), one thread. Both stores ran one
after the other in the same job, so each table compares them on one machine; absolute numbers move
between runs (redb's Linux insert rate was 2,121, 4,184 and 2,388 per second in three runs).

`redb` is the spill index before #26 stage 6. `segments` is `s3_server::spill::SpillIndex` with the
server's 64 MiB spill budget (a memtable of a quarter of it, then levelled segments, decision 68).
Peak heap is counted by the benchmark's allocator; RSS by the #109 sampler.

## Linux (job 135653671367)

| | redb | segments |
|---|---|---|
| inserts per second | 2,388 | 131,455 |
| build time, s | 4,187 | 76.071 |
| bytes on disk | 2,156,408,832 | 1,243,960,136 |
| peak RSS while building, MiB | 66.039 | 54.730 |
| open time, s | 0.001 | 0.016 |
| peak RSS while querying, MiB | 79.426 | 49.441 |
| read calls per get | 1.376 | 1.042 |
| get P50, us | 3.230 | 2.400 |
| get P99, us | 5.820 | 4.500 |
| read calls per absent get | 1.555 | 0.056 |
| absent get P50, us | 3.640 | 0.340 |
| absent get P99, us | 4.600 | 2.680 |
| prefix scan of up to 1000 keys, ms | 0.087 | 0.072 |
| open time after dropping the page cache, s | 0.165 | 0.057 |
| get P50 after dropping the page cache, us | 4.550 | 2.550 |

Answers hash: redb 47cff400f22342b1, segments 47cff400f22342b1. Wrong answers: 0.

## Windows (job 135653671956)

| | redb | segments |
|---|---|---|
| inserts per second | 1,414 | 115,428 |
| build time, s | 7,073 | 86.634 |
| bytes on disk | 2,156,408,832 | 1,243,960,136 |
| peak RSS while building, MiB | 86.852 | 60.293 |
| peak heap while building, MiB | 62.482 | 49.222 |
| open time, s | 0.034 | 0.037 |
| peak RSS while querying, MiB | 94.703 | 48.430 |
| read calls per get | 1.376 | 1.042 |
| get P50, us | 12.100 | 8.100 |
| get P99, us | 57.800 | 35.400 |
| read calls per absent get | 1.555 | 0.056 |
| absent get P50, us | 13.200 | 0.800 |
| absent get P99, us | 81.400 | 11.600 |
| prefix scan of up to 1000 keys, ms | 0.268 | 0.188 |

Answers hash: redb 47cff400f22342b1, segments 47cff400f22342b1. Wrong answers: 0. (Windows has no
page-cache drop, so no cold rows.)

## #26's gate lines for this benchmark

| Line | Linux | Windows |
|---|---|---|
| same answers hash, 0 wrong | yes | yes |
| peak RSS while building at most redb's | 54.7 ≤ 66.0 | 60.3 ≤ 86.9 |
| prefix scans at least as fast as redb's | 0.072 ≤ 0.087 ms | 0.188 ≤ 0.268 ms |
| at most 1.1 reads per present lookup | 1.042 | 1.042 |
| at most 0.15 reads per absent lookup | 0.056 | 0.056 |
| open under 1 s | 0.016 s | 0.037 s |
| 64 concurrent writers, both stores | not measured | not measured |

## How the two failing lines were fixed

The first full run, [44499924465](https://github.com/iskandarnorazli/Awanfs/actions/runs/44499924465)
(commit before 09a0569), missed two lines: peak RSS while building on Linux (83.0 MiB against
redb's 66.2) and warm prefix scans on both platforms (Linux 0.152 ms against 0.118, Windows 0.261
against 0.229).

- Scans: each scanned key was allocated four times (the block parse, a cursor clone, a merge-heap
  clone, the bucket-prefix copy). Commit 09a0569 keeps the first only.
- Memory: commit 09a0569 stores a segment's fences in one buffer instead of a vector per block.
  Commit 912af8c gives the memtable a quarter of the spill budget instead of half. The experiment
  that chose it, run [44522807804](https://github.com/iskandarnorazli/Awanfs/actions/runs/44522807804)
  (the production store alone at three budgets; `prod@M` is a budget of M MiB):

| store (budget, memtable at the then half share) | Linux RSS building, MiB | Linux inserts/s | Windows RSS building, MiB | Windows inserts/s | peak heap, MiB |
|---|---|---|---|---|---|
| prod (64 MiB, memtable 32 MiB) | 73.094 | 326,379 | 77.148 | 155,026 | 66.340 |
| prod@32 (32 MiB, memtable 16 MiB) | 55.867 | 236,382 | 59.242 | 119,135 | 49.222 |
| prod@16 (16 MiB, memtable 8 MiB) | 47.051 | 149,015 | 51.781 | 80,469 | 38.854 |

A memtable of 16 MiB is what the server now gets from its 64 MiB budget at a quarter.

Peak heap fell from 66.340 MiB to 49.222 MiB with the quarter share; 28.93 MiB of it is what the open
store keeps after the build (the segments' Bloom filters and fences).
