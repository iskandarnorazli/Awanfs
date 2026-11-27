`for round in 1 2 3; do for cores in 1 2; do for build in before after; do [ "$build" = after ] && export AWANFS_WORKER_THREADS=$cores || unset AWANFS_WORKER_THREADS; python -u benchmarks/core_scaling.py --matrix --servers AwanFS --pattern seq-write --sizes 4KiB --concurrencies 16,64 --cores $cores --runs 1 --duration 5 --no-compare-fast --data-dir "$RUNNER_TEMP" --server-bin "$RUNNER_TEMP/bin/$build.exe" --output "$RUNNER_TEMP/r${round}_${cores}_${build}.md"; done; done; done` (workflow `.github/workflows/try-456-ab.yml` on the unmerged branch `try/456-ab`, job `ab` on windows-latest, run 45915521019; then the same loop at 2 cores only, six rounds, run 45918480087; `before` is `main`, `after` is this branch, both built in the same job)

# Tokio worker count on Windows (#456)

`core_scaling.py` pins the server after it starts on Windows (`_spawn_pinned` then `pin_process`), so a server reading its affinity at startup still sees every core; #474 fixes that. Here the `after` build is given the count the new default picks for a server started pinned, through `AWANFS_WORKER_THREADS` ([owner decision](https://github.com/iskandarnorazli/Awanfs/issues/456#issuecomment-7206588750)). `before` runs tokio's default: one worker per logical processor, 4 on this runner. The pin-at-start path is tested by `worker_threads_test::a_server_started_on_two_cores_runs_two_workers`.

## Run 45915521019 (AMD64 Family 26, 4 logical processors), PUT 4 KiB

| Round | Cores | Build | c=16 ops/s | c=16 P99 ms | c=64 ops/s | c=64 P99 ms |
|---|---|---|---|---|---|---|
| 1 | 1 | before | 4,848 | 56.61 | 4,963 | 98.29 |
| 1 | 1 | after | 5,472 | 5.39 | 5,429 | 24.31 |
| 2 | 1 | before | 5,405 | 47.64 | 5,347 | 84.45 |
| 2 | 1 | after | 5,400 | 7.69 | 5,403 | 28.29 |
| 3 | 1 | before | 5,225 | 49.61 | 5,482 | 85.21 |
| 3 | 1 | after | 5,471 | 5.40 | 5,381 | 28.15 |
| 1 | 2 | before | 11,635 | 3.35 | 11,338 | 12.99 |
| 1 | 2 | after | 11,716 | 2.77 | 11,495 | 12.03 |
| 2 | 2 | before | 11,931 | 3.22 | 11,070 | 12.72 |
| 2 | 2 | after | 9,151 | 15.20 | 10,350 | 21.16 |
| 3 | 2 | before | 10,650 | 4.24 | 10,261 | 18.45 |
| 3 | 2 | after | 10,641 | 3.40 | 9,824 | 26.91 |

One `after` round at 2 cores (round 2) fell below every `before` round, so 2 cores ran again with six rounds.

## Run 45918480087 (2 cores only, six rounds), PUT 4 KiB

| Round | Build | c=16 ops/s | c=16 P99 ms | c=64 ops/s | c=64 P99 ms |
|---|---|---|---|---|---|
| 1 | before | 6,658 | 5.79 | 6,636 | 21.82 |
| 1 | after | 6,646 | 4.71 | 6,897 | 20.27 |
| 2 | before | 5,393 | 15.85 | 5,895 | 30.91 |
| 2 | after | 6,115 | 7.28 | 6,466 | 27.68 |
| 3 | before | 6,718 | 5.67 | 6,729 | 21.44 |
| 3 | after | 6,759 | 4.64 | 6,866 | 18.51 |
| 4 | before | 6,782 | 5.90 | 6,930 | 20.50 |
| 4 | after | 6,733 | 4.63 | 7,003 | 17.72 |
| 5 | before | 6,658 | 6.21 | 6,468 | 22.17 |
| 5 | after | 6,743 | 4.66 | 6,712 | 19.87 |
| 6 | before | 6,949 | 5.84 | 6,819 | 20.27 |
| 6 | after | 6,699 | 4.70 | 6,768 | 20.34 |

## Result

- 1 core: throughput 4,848 to 5,482 before, 5,381 to 5,472 after: within the runs' spread. P99 at c=16 47.64 to 56.61 ms before, 5.39 to 7.69 ms after; at c=64 84.45 to 98.29 ms before, 24.31 to 28.29 ms after.
- 2 cores, six rounds: c=16 5,393 to 6,949 before, 6,115 to 6,759 after; c=64 5,895 to 6,930 before, 6,466 to 7,003 after: within the runs' spread. P99 at c=16 5.67 to 15.85 ms before, 4.63 to 7.28 ms after.
- The two runs had different machines (the second runner wrote about 40% fewer ops/s for both builds), so they are not compared with each other.
- Linux: not measured; its default does not change (`available_parallelism` already follows the affinity there).
