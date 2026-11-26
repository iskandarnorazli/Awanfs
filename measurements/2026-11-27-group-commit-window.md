`gh api -X POST repos/iskandarnorazli/Awanfs/actions/workflows/bench-scaling.yml/dispatches -f ref=try/436-batching -F inputs[include_self_hosted]=false -F inputs[matrix]=true` (run 45884392925, Linux) and the same with `-F inputs[include_self_hosted]=true` (run 45876927099, the laptop)

Two runs of one experiment, from the unmerged branch `try/436-batching` (`main` at `de1959c` plus a prototype; not for merge). With `AWANFS_GROUP_COMMIT_DELAY_US` set, the group-commit leader waits up to that long for more PUTs before its sync; `AWANFS_GROUP_COMMIT_COUNT` stops the wait once that many PUTs are waiting. Each combination ran `python benchmarks/core_scaling.py --matrix --servers AwanFS --pattern seq-write --sizes 4KiB,64KiB --concurrencies 16,64,256 --cores <cores> --runs 2 --duration 5 --client-procs 8 --seed 42 --data-dir <runner temp> --output <runner temp>/core_scaling/window_d<delay>_n<count>.md --compare-fast`, with `python benchmarks/sync_probe.py --dir <runner temp>` before and after:
- Linux: `gh api -X POST repos/iskandarnorazli/Awanfs/actions/workflows/bench-scaling.yml/dispatches -f ref=try/436-batching -F inputs[include_self_hosted]=false -F inputs[matrix]=true` (run 45884392925, hosted `ubuntu-latest`, 4 logical cores, cores 2 and 4).
- Laptop: the same dispatch with `-F inputs[include_self_hosted]=true` (run 45876927099, LAPTOP-6RBIQUM5, Windows 11, 16 logical cores, cores 2, 4 and 8; the laptop job also wrote 2 GiB and timed one fsync before and after).

# Group-commit window: delay and count, measured (#436)

#436 asks whether a short wait before each shared sync, so that more PUTs join it, is worth two settings. Each cell is ops/s, then the change against delay 0 (computed), then P99 and PUTs per sync from the run's `/_awanfs/metrics` counters.

## Linux (run 45884392925)

| size | cores | in flight | 0 µs / 0 | 100 µs / 0 | 500 µs / 0 | 2000 µs / 0 | 100 µs / 32 | 500 µs / 32 | 2000 µs / 32 |
|---|---|---|---|---|---|---|---|---|---|
| 4 KiB | 2 | 16 | 6,076<br>P99 4.81 ms, 3.1 PUTs/sync | 7,055 (+16%)<br>P99 4.31 ms, 4.2 PUTs/sync | 6,361 (+5%)<br>P99 4.34 ms, 5.4 PUTs/sync | 4,645 (-24%)<br>P99 6.81 ms, 9.4 PUTs/sync | 7,239 (+19%)<br>P99 4.13 ms, 4.1 PUTs/sync | 6,490 (+7%)<br>P99 4.12 ms, 5.4 PUTs/sync | 4,761 (-22%)<br>P99 6.67 ms, 9.5 PUTs/sync |
| 4 KiB | 2 | 64 | 7,835<br>P99 18.24 ms, 4.2 PUTs/sync | 8,798 (+12%)<br>P99 16.45 ms, 5.1 PUTs/sync | 8,828 (+13%)<br>P99 15.94 ms, 7.5 PUTs/sync | 8,679 (+11%)<br>P99 14.00 ms, 16.9 PUTs/sync | 8,825 (+13%)<br>P99 16.34 ms, 5.2 PUTs/sync | 8,768 (+12%)<br>P99 16.16 ms, 7.5 PUTs/sync | 8,729 (+11%)<br>P99 14.16 ms, 14.1 PUTs/sync |
| 4 KiB | 2 | 256 | 8,302<br>P99 77.89 ms, 4.6 PUTs/sync | 7,928 (-5%)<br>P99 164.61 ms, 4.6 PUTs/sync | 8,378 (+1%)<br>P99 179.03 ms, 7.2 PUTs/sync | 8,340 (+0%)<br>P99 163.66 ms, 15.9 PUTs/sync | 8,032 (-3%)<br>P99 179.23 ms, 4.6 PUTs/sync | 8,361 (+1%)<br>P99 173.27 ms, 7.1 PUTs/sync | 8,347 (+1%)<br>P99 178.52 ms, 13.8 PUTs/sync |
| 4 KiB | 4 | 16 | 6,135<br>P99 4.98 ms, 3.6 PUTs/sync | 6,319 (+3%)<br>P99 4.48 ms, 4.3 PUTs/sync | 6,100 (-1%)<br>P99 4.72 ms, 5.3 PUTs/sync | 4,248 (-31%)<br>P99 7.36 ms, 8.6 PUTs/sync | 6,037 (-2%)<br>P99 4.85 ms, 4.3 PUTs/sync | 6,005 (-2%)<br>P99 4.65 ms, 5.3 PUTs/sync | 4,179 (-32%)<br>P99 7.49 ms, 8.7 PUTs/sync |
| 4 KiB | 4 | 64 | 8,400<br>P99 14.20 ms, 6.4 PUTs/sync | 8,252 (-2%)<br>P99 14.81 ms, 7.1 PUTs/sync | 8,594 (+2%)<br>P99 13.73 ms, 9.2 PUTs/sync | 8,556 (+2%)<br>P99 13.93 ms, 16.8 PUTs/sync | 8,382 (-0%)<br>P99 14.17 ms, 7.5 PUTs/sync | 8,334 (-1%)<br>P99 14.32 ms, 8.4 PUTs/sync | 8,658 (+3%)<br>P99 14.13 ms, 12.7 PUTs/sync |
| 4 KiB | 4 | 256 | 8,248<br>P99 64.20 ms, 6.4 PUTs/sync | 8,343 (+1%)<br>P99 60.92 ms, 6.9 PUTs/sync | 8,208 (-0%)<br>P99 63.66 ms, 8.8 PUTs/sync | 8,798 (+7%)<br>P99 57.99 ms, 17.5 PUTs/sync | 8,305 (+1%)<br>P99 63.53 ms, 7.2 PUTs/sync | 8,480 (+3%)<br>P99 63.31 ms, 8.5 PUTs/sync | 8,376 (+2%)<br>P99 62.83 ms, 12.6 PUTs/sync |
| 64 KiB | 2 | 16 | 3,370<br>P99 8.21 ms, 2.4 PUTs/sync | 3,391 (+1%)<br>P99 8.31 ms, 2.5 PUTs/sync | 3,476 (+3%)<br>P99 8.16 ms, 3.7 PUTs/sync | 3,158 (-6%)<br>P99 9.27 ms, 6.5 PUTs/sync | 3,401 (+1%)<br>P99 8.22 ms, 2.6 PUTs/sync | 3,452 (+2%)<br>P99 8.44 ms, 3.7 PUTs/sync | 3,244 (-4%)<br>P99 8.76 ms, 6.5 PUTs/sync |
| 64 KiB | 2 | 64 | 4,107<br>P99 36.70 ms, 3.3 PUTs/sync | 4,187 (+2%)<br>P99 37.38 ms, 3.4 PUTs/sync | 4,376 (+7%)<br>P99 37.74 ms, 5.0 PUTs/sync | 4,587 (+12%)<br>P99 34.46 ms, 11.1 PUTs/sync | 4,176 (+2%)<br>P99 39.19 ms, 3.6 PUTs/sync | 4,362 (+6%)<br>P99 37.58 ms, 4.9 PUTs/sync | 4,581 (+12%)<br>P99 34.99 ms, 10.4 PUTs/sync |
| 64 KiB | 2 | 256 | 4,248<br>P99 106.93 ms, 3.4 PUTs/sync | 4,371 (+3%)<br>P99 102.81 ms, 3.5 PUTs/sync | 4,527 (+7%)<br>P99 100.84 ms, 5.2 PUTs/sync | 4,790 (+13%)<br>P99 95.55 ms, 11.6 PUTs/sync | 4,369 (+3%)<br>P99 106.62 ms, 3.9 PUTs/sync | 4,561 (+7%)<br>P99 107.64 ms, 5.1 PUTs/sync | 4,771 (+12%)<br>P99 99.70 ms, 10.6 PUTs/sync |
| 64 KiB | 4 | 16 | 3,777<br>P99 7.73 ms, 2.7 PUTs/sync | 3,836 (+2%)<br>P99 7.64 ms, 3.0 PUTs/sync | 3,773 (-0%)<br>P99 7.98 ms, 4.0 PUTs/sync | 3,034 (-20%)<br>P99 10.08 ms, 6.8 PUTs/sync | 3,762 (-0%)<br>P99 7.91 ms, 3.3 PUTs/sync | 3,767 (-0%)<br>P99 7.80 ms, 4.0 PUTs/sync | 2,990 (-21%)<br>P99 10.27 ms, 7.0 PUTs/sync |
| 64 KiB | 4 | 64 | 5,162<br>P99 28.23 ms, 5.0 PUTs/sync | 5,169 (+0%)<br>P99 27.48 ms, 5.2 PUTs/sync | 5,237 (+1%)<br>P99 29.97 ms, 6.8 PUTs/sync | 5,336 (+3%)<br>P99 25.93 ms, 12.6 PUTs/sync | 5,125 (-1%)<br>P99 26.80 ms, 5.9 PUTs/sync | 5,220 (+1%)<br>P99 27.87 ms, 6.5 PUTs/sync | 5,410 (+5%)<br>P99 26.48 ms, 10.7 PUTs/sync |
| 64 KiB | 4 | 256 | 5,261<br>P99 106.13 ms, 5.3 PUTs/sync | 5,241 (-0%)<br>P99 99.96 ms, 5.3 PUTs/sync | 5,350 (+2%)<br>P99 100.24 ms, 7.1 PUTs/sync | 5,480 (+4%)<br>P99 98.80 ms, 12.7 PUTs/sync | 5,307 (+1%)<br>P99 98.34 ms, 6.3 PUTs/sync | 5,247 (-0%)<br>P99 108.02 ms, 6.8 PUTs/sync | 5,440 (+3%)<br>P99 95.59 ms, 11.0 PUTs/sync |
- **The gain is small and depends on the setting:**
  - Best case: +16% to +19% at 4 KiB, 2 cores, 16 in flight, with a 100 µs delay.
  - At 64 in flight: +11% to +13%.
  - At 4 cores (which share the client's cores on this runner): within ±3%, except +7% at 4 KiB with 256 in flight.
  - A 2 ms delay at 16 in flight costs 20% to 32%.
- **More PUTs share a sync, but throughput mostly does not follow:** from 3 to 6 PUTs per sync at delay 0 up to 17 at 2 ms. So on this runner the sync is not what limits PUT.

## Laptop (run 45876927099): Windows waits in 15.6 ms steps

| size | cores | in flight | 0 µs / 0 | 100 µs / 0 | 500 µs / 0 | 2000 µs / 0 | 100 µs / 32 | 500 µs / 32 | 2000 µs / 32 |
|---|---|---|---|---|---|---|---|---|---|
| 4 KiB | 2 | 16 | 3,857<br>P99 23.44 ms, 4.5 PUTs/sync | 953 (-75%)<br>P99 39.69 ms, 9.2 PUTs/sync | 954 (-75%)<br>P99 50.34 ms, 9.2 PUTs/sync | 955 (-75%)<br>P99 57.46 ms, 9.3 PUTs/sync | 957 (-75%)<br>P99 39.68 ms, 9.3 PUTs/sync | 950 (-75%)<br>P99 39.34 ms, 9.2 PUTs/sync | 949 (-75%)<br>P99 51.81 ms, 9.3 PUTs/sync |
| 4 KiB | 2 | 64 | 6,133<br>P99 27.22 ms, 7.4 PUTs/sync | 3,507 (-43%)<br>P99 46.17 ms, 33.4 PUTs/sync | 3,512 (-43%)<br>P99 46.27 ms, 33.4 PUTs/sync | 3,519 (-43%)<br>P99 45.10 ms, 33.5 PUTs/sync | 6,718 (+10%)<br>P99 22.92 ms, 15.7 PUTs/sync | 6,777 (+11%)<br>P99 23.30 ms, 16.0 PUTs/sync | 6,783 (+11%)<br>P99 23.13 ms, 16.0 PUTs/sync |
| 4 KiB | 2 | 256 | 5,825<br>P99 102.08 ms, 6.4 PUTs/sync | 7,102 (+22%)<br>P99 92.85 ms, 67.5 PUTs/sync | 7,113 (+22%)<br>P99 94.07 ms, 67.6 PUTs/sync | 7,122 (+22%)<br>P99 92.78 ms, 67.7 PUTs/sync | 6,429 (+10%)<br>P99 91.85 ms, 14.9 PUTs/sync | 6,606 (+13%)<br>P99 87.65 ms, 15.4 PUTs/sync | 6,672 (+15%)<br>P99 87.98 ms, 15.6 PUTs/sync |
| 4 KiB | 4 | 16 | 5,539<br>P99 5.47 ms, 4.6 PUTs/sync | 943 (-83%)<br>P99 40.24 ms, 9.3 PUTs/sync | 930 (-83%)<br>P99 46.52 ms, 9.4 PUTs/sync | 954 (-83%)<br>P99 37.27 ms, 9.2 PUTs/sync | 929 (-83%)<br>P99 60.32 ms, 9.3 PUTs/sync | 931 (-83%)<br>P99 61.91 ms, 9.2 PUTs/sync | 928 (-83%)<br>P99 57.63 ms, 9.2 PUTs/sync |
| 4 KiB | 4 | 64 | 10,382<br>P99 13.85 ms, 14.6 PUTs/sync | 3,493 (-66%)<br>P99 46.86 ms, 34.2 PUTs/sync | 3,500 (-66%)<br>P99 46.85 ms, 33.9 PUTs/sync | 3,504 (-66%)<br>P99 46.75 ms, 33.9 PUTs/sync | 10,096 (-3%)<br>P99 15.29 ms, 15.1 PUTs/sync | 10,199 (-2%)<br>P99 14.87 ms, 15.5 PUTs/sync | 10,299 (-1%)<br>P99 14.37 ms, 15.2 PUTs/sync |
| 4 KiB | 4 | 256 | 10,765<br>P99 52.53 ms, 16.7 PUTs/sync | 8,973 (-17%)<br>P99 87.02 ms, 87.0 PUTs/sync | 9,091 (-16%)<br>P99 81.53 ms, 86.3 PUTs/sync | 9,021 (-16%)<br>P99 84.51 ms, 86.9 PUTs/sync | 10,386 (-4%)<br>P99 55.97 ms, 16.6 PUTs/sync | 10,846 (+1%)<br>P99 51.65 ms, 17.1 PUTs/sync | 10,722 (-0%)<br>P99 52.48 ms, 17.0 PUTs/sync |
| 4 KiB | 8 | 16 | 6,173<br>P99 5.36 ms, 4.6 PUTs/sync | 931 (-85%)<br>P99 39.80 ms, 9.2 PUTs/sync | 970 (-84%)<br>P99 39.53 ms, 9.1 PUTs/sync | 933 (-85%)<br>P99 62.11 ms, 9.3 PUTs/sync | 946 (-85%)<br>P99 40.03 ms, 9.2 PUTs/sync | 965 (-84%)<br>P99 31.88 ms, 9.2 PUTs/sync | 928 (-85%)<br>P99 58.05 ms, 9.2 PUTs/sync |
| 4 KiB | 8 | 64 | 11,421<br>P99 12.43 ms, 15.8 PUTs/sync | 3,467 (-70%)<br>P99 46.64 ms, 33.7 PUTs/sync | 4,127 (-64%)<br>P99 39.62 ms, 30.4 PUTs/sync | 3,474 (-70%)<br>P99 46.34 ms, 34.5 PUTs/sync | 11,279 (-1%)<br>P99 12.90 ms, 15.7 PUTs/sync | 11,477 (+0%)<br>P99 12.21 ms, 15.7 PUTs/sync | 11,553 (+1%)<br>P99 12.29 ms, 15.8 PUTs/sync |
| 4 KiB | 8 | 256 | 12,620<br>P99 50.34 ms, 29.0 PUTs/sync | 9,100 (-28%)<br>P99 82.38 ms, 88.0 PUTs/sync | 10,921 (-13%)<br>P99 66.75 ms, 75.6 PUTs/sync | 9,093 (-28%)<br>P99 83.67 ms, 88.3 PUTs/sync | 13,192 (+5%)<br>P99 41.27 ms, 29.2 PUTs/sync | 12,908 (+2%)<br>P99 43.95 ms, 27.7 PUTs/sync | 13,322 (+6%)<br>P99 42.81 ms, 29.3 PUTs/sync |
| 64 KiB | 2 | 16 | 2,003<br>P99 16.52 ms, 3.5 PUTs/sync | 969 (-52%)<br>P99 31.95 ms, 9.3 PUTs/sync | 973 (-51%)<br>P99 31.90 ms, 9.3 PUTs/sync | 972 (-51%)<br>P99 31.87 ms, 9.4 PUTs/sync | 975 (-51%)<br>P99 31.88 ms, 9.3 PUTs/sync | 971 (-52%)<br>P99 32.09 ms, 9.3 PUTs/sync | 969 (-52%)<br>P99 31.96 ms, 9.2 PUTs/sync |
| 64 KiB | 2 | 64 | 1,268<br>P99 99.44 ms, 2.1 PUTs/sync | 1,970 (+55%)<br>P99 64.10 ms, 18.9 PUTs/sync | 2,006 (+58%)<br>P99 64.41 ms, 19.2 PUTs/sync | 1,945 (+53%)<br>P99 68.94 ms, 19.1 PUTs/sync | 1,989 (+57%)<br>P99 65.00 ms, 13.2 PUTs/sync | 1,954 (+54%)<br>P99 64.76 ms, 13.0 PUTs/sync | 1,815 (+43%)<br>P99 71.63 ms, 13.2 PUTs/sync |
| 64 KiB | 2 | 256 | 1,395<br>P99 814.26 ms, 2.0 PUTs/sync | 1,314 (-6%)<br>P99 862.23 ms, 13.0 PUTs/sync | 1,340 (-4%)<br>P99 791.15 ms, 13.1 PUTs/sync | 1,347 (-3%)<br>P99 890.72 ms, 12.9 PUTs/sync | 1,308 (-6%)<br>P99 823.80 ms, 12.6 PUTs/sync | 1,315 (-6%)<br>P99 759.20 ms, 12.7 PUTs/sync | 1,450 (+4%)<br>P99 819.08 ms, 13.3 PUTs/sync |
| 64 KiB | 4 | 16 | 2,027<br>P99 33.69 ms, 2.7 PUTs/sync | 972 (-52%)<br>P99 31.82 ms, 9.3 PUTs/sync | 979 (-52%)<br>P99 31.72 ms, 9.3 PUTs/sync | 969 (-52%)<br>P99 31.85 ms, 9.3 PUTs/sync | 976 (-52%)<br>P99 32.08 ms, 9.3 PUTs/sync | 974 (-52%)<br>P99 31.93 ms, 9.3 PUTs/sync | 975 (-52%)<br>P99 31.73 ms, 9.3 PUTs/sync |
| 64 KiB | 4 | 64 | 1,404<br>P99 92.87 ms, 3.5 PUTs/sync | 2,191 (+56%)<br>P99 61.77 ms, 21.1 PUTs/sync | 2,229 (+59%)<br>P99 60.48 ms, 21.3 PUTs/sync | 2,217 (+58%)<br>P99 59.15 ms, 21.4 PUTs/sync | 2,333 (+66%)<br>P99 61.02 ms, 13.1 PUTs/sync | 2,337 (+66%)<br>P99 60.65 ms, 13.3 PUTs/sync | 2,344 (+67%)<br>P99 61.68 ms, 13.7 PUTs/sync |
| 64 KiB | 4 | 256 | 1,574<br>P99 632.72 ms, 1.9 PUTs/sync | 1,390 (-12%)<br>P99 763.80 ms, 13.3 PUTs/sync | 1,335 (-15%)<br>P99 849.94 ms, 12.8 PUTs/sync | 1,310 (-17%)<br>P99 813.75 ms, 12.6 PUTs/sync | 1,253 (-20%)<br>P99 843.70 ms, 11.8 PUTs/sync | 1,227 (-22%)<br>P99 825.44 ms, 11.6 PUTs/sync | 1,234 (-22%)<br>P99 909.47 ms, 11.6 PUTs/sync |
| 64 KiB | 8 | 16 | 1,923<br>P99 20.84 ms, 2.4 PUTs/sync | 975 (-49%)<br>P99 31.83 ms, 9.3 PUTs/sync | 1,014 (-47%)<br>P99 31.85 ms, 8.8 PUTs/sync | 967 (-50%)<br>P99 32.09 ms, 9.4 PUTs/sync | 970 (-50%)<br>P99 31.93 ms, 9.4 PUTs/sync | 972 (-49%)<br>P99 31.97 ms, 9.3 PUTs/sync | 975 (-49%)<br>P99 31.80 ms, 9.3 PUTs/sync |
| 64 KiB | 8 | 64 | 1,457<br>P99 84.32 ms, 2.1 PUTs/sync | 2,183 (+50%)<br>P99 62.01 ms, 21.2 PUTs/sync | 2,262 (+55%)<br>P99 58.82 ms, 19.2 PUTs/sync | 2,201 (+51%)<br>P99 57.55 ms, 21.2 PUTs/sync | 2,341 (+61%)<br>P99 60.83 ms, 13.1 PUTs/sync | 2,323 (+59%)<br>P99 63.19 ms, 13.4 PUTs/sync | 2,342 (+61%)<br>P99 60.13 ms, 13.6 PUTs/sync |
| 64 KiB | 8 | 256 | 1,688<br>P99 681.27 ms, 1.9 PUTs/sync | 1,342 (-20%)<br>P99 804.28 ms, 12.8 PUTs/sync | 1,332 (-21%)<br>P99 768.05 ms, 12.4 PUTs/sync | 1,317 (-22%)<br>P99 861.69 ms, 12.7 PUTs/sync | 1,175 (-30%)<br>P99 892.48 ms, 11.1 PUTs/sync | 1,219 (-28%)<br>P99 915.59 ms, 11.6 PUTs/sync | 1,231 (-27%)<br>P99 858.40 ms, 11.7 PUTs/sync |
- **Every delay from 100 µs to 2 ms gave the same numbers.** At 16 in flight that was about 950 ops/s with 9.2 PUTs per sync: about 16 writers per 16 ms (computed).
  - That matches Windows' default timer step (15.6 ms). A `Condvar::wait_timeout` shorter than a step sleeps a whole step.
  - A Windows build needs a high-resolution wait before a sub-millisecond delay means what it says. That is inferred from the numbers, not tested.
- **Batching by count reached +55% to +67% at 64 KiB with 64 in flight,** but from an odd baseline. At delay 0, 64 KiB PUTs with 64 in flight ran slower than with 16 (1,268 against 2,003 ops/s at 2 cores). That is not explained.
- **The laptop's disk measured fast this time:** 2 GiB then one fsync took 1.69 s (1,210.1 MiB/s) before, and 1.82 s (1,127.0 MiB/s) after. The earlier laptop run (45876505911) measured 155.9 and 629.7 MiB/s. So the disk varies between runs.

## Also measured on the laptop (#430 steps 2 and 3), run 45876505911

`gh api -X POST repos/iskandarnorazli/Awanfs/actions/workflows/bench-scaling.yml/dispatches -f ref=try/430-laptop -F inputs[include_self_hosted]=true -F inputs[matrix]=true` (branch `try/430-laptop`, not merged), which ran `python benchmarks/core_scaling.py --matrix --servers AwanFS --runs 3 --duration 5 --client-procs 8 --seed 42 --data-dir <runner temp> --output <runner temp>/core_scaling/<name>.md --compare-fast --pattern seq-write --sizes 4KiB --concurrencies 16,64 --cores 1,2,4,8`, once with tokio's default worker count (16) and once with `TOKIO_WORKER_THREADS` set to the pinned core count.

| PUT 4 KiB, group commit | 16 workers: ops/s, P99 | workers = cores: ops/s, P99 |
|---|---|---|
| 1 core, c=16 | 2,512, 33.85 ms | 2,928, 10.59 ms |
| 1 core, c=64 | 3,152, 91.52 ms | 2,906, 35.01 ms |
| 2 cores, c=16 | 4,882, 6.22 ms | 5,007, 5.87 ms |
| 2 cores, c=64 | 6,241, 25.53 ms | 5,820, 19.03 ms |
| 4 cores, c=16 | 5,940, 5.03 ms | 6,025, 5.02 ms |
| 4 cores, c=64 | 10,599, 13.51 ms | 9,872, 11.49 ms |
| 8 cores, c=16 | 6,245, 4.84 ms | 6,173, 4.91 ms |
| 8 cores, c=64 | 11,507, 12.06 ms | 11,951, 11.79 ms |

Matching the worker count to the cores does not raise throughput: the changes run from −8% to +17%, either way. It lowers P99 on 1 core (33.85 to 10.59 ms at c=16). #430's guess, that 16 workers on one core cost throughput, is not supported.

The same run's PUT 1 MiB and 4 MiB with thread samples (cores 4 and 8, c=64) found no single hot thread: CPU spread over the tokio workers, 6% to 8% each. The 2 GiB write took 13.13 s (155.9 MiB/s) before and 3.25 s (629.7 MiB/s) after, so that run's large-object numbers are as noisy as its disk.
