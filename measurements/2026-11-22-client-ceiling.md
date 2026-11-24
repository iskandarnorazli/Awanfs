Command: `gh workflow run bench-scaling.yml --ref issue/326-core-scaling-client-procs -f cores=1,2,4 -f matrix=false` (run [44964873504](https://github.com/iskandarnorazli/Awanfs/actions/runs/44964873504), commit `ae9a37e`), running `python -u benchmarks/core_scaling.py --ceiling-probe --runs 2 --duration 2 --data-dir "$RUNNER_TEMP" --output "$RUNNER_TEMP/core_scaling/client_ceiling.md"`

# Client Ceiling Probe: Trivial Local HTTP Server (#326, 2026-11-22)

Measures `--client-procs 1` vs `--client-procs 4` at `concurrency = 32`, `size = 4 KiB` against the repository's local non-S3 Rust `std::net` HTTP/1.1 keep-alive responder (`benchmarks/core_scaling.py`) on 4-vCPU GitHub-hosted runners (`ubuntu-latest` and `windows-latest`, 2 alternating runs per point, medians reported).

## Dispatched run (`44964873504`, `-f cores=1,2,4 -f matrix=false`)

### Linux (`ubuntu-latest`, job 137031519208, AMD EPYC 9V74, 4 logical cores, 15.6 GB RAM)

Probe command on runner: `python benchmarks/core_scaling.py --ceiling-probe --runs 2 --duration 2 --data-dir /home/runner/work/_temp --output /home/runner/work/_temp/core_scaling/client_ceiling.md`

| Workload | Concurrency | Client Procs | Client Cores | ops/s | MiB/s | P50 (ms) | P99 (ms) | Client CPU % | Server CPU % | Speedup vs 1 proc |
|---|---|---|---|---|---|---|---|---|---|---|
| PUT 4 KiB | 32 | 1 | 4 | 8,468 | 33.08 | 3.37 | 9.91 | 163.8% (41%) | 17.3% | 1.00x |
| PUT 4 KiB | 32 | 4 | 4 | 26,248 | 102.53 | 1.20 | 2.49 | 348.4% (87%) | 50.4% | 3.10x |
| GET 4 KiB | 32 | 1 | 4 | 10,595 | 41.39 | 2.39 | 9.55 | 142.3% (36%) | 17.7% | 1.00x |
| GET 4 KiB | 32 | 4 | 4 | 30,133 | 117.71 | 1.03 | 2.32 | 352.1% (88%) | 46.2% | 2.84x |

- **Scaling (4/1 client procs):** PUT 4 KiB: 3.10x (8,468 -> 26,248 ops/s); GET 4 KiB: 2.84x (10,595 -> 30,133 ops/s).
- On this 4-core VM, `--client-procs 4` pushes total CPU (client + trivial server) to `398.8%` on PUT (`348.4% + 50.4%`) and `398.3%` on GET (`352.1% + 46.2%`) out of `400%` host capacity.

### Windows (`windows-latest`, job 137031518855, Windows-2025Server, 4 logical cores, 16.0 GB RAM)

Probe command on runner: `python benchmarks/core_scaling.py --ceiling-probe --runs 2 --duration 2 --data-dir D:\a\_temp --output D:\a\_temp/core_scaling/client_ceiling.md`

| Workload | Concurrency | Client Procs | Client Cores | ops/s | MiB/s | P50 (ms) | P99 (ms) | Client CPU % | Server CPU % | Speedup vs 1 proc |
|---|---|---|---|---|---|---|---|---|---|---|
| PUT 4 KiB | 32 | 1 | 4 | 3,967 | 15.50 | 7.58 | 18.51 | 150.7% (38%) | 16.2% | 1.00x |
| PUT 4 KiB | 32 | 4 | 4 | 15,164 | 59.24 | 0.22 | 35.06 | 325.7% (81%) | 42.1% | 3.82x |
| GET 4 KiB | 32 | 1 | 4 | 4,666 | 18.23 | 6.42 | 17.79 | 130.0% (32%) | 10.8% | 1.00x |
| GET 4 KiB | 32 | 4 | 4 | 18,281 | 71.41 | 0.18 | 31.35 | 329.0% (82%) | 43.8% | 3.92x |

- **Scaling (4/1 client procs):** PUT 4 KiB: 3.82x (3,967 -> 15,164 ops/s); GET 4 KiB: 3.92x (4,666 -> 18,281 ops/s).

## PR check run on the same commit ([44964877433](https://github.com/iskandarnorazli/Awanfs/actions/runs/44964877433), commit `ae9a37e`)

### Linux (`ubuntu-latest`, job 137031528625, Intel Xeon 6975P-C, 4 logical cores, 15.3 GB RAM)

| Workload | Concurrency | Client Procs | Client Cores | ops/s | MiB/s | P50 (ms) | P99 (ms) | Client CPU % | Server CPU % | Speedup vs 1 proc |
|---|---|---|---|---|---|---|---|---|---|---|
| PUT 4 KiB | 32 | 1 | 4 | 11,288 | 44.09 | 2.39 | 8.84 | 153.2% (38%) | 14.1% | 1.00x |
| PUT 4 KiB | 32 | 4 | 4 | 45,713 | 178.56 | 0.68 | 1.37 | 362.2% (91%) | 37.6% | 4.05x |
| GET 4 KiB | 32 | 1 | 4 | 14,854 | 58.02 | 1.58 | 9.60 | 140.4% (35%) | 13.8% | 1.00x |
| GET 4 KiB | 32 | 4 | 4 | 50,564 | 197.52 | 0.62 | 1.25 | 365.6% (91%) | 33.3% | 3.40x |

- **Scaling (4/1 client procs):** PUT 4 KiB: 4.05x (11,288 -> 45,713 ops/s); GET 4 KiB: 3.40x (14,854 -> 50,564 ops/s).

### Windows (`windows-latest`, job 137031528819, Windows-2025Server, 4 logical cores, 16.0 GB RAM)

| Workload | Concurrency | Client Procs | Client Cores | ops/s | MiB/s | P50 (ms) | P99 (ms) | Client CPU % | Server CPU % | Speedup vs 1 proc |
|---|---|---|---|---|---|---|---|---|---|---|
| PUT 4 KiB | 32 | 1 | 4 | 3,730 | 14.57 | 8.05 | 20.17 | 150.0% (38%) | 12.7% | 1.00x |
| PUT 4 KiB | 32 | 4 | 4 | 12,511 | 48.87 | 0.21 | 36.51 | 268.2% (67%) | 32.1% | 3.35x |
| GET 4 KiB | 32 | 1 | 4 | 4,507 | 17.61 | 6.67 | 18.64 | 128.7% (32%) | 11.6% | 1.00x |
| GET 4 KiB | 32 | 4 | 4 | 16,319 | 63.75 | 0.19 | 37.61 | 291.7% (73%) | 41.4% | 3.62x |

- **Scaling (4/1 client procs):** PUT 4 KiB: 3.35x (3,730 -> 12,511 ops/s); GET 4 KiB: 3.62x (4,507 -> 16,319 ops/s).
