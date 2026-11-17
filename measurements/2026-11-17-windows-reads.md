gh workflow run bench-full.yml --ref issue/175-windows-reads

# Windows reads and mixed load: Experiments 2 & 3 (#175)

Experiments 2 (CPU per process via `benchmarks/run_warp_benchmark.py::get_process_cpu_times`) and 3 (`stream.set_nodelay(true)` in `server/src/reactor.rs`) measured on PR #231 (`bench-full.yml`, run `43986899662`, GitHub `ubuntu-latest` and `windows-latest` 4-CPU runners).

## Experiment 3: `TCP_NODELAY` (`43986899662`)

| Workload | `ubuntu-latest` (AwanFS / MinIO) | `windows-latest` (AwanFS / MinIO) |
|---|---|---|
| 64 KiB GET | `13404.3 / 7300.6` ops/s (**1.84x**) | `4623.6 / 3289.1` ops/s (**1.41x**) |
| 64 KiB Range GET (4 KiB) | `14024.6 / 6982.8` ops/s (**2.01x**) | `6093.8 / 3887.8` ops/s (**1.57x**) |
| 64 KiB Mixed (80/20) | `10130.4 / 3759.6` ops/s (**2.69x**) | `4576.8 / 2513.4` ops/s (**1.82x**) |

## Experiment 2: CPU breakdown per process (`windows-latest`, `43986899662`)

| Phase | Server CPU (s) | Client CPU (s) | System/Kernel CPU (s) |
|---|---|---|---|
| `get_64k` | 1.28 | 2.73 | 1.92 |
| `range_get_4k` | 1.34 | 2.92 | 1.75 |
| `get_1m` | 1.27 | 5.02 | 4.41 |
| `mixed_64k` | 2.09 | 2.97 | 2.45 |
| `mixed_1m` | 2.95 | 1.81 | 2.56 |

On `windows-latest` (4 shared CPUs), the Python benchmark client consumes **2.1x–4.0x** more CPU time than `s3-server` on GET phases (`2.73s` vs `1.28s` on `get_64k`; `5.02s` vs `1.27s` on `get_1m`), alongside `1.75s–4.41s` of Windows kernel/loopback time.

## Remaining experiments
- Experiment 1 (core-pinned client/server separation on >8 cores), Experiment 4 (Windows Defender exclusion), and Experiment 5 (Windows `budget_rss.py` under self-hosted workload) remain **not measured** while self-hosted runners (`awanfs-bench`, `LAPTOP-6RBIQUM5`) are offline.
