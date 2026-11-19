gh workflow run bench-selfhosted.yml --ref issue/175-windows-reads -f runs=3 -f duration=30

# Windows reads and mixed load: Experiments 1–5 (#175)

Completes all five experiments for #175 across self-hosted 16-core Windows (`bench-selfhosted.yml`, run `44282884671` on `LAPTOP-6RBIQUM5`, Windows 11 AMD64, 16 logical CPUs, 31.4 GB RAM) and PR #255's hosted runs (`bench-full.yml` run `44256981929`, `bench-ratio.yml` run `44256981862` on `ubuntu-latest` and `windows-latest`, compared with pre-exclusion PR #231 `bench-full.yml` run `43986899662`).

---

## Experiment 1: Core-separated client vs server (`LAPTOP-6RBIQUM5`, 16 CPUs)

Commands (inside `bench-selfhosted.yml`, run `44282884671`):
- `python -u benchmarks/run_warp_benchmark.py --sizes 64KiB,1MiB,1GiB --concurrent 64 --duration 30s --output selfhosted_full.md` (Server pinned to `Cores 0..7`, Affinity Mask `0x00FF`; Client pinned to `Cores 8..15`, Affinity Mask `0xFF00`)
- `python -u benchmarks/bench.py --runs 3 --duration 30 --output selfhosted_ratio.md` (invoking `ratio_check.py --runs 3 --duration 30.0`, same `0x00FF` / `0xFF00` pin)

Linux reference (PR #255, same commit `c682a04`, `ubuntu-latest` 4 CPUs unpinned):
- `python benchmarks/run_warp_benchmark.py --sizes 64KiB,1MiB --concurrent 64 --duration 2 --output ci_full_matrix.md` (`bench-full.yml`, run `44256981929`)
- `python benchmarks/ratio_check.py --server-bin server/target/release/s3-server --minio-bin .tmp/minio/minio --mc-bin .tmp/minio/mc --runs 5 --duration 3 --report ratio_report.md` (`bench-ratio.yml`, run `44256981862`)

### Core-separated Windows vs Linux ratio summary (AwanFS ops/s over MinIO's)

| Workload | Linux (`44256981929`, 4 CPUs unpinned) | Windows (`44282884671`, 16 CPUs `0x00FF`/`0xFF00`) |
|---|---|---|
| 64 KiB GET | **1.54x** (`5490.1 / 3565.2` ops/s; ratio check `1.60x`) | **1.60x** (`5388.1 / 3371.0` ops/s; ratio check `1.61x`) |
| 64 KiB range GET | **1.65x** (`5771.3 / 3501.2` ops/s; ratio check `1.64x`) | **1.55x** (`5623.9 / 3639.0` ops/s; ratio check `1.56x`) |
| 64 KiB mixed | **1.65x** (`4906.2 / 2973.9` ops/s; ratio check `3.01x`) | **7.17x** (`4509.5 / 628.9` ops/s; ratio check `6.51x`) |
| mixed, 8 to 64 clients | **1.65x to 1.93x** (C=8: `1.73x`, C=32: `1.65x`, C=64: `1.93x`) | **3.79x to 7.84x** (C=8: `3.79x`, C=32: `7.84x`, C=64: `7.70x`) |

Note on `44282884671` Step 7 (`ratio_check.py`): all 3 runs completed and wrote `selfhosted_ratio.md`, exiting 1 solely because Windows 16-core loopback `get_1m` (`1818.89` vs `1155.57 MiB/s`, ratio `1.57x`) is checked against the Linux `ratio_baseline.json` (`2.74`, allowed down to `1.64`). `ratio_baseline.json` was not altered; on `ubuntu-latest` (`44256981862`), all 6 ratio rows passed (`ok` / `faster: raise the baseline`). Note also that on `44282884671`, `Add-MpPreference` failed due to non-admin runner permissions (see Experiment 4), which left Defender scanning MinIO's per-object writes on mixed phases (`628.9` ops/s vs AwanFS `4509.5` ops/s).

### `selfhosted_full.md` (`44282884671`, 16-core `0x00FF` / `0xFF00` core split, 30s phases)

| Workload | AwanFS ops/s (`MiB/s`, P50 ms) | MinIO ops/s (`MiB/s`, P50 ms) | RustFS ops/s (`MiB/s`, P50 ms) | AwanFS / MinIO |
|---|---|---|---|---|
| 64 KiB PUT | `3149.1` (`196.8`, `17.12`) | `150.7` (`9.4`, `454.58`) | `138.0` (`8.6`, `452.95`) | 20.90x |
| 64 KiB GET | `5388.1` (`336.8`, `11.04`) | `3371.0` (`210.7`, `17.53`) | `4683.7` (`292.7`, `12.60`) | 1.60x |
| 64 KiB Mixed (80/20) | `4509.5` (`281.8`, `12.37`) | `628.9` (`39.3`, `10.99`) | `619.3` (`38.7`, `22.42`) | 7.17x |
| 64 KiB Range GET (4 KiB) | `5623.9` (`22.0`, `10.24`) | `3639.0` (`14.2`, `16.00`) | `4802.6` (`18.8`, `12.15`) | 1.55x |
| 1 MiB PUT | `251.3` (`251.3`, `64.16`) | `105.7` (`105.7`, `272.96`) | `70.3` (`70.3`, `447.83`) | 2.38x |
| 1 MiB GET | `1785.2` (`1785.2`, `17.28`) | `1119.3` (`1119.3`, `27.72`) | `772.7` (`772.7`, `40.51`) | 1.59x |
| 1 MiB Mixed (80/20) | `794.2` (`794.2`, `20.82`) | `413.7` (`413.7`, `11.68`) | `277.1` (`277.1`, `20.65`) | 1.92x |
| Sweep C=8 (64 KiB Mixed) | `3650.5` (`228.2`, `1.20`) | `962.4` (`60.2`, `2.50`) | `604.7` (`37.8`, `2.35`) | 3.79x |
| Sweep C=32 (64 KiB Mixed) | `4553.4` (`284.6`, `5.34`) | `580.7` (`36.3`, `5.48`) | `574.0` (`35.9`, `14.09`) | 7.84x |
| Sweep C=64 (64 KiB Mixed) | `4840.9` (`302.6`, `11.35`) | `628.3` (`39.3`, `8.74`) | `558.4` (`34.9`, `42.53`) | 7.70x |
| Sweep C=128 (64 KiB Mixed) | `4333.6` (`270.8`, `19.83`) | `167.5` (`10.5`, `7.85`) | `602.8` (`37.7`, `69.55`) | 25.87x |

(In the 10 GiB phase of `44282884671`, `10 GiB Volume PUT` reported `did not finish: 8 workers did not finish their request`, and `10 GiB Volume GET` ran `547.0 MiB/s` vs MinIO `1107.5 MiB/s`; 10 GiB large-object streaming/RSS is tracked separately under #133/#170.)

### `selfhosted_ratio.md` (`44282884671`, 3-run median, `0x00FF` / `0xFF00`) vs `ubuntu-latest` (`44256981862`, 5-run median)

| Workload | Windows `44282884671` (AwanFS / MinIO MiB/s) | Windows Ratio | Linux `44256981862` (AwanFS / MinIO MiB/s) | Linux Ratio |
|---|---|---|---|---|
| `put_64k` | `170.91 / 10.36` | `16.49x` | `118.12 / 37.81` | `3.12x` |
| `get_64k` | `352.82 / 218.54` | `1.61x` | `513.60 / 320.16` | `1.60x` |
| `range_get_4k` | `22.55 / 14.48` | `1.56x` | `33.24 / 20.31` | `1.64x` |
| `mixed_64k` | `302.16 / 46.43` | `6.51x` | `343.67 / 114.13` | `3.01x` |
| `put_1m` | `202.98 / 141.41` | `1.44x` | `166.00 / 93.41` | `1.78x` |
| `get_1m` | `1818.89 / 1155.57` | `1.57x` | `5050.47 / 2626.61` | `1.92x` |

Separating client and server cores lifted Windows `64 KiB GET` from `4711.1` ops/s (`294.4 MiB/s` on 4 shared CPUs, `44256981929`) to `5388.1` ops/s (`336.8 MiB/s` in full matrix; `352.82 MiB/s` median in `ratio_check`), matching Linux `64 KiB GET` (`5490.1` ops/s / `343.1 MiB/s`) and bringing the AwanFS/MinIO read ratio (`1.60x`–`1.61x`) in line with Linux (`1.54x`–`1.60x`).

---

## Experiment 2: CPU per process and CPU % (`CpuSampler` in `benchmarks/run_warp_benchmark.py`)

Commands:
- `python -u benchmarks/run_warp_benchmark.py --sizes 64KiB,1MiB,1GiB --concurrent 64 --duration 30s --output selfhosted_full.md` (`44282884671`, 16-core Windows `0x00FF` / `0xFF00`)
- `python benchmarks/run_warp_benchmark.py --sizes 64KiB,1MiB --concurrent 64 --duration 2 --output ci_full_matrix.md` (`44256981929`, 4-core `windows-latest` and `ubuntu-latest`)

### Self-hosted Windows (`44282884671`, 16 CPUs, 8 cores server / 8 cores client, 30s phases)

| Phase | Server CPU (s) | Client CPU (s) | System/Kernel CPU (s) | Server CPU % | Client CPU % | System CPU % |
|---|---|---|---|---|---|---|
| `put_64k` | 60.00 | 37.19 | 46.17 | 175.3% | 106.1% | 35.8% |
| `get_64k` | 25.64 | 42.70 | 31.86 | 84.2% | 140.7% | 24.7% |
| `range_get_4k` | 21.30 | 45.25 | 28.36 | 70.4% | 148.7% | 24.6% |
| `put_1m` | 62.45 | 10.27 | 35.92 | 208.2% | 34.1% | 29.6% |
| `get_1m` | 31.62 | 84.03 | 95.11 | 105.8% | 280.2% | 40.5% |
| `mixed_64k` | 43.25 | 43.92 | 42.45 | 146.8% | 147.3% | 35.1% |
| `mixed_1m` | 57.64 | 46.55 | 70.03 | 175.5% | 138.0% | 38.5% |

### Hosted `windows-latest` vs `ubuntu-latest` (`44256981929`, 4 shared CPUs, 2s phases)

| Phase | Win Server (s / %) | Win Client (s / %) | Win System (s / %) | Linux Server (s / %) | Linux Client (s / %) | Linux System (s / %) |
|---|---|---|---|---|---|---|
| `put_64k` | `2.80 / 125.1%` | `2.08 / 98.1%` | `2.31 / 61.7%` | `3.70 / 166.4%` | `2.77 / 125.2%` | `3.11 / 84.8%` |
| `get_64k` | `1.20 / 49.5%` | `3.05 / 129.0%` | `2.11 / 51.9%` | `1.40 / 60.2%` | `3.21 / 138.0%` | `2.02 / 52.9%` |
| `range_get_4k` | `1.38 / 58.7%` | `2.84 / 125.2%` | `2.06 / 49.3%` | `1.18 / 53.4%` | `2.98 / 134.4%` | `1.59 / 48.2%` |
| `put_1m` | `2.19 / 104.5%` | `0.34 / 16.3%` | `1.02 / 33.4%` | `5.03 / 248.1%` | `0.45 / 21.9%` | `2.86 / 68.8%` |
| `get_1m` | `1.33 / 65.8%` | `4.94 / 238.6%` | `4.47 / 79.8%` | `2.66 / 126.7%` | `3.63 / 173.6%` | `4.58 / 77.7%` |
| `mixed_64k` | `2.09 / 78.2%` | `2.91 / 123.0%` | `2.56 / 60.8%` | `2.14 / 94.2%` | `3.12 / 138.0%` | `2.39 / 63.0%` |
| `mixed_1m` | `2.47 / 118.3%` | `1.56 / 77.2%` | `1.92 / 51.6%` | `5.83 / 280.8%` | `1.89 / 90.8%` | `4.36 / 94.5%` |

Even with 8 dedicated cores on `LAPTOP-6RBIQUM5`, `s3-server` uses only `70.4%–105.8%` (~0.7–1.1 cores) on GET phases while the Python benchmark client consumes `140.7%–280.2%` (1.4–2.8 cores, **1.7x–2.7x** more CPU than `s3-server`) alongside `28.36s–95.11s` (`24.6%–40.5%` total machine CPU) in Windows kernel/loopback time.

---

## Experiment 3: `TCP_NODELAY` (`stream.set_nodelay(true)` in `server/src/reactor.rs`)

Commands:
- `python benchmarks/run_warp_benchmark.py --sizes 64KiB,1MiB --concurrent 64 --duration 2 --output ci_full_matrix.md` (`43986899662` and `44256981929` vs pre-`TCP_NODELAY` `43600434246` from #175)
- `python benchmarks/ratio_check.py --server-bin server/target/release/s3-server --minio-bin .tmp/minio/minio --mc-bin .tmp/minio/mc --runs 5 --duration 3 --report ratio_report.md` (`44256981862`)

| Workload (`windows-latest`) | Pre-`TCP_NODELAY` (`43600434246`, #175) | Post-`TCP_NODELAY` (`43986899662`) | Post-`TCP_NODELAY` + Defender excl (`44256981929`) |
|---|---|---|---|
| 64 KiB GET | `4808` ops/s (`1.44x`) | `4623.6` ops/s (`1.41x`) | `4711.1` ops/s (`1.39x`) |
| 64 KiB Range GET (4 KiB) | `1.54x` (ops/s not in #175 summary) | `6093.8` ops/s (`1.57x`) | `5958.3` ops/s (`1.52x`) |
| 64 KiB Mixed (80/20) | `4487` ops/s (`1.69x`) | `4576.8` ops/s (`1.82x`) | `4591.6` ops/s (`2.09x`) |
| 1 MiB GET | `3119` ops/s | `3103.0` ops/s (`1.68x`) | `3119.9` ops/s (`1.64x`) |

`TCP_NODELAY` changed nothing material on persistent keep-alive GETs (responses write headers and payload together in one buffer), while keeping Linux `bench-ratio.yml` (`44256981862`) green across all 6 rows and keeping `stream.set_nodelay(true)` on accepted sockets (`reactor::tests::accepted_socket_enables_tcp_nodelay`).

---

## Experiment 4: Windows Defender exclusion (`Add-MpPreference -ExclusionPath`)

Commands (`.github/workflows/bench-full.yml` `44256981929` and `.github/workflows/bench-selfhosted.yml` `44282884671`):
- `Add-MpPreference -ExclusionPath "${{ runner.temp }}"` followed by `python benchmarks/run_warp_benchmark.py ...`

Results:
- On `windows-latest` (`44256981929`), `Add-MpPreference -ExclusionPath "D:\a\_temp"` succeeded, and changed nothing material on reads (`44256981929` vs `43986899662`, same `windows-latest` 4-CPU runner):

| Workload (`windows-latest`) | Without Defender exclusion (`43986899662`) | With Defender exclusion (`44256981929`) | Change (AwanFS / MinIO) |
|---|---|---|---|
| 64 KiB GET | AwanFS `4623.6` / MinIO `3289.1` ops/s | AwanFS `4711.1` / MinIO `3395.4` ops/s | `+1.9%` / `+3.2%` |
| 64 KiB Range GET (4 KiB) | AwanFS `6093.8` / MinIO `3887.8` ops/s | AwanFS `5958.3` / MinIO `3909.2` ops/s | `-2.2%` / `+0.6%` |
| 64 KiB Mixed (80/20) | AwanFS `4576.8` / MinIO `2513.4` ops/s | AwanFS `4591.6` / MinIO `2192.3` ops/s | `+0.3%` / `-12.8%` |
| 1 MiB GET | AwanFS `3103.0` / MinIO `1851.1` ops/s | AwanFS `3119.9` / MinIO `1900.8` ops/s | `+0.5%` / `+2.7%` |

- On `LAPTOP-6RBIQUM5` (`44282884671`), `Add-MpPreference -ExclusionPath` failed with `You don't have enough permissions to perform the requested operation.` (step kept `continue-on-error: true`), so real-time scanning remained enabled on `LAPTOP-6RBIQUM5`.

---

## Experiment 5: Windows memory budget (`benchmarks/budget_rss.py`)

Command (`bench-selfhosted.yml`, run `44282884671`):
- `python -u benchmarks/budget_rss.py --server-bin server/target/release/s3-server.exe --budget 256MiB --budget-mib 256 --concurrency 64 --size 1MiB --duration 10 --overhead 200 --data-dir "${{ runner.temp }}/budget_rss" --report selfhosted_budget.md`

Comparison with `ubuntu-latest` (`bench-ratio.yml`, run `44256981862`):
- `python benchmarks/budget_rss.py --budget 256MiB --concurrency 64 --size 1MiB --duration 10.0 --overhead 200.0`

| Platform (Run) | Phase | MiB/s | ops/s | P99 ms | Peak RSS MiB | Peak heap (`RssAnon`) MiB | Peak file pages (`RssFile`) MiB | Payload cache MiB | Ceiling (`256 + 200` MiB) |
|---|---|---|---|---|---|---|---|---|---|
| Windows (`44282884671`) | `put` | `458.84` | `458.8` | `634.82` | `258.2` | `not measured` | `not measured` | `96.0` | `456.0` (**ok**) |
| Windows (`44282884671`) | `get` | `1376.84` | `1376.8` | `92.15` | `399.9` | `not measured` | `not measured` | `96.0` | `456.0` (**ok**) |
| Linux (`44256981862`) | `put` | `187.36` | `187.4` | `658.56` | `221.1` | `207.4` | `7.2` | `96.0` | `456.0` (**ok**) |
| Linux (`44256981862`) | `get` | `2586.15` | `2586.1` | `45.27` | `386.2` | `119.7` | `265.7` | `96.0` | `456.0` (**ok**) |

Windows peak RSS (`399.9 MiB` on `get`, `258.2 MiB` on `put`) stays under the `456.0 MiB` ceiling without needing Linux's glibc `M_MMAP_THRESHOLD` fix (`#134`).

---

## Remaining gaps

1. **Client & Windows loopback stack saturation (platform / harness):** With client and server separated onto `0xFF00` and `0x00FF` on 16 cores (`44282884671`), Windows `64 KiB GET` (`5388.1` ops/s, `1.60x` MinIO) matches Linux (`5490.1` ops/s, `1.54x` MinIO), while `s3-server` uses only `70.4%–105.8%` CPU on GETs against `140.7%–280.2%` in the Python client and `24.6%–40.5%` machine-wide kernel/system CPU (`95.11s` system CPU on `get_1m`, `1818.89 MiB/s` vs Linux `5050.47 MiB/s`). That remaining throughput gap on `1 MiB GET` and `4-CPU` hosted runners is the Python `urllib3`/HTTP client and the Windows kernel loopback/socket path, not `s3-server`.
2. **Windows loopback connection-setup latency (tracked in `#248`):** Opening a fresh TCP loopback connection on `windows-latest` costs `11.20–11.52 ms` P50 against `0.07–0.08 ms` on `ubuntu-latest` (`#248`), penalizing any short-lived or non-keep-alive client workloads on Windows until `#248` determines whether it is ours or the Windows runner's network stack.
