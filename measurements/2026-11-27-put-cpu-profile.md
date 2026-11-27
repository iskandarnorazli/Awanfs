`for round in 1 2 3; do for build in before after; do python -u benchmarks/core_scaling.py --matrix --servers AwanFS --pattern seq-write --sizes 4KiB,1MiB --concurrencies 16 --cores 2 --runs 1 --duration 5 --no-compare-fast --data-dir "$RUNNER_TEMP" --server-bin "$RUNNER_TEMP/bin/$build" --output "$RUNNER_TEMP/r${round}_${build}.md"; done; done` (workflow `.github/workflows/try-454-ab.yml` on the unmerged branch `try/454-ab2`, run 45915148877, job `ab`; `before` is `main` at `d849593`, `after` is this branch; both built in the same job)

# PUT CPU: profile, then two fixes (#454)

## Profile (before the change)

`sudo perf record -a -g --call-graph fp -F 999 -- sleep 20` during `core_scaling.py --matrix --servers AwanFS --pattern seq-write --sizes 1MiB --concurrencies 16 --cores 2 --runs 1 --duration 30 --client-procs 8`, then `perf report --no-children --comm tokio-rt-worker,awanfs-log --sort dso,symbol` and `perf report --sort comm` (unmerged branch `try/454-profile`, runs 45886000609 and 45886897836, hosted Linux, release build with frame pointers). Shares are of all samples on the 4-CPU machine; the server's threads (`tokio-rt-worker`) took 19.08% and 20.94% of them.

| Function | Run 45886000609 | Run 45886897836 | Share of the server's CPU |
|---|---|---|---|
| `md5::compress::soft::compress` (ETag) | 7.46% | 7.53% | 39%, 36% |
| `_copy_to_iter` (socket receive copy) | 0.43% | 1.85% | 2%, 9% |
| `do_user_addr_fault` | 1.11% | 1.20% | page faults, about 14% together with the next three |
| `kernel_init_pages` | 0.37% | 1.19% | 0.68% of it under `S3ServiceState::store_object` |
| `do_anonymous_page` | 0.49% | 0.29% | |
| `__handle_mm_fault` | 0.34% | 0.23% | |
| `crc32c::hw_x86_64::crc_u64_parallel3` | not listed | 0.23% | 1% |

No other function reached 0.2% of samples. The page faults came from the aligned buffer `write_needle_with_attrs` allocated zeroed for every needle (`main.rs` sets glibc's mmap threshold to 128 KiB, so every 1 MiB buffer was a fresh mapping).

## The change

- `volume.rs`: needle buffers come from a per-volume pool (64 KiB size steps, at most 8 MiB kept, none over 4 MiB + 128 KiB, emptied by `seal`).
- `md-5` with its `asm` feature on x86 and x86-64 outside MSVC (owner decision on #454).
- `s3_api.rs`: a buffered body is read into a buffer of its declared length, not one that doubles from the first frame.

## Before and after, alternating in one job (AMD EPYC 7763, 4 vCPUs, server on 2 cores, c=16)

CPU per MiB is the server CPU % times 10 divided by MiB/s.

| Round | Build | PUT 4 KiB ops/s | PUT 1 MiB ops/s | 1 MiB P99 ms | Server CPU | CPU per MiB |
|---|---|---|---|---|---|---|
| 1 | before | 6,981 | 336 | 87.17 | 193.0% | 5.74 ms |
| 1 | after | 7,160 | 380 | 76.26 | 193.4% | 5.09 ms |
| 2 | before | 7,072 | 325 | 98.76 | 189.5% | 5.83 ms |
| 2 | after | 6,879 | 386 | 79.19 | 195.4% | 5.06 ms |
| 3 | before | 7,109 | 335 | 83.64 | 194.0% | 5.79 ms |
| 3 | after | 6,922 | 380 | 80.60 | 192.9% | 5.07 ms |

- CPU per MiB at 1 MiB: 5.74 to 5.83 ms before, 5.06 to 5.09 ms after: **12% less**. The gate asks for 25%.
- PUT 1 MiB: 325 to 336 ops/s before, 380 to 386 after (+14%), still CPU-bound at about 193%.
- PUT 4 KiB: 6,981 to 7,109 before, 6,879 to 7,160 after: the ranges overlap.

An earlier run of the first two fixes alone (same command, unmerged branch `try/454-ab`, run 45914608316, AMD EPYC 9V45, where 1 MiB was not CPU-bound at 63% to 72% CPU) gave 3.80 to 3.83 ms before and 3.30 to 3.33 ms after: 13% less.

Not measured: a profile after the change.
