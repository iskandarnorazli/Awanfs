`sudo perf record -a -g --call-graph fp -F 999 -- sleep 20` during `python -u benchmarks/core_scaling.py --matrix --servers AwanFS --pattern seq-write --sizes 1MiB --concurrencies 16 --cores 2 --runs 1 --duration 30 --client-procs 8 --seed 42`, then `perf report --no-children --comm tokio-rt-worker,awanfs-log --sort dso,symbol --stdio --percent-limit 0.2 -g none` and `perf report --no-children --sort comm` (workflow `bench-scaling.yml` on the unmerged branch `try/473-profile`, run 45918519879, hosted Linux, 4 logical cores; `main` at `06feaa4`, which has #471; release build with line tables and frame pointers)

# PUT 1 MiB profile after #471 (#473)

The server's threads (`tokio-rt-worker`) took 17.77% of all samples; the machine was idle (`swapper`) for 77.67%. This runner did 184 PUT 1 MiB/s at 70.0% server CPU while profiled: the disk, not the CPU, set the rate, so the shares below are of the work per request, not of a CPU-bound server.

| Function | Share of all samples | Share of the server's samples |
|---|---|---|
| `md5_compress` (md5-asm, the ETag) | 7.14% | 40% |
| `_raw_spin_unlock_irqrestore` (kernel) | 0.99% | 6% |
| `do_user_addr_fault` | 0.87% | page faults, 10% with the next three |
| `_copy_to_iter` (socket receive copy) | 0.50% | 3% |
| `do_anonymous_page` | 0.38% | |
| `kernel_init_pages` | 0.35% | |
| `__handle_mm_fault` | 0.24% | |
| `rwsem_spin_on_owner` | 0.23% | 1% |
| `crc32c::hw_x86_64::crc_u64_parallel3` | 0.21% | 1% |
| `zap_present_ptes` | 0.21% | 1% |

No other function reached 0.2% of samples.

- MD5 is still the largest cost: 40% of the server's CPU with the assembly code, against 36% to 39% with the portable code before #471 (runs 45886000609, 45886897836). The assembly code is faster per byte, but the rest got cheaper too.
- Page faults are about 10% of the server's CPU, from about 14% before #471. What remains is the request body: `main.rs` fixes glibc's mmap threshold at 128 KiB (#127, owner decision 2026-11-14), so each 1 MiB body is a fresh mapping, and the payload cache keeps the body, so it cannot be reused.
- The rest, about half the server's CPU, is spread over hyper, tokio and the kernel's network path, none above 0.2% of samples.
