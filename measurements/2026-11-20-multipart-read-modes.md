`gh workflow run bench-full.yml --ref experiment/155-mpu-read-modes` (run 44692576604, commit on top of 71cf1ac). On each runner it ran `AWAN_MPU_READ=$mode python -u benchmarks/run_warp_benchmark.py --sizes 1GiB --concurrent 64 --duration 2 --runs 3` for mode `map`, `pread`, `window` and `map` again, one after another on the same VM.

# Multipart parts: mapping against positioned reads (#155 requirement 5, PR #340)

`map` is the old path: each part file is mapped and the mapping is kept. `pread` is PR #340: each part is read with positioned reads into pooled 1 MiB buffers. `window` maps only the chunk being sent and unmaps it once sent; it was an experiment and is not merged. GitHub-hosted runners (`ubuntu-latest`, `windows-latest`, 4 vCPUs). Medians of 3 rounds; peak RSS shows median (min..max).

## 1 GiB multipart range GET (16 parallel 64 MiB ranges)

| mode | Linux MiB/s | MinIO, same pass | Linux peak RSS (file pages) | Windows MiB/s | MinIO, same pass | Windows peak RSS (private) |
|---|---|---|---|---|---|---|
| map, first | 1,211.6 | 1,169.5 | 1,188.9 MB (1,033.1) | 2,919.0 | 2,428.1 | 1,140.0 MB (136.5) |
| pread | 1,131.3 | 1,150.2 | 274.5 MB (9.0) | 3,134.7 | 2,466.9 | 265.3 MB (165.4) |
| window | 1,048.6 | 1,145.6 | 276.3 MB (41.0) | 2,235.6 | 2,392.4 | 266.3 MB (138.0) |
| map, last | 1,134.3 | 1,051.8 | 1,191.3 MB (1,033.1) | 3,151.8 | 2,576.4 | 1,139.1 MB (138.8) |

## What it shows

- `pread` falls within `map`'s own spread between its two passes on both platforms. On Linux, `map` ran at 1,134.3 and 1,211.6 MiB/s and `pread` at 1,131.3. On Windows, `map` ran at 2,919.0 and 3,151.8 and `pread` at 3,134.7. No loss is measurable at this run count.
- `pread` lowers peak RSS from about 1,190 MB to 274.5 MB on Linux and from about 1,140 MB to 265.3 MB on Windows. The file pages fall from 1,033.1 MB to 9.0 MB on Linux.
- `window` is slower on both platforms (Windows 2,235.6 against 2,919.0 to 3,151.8) and is not merged.
- An earlier comparison across two different VMs suggested `pread` was 17% (Linux) and 20% (Windows) slower. It set the branch's full-matrix run (44690219193) against the nightly run (44674110492). On a single VM the gap disappears.
