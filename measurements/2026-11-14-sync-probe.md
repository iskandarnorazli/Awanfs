Command: `python -u benchmarks/sync_probe.py --count 200 --dir <python temp dir> --dir .tmp/probe` (`.github/workflows/probe-sync.yml`)

Run on GitHub Actions: https://github.com/iskandarnorazli/Awanfs/actions/runs/43597238771 (PR #171), windows-latest job. Copied from the job log.

# What one durable 64 KiB append costs (2026-11-14 12:57 UTC)

Machine: runnervmm7ud1 (Windows-2025Server-10.0.26100-SP0, 4 CPUs). 200 appends per variant, one writer.

## C:\Users\RUNNER~1\AppData\Local\Temp

| variant | median ms | P90 ms | appends/s |
|---|---|---|---|
| write only, growing | 0.017 | 0.028 | 49708 |
| flush, growing | 3.232 | 5.176 | 255 |
| flush x2, growing | 5.831 | 8.426 | 153 |
| flush, preallocated | 3.160 | 4.881 | 228 |
| flush x2, preallocated | 6.015 | 8.942 | 149 |
| unbuffered, flush, growing | 3.215 | 4.623 | 265 |
| unbuffered, flush x2, growing (what the server does) | 6.295 | 8.643 | 142 |
| unbuffered, flush, preallocated | 3.067 | 4.440 | 282 |
| unbuffered, flush x2, preallocated | 5.941 | 7.873 | 147 |
| writes during flushes, same handle [5745 flushes ran] | 2.754 | 9.711 | 172 |
| writes during flushes, second handle [472 flushes ran] | 0.026 | 0.112 | 157 |
| unbuffered writes during flushes, same handle (what the server does) [3572 flushes ran] | 2.515 | 6.505 | 208 |
| unbuffered writes during flushes, second handle [43035 flushes ran] | 14.454 | 28.587 | 62 |

## D:/a/Awanfs/Awanfs/.tmp/probe

| variant | median ms | P90 ms | appends/s |
|---|---|---|---|
| write only, growing | 0.016 | 0.024 | 54950 |
| flush, growing | 0.127 | 1.074 | 3904 |
| flush x2, growing | 0.244 | 0.408 | 3602 |
| flush, preallocated | 0.130 | 1.018 | 3963 |
| flush x2, preallocated | 0.240 | 0.378 | 3494 |
| unbuffered, flush, growing | 0.107 | 1.222 | 4061 |
| unbuffered, flush x2, growing (what the server does) | 0.233 | 0.472 | 3595 |
| unbuffered, flush, preallocated | 0.101 | 1.227 | 4007 |
| unbuffered, flush x2, preallocated | 0.221 | 0.531 | 3614 |
| writes during flushes, same handle [47155 flushes ran] | 0.065 | 4.134 | 785 |
| writes during flushes, second handle [36742 flushes ran] | 0.029 | 0.054 | 26672 |
| unbuffered writes during flushes, same handle (what the server does) [55260 flushes ran] | 0.083 | 2.074 | 1234 |
| unbuffered writes during flushes, second handle [47858 flushes ran] | 0.075 | 0.098 | 12738 |


## What this run shows

- **The two disks of the Windows runner differ by about 40 times.** One flush takes 3.9 ms on
  C: (where Python's temp directory is, and where the full matrix put every server's data
  until now) and 0.09 ms on D: (the workspace and the runner's temp directory). The Linux
  runner measured 0.5 ms in the same run.
- **The server's commit is two flushes** (the volume, then its journal): 6 to 8 ms on C:, so
  at most about 120 to 150 commits a second there. The full matrix measured 118 PUTs a second
  at 64 clients (run 43579954027): about one PUT per commit, where a group commit should
  carry many.
- **On Windows a write waits for a flush of the same handle.** With another thread flushing
  through a duplicate of the writer's handle, the median append takes 2.5 to 2.8 ms on C:
  against 0.02 ms alone, and the P90 on D: is 2 to 4 ms against 0.05 to 0.10 ms through a
  second handle. On Linux it makes no difference (0.015 ms either way). The server flushes
  through the handle it writes with (the volume) and through a `try_clone` of it (the
  journal), and its writes run on the four async worker threads, so each flush stops them.
- **One row does not fit and is not explained:** unbuffered writes with a second handle on C:
  took 14.5 ms (median) while the flusher completed 43,035 flushes. The flusher in this probe
  spins, which no server does; that row needs a paced flusher before it means anything.
