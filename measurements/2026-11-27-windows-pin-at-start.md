`for round in 1 2; do for build in before after; do cp core_scaling_${build}.py benchmarks/core_scaling_ab.py; python -u benchmarks/core_scaling_ab.py --matrix --servers MinIO,RustFS --pattern seq-write,seq-read --sizes 4KiB --concurrencies 16 --cores 1,2 --runs 1 --duration 5 --no-compare-fast --no-fetch-minio --data-dir "$RUNNER_TEMP" --output "$RUNNER_TEMP/r${round}_${build}.md"; done; done` (workflow `.github/workflows/try-474-competitors.yml` on the unmerged branch `try/474-show`, run 45923184810, windows-latest; `before` is `benchmarks/core_scaling.py` from `main`, `after` is this branch's, which starts each server already pinned)

# Windows: servers start pinned (#474)

## AwanFS sees its cores at startup

Run 45922803784 (same branch, step "The worker count a server started by _spawn_pinned prints"): a server started by `core_scaling._spawn_pinned` printed `Worker threads: 1 (process affinity)` with 1 core and `Worker threads: 2 (process affinity)` with 2 cores.

## Competitors, before and after (c=16, 4 KiB, ops/s)

| Server | Pattern | Cores | Before r1 | Before r2 | After r1 | After r2 |
|---|---|---|---|---|---|---|
| MinIO | seq-write | 1 | 1,047 | 1,070 | 1,074 | 1,051 |
| MinIO | seq-write | 2 | 1,511 | 1,473 | 1,482 | 1,501 |
| MinIO | seq-read | 1 | 3,136 | 3,114 | 3,116 | 3,004 |
| MinIO | seq-read | 2 | 5,041 | 4,966 | 5,018 | 5,016 |
| RustFS | seq-write | 1 | 614 | 570 | 579 | 607 |
| RustFS | seq-write | 2 | 831 | 773 | 814 | 885 |
| RustFS | seq-read | 1 | 2,407 | 2,290 | 1,691 | 2,313 |
| RustFS | seq-read | 2 | 3,619 | 3,131 | 3,453 | 3,553 |

- MinIO: every after row is within 3.5% of the before rows (one, seq-read at 1 core round 2, is 3.5% under both before rows).
- RustFS: 7 of 8 after rows are within or above the before rows; seq-read at 1 core round 1 (1,691) is 26% under, and round 2 of the same row (2,313) is within. One outlier in two rounds, not a shift in both.
- Not measured: Garage and SeaweedFS on Windows (Garage has no Windows build; SeaweedFS was not in this run).
