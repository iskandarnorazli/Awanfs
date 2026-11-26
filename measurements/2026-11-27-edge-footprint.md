Command: `gh workflow run bench-edge.yml --ref main` (run `45902220481`, https://github.com/iskandarnorazli/Awanfs/actions/runs/45902220481)

# Edge footprint on ARM (`128 MiB` and `256 MiB`, 2026-11-27)

Machine: `runnervmu7lcj` (`Linux-7.0.0-1020-azure-aarch64-with-glibc2.39`, 4 CPUs). Parameters: `--concurrency 64 --duration 10 --overhead 512` via [`benchmarks/budget_rss.py`](../benchmarks/budget_rss.py) on [`bench-edge.yml`](../.github/workflows/bench-edge.yml).

## Summary

| Budget | Size | Phase | MiB/s | ops/s | P99 ms | Peak RSS MiB | RssAnon MiB | RssFile MiB | Payload cache MiB | Measured overhead above cache MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| `128MiB` | `4KiB` | put | 39.34 | 10070.2 | 16.74 | 52.5 | 46.7 | 5.8 | 2.0 | 50.5 |
| `128MiB` | `4KiB` | get | 55.00 | 14078.9 | 14.86 | 53.8 | 47.8 | 6.0 | 2.0 | 51.8 |
| `128MiB` | `1MiB` | put | 248.03 | 248.0 | 487.14 | 173.0 | 164.1 | 5.8 | 48.0 | 125.0 |
| `128MiB` | `1MiB` | get | 4295.30 | 4295.3 | 27.28 | 340.8 | 74.6 | 265.3 | 48.0 | 292.8 |
| `256MiB` | `4KiB` | put | 41.44 | 10609.2 | 16.00 | 52.1 | 46.3 | 5.8 | 2.0 | 50.1 |
| `256MiB` | `4KiB` | get | 55.11 | 14109.4 | 14.83 | 53.6 | 47.7 | 6.0 | 2.0 | 51.6 |
| `256MiB` | `1MiB` | put | 254.41 | 254.4 | 480.69 | 222.5 | 213.8 | 5.8 | 96.0 | 126.5 |
| `256MiB` | `1MiB` | get | 4392.73 | 4392.7 | 27.49 | 388.8 | 122.7 | 265.4 | 96.0 | 292.8 |

## Verbatim reports (run `45902220481`)

### `128MiB`, `4KiB`
Command: `python benchmarks/budget_rss.py --budget 128MiB --concurrency 64 --size 4KiB --duration 10.0 --overhead 512.0`

| phase | MiB/s | ops/s | P99 ms | peak RSS MiB | peak heap (RssAnon) MiB | peak file pages (RssFile) MiB | payload cache MiB | heap less cache MiB |
|---|---|---|---|---|---|---|---|---|
| put | 39.34 | 10070.2 | 16.74 | 52.5 | 46.7 | 5.8 | 2.0 | 44.7 |
| get | 55.00 | 14078.9 | 14.86 | 53.8 | 47.8 | 6.0 | 2.0 | 45.8 |

### `128MiB`, `1MiB`
Command: `python benchmarks/budget_rss.py --budget 128MiB --concurrency 64 --size 1MiB --duration 10.0 --overhead 512.0`

| phase | MiB/s | ops/s | P99 ms | peak RSS MiB | peak heap (RssAnon) MiB | peak file pages (RssFile) MiB | payload cache MiB | heap less cache MiB |
|---|---|---|---|---|---|---|---|---|
| put | 248.03 | 248.0 | 487.14 | 173.0 | 164.1 | 5.8 | 48.0 | 116.1 |
| get | 4295.30 | 4295.3 | 27.28 | 340.8 | 74.6 | 265.3 | 48.0 | 26.6 |

### `256MiB`, `4KiB`
Command: `python benchmarks/budget_rss.py --budget 256MiB --concurrency 64 --size 4KiB --duration 10.0 --overhead 512.0`

| phase | MiB/s | ops/s | P99 ms | peak RSS MiB | peak heap (RssAnon) MiB | peak file pages (RssFile) MiB | payload cache MiB | heap less cache MiB |
|---|---|---|---|---|---|---|---|---|
| put | 41.44 | 10609.2 | 16.00 | 52.1 | 46.3 | 5.8 | 2.0 | 44.3 |
| get | 55.11 | 14109.4 | 14.83 | 53.6 | 47.7 | 6.0 | 2.0 | 45.7 |

### `256MiB`, `1MiB`
Command: `python benchmarks/budget_rss.py --budget 256MiB --concurrency 64 --size 1MiB --duration 10.0 --overhead 512.0`

| phase | MiB/s | ops/s | P99 ms | peak RSS MiB | peak heap (RssAnon) MiB | peak file pages (RssFile) MiB | payload cache MiB | heap less cache MiB |
|---|---|---|---|---|---|---|---|---|
| put | 254.41 | 254.4 | 480.69 | 222.5 | 213.8 | 5.8 | 96.0 | 117.8 |
| get | 4392.73 | 4392.7 | 27.49 | 388.8 | 122.7 | 265.4 | 96.0 | 26.7 |

## Findings

For `4KiB` objects at 64 concurrent connections, peak RSS stayed at `52.1` to `53.8 MiB` under both `128 MiB` and `256 MiB` budgets (around `51.6` to `51.8 MiB` measured overhead above the `2.0 MiB` cache), delivering `10,070` to `10,609` PUT ops/s and `14,079` to `14,109` GET ops/s. For `1MiB` objects, anonymous heap (`RssAnon`) remained bounded within budget plus runtime and in-flight buffers (`164.1 MiB` PUT / `74.6 MiB` GET under `128 MiB`; `213.8 MiB` PUT / `122.7 MiB` GET under `256 MiB`), while `heap less cache` during PUT (`116.1` to `117.8 MiB`) reflects 64 concurrent `1 MiB` request bodies plus runtime/index heap. What stood out on `1MiB` GETs (`4,295` to `4,393 MiB/s`) is kernel-reclaimable `RssFile` volume mmap pages rising to `265.3` to `265.4 MiB`, bringing total peak RSS to `340.8 MiB` (`128 MiB` budget) and `388.8 MiB` (`256 MiB` budget), an exact `292.8 MiB` measured total overhead above cache in both budgets (within +3.9% of `budget + 200` on `128 MiB`, though 6.4x higher than `4KiB` because `RssFile` counts faulted mmap volume pages under `ubuntu-24.04-arm`).
