Command: `python -u benchmarks/compaction_cost.py --bin server/target/release/examples/compaction_cost --data-dir $RUNNER_TEMP/compaction_cost/data --runs 3 --mixes small-heavy,balanced,90-10 --delete-fractions 0.50,0.90 --compaction-rate 0 --results $RUNNER_TEMP/compaction_cost/results.jsonl --output $RUNNER_TEMP/compaction_cost/report.md` (`.github/workflows/bench-compaction-cost.yml`, run 44528864978, commit 17c31e2)

# Compaction cost with live large and small objects (#269)

Run [44528864978](https://github.com/iskandarnorazli/Awanfs/actions/runs/44528864978), GitHub-hosted runners, unthrottled (`--compaction-rate 0`; production `s3-server` defaults to 50 MB/s).
Jobs: Linux `ubuntu-latest` [135576579758](https://github.com/iskandarnorazli/Awanfs/actions/runs/44528864978/job/135576579758), Windows `windows-latest` [135576579292](https://github.com/iskandarnorazli/Awanfs/actions/runs/44528864978/job/135576579292).

Settings:
- Store opened via `S3ServiceState::try_with_data_dir` and compacted via `perform_online_compaction()` (`server/examples/compaction_cost.rs`, no `server/src/` changes).
- Small objects: 4 KiB payload (8 KiB sector-aligned needle on disk). Large objects: 64 MiB payload (64 MiB + 4 KiB sector-aligned needle on disk).
- Mixes:
  - `small-heavy`: 32,768 x 4 KiB + 1 x 64 MiB (33% large payload)
  - `balanced`: 16,384 x 4 KiB + 4 x 64 MiB (80% large payload)
  - `90-10`: 90 x 4 KiB + 10 x 64 MiB (90% / 10% by count)
- Two small-object delete fractions (`50%`, `90%`), median of 3 runs each, plus matched `small-only` baselines (0 large objects).
- All 36 runs on `ubuntu-latest` and all 36 runs on `windows-latest` exited 0 with `volume_removed: true`, `busy: false`, `work_disk_write_bytes == bytes_copied`, `work_disk_syncs: 2`, and `bytes_reclaimed >= 552,960`.

## Linux (`ubuntu-latest`, job 135576579758)

| mix | small delete | runs | bytes copied | large copied | small copied | bytes reclaimed | seconds (range) | large share | write amp | status |
|---|---|---|---|---|---|---|---|---|---|---|
| `small-heavy` | 50% | 3 | 192.00 MiB | 64.00 MiB | 128.00 MiB | 192.00 MiB | 1.321 (1.317..1.381) | 33.33% | 1.00x | ok |
| `small-heavy` | 90% | 3 | 89.61 MiB | 64.00 MiB | 25.60 MiB | 345.60 MiB | 0.702 (0.697..0.710) | 71.43% | 0.26x | ok |
| `balanced` | 50% | 3 | 320.02 MiB | 256.02 MiB | 64.00 MiB | 96.00 MiB | 1.973 (1.954..1.974) | 80.00% | 3.33x | ok |
| `balanced` | 90% | 3 | 268.81 MiB | 256.02 MiB | 12.80 MiB | 172.80 MiB | 1.659 (1.656..1.666) | 95.24% | 1.56x | ok |
| `90-10` | 50% | 3 | 640.39 MiB | 640.04 MiB | 360.0 KiB | 540.0 KiB | 3.778 (3.775..3.794) | 99.95% | 1214.37x | ok |
| `90-10` | 90% | 3 | 640.11 MiB | 640.04 MiB | 72.0 KiB | 972.0 KiB | 3.766 (3.729..3.766) | 99.99% | 674.35x | ok |

### Against live small objects alone (`ubuntu-latest`)

| mix | small delete | mix seconds | small-only seconds | extra seconds from large | mix copied | small-only copied |
|---|---|---|---|---|---|---|
| `small-heavy` | 50% | 1.321 | 0.956 | +0.365 | 192.00 MiB | 128.00 MiB |
| `small-heavy` | 90% | 0.702 | 0.336 | +0.366 | 89.61 MiB | 25.60 MiB |
| `balanced` | 50% | 1.973 | 0.379 | +1.594 | 320.02 MiB | 64.00 MiB |
| `balanced` | 90% | 1.659 | 0.140 | +1.519 | 268.81 MiB | 12.80 MiB |
| `90-10` | 50% | 3.778 | 0.002 | +3.776 | 640.39 MiB | 360.0 KiB |
| `90-10` | 90% | 3.766 | 0.003 | +3.762 | 640.11 MiB | 72.0 KiB |

## Windows (`windows-latest`, job 135576579292)

| mix | small delete | runs | bytes copied | large copied | small copied | bytes reclaimed | seconds (range) | large share | write amp | status |
|---|---|---|---|---|---|---|---|---|---|---|
| `small-heavy` | 50% | 3 | 192.00 MiB | 64.00 MiB | 128.00 MiB | 192.00 MiB | 0.670 (0.660..0.679) | 33.33% | 1.00x | ok |
| `small-heavy` | 90% | 3 | 89.61 MiB | 64.00 MiB | 25.60 MiB | 345.60 MiB | 0.390 (0.377..0.397) | 71.43% | 0.26x | ok |
| `balanced` | 50% | 3 | 320.02 MiB | 256.02 MiB | 64.00 MiB | 96.00 MiB | 0.850 (0.813..0.872) | 80.00% | 3.33x | ok |
| `balanced` | 90% | 3 | 268.81 MiB | 256.02 MiB | 12.80 MiB | 172.80 MiB | 0.747 (0.738..0.748) | 95.24% | 1.56x | ok |
| `90-10` | 50% | 3 | 640.39 MiB | 640.04 MiB | 360.0 KiB | 540.0 KiB | 1.648 (1.635..1.738) | 99.95% | 1214.37x | ok |
| `90-10` | 90% | 3 | 640.11 MiB | 640.04 MiB | 72.0 KiB | 972.0 KiB | 1.612 (1.587..1.674) | 99.99% | 674.35x | ok |

### Against live small objects alone (`windows-latest`)

| mix | small delete | mix seconds | small-only seconds | extra seconds from large | mix copied | small-only copied |
|---|---|---|---|---|---|---|
| `small-heavy` | 50% | 0.670 | 0.464 | +0.206 | 192.00 MiB | 128.00 MiB |
| `small-heavy` | 90% | 0.390 | 0.266 | +0.124 | 89.61 MiB | 25.60 MiB |
| `balanced` | 50% | 0.850 | 0.241 | +0.609 | 320.02 MiB | 64.00 MiB |
| `balanced` | 90% | 0.747 | 0.136 | +0.611 | 268.81 MiB | 12.80 MiB |
| `90-10` | 50% | 1.648 | 0.004 | +1.643 | 640.39 MiB | 360.0 KiB |
| `90-10` | 90% | 1.612 | 0.003 | +1.609 | 640.11 MiB | 72.0 KiB |

## What it says

- **Live large objects dominate compaction bytes and time whenever they share a volume with small objects.** Across the three mixes, live large objects account for `33.33%` to `99.99%` of copied bytes and add `+0.365 s` to `+3.776 s` on `ubuntu-latest` (`+0.124 s` to `+1.643 s` on `windows-latest`).
- **Write amplification reaches 674x to 1,214x on the `90-10` mix.** Reclaiming `540.0 KiB` to `972.0 KiB` of deleted small needles forces compaction to rewrite `640.04 MiB` of unchanged large objects, slowing compaction from `0.002` to `0.004 s` (small objects alone) to `3.77` to `3.78 s` on Linux and `1.61` to `1.65 s` on Windows.
- **Limiter.** With `--compaction-rate 0`, compaction is bound by sequential volume write plus final `sync_data`: about `170 MiB/s` on `ubuntu-latest` (`640 MiB / 3.77 s`) and `395 MiB/s` on `windows-latest` `D:\a\_temp` (`640 MiB / 1.62 s`). Under production `s3-server` default throttling (`--compaction-rate 50` MB/s, 1 s initial burst in `server/src/throttle.rs`), copying `640.04 MiB` (`671.13 MB`) of live large objects takes at least `(671.13 - 50) / 50 = 12.4 s` per compaction pass.
- **Decision evidence for `#26` (idea 3).** Routing large objects above a size threshold (such as `4 MiB`) into separate volumes avoids rewriting live large data during small-object compaction, eliminating `33%` to `99.99%` of compaction copy volume in mixed buckets.
