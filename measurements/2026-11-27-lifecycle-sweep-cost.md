`./server/target/release/examples/boot_bench fill --dir "$d" --objects 1000000 && for i in 1 2; do ./server/target/release/examples/lifecycle_sweep_bench --dir "$d" --puts 20000 --concurrency 16; done` (workflow `.github/workflows/try-464-run.yml` on the unmerged branch `try/464-run`, run 45932273036, ubuntu-latest, AMD EPYC 9V45, 4 logical cores, 15,983 MiB RAM; `main` at the merge of #476, so the sweep lists the spill index too)

# Lifecycle sweep cost on a bucket with 1M objects (#464)

The bucket holds 1,000,000 objects of 64 bytes (run 2: 1,040,000, with run 1's PUTs), 100,000 of them in the in-memory table and the rest in the spill index. One enabled rule expires nothing (Expiration after 36,500 days), so a pass visits every object and removes none.

| Run | Pass wall time | Pass CPU time | VmRSS before / after the pass | PUT 4 KiB P50 / P99, no sweep | PUT 4 KiB P50 / P99, sweeping | Passes during the PUTs |
|---|---|---|---|---|---|---|
| 1 | 2.787 s | 2.786 s | 71,168 / 71,168 KiB | 1.30 / 2.65 ms | 1.29 / 2.70 ms | 1 |
| 2 | 2.844 s | 2.844 s | 71,132 / 71,132 KiB | 1.26 / 2.47 ms | 1.32 / 2.75 ms | 1 |

Against the thresholds in #464:
- **A pass over 1 s: exceeded.** 2.79 s and 2.84 s of CPU on one core, for 1M objects. The server runs a pass every 60 s (`main.rs`), so about 4.7% of a core at all times at this size; at 10M objects that would be about 28 s per pass (scaled, not measured).
- **RSS increase over 100 MiB: not exceeded.** VmRSS did not grow during a pass alone (`list_keys` pages of 1,024 keys since #475). The peak during the PUT phase (325 MiB) includes the 20,000 PUTs and is not the sweep's alone.
- **PUT P99 more than doubled: not exceeded.** 2.65 to 2.70 ms and 2.47 to 2.75 ms. Only one pass overlapped each PUT phase, so this covers about 3 s of overlap.

Not measured: a bucket with versioning; objects with tags (the sweep reads tags per object); Windows.

## The period after #486

The cost of one pass does not change with #486; how often it runs does. From the passes above (2.786 s and 2.844 s of CPU for 1M objects), computed, not measured separately:

| Period | Passes per hour | CPU per hour at 1M objects | Share of one core |
|---|---|---|---|
| 60 s (before #486) | 60 | 167 to 171 s | 4.6% to 4.7% |
| 1 h (default after #486) | 1 | 2.8 s | 0.08% |
