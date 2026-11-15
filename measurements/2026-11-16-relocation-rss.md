python -u benchmarks/relocation_rss.py --server-bin server/target/release/s3-server --size 64MiB --budget 32MiB --budget-mib 32 --overhead-mib 16 --report relocation_rss.md

# Peak RSS while a 64 MiB streamed PUT is relocated, under --memory-budget 32MiB (#185)

The command above, run by `.github/workflows/bench-large.yml` (step "Relocation under a memory
budget") on GitHub `ubuntu-latest`, 4 CPUs, one run each. The numbers are that step's output.
"Before" is `try/185-relocation-via-mapping`: main with the relocation reading the needle through
the volume mapping again, as before PR #199. "After" is this branch at `111ec61` (main's
relocation, plus the exported counter the script reads).

| | before (run 43737008888) | after (run 43737004042) |
|---|---|---|
| streamed PUT answer | 200 | 200 |
| relocations (`awanfs_stream_relocations_total`) | 1 | 1 |
| RSS before the PUT, MiB | 11.0 | 10.8 |
| peak RSS, MiB | **76.0** | **12.9** |
| peak heap (RssAnon), MiB | 5.8 | 5.8 |
| peak file pages (RssFile), MiB | 71.0 | 6.8 |
| ceiling (budget + overhead), MiB | 48.0 | 48.0 |
| verdict | over the ceiling | under the ceiling |

Before, the copy faulted the 64 MiB needle into the process's file pages (71.0 MiB), outside the
budget. After, the copy reads 1 MiB at a time with aligned reads, and the peak RSS is 2.1 MiB above
the idle server.
