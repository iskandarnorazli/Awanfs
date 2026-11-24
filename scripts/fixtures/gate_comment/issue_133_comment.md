## Gate evidence for #133 (PR #257)

- **Checks (`gh pr checks <pr>`):** one PR for #133 alone (PR #257 on `issue/133-large-object-rss`, head `3eecc35`); `gh pr checks 257` all green (exit `0`):

```
Dashboard and alert rules	pass	15s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283328058/job/134729480973	
Drive the server (aws CLI)	pass	2m20s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283328058/job/134729481015	
Full matrix (ubuntu-latest)	pass	5m10s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283327824/job/134729480109	
Full matrix (windows-latest)	pass	3m44s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283327824/job/134729479591	
Fuzz attrs_block	pass	2m29s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283327853/job/134729480410	
Fuzz attrs_values	pass	2m26s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283327853/job/134729480271	
Fuzz aws_chunked	pass	2m26s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283327853/job/134729480328	
Fuzz cors_xml	pass	2m30s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283327853/job/134729480124	
Fuzz form_upload	pass	2m30s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283327853/job/134729480226	
Fuzz iso8601	pass	2m10s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283327853/job/134729480145	
Fuzz lifecycle_config_xml	pass	2m14s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283327853/job/134729480108	
Fuzz lifecycle_xml	pass	2m26s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283327853/job/134729479727	
Fuzz object_lock_xml	pass	2m17s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283327853/job/134729480182	
Fuzz parse_size	pass	2m7s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283327853/job/134729480510	
Fuzz range	pass	2m33s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283327853/job/134729480378	
Fuzz request_xml	pass	2m9s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283327853/job/134729480219	
Fuzz s3_target	pass	2m19s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283327853/job/134729480367	
Fuzz sigv4	pass	2m32s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283327853/job/134729480243	
Fuzz tagging_xml	pass	2m13s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283327853/job/134729480309	
Fuzz traceparent	pass	2m18s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283327853/job/134729480354	
Fuzz version_id	pass	2m32s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283327853/job/134729480277	
Model check (TLA+)	pass	34s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283328058/job/134729480545	
Test (ubuntu-latest)	pass	6m57s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283328058/job/134729481133	
Test (windows-latest)	pass	8m4s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283328058/job/134729480997	
s3tests	pass	18m1s	https://github.com/iskandarnorazli/Awanfs/actions/runs/44283327954/job/134729479902	
```

- The summary carries peak RSS per workload for AwanFS and MinIO, median of 3 rounds: `bench-selfhosted.yml` run `44257320568` (`LAPTOP-6RBIQUM5`, 16 CPUs, 3 rounds) completed with `selfhosted_full.md` reporting 3-round median + min..max spread for AwanFS (`110.5` to `1210.5` MB) and MinIO (`548.1` to `994.6` MB) across all 12 workloads (`64 KiB` to `10 GiB`), plus AwanFS Peak In-Flight Request Bytes (`4.00` MB at 64 KiB, `32.00` MB at 1 MiB–10 GiB). Note: `10 GiB Volume PUT` for AwanFS recorded `did not finish: 8 workers did not finish their request` while reporting `1180.0 MB (1170.2..1186.9)` peak RSS.
- `measurements/<date>-large-object-rss.md` (`measurements/2026-11-18-large-object-rss.md`) holds that table, the command on line 1 (`gh workflow run bench-selfhosted.yml --ref issue/133-large-object-rss -f runs=3 -f duration=30`), run id `44257320568`, AwanFS Peak In-Flight Request Bytes, and `_rss_ceiling_source`.
- The self-hosted report fails the run when AwanFS's median peak RSS is over the ceiling the measurement sets: `.github/workflows/bench-selfhosted.yml` passes `--rss-ceiling-mib ${{ inputs.rss_ceiling_mib || '2500' }}` (`_rss_ceiling_source`: run `44257320568` measured `1210.5` MiB median peak RSS -> ceiling `2500.0` MiB ~= `2.07x`), run `44257320568` reported `ok` (`1210.5` <= `2500.0`), `python benchmarks/test_run_warp_benchmark.py` passed (`Ran 23 tests ... OK`, including `test_check_rss_ceiling_passes_under_and_fails_over_ceiling`), `python benchmarks/test_ratio_check.py` passed (`Ran 14 tests ... OK`), and `python scripts/check_workflow_outputs.py` exited `0` (`4 --output paths, none of them committed files`).
- `gh workflow run bench-ratio.yml --ref <branch>` (`gh workflow run bench-ratio.yml --ref issue/133-large-object-rss`) stayed green on run `44283336936`: AwanFS peak RSS `96.6` MiB (ceiling `250.0` MiB, `ok`) vs MinIO `586.6` MiB, and all six rows `ok` (`put_64k` `5.38`, `get_64k` `1.59`, `range_get_4k` `1.64`, `mixed_64k` `1.79`, `put_1m` `0.98`, `get_1m` `1.92`).

