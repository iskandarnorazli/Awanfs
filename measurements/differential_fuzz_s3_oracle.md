`python awan-storage/tests/differential_minio_fuzz.py --count 10000 --seed 20261110`

# S3 Differential Testing & Metamorphic Fuzzing Oracle Report (#465)

- **Target Under Test:** `awan-storage-server` (live TCP HTTP/1.1 wire reactor via `boto3`)
- **Differential Oracle:** In-process RFC 9110 `ReferenceS3Model` Oracle (upstream `dl.min.io/server/minio/release/windows-amd64/minio.exe` returned `HTTP 410 Gone` as open-source MinIO binaries were archived; `differential_minio_fuzz.py` automatically falls back to the deterministic RFC 9110 / S3 wire reference oracle when `--minio-endpoint` is not reachable)
- **Total Operations:** 10000 / 10000
- **Divergences / Mismatches:** 0
- **Elapsed Wall-Clock Time:** 98.96 s
- **Effective Throughput:** 101.1 ops/sec
- **RNG Seed:** 20261110

## Verified Fuzzing Dimensions
1. **Key Namespaces:** Deep hierarchy (`a/b/c/d/e`), URL-encoded characters (`%20`, `+`, `&`, `=`), UTF-8 multi-byte characters (`üñîçødé`, `日本語`), directory markers ending in `/`, and overlong keys (up to 1024B).
2. **Payload Extremes:** `0 B`, `1 B`, `13 B`, `4095 B`, `4096 B` (sector boundary), `4097 B`, `64 KiB`, and `5 MiB` boundaries.
3. **Byte Ranges (RFC 9110):** Closed sub-slices `bytes=start-end`, open tail slices `bytes=start-`, suffix queries `bytes=-suffix`, and boundary queries.
4. **Wire Invariants:** Bit-identical body comparison and MD5 ETag equality.

## Result
`10000/10000 operations matched (0 mismatches)`
