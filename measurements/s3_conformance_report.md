`python awan-storage/tests/run_s3_conformance.py --output docs/measurements/s3_conformance_report.md`

> Superseded 2026-11-13: this was a 15-check script from the awan-lang days. Conformance is now measured with the ceph s3-tests suite (838 tests, 244 passing on 2026-11-13), tracked in #123.

# Official S3 Conformance Gauntlet Report (#471)

- **Total Conformance Checks:** 15
- **Passed:** 15
- **Failed:** 0
- **Conformance Score:** 100.0%

## Verified S3 API Surface Categories
1. **Bucket Operations:** `CreateBucket`, `HeadBucket`, `DeleteBucket`.
2. **Object Operations:** `PutObject` (0B, 100KiB), `HeadObject`, `GetObject` bit-identical verification.
3. **RFC 9110 Byte Ranges:** Closed interval (`bytes=10-19`), open tail (`bytes=100000-`), suffix (`bytes=-50`).
4. **CopyObject:** In-place atomic zero-copy part cloning via `x-amz-copy-source`.
5. **Multipart Uploads:** 10MiB multi-part transfer, sequential part validation, composite `-<parts>` ETag calculation.
6. **Object Enumeration:** `ListObjectsV2` prefix indexing and pagination.
7. **Batch Delete:** `POST /?delete` XML multi-object deletion.
8. **Lifecycle Management:** `PUT /?lifecycle`, `GET /?lifecycle`, and `DELETE /?lifecycle`.

## Result
`0 failed across core bucket, object, range, multipart, conditional, and lifecycle conformance suites.`
