# AwanFS feature map

> **Source of truth:** the repository feature registry is [`features.toml`](../../../../features.toml) (status, owners, `ceph` s3-tests, `server_tests`, `drive` journeys, `fuzz` targets) and its generated report is [`FEATURES.md`](../../../../FEATURES.md). Run `python3 scripts/features.py check` to verify every journey and test reference stays in sync.

| Feature | Journey | File |
|---|---|---|
| Buckets | `buckets` | [buckets.md](buckets.md) |
| Objects: PUT, GET, HEAD, DELETE, multi-object delete, conditional 304/412 | `objects` | [objects.md](objects.md) |
| Range GET | `ranges` | [ranges.md](ranges.md) |
| Multipart upload | `multipart` | [multipart.md](multipart.md) |
| CopyObject | `copy` | [copy.md](copy.md) |
| Versioning | `versioning` | [versioning.md](versioning.md) |
| Listing | `listing` | [listing.md](listing.md) |
| Object tagging | `tagging` | [tagging.md](tagging.md) |
| Bucket tagging, multipart & copy tagging | `bucket_tagging` | [tagging.md](tagging.md) |
| Bucket CORS | `cors` | [cors.md](cors.md) |
| Checksums & GetObjectAttributes | `checksums` | [checksums.md](checksums.md) |
| Checksums CRC64NVME, SHA256, part checksums & BadDigest | `checksums_extra` | [checksums.md](checksums.md) |
| ListBuckets pagination & prefix | `list_buckets` | [buckets.md](buckets.md) |
| ListObjects v1 prefix encoding | `v1_prefix` | [listing.md](listing.md) |
| Suspended bucket omits PutObject VersionId | `suspended` | [versioning.md](versioning.md) |
| Object metadata & overrides | `metadata` | [metadata.md](metadata.md) |
| Restart keeps data | `restart` | [restart.md](restart.md) |

Signatures are checked on every journey: `doctor` proves an unsigned request gets 403 and a signed one 200, and every journey runs signed with the keys the script gives the server.

## Not mapped yet

Each has a source path, and none has a journey. The reason is in brackets.

- Lifecycle rules, `?lifecycle` (`lifecycle.rs`): stored and returned, but nothing expires objects (#118).
- Presigned URLs (`auth.rs`, `X-Amz-Expires`): the aws CLI can make one (`aws s3 presign`), not yet driven.
- IAM users and policies, `--config` (`iam.rs`): needs a config file and a second key pair; covered by `iam_test.rs` (#53, #130).
- Per-bucket compression, `[[buckets]] compression = "zstd"` (`compress.rs`): needs `--config`; covered by `compression_test.rs` (#51).
- Object lock, ACLs, policies: not implemented (#120, #122).
- `411 MissingContentLength` (`ops/object.rs`, #160) and raw invalid-UTF-8 key bytes (`400 InvalidURI`, `ops/mod.rs`, #140): cannot be driven with the `aws` CLI (`botocore` always sets `Content-Length` or `Transfer-Encoding: chunked`, rejects invalid UTF-8 surrogates, and percent-encodes `%`); covered by `server/tests/s3_details_test.rs` and `tests/s3tests/passing.txt`.
- Operations surface, for an operator rather than an S3 client: health (`/_awanfs/health/live`, `/ready`), metrics (`/_awanfs/metrics`), TLS (`--cert`, `--key`, `--http-port`), the access log (`--access-log`), the request log on stderr (`AWANFS_LOG`, #138), compaction (`--auto-compaction`, `--compaction-rate`), traces (`--otlp-endpoint`). Covered by Rust tests, not by this skill.
