# AwanFS feature map

| Feature | Journey | File |
|---|---|---|
| Buckets | `buckets` | [buckets.md](buckets.md) |
| Objects: PUT, GET, HEAD, DELETE, multi-object delete | `objects` | [objects.md](objects.md) |
| Range GET | `ranges` | [ranges.md](ranges.md) |
| Multipart upload | `multipart` | [multipart.md](multipart.md) |
| CopyObject | `copy` | [copy.md](copy.md) |
| Versioning | `versioning` | [versioning.md](versioning.md) |
| Listing | `listing` | [listing.md](listing.md) |
| Restart keeps data | `restart` | [restart.md](restart.md) |

Signatures are checked on every journey: `doctor` proves an unsigned request gets 403 and a signed one 200, and every journey runs signed with the keys the script gives the server.

## Not mapped yet

Each has a source path, and none has a journey. The reason is in brackets.

- Lifecycle rules, `?lifecycle` (`lifecycle.rs`): stored and returned, but nothing expires objects (#118).
- Presigned URLs (`auth.rs`, `X-Amz-Expires`): the aws CLI can make one (`aws s3 presign`), not yet driven.
- IAM users and policies, `--config` (`iam.rs`): needs a config file and a second key pair; covered by `iam_test.rs` (#53, #130).
- Per-bucket compression, `[[buckets]] compression = "zstd"` (`compress.rs`): needs `--config`; covered by `compression_test.rs` (#51).
- Object lock, ACLs, policies, tagging: not implemented (#120, #122, #115).
- Operations surface, for an operator rather than an S3 client: health (`/_awanfs/health/live`, `/ready`), metrics (`/_awanfs/metrics`), TLS (`--cert`, `--key`, `--http-port`), the access log (`--access-log`), the request log on stderr (`AWANFS_LOG`, #138), compaction (`--auto-compaction`, `--compaction-rate`), traces (`--otlp-endpoint`). Covered by Rust tests, not by this skill.
