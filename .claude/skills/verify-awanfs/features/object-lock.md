# Object lock, retention, and legal hold

## Sub-features
- `PUT`/`GET /<bucket>?object-lock`: enables Object Lock on versioned buckets (including enabling after creation when versioning is already `Enabled`) with optional `DefaultRetention` (`GOVERNANCE` or `COMPLIANCE`, `Days` or `Years`). Prevents suspending versioning (`409 InvalidBucketState`).
- `PUT`/`GET ?retention` and `PUT`/`GET ?legal-hold` on object versions; `x-amz-object-lock-mode`, `x-amz-object-lock-retain-until-date`, and `x-amz-object-lock-legal-hold` on `PutObject`, `CopyObject`, `CreateMultipartUpload`, and in `GET`/`HEAD` responses.
- Enforces `Content-MD5` or an `x-amz-checksum-*` header on `PutObject` into buckets with Object Lock when lock headers or `DefaultRetention` apply.
- Protection across `DeleteObject`, `DeleteObjects`, `PutBucketVersioning`, compaction dead-version cleanup, and lifecycle expiration through `fsm::lock_step` and `Lock::may_delete`. `COMPLIANCE` retention cannot be shortened or removed even by root; `GOVERNANCE` override requires `x-amz-bypass-governance-retention: true` plus `s3:BypassGovernanceRetention`.

## How to get to it (user POV)
Bucket creation with `--object-lock-enabled-for-bucket` or `put-object-lock-configuration` on an existing versioned bucket, followed by `put-object`, `put-object-retention`, `put-object-legal-hold`, and `delete-object --version-id` (with `--bypass-governance-retention` for `GOVERNANCE`).

## Driving it
`j_lock` in `scripts/verify_drive.sh` creates an object-lock bucket, verifies `put-bucket-versioning Status=Suspended` fails with `InvalidBucketState`, configures 1-day `GOVERNANCE` default retention, uploads an object with `--checksum-algorithm SHA256`, confirms default retention applies, enables legal hold (`ON`), verifies versioned deletion is denied (`AccessDenied`), clears legal hold (`OFF`), and deletes the version using `--bypass-governance-retention`.

Source: `server/src/ops/lock.rs`, `server/src/object_lock.rs`, `server/src/ops/bucket.rs`, `server/src/ops/object.rs`, `server/src/ops/multipart.rs`, `server/src/s3_api.rs`, `server/src/lifecycle.rs`, `server/src/fsm.rs` (`lock_step`, `versioning_step`).
