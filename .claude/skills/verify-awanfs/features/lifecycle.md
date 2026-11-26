# Bucket lifecycle configuration and expiration sweep

## Sub-features
- `PUT`/`GET`/`DELETE /<bucket>?lifecycle`: persists validated rules in `buckets.manifest` (`ConfigKind::Lifecycle`) across restarts, refusing empty actions, `Days <= 0`, non-midnight ISO-8601 `Date`, `>255`-char or duplicate rule `ID`, invalid `<Status>`, and `>1000` rules (`400 InvalidArgument` / `MalformedXML`).
- Filters: legacy top-level `<Prefix>` (round-tripped without `<Filter>`), `<Filter><Prefix>`, `<Tag>`, `<ObjectSizeGreaterThan>`, `<ObjectSizeLessThan>`, and `<And>` compound predicates; `<Transition>` rules round-trip cleanly.
- `x-amz-expiration` response header returned on `PUT`, `GET`, and `HEAD` (including current versioned objects) when an enabled `Expiration` rule with `Days` or `Date` matches.
- Background sweep driven strictly by `fsm::expiry_step` under `lock_key`: expires current objects (inserting delete markers on versioned buckets), expires noncurrent versions respecting `NewerNoncurrentVersions`, cleans up sole delete markers (`ExpiredObjectDeleteMarker` or elapsed `Days`/`Date`), and aborts stale incomplete multipart uploads (`AbortIncompleteMultipartUpload`). Always respects Object Lock retention and legal hold via `fsm::Lock::may_delete`. Exposes Prometheus expiration metrics (`awanfs_lifecycle_objects_expired_total`, `awanfs_lifecycle_bytes_expired_total`, `awanfs_lifecycle_uploads_aborted_total`).

## How to get to it (user POV)
Configure rules with `put-bucket-lifecycle-configuration`, verify `x-amz-expiration` on `put-object` / `head-object`, inspect or delete rules with `get-bucket-lifecycle-configuration` / `delete-bucket-lifecycle`, and use `--lifecycle-day-seconds` when testing shortened lifecycle days.

## Driving it
`j_lifecycle` in `scripts/verify_drive.sh` verifies `put-bucket-lifecycle-configuration`, `get-bucket-lifecycle-configuration`, `x-amz-expiration` headers on `put-object` and `head-object`, `InvalidArgument` rejection of `Days: 0`, `delete-bucket-lifecycle` followed by `NoSuchLifecycleConfiguration`, and end-to-end background sweep expiration of current, noncurrent, and delete-marker versions on an isolated `--lifecycle-day-seconds 1` instance.

Source: `server/src/lifecycle_config.rs`, `server/src/lifecycle.rs`, `server/src/ops/lifecycle.rs`, `server/src/ops/object.rs`, `server/src/s3_api.rs`, `server/src/bucket_config.rs`, `server/src/fsm.rs` (`expiry_step`).
