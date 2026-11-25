# Conditional writes and deletes

## Sub-features
- `PutObject` with `If-None-Match` (`*` or ETag) and `If-Match` (`*` or ETag): 412 `PreconditionFailed` on mismatch, 404 `NoSuchKey` when `If-Match` targets an absent object or delete marker, evaluated at commit under the key lock.
- `CompleteMultipartUpload` with `If-None-Match` and `If-Match`, evaluated at commit under the key lock.
- `DeleteObject` with `If-Match`, `x-amz-if-match-last-modified-time`, and `x-amz-if-match-size` (204 when absent, 412 on mismatch).
- `DeleteObjects` (`POST ?delete`) with `<ETag>`, `<LastModifiedTime>`, and `<Size>` per object, returning `PreconditionFailed` in `<Error>` on mismatch.

## How to get to it (user POV)
`put-object --if-none-match`, `put-object --if-match`, `complete-multipart-upload --if-none-match`, `delete-object --if-match`, `delete-objects`.

## Driving it with the aws CLI
`j_conditional`. Proof: `put-object --if-match "*"` on a missing key returns `NoSuchKey`; `--if-none-match "*"` succeeds once and fails `PreconditionFailed` on repeat; `--if-match` CAS succeeds with matching ETag and fails with wrong ETag; `delete-object --if-match` rejects wrong ETag and deletes on matching ETag; `delete-objects` returns `PreconditionFailed` in `Errors` on mismatch and deletes on matching ETag; `complete-multipart-upload --if-none-match "*"` succeeds on new key.

Source: `server/src/precondition.rs`, `server/src/ops/object.rs`, `server/src/ops/multipart.rs`, `server/src/ops/bucket.rs`, `server/src/s3_api.rs`.
