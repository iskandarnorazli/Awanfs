# Object & Bucket Tagging

## Sub-features
- `PUT` / `GET` / `DELETE /b/k?tagging` (and `?versionId=...`)
- `PUT` / `GET` / `DELETE /b?tagging` (bucket tagging, 404 `NoSuchTagSet` when unset, sorted by key, persisted in `buckets.manifest`, #115)
- `x-amz-tagging` header at `PutObject` and `CreateMultipartUpload` (#115)
- `CopyObject` `x-amz-tagging-directive` (`COPY` default, `REPLACE`)
- `x-amz-tagging-count` (`TagCount`) returned on `GET` / `HEAD` when tags exist (#115)

## How to get to it (user POV)
`put-object-tagging`, `get-object-tagging`, `delete-object-tagging`, `put-object --tagging`, `head-object`, `put-bucket-tagging`, `get-bucket-tagging`, `delete-bucket-tagging`, `create-multipart-upload --tagging`, `copy-object --tagging-directive`.

## Driving it with the aws CLI
- `j_tagging`: object tagging roundtrip via `put-object --tagging "a=1&b=2"`, `head-object` (`TagCount: 2`), `get-object-tagging`, `put-object-tagging` replace, and `delete-object-tagging`.
- `j_bucket_tagging`: `get-bucket-tagging` fails `NoSuchTagSet` when unset; `put-bucket-tagging` with unsorted keys (`z=2`, `a=1`); `get-bucket-tagging` returns sorted `a=1`, `z=2`; `delete-bucket-tagging` clears tags back to `NoSuchTagSet`; `create-multipart-upload --tagging` preserves tags on completion; `copy-object` preserves tags on default `COPY`, replaces with `--tagging-directive REPLACE --tagging`, and clears with `--tagging-directive REPLACE` alone.

Source: `server/src/ops/tagging.rs`, `server/src/ops/object.rs`, `server/src/ops/multipart.rs`, `server/src/bucket_config.rs`, `server/src/attrs.rs` (`TAG_TAGGING = 0x0010`).
