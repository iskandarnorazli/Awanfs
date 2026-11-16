# Object Tagging

## Sub-features
- `PUT` / `GET` / `DELETE /b/k?tagging` (and `?versionId=...`)
- `x-amz-tagging` header at `PutObject`
- `x-amz-tagging-count` (`TagCount`) returned on `GET` / `HEAD` when tags exist (#115)

## How to get to it (user POV)
`put-object-tagging`, `get-object-tagging`, `delete-object-tagging`, `put-object --tagging`, `head-object`.

## Driving it with the aws CLI
`j_tagging`. Proof: `put-object --tagging "a=1&b=2"` + `head-object` shows `"TagCount": 2`; `get-object-tagging` returns `a=1, b=2`; `put-object-tagging` replaces with `c=3`; `delete-object-tagging` clears tags so `get-object-tagging` returns empty `TagSet`.

## Gotchas
- Bucket-level tagging (`PUT`/`GET`/`DELETE /b?tagging`) is not implemented (501 `NotImplemented`).

Source: `server/src/ops/tagging.rs`, `server/src/ops/object.rs`, `server/src/attrs.rs` (`TAG_TAGGING = 0x0010`).
