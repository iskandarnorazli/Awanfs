# Objects

## Sub-features
- PUT, GET, HEAD, DELETE of a single object
- ETag of a single-part object is its quoted MD5
- Zero-byte objects: stored, ETag `"d41d8cd98f00b204e9800998ecf8427e"`, empty body on GET
- DELETE of a key that does not exist answers 204, as S3 does
- Multi-object delete, `POST /b?delete`: each key gets a tombstone; the answer lists `Deleted` and `Error`
- Also answered, not driven: `Content-MD5` checked (400 `BadDigest`), `If-Match` (412) and `If-None-Match` (304) on GET; keys rejected when empty, over 1024 bytes, holding control bytes, or with a `..` segment

## How to get to it (user POV)
`put-object`, `get-object`, `head-object`, `delete-object`, `delete-objects`.

## Driving it with the aws CLI
`j_objects`. Proof: the ETag equals the file's MD5; the downloaded bytes equal the uploaded file (`cmp`); HEAD reports the length; a zero-byte object round-trips with the empty-MD5 ETag; DELETE of a missing key succeeds; `delete-objects` reports both keys deleted and both then head 404.

## Gotchas
- Random bytes, so a cache or an all-zero body cannot pass by accident.
- `Content-Type` on GET is always `application/octet-stream` and `Last-Modified` is a constant (#140); stored metadata (`x-amz-meta-*`) is not kept.

Source: `server/src/s3_api.rs` object arm (~2482), PUT (~2884), GET/HEAD (~3240), DELETE (~3479), `POST ?delete` (~2393).
