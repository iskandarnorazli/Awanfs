# POST object (browser form uploads)

## Sub-features
- `POST /<bucket>` with `multipart/form-data`: the `key` field (with `${filename}`), the `file` part last, and fields that become the object's headers: `Content-Type`, `Cache-Control`, `Content-Disposition`, `Content-Encoding`, `Expires`, `x-amz-meta-*`, `x-amz-checksum-*`, and `tagging` (XML).
- SigV4 forms only (`x-amz-algorithm`, `x-amz-credential`, `x-amz-date`, `x-amz-signature`, `policy`). A SigV2 form (`AWSAccessKeyId`, `signature`) is refused with 400. An unsigned form is anonymous, so it needs anonymous access.
- The policy: `expiration` and `conditions` (case-sensitive names); exact match, `starts-with`, `content-length-range`. Every field must be covered except `policy`, `x-amz-signature`, `file`, `x-ignore-*` and `x-amz-checksum-*`, and the bucket always needs a condition.
- Answers: 204 by default, `success_action_status` 200 or 201 (`PostResponse` XML), `success_action_redirect` 303 with `bucket`, `key` and `etag` in the query.
- Refusals (signature 403, policy 400/403, size 400 `EntityTooLarge`/`EntityTooSmall`) happen before any byte is stored. A file above 4 MiB streams, and nothing is stored unless the body ends exactly where its Content-Length says.

## How to get to it (user POV)
An HTML form or an SDK's presigned POST (`generate_presigned_post` in boto3, `createPresignedPost` in the JavaScript SDK). The aws CLI has no POST command.

## Driving it
`j_post` signs the form with Python's standard library, the same way the SDKs do, and checks three things:
- a 204 upload reads back the same bytes, with its `Content-Type` and `x-amz-meta-journey`;
- a 6 MB upload with `success_action_status=201` returns the XML and streams;
- an expired policy gets 403 `AccessDenied` and stores nothing.

Source: `server/src/post_object.rs`, `server/src/ops/post_object.rs`, `server/src/form_upload.rs`, `server/src/fsm.rs` (`form_step`).
