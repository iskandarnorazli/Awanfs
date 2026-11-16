# Object Metadata & Copy Directives

## Sub-features
- Standard headers (`Content-Type`, `Content-Encoding`, `Content-Disposition`, `Content-Language`, `Cache-Control`, `Expires`, `x-amz-website-redirect-location`)
- User metadata (`x-amz-meta-*`, lower-cased, 2 KiB limit, CR/LF refused)
- `CopyObject` `x-amz-metadata-directive` (`COPY` default, `REPLACE`)
- `GET` / `HEAD` `response-*` query overrides (refused for anonymous requests when auth is enabled) (#152)

## How to get to it (user POV)
`put-object --metadata ... --cache-control ...`, `head-object`, `copy-object --metadata-directive REPLACE`.

## Driving it with the aws CLI
`j_metadata`. Proof: `put-object --metadata owner=alice --cache-control max-age=60` round-trips on `head-object`; `copy-object --metadata-directive REPLACE --metadata owner=bob` updates `Metadata.owner` to `bob` on `head-object`.

## Gotchas
- Non-ASCII header values in SigV4 canonicalization remain a follow-up (`server/src/sigv4.rs` reserved).

Source: `server/src/ops/object.rs`, `server/src/attrs.rs` (`0x0002`..`0x0008`).
