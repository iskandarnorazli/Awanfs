# Range GET

## Sub-features
- `bytes=a-b`: 206 with `Content-Range: bytes a-b/size`; an end past the object is clamped to `size-1`
- `bytes=a-` (open end) and `bytes=-n` (suffix, the last n bytes)
- A start at or past the end, `a > b`, `bytes=-0`, a malformed spec, several ranges, or any range of a zero-byte object: 416 `InvalidRange` with `Content-Range: bytes */size`
- A header not starting `bytes=` is ignored: 200 with the whole object
- Compressed objects decode only the 64 KiB blocks the range overlaps; multipart objects slice the parts the range crosses

## How to get to it (user POV)
`get-object --range`.

## Driving it with the aws CLI
`j_ranges`, on a 20,000-byte object. Proof, each by `cmp` against a slice of the file and by the `ContentRange` the CLI prints: `bytes=100-4195`, `bytes=-500`, `bytes=19990-`; and `bytes=20000-20010` refused with `InvalidRange`.

## Gotchas
- S3 answers 200 with the whole object for a malformed or multi-range header; AwanFS answers 416. Not driven.
- A range of a specific version (`?versionId`) reads and decodes the whole version before slicing: correct bytes, whole-object cost.
- Ranges inside one part of a multipart object are covered by Rust tests; the journey's object is single-part.

Source: `server/src/s3_api.rs` `parse_range_header` (~332), response (~3299-3412), versioned (~1633); `compress.rs` `decode_range` (~124).
