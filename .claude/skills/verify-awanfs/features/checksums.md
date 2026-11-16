# Object Checksums & GetObjectAttributes

## Sub-features
- `PutObject`, `UploadPart`, `CompleteMultipartUpload` with `CRC32`, `CRC32C`, `CRC64NVME`, `SHA1`, `SHA256` (#117)
- `GetObject` (whole-object `200 OK`) and `HeadObject` with `x-amz-checksum-mode: ENABLED`
- `GET /b/k?attributes` (`GetObjectAttributes`: `ETag`, `Checksum`, `ObjectParts`, `StorageClass`, `ObjectSize`)

## How to get to it (user POV)
`put-object --checksum-algorithm SHA256`, `head-object --checksum-mode ENABLED`, `get-object-attributes --object-attributes Checksum ObjectSize`.

## Driving it with the aws CLI
`j_checksums`. Proof: `put-object --checksum-algorithm SHA256` returns `ChecksumSHA256`; `head-object --checksum-mode ENABLED` returns matching `ChecksumSHA256`; `get-object-attributes` returns `Checksum.ChecksumSHA256` and `ObjectSize`.

## Gotchas
- Byte-range `GET` (`206 Partial Content`) intentionally omits `x-amz-checksum-*` headers (matching S3, so `aws` CLI does not compare a whole-object checksum against a partial byte range).

Source: `server/src/checksum.rs`, `server/src/ops/object.rs`, `server/src/ops/multipart.rs`.
