# Object Checksums & GetObjectAttributes

## Sub-features
- `PutObject`, `UploadPart`, `CompleteMultipartUpload` with `CRC32`, `CRC32C`, `CRC64NVME`, `SHA1`, `SHA256` (#117)
- `GetObject` (whole-object `200 OK`) and `HeadObject` with `x-amz-checksum-mode: ENABLED`
- `GetObject` / `HeadObject` with `?partNumber=` on a multipart object returns `x-amz-checksum-type` (`FULL_OBJECT` or `COMPOSITE`) and the part's `x-amz-checksum-<alg>` (#117)
- Malformed or wrong-length `x-amz-checksum-*` header values fail with `400 BadDigest` (#117)
- `GET /b/k?attributes` (`GetObjectAttributes`: `ETag`, `Checksum`, `ObjectParts`, `StorageClass`, `ObjectSize`)

## How to get to it (user POV)
`put-object --checksum-algorithm SHA256|CRC64NVME`, `head-object --checksum-mode ENABLED`, `get-object --checksum-mode ENABLED`, `get-object --part-number`, `create-multipart-upload --checksum-algorithm ... --checksum-type FULL_OBJECT|COMPOSITE`, `get-object-attributes --object-attributes Checksum ObjectSize`.

## Driving it with the aws CLI
- `j_checksums`. Proof: `put-object --checksum-algorithm SHA256` returns `ChecksumSHA256`; `head-object --checksum-mode ENABLED` returns matching `ChecksumSHA256`; `get-object-attributes` returns `Checksum.ChecksumSHA256` and `ObjectSize`.
- `j_checksums_extra`. Proof: `CRC64NVME` and `SHA256` round-trip via `put-object`, `head-object`, `get-object --checksum-mode ENABLED` (`cmp`), and `get-object-attributes`; multipart `CRC64NVME` (`FULL_OBJECT`) and `SHA256` (`COMPOSITE`) return expected `ChecksumType` and part checksums on `get-object --part-number 1`; invalid `--checksum-crc64-nvme AAAA` and `--checksum-sha256 bad!` return `BadDigest`.

## Gotchas
- Byte-range `GET` (`206 Partial Content`) intentionally omits `x-amz-checksum-*` headers (matching S3, so `aws` CLI does not compare a whole-object checksum against a partial byte range).

Source: `server/src/checksum.rs`, `server/src/ops/object.rs`, `server/src/ops/multipart.rs`.
