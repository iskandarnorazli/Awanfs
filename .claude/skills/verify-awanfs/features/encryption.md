# Server-side encryption (#121)

## Sub-features
- **SSE-C (PR 1/3):** customer-supplied AES-256 keys (`x-amz-server-side-encryption-customer-algorithm: AES256`, `-key`, `-key-MD5`) across single-part `PutObject` (buffered & > 4 MiB streamed), `PostObject`, `GetObject` (full, byte-range touching only overlapped 64 KiB chunks, `?partNumber=N`), `HeadObject`, `GetObjectAttributes`, `CopyObject` (including key rotation and > 4 MiB streamed copy), `CreateMultipartUpload`, `UploadPart`, `UploadPartCopy`, and `CompleteMultipartUpload`.
- Encrypted objects use 64 KiB `AWE1` AES-256-GCM chunks keyed via per-object salted `HKDF-SHA256`, carry `TAG_SSE` (`0x0050`), skip zstd compression (Decision 71), and never cache plaintext in `payload_cache`.

## How to get to it (user POV)
Pass `--sse-customer-algorithm AES256 --sse-customer-key <base64> --sse-customer-key-md5 <md5>` to `aws s3api` commands; omit or send a wrong key to verify 400 refusal; supply `--copy-source-sse-customer-*` on `copy-object` to rotate keys.

## Driving it
`j_encryption` in `scripts/verify_drive.sh` verifies single-part SSE-C PUT/GET/HEAD, missing/wrong key 400 refusal, ranged GET, `copy-object` key rotation, and encrypted multipart upload end-to-end.

Source: `server/src/sse.rs`, `server/src/attrs.rs`, `server/src/fsm.rs` (`upload_step`), `server/src/s3_api.rs`, `server/src/ops/object.rs`, `server/src/ops/multipart.rs`.
