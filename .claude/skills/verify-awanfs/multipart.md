# Multipart upload

## Sub-features
- create, two parts (6,000,000 and 1,000 bytes), complete, GET

## How to get to it (user POV)
`create-multipart-upload`, `upload-part`, `complete-multipart-upload`.

## Driving it with the aws CLI
`j_multipart`. Proof: the object equals the two parts concatenated.

## Gotchas
The server accepts parts under 5 MiB, which S3 rejects; this journey uses a real-sized first part.
