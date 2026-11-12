# Buckets

## Sub-features
- create, list, head

## How to get to it (user POV)
`aws s3api create-bucket`, then list and head it.

## Driving it with the aws CLI
`j_buckets` in `scripts/verify_drive.sh`. Proof: the bucket is in `list-buckets` and `head-bucket` succeeds.

## Gotchas
Bucket names must be 3-63 characters; one-letter names get 400.
