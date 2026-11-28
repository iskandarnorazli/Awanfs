# Access control: bucket policies, ACLs, Ownership Controls, and Public Access Block

## Sub-features
- `PUT` / `GET` / `DELETE /b?policy`, `GET /b?policyStatus`
- `PUT` / `GET /b?acl`, `PUT` / `GET /b/k?acl`
- `PUT` / `GET` / `DELETE /b?ownershipControls` (`BucketOwnerEnforced`, `BucketOwnerPreferred`, `ObjectWriter`)
- `PUT` / `GET` / `DELETE /b?publicAccessBlock`
- Anonymous requests when `--allow-anonymous` is off (`s3:GetObject` / `s3:ListBucket` / `s3:PutObject` governed by policy and ACLs)

## How to get to it (user POV)
`put-bucket-policy`, `get-bucket-policy`, `get-bucket-policy-status`, `delete-bucket-policy`, `put-bucket-ownership-controls`, `get-bucket-ownership-controls`, `delete-bucket-ownership-controls`, `put-bucket-acl`, `get-bucket-acl`, `put-object-acl`, `get-object-acl`, `put-public-access-block`, `get-public-access-block`, `delete-public-access-block`, and `--no-sign-request` calls.

## Driving it with the aws CLI
`j_access`. Proof: `curl --no-sign-request` fails with 403 before a policy is set; `put-bucket-policy` allows anonymous `s3:GetObject` and `get-bucket-policy-status` reports `IsPublic=true`; `curl --no-sign-request` returns the object; `put-public-access-block` with `RestrictPublicBuckets=true` blocks anonymous reads again with 403; `put-bucket-ownership-controls` (`ObjectWriter`) + `put-bucket-acl --acl public-read` + `get-bucket-acl` round-trips; cleanup deletes PAB, policy, and ownership controls.

## Gotchas
- New buckets default to `BucketOwnerEnforced` unless `--default-bucket-ownership ObjectWriter` is passed (for example in `tests/s3tests/start_server.sh`).
- Bucket policies are parsed strictly on PUT (unknown Condition operators/keys, `NotPrincipal`, unknown principals, empty Resource/Action are refused); `BlockPublicPolicy` rejects public policies on PUT.

Source: `server/src/policy.rs`, `server/src/acl.rs`, `server/src/ops/policy.rs`, `server/src/ops/acl.rs`, `server/src/ops/mod.rs`.
