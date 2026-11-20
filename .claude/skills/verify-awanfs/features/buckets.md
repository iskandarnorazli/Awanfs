# Buckets

## Sub-features
- Create (`PUT /b`), list (`GET /` with `prefix`, `max-buckets`, `continuation-token`, #114), head (`HEAD /b`), delete (`DELETE /b`)
- Creating a bucket that exists answers 200 and keeps it, not `BucketAlreadyOwnedByYou`
- Name validation: 3 to 63 characters, otherwise 400 `InvalidBucketName`; length is the only rule checked (#140)
- Delete refuses a bucket with objects or versions: 409 `BucketNotEmpty`
- Bucket subresources handled elsewhere in this map: `?versioning` and `?versions` ([versioning.md](versioning.md)), `?uploads` ([multipart.md](multipart.md)), `?delete` ([objects.md](objects.md)), `?tagging` ([tagging.md](tagging.md)), `?cors` ([cors.md](cors.md)), listing ([listing.md](listing.md))
- Also answered, not driven: `GET ?location` (always `us-east-1` or configured `--region`), `PUT`/`GET`/`DELETE ?lifecycle` (stored only, see the README)

## How to get to it (user POV)
`create-bucket`, `list-buckets` (`--prefix`, `--max-buckets`, `--continuation-token`), `head-bucket`, `delete-bucket`.

## Driving it with the aws CLI
- `j_buckets`. Proof: `verify-bkt` is listed and heads 200; `ab` is refused with `InvalidBucketName`; a second bucket with an object refuses deletion with `BucketNotEmpty`, deletes once empty, and then heads 404.
- `j_list_buckets`. Proof: with three `verify-lb-*` buckets, `list-buckets --prefix verify-lb- --max-buckets 1 --no-paginate` walks all three one per page via `ContinuationToken` (with `Prefix` echoed and `None` on the last page), auto-pagination gathers all three, and an invalid `--continuation-token` returns `InvalidArgument`.

## Gotchas
- `j_buckets` always runs first: every other journey writes into `verify-bkt`. Journeys that need their own state make their own bucket.
- `CreationDate` in `list-buckets` is a constant, `2026-11-10T00:00:00.000Z` (#140).
- `?acl` and `?policy` have no handler and fall through to list-objects (#122).

Source: `server/src/ops/bucket.rs`, `server/src/ops/service.rs`, `server/src/meta_index.rs` `create_bucket`.
