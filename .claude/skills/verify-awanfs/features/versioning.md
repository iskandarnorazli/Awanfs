# Versioning

## Sub-features
- `PUT`/`GET ?versioning` with `Enabled` or `Suspended`
- Every PUT to an enabled bucket makes a new version with its own id; GET returns the newest
- `GET ?versionId=` reads an older version
- DELETE without an id writes a delete marker: the key then answers 404, older versions stay readable by id
- `GET /b?versions` (ListObjectVersions) lists versions and delete markers
- `Suspended` buckets record the `null` version (`GET`/`HEAD`/`ListObjectVersions` report `VersionId: null`), keep earlier `Enabled` versions readable by id, and omit `x-amz-version-id` from the `PutObject` HTTP response (#113)
- Also answered, not driven: `DELETE ?versionId=` removes one version

## How to get to it (user POV)
`put-bucket-versioning`, `get-bucket-versioning`, `put-object`, `get-object --version-id`, `list-object-versions`, `delete-object`.

## Driving it with the aws CLI
- `j_versioning`, in its own bucket. Proof: two PUTs return different `VersionId`s; GET returns the second (`cmp`); GET by the first id returns the first (`cmp`); two versions are listed; after DELETE the key heads 404, one delete marker is listed, and the second version still reads back by id.
- `j_suspended`, in `verify-susp`. Proof: after suspending an `Enabled` bucket, `put-object` (both buffered and >4 MiB streamed) omits `VersionId` (`None`), while `head-object`, `get-object`, and `list-object-versions` see `null`, and the earlier `Enabled` version stays readable by `--version-id` (`cmp`).

## Gotchas
- Versioning lives in `verify-ver` so it does not change how DELETE behaves for the other journeys.
- Restart survival is in [restart.md](restart.md).

Source: `server/src/versions.rs`; `server/src/s3_api.rs` `?versioning` (~2085, ~2108), `version_get_response` (~1579), the delete arm (~3479).
