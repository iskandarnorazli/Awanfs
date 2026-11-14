# Listing objects

## Sub-features
- `GET /b` (ListObjects V1) and `?list-type=2` (V2), with `prefix`, `delimiter` (`CommonPrefixes`), `max-keys` (capped at 1000), `marker` / `continuation-token`, `encoding-type=url`

## How to get to it (user POV)
`list-objects-v2`, `list-objects`.

## Driving it with the aws CLI
`j_listing`, in its own bucket with keys `a/1 a/2 b/1 top`. Proof: with delimiter `/` the common prefixes are `a/ b/` and the only key is `top`; prefix `a/` lists `a/1 a/2`; one key per page gives a continuation token, and the next page from it lists `a/2 b/1 top`.

## Gotchas
- `--no-paginate` is needed: the aws CLI otherwise follows continuation tokens itself and hides the paging.
- V1 and `encoding-type=url` are not driven here; the ceph s3-tests ratchet covers them (#114, #123).

Source: `server/src/s3_api.rs` bucket GET arm (~2321-2391).
