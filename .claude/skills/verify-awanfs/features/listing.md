# Listing objects

## Sub-features
- `GET /b` (ListObjects V1) and `?list-type=2` (V2), with `prefix`, `delimiter` (`CommonPrefixes`), `max-keys` (capped at 1000), `marker` / `continuation-token`, `encoding-type=url`

## How to get to it (user POV)
`list-objects-v2`, `list-objects`.

## Driving it with the aws CLI
- `j_listing`, in its own bucket with keys `a/1 a/2 b/1 top` and `sp key`. Proof: with delimiter `/` the common prefixes are `a/ b/` and the only key is `top`; prefix `a/` lists `a/1 a/2`; one key per page gives a continuation token, and the next page from it lists `a/2 b/1 top`; `list-objects-v2 --encoding-type url` returns `sp%20key`; `list-objects` returns `Owner.ID`.
- `j_v1_prefix`, in `verify-v1p` with key `sp dir/item 1`. Proof: `list-objects` (V1) with `--prefix "sp "` leaves `<Prefix>` raw (`"sp "`) both when botocore auto-sets `encoding-type=url` and when `--encoding-type url` is passed explicitly (where `Key` is `sp%20dir/item%201`), while `list-objects-v2 --encoding-type url` returns `sp%20` for `Prefix` (#114).
- Bucket listing pagination (`list-buckets`) is driven by `j_list_buckets` ([buckets.md](buckets.md)).

## Gotchas
- `--no-paginate` is needed: the aws CLI otherwise follows continuation tokens itself and hides the paging.
- Botocore's `decode_list_object` for V1 does not URL-decode top-level `<Prefix>`, so V1 must emit `<Prefix>` unencoded even with `encoding-type=url` (#114).

Source: `server/src/ops/bucket.rs` `list_objects`, `server/src/ops/service.rs` `list_buckets`.
