# Objects

## Sub-features
- PUT, GET, HEAD, DELETE of a 70,000-byte object under a prefix

## How to get to it (user POV)
`put-object`, `get-object`, `head-object`, `delete-object`.

## Driving it with the aws CLI
`j_objects`. Proof: the downloaded bytes equal the uploaded file (`cmp`), HEAD reports the length, HEAD after DELETE fails.

## Gotchas
Random bytes, so a cache or an all-zero body cannot pass by accident.
