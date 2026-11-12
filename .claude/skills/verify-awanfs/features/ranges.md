# Range GET

## Sub-features
- `Range: bytes=100-4195` of a 20,000-byte object

## How to get to it (user POV)
`get-object --range`.

## Driving it with the aws CLI
`j_ranges`. Proof: the 4,096 bytes equal the same slice cut from the input with `dd`.

## Gotchas
Ranges inside one multipart part are covered by the Rust work-count tests, not here.
