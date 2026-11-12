# CopyObject

## Sub-features
- copy within a bucket

## How to get to it (user POV)
`copy-object --copy-source`.

## Driving it with the aws CLI
`j_copy`. Proof: the copy's bytes equal the source's.

## Gotchas
Overwrite and restart of the source are covered by `key_order_test.rs`.
