# CopyObject

## Sub-features
- Copy within a bucket and across buckets (`PUT` with `x-amz-copy-source`)
- The copy writes new bytes: it survives the source being deleted or overwritten
- A missing source key answers 404 `NoSuchKey`; a missing source bucket 404 `NoSuchBucket`; a source with no `/` 400 `InvalidArgument`
- A compressed source is decoded and the copy compressed by the destination bucket's setting
- A versioned destination answers with `x-amz-version-id`
- `?versionId=` in the copy source copies that version and answers with `x-amz-copy-source-version-id`; it may be an old version of a deleted key. An id that names no version answers 404 `NoSuchVersion`, as a GET does; a delete marker's id answers 400 `InvalidRequest` (#139)

## How to get to it (user POV)
`copy-object`.

## Driving it with the aws CLI
`j_copy`. Proof: the copy's bytes equal the source's (`cmp`), in the same bucket and in another; the cross-bucket copy still reads back after the source is deleted; a missing source is refused with `NoSuchKey`; in a versioned bucket, a copy naming the first of two versions reports that id and reads back the first version's bytes.

## Gotchas
- A copy of a multipart object is stored as one plain object, so its ETag is plain hex, not `<hex>-N`.
- Metadata directives (`x-amz-metadata-directive`) are not read.
- Overwrite and restart cases are covered by `key_order_test.rs`.

Source: `server/src/s3_api.rs` copy branch of the PUT arm (~2886-2988), `read_object_bytes` (~1222), `store_object` (~1288).
