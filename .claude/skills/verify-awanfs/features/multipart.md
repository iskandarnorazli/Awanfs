# Multipart upload

## Sub-features
- Create, upload part, complete, abort, list parts (`GET ?uploadId`), list uploads (`GET /b?uploads`)
- A part's ETag is its quoted MD5; the completed object's ETag is the MD5 of the parts' binary MD5s, then `-<part count>`
- Re-uploading a part number replaces the part
- Complete refuses parts out of order (`InvalidPartOrder`) and parts it does not have or whose ETag differs (`InvalidPart`)
- Abort answers 204; afterwards the upload id answers `NoSuchUpload`
- Upload ids are never reused, also across restarts

## How to get to it (user POV)
`create-multipart-upload`, `upload-part`, `list-parts`, `complete-multipart-upload`, `abort-multipart-upload`, `list-multipart-uploads`.

## Driving it with the aws CLI
`j_multipart`. Proof: each part ETag equals the part's MD5; `list-parts` shows sizes 6,000,000 and 1,000; the completed ETag equals the composite computed locally (`mpetag`); the object's bytes equal both parts concatenated (`cmp`); `upload-part-copy` is refused with `NotImplemented`; a second upload aborted answers `NoSuchUpload` to `list-parts`.

## Gotchas
- Parts under 5 MiB are accepted: the minimum-size check exists but is off by default.
- Uploads in progress do not survive a restart; their staged files stay on disk. Completed multipart objects do survive ([restart.md](restart.md)).
- UploadPartCopy is not implemented. A part request carrying `x-amz-copy-source` is refused with 501 `NotImplemented`; it used to be stored as an empty part and answered 200 (#111).
- `LastModified` in `list-parts` and `Initiated` in `list-multipart-uploads` are constants (#140).

Source: `server/src/multipart.rs` (`create_upload` ~125, `upload_part` ~187, `prepare_complete` ~249); `server/src/s3_api.rs` arms ~2508, ~2522, ~2709, ~2834, ~2858, ~2295.
