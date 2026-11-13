# Restart keeps data

## Sub-features
- Plain objects, deleted keys (tombstones), completed multipart objects, versioned objects and their bucket's versioning status survive a stop and start on the same data directory
- Uploads in progress do not survive (their part files stay in `.staging/`)
- Durability, `--durability` or `AWANFS_DURABILITY`: `group` (default) and `strict` acknowledge a PUT only after its fsync, which is what survives a power cut; `fast` acknowledges first and syncs about once a second. A graceful stop syncs in every mode.

## How to get to it (user POV)
Stopping and starting the server process.

## Driving it with the aws CLI
`j_restart`. Before the restart it writes a plain object, a key it then deletes, a two-part multipart object, and two versions in a versioned bucket. After: the plain object and the multipart object read back equal (`cmp`), the deleted key heads 404, the bucket is still `Enabled`, and the first version reads back by its id.

## Gotchas
- A process stop is not a power cut, and nothing here or in the tests cuts power. `durability_test.rs` counts the fsyncs each mode issues before it acknowledges and kills the server in each mode; a kill keeps the page cache, so it proves the acknowledgement order, not survival of a power loss.
- Recovery rebuilds the index by scanning the volumes, so restart time grows with the data.

Source: `server/src/s3_api.rs` `try_with_data_dir` (~649), manifest replay (~680), needle replay (~715-840); `server/src/volume.rs` `scan_needles` (~975), durability (~222, ~659).
