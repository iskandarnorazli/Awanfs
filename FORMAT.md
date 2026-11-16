# AwanFS on-disk format

Format version **3** (`FORMAT_VERSION` in `server/src/volume.rs`). Owner decision 20: a newer
version is refused at startup with an error naming both versions; an older version is read as
it is; nothing is rewritten in place. `server/tests/format_version_test.rs` builds a format 1
volume from this document, byte by byte, and checks the server still serves it.

| Version | Adds |
|---|---|
| 1 | The layout below. |
| 2 | Compressed needles (flag 0x20, #51) and versions (flags 0x40, 0x80, 0x100, #31). A needle with any of these flags is never written to a format 1 volume (the server opens a new volume first), so an older AwanFS never misreads one. |
| 3 | The attributes block (flag 0x400, header bytes 18..20, decision 43, #150). New volumes are written as format 3; a needle with a block is never written to an older volume. A needle without one is byte for byte a format 2 needle, and format 2 volumes are read as they are. An AwanFS that reads only format 2 refuses a format 3 volume at startup (decision 20). |

All integers are little-endian. Sectors are 4096 bytes.

## Data directory

| File | Content |
|---|---|
| `volume_NNNN.dat` | A volume: superblock, then needles. Writes go to the highest id (#37). A growing volume's file may end in zeros past its last needle: past 64 MiB it is extended ahead of its writes (#178), and the next open trims it to its last needle. Its length is never exactly 32 GiB, the length that marks a sparse volume. |
| `volume_NNNN.idx` | The volume's journal: one 42-byte record per needle. |
| `buckets.manifest` | One line per bucket event: `+<id>:<name>` created, `-<id>:<name>` deleted. |
| `metadata.db` | redb spill index (rebuilt from the volumes if lost). To be replaced by metadata segment files and their manifest (#26, decision 59), documented here before they are written. |
| `.staging/upload-<hex>/part_NNNNN.dat` | Multipart part files; the parts of completed objects stay here. |

## Superblock (sector 0 of a volume)

| Bytes | Field |
|---|---|
| 0..4 | magic `0x53555042` ("SUPB") |
| 4..8 | volume id |
| 8..16 | creation time, ns since the Unix epoch |
| 16..20 | CRC32C of bytes 0..16 |
| 20..24 | format version; 0 in volumes written before #50, read as 1 |
| 24..4096 | zero |

## Needle (sector aligned, from offset 4096)

| Bytes | Field |
|---|---|
| 0..4 | magic `0x41574E33` ("AWN3") |
| 4..8 | CRC32C of header bytes 8..64 |
| 8..12 | CRC32C of the payload |
| 12..14 | flags: 0x1 active, 0x2 tombstone, 0x4 multipart, 0x8 shared, 0x10 zero-byte, 0x20 compressed, 0x40 versioned, 0x80 null version, 0x100 version delete, 0x200 attribute update (reserved, #150), 0x400 has an attributes block (`meta_index.rs`) |
| 14..16 | bucket id |
| 16..18 | key length |
| 18..20 | attributes block length (format 3); 0 for none |
| 20..28 | xxh3-64 of the key |
| 28..36 | payload length |
| 36..44 | creation time, ns |
| 44..60 | ETag (MD5) high and low halves |
| 60..64 | reserved |
| 64.. | key, then the attributes block (if any), then payload, then zero padding to the next sector |

The header sector of a needle written as it streams in (#36) is written last, so a crash
leaves a needle that fails its header check, never one that passes with missing data.

## Attributes block (flag 0x400, format 3)

Between the key and the payload; its length is header bytes 18..20, at most 65,535 bytes
(`server/src/attrs.rs`). The payload CRC covers the payload only, as before.

| Bytes | Field |
|---|---|
| 0..4 | CRC32C of the rest of the block |
| 4.. | entries: tag (2), value length (2), value; tags strictly ascending |

A block whose CRC does not match is a read error: its attributes are never served. Readers
skip tags they do not know, and compaction and the copy of a streamed PUT (decision 51) write
the block again unchanged. An object's index row carries flag 0x400 too, so GET and HEAD read
the block only for objects that have one.

### Tag registry

A tag number is never reused. 0x8000 and above are for tests.

| Tag | Name | Group | Owner |
|---|---|---|---|
| 0x0001 | Content-Type, as sent | headers (write-time) | #150 |
| 0x0010 | Object tags (`TagSet`) | tagging (mutable) | #115 |
| 0x0020 | Object checksum (`<ALG>:<TYPE>:<base64>`) | checksum (write-time) | #117 |
| 0x0021 | Multipart part sizes (LE `u64` array) | multipart (write-time) | #117 |

## Journal record (`.idx`, 42 bytes)

key hash (8), volume id (4), offset (8), total length (8), payload length (8), flags (2),
payload CRC32C (4). Recovery uses it to step over gaps left by writes that did not finish.

### Replay order

Recovery replays the volumes oldest first and each volume's needles in offset order. For a key
without versions the last needle or tombstone wins. A needle's offset is reserved when its
write starts, and a streamed PUT (above 4 MiB) reserves before its body arrives, so the server
keeps one rule when it writes: among the committed records of a key, a later commit has a
later offset. A streamed PUT that finds a record of its key logged after its reservation is
written again at the end of the log before it is indexed, and its first needle stays behind as
an older record of the same object (decision 51). Versions are ordered by their creation
times instead; see Versions.

## Compressed payload (flag 0x20, format 2)

Buckets with `compression = "zstd"` in the config file (`[[buckets]]`, decision 22) store a
single-part object of up to 4 MiB (`STREAM_PUT_MIN`) this way when its first 64 KiB shrink by
at least an eighth (`server/src/compress.rs`). Streamed and multipart objects are stored as
they are. The payload length in the header and the payload CRC are those of these bytes; the
index row holds the object's size, which recovery reads from bytes 8..16.

| Bytes | Field |
|---|---|
| 0..4 | magic `AWZ1` |
| 4..8 | block size, logical bytes per block (65536) |
| 8..16 | object size |
| 16..20 | block count n, `ceil(size / block size)` |
| 20..20+4n | stored length of each block |
| then | the blocks, each one zstd frame (level 1), with nothing after the last |

A range GET decodes only the blocks it overlaps.

## Versions (flags 0x40, 0x80, 0x100, format 2)

In a bucket whose versioning was set (`~<id>:Enabled` or `~<id>:Suspended` in
`buckets.manifest`), every write is a version (#31, `server/src/versions.rs`):

- An object or multipart manifest flagged 0x40 is a version. Its creation time (header bytes
  36..44, ns) is its id, written as 16 hex digits; compaction keeps that time when it copies
  the needle. With 0x80 too it is the null version (Suspended buckets) and replaces the
  previous null version only.
- A tombstone flagged 0x40 is a delete marker (0x80: the null delete marker).
- A tombstone flagged 0x100 removes one version: its 8-byte payload is the id (0 for null).

Replay orders the versions of a key by creation time, not by log position: compaction moves
old versions into newer volumes. A plain needle (none of these flags) of a key with versions
is its null version.

## Multipart manifest (payload of a needle flagged multipart)

Text. Line 1: `AWNMPU<version>` (`AWNMPU1`). Line 2: total size. Then one line per part:
`part size \t cumulative end \t volume offset \t md5 hex \t staged path relative to .staging`.
A manifest of a newer version stops startup.

## Bucket manifest (`buckets.manifest`)

Text log, one record per line (`\n`-terminated). Writes append and `sync_all` before acknowledging.

| Prefix | Format | Meaning |
|---|---|---|
| `+` | `+<id>:<name>[:<created_ns>[:<owner>[:<namespace>]]]` | Bucket created; trailing fields after `<name>` are optional when reading older manifests. |
| `-` | `-<id>:<name>` | Bucket deleted; drops all configurations for `<id>`. |
| `~` | `~<id>:<Enabled\|Suspended>` | Bucket versioning status (#31); also populates kind `versioning`. |
| `=` | `=<id>:<kind>:<base64>` | Sets configuration `<kind>` for bucket `<id>` to the decoded document bytes. |
| `=` | `=<id>:<kind>:-` | Deletes (unsets) configuration `<kind>` for bucket `<id>`. An empty payload `=<id>:<kind>:` is also read as delete. |

### Configuration kinds, limits, and GET rendering

| Kind | Max bytes | GET rendering |
|---|---|---|
| `owner` | 64 KiB | Internal metadata (no dedicated GET subresource) |
| `policy` | 20 KiB | Exact stored JSON bytes |
| `acl` | 64 KiB | Canonical XML of the parsed form |
| `ownership` | 64 KiB | Canonical XML of the parsed form |
| `public-access-block` | 64 KiB | Canonical XML of the parsed form |
| `cors` | 64 KiB | Canonical XML of the parsed form |
| `lifecycle` | 64 KiB | Canonical XML of the parsed form |
| `logging` | 64 KiB | Canonical XML of the parsed form |
| `encryption` | 64 KiB | Canonical XML of the parsed form |
| `object-lock` | 64 KiB | Canonical XML of the parsed form |
| `tagging` | 64 KiB | Canonical XML of the parsed form |
| `versioning` | 64 KiB | Canonical XML of the parsed form |

### Replay and compaction

- An unknown `<kind>` in an `=` record stops startup with an error naming the kind.
- A torn last line (not terminated by `\n` and failing syntax or base64 checks) is ignored with a warning.
- A corrupt line in the middle stops startup with the 1-based line number.
- At startup, when dead records strictly outnumber live records, `buckets.manifest` is rewritten to `buckets.manifest.tmp`, synced, and renamed into place.
