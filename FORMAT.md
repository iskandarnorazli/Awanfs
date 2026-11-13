# AwanFS on-disk format

Format version **2** (`FORMAT_VERSION` in `server/src/volume.rs`). Owner decision 20: a newer
version is refused at startup with an error naming both versions; an older version is read as
it is; nothing is rewritten in place. `server/tests/format_version_test.rs` builds a format 1
volume from this document, byte by byte, and checks the server still serves it.

| Version | Adds |
|---|---|
| 1 | The layout below. |
| 2 | Compressed needles (flag 0x20, #51) and versions (flags 0x40, 0x80, 0x100, #31). New volumes are written as format 2; a needle with any of these flags is never written to a format 1 volume (the server opens a new volume first), so an older AwanFS never misreads one. |

All integers are little-endian. Sectors are 4096 bytes.

## Data directory

| File | Content |
|---|---|
| `volume_NNNN.dat` | A volume: superblock, then needles. Writes go to the highest id (#37). |
| `volume_NNNN.idx` | The volume's journal: one 42-byte record per needle. |
| `buckets.manifest` | One line per bucket event: `+<id>:<name>` created, `-<id>:<name>` deleted. |
| `metadata.db` | redb spill index (rebuilt from the volumes if lost). |
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
| 12..14 | flags: 0x1 active, 0x2 tombstone, 0x4 multipart, 0x8 shared, 0x10 zero-byte, 0x20 compressed, 0x40 versioned, 0x80 null version, 0x100 version delete (`meta_index.rs`) |
| 14..16 | bucket id |
| 16..18 | key length |
| 18..20 | reserved |
| 20..28 | xxh3-64 of the key |
| 28..36 | payload length |
| 36..44 | creation time, ns |
| 44..60 | ETag (MD5) high and low halves |
| 60..64 | reserved |
| 64.. | key, then payload, then zero padding to the next sector |

The header sector of a needle written as it streams in (#36) is written last, so a crash
leaves a needle that fails its header check, never one that passes with missing data.

## Journal record (`.idx`, 42 bytes)

key hash (8), volume id (4), offset (8), total length (8), payload length (8), flags (2),
payload CRC32C (4). Recovery uses it to step over gaps left by writes that did not finish.

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
