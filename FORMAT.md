# AwanFS on-disk format

Format version **1** (`FORMAT_VERSION` in `server/src/volume.rs`). Owner decision 20: a newer
version is refused at startup with an error naming both versions; an older version is read as
it is; nothing is rewritten in place. `server/tests/format_version_test.rs` builds a format 1
volume from this document, byte by byte, and checks the server still serves it.

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
| 12..14 | flags: 0x1 active, 0x2 tombstone, multipart and zero-byte flags (`meta_index.rs`) |
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

## Multipart manifest (payload of a needle flagged multipart)

Text. Line 1: `AWNMPU<version>` (`AWNMPU1`). Line 2: total size. Then one line per part:
`part size \t cumulative end \t volume offset \t md5 hex \t staged path relative to .staging`.
A manifest of a newer version stops startup.
