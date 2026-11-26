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
| `metadata.db` | The redb spill index of servers before #26 stage 6. A server since then deletes it at boot: it was rebuilt from the volumes at every boot, so nothing in it is needed (decision 59). |
| `meta/seg_<seq>.seg` | A metadata segment (#26, decision 59): sorted rows of the metadata index, immutable once written. `<seq>` is 16 lowercase hex digits. Below. The server's spill index since #26 stage 6; it clears them at boot and rebuilds them from the volumes. Booting from the manifest's checkpoint moved to the partitioned store (#322). |
| `meta/manifest.0`, `meta/manifest.1` | The metadata manifest's two slots (#26 stage 4): which segments are live, their levels (stage 5), and the log position they cover. Below. |
| `.staging/upload-<hex>/part_NNNNN.dat` | Multipart part files; the parts of completed objects stay here. |

## Metadata segment (`meta/seg_<seq>.seg`, #26 stage 2)

A segment is an immutable, sorted run of index entries, written once and synced, then only
read: with positioned reads through the page cache, never a mapping (decision 58). Every
number is little-endian except the bucket id inside a sort key. The file is 4096-byte blocks:
the header block, the data blocks, the index blocks, and the trailer block. At most 64 MiB.

**Sort key.** `bucket id (u32, big-endian) || object key bytes`. Big-endian puts one bucket's
keys together and in bucket order, so byte order on the sort key is (bucket, key) order, and
listing a bucket is one range. An object key is at most 1024 bytes.

### Header (block 0)

| Bytes | Field |
|---|---|
| 0..4 | magic `0x47535741` ("AWSG") |
| 4..6 | segment format version, 1 |
| 6..8 | flags, 0 |
| 8..16 | sequence number (the `<seq>` of the file name) |
| 16..24 | creation time, ns since the Unix epoch |
| 24..32 | entry count |
| 32..40 | data block count `D` |
| 40..44 | CRC32C of bytes 0..40 |
| 44..4096 | zero |

### Data block (blocks 1..=D)

| Bytes | Field |
|---|---|
| 0..2 | entry count in this block |
| 2.. | entries, packed, in strictly increasing sort-key order across all blocks |
| ..4092 | zero |
| 4092..4096 | CRC32C of bytes 0..4092 |

An entry never spans two blocks. Entry:

| Bytes | Field |
|---|---|
| 0 | kind: 1 = row, 2 = tombstone (the key is deleted; it hides older segments) |
| 1..3 | sort-key length `K` (u16) |
| 3..3+K | sort key |
| 3+K..67+K | row (kind 1 only): the 64-byte `ObjectMeta`, little-endian, in its field order: `key_hash`, `bucket_flags`, `vol_offset`, `size`, `etag_hi`, `etag_lo`, `created_at`, `key_slice`. `key_slice` is written 0: it points into the in-memory key arena, and on disk the key is the sort key. |

### Index blocks (after the data blocks)

The fence keys, then the Bloom filter, padded with zeros to a block boundary:
- **Fences:** for each data block in order, its first entry's sort key as `u16 length || bytes`.
- **Bloom filter:** `B` blocks of 64 bytes (512 bits each). A sort key sets `P` bits, all in one
  block: `h = xxh3_64(sort key)`, block `h % B`, and `P` bit positions in it from a second hash,
  `xxh3_64_with_seed(sort key, 0x5345474D454E5453)` (`bloom_probes` in `server/src/segment.rs`). Rows and tombstones are both in the filter.

### Trailer (last block)

| Bytes | Field |
|---|---|
| 0..4 | magic `0x46535741` ("AWSF") |
| 4..12 | first index block |
| 12..20 | fence region length in bytes |
| 20..28 | fence count (equals `D`) |
| 28..36 | Bloom byte offset from the first index block |
| 36..44 | Bloom block count `B` |
| 44..48 | probes per key `P` |
| 48..52 | CRC32C of the index region (fences and Bloom, unpadded) |
| 52..56 | CRC32C of bytes 0..52 |
| 56..4096 | zero |

### Reading and the rules
- **Open** reads the header, the trailer and the index region, and checks all three CRCs. A
  mismatch, a wrong magic, a newer segment format, or a length that does not fit the file is an
  error: the segment is not used, never read past.
- **Lookup:** the Bloom filter (a miss costs no read), then the fence to find the one data block,
  one 4096-byte read, its CRC, then a scan of its entries. A block whose CRC fails is an error,
  never "not found".
- **Several segments:** they are in levels (the manifest records each one's level, #26 stage 5).
  A lookup takes the level-0 segments, higher sequence number first, then in each deeper level
  the one segment whose key range holds the key. The first entry found wins; a tombstone hides
  every entry after it in that order. Below.
- Written in full, then `sync_data`, then (stage 4) named in the manifest. A segment the
  manifest does not name is ignored, so a crash mid-write leaves nothing visible.

## Metadata manifest (`meta/manifest.0`, `meta/manifest.1`, #26 stage 4)

The checkpoint of the metadata index: the live segments, and the position in the volume log
they cover. It is written at every flush. Boot does not read it yet: it clears the segments and
replays every volume. A boot that reads it would open those segments and replay only the needles
after that position; that moved to the partitioned store, a checkpoint per partition (#322,
[owner decision, 2026-11-20](https://github.com/iskandarnorazli/Awanfs/issues/26#issuecomment-7028770759)). Without a valid manifest, boot replays every volume: the volumes are the record, so losing
the manifest costs time, never data.

**Two slots.** A new manifest is written to the slot that does not hold the current one, with a
generation one higher than both, then synced. Boot reads both slots and uses the valid one with
the higher generation. A slot whose write a crash tore fails its length or CRC check and is not
valid, so the other slot, the previous manifest, is used. No rename is needed.

| Bytes | Field |
|---|---|
| 0..4 | magic `0x4D535741` ("AWSM") |
| 4..6 | manifest format version, 2 (format 1 is still read: below) |
| 6..8 | flags, 0 |
| 8..16 | generation |
| 16..20 | replay volume id: the volume holding the first needle not covered |
| 20..28 | replay offset in that volume: needles at this offset and after, and every needle of a higher volume id, are replayed; everything before is in the segments |
| 28..36 | next segment sequence number |
| 36..40 | live segment count `N` (at most 65536) |
| 40..40+16N | the live segments, by strictly increasing sequence number, 16 bytes each: sequence number (u64), level (u8, 0 to 7), 7 bytes zero |
| 40+16N..44+16N | CRC32C of bytes 0..40+16N |

The file is exactly `44 + 16N` bytes. A length that disagrees, a wrong magic, a newer format, a
level above 7, nonzero reserved bytes or a CRC mismatch makes the slot invalid.

**Format 1** (stage 4, before levels) has 8-byte entries, the sequence number alone, and is
exactly `44 + 8N` bytes. Its segments are read as level 0. No release wrote it: the server did
not use the manifest before stage 6.

### Levels and compaction (#26 stage 5, checked by `verification/tla/Levels.tla`)
- A flush writes a **level-0** segment. Level-0 segments may overlap; the higher sequence number
  is newer.
- In each level from **1 down**, segments do not overlap: each key range belongs to at most one
  segment of the level. Data in a shallower level is newer than data deeper down for the same
  key.
- A **compaction** writes the merge of its inputs one level down, as segments of at most 64 MiB
  split at key boundaries, and switches to a manifest that names them in place of the inputs:
  - from level 0: every level-0 segment, and the level-1 segments that overlap their key span
    (`levels_l0_subset`: taking only some level-0 segments puts a newer one below an older one);
  - from level `i`: one segment, and the level-`i+1` segments that overlap its key range
    (`levels_partial_overlap`: skipping one leaves two overlapping segments in a level).
  A segment with nothing to merge below it moves down by changing its level in the manifest.
- **Tombstones** are dropped only when no segment deeper than the output overlaps the output's
  key range (`levels_early_tombstones`: an older row comes back).
- The rules of the manifest switch above apply to every compaction.

### The rules (checked by `verification/tla/Manifest.tla`)
- **Sync the segment first:** a manifest names only segments already written in full and synced.
  (`manifest_switch_before_sync` breaks this: a crash loses a named segment whose rows are
  before the replay position.)
- **Alternate slots:** a new manifest never overwrites the slot holding the current one.
  (`manifest_one_slot`: a torn write leaves boot with no manifest.)
- **Delete after the switch is durable:** a segment is deleted only after a durable manifest of a
  higher generation stops naming it. (`manifest_gc_before_sync`: a merge deleted its inputs, a
  crash tore the new manifest, and the old one named segments that were gone.)
- A segment file the valid manifest does not name is ignored at boot and may be deleted.

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
| 12..14 | flags: 0x1 active, 0x2 tombstone, 0x4 multipart, 0x8 shared, 0x10 zero-byte, 0x20 compressed, 0x40 versioned, 0x80 null version, 0x100 version delete, 0x200 attribute update (#150 stage 2, #203), 0x400 has an attributes block (`meta_index.rs`) |
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
| 0x0010 | Object tags (`TagSet`) | tagging (mutable) | #115, #203 |
| 0x0011 | Retired, never written: registered for ACL by #402 before Decision 3A's numbers (#410) | — | #410 |
| 0x0012 | Retired, never written: registered for retention by #402 (#410) | — | #410 |
| 0x0013 | Retired, never written: registered for legal hold by #402 (#410) | — | #410 |
| 0x0014 | Retired, never written: registered for storage class by #402 (#410) | — | #410 |
| 0x0020 | Object checksum (`<ALG>:<TYPE>:<base64>`) | checksum (write-time) | #117 |
| 0x0021 | Multipart part sizes (LE `u64` array) | multipart (write-time) | #117 |
| 0x0002 | Content-Encoding (with `aws-chunked` stripped) | headers (write-time) | #152 |
| 0x0003 | Content-Disposition | headers (write-time) | #152 |
| 0x0004 | Content-Language | headers (write-time) | #152 |
| 0x0005 | Cache-Control | headers (write-time) | #152 |
| 0x0006 | Expires | headers (write-time) | #152 |
| 0x0007 | x-amz-website-redirect-location | headers (write-time) | #152 |
| 0x0008 | User metadata (`x-amz-meta-*`) | metadata | #152 |
| 0x0030 | `TAG_OWNER`: object owner account ID (UTF-8, written only when different from bucket owner) | owner (write-time) | #151 (Decision 3A) |
| 0x0031 | Object ACL | acl (mutable) | #116, #203 (Decision 3A) |
| 0x0040 | Object Lock retention (`mode (u8: 0=Governance, 1=Compliance) \|\| retain_until_ns (u64 LE) \|\| iso_text (UTF-8)`; empty = cleared) | retention (mutable) | #120, #203 (Decision 3A) |
| 0x0041 | Object Lock legal hold (`u8`: 0=OFF, 1=ON) | legal hold (mutable) | #120, #203 (Decision 3A) |
| 0x0050 | Reserved for server-side encryption | — | #121 (Decision 3A) |
| 0x0060 | Storage class (`STANDARD`, etc.) | storage class (mutable) | #159, #203 (Decision 3A) |

## Attribute update needle (flag 0x200, format 3, #203)

A mutable attribute change (tags, ACL, retention, legal hold, storage class) appends an
attribute-update needle (`FLAG_ACTIVE | FLAG_ATTR_UPDATE`, without `FLAG_ATTRS` in the base
attributes slot; `attrs_len = 0`) instead of rewriting the object's payload needle.

Its payload holds the target version followed by an encoded Attributes block (CRC32C + sorted
tag entries, identical to flag 0x400's block layout):

| Bytes | Field |
|---|---|
| 0..8 | `target_version_id` (`u64` little-endian): version timestamp (`ts`) of the target version, or `0` for an unversioned object or the `null` version |
| 8.. | Encoded `Attrs` block (4-byte CRC32C of bytes 12.., then sorted `tag \| len \| val` entries) |

Header `created_at_ns` is stamped under the per-key lock (`next_ts` monotonic relative to the
target and any prior update, decision 56). `payload_crc32c` covers bytes `0..` of this payload,
and `etag_hi`/`etag_lo` are `0`. Recovery replays base object/tombstone records first and then
folds surviving `0x200` updates through `fsm::key_step` in `created_at_ns` order. Compaction
retains only the latest live `0x200` update per `(key, version, group)` and drops updates whose
version was deleted or overwritten or whose update was superseded.

## Journal record (`.idx`, 42 bytes)

key hash (8), volume id (4), offset (8), total length (8), payload length (8), flags (2),
payload CRC32C (4). Recovery uses it to step over gaps left by writes that did not finish.

A record is written only by a sync, after the sync of the volume that holds its needle (#412,
option B), so a record on disk names a needle whose bytes were on disk first. The open checks
the payload CRC only of needles the journal does not record; a recorded needle's header, key
and attributes are still checked. The bytes of a record are unchanged.

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
| `+` | `+<id>:<name>[:<created_ns>[:<owner>[:<namespace>]]]` | Bucket created (#159 adds `<namespace>` for tenant isolation); empty for all requests today, lookup is by `(namespace, name)`, trailing fields after `<name>` are optional on older manifests, and absent/empty `<owner>` defaults to `"root"`. |
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
