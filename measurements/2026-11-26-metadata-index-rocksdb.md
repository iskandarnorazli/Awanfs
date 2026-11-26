`gh workflow run bench-metaindex.yml --ref experiment/rocksdb-metaindex -f stores="prod rocks rocks-tuned"` (run 45773109976, commit 9ffce23: KEYS=10000000 GETS=1000000 SCANS=1000 WRITER_KEYS=200000 WRITERS=1,4,16,64); the tables are the JSON lines in each job's log

# Metadata index at 10M keys: production segments against RocksDB (experiment)

Run [45773109976](https://github.com/iskandarnorazli/Awanfs/actions/runs/45773109976), GitHub-hosted runners, jobs 139750219995 (Linux `ubuntu-latest`) and 139750220480 (Windows `windows-latest`). It is an experiment on the unmerged branch `experiment/rocksdb-metaindex` (owner, 2026-11-26), which adds the `rocksdb` crate (v0.25.0, RocksDB 11.8.1) to `server/examples/meta_index_bench.rs`:

- **production segments**: `s3_server::spill::SpillIndex`, 64 MiB spill budget, as the server uses it.
- **RocksDB defaults**: `Options::default()` with `create_if_missing`; write-ahead log on, no sync per write, no bloom filter.
- **RocksDB tuned**, to match the production store: a 10-bit bloom filter, a 64 MiB LRU block cache, a 16 MiB write buffer (prod's memtable is a quarter of its 64 MiB budget), no write-ahead log (the index is rebuilt from the volumes).

All three gave the same answers (hash 47cff400f22342b1 for each on Linux) with 0 wrong. A build ends with a flush. It does not wait for RocksDB's background compactions, so RocksDB's build time may leave compaction work for later: not measured. RocksDB compresses blocks by default and the segments do not. The writers phase (200,000 keys) fits in memory for every store. For redb on the same benchmark, see `measurements/2026-11-19-tiered-metadata.md` (run 44550797419).

## Linux (`ubuntu-latest`, job 139750219995)

| | production segments | RocksDB defaults | RocksDB tuned |
|---|---|---|---|
| inserts per second | 128,583 | 314,238 | 539,910 |
| build time, s | 77.771 | 31.823 | 18.522 |
| bytes on disk | 1,243,960,136 | 700,795,202 | 575,425,341 |
| peak RSS while building, MiB | 64.543 | 110.02 | 81.52 |
| peak RSS while querying, MiB | 57.762 | 69.898 | 115.895 |
| read calls per get | 1.042 | 1.516 | 0.946 |
| get P50, us | 2.54 | 7.26 | 5.47 |
| get P99, us | 4.75 | 10.81 | 9.64 |
| read calls per absent get | 0.056 | 2.054 | 0.016 |
| absent get P50, us | 0.36 | 7.78 | 0.64 |
| absent get P99, us | 2.8 | 13.14 | 6.58 |
| prefix scan of up to 1000 keys, ms | 0.075 | 0.144 | 0.157 |
| get P50 after dropping the page cache, us | 2.74 | 8.06 | 5.84 |
| open time after dropping the page cache, s | 0.055 | 0.019 | 0.041 |
| 1 writers, puts per second (200,000 keys) | 853,482 (P99 3.06 us) | 339,286 (P99 4.45 us) | 773,547 (P99 2.54 us) |
| 4 writers, puts per second (200,000 keys) | 906,742 (P99 20.12 us) | 548,962 (P99 13.35 us) | 1,177,658 (P99 5.69 us) |
| 16 writers, puts per second (200,000 keys) | 957,281 (P99 132.55 us) | 142,364 (P99 224.98 us) | 581,869 (P99 346.55 us) |
| 64 writers, puts per second (200,000 keys) | 871,645 (P99 564.22 us) | 127,249 (P99 1203.07 us) | 239,560 (P99 1790.27 us) |

## Windows (`windows-latest`, job 139750220480)

| | production segments | RocksDB defaults | RocksDB tuned |
|---|---|---|---|
| inserts per second | 133,370 | 225,515 | 499,075 |
| build time, s | 74.98 | 44.343 | 20.037 |
| bytes on disk | 1,243,960,136 | 598,202,094 | 563,755,032 |
| peak RSS while building, MiB | 61.02 | 105.695 | 111.812 |
| peak RSS while querying, MiB | 50.855 | 77.809 | 163.766 |
| read calls per get | 1.042 | 1.762 | 0.977 |
| get P50, us | 5.7 | 12.4 | 7.7 |
| get P99, us | 18.2 | 65.6 | 24.1 |
| read calls per absent get | 0.056 | 2.055 | 0.016 |
| absent get P50, us | 0.5 | 12.3 | 0.6 |
| absent get P99, us | 6.3 | 27.8 | 7.7 |
| prefix scan of up to 1000 keys, ms | 0.123 | 0.199 | 0.209 |
| 1 writers, puts per second (200,000 keys) | 691,776 (P99 1.0 us) | 259,487 (P99 5.9 us) | 665,625 (P99 2.0 us) |
| 4 writers, puts per second (200,000 keys) | 825,634 (P99 9.8 us) | 351,710 (P99 26.1 us) | 1,095,101 (P99 5.6 us) |
| 16 writers, puts per second (200,000 keys) | 761,217 (P99 250.2 us) | 171,208 (P99 312.2 us) | 724,787 (P99 167.6 us) |
| 64 writers, puts per second (200,000 keys) | 730,180 (P99 798.6 us) | 239,851 (P99 390.2 us) | 573,116 (P99 1220.6 us) |

Cold reads on Windows: not measured (the workflow drops the page cache only on Linux).

## What it says

- **The segments win** lookups (P50 2.54 against 5.47 us tuned on Linux), absent lookups (0.36 against 0.64 us), prefix scans (0.075 against 0.157 ms), memory while querying (58 against 116 MiB on Linux, 51 against 164 MiB on Windows), and 16 and 64 concurrent writers.
- **RocksDB tuned wins** the bulk build (539,910 against 128,583 inserts per second on Linux), bytes on disk (575 MB against 1.24 GB, likely its block compression), and 4 concurrent writers.
- **RocksDB defaults** loses every lookup row: with no bloom filter, an absent get makes about 2 read calls.
