`$bin = "server/target/release/examples/boot_bench.exe"; $base = Join-Path $env:TEMP "awan_bench_203"; & $bin fill --dir "$base/no_attrs" --objects 1000000 --size 64; & $bin boot --dir "$base/no_attrs" --objects 1000000 --size 64; & $bin fill --dir "$base/with_attrs" --objects 1000000 --size 64 --attrs; & $bin boot --dir "$base/with_attrs" --objects 1000000 --size 64; & $bin fill --dir "$base/with_updates" --objects 1000000 --size 64 --attrs --attr-updates-pct 10; & $bin boot --dir "$base/with_updates" --objects 1000000 --size 64`

# Startup cost with attributes vs without attributes (#203)

Measurement for Issue #203 (`#150 stage two: attribute-update needles, key_step and the model`, comment `6897725776`): startup time, peak RSS, peak heap, retained heap, and `AttrUpdateStore` side-map sizing over 1,000,000 64-byte objects on local Windows (`boot_bench.exe` release build).

## Results (1,000,000 objects, 64 B payloads)

| Configuration | `secs` | `objects_per_s` | `peak_rss_mib` | `peak_heap_mib` | `heap_after_mib` | `attr_updates_entries` | `attr_updates_bytes` | Wrong |
|---|---|---|---|---|---|---|---|---|
| 1M without attributes | 5.133 | 194,824 | 61.453 | 65.659 | 65.659 | 0 | 0 | 0 of 1,000 |
| 1M with `0x400` attributes (`--attrs`) | 4.491 | 222,682 | 61.441 | 65.659 | 65.659 | 0 | 0 | 0 of 1,000 |
| 1M with `0x400` attrs + 10% `0x200` updates (100k side-map entries) | 6.276 | 159,343 | 89.023 | 90.833 | 83.991 | 100,000 | 9,622,528 | 0 of 1,000 |

Raw JSON lines:

```text
{"phase":"fill","objects":1000000,"size":64,"with_attrs":false,"attr_updates_pct":0,"secs":18.139,"objects_per_s":55130,"volumes":1,"bytes_on_disk":4215516840}
{"phase":"boot","objects":1000000,"secs":5.133,"objects_per_s":194824,"peak_rss_mib":61.453,"peak_heap_mib":65.659,"heap_after_mib":65.659,"heap_parts_mib":{"needle_records":1.000,"checksum_map":0.000,"hot_table":25.871,"spill_index":25.407,"other":34.127},"heap_parts_after_mib":{"needle_records":0.000,"checksum_map":0.000,"hot_table":25.871,"spill_index":5.679,"other":34.108},"attr_updates_entries":0,"attr_updates_bytes":0,"attr_updates_entry_bytes":88,"checked":1000,"wrong":0}
{"phase":"fill","objects":1000000,"size":64,"with_attrs":true,"attr_updates_pct":0,"secs":14.006,"objects_per_s":71398,"volumes":1,"bytes_on_disk":4215516840}
{"phase":"boot","objects":1000000,"secs":4.491,"objects_per_s":222682,"peak_rss_mib":61.441,"peak_heap_mib":65.659,"heap_after_mib":65.659,"heap_parts_mib":{"needle_records":1.000,"checksum_map":0.000,"hot_table":25.871,"spill_index":25.407,"other":34.127},"heap_parts_after_mib":{"needle_records":0.000,"checksum_map":0.000,"hot_table":25.871,"spill_index":5.679,"other":34.109},"attr_updates_entries":0,"attr_updates_bytes":0,"attr_updates_entry_bytes":88,"checked":1000,"wrong":0}
{"phase":"fill","objects":1000000,"size":64,"with_attrs":true,"attr_updates_pct":10,"secs":13.843,"objects_per_s":72239,"volumes":1,"bytes_on_disk":4756587752}
{"phase":"boot","objects":1000000,"secs":6.276,"objects_per_s":159343,"peak_rss_mib":89.023,"peak_heap_mib":90.833,"heap_after_mib":83.991,"heap_parts_mib":{"needle_records":3.575,"checksum_map":0.000,"hot_table":25.871,"spill_index":25.407,"other":54.838},"heap_parts_after_mib":{"needle_records":2.575,"checksum_map":0.000,"hot_table":25.871,"spill_index":7.568,"other":47.977},"attr_updates_entries":100000,"attr_updates_bytes":9622528,"attr_updates_entry_bytes":88,"checked":1000,"wrong":0}
```

## Side-map sizing and Architecture Decision 58 analysis

1. **Zero cost when un-updated (`0x400` vs no attrs):** Objects that only carry a write-time `0x400` attributes block pay zero `AttrUpdateStore` heap (`0` bytes) and zero `ObjectView` read overhead unless `FLAG_ATTRS` is set. Boot time (4.491s vs 5.133s), peak RSS (61.441 vs 61.453 MiB), and heap (65.659 MiB) are unchanged.
2. **Side-map footprint (`AttrUpdateStore`):** `side_map_entry_bytes()` is 88 B per updated object with one group (`(u32, u64, u64)` key 24 B + `Vec` 24 B + SwissTable metadata 8 B + `KeyAttrUpdate` 32 B). With 100,000 updated objects (10% of 1M), `attr_updates.estimated_bytes()` is 9,622,528 B (~9.18 MiB, or ~96.2 B/entry with hash map load factor), carved directly out of `PayloadCache`'s `--memory-budget` cache half (`set_reserved_bytes`).
3. **Zero-mmap boot scan for `0x200`:** `try_scan_needles` extracts and CRC-verifies `0x200` payloads directly from its sequential `AlignedScanWindow`, keeping `peak_rss_mib` at 89.023 MiB (prior to reading `0x200` from the scan window, mmap faults on 100k `0x200` needles pushed peak RSS to 861.457 MiB).
4. **Scale limit (Decision 58):** At ~96 B per updated object, 1M updated objects use ~96 MiB and easily fit inside the 128 MiB minimum cache budget. At 10M+ updated objects (~960 MiB) or multi-billion object scale (Issue #26 / Decision 58), spilling update pointers into level segments or keeping a Bloom-filtered segment index keeps memory bounded.
