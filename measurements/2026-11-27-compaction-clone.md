Command: `gh workflow run probe-clone.yml --ref main` (run id `45991832882`: https://github.com/iskandarnorazli/Awanfs/actions/runs/45991832882)

# Compaction Copy vs FICLONERANGE Probe (#104)

- **Source logical size:** 32.00 GiB (34359738368 bytes), 50% live, 4 KiB-aligned extents, dead half sparse
- **Runs per mix:** 3 (medians reported)

| mix | extents | live payload | source alloc | copy s | clone s | time ratio | copy bytes written | clone bytes written | bytes ratio |
|---|---|---|---|---|---|---|---|---|---|
| `small` | 4194304 | 16384.00 MiB (17179869184) | 16449.54 MiB (17248595968) | 359.952 | 143.670 | 2.51x | 17156.42 MiB (17989814784) | 872.54 MiB (914924032) | 19.66x |
| `large` | 256 | 16384.00 MiB (17179869184) | 16384.01 MiB (17179877376) | 83.421 | 0.056 | 1485x | 16439.85 MiB (17238429696) | 3.83 MiB (4017664) | 4291x |
| `mixed` | 852172 | 16384.00 MiB (17179869184) | 16397.32 MiB (17193840640) | 93.513 | 31.562 | 2.96x | 16590.46 MiB (17396361728) | 281.42 MiB (295088128) | 58.95x |

## Per-run samples

| mix | run | copy s | clone s | copy block B | clone block B | copy proc B | clone proc B |
|---|---|---|---|---|---|---|---|
| `small` | 1 | 309.692 | 169.399 | 17188246528 | 961442304 | 17179869184 | 0 |
| `small` | 2 | 386.355 | 143.057 | 17989814784 | 914924032 | 17179869184 | 0 |
| `small` | 3 | 359.952 | 143.670 | 17996355072 | 905469952 | 17179869184 | 0 |
| `large` | 1 | 83.831 | 0.034 | 17294211584 | 2251264 | 17179869184 | 0 |
| `large` | 2 | 83.282 | 0.062 | 17214022656 | 7031808 | 17179869184 | 0 |
| `large` | 3 | 83.421 | 0.056 | 17238429696 | 4017664 | 17179869184 | 0 |
| `mixed` | 1 | 95.605 | 32.449 | 17395444736 | 365682688 | 17179869184 | 0 |
| `mixed` | 2 | 93.513 | 31.562 | 17396361728 | 295088128 | 17179869184 | 0 |
| `mixed` | 3 | 92.841 | 31.319 | 17408857600 | 148031488 | 17179869184 | 0 |

≥ 2x
