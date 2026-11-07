# Performance measurements

Machine: Apple M5, 10 logical cores, macOS-27.2-arm64-arm-64bit, Python 3.9.6.

**What is measured.** Single-thread, in-process processing time per message on this laptop, input already in the page cache. This is *not* exchange-to-trader or network latency, and it says nothing about a production feed handler's latency under load. Per-message percentiles come from a separate pass that timestamps every call (timer overhead is listed and is included in the numbers); throughput comes from passes with no timers.


## AAPL slice

| impl | stage | messages | msgs/s (best) | msgs/s (median) | p50 ns | p95 ns | p99 ns | peak RSS MB |
|---|---|---|---|---|---|---|---|---|
| Python | framing only | 1,519,871 | 4,195,643 | 4,184,409 | - | - | - | 76 |
| Python | parser-only | 1,519,871 | 5,871,298 | 5,816,912 | 208 | 250 | 250 | 185 |
| Python | book-update-only | 1,512,179 | 1,199,810 | 1,191,070 | 834 | 1,584 | 1,958 | 497 |
| Python | complete replay (in memory) | 1,519,871 | 816,947 | 804,050 | 1,083 | 1,791 | 2,125 | 234 |
| Python | end-to-end (file to final state) | 1,519,871 | 650,738 | 646,357 | - | - | - | 94 |
| C++ | parser-only | 1,519,871 | 180,748,744 | 180,261,943 | 0 | 42 | 42 | 143 |
| C++ | book-update-only | 1,512,179 | 33,189,994 | 32,955,816 | 41 | 83 | 84 | 268 |
| C++ | complete replay (in memory) | 1,519,871 | 28,772,630 | 28,510,544 | 41 | 83 | 84 | 144 |
| C++ | end-to-end (file to final state) | 1,519,871 | 26,654,505 | 26,654,505 | - | - | - | 40 |

- C++ parse: per-op timer resolution 41 ns (per-op percentiles above are quantized to it); mean cost of 64-message batches: p50 5 / p95 6 / p99 7 ns per message.
- C++ book: per-op timer resolution 41 ns (per-op percentiles above are quantized to it); mean cost of 64-message batches: p50 30 / p95 38 / p99 42 ns per message.
- C++ full: per-op timer resolution 41 ns (per-op percentiles above are quantized to it); mean cost of 64-message batches: p50 35 / p95 42 / p99 46 ns per message.
- Complete replay speed-up C++ vs Python: x35.

## SPY slice

| impl | stage | messages | msgs/s (best) | msgs/s (median) | p50 ns | p95 ns | p99 ns | peak RSS MB |
|---|---|---|---|---|---|---|---|---|
| Python | framing only | 2,138,938 | 3,691,509 | 3,668,904 | - | - | - | 79 |
| Python | parser-only | 2,138,938 | 5,779,162 | 5,761,073 | 208 | 250 | 250 | 237 |
| Python | book-update-only | 2,132,141 | 1,322,818 | 1,313,447 | 833 | 1,167 | 1,625 | 750 |
| Python | complete replay (in memory) | 2,138,938 | 827,576 | 817,175 | 1,083 | 1,458 | 1,917 | 271 |
| Python | end-to-end (file to final state) | 2,138,938 | 674,757 | 674,282 | - | - | - | 80 |
| C++ | parser-only | 2,138,938 | 178,786,168 | 178,468,529 | 0 | 42 | 42 | 220 |
| C++ | book-update-only | 2,132,141 | 61,031,804 | 60,738,845 | 0 | 42 | 42 | 425 |
| C++ | complete replay (in memory) | 2,138,938 | 48,015,040 | 47,897,350 | 41 | 42 | 42 | 220 |
| C++ | end-to-end (file to final state) | 2,138,938 | 42,077,331 | 42,077,331 | - | - | - | 38 |

- C++ parse: per-op timer resolution 41 ns (per-op percentiles above are quantized to it); mean cost of 64-message batches: p50 5 / p95 6 / p99 7 ns per message.
- C++ book: per-op timer resolution 41 ns (per-op percentiles above are quantized to it); mean cost of 64-message batches: p50 16 / p95 22 / p99 31 ns per message.
- C++ full: per-op timer resolution 41 ns (per-op percentiles above are quantized to it); mean cost of 64-message batches: p50 20 / p95 25 / p99 29 ns per message.
- Complete replay speed-up C++ vs Python: x58.

Timer overhead per clock read: Python 57 ns (resolution 41 ns); C++ 14 ns (resolution 41 ns). RSS of the staged runs includes the pre-loaded input messages (Python keeps one bytes object per message), so only the end-to-end rows show the footprint of a streaming replay.

## Whole-day file scan (C++)

268,744,780 frames, gunzip + framing + length check of every message in 8.6 s (31,093,506 frames/s), 0 bad lengths, 0 unknown types, peak RSS 38 MB.

Python reader on the same file (gunzip + framing + per-type/locate counting): 268,744,780 frames in 128 s (2,099,752 frames/s).
