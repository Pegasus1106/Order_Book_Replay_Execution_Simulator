# Order Book Replay & Execution Simulator (Nasdaq TotalView-ITCH 5.0)

Rebuilds Nasdaq's displayed limit order book from the official ITCH sample feed, replays it
deterministically, simulates marketable and passive orders against it, runs two simple quoting
strategies under risk limits, and re-implements the parser and book in C++17 with a verified
state match.

This uses **equity** data from one venue. See `docs/DESIGN.md` §10 for what that does and does not say about options markets.

* Data: `12302019.NASDAQ_ITCH50.gz` from `emi.nasdaq.com/ITCH/Nasdaq ITCH/` (30 Dec 2019; 3.52 GB gzip, 8.25 GB raw, **268,744,780 messages**, 8,906 symbols). Spec: `docs/NQTVITCHspecification.pdf`.
* Symbols replayed: AAPL, MSFT, SPY, AMD, INTC, NIO (7.9 M frames in total across the six per-symbol slices).
* Python 3.9 standard library only. C++17 + zlib (macOS/Linux). Everything below was measured on an Apple M5 laptop.

## Layout

```
itchlob/          Python reference implementation
  messages.py       layouts + decoders          framing.py   streaming reader, symbol lookup, slice extraction
  book.py           order book, counters        checks.py    invariant verification
  replay.py         replay engine: pacing, pause/resume, snapshots, checkpoints, diagnostics
  sim.py            execution simulator         strategy.py  quoting rules, risk layer, P&L, markouts
  experiments.py    marketable / queue studies, parallel scenario grid
  bench.py report.py synth.py cli.py
cpp/              C++17 parser + book + replay/bench/extract (itch.hpp, book.hpp, main.cpp, Makefile)
scripts/          fetch_itch.py  extract_slices.py  verify_all.sh  run_benchmarks.sh ...
results/          every number quoted below, as produced
docs/DESIGN.md    semantics, anomaly policy, simulator assumptions, limitations
```

## Reproduce

```bash
python3 scripts/fetch_itch.py 12302019.NASDAQ_ITCH50.gz         # parallel ranged download + gzip -t
make -C cpp
python3 scripts/extract_slices.py data/12302019.NASDAQ_ITCH50.gz --symbols AAPL,MSFT,SPY,AMD,INTC,NIO
bash scripts/verify_all.sh                                       # all correctness checks
python3 -m itchlob replay data/slices/AAPL_12302019.itch --symbol AAPL --snapshot-every 1800 --check-every 250000
python3 -m itchlob replay ... --speed 100 --interactive          # scaled speed; p / r / s <x> / q on stdin
python3 -m itchlob marketable data/slices/AAPL_12302019.itch --symbol AAPL
python3 -m itchlob queue data/slices/SPY_12302019.itch --symbol SPY --hold-s 30
python3 -m itchlob grid --slices AAPL=data/slices/AAPL_12302019.itch ... --workers 9
python3 -m itchlob report results/grid.jsonl
bash scripts/run_benchmarks.sh
```

## Milestone A/B: reconstruction, replay, diagnostics

* All seven book messages are implemented (A, F, E, C, X, D, U). Replace removes the old reference and adds the new one at the **back of the queue**, inheriting side and MPID. Integer prices, wire-order processing, replay always starts at the session's first message (`System Event 'O'` seen on all six symbols) or from a checksum-verified snapshot; a mid-stream start with an empty book is refused.
* Replay modes: as fast as possible, scaled historical speed, pause/resume/stop/speed change from another thread or stdin, periodic snapshots (34 per symbol at 30-minute event-time boundaries), 64-bit state checksum, counters for malformed messages, unknown references, quantity inconsistencies and others (`docs/DESIGN.md` §4), invariant checks.
* **Result on this day:** all six symbols replay with zero unknown references, quantity inconsistencies, duplicate references, malformed or unknown messages, and the displayed book was **never locked or crossed**. The anomaly counters and the locked/crossed episode tracking are therefore exercised by the synthetic fuzz generator rather than by this day's data.

Correctness evidence (`scripts/verify_all.sh`, output in `results/`):

| check | result |
|---|---|
| 3 replays of identical input (AAPL, first 1 M frames) | same checksum, counters and checkpoints |
| snapshot at frame 500,000 then resume | final state equals the uninterrupted run |
| mid-stream start with empty book | refused |
| 30,000 frames at 1000x vs unpaced; separately a 1.5 s pause, resume, mid-run speed change and a stop from another thread | same state; 1.49 s of pause recorded; stop took effect in 0.34 s |
| Python vs C++, 6 symbols, end of day | counters, per-type counts, checksum every 100k frames, and a full dump of every level and order in queue order: identical |
| Python vs C++, 6 symbols, cut at frame 700,000 (1.8k to 21.8k live orders) | identical |
| Python vs C++, 8 seeded synthetic streams, 4 with injected anomalies (~2%) | identical, including ~700 unknown refs per anomalous stream plus duplicates, over-executions, truncated frames and crossed episodes; C++ also clean under ASan/UBSan on one of the anomaly-heavy streams |
| C++ slice extractor vs Python slice extractor | byte-identical for all 6 symbols |
| scenario in the process pool vs the same scenario run alone | identical results |
| book checksum at a mid-session cut with strategy + simulator attached vs plain replay | identical (the simulator never touches the book) |

## Milestone C: execution simulator

Full assumptions are at the top of `itchlob/sim.py`; the short version:

* **Counterfactual limitation.** Simulated orders are not inserted into the historical book, so they cannot move the market or change anyone's behaviour. Sizes are restricted (max order quantity, optional depth-participation cap, and a ledger that stops two simulated orders from taking the same displayed shares).
* **Marketable orders** sweep displayed depth after a configurable delay, report VWAP, slippage, fees, and unfilled quantity. AAPL, 34 times of day x 2 sides, fee $0.003/sh, slippage vs the mid at decision time:

| size | fill | mean slippage c/sh | p95 | note |
|---|---|---|---|---|
| 100 | 100% | 1.35 | 2.44 | |
| 2,000 | 100% | 7.06 | 10.7 | |
| 10,000 | 99.7% | 46.3 | 105.6 | 1 order not completely filled |
| 100,000 | 42.0% | (not meaningful) | | 47 of 68 orders ran out of displayed liquidity |

  Latency from 0 to 10 ms moves mean slippage by at most 0.06 c/sh for sizes up to 2,000. That is a property of this one day's mid-price drift, not a general result.
* **Passive orders** use a displayed-quantity queue model with partial fills, cancel/replace delays that race against fills, and trade-through fills. Four assumptions for cancels at our level: `conservative` (cancel was behind us), `optimistic` (ahead of us), `proportional`, `identity` (uses ITCH order references and arrival order). Probe study (100 shares at the touch every 30 s, cancelled after the hold time, 100 us one-way latency; share fill rate):

| symbol (avg queue ahead) | hold | conservative | proportional | identity | optimistic |
|---|---|---|---|---|---|
| AAPL (246 sh) | 2 s | 22.6% | 24.2% | 23.0% | 24.9% |
| AAPL | 30 s | 75.4% | 76.5% | 75.9% | 77.0% |
| SPY (1,091 sh) | 2 s | 13.7% | 19.6% | 16.3% | 21.1% |
| SPY | 30 s | 67.1% | 71.5% | 70.0% | 72.5% |

  The assumptions bracket each other as expected, and the gap grows with queue depth (SPY 2 s: optimistic is 1.5x conservative).

## Milestone D: strategy, risk and sensitivity

Rules, features and risk controls are described in `itchlob/strategy.py` and `docs/DESIGN.md` §8. 192 scenarios = 6 symbols x 2 rules x 4 queue assumptions x 4 one-way latencies (50 us, 250 us, 1 ms, 5 ms), one session, 100-share quotes, 500-share position limit, $2,500 loss limit, 09:35 to 15:55 ET, flat at the end. Full tables: `results/GRID_REPORT.md`. The 192 scenarios ran in 177 s on 9 processes (sum of scenario time 1,571 s, **x8.9**); per-symbol event order is untouched because each scenario replays its own book sequentially.

What the grid shows (single day, six symbols, cells are not independent samples):

* **The strategies lose money on average**: mean net P&L over all 192 scenarios is -$546 per symbol-day, and the per-cell means (averaged over the six symbols) range from -$150 to -$905 depending on rule, latency and queue assumption. AAPL (spread = 1 tick on a $290 stock) hit the loss limit in all 32 of its scenarios (between 10.9 h and 14.3 h, i.e. 10:55 to 14:16 ET; median 11.7 h for fixed, 12.4 h for inventory-aware): passive fills there are followed by a ~1.2 c/sh adverse move within 1 s (mean over its scenarios), against ~0.5 c of half-spread plus 0.2 c rebate. NIO was profitable in 30 of 32 scenarios and SPY in 24 of 32; one day and six symbols do not support reading that as an edge.
* **Inventory-aware vs fixed:** it reduced mean |inventory| in 96 of 96 paired cells (at 250 us / identity: AAPL 118 to 70 shares, MSFT 299 to 98) and max |inventory| (500-585 across fixed cells vs 229-394 across inventory-aware cells), but it beat fixed on net P&L in only 49 of 96 cells (median difference $2, mean -$104). It controls risk; it does not improve P&L here. It also trades fewer shares (median 78% of fixed's, range 70% to 122%).
* **Latency:** going from 50 us to 5 ms one-way costs, on average, about $330 per symbol-day for fixed quoting under `identity` queue (-150 to -478) while the share fill rate barely moves (15.9% to 15.3%): the loss comes from stale quotes being picked off, not from fewer fills.
* **Queue assumption:** the share fill rate for the same strategy (fixed, mean over symbols) varies 10.8% (conservative) to 20.1% (optimistic) at 50 us, and the ordering of net P&L across assumptions differs by symbol (e.g. NIO fixed: +$382 conservative, +$1,147 optimistic, +$1,430 identity; AMD fixed: -$481, -$1,273, -$413). Conclusions that depend on the fill rate should be shown across all assumptions.
* **Risk controls exercised:** 131 position-limit breaches in 48 scenarios (max |inventory| 585 vs 500). These come from the in-flight replace window, where an old order can still fill after a new one is sent; the limit is enforced on intended quotes, not on in-flight exposure. 254,827 order messages were blocked by the rate limit. 32 scenarios hit the loss-limit kill switch and flattened. The stale-data stand-down triggered 18,496 times, mostly on quiet stocks (see the caveat in `docs/DESIGN.md` §8). Cash-flow accounting identity holds in every scenario (max gap 8e-6 price units).

Charts: `results/NIO_fixed.svg`, `results/NIO_inventory_aware.svg` (inventory and cumulative P&L through the day).

## Performance (`results/BENCHMARKS.md`)

Single-thread processing time per message on this laptop, input in memory unless stated. **This is not exchange-to-trader or network latency.** Per-message percentiles are from a separate timed pass (timer overhead: Python 57 ns, C++ 14 ns, clock resolution 41 ns, so C++ per-message percentiles are quantized; the batched figures are means over 64 messages).

AAPL slice (1,519,871 messages):

| stage | Python msgs/s | C++ msgs/s | Python p50 / p95 / p99 (ns) | C++ p50 / p95 / p99 (ns, 64-msg batch mean) |
|---|---|---|---|---|
| parser-only | 5.87 M | 180.7 M | 208 / 250 / 250 | 5 / 6 / 7 |
| book-update-only | 1.20 M | 33.2 M | 834 / 1,584 / 1,958 | 30 / 38 / 42 |
| complete replay (in memory) | 0.82 M | 28.8 M | 1,083 / 1,791 / 2,125 | 35 / 42 / 46 |
| end-to-end (file to final state) | 0.65 M | 26.7 M | | |
| peak RSS, end-to-end | 94 MB | 40 MB | | |

SPY slice (2,138,938 messages): complete replay 0.83 M msgs/s (Python) vs 48.0 M (C++), end-to-end 0.67 M vs 42.1 M. C++ is x35 to x58 faster than the Python reference for the complete replay.
Whole-day file (268.7 M frames, gunzip + framing + length validation of every message): C++ 8.6 s (31.1 M frames/s), Python 128 s (2.1 M frames/s). The Python book is 1 to 2 orders of magnitude slower than C++; its role here is the reference.

## Resume bullets (numbers from this repository)

* Built a Python/C++ Nasdaq ITCH replay engine supporting add, add-with-MPID, execute, execute-with-price, partial cancel, delete and replace events, reconstructing books from a 268.7 M-message trading day (7.9 M frames across six symbols; whole file framed and validated) with deterministic state verification (state checksum at every 100,000th frame, final counters and full book dump identical between Python and C++, plus seeded anomaly-injection fuzzing and ASan/UBSan runs) at 26.7 to 42.1 M messages/s end-to-end in C++ versus 0.65 to 0.67 M messages/s in the Python reference.
* Implemented an execution simulator with partial fills, configurable latency, four queue-position assumptions, and position/order-size/rate/loss limits; evaluated two quoting rules across 192 scenarios (6 symbols x 4 queue assumptions x 4 latencies, one session) in parallel at x8.9 speed-up, quantifying how latency and queue assumptions change fill rate, adverse selection and P&L.

## Limitations

See `docs/DESIGN.md` §11. In short: one day; six symbols; one venue; no market impact, hidden liquidity or self-impact; the strategies are illustrative and lose money on average here; the Python and C++ processing times are laptop in-process numbers.
