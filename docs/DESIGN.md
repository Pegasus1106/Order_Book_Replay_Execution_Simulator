# Design notes, assumptions and limitations

## 1. Data and scope

* Source: Nasdaq's public sample directory (`emi.nasdaq.com/ITCH/Nasdaq ITCH/`), file
  `12302019.NASDAQ_ITCH50.gz` (30 Dec 2019, 3.52 GB compressed, 268,744,780 messages, 8,906 symbols).
  Format: TotalView-ITCH 5.0 in BinaryFILE framing (2-byte big-endian length + message). The
  specification is saved at `docs/NQTVITCHspecification.pdf`.
* Scope: one trading day. Per-symbol books are rebuilt for six symbols with different liquidity
  profiles (AAPL, MSFT, SPY, AMD, INTC, NIO); the project is built around one book at a time.
* **Replay always starts at the start of the session.** The first frame of every slice is the
  System Event `O` ("start of messages"). A replay that begins mid-stream is refused unless it is
  restored from a snapshot whose checksum and invariants verify (`ReplayStartError`).
* **Slices.** `scripts/extract_slices.py` (Python) and `itch_replay extract` (C++) copy every
  message of one stock-locate plus all System Events into a small file with identical framing and
  original order. This is exact for a single-symbol book, because every message that changes the
  book (A, F, E, C, X, D, U) carries its symbol's stock-locate in the common header, so no other
  symbol's messages are ever needed. Nothing is reordered or edited. SHA-256 of each slice is in
  `data/slices/manifest_12302019.json`; the two extractors produce byte-identical output (checked,
  see README).

## 2. ITCH semantics implemented

| Message | Book effect |
|---|---|
| `A`, `F` Add Order (without / with MPID) | New displayed order joins the back of its price level. |
| `E` Order Executed | Reduce displayed shares by the executed quantity; order keeps priority; removed at 0. |
| `C` Order Executed With Price | Same, and the book reduces the order at its *display* price (the message price is only the trade price). |
| `X` Order Cancel | Partial cancel: reduce displayed shares, keep priority; removed at 0. |
| `D` Order Delete | Remove all remaining shares. |
| `U` Order Replace | Remove the old order entirely; add a **new order under the new reference** with the new shares and price. Side, symbol and MPID are inherited from the old order (the message does not carry them). The new order goes to the **back of the queue even if the price is unchanged**. |
| `S` System Event, `H` Trading Action, `R` Stock Directory | Recorded for diagnostics / symbol lookup. They do not change the book. |
| `P` Trade (non-cross), `Q` Cross, `B` Broken Trade, `I` NOII, `N` RPII, `Y`, `L`, `V`, `W`, `K`, `J`, `h`, `O` | Length-checked and counted; no effect on the *displayed* book. Non-displayed executions (`P`) and cross prints (`Q`) do not consume displayed shares. |

Integer prices (1/10,000 dollar) and integer share counts are used everywhere in the book.
Prices in the simulator and strategy are the same integer units; P&L is accumulated in price
units and converted to dollars only for display.

Frames are applied strictly in file order. Timestamps are never used to sort, so events with
identical timestamps keep their wire order.

## 3. Data structures

Python: `orders` dict (ref → Order), per-side `price → Level` dicts, per-side sorted price lists
(bisect), and an insertion-ordered dict inside each level that *is* the FIFO queue (O(1) add,
O(1) delete anywhere, iteration in time priority). C++: open-addressing hash for ref → pool index,
orders pooled in a vector and chained into an intrusive FIFO list per level, `std::map` for the
price levels. Both give identical FIFO order, which the checksum covers.

## 4. Diagnostics and anomaly policy

Nothing raises on bad data; everything is counted (identical names in Python and C++):

| Condition | Action | Counter |
|---|---|---|
| frame length wrong for its type, empty frame | skipped | `malformed_messages` |
| type byte not in the 5.0 spec | skipped | `unknown_message_type` |
| bad frame header (<3 bytes) / truncated final frame | skipped | `bad_frames` / `truncated_tail_bytes` |
| E/C/X/D/U for a reference not in the book | ignored | `unknown_ref` (+ by message type) |
| Add with an existing reference | ignored (original kept) | `duplicate_ref` |
| execute / cancel shares > remaining | **clamped to remaining** (so quantity never goes negative) | `qty_inconsistency`, `qty_excess_exec/cancel` |
| zero-share add / execute / cancel / replace | ignored (replace: old order is still removed) | `zero_qty_event` |
| side not B/S | ignored | `bad_side` |
| Add's stock ≠ book symbol | applied, flagged | `symbol_mismatch` |
| message locate ≠ book locate | skipped | `locate_mismatch` |
| best bid == best ask (locked) or > (crossed) | **state is kept and recorded**, never asserted away | `locked_episodes`, `crossed_episodes`, `events_while_*`, `max_*_ns`, `total_*_ns`, plus timestamped samples |
| invariant violation | counted and recorded | `invariant_failures` |

Why locked/crossed is a measurement and not an assertion: the displayed Nasdaq book can legitimately
be locked or crossed around auctions, halts and with unsupported events, and a crossed book may
also be the first visible symptom of a missed message. The replay therefore reports the episode
count, duration, and the first samples with timestamps so they can be investigated.
(On the six symbols replayed here the day produced none; the anomaly paths are exercised by the
fuzz generator, which injects crossed prices, unknown references, over-executions, duplicates,
zero quantities, truncated and unknown messages.)

**Invariant checks** (`itchlob/checks.py`, run at the end of every replay and optionally every N
frames; per-removal lookup checks with `--debug`):
remaining quantity > 0 for every resting order · `Level.total` equals the sum of its orders ·
the order map and the level queues describe the same set of orders (no ghost in either direction) ·
each order sits in the level of its own side and price · price lists strictly sorted and equal to the
level keys · no empty levels · a removed order is absent from both the reference map and the level.

## 5. Determinism, checksum, snapshots

* **State checksum**: a 64-bit order-sensitive mix over `(order count, then for bids best-to-worst
  and asks best-to-worst: price, total, order count, and for each order in FIFO order: reference,
  remaining shares)`. It is identical in Python and C++. It is a check on state identity, not a cryptographic digest.
* `determinism` command: N replays of identical input produce identical checksum, counters and
  periodic checkpoints; a snapshot taken mid-session and resumed gives the same final state as an
  uninterrupted run; paced and unpaced replays give the same state; starting mid-stream with an empty
  book is refused.
* Pacing (`--speed`), pause/resume (`ReplayController`, or stdin with `--interactive`) only affect
  wall-clock behaviour. Periodic snapshots are taken on event-time boundaries (before the first event at
  or after the boundary), so they are reproducible.

## 6. Python/C++ equivalence

For each symbol both implementations replay the same slice and are compared on: frame counts,
first/last timestamp, every counter, per-type message counts, the checksum at every 100,000th frame,
and a full dump of the final book (every level, every order in queue order) compared byte-for-byte. The same is done at a mid-session
cut where thousands of orders are live (end-of-day books are empty, so a final-state comparison alone
would be weak). In addition, seeded synthetic streams with injected anomalies are compared the same way,
and the C++ binary was run under AddressSanitizer/UBSan on them.

## 7. Execution simulator

The counterfactual limitation, the timing convention, the marketable-order algorithm, and the four
queue-position models are documented at the top of `itchlob/sim.py`. In short:

* Simulated orders are never inserted into the book. They cannot move the market, cannot change other
  participants' behaviour, and cannot be filled by hidden liquidity. Results are only meaningful for
  sizes that are small relative to displayed depth. Enforced: `max_order_qty`, an optional
  depth-participation cap, and a virtual-consumption ledger that prevents two simulated marketable
  orders from taking the same displayed shares.
* Latency: decision time, order-entry, cancel and replace delays are configurable (plus seeded jitter).
  An action arriving at time A is applied after every event stamped ≤ A. Cancels race against fills.
* Passive queue position: displayed quantity ahead at arrival, reduced by executions at that level
  (front of queue), and by cancels according to the chosen model (conservative: cancels were behind
  us; optimistic: ahead of us; proportional; identity: use ITCH order references to know which side
  of our marker the cancelled order was on). A trade-through fills us. Hidden liquidity, odd-lot
  rules, self-impact and exchange-specific priority exceptions are not modelled.
* Fees default to `+$0.0030`/share removing liquidity and `-$0.0020`/share (rebate) adding it. These are
  placeholders in the range of published schedules, not the schedule of any specific account.

## 8. Strategy and risk

Two transparent quoting rules (`fixed`, `inventory_aware`) and the shared risk layer are described
at the top of `itchlob/strategy.py`. Parameters are round numbers and were not tuned on the data.
Risk: max position, max order quantity, order-message rate limit (sliding 1 s window), stale-data
stand-down (no update for 2 s of event time pulls quotes, and quoting resumes after 3 fresh events),
loss limit with kill switch and flatten, trading window with end-of-window flatten.

Stale data caveat: ITCH slices contain no heartbeat, so "no update for 2 s" is also what a quiet
stock looks like. For liquid names the rule is rarely triggered; for quiet names it can trigger on normal quiet periods.

## 9. Performance methodology (labels matter)

Numbers in `results/BENCHMARKS.md` are **single-thread processing time per message inside one process on
a laptop**, with the input already in memory (and a separate end-to-end figure including file reads).
They are *not* network, exchange-to-trader or tick-to-trade latencies. The per-message percentiles come
from a separate pass that timestamps each call; the timer's own overhead and resolution (41 ns on this
Apple-silicon machine) are reported next to the results, and C++ per-message percentiles are therefore
quantized. Throughput comes from passes with no timers.

## 10. Relevance to an options firm

This project uses **equity** market data from one venue's displayed order-level feed. It does not
touch options data, option pricing, implied volatility, Greeks, or OPRA-style consolidated quote feeds.
What transfers is the engineering and microstructure skill set: reconstructing state from an
ordered event stream with strict message-ordering rules, anomaly accounting instead of assertions,
deterministic replay with state checksums, queue-position and latency modelling of passive orders,
risk limits around an automated quoting loop, and measured C++ vs Python performance. Option-market
specifics (large instrument universe per underlying, quote-driven books, multi-leg orders,
hedging against the underlying) are outside this project, and I did not verify which of the options venues' feeds
publish order-level data, so I make no claim that the same replay would apply to them unchanged.

## 11. Known limitations

* One day, six symbols: scenario counts are cells of a parameter grid on the same market data,
  not independent samples; do not read significance into small differences.
* The strategies lose money on this day. They are there to exercise the simulator and to measure
  sensitivities, not to demonstrate an edge. Markouts show adverse selection exceeds spread capture at
  a one-tick spread.
* Single venue: a real quoter sees the consolidated market (other exchanges' queues, SIP/direct feed
  price moves), which this replay does not have. Price moves driven by other venues appear here only
  through their effect on Nasdaq's displayed book.
* Self-impact and hidden liquidity are not modelled (section 7).
