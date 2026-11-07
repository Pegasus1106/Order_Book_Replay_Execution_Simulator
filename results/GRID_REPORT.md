# Strategy scenario results

192 scenarios = 6 symbols x 2 quoting rules x 4 queue assumptions x 4 latencies, one trading day (2019-12-30), window 09:35-15:55 ET, flat at the end of the window.

## 1. Fixed vs inventory-aware (queue = identity, one-way latency 250 us)

| symbol | rule | net P&L $ | fees $ (neg = rebate) | turnover $M | shares | share fill rate | max |inv| | mean |inv| | 1s markout c/sh (passive) | loss-limit halt |
|---|---|---|---|---|---|---|---|---|---|---|
| AAPL | fixed | -2,511 | -472 | 72.98 | 253,482 | 4.3% | 500 | 118 | -1.10 | yes |
| AAPL | inventory_aware | -2,503 | -490 | 74.60 | 258,238 | 3.5% | 324 | 70 | -1.16 | yes |
| AMD | fixed | -321 | -722 | 17.66 | 389,724 | 24.5% | 500 | 267 | -0.31 | no |
| AMD | inventory_aware | -534 | -529 | 12.86 | 283,608 | 17.2% | 333 | 102 | -0.32 | no |
| INTC | fixed | -128 | -317 | 10.51 | 176,232 | 26.5% | 500 | 264 | -0.26 | no |
| INTC | inventory_aware | -127 | -257 | 8.58 | 143,828 | 20.4% | 325 | 99 | -0.26 | no |
| MSFT | fixed | -634 | -634 | 56.47 | 358,164 | 10.4% | 524 | 299 | -0.23 | no |
| MSFT | inventory_aware | -361 | -524 | 45.97 | 291,598 | 8.2% | 302 | 98 | -0.23 | no |
| NIO | fixed | 1,595 | -1,210 | 2.52 | 635,630 | 20.1% | 507 | 291 | 0.13 | no |
| NIO | inventory_aware | 774 | -802 | 1.93 | 491,744 | 14.3% | 349 | 112 | 0.06 | no |
| SPY | fixed | 1,029 | -946 | 164.77 | 512,716 | 8.8% | 500 | 320 | -0.09 | no |
| SPY | inventory_aware | 463 | -732 | 127.65 | 397,202 | 6.2% | 263 | 104 | -0.08 | no |

## 2. Paired comparison across all cells (same symbol, queue model, latency)

- inventory-aware net P&L beats fixed in 49/96 cells; median difference $2, mean $-104.
- inventory-aware has lower mean |inventory| in 96/96 cells.
- Cells are not independent samples: they share one day of one set of symbols.


## 3.1 Sensitivity, fixed: mean over symbols


*net P&L $ per symbol-day*

| one-way latency us | conservative | optimistic | proportional | identity |
|---|---|---|---|---|
| 50 | -408 | -359 | -470 | -150 |
| 250 | -397 | -437 | -520 | -162 |
| 1000 | -492 | -680 | -671 | -261 |
| 5000 | -643 | -905 | -872 | -478 |

*share fill rate*

| one-way latency us | conservative | optimistic | proportional | identity |
|---|---|---|---|---|
| 50 | 10.8% | 20.1% | 18.2% | 15.9% |
| 250 | 10.8% | 19.9% | 18.0% | 15.8% |
| 1000 | 10.8% | 19.8% | 17.9% | 15.7% |
| 5000 | 10.9% | 19.2% | 17.4% | 15.3% |

*passive 1s markout c/share*

| one-way latency us | conservative | optimistic | proportional | identity |
|---|---|---|---|---|
| 50 | -0.39 | -0.31 | -0.35 | -0.31 |
| 250 | -0.40 | -0.31 | -0.36 | -0.31 |
| 1000 | -0.40 | -0.32 | -0.36 | -0.32 |
| 5000 | -0.44 | -0.35 | -0.40 | -0.35 |

## 3.2 Sensitivity, inventory_aware: mean over symbols


*net P&L $ per symbol-day*

| one-way latency us | conservative | optimistic | proportional | identity |
|---|---|---|---|---|
| 50 | -461 | -488 | -583 | -358 |
| 250 | -478 | -540 | -621 | -381 |
| 1000 | -592 | -691 | -739 | -492 |
| 5000 | -747 | -858 | -878 | -663 |

*share fill rate*

| one-way latency us | conservative | optimistic | proportional | identity |
|---|---|---|---|---|
| 50 | 8.1% | 14.2% | 13.0% | 11.8% |
| 250 | 8.1% | 13.9% | 12.9% | 11.7% |
| 1000 | 8.0% | 13.7% | 12.7% | 11.5% |
| 5000 | 8.0% | 13.4% | 12.4% | 11.3% |

*passive 1s markout c/share*

| one-way latency us | conservative | optimistic | proportional | identity |
|---|---|---|---|---|
| 50 | -0.39 | -0.33 | -0.38 | -0.33 |
| 250 | -0.40 | -0.34 | -0.38 | -0.33 |
| 1000 | -0.42 | -0.33 | -0.38 | -0.35 |
| 5000 | -0.46 | -0.36 | -0.40 | -0.37 |

## 4. How much does the queue assumption move the answer?

Mean over latencies of `net P&L $ / share fill rate`.

| symbol | rule | conservative | optimistic | proportional | identity |
|---|---|---|---|---|---|
| AAPL | fixed | -2,512 / 4.1% | -2,516 / 5.1% | -2,515 / 4.7% | -2,514 / 4.4% |
| AAPL | inventory_aware | -2,506 / 3.3% | -2,504 / 3.9% | -2,505 / 3.6% | -2,505 / 3.5% |
| AMD | fixed | -481 / 17.4% | -1,273 / 30.5% | -1,023 / 28.3% | -413 / 24.2% |
| AMD | inventory_aware | -583 / 12.4% | -929 / 20.3% | -933 / 18.9% | -617 / 17.0% |
| INTC | fixed | -239 / 15.3% | -406 / 33.5% | -274 / 30.4% | -115 / 26.2% |
| INTC | inventory_aware | -131 / 11.5% | -305 / 23.5% | -276 / 22.3% | -142 / 20.2% |
| MSFT | fixed | -686 / 7.3% | -894 / 15.3% | -937 / 13.1% | -751 / 10.3% |
| MSFT | inventory_aware | -385 / 5.7% | -696 / 11.3% | -687 / 9.9% | -441 / 8.1% |
| NIO | fixed | 382 / 15.2% | 1,147 / 21.8% | 635 / 19.7% | 1,430 / 20.1% |
| NIO | inventory_aware | 104 / 11.3% | 529 / 15.0% | 260 / 13.9% | 655 / 14.2% |
| SPY | fixed | 627 / 5.6% | 371 / 12.3% | 316 / 11.0% | 788 / 8.7% |
| SPY | inventory_aware | 83 / 4.0% | 40 / 8.7% | -91 / 7.9% | 208 / 6.2% |

## 5. Loss-limit halts

32 of 192 scenarios hit the loss limit (fixed: 16, inventory_aware: 16).

| symbol | rule | halted scenarios | median halt time |
|---|---|---|---|
| AAPL | fixed | 16/16 | 11.70 h ET |
| AAPL | inventory_aware | 16/16 | 12.45 h ET |
| AMD | fixed | 0/16 | - |
| AMD | inventory_aware | 0/16 | - |
| INTC | fixed | 0/16 | - |
| INTC | inventory_aware | 0/16 | - |
| MSFT | fixed | 0/16 | - |
| MSFT | inventory_aware | 0/16 | - |
| NIO | fixed | 0/16 | - |
| NIO | inventory_aware | 0/16 | - |
| SPY | fixed | 0/16 | - |
| SPY | inventory_aware | 0/16 | - |
