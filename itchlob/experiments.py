"""Experiments built on the replay + simulator, and the parallel scenario runner.

Parallelism: scenarios are independent (each replays its own symbol slice from the start
of the session with its own book), so they are farmed out to worker processes. Inside a
scenario, events are applied strictly in file order, so per-symbol ordering is preserved.
"""
from __future__ import annotations

import concurrent.futures as cf
import itertools
import json
import os
import statistics
import sys
import time
from dataclasses import asdict, fields
from typing import Dict, List, Optional

from .book import OrderBook
from .framing import FrameReader, find_locates
from .messages import SIDE_BUY, SIDE_SELL
from .replay import Replay, ReplayConfig
from .sim import (CONSERVATIVE, IDENTITY, OPTIMISTIC, PROPORTIONAL, QUEUE_MODELS, UNITS_PER_USD,
                  ExecutionSimulator, FeeModel, LatencyModel)
from .strategy import S, Strategy, StrategyConfig

HMS = lambda h, m=0, s=0: (h * 3600 + m * 60 + s) * S


def fanout(*sims):
    fns = [s.on_book_event for s in sims]

    def f(kind, side, price, qty, seq, ref, ts):
        for fn in fns:
            fn(kind, side, price, qty, seq, ref, ts)
    return f


def _replay_with_hooks(path, symbol, locate, book, pre, post):
    fr = FrameReader(path, locates={locate})
    res = Replay(ReplayConfig(final_check=False), pre_hook=pre, post_hook=post).run(fr, book)
    fr.close()
    return res


# ----------------------------------------------------------------------------- strategy scenario
def run_scenario(p: dict) -> dict:
    """One strategy scenario. `p` is a plain dict so it can cross process boundaries."""
    t0 = time.perf_counter()
    path, symbol = p["path"], p["symbol"]
    locate = p.get("locate") or find_locates(path, [symbol])[symbol]
    cfg_kw = {k: v for k, v in p.get("strategy", {}).items() if k in {f.name for f in fields(StrategyConfig)}}
    cfg = StrategyConfig(**cfg_kw)
    lat = LatencyModel(**p.get("latency", {}))
    fees = FeeModel(**p.get("fees", {}))
    book = OrderBook(symbol, locate)
    sim = ExecutionSimulator(book, fees, lat, p.get("queue_model", IDENTITY),
                             max_order_qty=cfg.max_order_qty)
    strat = Strategy(book, sim, cfg)
    res = _replay_with_hooks(path, symbol, locate, book, strat.pre, strat.post)
    sim.finish()
    summ = strat.summary()
    out = {"params": p, "summary": summ, "frames": res.frames, "book_checksum": f"{res.checksum:016x}",
           "book_counters_nonzero": {k: v for k, v in res.counters.items() if v and k in
                                     ("unknown_ref", "qty_inconsistency", "duplicate_ref")},
           "seconds": time.perf_counter() - t0}
    if p.get("keep_series"):
        out["series"] = strat.series
        out["fills"] = [list(f) for f in sim.fills]
    return out


def _worker(p):
    try:
        return run_scenario(p)
    except Exception as e:  # keep the pool alive; report the failure with its parameters
        import traceback
        return {"params": p, "error": repr(e), "traceback": traceback.format_exc()}


def build_grid(args) -> List[dict]:
    slices = {}
    for spec in args.slices:
        sym, path = spec.split("=")
        slices[sym.upper()] = path
    scenarios = []
    lat_list = [int(float(x) * 1000) for x in args.latencies_us.split(",")]   # one-way, in ns
    models = args.queue_models.split(",")
    kinds = args.kinds.split(",")
    for sym, kind, model, lat in itertools.product(slices, kinds, models, lat_list):
        scenarios.append({
            "path": slices[sym], "symbol": sym, "queue_model": model,
            "latency": {"decision_ns": args.decision_us * 1000, "submit_ns": lat, "cancel_ns": lat,
                        "replace_ns": lat, "jitter_ns": 0},
            "fees": {"taker_per_share": args.taker_fee, "maker_per_share": args.maker_fee},
            "strategy": {"kind": kind, "quote_size": args.quote_size, "max_position": args.max_position,
                         "max_order_qty": args.max_order_qty, "loss_limit_usd": args.loss_limit},
        })
    return scenarios


def cmd_grid(a) -> int:
    scen = build_grid(a)
    print(f"{len(scen)} scenarios on {a.workers} worker processes", flush=True)
    t0 = time.perf_counter()
    results = []
    with cf.ProcessPoolExecutor(max_workers=a.workers) as ex:
        futs = [ex.submit(_worker, s) for s in scen]
        for i, f in enumerate(cf.as_completed(futs), 1):
            r = f.result()
            results.append(r)
            if "error" in r:
                print("ERROR", r["params"]["symbol"], r["error"], file=sys.stderr)
            if i % 10 == 0 or i == len(scen):
                print(f"  {i}/{len(scen)} done ({time.perf_counter() - t0:.0f}s)", flush=True)
    order = {json.dumps(s, sort_keys=True): i for i, s in enumerate(scen)}
    results.sort(key=lambda r: order.get(json.dumps(r["params"], sort_keys=True), 0))
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    with open(a.out, "w") as f:
        for r in results:
            f.write(json.dumps(r) + "\n")
    wall = time.perf_counter() - t0
    cpu = sum(r.get("seconds", 0) for r in results)
    print(f"wrote {a.out}: {len(results)} scenarios, wall {wall:.0f}s, sum of scenario time {cpu:.0f}s "
          f"(parallel speed-up x{cpu / wall:.1f})")
    return 0 if all("error" not in r for r in results) else 1


def cmd_strategy(a) -> int:
    p = {"path": a.file, "symbol": a.symbol.upper(), "queue_model": a.queue_model, "keep_series": True,
         "latency": {"decision_ns": a.decision_us * 1000, "submit_ns": int(a.latency_us * 1000),
                     "cancel_ns": int(a.latency_us * 1000), "replace_ns": int(a.latency_us * 1000)},
         "fees": {"taker_per_share": a.taker_fee, "maker_per_share": a.maker_fee},
         "strategy": {"kind": a.kind, "quote_size": a.quote_size, "max_position": a.max_position,
                      "max_order_qty": a.max_order_qty, "loss_limit_usd": a.loss_limit}}
    r = run_scenario(p)
    s = r["summary"]
    print(json.dumps({k: v for k, v in s.items() if k not in ("markouts", "sim_stats", "strategy_counters")}, indent=1))
    print("markouts (cents/share):")
    for liq, row in s["markouts"].items():
        print("  ", liq, {h: (round(v["cents_per_share"], 3) if v["cents_per_share"] is not None else None)
                          for h, v in row.items()})
    print("strategy counters:", s["strategy_counters"])
    print("sim stats:", s["sim_stats"])
    if a.out:
        with open(a.out, "w") as f:
            json.dump(r, f)
    return 0


# ----------------------------------------------------------------------------- marketable demo
def cmd_marketable(a) -> int:
    """Marketable orders at fixed times, at several latencies and sizes (no strategy)."""
    path, symbol = a.file, a.symbol.upper()
    locate = find_locates(path, [symbol])[symbol]
    lats = [int(float(x) * 1000) for x in a.latencies_us.split(",")]
    sizes = [int(x) for x in a.sizes.split(",")]
    fees = FeeModel(a.taker_fee, a.maker_fee)
    book = OrderBook(symbol, locate)
    sims = {l: ExecutionSimulator(book, fees, LatencyModel(0, l, l, l), max_order_qty=10**9,
                                  max_depth_fraction=None, attach_observer=False, keep_fill_log=False)
            for l in lats}
    book.observer = fanout(*sims.values())
    times = list(range(HMS(10), HMS(15, 30) + 1, a.every_min * 60 * S))
    state = {"i": 0}

    def pre(ts):
        while state["i"] < len(times) and ts >= times[state["i"]]:
            t = times[state["i"]]
            for l, sm in sims.items():
                sm.advance(t)
                for sz in sizes:
                    for side in (SIDE_BUY, SIDE_SELL):
                        sm.submit_marketable(t, side, sz)
            state["i"] += 1
        for sm in sims.values():
            sm.advance(ts)

    _replay_with_hooks(path, symbol, locate, book, pre, None)
    for sm in sims.values():
        sm.finish()
    rows = []
    for l, sm in sims.items():
        for sz in sizes:
            rs = [r for r in sm.market_results if r["qty"] == sz]
            filled = sum(r["filled"] for r in rs)
            tot = sum(r["qty"] for r in rs)
            slip = [r["slippage_vs_decision_mid_units"] for r in rs if r["slippage_vs_decision_mid_units"] is not None]
            slip_a = [r["slippage_vs_arrival_mid_units"] for r in rs if r["slippage_vs_arrival_mid_units"] is not None]
            fee = sum(r["fee_usd"] for r in rs)
            ss = sorted(slip)
            rows.append({
                "latency_us": l / 1000, "size": sz, "orders": len(rs),
                "fill_pct": 100.0 * filled / tot if tot else 0,
                "orders_with_unfilled": sum(1 for r in rs if r["unfilled"] > 0),
                "mean_slippage_vs_decision_mid_cents": statistics.mean(slip) / 100 if slip else None,
                "p95_slippage_vs_decision_mid_cents": ss[int(0.95 * (len(ss) - 1))] / 100 if ss else None,
                "stdev_slippage_vs_decision_mid_cents": statistics.pstdev(slip) / 100 if slip else None,
                "mean_slippage_vs_arrival_mid_cents": statistics.mean(slip_a) / 100 if slip_a else None,
                "fees_usd_per_share": fee / filled if filled else None,
            })
    print(f"{symbol} marketable orders, {len(times)} times x {len(sizes)} sizes x 2 sides")
    print(f"{'lat(us)':>8} {'size':>7} {'fill%':>6} {'unfilled':>8} {'mean slip':>10} {'p95 slip':>9} "
          f"{'stdev':>7} {'vs arrival mid':>15} {'fee $/sh':>9}   (slippage: cents/share vs decision-time mid)")
    for r in rows:
        f = lambda x: "-" if x is None else f"{x:.3f}"
        print(f"{r['latency_us']:>8g} {r['size']:>7} {r['fill_pct']:>6.1f} {r['orders_with_unfilled']:>8} "
              f"{f(r['mean_slippage_vs_decision_mid_cents']):>10} {f(r['p95_slippage_vs_decision_mid_cents']):>9} "
              f"{f(r['stdev_slippage_vs_decision_mid_cents']):>7} {f(r['mean_slippage_vs_arrival_mid_cents']):>15} "
              f"{f(r['fees_usd_per_share']):>9}")
    if a.out:
        with open(a.out, "w") as f:
            json.dump({"symbol": symbol, "rows": rows,
                       "example": next((sm.market_results[:6] for sm in sims.values()), [])}, f, indent=1)
    return 0


# ----------------------------------------------------------------------------- queue-assumption study
def cmd_queue(a) -> int:
    """Passive probe orders at the touch, compared across queue-position assumptions."""
    path, symbol = a.file, a.symbol.upper()
    locate = find_locates(path, [symbol])[symbol]
    lat = int(a.latency_us * 1000)
    fees = FeeModel(a.taker_fee, a.maker_fee)
    book = OrderBook(symbol, locate)
    sims = {m: ExecutionSimulator(book, fees, LatencyModel(0, lat, lat, lat), queue_model=m,
                                  max_order_qty=10**9, attach_observer=False) for m in QUEUE_MODELS}
    book.observer = fanout(*sims.values())
    times = list(range(HMS(10), HMS(15, 30), a.every_s * S))
    hold = a.hold_s * S
    state = {"i": 0}
    probes = {m: [] for m in QUEUE_MODELS}     # (order, decision_ts, ahead_at_arrival)
    pending_cancel = []                         # (cancel_ts, model, order)

    def pre(ts):
        while state["i"] < len(times) and ts >= times[state["i"]]:
            t = times[state["i"]]
            state["i"] += 1
            if not book.bid_prices or not book.ask_prices:
                continue
            bb, ba = book.bid_prices[-1], book.ask_prices[0]
            for m, sm in sims.items():
                sm.advance(t)
                for side, px in ((SIDE_BUY, bb), (SIDE_SELL, ba)):
                    o = sm.submit_passive(t, side, a.size, px)
                    probes[m].append(o)
                    pending_cancel.append((t + hold, m, o))
        # issue cancels that are due
        while pending_cancel and pending_cancel[0][0] <= ts:
            ct, m, o = pending_cancel.pop(0)
            sims[m].advance(ct)
            sims[m].cancel(ct, o.id)
        for sm in sims.values():
            sm.advance(ts)

    _replay_with_hooks(path, symbol, locate, book, pre, None)
    for sm in sims.values():
        sm.finish()
    print(f"{symbol}: {len(times)} probe times x 2 sides, {a.size} shares at the touch, "
          f"hold {a.hold_s}s, one-way latency {a.latency_us:g}us")
    print(f"{'model':>13} {'orders':>7} {'any-fill%':>10} {'full-fill%':>11} {'share-fill%':>12} "
          f"{'median t-first-fill(s)':>23} {'mean ahead@arrival':>19}")
    # NOTE: fills here include trade-through fills (price moving through the probe), which
    # are identical across queue models; the models only differ for fills at the touch.
    rows = []
    for m in QUEUE_MODELS:
        sm = sims[m]
        first = {}
        for f in sm.fills:
            first.setdefault(f.order_id, f.ts)
        os_ = [o for o in probes[m] if o.arrive_ts is not None]
        anyf = sum(1 for o in os_ if o.filled > 0)
        full = sum(1 for o in os_ if o.filled >= o.qty)
        shares = sum(o.filled for o in os_)
        tt = [(first[o.id] - o.arrive_ts) / S for o in os_ if o.id in first]
        aheads = [o.ahead0 for o in os_ if o.ahead0 is not None]
        row = {"model": m, "orders": len(os_), "any_fill_pct": 100.0 * anyf / len(os_),
               "full_fill_pct": 100.0 * full / len(os_),
               "share_fill_pct": 100.0 * shares / sum(o.qty for o in os_),
               "median_time_to_first_fill_s": statistics.median(tt) if tt else None,
               "mean_ahead_at_arrival": statistics.mean(aheads) if aheads else None,
               "median_ahead_at_arrival": statistics.median(aheads) if aheads else None,
               "cancel_too_late": sm.stats["cancel_too_late"], "through_fills": sm.stats["through_fills"]}
        rows.append(row)
        mt = row["median_time_to_first_fill_s"]
        print(f"{m:>13} {row['orders']:>7} {row['any_fill_pct']:>10.1f} {row['full_fill_pct']:>11.1f} "
              f"{row['share_fill_pct']:>12.1f} {('-' if mt is None else f'{mt:.2f}'):>23} "
              f"{row['mean_ahead_at_arrival']:>19.0f}")
    if a.out:
        with open(a.out, "w") as f:
            json.dump({"symbol": symbol, "latency_us": a.latency_us, "hold_s": a.hold_s, "rows": rows}, f, indent=1)
    return 0


# ----------------------------------------------------------------------------- registration
def register(sub):
    def common(p):
        p.add_argument("--taker-fee", type=int, default=30, help="price units/share (30 = $0.0030)")
        p.add_argument("--maker-fee", type=int, default=-20, help="price units/share (-20 = $0.0020 rebate)")
        p.add_argument("--decision-us", type=int, default=20)

    m = sub.add_parser("marketable", help="marketable-order simulation: VWAP, slippage, shortfall")
    m.add_argument("file")
    m.add_argument("--symbol", required=True)
    m.add_argument("--latencies-us", default="0,100,1000,10000")
    m.add_argument("--sizes", default="100,500,2000,10000,100000")
    m.add_argument("--every-min", type=int, default=10)
    m.add_argument("--taker-fee", type=int, default=30)
    m.add_argument("--maker-fee", type=int, default=-20)
    m.add_argument("--out")
    m.set_defaults(fn=cmd_marketable)

    q = sub.add_parser("queue", help="compare queue-position assumptions with passive probe orders")
    q.add_argument("file")
    q.add_argument("--symbol", required=True)
    q.add_argument("--size", type=int, default=100)
    q.add_argument("--every-s", type=int, default=30)
    q.add_argument("--hold-s", type=int, default=30)
    q.add_argument("--latency-us", type=float, default=100)
    q.add_argument("--taker-fee", type=int, default=30)
    q.add_argument("--maker-fee", type=int, default=-20)
    q.add_argument("--out")
    q.set_defaults(fn=cmd_queue)

    s = sub.add_parser("strategy", help="run a single strategy scenario")
    s.add_argument("file")
    s.add_argument("--symbol", required=True)
    s.add_argument("--kind", choices=["fixed", "inventory_aware"], default="fixed")
    s.add_argument("--queue-model", choices=list(QUEUE_MODELS), default=IDENTITY)
    s.add_argument("--latency-us", type=float, default=100)
    s.add_argument("--quote-size", type=int, default=100)
    s.add_argument("--max-position", type=int, default=500)
    s.add_argument("--max-order-qty", type=int, default=200)
    s.add_argument("--loss-limit", type=float, default=2500.0)
    s.add_argument("--out")
    common(s)
    s.set_defaults(fn=cmd_strategy)

    g = sub.add_parser("grid", help="parallel scenario grid: symbols x rules x queue models x latencies")
    g.add_argument("--slices", nargs="+", required=True, help="SYMBOL=path.itch ...")
    g.add_argument("--kinds", default="fixed,inventory_aware")
    g.add_argument("--queue-models", default=",".join(QUEUE_MODELS))
    g.add_argument("--latencies-us", default="50,250,1000,5000")
    g.add_argument("--quote-size", type=int, default=100)
    g.add_argument("--max-position", type=int, default=500)
    g.add_argument("--max-order-qty", type=int, default=200)
    g.add_argument("--loss-limit", type=float, default=2500.0)
    g.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 1))
    g.add_argument("--out", default="results/grid.jsonl")
    common(g)
    g.set_defaults(fn=cmd_grid)

    from . import report as _report
    _report.register(sub)
