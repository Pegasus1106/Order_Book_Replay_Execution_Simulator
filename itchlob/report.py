"""Aggregate scenario-grid results into markdown tables and a small SVG chart."""
from __future__ import annotations

import json
import statistics
from collections import defaultdict
from typing import Dict, List

from .sim import QUEUE_MODELS


def load(path: str) -> List[dict]:
    rows = []
    with open(path) as f:
        for line in f:
            r = json.loads(line)
            if "error" in r:
                continue
            p, s = r["params"], r["summary"]
            mo = s["markouts"]["passive"]
            rows.append({
                "symbol": p["symbol"], "kind": p["strategy"]["kind"], "model": p["queue_model"],
                "lat_us": p["latency"]["submit_ns"] / 1000,
                "net": s["net_pnl_usd"], "realized": s["realized_pnl_usd"], "fees": s["fees_usd"],
                "turnover": s["turnover_usd"], "shares": s["shares_traded"],
                "fill_sh": s["fill_rate_shares"], "fill_ord": s["fill_rate_orders"],
                "max_inv": s["max_abs_inventory"], "mean_inv": s["mean_abs_inventory_sampled"],
                "halted": s["halted"], "halt_s": s["halt_time_s"],
                "mo1": (mo.get("1s") or {}).get("cents_per_share"),
                "mo0": (mo.get("0s") or {}).get("cents_per_share"),
                "mo30": (mo.get("30s") or {}).get("cents_per_share"),
                "net_cps": s["net_pnl_per_share_cents"],
                "passive_inst": s["passive_instances"], "msgs": s["strategy_counters"]["new_orders"]
                + s["strategy_counters"]["cancels"] + s["strategy_counters"]["replaces"],
                "seconds": r.get("seconds", 0),
            })
    return rows


def _fmt(x, nd=0, pct=False):
    if x is None:
        return "-"
    if pct:
        return f"{100 * x:.1f}%"
    return f"{x:,.{nd}f}"


def _table(header, rows):
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(c) for c in r) + " |")
    return "\n".join(out)


def _mean(xs):
    xs = [x for x in xs if x is not None]
    return statistics.mean(xs) if xs else None


def build_report(rows: List[dict]) -> str:
    syms = sorted({r["symbol"] for r in rows})
    lats = sorted({r["lat_us"] for r in rows})
    kinds = ["fixed", "inventory_aware"]
    out = []
    out.append(f"# Strategy scenario results\n\n{len(rows)} scenarios = {len(syms)} symbols x "
               f"{len(kinds)} quoting rules x {len(QUEUE_MODELS)} queue assumptions x {len(lats)} latencies, "
               f"one trading day (2019-12-30), window 09:35-15:55 ET, flat at the end of the window.\n")

    # 1. rule comparison per symbol at a reference cell
    ref_lat = 250.0 if 250.0 in lats else lats[len(lats) // 2]
    out.append(f"## 1. Fixed vs inventory-aware (queue = identity, one-way latency {ref_lat:g} us)\n")
    t = []
    for s in syms:
        for k in kinds:
            c = [r for r in rows if r["symbol"] == s and r["kind"] == k and r["model"] == "identity"
                 and r["lat_us"] == ref_lat]
            if not c:
                continue
            r = c[0]
            t.append([s, k, _fmt(r["net"]), _fmt(r["fees"]), _fmt(r["turnover"] / 1e6, 2), f"{r['shares']:,}",
                      _fmt(r["fill_sh"], pct=True), r["max_inv"], _fmt(r["mean_inv"], 0),
                      _fmt(r["mo1"], 2), "yes" if r["halted"] else "no"])
    out.append(_table(["symbol", "rule", "net P&L $", "fees $ (neg = rebate)", "turnover $M", "shares",
                       "share fill rate", "max |inv|", "mean |inv|", "1s markout c/sh (passive)",
                       "loss-limit halt"], t))

    # 2. paired comparison
    out.append("\n## 2. Paired comparison across all cells (same symbol, queue model, latency)\n")
    idx = {(r["symbol"], r["kind"], r["model"], r["lat_us"]): r for r in rows}
    better = tot = 0
    inv_better = 0
    diffs = []
    for (s, k, m, l), r in idx.items():
        if k != "inventory_aware":
            continue
        f = idx.get((s, "fixed", m, l))
        if not f:
            continue
        tot += 1
        better += r["net"] > f["net"]
        inv_better += r["mean_inv"] < f["mean_inv"]
        diffs.append(r["net"] - f["net"])
    if tot:
        out.append(f"- inventory-aware net P&L beats fixed in {better}/{tot} cells; "
                   f"median difference ${statistics.median(diffs):,.0f}, mean ${statistics.mean(diffs):,.0f}.")
        out.append(f"- inventory-aware has lower mean |inventory| in {inv_better}/{tot} cells.")
    out.append("- Cells are not independent samples: they share one day of one set of symbols.\n")

    # 3. sensitivity: latency x queue model, averaged over symbols
    for k in kinds:
        out.append(f"\n## 3.{kinds.index(k) + 1} Sensitivity, {k}: mean over symbols\n")
        hdr = ["one-way latency us"] + [f"{m}" for m in QUEUE_MODELS]
        for metric, label, kw in (("net", "net P&L $ per symbol-day", dict(nd=0)),
                                  ("fill_sh", "share fill rate", dict(pct=True)),
                                  ("mo1", "passive 1s markout c/share", dict(nd=2))):
            out.append(f"\n*{label}*\n")
            t = []
            for l in lats:
                row = [f"{l:g}"]
                for m in QUEUE_MODELS:
                    v = _mean([r[metric] for r in rows if r["kind"] == k and r["model"] == m and r["lat_us"] == l])
                    row.append(_fmt(v, **kw))
                t.append(row)
            out.append(_table(hdr, t))

    # 4. spread of outcomes across queue assumptions (how much the assumption matters)
    out.append("\n## 4. How much does the queue assumption move the answer?\n")
    t = []
    for s in syms:
        for k in kinds:
            net = {m: _mean([r["net"] for r in rows if r["symbol"] == s and r["kind"] == k and r["model"] == m])
                   for m in QUEUE_MODELS}
            fr = {m: _mean([r["fill_sh"] for r in rows if r["symbol"] == s and r["kind"] == k and r["model"] == m])
                  for m in QUEUE_MODELS}
            t.append([s, k] + [f"{_fmt(net[m])} / {_fmt(fr[m], pct=True)}" for m in QUEUE_MODELS])
    out.append("Mean over latencies of `net P&L $ / share fill rate`.\n")
    out.append(_table(["symbol", "rule"] + list(QUEUE_MODELS), t))

    # 5. halts
    h = [r for r in rows if r["halted"]]
    out.append(f"\n## 5. Loss-limit halts\n\n{len(h)} of {len(rows)} scenarios hit the loss limit "
               f"(fixed: {sum(1 for r in h if r['kind'] == 'fixed')}, "
               f"inventory_aware: {sum(1 for r in h if r['kind'] == 'inventory_aware')}).\n")
    t = []
    for s in syms:
        for k in kinds:
            c = [r for r in rows if r["symbol"] == s and r["kind"] == k]
            hh = [r for r in c if r["halted"]]
            med = statistics.median([r["halt_s"] for r in hh]) / 3600 if hh else None
            t.append([s, k, f"{len(hh)}/{len(c)}", ("-" if med is None else f"{med:.2f} h ET")])
    out.append(_table(["symbol", "rule", "halted scenarios", "median halt time"], t))
    return "\n".join(out) + "\n"


def svg_series(series: List[list], title: str, path: str, window=(34500, 57300)) -> None:
    """Inventory and cumulative P&L (USD) over the trading window as a self-contained SVG."""
    W, H, M = 900, 520, 60
    pts = [s for s in series if window[0] <= s[0] / 1e9 <= window[1] + 600]
    if len(pts) < 2:
        return
    t0, t1 = pts[0][0], pts[-1][0]
    inv = [p[1] for p in pts]
    pnl = [(p[3] + p[4] - p[5]) / 1e4 for p in pts]

    def poly(vals, y0, h):
        lo, hi = min(vals + [0]), max(vals + [0])
        if hi == lo:
            hi = lo + 1
        xs = [M + (p[0] - t0) / (t1 - t0) * (W - 2 * M) for p in pts]
        ys = [y0 + h - (v - lo) / (hi - lo) * h for v in vals]
        zero = y0 + h - (0 - lo) / (hi - lo) * h
        return " ".join(f"{x:.1f},{y:.1f}" for x, y in zip(xs, ys)), zero, lo, hi

    a, z1, lo1, hi1 = poly(inv, 50, 180)
    b, z2, lo2, hi2 = poly(pnl, 300, 180)
    hrs = lambda ns: f"{int(ns / 3.6e12):02d}:{int(ns / 6e10) % 60:02d}"
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" font-family="sans-serif" font-size="12">
<rect width="100%" height="100%" fill="white"/>
<text x="{M}" y="22" font-size="15" font-weight="bold">{title}</text>
<text x="{M}" y="42">Inventory (shares), range {lo1:g} to {hi1:g}</text>
<line x1="{M}" y1="{z1:.1f}" x2="{W - M}" y2="{z1:.1f}" stroke="#bbb" stroke-dasharray="4"/>
<polyline points="{a}" fill="none" stroke="#1f77b4" stroke-width="1.2"/>
<text x="{M}" y="292">Net P&amp;L (USD, realized + unrealized - fees), range {lo2:,.0f} to {hi2:,.0f}</text>
<line x1="{M}" y1="{z2:.1f}" x2="{W - M}" y2="{z2:.1f}" stroke="#bbb" stroke-dasharray="4"/>
<polyline points="{b}" fill="none" stroke="#d62728" stroke-width="1.2"/>
<text x="{M}" y="{H - 20}">{hrs(t0)} ET</text><text x="{W - M - 40}" y="{H - 20}">{hrs(t1)} ET</text>
</svg>"""
    with open(path, "w") as f:
        f.write(svg)


def cmd_report(a) -> int:
    rows = load(a.grid)
    md = build_report(rows)
    with open(a.out, "w") as f:
        f.write(md)
    print(md)
    return 0


def cmd_plot(a) -> int:
    with open(a.run) as f:
        r = json.load(f)
    p = r["params"]
    svg_series(r["series"], f"{p['symbol']} {p['strategy']['kind']} queue={p['queue_model']} "
               f"latency={p['latency']['submit_ns'] / 1000:g}us", a.out)
    print("wrote", a.out)
    return 0


def register(sub):
    r = sub.add_parser("report", help="markdown tables from a grid result file")
    r.add_argument("grid")
    r.add_argument("--out", default="results/GRID_REPORT.md")
    r.set_defaults(fn=cmd_report)
    p = sub.add_parser("plot", help="SVG of inventory and P&L from a `strategy --out` file")
    p.add_argument("run")
    p.add_argument("--out", required=True)
    p.set_defaults(fn=cmd_plot)
