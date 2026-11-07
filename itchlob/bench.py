"""Performance measurement for the Python reference implementation.

Each stage is measured in its own process (run `bench --stage X`) so that peak RSS is
attributable. Stages:

  frame   file -> frames only (gunzip if needed, length-prefix walk, locate filter)
  parse   decode pre-loaded frames into event tuples                  (parser-only)
  book    apply pre-decoded events to the book                        (book-update-only)
  replay  pre-loaded frames -> decode + book, via the Replay engine   (complete, in memory)
  e2e     file -> frames -> decode -> book -> final checksum          (end-to-end replay)

Per-operation latency is wall-clock time to process one message in this process on this
machine (perf_counter_ns around the call). It is NOT exchange-to-trader network latency.
"""
from __future__ import annotations

import json
import resource
import sys
import time

from .book import OrderBook
from .framing import FrameReader, find_locates
from .messages import BOOK_TYPES, DECODERS, MSG_LEN
from .replay import Replay, ReplayConfig

perf = time.perf_counter
now_ns = time.perf_counter_ns


def peak_rss_bytes() -> int:
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r if sys.platform == "darwin" else r * 1024


def pcts(samples):
    if not samples:
        return {}
    s = sorted(samples)
    n = len(s)

    def at(q):
        return float(s[min(n - 1, int(q * n))])

    return {"p50_ns": at(0.50), "p95_ns": at(0.95), "p99_ns": at(0.99), "p99_9_ns": at(0.999),
            "max_ns": float(s[-1]), "mean_ns": sum(s) / n}


def timer_stats(n=200000):
    t0 = perf()
    deltas = []
    prev = now_ns()
    for _ in range(n):
        t = now_ns()
        deltas.append(t - prev)
        prev = t
    ov = (perf() - t0) * 1e9 / n
    nz = [d for d in deltas if d > 0]
    return ov, (min(nz) if nz else 0)


def run_stage(stage, path, symbol, locate, repeat, lat_limit, limit):
    out = {"impl": "python", "stage": stage, "path": path, "symbol": symbol}
    if locate is None:
        locate = find_locates(path, [symbol])[symbol]
    times = []
    lat = []

    def load_frames():
        t0 = perf()
        with FrameReader(path, locates={locate}) as fr:
            fl = list(fr) if not limit else [m for _, m in zip(range(limit), fr)]
        return fl, perf() - t0

    if stage == "frame":
        for _ in range(repeat):
            t0 = perf()
            n = 0
            with FrameReader(path, locates={locate}) as fr:
                for _m in fr:
                    n += 1
                    if limit and n >= limit:
                        break
            times.append(perf() - t0)
        ops = n
    elif stage == "parse":
        frames, read_s = load_frames()
        out["load_seconds"] = read_s
        dec_get = DECODERS.get
        lens = MSG_LEN
        for _ in range(repeat):
            t0 = perf()
            bad = 0
            for m in frames:
                d = dec_get(m[0])
                if d is not None:
                    try:
                        d(m)
                    except Exception:
                        bad += 1
            times.append(perf() - t0)
        ops = len(frames)
        for m in frames[:lat_limit]:
            t1 = now_ns()
            d = dec_get(m[0])
            if d is not None:
                d(m)
            lat.append(now_ns() - t1)
    elif stage == "book":
        frames, read_s = load_frames()
        out["load_seconds"] = read_s
        events = []
        for m in frames:
            d = DECODERS.get(m[0])
            if d is not None and m[0] in BOOK_TYPES:
                events.append(d(m))
        del frames
        ops = len(events)
        for _ in range(repeat):
            b = OrderBook(symbol, locate)
            apply = b.apply
            t0 = perf()
            for ev in events:
                apply(ev)
            times.append(perf() - t0)
        b = OrderBook(symbol, locate)
        for ev in events[:lat_limit]:
            t1 = now_ns()
            b.apply(ev)
            lat.append(now_ns() - t1)
    elif stage == "replay":
        frames, read_s = load_frames()
        out["load_seconds"] = read_s
        for _ in range(repeat):
            b = OrderBook(symbol, locate)
            t0 = perf()
            r = Replay(ReplayConfig(final_check=False)).run(frames, b)
            times.append(perf() - t0)
        ops = len(frames)
        # per-frame latency: decode + apply, mirroring the replay loop body
        b = OrderBook(symbol, locate)
        dec_get = DECODERS.get
        apply = b.apply
        for m in frames[:lat_limit]:
            t1 = now_ns()
            d = dec_get(m[0])
            if d is not None:
                ev = d(m)
                if m[0] in BOOK_TYPES:
                    apply(ev)
            lat.append(now_ns() - t1)
    elif stage == "e2e":
        for _ in range(repeat):
            t0 = perf()
            b = OrderBook(symbol, locate)
            fr = FrameReader(path, locates={locate})
            r = Replay(ReplayConfig()).run(fr, b)
            fr.close()
            times.append(perf() - t0)
        ops = r.frames
        out["checksum"] = f"{r.checksum:016x}"
    else:
        raise SystemExit(f"unknown stage {stage}")

    st = sorted(times)
    out.update({
        "operations": ops, "best_seconds": st[0], "median_seconds": st[len(st) // 2],
        "worst_seconds": st[-1], "ops_per_sec_best": ops / st[0] if st[0] else 0,
        "ops_per_sec_median": ops / st[len(st) // 2] if st[len(st) // 2] else 0,
        "latency_per_op": pcts(lat), "latency_samples": len(lat),
        "peak_rss_bytes": peak_rss_bytes(),
    })
    ov, res = timer_stats()
    out["timer_overhead_ns"] = ov
    out["timer_resolution_ns"] = res
    return out


def cmd_bench(a) -> int:
    out = run_stage(a.stage, a.file, a.symbol.upper(), a.locate, a.repeat, a.lat_limit, a.limit)
    s = json.dumps(out, indent=1)
    if a.json:
        with open(a.json, "w") as f:
            f.write(s)
    print(s)
    return 0


def register(sub):
    b = sub.add_parser("bench", help="Python reference performance (one stage per process)")
    b.add_argument("file")
    b.add_argument("--symbol", required=True)
    b.add_argument("--locate", type=int)
    b.add_argument("--stage", choices=["frame", "parse", "book", "replay", "e2e"], required=True)
    b.add_argument("--repeat", type=int, default=3)
    b.add_argument("--lat-limit", type=int, default=1_000_000)
    b.add_argument("--limit", type=int)
    b.add_argument("--json")
    b.set_defaults(fn=cmd_bench)
