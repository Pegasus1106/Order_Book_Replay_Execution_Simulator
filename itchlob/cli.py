"""Command line entry point:  python3 -m itchlob <command> ...

    replay        replay one symbol (fast / scaled speed, snapshots, checkpoints, diagnostics)
    determinism   replay twice (and via snapshot-resume) and require identical state
    compare       replay with Python and C++ and require identical state at every checkpoint
    fuzz          synthetic streams with injected anomalies, Python vs C++
    bench         parser-only / book-only / full-replay performance of the Python reference
    marketable    marketable-order simulation: VWAP, slippage, unfilled quantity, fees
    queue         passive probe orders compared across queue-position assumptions
    strategy      one strategy scenario (fixed or inventory_aware quoting)
    grid          parallel scenario grid: symbols x rules x queue models x latencies
    report, plot  tables / SVG from grid and strategy results
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time

from .book import OrderBook
from .messages import fmt_price, fmt_time
from .replay import (Replay, ReplayConfig, ReplayController, load_snapshot, replay_file,
                     result_to_json, save_snapshot)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CPP_BIN = os.path.join(ROOT, "cpp", "itch_replay")


def _dump_state(book: OrderBook, path: str) -> None:
    with open(path, "w") as f:
        f.write("\n".join(book.dump_lines()) + "\n")


def _interactive(ctl: ReplayController) -> None:
    def loop():
        print("controls: p=pause  r=resume  s <x>=set speed (0=max)  q=stop", file=sys.stderr)
        for line in sys.stdin:
            w = line.split()
            if not w:
                continue
            if w[0] == "p":
                ctl.pause()
            elif w[0] == "r":
                ctl.resume()
            elif w[0] == "s" and len(w) > 1:
                v = float(w[1])
                ctl.set_speed(v if v > 0 else None)
            elif w[0] == "q":
                ctl.stop()
                return
    threading.Thread(target=loop, daemon=True).start()


def cmd_replay(a) -> int:
    cfg = ReplayConfig(
        speed=a.speed, snapshot_every_ns=int(a.snapshot_every * 1e9) if a.snapshot_every else None,
        snapshot_depth=a.depth, checkpoint_every=a.checkpoint_every, check_every=a.check_every)
    ctl = ReplayController(a.speed) if (a.speed or a.interactive) else None
    if a.interactive and ctl:
        _interactive(ctl)
    snaps_f = open(a.snapshots_out, "w") if a.snapshots_out else None

    def on_snap(s):
        if snaps_f:
            snaps_f.write(json.dumps(s) + "\n")

    book, res = replay_file(a.file, a.symbol, a.locate, cfg, ctl, debug=a.debug, resume=a.resume,
                            limit=a.limit, on_snapshot=on_snap)
    if snaps_f:
        snaps_f.close()
    if a.save_snapshot:
        meta = save_snapshot(a.save_snapshot, book, res.frames, a.file, [book.locate])
        print("saved snapshot:", meta, file=sys.stderr)
    if a.dump_state:
        _dump_state(book, a.dump_state)
    out = result_to_json(book, res)
    out["invariant_errors"] = res.invariant_errors[:20]
    out["anomaly_samples"] = book.anomalies[:20]
    out["system_events"] = res.system_events
    out["trading_events"] = res.trading_events[:20]
    out["final_snapshot"] = res.final_snapshot
    out["snapshots_taken"] = len(res.snapshots)
    out["paused_seconds"] = res.paused_seconds
    out["stopped_early"] = res.stopped_early
    if a.json:
        with open(a.json, "w") as f:
            json.dump(out, f, indent=1)
    fs = res.final_snapshot
    print(f"{a.symbol} frames={res.frames:,} wall={res.wall_seconds:.2f}s "
          f"({res.msgs_per_sec:,.0f} frames/s) checksum={res.checksum:016x}")
    print(f"  session start seen: {res.session_start_seen}   first={fmt_time(res.first_ts or 0)} "
          f"last={fmt_time(res.last_ts or 0)}")
    print(f"  final book: bid {fmt_price(fs['best_bid']) if fs['best_bid'] else '-'} x "
          f"{fs['best_bid_size']}  ask {fmt_price(fs['best_ask']) if fs['best_ask'] else '-'} x "
          f"{fs['best_ask_size']}  orders={fs['orders']}")
    nz = {k: v for k, v in res.counters.items() if v}
    print("  counters:", json.dumps(nz))
    if res.invariant_errors:
        print("  INVARIANT ERRORS:", res.invariant_errors[:5])
    return 0 if not res.invariant_errors else 1


def _py_json(args_ns, path, symbol, locate, ck, limit=None):
    cfg = ReplayConfig(checkpoint_every=ck, check_every=0)
    book, res = replay_file(path, symbol, locate, cfg, limit=limit)
    return book, res, result_to_json(book, res)


def _cpp_run(path, symbol, locate, ck, dump, limit=None):
    cmd = [CPP_BIN, "replay", path, "--symbol", symbol, "--locate", str(locate),
           "--checkpoint-every", str(ck), "--dump-state", dump]
    if limit:
        cmd += ["--limit", str(limit)]
    out = subprocess.run(cmd, capture_output=True, text=True, check=True).stdout
    return json.loads(out)


def diff_results(py: dict, cpp: dict) -> list:
    """Compare state-bearing fields of the two implementations. [] means equivalent."""
    d = []
    for k in ("frames", "checksum", "orders", "first_ts", "last_ts", "session_start_seen"):
        if py[k] != cpp[k]:
            d.append(f"{k}: py={py[k]} cpp={cpp[k]}")
    for k in sorted(set(py["counters"]) | set(cpp["counters"])):
        if py["counters"].get(k, 0) != cpp["counters"].get(k, 0):
            d.append(f"counter {k}: py={py['counters'].get(k)} cpp={cpp['counters'].get(k)}")
    if py["types"] != cpp["types"]:
        d.append(f"type counts differ: py={py['types']} cpp={cpp['types']}")
    if py["checkpoints"] != cpp["checkpoints"]:
        bad = [(p, c) for p, c in zip(py["checkpoints"], cpp["checkpoints"]) if p != c]
        d.append(f"checkpoints differ ({len(bad)} of {len(py['checkpoints'])}); first: {bad[:1]}")
    return d


def cmd_compare(a) -> int:
    if not os.path.exists(CPP_BIN):
        print("build the C++ binary first: make -C cpp", file=sys.stderr)
        return 2
    tmp = a.tmpdir
    os.makedirs(tmp, exist_ok=True)
    ok = True
    for path, sym in zip(a.files, a.symbols.split(",")):
        from .framing import find_locates
        loc = find_locates(path, [sym])[sym]
        t0 = time.perf_counter()
        pbook, pres, pj = _py_json(a, path, sym, loc, a.checkpoint_every, a.limit)
        t1 = time.perf_counter()
        cj = _cpp_run(path, sym, loc, a.checkpoint_every, os.path.join(tmp, f"{sym}_cpp_state.txt"),
                      a.limit)
        t2 = time.perf_counter()
        _dump_state(pbook, os.path.join(tmp, f"{sym}_py_state.txt"))
        same_dump = subprocess.run(["cmp", "-s", os.path.join(tmp, f"{sym}_py_state.txt"),
                                    os.path.join(tmp, f"{sym}_cpp_state.txt")]).returncode == 0
        diffs = diff_results(pj, cj)
        if not same_dump:
            diffs.append("full state dump (every level, every order in FIFO order) differs")
        status = "EQUIVALENT" if not diffs else "MISMATCH"
        ok &= not diffs
        print(f"{sym}: {status}  frames={pj['frames']:,} checksum={pj['checksum']} "
              f"checkpoints={len(pj['checkpoints'])} orders={pj['orders']} "
              f"state_dump_identical={same_dump}  py={t1 - t0:.1f}s cpp={t2 - t1:.2f}s")
        for x in diffs[:10]:
            print("   ", x)
    return 0 if ok else 1


def cmd_determinism(a) -> int:
    ok = True
    cfg = ReplayConfig(checkpoint_every=a.checkpoint_every, final_check=True)
    runs = []
    for i in range(a.runs):
        book, res = replay_file(a.file, a.symbol, a.locate, cfg, debug=False, limit=a.limit)
        runs.append((res.checksum, tuple(res.checkpoints), tuple(sorted(res.counters.items()))))
    same = all(r == runs[0] for r in runs)
    print(f"{a.runs} identical-input replays -> same checksum, checkpoints and counters: {same}"
          f"  ({runs[0][0]:016x})")
    ok &= same
    # snapshot / resume at the midpoint must reproduce the full-run state
    mid = (a.limit or runs and 0) or 0
    book_full, res_full = replay_file(a.file, a.symbol, a.locate, ReplayConfig(), limit=a.limit)
    cut = max(1, res_full.frames // 2)
    book1, res1 = replay_file(a.file, a.symbol, a.locate, ReplayConfig(), limit=cut)
    snap = os.path.join(a.tmpdir, f"{a.symbol}_mid_snapshot.json")
    os.makedirs(a.tmpdir, exist_ok=True)
    save_snapshot(snap, book1, res1.frames, a.file, [book1.locate])
    book2, res2 = replay_file(a.file, a.symbol, a.locate, ReplayConfig(), resume=snap, limit=a.limit)
    resumed_ok = (res2.checksum == res_full.checksum and
                  {k: v for k, v in res2.counters.items() if k in book_full.c} ==
                  {k: v for k, v in book_full.c.items()})
    print(f"restore snapshot at frame {cut:,} and resume -> final state equals uninterrupted run: "
          f"{resumed_ok}  ({res2.checksum:016x} vs {res_full.checksum:016x})")
    ok &= resumed_ok
    # starting mid-stream with an empty book must be refused
    from .replay import ReplayStartError
    from .framing import FrameReader
    try:
        Replay().run(FrameReader(a.file, locates={book_full.locate}, skip=cut), OrderBook(a.symbol, book_full.locate),
                     start_seq=cut)
        refused = False
    except ReplayStartError:
        refused = True
    print(f"replay from mid-stream with an empty book is refused: {refused}")
    ok &= refused
    # pacing must not change state
    book3, res3 = replay_file(a.file, a.symbol, a.locate,
                              ReplayConfig(speed=a.paced_speed), limit=a.paced_frames)
    book4, res4 = replay_file(a.file, a.symbol, a.locate, ReplayConfig(), limit=a.paced_frames)
    paced_ok = res3.checksum == res4.checksum
    print(f"first {a.paced_frames:,} frames at {a.paced_speed:g}x speed ({res3.wall_seconds:.2f}s wall) "
          f"vs as-fast-as-possible: same state: {paced_ok}")
    ok &= paced_ok
    return 0 if ok else 1


def cmd_fuzz(a) -> int:
    from .synth import gen_stream
    if not os.path.exists(CPP_BIN):
        print("build the C++ binary first: make -C cpp", file=sys.stderr)
        return 2
    os.makedirs(a.tmpdir, exist_ok=True)
    ok = True
    for seed in range(a.seeds):
        rate = a.anomaly_rate if seed % 2 else 0.0
        data = gen_stream(a.events, seed=seed, anomaly_rate=rate)
        path = os.path.join(a.tmpdir, f"fuzz_{seed}.itch")
        with open(path, "wb") as f:
            f.write(data)
        book, res, pj = _py_json(a, path, "TEST", 7, 10000)
        pj["orders"] = len(book.orders)
        cj = _cpp_run(path, "TEST", 7, 10000, os.path.join(a.tmpdir, f"fuzz_{seed}_cpp.txt"))
        _dump_state(book, os.path.join(a.tmpdir, f"fuzz_{seed}_py.txt"))
        same = subprocess.run(["cmp", "-s", os.path.join(a.tmpdir, f"fuzz_{seed}_py.txt"),
                               os.path.join(a.tmpdir, f"fuzz_{seed}_cpp.txt")]).returncode == 0
        diffs = diff_results(pj, cj)
        if not same:
            diffs.append("state dump differs")
        ok &= not diffs
        c = pj["counters"]
        print(f"seed {seed} anomalies={rate}: {'OK ' if not diffs else 'MISMATCH'} frames={pj['frames']:,} "
              f"unknown_ref={c['unknown_ref']} dup={c['duplicate_ref']} qty_inc={c['qty_inconsistency']} "
              f"malformed={c['malformed_messages']} crossed_ep={c['crossed_episodes']} "
              f"checksum={pj['checksum']}")
        for x in diffs[:5]:
            print("   ", x)
    return 0 if ok else 1


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="itchlob", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("replay")
    r.add_argument("file")
    r.add_argument("--symbol", required=True)
    r.add_argument("--locate", type=int)
    r.add_argument("--speed", type=float, help="historical speed multiple (omit: as fast as possible)")
    r.add_argument("--interactive", action="store_true", help="pause/resume/speed via stdin")
    r.add_argument("--snapshot-every", type=float, help="seconds of event time between snapshots")
    r.add_argument("--snapshots-out")
    r.add_argument("--depth", type=int, default=5)
    r.add_argument("--checkpoint-every", type=int, default=0)
    r.add_argument("--check-every", type=int, default=0, help="run full invariants every N frames")
    r.add_argument("--debug", action="store_true", help="per-removal lookup checks")
    r.add_argument("--limit", type=int)
    r.add_argument("--resume", help="restore a saved snapshot and continue after it")
    r.add_argument("--save-snapshot", help="save restorable state at the end of this run")
    r.add_argument("--dump-state")
    r.add_argument("--json")
    r.set_defaults(fn=cmd_replay)

    d = sub.add_parser("determinism")
    d.add_argument("file")
    d.add_argument("--symbol", required=True)
    d.add_argument("--locate", type=int)
    d.add_argument("--runs", type=int, default=3)
    d.add_argument("--limit", type=int)
    d.add_argument("--checkpoint-every", type=int, default=250000)
    d.add_argument("--paced-frames", type=int, default=20000)
    d.add_argument("--paced-speed", type=float, default=2.0e6)
    d.add_argument("--tmpdir", default="results/tmp")
    d.set_defaults(fn=cmd_determinism)

    c = sub.add_parser("compare")
    c.add_argument("files", nargs="+")
    c.add_argument("--symbols", required=True, help="comma list matching files")
    c.add_argument("--checkpoint-every", type=int, default=100000)
    c.add_argument("--limit", type=int)
    c.add_argument("--tmpdir", default="results/tmp")
    c.set_defaults(fn=cmd_compare)

    z = sub.add_parser("fuzz")
    z.add_argument("--seeds", type=int, default=8)
    z.add_argument("--events", type=int, default=200000)
    z.add_argument("--anomaly-rate", type=float, default=0.02)
    z.add_argument("--tmpdir", default="results/tmp")
    z.set_defaults(fn=cmd_fuzz)

    # bench / sim are registered by their own modules to keep this file small
    from . import bench as _bench
    from . import experiments as _exp
    _bench.register(sub)
    _exp.register(sub)

    a = p.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
