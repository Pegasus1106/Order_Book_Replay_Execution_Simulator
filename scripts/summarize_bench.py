#!/usr/bin/env python3
"""Turn results/bench/*.json into results/BENCHMARKS.md."""
import glob
import json
import os
import platform
import subprocess

OUT = "results/bench"


def j(path):
    with open(path) as f:
        return json.load(f)


def mb(x):
    return f"{x / 1e6:,.0f}"


def num(x, nd=0):
    return "-" if x is None else f"{x:,.{nd}f}"


def lat(d):
    l = d.get("latency_per_op") or {}
    if not l:
        return "-", "-", "-"
    return num(l["p50_ns"]), num(l["p95_ns"]), num(l["p99_ns"])


def cpu():
    try:
        return subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True,
                              text=True).stdout.strip()
    except Exception:
        return platform.processor()


lines = []
lines.append("# Performance measurements\n")
lines.append(f"Machine: {cpu()}, {os.cpu_count()} logical cores, {platform.platform()}, "
             f"Python {platform.python_version()}.\n")
lines.append("**What is measured.** Single-thread, in-process processing time per message on this laptop, "
             "input already in the page cache. This is *not* exchange-to-trader or network latency, and "
             "it says nothing about a production feed handler's latency under load. Per-message "
             "percentiles come from a separate pass that timestamps every call (timer overhead is "
             "listed and is included in the numbers); throughput comes from passes with no timers.\n")
stage_names = {"frame": "framing only", "parse": "parser-only", "book": "book-update-only",
               "replay": "complete replay (in memory)", "e2e": "end-to-end (file to final state)"}

for sym in sorted({os.path.basename(p).split("_")[1] for p in glob.glob(f"{OUT}/py_*_*.json")}):
    lines.append(f"\n## {sym} slice\n")
    rows = []
    for stage in ("frame", "parse", "book", "replay", "e2e"):
        p = f"{OUT}/py_{sym}_{stage}.json"
        if os.path.exists(p):
            d = j(p)
            p50, p95, p99 = lat(d)
            rows.append(("Python", stage_names[stage], num(d["operations"]), num(d["ops_per_sec_best"]),
                         num(d["ops_per_sec_median"]), p50, p95, p99, mb(d["peak_rss_bytes"])))
    for mode, stage in (("parse", "parse"), ("book", "book"), ("full", "replay")):
        p = f"{OUT}/cpp_{sym}_{mode}.json"
        if os.path.exists(p):
            d = j(p)
            p50, p95, p99 = lat(d)
            rows.append(("C++", stage_names[stage], num(d["operations"]), num(d["ops_per_sec_best"]),
                         num(d["ops_per_sec_median"]), p50, p95, p99, mb(d["peak_rss_bytes"])))
    p = f"{OUT}/cpp_{sym}_e2e.json"
    if os.path.exists(p):
        d = j(p)
        rows.append(("C++", stage_names["e2e"], num(d["frames"]), num(d["msgs_per_sec"]), num(d["msgs_per_sec"]),
                     "-", "-", "-", mb(d["peak_rss_bytes"])))
    lines.append("| impl | stage | messages | msgs/s (best) | msgs/s (median) | p50 ns | p95 ns | p99 ns | peak RSS MB |")
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        lines.append("| " + " | ".join(r) + " |")
    lines.append("")
    # batched C++ latency (sub-timer-tick resolution)
    for mode in ("parse", "book", "full"):
        p = f"{OUT}/cpp_{sym}_{mode}.json"
        if os.path.exists(p):
            d = j(p)
            b = d.get("latency_batch64_mean_per_op")
            if b:
                lines.append(f"- C++ {mode}: per-op timer resolution {d['timer_resolution_ns']} ns "
                             f"(per-op percentiles above are quantized to it); mean cost of 64-message batches: "
                             f"p50 {b['p50_ns']:.0f} / p95 {b['p95_ns']:.0f} / p99 {b['p99_ns']:.0f} ns per message.")
    py_full = f"{OUT}/py_{sym}_replay.json"
    cp_full = f"{OUT}/cpp_{sym}_full.json"
    if os.path.exists(py_full) and os.path.exists(cp_full):
        a, b = j(py_full), j(cp_full)
        lines.append(f"- Complete replay speed-up C++ vs Python: x{b['ops_per_sec_best'] / a['ops_per_sec_best']:.0f}.")

ov = []
for impl, pat in (("Python", "py_*_replay.json"), ("C++", "cpp_*_full.json")):
    fs = sorted(glob.glob(f"{OUT}/{pat}"))
    if fs:
        d = j(fs[0])
        ov.append(f"{impl} {d['timer_overhead_ns']:.0f} ns (resolution {d['timer_resolution_ns']} ns)")
lines.append("\nTimer overhead per clock read: " + "; ".join(ov) + ". RSS of the staged runs includes the "
             "pre-loaded input messages (Python keeps one bytes object per message), so only the "
             "end-to-end rows show the footprint of a streaming replay.")

p = f"{OUT}/cpp_fullday_scan.json"
if os.path.exists(p):
    d = j(p)
    lines.append(f"\n## Whole-day file scan (C++)\n\n{d['frames']:,} frames, gunzip + framing + length check of every "
                 f"message in {d['seconds_incl_gunzip']:.1f} s ({d['frames_per_sec']:,.0f} frames/s), "
                 f"{d['bad_length']} bad lengths, {d['unknown_type']} unknown types, peak RSS {mb(d['peak_rss_bytes'])} MB.")
p = "results/full_scan_python.json"
if os.path.exists(p):
    d = j(p)
    lines.append(f"\nPython reader on the same file (gunzip + framing + per-type/locate counting): "
                 f"{d['frames']:,} frames in {d['seconds']:.0f} s ({d['frames_per_sec']:,.0f} frames/s).")

with open("results/BENCHMARKS.md", "w") as f:
    f.write("\n".join(lines) + "\n")
print("\n".join(lines))
