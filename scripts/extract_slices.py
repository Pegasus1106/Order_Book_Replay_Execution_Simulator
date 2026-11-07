#!/usr/bin/env python3
"""Extract per-symbol slices (same ITCH framing) from a full-day file in ONE pass.

Every message for the symbol's stock-locate is kept in original order, plus the
system-event messages, so the slice starts at 'Start of Messages' and the book can be
rebuilt from the true beginning of the session. Nothing is re-ordered or edited.
"""
import argparse
import hashlib
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from itchlob.framing import extract_slices, find_locates  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("source")
ap.add_argument("--symbols", required=True)
ap.add_argument("--outdir", default="data/slices")
ap.add_argument("--tag", default="12302019")
a = ap.parse_args()

syms = [s.strip().upper() for s in a.symbols.split(",")]
os.makedirs(a.outdir, exist_ok=True)
locs = find_locates(a.source, syms)
targets = {loc: os.path.join(a.outdir, f"{s}_{a.tag}.itch") for s, loc in locs.items()}
t0 = time.perf_counter()
counts = extract_slices(a.source, targets)
dt = time.perf_counter() - t0
manifest = {}
for s, loc in locs.items():
    p = targets[loc]
    h = hashlib.sha256(open(p, "rb").read()).hexdigest()
    manifest[s] = {"locate": loc, "path": p, "frames": counts[loc], "bytes": os.path.getsize(p),
                   "sha256": h}
json.dump({"source": a.source, "seconds": dt, "slices": manifest},
          open(os.path.join(a.outdir, f"manifest_{a.tag}.json"), "w"), indent=1)
print(json.dumps(manifest, indent=1), f"\n{dt:.1f}s")
