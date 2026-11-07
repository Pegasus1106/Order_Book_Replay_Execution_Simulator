#!/usr/bin/env python3
"""Full-file framing scan with the Python reader: counts by type / locate, and time."""
import json
import sys
import time

sys.path.insert(0, ".")
from itchlob.framing import list_directory, scan_stats  # noqa: E402
from itchlob.messages import MSG_NAME  # noqa: E402

path = sys.argv[1]
out = sys.argv[2]
t0 = time.perf_counter()
directory = list_directory(path)
st = scan_stats(path)
dt = time.perf_counter() - t0
st["by_type"] = {MSG_NAME.get(k, chr(k)): v for k, v in st["by_type"].items()}
st["seconds"] = dt
st["frames_per_sec"] = st["frames"] / dt
top = sorted(st["by_locate"].items(), key=lambda kv: -kv[1])[:60]
st["top_symbols"] = [(directory.get(loc, "?"), loc, n) for loc, n in top]
st["n_symbols_in_directory"] = len(directory)
del st["by_locate"]
json.dump(st, open(out, "w"), indent=1)
print(json.dumps({k: v for k, v in st.items() if k != "top_symbols"}, indent=1))
for s in st["top_symbols"][:30]:
    print(s)
