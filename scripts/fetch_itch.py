#!/usr/bin/env python3
"""Segmented, resumable downloader for Nasdaq's public ITCH sample files.

The server throttles single connections, so the file is fetched as N byte ranges
in parallel, each appended to its own part file (safe to re-run: finished parts
are skipped and partial parts resume), then concatenated and checked with
`gzip -t` (CRC32 of the whole stream).

    python3 scripts/fetch_itch.py 12302019.NASDAQ_ITCH50.gz
"""
import argparse
import concurrent.futures as cf
import os
import subprocess
import sys
import urllib.request

BASE = "https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/"


def remote_size(url):
    req = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(req, timeout=60) as r:
        return int(r.headers["Content-Length"])


def fetch_part(url, path, start, end, retries=40):
    expected = end - start + 1
    for _ in range(retries):
        have = os.path.getsize(path) if os.path.exists(path) else 0
        if have == expected:
            return True
        if have > expected:
            os.remove(path)
            have = 0
        with open(path, "ab") as out:
            subprocess.run(
                ["curl", "-s", "--max-time", "600", "-r", f"{start + have}-{end}", url],
                stdout=out,
                check=False,
            )
    return os.path.getsize(path) == expected


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("name", help="file name in the Nasdaq ITCH directory")
    ap.add_argument("--dest", default="data")
    ap.add_argument("--jobs", type=int, default=12)
    ap.add_argument("--part-mb", type=int, default=64)
    a = ap.parse_args()

    url = BASE + a.name
    os.makedirs(a.dest, exist_ok=True)
    final = os.path.join(a.dest, a.name)
    total = remote_size(url)
    if os.path.exists(final) and os.path.getsize(final) == total:
        print("already complete:", final)
        return 0

    step = a.part_mb * 1024 * 1024
    parts = []
    for i, s in enumerate(range(0, total, step)):
        parts.append((f"{final}.part{i:04d}", s, min(s + step, total) - 1))
    print(f"{total/1e9:.2f} GB in {len(parts)} parts, {a.jobs} connections", flush=True)

    done = 0
    with cf.ThreadPoolExecutor(a.jobs) as ex:
        futs = {ex.submit(fetch_part, url, p, s, e): p for p, s, e in parts}
        for f in cf.as_completed(futs):
            if not f.result():
                print("FAILED part", futs[f], file=sys.stderr)
                return 1
            done += 1
            if done % 4 == 0 or done == len(parts):
                print(f"  {done}/{len(parts)} parts", flush=True)

    with open(final + ".tmp", "wb") as out:
        for p, _, _ in parts:
            with open(p, "rb") as f:
                while True:
                    b = f.read(16 << 20)
                    if not b:
                        break
                    out.write(b)
    os.replace(final + ".tmp", final)
    for p, _, _ in parts:
        os.remove(p)
    print("verifying gzip CRC ...", flush=True)
    rc = subprocess.run(["gzip", "-t", final]).returncode
    print("gzip -t:", "OK" if rc == 0 else f"FAILED ({rc})")
    return rc


if __name__ == "__main__":
    sys.exit(main())
