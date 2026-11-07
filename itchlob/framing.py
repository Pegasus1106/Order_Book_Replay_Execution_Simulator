"""Binary framing for Nasdaq ITCH 5.0 BinaryFILE streams and symbol lookup.

Frame = 2-byte big-endian length + message body. The reader streams from a plain
file, a .gz file, or stdin, and optionally filters by stock-locate code *without
decoding*. Filtering by locate is exact for a per-symbol book because every
book-affecting message (A, F, E, C, X, D, U) carries the locate of its symbol in
the common header, so no cross-symbol state is ever needed.
"""
from __future__ import annotations

import gzip
import sys
from typing import BinaryIO, Dict, Iterable, Iterator, List, Optional, Set

from .messages import T_ADD, T_ADD_MPID, T_DIRECTORY, T_SYSTEM, MSG_LEN


def open_source(path: str) -> BinaryIO:
    if path == "-":
        return sys.stdin.buffer
    if path.endswith(".gz"):
        return gzip.open(path, "rb")
    return open(path, "rb")


class FrameReader:
    """Iterate message bodies (bytes) in file order.

    locates      keep only these stock-locate codes (plus system events, type 'S').
                 None keeps everything.
    skip         discard this many *kept* frames first (used to resume after a snapshot).
    Counters: frames_scanned, frames_yielded, bad_frames, truncated_tail, bytes_read.
    """

    def __init__(self, source, locates: Optional[Iterable[int]] = None, skip: int = 0,
                 chunk_size: int = 1 << 24):
        self.path = source if isinstance(source, str) else None
        self._f = open_source(source) if isinstance(source, str) else source
        self._own = isinstance(source, str)
        self.locates: Optional[Set[int]] = set(locates) if locates is not None else None
        self.skip = skip
        self.chunk_size = chunk_size
        self.frames_scanned = 0
        self.frames_yielded = 0
        self.bad_frames = 0
        self.truncated_tail = 0
        self.bytes_read = 0

    def close(self) -> None:
        if self._own:
            self._f.close()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()

    def __iter__(self) -> Iterator[bytes]:
        read = self._f.read
        locs = self.locates
        skip = self.skip
        chunk = self.chunk_size
        buf = b""
        scanned = yielded = bad = 0
        while True:
            data = read(chunk)
            if not data:
                break
            self.bytes_read += len(data)
            buf = buf + data if buf else data
            pos = 0
            end = len(buf)
            while pos + 2 <= end:
                n = (buf[pos] << 8) | buf[pos + 1]
                nxt = pos + 2 + n
                if nxt > end:
                    break
                scanned += 1
                if locs is None:
                    keep = True
                elif n >= 3:
                    keep = ((buf[pos + 3] << 8) | buf[pos + 4]) in locs or buf[pos + 2] == T_SYSTEM
                else:
                    keep = False
                    bad += 1
                if keep:
                    if skip:
                        skip -= 1
                    else:
                        yielded += 1
                        yield buf[pos + 2:nxt]
                pos = nxt
            buf = buf[pos:]
            self.frames_scanned = scanned
            self.frames_yielded = yielded
            self.bad_frames = bad
        self.frames_scanned = scanned
        self.frames_yielded = yielded
        self.bad_frames = bad
        if buf:
            self.truncated_tail = len(buf)


def find_locates(path: str, symbols: Iterable[str], max_frames: int = 2_000_000) -> Dict[str, int]:
    """Resolve symbols to stock-locate codes from the Stock Directory ('R') messages.

    Directory messages are sent at the start of the day, before any order traffic, so
    the scan stops at the first Add Order. Raises KeyError for symbols not found.
    """
    want = {s.upper().ljust(8).encode("ascii"): s.upper() for s in symbols}
    found: Dict[str, int] = {}
    with FrameReader(path) as fr:
        for i, m in enumerate(fr):
            t = m[0]
            if t == T_DIRECTORY and len(m) >= 19:
                stock = bytes(m[11:19])
                if stock in want:
                    found[want[stock]] = (m[1] << 8) | m[2]
                    if len(found) == len(want):
                        break
            elif t in (T_ADD, T_ADD_MPID) or i >= max_frames:
                break
    missing = [s for s in want.values() if s not in found]
    if missing:
        raise KeyError(f"symbols not found in stock directory: {missing}")
    return found


def list_directory(path: str, max_frames: int = 2_000_000) -> Dict[int, str]:
    """locate -> symbol for every Stock Directory message at the start of the file."""
    out: Dict[int, str] = {}
    with FrameReader(path) as fr:
        for i, m in enumerate(fr):
            t = m[0]
            if t == T_DIRECTORY and len(m) >= 19:
                out[(m[1] << 8) | m[2]] = bytes(m[11:19]).decode("ascii").strip()
            elif t in (T_ADD, T_ADD_MPID) or i >= max_frames:
                break
    return out


def extract_slices(path: str, targets: Dict[int, str]) -> Dict[int, int]:
    """One pass over `path`, writing {locate: out_path} slices in the same framing.

    Each slice keeps every message for that locate and every System Event ('S'),
    in original order. Returns frames written per locate.
    """
    outs: Dict[int, BinaryIO] = {loc: open(p, "wb") for loc, p in targets.items()}
    counts = {loc: 0 for loc in targets}
    try:
        with FrameReader(path, locates=set(targets)) as fr:
            for m in fr:
                frame = len(m).to_bytes(2, "big") + m
                if m[0] == T_SYSTEM:
                    for loc, f in outs.items():
                        f.write(frame)
                        counts[loc] += 1
                else:
                    loc = (m[1] << 8) | m[2]
                    outs[loc].write(frame)
                    counts[loc] += 1
    finally:
        for f in outs.values():
            f.close()
    return counts


def scan_stats(path: str, limit: Optional[int] = None) -> Dict[str, object]:
    """Full-file framing scan: message counts by type and by locate (no decoding)."""
    by_type: Dict[int, int] = {}
    by_loc: Dict[int, int] = {}
    bad = unknown = 0
    n = 0
    with FrameReader(path) as fr:
        for m in fr:
            n += 1
            t = m[0] if m else 0
            by_type[t] = by_type.get(t, 0) + 1
            exp = MSG_LEN.get(t)
            if exp is None:
                unknown += 1
            elif len(m) != exp:
                bad += 1
            if len(m) >= 3 and t in (65, 70, 69, 67, 88, 68, 85):
                loc = (m[1] << 8) | m[2]
                by_loc[loc] = by_loc.get(loc, 0) + 1
            if limit and n >= limit:
                break
        trunc = fr.truncated_tail
    return {"frames": n, "by_type": by_type, "by_locate": by_loc, "bad_length": bad,
            "unknown_type": unknown, "truncated_tail": trunc}
