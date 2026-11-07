"""Deterministic replay engine.

Determinism contract: the book state after N frames is a pure function of the first
N frames of the input. Pacing, pausing, snapshots and hooks never feed back into the
book, and frames are applied strictly in file order (no sorting by timestamp, so
events with identical timestamps keep their wire order).
"""
from __future__ import annotations

import json
import struct
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Optional

from .book import OrderBook
from .checks import verify_book
from .messages import (DECODERS, MSG_LEN, MSG_NAME, T_SYSTEM, T_TRADING, fmt_time)

perf = time.perf_counter


class ReplayStartError(RuntimeError):
    pass


@dataclass
class ReplayConfig:
    speed: Optional[float] = None         # None: as fast as possible. 1.0: historical speed.
    snapshot_every_ns: Optional[int] = None  # event-time interval, aligned to multiples
    snapshot_depth: int = 5
    checkpoint_every: int = 0             # record (frame_no, checksum) every N frames
    check_every: int = 0                  # run verify_book every N frames (0 = never)
    final_check: bool = True
    control_poll: int = 4096              # frames between pause/stop polls in fast mode


class ReplayController:
    """Thread-safe handle for pause / resume / stop / speed changes during a replay."""

    def __init__(self, speed: Optional[float] = None):
        self._run = threading.Event()
        self._run.set()
        self._stop = False
        self.speed = speed

    def pause(self):
        self._run.clear()

    def resume(self):
        self._run.set()

    def stop(self):
        self._stop = True
        self._run.set()

    def set_speed(self, speed: Optional[float]):
        self.speed = speed

    @property
    def paused(self) -> bool:
        return not self._run.is_set()


class _Pacer:
    def __init__(self, ctl: ReplayController):
        self.ctl = ctl
        self.wall0 = 0.0
        self.ts0 = 0
        self.sp = None
        self.rebase = True
        self.paused_s = 0.0

    def tick(self, ts: int) -> bool:
        """Honour pause/stop/speed before releasing the event at `ts`.

        Returns True when the replay should stop. Waits are sliced (<= 50 ms) so a pause,
        stop or speed change takes effect promptly even across a long gap in the data.
        """
        ctl = self.ctl
        while True:
            if not ctl._run.is_set():
                t0 = perf()
                ctl._run.wait()
                self.paused_s += perf() - t0
                self.rebase = True
            if ctl._stop:
                return True
            sp = ctl.speed
            if sp is None:
                self.rebase = True
                return False
            if self.rebase or sp != self.sp:
                self.wall0 = perf()
                self.ts0 = ts
                self.sp = sp
                self.rebase = False
            delay = self.wall0 + (ts - self.ts0) / (1e9 * sp) - perf()
            if delay <= 0.0002:
                return False
            time.sleep(delay if delay < 0.05 else 0.05)


@dataclass
class ReplayResult:
    frames: int = 0                       # frames consumed (including start_seq)
    wall_seconds: float = 0.0
    paused_seconds: float = 0.0
    first_ts: Optional[int] = None
    last_ts: Optional[int] = None
    checksum: int = 0
    counters: Dict[str, int] = field(default_factory=dict)
    type_counts: Dict[str, int] = field(default_factory=dict)
    snapshots: List[dict] = field(default_factory=list)
    checkpoints: List[tuple] = field(default_factory=list)
    system_events: List[tuple] = field(default_factory=list)
    trading_events: List[tuple] = field(default_factory=list)
    invariant_errors: List[str] = field(default_factory=list)
    session_start_seen: bool = False
    stopped_early: bool = False
    final_snapshot: Optional[dict] = None

    @property
    def msgs_per_sec(self) -> float:
        return self.frames / self.wall_seconds if self.wall_seconds > 0 else 0.0


class Replay:
    def __init__(self, cfg: Optional[ReplayConfig] = None,
                 controller: Optional[ReplayController] = None,
                 pre_hook: Optional[Callable[[int], None]] = None,
                 post_hook: Optional[Callable[[int], None]] = None,
                 on_snapshot: Optional[Callable[[dict], None]] = None):
        self.cfg = cfg or ReplayConfig()
        self.ctl = controller
        if self.ctl is None and self.cfg.speed is not None:
            self.ctl = ReplayController(self.cfg.speed)
        elif self.ctl is not None and self.cfg.speed is not None and self.ctl.speed is None:
            self.ctl.speed = self.cfg.speed
        self.pre_hook = pre_hook      # called with ts before each decoded event is applied
        self.post_hook = post_hook    # called with ts after each decoded event is applied
        self.on_snapshot = on_snapshot

    def run(self, frames: Iterable[bytes], book: OrderBook, start_seq: int = 0) -> ReplayResult:
        cfg = self.cfg
        if start_seq and not book.orders and not book.ev_count:
            raise ReplayStartError(
                "refusing to replay from the middle of a session with an empty book; "
                "start from the beginning or restore a snapshot (load_snapshot)")

        res = ReplayResult()
        decoders = DECODERS
        lens = MSG_LEN
        apply = book.apply
        loc = book.locate
        pre = self.pre_hook
        post = self.post_hook
        ctl = self.ctl
        pacer = _Pacer(ctl) if ctl is not None else None
        poll_mask = max(1, cfg.control_poll) - 1 if (cfg.control_poll & (cfg.control_poll - 1)) == 0 else 4095
        snap_iv = cfg.snapshot_every_ns
        next_snap: Optional[int] = None
        ck_every = cfg.checkpoint_every
        verify_every = cfg.check_every
        types: Dict[int, int] = {}
        malformed = unknown_type = loc_mismatch = informational = 0
        n = start_seq
        first_ts = None
        ts = 0
        t0 = perf()

        for m in frames:
            n += 1
            # Checkpoints are keyed on frame count (any message type) so that independent
            # implementations that count frames the same way produce the same series.
            # The state after frame n-1 is recorded just before frame n is processed.
            if (ck_every or verify_every) and n - 1 > start_seq:
                p = n - 1
                if ck_every and p % ck_every == 0:
                    res.checkpoints.append((p, book.checksum()))
                if verify_every and p % verify_every == 0:
                    errs = verify_book(book)
                    if errs:
                        book.c["invariant_failures"] += len(errs)
                        res.invariant_errors.extend(f"frame {p}: {e}" for e in errs)
            if not m:
                malformed += 1
                continue
            t = m[0]
            types[t] = types.get(t, 0) + 1
            dec = decoders.get(t)
            if dec is None:
                exp = lens.get(t)
                if exp is None:
                    unknown_type += 1
                elif len(m) != exp:
                    malformed += 1
                else:
                    informational += 1
                continue
            try:
                ev = dec(m)
            except struct.error:
                malformed += 1
                continue
            ts = ev[1]
            if first_ts is None:
                first_ts = ts
                if t == T_SYSTEM and ev[3] == 79 and start_seq == 0:   # 'O'
                    res.session_start_seen = True
                if snap_iv:
                    next_snap = (ts // snap_iv + 1) * snap_iv
            if pacer is not None and (ctl.speed is not None or not (n & poll_mask)):
                if pacer.tick(ts):
                    res.stopped_early = True
                    n -= 1
                    break

            if t == T_SYSTEM:
                if len(res.system_events) < 100:
                    res.system_events.append((n, ts, chr(ev[3])))
                continue
            if t == T_TRADING:
                if len(res.trading_events) < 200:
                    res.trading_events.append((n, ts, ev[3].decode("ascii").strip(), chr(ev[4])))
                continue
            if loc is not None and ev[2] != loc:
                loc_mismatch += 1
                continue
            if t not in _BOOK_T:
                continue

            while next_snap is not None and ts >= next_snap:
                snap = book.snapshot(cfg.snapshot_depth)
                snap["boundary_ts"] = next_snap
                snap["frame"] = n - 1
                snap["checksum"] = f"{book.checksum():016x}"
                res.snapshots.append(snap)
                if self.on_snapshot:
                    self.on_snapshot(snap)
                next_snap += snap_iv

            if pre is not None:
                pre(ts)
            apply(ev)
            if post is not None:
                post(ts)

        if ck_every and n > start_seq and n % ck_every == 0:
            res.checkpoints.append((n, book.checksum()))

        res.wall_seconds = perf() - t0
        if pacer is not None:
            res.paused_seconds = pacer.paused_s
            res.wall_seconds -= pacer.paused_s
        book.finalize()
        res.frames = n
        res.first_ts = first_ts
        res.last_ts = ts if first_ts is not None else None
        if cfg.final_check:
            errs = verify_book(book)
            if errs:
                book.c["invariant_failures"] += len(errs)
                res.invariant_errors.extend(f"final: {e}" for e in errs)
        res.checksum = book.checksum()
        res.final_snapshot = book.snapshot(cfg.snapshot_depth)
        rc = {"frames": n - start_seq, "malformed_messages": malformed,
              "unknown_message_type": unknown_type, "locate_mismatch": loc_mismatch,
              "informational_messages": informational}
        src = getattr(frames, "bad_frames", None)
        res.counters = dict(book.c)
        res.counters.update(rc)
        if src is not None:
            res.counters["bad_frames"] = frames.bad_frames
            res.counters["truncated_tail_bytes"] = frames.truncated_tail
        res.type_counts = {MSG_NAME.get(k, chr(k)): v for k, v in sorted(types.items())}
        return res


_BOOK_T = frozenset((65, 70, 69, 67, 88, 68, 85))


# ----------------------------------------------------------------------------- snapshots
def save_snapshot(path: str, book: OrderBook, frame_seq: int, source: str = "",
                  locates: Optional[Iterable[int]] = None) -> dict:
    """Persist a full restorable book state plus the stream position it corresponds to."""
    meta = {"frame_seq": frame_seq, "source": source,
            "locates": sorted(locates) if locates is not None else None,
            "checksum": f"{book.checksum():016x}", "ts": book.last_ts,
            "ts_text": fmt_time(book.last_ts)}
    with open(path, "w") as f:
        json.dump({"meta": meta, "state": book.to_state()}, f)
    return meta


def load_snapshot(path: str, **book_kw):
    """Restore (book, meta). Refuses snapshots whose recomputed checksum differs."""
    with open(path) as f:
        d = json.load(f)
    book = OrderBook.from_state(d["state"], **book_kw)
    if f"{book.checksum():016x}" != d["meta"]["checksum"]:
        raise ReplayStartError("snapshot checksum mismatch: file is corrupt or not canonical")
    errs = verify_book(book)
    if errs:
        raise ReplayStartError("snapshot violates book invariants: " + "; ".join(errs[:3]))
    return book, d["meta"]


# ----------------------------------------------------------------------------- convenience
def replay_file(path: str, symbol: str, locate: Optional[int] = None,
                cfg: Optional[ReplayConfig] = None, controller: Optional[ReplayController] = None,
                debug: bool = False, resume: Optional[str] = None, limit: Optional[int] = None,
                pre_hook=None, post_hook=None, on_snapshot=None, observer=None):
    """Replay `path` (plain/.gz ITCH file) for one symbol from the start of the file,
    or from a restored snapshot. Returns (book, result)."""
    from .framing import FrameReader, find_locates   # local import: avoid cycles

    if locate is None:
        locate = find_locates(path, [symbol])[symbol.upper()]
    skip = 0
    if resume:
        book, meta = load_snapshot(resume, debug=debug)
        if meta.get("locates") and locate not in meta["locates"]:
            raise ReplayStartError("snapshot was taken with a different locate filter")
        skip = meta["frame_seq"]
    else:
        book = OrderBook(symbol.upper(), locate, debug=debug)
    book.observer = observer
    reader = FrameReader(path, locates={locate}, skip=skip)
    frames: Iterable[bytes] = reader
    if limit:
        import itertools
        frames = itertools.islice(reader, max(0, limit - skip))
    rep = Replay(cfg, controller, pre_hook, post_hook, on_snapshot)
    try:
        res = rep.run(frames if limit else reader, book, start_seq=skip)
    finally:
        reader.close()
    if not limit:
        res.counters["bad_frames"] = reader.bad_frames
        res.counters["truncated_tail_bytes"] = reader.truncated_tail
    return book, res


def result_to_json(book: OrderBook, res: ReplayResult, impl: str = "python") -> dict:
    """Same schema as the C++ `replay` command so the two can be diffed key-by-key."""
    inv = {v: k for k, v in MSG_NAME.items()}
    return {
        "impl": impl, "frames": res.frames, "checksum": f"{res.checksum:016x}",
        "orders": len(book.orders), "wall_seconds": res.wall_seconds,
        "msgs_per_sec": res.msgs_per_sec, "first_ts": res.first_ts, "last_ts": res.last_ts,
        "session_start_seen": res.session_start_seen,
        "counters": res.counters,
        "types": {chr(inv[k]): v for k, v in res.type_counts.items()},
        "checkpoints": [[n, f"{h:016x}"] for n, h in res.checkpoints],
    }
