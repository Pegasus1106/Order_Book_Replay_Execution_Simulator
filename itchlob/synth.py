"""ITCH 5.0 message encoders and a seeded synthetic stream generator.

The generator produces a valid-looking single-symbol stream (with ties in timestamps,
partial executions, partial cancels, replaces that keep/change price, informational
messages) and, optionally, deliberate anomalies: unknown references, over-executions,
duplicate references, crossed/locked prices, zero quantities, truncated and unknown
messages. It is used to cross-check the Python and C++ implementations on inputs the
real feed rarely produces.
"""
from __future__ import annotations

import random
import struct
from typing import Dict, List, Optional

ST = struct.Struct


def _hdr(t: str, loc: int, ts: int) -> bytes:
    return t.encode() + struct.pack(">HH", loc, 0) + ts.to_bytes(6, "big")


def enc_system(ts: int, code: str, loc: int = 0) -> bytes:
    return _hdr("S", loc, ts) + code.encode()


def enc_directory(ts: int, loc: int, stock: str) -> bytes:
    body = stock.ljust(8).encode() + b"Q" + b"N" + struct.pack(">I", 100) + b"N" + b"C" + b"  " \
        + b"P" + b"N" + b"N" + b"1" + b"N" + struct.pack(">I", 0) + b"N"
    m = _hdr("R", loc, ts) + body
    assert len(m) == 39, len(m)
    return m


def enc_add(ts, loc, ref, side, shares, stock, price, mpid: Optional[str] = None) -> bytes:
    m = _hdr("F" if mpid else "A", loc, ts) + struct.pack(">QcI8sI", ref, side.encode(), shares,
                                                          stock.ljust(8).encode(), price)
    return m + mpid.ljust(4).encode() if mpid else m


def enc_exec(ts, loc, ref, shares, match=1) -> bytes:
    return _hdr("E", loc, ts) + struct.pack(">QIQ", ref, shares, match)


def enc_exec_px(ts, loc, ref, shares, price, match=1, printable="Y") -> bytes:
    return _hdr("C", loc, ts) + struct.pack(">QIQcI", ref, shares, match, printable.encode(), price)


def enc_cancel(ts, loc, ref, shares) -> bytes:
    return _hdr("X", loc, ts) + struct.pack(">QI", ref, shares)


def enc_delete(ts, loc, ref) -> bytes:
    return _hdr("D", loc, ts) + struct.pack(">Q", ref)


def enc_replace(ts, loc, old, new, shares, price) -> bytes:
    return _hdr("U", loc, ts) + struct.pack(">QQII", old, new, shares, price)


def enc_trade(ts, loc, shares, stock, price, match=1) -> bytes:
    return _hdr("P", loc, ts) + struct.pack(">QcI8sIQ", 0, b"B", shares, stock.ljust(8).encode(),
                                           price, match)


def enc_trading(ts, loc, stock, state="T") -> bytes:
    return _hdr("H", loc, ts) + stock.ljust(8).encode() + state.encode() + b" " + b"    "


def frame(m: bytes) -> bytes:
    return len(m).to_bytes(2, "big") + m


def gen_stream(n_events: int, seed: int = 1, locate: int = 7, symbol: str = "TEST",
               anomaly_rate: float = 0.0, other_locate_noise: float = 0.2,
               start_price: int = 1_000_000, tick: int = 100) -> bytes:
    """Return concatenated frames (BinaryFILE format) for ~n_events book events."""
    rnd = random.Random(seed)
    out: List[bytes] = []
    ts = 4 * 3600 * 10**9
    out.append(frame(enc_system(ts, "O")))
    out.append(frame(enc_directory(ts, locate, symbol)))
    out.append(frame(enc_directory(ts, locate + 1, "OTHER")))
    out.append(frame(enc_system(ts, "S")))
    out.append(frame(enc_system(ts + 10, "Q")))
    live: Dict[int, list] = {}          # ref -> [side, price, shares]
    refs: List[int] = []
    next_ref = 1000
    mid = start_price
    for i in range(n_events):
        if rnd.random() < 0.6:
            ts += rnd.choice((0, 0, 1, 5, 100, 5000))
        if rnd.random() < 0.002:
            mid += rnd.choice((-1, 1)) * tick * rnd.randint(1, 3)
        if rnd.random() < other_locate_noise:
            # traffic for another symbol: must be filtered out / ignored
            out.append(frame(enc_add(ts, locate + 1, 900000 + i, "B", 100, "OTHER", mid - tick)))
            continue
        r = rnd.random()
        an = rnd.random() < anomaly_rate
        if an:
            k = rnd.randint(0, 8)
            if k == 0:
                out.append(frame(enc_delete(ts, locate, 5_000_000 + i)))
            elif k == 1:
                out.append(frame(enc_exec(ts, locate, 5_000_000 + i, 10)))
            elif k == 2 and refs:
                ref = rnd.choice(refs)
                if ref in live:
                    out.append(frame(enc_exec(ts, locate, ref, live[ref][2] + rnd.randint(1, 50))))
            elif k == 3 and refs:
                ref = rnd.choice(refs)
                if ref in live:
                    out.append(frame(enc_add(ts, locate, ref, "B", 100, symbol, mid)))
            elif k == 4:
                # locked / crossed add far through the market
                out.append(frame(enc_add(ts, locate, next_ref, "S", 100, symbol, mid - 5 * tick)))
                live[next_ref] = ["S", mid - 5 * tick, 100]
                refs.append(next_ref)
                next_ref += 1
            elif k == 5:
                out.append(frame(enc_add(ts, locate, next_ref, "B", 0, symbol, mid)))
                next_ref += 1
            elif k == 6:
                out.append(frame(enc_delete(ts, locate, 1)[:10]))      # truncated message
            elif k == 7:
                out.append(frame(b"Z" + bytes(10)))                      # unknown type
            elif k == 8 and refs:
                ref = rnd.choice(refs)
                if ref in live:
                    out.append(frame(enc_cancel(ts, locate, ref, live[ref][2] + 7)))
            continue
        if r < 0.34 or not live:
            side = rnd.choice("BS")
            off = int(rnd.expovariate(0.5)) * tick + (tick if side == "B" else tick)
            price = mid - off if side == "B" else mid + off
            if rnd.random() < 0.3:
                price = (mid - tick) if side == "B" else (mid + tick)
            shares = rnd.choice((100, 100, 200, 300, 500, 1000))
            mpid = "NSDQ" if rnd.random() < 0.05 else None
            out.append(frame(enc_add(ts, locate, next_ref, side, shares, symbol, price, mpid)))
            live[next_ref] = [side, price, shares]
            refs.append(next_ref)
            next_ref += 1
        elif r < 0.58:
            ref = rnd.choice(list(live.keys())[-400:])
            out.append(frame(enc_delete(ts, locate, ref)))
            del live[ref]
        elif r < 0.68:
            ref = rnd.choice(list(live.keys())[-400:])
            side, price, sh = live.pop(ref)
            nprice = price if rnd.random() < 0.4 else price + rnd.choice((-1, 1)) * tick
            nsh = rnd.choice((100, 200, sh))
            out.append(frame(enc_replace(ts, locate, ref, next_ref, nsh, nprice)))
            live[next_ref] = [side, nprice, nsh]
            refs.append(next_ref)
            next_ref += 1
        elif r < 0.82:
            ref = rnd.choice(list(live.keys())[-400:])
            side, price, sh = live[ref]
            q = sh if rnd.random() < 0.4 else max(1, sh // 2)
            if rnd.random() < 0.3:
                out.append(frame(enc_exec_px(ts, locate, ref, q, price + tick // 2)))
            else:
                out.append(frame(enc_exec(ts, locate, ref, q)))
            if q >= sh:
                del live[ref]
            else:
                live[ref][2] = sh - q
        elif r < 0.92:
            ref = rnd.choice(list(live.keys())[-400:])
            side, price, sh = live[ref]
            q = max(1, sh // 3)
            out.append(frame(enc_cancel(ts, locate, ref, q)))
            if q >= sh:
                del live[ref]
            else:
                live[ref][2] = sh - q
        elif r < 0.96:
            out.append(frame(enc_trade(ts, locate, 100, symbol, mid)))
        else:
            out.append(frame(enc_trading(ts, locate, symbol, rnd.choice("TTTHQ"))))
    out.append(frame(enc_system(ts + 1, "M")))
    out.append(frame(enc_system(ts + 2, "C")))
    return b"".join(out)
