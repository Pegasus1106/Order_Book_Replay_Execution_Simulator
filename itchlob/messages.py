"""Nasdaq TotalView-ITCH 5.0 message layout and decoding.

Wire format (BinaryFILE): every message is preceded by a 2-byte big-endian length.
All integers are big-endian. Timestamps are 48-bit nanoseconds since midnight.
Prices are unsigned 32-bit integers with 4 implied decimals, and are kept as
integers ("price units", 1 unit = $0.0001) everywhere in this project.

Decoded events are plain tuples to keep the Python hot path cheap. Every event
starts with (type, ts_ns, locate); the rest depends on the type:

    A / F  (65, 70):  (t, ts, loc, ref, side, shares, price, stock, mpid)
    E      (69):      (t, ts, loc, ref, shares, match)
    C      (67):      (t, ts, loc, ref, shares, match, printable, price)
    X      (88):      (t, ts, loc, ref, shares)
    D      (68):      (t, ts, loc, ref)
    U      (85):      (t, ts, loc, old_ref, new_ref, shares, price)
    S      (83):      (t, ts, loc, event_code)
    H      (72):      (t, ts, loc, stock, trading_state)
    R      (82):      (t, ts, loc, stock)

`side` is the ASCII code of 'B' (66) or 'S' (83).
"""
from __future__ import annotations

import struct
from typing import Callable, Dict, Tuple

# --- message type codes (ASCII) ------------------------------------------------
T_SYSTEM = 83      # 'S'
T_DIRECTORY = 82   # 'R'
T_TRADING = 72     # 'H'
T_ADD = 65         # 'A'
T_ADD_MPID = 70    # 'F'
T_EXEC = 69        # 'E'
T_EXEC_PX = 67     # 'C'
T_CANCEL = 88      # 'X'
T_DELETE = 68      # 'D'
T_REPLACE = 85     # 'U'

SIDE_BUY = 66   # 'B'
SIDE_SELL = 83  # 'S'

BOOK_TYPES = frozenset((T_ADD, T_ADD_MPID, T_EXEC, T_EXEC_PX, T_CANCEL, T_DELETE, T_REPLACE))

# Total message length in bytes (excluding the 2-byte frame prefix), per the 5.0 spec.
MSG_LEN: Dict[int, int] = {
    ord("S"): 12, ord("R"): 39, ord("H"): 25, ord("Y"): 20, ord("L"): 26,
    ord("V"): 35, ord("W"): 12, ord("K"): 28, ord("J"): 35, ord("h"): 21,
    ord("A"): 36, ord("F"): 40, ord("E"): 31, ord("C"): 36, ord("X"): 23,
    ord("D"): 19, ord("U"): 35, ord("P"): 44, ord("Q"): 40, ord("B"): 19,
    ord("I"): 50, ord("N"): 20, ord("O"): 48,
}

MSG_NAME: Dict[int, str] = {
    ord("S"): "SystemEvent", ord("R"): "StockDirectory", ord("H"): "TradingAction",
    ord("Y"): "RegSHO", ord("L"): "MPPosition", ord("V"): "MWCBLevel", ord("W"): "MWCBStatus",
    ord("K"): "IPOQuoting", ord("J"): "LULDCollar", ord("h"): "OperationalHalt",
    ord("A"): "AddOrder", ord("F"): "AddOrderMPID", ord("E"): "OrderExecuted",
    ord("C"): "OrderExecutedPrice", ord("X"): "OrderCancel", ord("D"): "OrderDelete",
    ord("U"): "OrderReplace", ord("P"): "Trade", ord("Q"): "CrossTrade",
    ord("B"): "BrokenTrade", ord("I"): "NOII", ord("N"): "RPII", ord("O"): "DLCR",
}

_S_ADD = struct.Struct(">xHHHIQBI8sI")
_S_ADDF = struct.Struct(">xHHHIQBI8sI4s")
_S_EXEC = struct.Struct(">xHHHIQIQ")
_S_EXECPX = struct.Struct(">xHHHIQIQcI")
_S_CANCEL = struct.Struct(">xHHHIQI")
_S_DELETE = struct.Struct(">xHHHIQ")
_S_REPLACE = struct.Struct(">xHHHIQQII")
_S_SYSTEM = struct.Struct(">xHHHIB")
_S_TRADING = struct.Struct(">xHHHI8sBc4s")
_S_DIR = struct.Struct(">xHHHI8s")  # prefix only (first 19 bytes)

assert _S_ADD.size == 36 and _S_ADDF.size == 40 and _S_EXEC.size == 31
assert _S_EXECPX.size == 36 and _S_CANCEL.size == 23 and _S_DELETE.size == 19
assert _S_REPLACE.size == 35 and _S_SYSTEM.size == 12 and _S_TRADING.size == 25


class MalformedMessage(ValueError):
    """Known type with wrong length / undecodable body."""


class UnknownMessageType(ValueError):
    """Type byte not defined by the ITCH 5.0 specification."""


def decode_add(m: bytes):
    loc, _trk, hi, lo, ref, side, shares, stock, price = _S_ADD.unpack(m)
    return (T_ADD, (hi << 32) | lo, loc, ref, side, shares, price, stock, None)


def decode_add_mpid(m: bytes):
    loc, _trk, hi, lo, ref, side, shares, stock, price, mpid = _S_ADDF.unpack(m)
    return (T_ADD_MPID, (hi << 32) | lo, loc, ref, side, shares, price, stock, mpid)


def decode_exec(m: bytes):
    loc, _trk, hi, lo, ref, shares, match = _S_EXEC.unpack(m)
    return (T_EXEC, (hi << 32) | lo, loc, ref, shares, match)


def decode_exec_px(m: bytes):
    loc, _trk, hi, lo, ref, shares, match, printable, price = _S_EXECPX.unpack(m)
    return (T_EXEC_PX, (hi << 32) | lo, loc, ref, shares, match, printable, price)


def decode_cancel(m: bytes):
    loc, _trk, hi, lo, ref, shares = _S_CANCEL.unpack(m)
    return (T_CANCEL, (hi << 32) | lo, loc, ref, shares)


def decode_delete(m: bytes):
    loc, _trk, hi, lo, ref = _S_DELETE.unpack(m)
    return (T_DELETE, (hi << 32) | lo, loc, ref)


def decode_replace(m: bytes):
    loc, _trk, hi, lo, old, new, shares, price = _S_REPLACE.unpack(m)
    return (T_REPLACE, (hi << 32) | lo, loc, old, new, shares, price)


def decode_system(m: bytes):
    loc, _trk, hi, lo, code = _S_SYSTEM.unpack(m)
    return (T_SYSTEM, (hi << 32) | lo, loc, code)


def decode_trading(m: bytes):
    loc, _trk, hi, lo, stock, state, _res, _reason = _S_TRADING.unpack(m)
    return (T_TRADING, (hi << 32) | lo, loc, stock, state)


def decode_directory(m: bytes):
    if len(m) != 39:
        raise struct.error("stock directory length")
    loc, _trk, hi, lo, stock = _S_DIR.unpack_from(m)
    return (T_DIRECTORY, (hi << 32) | lo, loc, stock)


# Types the replay engine decodes. Everything else in MSG_LEN is length-checked and
# counted but not decoded (they do not change the displayed book).
DECODERS: Dict[int, Callable[[bytes], tuple]] = {
    T_ADD: decode_add,
    T_ADD_MPID: decode_add_mpid,
    T_EXEC: decode_exec,
    T_EXEC_PX: decode_exec_px,
    T_CANCEL: decode_cancel,
    T_DELETE: decode_delete,
    T_REPLACE: decode_replace,
    T_SYSTEM: decode_system,
    T_TRADING: decode_trading,
    T_DIRECTORY: decode_directory,
}


def decode(m: bytes) -> Tuple:
    """Strict decode of one message. Raises MalformedMessage / UnknownMessageType."""
    if not m:
        raise MalformedMessage("empty frame")
    t = m[0]
    want = MSG_LEN.get(t)
    if want is None:
        raise UnknownMessageType(chr(t))
    if len(m) != want:
        raise MalformedMessage(f"type {chr(t)!r}: length {len(m)} != {want}")
    dec = DECODERS.get(t)
    if dec is None:
        loc, _trk, hi, lo = struct.unpack_from(">xHHHI", m)
        return (t, (hi << 32) | lo, loc)
    try:
        return dec(m)
    except struct.error as e:  # pragma: no cover - length already checked
        raise MalformedMessage(str(e))


def peek_locate(m: bytes) -> int:
    return (m[1] << 8) | m[2]


def fmt_price(p: int) -> str:
    return f"{p // 10000}.{p % 10000:04d}"


def fmt_time(ts_ns: int) -> str:
    s, ns = divmod(ts_ns, 1_000_000_000)
    return f"{s // 3600:02d}:{(s // 60) % 60:02d}:{s % 60:02d}.{ns:09d}"
