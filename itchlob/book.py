"""Single-symbol limit order book reconstructed from ITCH order-level events.

Data structures
    orders      dict  ref -> Order            (order-reference lookup)
    bid_levels  dict  price -> Level          (Level.orders is an insertion-ordered
    ask_levels  dict  price -> Level           dict ref -> Order, i.e. the FIFO queue)
    bid_prices  list  ascending sorted prices (best bid = last)
    ask_prices  list  ascending sorted prices (best ask = first)

Prices are integers in 1/10000 dollar. Quantities are integer shares.

Semantics (TotalView-ITCH 5.0 section 1.3-1.4)
    Add      new displayed order, joins the back of its price level's FIFO.
    Execute  (E, C) reduce displayed shares; the order keeps queue priority; at 0 it
             is removed. For C the execution price may differ from the resting price;
             the book always reduces the order at its *display* price.
    Cancel   (X) partial cancel; reduces displayed shares, keeps priority.
    Delete   (D) removes all remaining shares.
    Replace  (U) removes the old order entirely, and adds a *new* order under the new
             reference number with the new shares and price. Side, symbol and MPID
             are inherited from the old order. The new order always goes to the BACK
             of its level's queue, even when the price is unchanged (priority is lost).

Anomalies never raise: they are counted (see `new_counters`) and the offending
event is either ignored or clamped, as documented per branch.
"""
from __future__ import annotations

from bisect import bisect_left, insort
from typing import Callable, Dict, List, Optional, Tuple

from .messages import (T_ADD, T_ADD_MPID, T_CANCEL, T_DELETE, T_EXEC, T_EXEC_PX, T_REPLACE,
                       SIDE_BUY, SIDE_SELL)

# Observer event kinds (see OrderBook.observer)
EV_ADD, EV_EXEC, EV_CANCEL, EV_DELETE = 1, 2, 3, 4

M64 = (1 << 64) - 1
_MUL = 0x9E3779B97F4A7C15
CHECKSUM_SEED = 0xCBF29CE484222325

COUNTER_NAMES = (
    # applied events
    "add", "exec", "exec_px", "cancel", "delete", "replace",
    "exec_shares", "cancel_shares",
    # anomalies / diagnostics
    "unknown_ref", "unknown_ref_exec", "unknown_ref_cancel", "unknown_ref_delete",
    "unknown_ref_replace",
    "duplicate_ref", "qty_inconsistency", "qty_excess_exec", "qty_excess_cancel",
    "zero_qty_event", "bad_side", "symbol_mismatch",
    # unusual market state
    "locked_episodes", "crossed_episodes", "events_while_locked", "events_while_crossed",
    "max_locked_ns", "max_crossed_ns", "total_locked_ns", "total_crossed_ns",
    # internal consistency
    "invariant_failures",
)


def new_counters() -> Dict[str, int]:
    return {k: 0 for k in COUNTER_NAMES}


def mix(h: int, w: int) -> int:
    """Order-sensitive 64-bit combine. Identical in the C++ implementation."""
    h = ((h ^ w) * _MUL) & M64
    return h ^ (h >> 32)


class Order:
    __slots__ = ("ref", "side", "price", "shares", "seq", "mpid")

    def __init__(self, ref, side, price, shares, seq, mpid=None):
        self.ref = ref
        self.side = side
        self.price = price
        self.shares = shares
        self.seq = seq      # arrival priority; only meaningful relative to others in a level
        self.mpid = mpid

    def __repr__(self):  # pragma: no cover
        return f"Order(ref={self.ref}, {chr(self.side)} {self.shares}@{self.price}, seq={self.seq})"


class Level:
    __slots__ = ("price", "total", "orders")

    def __init__(self, price):
        self.price = price
        self.total = 0
        self.orders: Dict[int, Order] = {}


Observer = Callable[[int, int, int, int, int, int, int], None]


class OrderBook:
    def __init__(self, symbol: str = "", locate: Optional[int] = None, debug: bool = False,
                 strict: bool = False):
        self.symbol = symbol
        self.locate = locate
        self._sym8 = symbol.ljust(8).encode("ascii") if symbol else None
        self.debug = debug          # per-removal lookup checks (slower)
        self.strict = strict        # raise on invariant failure instead of counting
        self.orders: Dict[int, Order] = {}
        self.bid_levels: Dict[int, Level] = {}
        self.ask_levels: Dict[int, Level] = {}
        self.bid_prices: List[int] = []
        self.ask_prices: List[int] = []
        self.seq = 0                # monotonically increasing priority counter
        self.ev_count = 0           # successful book mutations
        self.last_ts = 0
        self.c = new_counters()
        self.anomalies: List[dict] = []   # first few unusual-state samples
        self.failures: List[str] = []     # first few invariant failures
        self.observer: Optional[Observer] = None
        self._xstate = 0            # 0 normal, 1 locked, 2 crossed
        self._xstart = 0

    # ------------------------------------------------------------------ dispatch
    def apply(self, ev: tuple) -> bool:
        """Apply one decoded event tuple (see messages.py). Returns True if book changed."""
        t = ev[0]
        if t == T_ADD or t == T_ADD_MPID:
            return self.add_order(ev[1], ev[3], ev[4], ev[5], ev[6], ev[8], ev[7])
        if t == T_DELETE:
            return self.delete_order(ev[1], ev[3])
        if t == T_REPLACE:
            return self.replace_order(ev[1], ev[3], ev[4], ev[5], ev[6])
        if t == T_EXEC:
            return self.execute_order(ev[1], ev[3], ev[4], False)
        if t == T_CANCEL:
            return self.cancel_order(ev[1], ev[3], ev[4])
        if t == T_EXEC_PX:
            return self.execute_order(ev[1], ev[3], ev[4], True)
        return False

    # ------------------------------------------------------------------ mutations
    def add_order(self, ts, ref, side, shares, price, mpid=None, stock=None) -> bool:
        c = self.c
        if stock is not None and self._sym8 is not None and stock != self._sym8:
            c["symbol_mismatch"] += 1
        if side != SIDE_BUY and side != SIDE_SELL:
            c["bad_side"] += 1
            return False
        if shares <= 0:
            c["zero_qty_event"] += 1
            return False
        if ref in self.orders:
            c["duplicate_ref"] += 1
            return False
        self.seq += 1
        o = Order(ref, side, price, shares, self.seq, mpid)
        self.orders[ref] = o
        self._level_add(o)
        c["add"] += 1
        if self.observer is not None:
            self.observer(EV_ADD, side, price, shares, o.seq, ref, ts)
        self._post(ts)
        return True

    def delete_order(self, ts, ref) -> bool:
        o = self.orders.pop(ref, None)
        c = self.c
        if o is None:
            c["unknown_ref"] += 1
            c["unknown_ref_delete"] += 1
            return False
        self._level_remove(o)
        if self.debug:
            self._check_gone(o)
        c["delete"] += 1
        if self.observer is not None:
            self.observer(EV_DELETE, o.side, o.price, o.shares, o.seq, ref, ts)
        self._post(ts)
        return True

    def execute_order(self, ts, ref, shares, with_price=False) -> bool:
        """E / C. Reduces displayed shares by `shares` (clamped to the remainder)."""
        o = self.orders.get(ref)
        c = self.c
        if o is None:
            c["unknown_ref"] += 1
            c["unknown_ref_exec"] += 1
            return False
        if shares <= 0:
            c["zero_qty_event"] += 1
            return False
        rem = o.shares
        if shares > rem:
            c["qty_inconsistency"] += 1
            c["qty_excess_exec"] += shares - rem
            shares = rem
        self._reduce(o, shares)
        c["exec_px" if with_price else "exec"] += 1
        c["exec_shares"] += shares
        if self.observer is not None:
            self.observer(EV_EXEC, o.side, o.price, shares, o.seq, ref, ts)
        if self.debug and o.shares == 0:
            self._check_gone(o)
        self._post(ts)
        return True

    def cancel_order(self, ts, ref, shares) -> bool:
        """X: partial cancel. Keeps queue priority. Reaching 0 removes the order."""
        o = self.orders.get(ref)
        c = self.c
        if o is None:
            c["unknown_ref"] += 1
            c["unknown_ref_cancel"] += 1
            return False
        if shares <= 0:
            c["zero_qty_event"] += 1
            return False
        rem = o.shares
        if shares > rem:
            c["qty_inconsistency"] += 1
            c["qty_excess_cancel"] += shares - rem
            shares = rem
        self._reduce(o, shares)
        c["cancel"] += 1
        c["cancel_shares"] += shares
        if self.observer is not None:
            self.observer(EV_CANCEL, o.side, o.price, shares, o.seq, ref, ts)
        if self.debug and o.shares == 0:
            self._check_gone(o)
        self._post(ts)
        return True

    def replace_order(self, ts, old_ref, new_ref, shares, price) -> bool:
        """U: remove `old_ref`, add `new_ref` (back of queue) inheriting side and MPID."""
        c = self.c
        o = self.orders.pop(old_ref, None)
        if o is None:
            c["unknown_ref"] += 1
            c["unknown_ref_replace"] += 1
            return False
        self._level_remove(o)
        if self.debug:
            self._check_gone(o)
        if self.observer is not None:
            self.observer(EV_DELETE, o.side, o.price, o.shares, o.seq, old_ref, ts)
        if shares <= 0:
            c["zero_qty_event"] += 1       # old order is gone, nothing to add
            self._post(ts)
            return True
        if new_ref in self.orders:
            c["duplicate_ref"] += 1
            self._post(ts)
            return True
        self.seq += 1
        n = Order(new_ref, o.side, price, shares, self.seq, o.mpid)
        self.orders[new_ref] = n
        self._level_add(n)
        c["replace"] += 1
        if self.observer is not None:
            self.observer(EV_ADD, n.side, price, shares, n.seq, new_ref, ts)
        self._post(ts)
        return True

    # ------------------------------------------------------------------ internals
    def _level_add(self, o: Order) -> None:
        if o.side == SIDE_BUY:
            levels, prices = self.bid_levels, self.bid_prices
        else:
            levels, prices = self.ask_levels, self.ask_prices
        lv = levels.get(o.price)
        if lv is None:
            lv = Level(o.price)
            levels[o.price] = lv
            insort(prices, o.price)
        lv.orders[o.ref] = o
        lv.total += o.shares

    def _level_remove(self, o: Order) -> None:
        """Remove the whole order (with its current remaining shares) from its level."""
        if o.side == SIDE_BUY:
            levels, prices = self.bid_levels, self.bid_prices
        else:
            levels, prices = self.ask_levels, self.ask_prices
        lv = levels[o.price]
        lv.total -= o.shares
        del lv.orders[o.ref]
        if not lv.orders:
            del levels[o.price]
            del prices[bisect_left(prices, o.price)]

    def _reduce(self, o: Order, shares: int) -> None:
        if o.side == SIDE_BUY:
            levels, prices = self.bid_levels, self.bid_prices
        else:
            levels, prices = self.ask_levels, self.ask_prices
        lv = levels[o.price]
        o.shares -= shares
        lv.total -= shares
        if o.shares == 0:
            del self.orders[o.ref]
            del lv.orders[o.ref]
            if not lv.orders:
                del levels[o.price]
                del prices[bisect_left(prices, o.price)]

    def _post(self, ts: int) -> None:
        self.last_ts = ts
        self.ev_count += 1
        bp = self.bid_prices
        ap = self.ask_prices
        st = 0
        if bp and ap:
            b = bp[-1]
            a = ap[0]
            if b >= a:
                st = 2 if b > a else 1
        if st or self._xstate:
            self._cross_update(st, ts)

    def _cross_update(self, st: int, ts: int) -> None:
        c = self.c
        prev = self._xstate
        if st:
            c["events_while_crossed" if st == 2 else "events_while_locked"] += 1
        if st == prev:
            return
        if prev:
            dur = ts - self._xstart
            kind = "crossed" if prev == 2 else "locked"
            c["total_" + kind + "_ns"] += dur
            if dur > c["max_" + kind + "_ns"]:
                c["max_" + kind + "_ns"] = dur
            self._note(kind + "_end", ts, dur)
        if st:
            kind = "crossed" if st == 2 else "locked"
            c[kind + "_episodes"] += 1
            self._xstart = ts
            self._note(kind + "_start", ts, 0)
        self._xstate = st

    def _note(self, kind: str, ts: int, dur: int) -> None:
        if len(self.anomalies) < 200:
            bb = self.bid_prices[-1] if self.bid_prices else None
            ba = self.ask_prices[0] if self.ask_prices else None
            self.anomalies.append({"kind": kind, "ts": ts, "ev": self.ev_count,
                                   "best_bid": bb, "best_ask": ba, "duration_ns": dur})

    def finalize(self) -> None:
        """Close an open locked/crossed episode at the last seen timestamp."""
        if self._xstate:
            self._cross_update(0, self.last_ts)

    def _fail(self, msg: str) -> None:
        self.c["invariant_failures"] += 1
        if len(self.failures) < 50:
            self.failures.append(msg)
        if self.strict:
            raise AssertionError(msg)

    def _check_gone(self, o: Order) -> None:
        """A removed order must be absent from the ref map and from its price level."""
        if o.ref in self.orders:
            self._fail(f"ref {o.ref} still in order map after removal")
        levels = self.bid_levels if o.side == SIDE_BUY else self.ask_levels
        lv = levels.get(o.price)
        if lv is not None and o.ref in lv.orders:
            self._fail(f"ref {o.ref} still in level {o.price} after removal")

    # ------------------------------------------------------------------ queries
    def best_bid(self) -> Optional[Tuple[int, int]]:
        if not self.bid_prices:
            return None
        p = self.bid_prices[-1]
        return p, self.bid_levels[p].total

    def best_ask(self) -> Optional[Tuple[int, int]]:
        if not self.ask_prices:
            return None
        p = self.ask_prices[0]
        return p, self.ask_levels[p].total

    def spread(self) -> Optional[int]:
        if self.bid_prices and self.ask_prices:
            return self.ask_prices[0] - self.bid_prices[-1]
        return None

    def mid2(self) -> Optional[int]:
        """Twice the mid price (integer, avoids rounding)."""
        if self.bid_prices and self.ask_prices:
            return self.ask_prices[0] + self.bid_prices[-1]
        return None

    def bid_levels_best_first(self, n: Optional[int] = None):
        ps = self.bid_prices
        k = len(ps) if n is None else min(n, len(ps))
        for i in range(1, k + 1):
            yield self.bid_levels[ps[-i]]

    def ask_levels_best_first(self, n: Optional[int] = None):
        ps = self.ask_prices
        k = len(ps) if n is None else min(n, len(ps))
        for i in range(k):
            yield self.ask_levels[ps[i]]

    def depth(self, n: int = 5):
        """([(price, total_shares, n_orders)] bids best-first, same for asks)."""
        bids = [(lv.price, lv.total, len(lv.orders)) for lv in self.bid_levels_best_first(n)]
        asks = [(lv.price, lv.total, len(lv.orders)) for lv in self.ask_levels_best_first(n)]
        return bids, asks

    def imbalance(self, n: int = 1) -> Optional[float]:
        """(bid - ask) / (bid + ask) displayed shares over the top n levels, in [-1, 1]."""
        b = sum(lv.total for lv in self.bid_levels_best_first(n))
        a = sum(lv.total for lv in self.ask_levels_best_first(n))
        return (b - a) / (b + a) if (b + a) else None

    def snapshot(self, depth: int = 5) -> dict:
        bids, asks = self.depth(depth)
        bb = self.best_bid()
        ba = self.best_ask()
        return {
            "ts": self.last_ts,
            "best_bid": bb[0] if bb else None, "best_bid_size": bb[1] if bb else None,
            "best_ask": ba[0] if ba else None, "best_ask_size": ba[1] if ba else None,
            "spread": self.spread(),
            "orders": len(self.orders),
            "bid_levels": len(self.bid_prices), "ask_levels": len(self.ask_prices),
            "bids": bids, "asks": asks,
        }

    # ------------------------------------------------------------------ state identity
    def checksum(self) -> int:
        """Deterministic 64-bit digest of the full book including FIFO order in levels."""
        h = CHECKSUM_SEED
        h = mix(h, len(self.orders))
        for side, levels, prices, rev in ((SIDE_BUY, self.bid_levels, self.bid_prices, True),
                                          (SIDE_SELL, self.ask_levels, self.ask_prices, False)):
            h = mix(h, side)
            h = mix(h, len(prices))
            seq = reversed(prices) if rev else prices
            for p in seq:
                lv = levels[p]
                h = mix(h, p)
                h = mix(h, lv.total)
                h = mix(h, len(lv.orders))
                for o in lv.orders.values():
                    h = mix(h, o.ref)
                    h = mix(h, o.shares)
        return h

    def dump_lines(self) -> List[str]:
        """Canonical text rendering of the full state (used to diff Python vs C++)."""
        out = [f"orders {len(self.orders)}"]
        for name, levels, prices, rev in (("BID", self.bid_levels, self.bid_prices, True),
                                          ("ASK", self.ask_levels, self.ask_prices, False)):
            for p in (reversed(prices) if rev else prices):
                lv = levels[p]
                out.append(f"{name} {p} {lv.total} {len(lv.orders)}")
                for o in lv.orders.values():
                    out.append(f"  {o.ref} {o.shares}")
        return out

    def to_state(self) -> dict:
        orders = []
        for levels, prices, rev in ((self.bid_levels, self.bid_prices, True),
                                    (self.ask_levels, self.ask_prices, False)):
            for p in (reversed(prices) if rev else prices):
                for o in levels[p].orders.values():
                    orders.append([o.ref, chr(o.side), o.price, o.shares,
                                   o.mpid.decode("ascii") if o.mpid else None])
        return {"symbol": self.symbol, "locate": self.locate, "last_ts": self.last_ts,
                "orders": orders, "counters": dict(self.c), "ev_count": self.ev_count,
                "xstate": self._xstate, "xstart": self._xstart}

    @classmethod
    def from_state(cls, st: dict, **kw) -> "OrderBook":
        b = cls(st.get("symbol", ""), st.get("locate"), **kw)
        for ref, side, price, shares, mpid in st["orders"]:
            b.seq += 1
            o = Order(ref, ord(side), price, shares, b.seq, mpid.encode("ascii") if mpid else None)
            b.orders[ref] = o
            b._level_add(o)
        b.last_ts = st["last_ts"]
        b.c.update(st["counters"])
        b.ev_count = st.get("ev_count", 0)
        b._xstate = st.get("xstate", 0)
        b._xstart = st.get("xstart", 0)
        return b
