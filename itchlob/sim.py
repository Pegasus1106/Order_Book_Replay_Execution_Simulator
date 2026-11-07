"""Execution simulator driven by the replayed order book.

THE COUNTERFACTUAL LIMITATION (read this before trusting any number)
    The replay is a record of what happened *without* our orders. Simulated orders are
    never inserted into the book: they do not consume liquidity that other participants
    then miss, do not change anyone's behaviour, and do not trigger the reactions a real
    order would. Results are therefore only credible for small sizes relative to
    displayed depth. The simulator enforces `max_order_qty` and a depth-participation
    cap for marketable orders, and keeps a "virtual consumption" ledger so that two
    simulated marketable orders cannot take the same displayed shares twice.

Timing model
    A strategy decision made at event time T arrives at the matching engine at
        T + decision_ns + {submit,cancel,replace}_ns (+ seeded jitter)
    An action that arrives at time A is applied after every book event with timestamp
    <= A and before the first event with timestamp > A. Cancels and replaces therefore
    race against fills: a passive order can be filled while its cancel is in flight.

Marketable orders
    Sweep the displayed opposing book from the best price inwards, up to an optional
    limit price, skipping shares already virtually consumed by earlier simulated
    orders. Unfilled quantity is reported (insufficient liquidity), never invented.

Passive orders and queue position (displayed-quantity approximation)
    On arrival the order joins the BACK of the queue at its price. `ahead` is the
    estimated displayed quantity in front of it; the order fills only after `ahead`
    reaches zero. Updated per book event at that price level:
      Execution at the level   executions take from the front: ahead shrinks first,
                               any excess fills us (partial fills allowed).
      Cancel / delete          unknown whether the cancelled order was ahead or behind:
        conservative           assume it was BEHIND us (ahead unchanged)
        optimistic             assume it was AHEAD of us (ahead -= qty)
        proportional           pro rata: ahead -= qty * ahead / (displayed on level)
        identity               ITCH gives order references and arrival order, so the
                               position of the cancelled order relative to our marker
                               is known: ahead only shrinks if it was in front.
      Add                      always behind us.
      Trade-through            an execution at a price WORSE than ours on our side
                               means our level would have been exhausted: fill us.
    Hidden (non-displayed) liquidity, odd-lot handling, order-type priority rules and
    the effect of our own presence are not modelled. `identity` is the closest to the
    truth for displayed shares; `conservative` and `optimistic` bound the plausible range
    for feeds that do not publish order references.
"""
from __future__ import annotations

import heapq
import random
from dataclasses import dataclass
from typing import Callable, Dict, List, NamedTuple, Optional

from .book import EV_ADD, EV_CANCEL, EV_DELETE, EV_EXEC, OrderBook
from .messages import SIDE_BUY, SIDE_SELL

CONSERVATIVE, OPTIMISTIC, PROPORTIONAL, IDENTITY = "conservative", "optimistic", "proportional", "identity"
QUEUE_MODELS = (CONSERVATIVE, OPTIMISTIC, PROPORTIONAL, IDENTITY)

PENDING, ACTIVE, FILLED, CANCELLED, REJECTED = "PENDING", "ACTIVE", "FILLED", "CANCELLED", "REJECTED"

UNITS_PER_USD = 10_000


@dataclass
class FeeModel:
    """Fees in price units (1 unit = $0.0001). Positive = cost, negative = rebate.

    Defaults are placeholders in the range of published exchange schedules
    (remove liquidity ~$0.0030/share, add liquidity rebate ~$0.0020/share); replace with
    the schedule that applies to the account being modelled.
    """
    taker_per_share: int = 30
    maker_per_share: int = -20
    taker_bps: float = 0.0
    maker_bps: float = 0.0

    def fee(self, liquidity: str, qty: int, price: int) -> int:
        if liquidity == "T":
            per, bps = self.taker_per_share, self.taker_bps
        else:
            per, bps = self.maker_per_share, self.maker_bps
        f = per * qty + bps * 1e-4 * price * qty
        return int(round(f))


@dataclass
class LatencyModel:
    """One-way delays in nanoseconds. All simulated, deterministic given `seed`."""
    decision_ns: int = 20_000      # market-data arrival -> order leaves strategy
    submit_ns: int = 100_000       # order leaves strategy -> reaches matching engine
    cancel_ns: int = 100_000
    replace_ns: int = 100_000
    jitter_ns: int = 0             # uniform [0, jitter_ns] added per action
    seed: int = 12345

    def __post_init__(self):
        self._rng = random.Random(self.seed)

    def _j(self) -> int:
        return self._rng.randint(0, self.jitter_ns) if self.jitter_ns else 0

    def submit_delay(self) -> int:
        return self.decision_ns + self.submit_ns + self._j()

    def cancel_delay(self) -> int:
        return self.decision_ns + self.cancel_ns + self._j()

    def replace_delay(self) -> int:
        return self.decision_ns + self.replace_ns + self._j()


class Fill(NamedTuple):
    ts: int
    order_id: int
    side: int
    price: int
    qty: int
    liquidity: str   # "T" took displayed liquidity, "M" provided (passive)
    fee: int


class SimOrder:
    __slots__ = ("id", "side", "qty", "price", "limit", "kind", "tif", "status", "remaining",
                 "filled", "ahead", "ahead0", "marker", "decision_ts", "arrive_ts", "cancel_pending",
                 "n_replace", "inst_filled", "decision_mid2", "arrival_mid2", "reject_reason",
                 "vwap_num", "fee")

    def __init__(self, oid, side, qty, price, kind, tif, decision_ts):
        self.id = oid
        self.side = side
        self.qty = qty
        self.price = price          # passive: resting price. marketable: None
        self.limit = None           # marketable: optional limit price
        self.kind = kind            # "MKT" or "PASSIVE"
        self.tif = tif
        self.status = PENDING
        self.remaining = qty
        self.filled = 0
        self.ahead = 0.0
        self.ahead0 = None          # displayed quantity ahead when first joining the queue
        self.marker = 0
        self.decision_ts = decision_ts
        self.arrive_ts = None
        self.cancel_pending = False
        self.n_replace = 0
        self.inst_filled = 0
        self.decision_mid2 = None
        self.arrival_mid2 = None
        self.reject_reason = ""
        self.vwap_num = 0           # sum(price*qty) over fills
        self.fee = 0


class ExecutionSimulator:
    def __init__(self, book: OrderBook, fees: Optional[FeeModel] = None,
                 latency: Optional[LatencyModel] = None, queue_model: str = IDENTITY,
                 max_order_qty: int = 1000, max_depth_fraction: Optional[float] = 0.5,
                 depth_levels: int = 5, on_fill: Optional[Callable[[Fill], None]] = None,
                 keep_fill_log: bool = True, attach_observer: bool = True):
        if queue_model not in QUEUE_MODELS:
            raise ValueError(f"queue_model must be one of {QUEUE_MODELS}")
        self.book = book
        self.fees = fees or FeeModel()
        self.lat = latency or LatencyModel()
        self.model = queue_model
        self.max_order_qty = max_order_qty
        self.max_depth_fraction = max_depth_fraction
        self.depth_levels = depth_levels
        self.on_fill = on_fill
        self.fills: List[Fill] = []
        self.keep_fill_log = keep_fill_log
        self.orders: Dict[int, SimOrder] = {}
        self.market_results: List[dict] = []
        self.consumed: Dict[int, int] = {}      # book ref -> shares virtually taken by us
        self.by_level: Dict[tuple, List[SimOrder]] = {}
        self.active: List[SimOrder] = []
        self._heap: list = []
        self._ctr = 0
        self._next_id = 1
        self.now = 0
        self.stats = {k: 0 for k in (
            "market_orders", "market_unfilled_orders", "market_filled_qty", "market_unfilled_qty",
            "passive_orders", "passive_instances", "passive_instances_filled", "passive_posted_qty",
            "passive_filled_qty", "rejected_size", "rejected_no_liquidity", "cancel_requests",
            "cancel_too_late", "replace_requests", "replace_too_late", "through_fills")}
        if attach_observer:
            book.observer = self.on_book_event

    # ------------------------------------------------------------------ public API
    def submit_marketable(self, ts: int, side: int, qty: int, limit_price: Optional[int] = None,
                          tif: str = "IOC") -> SimOrder:
        o = SimOrder(self._next_id, side, qty, None, "MKT", tif, ts)
        self._next_id += 1
        o.limit = limit_price
        o.decision_mid2 = self.book.mid2()
        self.orders[o.id] = o
        self.stats["market_orders"] += 1
        self._schedule(ts + self.lat.submit_delay(), "NEW", o.id, None)
        return o

    def submit_passive(self, ts: int, side: int, qty: int, price: int, tif: str = "DAY") -> SimOrder:
        o = SimOrder(self._next_id, side, qty, price, "PASSIVE", tif, ts)
        self._next_id += 1
        o.decision_mid2 = self.book.mid2()
        self.orders[o.id] = o
        self.stats["passive_orders"] += 1
        self._schedule(ts + self.lat.submit_delay(), "NEW", o.id, None)
        return o

    def cancel(self, ts: int, order_id: int) -> None:
        self.stats["cancel_requests"] += 1
        self._schedule(ts + self.lat.cancel_delay(), "CANCEL", order_id, None)

    def replace(self, ts: int, order_id: int, price: int, qty: Optional[int] = None) -> None:
        self.stats["replace_requests"] += 1
        self._schedule(ts + self.lat.replace_delay(), "REPLACE", order_id, (price, qty))

    def advance(self, ts: int) -> None:
        """Apply all actions whose arrival time is strictly before `ts` (call before an event)."""
        h = self._heap
        while h and h[0][0] < ts:
            arrive, _, kind, oid, payload = heapq.heappop(h)
            self.now = arrive
            self._run(kind, oid, payload, arrive)

    def finish(self) -> None:
        """Apply anything still in flight against the final book state."""
        h = self._heap
        while h:
            arrive, _, kind, oid, payload = heapq.heappop(h)
            self.now = arrive
            self._run(kind, oid, payload, arrive)

    # ------------------------------------------------------------------ internals
    def _schedule(self, arrive: int, kind: str, oid: int, payload) -> None:
        self._ctr += 1
        heapq.heappush(self._heap, (arrive, self._ctr, kind, oid, payload))

    def _run(self, kind: str, oid: int, payload, ts: int) -> None:
        o = self.orders.get(oid)
        if o is None:
            return
        if kind == "NEW":
            o.arrive_ts = ts
            if o.kind == "MKT":
                self._arrive_marketable(o, ts)
            else:
                self._arrive_passive(o, ts)
                if o.cancel_pending and o.status == ACTIVE:
                    self._end_instance(o)
                    self._deactivate(o)
                    o.status = CANCELLED
        elif kind == "CANCEL":
            if o.status == ACTIVE:
                self._end_instance(o)
                self._deactivate(o)
                o.status = CANCELLED
            elif o.status == PENDING:
                o.cancel_pending = True
            elif o.status == FILLED:
                self.stats["cancel_too_late"] += 1
        elif kind == "REPLACE":
            price, qty = payload
            if o.status == ACTIVE:
                self._end_instance(o)
                self._deactivate(o)
                o.price = price
                if qty is not None:
                    o.remaining = qty        # new total working quantity after the replace
                o.n_replace += 1
                self._arrive_passive(o, ts)
            elif o.status == PENDING:
                o.price = price
                if qty is not None:
                    o.remaining = qty
                    o.qty = qty
            else:
                self.stats["replace_too_late"] += 1

    # ---- marketable
    def _depth_available(self, side: int) -> int:
        it = self.book.ask_levels_best_first(self.depth_levels) if side == SIDE_BUY \
            else self.book.bid_levels_best_first(self.depth_levels)
        return sum(lv.total for lv in it)

    def _sweep(self, side: int, qty: int, limit: Optional[int]):
        """Walk the displayed opposing book. Returns [(price, qty)] and updates the ledger."""
        levels = self.book.ask_levels_best_first() if side == SIDE_BUY \
            else self.book.bid_levels_best_first()
        out = []
        need = qty
        consumed = self.consumed
        for lv in levels:
            p = lv.price
            if limit is not None and ((side == SIDE_BUY and p > limit) or
                                      (side == SIDE_SELL and p < limit)):
                break
            took = 0
            for ref, bo in lv.orders.items():
                avail = bo.shares - consumed.get(ref, 0)
                if avail <= 0:
                    continue
                q = avail if avail < need else need
                consumed[ref] = consumed.get(ref, 0) + q
                took += q
                need -= q
                if need == 0:
                    break
            if took:
                out.append((p, took))
            if need == 0:
                break
        return out

    def _arrive_marketable(self, o: SimOrder, ts: int) -> None:
        o.arrival_mid2 = self.book.mid2()
        opp = self.book.ask_prices if o.side == SIDE_BUY else self.book.bid_prices
        if o.qty > self.max_order_qty:
            return self._reject(o, "size_limit", "rejected_size")
        if not opp:
            return self._reject(o, "no_liquidity", "rejected_no_liquidity")
        if self.max_depth_fraction is not None:
            displayed = self._depth_available(o.side)
            if o.qty > self.max_depth_fraction * displayed:
                return self._reject(o, "depth_participation_limit", "rejected_size")
        pieces = self._sweep(o.side, o.qty, o.limit)
        for p, q in pieces:
            self._record_fill(o, p, q, "T", ts)
        self._finish_marketable(o)

    def _reject(self, o: SimOrder, reason: str, stat: str) -> None:
        o.status = REJECTED
        o.reject_reason = reason
        self.stats[stat] += 1
        if o.kind == "MKT":
            self._finish_marketable(o)

    def _finish_marketable(self, o: SimOrder) -> None:
        if o.status == REJECTED:
            pass
        else:
            o.status = FILLED if o.remaining == 0 else CANCELLED
        if o.remaining > 0:
            self.stats["market_unfilled_orders"] += 1
            self.stats["market_unfilled_qty"] += o.remaining
        self.stats["market_filled_qty"] += o.filled
        vwap = o.vwap_num / o.filled if o.filled else None
        sign = 1 if o.side == SIDE_BUY else -1
        slip_arr = None
        slip_dec = None
        if vwap is not None:
            if o.arrival_mid2 is not None:
                slip_arr = sign * (vwap - o.arrival_mid2 / 2.0)
            if o.decision_mid2 is not None:
                slip_dec = sign * (vwap - o.decision_mid2 / 2.0)
        self.market_results.append({
            "id": o.id, "side": chr(o.side), "qty": o.qty, "filled": o.filled,
            "unfilled": o.qty - o.filled, "status": o.status, "reason": o.reject_reason,
            "decision_ts": o.decision_ts, "arrive_ts": o.arrive_ts,
            "vwap_units": vwap, "vwap_usd": vwap / UNITS_PER_USD if vwap is not None else None,
            "notional_usd": o.vwap_num / UNITS_PER_USD, "fee_usd": o.fee / UNITS_PER_USD,
            "slippage_vs_arrival_mid_units": slip_arr, "slippage_vs_decision_mid_units": slip_dec,
            "arrival_mid_usd": o.arrival_mid2 / 2.0 / UNITS_PER_USD if o.arrival_mid2 else None,
        })

    # ---- passive
    def _arrive_passive(self, o: SimOrder, ts: int) -> None:
        if o.remaining > self.max_order_qty:
            return self._reject(o, "size_limit", "rejected_size")
        b = self.book
        if o.side == SIDE_BUY:
            marketable = bool(b.ask_prices) and o.price >= b.ask_prices[0]
            levels = b.bid_levels
        else:
            marketable = bool(b.bid_prices) and o.price <= b.bid_prices[-1]
            levels = b.ask_levels
        if marketable:
            o.arrival_mid2 = b.mid2()
            for p, q in self._sweep(o.side, o.remaining, o.price):
                self._record_fill(o, p, q, "T", ts)
            if o.remaining == 0:
                o.status = FILLED
                return
            if o.tif == "IOC":
                o.status = CANCELLED
                return
        lv = levels.get(o.price)
        o.ahead = float(lv.total) if lv is not None else 0.0
        if o.ahead0 is None:
            o.ahead0 = o.ahead
        o.marker = b.seq
        o.status = ACTIVE
        o.inst_filled = 0
        self.stats["passive_instances"] += 1
        self.stats["passive_posted_qty"] += o.remaining
        self.by_level.setdefault((o.side, o.price), []).append(o)
        self.active.append(o)

    def _deactivate(self, o: SimOrder) -> None:
        key = (o.side, o.price)
        lst = self.by_level.get(key)
        if lst is not None:
            try:
                lst.remove(o)
            except ValueError:
                pass
            if not lst:
                del self.by_level[key]
        try:
            self.active.remove(o)
        except ValueError:
            pass

    def _end_instance(self, o: SimOrder) -> None:
        if o.inst_filled > 0:
            self.stats["passive_instances_filled"] += 1
        o.inst_filled = 0

    def _fill_passive(self, o: SimOrder, price: int, qty: int, ts: int) -> None:
        qty = min(qty, o.remaining)
        if qty <= 0:
            return
        o.inst_filled += qty
        self.stats["passive_filled_qty"] += qty
        self._record_fill(o, price, qty, "M", ts)
        if o.remaining == 0:
            self._end_instance(o)
            self._deactivate(o)
            o.status = FILLED

    def _record_fill(self, o: SimOrder, price: int, qty: int, liq: str, ts: int) -> None:
        fee = self.fees.fee(liq, qty, price)
        o.filled += qty
        o.remaining -= qty
        o.vwap_num += price * qty
        o.fee += fee
        f = Fill(ts, o.id, o.side, price, qty, liq, fee)
        if self.keep_fill_log:
            self.fills.append(f)
        if self.on_fill is not None:
            self.on_fill(f)

    # ---- book observer
    def on_book_event(self, kind: int, side: int, price: int, qty: int, seq: int, ref: int,
                      ts: int) -> None:
        cons = self.consumed
        if cons and ref in cons:
            if kind == EV_DELETE:
                del cons[ref]
            elif kind != EV_ADD:
                bo = self.book.orders.get(ref)
                if bo is None:
                    del cons[ref]
                elif cons[ref] > bo.shares:
                    cons[ref] = bo.shares
        if not self.active:
            return
        if kind == EV_EXEC:
            # trade-through: an execution on our side at a worse price than ours
            for o in tuple(self.active):
                if o.side == side and ((side == SIDE_BUY and o.price > price) or
                                       (side == SIDE_SELL and o.price < price)):
                    self.stats["through_fills"] += 1
                    o.ahead = 0.0
                    self._fill_passive(o, o.price, o.remaining, ts)
            lst = self.by_level.get((side, price))
            if lst:
                for o in tuple(lst):
                    self._queue_exec(o, qty, seq, ts)
        elif kind == EV_CANCEL or kind == EV_DELETE:
            lst = self.by_level.get((side, price))
            if lst:
                for o in lst:
                    self._queue_removal(o, qty, seq, side, price)

    def _queue_exec(self, o: SimOrder, qty: int, seq: int, ts: int) -> None:
        if self.model == IDENTITY and seq > o.marker:
            # an order that arrived after us was executed, so FIFO says we were reached first
            o.ahead = 0.0
            fill = min(o.remaining, qty)
        else:
            consume = o.ahead if o.ahead < qty else float(qty)
            o.ahead -= consume
            fill = min(o.remaining, int(qty - consume))
        if fill > 0:
            self._fill_passive(o, o.price, fill, ts)
        else:
            self._cap_ahead(o)

    def _queue_removal(self, o: SimOrder, qty: int, seq: int, side: int, price: int) -> None:
        m = self.model
        if m == OPTIMISTIC:
            o.ahead = max(0.0, o.ahead - qty)
        elif m == PROPORTIONAL:
            lv = (self.book.bid_levels if side == SIDE_BUY else self.book.ask_levels).get(price)
            before = (lv.total if lv is not None else 0) + qty
            if before > 0:
                o.ahead -= qty * o.ahead / before
        elif m == IDENTITY:
            if seq <= o.marker:
                o.ahead = max(0.0, o.ahead - qty)
        self._cap_ahead(o)

    def _cap_ahead(self, o: SimOrder) -> None:
        """Quantity ahead of us can never exceed what the level actually displays."""
        lv = (self.book.bid_levels if o.side == SIDE_BUY else self.book.ask_levels).get(o.price)
        tot = float(lv.total) if lv is not None else 0.0
        if o.ahead > tot:
            o.ahead = tot
