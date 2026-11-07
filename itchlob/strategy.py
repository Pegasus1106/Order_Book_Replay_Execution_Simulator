"""A deliberately transparent passive-quoting strategy with a risk layer and metrics.

Two quoting rules share the same plumbing and the same risk limits:

  fixed             Join the best bid and best ask with a fixed size, always.
                    Inventory only matters through the hard position limit.

  inventory_aware   Starts from the same touch quotes and adjusts them using
                      * spread           quote only if there is a spread to earn
                      * best-level depth if the queue at the touch is deep and the
                                         spread allows it, improve by one tick to
                                         get ahead of the queue
                      * imbalance        top-N displayed imbalance: if bids dominate
                                         (price more likely to rise) do not offer, and
                                         vice versa
                      * volatility       rolling RMS of 1-second mid changes; above a
                                         threshold both quotes back off one tick
                      * inventory        long: bid retreats (skew_ticks * inv/max_pos),
                                         offer moves toward the touch, bid size shrinks;
                                         short: the mirror image

Risk layer (applies to both): maximum position, maximum order quantity, order-message
rate limit, stale-data stand-down, loss limit with kill switch + flatten, trading
window with end-of-window flatten.

This is not a claim of profitability. Parameters are round numbers that were not tuned
on the evaluation data; the point is to compare rules and sensitivities in a controlled
replay, subject to the counterfactual limits documented in sim.py.
"""
from __future__ import annotations

import math
from bisect import bisect_right
from array import array
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

from .messages import SIDE_BUY, SIDE_SELL
from .sim import ACTIVE, FILLED, PENDING, UNITS_PER_USD, ExecutionSimulator, Fill, SimOrder

S = 10 ** 9


@dataclass
class StrategyConfig:
    kind: str = "fixed"                     # "fixed" | "inventory_aware"
    quote_size: int = 100
    max_position: int = 500
    max_order_qty: int = 200
    tick: int = 100                         # $0.01 in price units
    eval_interval_ns: int = 1_000_000       # re-evaluate at most every 1 ms of event time
    window_start_ns: int = (9 * 3600 + 35 * 60) * S     # 09:35:00 ET
    window_end_ns: int = (15 * 3600 + 55 * 60) * S      # 15:55:00 ET
    # inventory_aware signal parameters
    skew_ticks: float = 2.0
    imbalance_levels: int = 3
    imbalance_thr: float = 0.6
    vol_widen_ticks: float = 4.0
    join_depth_max: int = 5000
    vol_sample_ns: int = 1 * S
    vol_window: int = 30
    # risk
    rate_limit_per_sec: int = 50
    stale_after_ns: int = 2 * S
    stale_resume_events: int = 3
    loss_limit_usd: float = 2500.0
    # reporting
    markout_horizons_ns: Tuple[int, ...] = (0, S // 10, 1 * S, 5 * S, 30 * S)
    sample_ns: int = 10 * S


class Portfolio:
    """Average-cost position accounting in price units (floats; reporting only)."""

    def __init__(self):
        self.inv = 0
        self.avg = 0.0
        self.realized = 0.0
        self.cash = 0.0
        self.fees = 0
        self.shares = 0
        self.notional = 0.0
        self.maker_shares = 0
        self.taker_shares = 0
        self.n_fills = 0
        self.max_abs_inv = 0

    def on_fill(self, f: Fill) -> None:
        sign = 1 if f.side == SIDE_BUY else -1
        q = f.qty
        inv = self.inv
        if inv == 0 or (inv > 0) == (sign > 0):
            tot = self.avg * abs(inv) + f.price * q
            self.inv = inv + sign * q
            self.avg = tot / abs(self.inv)
        else:
            closing = min(q, abs(inv))
            self.realized += closing * (f.price - self.avg) * (1 if inv > 0 else -1)
            self.inv = inv + sign * q
            if q > closing:
                self.avg = float(f.price)     # position flipped: new lot at fill price
            elif self.inv == 0:
                self.avg = 0.0
        self.cash -= sign * f.price * q
        self.fees += f.fee
        self.shares += q
        self.notional += f.price * q
        if f.liquidity == "M":
            self.maker_shares += q
        else:
            self.taker_shares += q
        self.n_fills += 1
        a = abs(self.inv)
        if a > self.max_abs_inv:
            self.max_abs_inv = a

    def unrealized(self, mid: float) -> float:
        return self.inv * (mid - self.avg) if self.inv else 0.0

    def pnl(self, mid: float) -> float:
        """Net P&L in price units: realized + unrealized - fees."""
        return self.realized + self.unrealized(mid) - self.fees


class _Slot:
    __slots__ = ("order", "sent", "cancel_sent")

    def __init__(self):
        self.order: Optional[SimOrder] = None
        self.sent: Optional[Tuple[int, int]] = None
        self.cancel_sent = False


class Strategy:
    def __init__(self, book, sim: ExecutionSimulator, cfg: Optional[StrategyConfig] = None):
        self.book = book
        self.sim = sim
        self.cfg = cfg or StrategyConfig()
        self.port = Portfolio()
        sim.on_fill = self.on_fill
        self.slots: Dict[int, _Slot] = {SIDE_BUY: _Slot(), SIDE_SELL: _Slot()}
        self.halted = False
        self.halt_ts: Optional[int] = None
        self.halt_reason = ""
        self.closing = False
        self.close_ts: Optional[int] = None
        self._flat: Optional[SimOrder] = None
        self._dirty = False
        self._next_eval = 0
        self._stale_flag = False
        self._fresh = 0
        self._msgs: Deque[int] = deque()
        c = self.cfg
        self._t_lo = c.window_start_ns - 60 * S
        self._rec_end = c.window_end_ns + max(c.markout_horizons_ns) + 5 * S
        self._mid_ts = array("q")
        self._mid_v = array("q")
        self._last_m2 = -1
        self._mids: Deque[float] = deque(maxlen=c.vol_window + 1)
        self._next_vol = 0
        self._next_sample = 0
        self.series: List[tuple] = []
        self.counters = {k: 0 for k in (
            "evaluations", "new_orders", "cancels", "replaces", "rate_limited", "stale_events",
            "stale_rejects", "standdown_no_spread", "position_limit_binding", "limit_breaches",
            "imbalance_suppressed", "vol_widened", "depth_improved", "flatten_orders")}

    # ------------------------------------------------------------------ hooks
    def pre(self, ts: int) -> None:
        """Before an event is applied to the book."""
        sim = self.sim
        if ts < self._t_lo:
            return
        b = self.book
        if ts <= self._rec_end:
            gap = ts - b.last_ts
            if gap > self.cfg.stale_after_ns and not self._stale_flag and self._in_window(b.last_ts):
                self._stale_flag = True
                self._fresh = 0
                self.counters["stale_events"] += 1
                self._pull_quotes(b.last_ts + self.cfg.stale_after_ns)
            if self._dirty and self._next_eval < ts:
                te = self._next_eval
                sim.advance(te)
                self._evaluate(te, te - b.last_ts)
                self._dirty = False
                self._next_eval = te + self.cfg.eval_interval_ns
        elif not sim._heap:
            return
        c = self.cfg
        if not self.closing and ts >= c.window_end_ns and b.last_ts >= c.window_start_ns:
            # window ended while the stock was quiet: close at the window boundary, not at the
            # next (possibly much later) event
            sim.advance(c.window_end_ns)
            self._begin_close(c.window_end_ns, "window_end")
            self._flatten(c.window_end_ns)
        sim.advance(ts)

    def post(self, ts: int) -> None:
        """After an event is applied to the book."""
        if ts < self._t_lo or ts > self._rec_end:
            return
        b = self.book
        bp = b.bid_prices
        ap = b.ask_prices
        if not bp or not ap:
            self._dirty = True
            return
        m2 = bp[-1] + ap[0]
        if m2 != self._last_m2:
            self._mid_ts.append(ts)
            self._mid_v.append(m2)
            self._last_m2 = m2
        c = self.cfg
        if ts < c.window_start_ns:
            return
        if ts >= self._next_vol:
            self._mids.append(m2 / 2.0)
            self._next_vol = ts + c.vol_sample_ns
        if ts >= self._next_sample:
            mid = m2 / 2.0
            p = self.port
            self.series.append((ts, p.inv, mid, p.realized, p.unrealized(mid), p.fees))
            self._next_sample = ts + c.sample_ns
        if self._stale_flag:
            self._fresh += 1
            if self._fresh >= c.stale_resume_events:
                self._stale_flag = False
            else:
                return
        if ts >= self._next_eval:
            self._next_eval = ts + c.eval_interval_ns
            self._dirty = False
            self._evaluate(ts, 0)
        else:
            self._dirty = True

    def on_fill(self, f: Fill) -> None:
        self.port.on_fill(f)
        if abs(self.port.inv) > self.cfg.max_position:
            self.counters["limit_breaches"] += 1

    # ------------------------------------------------------------------ decision logic
    def _in_window(self, ts: int) -> bool:
        return self.cfg.window_start_ns <= ts < self.cfg.window_end_ns

    def _allow(self, ts: int) -> bool:
        q = self._msgs
        while q and q[0] <= ts - S:
            q.popleft()
        if len(q) >= self.cfg.rate_limit_per_sec:
            self.counters["rate_limited"] += 1
            return False
        q.append(ts)
        return True

    def _live(self, o: Optional[SimOrder]) -> bool:
        return o is not None and (o.status == PENDING or o.status == ACTIVE)

    def _pull_quotes(self, ts: int) -> None:
        for slot in self.slots.values():
            o = slot.order
            if self._live(o) and not slot.cancel_sent:
                self.sim.cancel(ts, o.id)
                self.counters["cancels"] += 1
                slot.cancel_sent = True

    def _vol_ticks(self) -> Optional[float]:
        m = self._mids
        if len(m) < 6:
            return None
        s = 0.0
        prev = None
        n = 0
        for x in m:
            if prev is not None:
                d = x - prev
                s += d * d
                n += 1
            prev = x
        return math.sqrt(s / n) / self.cfg.tick

    def _begin_close(self, ts: int, reason: str, halt: bool = False) -> None:
        if halt:
            self.halted = True
            self.halt_ts = ts
            self.halt_reason = reason
        self.closing = True
        if self.close_ts is None:
            self.close_ts = ts
        self._pull_quotes(ts)

    def _flatten(self, ts: int) -> None:
        inv = self.port.inv
        if inv == 0:
            return
        if self._flat is not None and (self._flat.status == PENDING or
                                       ts - self._flat.decision_ts < S // 10):
            return                      # one flatten order in flight at a time, 100 ms apart
        side = SIDE_SELL if inv > 0 else SIDE_BUY
        qty = min(abs(inv), self.cfg.max_order_qty)
        self._flat = self.sim.submit_marketable(ts, side, qty)
        self.counters["flatten_orders"] += 1

    def _evaluate(self, ts: int, data_age: int) -> None:
        c = self.cfg
        b = self.book
        if ts < c.window_start_ns:
            return
        if self.closing:
            self._flatten(ts)
            return
        if ts >= c.window_end_ns:
            self._begin_close(ts, "window_end")
            return
        bp = b.bid_prices
        ap = b.ask_prices
        if not bp or not ap:
            return
        if data_age > c.stale_after_ns:
            self.counters["stale_rejects"] += 1
            return
        self.counters["evaluations"] += 1
        bb = bp[-1]
        ba = ap[0]
        mid = (bb + ba) / 2.0
        p = self.port
        if p.pnl(mid) <= -c.loss_limit_usd * UNITS_PER_USD:
            self._begin_close(ts, "loss_limit", halt=True)
            return
        spread = ba - bb
        if spread < c.tick:       # locked or crossed: do not quote into an unusual state
            self.counters["standdown_no_spread"] += 1
            self._reconcile(ts, SIDE_BUY, None)
            self._reconcile(ts, SIDE_SELL, None)
            return
        if c.kind == "fixed":
            bid, ask = self._targets_fixed(bb, ba, p.inv)
        else:
            bid, ask = self._targets_aware(bb, ba, spread, p.inv)
        self._reconcile(ts, SIDE_BUY, bid)
        self._reconcile(ts, SIDE_SELL, ask)

    def _sizes(self, inv: int) -> Tuple[int, int]:
        c = self.cfg
        q = min(c.quote_size, c.max_order_qty)
        bq = min(q, c.max_position - inv)
        sq = min(q, c.max_position + inv)
        if bq < q or sq < q:
            self.counters["position_limit_binding"] += 1
        return max(0, bq), max(0, sq)

    def _targets_fixed(self, bb, ba, inv):
        bq, sq = self._sizes(inv)
        return ((bb, bq) if bq > 0 else None), ((ba, sq) if sq > 0 else None)

    def _targets_aware(self, bb, ba, spread, inv):
        c = self.cfg
        t = c.tick
        b = self.book
        bq, sq = self._sizes(inv)
        ratio = max(-1.0, min(1.0, inv / c.max_position))
        bid_px, ask_px = bb, ba
        # depth-aware: deep queue at the touch and room inside the spread -> improve by a tick
        if spread >= 2 * t:
            if b.bid_levels[bb].total >= c.join_depth_max:
                bid_px = bb + t
                self.counters["depth_improved"] += 1
            if b.ask_levels[ba].total >= c.join_depth_max:
                ask_px = ba - t
                self.counters["depth_improved"] += 1
        # inventory skew
        if ratio > 0:
            bid_px -= t * int(round(c.skew_ticks * ratio))
            if ratio > 0.5 and ask_px - t > bb:
                ask_px -= t
            bq = int(bq * (1.0 - ratio))
        elif ratio < 0:
            ask_px += t * int(round(c.skew_ticks * -ratio))
            if ratio < -0.5 and bid_px + t < ba:
                bid_px += t
            sq = int(sq * (1.0 + ratio))
        # volatility: back off one tick on both sides
        vol = self._vol_ticks()
        if vol is not None and vol > c.vol_widen_ticks:
            bid_px -= t
            ask_px += t
            self.counters["vol_widened"] += 1
        # imbalance: do not offer into a bid-heavy book, do not bid into an ask-heavy one
        imb = b.imbalance(c.imbalance_levels)
        if imb is not None:
            if imb > c.imbalance_thr:
                sq = 0
                self.counters["imbalance_suppressed"] += 1
            elif imb < -c.imbalance_thr:
                bq = 0
                self.counters["imbalance_suppressed"] += 1
        # never cross or lock the market, never go non-positive
        bid_px = min(bid_px, ba - t)
        ask_px = max(ask_px, bb + t)
        bid = (bid_px, bq) if (bq > 0 and bid_px > 0) else None
        ask = (ask_px, sq) if sq > 0 else None
        return bid, ask

    def _reconcile(self, ts: int, side: int, target: Optional[Tuple[int, int]]) -> None:
        slot = self.slots[side]
        o = slot.order
        live = self._live(o)
        if target is None:
            if live and not slot.cancel_sent and self._allow(ts):
                self.sim.cancel(ts, o.id)
                self.counters["cancels"] += 1
                slot.cancel_sent = True
            return
        if live:
            if slot.cancel_sent:
                return                      # wait for the cancel to land, then re-quote
            if slot.sent != target and self._allow(ts):
                self.sim.replace(ts, o.id, target[0], target[1])
                self.counters["replaces"] += 1
                slot.sent = target
            return
        if self._allow(ts):
            slot.order = self.sim.submit_passive(ts, side, target[1], target[0])
            slot.sent = target
            slot.cancel_sent = False
            self.counters["new_orders"] += 1

    # ------------------------------------------------------------------ reporting
    def mid_at(self, ts: int) -> Optional[float]:
        i = bisect_right(self._mid_ts, ts) - 1
        return self._mid_v[i] / 2.0 if i >= 0 else None

    def markouts(self) -> Dict[str, dict]:
        """Signed mid move after each fill (price units/share; positive = favourable)."""
        c = self.cfg
        out: Dict[str, dict] = {}
        end_ts = self._mid_ts[-1] if len(self._mid_ts) else 0
        for liq, name in (("M", "passive"), ("T", "taker"), (None, "all")):
            fills = [f for f in self.sim.fills if liq is None or f.liquidity == liq]
            row = {}
            for h in c.markout_horizons_ns:
                num = den = n = 0
                for f in fills:
                    if f.ts + h > min(end_ts, self._rec_end):
                        continue
                    m = self.mid_at(f.ts + h)
                    if m is None:
                        continue
                    sign = 1 if f.side == SIDE_BUY else -1
                    num += sign * (m - f.price) * f.qty
                    den += f.qty
                    n += 1
                row[f"{h / S:g}s"] = {"units_per_share": (num / den) if den else None,
                                      "cents_per_share": (num / den / 100.0) if den else None,
                                      "fills": n, "shares": den}
            out[name] = row
        return out

    def summary(self) -> dict:
        c = self.cfg
        p = self.port
        sim = self.sim
        last_mid = self.mid_at(self._rec_end) or (self._mid_v[-1] / 2.0 if len(self._mid_v) else 0.0)
        unreal = p.unrealized(last_mid)
        st = sim.stats
        posted = st["passive_posted_qty"]
        inst = st["passive_instances"]
        invs = [abs(s[1]) for s in self.series]
        mean_abs = sum(invs) / len(invs) if invs else 0.0
        to_usd = 1.0 / UNITS_PER_USD
        # cash-flow identity: cash + inv*mark == realized + unrealized (fees excluded)
        ident_gap = abs((p.cash + p.inv * last_mid) - (p.realized + unreal))
        return {
            "kind": c.kind,
            "fills": p.n_fills, "shares_traded": p.shares,
            "maker_shares": p.maker_shares, "taker_shares": p.taker_shares,
            "turnover_usd": p.notional * to_usd,
            "fees_usd": p.fees * to_usd,
            "realized_pnl_usd": p.realized * to_usd,
            "unrealized_pnl_usd": unreal * to_usd,
            "net_pnl_usd": (p.realized + unreal - p.fees) * to_usd,
            "net_pnl_per_share_cents": ((p.realized + unreal - p.fees) / p.shares / 100.0
                                        if p.shares else None),
            "final_inventory": p.inv,
            "max_abs_inventory": p.max_abs_inv, "mean_abs_inventory_sampled": mean_abs,
            "passive_instances": inst, "passive_instances_filled": st["passive_instances_filled"],
            "fill_rate_orders": st["passive_instances_filled"] / inst if inst else None,
            "passive_posted_shares": posted, "passive_filled_shares": st["passive_filled_qty"],
            "fill_rate_shares": st["passive_filled_qty"] / posted if posted else None,
            "halted": self.halted, "halt_reason": self.halt_reason,
            "halt_time_s": (self.halt_ts / S) if self.halt_ts else None,
            "markouts": self.markouts(),
            "sim_stats": dict(st), "strategy_counters": dict(self.counters),
            "accounting_identity_gap_units": ident_gap,
        }
