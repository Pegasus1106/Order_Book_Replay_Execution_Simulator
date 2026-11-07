"""Full-book invariant verification (O(orders)); run periodically, not per event."""
from __future__ import annotations

from typing import List

from .book import OrderBook
from .messages import SIDE_BUY, SIDE_SELL


def verify_book(book: OrderBook, max_errors: int = 20) -> List[str]:
    """Return a list of violated invariants (empty when the book is consistent).

    1. Every order has shares > 0 (remaining quantity never <= 0).
    2. Level.total equals the sum of its constituent orders' remaining shares.
    3. The order map and the level queues describe exactly the same set of orders
       (no ghost orders in either direction), and each order sits in the level that
       matches its own side and price.
    4. Sorted price lists are strictly ascending and equal the level dictionary keys;
       there are no empty levels.
    """
    errs: List[str] = []

    def err(msg):
        if len(errs) < max_errors:
            errs.append(msg)

    seen = 0
    for side, levels, prices in ((SIDE_BUY, book.bid_levels, book.bid_prices),
                                 (SIDE_SELL, book.ask_levels, book.ask_prices)):
        s = chr(side)
        if any(prices[i] >= prices[i + 1] for i in range(len(prices) - 1)):
            err(f"{s}: price list not strictly ascending")
        if set(prices) != set(levels):
            err(f"{s}: price list and level map differ")
        for p, lv in levels.items():
            if not lv.orders:
                err(f"{s} {p}: empty level retained")
            if lv.price != p:
                err(f"{s} {p}: level.price {lv.price} mismatch")
            tot = 0
            for ref, o in lv.orders.items():
                tot += o.shares
                seen += 1
                if o.shares <= 0:
                    err(f"order {ref}: non-positive remaining {o.shares}")
                if o.ref != ref:
                    err(f"order {ref}: key/ref mismatch")
                if o.side != side or o.price != p:
                    err(f"order {ref}: sits in {s} {p} but is {chr(o.side)} {o.price}")
                if book.orders.get(ref) is not o:
                    err(f"order {ref}: in level {s} {p} but missing from order map")
            if tot != lv.total:
                err(f"{s} {p}: level total {lv.total} != sum of orders {tot}")
    if seen != len(book.orders):
        err(f"order map has {len(book.orders)} orders, levels hold {seen}")
    return errs
