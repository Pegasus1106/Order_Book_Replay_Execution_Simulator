// Single-symbol limit order book (C++17). Behaviourally identical to itchlob/book.py:
// same event semantics, same anomaly counters, same state checksum.
//
//  * ref -> order: open-addressing hash table (linear probing, backward-shift delete)
//  * orders live in a pooled array and form an intrusive FIFO list per price level
//  * price levels: std::map per side (bids descending, asks ascending)
#pragma once
#include <cstdint>
#include <cstdio>
#include <functional>
#include <map>
#include <string>
#include <vector>

#include "itch.hpp"

namespace itch {

constexpr uint64_t CHECKSUM_SEED = 0xCBF29CE484222325ULL;
inline uint64_t mix(uint64_t h, uint64_t w) {
  h = (h ^ w) * 0x9E3779B97F4A7C15ULL;
  return h ^ (h >> 32);
}

#define ITCH_COUNTERS(X)                                                                          \
  X(add) X(exec) X(exec_px) X(cancel) X(delete_) X(replace) X(exec_shares) X(cancel_shares)       \
  X(unknown_ref) X(unknown_ref_exec) X(unknown_ref_cancel) X(unknown_ref_delete)                  \
  X(unknown_ref_replace) X(duplicate_ref) X(qty_inconsistency) X(qty_excess_exec)                 \
  X(qty_excess_cancel) X(zero_qty_event) X(bad_side) X(symbol_mismatch) X(locked_episodes)        \
  X(crossed_episodes) X(events_while_locked) X(events_while_crossed) X(max_locked_ns)             \
  X(max_crossed_ns) X(total_locked_ns) X(total_crossed_ns) X(invariant_failures)

struct Counters {
#define X(n) uint64_t n = 0;
  ITCH_COUNTERS(X)
#undef X
};

class RefMap {
 public:
  static constexpr uint32_t NIL = 0xFFFFFFFFu;
  explicit RefMap(size_t cap_pow2 = 1 << 16) { init(cap_pow2); }
  size_t size() const { return size_; }

  uint32_t find(uint64_t k) const {
    size_t i = h(k);
    while (keys_[i] != EMPTY) {
      if (keys_[i] == k) return vals_[i];
      i = (i + 1) & mask_;
    }
    return NIL;
  }
  void insert(uint64_t k, uint32_t v) {  // k must be absent
    if ((size_ + 1) * 2 > keys_.size()) grow();
    size_t i = h(k);
    while (keys_[i] != EMPTY) i = (i + 1) & mask_;
    keys_[i] = k; vals_[i] = v; ++size_;
  }
  void erase(uint64_t k) {
    size_t i = h(k);
    while (keys_[i] != k) {
      if (keys_[i] == EMPTY) return;
      i = (i + 1) & mask_;
    }
    size_t j = i;
    for (;;) {
      j = (j + 1) & mask_;
      if (keys_[j] == EMPTY) break;
      const size_t home = h(keys_[j]);
      // entry at j may move to i unless its home lies cyclically in (i, j]
      const bool in_range = (i <= j) ? (home > i && home <= j) : (home > i || home <= j);
      if (!in_range) { keys_[i] = keys_[j]; vals_[i] = vals_[j]; i = j; }
    }
    keys_[i] = EMPTY;
    --size_;
  }

 private:
  static constexpr uint64_t EMPTY = ~0ULL;
  size_t h(uint64_t k) const { return size_t((k * 0x9E3779B97F4A7C15ULL) >> shift_); }
  void init(size_t cap) {
    keys_.assign(cap, EMPTY); vals_.assign(cap, 0); mask_ = cap - 1; size_ = 0;
    shift_ = 64; for (size_t c = cap; c > 1; c >>= 1) --shift_;
  }
  void grow() {
    std::vector<uint64_t> ok = std::move(keys_);
    std::vector<uint32_t> ov = std::move(vals_);
    init(ok.size() * 2);
    for (size_t i = 0; i < ok.size(); ++i)
      if (ok[i] != EMPTY) insert(ok[i], ov[i]);
  }
  std::vector<uint64_t> keys_;
  std::vector<uint32_t> vals_;
  size_t mask_ = 0, size_ = 0;
  unsigned shift_ = 0;
};

class Book {
 public:
  static constexpr uint32_t NIL = RefMap::NIL;
  struct Level {
    uint64_t total = 0;
    uint32_t head = NIL, tail = NIL, count = 0;
  };
  struct Order {
    uint64_t ref;
    uint32_t price, shares, prev, next;
    uint8_t side;
    Level* lvl;
  };

  Book() { pool_.reserve(1 << 16); }
  void set_symbol(uint64_t packed) { sym_ = packed; has_sym_ = true; }

  Counters c;
  uint64_t last_ts = 0, ev_count = 0;

  // ---- mutations (return true if the book changed) ----
  bool add(uint64_t ts, uint64_t ref, uint8_t side, uint32_t shares, uint32_t price, uint64_t stock) {
    if (has_sym_ && stock != sym_) ++c.symbol_mismatch;
    if (side != SIDE_BUY && side != SIDE_SELL) { ++c.bad_side; return false; }
    if (shares == 0) { ++c.zero_qty_event; return false; }
    if (map_.find(ref) != NIL) { ++c.duplicate_ref; return false; }
    link_new(ref, side, price, shares);
    ++c.add;
    post(ts);
    return true;
  }

  bool del(uint64_t ts, uint64_t ref) {
    const uint32_t i = map_.find(ref);
    if (i == NIL) { ++c.unknown_ref; ++c.unknown_ref_delete; return false; }
    remove_order(i);
    ++c.delete_;
    post(ts);
    return true;
  }

  bool execute(uint64_t ts, uint64_t ref, uint32_t shares, bool with_price) {
    const uint32_t i = map_.find(ref);
    if (i == NIL) { ++c.unknown_ref; ++c.unknown_ref_exec; return false; }
    if (shares == 0) { ++c.zero_qty_event; return false; }
    const uint32_t rem = pool_[i].shares;
    if (shares > rem) { ++c.qty_inconsistency; c.qty_excess_exec += shares - rem; shares = rem; }
    reduce(i, shares);
    if (with_price) ++c.exec_px; else ++c.exec;
    c.exec_shares += shares;
    post(ts);
    return true;
  }

  bool cancel(uint64_t ts, uint64_t ref, uint32_t shares) {
    const uint32_t i = map_.find(ref);
    if (i == NIL) { ++c.unknown_ref; ++c.unknown_ref_cancel; return false; }
    if (shares == 0) { ++c.zero_qty_event; return false; }
    const uint32_t rem = pool_[i].shares;
    if (shares > rem) { ++c.qty_inconsistency; c.qty_excess_cancel += shares - rem; shares = rem; }
    reduce(i, shares);
    ++c.cancel;
    c.cancel_shares += shares;
    post(ts);
    return true;
  }

  bool replace(uint64_t ts, uint64_t old_ref, uint64_t new_ref, uint32_t shares, uint32_t price) {
    const uint32_t i = map_.find(old_ref);
    if (i == NIL) { ++c.unknown_ref; ++c.unknown_ref_replace; return false; }
    const uint8_t side = pool_[i].side;
    remove_order(i);
    if (shares == 0) { ++c.zero_qty_event; post(ts); return true; }
    if (map_.find(new_ref) != NIL) { ++c.duplicate_ref; post(ts); return true; }
    link_new(new_ref, side, price, shares);  // back of queue; side inherited
    ++c.replace;
    post(ts);
    return true;
  }

  bool apply(const Event& e) {
    switch (e.type) {
      case T_ADD: case T_ADD_MPID: return add(e.ts, e.ref, e.side, e.shares, e.price, e.stock);
      case T_DELETE: return del(e.ts, e.ref);
      case T_REPLACE: return replace(e.ts, e.ref, e.ref2, e.shares, e.price);
      case T_EXEC: return execute(e.ts, e.ref, e.shares, false);
      case T_CANCEL: return cancel(e.ts, e.ref, e.shares);
      case T_EXEC_PX: return execute(e.ts, e.ref, e.shares, true);
      default: return false;
    }
  }

  void finalize() { if (xstate_) cross_update(0, last_ts); }

  // ---- queries ----
  size_t num_orders() const { return map_.size(); }
  bool best_bid(uint32_t& p, uint64_t& q) const {
    if (bids_.empty()) return false;
    p = bids_.begin()->first; q = bids_.begin()->second.total; return true;
  }
  bool best_ask(uint32_t& p, uint64_t& q) const {
    if (asks_.empty()) return false;
    p = asks_.begin()->first; q = asks_.begin()->second.total; return true;
  }

  uint64_t checksum() const {
    uint64_t h = CHECKSUM_SEED;
    h = mix(h, map_.size());
    h = side_checksum(h, SIDE_BUY, bids_);
    h = side_checksum(h, SIDE_SELL, asks_);
    return h;
  }

  // Canonical text rendering, byte-identical to OrderBook.dump_lines() in Python.
  void dump(FILE* f) const {
    std::fprintf(f, "orders %zu\n", map_.size());
    dump_side(f, "BID", bids_);
    dump_side(f, "ASK", asks_);
  }

  // Full-book invariant check; returns number of violations.
  size_t verify() const {
    size_t errs = 0, seen = 0;
    auto side = [&](const auto& levels, uint8_t s) {
      for (const auto& kv : levels) {
        uint64_t tot = 0; uint32_t cnt = 0;
        if (kv.second.count == 0) ++errs;
        for (uint32_t i = kv.second.head; i != NIL; i = pool_[i].next) {
          const Order& o = pool_[i];
          tot += o.shares; ++cnt; ++seen;
          if (o.shares == 0 || o.side != s || o.price != kv.first || o.lvl != &kv.second) ++errs;
          if (map_.find(o.ref) != i) ++errs;
        }
        if (tot != kv.second.total || cnt != kv.second.count) ++errs;
      }
    };
    side(bids_, SIDE_BUY);
    side(asks_, SIDE_SELL);
    if (seen != map_.size()) ++errs;
    return errs;
  }

 private:
  using Bids = std::map<uint32_t, Level, std::greater<uint32_t>>;
  using Asks = std::map<uint32_t, Level>;

  uint32_t alloc() {
    if (free_ != NIL) { const uint32_t i = free_; free_ = pool_[i].next; return i; }
    pool_.emplace_back();
    return uint32_t(pool_.size() - 1);
  }

  void link_new(uint64_t ref, uint8_t side, uint32_t price, uint32_t shares) {
    Level* lv = (side == SIDE_BUY) ? &bids_[price] : &asks_[price];
    const uint32_t i = alloc();
    Order& o = pool_[i];
    o.ref = ref; o.price = price; o.shares = shares; o.side = side; o.lvl = lv;
    o.prev = lv->tail; o.next = NIL;
    if (lv->tail != NIL) pool_[lv->tail].next = i; else lv->head = i;
    lv->tail = i; ++lv->count; lv->total += shares;
    map_.insert(ref, i);
  }

  void unlink(uint32_t i) {  // removes from level list; erases level if it empties
    Order& o = pool_[i];
    Level* lv = o.lvl;
    if (o.prev != NIL) pool_[o.prev].next = o.next; else lv->head = o.next;
    if (o.next != NIL) pool_[o.next].prev = o.prev; else lv->tail = o.prev;
    --lv->count;
    if (lv->count == 0) {
      if (o.side == SIDE_BUY) bids_.erase(o.price); else asks_.erase(o.price);
    }
    map_.erase(o.ref);
    o.next = free_; free_ = i;
  }

  void remove_order(uint32_t i) {  // whole remaining quantity
    pool_[i].lvl->total -= pool_[i].shares;
    unlink(i);
  }

  void reduce(uint32_t i, uint32_t shares) {
    Order& o = pool_[i];
    o.shares -= shares;
    o.lvl->total -= shares;
    if (o.shares == 0) unlink(i);
  }

  void post(uint64_t ts) {
    last_ts = ts;
    ++ev_count;
    unsigned st = 0;
    if (!bids_.empty() && !asks_.empty()) {
      const uint32_t b = bids_.begin()->first, a = asks_.begin()->first;
      if (b >= a) st = (b > a) ? 2 : 1;
    }
    if (st || xstate_) cross_update(st, ts);
  }

  void cross_update(unsigned st, uint64_t ts) {
    const unsigned prev = xstate_;
    if (st) { if (st == 2) ++c.events_while_crossed; else ++c.events_while_locked; }
    if (st == prev) return;
    if (prev) {
      const uint64_t dur = ts - xstart_;
      if (prev == 2) { c.total_crossed_ns += dur; if (dur > c.max_crossed_ns) c.max_crossed_ns = dur; }
      else { c.total_locked_ns += dur; if (dur > c.max_locked_ns) c.max_locked_ns = dur; }
    }
    if (st) {
      if (st == 2) ++c.crossed_episodes; else ++c.locked_episodes;
      xstart_ = ts;
    }
    xstate_ = st;
  }

  template <class L>
  uint64_t side_checksum(uint64_t h, uint8_t side, const L& levels) const {
    h = mix(h, side);
    h = mix(h, levels.size());
    for (const auto& kv : levels) {
      h = mix(h, kv.first);
      h = mix(h, kv.second.total);
      h = mix(h, kv.second.count);
      for (uint32_t i = kv.second.head; i != NIL; i = pool_[i].next) {
        h = mix(h, pool_[i].ref);
        h = mix(h, pool_[i].shares);
      }
    }
    return h;
  }

  template <class L>
  void dump_side(FILE* f, const char* name, const L& levels) const {
    for (const auto& kv : levels) {
      std::fprintf(f, "%s %u %llu %u\n", name, kv.first, (unsigned long long)kv.second.total,
                   kv.second.count);
      for (uint32_t i = kv.second.head; i != NIL; i = pool_[i].next)
        std::fprintf(f, "  %llu %u\n", (unsigned long long)pool_[i].ref, pool_[i].shares);
    }
  }

  RefMap map_;
  std::vector<Order> pool_;
  uint32_t free_ = NIL;
  Bids bids_;
  Asks asks_;
  uint64_t sym_ = 0;
  bool has_sym_ = false;
  unsigned xstate_ = 0;
  uint64_t xstart_ = 0;
};

}  // namespace itch
