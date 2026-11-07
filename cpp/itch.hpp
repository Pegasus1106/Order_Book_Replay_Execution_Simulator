// Nasdaq TotalView-ITCH 5.0 message parsing and framing (C++17).
// Mirrors itchlob/messages.py + itchlob/framing.py. All integers are big-endian.
#pragma once
#include <zlib.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace itch {

constexpr uint8_t T_SYSTEM = 'S', T_DIRECTORY = 'R', T_TRADING = 'H', T_ADD = 'A', T_ADD_MPID = 'F',
                  T_EXEC = 'E', T_EXEC_PX = 'C', T_CANCEL = 'X', T_DELETE = 'D', T_REPLACE = 'U';
constexpr uint8_t SIDE_BUY = 'B', SIDE_SELL = 'S';

inline uint16_t rd16(const uint8_t* p) { return uint16_t((p[0] << 8) | p[1]); }
inline uint32_t rd32(const uint8_t* p) {
  return (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) | (uint32_t(p[2]) << 8) | p[3];
}
inline uint64_t rd48(const uint8_t* p) {
  return (uint64_t(p[0]) << 40) | (uint64_t(p[1]) << 32) | (uint64_t(p[2]) << 24) |
         (uint64_t(p[3]) << 16) | (uint64_t(p[4]) << 8) | p[5];
}
inline uint64_t rd64(const uint8_t* p) { return (uint64_t(rd32(p)) << 32) | rd32(p + 4); }

// Expected total length per message type per the 5.0 spec; 0 = type not defined.
inline uint16_t msg_len(uint8_t t) {
  switch (t) {
    case 'S': return 12; case 'R': return 39; case 'H': return 25; case 'Y': return 20;
    case 'L': return 26; case 'V': return 35; case 'W': return 12; case 'K': return 28;
    case 'J': return 35; case 'h': return 21; case 'A': return 36; case 'F': return 40;
    case 'E': return 31; case 'C': return 36; case 'X': return 23; case 'D': return 19;
    case 'U': return 35; case 'P': return 44; case 'Q': return 40; case 'B': return 19;
    case 'I': return 50; case 'N': return 20; case 'O': return 48; default: return 0;
  }
}

// Decoded event. Field use by type:
//  A/F: ref, side, shares, price, stock     E/C: ref, shares     X: ref, shares
//  D: ref                                    U: ref (old), ref2 (new), shares, price
//  S: side holds the event code              H: stock, side holds the trading state
struct Event {
  uint64_t ts = 0;
  uint64_t ref = 0;
  uint64_t ref2 = 0;
  uint64_t stock = 0;  // 8 ASCII bytes, big-endian packed
  uint32_t shares = 0;
  uint32_t price = 0;
  uint16_t locate = 0;
  uint8_t type = 0;
  uint8_t side = 0;
};

enum class Parse : uint8_t { Book, System, Trading, Directory, Info, Malformed, Unknown };

// Same classification as itchlob.replay: decoder types are length-checked strictly.
inline Parse parse(const uint8_t* m, uint32_t len, Event& e) {
  if (len == 0) return Parse::Malformed;
  const uint8_t t = m[0];
  const uint16_t want = msg_len(t);
  if (want == 0) return Parse::Unknown;
  if (len != want) return Parse::Malformed;
  e.type = t;
  switch (t) {
    case T_ADD: case T_ADD_MPID:
      e.locate = rd16(m + 1); e.ts = rd48(m + 5); e.ref = rd64(m + 11); e.side = m[19];
      e.shares = rd32(m + 20); e.stock = rd64(m + 24); e.price = rd32(m + 32);
      return Parse::Book;
    case T_EXEC: case T_EXEC_PX:
      e.locate = rd16(m + 1); e.ts = rd48(m + 5); e.ref = rd64(m + 11); e.shares = rd32(m + 19);
      return Parse::Book;
    case T_CANCEL:
      e.locate = rd16(m + 1); e.ts = rd48(m + 5); e.ref = rd64(m + 11); e.shares = rd32(m + 19);
      return Parse::Book;
    case T_DELETE:
      e.locate = rd16(m + 1); e.ts = rd48(m + 5); e.ref = rd64(m + 11);
      return Parse::Book;
    case T_REPLACE:
      e.locate = rd16(m + 1); e.ts = rd48(m + 5); e.ref = rd64(m + 11); e.ref2 = rd64(m + 19);
      e.shares = rd32(m + 27); e.price = rd32(m + 31);
      return Parse::Book;
    case T_SYSTEM:
      e.locate = rd16(m + 1); e.ts = rd48(m + 5); e.side = m[11];
      return Parse::System;
    case T_TRADING:
      e.locate = rd16(m + 1); e.ts = rd48(m + 5); e.stock = rd64(m + 11); e.side = m[19];
      return Parse::Trading;
    case T_DIRECTORY:
      e.locate = rd16(m + 1); e.ts = rd48(m + 5); e.stock = rd64(m + 11);
      return Parse::Directory;
    default:
      return Parse::Info;
  }
}

inline uint64_t pack_symbol(const std::string& s) {
  uint8_t b[8];
  std::memset(b, ' ', 8);
  for (size_t i = 0; i < s.size() && i < 8; ++i) b[i] = uint8_t(s[i]);
  return rd64(b);
}

// Streaming frame reader (plain or .gz via zlib, "-" = stdin). Optionally filters by locate
// (keeping system events), exactly like itchlob.framing.FrameReader.
class FrameSource {
 public:
  explicit FrameSource(const std::string& path, size_t chunk = 1 << 24) : buf_(chunk * 2) {
    f_ = (path == "-") ? gzdopen(0, "rb") : gzopen(path.c_str(), "rb");
    if (!f_) throw std::runtime_error("cannot open " + path);
    gzbuffer(f_, 1 << 20);
    chunk_ = chunk;
  }
  ~FrameSource() { if (f_) gzclose(f_); }
  FrameSource(const FrameSource&) = delete;
  FrameSource& operator=(const FrameSource&) = delete;

  void set_filter(const std::vector<uint16_t>& locs) {
    filter_ = true;
    keep_.assign(65536, 0);
    for (auto l : locs) keep_[l] = 1;
  }

  // Next kept frame; false at EOF.
  bool next(const uint8_t*& msg, uint32_t& len) {
    for (;;) {
      if (pos_ + 2 <= end_) {
        const uint32_t n = rd16(&buf_[pos_]);
        if (pos_ + 2 + n <= end_) {
          const uint8_t* p = &buf_[pos_ + 2];
          pos_ += 2 + n;
          ++scanned;
          if (filter_) {
            if (n < 3) { ++bad_frames; continue; }
            if (!(keep_[rd16(p + 1)] || p[0] == T_SYSTEM)) continue;
          }
          msg = p; len = n; ++yielded;
          return true;
        }
      }
      if (eof_) { truncated_tail = end_ - pos_; return false; }
      // refill: move leftover to the front
      const size_t left = end_ - pos_;
      if (left) std::memmove(buf_.data(), buf_.data() + pos_, left);
      pos_ = 0; end_ = left;
      const int got = gzread(f_, buf_.data() + end_, unsigned(buf_.size() - end_));
      if (got <= 0) { eof_ = true; continue; }
      end_ += size_t(got);
      bytes_read += size_t(got);
    }
  }

  uint64_t scanned = 0, yielded = 0, bad_frames = 0, truncated_tail = 0, bytes_read = 0;

 private:
  gzFile f_ = nullptr;
  std::vector<uint8_t> buf_;
  size_t chunk_ = 0, pos_ = 0, end_ = 0;
  bool eof_ = false, filter_ = false;
  std::vector<uint8_t> keep_;
};

}  // namespace itch
