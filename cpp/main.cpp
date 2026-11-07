// itch_replay: C++17 ITCH 5.0 parser + order book replay, benchmark and slice extractor.
//
//   itch_replay replay  FILE [--symbol S] [--locate N] [--checkpoint-every K] [--dump-state F]
//                            [--json F] [--limit N] [--verify-every K]
//   itch_replay bench   FILE --mode parse|book|full [--symbol S] [--locate N] [--repeat R]
//                            [--lat-limit N] [--json F]
//   itch_replay extract FILE --symbols A,B --outdir D [--tag T]
//   itch_replay scan    FILE [--json F]
//
// FILE may be plain, .gz, or "-" for stdin.
#include <sys/resource.h>

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
#include <vector>

#include "book.hpp"
#include "itch.hpp"

using namespace itch;
using Clock = std::chrono::steady_clock;

static double secs(Clock::time_point a, Clock::time_point b) {
  return std::chrono::duration<double>(b - a).count();
}
static uint64_t peak_rss_bytes() {
  struct rusage ru;
  getrusage(RUSAGE_SELF, &ru);
#ifdef __APPLE__
  return uint64_t(ru.ru_maxrss);  // bytes on macOS
#else
  return uint64_t(ru.ru_maxrss) * 1024;  // KB on Linux
#endif
}

// ----------------------------------------------------------------------------- args
struct Args {
  std::string cmd, file, symbol, mode = "full", json, dump, symbols, outdir = ".", tag = "";
  int locate = -1;
  uint64_t checkpoint_every = 0, limit = 0, verify_every = 0, lat_limit = 0;
  int repeat = 1;
};
static Args parse_args(int argc, char** argv) {
  Args a;
  if (argc < 3) {
    std::fprintf(stderr, "usage: itch_replay replay|bench|extract|scan FILE [options]\n");
    std::exit(2);
  }
  a.cmd = argv[1];
  a.file = argv[2];
  for (int i = 3; i < argc; ++i) {
    std::string k = argv[i];
    auto val = [&]() -> std::string {
      if (i + 1 >= argc) { std::fprintf(stderr, "missing value for %s\n", k.c_str()); std::exit(2); }
      return argv[++i];
    };
    if (k == "--symbol") a.symbol = val();
    else if (k == "--locate") a.locate = std::stoi(val());
    else if (k == "--mode") a.mode = val();
    else if (k == "--json") a.json = val();
    else if (k == "--dump-state") a.dump = val();
    else if (k == "--symbols") a.symbols = val();
    else if (k == "--outdir") a.outdir = val();
    else if (k == "--tag") a.tag = val();
    else if (k == "--checkpoint-every") a.checkpoint_every = std::stoull(val());
    else if (k == "--limit") a.limit = std::stoull(val());
    else if (k == "--verify-every") a.verify_every = std::stoull(val());
    else if (k == "--lat-limit") a.lat_limit = std::stoull(val());
    else if (k == "--repeat") a.repeat = std::stoi(val());
    else { std::fprintf(stderr, "unknown option %s\n", k.c_str()); std::exit(2); }
  }
  return a;
}

// ----------------------------------------------------------------------------- replay core
struct Replayer {
  Book& book;
  bool has_locate = false;
  uint16_t locate = 0;
  uint64_t frames = 0, malformed = 0, unknown = 0, loc_mismatch = 0, informational = 0;
  bool first_set = false, session_start_seen = false;
  uint64_t first_ts = 0, last_ts = 0;
  uint64_t types[256] = {0};
  uint64_t ck_every = 0, verify_every = 0, verify_errors = 0;
  std::vector<std::pair<uint64_t, uint64_t>> checkpoints;

  explicit Replayer(Book& b) : book(b) {}

  inline void process(const uint8_t* m, uint32_t len) {
    ++frames;
    handle(m, len);
    if (ck_every && frames % ck_every == 0) checkpoints.emplace_back(frames, book.checksum());
    if (verify_every && frames % verify_every == 0) {
      const size_t e = book.verify();
      verify_errors += e;
      book.c.invariant_failures += e;
    }
  }

  inline void handle(const uint8_t* m, uint32_t len) {
    if (len == 0) { ++malformed; return; }
    ++types[m[0]];
    Event e;
    const Parse p = parse(m, len, e);
    switch (p) {
      case Parse::Unknown: ++unknown; return;
      case Parse::Malformed: ++malformed; return;
      case Parse::Info: ++informational; return;
      default: break;
    }
    last_ts = e.ts;
    if (!first_set) {
      first_set = true;
      first_ts = e.ts;
      if (e.type == T_SYSTEM && e.side == 'O') session_start_seen = true;
    }
    if (p == Parse::System || p == Parse::Trading) return;
    if (has_locate && e.locate != locate) { ++loc_mismatch; return; }
    if (p != Parse::Book) return;
    book.apply(e);
  }
};

// ----------------------------------------------------------------------------- JSON helpers
struct Json {
  std::string s;
  bool first = true;
  void sep() { if (!first) s += ","; first = false; }
  void raw(const char* k, const std::string& v) { sep(); s += "\"" + std::string(k) + "\":" + v; }
  void u64(const char* k, uint64_t v) { raw(k, std::to_string(v)); }
  void f64(const char* k, double v) { char b[64]; std::snprintf(b, sizeof b, "%.9g", v); raw(k, b); }
  void str(const char* k, const std::string& v) { raw(k, "\"" + v + "\""); }
  void b(const char* k, bool v) { raw(k, v ? "true" : "false"); }
};
static std::string hex64(uint64_t v) { char b[32]; std::snprintf(b, sizeof b, "%016" PRIx64, v); return b; }

static std::string counters_json(const Counters& c) {
  Json j;
#define X(n) { std::string nm = #n; if (nm.back() == '_') nm.pop_back(); j.u64(nm.c_str(), c.n); }
  ITCH_COUNTERS(X)
#undef X
  return "{" + j.s + "}";
}

static void write_file(const std::string& path, const std::string& body) {
  FILE* f = std::fopen(path.c_str(), "w");
  if (!f) { std::perror(path.c_str()); std::exit(1); }
  std::fputs(body.c_str(), f);
  std::fclose(f);
}

static void resolve_symbol(Args& a, Book& book, Replayer& rp) {
  if (!a.symbol.empty()) book.set_symbol(pack_symbol(a.symbol));
  if (a.locate < 0 && !a.symbol.empty()) {  // find locate from the stock directory
    const uint64_t want = pack_symbol(a.symbol);
    FrameSource src(a.file);
    const uint8_t* m; uint32_t len;
    while (src.next(m, len)) {
      if (m[0] == T_DIRECTORY && len >= 19 && rd64(m + 11) == want) { a.locate = rd16(m + 1); break; }
      if (m[0] == T_ADD || m[0] == T_ADD_MPID) break;
    }
    if (a.locate < 0) { std::fprintf(stderr, "symbol %s not found\n", a.symbol.c_str()); std::exit(1); }
  }
  if (a.locate >= 0) { rp.has_locate = true; rp.locate = uint16_t(a.locate); }
}

// ----------------------------------------------------------------------------- commands
static int cmd_replay(Args& a) {
  Book book;
  Replayer rp(book);
  rp.ck_every = a.checkpoint_every;
  rp.verify_every = a.verify_every;
  resolve_symbol(a, book, rp);
  FrameSource src(a.file);
  if (a.locate >= 0) src.set_filter({uint16_t(a.locate)});
  const auto t0 = Clock::now();
  const uint8_t* m; uint32_t len;
  while (src.next(m, len)) {
    rp.process(m, len);
    if (a.limit && rp.frames >= a.limit) break;
  }
  book.finalize();
  const double dt = secs(t0, Clock::now());
  const size_t final_errs = book.verify();
  book.c.invariant_failures += final_errs;

  Json j;
  j.str("impl", "cpp");
  j.u64("frames", rp.frames);
  j.str("checksum", hex64(book.checksum()));
  j.u64("orders", book.num_orders());
  j.f64("wall_seconds", dt);
  j.f64("msgs_per_sec", dt > 0 ? double(rp.frames) / dt : 0);
  j.u64("peak_rss_bytes", peak_rss_bytes());
  j.u64("first_ts", rp.first_ts);
  j.u64("last_ts", rp.last_ts);
  j.b("session_start_seen", rp.session_start_seen);
  Counters c = book.c;
  Json cj;
  {
    std::string base = counters_json(c);
    base.pop_back();  // drop '}' and append replay-level counters
    std::string extra;
    auto add = [&](const char* k, uint64_t v) { extra += ",\"" + std::string(k) + "\":" + std::to_string(v); };
    add("frames", rp.frames); add("malformed_messages", rp.malformed);
    add("unknown_message_type", rp.unknown); add("locate_mismatch", rp.loc_mismatch);
    add("informational_messages", rp.informational); add("bad_frames", src.bad_frames);
    add("truncated_tail_bytes", src.truncated_tail);
    j.raw("counters", base + extra + "}");
  }
  Json tj;
  for (int t = 0; t < 256; ++t)
    if (rp.types[t]) { char k[2] = {char(t), 0}; tj.u64(k, rp.types[t]); }
  j.raw("types", "{" + tj.s + "}");
  std::string ck = "[";
  for (size_t i = 0; i < rp.checkpoints.size(); ++i) {
    if (i) ck += ",";
    ck += "[" + std::to_string(rp.checkpoints[i].first) + ",\"" + hex64(rp.checkpoints[i].second) + "\"]";
  }
  j.raw("checkpoints", ck + "]");
  const std::string out = "{" + j.s + "}\n";
  if (a.json.empty()) std::fputs(out.c_str(), stdout); else write_file(a.json, out);
  if (!a.dump.empty()) {
    FILE* f = std::fopen(a.dump.c_str(), "w");
    book.dump(f);
    std::fclose(f);
  }
  return 0;
}

// ---- benchmark
struct Raw {
  std::vector<uint8_t> data;  // frames with 2-byte length prefix, filtered, in order
  uint64_t frames = 0;
};

static Raw load_raw(const Args& a) {
  Raw r;
  FrameSource src(a.file);
  if (a.locate >= 0) src.set_filter({uint16_t(a.locate)});
  const uint8_t* m; uint32_t len;
  while (src.next(m, len)) {
    r.data.push_back(uint8_t(len >> 8));
    r.data.push_back(uint8_t(len));
    r.data.insert(r.data.end(), m, m + len);
    ++r.frames;
    if (a.limit && r.frames >= a.limit) break;
  }
  return r;
}

struct Pcts { double p50, p95, p99, p999, max, mean; };
static Pcts pcts(std::vector<uint32_t>& v) {
  Pcts p{0, 0, 0, 0, 0, 0};
  if (v.empty()) return p;
  std::sort(v.begin(), v.end());
  auto at = [&](double q) { return double(v[std::min(v.size() - 1, size_t(q * double(v.size())))]); };
  p.p50 = at(0.50); p.p95 = at(0.95); p.p99 = at(0.99); p.p999 = at(0.999); p.max = double(v.back());
  double s = 0; for (auto x : v) s += x;
  p.mean = s / double(v.size());
  return p;
}
static std::string pcts_json(const Pcts& p) {
  Json j;
  j.f64("p50_ns", p.p50); j.f64("p95_ns", p.p95); j.f64("p99_ns", p.p99);
  j.f64("p99_9_ns", p.p999); j.f64("max_ns", p.max); j.f64("mean_ns", p.mean);
  return "{" + j.s + "}";
}
static uint64_t now_ns() {
  return uint64_t(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count());
}

static int cmd_bench(Args& a) {
  Book probe;
  Replayer prp(probe);
  resolve_symbol(a, probe, prp);
  const uint64_t sym = a.symbol.empty() ? 0 : pack_symbol(a.symbol);

  const auto t_read0 = Clock::now();
  Raw raw = load_raw(a);
  const double read_s = secs(t_read0, Clock::now());

  // timer characteristics
  double timer_overhead = 0;
  uint64_t timer_res = ~0ULL;
  {
    const int N = 200000;
    uint64_t prev = now_ns();
    const auto t0 = Clock::now();
    for (int i = 0; i < N; ++i) {
      uint64_t t = now_ns();
      if (t > prev && t - prev < timer_res) timer_res = t - prev;
      prev = t;
    }
    timer_overhead = secs(t0, Clock::now()) * 1e9 / N;
  }

  // pre-decoded events for the book-only stage
  std::vector<Event> events;
  if (a.mode == "book") {
    const uint8_t* p = raw.data.data();
    const uint8_t* end = p + raw.data.size();
    while (p < end) {
      const uint32_t len = rd16(p); p += 2;
      Event e;
      if (parse(p, len, e) == Parse::Book && (a.locate < 0 || e.locate == a.locate)) events.push_back(e);
      p += len;
    }
  }

  // Batch timer: one clock read per 64 operations; stores the mean per-op time of each batch.
  // This sees sub-tick (< timer resolution) costs that per-op timestamps quantize away.
  struct BatchTimer {
    uint64_t i = 0, tb = 0;
    std::vector<uint32_t> v;
    inline void tick() {
      if ((i++ & 63) == 0) {
        const uint64_t t = now_ns();
        if (tb) v.push_back(uint32_t((t - tb) / 64));
        tb = t;
      }
    }
  };
  BatchTimer bt;
  bool batch_pass = false;

  auto run_once = [&](bool latency, std::vector<uint32_t>* lat, uint64_t& sink, Book* out_book) -> double {
    Book book;
    if (sym) book.set_symbol(sym);
    Replayer rp(book);
    if (a.locate >= 0) { rp.has_locate = true; rp.locate = uint16_t(a.locate); }
    const uint8_t* p = raw.data.data();
    const uint8_t* end = p + raw.data.size();
    const auto t0 = Clock::now();
    if (batch_pass) {
      if (a.mode == "parse") {
        uint64_t s = 0;
        Event e;
        while (p < end) {
          const uint32_t len = rd16(p); p += 2;
          bt.tick();
          if (parse(p, len, e) == Parse::Book) s += e.ref ^ e.ts ^ e.price ^ (uint64_t(e.shares) << 7);
          p += len;
        }
        sink += s;
      } else if (a.mode == "book") {
        for (const Event& e : events) { bt.tick(); book.apply(e); }
      } else {
        while (p < end) {
          const uint32_t len = rd16(p); p += 2;
          bt.tick();
          rp.process(p, len);
          p += len;
        }
      }
      sink += book.checksum();
      return secs(t0, Clock::now());
    }
    if (a.mode == "parse") {
      uint64_t s = 0;
      Event e;
      if (!latency) {
        while (p < end) {
          const uint32_t len = rd16(p); p += 2;
          if (parse(p, len, e) == Parse::Book) s += e.ref ^ e.ts ^ e.price ^ (uint64_t(e.shares) << 7);
          p += len;
        }
      } else {
        while (p < end) {
          const uint32_t len = rd16(p); p += 2;
          const uint64_t t1 = now_ns();
          if (parse(p, len, e) == Parse::Book) s += e.ref ^ e.ts ^ e.price ^ (uint64_t(e.shares) << 7);
          const uint64_t t2 = now_ns();
          lat->push_back(uint32_t(t2 - t1));
          p += len;
        }
      }
      sink += s;
    } else if (a.mode == "book") {
      if (!latency) {
        for (const Event& e : events) book.apply(e);
      } else {
        for (const Event& e : events) {
          const uint64_t t1 = now_ns();
          book.apply(e);
          const uint64_t t2 = now_ns();
          lat->push_back(uint32_t(t2 - t1));
        }
      }
    } else {  // full: frame -> parse -> apply
      if (!latency) {
        while (p < end) {
          const uint32_t len = rd16(p); p += 2;
          rp.process(p, len);
          p += len;
        }
      } else {
        while (p < end) {
          const uint32_t len = rd16(p); p += 2;
          const uint64_t t1 = now_ns();
          rp.process(p, len);
          const uint64_t t2 = now_ns();
          lat->push_back(uint32_t(t2 - t1));
          p += len;
        }
      }
    }
    const double dt = secs(t0, Clock::now());
    sink += book.checksum();
    if (out_book) sink += book.num_orders();
    return dt;
  };

  uint64_t sink = 0;
  std::vector<double> times;
  for (int r = 0; r < std::max(1, a.repeat); ++r) times.push_back(run_once(false, nullptr, sink, nullptr));
  std::vector<double> sorted = times;
  std::sort(sorted.begin(), sorted.end());
  const double med = sorted[sorted.size() / 2];

  std::vector<uint32_t> lat;
  lat.reserve(a.mode == "book" ? events.size() : raw.frames);
  run_once(true, &lat, sink, nullptr);
  if (a.lat_limit && lat.size() > a.lat_limit) lat.resize(a.lat_limit);
  const uint64_t n_ops = (a.mode == "book") ? events.size() : raw.frames;
  const Pcts pc = pcts(lat);
  batch_pass = true;
  bt.v.reserve(n_ops / 64 + 8);
  run_once(false, nullptr, sink, nullptr);
  const Pcts pb = pcts(bt.v);

  Json j;
  j.str("impl", "cpp");
  j.str("stage", a.mode == "parse" ? "parser-only (framing + field decode)"
                 : a.mode == "book" ? "book-update-only (pre-decoded events)"
                                    : "complete replay (framing + decode + book, in memory)");
  j.u64("operations", n_ops);
  j.u64("input_bytes", raw.data.size());
  j.f64("read_seconds_incl_gunzip", read_s);
  j.f64("best_seconds", sorted.front());
  j.f64("median_seconds", med);
  j.f64("worst_seconds", sorted.back());
  j.f64("ops_per_sec_best", double(n_ops) / sorted.front());
  j.f64("ops_per_sec_median", double(n_ops) / med);
  j.raw("latency_per_op", pcts_json(pc));
  j.raw("latency_batch64_mean_per_op", pcts_json(pb));
  j.f64("timer_overhead_ns", timer_overhead);
  j.u64("timer_resolution_ns", timer_res);
  j.u64("peak_rss_bytes", peak_rss_bytes());
  j.u64("sink", sink);
  const std::string out = "{" + j.s + "}\n";
  if (a.json.empty()) std::fputs(out.c_str(), stdout); else write_file(a.json, out);
  return 0;
}

static int cmd_extract(Args& a) {
  std::vector<std::string> names;
  {
    size_t st = 0;
    while (st <= a.symbols.size()) {
      size_t e = a.symbols.find(',', st);
      if (e == std::string::npos) e = a.symbols.size();
      if (e > st) names.push_back(a.symbols.substr(st, e - st));
      st = e + 1;
    }
  }
  std::map<uint64_t, size_t> by_sym;
  std::vector<FILE*> outs;
  std::vector<uint64_t> counts(names.size(), 0);
  for (size_t i = 0; i < names.size(); ++i) {
    by_sym[pack_symbol(names[i])] = i;
    const std::string p = a.outdir + "/" + names[i] + "_" + a.tag + ".itch";
    FILE* f = std::fopen(p.c_str(), "wb");
    if (!f) { std::perror(p.c_str()); return 1; }
    std::setvbuf(f, nullptr, _IOFBF, 1 << 20);
    outs.push_back(f);
  }
  std::vector<int> loc2idx(65536, -1);
  FrameSource src(a.file);
  const uint8_t* m; uint32_t len;
  auto put = [&](size_t i, const uint8_t* p, uint32_t n) {
    const uint8_t h[2] = {uint8_t(n >> 8), uint8_t(n)};
    std::fwrite(h, 1, 2, outs[i]);
    std::fwrite(p, 1, n, outs[i]);
    ++counts[i];
  };
  const auto t0 = Clock::now();
  while (src.next(m, len)) {
    if (len == 0) continue;
    if (m[0] == T_SYSTEM) { for (size_t i = 0; i < outs.size(); ++i) put(i, m, len); continue; }
    if (len < 3) continue;
    const uint16_t loc = rd16(m + 1);
    if (m[0] == T_DIRECTORY && len >= 19) {
      auto it = by_sym.find(rd64(m + 11));
      if (it != by_sym.end()) loc2idx[loc] = int(it->second);
    }
    const int idx = loc2idx[loc];
    if (idx >= 0) put(size_t(idx), m, len);
  }
  const double dt = secs(t0, Clock::now());
  for (auto f : outs) std::fclose(f);
  Json j;
  j.f64("seconds", dt);
  j.u64("frames_scanned", src.scanned);
  Json cj;
  for (size_t i = 0; i < names.size(); ++i) cj.u64(names[i].c_str(), counts[i]);
  j.raw("frames_written", "{" + cj.s + "}");
  std::fputs(("{" + j.s + "}\n").c_str(), stdout);
  return 0;
}

static int cmd_scan(Args& a) {
  FrameSource src(a.file);
  uint64_t types[256] = {0}, n = 0, bad = 0, unk = 0;
  const uint8_t* m; uint32_t len;
  const auto t0 = Clock::now();
  while (src.next(m, len)) {
    ++n;
    if (len == 0) { ++bad; continue; }
    ++types[m[0]];
    const uint16_t w = msg_len(m[0]);
    if (!w) ++unk; else if (w != len) ++bad;
  }
  const double dt = secs(t0, Clock::now());
  Json j;
  j.str("impl", "cpp");
  j.u64("frames", n);
  j.f64("seconds_incl_gunzip", dt);
  j.f64("frames_per_sec", double(n) / dt);
  j.u64("bad_length", bad);
  j.u64("unknown_type", unk);
  j.u64("bytes_read", src.bytes_read);
  j.u64("peak_rss_bytes", peak_rss_bytes());
  Json tj;
  for (int t = 0; t < 256; ++t) if (types[t]) { char k[2] = {char(t), 0}; tj.u64(k, types[t]); }
  j.raw("types", "{" + tj.s + "}");
  const std::string out = "{" + j.s + "}\n";
  if (a.json.empty()) std::fputs(out.c_str(), stdout); else write_file(a.json, out);
  return 0;
}

int main(int argc, char** argv) {
  Args a = parse_args(argc, argv);
  try {
    if (a.cmd == "replay") return cmd_replay(a);
    if (a.cmd == "bench") return cmd_bench(a);
    if (a.cmd == "extract") return cmd_extract(a);
    if (a.cmd == "scan") return cmd_scan(a);
  } catch (const std::exception& e) {
    std::fprintf(stderr, "error: %s\n", e.what());
    return 1;
  }
  std::fprintf(stderr, "unknown command %s\n", a.cmd.c_str());
  return 2;
}
