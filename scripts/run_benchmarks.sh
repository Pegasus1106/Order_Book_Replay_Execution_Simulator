#!/usr/bin/env bash
# Measure parser-only / book-update / complete-replay performance, Python and C++, one stage
# per process (so peak RSS is attributable). Run on an otherwise idle machine.
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=results/bench
mkdir -p "$OUT"
SYMS=${SYMS:-"AAPL SPY"}
REPEAT_PY=${REPEAT_PY:-3}
REPEAT_CPP=${REPEAT_CPP:-7}

make -C cpp itch_replay >/dev/null

for S in $SYMS; do
  F=data/slices/${S}_12302019.itch
  for STAGE in frame parse book replay e2e; do
    echo "python $S $STAGE"
    python3 -m itchlob bench "$F" --symbol "$S" --stage "$STAGE" --repeat "$REPEAT_PY" \
        --json "$OUT/py_${S}_${STAGE}.json" >/dev/null
  done
  for MODE in parse book full; do
    echo "cpp $S $MODE"
    ./cpp/itch_replay bench "$F" --symbol "$S" --mode "$MODE" --repeat "$REPEAT_CPP" \
        --json "$OUT/cpp_${S}_${MODE}.json"
  done
  echo "cpp $S e2e (file -> final checksum, includes read)"
  ./cpp/itch_replay replay "$F" --symbol "$S" --json "$OUT/cpp_${S}_e2e.json"
done

echo "cpp full-day scan (gunzip + framing + length validation of every message)"
./cpp/itch_replay scan data/12302019.NASDAQ_ITCH50.gz --json "$OUT/cpp_fullday_scan.json"
python3 scripts/summarize_bench.py
