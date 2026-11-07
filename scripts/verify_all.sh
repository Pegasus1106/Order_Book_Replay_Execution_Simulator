#!/usr/bin/env bash
# Re-run every correctness check. Exit status is non-zero if anything disagrees.
#   1. synthetic fuzz with injected anomalies: Python vs C++ (counters, checkpoints, full state dump)
#   2. real slices, six symbols: Python vs C++ at the end of the day and at a mid-session cut
#   3. determinism: repeat runs, snapshot-resume, empty-book refusal, paced vs unpaced
#   4. C++ slice extractor vs Python slice extractor (byte-for-byte)
set -euo pipefail
cd "$(dirname "$0")/.."
make -C cpp itch_replay itch_replay_asan >/dev/null
mkdir -p results
SL="data/slices"; TAG=12302019
FILES="$SL/AAPL_$TAG.itch $SL/MSFT_$TAG.itch $SL/SPY_$TAG.itch $SL/AMD_$TAG.itch $SL/INTC_$TAG.itch $SL/NIO_$TAG.itch"
SYMS=AAPL,MSFT,SPY,AMD,INTC,NIO

echo "== 1. fuzz (Python vs C++)"
python3 -m itchlob fuzz --seeds 8 --events 200000 | tee results/fuzz.txt
echo "   C++ under ASan/UBSan on an anomaly-heavy stream"
./cpp/itch_replay_asan replay results/tmp/fuzz_1.itch --symbol TEST --locate 7 --verify-every 5000 \
    --checkpoint-every 10000 --json /tmp/asan.json
python3 -c "import json; d=json.load(open('/tmp/asan.json')); assert d['counters']['invariant_failures']==0; print('   ASan run clean, invariant_failures=0')"

echo "== 2. real data, end of day (checkpoint every 100k frames)"
python3 -m itchlob compare $FILES --symbols $SYMS --checkpoint-every 100000 | tee results/compare_py_cpp.txt
echo "== 2b. real data, mid-session cut at frame 700,000 (thousands of live orders)"
python3 -m itchlob compare $FILES --symbols $SYMS --checkpoint-every 100000 --limit 700000 \
    --tmpdir results/tmp_mid | tee results/compare_py_cpp_midday.txt

echo "== 3. determinism"
python3 -m itchlob determinism $SL/AAPL_$TAG.itch --symbol AAPL --runs 3 --limit 1000000 \
    --paced-frames 30000 --paced-speed 1000 | tee results/determinism_AAPL.txt

if [ -f data/$TAG.NASDAQ_ITCH50.gz ]; then
  echo "== 4. extractor equivalence"
  mkdir -p results/tmp_cpp_slices
  ./cpp/itch_replay extract data/$TAG.NASDAQ_ITCH50.gz --symbols $SYMS --outdir results/tmp_cpp_slices --tag $TAG >/dev/null
  for s in ${SYMS//,/ }; do cmp "$SL/${s}_$TAG.itch" "results/tmp_cpp_slices/${s}_$TAG.itch" && echo "   $s: identical"; done
fi
echo "ALL CHECKS PASSED"
