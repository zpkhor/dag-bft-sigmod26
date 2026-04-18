#!/usr/bin/env bash
# Run benchmark/tps_timeline.py on every run subdir of a fig_3_tps_timeline_lb.sh results dir.
# Saves output to <run_dir>/tps_timeline.csv; skips runs that already have it.
#
# Usage: bash benchmark/exp/tps_timeline_batch.sh <RESULTS_DIR> [--bin SECONDS] [--warmup SECONDS]
set -euo pipefail

RESULTS_DIR="${1:-}"
if [[ -z "$RESULTS_DIR" || ! -d "$RESULTS_DIR" ]]; then
    echo "Usage: $0 <RESULTS_DIR> [--bin SECONDS] [--warmup SECONDS]" >&2
    exit 1
fi
shift

BIN_ARG=""
WARMUP_ARG=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --bin) BIN_ARG="--bin $2"; shift 2 ;;
        --warmup) WARMUP_ARG="--warmup $2"; shift 2 ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done
RESULTS_DIR="$(cd "$RESULTS_DIR" && pwd)"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$BENCH_DIR"

n_processed=0
n_skipped=0
n_failed=0

for subdir in "$RESULTS_DIR"/*/; do
    [[ -d "$subdir" ]] || continue

    if [[ ! -f "$subdir/bench-params.json" ]]; then
        echo "Skipping $subdir (no bench-params.json)"
        (( n_skipped++ )) || true
        continue
    fi

    out="$subdir/tps_timeline.csv"

    if [[ -f "$out" ]]; then
        echo "Skipping $subdir (tps_timeline.csv exists)"
        (( n_skipped++ )) || true
        continue
    fi

    echo "Processing $subdir"
    if python tps_timeline.py "$subdir" $BIN_ARG $WARMUP_ARG > "$out"; then
        echo "  -> $out"
        (( n_processed++ )) || true
    else
        echo "WARNING: failed for $subdir" >&2
        rm -f "$out"
        (( n_failed++ )) || true
    fi
done

echo ""
echo "Done. processed=$n_processed skipped=$n_skipped failed=$n_failed"
