#!/usr/bin/env bash
# Phase 1: Sweep rates with balanced load to find saturation point.
set -euo pipefail

RATES=(1000 15000 20000 22000)
LATENCIES=(100 200)
BASELINES=("" "1")
RETRIES=2
DURATION=${DURATION:-120}
WARMUP=${WARMUP:-10}
RESULTS_DIR="results/phase1_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"
OUTPUT_LOG="$RESULTS_DIR/merged_output.log" # /home/zpkhor/narwhal/benchmark/results/phase1_20260318_121451/merged_output.log

cd "$(dirname "$0")"

echo "Phase 1: Saturation sweep (balanced load)"
echo "Rates: ${RATES[*]}"
echo "Latencies: ${LATENCIES[*]}ms"
echo "Baselines: ${BASELINES[*]}"
echo "Duration: ${DURATION}s, Warmup: ${WARMUP}s, Retries: ${RETRIES}"
echo "Results: $RESULTS_DIR"
echo "==========================================="

for RATE in "${RATES[@]}"; do
    for LATENCY in "${LATENCIES[@]}"; do
        for BASELINE in "${BASELINES[@]}"; do
            for RETRY in $(seq 1 "$RETRIES"); do
                RUN_DIR="$RESULTS_DIR/rate_${RATE}_latency_${LATENCY}ms_baseline_${BASELINE:-0}_run_${RETRY}"
                mkdir -p "$RUN_DIR"

                echo ""
                echo "--- Rate: $RATE tx/s, Latency: ${LATENCY}ms, Baseline: ${BASELINE:-0}, Run: $RETRY/$RETRIES ---"

                echo "CMD: RATE=$RATE BASELINE=${BASELINE:-0} DURATION=$DURATION WARMUP=$WARMUP fab docker --cpus-per-validator=16 --bandwidth=100mbit --latency=${LATENCY}ms --primary-bw=25mbit" | tee -a "$OUTPUT_LOG"
                OUTPUT=$(RATE=$RATE BASELINE=$BASELINE DURATION=$DURATION WARMUP=$WARMUP fab docker --cpus-per-validator=16 --bandwidth=100mbit --latency="${LATENCY}ms" --primary-bw=25mbit 2>&1) || true
                echo "$OUTPUT" | tee "$RUN_DIR/output.log" >> "$OUTPUT_LOG"

                # Copy logs for this run
                cp -r logs/* "$RUN_DIR/" 2>/dev/null || true

                # Check for errors/panics
                if echo "$OUTPUT" | grep -qiE 'panic|error|failed'; then
                    echo "WARNING: Errors detected in rate=$RATE latency=${LATENCY}ms baseline=${BASELINE:-0} run=$RETRY"
                    echo "$OUTPUT" | grep -iE 'panic|error|failed' > "$RUN_DIR/errors.log"
                fi

                sleep 2
            done
        done
    done
done

echo ""
echo "==========================================="
echo "Plotting combined phase1 figure..."
python plot_phase1.py "$RESULTS_DIR" -o "$RESULTS_DIR/phase1.png" || \
    echo "WARNING: combined plot failed"

echo ""
echo "==========================================="
echo "Phase 1 complete. Results in $RESULTS_DIR/merged_output.log"
