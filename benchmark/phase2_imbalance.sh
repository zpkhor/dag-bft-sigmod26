#!/usr/bin/env bash
# Phase 2: Sweep imbalance ratios at chosen rate levels.
# Set RATES env var based on Phase 1 results (~30%, ~70%, ~90% of saturation).
set -euo pipefail

RATES=(${RATES:-6000 3000})
RATE_WEIGHTS_LIST=("1,1,1,1" "10,1,1,1") # /home/zpkhor/narwhal-validator/benchmark/results/phase2_20260310_160943/merged_output.log
# RATE_WEIGHTS_LIST=("1,1,1,1" "2,1,1,1" "10,1,1,1" "20,1,1,1" "100,1,1,1")
# RATE_WEIGHTS_LIST=("2,2,1,1" "6,6,1,1" "7,7,7,1")
RETRIES=2
DURATION=${DURATION:-120}
WARMUP=${WARMUP:-5}
RESULTS_DIR="results/phase2_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"
OUTPUT_LOG="$RESULTS_DIR/merged_output.log"

cd "$(dirname "$0")"

echo "Phase 2: Imbalance sweep"
echo "Rates: ${RATES[*]}"
echo "Weights: ${RATE_WEIGHTS_LIST[*]}"
echo "Duration: ${DURATION}s, Warmup: ${WARMUP}s, Retries: ${RETRIES}"
echo "Results: $RESULTS_DIR"
echo "==========================================="

for RATE in "${RATES[@]}"; do
    for WEIGHTS in "${RATE_WEIGHTS_LIST[@]}"; do
        for RETRY in $(seq 1 "$RETRIES"); do
            WEIGHTS_TAG=$(echo "$WEIGHTS" | tr ',' '_')
            RUN_DIR="$RESULTS_DIR/rate_${RATE}_w_${WEIGHTS_TAG}_run_${RETRY}"
            mkdir -p "$RUN_DIR"

            echo ""
            echo "--- Rate: $RATE tx/s, Weights: $WEIGHTS, Run: $RETRY/$RETRIES ---"

            echo "CMD: RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP RATE_WEIGHTS=$WEIGHTS fab docker --cpus-per-validator=16 --bandwidth=50mbit --latency=100ms --primary-bw=10mbit" | tee -a "$OUTPUT_LOG"
            OUTPUT=$(RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP RATE_WEIGHTS=$WEIGHTS fab docker --cpus-per-validator=16 --bandwidth=50mbit --latency=100ms --primary-bw=10mbit 2>&1) || true
            echo "$OUTPUT" | tee "$RUN_DIR/output.log" >> "$OUTPUT_LOG"

            # Copy logs for this run
            cp -r logs/* "$RUN_DIR/" 2>/dev/null || true

            # Check for errors/panics
            if echo "$OUTPUT" | grep -qiE 'panic|error|failed'; then
                echo "WARNING: Errors detected in rate=$RATE weights=$WEIGHTS run=$RETRY"
                echo "$OUTPUT" | grep -iE 'panic|error|failed' > "$RUN_DIR/errors.log"
            fi

            sleep 2
        done
    done
done

echo ""
echo "==========================================="
echo "Phase 2 complete. Results in $RESULTS_DIR/merged_output.log"
