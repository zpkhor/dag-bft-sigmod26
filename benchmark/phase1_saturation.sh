#!/usr/bin/env bash
# Phase 1: Sweep rates with balanced load to find saturation point.
set -euo pipefail

RATES=(10000 20000 40000 60000 100000 150000 200000)
RETRIES=3
DURATION=${DURATION:-120}
WARMUP=${WARMUP:-10}
TOKIO_THREADS=${TOKIO_THREADS:-16}

RESULTS_DIR="results/phase1_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"

cd "$(dirname "$0")"

echo "Phase 1: Saturation sweep (balanced load)"
echo "Rates: ${RATES[*]}"
echo "Duration: ${DURATION}s, Warmup: ${WARMUP}s, Retries: ${RETRIES}"
echo "Results: $RESULTS_DIR"
echo "==========================================="

for RATE in "${RATES[@]}"; do
    for RETRY in $(seq 1 "$RETRIES"); do
        RUN_DIR="$RESULTS_DIR/rate_${RATE}_run_${RETRY}"
        mkdir -p "$RUN_DIR"

        echo ""
        echo "--- Rate: $RATE tx/s, Run: $RETRY/$RETRIES ---"

        OUTPUT=$(TOKIO_THREADS=$TOKIO_THREADS RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP fab local 2>&1) || true
        echo "$OUTPUT" | tee "$RUN_DIR/output.log"

        # Copy logs for this run
        cp -r logs/* "$RUN_DIR/" 2>/dev/null || true

        # Check for errors/panics
        if echo "$OUTPUT" | grep -qiE 'panic|error|failed'; then
            echo "WARNING: Errors detected in rate=$RATE run=$RETRY"
            echo "$OUTPUT" | grep -iE 'panic|error|failed' > "$RUN_DIR/errors.log"
        fi

        sleep 2
    done
done

echo ""
echo "==========================================="
echo "Phase 1 complete. Results in $RESULTS_DIR"
