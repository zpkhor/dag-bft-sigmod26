#!/usr/bin/env bash
# Phase 1: Sweep rates with routing modes and netem queue limits to find saturation point.
set -euo pipefail

ROUTING_MODES=("" "round-robin")
TC_NETEM_LIMIT_CLIENT=(1000 1000000)
RATES=(8600 10000 11000)
RETRIES=1
DURATION=${DURATION:-120}
WARMUP=${WARMUP:-10}
RESULTS_DIR="results/phase1_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"
OUTPUT_LOG="$RESULTS_DIR/merged_output.log" # /home/zpkhor/narwhal/benchmark/results/phase1_20260317_163235/merged_output.log

cd "$(dirname "$0")"

echo "Phase 1: Saturation sweep (routing mode x netem limit)"
echo "Routing modes: ${ROUTING_MODES[*]:-baseline}"
echo "Netem limits: ${TC_NETEM_LIMIT_CLIENT[*]}"
echo "Rates: ${RATES[*]}"
echo "Duration: ${DURATION}s, Warmup: ${WARMUP}s, Retries: ${RETRIES}"
echo "Results: $RESULTS_DIR"
echo "==========================================="

for RMODE in "${ROUTING_MODES[@]}"; do
    RMODE_LABEL="${RMODE:-baseline}"
    for LIMIT in "${TC_NETEM_LIMIT_CLIENT[@]}"; do
        for RATE in "${RATES[@]}"; do
            for RETRY in $(seq 1 "$RETRIES"); do
                RUN_DIR="$RESULTS_DIR/${RMODE_LABEL}_limit_${LIMIT}_rate_${RATE}_run_${RETRY}"
                mkdir -p "$RUN_DIR"

                echo ""
                echo "--- Mode: $RMODE_LABEL, Limit: $LIMIT, Rate: $RATE tx/s, Run: $RETRY/$RETRIES ---"

                echo "CMD: ROUTING_MODE=$RMODE TC_NETEM_LIMIT_CLIENT=$LIMIT RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP fab docker --cpus-per-validator=16 --bandwidth=50mbit --latency=100ms --primary-bw=10mbit" | tee -a "$OUTPUT_LOG"
                OUTPUT=$(ROUTING_MODE=$RMODE TC_NETEM_LIMIT_CLIENT=$LIMIT RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP fab docker --cpus-per-validator=16 --bandwidth=50mbit --latency=100ms --primary-bw=10mbit 2>&1) || true
                echo "$OUTPUT" | tee "$RUN_DIR/output.log" >> "$OUTPUT_LOG"

                # Copy logs for this run
                cp -r logs/* "$RUN_DIR/" 2>/dev/null || true

                # Check for errors/panics
                if echo "$OUTPUT" | grep -qiE 'panic|error|failed'; then
                    echo "WARNING: Errors detected in mode=$RMODE_LABEL limit=$LIMIT rate=$RATE run=$RETRY"
                    echo "$OUTPUT" | grep -iE 'panic|error|failed' > "$RUN_DIR/errors.log"
                fi

                sleep 2
            done
        done
    done
done

echo ""
echo "==========================================="
echo "Phase 1 complete. Results in $RESULTS_DIR/merged_output.log"
