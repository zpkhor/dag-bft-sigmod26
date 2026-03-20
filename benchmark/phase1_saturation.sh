#!/usr/bin/env bash
# Phase 1: Sweep rates with balanced load to find saturation point.
set -euo pipefail

RATES=(11800 14800 15000 15100)
BWS=(75)
ROUTING_MODES=("" "round-robin")
RETRIES=3
DURATION=${DURATION:-80}
WARMUP=${WARMUP:-8}
RESULTS_DIR="$(pwd)/results/phase1_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"
OUTPUT_LOG="$RESULTS_DIR/merged_output.log" # hilbit2:/home/zpkhor/narwhal/benchmark/results/phase1_20260320_182449/merged_output.log

cd "$(dirname "$0")"

echo "Phase 1: Saturation sweep (balanced load)"
echo "Rates: ${RATES[*]}"
echo "BWs: ${BWS[*]}mbit"
echo "Routing modes: ${ROUTING_MODES[*]}"
echo "Duration: ${DURATION}s, Warmup: ${WARMUP}s, Retries: ${RETRIES}"
echo "Results: $RESULTS_DIR"
echo "==========================================="

for RATE in "${RATES[@]}"; do
    for BW in "${BWS[@]}"; do
        for ROUTING_MODE in "${ROUTING_MODES[@]}"; do
            for RETRY in $(seq 1 "$RETRIES"); do
                RUN_DIR="$RESULTS_DIR/rate_${RATE}_bw_${BW}mbit_routing_${ROUTING_MODE:-none}_run_${RETRY}"
                mkdir -p "$RUN_DIR"

                echo ""
                echo "--- Rate: $RATE tx/s, BW: ${BW}mbit, RoutingMode: ${ROUTING_MODE:-none}, Run: $RETRY/$RETRIES ---"

                echo "CMD: BASELINE=1 RATE=$RATE ROUTING_MODE=${ROUTING_MODE} DURATION=$DURATION WARMUP=$WARMUP fab docker --cpus-per-validator=16 --worker-bw=${BW}mbit --latency=100ms --primary-bw=25mbit" | tee -a "$OUTPUT_LOG"
                OUTPUT=$(BASELINE=1 RATE=$RATE ROUTING_MODE=$ROUTING_MODE DURATION=$DURATION WARMUP=$WARMUP fab docker --cpus-per-validator=16 --worker-bw="${BW}mbit" --latency=100ms --primary-bw=25mbit 2>&1) || true
                echo "$OUTPUT" | tee "$RUN_DIR/output.log" >> "$OUTPUT_LOG"

                # Copy logs for this run
                cp -r logs/* "$RUN_DIR/" 2>/dev/null || true

                # Check for errors/panics
                if echo "$OUTPUT" | grep -qiE 'panic|error|failed'; then
                    echo "WARNING: Errors detected in rate=$RATE bw=${BW}mbit routing=${ROUTING_MODE:-none} run=$RETRY"
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
