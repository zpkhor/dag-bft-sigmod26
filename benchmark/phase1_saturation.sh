#!/usr/bin/env bash
# Phase 1: Sweep rates with balanced load to find saturation point.
set -euo pipefail

RATES=(11000)
RRS=(0)
OPEN_LOOPS=(1)
BANDWIDTHS_MBPS_LIST=("50,50,50,50" "50,50,50,30" "50,50,30,30" "50,30,30,30")
RETRIES=2
DURATION=${DURATION:-120}
WARMUP=${WARMUP:-10}
RESULTS_DIR="results/phase1_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"
OUTPUT_LOG="$RESULTS_DIR/merged_output.log" # /home/zpkhor/narwhal/benchmark/results/phase1_20260228_140003/merged_output.log

cd "$(dirname "$0")"

echo "Phase 1: Saturation sweep (balanced load)"
echo "Rates: ${RATES[*]}"
echo "Open loops: ${OPEN_LOOPS[*]}"
echo "Bandwidths: ${BANDWIDTHS_MBPS_LIST[*]}"
echo "Duration: ${DURATION}s, Warmup: ${WARMUP}s, Retries: ${RETRIES}"
echo "Results: $RESULTS_DIR"
echo "==========================================="

for RR in "${RRS[@]}"; do
    for BW in "${BANDWIDTHS_MBPS_LIST[@]}"; do
        BW_LABEL="${BW//,/_}"
        for OPEN_LOOP in "${OPEN_LOOPS[@]}"; do
            for RATE in "${RATES[@]}"; do
                for RETRY in $(seq 1 "$RETRIES"); do
                    RUN_DIR="$RESULTS_DIR/rate_${RATE}_ol_${OPEN_LOOP}_bw_${BW_LABEL}_run_${RETRY}"
                    mkdir -p "$RUN_DIR"

                    echo ""
                    echo "--- Rate: $RATE tx/s, Open loop: $OPEN_LOOP, BW: $BW, Run: $RETRY/$RETRIES ---"

                    # echo "CMD: RR=$RR OPEN_LOOP=$OPEN_LOOP RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP fab docker --cpus-per-validator=16  --bandwidth=50mbit --latency=100ms --primary-bw=10mbit" | tee -a "$OUTPUT_LOG"
                    # OUTPUT=$(RR=$RR OPEN_LOOP=$OPEN_LOOP RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP fab docker --cpus-per-validator=16  --bandwidth=50mbit --latency=100ms --primary-bw=10mbit 2>&1) || true
                    echo "CMD: RR=$RR OPEN_LOOP=$OPEN_LOOP RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP BANDWIDTHS_MBPS=$BW fab docker --cpus-per-validator=16 --primary-bw=10mbit" | tee -a "$OUTPUT_LOG"
                    OUTPUT=$(RR=$RR OPEN_LOOP=$OPEN_LOOP RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP BANDWIDTHS_MBPS=$BW fab docker --cpus-per-validator=16 --primary-bw=10mbit 2>&1) || true
                    echo "$OUTPUT" | tee "$RUN_DIR/output.log" >> "$OUTPUT_LOG"

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
        done
    done
done

echo ""
echo "==========================================="
echo "Phase 1 complete. Results in $RESULTS_DIR/merged_output.log"
