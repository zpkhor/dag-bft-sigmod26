#!/usr/bin/env bash
# Phase 3: Compare balanced vs imbalance configs across multiple rates.
set -euo pipefail

RETRIES=${RETRIES:-2}
DURATION=${DURATION:-120}
WARMUP=${WARMUP:-10}
RESULTS_DIR="results/phase3_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"
OUTPUT_LOG="$RESULTS_DIR/merged_output.log" # hilbit2:/home/zpkhor/narwhal/benchmark/results/phase3_20260312_230816/merged_output.log

cd "$(dirname "$0")"

# Each entry: "label|BANDWIDTHS_MBPS|RATE_WEIGHTS"
# CONFIGS=(
#     "balanced|50,50,50,50|1,1,1,1"
#     "imbalance_rate|50,50,50,50|5,1,1,1"
#     "imbalance_bw1|30,50,50,50|1,1,1,1"
#     "imbalance_bw2|30,30,50,50|1,1,1,1"
#     "imbalance_bw_rate|30,30,50,50|3,3,1,1"
# )
CONFIGS=(
    "balanced|50,50,50,50|1,1,1,1"
    "imbalance_rate_5|50,50,50,50|5,1,1,1"
    "imbalance_rate_10|50,50,50,50|10,1,1,1"
    "imbalance_bw1|30,50,50,50|1,1,1,1"
    "imbalance_bw2|30,30,50,50|1,1,1,1"
    "imbalance_bw3|30,30,30,50|1,1,1,1"
)

# Validate unique labels
declare -A seen_labels
for CONFIG in "${CONFIGS[@]}"; do
    IFS='|' read -r LABEL _ _ <<< "$CONFIG"
    if [[ -v seen_labels["$LABEL"] ]]; then
        echo "ERROR: Duplicate label '$LABEL' in CONFIGS" >&2
        exit 1
    fi
    seen_labels["$LABEL"]=1
done


RATES=(2700 6500 8800)

echo "Phase 3: Scenario comparison"
echo "Duration: ${DURATION}s, Warmup: ${WARMUP}s, Retries: ${RETRIES}"
echo "Results: $RESULTS_DIR"
echo "==========================================="

for CONFIG in "${CONFIGS[@]}"; do
    IFS='|' read -r LABEL BANDWIDTHS_MBPS RATE_WEIGHTS <<< "$CONFIG"

    for RATE in "${RATES[@]}"; do
        for RETRY in $(seq 1 "$RETRIES"); do
            RUN_DIR="$RESULTS_DIR/${LABEL}_rate${RATE}_run_${RETRY}"
            mkdir -p "$RUN_DIR"

            echo ""
            echo "--- $LABEL | bw=$BANDWIDTHS_MBPS rate_w=$RATE_WEIGHTS rate=$RATE Run: $RETRY/$RETRIES ---"

            CMD="BANDWIDTHS_MBPS=$BANDWIDTHS_MBPS RATE_WEIGHTS=$RATE_WEIGHTS RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP fab docker --cpus-per-validator=12 --bandwidth=50mbit --latency=100ms --primary-bw=10mbit"
            echo "CMD: $CMD" | tee -a "$OUTPUT_LOG"
            OUTPUT=$(BANDWIDTHS_MBPS=$BANDWIDTHS_MBPS RATE_WEIGHTS=$RATE_WEIGHTS RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP fab docker --cpus-per-validator=12 --bandwidth=50mbit --latency=100ms --primary-bw=10mbit 2>&1) || true
            echo "$OUTPUT" | tee "$RUN_DIR/output.log" >> "$OUTPUT_LOG"

            mv logs "$RUN_DIR/logs" 2>/dev/null || true

            if echo "$OUTPUT" | grep -qiE 'panic|error|failed'; then
                echo "WARNING: Errors detected in $LABEL run=$RETRY"
                echo "$OUTPUT" | grep -iE 'panic|error|failed' > "$RUN_DIR/errors.log"
            fi

            sleep 2
        done
    done
done

echo ""
echo "==========================================="
echo "Plotting combined phase3 figure..."
python plot_phase3.py "$RESULTS_DIR" -o "$RESULTS_DIR/phase3.png" || \
    echo "WARNING: combined plot failed"

echo ""
echo "==========================================="
echo "Phase 3 complete. Results in $RESULTS_DIR/merged_output.log"
