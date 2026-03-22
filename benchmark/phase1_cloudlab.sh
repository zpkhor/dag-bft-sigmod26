#!/usr/bin/env bash
# Phase 1 CloudLab: Sweep rates with balanced load to find saturation point.
# Takes one or more manifest files and loops over them sequentially
# (they share working directory state so cannot run in parallel).
# Usage:
#   ./phase1_cloudlab.sh multi-client-imbalance-bw1.xml multi-client-imbalance-bw2.xml
set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 <manifest1.xml> [manifest2.xml ...]"
    exit 1
fi

MANIFESTS=("$@")

RATES=(1000 5000 14000)
RETRIES=2
DURATION=${DURATION:-80}
WARMUP=${WARMUP:-8}
RESULTS_DIR="$(pwd)/results/phase1_cloudlab_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"
OUTPUT_LOG="$RESULTS_DIR/merged_output.log"

cd "$(dirname "$0")"

echo "Phase 1 CloudLab: Saturation sweep"
echo "Manifests: ${MANIFESTS[*]}"
echo "Rates: ${RATES[*]}"
echo "Duration: ${DURATION}s, Warmup: ${WARMUP}s, Retries: ${RETRIES}"
echo "Results: $RESULTS_DIR"
echo "==========================================="

for MANIFEST in "${MANIFESTS[@]}"; do
    MANIFEST_STEM="$(basename "$MANIFEST" .xml)"
    for RATE in "${RATES[@]}"; do
        for RETRY in $(seq 1 "$RETRIES"); do
            RUN_DIR="$RESULTS_DIR/manifest_${MANIFEST_STEM}/rate_${RATE}_run_${RETRY}"
            mkdir -p "$RUN_DIR"

            echo ""
            echo "--- Manifest: $MANIFEST_STEM, Rate: $RATE tx/s, Run: $RETRY/$RETRIES ---"

            echo "CMD: BASELINE=1 RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP fab cloudlab --manifest=$MANIFEST --latency-ms=100" | tee -a "$OUTPUT_LOG"
            OUTPUT=$(BASELINE=1 RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP fab cloudlab --manifest="$MANIFEST" --latency-ms=100 2>&1) || true
            echo "$OUTPUT" | tee "$RUN_DIR/output.log" >> "$OUTPUT_LOG"

            # Copy logs for this run
            cp -r logs/* "$RUN_DIR/" 2>/dev/null || true

            # Check for errors/panics
            if echo "$OUTPUT" | grep -qiE 'panic|error|failed'; then
                echo "WARNING: Errors detected in manifest=$MANIFEST_STEM rate=$RATE run=$RETRY"
                echo "$OUTPUT" | grep -iE 'panic|error|failed' > "$RUN_DIR/errors.log"
            fi

            sleep 2
        done
    done
done

echo ""
echo "==========================================="
echo "Plotting combined phase1 CloudLab figure..."
python plot_phase1_cloudlab.py "$RESULTS_DIR" -o "$RESULTS_DIR/phase1-cloudlab.png" || \
    echo "WARNING: combined plot failed"

echo ""
echo "==========================================="
echo "Phase 1 CloudLab complete. Results in $RESULTS_DIR"
