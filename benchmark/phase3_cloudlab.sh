#!/usr/bin/env bash
# Phase 3 CloudLab: Compare balanced vs imbalance configs.
# Each config entry specifies its own rate, so the loop is configs × retries.
set -euo pipefail

RETRIES=${RETRIES:-2}
DURATION=${DURATION:-120}
WARMUP=${WARMUP:-10}
cd "$(dirname "$0")"
RESULTS_DIR="$(pwd)/results/phase3_cloudlab_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"
OUTPUT_LOG="$RESULTS_DIR/merged_output.log" # hilbit1:/home/zpkhor/narwhal-validator/benchmark/results/phase3_cloudlab_20260322_195657/

check_certified_tps_consistency() {
    local run_dir="$1"
    local logs_dir="$2"
    local output_log="$3"
    local check_log="$run_dir/certified_tps_check.log"
    local errors_log="$run_dir/errors.log"

    { echo "=== certified_tps consistency check ==="; date -Iseconds; } > "$check_log"

    mapfile -t primary_logs < <(ls "$logs_dir"/primary-*.log 2>/dev/null | sort)
    if [[ ${#primary_logs[@]} -eq 0 ]]; then
        echo "ERROR: certified_tps check: no primary logs under $logs_dir" | tee -a "$errors_log" >> "$output_log"
        return 1
    fi

    local tmpdir
    tmpdir=$(mktemp -d "$run_dir/.ctps_XXXXXX")
    local total_lines=0
    for f in "${primary_logs[@]}"; do
        local norm="$tmpdir/$(basename "$f" .log).norm"
        grep "certified_tps" "$f" \
            | sed -E 's/.*round=([0-9]+)\) validator ([^:]+): (.*)/\1 \2 \3/' \
            > "$norm" || true
        local c
        c=$(wc -l < "$norm")
        echo "$(basename "$f"): $c lines" >> "$check_log"
        total_lines=$((total_lines + c))
    done

    if [[ "$total_lines" -eq 0 ]]; then
        echo "ERROR: certified_tps check: zero lines across ${#primary_logs[@]} primaries" \
            | tee -a "$errors_log" >> "$output_log"
        rm -rf "$tmpdir"
        return 1
    fi

    local n_unique_lines n_unique_keys
    n_unique_lines=$(cat "$tmpdir"/*.norm | sort -u | wc -l)
    n_unique_keys=$(cat "$tmpdir"/*.norm | sort -k1,2 -u | wc -l)
    echo "unique lines: $n_unique_lines, unique (round,validator): $n_unique_keys" >> "$check_log"

    if [[ "$n_unique_lines" -ne "$n_unique_keys" ]]; then
        {
            echo "CONFLICT: $n_unique_lines unique value-lines vs $n_unique_keys unique (round,validator) keys"
            echo "--- entries by frequency ---"
            cat "$tmpdir"/*.norm | sort | uniq -c | sort -rn | head -40
        } >> "$check_log"
        echo "ERROR: certified_tps values differ across primaries for same (round,validator). Details: $check_log" \
            | tee -a "$errors_log" >> "$output_log"
        rm -rf "$tmpdir"
        return 1
    fi

    rm -rf "$tmpdir"
}

# Each entry: "label|BANDWIDTHS_MBPS|RATE_WEIGHTS|BASELINE|RATE"
CONFIGS=(
    "balanced_r15000|75,75,75,75|1,1,1,1|1|15000"
    "balanced_r11800|75,75,75,75|1,1,1,1|1|11800"
    "balanced_r5000|75,75,75,75|1,1,1,1|1|5000"
    "imbalanced_r11800|75,75,75,75|5,1,1,1|1|11800"
    "imbalanced_bw1_r15000|25,75,75,75|1,1,1,1|1|15000"
    "imbalanced_bw1_r11800|25,75,75,75|1,1,1,1|1|11800"
    "imbalanced_bw1_r5000|25,75,75,75|1,1,1,1|1|5000"
    "imbalanced_bw2_r7000|25,25,75,75|1,1,1,1|1|11800"
    "imbalanced_bw2_r4000|25,25,75,75|1,1,1,1|1|5000"
)

# Validate unique labels
declare -A seen_labels
for CONFIG in "${CONFIGS[@]}"; do
    IFS='|' read -r LABEL _ _ _ _ <<< "$CONFIG"
    if [[ -v seen_labels["$LABEL"] ]]; then
        echo "ERROR: Duplicate label '$LABEL' in CONFIGS" >&2
        exit 1
    fi
    seen_labels["$LABEL"]=1
done

echo "Phase 3 CloudLab: Scenario comparison"
echo "Duration: ${DURATION}s, Warmup: ${WARMUP}s, Retries: ${RETRIES}"
echo "Results: $RESULTS_DIR"
echo "==========================================="

for CONFIG in "${CONFIGS[@]}"; do
    IFS='|' read -r LABEL BANDWIDTHS_MBPS RATE_WEIGHTS BASELINE RATE <<< "$CONFIG"

    for RETRY in $(seq 1 "$RETRIES"); do
        RUN_DIR="$RESULTS_DIR/${LABEL}_run_${RETRY}"
        mkdir -p "$RUN_DIR"

        echo ""
        echo "--- $LABEL | bw=$BANDWIDTHS_MBPS rate_w=$RATE_WEIGHTS baseline=$BASELINE rate=$RATE Run: $RETRY/$RETRIES ---"

        CMD="WORKER_BANDWIDTHS_MBPS=$BANDWIDTHS_MBPS RATE_WEIGHTS=$RATE_WEIGHTS BASELINE=$BASELINE RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP fab cloudlab --latency-ms=100"
        echo "CMD: $CMD" | tee -a "$OUTPUT_LOG"
        OUTPUT=$(WORKER_BANDWIDTHS_MBPS=$BANDWIDTHS_MBPS RATE_WEIGHTS=$RATE_WEIGHTS BASELINE=$BASELINE RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP fab cloudlab --latency-ms=100 2>&1) || true
        echo "$OUTPUT" | tee "$RUN_DIR/output.log" >> "$OUTPUT_LOG"

        cp -r logs/* "$RUN_DIR/" 2>/dev/null || true
        check_certified_tps_consistency "$RUN_DIR" "$RUN_DIR" "$OUTPUT_LOG" || true

        if echo "$OUTPUT" | grep -qiE 'panic|error|failed'; then
            echo "WARNING: Errors detected in $LABEL run=$RETRY"
            echo "$OUTPUT" | grep -iE 'panic|error|failed' > "$RUN_DIR/errors.log"
        fi

        sleep 2
    done
done

echo ""
echo "==========================================="
echo "Plotting combined phase3 CloudLab figure..."
python plot_phase3_cloudlab.py "$RESULTS_DIR" -o "$RESULTS_DIR/phase3-cloudlab.png" || \
    echo "WARNING: combined plot failed"

echo ""
echo "==========================================="
echo "Phase 3 CloudLab complete. Results in $RESULTS_DIR"
