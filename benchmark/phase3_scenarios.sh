#!/usr/bin/env bash
# Phase 3: Compare balanced vs imbalance configs across multiple rates.
set -euo pipefail

RETRIES=${RETRIES:-2}
DURATION=${DURATION:-120}
WARMUP=${WARMUP:-10}
cd "$(dirname "$0")"
RESULTS_DIR="$(pwd)/results/phase3_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"
OUTPUT_LOG="$RESULTS_DIR/merged_output.log" # hilbit2:/home/zpkhor/narwhal/benchmark/results/phase3_20260315_204913/merged_output.log

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

    # Normalize each primary's certified_tps lines to: "round validator rest_of_values"
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

    # For each (round, validator) appearing in multiple primaries, values must agree.
    # n_unique_lines: distinct full lines (round + validator + values)
    # n_unique_keys:  distinct (round, validator) pairs
    # If n_unique_lines > n_unique_keys, some (round,validator) has conflicting values.
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

# Each entry: "label|BANDWIDTHS_MBPS|RATE_WEIGHTS"
CONFIGS=(
    "balanced|50,50,50,50|1,1,1,1"
    "imbalance_rate_5|50,50,50,50|5,1,1,1"
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


RATES=(2700 6500 8700)

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
            check_certified_tps_consistency "$RUN_DIR" "$RUN_DIR/logs" "$OUTPUT_LOG" || true

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
