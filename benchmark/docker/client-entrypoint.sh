#!/bin/bash
set -e

# Apply TC latency to traffic NOT going to own validator
if [ -n "$TC_LATENCY" ] && [ "$TC_LATENCY" != "0ms" ]; then
    tc qdisc add dev eth0 root handle 1: prio bands 2 priomap 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1
    # Band 0: own validator — no delay
    tc filter add dev eth0 parent 1: protocol ip u32 match ip dst ${OWN_VALIDATOR_IP}/32 flowid 1:1
    # Band 1 (default): other validators — add latency
    JITTER_ARG=""
    if [ -n "$TC_JITTER" ] && [ "$TC_JITTER" != "0ms" ]; then
        JITTER_ARG="$TC_JITTER"
    fi
    tc qdisc add dev eth0 parent 1:2 handle 20: netem delay $TC_LATENCY $JITTER_ARG

    echo "tc latency rules applied: own_validator=$OWN_VALIDATOR_IP (no delay), others=${TC_LATENCY} jitter=${TC_JITTER:-none}"
    tc qdisc show dev eth0
fi

PIDS=()

cleanup() {
    echo "Shutting down client..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait
    exit 0
}
trap cleanup SIGTERM SIGINT

eval "${CLIENT_CMD}" &
PIDS+=($!)

# Wait for all children
wait
