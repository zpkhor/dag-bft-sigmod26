#!/bin/bash
set -e

# Apply tc egress shaping on eth0 if bandwidth is specified
if [ -n "$TC_BANDWIDTH" ] && [ "$TC_BANDWIDTH" != "0" ]; then
    tc qdisc add dev eth0 root handle 1: htb default 10
    tc class add dev eth0 parent 1: classid 1:1 htb rate $TC_BANDWIDTH
    tc class add dev eth0 parent 1:1 classid 1:10 htb rate $TC_BANDWIDTH ceil $TC_BANDWIDTH

    if [ -n "$TC_LATENCY" ] && [ "$TC_LATENCY" != "0ms" ]; then
        JITTER_ARG=""
        if [ -n "$TC_JITTER" ] && [ "$TC_JITTER" != "0ms" ]; then
            JITTER_ARG="$TC_JITTER"
        fi
        tc qdisc add dev eth0 parent 1:10 handle 10: netem delay $TC_LATENCY $JITTER_ARG limit ${TC_NETEM_LIMIT:-1000000}
    fi

    echo "tc rules applied: bandwidth=$TC_BANDWIDTH latency=${TC_LATENCY:-none} jitter=${TC_JITTER:-none}"
    tc qdisc show dev eth0

    # Exempt replies to own client from latency (own client is colocated)
    if [ -n "$OWN_CLIENT_IP" ] && [ -n "$TC_LATENCY" ] && [ "$TC_LATENCY" != "0ms" ]; then
        tc class add dev eth0 parent 1:1 classid 1:20 htb rate $TC_BANDWIDTH ceil $TC_BANDWIDTH
        tc filter add dev eth0 parent 1:0 protocol ip u32 match ip dst ${OWN_CLIENT_IP}/32 flowid 1:20
        echo "tc: exempt own client $OWN_CLIENT_IP from latency"
    fi
fi

# Apply ingress shaping on eth0 via IFB device
if [ -n "$TC_BANDWIDTH" ] && [ "$TC_BANDWIDTH" != "0" ]; then
    ip link add ifb0 type ifb 2>/dev/null || true
    ip link set dev ifb0 up
    tc qdisc add dev eth0 handle ffff: ingress
    tc filter add dev eth0 parent ffff: protocol ip u32 match u32 0 0 \
        action mirred egress redirect dev ifb0
    tc qdisc add dev ifb0 root handle 1: htb default 10
    tc class add dev ifb0 parent 1: classid 1:10 htb rate $TC_BANDWIDTH
    echo "ingress shaping applied via ifb0: rate=$TC_BANDWIDTH"
fi

# Apply tc shaping on lo if LAN bandwidth is specified
if [ -n "$TC_LAN_BANDWIDTH" ] && [ "$TC_LAN_BANDWIDTH" != "0" ]; then
    tc qdisc add dev lo root handle 1: htb default 10
    tc class add dev lo parent 1: classid 1:10 htb rate $TC_LAN_BANDWIDTH
    echo "tc rules applied on lo: lan_bandwidth=$TC_LAN_BANDWIDTH"
    tc qdisc show dev lo
fi

# Set tokio threads if specified
ENV_PREFIX=""
if [ -n "$TOKIO_WORKER_THREADS" ] && [ "$TOKIO_WORKER_THREADS" != "0" ]; then
    ENV_PREFIX="TOKIO_WORKER_THREADS=$TOKIO_WORKER_THREADS "
fi

PIDS=()

cleanup() {
    echo "Shutting down..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait
    exit 0
}
trap cleanup SIGTERM SIGINT

# Start primary
eval "${ENV_PREFIX}${PRIMARY_CMD}" &
PIDS+=($!)

# Start workers
IFS=';' read -ra WORKER_CMDS <<< "$WORKER_CMD"
for cmd in "${WORKER_CMDS[@]}"; do
    eval "${ENV_PREFIX}${cmd}" &
    PIDS+=($!)
done

# Wait for remote ports before starting clients
if [ -n "$WAIT_PORTS" ]; then
    sleep 1
    for addr in $WAIT_PORTS; do
        host="${addr%%:*}"
        port="${addr##*:}"
        echo "Waiting for $host:$port..."
        for i in $(seq 1 60); do
            (echo > /dev/tcp/$host/$port) 2>/dev/null && break
            sleep 0.5
        done
    done
    echo "All remote ports reachable."
fi

# Wait for all children
wait
