#!/usr/bin/env python
"""Narwhal WAN topology: single shared LAN, single consolidated client.

N validators and 1 client on one broadcast domain. Validator interfaces are
shaped (50Mbps, 25ms per interface -> 100ms RTT between validators). The single
client interface is unshaped on the LAN (baseline 50ms RTT to any validator).
SO_MARK-based tc rules on the client add extra one-way delay for remote flows
so that region i -> validator j (i!=j) gets 100ms RTT.

The benchmark_client sets SO_MARK = region_id * N + v_idx + 1 on each TCP
socket. The tc fw filters match these marks and route remote flows through
netem delay qdiscs.

Latency breakdown:
  node-i <-> node-j RTT: 25ms(i) + 25ms(i) + 25ms(j) + 25ms(j) = 100ms
  client region-i -> own node-i RTT: 0(tc) + 25(delay) + 25(delay) + 0 = 50ms
  client region-i -> remote node-j RTT: 50(tc) + 25(delay) + 25(delay) + 0 = 100ms
"""

import geni.portal as portal
import geni.rspec.pg as RSpec

NUM_VALIDATORS = 4
PRIMARY_BW = 25000   # Kbps, consensus traffic (same for all validators)
WORKER_BWS = [25000, 25000, 75000, 75000]  # Kbps, batch traffic per validator
NODE_BWS = [PRIMARY_BW + w for w in WORKER_BWS]  # total per validator: [50000, 100000, 100000, 100000]
NODE_LAT = 25     # 25ms per interface -> 50ms one-way -> 100ms RTT between nodes
# Extra one-way delay added on client egress to remote validators via SO_MARK tc.
# region-i to own node-i RTT: 0 + 25 + 25 + 0 = 50ms
# region-i to remote node-j RTT: 50 + 25 + 25 + 0 = 100ms
CLIENT_REMOTE_EXTRA_LAT = 50
DISK_IMAGE = "urn:publicid:IDN+emulab.net+image+emulab-ops:UBUNTU22-64-STD"
APT_PACKAGES = "sudo apt update && sudo apt install python3-pip iperf3 -y"

rspec = RSpec.Request()
pc = portal.Context()

lan = RSpec.LAN("wan-lan")
rspec.addResource(lan)

for i in range(NUM_VALIDATORS):
    node = RSpec.RawPC("node-%d" % i)
    node.disk_image = DISK_IMAGE
    rspec.addResource(node)
    node.addService(RSpec.Execute(shell="sh", command=APT_PACKAGES))

    iface = node.addInterface("if-lan")
    iface.bandwidth = NODE_BWS[i]
    iface.latency = NODE_LAT
    lan.addInterface(iface)

# Single consolidated client node
client = RSpec.RawPC("client-0")
client.disk_image = DISK_IMAGE
rspec.addResource(client)
client.addService(RSpec.Execute(shell="sh", command=APT_PACKAGES))

# SO_MARK-based tc: add extra one-way latency for remote (region_id != v_idx) flows.
# The benchmark_client sets SO_MARK = region_id * N + v_idx + 1 on each TCP socket.
N = NUM_VALIDATORS
tc_script_lines = [
    "set -e",
    # Wait for the LAN interface to get its 10.x.x.x IP (Emulab assigns it after boot)
    "for i in $(seq 1 30); do",
    "  IFACE=$(ip -o addr show | grep '10\\.' | awk '{print $2}' | head -1)",
    "  [ -n \"$IFACE\" ] && break",
    "  sleep 2",
    "done",
    "[ -z \"$IFACE\" ] && echo 'FATAL: no 10.x interface found' && exit 1",
    "tc qdisc replace dev $IFACE root handle 1: prio bands 3 "
    "priomap 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1",
]
for region_id in range(N):
    for v_idx in range(N):
        mark = region_id * N + v_idx + 1
        if region_id != v_idx:
            tc_script_lines.append(
                "tc filter add dev $IFACE parent 1: protocol ip prio 1 "
                "handle %d fw flowid 1:3" % mark
            )
tc_script_lines.append(
    "tc qdisc add dev $IFACE parent 1:3 handle 30: netem delay %dms" % CLIENT_REMOTE_EXTRA_LAT
)
# Emulab runs Execute commands via /bin/sh regardless of shell= attribute on Ubuntu 24
# (where /bin/sh is dash). Use a heredoc to write a bash script and run it.
tc_script_body = "\n".join(tc_script_lines)
tc_cmd = "cat > /tmp/tc-setup.sh << 'TCEOF'\n%s\nTCEOF\nbash /tmp/tc-setup.sh" % tc_script_body
client.addService(RSpec.Execute(shell="sh", command=tc_cmd))

lan.addInterface(client.addInterface("if-lan"))

pc.printRequestRSpec(rspec)
