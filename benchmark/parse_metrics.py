#!/usr/bin/env python3
import csv
import re
import sys


def parse_block(block):
    # First line of block is everything after the split on "CMD: ", i.e. the rest of the CMD line
    lines = block.split('\n')
    cmd_line = lines[0].strip()

    def find(pattern, text, default='', keep_commas=False):
        m = re.search(pattern, text)
        if not m:
            return default
        return m.group(1) if keep_commas else m.group(1).replace(',', '')

    rr = find(r'\bRR=(\S+)', cmd_line)
    open_loop = find(r'\bOPEN_LOOP=(\S+)', cmd_line)
    rate_weights = find(r'\bRATE_WEIGHTS=(\S+)', cmd_line, keep_commas=True)
    rate = find(r'\bRATE=(\S+)', cmd_line)
    duration = find(r'\bDURATION=(\S+)', cmd_line)
    warmup = find(r'\bWARMUP=(\S+)', cmd_line)
    cpus_per_validator = find(r'--cpus-per-validator=(\S+)', cmd_line)
    bandwidth = find(r'--bandwidth=(\S+)', cmd_line)
    bandwidths_mbps = find(r'\bBANDWIDTHS_MBPS=(\S+)', cmd_line, keep_commas=True)
    latency = find(r'--latency=(\S+)', cmd_line)
    primary_bw = find(r'--primary-bw=(\S+)', cmd_line)

    committee_size = find(r'Committee size:\s*([\d,]+)\s*node', block)
    input_rate = find(r'Input rate:\s*([\d,]+)\s*tx/s', block)
    tx_size_B = find(r'Transaction size:\s*([\d,]+)\s*B', block)
    bench_duration_s = find(r'Benchmark duration:\s*([\d,.]+)\s*s', block)
    commit_lat_mean_ms = find(r'f\+1 Commit latency \(workers\) \(mean\):\s*([\d,]+)\s*ms', block)
    commit_lat_p95_ms = find(r'f\+1 Commit latency \(workers\) \(p95\):\s*([\d,]+)\s*ms', block)
    consensus_tps = find(r'Consensus TPS:\s*([\d,]+)\s*tx/s', block)
    consensus_bps = find(r'Consensus BPS:\s*([\d,]+)\s*B/s', block)
    committed_tps = find(r'Committed TPS:\s*([\d,]+)\s*tx/s', block)
    committed_bps = find(r'Committed BPS:\s*([\d,]+)\s*B/s', block)

    # Batch seal -> Quorum from the PER-STAGE section (the "All" column, not per-validator)
    # Match the line in "PER-STAGE LATENCY BREAKDOWN (mean, ms)" section
    # The line looks like: "  Batch seal -> Quorum:            475"
    # We want only the first occurrence (the "All" column table)
    batch_seal_to_quorum_mean_ms = find(r'Batch seal -> Quorum:\s*([\d,]+)', block)

    # Per-validator TPS and latency from PER-VALIDATOR COMMIT METRICS
    # Lines look like: " 0            647           52,615          0"
    validator_metrics = re.findall(r'^\s+(\d+)\s+([\d,]+)\s+([\d,]+)\s+\d+', block, re.MULTILINE)
    per_validator_tps = ','.join(m[1].replace(',', '') for m in validator_metrics)
    per_validator_latency = ','.join(m[2].replace(',', '') for m in validator_metrics)

    return [
        rr, duration, open_loop, rate_weights, warmup,
        cpus_per_validator, bandwidth, bandwidths_mbps, latency, primary_bw,
        committee_size, input_rate, tx_size_B,
        commit_lat_mean_ms, commit_lat_p95_ms,
        consensus_tps, consensus_bps,
        committed_tps, committed_bps,
        batch_seal_to_quorum_mean_ms,
        per_validator_tps, per_validator_latency,
        cmd_line,
    ]


HEADER = [
    'rr', 'duration', 'open_loop', 'rate_weights', 'warmup',
    'cpus_per_validator', 'bandwidth', 'bandwidths_mbps', 'latency', 'primary_bw',
    'committee_size', 'input_rate', 'tx_size_B', 
    'commit_lat_mean_ms', 'commit_lat_p95_ms',
    'consensus_tps', 'consensus_bps',
    'committed_tps', 'committed_bps',
    'batch_seal_to_quorum_mean_ms',
    'per_validator_tps', 'per_validator_latency',
    'cmd',
]

EXCLUDE_HEADER = [
    'rr',
    'open_loop',
    # 'rate_weights',
    'cpus_per_validator',
    'latency',
    'bandwidth',
    # 'bandwidths_mbps',
    'primary_bw',
    'tx_size_B',
    # 'duration',
    'warmup',
    'consensus_tps', 'consensus_bps',
    'committee_size',
    # 'tx_size_B',
    # 'consensus_tps',
    # 'consensus_bps',
    # 'committed_bps',
    # 'cmd',
]


def print_aligned(rows):
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    for row in rows:
        print('  '.join(cell.ljust(widths[i]) for i, cell in enumerate(row)))


def main():
    align = '--align' in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    if not args:
        print(f'Usage: {sys.argv[0]} [--align] <merged_output.log>', file=sys.stderr)
        sys.exit(1)

    with open(args[0]) as f:
        content = f.read()

    # Split on CMD: — first element before first CMD: is discarded if empty
    parts = content.split('CMD: ')
    blocks = [p for p in parts[1:] if p.strip()]

    exclude = set(EXCLUDE_HEADER)
    active_header = [c for c in HEADER if c not in exclude]
    active_indices = [HEADER.index(c) for c in active_header]

    rows = [active_header]
    for block in blocks:
        row = parse_block(block)
        rows.append([row[i] for i in active_indices])

    if align:
        print_aligned(rows)
    else:
        writer = csv.writer(sys.stdout)
        for row in rows:
            writer.writerow(row)


if __name__ == '__main__':
    main()
