#!/usr/bin/env python3
"""
Plot E2E throughput vs executors/workers for a benchmark CSV.

Usage: python3 plot_benchmark.py <csv_file>

One line per unique (WRITEBACK_EXECUTOR, DISTRIBUTED_TX_RATE, NO_SEND_PAYMENT).
x-axis: NUM_E / NUM_W
y-axis: E2E TPS
Saves PNG next to the CSV.
"""
import csv
import sys
import os
from collections import defaultdict
import matplotlib.pyplot as plt


def main(csv_path):
    with open(csv_path, 'r', newline='') as f:
        rows = list(csv.reader(f))

    header = rows[0]
    num_idx = header.index('NUM_E / NUM_W')
    wb_idx = header.index('WRITEBACK_EXECUTOR')
    dtx_idx = header.index('DISTRIBUTED_TX_RATE')
    nsp_idx = header.index('NO_SEND_PAYMENT')
    e2e_idx = header.index('E2E TPS')

    # Group by (WRITEBACK, DIST_TX, NO_SEND) → list of (num, e2e)
    series = defaultdict(list)

    for row in rows[1:]:
        if not row or len(row) <= e2e_idx:
            continue
        if not row[e2e_idx].strip():
            continue  # skip missing data
        try:
            num = int(row[num_idx])
            e2e = int(row[e2e_idx].replace(',', ''))
        except ValueError:
            continue
        key = (row[wb_idx], row[dtx_idx], row[nsp_idx])
        series[key].append((num, e2e))

    if not series:
        print('No data to plot.')
        sys.exit(1)

    fig, ax = plt.subplots(figsize=(10, 6))

    # Color encodes executor mode, marker encodes workload — two orthogonal
    # visual channels so the mode gap and workload gap are each readable at a glance.
    MODE_COLOR = {'0': '#1f77b4', '1': '#d62728'}  # DataFusion=blue, Writeback=red
    WORKLOAD_MARKER = {
        ('0', '1'): 'o',  # dist_tx=0, no_payment
        ('0.2', '0'): 's',  # dist_tx=0.2, with_payment
    }

    def label_for(key):
        wb, dtx, nsp = key
        mode = 'Writeback' if wb == '1' else 'DataFusion'
        dist = f'dist_tx={dtx}'
        send = 'no_payment' if nsp == '1' else 'with_payment'
        return f'{mode}, {dist}, {send}'

    # Sort keys for stable legend order
    for key in sorted(series.keys()):
        wb, dtx, nsp = key
        points = sorted(series[key])
        xs = [p[0] for p in points]
        ys = [p[1] for p in points]
        color = MODE_COLOR.get(wb, 'gray')
        marker = WORKLOAD_MARKER.get((dtx, nsp), 'x')
        ax.plot(xs, ys, color=color, marker=marker, markersize=9,
                linewidth=2, label=label_for(key))

    ax.set_xlabel('NUM_EXECUTORS / NUM_WORKERS')
    ax.set_ylabel('E2E TPS')
    ax.set_title(f'E2E Throughput — {os.path.basename(csv_path)}')
    ax.grid(True, alpha=0.3)
    ax.legend(loc='best', fontsize=9)
    ax.ticklabel_format(axis='y', style='plain')

    out_path = os.path.splitext(csv_path)[0] + '.png'
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    print(f'Saved {out_path}')


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: python3 plot_benchmark.py <csv_file>')
        sys.exit(1)
    main(sys.argv[1])
