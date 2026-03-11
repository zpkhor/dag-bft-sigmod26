#!/usr/bin/env python3
import argparse
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

from plot_worker_batches import (
    extract_batches,
    extract_quorum_times_by_digest,
    find_worker_logs,
)


COLUMN_ORDER = [
    "balanced",
    "imbalance_rate",
    "imbalance_bw",
    "imbalance_bw_rate",
]

LATENCY_RE = re.compile(r"f\+1 Commit latency \(workers\) \(mean\): ([\d,]+) ms")
TPS_RE = re.compile(r"Committed TPS: ([\d,]+) tx/s")

RUN_DIR_RE = re.compile(r"^(.+)_run_(\d+)$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot all phase3 scenario runs in a single combined figure."
    )
    parser.add_argument(
        "results_dir",
        help="Path to a phase3 results directory containing *_run_* subdirectories",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output image path. Defaults to benchmark/<results_dir_name>-phase3.png",
    )
    return parser.parse_args()


def discover_runs(results_dir: Path) -> Dict[str, List[Tuple[int, Path]]]:
    """Returns {label: [(run_number, run_dir), ...]} sorted by run number."""
    groups: Dict[str, List[Tuple[int, Path]]] = {}
    for subdir in sorted(results_dir.iterdir()):
        if not subdir.is_dir():
            continue
        match = RUN_DIR_RE.match(subdir.name)
        if not match:
            continue
        label, run_num = match.group(1), int(match.group(2))
        groups.setdefault(label, []).append((run_num, subdir))
    for label in groups:
        groups[label].sort(key=lambda x: x[0])
    return groups


def sort_labels(labels):
    known = [l for l in COLUMN_ORDER if l in labels]
    unknown = sorted(l for l in labels if l not in COLUMN_ORDER)
    return known + unknown


def parse_metrics(run_dir: Path) -> Tuple[str, str]:
    output_log = run_dir / "output.log"
    if not output_log.exists():
        return "?", "?"
    text = output_log.read_text(encoding="utf-8")
    latency_match = LATENCY_RE.search(text)
    tps_match = TPS_RE.search(text)
    latency = latency_match.group(1) + "ms" if latency_match else "?"
    tps = tps_match.group(1) + "tx/s" if tps_match else "?"
    return latency, tps


def plot_phase3(results_dir: Path, output_path: Optional[Path]) -> None:
    groups = discover_runs(results_dir)
    if not groups:
        raise ValueError(f"No *_run_* subdirectories found in {results_dir}")

    labels = sort_labels(groups.keys())
    n_cols = len(labels)

    fig = plt.figure(figsize=(6 * n_cols, 16))
    gs = gridspec.GridSpec(
        3, n_cols,
        figure=fig,
        height_ratios=[3, 3, 1],
        hspace=0.45,
        wspace=0.35,
    )
    axes = [[fig.add_subplot(gs[row, col]) for col in range(n_cols)] for row in range(3)]

    for col_idx, label in enumerate(labels):
        runs = groups[label]
        metrics_lines = []

        for run_num, run_dir in runs:
            alpha = 1.0 if run_num == runs[0][0] else 0.5
            worker_logs = find_worker_logs(run_dir)

            for worker_log in worker_logs:
                worker_label = f"{worker_log.stem} (run {run_num})"

                batches = extract_batches(worker_log)
                if batches:
                    sizes = [s for _, _, s in batches]
                    axes[0][col_idx].plot(
                        range(len(sizes)), sizes,
                        linewidth=1.0, alpha=alpha, label=worker_label,
                    )

                quorum_by_digest = extract_quorum_times_by_digest(worker_log)
                latency_idxs, latency_ms = [], []
                for i, (batch_ts, digest, _) in enumerate(batches):
                    q_ts = quorum_by_digest.get(digest)
                    if q_ts is not None:
                        latency_idxs.append(i)
                        latency_ms.append((q_ts - batch_ts).total_seconds() * 1000.0)
                if latency_ms:
                    axes[1][col_idx].plot(
                        latency_idxs, latency_ms,
                        linewidth=1.0, alpha=alpha, label=worker_label,
                    )

            latency, tps = parse_metrics(run_dir)
            metrics_lines.append(f"Run {run_num}: Latency={latency}  TPS={tps}")

        axes[0][col_idx].set_title(label, fontsize=10, fontweight="bold")

        ax_text = axes[2][col_idx]
        ax_text.axis("off")
        ax_text.text(
            0.5, 0.5,
            "\n".join(metrics_lines),
            transform=ax_text.transAxes,
            ha="center", va="center",
            fontsize=9, family="monospace",
        )

    axes[0][0].set_ylabel("Batch size (B)")
    axes[1][0].set_ylabel("Quorum latency (ms)")

    for col_idx in range(n_cols):
        axes[1][col_idx].set_xlabel("Batch index")
        for row_idx in range(2):
            axes[row_idx][col_idx].grid(True, alpha=0.3)
            if axes[row_idx][col_idx].get_lines():
                axes[row_idx][col_idx].legend(fontsize=6, loc="upper right")

    fig.suptitle(results_dir.name, fontsize=12)

    if output_path is None:
        benchmark_dir = Path(__file__).resolve().parent
        output_path = benchmark_dir / f"{results_dir.name}-phase3.png"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    print(output_path)


def main() -> int:
    args = parse_args()
    results_dir = Path(args.results_dir)

    if not results_dir.exists():
        print(f"Path not found: {results_dir}", file=sys.stderr)
        return 1

    output_path = Path(args.output) if args.output else None

    try:
        plot_phase3(results_dir, output_path)
    except ValueError as error:
        print(error, file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
