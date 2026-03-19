#!/usr/bin/env python3
import argparse
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

from plot_worker_batches import (
    find_worker_logs,
    plot_worker_log,
    WINDOW_SIZE,
)


LATENCY_RE = re.compile(r"f\+1 Commit latency \(workers\) \(mean\): ([\d,]+) ms")
TPS_RE = re.compile(r"Committed TPS: ([\d,]+) tx/s")
PER_VALIDATOR_SECTION_RE = re.compile(
    r"\+ PER-VALIDATOR COMMIT METRICS:.*?(?=\n\s*\+|\Z)", re.DOTALL
)
PER_VALIDATOR_ROW_RE = re.compile(r"^\s+(\d+)\s+([\d,]+)\s+([\d,]+)", re.MULTILINE)

# rate_16000_w_1_1_1_1_run_1
RUN_DIR_RE = re.compile(r"^rate_(\d+)_w_(.+)_run_(\d+)$")

RunKey = Tuple[int, str]  # (rate, weights_tag)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot all phase2 imbalance runs in a single combined figure."
    )
    parser.add_argument(
        "results_dir",
        help="Path to a phase2 results directory containing rate_*_w_*_run_* subdirectories",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output image path. Defaults to benchmark/<results_dir_name>-phase2.png",
    )
    parser.add_argument(
        "--no-smooth",
        action="store_true",
        help=f"Disable moving average smoothing (window={WINDOW_SIZE}).",
    )
    return parser.parse_args()


def discover_runs(results_dir: Path) -> Dict[RunKey, List[Tuple[int, Path]]]:
    """Returns {(rate, weights_tag): [(run_number, run_dir), ...]} sorted by run number."""
    groups: Dict[RunKey, List[Tuple[int, Path]]] = {}
    for subdir in sorted(results_dir.iterdir()):
        if not subdir.is_dir():
            continue
        match = RUN_DIR_RE.match(subdir.name)
        if not match:
            continue
        rate = int(match.group(1))
        weights_tag = match.group(2)
        run_num = int(match.group(3))
        key: RunKey = (rate, weights_tag)
        groups.setdefault(key, []).append((run_num, subdir))
    for key in groups:
        groups[key].sort(key=lambda x: x[0])
    return groups


def sort_keys(keys: Set[RunKey]) -> List[RunKey]:
    # Lower rate first (left), then lower first-weight first within same rate.
    return sorted(keys, key=lambda k: (k[0], float(k[1].split('_')[0])))


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


def parse_per_validator_metrics(run_dir: Path) -> List[Tuple[int, str, str]]:
    output_log = run_dir / "output.log"
    if not output_log.exists():
        return []
    text = output_log.read_text(encoding="utf-8")
    section_match = PER_VALIDATOR_SECTION_RE.search(text)
    if not section_match:
        return []
    results = []
    for m in PER_VALIDATOR_ROW_RE.finditer(section_match.group(0)):
        v_id = int(m.group(1))
        tps = m.group(2).replace(",", "")
        mean_ms = m.group(3).replace(",", "")
        results.append((v_id, tps, mean_ms))
    return results


def plot_phase2(results_dir: Path, output_path: Optional[Path], smooth: bool) -> None:
    groups = discover_runs(results_dir)
    if not groups:
        raise ValueError(
            f"No rate_*_w_*_run_* subdirectories found in {results_dir}"
        )

    columns = sort_keys(set(groups.keys()))
    n_cols = len(columns)

    fig = plt.figure(figsize=(6 * n_cols, 20))
    gs = gridspec.GridSpec(
        4, n_cols,
        figure=fig,
        height_ratios=[3, 3, 3, 1],
        hspace=0.45,
        wspace=0.35,
    )
    axes = [[fig.add_subplot(gs[row, col]) for col in range(n_cols)] for row in range(4)]

    for col_idx, (rate, weights_tag) in enumerate(columns):
        runs = groups[(rate, weights_tag)]
        metrics_lines = []

        for run_num, run_dir in runs:
            alpha = 1.0 if run_num == runs[0][0] else 0.5
            worker_logs = find_worker_logs(run_dir)

            for worker_log in worker_logs:
                worker_label = f"{worker_log.stem} (run {run_num})"
                plot_worker_log(
                    worker_log,
                    axes[0][col_idx], axes[1][col_idx], axes[2][col_idx],
                    worker_label, smooth, 1.0, alpha,
                )

            latency, tps = parse_metrics(run_dir)
            metrics_lines.append(f"Run {run_num}: Latency={latency}  TPS={tps}")
            for v_id, v_tps, v_mean in parse_per_validator_metrics(run_dir):
                metrics_lines.append(f"  V{v_id}: {v_tps}tx/s  {v_mean}ms")

        col_title = f"rate={rate}  w={weights_tag.replace('_', ',')}"
        axes[2][col_idx].set_xlabel(col_title, fontsize=10, fontweight="bold")

        ax_text = axes[3][col_idx]
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
    axes[2][0].set_ylabel("Queue delay (ms)")

    for col_idx in range(n_cols):
        for row_idx in range(3):
            axes[row_idx][col_idx].grid(True, alpha=0.3)
            if axes[row_idx][col_idx].get_lines():
                axes[row_idx][col_idx].legend(fontsize=6, loc="upper right")

    fig.text(0.5, 0.01, results_dir.name, ha="center", fontsize=12)
    fig.tight_layout(rect=(0, 0.03, 1, 1))

    if output_path is None:
        benchmark_dir = Path(__file__).resolve().parent
        output_path = benchmark_dir / f"{results_dir.name}-phase2.png"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    print(output_path)


def main() -> int:
    args = parse_args()
    results_dir = Path(args.results_dir)

    if not results_dir.exists():
        print(f"Path not found: {results_dir}", file=sys.stderr)
        return 1

    output_path = Path(args.output) if args.output else None

    try:
        plot_phase2(results_dir, output_path, not args.no_smooth)
    except ValueError as error:
        print(error, file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
