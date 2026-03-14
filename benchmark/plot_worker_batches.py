#!/usr/bin/env python3
import argparse
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt


BATCH_LINE_PATTERN = re.compile(
    r"^\[(?P<timestamp>[^\]]+)\s+INFO\s+worker::batch_maker\]\s+"
    r"Batch\s+(?P<digest>\S+)\s+contains\s+(?P<size_bytes>\d+)\s+B$"
)

QUORUM_LINE_PATTERN = re.compile(
    r"^\[(?P<timestamp>[^\]]+)\s+INFO\s+worker::quorum_waiter\]\s+"
    r"Quorum\s+for\s+batch\s+(?P<digest>\S+)\s+"
    r"queue_delay\s+(?P<queue_delay_ms>\d+)ms\s+quorum_latency\s+(?P<quorum_latency_ms>\d+)ms$"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Plot batch size, inter-quorum time, and quorum latency from worker logs."
        )
    )
    parser.add_argument(
        "path",
        help="Path to a worker log file or to a directory containing worker-*.log files",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Optional output image path. If omitted, the plot is saved in the benchmark directory.",
    )
    return parser.parse_args()


def parse_timestamp(raw_timestamp: str) -> datetime:
    if raw_timestamp.endswith("Z"):
        raw_timestamp = raw_timestamp[:-1] + "+00:00"
    return datetime.fromisoformat(raw_timestamp)


def extract_batches(log_path: Path) -> List[Tuple[datetime, str, int]]:
    batches = []
    with log_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            match = BATCH_LINE_PATTERN.match(line.strip())
            if not match:
                continue

            timestamp = parse_timestamp(match.group("timestamp"))
            digest = match.group("digest")
            size_bytes = int(match.group("size_bytes"))
            batches.append((timestamp, digest, size_bytes))

    return batches


def extract_quorum_metrics_by_digest(log_path: Path) -> Dict[str, Tuple[int, int]]:
    metrics = {}
    with log_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            match = QUORUM_LINE_PATTERN.match(line.strip())
            if not match:
                continue
            metrics[match.group("digest")] = (
                int(match.group("queue_delay_ms")),
                int(match.group("quorum_latency_ms")),
            )
    return metrics


def plot_worker_log(
    worker_log: Path,
    batch_ax,
    latency_ax,
    queue_ax,
    label: str,
    linewidth: float = 1.2,
    alpha: float = 1.0,
) -> Tuple[bool, bool]:
    """Plot batch sizes, quorum latency, and queue delay from a single worker log onto the given axes.

    Returns (plotted_batches, plotted_quorum).
    """
    batches = extract_batches(worker_log)
    if not batches:
        return False, False

    sizes = [s for _, _, s in batches]
    batch_ax.plot(range(len(sizes)), sizes, linewidth=linewidth, alpha=alpha, label=label)

    quorum_by_digest = extract_quorum_metrics_by_digest(worker_log)
    latency_idxs, latency_ms, queue_ms = [], [], []
    for i, (_, digest, _) in enumerate(batches):
        m = quorum_by_digest.get(digest)
        if m is not None:
            latency_idxs.append(i)
            queue_ms.append(m[0])
            latency_ms.append(m[1])

    if latency_ms:
        latency_ax.plot(latency_idxs, latency_ms, linewidth=linewidth, alpha=alpha, label=label)
        queue_ax.plot(latency_idxs, queue_ms, linewidth=linewidth, alpha=alpha, label=label)
        return True, True

    return True, False


def find_worker_logs(path: Path) -> List[Path]:
    if path.is_file():
        return [path]

    if not path.is_dir():
        return []

    return sorted(path.glob("worker-*.log"))


def default_output_path(path: Path) -> Path:
    benchmark_dir = Path(__file__).resolve().parent
    if path.is_dir():
        return benchmark_dir / f"{path.name}-worker-batches.png"
    return benchmark_dir / f"{path.stem}-batches.png"


def plot_batches(path: Path, output_path: Optional[Path]) -> None:
    worker_logs = find_worker_logs(path)
    if not worker_logs:
        raise ValueError("No worker log files were found")

    fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=False)

    plotted_logs = 0
    plotted_quorum_latency_logs = 0
    for worker_log in worker_logs:
        label = worker_log.stem
        plotted_batches, plotted_quorum = plot_worker_log(worker_log, axes[0], axes[1], axes[2], label)
        if plotted_batches:
            plotted_logs += 1
        if plotted_quorum:
            plotted_quorum_latency_logs += 1

    if plotted_logs == 0:
        raise ValueError(
            "No worker::batch_maker lines matching 'Batch ... contains ... B' were found"
        )

    axes[0].set_ylabel("Batch size (B)")
    axes[0].grid(True, alpha=0.3)
    axes[0].set_title(path.name)
    axes[0].legend(loc="upper right")

    axes[1].set_ylabel("Quorum latency (ms)")
    axes[1].grid(True, alpha=0.3)
    if plotted_quorum_latency_logs > 0:
        axes[1].legend(loc="upper right")

    axes[2].set_xlabel("Batch index")
    axes[2].set_ylabel("Queue delay (ms)")
    axes[2].grid(True, alpha=0.3)
    if plotted_quorum_latency_logs > 0:
        axes[2].legend(loc="upper right")

    fig.tight_layout()

    if output_path is None:
        output_path = default_output_path(path)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    print(output_path)


def main() -> int:
    args = parse_args()
    path = Path(args.path)

    if not path.exists():
        print(f"Path not found: {path}", file=sys.stderr)
        return 1

    output_path = Path(args.output) if args.output else None

    try:
        plot_batches(path, output_path)
    except ValueError as error:
        print(error, file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())