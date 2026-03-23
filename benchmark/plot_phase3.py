#!/usr/bin/env python3
import argparse
import re
from pathlib import Path
from typing import Dict, List, Set, Tuple

from plot_worker_batches import WINDOW_SIZE
from plot_common import plot_experiment


COLUMN_ORDER = [
    "balanced_bsl0",
    "balanced_bsl1",
    "imbalance_rate_5_bsl0",
    "imbalance_rate_5_bsl1",
    "imbalance_bw1_bsl0",
    "imbalance_bw1_bsl1",
    "imbalance_bw2_bsl0",
    "imbalance_bw2_bsl1",
]


INCLUDED = [
    "balanced_bsl0",
    "balanced_bsl1",
    "imbalance_rate_5_bsl0",
    "imbalance_rate_5_bsl1",
    "imbalance_bw1_bsl0",
    "imbalance_bw1_bsl1",
    "imbalance_bw2_bsl0",
    "imbalance_bw2_bsl1",
]

RUN_DIR_RE = re.compile(r"^(.+)_rate(\d+)_bsl(\d+)_run_(\d+)$")


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
    parser.add_argument(
        "--no-smooth",
        action="store_true",
        help=f"Disable moving average smoothing (window={WINDOW_SIZE}).",
    )
    return parser.parse_args()


def discover_runs(results_dir: Path) -> Dict[Tuple[str, int], List[Tuple[int, Path]]]:
    """Returns {(label, rate): [(run_number, run_dir), ...]} sorted by run number."""
    groups: Dict[Tuple[str, int], List[Tuple[int, Path]]] = {}
    for subdir in sorted(results_dir.iterdir()):
        if not subdir.is_dir():
            continue
        match = RUN_DIR_RE.match(subdir.name)
        if not match:
            continue
        label, rate, bsl, run_num = match.group(1), int(match.group(2)), match.group(3), int(match.group(4))
        label = f"{label}_bsl{bsl}"
        groups.setdefault((label, rate), []).append((run_num, subdir))
    for key in groups:
        groups[key].sort(key=lambda x: x[0])
    return groups


def sort_label_rate_pairs(pairs: Set[Tuple[str, int]]) -> List[Tuple[str, int]]:
    label_order = {l: i for i, l in enumerate(COLUMN_ORDER)}
    def key(pair):
        label, rate = pair
        return (label_order.get(label, len(COLUMN_ORDER)), label, rate)
    return sorted(pairs, key=key)


def main() -> int:
    args = parse_args()
    results_dir = Path(args.results_dir)

    assert results_dir.exists(), f"Path not found: {results_dir}"

    groups = discover_runs(results_dir)
    # filter groups
    groups = {k: v for k, v in groups.items() if k[0] in INCLUDED}
    assert groups, f"No *_rate*_run_* subdirectories found in {results_dir}"

    sorted_keys = sort_label_rate_pairs(set(groups.keys()))
    columns = [
        (label, groups[(label, rate)])
        for label, rate in sorted_keys
    ]

    output_path = Path(args.output) if args.output else (
        Path(__file__).resolve().parent / f"{results_dir.name}-phase3.png"
    )

    plot_experiment(results_dir, output_path, not args.no_smooth, columns, lambda d: d)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
