#!/usr/bin/env python3
import argparse
import re
from pathlib import Path
from typing import Dict, List, Set, Tuple

from plot_worker_batches import WINDOW_SIZE
from plot_common import plot_experiment


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


def main() -> int:
    args = parse_args()
    results_dir = Path(args.results_dir)

    assert results_dir.exists(), f"Path not found: {results_dir}"

    groups = discover_runs(results_dir)
    assert groups, f"No rate_*_w_*_run_* subdirectories found in {results_dir}"

    sorted_keys = sort_keys(set(groups.keys()))
    columns = [
        (f"rate={rate}  w={weights_tag.replace('_', ',')}", groups[(rate, weights_tag)])
        for rate, weights_tag in sorted_keys
    ]

    output_path = Path(args.output) if args.output else (
        Path(__file__).resolve().parent / f"{results_dir.name}-phase2.png"
    )

    plot_experiment(results_dir, output_path, not args.no_smooth, columns, lambda d: d)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
