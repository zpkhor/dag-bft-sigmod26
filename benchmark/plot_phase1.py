#!/usr/bin/env python3
import argparse
import re
from pathlib import Path
from typing import Dict, List, Set, Tuple

from plot_worker_batches import WINDOW_SIZE
from plot_common import plot_experiment


# rate_15000_bw_75mbit_routing_none_run_1
RUN_DIR_RE = re.compile(r"^rate_(\d+)_bw_(\d+)mbit_routing_([a-zA-Z0-9-]+)_run_(\d+)$")

RunKey = Tuple[int, int, str]  # (rate, bw_mbit, routing_mode)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot all phase1 saturation runs in a single combined figure."
    )
    parser.add_argument(
        "results_dir",
        help="Path to a phase1 results directory containing rate_*_bw_*mbit_routing_*_run_* subdirectories",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output image path. Defaults to benchmark/<results_dir_name>-phase1.png",
    )
    parser.add_argument(
        "--no-smooth",
        action="store_true",
        help=f"Disable moving average smoothing (window={WINDOW_SIZE}).",
    )
    return parser.parse_args()


def discover_runs(results_dir: Path) -> Dict[RunKey, List[Tuple[int, Path]]]:
    """Returns {(rate, bw_mbit, routing_mode): [(run_number, run_dir), ...]} sorted by run number."""
    groups: Dict[RunKey, List[Tuple[int, Path]]] = {}
    for subdir in sorted(results_dir.iterdir()):
        if not subdir.is_dir():
            continue
        match = RUN_DIR_RE.match(subdir.name)
        if not match:
            continue
        rate = int(match.group(1))
        bw_mbit = int(match.group(2))
        routing_mode = match.group(3)
        run_num = int(match.group(4))
        key: RunKey = (rate, bw_mbit, routing_mode)
        groups.setdefault(key, []).append((run_num, subdir))
    for key in groups:
        groups[key].sort(key=lambda x: x[0])
    return groups


def sort_keys(keys: Set[RunKey]) -> List[RunKey]:
    return sorted(keys, key=lambda k: (k[0], k[1], k[2]))  # rate, bw_mbit, routing_mode


def main() -> int:
    args = parse_args()
    results_dir = Path(args.results_dir)

    assert results_dir.exists(), f"Path not found: {results_dir}"

    groups = discover_runs(results_dir)
    assert groups, f"No rate_*_bw_*mbit_routing_*_run_* subdirectories found in {results_dir}"

    sorted_keys = sort_keys(set(groups.keys()))
    columns = [
        (f"rate={rate}  bw={bw_mbit}mbit  routing={routing_mode}", groups[(rate, bw_mbit, routing_mode)])
        for rate, bw_mbit, routing_mode in sorted_keys
    ]

    output_path = Path(args.output) if args.output else (
        Path(__file__).resolve().parent / f"{results_dir.name}-phase1.png"
    )

    plot_experiment(results_dir, output_path, not args.no_smooth, columns, lambda d: d)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
