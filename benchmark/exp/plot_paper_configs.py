#!/usr/bin/env python3
"""SIGMOD-style TPS / latency / migration plots for configs a, b, c, d.

Produces one single-column PNG per config (4 total).

Usage:
    python benchmark/exp/plot_paper_configs.py [--output-dir DIR] [--configs a b c d]
"""
import argparse
import re
import sys
from pathlib import Path

import numpy as np
np.Inf = np.inf  # patch for matplotlib compatibility with NumPy 2.0
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import matplotlib.ticker as ticker
from matplotlib.ticker import MaxNLocator

sys.path.insert(0, str(Path(__file__).parent))
from plot_lb_configs import (
    load_run,
    _smoothed_series,
    TRIM_LEFT_DUR,
    TRIM_RIGHT_DUR,
)

# Paper palette — teal, coral, blue, orange (order matches reference image)
COLORS = ["#2A9D8F", "#E76F51", "#1f77b4", "#ff7f0e"]


# ── hardcoded run directories ──────────────────────────────────────────────────

_RESULTS = Path(__file__).parent / "results"

LB_DIR_ABC = _RESULTS / "tps_timeline_lb_cloud_20260414_132149"
BL_DIR_ABC = _RESULTS / "tps_timeline_baseline_cloud_20260414_132153"
LB_DIR_D   = _RESULTS / "tps_timeline_lb_nodes_cloud_20260415_114941"
BL_DIR_D   = _RESULTS / "config_d_scalablit_baseline"


# ── config definitions ─────────────────────────────────────────────────────────
# series entries: (run_subdir_name, legend_label)

CONFIGS = {
    "a": {
        "title":  "Effect of Submission Rate (n=4, 90% skew)",
        "lb_dir": LB_DIR_ABC,
        "bl_dir": BL_DIR_ABC,
        "series": [
            ("n4_v_rate_imb90_r110000_run_1", "110k tx/s"),
            ("n4_v_rate_imb90_r80000_run_1",  "80k tx/s"),
            ("n4_v_rate_imb90_r50000_run_2",  "50k tx/s"),
            ("n4_v_rate_imb90_r20000_run_1",  "20k tx/s"),
        ],
        "output": "config_a.png",
    },
    "b": {
        "title":  "Effect of Load Skew (n=4, 110k tx/s)",
        "lb_dir": LB_DIR_ABC,
        "bl_dir": BL_DIR_ABC,
        "series": [
            ("n4_v_rate_imb60_r110000_run_1", "60% skew"),
            ("n4_v_rate_imb90_r110000_run_1", "90% skew"),
            ("n4_v_rate_imb99_r110000_run_1", "99% skew"),
        ],
        "output": "config_b.png",
    },
    "c": {
        "title":  "Effect of Bandwidth Constraints (n=4, 90% skew)",
        "lb_dir": LB_DIR_ABC,
        "bl_dir": BL_DIR_ABC,
        "series": [
            ("n4_v_rate_imb90_r110000_run_1",    "Balanced BW"),
            ("n4_bw_f_rate_imb90_r110000_run_1",  "f nodes BW-limited"),
            ("n4_bw_f1_rate_imb90_r65000_run_1",  "f+1 nodes BW-limited"),
        ],
        "output": "config_c.png",
    },
    "d": {
        "title":  "Scalability with Committee Size (60% skew, 110k tx/s)",
        "lb_dir": LB_DIR_D,
        "bl_dir": BL_DIR_D,
        "series": [
            ("n4_v_rate_imb60_r110000_run_1",  "n=4"),
            ("n10_v_rate_imb60_r110000_run_1", "n=10"),
            ("n16_v_rate_imb60_r110000_run_1", "n=16"),
            ("n21_v_rate_imb60_r110000_run_1", "n=21"),
        ],
        "output": "config_d.png",
    },
}

CONFIG_ORDER = ["a", "b", "c", "d"]


# ── warmup detection ───────────────────────────────────────────────────────────

_BALANCED_END_RE = re.compile(r'Balanced phase end spread: \d+ ms \((.+?)\)')
_REGION_OFFSET_RE = re.compile(r'R\d+:([\d.]+)s')


def _detect_warmup(lb_dir, bl_dir, run_subdirs):
    """Return min balanced-phase-end offset (seconds after start), or None."""
    candidates = []
    for subdir in run_subdirs:
        for base in (lb_dir, bl_dir):
            log = Path(base) / subdir / "output.log"
            if not log.exists():
                continue
            text = log.read_text()
            m = _BALANCED_END_RE.search(text)
            if not m:
                continue
            offsets = _REGION_OFFSET_RE.findall(m.group(1))
            if offsets:
                candidates.append(min(float(v) for v in offsets))
    return min(candidates) if candidates else None


# ── SIGMOD style ───────────────────────────────────────────────────────────────

def _apply_sigmod_style():
    plt.rcParams.update({
        "font.family":       "sans-serif",
        "font.size":         8,
        "axes.titlesize":    8,
        "axes.labelsize":    8,
        "xtick.labelsize":   7,
        "ytick.labelsize":   7,
        "legend.fontsize":   7,
        "lines.linewidth":   1.0,
        "axes.linewidth":    0.6,
        "grid.linewidth":    0.4,
        "xtick.major.width": 0.6,
        "ytick.major.width": 0.6,
        "pdf.fonttype":      42,
        "ps.fonttype":       42,
        "grid.linestyle":    "--",
        "grid.color":        "lightgray",
        "grid.alpha":        0.8,
    })


def _k_fmt():
    return ticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}k" if x >= 1000 else f"{x:.0f}")


# ── per-config plot ────────────────────────────────────────────────────────────

def plot_config(cfg_key, output_dir, show_title, save_pdf):
    cfg      = CONFIGS[cfg_key]
    lb_dir   = Path(cfg["lb_dir"])
    bl_dir   = Path(cfg["bl_dir"])
    series   = cfg["series"]
    title    = cfg["title"]
    out_path = Path(output_dir) / cfg["output"]

    run_subdirs = [subdir for subdir, _ in series]
    warmup_s = _detect_warmup(lb_dir, bl_dir, run_subdirs)
    if warmup_s is None:
        print(f"  WARNING: no warmup line detected for config {cfg_key}")

    fig, axes = plt.subplots(
        3, 1, figsize=(2.7, 4.2), sharex=True,
        gridspec_kw={"height_ratios": [3, 2, 2]},
    )
    ax_tps, ax_lat, ax_mig = axes

    max_t    = 0.0
    any_mig  = False
    color_handles = []

    for i, (subdir, label) in enumerate(series):
        color   = COLORS[i % len(COLORS)]
        lb_data = load_run(lb_dir / subdir, "mean")
        bl_data = load_run(bl_dir / subdir, "mean")

        lb_tps_times, lb_tps_vals, lb_lat_pairs, mig_times, mig_cumulative = lb_data
        bl_tps_times, bl_tps_vals, bl_lat_pairs, _, _ = bl_data

        if lb_tps_times:
            max_t = max(max_t, lb_tps_times[-1])
        if bl_tps_times:
            max_t = max(max_t, bl_tps_times[-1])

        # TPS
        pt, pv = _smoothed_series(lb_tps_times, lb_tps_vals)
        ax_tps.plot(pt, pv, color=color, linestyle="-",  linewidth=1.0)
        pt, pv = _smoothed_series(bl_tps_times, bl_tps_vals)
        ax_tps.plot(pt, pv, color=color, linestyle="--", linewidth=0.8)

        # Latency
        if lb_lat_pairs:
            lt, lv = zip(*lb_lat_pairs)
            pt, pv = _smoothed_series(list(lt), list(lv))
            ax_lat.plot(pt, pv, color=color, linestyle="-",  linewidth=1.0)
        if bl_lat_pairs:
            bt, bv = zip(*bl_lat_pairs)
            pt, pv = _smoothed_series(list(bt), list(bv))
            ax_lat.plot(pt, pv, color=color, linestyle="--", linewidth=0.8)

        # Migrations — LB only (baseline has zero migrations)
        if mig_times:
            any_mig = True
            ax_mig.step([0.0] + mig_times, [0] + mig_cumulative,
                        where="post", color=color, linewidth=1.0)

        color_handles.append(
            mlines.Line2D([], [], color=color, linewidth=1.0, label=label)
        )

    if not any_mig:
        ax_mig.text(0.5, 0.5, "no migrations", transform=ax_mig.transAxes,
                    ha="center", va="center", fontsize=7, color="gray")

    # x limits
    left  = TRIM_LEFT_DUR
    right = max_t - TRIM_RIGHT_DUR
    if right > left:
        ax_tps.set_xlim(left=left, right=right)

    # Warmup line across all panels
    if warmup_s is not None:
        for ax in axes:
            ax.axvline(warmup_s, color="black", linestyle="--", linewidth=0.8, alpha=0.7)

    # Panel labels / formatting
    if show_title:
        ax_tps.set_title(title)
    ax_tps.set_ylabel("Throughput [ktrans/s]", fontsize=6)
    ax_tps.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}"))
    ax_tps.grid(True)

    ax_lat.set_ylabel("Mean Latency [sec]", fontsize=6)
    ax_lat.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x/1000:.1f}"))
    ax_lat.grid(True)

    ax_mig.set_ylabel("Cumul. Migs", fontsize=6)
    ax_mig.yaxis.set_major_formatter(_k_fmt())
    ax_mig.set_xlabel("Time (s)")
    ax_mig.grid(True)

    for ax in axes:
        ax.yaxis.set_major_locator(MaxNLocator(nbins=3))
        ax.xaxis.set_major_locator(MaxNLocator(nbins=4))

    # Legend: above the panels at figure level so it can use full figure width
    baseline_proxy = mlines.Line2D(
        [], [], color="gray", linestyle="--", linewidth=0.8, label="Without load bal."
    )
    n_entries = len(color_handles) + 1
    ncol = (n_entries + 1) // 2  # two rows max
    fig.legend(
        handles=color_handles + [baseline_proxy],
        loc="upper center",
        bbox_to_anchor=(0.57, 0.95),
        ncol=ncol,
        frameon=False,
        fontsize=6,
        handlelength=1.4,
        handletextpad=0.4,
        columnspacing=0.8,
        borderpad=0.2,
        labelspacing=0.25,
    )

    fig.subplots_adjust(left=0.20, right=0.97, top=0.88, bottom=0.10, hspace=0.18)
    plt.savefig(out_path, dpi=200, bbox_inches="tight", pad_inches=0.02)
    print(f"Saved: {out_path.resolve()}")
    if save_pdf:
        pdf_path = out_path.with_suffix(".pdf")
        plt.savefig(pdf_path, bbox_inches="tight", pad_inches=0.02)
        print(f"Saved: {pdf_path.resolve()}")
    plt.close(fig)


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="SIGMOD-style TPS/latency/migration timeline plots for configs a–d."
    )
    ap.add_argument("--output-dir", default=".", help="Directory to write PNGs into.")
    ap.add_argument(
        "--configs", nargs="+", choices=CONFIG_ORDER, default=CONFIG_ORDER,
        metavar="CONFIG",
        help="Which configs to plot (default: all). Choices: a b c d",
    )
    ap.add_argument("--title", action="store_true", help="Show title on each plot.")
    ap.add_argument("--pdf",   action="store_true", help="Also save a PDF alongside the PNG.")
    args = ap.parse_args()

    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    _apply_sigmod_style()

    for cfg_key in args.configs:
        print(f"Plotting config {cfg_key} ...")
        plot_config(cfg_key, args.output_dir, args.title, args.pdf)


if __name__ == "__main__":
    main()
