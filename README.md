# DAG-BFT with Validator-Level Load Balancing

Anonymized artifact accompanying the SIGMOD 2026 submission. This repository
extends a Narwhal-based DAG-BFT mempool with a load-balancing mechanism that
migrates clients across validators via `MigrationNotice` messages from the
worker's `Synchronizer`.

## Prerequisites

- Rust toolchain (1.70+) and `clang` (required by rocksdb).
- Python 3.9 and `tmux`.
- One of:
  - **Docker** (for `MODE=docker`, the default), with `docker-compose`.
  - **CloudLab** allocation plus a `manifest.xml` (for `MODE=cloudlab`).

Setup:

```bash
# Rust build
cargo build --release --features benchmark

# Python deps (conda env 'narwhal39' used during development)
pip install -r benchmark/requirements.txt
```

## Reproducing Figure 3 (TPS timeline)

Two sweep scripts drive the experiment. Each iterates over the configs listed
at the top of the file and writes results under
`benchmark/exp/results/tps_timeline_{lb,baseline}_{docker,cloud}[_isolate]_<timestamp>/`.

### Load-balancing variant

```bash
# Docker (default MODE)
bash benchmark/exp/fig_3_tps_timeline_lb.sh

# CloudLab
MODE=cloudlab MANIFEST=benchmark/manifest.xml \
    bash benchmark/exp/fig_3_tps_timeline_lb.sh
```

### Baseline variant (no load balancing; `BASELINE=1` is set by the script)

```bash
# Docker
bash benchmark/exp/fig_3_tps_timeline_baseline.sh

# CloudLab
MODE=cloudlab MANIFEST=benchmark/manifest.xml \
    bash benchmark/exp/fig_3_tps_timeline_baseline.sh
```

### Common environment variables

| Variable | Default | Description |
|---|---|---|
| `MODE` | `docker` | `docker` or `cloudlab`. |
| `DURATION` | `900` | Run length in seconds. |
| `WARMUP` | `240` | Warm-up seconds excluded from metrics. |
| `RETRIES` | `1` | Runs per (label, rate) pair. |
| `CPUS_PER_VALIDATOR` | `8` | Pinned CPUs per validator (docker). |
| `LATENCY` | `100ms` | Inter-validator link latency (docker). |
| `PRIMARY_BW` | `300mbit` | Primary-network bandwidth (docker). |
| `MANIFEST` | `manifest.xml` | CloudLab manifest path (cloudlab). |
| `LOCAL_ORCH` | `0` | Orchestrate from local host if `1` (cloudlab). |

Existing `RUN_DIR`s are skipped, so a sweep can be resumed by re-running the
same script.

## Plotting

After a sweep completes, produce the Figure 3 TPS-timeline plots with:

```bash
python benchmark/exp/parse_and_plot_tps_migration_timeline.py <results_dir>
python benchmark/exp/plot_3phase_tps_latency.py <results_dir>
```

## License

Apache-2.0 (see `LICENSE`).
