"""Plot the results saved by the streaming consumer.

Reads ``outputs/streaming/window_metrics.csv`` and keeps, for every
(window, department), the value from the latest micro-batch (update mode emits
partial results as events arrive). Produces two charts:

* events received per window (all departments), and
* beneficiaries per department over the whole run (top 10).

Usage:  python3 jobs/plot_stream_results.py
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

from mfa_analytics import plotting
from mfa_analytics.config import DEFAULT_FIGURES_DIR, DEFAULT_STREAM_OUTPUT_DIR

logger = logging.getLogger("plot_stream_results")
NOTE = "Simulated stream: historical MFA records replayed through Kafka (not live data)."


def load_final_windows(metrics_file: Path) -> pd.DataFrame:
    """Load window metrics keeping the last emitted value per window and department."""
    df = pd.read_csv(metrics_file, parse_dates=["window_start", "window_end"])
    df = df.sort_values("batch_id")
    return df.drop_duplicates(["window_start", "department_name"], keep="last")


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns a process exit code."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--metrics-file", type=Path, default=Path(DEFAULT_STREAM_OUTPUT_DIR) / "window_metrics.csv"
    )
    parser.add_argument("--figures-dir", type=Path, default=Path(DEFAULT_FIGURES_DIR))
    args = parser.parse_args(argv)

    if not args.metrics_file.is_file():
        logger.error("No results found at %s. Run the stream consumer first.", args.metrics_file)
        return 2

    windows = load_final_windows(args.metrics_file)
    per_window = windows.groupby("window_start", as_index=False)["events"].sum()
    per_department = (
        windows.groupby("department_name", as_index=False)["beneficiaries"]
        .sum()
        .nlargest(10, "beneficiaries")
    )

    per_window["window"] = per_window["window_start"].dt.strftime("%H:%M")
    plotting.plot_vertical_bars(
        per_window,
        "window",
        "events",
        title="Valid events processed per 1-minute window (UTC start time)",
        ylabel="Valid events",
        output_dir=args.figures_dir,
        name="06_stream_events_per_window",
        source_note=NOTE,
    )
    plotting.plot_horizontal_bars(
        per_department,
        "department_name",
        "beneficiaries",
        title="Beneficiaries received in the stream, top 10 departments",
        xlabel="Beneficiaries (sum of beneficiary_count)",
        output_dir=args.figures_dir,
        name="07_stream_beneficiaries_by_department",
        source_note=NOTE,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
