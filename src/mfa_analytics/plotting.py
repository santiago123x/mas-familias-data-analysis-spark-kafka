"""Static charts (PNG) built from small, already-aggregated pandas tables.

Spark aggregates the data first; only the aggregated result (tens of rows) is
converted to pandas for plotting, never the full dataset.
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless backend: the VM has no display

import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.ticker import FuncFormatter, PercentFormatter  # noqa: E402

logger = logging.getLogger(__name__)
logging.getLogger("matplotlib").setLevel(logging.WARNING)  # silence category-unit notices

# Colour-blind-safe categorical slots and recessive ink/grid colours.
PRIMARY = "#2a78d6"
SECONDARY = "#eb6834"
TEXT = "#0b0b0b"
MUTED = "#52514e"
GRID = "#e5e4e0"

plt.rcParams.update(
    {
        "figure.dpi": 150,
        "font.size": 9,
        "axes.edgecolor": GRID,
        "axes.labelcolor": MUTED,
        "axes.titlesize": 11,
        "axes.titleweight": "bold",
        "axes.titlecolor": TEXT,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "grid.color": GRID,
        "grid.linewidth": 0.8,
    }
)

_thousands = FuncFormatter(lambda value, _: f"{value:,.0f}")


def _save(fig: plt.Figure, output_dir: Path, name: str, source_note: str) -> Path:
    """Add the source note, save the figure and release its memory."""
    fig.tight_layout()
    # Placed just below the figure area; bbox_inches="tight" expands the canvas to fit it,
    # so the note never overlaps the axis labels.
    fig.text(0.01, -0.01, source_note, fontsize=7, color=MUTED, ha="left", va="top")
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f"{name}.png"
    fig.savefig(target, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved figure %s", target)
    return target


def plot_horizontal_bars(
    pdf: pd.DataFrame,
    label_col: str,
    value_col: str,
    *,
    title: str,
    xlabel: str,
    output_dir: Path,
    name: str,
    source_note: str,
    as_percent: bool = False,
    reference_value: float | None = None,
    reference_label: str = "",
) -> Path:
    """Sorted horizontal bar chart (largest at the top) with optional reference line."""
    data = pdf.sort_values(value_col, ascending=True)
    fig, ax = plt.subplots(figsize=(8, max(3.0, 0.26 * len(data) + 1.2)))
    ax.barh(data[label_col].astype(str), data[value_col], color=PRIMARY, height=0.7)
    ax.set_title(title, loc="left")
    ax.set_xlabel(xlabel)
    ax.grid(axis="x")
    ax.set_axisbelow(True)
    ax.xaxis.set_major_formatter(PercentFormatter(1.0) if as_percent else _thousands)
    if reference_value is not None:
        ax.axvline(reference_value, color=TEXT, linewidth=1, linestyle="--")
        ax.text(
            reference_value,
            len(data) - 0.3,
            f" {reference_label}",
            color=TEXT,
            fontsize=8,
            va="bottom",
        )
    return _save(fig, output_dir, name, source_note)


def plot_vertical_bars(
    pdf: pd.DataFrame,
    label_col: str,
    value_col: str,
    *,
    title: str,
    ylabel: str,
    output_dir: Path,
    name: str,
    source_note: str,
) -> Path:
    """Simple vertical bar chart for an ordered category (e.g. years)."""
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(pdf[label_col].astype(str), pdf[value_col], color=PRIMARY, width=0.7)
    ax.set_title(title, loc="left")
    ax.set_ylabel(ylabel)
    ax.grid(axis="y")
    ax.set_axisbelow(True)
    ax.yaxis.set_major_formatter(_thousands)
    return _save(fig, output_dir, name, source_note)


def plot_grouped_bars(
    pdf: pd.DataFrame,
    category_col: str,
    series_col: str,
    value_col: str,
    *,
    title: str,
    ylabel: str,
    output_dir: Path,
    name: str,
    source_note: str,
    series_order: list[str] | None = None,
) -> Path:
    """Grouped vertical bars: one group per category, one bar per series (max two colours)."""
    wide = pdf.pivot_table(index=category_col, columns=series_col, values=value_col, fill_value=0)
    series = [s for s in (series_order or list(wide.columns)) if s in wide.columns][:2]
    colors = [PRIMARY, SECONDARY]
    width = 0.38
    positions = range(len(wide.index))
    fig, ax = plt.subplots(figsize=(8, 4))
    for offset, (serie, color) in enumerate(zip(series, colors, strict=False)):
        ax.bar(
            [p + (offset - 0.5) * width for p in positions],
            wide[serie],
            width=width * 0.95,
            color=color,
            label=serie.title(),
        )
    ax.set_xticks(list(positions), wide.index.astype(str))
    ax.set_title(title, loc="left")
    ax.set_ylabel(ylabel)
    ax.grid(axis="y")
    ax.set_axisbelow(True)
    ax.yaxis.set_major_formatter(_thousands)
    ax.legend(frameon=False)
    return _save(fig, output_dir, name, source_note)
