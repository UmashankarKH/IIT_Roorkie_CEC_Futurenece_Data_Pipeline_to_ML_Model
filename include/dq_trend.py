"""Data-quality trend built from the dq_results history (bonus).

``dq_results`` is appended on every run, so it already is a time series of
the quality scorecard. ``load_dq_trend`` summarises it per run and
``plot_dq_trend`` draws two small multiples: failed checks by severity, and
order rows failing Critical checks. They are separate panels because the two
measures have different scales (no dual axis).
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

# Reference status palette: severity is a state, so it uses the reserved
# status colours and is always labelled, never colour alone.
CRITICAL = "#d03b3b"
WARNING = "#fab219"
SERIES = "#2a78d6"
SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e4e3df"


def load_dq_trend(warehouse_path: str | Path, dag_id: str | None = None) -> pd.DataFrame:
    """One row per run: failed checks by severity and rows failing Critical checks."""

    con = duckdb.connect(str(warehouse_path), read_only=True)
    try:
        return con.execute(
            """
            SELECT run_id,
                   min(CAST(checked_at AS TIMESTAMP)) AS checked_at,
                   count(*) FILTER (WHERE status = 'FAIL' AND severity = 'Critical') AS critical_failed_checks,
                   count(*) FILTER (WHERE status = 'FAIL' AND severity = 'Warning') AS warning_failed_checks,
                   coalesce(sum(rows_failed) FILTER (WHERE severity = 'Critical'), 0) AS critical_failed_rows,
                   coalesce(sum(rows_failed) FILTER (WHERE severity = 'Critical' AND check_name LIKE 'orders.csv:%'), 0)
                       AS critical_failed_order_rows,
                   max(rows_checked) FILTER (WHERE check_name LIKE 'orders.csv:%') AS orders_checked,
                   count(*) AS checks
            FROM dq_results
            WHERE ? IS NULL OR dag_id = ?
            GROUP BY run_id
            ORDER BY checked_at
            """,
            [dag_id, dag_id],
        ).fetchdf()
    finally:
        con.close()


def plot_dq_trend(trend: pd.DataFrame, output_path: str | Path, title: str, labels: list[str] | None = None) -> Path:
    """Save the two-panel trend chart as a PNG and return its path."""

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    labels = labels or [str(run)[-12:] for run in trend["run_id"]]
    x = list(range(len(trend)))
    fig, (left, right) = plt.subplots(1, 2, figsize=(11, 4.2), facecolor=SURFACE)

    def style(ax, heading: str) -> None:
        ax.set_facecolor(SURFACE)
        ax.set_title(heading, loc="left", fontsize=11, color=TEXT_PRIMARY)
        ax.set_xticks(x, labels, rotation=0, fontsize=8, color=TEXT_SECONDARY)
        ax.tick_params(axis="y", labelsize=8, colors=TEXT_SECONDARY, length=0)
        ax.tick_params(axis="x", length=0)
        ax.grid(axis="y", color=GRID, linewidth=0.8)
        for side in ("top", "right", "left"):
            ax.spines[side].set_visible(False)
        ax.spines["bottom"].set_color(GRID)
        ax.set_ylim(bottom=0)

    for column, colour, name in (
        ("critical_failed_checks", CRITICAL, "Critical"),
        ("warning_failed_checks", WARNING, "Warning"),
    ):
        left.plot(x, trend[column], color=colour, linewidth=2, marker="o", markersize=7,
                  markeredgecolor=SURFACE, markeredgewidth=2, label=name)
        # Direct label at the line end so identity is never colour alone.
        left.annotate(f"{name} {int(trend[column].iloc[-1])}", (x[-1], trend[column].iloc[-1]),
                      xytext=(8, 0), textcoords="offset points", va="center", fontsize=8, color=TEXT_PRIMARY)
    style(left, "Failed checks per run, by severity")
    left.legend(frameon=False, fontsize=8, loc="lower left", ncol=2, labelcolor=TEXT_SECONDARY)
    left.set_xlim(-0.3, len(x) - 0.3 + 0.6)

    # Order rows only: payment checks would swamp the chart on a low-volume
    # day (payments then reference "unknown" orders), hiding smaller changes.
    rows = trend["critical_failed_order_rows"]
    right.plot(x, rows, color=SERIES, linewidth=2, marker="o", markersize=7,
               markeredgecolor=SURFACE, markeredgewidth=2)
    for i in range(len(x)):
        if i == 0 or rows.iloc[i] != rows.iloc[i - 1]:
            right.annotate(f"{int(rows.iloc[i])}", (x[i], rows.iloc[i]), xytext=(0, 9),
                           textcoords="offset points", ha="center", fontsize=8, color=TEXT_PRIMARY)
    style(right, "Order rows failing Critical checks per run")
    right.set_ylim(top=rows.max() * 1.18)
    right.set_xlim(-0.3, len(x) - 0.7)

    fig.suptitle(title, x=0.01, ha="left", fontsize=13, color=TEXT_PRIMARY)
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, facecolor=SURFACE)
    plt.close(fig)
    return output_path
