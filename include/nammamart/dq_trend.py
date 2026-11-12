"""Generate a DQ trend report and standalone HTML chart from audit history."""
from __future__ import annotations

import html
from pathlib import Path

import duckdb
import pandas as pd

from include.nammamart.audit import AUDIT_DB_PATH

OUTPUT_DIR = Path(__file__).resolve().parents[1] / "output"
TREND_CSV = OUTPUT_DIR / "dq_trend.csv"
SUMMARY_CSV = OUTPUT_DIR / "dq_trend_summary.csv"
CHART_HTML = OUTPUT_DIR / "dq_trend.html"


def build_dq_trend() -> dict[str, str | int]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(AUDIT_DB_PATH), read_only=True) as con:
        trend = con.execute("""
            SELECT
                dag_id,
                run_id,
                MAX(checked_at) AS checked_at,
                COUNT(*) AS total_checks,
                COUNT(*) FILTER (WHERE UPPER(status) = 'PASS')
                    AS passed_checks,
                COUNT(*) FILTER (WHERE UPPER(status) = 'FAIL')
                    AS failed_checks,
                COALESCE(SUM(rows_failed), 0) AS failed_rows,
                ROUND(
                    100.0 * COUNT(*) FILTER (WHERE UPPER(status) = 'PASS')
                    / NULLIF(COUNT(*), 0),
                    2
                ) AS pass_rate_pct
            FROM dq_results
            GROUP BY dag_id, run_id
            ORDER BY checked_at, dag_id
        """).fetchdf()

    if trend.empty:
        raise ValueError("No DQ history found in dq_results")

    trend["checked_at"] = pd.to_datetime(trend["checked_at"], utc=True)
    trend.to_csv(TREND_CSV, index=False)

    summary = (
        trend.groupby("dag_id", as_index=False)
        .agg(
            runs=("run_id", "nunique"),
            checks=("total_checks", "sum"),
            failed_checks=("failed_checks", "sum"),
            failed_rows=("failed_rows", "sum"),
            average_pass_rate_pct=("pass_rate_pct", "mean"),
        )
    )
    summary["average_pass_rate_pct"] = summary[
        "average_pass_rate_pct"
    ].round(2)
    summary.to_csv(SUMMARY_CSV, index=False)

    pipelines = list(trend["dag_id"].drop_duplicates())
    width, height = 1000, 460
    left, right, top, bottom = 80, 30, 35, 90
    plot_width = width - left - right
    plot_height = height - top - bottom

    max_failed = max(1, int(trend["failed_checks"].max()))
    max_y = max(5, ((max_failed + 4) // 5) * 5)
    dates = trend["checked_at"]
    min_time, max_time = dates.min(), dates.max()
    time_span = (max_time - min_time).total_seconds()

    def x_position(value) -> float:
        if time_span == 0:
            return left + plot_width / 2
        return left + (
            (value - min_time).total_seconds() / time_span
        ) * plot_width

    def y_position(value: int) -> float:
        return top + plot_height - (value / max_y) * plot_height

    colors = ["#2563eb", "#dc2626", "#059669", "#9333ea"]
    svg_parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" '
        'aria-label="Failed data quality checks by pipeline run">'
    ]

    for tick in range(0, max_y + 1, max(1, max_y // 5)):
        y = y_position(tick)
        svg_parts.append(
            f'<line x1="{left}" y1="{y:.1f}" '
            f'x2="{width-right}" y2="{y:.1f}" stroke="#d1d5db"/>'
            f'<text x="{left-12}" y="{y+4:.1f}" '
            'text-anchor="end" font-size="12">'
            f'{tick}</text>'
        )

    svg_parts.append(
        f'<line x1="{left}" y1="{top}" x2="{left}" '
        f'y2="{height-bottom}" stroke="#374151"/>'
    )
    svg_parts.append(
        f'<line x1="{left}" y1="{height-bottom}" '
        f'x2="{width-right}" y2="{height-bottom}" stroke="#374151"/>'
    )

    legend = []
    for index, pipeline in enumerate(pipelines):
        group = trend[trend["dag_id"] == pipeline].sort_values("checked_at")
        color = colors[index % len(colors)]
        points = [
            (
                x_position(row.checked_at),
                y_position(int(row.failed_checks)),
            )
            for row in group.itertuples()
        ]
        if points:
            coords = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
            svg_parts.append(
                f'<polyline points="{coords}" fill="none" '
                f'stroke="{color}" stroke-width="2.5"/>'
            )
            for x, y in points:
                svg_parts.append(
                    f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" '
                    f'fill="{color}"/>'
                )
        legend.append(
            f'<span><i style="background:{color}"></i>'
            f'{html.escape(pipeline)}</span>'
        )

    for index, value in enumerate([min_time, max_time]):
        x = x_position(value)
        label = value.strftime("%Y-%m-%d %H:%M UTC")
        anchor = "start" if index == 0 else "end"
        svg_parts.append(
            f'<text x="{x:.1f}" y="{height-bottom+24}" '
            f'text-anchor="{anchor}" font-size="11">'
            f'{html.escape(label)}</text>'
        )

    svg_parts.append(
        f'<text x="20" y="{top + plot_height/2}" '
        'transform="rotate(-90 20 '
        f'{top + plot_height/2})" text-anchor="middle" '
        'font-size="13">Failed checks</text>'
    )
    svg_parts.append("</svg>")

    table_rows = []
    for row in trend.sort_values("checked_at", ascending=False).head(20).itertuples():
        table_rows.append(
            "<tr>"
            f"<td>{html.escape(str(row.checked_at))}</td>"
            f"<td>{html.escape(row.dag_id)}</td>"
            f"<td>{html.escape(row.run_id)}</td>"
            f"<td>{row.total_checks}</td>"
            f"<td>{row.failed_checks}</td>"
            f"<td>{row.failed_rows}</td>"
            f"<td>{row.pass_rate_pct:.2f}%</td>"
            "</tr>"
        )

    page = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NammaMart Data Quality Trends</title>
<style>
body{{font-family:system-ui,sans-serif;max-width:1100px;margin:2rem auto;
padding:0 1rem;color:#111827}}
svg{{width:100%;height:auto}}
.legend{{display:flex;flex-wrap:wrap;gap:1rem;margin:1rem 0}}
.legend i{{display:inline-block;width:12px;height:12px;margin-right:6px}}
table{{border-collapse:collapse;width:100%;font-size:13px}}
th,td{{padding:8px;border-bottom:1px solid #e5e7eb;text-align:left}}
th{{background:#f3f4f6}}
.note{{color:#4b5563}}
</style>
</head>
<body>
<h1>NammaMart Data Quality Trends</h1>
<p class="note">Failed checks by pipeline run. A run may contain both
PASS and FAIL checks; each check is counted independently.</p>
<div class="legend">{''.join(legend)}</div>
{''.join(svg_parts)}
<h2>Latest 20 run summaries</h2>
<table><thead><tr><th>Checked at</th><th>Pipeline</th>
<th>Run ID</th><th>Total checks</th><th>Failed checks</th>
<th>Failed rows</th><th>Pass rate</th></tr></thead>
<tbody>{''.join(table_rows)}</tbody></table>
<p class="note">Source: dq_results in the NammaMart audit database.</p>
</body></html>
"""
    CHART_HTML.write_text(page, encoding="utf-8")

    return {
        "runs": int(len(trend)),
        "trend_csv": str(TREND_CSV),
        "summary_csv": str(SUMMARY_CSV),
        "chart_html": str(CHART_HTML),
    }


if __name__ == "__main__":
    print(build_dq_trend())
