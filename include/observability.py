"""Slack failure alert and dependency-free SVG DQ trend chart helpers."""

from __future__ import annotations

import html
import json
import logging
from pathlib import Path
from urllib.request import Request, urlopen

import duckdb


def slack_dag_failure_alert(context: dict) -> None:
    """Send one concise DAG failure alert when SLACK_WEBHOOK_URL is configured."""
    from airflow.models import Variable

    webhook = Variable.get("SLACK_WEBHOOK_URL", default_var="").strip()
    if not webhook:
        logging.getLogger(__name__).warning(
            "DAG failed but SLACK_WEBHOOK_URL is not configured; no Slack alert sent."
        )
        return

    dag_run = context.get("dag_run")
    dag_id = getattr(dag_run, "dag_id", "unknown DAG")
    run_id = getattr(dag_run, "run_id", "unknown run")
    message = f"Airflow DAG failed: {dag_id} | run_id={run_id}"
    request = Request(
        webhook,
        data=json.dumps({"text": message}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=10) as response:
            if response.status >= 300:
                raise RuntimeError(f"Slack webhook returned HTTP {response.status}")
    except Exception:
        logging.getLogger(__name__).exception("Could not send DAG failure alert to Slack.")


def generate_dq_trend_svg(
    warehouse_path: str | Path,
    dag_id: str,
    output_path: str | Path,
    limit: int = 20,
) -> str:
    """Plot recent failed and warning check counts as a standalone SVG."""
    connection = duckdb.connect(str(warehouse_path), read_only=True)
    try:
        rows = connection.execute(
            """
            SELECT run_id,
                   COUNT(*) FILTER (WHERE status = 'FAIL') AS failed_checks,
                   COUNT(*) FILTER (WHERE status = 'WARN') AS warning_checks,
                   MAX(checked_at) AS checked_at
            FROM dq_results
            WHERE dag_id = ?
            GROUP BY run_id
            ORDER BY checked_at DESC
            LIMIT ?
            """,
            [dag_id, limit],
        ).fetchall()
    finally:
        connection.close()

    rows.reverse()
    width, height = 1000, 420
    left, right, top, bottom = 72, 28, 52, 94
    plot_width, plot_height = width - left - right, height - top - bottom
    peak = max([1] + [max(int(row[1]), int(row[2])) for row in rows])

    def point(index: int, value: int) -> tuple[float, float]:
        x = left if len(rows) < 2 else left + index * plot_width / (len(rows) - 1)
        y = top + plot_height - int(value) * plot_height / peak
        return x, y

    def polyline(column: int, color: str) -> str:
        coords = " ".join(
            f"{x:.1f},{y:.1f}" for i, row in enumerate(rows)
            for x, y in [point(i, int(row[column]))]
        )
        return f'<polyline fill="none" stroke="{color}" stroke-width="3" points="{coords}"/>' if coords else ""

    elements = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        f'<text x="{left}" y="30" font-family="sans-serif" font-size="20" font-weight="bold">DQ trend — {html.escape(dag_id)}</text>',
    ]
    for tick in range(5):
        value = round(peak * tick / 4)
        y = top + plot_height - tick * plot_height / 4
        elements.append(f'<line x1="{left}" y1="{y:.1f}" x2="{width-right}" y2="{y:.1f}" stroke="#ddd"/>')
        elements.append(f'<text x="{left-12}" y="{y+4:.1f}" text-anchor="end" font-family="sans-serif" font-size="11" fill="#555">{value}</text>')

    elements.extend([polyline(1, "#c0392b"), polyline(2, "#d68910")])
    for i, row in enumerate(rows):
        x, y = point(i, int(row[1]))
        elements.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="#c0392b"/>')
        x, y = point(i, int(row[2]))
        elements.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="#d68910"/>')
        if len(rows) <= 12 or i % max(1, len(rows) // 10) == 0:
            label = html.escape(str(row[0])[-12:])
            elements.append(f'<text transform="translate({x:.1f},{height-bottom+18}) rotate(45)" font-family="sans-serif" font-size="9" fill="#555">{label}</text>')

    elements.extend([
        f'<line x1="{left}" y1="{top+plot_height}" x2="{width-right}" y2="{top+plot_height}" stroke="#555"/>',
        f'<text x="{left}" y="{height-20}" font-family="sans-serif" font-size="12" fill="#c0392b">Failed checks</text>',
        f'<text x="{left+130}" y="{height-20}" font-family="sans-serif" font-size="12" fill="#d68910">Warning checks</text>',
        f'<text x="{width-right}" y="{height-20}" text-anchor="end" font-family="sans-serif" font-size="11" fill="#555">Most recent {len(rows)} runs (oldest to newest)</text>',
        '</svg>',
    ])
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text("\n".join(elements), encoding="utf-8")
    print(f"[DQ trend] Wrote {len(rows)} runs to {destination}")
    return str(destination)
