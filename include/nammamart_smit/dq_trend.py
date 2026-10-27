"""
include/nammamart_smit/dq_trend.py
----------------------------------
Bonus: data quality trend chart, built from dq_results after every run.

One small chart per quality dimension (Accuracy, Completeness, ...), each
showing two lines over the DAG's runs:
  - rows failing Critical checks (quarantined)
  - rows flagged by Warning checks (loaded, but logged)
Each panel has its own y-scale: one bad run (e.g. 2,200 orphan payments)
would otherwise flatten every other dimension to zero.

Output is one self-contained HTML file (inline SVG + a few lines of JS for
the hover tooltip), so it opens anywhere without installing anything.
"""

import html
import json
import math
import os

from include.nammamart_smit.config import INCLUDE_DIR

REPORT_DIR = os.path.join(INCLUDE_DIR, "reports")
DIMENSIONS = ["Accuracy", "Completeness", "Consistency", "Timeliness", "Uniqueness", "Validity"]
SERIES = [("critical", "Critical rows failed"), ("warning", "Warning rows flagged")]

# panel geometry (SVG units)
W, H = 360, 190
LEFT, RIGHT, TOP, BOTTOM = 46, 14, 14, 30


def load_trend(con, dag_id):
    runs = con.execute("""
        SELECT run_id, min(checked_at) AS run_time
        FROM dq_results WHERE dag_id = ?
        GROUP BY 1 ORDER BY 2
    """, [dag_id]).fetchall()
    values = con.execute("""
        SELECT run_id, dimension,
               coalesce(sum(rows_failed) FILTER (WHERE severity = 'Critical'), 0) AS critical,
               coalesce(sum(rows_failed) FILTER (WHERE severity = 'Warning'), 0) AS warning
        FROM dq_results WHERE dag_id = ?
        GROUP BY ALL
    """, [dag_id]).fetchall()
    lookup = {(r, d): (int(c), int(w)) for r, d, c, w in values}
    data = {dim: {"critical": [], "warning": []} for dim in DIMENSIONS}
    for run_id, _ in runs:
        for dim in DIMENSIONS:
            critical, warning = lookup.get((run_id, dim), (0, 0))
            data[dim]["critical"].append(critical)
            data[dim]["warning"].append(warning)
    run_info = [{"run": i + 1, "run_id": r, "time": t.strftime("%Y-%m-%d %H:%M UTC")} for i, (r, t) in enumerate(runs)]
    return run_info, data


def nice_max(value):
    """Round the axis top up to 1, 2, 2.5 or 5 x a power of ten (e.g. 37 -> 50, 2206 -> 2500)."""
    if value <= 0:
        return 1
    power = 10 ** math.floor(math.log10(value))
    for step in [1, 2, 2.5, 5, 10]:
        if value <= step * power:
            return step * power
    return 10 * power


def panel_svg(dim, series, n_runs):
    peak = nice_max(max(max(series["critical"]), max(series["warning"])))
    plot_w, plot_h = W - LEFT - RIGHT, H - TOP - BOTTOM

    def x(i):
        return LEFT + (plot_w * i / max(n_runs - 1, 1))

    def y(v):
        return TOP + plot_h - plot_h * v / peak

    parts = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="{dim}: rows failing per run">']
    # gridlines + y labels (0, half, max)
    for v in [0, peak / 2, peak]:
        cls = "axis" if v == 0 else "grid"
        parts.append(f'<line class="{cls}" x1="{LEFT}" x2="{W - RIGHT}" y1="{y(v):.1f}" y2="{y(v):.1f}"/>')
        label = f"{v:,.0f}" if float(v).is_integer() else f"{v:,.1f}"
        parts.append(f'<text class="tick" x="{LEFT - 6}" y="{y(v) + 4:.1f}" text-anchor="end">{label}</text>')
    # x labels: first, every 5th, last
    for i in range(n_runs):
        if i == 0 or i == n_runs - 1 or (i + 1) % 5 == 0:
            parts.append(f'<text class="tick" x="{x(i):.1f}" y="{H - 10}" text-anchor="middle">{i + 1}</text>')
    # lines + end dots (warning first so critical draws on top)
    for key, _ in reversed(SERIES):
        points = " ".join(f"{x(i):.1f},{y(v):.1f}" for i, v in enumerate(series[key]))
        parts.append(f'<polyline class="line {key}" points="{points}"/>')
        last = series[key][-1]
        parts.append(f'<circle class="dot {key}" cx="{x(n_runs - 1):.1f}" cy="{y(last):.1f}" r="4"/>')
    # hover layer: crosshair + one dot per series, moved by JS
    parts.append(f'<line class="crosshair" x1="0" x2="0" y1="{TOP}" y2="{TOP + plot_h}" visibility="hidden"/>')
    for key, _ in SERIES:
        parts.append(f'<circle class="hover-dot {key}" r="5" visibility="hidden"/>')
    parts.append(f'<rect class="hit" x="{LEFT}" y="{TOP}" width="{plot_w}" height="{plot_h}" '
                 f'data-dim="{dim}" data-peak="{peak}"/>')
    parts.append("</svg>")
    return "".join(parts)


def build_chart(con, dag_id):
    """Write include/reports/dq_trend_<dag_id>.html and return its path."""
    runs, data = load_trend(con, dag_id)
    if not runs:
        return None
    os.makedirs(REPORT_DIR, exist_ok=True)
    path = os.path.join(REPORT_DIR, f"dq_trend_{dag_id}.html")

    panels = "".join(
        f'<figure class="panel"><figcaption>{dim}</figcaption>{panel_svg(dim, data[dim], len(runs))}</figure>'
        for dim in DIMENSIONS
    )
    legend = "".join(f'<span class="key"><i class="swatch {k}"></i>{label}</span>' for k, label in SERIES)
    header = "".join(f"<th>{d}<br><small>critical / warning</small></th>" for d in DIMENSIONS)
    rows = "".join(
        f"<tr><td>{r['run']}</td><td>{html.escape(r['run_id'])}</td><td>{r['time']}</td>"
        + "".join(f"<td class='num'>{data[d]['critical'][i]:,} / {data[d]['warning'][i]:,}</td>" for d in DIMENSIONS)
        + "</tr>"
        for i, r in enumerate(runs)
    )
    payload = json.dumps({"runs": runs, "data": data, "W": W, "LEFT": LEFT, "RIGHT": RIGHT, "TOP": TOP, "BOTTOM": BOTTOM, "H": H})

    page = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>DQ trend · {dag_id}</title>
<style>
.viz-root {{
  color-scheme: light;
  --surface-1: #fcfcfb; --page: #f9f9f7; --text-primary: #0b0b0b; --text-secondary: #52514e;
  --muted: #898781; --grid: #e1e0d9; --axis: #c3c2b7; --border: rgba(11,11,11,0.10);
  --series-1: #2a78d6; --series-2: #eb6834;
}}
@media (prefers-color-scheme: dark) {{
  :root:where(:not([data-theme="light"])) .viz-root {{
    color-scheme: dark;
    --surface-1: #1a1a19; --page: #0d0d0d; --text-primary: #f0efec; --text-secondary: #c3c2b7;
    --muted: #898781; --grid: #2c2c2a; --axis: #383835; --border: rgba(255,255,255,0.10);
    --series-1: #3987e5; --series-2: #d95926;
  }}
}}
:root[data-theme="dark"] .viz-root {{
  color-scheme: dark;
  --surface-1: #1a1a19; --page: #0d0d0d; --text-primary: #f0efec; --text-secondary: #c3c2b7;
  --muted: #898781; --grid: #2c2c2a; --axis: #383835; --border: rgba(255,255,255,0.10);
  --series-1: #3987e5; --series-2: #d95926;
}}
body {{ margin: 0; }}
.viz-root {{ background: var(--page); color: var(--text-primary); font: 14px system-ui, -apple-system, "Segoe UI", sans-serif;
  padding: 24px 16px; min-height: 100vh; box-sizing: border-box; }}
h1 {{ font-size: 18px; margin: 0 0 4px; }}
.sub {{ color: var(--text-secondary); margin: 0 0 14px; max-width: 900px; }}
.legend {{ display: flex; gap: 18px; flex-wrap: wrap; margin-bottom: 12px; color: var(--text-secondary); }}
.key {{ display: inline-flex; align-items: center; gap: 6px; }}
.swatch {{ width: 14px; height: 3px; border-radius: 2px; display: inline-block; }}
.swatch.critical {{ background: var(--series-1); }} .swatch.warning {{ background: var(--series-2); }}
.grid-panels {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 12px; max-width: 1200px; }}
.panel {{ margin: 0; background: var(--surface-1); border: 1px solid var(--border); border-radius: 8px; padding: 10px 8px 4px; position: relative; }}
figcaption {{ font-weight: 600; font-size: 13px; padding-left: 6px; }}
svg {{ width: 100%; height: auto; display: block; }}
.grid {{ stroke: var(--grid); stroke-width: 1; }} .axis {{ stroke: var(--axis); stroke-width: 1; }}
.tick {{ fill: var(--muted); font-size: 10px; font-variant-numeric: tabular-nums; }}
.line {{ fill: none; stroke-width: 2; stroke-linejoin: round; stroke-linecap: round; }}
.line.critical {{ stroke: var(--series-1); }} .line.warning {{ stroke: var(--series-2); }}
.dot, .hover-dot {{ stroke: var(--surface-1); stroke-width: 2; }}
.dot.critical, .hover-dot.critical {{ fill: var(--series-1); }} .dot.warning, .hover-dot.warning {{ fill: var(--series-2); }}
.crosshair {{ stroke: var(--axis); stroke-width: 1; }}
.hit {{ fill: transparent; cursor: crosshair; }}
.tip {{ position: fixed; pointer-events: none; background: var(--surface-1); color: var(--text-primary); border: 1px solid var(--border);
  border-radius: 6px; padding: 8px 10px; font-size: 12px; box-shadow: 0 4px 14px rgba(0,0,0,.12); max-width: 320px; }}
.tip .row {{ display: flex; align-items: center; gap: 6px; }} .tip .muted {{ color: var(--text-secondary); }}
details {{ margin-top: 16px; max-width: 1200px; }}
summary {{ cursor: pointer; color: var(--text-secondary); }}
.table-wrap {{ overflow-x: auto; }}
table {{ border-collapse: collapse; font-size: 12px; margin-top: 8px; background: var(--surface-1); }}
th, td {{ border: 1px solid var(--grid); padding: 4px 8px; text-align: left; white-space: nowrap; }}
td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
</style></head>
<body><div class="viz-root">
<h1>Data quality trend · {dag_id}</h1>
<p class="sub">Rows failing quality checks on every run of the DAG ({len(runs)} runs, oldest on the left), by dimension.
Each panel has its own scale. Source: <code>dq_results</code>, rebuilt after each run.</p>
<div class="legend">{legend}</div>
<div class="grid-panels">{panels}</div>
<details><summary>Table view</summary><div class="table-wrap"><table>
<tr><th>Run</th><th>run_id</th><th>Checked at</th>{header}</tr>{rows}</table></div></details>
<div class="tip" hidden></div>
</div>
<script>
const P = {payload};
const tip = document.querySelector('.tip');
const plotW = P.W - P.LEFT - P.RIGHT, plotH = P.H - P.TOP - P.BOTTOM, n = P.runs.length;
const xAt = i => P.LEFT + plotW * i / Math.max(n - 1, 1);
const fmt = v => v.toLocaleString('en-IN');
function show(i, evt) {{
  document.querySelectorAll('.hit').forEach(hit => {{
    const svg = hit.ownerSVGElement, dim = hit.dataset.dim, peak = +hit.dataset.peak;
    const cx = xAt(i), line = svg.querySelector('.crosshair');
    line.setAttribute('x1', cx); line.setAttribute('x2', cx); line.setAttribute('visibility', 'visible');
    ['critical', 'warning'].forEach(k => {{
      const dot = svg.querySelector('.hover-dot.' + k), v = P.data[dim][k][i];
      dot.setAttribute('cx', cx); dot.setAttribute('cy', P.TOP + plotH - plotH * v / peak); dot.setAttribute('visibility', 'visible');
    }});
  }});
  const dim = evt.target.dataset.dim, r = P.runs[i];
  tip.innerHTML = `<strong>Run ${{r.run}} · ${{dim}}</strong><div class="muted">${{r.run_id}}<br>${{r.time}}</div>` +
    `<div class="row"><i class="swatch critical"></i>Critical rows failed: <strong>${{fmt(P.data[dim].critical[i])}}</strong></div>` +
    `<div class="row"><i class="swatch warning"></i>Warning rows flagged: <strong>${{fmt(P.data[dim].warning[i])}}</strong></div>`;
  tip.hidden = false;
  const x = Math.min(evt.clientX + 14, window.innerWidth - tip.offsetWidth - 8);
  const y = Math.min(evt.clientY + 14, window.innerHeight - tip.offsetHeight - 8);
  tip.style.left = x + 'px'; tip.style.top = y + 'px';
}}
function hide() {{
  tip.hidden = true;
  document.querySelectorAll('.crosshair, .hover-dot').forEach(el => el.setAttribute('visibility', 'hidden'));
}}
document.querySelectorAll('.hit').forEach(hit => {{
  hit.addEventListener('mousemove', evt => {{
    const box = hit.getBoundingClientRect();
    const i = Math.round((evt.clientX - box.left) / box.width * (n - 1));
    show(Math.max(0, Math.min(n - 1, i)), evt);
  }});
  hit.addEventListener('mouseleave', hide);
}});
</script>
</body></html>
"""
    with open(path, "w") as f:
        f.write(page)
    print(f"[DQ trend] chart with {len(runs)} runs written to {path}")
    return path
