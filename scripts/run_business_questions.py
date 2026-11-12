#!/usr/bin/env python
"""
scripts/run_business_questions.py
---------------------------------
Runs the six business questions from the assignment against **both** pipelines
and writes the SQL plus the results to ``docs/evidence/business_questions.md``.

Usage:
    python scripts/run_business_questions.py
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from include.nammamart import config, warehouse  # noqa: E402

QUESTIONS = {
    1: "Daily net revenue by zone and by store (delivered orders only, after discount)",
    2: "Revenue and gross margin by product category (margin = revenue - quantity x cost_price)",
    3: "Average delivery time per store, and the % of delivered orders within 15 minutes",
    4: "Revenue by payment mode, and the total amount that doesn't reconcile with the gateway",
    5: "Top 10 customers by spend, with loyalty tier (no raw email or phone shown)",
    6: "Number of rows rejected today, by file and by reason",
}

EVIDENCE_DIR = PROJECT_ROOT / "docs" / "evidence"
SQL_DIR = config.SQL_DIR / "business_questions"
MAX_ROWS_SHOWN = 10


def run_question(con, number: int, pipeline: str) -> tuple[str, pd.DataFrame]:
    sql = (SQL_DIR / f"q{number}_{pipeline}.sql").read_text(encoding="utf-8")
    body = "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--")).strip()
    return sql.strip(), warehouse.query(con, body)


def to_markdown(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_(no rows)_"
    shown = frame.head(MAX_ROWS_SHOWN)
    header = "| " + " | ".join(str(c) for c in shown.columns) + " |"
    divider = "| " + " | ".join("---" for _ in shown.columns) + " |"
    rows = ["| " + " | ".join("" if pd.isna(v) else str(v) for v in record) + " |"
            for record in shown.itertuples(index=False)]
    table = "\n".join([header, divider, *rows])
    if len(frame) > MAX_ROWS_SHOWN:
        table += f"\n\n_{len(frame)} rows in total, first {MAX_ROWS_SHOWN} shown._"
    return table


def main() -> int:
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Six business questions - answered from both pipelines",
        "",
        f"Generated: {datetime.now():%Y-%m-%d %H:%M} by `scripts/run_business_questions.py` "
        f"against `{config.WAREHOUSE_PATH.name}`.",
        "",
        "Each question is answered twice: once from the **ETL** star schema "
        "(`etl_fact_orders` and its dimensions) and once from the **ELT** mart "
        "(`mart_*` tables built with SQL only).",
        "",
    ]

    with warehouse.connect(read_only=True) as con:
        for number, title in QUESTIONS.items():
            print(f"Q{number}: {title}")
            lines += [f"## Question {number}: {title}", ""]
            for pipeline, label in (("etl", "ETL"), ("elt", "ELT")):
                sql, frame = run_question(con, number, pipeline)
                lines += [
                    f"### {label}",
                    "",
                    "```sql",
                    sql,
                    "```",
                    "",
                    to_markdown(frame),
                    "",
                ]

    (EVIDENCE_DIR / "business_questions.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"\nWrote {EVIDENCE_DIR / 'business_questions.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
