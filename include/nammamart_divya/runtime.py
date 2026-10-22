"""
Airflow-facing helpers shared by both DAGs: schedule settings, business-date
cutoff, per-run work folder, failure alert and the run-audit row.
"""

import re
from datetime import datetime, timedelta

import pandas as pd
import pendulum

from nammamart_divya.config import DEFAULT_BUSINESS_DATE, EXPECTED_COLUMNS, INCLUDE_DIR

IST = pendulum.timezone("Asia/Kolkata")

# 09:00, Monday (1) to Saturday (6), evaluated in the DAG's timezone (IST).
SCHEDULE_CRON = "0 9 * * 1-6"
START_DATE = pendulum.datetime(2026, 10, 1, tz=IST)

DEFAULT_ARGS = {
    "owner": "divya",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}

WORK_ROOT = INCLUDE_DIR / "tmp" / "divya"


def work_dir(dag_id, run_id):
    """Run-scoped folder for intermediate files (kept small; XCom only carries paths)."""
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)
    path = WORK_ROOT / dag_id / safe
    path.mkdir(parents=True, exist_ok=True)
    return path


def business_cutoff():
    """
    Timestamps at or after this instant are 'future-dated'.
    = end of the business date of the latest export, never later than now.
    """
    from airflow.sdk import Variable

    business_date = Variable.get("nammamart_business_date", default=DEFAULT_BUSINESS_DATE)
    end_of_day = datetime.strptime(business_date, "%Y-%m-%d") + timedelta(days=1)
    now_ist = datetime.now(IST).replace(tzinfo=None)
    return min(end_of_day, now_ist)


def read_source_csv(path, file_key):
    """Read a source file as text exactly as received and enforce the column contract."""
    if not path.exists():
        raise FileNotFoundError(f"Source file missing: {path}")
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = [c for c in EXPECTED_COLUMNS[file_key] if c not in df.columns]
    extra = [c for c in df.columns if c not in EXPECTED_COLUMNS[file_key]]
    if missing or extra:
        raise ValueError(
            f"Schema change in {path.name}: missing columns {missing}, unexpected columns {extra}"
        )
    return df


def notify_failure(context):
    """on_failure_callback: Slack alert if the Variable SLACK_WEBHOOK_URL is set, else log only."""
    from airflow.sdk import Variable

    ti = context["ti"]
    message = (
        f":red_circle: NammaMart pipeline failure\n"
        f"DAG: {ti.dag_id}\nTask: {ti.task_id}\nRun: {ti.run_id}\n"
        f"Try: {ti.try_number}\nError: {context.get('exception')}"
    )
    webhook = Variable.get("SLACK_WEBHOOK_URL", default=None)
    if not webhook:
        print("[alert] SLACK_WEBHOOK_URL not set; alert logged only:\n" + message)
        return
    import json
    import urllib.request

    req = urllib.request.Request(
        webhook, data=json.dumps({"text": message}).encode(), method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as exc:          # an alert must never break the pipeline
        print(f"[alert] Slack post failed: {exc}")


def run_audit(dag_id, counts, warehouse_write):
    """
    Final all_done task body: collects every task state for this run, writes one
    pipeline_runs row, then fails itself if anything upstream failed so the DAG
    run is not wrongly shown as green.
    """
    from airflow.sdk import get_current_context
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

    from nammamart_divya.warehouse import now_utc

    ctx = get_current_context()
    ti, dag_run = ctx["ti"], ctx["dag_run"]
    states = RuntimeTaskInstance.get_task_states(dag_id=dag_id, run_ids=[ti.run_id]).get(ti.run_id, {})
    failed = sorted(t for t, s in states.items()
                    if t != ti.task_id and s in ("failed", "upstream_failed"))
    status = "FAILED" if failed else "SUCCESS"
    start = dag_run.start_date
    row = {
        "run_id": ti.run_id,
        "dag_id": dag_id,
        "start_time": start.astimezone(pendulum.UTC).replace(tzinfo=None) if start else None,
        "end_time": now_utc(),
        "rows_extracted": counts.get("rows_extracted"),
        "rows_loaded": counts.get("rows_loaded"),
        "rows_quarantined": counts.get("rows_quarantined"),
        "status": status,
        "failed_tasks": ",".join(failed) or None,
    }
    warehouse_write(row)
    print(f"[audit] {row}")
    print(f"[audit] task states: {states}")
    if failed:
        raise RuntimeError(f"Run {ti.run_id} FAILED; failed tasks: {failed}")
    return row
