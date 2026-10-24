"""Slack alerting shared by the NammaMart DAGs.

The webhook URL is read from the Airflow Variable ``SLACK_WEBHOOK_URL`` at
alert time and is never stored in code. Without it, ``send_slack_alert``
prints the message to the task log instead, so alerts stay visible locally.
"""

from __future__ import annotations

from typing import Any

from slack_notifier import send_slack_alert


def _webhook_url() -> str | None:
    """Read SLACK_WEBHOOK_URL from Airflow Variables (None outside Airflow)."""

    try:
        from airflow.sdk import Variable

        return Variable.get("SLACK_WEBHOOK_URL", default=None)
    except Exception:  # noqa: BLE001 - alerting must never fail the task itself.
        return None


def format_failure_message(context: dict[str, Any]) -> str:
    """Build a short, actionable failure message from an Airflow task context."""

    ti = context.get("task_instance") or context.get("ti")
    dag_id = getattr(ti, "dag_id", None) or getattr(context.get("dag"), "dag_id", "unknown DAG")
    task_id = getattr(ti, "task_id", "unknown task")
    attempt = getattr(ti, "try_number", "?")
    error = str(context.get("exception") or "see task log")[:300]
    lines = [
        ":red_circle: *NammaMart pipeline failure*",
        f"*DAG:* {dag_id}",
        f"*Task:* {task_id} (attempt {attempt}, retries exhausted)",
        f"*Run:* {context.get('run_id', 'unknown run')}",
        f"*Error:* {error}",
    ]
    log_url = getattr(ti, "log_url", None)
    if log_url:
        lines.append(f"*Logs:* {log_url}")
    lines.append("The 9:00 AM report is not ready; fix the cause and Clear the failed task.")
    return "\n".join(lines)


def notify_failure(context: dict[str, Any]) -> None:
    """``on_failure_callback``: runs once a task has failed its final retry."""

    send_slack_alert(format_failure_message(context), webhook_url=_webhook_url())


def notify_quality_warning(dag_id: str, run_id: str, failed_checks: list[str]) -> None:
    """Alert on operational quality warnings that do not fail the run (e.g. volume drop)."""

    if not failed_checks:
        return
    message = "\n".join(
        [f":warning: *NammaMart data-quality warning* ({dag_id}, run {run_id})"]
        + [f"• {check}" for check in failed_checks]
        + ["The run completed; check dq_results before using today's numbers."]
    )
    send_slack_alert(message, webhook_url=_webhook_url())
