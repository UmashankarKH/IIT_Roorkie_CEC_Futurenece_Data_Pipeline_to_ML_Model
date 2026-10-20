"""
include/nammamart/alerts.py
---------------------------
Failure notification helper (assignment bonus: "a Slack or email alert on
failure using an Airflow Variable").

**Airflow Variable**: a key/value setting stored by Airflow, outside the DAG
file.  Java comparison (learning aid): the same role as an external property in
``application.yml`` or an environment variable - the value lives with the
deployment, never in the source code.

Security rule applied here: the webhook URL is read at run time with
``Variable.get`` and is never printed, logged or committed.  If the variable is
not configured the pipeline still runs; it simply logs that no alert channel is
set up.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

SLACK_WEBHOOK_VARIABLE = "SLACK_WEBHOOK_URL"
REQUEST_TIMEOUT_SECONDS = 10


def _get_webhook_url() -> str | None:
    """Read the webhook URL from an Airflow Variable, if Airflow is available."""
    try:
        from airflow.models import Variable  # imported lazily: tests run without Airflow
    except ImportError:
        logger.debug("Airflow not available - no alert channel")
        return None
    try:
        url = Variable.get(SLACK_WEBHOOK_VARIABLE, default_var=None)
    except Exception as exc:  # pragma: no cover - depends on the Airflow metadata DB
        logger.warning("Could not read Airflow Variable %s: %s", SLACK_WEBHOOK_VARIABLE, exc)
        return None
    return url or None


def send_failure_alert(message: str) -> bool:
    """Post a short failure message to Slack. Returns True when it was sent.

    The function never raises: an alerting problem must not hide the original
    pipeline failure.
    """
    url = _get_webhook_url()
    if not url:
        logger.warning("[Alert] %s (no %s variable configured, alert not sent)",
                       message, SLACK_WEBHOOK_VARIABLE)
        return False

    payload = json.dumps({"text": message}).encode("utf-8")
    request = urllib.request.Request(url, data=payload,
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            ok = 200 <= response.status < 300
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        # The URL is deliberately not included in the log line.
        logger.error("[Alert] could not reach the Slack webhook: %s", type(exc).__name__)
        return False

    logger.info("[Alert] failure notification sent to Slack")
    return ok


def task_failure_callback(context: dict) -> None:
    """Airflow ``on_failure_callback``: announce which task failed, and why."""
    task_instance = context.get("task_instance")
    dag_id = getattr(task_instance, "dag_id", "unknown_dag")
    task_id = getattr(task_instance, "task_id", "unknown_task")
    run_id = context.get("run_id", "unknown_run")
    exception = context.get("exception")
    message = (f":rotating_light: NammaMart pipeline failure\n"
               f"DAG: {dag_id}\nTask: {task_id}\nRun: {run_id}\n"
               f"Error: {type(exception).__name__ if exception else 'unknown'}: "
               f"{str(exception)[:300]}")
    send_failure_alert(message)
