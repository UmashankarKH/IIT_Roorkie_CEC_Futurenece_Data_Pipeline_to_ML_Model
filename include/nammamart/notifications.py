"""Failure notifications for NammaMart Airflow pipelines."""

import logging
import subprocess

from airflow.sdk import Variable


def _get_webhook_url():
    """Resolve the configured webhook without logging its value."""
    value = Variable.get("NAMMAMART_SLACK_WEBHOOK_URL", default=None)

    if value:
        return value

    # Fallback for environments where the CLI sees Variables
    # but the SDK lookup does not.
    result = subprocess.run(
        [
            "airflow", "variables", "get",
            "NAMMAMART_SLACK_WEBHOOK_URL",
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )

    if result.returncode == 0:
        return result.stdout.strip() or None

    return None


def notify_pipeline_failure(context):
    """Notify Slack or simulate delivery for a dummy webhook."""
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")

    dag_id = getattr(task_instance, "dag_id", "unknown")
    task_id = getattr(task_instance, "task_id", "unknown")
    run_id = getattr(dag_run, "run_id", "unknown")
    exception = context.get("exception")

    try:
        webhook_url = _get_webhook_url()

        if not webhook_url:
            logging.warning(
                "Failure notification skipped: webhook Variable unavailable."
            )
            return

        if "example.com" in webhook_url:
            logging.info(
                "DEMO MODE: Slack notification simulated for %s.%s "
                "(run_id=%s). No HTTP request sent.",
                dag_id, task_id, run_id,
            )
            return

        if not webhook_url.startswith("https://hooks.slack.com/services/"):
            logging.warning(
                "Failure notification skipped: webhook URL is not a "
                "recognized Slack incoming webhook."
            )
            return

        import requests

        response = requests.post(
            webhook_url,
            json={
                "text": (
                    ":red_circle: *NammaMart pipeline task failed*\n"
                    f"*DAG:* `{dag_id}`\n"
                    f"*Task:* `{task_id}`\n"
                    f"*Run:* `{run_id}`\n"
                    f"*Error:* {str(exception)[:1000] if exception else 'See Airflow logs'}"
                )
            },
            timeout=10,
        )
        response.raise_for_status()
        logging.info(
            "Slack failure notification sent for %s.%s",
            dag_id, task_id,
        )

    except Exception:
        logging.exception("Unable to process NammaMart failure notification")
