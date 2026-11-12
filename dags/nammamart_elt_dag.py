"""Airflow DAG for the NammaMart DuckDB ELT pipeline."""

import logging
from datetime import datetime, timezone
from pathlib import Path
from datetime import timedelta

import pandas as pd
import pendulum
from airflow.sdk import dag, task, get_current_context
from airflow.utils.trigger_rule import TriggerRule

from include.nammamart.audit import record_pipeline_run
from include.nammamart.elt import OUTPUT_DIR, run_elt
from include.nammamart.reconciliation import get_source_reconciliation_metrics
from include.nammamart.elt_audit import build_elt_dq_results


@dag(
    dag_id="nammamart_elt_pipeline",
    description="NammaMart SQL ELT pipeline using DuckDB",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 13, tz="Asia/Kolkata"),
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["nammamart", "elt", "duckdb", "data-quality"],
)
def nammamart_elt_pipeline():

    @task
    def run_transformations() -> dict:
        """Run ELT and persist run-level data-quality checks."""
        context = get_current_context()
        run_id = context["dag_run"].run_id

        results = run_elt(run_id=run_id)
        counts = {}

        for dataset in (
            "customers", "products", "stores", "orders", "payments"
        ):
            counts[dataset] = {
                "curated": int(results[f"{dataset}_curated"]["rows"]),
                "quarantine": int(results[f"{dataset}_quarantine"]["rows"]),
            }

        dq_results = build_elt_dq_results(run_id, counts)
        logging.info("ELT transformations completed: %s", counts)
        logging.info("Persisted %d ELT DQ checks", len(dq_results))

        rows_loaded = sum(v["curated"] for v in counts.values())
        rows_quarantined = sum(v["quarantine"] for v in counts.values())

        source_metrics = get_source_reconciliation_metrics()
        logging.info("Source reconciliation metrics: %s", source_metrics)

        return {
            "run_id": run_id,
            "counts": counts,
            "rows_extracted": source_metrics["rows_extracted"],
            "rows_deduplicated": source_metrics["rows_deduplicated"],
            "rows_superseded": source_metrics["rows_superseded"],
            "rows_loaded": rows_loaded,
            "rows_quarantined": rows_quarantined,
        }

    @task
    def validate_and_report(run_info: dict) -> dict:
        """Validate the generated ELT quality report."""
        report_path = OUTPUT_DIR / "elt_quality_report.csv"

        if not Path(report_path).is_file():
            raise FileNotFoundError(
                f"ELT quality report not found: {report_path}"
            )

        report = pd.read_csv(report_path)
        expected_counts = run_info["counts"]

        for _, row in report.iterrows():
            dataset = row["dataset"]

            if dataset not in expected_counts:
                raise ValueError(
                    f"Unexpected dataset in quality report: {dataset}"
                )

            expected = expected_counts[dataset]

            if int(row["curated_rows"]) != expected["curated"]:
                raise ValueError(f"{dataset}: curated row count mismatch")

            if int(row["quarantined_rows"]) != expected["quarantine"]:
                raise ValueError(f"{dataset}: quarantine row count mismatch")

            if not bool(row["business_key_unique"]):
                raise ValueError(
                    f"{dataset}: duplicate business keys detected"
                )

            if int(row["missing_business_keys"]) != 0:
                raise ValueError(
                    f"{dataset}: missing business keys detected"
                )

        if len(report) != len(expected_counts):
            raise ValueError("Quality report is missing datasets")

        # Check DQ results for this exact Airflow run.
        import duckdb
        from include.nammamart.audit import AUDIT_DB_PATH

        with duckdb.connect(str(AUDIT_DB_PATH), read_only=True) as con:
            dq = con.execute("""
                SELECT dimension, check_name, severity,
                       rows_failed, status
                FROM dq_results
                WHERE run_id = ? AND dag_id = ?
                ORDER BY dimension
            """, [run_info["run_id"], "nammamart_elt_pipeline"]).fetchdf()

        if dq.empty:
            raise ValueError(
                f"No DQ results found for run {run_info['run_id']}"
            )

        critical_failures = dq[
            (dq["severity"] == "Critical") & (dq["status"] != "PASS")
        ]
        warning_failures = dq[
            (dq["severity"] == "Warning") & (dq["status"] != "PASS")
        ]

        for _, row in warning_failures.iterrows():
            logging.warning(
                "DQ warning: %s (%s), failed rows=%s",
                row["check_name"], row["dimension"], row["rows_failed"],
            )

        if not critical_failures.empty:
            raise ValueError(
                "Critical DQ checks failed: "
                + ", ".join(critical_failures["check_name"].tolist())
            )

        logging.info(
            "ELT quality validation passed; no Critical DQ failures."
        )
        logging.info("\\n%s", report.to_string(index=False))

        return {
            "status": (
                "PASS_WITH_WARNINGS"
                if not warning_failures.empty else "PASS"
            ),
            "run_info": run_info,
            "datasets_validated": len(report),
            "quality_report": str(report_path),
            "critical_failures": len(critical_failures),
            "warning_failures": len(warning_failures),
        }

    @task
    def publish_summary(validation: dict) -> dict:
        """Publish and return the validation summary."""
        logging.info("NammaMart ELT pipeline summary: %s", validation)
        return validation

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def record_run() -> None:
        """Record run status even if a preceding task fails."""
        context = get_current_context()
        dag_run = context["dag_run"]
        ti = context["ti"]

        # Airflow 3-compatible status detection using upstream XComs.
        run_info = ti.xcom_pull(task_ids="run_transformations")
        validation = ti.xcom_pull(task_ids="validate_and_report")
        published = ti.xcom_pull(task_ids="publish_summary")

        failed = not all(
            isinstance(result, dict)
            for result in (run_info, validation, published)
        )

        run_info = run_info if isinstance(run_info, dict) else {}
        now = datetime.now(timezone.utc)

        record_pipeline_run(
            run_id=dag_run.run_id,
            dag_id=dag_run.dag_id,
            start_time=dag_run.start_date or now,
            end_time=now,
            rows_extracted=run_info.get("rows_extracted", 0),
            rows_loaded=run_info.get("rows_loaded", 0),
            rows_quarantined=run_info.get("rows_quarantined", 0),
            rows_deduplicated=run_info.get("rows_deduplicated", 0),
            rows_superseded=run_info.get("rows_superseded", 0),
            status="FAILED" if failed else "SUCCESS",
        )

    run_info = run_transformations()
    validation = validate_and_report(run_info)
    published = publish_summary(validation)
    published >> record_run()


nammamart_elt_dag = nammamart_elt_pipeline()
