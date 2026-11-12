"""Airflow DAG for the NammaMart pandas ETL and DuckDB star schema."""
from __future__ import annotations

import logging
import re
import shutil
from datetime import timedelta, timezone, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pendulum
from airflow.sdk import dag, task, get_current_context
from airflow.utils.trigger_rule import TriggerRule

from include.nammamart.audit import AUDIT_DB_PATH, record_pipeline_run
from include.nammamart.etl import OUTPUT_DIR, SOURCE_DIR, build_quality_report, run_etl
from include.nammamart.etl_warehouse import (
    WAREHOUSE_PATH,
    build_etl_payment_reconciliation,
    build_etl_reports,
    load_etl_star_schema,
)
from include.nammamart.reconciliation import get_source_reconciliation_metrics
from include.nammamart.notifications import notify_pipeline_failure

EXTRACT_DIR = OUTPUT_DIR.parent / "etl_extracts"


def _safe_run_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id).strip("_.") or "manual_run"


def _extract_group(run_id: str, filenames: list[str]) -> dict:
    """Copy source files into a run-scoped immutable extraction directory."""
    safe_id = _safe_run_id(run_id)
    run_dir = EXTRACT_DIR / safe_id
    run_dir.mkdir(parents=True, exist_ok=True)
    file_counts: dict[str, int] = {}

    for filename in filenames:
        source_path = SOURCE_DIR / filename
        if not source_path.is_file():
            raise FileNotFoundError(f"Required source file is missing: {source_path}")
        target_path = run_dir / filename
        shutil.copy2(source_path, target_path)
        # Parse the copied file to validate readability and count physical records.
        frame = pd.read_csv(target_path, dtype="string", keep_default_na=False)
        file_counts[filename] = int(len(frame))
        logging.info("Extracted %s: %d rows -> %s", filename, len(frame), target_path)

    return {
        "source_dir": str(run_dir),
        "files": file_counts,
        "rows_extracted": sum(file_counts.values()),
    }


@dag(
    dag_id="nammamart_etl_pipeline",
    description="Parallel source extraction, Python validation, and DuckDB star-schema ETL",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 13, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
        "on_failure_callback": notify_pipeline_failure,
    },
    tags=["nammamart", "etl", "data-quality", "duckdb"],
)
def nammamart_etl_pipeline():
    # Five independent source tasks run in parallel. The orders task extracts
    # both the base and incremental order exports as one logical feed.
    @task
    def extract_customers() -> dict:
        run_id = get_current_context()["dag_run"].run_id
        return _extract_group(run_id, ["customers.csv"])

    @task
    def extract_products() -> dict:
        run_id = get_current_context()["dag_run"].run_id
        return _extract_group(run_id, ["products.csv"])

    @task
    def extract_stores() -> dict:
        run_id = get_current_context()["dag_run"].run_id
        return _extract_group(run_id, ["stores.csv"])

    @task
    def extract_orders() -> dict:
        run_id = get_current_context()["dag_run"].run_id
        return _extract_group(
            run_id,
            ["orders.csv", "orders_2026-10-01_increment.csv"],
        )

    @task
    def extract_payments() -> dict:
        run_id = get_current_context()["dag_run"].run_id
        return _extract_group(run_id, ["payments.csv"])

    @task
    def validate_extractions(extractions: list[dict]) -> dict:
        """Validate all extracted file schemas before transformations begin."""
        if not extractions:
            raise ValueError("No extraction manifests were provided")

        source_dirs = {item["source_dir"] for item in extractions}
        if len(source_dirs) != 1:
            raise ValueError(f"Extraction tasks wrote to different run directories: {source_dirs}")
        source_dir = Path(next(iter(source_dirs)))
        extracted_files = {
            filename: count
            for item in extractions
            for filename, count in item["files"].items()
        }
        expected_columns = {
            "customers.csv": {
                "customer_id", "full_name", "email", "phone", "city", "area",
                "pincode", "signup_date", "loyalty_tier", "is_active",
            },
            "products.csv": {
                "product_id", "product_name", "category", "sub_category", "brand",
                "mrp", "cost_price", "unit", "is_perishable", "launch_date",
            },
            "stores.csv": {
                "store_id", "store_name", "zone", "area", "pincode", "open_date",
                "manager_name", "capacity_orders_per_day",
            },
            "orders.csv": {
                "order_id", "order_ts", "customer_id", "store_id", "product_id",
                "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode",
                "order_status", "channel", "delivery_minutes",
            },
            "orders_2026-10-01_increment.csv": {
                "order_id", "order_ts", "customer_id", "store_id", "product_id",
                "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode",
                "order_status", "channel", "delivery_minutes",
            },
            "payments.csv": {
                "payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid",
                "payment_status", "gateway_ref",
            },
        }
        if set(extracted_files) != set(expected_columns):
            raise ValueError(
                "Unexpected extracted file set: "
                f"expected={sorted(expected_columns)}, actual={sorted(extracted_files)}"
            )

        for filename, expected in expected_columns.items():
            frame = pd.read_csv(source_dir / filename, dtype="string", keep_default_na=False)
            actual = set(frame.columns)
            if actual != expected:
                raise ValueError(
                    f"Schema mismatch in {filename}; missing={sorted(expected - actual)}, "
                    f"unexpected={sorted(actual - expected)}"
                )
            logging.info(
                "Schema validation passed for %s: %d rows, %d columns",
                filename, len(frame), len(frame.columns),
            )

        source_metrics = get_source_reconciliation_metrics(source_dir=source_dir)
        rows_extracted = sum(extracted_files.values())
        if rows_extracted != source_metrics["rows_extracted"]:
            raise ValueError(
                f"Extraction count {rows_extracted} does not match source reconciliation "
                f"{source_metrics['rows_extracted']}"
            )
        logging.info("All source schemas validated; extracted rows=%d", rows_extracted)
        return {
            "source_dir": str(source_dir),
            "extracted_files": extracted_files,
            "rows_extracted": rows_extracted,
            "rows_deduplicated": source_metrics["rows_deduplicated"],
            "rows_superseded": source_metrics["rows_superseded"],
        }

    @task
    def transform_and_load(validated_sources: dict) -> dict:
        """Apply Python data-quality rules and load the DuckDB star schema."""
        context = get_current_context()
        run_id = context["dag_run"].run_id
        source_dir = Path(validated_sources["source_dir"])
        outputs = run_etl(run_id=run_id, source_dir=source_dir)
        quality_report = build_quality_report()
        star_counts = load_etl_star_schema(run_id=run_id)
        rows_loaded = int(quality_report["curated_rows"].sum())
        rows_quarantined = int(quality_report["quarantined_rows"].sum())

        logging.info("Parallel extraction counts: %s", validated_sources["extracted_files"])
        logging.info("ETL curated/quarantine report:\n%s", quality_report.to_string(index=False))
        logging.info("ETL star-schema table counts: %s", star_counts)
        logging.info("ETL generated %d output artifact groups", len(outputs))

        return {
            "run_id": run_id,
            "source_dir": str(source_dir),
            "extracted_files": validated_sources["extracted_files"],
            "rows_extracted": validated_sources["rows_extracted"],
            "rows_deduplicated": validated_sources["rows_deduplicated"],
            "rows_superseded": validated_sources["rows_superseded"],
            "rows_loaded": rows_loaded,
            "rows_quarantined": rows_quarantined,
            "star_schema_counts": star_counts,
            "quality_report": str(OUTPUT_DIR / "quality_report.csv"),
            "warehouse": str(WAREHOUSE_PATH),
        }

    @task
    def reconcile_payments(run_info: dict) -> dict:
        count = build_etl_payment_reconciliation(
            source_dir=Path(run_info["source_dir"]),
            run_id=run_info["run_id"],
        )
        logging.info(
            "Created etl_payment_reconciliation with %d mismatch rows", count
        )
        return {"run_id": run_info["run_id"], "mismatch_rows": count}

    @task
    def build_reports(run_info: dict, reconciliation: dict) -> dict:
        if run_info["run_id"] != reconciliation["run_id"]:
            raise ValueError("Run ID mismatch between ETL and reconciliation")
        report_results = build_etl_reports()
        logging.info("ETL report table counts: %s", report_results["table_counts"])
        logging.info(
            "ETL daily zone revenue sample: %s",
            report_results["daily_zone_revenue_sample"],
        )
        logging.info("ETL category margin report: %s", report_results["category_margin"])
        logging.info(
            "ETL store capacity sample: %s",
            report_results["store_capacity_sample"],
        )
        return {
            "run_id": run_info["run_id"],
            "report_table_counts": report_results["table_counts"],
            "daily_zone_revenue_sample_rows": len(report_results["daily_zone_revenue_sample"]),
            "category_margin_rows": len(report_results["category_margin"]),
            "store_capacity_sample_rows": len(report_results["store_capacity_sample"]),
        }

    @task
    def validate_and_report(run_info: dict, report_info: dict) -> dict:
        """Check curated keys, the six quality dimensions, and required tables."""
        report = build_quality_report()
        invalid_keys = report.loc[~report["business_key_unique"]]
        missing_keys = int(report["missing_business_keys"].sum())
        if not invalid_keys.empty:
            raise ValueError(
                "Duplicate business keys found in: "
                f"{invalid_keys['dataset'].tolist()}"
            )
        if missing_keys:
            raise ValueError(f"Curated datasets contain {missing_keys} missing business keys")

        with duckdb.connect(str(AUDIT_DB_PATH), read_only=True) as con:
            dq = con.execute(
                """SELECT dimension, check_name, severity, rows_failed, status
                   FROM dq_results WHERE run_id = ? AND dag_id = ?""",
                [run_info["run_id"], "nammamart_etl_pipeline"],
            ).fetchdf()
        if dq.empty:
            raise ValueError(f"No ETL dq_results found for run {run_info['run_id']}")
        critical_failures = dq[(dq["severity"] == "Critical") & (dq["status"] != "PASS")]
        warning_failures = dq[(dq["severity"] == "Warning") & (dq["status"] != "PASS")]
        if not critical_failures.empty:
            raise ValueError(
                "Critical DQ checks failed: "
                + ", ".join(critical_failures["check_name"].tolist())
            )
        dimensions = set(dq["dimension"].dropna().astype(str))
        required_dimensions = {
            "Completeness", "Uniqueness", "Validity",
            "Accuracy", "Consistency", "Timeliness",
        }
        missing_dimensions = required_dimensions - dimensions
        if missing_dimensions:
            raise ValueError(f"Missing ETL DQ dimensions: {sorted(missing_dimensions)}")

        logging.info("ETL DQ result count=%d; warnings=%d", len(dq), len(warning_failures))
        logging.info("ETL final validation report:\n%s", report.to_string(index=False))
        return {
            "run_info": run_info,
            "report_info": report_info,
            "datasets_validated": len(report),
            "dq_checks": len(dq),
            "critical_failures": len(critical_failures),
            "warning_failures": len(warning_failures),
            "status": "PASS_WITH_WARNINGS" if not warning_failures.empty else "PASS",
        }

    @task
    def publish_summary(validation: dict) -> dict:
        logging.info("NammaMart ETL pipeline final summary: %s", validation)
        return validation

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def record_run() -> None:
        """Persist the audit record even when an upstream task fails."""
        context = get_current_context()
        dag_run = context["dag_run"]
        ti = context["ti"]
        run_info = ti.xcom_pull(task_ids="transform_and_load")
        reconciliation = ti.xcom_pull(task_ids="reconcile_payments")
        report_info = ti.xcom_pull(task_ids="build_reports")
        validation = ti.xcom_pull(task_ids="validate_and_report")
        published = ti.xcom_pull(task_ids="publish_summary")
        results = [run_info, reconciliation, report_info, validation, published]
        failed = not all(isinstance(result, dict) for result in results)
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

    extraction_results = [
        extract_customers(),
        extract_products(),
        extract_stores(),
        extract_orders(),
        extract_payments(),
    ]
    validated_sources = validate_extractions(extraction_results)
    run_info = transform_and_load(validated_sources)
    reconciliation = reconcile_payments(run_info)
    report_info = build_reports(run_info, reconciliation)
    validation = validate_and_report(run_info, report_info)
    published = publish_summary(validation)
    published >> record_run()


nammamart_etl_dag = nammamart_etl_pipeline()
