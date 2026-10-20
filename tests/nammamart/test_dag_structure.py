"""
tests/nammamart/test_dag_structure.py
-------------------------------------
Checks that both DAGs parse and are wired the way the assignment requires
(Part A structure, Part B layers, Part C scheduling, Part F run audit).

The tests are skipped automatically when Apache Airflow is not installed, so
the rest of the suite still runs on a machine without it.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow", reason="Apache Airflow is not installed")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ETL_DAG_ID = "nammamart_etl_waseem"
ELT_DAG_ID = "nammamart_elt_waseem"


@pytest.fixture(scope="module")
def dagbag():
    from airflow.dag_processing.dagbag import DagBag

    return DagBag(dag_folder=str(PROJECT_ROOT / "dags"))


@pytest.fixture(scope="module")
def etl_dag(dagbag):
    return dagbag.dags[ETL_DAG_ID]


@pytest.fixture(scope="module")
def elt_dag(dagbag):
    return dagbag.dags[ELT_DAG_ID]


# ---------------------------------------------------------------------------
# Submission checklist: "both parsing with no import errors"
# ---------------------------------------------------------------------------
def test_dags_import_without_errors(dagbag):
    nammamart_errors = {path: error for path, error in dagbag.import_errors.items()
                        if "nammamart" in str(path)}
    assert not nammamart_errors, f"DAG import errors: {nammamart_errors}"
    assert {ETL_DAG_ID, ELT_DAG_ID}.issubset(set(dagbag.dag_ids))


# ---------------------------------------------------------------------------
# Part C: scheduling
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("dag_fixture", ["etl_dag", "elt_dag"])
def test_schedule_is_9am_ist_monday_to_saturday(dag_fixture, request):
    dag = request.getfixturevalue(dag_fixture)
    # start_date carries the timezone: 2026-10-01 00:00 IST == 2026-09-30 18:30 UTC
    assert dag.start_date.tzinfo is not None, "start_date must be timezone aware"
    assert str(dag.start_date) == "2026-09-30 18:30:00+00:00"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


@pytest.mark.parametrize("dag_fixture", ["etl_dag", "elt_dag"])
def test_retries_and_timeout_are_configured(dag_fixture, request):
    dag = request.getfixturevalue(dag_fixture)
    for task in dag.tasks:
        assert task.retries == 2, f"{task.task_id} has no retries"
        assert task.retry_delay == timedelta(minutes=2)
        assert task.execution_timeout == timedelta(minutes=30)


# ---------------------------------------------------------------------------
# Part A: the ETL shape
# ---------------------------------------------------------------------------
def test_etl_extracts_five_files_in_parallel(etl_dag):
    extracts = [task for task in etl_dag.tasks if task.task_id.startswith("extract_")]
    assert len(extracts) == 5
    for task in extracts:
        assert task.upstream_task_ids == set(), f"{task.task_id} must start immediately"
        assert task.downstream_task_ids == {"validate_and_quarantine"}


def test_etl_follows_extract_validate_transform_load_reconcile_report(etl_dag):
    expected_downstream = {
        "validate_and_quarantine": {"transform_star_schema"},
        "transform_star_schema": {"load_star_schema"},
        "load_star_schema": {"reconcile_payments"},
        "reconcile_payments": {"build_reports"},
        "build_reports": {"write_run_audit"},
    }
    for task_id, downstream in expected_downstream.items():
        assert etl_dag.get_task(task_id).downstream_task_ids == downstream


# ---------------------------------------------------------------------------
# Part B: the ELT layers
# ---------------------------------------------------------------------------
def test_elt_has_a_task_per_raw_table(elt_dag):
    raw_tasks = [task.task_id for task in elt_dag.tasks if task.task_id.startswith("load_raw_")]
    for dataset in ("orders", "payments", "customers", "products", "stores"):
        assert f"load_raw_{dataset}" in raw_tasks
    assert "load_raw_increment" in raw_tasks


def test_elt_runs_layers_in_order(elt_dag):
    order = ["build_staging_masters", "merge_orders_base", "merge_orders_increment",
             "build_staging_payments", "run_quality_checks", "build_mart", "write_run_audit"]
    for upstream, downstream in zip(order, order[1:]):
        assert downstream in elt_dag.get_task(upstream).downstream_task_ids, (
            f"{downstream} must run after {upstream}")


def test_payments_staging_runs_after_orders(elt_dag):
    """stg_payments compares amounts against the clean orders, so order matters."""
    upstream_ids = elt_dag.get_task("build_staging_payments").upstream_task_ids
    assert "merge_orders_increment" in upstream_ids


# ---------------------------------------------------------------------------
# Part F: the run audit must always run
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("dag_fixture", ["etl_dag", "elt_dag"])
def test_run_audit_uses_all_done_trigger_rule(dag_fixture, request):
    dag = request.getfixturevalue(dag_fixture)
    audit = dag.get_task("write_run_audit")
    assert str(audit.trigger_rule) == "TriggerRule.ALL_DONE", (
        "the audit row must be written even when an upstream task failed")
    assert audit.downstream_task_ids == set(), "write_run_audit is the last task"


# ---------------------------------------------------------------------------
# Security: no secrets in the DAG files
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("dag_file", ["nammamart_etl_waseem.py", "nammamart_elt_waseem.py"])
def test_dag_files_contain_no_secrets(dag_file):
    text = (PROJECT_ROOT / "dags" / dag_file).read_text(encoding="utf-8")
    for forbidden in ("https://hooks.slack.com", "password=", "api_key=", "token="):
        assert forbidden not in text, f"{dag_file} appears to contain a secret: {forbidden}"
