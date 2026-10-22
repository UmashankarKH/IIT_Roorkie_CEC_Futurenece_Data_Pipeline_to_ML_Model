"""NammaMart DAG tests: parse, schedule, timezone, retries, and the masking / DQ helpers (no warehouse needed)."""
import os
import sys

import pandas as pd
import pendulum
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "include"))

from airflow.models import DagBag  # noqa: E402

import nammamart_etl_lib as lib  # noqa: E402
import nammamart_quality as dq  # noqa: E402

DAG_IDS = ["nammamart_etl_mohanrajs", "nammamart_elt_mohanrajs"]


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(include_examples=False)


def test_no_import_errors(dagbag):
    assert not {k: v for k, v in dagbag.import_errors.items() if "nammamart" in k}


@pytest.mark.parametrize("dag_id", DAG_IDS)
def test_schedule_is_9am_ist_mon_sat(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    assert str(dag.schedule) == "0 9 * * 1-6" or getattr(dag.timetable, "_expression", "") == "0 9 * * 1-6"
    # Airflow stores start_date in UTC; the instant must equal midnight IST, and the DAG timezone must be IST
    assert dag.start_date == pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata")
    assert dag.timezone.name == "Asia/Kolkata"
    assert dag.catchup is False
    assert dag.default_args["retries"] >= 1 and dag.default_args["execution_timeout"]


@pytest.mark.parametrize("dag_id", DAG_IDS)
def test_run_audit_runs_even_after_failure(dagbag, dag_id):
    assert dagbag.get_dag(dag_id).get_task("run_audit").trigger_rule == "all_done"


def test_masking():
    assert lib.mask_email("priya.hegde199@example.com") == "pr***@example.com"
    assert lib.mask_email("N/A") == "***"
    assert lib.mask_phone("9774349021") == "97******21"
    assert lib.mask_phone("99560412") == "**********"


def test_orders_quality_rules_quarantine_bad_rows():
    cols = ["_row_num", "order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price",
            "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes"]
    ok = ["2", "O1", "2026-09-01 10:00:00", "C1", "S1", "P1", "2", "100.0", "10", "180.0", " upi ", "delivered", "App", "20"]
    bad_status = ["3", "O2", "2026-09-01 10:00:00", "C1", "S1", "P1", "2", "100.0", "10", "180.0", "UPI", "SHIPPED", "App", "20"]
    neg_qty = ["4", "O3", "2026-09-01 10:00:00", "C1", "S1", "P1", "-2", "100.0", "10", "-180.0", "UPI", "DELIVERED", "App", "20"]
    future = ["5", "O4", "2027-01-01 10:00:00", "C1", "S1", "P1", "2", "100.0", "10", "180.0", "UPI", "DELIVERED", "App", "20"]
    df = pd.DataFrame([ok, bad_status, neg_qty, future], columns=cols)
    passed, failed, results = dq.validate_orders(df, pd.Timestamp("2026-10-02"), {"C1"}, {"P1"}, {"S1"}, {"P1": 100.0})
    assert passed.order_id.tolist() == ["O1"]
    assert passed.payment_mode.iloc[0] == "UPI" and passed.order_status.iloc[0] == "DELIVERED"
    reasons = dict(zip(failed.order_id, failed.failure_reason))
    assert reasons == {"O2": "orders_status_valid", "O3": "orders_quantity_positive", "O4": "orders_order_ts_not_future"}
    assert {r["dimension"] for r in results} >= {"Completeness", "Uniqueness", "Validity", "Accuracy", "Consistency", "Timeliness"}
