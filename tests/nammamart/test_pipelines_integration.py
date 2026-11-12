"""
tests/nammamart/test_pipelines_integration.py
---------------------------------------------
End-to-end tests that run both pipelines against the real dataset in a
temporary DuckDB file.

They prove the client's acceptance criteria:

* criterion 2 - only validated rows reach the ETL fact table
* criterion 3 - the ELT raw layer keeps every source row
* criterion 4 - rejected rows are written with a reason
* criterion 5 - re-running never double-counts revenue
* criterion 6 - the next-day file inserts new orders and updates corrections
* criterion 7 - orders reconcile against payments
* criterion 8 - no unmasked personal data in any reporting table
* Part B      - the ELT mart totals match the ETL totals
"""

from __future__ import annotations

import re

import pandas as pd
import pytest

from include.nammamart import config, elt_pipeline, etl_pipeline, warehouse

CORRECTED_ORDERS = ["ORD000017", "ORD000271", "ORD000332", "ORD000337", "ORD000584",
                    "ORD000708", "ORD000822", "ORD001025", "ORD001067", "ORD001990"]


@pytest.fixture(scope="module")
def warehouse_path(tmp_path_factory):
    """A throw-away warehouse so tests never touch the real one."""
    return tmp_path_factory.mktemp("warehouse") / "test_warehouse.duckdb"


@pytest.fixture(scope="module")
def quarantine_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("quarantine")


@pytest.fixture(scope="module")
def september_runs(warehouse_path, quarantine_dir):
    """Run ETL then ELT once over the September files."""
    etl = etl_pipeline.run_etl(run_id="test_etl_1", db_path=warehouse_path,
                               quarantine_dir=quarantine_dir)
    elt = elt_pipeline.run_elt(run_id="test_elt_1", db_path=warehouse_path,
                               quarantine_dir=quarantine_dir)
    return {"etl": etl, "elt": elt}


def query(warehouse_path, sql: str) -> pd.DataFrame:
    with warehouse.connect(warehouse_path, read_only=True) as con:
        return warehouse.query(con, sql)


# ---------------------------------------------------------------------------
# Criterion 2 and 4 - clean fact table, rejected rows kept with a reason
# ---------------------------------------------------------------------------
def test_etl_loads_only_validated_rows(september_runs, warehouse_path):
    summary = september_runs["etl"]
    fact_rows = query(warehouse_path, "SELECT count(*) AS n FROM etl_fact_orders").iloc[0]["n"]
    assert fact_rows == summary["counts"]["etl_fact_orders"]
    assert fact_rows < 2412, "some orders must have been rejected"

    statuses = query(warehouse_path,
                     "SELECT DISTINCT order_status FROM etl_fact_orders")["order_status"]
    assert set(statuses).issubset(set(config.ALLOWED_ORDER_STATUS))


def test_quarantine_files_contain_a_failure_reason(september_runs, quarantine_dir):
    files = list(quarantine_dir.glob("orders_failed_*.csv"))
    assert files, "orders quarantine file must exist"
    rejected = pd.read_csv(files[0])
    assert "failure_reason" in rejected.columns
    assert rejected["failure_reason"].notna().all()
    assert rejected["failure_reason"].str.contains("RULE-").all()


def test_rejected_order_is_not_in_the_revenue_total(september_runs, warehouse_path):
    """Part E question 8: prove a rejected order is absent from the revenue total.

    Rows rejected by the uniqueness rule are excluded from this check on
    purpose: there the *redundant copy* is quarantined while one copy of the
    order correctly survives, so the order_id is still in the fact table.
    """
    rejected = query(warehouse_path, """
        SELECT row_key, failure_reason FROM etl_quarantine
        WHERE dataset = 'orders' AND failure_reason NOT LIKE '%RULE-002%'
        LIMIT 5
    """)
    assert not rejected.empty
    ids = "', '".join(rejected["row_key"])
    in_fact = query(warehouse_path,
                    f"SELECT count(*) AS n FROM etl_fact_orders WHERE order_id IN ('{ids}')")
    assert in_fact.iloc[0]["n"] == 0


def test_duplicate_order_keeps_exactly_one_copy(september_runs, warehouse_path):
    duplicate_key = query(warehouse_path, """
        SELECT row_key FROM etl_quarantine
        WHERE dataset = 'orders' AND failure_reason LIKE '%RULE-002%' LIMIT 1
    """).iloc[0]["row_key"]
    kept = query(warehouse_path,
                 f"SELECT count(*) AS n FROM etl_fact_orders WHERE order_id = '{duplicate_key}'")
    assert kept.iloc[0]["n"] == 1, "one copy survives, the redundant copy is quarantined"


# ---------------------------------------------------------------------------
# Criterion 3 - raw keeps everything
# ---------------------------------------------------------------------------
def test_elt_raw_keeps_every_source_row(september_runs, warehouse_path):
    counts = query(warehouse_path, """
        SELECT (SELECT count(*) FROM raw_orders)    AS orders,
               (SELECT count(*) FROM raw_payments)  AS payments,
               (SELECT count(*) FROM raw_customers) AS customers,
               (SELECT count(*) FROM raw_products)  AS products,
               (SELECT count(*) FROM raw_stores)    AS stores
    """).iloc[0]
    assert counts["orders"] == 2412
    assert counts["payments"] == 2228
    assert counts["customers"] == 405
    assert counts["products"] == 152
    assert counts["stores"] == 15


def test_elt_staging_labels_rejected_rows_instead_of_deleting_them(september_runs, warehouse_path):
    statuses = query(warehouse_path,
                     "SELECT dq_status, count(*) AS n FROM stg_orders GROUP BY 1")
    by_status = dict(zip(statuses["dq_status"], statuses["n"]))
    assert by_status.get("QUARANTINED", 0) > 0
    rejected = query(warehouse_path, """
        SELECT failure_reason FROM stg_orders WHERE dq_status = 'QUARANTINED'
    """)
    assert rejected["failure_reason"].str.contains("RULE-").all()


# ---------------------------------------------------------------------------
# Part B - the two pipelines must agree
# ---------------------------------------------------------------------------
def test_etl_and_elt_totals_match(september_runs, warehouse_path):
    totals = query(warehouse_path, """
        SELECT (SELECT round(sum(net_revenue), 2) FROM etl_fact_orders WHERE is_revenue) AS etl_revenue,
               (SELECT round(sum(net_revenue), 2) FROM mart_daily_store_kpis)            AS elt_revenue,
               (SELECT count(*) FROM etl_fact_orders WHERE is_revenue)                   AS etl_orders,
               (SELECT sum(delivered_orders) FROM mart_daily_store_kpis)                 AS elt_orders
    """).iloc[0]
    assert totals["etl_revenue"] == totals["elt_revenue"]
    assert totals["etl_orders"] == totals["elt_orders"]


def test_quality_scorecards_agree_rule_by_rule(september_runs, warehouse_path):
    differences = query(warehouse_path, """
        WITH e AS (SELECT rule_id, dataset, rows_failed FROM dq_results WHERE run_id = 'test_etl_1'),
             l AS (SELECT rule_id, dataset, rows_failed FROM dq_results WHERE run_id = 'test_elt_1')
        SELECT coalesce(e.rule_id, l.rule_id) AS rule_id,
               e.rows_failed AS etl, l.rows_failed AS elt
        FROM e FULL OUTER JOIN l ON e.rule_id = l.rule_id AND e.dataset = l.dataset
        WHERE coalesce(e.rows_failed, -1) <> coalesce(l.rows_failed, -1)
    """)
    assert differences.empty, f"pipelines disagree: {differences.to_dict('records')}"


# ---------------------------------------------------------------------------
# Criterion 8 - personal data is masked everywhere
# ---------------------------------------------------------------------------
def test_no_reporting_table_exposes_personal_data(september_runs, warehouse_path):
    columns = query(warehouse_path, """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_name NOT LIKE 'raw_%'
    """)
    forbidden = {"email", "phone", "full_name", "manager_name"}
    leaks = columns[columns["column_name"].str.lower().isin(forbidden)]
    assert leaks.empty, f"unmasked PII columns found: {leaks.to_dict('records')}"


def test_masked_values_are_actually_masked(september_runs, warehouse_path):
    sample = query(warehouse_path, """
        SELECT email_masked, phone_masked FROM etl_dim_customer
        WHERE email_masked IS NOT NULL AND phone_masked IS NOT NULL LIMIT 5
    """)
    assert (sample["email_masked"].str.contains(r"\*\*\*@")).all()
    assert (sample["phone_masked"].str.contains(r"\*")).all()


# ---------------------------------------------------------------------------
# Criterion 7 - reconciliation
# ---------------------------------------------------------------------------
def test_reconciliation_lists_mismatches_with_a_reason(september_runs, warehouse_path):
    reasons = query(warehouse_path, """
        SELECT mismatch_reason, count(*) AS n FROM etl_payment_reconciliation GROUP BY 1
    """)
    assert not reasons.empty
    assert {"MISSING_PAYMENT", "AMOUNT_MISMATCH"}.issubset(set(reasons["mismatch_reason"]))

    elt_reasons = query(warehouse_path, """
        SELECT mismatch_reason, count(*) AS n FROM mart_payment_reconciliation GROUP BY 1
    """)
    assert (reasons.sort_values("mismatch_reason").reset_index(drop=True)["n"].tolist()
            == elt_reasons.sort_values("mismatch_reason").reset_index(drop=True)["n"].tolist())


# ---------------------------------------------------------------------------
# Criterion 5 - idempotency
# ---------------------------------------------------------------------------
def test_second_run_does_not_change_the_numbers(september_runs, warehouse_path, quarantine_dir):
    before = query(warehouse_path, """
        SELECT (SELECT count(*) FROM etl_fact_orders)                       AS etl_rows,
               (SELECT round(sum(net_revenue), 2) FROM etl_fact_orders)     AS etl_revenue,
               (SELECT count(*) FROM stg_orders WHERE dq_status = 'PASS')   AS elt_rows,
               (SELECT round(sum(net_revenue), 2) FROM mart_daily_store_kpis) AS elt_revenue
    """).iloc[0]

    etl = etl_pipeline.run_etl(run_id="test_etl_2", db_path=warehouse_path,
                               quarantine_dir=quarantine_dir)
    elt = elt_pipeline.run_elt(run_id="test_elt_2", db_path=warehouse_path,
                               quarantine_dir=quarantine_dir)

    after = query(warehouse_path, """
        SELECT (SELECT count(*) FROM etl_fact_orders)                       AS etl_rows,
               (SELECT round(sum(net_revenue), 2) FROM etl_fact_orders)     AS etl_revenue,
               (SELECT count(*) FROM stg_orders WHERE dq_status = 'PASS')   AS elt_rows,
               (SELECT round(sum(net_revenue), 2) FROM mart_daily_store_kpis) AS elt_revenue
    """).iloc[0]

    assert after.to_dict() == before.to_dict(), "a re-run must not change the totals"
    assert etl["counts"]["etl_fact_orders_inserted"] == 0
    assert elt["merge_base"]["inserted"] == 0


def test_raw_layer_grows_on_a_re_run_by_design(warehouse_path):
    """ELT raw is an append-only audit log, so it is expected to grow."""
    loads = query(warehouse_path, "SELECT count(DISTINCT _run_id) AS runs FROM raw_orders")
    assert loads.iloc[0]["runs"] >= 2
    assert query(warehouse_path, "SELECT count(*) AS n FROM raw_orders").iloc[0]["n"] == 4824


# ---------------------------------------------------------------------------
# Criterion 6 - incremental load
# ---------------------------------------------------------------------------
def test_increment_inserts_new_orders_and_updates_corrections(warehouse_path, quarantine_dir):
    etl = etl_pipeline.run_etl(run_id="test_etl_3", db_path=warehouse_path,
                               quarantine_dir=quarantine_dir, include_increment=True)
    elt = elt_pipeline.run_elt(run_id="test_elt_3", db_path=warehouse_path,
                               quarantine_dir=quarantine_dir, include_increment=True)

    assert elt["merge_increment"]["inserted"] == 150
    assert elt["merge_increment"]["updated"] == 10
    assert etl["counts"]["etl_fact_orders_inserted"] == 150

    ids = "', '".join(CORRECTED_ORDERS)
    corrected = query(warehouse_path, f"""
        SELECT (SELECT count(*) FROM etl_fact_orders
                 WHERE order_id IN ('{ids}') AND order_status = 'RETURNED')      AS etl_returned,
               (SELECT count(*) FROM stg_orders
                 WHERE order_id IN ('{ids}') AND dq_status = 'PASS'
                   AND order_status = 'RETURNED')                                AS elt_returned,
               (SELECT count(*) FROM etl_fact_orders WHERE order_id IN ('{ids}')) AS etl_rows,
               (SELECT count(*) FROM stg_orders
                 WHERE order_id IN ('{ids}') AND dq_status = 'PASS')             AS elt_rows
    """).iloc[0]
    assert corrected["etl_returned"] == 10
    assert corrected["elt_returned"] == 10
    assert corrected["etl_rows"] == 10, "a correction must not create a second row"
    assert corrected["elt_rows"] == 10


def test_totals_still_match_after_the_increment(warehouse_path):
    totals = query(warehouse_path, """
        SELECT (SELECT round(sum(net_revenue), 2) FROM etl_fact_orders WHERE is_revenue) AS etl,
               (SELECT round(sum(net_revenue), 2) FROM mart_daily_store_kpis)            AS elt
    """).iloc[0]
    assert totals["etl"] == totals["elt"]


# ---------------------------------------------------------------------------
# Part F - the run audit table
# ---------------------------------------------------------------------------
def test_every_run_is_recorded_in_pipeline_runs(warehouse_path):
    runs = query(warehouse_path, """
        SELECT run_id, dag_id, rows_extracted, rows_loaded, rows_quarantined, status
        FROM pipeline_runs ORDER BY start_time
    """)
    assert len(runs) >= 6
    assert set(runs["status"]) == {"SUCCESS"}
    assert (runs["rows_extracted"] > 0).all()
    assert runs["dag_id"].nunique() == 2


# ---------------------------------------------------------------------------
# Submission rule: "no unmasked customer data in the repo outside include/data/"
# ---------------------------------------------------------------------------
UNMASKED_EMAIL = re.compile(r"[a-z]{3,}\.[a-z]+[0-9]*@")
# The lookbehind also excludes letters so a gateway reference such as
# GW7909341632 is not mistaken for a phone number.
UNMASKED_PHONE = re.compile(r"(?<![0-9*A-Za-z])[6-9][0-9]{9}(?![0-9*])")


def test_quarantine_files_do_not_contain_unmasked_personal_data(september_runs, quarantine_dir):
    """A rejected customer row must still be recognisable, but never readable.

    The unmasked original stays in the source file and in the ELT raw layer,
    both of which are access controlled; the quarantine file that lives in the
    repository is masked.
    """
    files = list(quarantine_dir.glob("*.csv"))
    assert files, "expected quarantine files to exist"
    for path in files:
        text = path.read_text(encoding="utf-8")
        assert not UNMASKED_EMAIL.search(text), f"unmasked email in {path.name}"
        assert not UNMASKED_PHONE.search(text), f"unmasked phone number in {path.name}"


def test_quarantined_customer_row_keeps_its_key_and_reason(september_runs, quarantine_dir):
    files = list(quarantine_dir.glob("customers_failed_*.csv"))
    assert files
    rejected = pd.read_csv(files[0], dtype=str)
    assert rejected["customer_id"].notna().all(), "the key must survive masking"
    assert rejected["failure_reason"].str.contains("RULE-").all()
    assert rejected["email"].str.contains(r"\*\*\*@").all()
