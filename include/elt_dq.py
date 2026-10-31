"""ELT quality scorecard: counts every check from the staging tables (SQL) and the raw tables.
Check names, dimensions and severities come from the same list the ETL uses."""
import duckdb
import pandas as pd

from include.dq_checks import CHECKSETS
from include.etl_audit import write_dq_results
from include.nammamart_config import ELT_DAG_ID, FILES, WAREHOUSE

KEYS = {"orders": "order_id", "payments": "payment_id", "customers": "customer_id",
        "products": "product_id", "stores": "store_id"}


def compute_dq_results(run_id):
    ctx = {"now": pd.Timestamp.now(tz="Asia/Kolkata").tz_localize(None)}              # lambdas inside the checks are never called here
    con = duckdb.connect(str(WAREHOUSE))
    results = []
    for file, factory in CHECKSETS.items():
        src = FILES[file]
        raw_n = con.execute(f"SELECT count(*) FROM raw_{file} WHERE _run_id = ? AND _source_file = ?",
                            [run_id, src]).fetchone()[0]
        for c in factory(ctx):
            if c.scope == "dataset":                # freshness: is the newest order older than 1 day?
                failed = con.execute(
                    "SELECT count(*) FROM (SELECT max(order_ts) AS m FROM stg_orders "
                    "WHERE dq_status <> 'REJECTED' "
                    "AND order_ts <= current_timestamp::TIMESTAMP) "
                    "WHERE m < current_timestamp::TIMESTAMP - INTERVAL 1 DAY").fetchone()[0]
                checked = 1
            elif c.name.endswith("_unique"):        # duplicates are collapsed in staging, so count them in raw
                k = KEYS[file]
                failed = con.execute(
                    f"SELECT count(*) - count(DISTINCT trim({k})) FROM raw_{file} "
                    f"WHERE _run_id = ? AND _source_file = ? AND trim({k}) <> ''",
                    [run_id, src]).fetchone()[0]
                checked = raw_n
            else:
                pat = f"%;{c.name};%"
                failed = con.execute(
                    f"SELECT count(*) FROM stg_{file} WHERE ';' || failure_reason || ';' LIKE ? "
                    f"OR ';' || warning_reason || ';' LIKE ?", [pat, pat]).fetchone()[0]
                checked = raw_n
            status = "PASS" if failed == 0 else ("FAIL" if c.severity == "Critical" else "WARN")
            results.append(dict(run_id=run_id, dag_id=ELT_DAG_ID, check_name=f"{file}.{c.name}",
                                dimension=c.dimension, severity=c.severity, rows_checked=checked,
                                rows_failed=failed, status=status, checked_at=pd.Timestamp.now(tz="Asia/Kolkata").tz_localize(None)))
    con.close()
    return results


def write_elt_dq(run_id):
    results = compute_dq_results(run_id)
    write_dq_results(results, ELT_DAG_ID, run_id)
    return results