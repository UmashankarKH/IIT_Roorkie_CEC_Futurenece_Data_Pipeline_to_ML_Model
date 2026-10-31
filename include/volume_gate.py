"""Volume gate: stop a run whose orders file is far smaller than in recent runs."""
import duckdb

from include.nammamart_config import WAREHOUSE

CHECK = "orders.volume_vs_recent_runs"
BASE_CHECK = "orders.order_id_not_null"      # its rows_checked = number of orders rows in that run


def recent_baseline(dag_id, run_id, n=7):
    """Largest orders row count seen in this DAG's last n runs (None if there is no history yet)."""
    try:
        con = duckdb.connect(str(WAREHOUSE), read_only=True)
        try:
            row = con.execute(
                "SELECT max(rows_checked) FROM (SELECT rows_checked FROM dq_results "
                "WHERE dag_id = ? AND check_name = ? AND run_id <> ? "
                "ORDER BY checked_at DESC LIMIT ?)", [dag_id, BASE_CHECK, run_id, n]).fetchone()
        finally:
            con.close()
        return int(row[0]) if row and row[0] is not None else None
    except Exception:
        return None


def volume_row(dag_id, run_id, rows_now, now, threshold=0.5):
    """Returns (dq_results row, baseline). The check FAILS when today's file is below threshold x baseline."""
    base = recent_baseline(dag_id, run_id)
    failed = 1 if (base and rows_now < threshold * base) else 0
    row = dict(run_id=run_id, dag_id=dag_id, check_name=CHECK, dimension="Completeness",
               severity="Critical", rows_checked=int(rows_now), rows_failed=failed,
               status="FAIL" if failed else "PASS", checked_at=now)
    return row, base
