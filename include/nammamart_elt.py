"""Task bodies for the NammaMart ELT DAG: Extract -> Load raw -> Transform in the warehouse with SQL."""
from __future__ import annotations

from datetime import datetime

import nammamart_common as nc

DAG_ID = f"nammamart_elt_{nc.OWNER}"


def extract(name: str, data_dir: str | None = None) -> dict:
    rows = nc.check_schema(name, data_dir)
    print(f"[Extract] {name}.csv: {rows} rows, schema OK")
    return {"name": name, "rows": rows}


def load_raw(run_id: str, load_increment: bool, data_dir: str | None = None) -> dict:
    """Append every CSV row, all columns as text, untouched. A retry of the same run replaces only its own rows."""
    files = [(f, f) for f in nc.BASE_FILES]
    if load_increment:
        files.append((nc.INCREMENT_FILE, "orders"))
    loaded_at = datetime.now(nc.IST).replace(tzinfo=None)
    counts = {}
    con = nc.connect()
    try:
        for file_name, table in files:
            path = nc.csv_path(file_name, data_dir)
            cols = nc.EXPECTED_COLUMNS[file_name]
            col_ddl = ", ".join(f'"{c}" VARCHAR' for c in cols)
            col_sel = ", ".join(f'"{c}"' for c in cols)
            col_types = "{" + ", ".join(f"'{c}': 'VARCHAR'" for c in cols) + "}"
            raw = f"raw_{table}"
            con.execute(f"CREATE TABLE IF NOT EXISTS {raw} ({col_ddl}, _row_id BIGINT, _loaded_at TIMESTAMP, "
                        "_source_file VARCHAR, _run_id VARCHAR)")
            con.execute(f"DELETE FROM {raw} WHERE _run_id = ? AND _source_file = ?", [run_id, path.name])
            # Fixed dialect: the header was already checked, and strict mode rejects files with mixed line endings.
            con.execute(
                f"INSERT INTO {raw} SELECT {col_sel}, ROW_NUMBER() OVER (), ?, ?, ? FROM read_csv(?, header=true, "
                f"delim=',', quote='\"', escape='\"', auto_detect=false, strict_mode=false, columns={col_types}, "
                "parallel=false)",
                [loaded_at, path.name, run_id, str(path)])
            counts[file_name] = con.execute(f"SELECT COUNT(*) FROM {raw} WHERE _run_id=? AND _source_file=?",
                                            [run_id, path.name]).fetchone()[0]
        totals = {t: con.execute(f"SELECT COUNT(*) FROM raw_{t}").fetchone()[0] for t in nc.BASE_FILES}
    finally:
        con.close()
    for name, n in counts.items():
        print(f"[Load raw] {name}: {n} rows appended")
    print(f"[Load raw] table totals after this run (append-only): {totals}")
    return {"rows": counts, "rows_extracted": sum(counts.values())}


def _render(sql: str, run_id: str, as_of: str | None) -> str:
    return (sql.replace("{run_id}", run_id.replace("'", "''")).replace("{dag_id}", DAG_ID)
            .replace("{as_of}", nc.today_ist(as_of).isoformat()))


def run_sql_file(file_name: str, run_id: str, as_of: str | None = None):
    sql = _render((nc.SQL_DIR / file_name).read_text(encoding="utf-8"), run_id, as_of)
    con = nc.connect()
    try:
        con.execute(sql)
    finally:
        con.close()
    print(f"[SQL] {file_name} done")


def merge_orders(run_id: str, as_of: str | None = None) -> dict:
    con = nc.connect()
    try:
        con.execute("CREATE TABLE IF NOT EXISTS stg_orders AS SELECT * FROM stg_orders_incoming WHERE FALSE")
        stats = con.execute(_render((nc.SQL_DIR / "stg_orders_merge_stats.sql").read_text(), run_id, as_of)).fetchone()
        con.execute(_render((nc.SQL_DIR / "stg_orders_merge.sql").read_text(), run_id, as_of))
        total = con.execute("SELECT COUNT(*), COUNT(DISTINCT order_id) FROM stg_orders").fetchone()
    finally:
        con.close()
    out = dict(zip(["inserted", "updated", "status_changed", "kept_existing"], map(int, stats)))
    out.update(stg_rows=total[0], stg_distinct_order_ids=total[1])
    print(f"[Merge] stg_orders: {out}")
    return out


def run_counts(run_id: str) -> dict:
    """Rows passed / quarantined across the five staging tables for this run (for the run audit)."""
    con = nc.connect()
    try:
        passed = failed = 0
        for table in ("stg_customers", "stg_products", "stg_stores", "stg_payments"):
            p, f = con.execute(f"SELECT COUNT(*) FILTER (WHERE dq_status <> 'FAILED'), "
                               f"COUNT(*) FILTER (WHERE dq_status = 'FAILED') FROM {table}").fetchone()
            passed, failed = passed + p, failed + f
        p, f = con.execute("SELECT COUNT(*) FILTER (WHERE dq_status <> 'FAILED'), COUNT(*) FILTER (WHERE dq_status = 'FAILED') "
                           "FROM stg_orders_incoming").fetchone()
        passed, failed = passed + p, failed + f
    finally:
        con.close()
    return {"rows_loaded": passed, "rows_quarantined": failed}


def report() -> dict:
    con = nc.connect()
    try:
        zone = con.execute("SELECT zone, ROUND(SUM(net_revenue), 2) AS net_revenue, SUM(delivered_orders) AS orders "
                           "FROM mart_daily_store_kpis GROUP BY zone ORDER BY net_revenue DESC").df()
        margin = con.execute("SELECT * FROM mart_category_margin ORDER BY revenue DESC").df()
        recon = con.execute("SELECT mismatch_reason, COUNT(*) AS n, ROUND(SUM(ABS(difference)), 2) AS amount "
                            "FROM mart_payment_reconciliation GROUP BY 1 ORDER BY 1").df()
    finally:
        con.close()
    print("[Report] Net revenue by zone\n" + zone.to_string(index=False))
    print("[Report] Category margin\n" + margin.to_string(index=False))
    print("[Report] Payment reconciliation\n" + recon.to_string(index=False))
    return {"net_revenue": float(zone.net_revenue.sum())}
