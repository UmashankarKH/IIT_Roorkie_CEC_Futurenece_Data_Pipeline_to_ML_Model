"""
NammaMart ELT — pipeline logic (Part B)
=======================================

Extract -> Load raw -> Transform in DuckDB with SQL (staging -> mart)

* Python only moves data: it reads each CSV as text and appends it, untouched,
  to raw_<table>. Every transformation is a .sql file in include/sql/, run by
  run_sql_files() below.
* Raw is append-only: each run adds its rows with _run_id, _source_file and
  _loaded_at, and nothing is ever cleaned or deleted. Staging reads only the
  rows the current run loaded.
* The data quality rules are the same ones the ETL uses: include/dq_rules.py
  writes them out as include/sql/staging/generated/*.sql.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

try:
    from include import nammamart_etl as etl, observability
    from include.dq_rules import FILES
except ImportError:
    import nammamart_etl as etl
    import observability
    from dq_rules import FILES

INCLUDE = Path(__file__).resolve().parent
SQL_DIR = INCLUDE / "sql"
WAREHOUSE = etl.WAREHOUSE
WORK_DIR = etl.WORK_DIR / "elt"          # separate from the ETL's hand-off files

BASE_FILES = ["orders", "payments", "customers", "products", "stores"]
INCREMENT = "orders_increment"
RAW_TABLE = {"orders": "raw_orders", INCREMENT: "raw_orders", "payments": "raw_payments",
             "customers": "raw_customers", "products": "raw_products", "stores": "raw_stores"}

MACROS = SQL_DIR / "common" / "00_macros.sql"
STAGING = SQL_DIR / "staging"
GENERATED = STAGING / "generated"
MART = SQL_DIR / "mart"
MART_FILES = sorted(MART.glob("[0-9][0-9]_*.sql"))


# ---------------------------------------------------------------------------
# SQL runner
# ---------------------------------------------------------------------------
_PLACEHOLDER = re.compile(r"\{\{\s*(\w+)\s*\}\}")
_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def render(sql: str, params: dict) -> str:
    """Fill {{ name }} placeholders. Values are quoted-safe; table names must be identifiers."""
    def sub(m):
        key = m.group(1)
        if key not in params:
            raise KeyError(f"SQL placeholder {{{{ {key} }}}} has no value")
        val = str(params[key])
        if key.endswith("_table") and not _SAFE_IDENT.match(val):
            raise ValueError(f"unsafe table name: {val!r}")
        return val.replace("'", "''")
    return _PLACEHOLDER.sub(sub, sql)


def split_statements(sql: str) -> list[str]:
    """Split on ';' at the end of a line (our files never put ';\\n' inside a string)."""
    body = "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--"))
    return [s.strip() for s in re.split(r";[ \t]*(?:--[^\n]*)?(?:\n|$)", body) if s.strip()]


def run_sql_file(con, path: Path, params: dict) -> None:
    """Run every statement in one .sql file; print what it created or returned."""
    rel = path.relative_to(SQL_DIR)
    for stmt in split_statements(render(path.read_text(), params)):
        con.execute(stmt)
        head = stmt.split(None, 6)
        m = re.match(r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s+AS",
                     stmt, re.I)
        if m:
            n = con.execute(f"SELECT count(*) FROM {m.group(1)}").fetchone()[0]
            print(f"  {rel}: {m.group(1)} -> {n:,} rows")
        elif head and head[0].upper() == "SELECT" and "error(" not in stmt:
            df = con.execute(stmt).df()
            with pd.option_context("display.width", 200, "display.max_columns", 30):
                print(f"  {rel}:\n{df.to_string(index=False)}")
    print(f"ran {rel}")


def run_sql_files(paths: list[Path], params: dict, warehouse: Path = WAREHOUSE) -> None:
    con = etl.connect(warehouse)
    try:
        run_sql_file(con, MACROS, params)
        for p in paths:
            run_sql_file(con, p, params)
    finally:
        con.close()


def base_params(run_id: str, dag_id: str, as_of: str) -> dict:
    as_of_ts = pd.Timestamp(as_of)
    return {
        "run_id": run_id, "dag_id": dag_id, "as_of": as_of_ts.strftime("%Y-%m-%d"),
        "next_day": (as_of_ts + pd.Timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
        "stale_before": (as_of_ts - pd.Timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
        "checked_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }


# ---------------------------------------------------------------------------
# 1. Extract (same as the ETL: schema checked, file read exactly as received)
# ---------------------------------------------------------------------------
def extract(name: str, run_id: str) -> dict:
    return etl.extract(name, run_id, work=WORK_DIR)


# ---------------------------------------------------------------------------
# 2. Load raw: append every row, untouched
# ---------------------------------------------------------------------------
def load_raw(extracted: dict, run_id: str, warehouse: Path = WAREHOUSE) -> dict:
    """Append each extracted file to its raw table. All columns are text.
    Idempotent per run: if this run already loaded a file (a retried task), it is skipped."""
    loaded_at = pd.Timestamp.now(tz="UTC").tz_localize(None).floor("s")
    con = etl.connect(warehouse)
    summary = []
    try:
        con.execute("BEGIN TRANSACTION")
        for name, info in extracted.items():
            if not info:
                continue
            table, cols = RAW_TABLE[name], etl.EXPECTED_COLUMNS[name]
            col_ddl = ", ".join(f'"{c}" VARCHAR' for c in cols)
            con.execute(f"CREATE TABLE IF NOT EXISTS {table} ({col_ddl}, _row INTEGER, "
                        f"_source_file VARCHAR, _run_id VARCHAR, _loaded_at TIMESTAMP)")
            already = con.execute(f"SELECT count(*) FROM {table} WHERE _run_id = ? AND _source_file = ?",
                                  [run_id, FILES[name]]).fetchone()[0]
            if already:
                summary.append(dict(file=FILES[name], table=table, appended=0,
                                    note=f"already loaded by this run ({already} rows)"))
                continue
            df = pd.read_pickle(info["path"])
            df = df[["_row"] + cols].assign(_source_file=FILES[name], _run_id=run_id, _loaded_at=loaded_at)
            con.register("incoming", df)
            col_list = ", ".join(f'"{c}"' for c in cols)
            con.execute(f"INSERT INTO {table} ({col_list}, _row, _source_file, _run_id, _loaded_at) "
                        f"SELECT {col_list}, _row, _source_file, _run_id, _loaded_at FROM incoming")
            con.unregister("incoming")
            summary.append(dict(file=FILES[name], table=table, appended=len(df), note=""))
        con.execute("COMMIT")
        totals = {t: con.execute(f"SELECT count(*), count(DISTINCT _run_id) FROM {t}").fetchone()
                  for t in sorted(set(s["table"] for s in summary))}
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    out = pd.DataFrame(summary)
    out["raw_table_rows_now"] = out["table"].map(lambda t: totals[t][0])
    out["runs_in_raw_table"] = out["table"].map(lambda t: totals[t][1])
    etl._print_table("Raw layer: rows appended this run (raw is never cleaned or deleted)", out)
    return {"files": {n: bool(i) for n, i in extracted.items()},
            "increment": bool(extracted.get(INCREMENT)),
            "rows_extracted": int(sum(i["rows"] for i in extracted.values() if i))}


# ---------------------------------------------------------------------------
# 3. Staging (SQL)
# ---------------------------------------------------------------------------
def _order_files(raw_info: dict) -> list[str]:
    return ["orders"] + ([INCREMENT] if raw_info.get("increment") else [])


def staging_standardise(raw_info: dict, params: dict) -> None:
    con = etl.connect()
    try:
        run_sql_file(con, MACROS, params)
        run_sql_file(con, STAGING / "00_setup.sql", params)
        for name in _order_files(raw_info):
            run_sql_file(con, STAGING / "01_std_orders.sql",
                         {**params, "std_table": f"stg_{name}_std", "source_file": FILES[name]})
        for name in ["payments", "customers", "products", "stores"]:
            run_sql_file(con, STAGING / f"01_std_{name}.sql", params)
    finally:
        con.close()


def staging_quality_checks(raw_info: dict, params: dict) -> None:
    files = BASE_FILES + ([INCREMENT] if raw_info.get("increment") else [])
    paths = [GENERATED / "02_ref_views.sql"]
    paths += [GENERATED / f"03_flag_{f}.sql" for f in files]
    paths += [GENERATED / f"04_dq_results_{f}.sql" for f in files]
    run_sql_files(paths, params)
    con = etl.connect()
    try:
        summary = con.execute("""
            SELECT check_name, severity, rows_checked, rows_failed, status FROM dq_results
            WHERE run_id = ? AND dag_id = ? AND rows_failed > 0 ORDER BY check_name""",
                              [params["run_id"], params["dag_id"]]).df()
        per_file = []
        for f in files:
            row = con.execute(f"""SELECT count(*), count(*) FILTER (WHERE dq_status <> 'FAIL'),
                                         count(*) FILTER (WHERE dq_status = 'WARN'),
                                         count(*) FILTER (WHERE dq_status = 'FAIL')
                                  FROM stg_{f}_flagged""").fetchone()
            per_file.append(dict(file=FILES[f], rows_in=row[0], passed=row[1], with_warnings=row[2],
                                 rejected=row[3]))
    finally:
        con.close()
    etl._print_table("Rows per file (rejected rows stay in staging, flagged FAIL)", pd.DataFrame(per_file))
    etl._print_table("Checks with failures", summary)


def staging_build(params: dict) -> None:
    paths = [STAGING / f"05_stg_{t}.sql" for t in ["customers", "products", "stores", "payments"]]
    run_sql_files(paths, params)


def merge_orders(name: str, raw_info: dict, params: dict) -> dict | None:
    if name == INCREMENT and not raw_info.get("increment"):
        print("load_increment is false: no increment rows in this run, nothing to merge")
        return None
    con = etl.connect()
    try:
        run_sql_file(con, MACROS, params)
        before = con.execute("SELECT count(*) FROM information_schema.tables "
                             "WHERE table_name = 'stg_orders'").fetchone()[0]
        before = con.execute("SELECT count(*) FROM stg_orders").fetchone()[0] if before else 0
        run_sql_file(con, STAGING / "06_merge_stg_orders.sql",
                     {**params, "batch_table": f"stg_{name}_flagged", "source_file": FILES[name]})
        stats = con.execute("SELECT * FROM merge_stats").df().iloc[0].to_dict()
        after = con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
        by_status = con.execute("""SELECT _source_file, dq_status, order_status, count(*) AS rows
                                   FROM stg_orders GROUP BY ALL ORDER BY ALL""").df()
    finally:
        con.close()
    print(f"stg_orders: {before:,} rows before -> {after:,} rows after")
    etl._print_table("stg_orders by source file and status", by_status)
    return {k: (int(v) if isinstance(v, (int, float)) or hasattr(v, "item") else v) for k, v in stats.items()}


# ---------------------------------------------------------------------------
# 4. Mart (SQL)
# ---------------------------------------------------------------------------
def build_marts(params: dict) -> dict:
    run_sql_files(MART_FILES, params)
    con = etl.connect()
    try:
        zone = con.execute("""SELECT zone, sum(delivered_orders) AS delivered_orders,
                                     round(sum(net_revenue), 2) AS net_revenue
                              FROM mart_daily_store_kpis GROUP BY zone ORDER BY net_revenue DESC""").df()
        cat = con.execute("SELECT * FROM mart_category_margin").df()
        recon = con.execute("""SELECT reason, count(*) AS rows, round(sum(abs(difference)), 2) AS amount
                               FROM mart_payment_reconciliation GROUP BY reason ORDER BY reason""").df()
        rejected = con.execute("SELECT * FROM mart_rejected_summary").df()
        total = con.execute("SELECT round(sum(net_revenue), 2) FROM int_orders_enriched").fetchone()[0]
    finally:
        con.close()
    etl._print_table("Net revenue by zone", zone)
    etl._print_table("Revenue and gross margin by category", cat)
    etl._print_table("Payment reconciliation by reason", recon)
    etl._print_table("Rows rejected this run, by file and check", rejected)
    print(f"\nTotal net revenue (delivered orders): {total:,.2f}")
    return {"total_net_revenue": float(total or 0)}


# ---------------------------------------------------------------------------
# 5. Compare with the ETL pipeline (Part B, point 4)
# ---------------------------------------------------------------------------
COMPARE_SQL = {
    "ETL": """SELECT count(*) FILTER (WHERE order_status = 'DELIVERED') AS delivered_orders,
                     count(*) AS orders_loaded,
                     round(sum(net_revenue), 2) AS net_revenue,
                     round(sum(gross_margin), 2) AS gross_margin
              FROM etl_fact_orders WHERE order_date < DATE '{cutoff}'""",
    "ELT": """SELECT count(*) FILTER (WHERE order_status = 'DELIVERED') AS delivered_orders,
                     count(*) AS orders_loaded,
                     round(sum(net_revenue), 2) AS net_revenue,
                     round(sum(gross_margin), 2) AS gross_margin
              FROM int_orders_enriched WHERE order_date < DATE '{cutoff}'""",
}
COMPARE_DETAIL = {
    "net revenue by zone": (
        """SELECT coalesce(s.zone, 'UNKNOWN') AS key, round(sum(f.net_revenue), 2) AS value
           FROM etl_fact_orders f LEFT JOIN etl_dim_store s USING (store_id)
           WHERE f.order_date < DATE '{cutoff}' GROUP BY 1""",
        """SELECT zone AS key, round(sum(net_revenue), 2) AS value FROM int_orders_enriched
           WHERE order_date < DATE '{cutoff}' GROUP BY 1"""),
    "gross margin by category": (
        "SELECT category AS key, gross_margin AS value FROM etl_category_margin",
        "SELECT category AS key, gross_margin AS value FROM mart_category_margin"),
    "reconciliation rows by reason": (
        "SELECT reason AS key, count(*) AS value FROM etl_payment_reconciliation GROUP BY 1",
        "SELECT reason AS key, count(*) AS value FROM mart_payment_reconciliation GROUP BY 1"),
}


def compare_with_etl(as_of: str, warehouse: Path = WAREHOUSE) -> dict:
    """Compare September totals between the two pipelines. Warns (does not fail) on a difference,
    because the ETL may simply not have run on the same data yet."""
    cutoff = pd.Timestamp(as_of).strftime("%Y-%m-%d")
    con = etl.connect(warehouse)
    try:
        has_etl = con.execute("SELECT count(*) FROM information_schema.tables "
                              "WHERE table_name = 'etl_fact_orders'").fetchone()[0]
        if not has_etl:
            print("ETL tables not found: run nammamart_etl_anaz first, then re-run this task")
            return {"compared": False}
        etl_row = con.execute(COMPARE_SQL["ETL"].format(cutoff=cutoff)).df().iloc[0]
        elt_row = con.execute(COMPARE_SQL["ELT"].format(cutoff=cutoff)).df().iloc[0]
        details = []
        for label, (q_etl, q_elt) in COMPARE_DETAIL.items():
            a = con.execute(q_etl.format(cutoff=cutoff)).df().set_index("key")["value"]
            b = con.execute(q_elt.format(cutoff=cutoff)).df().set_index("key")["value"]
            for key in sorted(set(a.index) | set(b.index), key=str):
                va, vb = a.get(key), b.get(key)
                same = (va is not None and vb is not None and pd.notna(va) and pd.notna(vb)
                        and abs(float(va) - float(vb)) < 0.011) or (pd.isna(va) and pd.isna(vb))
                details.append(dict(comparison=label, key=key, etl=va, elt=vb, match="yes" if same else "NO"))
        etl_has_inc = con.execute("SELECT count(*) FROM etl_fact_orders WHERE _source_file LIKE '%increment%'"
                                  ).fetchone()[0]
        elt_has_inc = con.execute("SELECT count(*) FROM stg_orders WHERE _source_file LIKE '%increment%'"
                                  ).fetchone()[0]
    finally:
        con.close()
    totals = pd.DataFrame([etl_row.rename("ETL"), elt_row.rename("ELT")]).T
    totals["match"] = ["yes" if abs(float(r.ETL) - float(r.ELT)) < 0.011 else "NO" for r in totals.itertuples()]
    totals = totals.reset_index(names="metric")
    etl._print_table(f"September totals (orders before {cutoff}): ETL vs ELT", totals)
    det = pd.DataFrame(details)
    etl._print_table("Detail", det)
    all_match = (totals["match"] == "yes").all() and (det["match"] == "yes").all()
    if (etl_has_inc > 0) != (elt_has_inc > 0):
        print("NOTE: only one pipeline has loaded the 1 October increment, so returned orders differ. "
              "Load it in both (or neither) before comparing.")
    print("\nRESULT:", "ETL and ELT MATCH" if all_match else "ETL and ELT DIFFER - see rows marked NO")
    return {"compared": True, "match": bool(all_match),
            "elt_net_revenue": float(elt_row["net_revenue"] or 0),
            "etl_net_revenue": float(etl_row["net_revenue"] or 0)}


# ---------------------------------------------------------------------------
# 5b. Run-level observability checks (same thresholds as the ETL)
# ---------------------------------------------------------------------------
def observe(run_id: str, dag_id: str, warehouse: Path = WAREHOUSE) -> dict:
    rid = run_id.replace("'", "''")
    con = etl.connect(warehouse)
    try:
        rows_loaded = con.execute("SELECT count(*) FROM stg_orders WHERE _run_id = ? AND dq_status <> 'FAIL'",
                                  [run_id]).fetchone()[0]
        return observability.run_checks(
            con, run_id, dag_id, rows_loaded=int(rows_loaded),
            orders_sql=("SELECT store_id, order_status, net_revenue, gross_margin "
                        f"FROM int_orders_enriched WHERE _run_id = '{rid}'"),
            stores_sql="SELECT store_id FROM stg_stores WHERE dq_status <> 'FAIL'")
    finally:
        con.close()


# ---------------------------------------------------------------------------
# 6. Run audit
# ---------------------------------------------------------------------------
def audit(run_id: str, dag_id: str, start_time, raw_info: dict | None, finished_ok: bool,
          status: str | None = None) -> dict:
    """One pipeline_runs row. rows_loaded = orders that reached the reporting layer in this run;
    rows_quarantined = rows flagged FAIL in staging in this run."""
    loaded = rejected = 0
    try:
        con = etl.connect()
        try:
            loaded = con.execute("SELECT count(*) FROM stg_orders WHERE _run_id = ? AND dq_status <> 'FAIL'",
                                 [run_id]).fetchone()[0]
            for t in ["stg_orders"]:
                rejected += con.execute(f"SELECT count(*) FROM {t} WHERE _run_id = ? AND dq_status = 'FAIL'",
                                        [run_id]).fetchone()[0]
            for t in ["stg_payments", "stg_customers", "stg_products", "stg_stores"]:
                rejected += con.execute(f"SELECT count(*) FROM {t} WHERE _run_id = ? AND dq_status = 'FAIL'",
                                        [run_id]).fetchone()[0]
        finally:
            con.close()
    except Exception as e:  # staging may not exist if the run failed early
        print(f"Could not count staged rows: {e}")
    extracted = [{"rows": raw_info["rows_extracted"]}] if raw_info else []
    return etl.audit(run_id, dag_id, start_time, extracted, [{"rows_quarantined": int(rejected)}],
                     {"rows_loaded": int(loaded)}, finished_ok, status=status)
