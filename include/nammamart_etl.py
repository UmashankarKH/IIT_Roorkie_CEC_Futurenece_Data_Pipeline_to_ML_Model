"""Task bodies for the NammaMart ETL DAG: Extract -> Validate -> Quarantine -> Transform -> Load -> Reconcile -> Report."""
from __future__ import annotations

import shutil
import time
from datetime import date
from pathlib import Path

import pandas as pd

import nammamart_common as nc
import nammamart_dq as dq

DAG_ID = f"nammamart_etl_{nc.OWNER}"


def _stage_dir(run_id: str) -> Path:
    path = nc.STAGE_DIR / nc.safe_run_id(run_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def extract(name: str, data_dir: str | None = None) -> dict:
    rows = nc.check_schema(name, data_dir)
    print(f"[Extract] {name}.csv: {rows} rows, schema OK")
    return {"name": name, "rows": rows}


def _finish(name: str, raw: pd.DataFrame, passed: pd.DataFrame, failed: pd.DataFrame, results: list[dict],
            run_id: str) -> dict:
    stage = _stage_dir(run_id)
    clean_path = stage / f"{name}_clean.csv"
    passed.to_csv(clean_path, index=False)
    quarantine_path = None
    if len(failed):
        nc.QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
        quarantine_path = nc.QUARANTINE_DIR / f"{name}_failed_{nc.safe_run_id(run_id)}.csv"
        out = raw.loc[failed.index].copy()  # quarantine keeps the row exactly as received
        out["failure_reason"] = failed["failure_reason"]
        out.to_csv(quarantine_path, index=False)
    print(f"[Validate] {name}: in={len(raw)} passed={len(passed)} quarantined={len(failed)}")
    for r in results:
        if r["rows_failed"]:
            print(f"    {r['status']:4} {r['severity']:8} {r['check_name']}: {r['rows_failed']}/{r['rows_checked']}")
    return {"name": name, "rows_in": len(raw), "rows_passed": len(passed), "rows_quarantined": len(failed),
            "clean_path": str(clean_path), "quarantine_path": str(quarantine_path) if quarantine_path else None,
            "dq": results}


def validate_dimension(name: str, run_id: str, data_dir: str | None = None, as_of: str | None = None) -> dict:
    validators = {"customers": dq.validate_customers, "products": dq.validate_products, "stores": dq.validate_stores}
    raw = dq.read_csv(nc.csv_path(name, data_dir))
    passed, failed, results = validators[name](raw, nc.today_ist(as_of))
    return _finish(name, raw, passed, failed, results, run_id)


def _ids(result: dict, col: str) -> set:
    return set(dq.read_csv(result["clean_path"])[col].dropna())


def validate_orders(file_name: str, run_id: str, customers: dict, products: dict, stores: dict,
                    data_dir: str | None = None, as_of: str | None = None) -> dict:
    dataset = "orders" if file_name == "orders" else "orders_increment"
    raw = dq.read_csv(nc.csv_path(file_name, data_dir))
    passed, failed, results = dq.validate_orders(
        raw, dataset, _ids(customers, "customer_id"), _ids(products, "product_id"), _ids(stores, "store_id"),
        nc.today_ist(as_of), nc.BATCH_OF[file_name])
    passed["source_batch"] = nc.BATCH_OF[file_name]
    res = _finish(file_name, raw, passed, failed, results, run_id)
    res["source_batch"] = nc.BATCH_OF[file_name]
    return res


def validate_payments(run_id: str, load_increment: bool, data_dir: str | None = None,
                      as_of: str | None = None) -> dict:
    raw = dq.read_csv(nc.csv_path("payments", data_dir))
    order_ids = set(dq.read_csv(nc.csv_path("orders", data_dir)).order_id.dropna().str.strip())
    if load_increment:
        order_ids |= set(dq.read_csv(nc.csv_path(nc.INCREMENT_FILE, data_dir)).order_id.dropna().str.strip())
    passed, failed, results = dq.validate_payments(raw, order_ids, nc.today_ist(as_of))
    passed["order_in_source"] = passed.order_id.isin(order_ids)
    return _finish("payments", raw, passed, failed, results, run_id)


# ---- Transform + Load ------------------------------------------------------------------------------------

def _dim_frames(customers: dict, products: dict, stores: dict):
    c = dq.read_csv(customers["clean_path"])
    c["loyalty_tier"] = c.loyalty_tier.str.title()
    dim_customer = pd.DataFrame({
        "customer_id": c.customer_id, "full_name": c.full_name,
        "email_masked": c.email.map(nc.mask_email), "phone_masked": c.phone.map(nc.mask_phone),
        "city": c.city, "area": c.area, "pincode": c.pincode,
        "signup_date": pd.to_datetime(c.signup_date).dt.date, "loyalty_tier": c.loyalty_tier,
        "is_active": c.is_active})
    p = dq.read_csv(products["clean_path"])
    dim_product = pd.DataFrame({
        "product_id": p.product_id, "product_name": p.product_name, "category": p.category.fillna("UNKNOWN"),
        "sub_category": p.sub_category, "brand": p.brand, "mrp": pd.to_numeric(p.mrp),
        "cost_price": pd.to_numeric(p.cost_price), "unit": p.unit, "is_perishable": p.is_perishable})
    s = dq.read_csv(stores["clean_path"])
    dim_store = pd.DataFrame({
        "store_id": s.store_id, "store_name": s.store_name, "zone": s.zone.fillna("UNKNOWN"), "area": s.area,
        "pincode": s.pincode, "manager_name": s.manager_name,
        "capacity_orders_per_day": pd.to_numeric(s.capacity_orders_per_day)})
    return dim_customer, dim_product, dim_store


def _fact_frame(order_results: list[dict], dim_product: pd.DataFrame, run_id: str) -> pd.DataFrame:
    frames = [dq.read_csv(r["clean_path"]) for r in order_results if r and r.get("clean_path")]
    o = pd.concat(frames, ignore_index=True).sort_values("source_batch", kind="stable")
    o = o.drop_duplicates("order_id", keep="last")  # the newest batch wins inside a run
    o = o.merge(dim_product[["product_id", "cost_price"]], on="product_id", how="left")
    ts = pd.to_datetime(o.order_ts, format=dq.TS_FORMAT)
    qty, amount, minutes = (pd.to_numeric(o[c]) for c in ("quantity", "order_amount", "delivery_minutes"))
    delivered = o.order_status == "DELIVERED"
    return pd.DataFrame({
        "order_id": o.order_id, "order_ts": ts, "order_date": ts.dt.date, "customer_id": o.customer_id,
        "store_id": o.store_id, "product_id": o.product_id, "quantity": qty.astype("int64"),
        "unit_price": pd.to_numeric(o.unit_price), "discount_pct": pd.to_numeric(o.discount_pct),
        "order_amount": amount, "payment_mode": o.payment_mode, "order_status": o.order_status,
        "channel": o.channel, "delivery_minutes": minutes,
        "net_revenue": amount.where(delivered, 0.0),
        "gross_margin": (amount - qty * pd.to_numeric(o.cost_price)).where(delivered, 0.0).round(2),
        "within_15_min": (minutes <= 15).astype("boolean").where(delivered & minutes.notna()),
        "source_batch": o.source_batch, "loaded_run_id": run_id})


def _write_quarantine_table(con, run_id: str, results) -> None:
    """Mirror the quarantine files into etl_quarantine so rejects can be counted with SQL."""
    frames = []
    for res in results:
        if res and res.get("quarantine_path"):
            q = dq.read_csv(res["quarantine_path"])
            frames.append(pd.DataFrame({"run_id": run_id, "source_file": f"{res['name']}.csv",
                                        "row_key": q.iloc[:, 0], "failure_reason": q.failure_reason}))
    con.execute("CREATE TABLE IF NOT EXISTS etl_quarantine (run_id VARCHAR, source_file VARCHAR, row_key VARCHAR, "
                "failure_reason VARCHAR, quarantined_at TIMESTAMP)")
    con.execute("DELETE FROM etl_quarantine WHERE run_id = ?", [run_id])
    if frames:
        con.register("q_frame", pd.concat(frames, ignore_index=True))
        con.execute("INSERT INTO etl_quarantine SELECT *, TIMEZONE('Asia/Kolkata', NOW()) FROM q_frame")
        con.unregister("q_frame")


def load_warehouse(run_id: str, customers: dict, products: dict, stores: dict, orders: dict,
                   increment: dict | None, payments: dict) -> dict:
    dim_customer, dim_product, dim_store = _dim_frames(customers, products, stores)
    fact = _fact_frame([orders, increment], dim_product, run_id)
    pay = dq.read_csv(payments["clean_path"])
    fact_pay = pd.DataFrame({
        "payment_id": pay.payment_id, "order_id": pay.order_id, "payment_ts": pd.to_datetime(pay.payment_ts),
        "payment_mode": pay.payment_mode, "amount_paid": pd.to_numeric(pay.amount_paid),
        "payment_status": pay.payment_status, "gateway_ref": pay.gateway_ref,
        "order_in_source": pay.order_in_source == "True", "loaded_run_id": run_id})

    con = nc.connect()
    try:
        con.execute("BEGIN")
        for table, frame in (("etl_dim_customer", dim_customer), ("etl_dim_product", dim_product),
                             ("etl_dim_store", dim_store), ("etl_fact_payments", fact_pay)):
            con.register("frame", frame)
            con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM frame")
            con.unregister("frame")
        con.register("new_fact", fact)
        con.execute("CREATE TABLE IF NOT EXISTS etl_fact_orders AS SELECT * FROM new_fact WHERE FALSE")
        counts = con.execute("""
            SELECT COUNT(*) FILTER (WHERE f.order_id IS NULL),
                   COUNT(*) FILTER (WHERE f.order_id IS NOT NULL AND f.source_batch <= n.source_batch),
                   COUNT(*) FILTER (WHERE f.order_id IS NOT NULL AND f.source_batch >  n.source_batch),
                   COUNT(*) FILTER (WHERE f.order_id IS NOT NULL AND f.source_batch <= n.source_batch
                                      AND f.order_status IS DISTINCT FROM n.order_status)
            FROM new_fact n LEFT JOIN etl_fact_orders f USING (order_id)""").fetchone()
        inserted, updated, ignored_older, status_changed = counts
        con.execute("""DELETE FROM etl_fact_orders WHERE EXISTS
                       (SELECT 1 FROM new_fact n WHERE n.order_id = etl_fact_orders.order_id
                          AND etl_fact_orders.source_batch <= n.source_batch)""")
        con.execute("""INSERT INTO etl_fact_orders SELECT * FROM new_fact n
                       WHERE NOT EXISTS (SELECT 1 FROM etl_fact_orders f WHERE f.order_id = n.order_id)""")
        con.unregister("new_fact")

        results = []
        for res in (customers, products, stores, orders, increment, payments):
            if res and "dq" in res:
                results += res["dq"]
        results.append(nc.volume_result(con, DAG_ID, run_id, "orders", orders["rows_in"]))
        nc.write_dq_results(con, run_id, DAG_ID, results)
        _write_quarantine_table(con, run_id, (customers, products, stores, orders, increment, payments))
        con.execute("COMMIT")
        total = con.execute("SELECT COUNT(*), COUNT(DISTINCT order_id) FROM etl_fact_orders").fetchone()
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    print(f"[Load] orders: inserted={inserted} updated={updated} (status changed={status_changed}) "
          f"kept_newer={ignored_older}; fact rows={total[0]} distinct order_id={total[1]}")
    parts = [r for r in (customers, products, stores, orders, increment, payments) if r and "dq" in r]
    return {"inserted": inserted, "updated": updated, "status_changed": status_changed, "fact_rows": total[0],
            "rows_loaded": sum(r["rows_passed"] for r in parts)}


RECONCILIATION_SQL = """
CREATE OR REPLACE TABLE etl_payment_reconciliation AS
WITH per_order AS (
    SELECT o.order_id, o.order_status, o.order_amount,
           CASE o.order_status WHEN 'DELIVERED' THEN 'SUCCESS' WHEN 'RETURNED' THEN 'REFUNDED' END AS expected_status,
           COUNT(p.payment_id) AS n_pay,
           COUNT(*) FILTER (WHERE p.payment_status = CASE o.order_status WHEN 'DELIVERED' THEN 'SUCCESS'
                                                       WHEN 'RETURNED' THEN 'REFUNDED' END) AS n_expected,
           SUM(p.amount_paid) FILTER (WHERE p.payment_status = CASE o.order_status WHEN 'DELIVERED' THEN 'SUCCESS'
                                                       WHEN 'RETURNED' THEN 'REFUNDED' END) AS expected_paid,
           SUM(p.amount_paid) AS any_paid,
           STRING_AGG(p.payment_id, ',') AS payment_ids,
           STRING_AGG(DISTINCT p.payment_status, ',') AS payment_statuses
    FROM etl_fact_orders o LEFT JOIN etl_fact_payments p USING (order_id)
    GROUP BY ALL
)
SELECT order_id, payment_ids, order_status, order_amount, payment_statuses,
       CASE WHEN order_status = 'CANCELLED' THEN any_paid ELSE expected_paid END AS payment_amount,
       ROUND(CASE WHEN order_status = 'CANCELLED' THEN -any_paid
                  ELSE order_amount - COALESCE(expected_paid, 0) END, 2) AS difference,
       CASE WHEN order_status = 'CANCELLED' THEN 'CANCELLED_ORDER_HAS_PAYMENT'
            WHEN n_pay = 0 THEN 'NO_PAYMENT_FOUND'
            WHEN n_expected = 0 THEN 'PAYMENT_STATUS_MISMATCH'
            ELSE 'AMOUNT_MISMATCH' END AS mismatch_reason,
       '{run_id}' AS run_id
FROM per_order
WHERE (order_status = 'CANCELLED' AND n_pay > 0)
   OR (order_status <> 'CANCELLED' AND (n_pay = 0 OR n_expected = 0 OR ABS(order_amount - expected_paid) > 0.01))
UNION ALL
SELECT p.order_id, p.payment_id, NULL, NULL, p.payment_status, p.amount_paid, ROUND(-p.amount_paid, 2),
       CASE WHEN p.order_in_source THEN 'PAYMENT_FOR_REJECTED_ORDER' ELSE 'PAYMENT_FOR_UNKNOWN_ORDER' END, '{run_id}'
FROM etl_fact_payments p
WHERE NOT EXISTS (SELECT 1 FROM etl_fact_orders o WHERE o.order_id = p.order_id)
"""


def reconcile(run_id: str) -> dict:
    con = nc.connect()
    try:
        con.execute(RECONCILIATION_SQL.replace("{run_id}", run_id.replace("'", "''")))
        rows = con.execute("SELECT mismatch_reason, COUNT(*), ROUND(SUM(ABS(difference)), 2) "
                           "FROM etl_payment_reconciliation GROUP BY 1 ORDER BY 1").fetchall()
    finally:
        con.close()
    for reason, n, amount in rows:
        print(f"[Reconcile] {reason}: {n} orders/payments, amount {amount}")
    return {"mismatches": sum(r[1] for r in rows), "amount": float(sum(r[2] or 0 for r in rows))}


REPORT_SQL = [
    """CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
       SELECT f.order_date, s.zone, s.store_id, s.store_name,
              ROUND(SUM(f.net_revenue), 2) AS net_revenue, COUNT(*) FILTER (WHERE f.order_status = 'DELIVERED') AS delivered_orders
       FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
       WHERE f.order_status = 'DELIVERED' GROUP BY ALL""",
    """CREATE OR REPLACE TABLE etl_category_margin AS
       SELECT p.category, ROUND(SUM(f.net_revenue), 2) AS revenue, ROUND(SUM(f.gross_margin), 2) AS gross_margin,
              ROUND(100 * SUM(f.gross_margin) / NULLIF(SUM(f.net_revenue), 0), 2) AS margin_pct
       FROM etl_fact_orders f JOIN etl_dim_product p USING (product_id)
       WHERE f.order_status = 'DELIVERED' GROUP BY p.category""",
]


def report() -> dict:
    con = nc.connect()
    try:
        for sql in REPORT_SQL:
            con.execute(sql)
        zone = con.execute("SELECT zone, ROUND(SUM(net_revenue), 2) AS net_revenue, SUM(delivered_orders) AS orders "
                           "FROM etl_daily_zone_revenue GROUP BY zone ORDER BY net_revenue DESC").df()
        margin = con.execute("SELECT * FROM etl_category_margin ORDER BY revenue DESC").df()
    finally:
        con.close()
    print("[Report] Net revenue by zone\n" + zone.to_string(index=False))
    print("[Report] Category margin\n" + margin.to_string(index=False))
    return {"net_revenue": float(zone.net_revenue.sum())}


def cleanup_stage(run_id: str, keep_days: int = 7) -> None:
    """Drop old staging folders, but keep this run's so that Clear can re-run downstream tasks."""
    cutoff = time.time() - keep_days * 86400
    for path in nc.STAGE_DIR.glob("*"):
        if path.name != nc.safe_run_id(run_id) and path.is_dir() and path.stat().st_mtime < cutoff:
            shutil.rmtree(path, ignore_errors=True)
