"""
ETL transform / load / reconcile / report logic (pandas + DuckDB).

Flow (see dags/nammamart_etl_mohanrajs.py):
  extract (5 parallel) -> validate (dims first, then orders/payments) -> transform -> load -> reconcile -> report
"""
import pandas as pd

from nammamart_common import QUARANTINE_DIR, connect

DIM_TABLES = ["etl_dim_customer", "etl_dim_product", "etl_dim_store"]


# ------------------------------------------------------------ quarantine
def write_quarantine(failed, source, run_id, key_col):
    """One CSV per file per run: every failed row, exactly as validated, plus failure_reason."""
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    safe = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in run_id)
    path = QUARANTINE_DIR / f"{source}_failed_{safe}.csv"
    failed.to_csv(path, index=False)
    # one log row per (row, broken rule), so "rejected rows by file and reason" is a plain GROUP BY
    log = pd.DataFrame({"run_id": run_id, "source_file": source, "source_row": failed["_row_num"].values,
                        "row_key": failed[key_col].values, "failure_reason": failed["failure_reason"].values})
    log["failure_reason"] = log.failure_reason.str.split("; ")
    log = log.explode("failure_reason", ignore_index=True)
    return str(path), log


# ------------------------------------------------------------ masking
def mask_email(e):
    if "@" not in e:
        return "***"
    user, domain = e.split("@", 1)
    return f"{user[:2]}***@{domain}"


def mask_phone(p):
    return f"{p[:2]}******{p[-2:]}" if len(p) == 10 and p.isdigit() else "**********"


# ------------------------------------------------------------ transform
def transform_dims(customers, products, stores):
    dc = customers.copy()
    dc["email_masked"] = dc.email.map(mask_email)
    dc["phone_masked"] = dc.phone.map(mask_phone)
    dc = dc[["customer_id", "full_name", "email_masked", "phone_masked", "city", "area", "pincode", "signup_date",
             "loyalty_tier", "is_active"]]
    dp = products[["product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit",
                   "is_perishable", "launch_date"]].copy()
    ds = stores[["store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name",
                 "capacity_orders_per_day"]].copy()
    for col in ("mrp", "cost_price"):
        dp[col] = pd.to_numeric(dp[col])
    ds["capacity_orders_per_day"] = pd.to_numeric(ds.capacity_orders_per_day).astype(int)
    return dc, dp, ds


def transform_orders(orders, dim_product, dim_store):
    """Clean orders (already validated + standardised) -> fact rows with derived measures."""
    f = orders.copy()
    for col in ("quantity", "discount_pct"):
        f[col] = pd.to_numeric(f[col]).astype(int)
    for col in ("unit_price", "order_amount"):
        f[col] = pd.to_numeric(f[col])
    f["delivery_minutes"] = pd.to_numeric(f.delivery_minutes, errors="coerce")
    f["order_ts"] = pd.to_datetime(f.order_ts, format="%Y-%m-%d %H:%M:%S")
    f["order_date"] = f.order_ts.dt.date
    f = f.merge(dim_product[["product_id", "cost_price"]], on="product_id", how="left")
    delivered = f.order_status.eq("DELIVERED")
    f["net_revenue"] = f.order_amount.where(delivered, 0.0).round(2)
    f["gross_margin"] = (f.net_revenue - f.quantity.where(delivered, 0) * f.cost_price).round(2)
    f["within_15_min"] = delivered & (f.delivery_minutes <= 15)
    f = f.drop(columns="cost_price")
    cols = ["order_id", "order_ts", "order_date", "customer_id", "store_id", "product_id", "quantity",
            "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status", "channel",
            "delivery_minutes", "net_revenue", "gross_margin", "within_15_min"]
    return f[cols]


# ------------------------------------------------------------ load
def load_dims(con, dc, dp, ds):
    """Dimensions are full-refresh: replace the table, so a re-run can never duplicate a row."""
    for name, df in (("etl_dim_customer", dc), ("etl_dim_product", dp), ("etl_dim_store", ds)):
        con.register("_df", df)
        con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM _df")
        con.unregister("_df")


FACT_DDL = """
CREATE TABLE IF NOT EXISTS etl_fact_orders (
    order_id VARCHAR PRIMARY KEY, order_ts TIMESTAMP, order_date DATE, customer_id VARCHAR, store_id VARCHAR,
    product_id VARCHAR, quantity INTEGER, unit_price DOUBLE, discount_pct INTEGER, order_amount DOUBLE,
    payment_mode VARCHAR, order_status VARCHAR, channel VARCHAR, delivery_minutes DOUBLE,
    net_revenue DOUBLE, gross_margin DOUBLE, within_15_min BOOLEAN, _run_id VARCHAR, _loaded_at TIMESTAMP
)"""
HISTORY_DDL = """
CREATE TABLE IF NOT EXISTS etl_order_status_history (
    order_id VARCHAR, old_status VARCHAR, new_status VARCHAR, old_net_revenue DOUBLE, new_net_revenue DOUBLE,
    changed_in_run VARCHAR, changed_at TIMESTAMP
)"""
QUARANTINE_DDL = """
CREATE TABLE IF NOT EXISTS etl_quarantine_log (
    run_id VARCHAR, source_file VARCHAR, source_row BIGINT, row_key VARCHAR, failure_reason VARCHAR,
    logged_at TIMESTAMP
)"""


def load_fact(con, fact, run_id, mode):
    """
    mode='full'        : TRUNCATE + INSERT. The September batch always rebuilds the same table.
    mode='incremental' : UPSERT on order_id (new orders inserted, corrected orders updated in place).
    Both are idempotent: running twice with the same input leaves the same rows.
    Returns dict(inserted, updated, total).
    """
    con.execute(FACT_DDL)
    con.execute(HISTORY_DDL)
    f = fact.assign(_run_id=run_id, _loaded_at=pd.Timestamp.now().floor("s"))
    con.register("_stage", f)
    if mode == "full":
        con.execute("DELETE FROM etl_fact_orders")
        con.execute("INSERT INTO etl_fact_orders SELECT * FROM _stage")
        inserted, updated = len(f), 0
    else:
        updated = con.execute("SELECT count(*) FROM _stage s JOIN etl_fact_orders t USING (order_id) "
                              "WHERE t.order_status <> s.order_status OR t.order_amount <> s.order_amount"
                              ).fetchone()[0]
        inserted = con.execute("SELECT count(*) FROM _stage s WHERE order_id NOT IN "
                               "(SELECT order_id FROM etl_fact_orders)").fetchone()[0]
        # keep the superseded version so finance can still see "as originally reported"
        con.execute("""
            INSERT INTO etl_order_status_history
            SELECT t.order_id, t.order_status, s.order_status, t.net_revenue, s.net_revenue, ?, now()
            FROM _stage s JOIN etl_fact_orders t USING (order_id)
            WHERE t.order_status <> s.order_status
              AND NOT EXISTS (SELECT 1 FROM etl_order_status_history h WHERE h.order_id = t.order_id
                              AND h.old_status = t.order_status AND h.new_status = s.order_status)""", [run_id])
        con.execute("DELETE FROM etl_fact_orders WHERE order_id IN (SELECT order_id FROM _stage)")
        con.execute("INSERT INTO etl_fact_orders SELECT * FROM _stage")
    con.unregister("_stage")
    total = con.execute("SELECT count(*) FROM etl_fact_orders").fetchone()[0]
    return {"inserted": int(inserted), "updated": int(updated), "total": int(total)}


def load_quarantine_log(con, logs):
    con.execute(QUARANTINE_DDL)
    for run_id in {l["run_id"].iloc[0] for l in logs if len(l)}:
        con.execute("DELETE FROM etl_quarantine_log WHERE run_id = ?", [run_id])
    for log in logs:
        if len(log):
            con.register("_q", log.assign(logged_at=pd.Timestamp.now().floor("s")))
            con.execute("INSERT INTO etl_quarantine_log SELECT run_id, source_file, source_row, row_key, "
                        "failure_reason, logged_at FROM _q")
            con.unregister("_q")


# ------------------------------------------------------------ reconcile
RECON_DDL = """
CREATE OR REPLACE TABLE etl_payment_reconciliation (
    order_id VARCHAR, payment_id VARCHAR, order_status VARCHAR, order_amount DOUBLE, payment_status VARCHAR,
    amount_paid DOUBLE, difference DOUBLE, mismatch_reason VARCHAR, run_id VARCHAR
)"""


def reconcile_payments(con, payments_clean, orphan_payments, run_id):
    """
    One row per mismatch between clean orders and clean payments:
      missing_payment, amount_mismatch, payment_failed_for_delivered_order, return_not_refunded,
      refund_without_return, payment_mode_mismatch, orphan_payment (payment for an order not in orders.csv).
    """
    con.execute(RECON_DDL)
    p = payments_clean[["payment_id", "order_id", "payment_status", "amount_paid", "payment_mode"]].copy()
    p["amount_paid"] = pd.to_numeric(p.amount_paid)
    con.register("_p", p)
    con.execute("""
        INSERT INTO etl_payment_reconciliation
        WITH j AS (
            SELECT o.order_id, p.payment_id, o.order_status, o.order_amount, p.payment_status, p.amount_paid,
                   o.payment_mode AS o_mode, p.payment_mode AS p_mode
            FROM etl_fact_orders o LEFT JOIN _p p ON p.order_id = o.order_id)
        SELECT order_id, payment_id, order_status, order_amount, payment_status, amount_paid,
               CASE WHEN payment_status = 'FAILED' THEN -order_amount ELSE round(coalesce(amount_paid, 0) - order_amount, 2) END,
               CASE
                 WHEN order_status = 'DELIVERED' AND payment_id IS NULL THEN 'missing_payment'
                 WHEN order_status = 'DELIVERED' AND payment_status = 'FAILED' THEN 'payment_failed_for_delivered_order'
                 WHEN order_status = 'DELIVERED' AND payment_status = 'SUCCESS' AND abs(amount_paid - order_amount) > 0.01
                      THEN 'amount_mismatch'
                 WHEN order_status = 'DELIVERED' AND payment_status = 'REFUNDED' THEN 'refund_without_return'
                 WHEN order_status = 'RETURNED' AND payment_id IS NOT NULL AND payment_status <> 'REFUNDED'
                      THEN 'return_not_refunded'
                 WHEN order_status = 'RETURNED' AND payment_status = 'REFUNDED' AND abs(amount_paid - order_amount) > 0.01
                      THEN 'amount_mismatch'
                 WHEN order_status = 'CANCELLED' AND payment_id IS NOT NULL THEN 'payment_for_cancelled_order'
                 WHEN payment_id IS NOT NULL AND o_mode <> p_mode THEN 'payment_mode_mismatch'
               END, ?
        FROM j""", [run_id])
    con.unregister("_p")
    if len(orphan_payments):
        o = orphan_payments[["payment_id", "order_id", "payment_status", "amount_paid"]].copy()
        o["amount_paid"] = pd.to_numeric(o.amount_paid, errors="coerce")
        con.register("_o", o)
        con.execute("""INSERT INTO etl_payment_reconciliation
                       SELECT order_id, payment_id, NULL, NULL, payment_status, amount_paid, amount_paid,
                              'orphan_payment', ? FROM _o""", [run_id])
        con.unregister("_o")
    con.execute("DELETE FROM etl_payment_reconciliation WHERE mismatch_reason IS NULL")
    return con.execute("SELECT count(*) FROM etl_payment_reconciliation").fetchone()[0]


# ------------------------------------------------------------ report
def build_reports(con):
    con.execute("""
        CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
        SELECT f.order_date, s.zone, f.store_id, s.store_name, count(*) AS delivered_orders,
               round(sum(f.net_revenue), 2) AS net_revenue,
               round(avg(f.delivery_minutes), 2) AS avg_delivery_minutes,
               round(100.0 * avg(f.within_15_min::int), 2) AS pct_within_15_min
        FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
        WHERE f.order_status = 'DELIVERED'
        GROUP BY ALL ORDER BY 1, 2, 3""")
    con.execute("""
        CREATE OR REPLACE TABLE etl_category_margin AS
        SELECT p.category, round(sum(f.net_revenue), 2) AS net_revenue,
               round(sum(f.net_revenue - f.gross_margin), 2) AS cogs, round(sum(f.gross_margin), 2) AS gross_margin,
               round(100 * sum(f.gross_margin) / sum(f.net_revenue), 2) AS margin_pct
        FROM etl_fact_orders f JOIN etl_dim_product p USING (product_id)
        WHERE f.order_status = 'DELIVERED' GROUP BY 1 ORDER BY 2 DESC""")
    con.execute("""
        CREATE OR REPLACE TABLE etl_payment_mode_revenue AS
        SELECT payment_mode, count(*) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
        FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY 3 DESC""")
    con.execute("""
        CREATE OR REPLACE TABLE etl_top_customers AS
        SELECT c.customer_id, c.full_name, c.loyalty_tier, c.email_masked, c.phone_masked,
               count(*) AS delivered_orders, round(sum(f.net_revenue), 2) AS total_spend
        FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id)
        WHERE f.order_status = 'DELIVERED' GROUP BY ALL ORDER BY total_spend DESC LIMIT 10""")
    out = {}
    for t in ("etl_daily_zone_revenue", "etl_category_margin"):
        out[t] = con.execute(f"SELECT * FROM {t}").fetchdf()
    return out
