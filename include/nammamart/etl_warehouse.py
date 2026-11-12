"""DuckDB star-schema load and reporting for the NammaMart ETL pipeline.

The curated CSV/Parquet artifacts remain useful for quarantine review, while
this module implements the assignment's required persistent ETL warehouse.
Each load rebuilds the derived tables transactionally from the validated
curated artifacts, making a retry idempotent for the same source snapshot.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
from include.nammamart.scd2 import apply_customer_loyalty_scd2
from include.nammamart.etl import OUTPUT_DIR

PROJECT_ROOT = Path(__file__).resolve().parents[2]
WAREHOUSE_PATH = PROJECT_ROOT / "include" / "warehouse_gowtham.duckdb"


def _as_numeric(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _mask_email(value: Any) -> str | None:
    if pd.isna(value) or not str(value).strip():
        return None
    text = str(value).strip()
    if "*" in text:
        return text
    if "@" not in text:
        return "***"
    local, domain = text.split("@", 1)
    return f"{local[:2]}***@{domain}" if local else f"***@{domain}"


def _mask_phone(value: Any) -> str | None:
    if pd.isna(value) or not str(value).strip():
        return None
    text = str(value).strip()
    if "*" in text:
        return text
    digits = "".join(char for char in text if char.isdigit())
    if len(digits) < 4:
        return "*" * max(len(digits), 4)
    return f"{digits[:2]}{'*' * max(len(digits) - 4, 4)}{digits[-2:]}"


def _create_table(con: duckdb.DuckDBPyConnection, table_name: str, frame: pd.DataFrame) -> None:
    temp_name = f"_load_{table_name}"
    con.register(temp_name, frame)
    con.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM "{temp_name}"')
    con.unregister(temp_name)


def load_etl_star_schema(run_id: str) -> dict[str, int]:
    """Create/update ETL dimensions and fact from validated curated outputs."""
    scd2_summary = {
    "history_rows": 0,
    "current_customers": 0,
    "historical_versions": 0,
    }
    customers = pd.read_parquet(OUTPUT_DIR / "customers.parquet").copy()
    products = pd.read_parquet(OUTPUT_DIR / "products.parquet").copy()
    stores = pd.read_parquet(OUTPUT_DIR / "stores.parquet").copy()
    orders = pd.read_parquet(OUTPUT_DIR / "orders.parquet").copy()

    customers = customers.drop_duplicates(subset=["customer_id"]).copy()
    products = products.drop_duplicates(subset=["product_id"]).copy()
    stores = stores.drop_duplicates(subset=["store_id"]).copy()


    customer_dim = customers.copy()
    if "email" in customer_dim.columns:
        customer_dim["email"] = customer_dim["email"].map(_mask_email)
    if "phone" in customer_dim.columns:
        customer_dim["phone"] = customer_dim["phone"].map(_mask_phone)

    # The reporting star schema contains only approved, validated records.
    orders = orders.drop_duplicates(subset=["order_id"]).copy()
    for column in ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"]:
        if column in orders.columns:
            orders[column] = pd.to_numeric(orders[column], errors="coerce")
    for column in ["mrp", "cost_price"]:
        if column in products.columns:
            products[column] = pd.to_numeric(products[column], errors="coerce")

    orders["order_ts"] = pd.to_datetime(orders["order_ts"], errors="coerce", format="mixed")
    orders["order_date"] = orders["order_ts"].dt.date
    product_cost = products[["product_id", "category", "cost_price"]].copy()
    store_zone = stores[["store_id", "store_name", "zone"]].copy()

    fact = orders.copy()
    fact = fact.merge(product_cost, on="product_id", how="left", validate="many_to_one")
    fact = fact.merge(store_zone, on="store_id", how="left", validate="many_to_one")
    fact["net_revenue"] = pd.to_numeric(fact["order_amount"], errors="coerce").round(2)
    fact["gross_margin"] = (
        fact["net_revenue"]
        - pd.to_numeric(fact["quantity"], errors="coerce")
        * pd.to_numeric(fact["cost_price"], errors="coerce")
    ).round(2)
    delivery = pd.to_numeric(fact["delivery_minutes"], errors="coerce")
    fact["within_15_min"] = fact["order_status"].astype("string").str.upper().eq("DELIVERED") & delivery.le(15)
    fact["run_id"] = run_id

    # Do not expose customer email/phone in the fact table.
    fact = fact.drop(columns=[c for c in ["email", "phone", "full_name", "city", "area", "pincode"] if c in fact.columns])

    WAREHOUSE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(WAREHOUSE_PATH)) as con:
        con.execute("BEGIN TRANSACTION")
        try:
            _create_table(con, "etl_dim_customer", customer_dim)
            scd2_summary = apply_customer_loyalty_scd2( con, source_table="etl_dim_customer",)
            _create_table(con, "etl_dim_product", products)
            _create_table(con, "etl_dim_store", stores)
            _create_table(con, "etl_fact_orders", fact)
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

    return {
        "etl_dim_customer": len(customer_dim),
        "etl_dim_product": len(products),
        "etl_dim_store": len(stores),
        "etl_fact_orders": len(fact),
        "dim_customer_scd2_history_rows": scd2_summary["history_rows"],
        "dim_customer_scd2_current_customers": scd2_summary["current_customers"],
        "dim_customer_scd2_historical_versions": scd2_summary["historical_versions"],
    }


def build_etl_payment_reconciliation(source_dir: Path, run_id: str) -> int:
    """Persist one row for each clean-order/payment mismatch or orphan payment."""
    orders = pd.read_parquet(OUTPUT_DIR / "orders.parquet").copy()
    payments = pd.read_csv(source_dir / "payments.csv", dtype="string", keep_default_na=True).drop_duplicates().copy()

    orders["order_amount"] = pd.to_numeric(orders["order_amount"], errors="coerce").round(2)
    orders["order_status"] = orders["order_status"].astype("string").str.strip().str.upper()
    payments["amount_paid"] = pd.to_numeric(payments["amount_paid"], errors="coerce").round(2)
    payments["payment_status_norm"] = payments["payment_status"].astype("string").str.strip().str.upper()
    payments["order_id_norm"] = payments["order_id"].astype("string").str.strip()

    successful = payments.loc[payments["payment_status_norm"].eq("SUCCESS")].copy()
    successful_summary = successful.groupby("order_id_norm", dropna=False).agg(
        successful_payment_amount=("amount_paid", "sum"),
        payment_ids=("payment_id", lambda values: ",".join(sorted(set(values.dropna().astype(str))))),
        successful_payment_count=("payment_id", "nunique"),
    ).reset_index().rename(columns={"order_id_norm": "order_id"})

    joined = orders.merge(successful_summary, on="order_id", how="left", validate="one_to_one")
    joined["amount_difference"] = (
        joined["order_amount"] - joined["successful_payment_amount"]
    ).round(2)
    mismatch_rows: list[dict[str, Any]] = []
    for row in joined.itertuples(index=False):
        status = str(getattr(row, "order_status", "")).upper()
        paid = getattr(row, "successful_payment_amount", None)
        paid = None if pd.isna(paid) else float(paid)
        order_amount = None if pd.isna(getattr(row, "order_amount", None)) else float(row.order_amount)
        reason = None
        if status != "CANCELLED" and paid is None:
            reason = "no_successful_payment"
        elif status != "CANCELLED" and order_amount is not None and paid is not None and abs(order_amount - paid) > 0.01:
            reason = "payment_amount_mismatch"
        if reason:
            mismatch_rows.append({
                "order_id": str(row.order_id),
                "order_amount": order_amount,
                "successful_payment_amount": paid,
                "amount_difference": None if order_amount is None or paid is None else round(order_amount - paid, 2),
                "payment_ids": getattr(row, "payment_ids", None),
                "mismatch_reason": reason,
                "order_status": status,
                "run_id": run_id,
            })


    valid_order_ids = set(orders["order_id"].dropna().astype(str).str.strip())

    # Include all original order IDs, including orders rejected by ETL.

    source_order_ids: set[str] = set()
    for filename in (
        "orders.csv",
        "orders_2026-10-01_increment.csv",
    ):
        source_path = source_dir / filename
        if not source_path.exists():
            continue

        source_orders = pd.read_csv(
            source_path,
            dtype={"order_id": "string"},
            keep_default_na=True,
        )
        if "order_id" in source_orders.columns:
            source_order_ids.update(
                source_orders["order_id"].dropna().str.strip()
            )


    # Read the actual ETL quarantine output to distinguish rejected orders.
    quarantine_path = OUTPUT_DIR / "orders_quarantine.csv"
    quarantined_order_ids: set[str] = set()

    if quarantine_path.exists():
        quarantined_orders = pd.read_csv(
            quarantine_path,
            dtype={"order_id": "string"},
            keep_default_na=True,
        )
        quarantined_order_ids.update(
            quarantined_orders["order_id"].dropna().str.strip()
        )

    orphan_payments = payments.loc[
        ~payments["order_id_norm"].isin(valid_order_ids)
    ].copy()

    for row in orphan_payments.itertuples(index=False):
        order_id = (
            None
            if pd.isna(row.order_id_norm)
            else str(row.order_id_norm).strip()
        )

        if order_id is not None and order_id in quarantined_order_ids:
            reason = "order_quarantined"
        elif order_id is not None and order_id in source_order_ids:
            reason = "order_not_curated"
        else:
            reason = "orphan_payment_order"

        mismatch_rows.append({
            "order_id": order_id,
            "order_amount": None,
            "successful_payment_amount": (
                float(row.amount_paid)
                if row.payment_status_norm == "SUCCESS"
                and pd.notna(row.amount_paid)
                else None
            ),
            "amount_difference": None,
            "payment_ids": (
                None if pd.isna(row.payment_id) else str(row.payment_id)
            ),
            "mismatch_reason": reason,
            "order_status": None,
            "run_id": run_id,
        })

    reconciliation = pd.DataFrame(mismatch_rows, columns=[
        "order_id", "order_amount", "successful_payment_amount", "amount_difference",
        "payment_ids", "mismatch_reason", "order_status", "run_id",
    ])
    print("DEBUG reconciliation rows:")
    print(pd.DataFrame(mismatch_rows)[["order_id", "mismatch_reason"]].to_string(index=False))
    with duckdb.connect(str(WAREHOUSE_PATH)) as con:
        _create_table(con, "etl_payment_reconciliation", reconciliation)
    return len(reconciliation)


def build_etl_reports() -> dict[str, list[dict[str, Any]]]:
    """Build ETL reports, including daily store-capacity monitoring."""
    with duckdb.connect(str(WAREHOUSE_PATH)) as con:
        con.execute("""
            CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT
                f.order_date,
                s.zone,
                f.store_id,
                s.store_name,
                COUNT(*) AS delivered_orders,
                ROUND(SUM(f.net_revenue), 2) AS net_revenue
            FROM etl_fact_orders f
            JOIN etl_dim_store s USING (store_id)
            WHERE UPPER(f.order_status) = 'DELIVERED'
            GROUP BY f.order_date, s.zone, f.store_id, s.store_name
            ORDER BY f.order_date, s.zone, f.store_id
        """)

        con.execute("""
            CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT
                p.category,
                ROUND(SUM(f.net_revenue), 2) AS revenue,
                ROUND(SUM(f.quantity * p.cost_price), 2) AS cost_of_goods,
                ROUND(SUM(f.net_revenue - f.quantity * p.cost_price), 2)
                    AS gross_margin
            FROM etl_fact_orders f
            JOIN etl_dim_product p USING (product_id)
            WHERE UPPER(f.order_status) = 'DELIVERED'
            GROUP BY p.category
            ORDER BY p.category
        """)

        # Count all valid orders by store and calendar day.
        # Exceeding capacity is an operational warning, not a DQ failure.
        con.execute("""
            CREATE OR REPLACE TABLE etl_daily_store_capacity AS
            WITH daily AS (
                SELECT
                    f.order_date,
                    f.store_id,
                    s.store_name,
                    TRY_CAST(s.capacity_orders_per_day AS DOUBLE)
                        AS capacity_orders_per_day,
                    COUNT(*) AS daily_orders
                FROM etl_fact_orders f
                JOIN etl_dim_store s USING (store_id)
                GROUP BY
                    f.order_date,
                    f.store_id,
                    s.store_name,
                    s.capacity_orders_per_day
            )
            SELECT
                order_date,
                store_id,
                store_name,
                daily_orders,
                capacity_orders_per_day,
                ROUND(
                    100.0 * daily_orders
                    / NULLIF(capacity_orders_per_day, 0),
                    2
                ) AS capacity_utilization_pct,
                daily_orders > capacity_orders_per_day AS capacity_breached
            FROM daily
            ORDER BY order_date, store_id
        """)

        daily = con.execute("""
            SELECT * FROM etl_daily_zone_revenue
            ORDER BY order_date, zone, store_id LIMIT 20
        """).fetchdf()

        categories = con.execute("""
            SELECT * FROM etl_category_margin ORDER BY category
        """).fetchdf()

        capacity = con.execute("""
            SELECT * FROM etl_daily_store_capacity
            ORDER BY order_date, store_id LIMIT 20
        """).fetchdf()

        counts = con.execute("""
            SELECT 'etl_fact_orders' AS table_name, COUNT(*) AS row_count
                FROM etl_fact_orders
            UNION ALL
            SELECT 'etl_payment_reconciliation', COUNT(*)
                FROM etl_payment_reconciliation
            UNION ALL
            SELECT 'etl_daily_zone_revenue', COUNT(*)
                FROM etl_daily_zone_revenue
            UNION ALL
            SELECT 'etl_category_margin', COUNT(*)
                FROM etl_category_margin
            UNION ALL
            SELECT 'etl_daily_store_capacity', COUNT(*)
                FROM etl_daily_store_capacity
        """).fetchdf()

    return {
        "table_counts": counts.to_dict(orient="records"),
        "daily_zone_revenue_sample": daily.to_dict(orient="records"),
        "category_margin": categories.to_dict(orient="records"),
        "store_capacity_sample": capacity.to_dict(orient="records"),
    }

# def build_etl_reports() -> dict[str, list[dict[str, Any]]]:
#     """Build the required ETL reporting tables and return loggable summaries."""
#     with duckdb.connect(str(WAREHOUSE_PATH)) as con:
#         con.execute("""
#             CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
#             SELECT
#                 f.order_date,
#                 s.zone,
#                 f.store_id,
#                 s.store_name,
#                 COUNT(*) AS delivered_orders,
#                 ROUND(SUM(f.net_revenue), 2) AS net_revenue
#             FROM etl_fact_orders f
#             JOIN etl_dim_store s USING (store_id)
#             WHERE UPPER(f.order_status) = 'DELIVERED'
#             GROUP BY f.order_date, s.zone, f.store_id, s.store_name
#             ORDER BY f.order_date, s.zone, f.store_id
#         """)
#         con.execute("""
#             CREATE OR REPLACE TABLE etl_category_margin AS
#             SELECT
#                 p.category,
#                 ROUND(SUM(f.net_revenue), 2) AS revenue,
#                 ROUND(SUM(f.quantity * p.cost_price), 2) AS cost_of_goods,
#                 ROUND(SUM(f.net_revenue - f.quantity * p.cost_price), 2) AS gross_margin
#             FROM etl_fact_orders f
#             JOIN etl_dim_product p USING (product_id)
#             WHERE UPPER(f.order_status) = 'DELIVERED'
#             GROUP BY p.category
#             ORDER BY p.category
#         """)
#         daily = con.execute("SELECT * FROM etl_daily_zone_revenue ORDER BY order_date, zone, store_id LIMIT 20").fetchdf()
#         categories = con.execute("SELECT * FROM etl_category_margin ORDER BY category").fetchdf()
#         counts = con.execute("""
#             SELECT 'etl_fact_orders' AS table_name, COUNT(*) AS row_count FROM etl_fact_orders
#             UNION ALL SELECT 'etl_payment_reconciliation', COUNT(*) FROM etl_payment_reconciliation
#             UNION ALL SELECT 'etl_daily_zone_revenue', COUNT(*) FROM etl_daily_zone_revenue
#             UNION ALL SELECT 'etl_category_margin', COUNT(*) FROM etl_category_margin
#         """).fetchdf()
#     return {
#         "table_counts": counts.to_dict(orient="records"),
#         "daily_zone_revenue_sample": daily.to_dict(orient="records"),
#         "category_margin": categories.to_dict(orient="records"),
#     }
