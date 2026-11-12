"""Run-level quality checks for the NammaMart DuckDB ELT pipeline."""

import duckdb

from include.nammamart.audit import persist_dq_results
from include.nammamart.dq_results import make_dq_result
from include.nammamart.elt import DB_PATH

DAG_ID = "nammamart_elt_pipeline"


def build_elt_dq_results(run_id: str, counts: dict) -> list[dict]:
    """Evaluate completeness, uniqueness, validity, accuracy,
    consistency, and timeliness on the final curated staging tables.
    """

    results = []

    datasets = {
        "customers": ("stg_customers", "customer_id"),
        "products": ("stg_products", "product_id"),
        "stores": ("stg_stores", "store_id"),
        "orders": ("stg_orders", "order_id"),
        "payments": ("stg_payments", "payment_id"),
    }

    with duckdb.connect(str(DB_PATH), read_only=True) as con:

        def record_query(check_name, dimension, severity, sql):
            checked, failed = con.execute(sql).fetchone()
            results.append(make_dq_result(
                run_id=run_id,
                dag_id=DAG_ID,
                check_name=check_name,
                dimension=dimension,
                severity=severity,
                rows_checked=int(checked or 0),
                rows_failed=int(failed or 0),
            ))

        # Completeness and uniqueness across all curated business keys.
        missing_keys = 0
        duplicate_keys = 0
        total_curated = 0
        invalid_curated = 0

        for table, key in datasets.values():
            checked, missing, duplicates, invalid = con.execute(f"""
                SELECT
                    COUNT(*),
                    COUNT(*) FILTER (
                        WHERE {key} IS NULL OR TRIM(CAST({key} AS VARCHAR)) = ''
                    ),
                    COUNT(*) - COUNT(DISTINCT {key})
                        - COUNT(*) FILTER (WHERE {key} IS NULL),
                    COUNT(*) FILTER (
                        WHERE UPPER(COALESCE(dq_status, '')) <> 'PASS'
                           OR NULLIF(TRIM(COALESCE(failure_reason, '')), '') IS NOT NULL
                    )
                FROM {table}
            """).fetchone()

            total_curated += int(checked or 0)
            missing_keys += int(missing or 0)
            duplicate_keys += int(duplicates or 0)
            invalid_curated += int(invalid or 0)

        results.append(make_dq_result(
            run_id=run_id, dag_id=DAG_ID,
            check_name="curated_business_key_completeness",
            dimension="Completeness", severity="Critical",
            rows_checked=total_curated, rows_failed=missing_keys,
        ))
        results.append(make_dq_result(
            run_id=run_id, dag_id=DAG_ID,
            check_name="curated_business_key_uniqueness",
            dimension="Uniqueness", severity="Critical",
            rows_checked=total_curated, rows_failed=duplicate_keys,
        ))
        results.append(make_dq_result(
            run_id=run_id, dag_id=DAG_ID,
            check_name="no_invalid_rows_in_curated_staging",
            dimension="Validity", severity="Critical",
            rows_checked=total_curated, rows_failed=invalid_curated,
        ))

        # Customer checks. Email is already masked; do not validate its
        # original syntax here. Null email values are logged as warnings.
        # record_query(
        #     "customer_email_completeness", "Completeness", "Warning",
        #     """
        #     SELECT COUNT(*),
        #            COUNT(*) FILTER (
        #                WHERE email IS NULL OR TRIM(email) = ''
        #            )
        #     FROM stg_customers
        #     """,
        # )
        record_query(
            "customer_pincode_format", "Validity", "Warning",
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (
                       WHERE NOT REGEXP_FULL_MATCH(
                           COALESCE(TRIM(pincode), ''), '[0-9]{6}'
                       )
                   )
            FROM stg_customers
            """,
        )
        record_query(
            "customer_loyalty_tier_casing", "Consistency", "Warning",
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (
                       WHERE loyalty_tier IS NOT NULL
                         AND loyalty_tier <> UPPER(TRIM(loyalty_tier))
                   )
            FROM stg_customers
            """,
        )
        record_query(
            "future_customer_signup_date", "Timeliness", "Critical",
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE signup_date > CURRENT_DATE)
            FROM stg_customers
            """,
        )

        # Product pricing and store capacity checks.
        record_query(
            "product_pricing_accuracy", "Accuracy", "Critical",
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (
                       WHERE mrp IS NULL OR mrp <= 0
                          OR cost_price IS NULL OR cost_price < 0
                          OR cost_price > mrp
                   )
            FROM stg_products
            """,
        )
        record_query(
            "store_capacity_validity", "Validity", "Critical",
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (
                       WHERE capacity_orders_per_day IS NULL
                          OR capacity_orders_per_day <= 0
                   )
            FROM stg_stores
            """,
        )

        # Order validity, accuracy, delivery, and timeliness.
        record_query(
            "order_timestamp_validity", "Validity", "Critical",
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE order_ts IS NULL)
            FROM stg_orders
            """,
        )
        record_query(
            "future_order_timestamp", "Timeliness", "Critical",
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE order_ts > CURRENT_TIMESTAMP)
            FROM stg_orders
            """,
        )
        record_query(
            "order_status_and_payment_mode_validity",
            "Validity", "Critical",
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (
                       WHERE order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED')
                          OR payment_mode NOT IN
                             ('UPI', 'Card', 'Cash on Delivery', 'Wallet')
                   )
            FROM stg_orders
            """,
        )
        record_query(
            "order_amount_formula_accuracy", "Accuracy", "Critical",
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (
                       WHERE order_amount IS DISTINCT FROM
                           ROUND(
                               quantity * unit_price
                               * (1 - discount_pct / 100),
                               2
                           )
                   )
            FROM stg_orders
            """,
        )
        record_query(
            "delivered_order_delivery_time_completeness",
            "Completeness", "Critical",
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (
                       WHERE order_status = 'DELIVERED'
                         AND delivery_minutes IS NULL
                   )
            FROM stg_orders
            """,
        )
        record_query(
            "delivery_time_accuracy", "Accuracy", "Critical",
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (
                       WHERE delivery_minutes IS NOT NULL
                         AND (delivery_minutes <= 0 OR delivery_minutes >= 120)
                   )
            FROM stg_orders
            """,
        )
        record_query(
            "order_dimension_referential_integrity",
            "Consistency", "Critical",
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (
                       WHERE c.customer_id IS NULL
                          OR p.product_id IS NULL
                          OR s.store_id IS NULL
                   )
            FROM stg_orders o
            LEFT JOIN stg_customers c USING (customer_id)
            LEFT JOIN stg_products p USING (product_id)
            LEFT JOIN stg_stores s USING (store_id)
            """,
        )

        # Payment checks run on curated payments. Mismatched payments that
        # were quarantined remain visible in the reconciliation mart.
        record_query(
            "payment_timestamp_validity", "Validity", "Critical",
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE payment_ts IS NULL)
            FROM stg_payments
            """,
        )
        record_query(
            "future_payment_timestamp", "Timeliness", "Critical",
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE payment_ts > CURRENT_TIMESTAMP)
            FROM stg_payments
            """,
        )
        record_query(
            "payment_status_and_mode_validity", "Validity", "Critical",
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (
                       WHERE payment_status NOT IN ('SUCCESS', 'FAILED', 'REFUNDED')
                          OR payment_mode NOT IN
                             ('UPI', 'Card', 'Cash on Delivery', 'Wallet')
                   )
            FROM stg_payments
            """,
        )
        record_query(
            "payment_order_referential_integrity",
            "Consistency", "Critical",
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE o.order_id IS NULL)
            FROM stg_payments p
            LEFT JOIN stg_orders o USING (order_id)
            """,
        )
        record_query(
            "successful_payment_amount_reconciliation",
            "Consistency", "Critical",
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (
                       WHERE o.order_id IS NULL
                          OR p.amount_paid IS DISTINCT FROM o.order_amount
                   )
            FROM stg_payments p
            LEFT JOIN stg_orders o USING (order_id)
            WHERE p.payment_status = 'SUCCESS'
            """,
        )

        # A freshness warning should not fail the pipeline.
        record_query(
            "latest_order_freshness_7_days", "Timeliness", "Warning",
            """
            SELECT 1,
                   COUNT(*) FILTER (
                       WHERE latest_order IS NULL
                          OR latest_order < CURRENT_TIMESTAMP - INTERVAL '7 days'
                   )
            FROM (
                SELECT MAX(order_ts) AS latest_order
                FROM stg_orders
            ) latest
            """,
        )

                # Customer checks. Check completeness against raw source data,
        # before masking changes invalid emails to NULL.
        record_query(
            "customer_email_completeness", "Completeness", "Warning",
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (
                       WHERE email IS NULL
                          OR TRIM(email) = ''
                          OR UPPER(TRIM(email)) = 'N/A'
                   )
            FROM raw_customers
            """,
        )

        # Check email format separately. Never return raw email values.
        record_query(
            "customer_email_format", "Validity", "Warning",
            """
            SELECT
                COUNT(*) FILTER (
                    WHERE email IS NOT NULL
                      AND TRIM(email) <> ''
                      AND UPPER(TRIM(email)) <> 'N/A'
                ),
                COUNT(*) FILTER (
                    WHERE email IS NOT NULL
                      AND TRIM(email) <> ''
                      AND UPPER(TRIM(email)) <> 'N/A'
                      AND NOT REGEXP_FULL_MATCH(
                          TRIM(email),
                          '[^@[:space:]]+@[^@[:space:]]+[.][^@[:space:]]+'
                      )
                )
            FROM raw_customers
            """,
        )

    persist_dq_results(results)
    return results
