import pandas as pd

from include.nammamart.reconciliation import (
    get_source_reconciliation_metrics,
)

from include.nammamart.etl import (
    OUTPUT_DIR,
    build_quality_report,
    read_source_csv,
    run_etl,
    transform_customers,
    transform_orders,
    transform_payments,
    transform_products,
    transform_stores,
)


def test_customer_and_product_cleaning():
    customers, customers_q = transform_customers()
    products, products_q = transform_products()

    assert len(customers) == 392
    assert len(customers_q) == 13
    assert customers["customer_id"].is_unique
    assert customers["email"].dropna().astype(str).str.contains(r"\*", regex=True).all()
    assert customers["phone"].dropna().astype(str).str.contains(r"\*", regex=True).all()

    assert len(products) == 143
    assert len(products_q) == 9
    assert products["product_id"].is_unique
    assert "quarantine_reason" in products_q.columns



def test_order_incremental_upsert():
    orders, quarantine = transform_orders()
    incremental = read_source_csv("orders_2026-10-01_increment.csv")

    # Invalid order values are quarantined before curation.
    assert len(orders) == 2545
    assert len(quarantine) == 5
    assert orders["order_id"].is_unique

    expected_reasons = {
        "ORD001812": "invalid_payment_mode",
        "ORD001414": "invalid_payment_mode",
        "ORD000500": "invalid_order_status",
        "ORD001970": "invalid_order_status",
        "ORD001637": "invalid_order_status",
    }
    actual_reasons = quarantine.set_index("order_id")["quarantine_reason"].to_dict()
    assert actual_reasons == expected_reasons

    # Five invalid base orders are excluded; all 150 incremental
    # source records remain represented in the curated output.
    assert (orders["source_file"] == "orders.csv").sum() == 2395
    assert (
        orders["source_file"] == "orders_2026-10-01_increment.csv"
    ).sum() == 150

    # The upsert must preserve every incremental order ID.
    incremental_ids = set(incremental["order_id"].dropna().astype(str))
    curated_ids = set(orders["order_id"].astype(str))
    assert len(incremental_ids) == 160
    assert incremental_ids.issubset(curated_ids)

    # Incremental status wins for overlapping IDs.
    base = read_source_csv("orders.csv")
    overlap = set(base["order_id"].dropna()) & set(
        incremental["order_id"].dropna()
    )
    assert len(overlap) == 10

    final_status = orders.set_index("order_id")["order_status"]
    incremental_status = incremental.set_index("order_id")["order_status"]

    for order_id in overlap:
        if order_id in final_status.index:
            assert final_status.loc[order_id] == incremental_status.loc[order_id]


def test_payment_and_store_transformations():
    orders, _ = transform_orders()
    payments, payments_q = transform_payments(
        set(orders["order_id"].astype(str))
    )
    stores, stores_q = transform_stores()

    assert len(payments) == 2211
    assert len(payments_q) == 13
    assert payments["payment_id"].is_unique

    assert stores["store_id"].is_unique
    assert len(stores) == 14
    assert len(stores_q) == 1
    assert stores_q.iloc[0]["store_id"] == "S12"
    assert stores_q.iloc[0]["quarantine_reason"] == (
        "invalid_capacity_orders_per_day"
    )


def test_complete_etl_outputs_and_quality_report():
    run_etl()
    orders = pd.read_parquet(OUTPUT_DIR / "orders.parquet")
    from include.nammamart.quality import order_accuracy_issues

    assert order_accuracy_issues(orders).empty
    expected_counts = {
    "customers": (392, 13),
    "products": (143, 9),
    "stores": (14, 1),
    "orders": (2177, 373),
    "payments": (1873, 351),
}

    for name, (expected_curated, expected_quarantine) in expected_counts.items():
        curated = pd.read_parquet(OUTPUT_DIR / f"{name}.parquet")
        quarantine = pd.read_parquet(
            OUTPUT_DIR / f"{name}_quarantine.parquet"
        )

        assert len(curated) == expected_curated, name
        assert len(quarantine) == expected_quarantine, name

        key = {
            "customers": "customer_id",
            "products": "product_id",
            "stores": "store_id",
            "orders": "order_id",
            "payments": "payment_id",
        }[name]

        assert curated[key].notna().all(), name
        assert curated[key].is_unique, name

    payment_q = pd.read_parquet(
        OUTPUT_DIR / "payments_quarantine.parquet"
    )
    assert payment_q["quarantine_reason"].notna().all()
    assert set(payment_q["quarantine_reason"]) == {
    "orphan_order_id",
    "payment_amount_mismatch",
    }

    report = build_quality_report()
    assert len(report) == 5
    assert report["business_key_unique"].all()

    # The assignment-required quarantine files are run-scoped and expose a
    # consistent failure_reason column, while generated outputs stay ignored.
    from pathlib import Path
    from include.nammamart.etl import PROJECT_ROOT
    quarantine_files = list((PROJECT_ROOT / "include" / "quarantine" / "gowtham").glob("orders_failed_*.csv"))
    assert quarantine_files
    latest_quarantine = pd.read_csv(sorted(quarantine_files, key=lambda p: p.stat().st_mtime)[-1])
    assert "failure_reason" in latest_quarantine.columns

    for name in [
        "customers",
        "products",
        "stores",
        "orders",
        "payments",
    ]:
        csv_df = pd.read_csv(OUTPUT_DIR / f"{name}.csv", dtype="string")
        parquet_df = pd.read_parquet(OUTPUT_DIR / f"{name}.parquet")
        assert len(csv_df) == len(parquet_df), name


def test_repeated_etl_run_is_idempotent():
    run_etl()
    first_orders = pd.read_parquet(OUTPUT_DIR / "orders.parquet")
    first_payments = pd.read_parquet(OUTPUT_DIR / "payments.parquet")

    run_etl()
    second_orders = pd.read_parquet(OUTPUT_DIR / "orders.parquet")
    second_payments = pd.read_parquet(OUTPUT_DIR / "payments.parquet")

    pd.testing.assert_frame_equal(first_orders, second_orders)
    pd.testing.assert_frame_equal(first_payments, second_payments)


def test_etl_writes_dq_results():
    from include.nammamart.etl import run_etl
    from include.nammamart.dq_results import DQ_COLUMNS

    run_etl()

    dq_path = OUTPUT_DIR / "dq_results.csv"
    assert dq_path.exists()

    dq = pd.read_csv(dq_path)
    assert list(dq.columns) == DQ_COLUMNS
    assert not dq.empty

    assert dq["run_id"].nunique() == 1
    assert set(dq["dag_id"]) == {"nammamart_etl_pipeline"}
    assert set(dq["dimension"]).issubset({
        "Completeness", "Uniqueness", "Validity",
        "Accuracy", "Consistency", "Timeliness",
    })
    assert set(dq["severity"]).issubset({"Critical", "Warning"})
    assert set(dq["status"]).issubset({"PASS", "FAIL"})



def test_order_value_normalization():
    from include.nammamart.etl import normalize_order_values

    df = pd.DataFrame({
        "order_id": ["O1", "O2", "O3", "O4", "O5"],
        "order_status": [
            "delivered", "Pending??", "RETURNED", "DELIVERED", "CANCELLED"
        ],
        "payment_mode": ["upi", "CARD", "cod", "Cashh", "Wallet"],
    })

    normalized, invalid = normalize_order_values(df)
    normalized_by_id = normalized.set_index("order_id")
    invalid_by_id = invalid.set_index("order_id")

    assert normalized_by_id.loc["O1", "order_status"] == "DELIVERED"
    assert normalized_by_id.loc["O1", "payment_mode"] == "UPI"
    assert normalized_by_id.loc["O3", "payment_mode"] == "Cash on Delivery"
    assert normalized_by_id.loc["O5", "payment_mode"] == "Wallet"

    assert set(invalid["order_id"]) == {"O2", "O4"}
    assert invalid_by_id.loc["O2", "order_status"] == "Pending??"
    assert invalid_by_id.loc["O2", "payment_mode"] == "Card"
    assert invalid_by_id.loc["O4", "payment_mode"] == "Cashh"
