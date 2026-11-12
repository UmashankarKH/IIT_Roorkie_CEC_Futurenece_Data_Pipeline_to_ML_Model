"""Tests for the required DuckDB ETL star schema and reconciliation outputs."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest


duckdb = pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")

import include.nammamart.etl_warehouse as warehouse


def _write_parquet_frames(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{
        "customer_id": "C1", "full_name": "Example Customer",
        "email": "person@example.com", "phone": "9876543210",
        "city": "Bengaluru", "area": "North", "pincode": "560001",
        "signup_date": "2026-01-01", "loyalty_tier": "Gold", "is_active": "true",
    }]).to_parquet(output_dir / "customers.parquet", index=False)
    pd.DataFrame([{
        "product_id": "P1", "product_name": "Rice", "category": "Staples",
        "sub_category": "Rice", "brand": "Brand", "mrp": "100",
        "cost_price": "60", "unit": "kg", "is_perishable": "false",
    }]).to_parquet(output_dir / "products.parquet", index=False)
    pd.DataFrame([{
        "store_id": "S1", "store_name": "North Store", "zone": "North",
        "area": "North", "pincode": "560001", "open_date": "2024-01-01",
        "manager_name": "Manager", "capacity_orders_per_day": "500",
    }]).to_parquet(output_dir / "stores.parquet", index=False)
    pd.DataFrame([{
        "order_id": "O1", "order_ts": "2026-09-01 10:00:00", "customer_id": "C1",
        "store_id": "S1", "product_id": "P1", "quantity": "2", "unit_price": "50",
        "discount_pct": "10", "order_amount": "90", "payment_mode": "UPI",
        "order_status": "DELIVERED", "channel": "APP", "delivery_minutes": "12",
        "source_file": "orders.csv",
    }]).to_parquet(output_dir / "orders.parquet", index=False)


def test_star_schema_load_is_idempotent_and_masks_customer_contact(tmp_path, monkeypatch):
    output_dir = tmp_path / "output"
    warehouse_path = tmp_path / "warehouse.duckdb"
    _write_parquet_frames(output_dir)
    monkeypatch.setattr(warehouse, "OUTPUT_DIR", output_dir)
    monkeypatch.setattr(warehouse, "WAREHOUSE_PATH", warehouse_path)

    first = warehouse.load_etl_star_schema("run-1")
    second = warehouse.load_etl_star_schema("run-2")

    assert first == second
    assert first["etl_fact_orders"] == 1
    with duckdb.connect(str(warehouse_path), read_only=True) as con:
        customer = con.execute(
            "SELECT email, phone FROM etl_dim_customer WHERE customer_id = 'C1'"
        ).fetchone()
        fact = con.execute(
            "SELECT order_date, net_revenue, gross_margin, within_15_min "
            "FROM etl_fact_orders WHERE order_id = 'O1'"
        ).fetchone()
        assert con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0] == 1

    assert customer[0] == "pe***@example.com"
    assert customer[1] == "98******10"
    assert str(fact[0]) == "2026-09-01"
    assert float(fact[1]) == 90.0
    assert float(fact[2]) == -30.0  # 90 revenue minus 2 * 60 cost
    assert fact[3] is True


def test_payment_reconciliation_captures_mismatch_and_orphan(tmp_path, monkeypatch):
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    warehouse_path = tmp_path / "warehouse.duckdb"
    monkeypatch.setattr(warehouse, "OUTPUT_DIR", output_dir)
    monkeypatch.setattr(warehouse, "WAREHOUSE_PATH", warehouse_path)

    pd.DataFrame([
        {"order_id": "O1", "order_amount": "100", "order_status": "DELIVERED"},
        {"order_id": "O2", "order_amount": "40", "order_status": "DELIVERED"},
    ]).to_parquet(output_dir / "orders.parquet", index=False)
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    pd.DataFrame([
        {"payment_id": "P1", "order_id": "O1", "amount_paid": "90", "payment_status": "SUCCESS"},
        {"payment_id": "P2", "order_id": "ORPHAN", "amount_paid": "25", "payment_status": "SUCCESS"},
    ]).to_csv(source_dir / "payments.csv", index=False)

    count = warehouse.build_etl_payment_reconciliation(source_dir, "run-1")
    assert count == 3
    with duckdb.connect(str(warehouse_path), read_only=True) as con:
        reasons = set(
            con.execute("SELECT mismatch_reason FROM etl_payment_reconciliation").fetchdf()["mismatch_reason"]
        )
    assert reasons == {"payment_amount_mismatch", "no_successful_payment", "orphan_payment_order"}
