from datetime import date

import duckdb
import pytest

from include.nammamart.scd2 import apply_customer_loyalty_scd2


@pytest.fixture
def con():
    connection = duckdb.connect(":memory:")
    connection.execute("""
        CREATE TABLE customers_curated (
            customer_id VARCHAR,
            loyalty_tier VARCHAR
        )
    """)
    yield connection
    connection.close()


def test_initial_load_and_unchanged_rerun(con):
    con.execute("INSERT INTO customers_curated VALUES ('C1', 'Silver')")

    first = apply_customer_loyalty_scd2(
        con, "customers_curated", date(2026, 10, 14)
    )
    second = apply_customer_loyalty_scd2(
        con, "customers_curated", date(2026, 10, 15)
    )

    assert first == {
        "history_rows": 1,
        "current_customers": 1,
        "historical_versions": 0,
    }
    assert second == first


def test_tier_change_preserves_history(con):
    con.execute("INSERT INTO customers_curated VALUES ('C1', 'Silver')")
    apply_customer_loyalty_scd2(
        con, "customers_curated", date(2026, 10, 14)
    )

    con.execute("UPDATE customers_curated SET loyalty_tier = 'Gold'")
    result = apply_customer_loyalty_scd2(
        con, "customers_curated", date(2026, 10, 16)
    )

    assert result == {
        "history_rows": 2,
        "current_customers": 1,
        "historical_versions": 1,
    }

    rows = con.execute("""
        SELECT loyalty_tier, effective_from, effective_to, is_current
        FROM dim_customer_scd2
        WHERE customer_id = 'C1'
        ORDER BY effective_from
    """).fetchall()

    assert rows == [
        ("SILVER", date(2026, 10, 14), date(2026, 10, 15), False),
        ("GOLD", date(2026, 10, 16), None, True),
    ]


def test_same_day_change_updates_current_version(con):
    con.execute("INSERT INTO customers_curated VALUES ('C1', 'Silver')")
    apply_customer_loyalty_scd2(
        con, "customers_curated", date(2026, 10, 14)
    )

    con.execute("UPDATE customers_curated SET loyalty_tier = 'Gold'")
    result = apply_customer_loyalty_scd2(
        con, "customers_curated", date(2026, 10, 14)
    )

    assert result["history_rows"] == 1
    assert result["current_customers"] == 1
    assert result["historical_versions"] == 0

    row = con.execute("""
        SELECT loyalty_tier, effective_from, effective_to, is_current
        FROM dim_customer_scd2
    """).fetchone()

    assert row == ("GOLD", date(2026, 10, 14), None, True)


def test_rejects_unsupported_source_table(con):
    with pytest.raises(ValueError):
        apply_customer_loyalty_scd2(
            con, "raw_customers", date(2026, 10, 14)
        )
