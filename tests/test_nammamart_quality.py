import pandas as pd

from include.nammamart.quality import (
    duplicate_key_rows,
    missing_key_rows,
    orphan_rows,
    exact_duplicate_rows,
)


def test_duplicate_key_rows():
    df = pd.DataFrame({
        "id": ["A", "A", "B", None, ""],
        "value": [1, 2, 3, 4, 5],
    })

    result = duplicate_key_rows(df, "id")

    assert len(result) == 2
    assert set(result["value"]) == {1, 2}


def test_missing_key_rows():
    df = pd.DataFrame({
        "id": ["A", None, "", "  ", "B"],
    })

    result = missing_key_rows(df, "id")

    assert len(result) == 3


def test_orphan_rows():
    child = pd.DataFrame({
        "customer_id": ["C1", "C2", None, ""],
    })
    parent = pd.DataFrame({
        "customer_id": ["C1"],
    })

    result = orphan_rows(child, "customer_id", parent, "customer_id")

    assert result["customer_id"].tolist() == ["C2"]


def test_exact_duplicate_rows():
    df = pd.DataFrame({
        "id": ["A", "A", "B"],
        "value": [1, 1, 2],
    })

    result = exact_duplicate_rows(df)

    assert len(result) == 1
    assert result.iloc[0]["id"] == "A"
def test_order_accuracy_check():
    from include.nammamart.quality import order_accuracy_issues

    df = pd.DataFrame({
        "order_id": ["OK", "BAD_QTY", "BAD_DISCOUNT", "BAD_AMOUNT"],
        "quantity": [2, 0, 1, 2],
        "unit_price": [100, 100, 100, 100],
        "discount_pct": [10, 10, 150, 10],
        "order_amount": [180, 0, -50, 175],
    })

    issues = order_accuracy_issues(df)

    assert set(issues["row_key"]) == {"BAD_QTY", "BAD_DISCOUNT", "BAD_AMOUNT"}
    assert {"Accuracy", "Validity"}.issubset(set(issues["quality_dimension"]))


def test_success_payment_requires_exact_amount():
    from include.nammamart.quality import payment_reconciliation_issues

    payments = pd.DataFrame({
        "payment_id": ["MATCH", "MISMATCH", "FAILED"],
        "order_id": ["O1", "O2", "O3"],
        "payment_status": ["SUCCESS", "SUCCESS", "FAILED"],
        "amount_paid": ["100.00", "99.99", "10.00"],
    })
    orders = pd.DataFrame({
        "order_id": ["O1", "O2", "O3"],
        "order_amount": ["100.00", "100.00", "10.00"],
    })

    issues = payment_reconciliation_issues(payments, orders)

    assert issues["row_key"].tolist() == ["MISMATCH"]
    assert issues.iloc[0]["severity"] == "Critical"


def test_product_pricing_rules():
    from include.nammamart.quality import product_pricing_issues

    products = pd.DataFrame({
        "product_id": ["OK", "BAD_MRP", "COST_HIGH"],
        "mrp": [100, 0, 50],
        "cost_price": [60, 0, 70],
    })

    issues = product_pricing_issues(products)

    assert set(issues["row_key"]) == {"BAD_MRP", "COST_HIGH"}
    assert (issues["quality_dimension"] == "Accuracy").all()


def test_dq_result_schema_and_pass_status():
    from include.nammamart.dq_results import DQ_COLUMNS, make_dq_result

    result = make_dq_result(
        run_id="run-001",
        dag_id="nammamart_etl",
        check_name="order_amount_formula",
        dimension="Accuracy",
        severity="Critical",
        rows_checked=100,
        rows_failed=0,
    )

    assert list(result) == DQ_COLUMNS
    assert result["status"] == "PASS"
    assert result["rows_checked"] == 100
    assert result["rows_failed"] == 0


def test_dq_result_failure_status():
    from include.nammamart.dq_results import make_dq_result

    result = make_dq_result(
        run_id="run-002",
        dag_id="nammamart_elt",
        check_name="payment_reconciliation",
        dimension="Consistency",
        severity="Critical",
        rows_checked=100,
        rows_failed=3,
    )

    assert result["status"] == "FAIL"
    assert result["rows_failed"] == 3


def test_dq_result_rejects_invalid_counts():
    import pytest
    from include.nammamart.dq_results import make_dq_result

    with pytest.raises(ValueError):
        make_dq_result(
            run_id="run-003",
            dag_id="nammamart_etl",
            check_name="business_key",
            dimension="Uniqueness",
            severity="Critical",
            rows_checked=10,
            rows_failed=11,
        )


def test_payment_reconciliation_distinguishes_orphans_from_amount_mismatches():
    from include.nammamart.quality import payment_reconciliation_issues

    payments = pd.DataFrame({
        "payment_id": ["P1", "P2", "P3"],
        "order_id": ["O1", "O2", "O404"],
        "payment_status": ["SUCCESS", "SUCCESS", "SUCCESS"],
        "amount_paid": [100.00, 99.00, 50.00],
    })
    orders = pd.DataFrame({
        "order_id": ["O1", "O2"],
        "order_amount": [100.00, 100.00],
    })

    issues = payment_reconciliation_issues(payments, orders)

    assert len(issues) == 2
    assert issues.loc[
        issues["row_key"] == "P2", "problem"
    ].iloc[0].startswith("Successful payment amount")
    assert issues.loc[
        issues["row_key"] == "P3", "problem"
    ].iloc[0] == "Successful payment has no matching order amount"


def test_customer_validity_issues():
    from include.nammamart.quality import customer_validity_issues

    df = pd.DataFrame({
        "customer_id": ["C1", "C2", "C3"],
        "email": [
            "valid@example.com",
            "invalid-email",
            "another@example.com",
        ],
        "phone": [
            "9876543210",
            "12345",
            "9876543211",
        ],
        "pincode": [
            "560001",
            "12345",
            "600001",
        ],
        "loyalty_tier": [
            "Gold",
            "silver",
            "Platinum",
        ],
    })

    issues = customer_validity_issues(df)

    assert len(issues) == 4
    assert set(issues["row_key"]) == {"C2"}
    assert set(issues["column"]) == {
        "email", "phone", "pincode", "loyalty_tier"
    }
    assert set(issues["severity"]) == {"Warning"}
    assert set(issues["quality_dimension"]) == {
        "Validity", "Consistency"
    }


def test_order_delivery_issues():
    from include.nammamart.quality import order_delivery_issues

    df = pd.DataFrame({
        "order_id": ["O1", "O2", "O3", "O4"],
        "order_status": ["DELIVERED", "DELIVERED", "CANCELLED", "RETURNED"],
        "delivery_minutes": ["25", None, "1440", "0"],
    })

    issues = order_delivery_issues(df)

    assert len(issues) == 3
    assert set(issues["row_key"]) == {"O2", "O3", "O4"}
    assert set(issues["severity"]) == {"Critical"}


def test_timestamp_parsing_issues():
    from include.nammamart.quality import timestamp_parsing_issues

    df = pd.DataFrame({
        "order_id": ["O1", "O2", "O3"],
        "order_ts": [
            "2026-10-01 10:00:00",
            "not-a-date",
            None,
        ],
    })

    issues = timestamp_parsing_issues(df, "order_ts", "order_id")

    assert len(issues) == 2
    assert set(issues["row_key"]) == {"O2", "O3"}
    assert set(issues["quality_dimension"]) == {"Validity"}
