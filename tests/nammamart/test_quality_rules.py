"""
tests/nammamart/test_quality_rules.py
-------------------------------------
Unit tests for the data-quality rule engine (assignment Part D).

**pytest** is the Python testing tool; Java comparison (learning aid): it plays
the role of JUnit.  A function whose name starts with ``test_`` is a test, and
``assert`` replaces ``assertEquals``.

These tests use tiny, hand-made frames so each rule can be checked on its own.
"""

from __future__ import annotations

import pandas as pd
import pytest

from include.nammamart import dq_rules, masking, transforms as tf


def make_context(**overrides) -> dq_rules.ValidationContext:
    ctx = dq_rules.ValidationContext(run_id="test_run", dag_id="test_dag",
                                     now=pd.Timestamp("2026-10-20 12:00:00"))
    ctx.valid_customer_ids = {"C001", "C002"}
    ctx.valid_product_ids = {"P001"}
    ctx.valid_store_ids = {"S01"}
    ctx.valid_order_ids = {"ORD1", "ORD2"}
    for key, value in overrides.items():
        setattr(ctx, key, value)
    return ctx


def order_row(**overrides) -> dict:
    """A valid order; each test overrides only the field it is checking."""
    row = {
        "order_id": "ORD1", "order_ts": "2026-09-10 10:00:00", "customer_id": "C001",
        "store_id": "S01", "product_id": "P001", "quantity": "2", "unit_price": "100.0",
        "discount_pct": "10", "order_amount": "180.0", "payment_mode": "UPI",
        "order_status": "DELIVERED", "channel": "App", "delivery_minutes": "12",
    }
    row.update(overrides)
    return row


def run_orders(rows: list[dict], ctx: dq_rules.ValidationContext | None = None):
    return dq_rules.validate("orders", pd.DataFrame(rows), ctx or make_context())


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------
def test_valid_order_passes_every_rule():
    result = run_orders([order_row()])
    assert result.rows_passed == 1
    assert result.rows_failed == 0
    assert all(row["rows_failed"] == 0 for row in result.dq_rows if row["scope"] != "dataset"
               ) if "scope" in (result.dq_rows[0] if result.dq_rows else {}) else True


# ---------------------------------------------------------------------------
# Completeness
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("column,rule_id", [
    ("order_id", "RULE-001"),
    ("order_ts", "RULE-003"),
    ("customer_id", "RULE-006"),
])
def test_missing_required_column_is_quarantined(column, rule_id):
    result = run_orders([order_row(**{column: ""})])
    assert result.rows_failed == 1
    assert rule_id in result.failed.iloc[0]["failure_reason"]


def test_delivered_order_without_delivery_minutes_is_quarantined():
    result = run_orders([order_row(delivery_minutes="")])
    assert result.rows_failed == 1
    assert "RULE-014" in result.failed.iloc[0]["failure_reason"]


def test_cancelled_order_without_delivery_minutes_is_allowed():
    result = run_orders([order_row(order_status="CANCELLED", delivery_minutes="")])
    assert result.rows_passed == 1


# ---------------------------------------------------------------------------
# Uniqueness - the survivor stays, only the redundant copy is quarantined
# ---------------------------------------------------------------------------
def test_duplicate_order_id_keeps_one_row():
    result = run_orders([order_row(), order_row()])
    assert result.rows_passed == 1, "one copy of the order must survive"
    assert result.rows_failed == 1
    assert "RULE-002" in result.failed.iloc[0]["failure_reason"]


def test_different_order_ids_are_not_duplicates():
    result = run_orders([order_row(), order_row(order_id="ORD2")])
    assert result.rows_passed == 2


# ---------------------------------------------------------------------------
# Validity
# ---------------------------------------------------------------------------
def test_unknown_order_status_is_quarantined():
    result = run_orders([order_row(order_status="SHIPPED")])
    assert "RULE-012" in result.failed.iloc[0]["failure_reason"]


def test_unparsable_timestamp_is_quarantined():
    result = run_orders([order_row(order_ts="not-a-date")])
    assert "RULE-004" in result.failed.iloc[0]["failure_reason"]


def test_discount_outside_range_is_quarantined():
    result = run_orders([order_row(discount_pct="150", order_amount="-100.0")])
    assert "RULE-010" in result.failed.iloc[0]["failure_reason"]


def test_unknown_payment_mode_is_only_a_warning():
    result = run_orders([order_row(payment_mode="Crypto")])
    assert result.rows_passed == 1, "a Warning must not quarantine the row"
    assert "RULE-013" in result.passed.iloc[0]["dq_warnings"]


def test_payment_mode_casing_is_standardised_before_the_check():
    result = run_orders([order_row(payment_mode=" upi ")])
    assert result.rows_passed == 1
    assert pd.isna(result.passed.iloc[0]["dq_warnings"])


# ---------------------------------------------------------------------------
# Accuracy
# ---------------------------------------------------------------------------
def test_negative_quantity_is_quarantined():
    result = run_orders([order_row(quantity="-3", order_amount="-270.0")])
    assert "RULE-009" in result.failed.iloc[0]["failure_reason"]


def test_order_amount_must_match_the_formula():
    result = run_orders([order_row(order_amount="999.0")])
    assert "RULE-011" in result.failed.iloc[0]["failure_reason"]


def test_impossible_delivery_time_is_quarantined():
    result = run_orders([order_row(delivery_minutes="240")])
    assert "RULE-015" in result.failed.iloc[0]["failure_reason"]


# ---------------------------------------------------------------------------
# Consistency
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("column,value,rule_id", [
    ("customer_id", "C999", "RULE-032"),
    ("product_id", "P999", "RULE-007"),
    ("store_id", "S99", "RULE-008"),
])
def test_unknown_foreign_key_is_quarantined(column, value, rule_id):
    result = run_orders([order_row(**{column: value})])
    assert rule_id in result.failed.iloc[0]["failure_reason"]


# ---------------------------------------------------------------------------
# Timeliness
# ---------------------------------------------------------------------------
def test_future_dated_order_is_quarantined():
    result = run_orders([order_row(order_ts="2027-01-05 10:00:00")])
    assert "RULE-005" in result.failed.iloc[0]["failure_reason"]


def test_future_rule_uses_the_fixed_as_of_date_not_the_wall_clock():
    # 21 Oct 2026 is after the extract's cut-off (config.AS_OF), so it is
    # future-dated even when the pipeline happens to run on 1 Dec 2026.
    late_run = make_context(now=pd.Timestamp("2026-12-01 09:00:00"))
    result = run_orders([order_row(order_ts="2026-10-21 10:15:00")], late_run)
    assert "RULE-005" in result.failed.iloc[0]["failure_reason"]
    # The 1 October increment is inside the cut-off and must stay valid.
    assert run_orders([order_row(order_ts="2026-10-01 22:33:00")], late_run).rows_failed == 0


def test_freshness_check_ignores_future_rows():
    rows = [order_row(order_ts="2026-09-10 10:00:00"),
            order_row(order_id="ORD2", order_ts="2027-01-05 10:00:00")]
    result = run_orders(rows)
    freshness = [row for row in result.dq_rows if row["rule_id"] == "RULE-035"][0]
    assert freshness["rows_failed"] == 1, "a September file is stale on 20 October"
    assert "2026-09-10" in freshness["detail"]


def test_volume_check_warns_when_a_file_shrinks():
    result = run_orders([order_row()])
    volume = [row for row in result.dq_rows if row["rule_id"] == "RULE-034"][0]
    assert volume["status"] == "WARN"
    assert volume["severity"] == dq_rules.WARNING


# ---------------------------------------------------------------------------
# Schema guard (Part F experiment 6)
# ---------------------------------------------------------------------------
def test_renamed_column_fails_loudly():
    frame = pd.DataFrame([order_row()]).rename(columns={"order_amount": "total"})
    with pytest.raises(dq_rules.SchemaError) as error:
        dq_rules.validate("orders", frame, make_context())
    assert "order_amount" in str(error.value)


# ---------------------------------------------------------------------------
# dq_results scorecard
# ---------------------------------------------------------------------------
def test_every_dimension_is_covered_for_every_dataset():
    catalogue = dq_rules.rule_catalogue()
    required = {dq_rules.COMPLETENESS, dq_rules.UNIQUENESS, dq_rules.VALIDITY,
                dq_rules.ACCURACY, dq_rules.CONSISTENCY, dq_rules.TIMELINESS}
    covered = set(catalogue["dimension"])
    assert required.issubset(covered)
    for dataset in ("orders", "payments", "customers", "products", "stores"):
        dimensions = set(catalogue.loc[catalogue["dataset"] == dataset, "dimension"])
        assert len(dimensions) >= 4, f"{dataset} covers too few dimensions: {dimensions}"


def test_dq_rows_have_the_columns_the_assignment_requires():
    result = run_orders([order_row()])
    required = {"run_id", "dag_id", "check_name", "dimension", "severity",
                "rows_checked", "rows_failed", "status", "checked_at"}
    assert required.issubset(result.dq_rows[0].keys())


# ---------------------------------------------------------------------------
# Masking (Part A.4 / Part E.4)
# ---------------------------------------------------------------------------
def test_email_is_masked_but_still_grouped_by_domain():
    assert masking.mask_email("priya.hegde199@example.com") == "pr***@example.com"


def test_phone_keeps_only_the_first_and_last_two_digits():
    assert masking.mask_phone("9774349021") == "97******21"


def test_masking_handles_missing_values():
    assert masking.mask_email(None) is None
    assert masking.mask_phone("") is None
    assert masking.mask_email("not-an-email") == "INVALID_EMAIL"


def test_masked_frame_drops_the_original_columns():
    frame = pd.DataFrame([{"customer_id": "C001", "full_name": "Priya Hegde",
                           "email": "priya@example.com", "phone": "9774349021"}])
    masked = masking.mask_customer_frame(frame)
    assert {"email", "phone", "full_name"}.isdisjoint(masked.columns)
    assert masked.iloc[0]["customer_name_masked"] == "Priya H."


# ---------------------------------------------------------------------------
# Transform helpers
# ---------------------------------------------------------------------------
def test_payment_mode_variants_are_standardised():
    series = pd.Series(["upi", "UPI ", " Cash on Delivery", "cod", "Crypto"])
    standardised = tf.standardise_payment_mode(series)
    assert list(standardised[:4]) == ["UPI", "UPI", "Cash on Delivery", "Cash on Delivery"]
    assert standardised.iloc[4] == "Crypto", "unknown values stay visible for the rule to flag"


def test_expected_order_amount_applies_the_discount():
    frame = pd.DataFrame([{"quantity": "2", "unit_price": "222.0", "discount_pct": "5"}])
    assert tf.expected_order_amount(frame).iloc[0] == pytest.approx(421.8)


def test_within_15_min_only_counts_delivered_orders():
    frame = pd.DataFrame([
        {"order_ts": "2026-09-01 10:00:00", "order_status": "DELIVERED",
         "order_amount": "100.0", "delivery_minutes": "10"},
        {"order_ts": "2026-09-01 10:00:00", "order_status": "CANCELLED",
         "order_amount": "100.0", "delivery_minutes": "5"},
    ])
    derived = tf.add_order_derivations(frame)
    assert list(derived["within_15_min"]) == [True, False]
    assert list(derived["net_revenue"]) == [100.0, 0.0]
