"""
include/nammamart/dq_rules.py
-----------------------------
Data-quality rule engine for the NammaMart pipelines (assignment Part D).

Vocabulary
----------
* **Data quality rule** - one testable statement about the data, for example
  "``order_id`` must be unique".
* **Quality dimension** - the family a rule belongs to: Completeness,
  Uniqueness, Validity, Accuracy, Consistency, Timeliness.
* **Severity** - ``Critical`` rows are quarantined (never reach the fact table),
  ``Warning`` rows still load but are logged in ``dq_results``.
* **Quarantine** - a separate CSV file / table holding rejected rows together
  with a ``failure_reason``.  Java comparison (learning aid): a dead-letter
  queue in a message-driven application.

Each rule keeps the ``RULE-0xx`` identifier produced during Part 0 profiling so
a finding in the issues log can be traced to the code that enforces it.

A rule is a small function that receives the DataFrame plus a
:class:`ValidationContext` and returns a boolean Series that is ``True`` for the
rows that **fail**.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Iterable, Sequence

import pandas as pd

from . import config, masking, transforms as tf

logger = logging.getLogger(__name__)

CRITICAL = "Critical"
WARNING = "Warning"

COMPLETENESS = "Completeness"
UNIQUENESS = "Uniqueness"
VALIDITY = "Validity"
ACCURACY = "Accuracy"
CONSISTENCY = "Consistency"
TIMELINESS = "Timeliness"


# ---------------------------------------------------------------------------
# Context passed to every rule
# ---------------------------------------------------------------------------
@dataclass
class ValidationContext:
    """Reference data a rule may need, for example the valid product keys."""

    run_id: str
    dag_id: str
    now: pd.Timestamp = field(default_factory=lambda: pd.Timestamp.now())
    # Keys present in the *source* master files. Foreign-key rules check against
    # these, so an order is only rejected when its key genuinely does not exist,
    # not because the master row failed an unrelated quality rule.
    valid_customer_ids: set[str] = field(default_factory=set)
    valid_product_ids: set[str] = field(default_factory=set)
    valid_store_ids: set[str] = field(default_factory=set)
    valid_order_ids: set[str] = field(default_factory=set)
    order_amounts: dict[str, float] = field(default_factory=dict)
    expected_counts: dict[str, int] = field(default_factory=lambda: dict(config.EXPECTED_ROW_COUNTS))


# ---------------------------------------------------------------------------
# Rule definitions
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class RowRule:
    """A rule evaluated row by row."""

    rule_id: str
    dataset: str
    check_name: str
    dimension: str
    severity: str
    predicate: Callable[[pd.DataFrame, ValidationContext], pd.Series]

    def failing_mask(self, df: pd.DataFrame, ctx: ValidationContext) -> pd.Series:
        mask = self.predicate(df, ctx)
        return pd.Series(mask, index=df.index).fillna(False).astype(bool)

    @property
    def reason(self) -> str:
        return f"{self.rule_id}: {self.check_name} [{self.dimension}/{self.severity}]"


@dataclass(frozen=True)
class DatasetRule:
    """A rule about the file as a whole (row count, freshness)."""

    rule_id: str
    dataset: str
    check_name: str
    dimension: str
    severity: str
    evaluate: Callable[[pd.DataFrame, ValidationContext], tuple[int, int, str]]


@dataclass
class ValidationResult:
    """What :func:`validate` gives back for one dataset."""

    dataset: str
    passed: pd.DataFrame
    failed: pd.DataFrame
    dq_rows: list[dict]
    rows_in: int

    @property
    def rows_passed(self) -> int:
        return len(self.passed)

    @property
    def rows_failed(self) -> int:
        return len(self.failed)


# --- small predicate helpers ------------------------------------------------


def _blank(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(True, index=df.index)
    return tf.is_blank(df[column])


def _duplicated(df: pd.DataFrame, column: str, order_by: str | None = None) -> pd.Series:
    """Flag redundant copies of a duplicated key (one survivor is kept)."""
    if column not in df.columns:
        return pd.Series(False, index=df.index)
    return tf.redundant_duplicate_mask(df, column, order_by=order_by)


def _not_in(df: pd.DataFrame, column: str, allowed: Iterable[str], upper: bool = True) -> pd.Series:
    if column not in df.columns:
        return pd.Series(True, index=df.index)
    values = tf.clean_text(df[column])
    if upper:
        values = values.str.upper()
        allowed_set = {a.upper() for a in allowed}
    else:
        allowed_set = set(allowed)
    return values.notna() & ~values.isin(allowed_set)


def _future(df: pd.DataFrame, column: str, ctx: ValidationContext) -> pd.Series:
    if column not in df.columns:
        return pd.Series(False, index=df.index)
    parsed = tf.to_timestamp(df[column])
    return parsed.notna() & (parsed > ctx.now)


def _missing_fk(df: pd.DataFrame, column: str, valid: set[str]) -> pd.Series:
    if column not in df.columns or not valid:
        return pd.Series(False, index=df.index)
    key = tf.clean_text(df[column])
    return key.notna() & ~key.isin(valid)


def _digits_not(df: pd.DataFrame, column: str, count: int) -> pd.Series:
    if column not in df.columns:
        return pd.Series(False, index=df.index)
    value = tf.clean_text(df[column])
    digits = value.str.replace(r"\D", "", regex=True)
    return value.notna() & (digits.str.len() != count)


# ---------------------------------------------------------------------------
# ORDERS
# ---------------------------------------------------------------------------
ORDER_RULES: Sequence[RowRule] = (
    RowRule("RULE-001", "orders", "order_id must be present", COMPLETENESS, CRITICAL,
            lambda df, ctx: _blank(df, "order_id")),
    RowRule("RULE-003", "orders", "order_ts must be present", COMPLETENESS, CRITICAL,
            lambda df, ctx: _blank(df, "order_ts")),
    RowRule("RULE-006", "orders", "customer_id must be present", COMPLETENESS, CRITICAL,
            lambda df, ctx: _blank(df, "customer_id")),
    RowRule("RULE-014", "orders", "DELIVERED orders must have delivery_minutes", COMPLETENESS, CRITICAL,
            lambda df, ctx: (tf.standardise_upper(df["order_status"]) == config.REVENUE_STATUS)
            & tf.to_number(df["delivery_minutes"]).isna()),
    RowRule("RULE-002", "orders", "order_id must be unique (redundant copies quarantined)",
            UNIQUENESS, CRITICAL,
            lambda df, ctx: _duplicated(df, "order_id", order_by="order_ts")),
    RowRule("RULE-004", "orders", "order_ts must parse as a valid timestamp", VALIDITY, CRITICAL,
            lambda df, ctx: tf.clean_text(df["order_ts"]).notna() & tf.to_timestamp(df["order_ts"]).isna()),
    RowRule("RULE-012", "orders", "order_status must be in the approved list", VALIDITY, CRITICAL,
            lambda df, ctx: _not_in(df, "order_status", config.ALLOWED_ORDER_STATUS)),
    RowRule("RULE-010", "orders", "discount_pct must be between 0 and 100", VALIDITY, CRITICAL,
            lambda df, ctx: tf.to_number(df["discount_pct"]).notna()
            & ~tf.to_number(df["discount_pct"]).between(0, 100)),
    RowRule("RULE-013", "orders", "payment_mode must be in the approved list", VALIDITY, WARNING,
            lambda df, ctx: _not_in(
                df.assign(_pm=tf.standardise_payment_mode(df["payment_mode"])),
                "_pm", config.ALLOWED_PAYMENT_MODE)),
    RowRule("RULE-033", "orders", "channel must be in the approved list", VALIDITY, WARNING,
            lambda df, ctx: _not_in(df, "channel", config.ALLOWED_CHANNELS)),
    RowRule("RULE-009", "orders", "quantity must be greater than zero", ACCURACY, CRITICAL,
            lambda df, ctx: tf.to_number(df["quantity"]).isna() | (tf.to_number(df["quantity"]) <= 0)),
    RowRule("RULE-011", "orders", "order_amount must match quantity x unit_price x (1 - discount_pct/100)",
            ACCURACY, CRITICAL,
            lambda df, ctx: (tf.to_number(df["order_amount"]) - tf.expected_order_amount(df)).abs()
            > config.AMOUNT_TOLERANCE),
    RowRule("RULE-015", "orders", f"delivery_minutes must be between 0 and {config.MAX_DELIVERY_MINUTES}",
            ACCURACY, CRITICAL,
            lambda df, ctx: tf.to_number(df["delivery_minutes"]).notna()
            & ~tf.to_number(df["delivery_minutes"]).between(
                config.MIN_DELIVERY_MINUTES, config.MAX_DELIVERY_MINUTES)),
    RowRule("RULE-032", "orders", "customer_id must exist in customers.csv", CONSISTENCY, CRITICAL,
            lambda df, ctx: _missing_fk(df, "customer_id", ctx.valid_customer_ids)),
    RowRule("RULE-007", "orders", "product_id must exist in products.csv", CONSISTENCY, CRITICAL,
            lambda df, ctx: _missing_fk(df, "product_id", ctx.valid_product_ids)),
    RowRule("RULE-008", "orders", "store_id must exist in stores.csv", CONSISTENCY, CRITICAL,
            lambda df, ctx: _missing_fk(df, "store_id", ctx.valid_store_ids)),
    RowRule("RULE-005", "orders", "order_ts must not be future-dated", TIMELINESS, CRITICAL,
            lambda df, ctx: _future(df, "order_ts", ctx)),
)

# ---------------------------------------------------------------------------
# PAYMENTS
# ---------------------------------------------------------------------------
PAYMENT_RULES: Sequence[RowRule] = (
    RowRule("RULE-039", "payments", "payment_id and order_id must be present", COMPLETENESS, CRITICAL,
            lambda df, ctx: _blank(df, "payment_id") | _blank(df, "order_id")),
    RowRule("RULE-020", "payments", "gateway_ref must be present for non-Cash-on-Delivery payments",
            COMPLETENESS, WARNING,
            lambda df, ctx: (tf.standardise_payment_mode(df["payment_mode"]) != "Cash on Delivery")
            & _blank(df, "gateway_ref")),
    RowRule("RULE-016", "payments", "payment_id must be unique (redundant copies quarantined)",
            UNIQUENESS, CRITICAL,
            lambda df, ctx: _duplicated(df, "payment_id", order_by="payment_ts")),
    RowRule("RULE-036", "payments", "payment_mode must be in the approved list", VALIDITY, WARNING,
            lambda df, ctx: _not_in(
                df.assign(_pm=tf.standardise_payment_mode(df["payment_mode"])),
                "_pm", config.ALLOWED_PAYMENT_MODE)),
    RowRule("RULE-037", "payments", "payment_status must be in the approved list", VALIDITY, WARNING,
            lambda df, ctx: _not_in(df, "payment_status", config.ALLOWED_PAYMENT_STATUS)),
    RowRule("RULE-038", "payments", "SUCCESS payments must have amount_paid greater than zero",
            ACCURACY, CRITICAL,
            lambda df, ctx: (tf.standardise_upper(df["payment_status"]) == "SUCCESS")
            & (tf.to_number(df["amount_paid"]).isna() | (tf.to_number(df["amount_paid"]) <= 0))),
    RowRule("RULE-017", "payments", "order_id must exist in orders.csv", CONSISTENCY, CRITICAL,
            lambda df, ctx: _missing_fk(df, "order_id", ctx.valid_order_ids)),
    RowRule("RULE-018", "payments", "SUCCESS payment amount must equal the order amount",
            CONSISTENCY, CRITICAL,
            lambda df, ctx: (tf.standardise_upper(df["payment_status"]) == "SUCCESS")
            & tf.clean_text(df["order_id"]).isin(ctx.order_amounts.keys())
            & ((tf.to_number(df["amount_paid"])
                - pd.to_numeric(tf.clean_text(df["order_id"]).map(ctx.order_amounts),
                                errors="coerce")).abs()
               > config.AMOUNT_TOLERANCE)),
    RowRule("RULE-019", "payments", "payment_ts must not be future-dated", TIMELINESS, CRITICAL,
            lambda df, ctx: _future(df, "payment_ts", ctx)),
)

# ---------------------------------------------------------------------------
# CUSTOMERS
# ---------------------------------------------------------------------------
CUSTOMER_RULES: Sequence[RowRule] = (
    RowRule("RULE-043", "customers", "customer_id must be present", COMPLETENESS, CRITICAL,
            lambda df, ctx: _blank(df, "customer_id")),
    RowRule("RULE-022", "customers", "city should not be blank", COMPLETENESS, WARNING,
            lambda df, ctx: _blank(df, "city")),
    RowRule("RULE-021", "customers", "customer_id must be unique (superseded copies quarantined)",
            UNIQUENESS, CRITICAL,
            lambda df, ctx: _duplicated(df, "customer_id", order_by="signup_date")),
    RowRule("RULE-040", "customers", "email must look like name@domain.tld", VALIDITY, WARNING,
            lambda df, ctx: tf.clean_text(df["email"]).notna()
            & ~df["email"].map(masking.is_valid_email)),
    RowRule("RULE-041", "customers", "phone must contain 10 digits", VALIDITY, WARNING,
            lambda df, ctx: tf.clean_text(df["phone"]).notna()
            & ~df["phone"].map(masking.is_valid_phone)),
    RowRule("RULE-042", "customers", "pincode must contain 6 digits", VALIDITY, WARNING,
            lambda df, ctx: _digits_not(df, "pincode", 6)),
    RowRule("RULE-024", "customers", "loyalty_tier must be in the approved list", VALIDITY, WARNING,
            lambda df, ctx: _not_in(df, "loyalty_tier", config.ALLOWED_LOYALTY_TIER)),
    RowRule("RULE-023", "customers", "signup_date must not be future-dated", TIMELINESS, WARNING,
            lambda df, ctx: _future(df, "signup_date", ctx)),
)

# ---------------------------------------------------------------------------
# PRODUCTS
# ---------------------------------------------------------------------------
PRODUCT_RULES: Sequence[RowRule] = (
    RowRule("RULE-044", "products", "product_id must be present", COMPLETENESS, CRITICAL,
            lambda df, ctx: _blank(df, "product_id")),
    # Deviation from the suggested severity, agreed with the business reasoning in
    # SUBMISSION.md: a blank category must not delete a real delivered order from
    # revenue. The row loads with category "Unknown" and is reported as a warning.
    RowRule("RULE-026", "products", "category must not be blank", COMPLETENESS, WARNING,
            lambda df, ctx: _blank(df, "category")),
    RowRule("RULE-025", "products", "product_id must be unique (superseded copies quarantined)",
            UNIQUENESS, CRITICAL,
            lambda df, ctx: _duplicated(df, "product_id", order_by="launch_date")),
    RowRule("RULE-027", "products", "mrp must be greater than zero", ACCURACY, CRITICAL,
            lambda df, ctx: tf.to_number(df["mrp"]).isna() | (tf.to_number(df["mrp"]) <= 0)),
    RowRule("RULE-028", "products", "cost_price should not exceed mrp", ACCURACY, WARNING,
            lambda df, ctx: tf.to_number(df["cost_price"]).notna()
            & tf.to_number(df["mrp"]).notna()
            & (tf.to_number(df["cost_price"]) > tf.to_number(df["mrp"]))),
    RowRule("RULE-047", "products", "launch_date must not be future-dated", TIMELINESS, WARNING,
            lambda df, ctx: _future(df, "launch_date", ctx)),
)

# ---------------------------------------------------------------------------
# STORES
# ---------------------------------------------------------------------------
STORE_RULES: Sequence[RowRule] = (
    RowRule("RULE-046", "stores", "store_id must be present", COMPLETENESS, CRITICAL,
            lambda df, ctx: _blank(df, "store_id")),
    RowRule("RULE-030", "stores", "zone should not be blank", COMPLETENESS, WARNING,
            lambda df, ctx: _blank(df, "zone")),
    RowRule("RULE-029", "stores", "store_id must be unique (superseded copies quarantined)",
            UNIQUENESS, CRITICAL,
            lambda df, ctx: _duplicated(df, "store_id", order_by="open_date")),
    RowRule("RULE-045", "stores", "zone must be one of North/South/East/West", VALIDITY, WARNING,
            lambda df, ctx: _not_in(df, "zone", config.ALLOWED_ZONES)),
    RowRule("RULE-031", "stores", "capacity_orders_per_day must be greater than zero", ACCURACY, WARNING,
            lambda df, ctx: tf.to_number(df["capacity_orders_per_day"]).notna()
            & (tf.to_number(df["capacity_orders_per_day"]) <= 0)),
    RowRule("RULE-048", "stores", "open_date must not be future-dated", TIMELINESS, WARNING,
            lambda df, ctx: _future(df, "open_date", ctx)),
)

ROW_RULES: dict[str, Sequence[RowRule]] = {
    "orders": ORDER_RULES,
    "payments": PAYMENT_RULES,
    "customers": CUSTOMER_RULES,
    "products": PRODUCT_RULES,
    "stores": STORE_RULES,
}


# ---------------------------------------------------------------------------
# Dataset-level rules (volume and freshness) - Part F experiments 7 and 9
# ---------------------------------------------------------------------------
def _volume_check(dataset: str):
    def _evaluate(df: pd.DataFrame, ctx: ValidationContext) -> tuple[int, int, str]:
        expected = ctx.expected_counts.get(dataset, 0)
        actual = len(df)
        if expected <= 0:
            return actual, 0, "no baseline row count configured"
        drop_pct = (expected - actual) / expected * 100
        failed = 1 if drop_pct > config.VOLUME_DROP_WARN_PCT else 0
        detail = f"expected~{expected} rows, received {actual} rows ({drop_pct:.1f}% below baseline)"
        return actual, failed, detail

    return _evaluate


def _freshness_check(column: str):
    def _evaluate(df: pd.DataFrame, ctx: ValidationContext) -> tuple[int, int, str]:
        if column not in df.columns or df.empty:
            return len(df), 1, f"{column} missing or file empty"
        parsed = tf.to_timestamp(df[column])
        # Future-dated rows are themselves a data error; if they were included
        # here they would make a stale file look fresh.
        latest = parsed[parsed <= ctx.now].max()
        if pd.isna(latest):
            return len(df), 1, f"no usable {column} value found"
        age_days = (ctx.now - latest).total_seconds() / 86400
        failed = 1 if age_days > config.MAX_DATA_AGE_DAYS else 0
        detail = f"latest {column} = {latest:%Y-%m-%d %H:%M}, {age_days:.1f} day(s) old"
        return len(df), failed, detail

    return _evaluate


DATASET_RULES: dict[str, Sequence[DatasetRule]] = {
    "orders": (
        DatasetRule("RULE-034", "orders", "orders row count must not drop more than 90% vs baseline",
                    "Volume", WARNING, _volume_check("orders")),
        DatasetRule("RULE-035", "orders", "latest order_ts must not be older than 1 day",
                    TIMELINESS, WARNING, _freshness_check("order_ts")),
    ),
    "payments": (
        DatasetRule("RULE-049", "payments", "payments row count must not drop more than 90% vs baseline",
                    "Volume", WARNING, _volume_check("payments")),
    ),
    "customers": (
        DatasetRule("RULE-050", "customers", "customers row count must not drop more than 90% vs baseline",
                    "Volume", WARNING, _volume_check("customers")),
    ),
    "products": (
        DatasetRule("RULE-051", "products", "products row count must not drop more than 90% vs baseline",
                    "Volume", WARNING, _volume_check("products")),
    ),
    "stores": (
        DatasetRule("RULE-052", "stores", "stores row count must not drop more than 90% vs baseline",
                    "Volume", WARNING, _volume_check("stores")),
    ),
}

#: Columns every source file must contain. Used by the schema guard below.
EXPECTED_COLUMNS: dict[str, tuple[str, ...]] = {
    "orders": ("order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity",
               "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status",
               "channel", "delivery_minutes"),
    "payments": ("payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid",
                 "payment_status", "gateway_ref"),
    "customers": ("customer_id", "full_name", "email", "phone", "city", "area", "pincode",
                  "signup_date", "loyalty_tier", "is_active"),
    "products": ("product_id", "product_name", "category", "sub_category", "brand", "mrp",
                 "cost_price", "unit", "is_perishable", "launch_date"),
    "stores": ("store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name",
               "capacity_orders_per_day"),
}


class SchemaError(ValueError):
    """Raised when a source file does not have the agreed columns.

    Failing loudly here is deliberate (Part F experiment 6): a renamed column
    must stop the pipeline instead of silently loading wrong numbers.
    """


def assert_schema(dataset: str, df: pd.DataFrame) -> None:
    """Fail fast when a required column is missing from a source file."""
    expected = set(EXPECTED_COLUMNS.get(dataset, ()))
    missing = sorted(expected - set(df.columns))
    if missing:
        raise SchemaError(
            f"{dataset}: missing expected column(s) {missing}. "
            f"Columns received: {sorted(df.columns)}"
        )
    extra = sorted(set(df.columns) - expected)
    if extra:
        logger.warning("%s: unexpected extra column(s) %s - they will be ignored", dataset, extra)


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------
def _dq_row(rule, ctx: ValidationContext, rows_checked: int, rows_failed: int,
            detail: str = "") -> dict:
    """Build one row for the ``dq_results`` quality scorecard table."""
    if rows_failed == 0:
        status = "PASS"
    else:
        status = "FAIL" if rule.severity == CRITICAL else "WARN"
    return {
        "run_id": ctx.run_id,
        "dag_id": ctx.dag_id,
        "rule_id": rule.rule_id,
        "dataset": rule.dataset,
        "check_name": rule.check_name,
        "dimension": rule.dimension,
        "severity": rule.severity,
        "rows_checked": int(rows_checked),
        "rows_failed": int(rows_failed),
        "status": status,
        "detail": detail,
        "checked_at": datetime.now().isoformat(timespec="seconds"),
    }


def validate(dataset: str, df: pd.DataFrame, ctx: ValidationContext) -> ValidationResult:
    """Run every rule for ``dataset`` and split the rows.

    Returns a :class:`ValidationResult` where

    * ``passed`` holds the rows that broke no **Critical** rule (rows that only
      broke Warning rules are included, with a ``dq_warnings`` column),
    * ``failed`` holds the quarantined rows plus a ``failure_reason`` column,
    * ``dq_rows`` holds one scorecard row per rule, written to ``dq_results``.
    """
    assert_schema(dataset, df)

    working = df.copy()
    rows_in = len(working)
    dq_rows: list[dict] = []
    critical_mask = pd.Series(False, index=working.index)
    reasons = pd.Series("", index=working.index, dtype="object")
    warnings = pd.Series("", index=working.index, dtype="object")

    for rule in ROW_RULES.get(dataset, ()):  # row-level rules
        try:
            mask = rule.failing_mask(working, ctx)
        except KeyError as exc:
            raise SchemaError(f"{dataset}: rule {rule.rule_id} needs column {exc}") from exc

        failed_count = int(mask.sum())
        dq_rows.append(_dq_row(rule, ctx, rows_in, failed_count))

        if failed_count:
            if rule.severity == CRITICAL:
                critical_mask |= mask
                reasons.loc[mask] = reasons.loc[mask].map(
                    lambda existing, r=rule.reason: f"{existing} | {r}" if existing else r)
            else:
                warnings.loc[mask] = warnings.loc[mask].map(
                    lambda existing, r=rule.reason: f"{existing} | {r}" if existing else r)
            logger.info("[DQ] %s %s -> %s rows failed (%s)",
                        dataset, rule.rule_id, failed_count, rule.severity)

    for ds_rule in DATASET_RULES.get(dataset, ()):  # file-level rules
        checked, failed, detail = ds_rule.evaluate(working, ctx)
        dq_rows.append(_dq_row(ds_rule, ctx, checked, failed, detail))
        if failed:
            logger.warning("[DQ] %s %s -> %s (%s)", dataset, ds_rule.rule_id, detail, ds_rule.severity)

    working["dq_warnings"] = warnings.replace("", pd.NA)
    passed = working.loc[~critical_mask].copy()
    passed["dq_status"] = "PASS"

    failed_df = working.loc[critical_mask].copy()
    failed_df["failure_reason"] = reasons[critical_mask]
    failed_df["dq_status"] = "QUARANTINED"
    failed_df["source_file"] = config.SOURCE_FILES.get(dataset, dataset)
    failed_df["run_id"] = ctx.run_id
    failed_df["dag_id"] = ctx.dag_id
    failed_df["quarantined_at"] = datetime.now().isoformat(timespec="seconds")

    logger.info("[DQ] %s: %s rows in, %s passed, %s quarantined",
                dataset, rows_in, len(passed), len(failed_df))
    return ValidationResult(dataset, passed, failed_df, dq_rows, rows_in)


def rule_catalogue() -> pd.DataFrame:
    """Return every configured rule as a DataFrame (used by the documentation)."""
    rows = []
    for dataset, rules in ROW_RULES.items():
        for rule in rules:
            rows.append({"rule_id": rule.rule_id, "dataset": dataset, "check_name": rule.check_name,
                         "dimension": rule.dimension, "severity": rule.severity, "scope": "row"})
    for dataset, rules in DATASET_RULES.items():
        for rule in rules:
            rows.append({"rule_id": rule.rule_id, "dataset": dataset, "check_name": rule.check_name,
                         "dimension": rule.dimension, "severity": rule.severity, "scope": "dataset"})
    return pd.DataFrame(rows).sort_values(["dataset", "rule_id"]).reset_index(drop=True)
