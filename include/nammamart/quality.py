"""Reusable data-quality checks for NammaMart pipelines."""

import pandas as pd


def duplicate_key_rows(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Return rows whose non-null business key occurs more than once."""
    valid = df[key].notna() & df[key].astype(str).str.strip().ne("")
    return df.loc[valid & df.duplicated(subset=[key], keep=False)].copy()


def missing_key_rows(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Return rows where the business key is missing or blank."""
    missing = df[key].isna() | df[key].astype(str).str.strip().eq("")
    return df.loc[missing].copy()


def orphan_rows(
    child: pd.DataFrame,
    child_key: str,
    parent: pd.DataFrame,
    parent_key: str,
) -> pd.DataFrame:
    """Return child rows with nonblank keys absent from the parent."""
    key = child[child_key].astype("string").str.strip()
    parent_keys = (
        parent[parent_key]
        .dropna()
        .astype("string")
        .str.strip()
    )
    valid = key.notna() & key.ne("")
    return child.loc[valid & ~key.isin(parent_keys)].copy()


def exact_duplicate_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Return repeated full rows after their first occurrence."""
    return df.loc[df.duplicated(keep="first")].copy()


def _quality_issue(row_key, column, problem, dimension, severity="Critical"):
    """Build a standardized data-quality issue record."""
    return {
        "row_key": row_key,
        "column": column,
        "problem": problem,
        "quality_dimension": dimension,
        "severity": severity,
    }


def order_accuracy_issues(df):
    """Identify invalid order quantities, discounts, and calculated amounts."""
    import pandas as pd

    issues = []

    quantity = pd.to_numeric(df["quantity"], errors="coerce")
    unit_price = pd.to_numeric(df["unit_price"], errors="coerce")
    discount = pd.to_numeric(df["discount_pct"], errors="coerce")
    amount = pd.to_numeric(df["order_amount"], errors="coerce")

    expected = (quantity * unit_price * (1 - discount / 100)).round(2)
    actual = amount.round(2)

    checks = [
        (
            quantity.notna() & (quantity <= 0),
            "quantity",
            "Quantity must be greater than zero",
            "Accuracy",
        ),
        (
            discount.notna() & ~discount.between(0, 100),
            "discount_pct",
            "Discount must be between 0 and 100",
            "Validity",
        ),
        (
            quantity.notna()
            & unit_price.notna()
            & discount.notna()
            & amount.notna()
            & ~expected.eq(actual),
            "order_amount",
            "Order amount does not match the required formula",
            "Accuracy",
        ),
    ]

    for mask, column, problem, dimension in checks:
        for index in df.index[mask]:
            issues.append(
                _quality_issue(
                    df.at[index, "order_id"],
                    column,
                    problem,
                    dimension,
                )
            )

    return pd.DataFrame(
        issues,
        columns=[
            "row_key",
            "column",
            "problem",
            "quality_dimension",
            "severity",
        ],
    )


def payment_reconciliation_issues(payments, orders):
    """Flag SUCCESS payments that do not exactly match their order amount."""
    import pandas as pd

    successful = payments.loc[
        payments["payment_status"].astype("string").str.strip().str.upper()
        .eq("SUCCESS")
    ].copy()

    order_amounts = orders[["order_id", "order_amount"]].copy()
    order_amounts["order_amount"] = pd.to_numeric(
        order_amounts["order_amount"], errors="coerce"
    ).round(2)

    successful["amount_paid"] = pd.to_numeric(
        successful["amount_paid"], errors="coerce"
    ).round(2)

    joined = successful.merge(
        order_amounts,
        on="order_id",
        how="left",
        validate="many_to_one",
    )

    mismatch = (
        joined["order_amount"].isna()
        | joined["amount_paid"].isna()
        | joined["amount_paid"].ne(joined["order_amount"])
    )

    issues = []
    for _, row in joined.loc[mismatch].iterrows():
        if pd.isna(row["order_amount"]):
            problem = "Successful payment has no matching order amount"
        elif pd.isna(row["amount_paid"]):
            problem = "Successful payment amount is missing or invalid"
        else:
            problem = (
                "Successful payment amount does not exactly match "
                "the order amount after rounding to two decimals"
            )

        issues.append(
            _quality_issue(
                row["payment_id"],
                "amount_paid",
                problem,
                "Consistency",
                "Critical",
            )
        )

    return pd.DataFrame(
        issues,
        columns=[
            "row_key",
            "column",
            "problem",
            "quality_dimension",
            "severity",
        ],
    )


def product_pricing_issues(df):
    """Identify invalid MRP and cost-price relationships."""
    import pandas as pd

    mrp = pd.to_numeric(df["mrp"], errors="coerce")
    cost = pd.to_numeric(df["cost_price"], errors="coerce")

    checks = [
        (
            mrp.isna() | (mrp <= 0),
            "mrp",
            "MRP must be a valid number greater than zero",
        ),
        (
            mrp.notna() & cost.notna() & (cost > mrp),
            "cost_price",
            "Cost price must not exceed MRP",
        ),
    ]

    issues = []
    for mask, column, problem in checks:
        for index in df.index[mask]:
            issues.append(
                _quality_issue(
                    df.at[index, "product_id"],
                    column,
                    problem,
                    "Accuracy",
                )
            )

    return pd.DataFrame(
        issues,
        columns=[
            "row_key",
            "column",
            "problem",
            "quality_dimension",
            "severity",
        ],
    )


def customer_validity_issues(df: pd.DataFrame) -> pd.DataFrame:
    """Identify invalid customer contact details and inconsistent loyalty tiers."""
    issues = []

    email = df["email"].astype("string").str.strip()
    phone = df["phone"].astype("string").str.strip()
    pincode = df["pincode"].astype("string").str.strip()
    loyalty_tier = df["loyalty_tier"].astype("string").str.strip()

    checks = [
        (
            email.isna() | ~email.str.fullmatch(
                r"[^@\s]+@[^@\s]+\.[^@\s]+", na=False
            ),
            "email",
            "Email format is invalid",
        ),
        (
            phone.isna() | ~phone.str.fullmatch(r"\d{10}", na=False),
            "phone",
            "Phone must contain exactly 10 digits",
        ),
        (
            pincode.isna() | ~pincode.str.fullmatch(r"\d{6}", na=False),
            "pincode",
            "Pincode must contain exactly 6 digits",
        ),
        (
             loyalty_tier.notna()
            & loyalty_tier.ne("")
            & loyalty_tier.ne(
                loyalty_tier.str.title()
            ),
            "loyalty_tier",
            "Loyalty tier must use consistent title casing",
        ),
    ]

    for mask, column, problem in checks:
        for index in df.index[mask]:
            issues.append(
                _quality_issue(
                    df.at[index, "customer_id"],
                    column,
                    problem,
                    "Validity" if column != "loyalty_tier" else "Consistency",
                    "Warning",
                )
            )

    return pd.DataFrame(
        issues,
        columns=[
            "row_key",
            "column",
            "problem",
            "quality_dimension",
            "severity",
        ],
    )


def order_delivery_issues(df: pd.DataFrame) -> pd.DataFrame:
    """Identify missing delivered-order times and invalid delivery durations."""
    status = df["order_status"].astype("string").str.strip().str.upper()
    delivery = pd.to_numeric(df["delivery_minutes"], errors="coerce")

    checks = [
        (
            status.eq("DELIVERED") & delivery.isna(),
            "delivery_minutes",
            "Delivered order must have a valid delivery time",
            "Completeness",
        ),
        (
            delivery.notna() & (delivery >= 120),
            "delivery_minutes",
            "Delivery time must be less than 120 minutes",
            "Accuracy",
        ),
        (
            delivery.notna() & (delivery <= 0),
            "delivery_minutes",
            "Delivery time must be greater than zero",
            "Accuracy",
        ),
    ]

    issues = []
    for mask, column, problem, dimension in checks:
        for index in df.index[mask]:
            issues.append(
                _quality_issue(
                    df.at[index, "order_id"],
                    column,
                    problem,
                    dimension,
                    "Critical",
                )
            )

    return pd.DataFrame(
        issues,
        columns=[
            "row_key",
            "column",
            "problem",
            "quality_dimension",
            "severity",
        ],
    )


def timestamp_parsing_issues(
    df: pd.DataFrame,
    column: str,
    key: str,
    severity: str = "Critical",
) -> pd.DataFrame:
    """Identify nonblank timestamps that cannot be parsed."""
    raw = df[column].astype("string").str.strip()
    parsed = pd.to_datetime(raw, errors="coerce", format="mixed")
    invalid = raw.isna() | raw.eq("") | parsed.isna()

    issues = []
    for index in df.index[invalid]:
        issues.append(
            _quality_issue(
                df.at[index, key],
                column,
                "Timestamp is missing or cannot be parsed",
                "Validity",
                severity,
            )
        )

    return pd.DataFrame(
        issues,
        columns=[
            "row_key",
            "column",
            "problem",
            "quality_dimension",
            "severity",
        ],
    )


def order_delivery_issues(df: pd.DataFrame) -> pd.DataFrame:
    """Identify missing delivered-order times and invalid delivery durations."""
    status = df["order_status"].astype("string").str.strip().str.upper()
    delivery = pd.to_numeric(df["delivery_minutes"], errors="coerce")

    checks = [
        (
            status.eq("DELIVERED") & delivery.isna(),
            "delivery_minutes",
            "Delivered order must have a valid delivery time",
            "Completeness",
        ),
        (
            delivery.notna() & (delivery >= 120),
            "delivery_minutes",
            "Delivery time must be less than 120 minutes",
            "Accuracy",
        ),
        (
            delivery.notna() & (delivery <= 0),
            "delivery_minutes",
            "Delivery time must be greater than zero",
            "Accuracy",
        ),
    ]

    issues = []
    for mask, column, problem, dimension in checks:
        for index in df.index[mask]:
            issues.append(
                _quality_issue(
                    df.at[index, "order_id"],
                    column,
                    problem,
                    dimension,
                    "Critical",
                )
            )

    return pd.DataFrame(
        issues,
        columns=[
            "row_key",
            "column",
            "problem",
            "quality_dimension",
            "severity",
        ],
    )


def timestamp_parsing_issues(
    df: pd.DataFrame,
    column: str,
    key: str,
    severity: str = "Critical",
) -> pd.DataFrame:
    """Identify nonblank timestamps that cannot be parsed."""
    raw = df[column].astype("string").str.strip()
    parsed = pd.to_datetime(raw, errors="coerce", format="mixed")
    invalid = raw.isna() | raw.eq("") | parsed.isna()

    issues = []
    for index in df.index[invalid]:
        issues.append(
            _quality_issue(
                df.at[index, key],
                column,
                "Timestamp is missing or cannot be parsed",
                "Validity",
                severity,
            )
        )

    return pd.DataFrame(
        issues,
        columns=[
            "row_key",
            "column",
            "problem",
            "quality_dimension",
            "severity",
        ],
    )
