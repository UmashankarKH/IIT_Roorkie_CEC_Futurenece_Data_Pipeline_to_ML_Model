#!/usr/bin/env python3
"""Part 0: profile the NammaMart exports before any pipeline code is written.

Run:  python include/profiling/profile_nammamart.py

Writes:
  include/profiling/profile_report.md   - human readable profile
  include/profiling/data_issues_log.csv - the Data Issues Log for SUBMISSION.md

Nothing here cleans or fixes data. The job is to find out what is wrong and how
often, so the quality rules in Part D are based on evidence and not on guesses.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

DATA = Path(__file__).resolve().parents[1] / "data" / "nammamart"
OUT = Path(__file__).resolve().parent

FILES = {
    "orders.csv": "order_id",
    "payments.csv": "payment_id",
    "customers.csv": "customer_id",
    "products.csv": "product_id",
    "stores.csv": "store_id",
    "orders_2026-10-01_increment.csv": "order_id",
}

VALID_STATUS = {"DELIVERED", "CANCELLED", "RETURNED"}
VALID_PAYMENT_MODE = {"UPI", "Card", "Cash on Delivery", "Wallet"}
DELIVERY_MIN, DELIVERY_MAX = 8, 35
HARD_DELIVERY_LIMIT = 120

# Columns that are not the primary key but still cannot be null, because a row
# without them cannot be joined or cannot be trusted.
MANDATORY_COLUMNS = {
    "orders.csv": ("customer_id", "store_id", "product_id"),
    "payments.csv": ("order_id",),
}

issues: list[dict] = []
lines: list[str] = []


def log_issue(file: str, row_key: str, column: str, problem: str,
              dimension: str, rule: str, severity: str, count: int = 1) -> None:
    issues.append({
        "file": file, "row_key": row_key, "column": column, "problem": problem,
        "quality_dimension": dimension, "proposed_rule": rule,
        "severity": severity, "rows_affected": count,
    })


def say(text: str = "") -> None:
    print(text)
    lines.append(text)


def keys(series: pd.Series, limit: int = 4) -> str:
    vals = [str(v) for v in series.head(limit).tolist()]
    more = "" if len(series) <= limit else f" +{len(series) - limit} more"
    return ", ".join(vals) + more


def load(name: str) -> pd.DataFrame:
    # Everything as text: parsing is a pipeline decision, not a profiling one.
    return pd.read_csv(DATA / name, dtype=str, keep_default_na=False, na_values=[""])


def num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def ts(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", format="mixed")


# --------------------------------------------------------------- generic pass

def profile_file(name: str, key: str, df: pd.DataFrame) -> None:
    say(f"\n### {name}")
    say(f"rows: {len(df)}  columns: {len(df.columns)}")

    nulls = df.isna().sum()
    nulls = nulls[nulls > 0]
    if len(nulls):
        say("nulls: " + ", ".join(f"{c}={n}" for c, n in nulls.items()))
        for col, n in nulls.items():
            bad = df[df[col].isna()]
            # A missing key or a missing foreign key means the row cannot be joined to
            # anything, so it is Critical. Everything else loads with a warning - the
            # 164 orders with no delivery_minutes are nearly all cancellations, and
            # quarantining those would delete every cancellation from the warehouse.
            critical = col == key or col in MANDATORY_COLUMNS.get(name, ())
            log_issue(name, keys(bad[key].fillna("(null key)")), col,
                      f"{n} rows with no {col}", "Completeness",
                      f"{col} must be present",
                      "Critical" if critical else "Warning", int(n))
    else:
        say("nulls: none")

    if key in df.columns:
        dupes = df[df.duplicated(key, keep=False) & df[key].notna()]
        if len(dupes):
            say(f"duplicate {key}: {dupes[key].nunique()} keys over {len(dupes)} rows")
            log_issue(name, keys(dupes[key].drop_duplicates()), key,
                      f"{dupes[key].nunique()} duplicated keys", "Uniqueness",
                      f"{key} must be unique; keep the last row by load order",
                      "Critical", int(len(dupes)))
        else:
            say(f"duplicate {key}: none")

    exact = df.duplicated(keep=False)
    if exact.any():
        say(f"exact duplicate rows: {int(exact.sum())}")


# ------------------------------------------------------------------ per file

def profile_orders(name: str, df: pd.DataFrame) -> None:
    qty, price = num(df["quantity"]), num(df["unit_price"])
    disc, amount = num(df["discount_pct"]), num(df["order_amount"])
    mins = num(df["delivery_minutes"])
    when = ts(df["order_ts"])

    say(f"order_ts range: {when.min()} .. {when.max()}  unparsed: {int(when.isna().sum() - df['order_ts'].isna().sum())}")
    say(f"quantity: {qty.min()} .. {qty.max()}   unit_price: {price.min()} .. {price.max()}")
    say(f"discount_pct: {disc.min()} .. {disc.max()}   order_amount: {amount.min()} .. {amount.max()}")
    say(f"delivery_minutes: {mins.min()} .. {mins.max()}")
    say("order_status: " + ", ".join(f"{k}={v}" for k, v in df["order_status"].value_counts(dropna=False).items()))
    say("payment_mode: " + ", ".join(f"{k}={v}" for k, v in df["payment_mode"].value_counts(dropna=False).items()))
    say("channel: " + ", ".join(f"{k}={v}" for k, v in df["channel"].value_counts(dropna=False).items()))

    bad_status = df[df["order_status"].notna() & ~df["order_status"].isin(VALID_STATUS)]
    if len(bad_status):
        log_issue(name, keys(bad_status["order_id"]), "order_status",
                  f"{len(bad_status)} rows outside {sorted(VALID_STATUS)}: "
                  + ", ".join(sorted(bad_status['order_status'].unique())[:5]),
                  "Validity", "order_status must be DELIVERED, CANCELLED or RETURNED",
                  "Critical", len(bad_status))

    raw_modes = df["payment_mode"].dropna().unique()
    messy = [m for m in raw_modes if m not in VALID_PAYMENT_MODE and m.strip().upper() in
             {v.upper() for v in VALID_PAYMENT_MODE}]
    if messy:
        n = int(df["payment_mode"].isin(messy).sum())
        log_issue(name, keys(df[df["payment_mode"].isin(messy)]["order_id"]), "payment_mode",
                  f"{n} rows with casing or spacing variants: {messy}", "Consistency",
                  "trim and title-case payment_mode onto the allowed list", "Warning", n)
    unknown_modes = [m for m in raw_modes if m.strip().upper() not in {v.upper() for v in VALID_PAYMENT_MODE}]
    if unknown_modes:
        n = int(df["payment_mode"].isin(unknown_modes).sum())
        log_issue(name, keys(df[df["payment_mode"].isin(unknown_modes)]["order_id"]), "payment_mode",
                  f"{n} rows with unmapped mode: {unknown_modes}", "Validity",
                  "payment_mode must map to the allowed list", "Critical", n)

    bad_disc = df[disc.notna() & ~disc.between(0, 100)]
    if len(bad_disc):
        log_issue(name, keys(bad_disc["order_id"]), "discount_pct",
                  f"{len(bad_disc)} rows with discount outside 0-100 "
                  f"(max {disc.max():.0f}%)", "Validity",
                  "discount_pct must be between 0 and 100", "Critical", len(bad_disc))

    neg_qty = df[qty.notna() & (qty <= 0)]
    if len(neg_qty):
        log_issue(name, keys(neg_qty["order_id"]), "quantity",
                  f"{len(neg_qty)} rows with quantity <= 0 (min {qty.min()})", "Accuracy",
                  "quantity must be > 0; negatives need a client ruling (return vs typo)",
                  "Critical", len(neg_qty))

    expected = (qty * price * (1 - disc / 100)).round(2)
    mismatch = df[amount.notna() & expected.notna() & ((amount - expected).abs() > 0.01)]
    if len(mismatch):
        worst = (amount - expected).abs().max()
        log_issue(name, keys(mismatch["order_id"]), "order_amount",
                  f"{len(mismatch)} rows where amount != qty*price*(1-disc) (max gap {worst:.2f})",
                  "Accuracy", "recompute order_amount from the formula and quarantine mismatches",
                  "Critical", len(mismatch))

    slow = df[mins.notna() & (mins > HARD_DELIVERY_LIMIT)]
    if len(slow):
        log_issue(name, keys(slow["order_id"]), "delivery_minutes",
                  f"{len(slow)} rows above {HARD_DELIVERY_LIMIT} minutes (max {mins.max()})",
                  "Accuracy", f"delivery_minutes must be <= {HARD_DELIVERY_LIMIT}",
                  "Critical", len(slow))
    odd = df[mins.notna() & (mins.between(DELIVERY_MAX + 1, HARD_DELIVERY_LIMIT))]
    if len(odd):
        log_issue(name, keys(odd["order_id"]), "delivery_minutes",
                  f"{len(odd)} rows outside the normal {DELIVERY_MIN}-{DELIVERY_MAX} minute band",
                  "Accuracy", "flag for ops review, load anyway", "Warning", len(odd))

    delivered_no_time = df[(df["order_status"] == "DELIVERED") & mins.isna()]
    if len(delivered_no_time):
        log_issue(name, keys(delivered_no_time["order_id"]), "delivery_minutes",
                  f"{len(delivered_no_time)} DELIVERED orders with no delivery time",
                  "Completeness", "DELIVERED must carry delivery_minutes", "Critical",
                  len(delivered_no_time))

    future = df[when.notna() & (when > pd.Timestamp("2026-10-02"))]
    if len(future):
        log_issue(name, keys(future["order_id"]), "order_ts",
                  f"{len(future)} orders dated after the export window (max {when.max()})",
                  "Timeliness", "reject future-dated orders", "Critical", len(future))

    unparsed = df[df["order_ts"].notna() & when.isna()]
    if len(unparsed):
        log_issue(name, keys(unparsed["order_id"]), "order_ts",
                  f"{len(unparsed)} timestamps that do not parse: "
                  + keys(unparsed["order_ts"].drop_duplicates(), 3),
                  "Validity", "order_ts must parse as a timestamp", "Critical", len(unparsed))


def profile_payments(name: str, df: pd.DataFrame) -> None:
    paid = num(df["amount_paid"])
    say(f"amount_paid: {paid.min()} .. {paid.max()}")
    say("payment_status: " + ", ".join(f"{k}={v}" for k, v in df["payment_status"].value_counts(dropna=False).items()))
    say("payment_mode: " + ", ".join(f"{k}={v}" for k, v in df["payment_mode"].value_counts(dropna=False).items()))

    neg = df[paid.notna() & (paid < 0)]
    if len(neg):
        log_issue(name, keys(neg["payment_id"]), "amount_paid",
                  f"{len(neg)} payments with a negative amount", "Accuracy",
                  "amount_paid must be >= 0", "Critical", len(neg))


def profile_customers(name: str, df: pd.DataFrame) -> None:
    say("loyalty_tier: " + ", ".join(f"{k}={v}" for k, v in df["loyalty_tier"].value_counts(dropna=False).items()))
    say("is_active: " + ", ".join(f"{k}={v}" for k, v in df["is_active"].value_counts(dropna=False).items()))
    say("cities: " + ", ".join(f"{k}={v}" for k, v in df["city"].value_counts(dropna=False).head(6).items()))

    tiers = df["loyalty_tier"].dropna()
    if tiers.str.upper().nunique() < tiers.nunique():
        variants = sorted(tiers.unique())
        log_issue(name, "(all rows)", "loyalty_tier",
                  f"mixed casing across {len(variants)} variants: {variants[:6]}", "Consistency",
                  "upper-case loyalty_tier before load", "Warning", int(len(tiers)))

    email_ok = df["email"].fillna("").str.match(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")
    bad_email = df[~email_ok & df["email"].notna()]
    if len(bad_email):
        log_issue(name, keys(bad_email["customer_id"]), "email",
                  f"{len(bad_email)} malformed email addresses: " + keys(bad_email['email'].drop_duplicates(), 3),
                  "Validity", "warn and load; mask regardless", "Warning", len(bad_email))

    phone_digits = df["phone"].fillna("").str.replace(r"\D", "", regex=True)
    bad_phone = df[(phone_digits.str.len() != 10) & df["phone"].notna()]
    if len(bad_phone):
        log_issue(name, keys(bad_phone["customer_id"]), "phone",
                  f"{len(bad_phone)} phone numbers that are not 10 digits: "
                  + keys(bad_phone['phone'].drop_duplicates(), 3),
                  "Validity", "warn and load; mask regardless", "Warning", len(bad_phone))

    bad_pin = df[(df["pincode"].fillna("").str.len() != 6) & df["pincode"].notna()]
    if len(bad_pin):
        log_issue(name, keys(bad_pin["customer_id"]), "pincode",
                  f"{len(bad_pin)} pincodes that are not 6 digits", "Validity",
                  "pincode must be 6 digits", "Warning", len(bad_pin))

    signup = ts(df["signup_date"])
    future = df[signup.notna() & (signup > pd.Timestamp("2026-10-02"))]
    if len(future):
        log_issue(name, keys(future["customer_id"]), "signup_date",
                  f"{len(future)} customers signed up in the future (max {signup.max()})",
                  "Timeliness", "reject future signup dates", "Critical", len(future))


def profile_products(name: str, df: pd.DataFrame) -> None:
    mrp, cost = num(df["mrp"]), num(df["cost_price"])
    say(f"mrp: {mrp.min()} .. {mrp.max()}   cost_price: {cost.min()} .. {cost.max()}")
    say("categories: " + ", ".join(f"{k}={v}" for k, v in df["category"].value_counts(dropna=False).items()))

    bad_margin = df[mrp.notna() & cost.notna() & (cost > mrp)]
    if len(bad_margin):
        log_issue(name, keys(bad_margin["product_id"]), "cost_price",
                  f"{len(bad_margin)} products where cost_price > mrp", "Accuracy",
                  "cost_price must be <= mrp", "Critical", len(bad_margin))

    bad_mrp = df[mrp.notna() & (mrp <= 0)]
    if len(bad_mrp):
        log_issue(name, keys(bad_mrp["product_id"]), "mrp",
                  f"{len(bad_mrp)} products with mrp <= 0", "Accuracy", "mrp must be > 0",
                  "Critical", len(bad_mrp))


def profile_stores(name: str, df: pd.DataFrame) -> None:
    cap = num(df["capacity_orders_per_day"])
    say("zones: " + ", ".join(f"{k}={v}" for k, v in df["zone"].value_counts(dropna=False).items()))
    say(f"capacity_orders_per_day: {cap.min()} .. {cap.max()}")

    zones = df["zone"].dropna()
    if zones.str.title().nunique() < zones.nunique():
        log_issue(name, "(all rows)", "zone",
                  f"zone casing is inconsistent: {sorted(zones.unique())}", "Consistency",
                  "title-case zone before load", "Warning", len(zones))


# ------------------------------------------------------ referential integrity

def check_relationships(frames: dict[str, pd.DataFrame]) -> None:
    orders = frames["orders.csv"]
    say("\n### Referential integrity")

    for child_col, parent_file, parent_col in (
        ("customer_id", "customers.csv", "customer_id"),
        ("product_id", "products.csv", "product_id"),
        ("store_id", "stores.csv", "store_id"),
    ):
        parent = set(frames[parent_file][parent_col].dropna())
        orphan = orders[orders[child_col].notna() & ~orders[child_col].isin(parent)]
        say(f"orders.{child_col} -> {parent_file}: {len(orphan)} orphans")
        if len(orphan):
            log_issue("orders.csv", keys(orphan["order_id"]), child_col,
                      f"{len(orphan)} orders pointing at a {child_col} that does not exist: "
                      + keys(orphan[child_col].drop_duplicates(), 3),
                      "Consistency", f"{child_col} must exist in {parent_file}", "Critical",
                      len(orphan))

    payments = frames["payments.csv"]
    order_ids = set(orders["order_id"].dropna())
    orphan_pay = payments[payments["order_id"].notna() & ~payments["order_id"].isin(order_ids)]
    say(f"payments.order_id -> orders.csv: {len(orphan_pay)} orphans")
    if len(orphan_pay):
        log_issue("payments.csv", keys(orphan_pay["payment_id"]), "order_id",
                  f"{len(orphan_pay)} payments with no matching order", "Consistency",
                  "payment must reference a known order", "Critical", len(orphan_pay))

    # Reconciliation: a SUCCESS payment should equal its order's amount.
    merged = payments.merge(orders[["order_id", "order_amount", "order_status"]],
                            on="order_id", how="inner")
    merged["paid"] = num(merged["amount_paid"])
    merged["owed"] = num(merged["order_amount"])
    success = merged[merged["payment_status"] == "SUCCESS"]
    gap = success[(success["paid"] - success["owed"]).abs() > 0.01]
    say(f"SUCCESS payments not matching order_amount: {len(gap)}")
    if len(gap):
        total_gap = (gap["paid"] - gap["owed"]).sum()
        log_issue("payments.csv", keys(gap["payment_id"]), "amount_paid",
                  f"{len(gap)} SUCCESS payments differ from order_amount (net {total_gap:,.2f})",
                  "Consistency", "write every mismatch to a reconciliation table", "Critical",
                  len(gap))

    delivered = orders[orders["order_status"] == "DELIVERED"]
    paid_ids = set(payments[payments["payment_status"] == "SUCCESS"]["order_id"])
    unpaid = delivered[~delivered["order_id"].isin(paid_ids)]
    say(f"DELIVERED orders with no SUCCESS payment: {len(unpaid)}")
    if len(unpaid):
        log_issue("orders.csv", keys(unpaid["order_id"]), "order_id",
                  f"{len(unpaid)} delivered orders with no successful payment", "Consistency",
                  "flag on the reconciliation table for finance", "Warning", len(unpaid))

    cancelled_paid = orders[(orders["order_status"] == "CANCELLED") &
                            (orders["order_id"].isin(paid_ids))]
    if len(cancelled_paid):
        log_issue("orders.csv", keys(cancelled_paid["order_id"]), "order_status",
                  f"{len(cancelled_paid)} cancelled orders that still have a successful payment",
                  "Consistency", "flag for refund review", "Warning", len(cancelled_paid))


def check_increment(frames: dict[str, pd.DataFrame]) -> None:
    orders = frames["orders.csv"]
    inc = frames["orders_2026-10-01_increment.csv"]
    say("\n### Increment file")
    existing = set(orders["order_id"])
    corrections = inc[inc["order_id"].isin(existing)]
    new_rows = inc[~inc["order_id"].isin(existing)]
    say(f"rows: {len(inc)}  new orders: {len(new_rows)}  corrections to existing orders: {len(corrections)}")

    if len(corrections):
        joined = corrections.merge(orders[["order_id", "order_status"]], on="order_id",
                                   suffixes=("_new", "_old"))
        changed = joined[joined["order_status_new"] != joined["order_status_old"]]
        say("status changes: " + ", ".join(
            f"{a} -> {b}: {n}" for (a, b), n in
            changed.groupby(["order_status_old", "order_status_new"]).size().items()) or "none")
        log_issue("orders_2026-10-01_increment.csv", keys(changed["order_id"]), "order_status",
                  f"{len(changed)} late status corrections to September orders",
                  "Timeliness", "MERGE on order_id so corrections overwrite, never duplicate",
                  "Critical", len(changed))


def three_questions() -> None:
    say("\n### Three questions for the client before building")
    say("1. Negative quantity: is that a return booked against the original order, or a "
        "data-entry error? It changes whether we quarantine the row or net it off revenue.")
    say("2. When a SUCCESS payment is larger than the order amount, which number is the truth "
        "for the CFO's revenue: the order or the gateway?")
    say("3. An order delivered in September and returned on 1 October: should September revenue "
        "be restated, or should the return land in October? Finance and ops usually disagree.")


def main() -> int:
    if not DATA.exists():
        print(f"Data not found at {DATA}", file=sys.stderr)
        return 2

    say("# NammaMart data profile (Part 0)")
    say(f"source: {DATA}")

    frames = {name: load(name) for name in FILES}
    for name, key in FILES.items():
        profile_file(name, key, frames[name])
        if name.startswith("orders"):
            profile_orders(name, frames[name])
        elif name == "payments.csv":
            profile_payments(name, frames[name])
        elif name == "customers.csv":
            profile_customers(name, frames[name])
        elif name == "products.csv":
            profile_products(name, frames[name])
        elif name == "stores.csv":
            profile_stores(name, frames[name])

    check_relationships(frames)
    check_increment(frames)
    three_questions()

    log = pd.DataFrame(issues, columns=["file", "row_key", "column", "problem",
                                        "quality_dimension", "proposed_rule", "severity",
                                        "rows_affected"])
    OUT.mkdir(parents=True, exist_ok=True)
    log.to_csv(OUT / "data_issues_log.csv", index=False)

    say("\n### Data Issues Log")
    say(f"{len(log)} issues across {log['file'].nunique()} files")
    by_dim = log.groupby(["quality_dimension", "severity"])["rows_affected"].sum()
    for (dim, sev), rows in by_dim.items():
        say(f"  {dim:<13} {sev:<8} {rows} rows")
    say("")
    say("| File | Column | Problem | Dimension | Severity | Rows |")
    say("| --- | --- | --- | --- | --- | --- |")
    for row in log.itertuples():
        say(f"| {row.file} | {row.column} | {row.problem} | {row.quality_dimension} "
            f"| {row.severity} | {row.rows_affected} |")

    (OUT / "profile_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\nWrote {OUT / 'profile_report.md'}")
    print(f"Wrote {OUT / 'data_issues_log.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
