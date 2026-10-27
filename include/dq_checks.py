"""Data-quality framework for the NammaMart ETL pipeline (include/dq_checks.py).

Every check belongs to one of the six dimensions (Completeness, Uniqueness,
Validity, Accuracy, Consistency, Timeliness) and has a severity:

  Critical -> the row is QUARANTINED (never reaches the warehouse)
  Warning  -> the row LOADS, but the problem is logged in dq_results
              (and kept in a `dq_warnings` column on the passed rows)

Usage inside an Airflow task
----------------------------
    raw = {t: pd.read_csv(f"{DATA_DIR}/{t}.csv", dtype=str, keep_default_na=False)
           for t in ["orders", "payments", "customers", "products", "stores"]}
    ctx = build_context(raw)                       # reference keys for FK checks
    passed, failed, results = run_checks(raw["orders"], "orders", ctx, run_id, dag_id)
    write_quarantine(failed, QUARANTINE_DIR, "orders", run_id)
    write_dq_results(con, results)

Always read CSVs with dtype=str, keep_default_na=False so nothing is silently
converted (the checks do their own parsing).

Design notes
------------
* Uniqueness checks flag the 2nd, 3rd... copy of a key and keep the first
  (products prefers the row that is NOT marked "(old)").
* The foreign-key checks compare against the ID sets in the Context. Build the
  Context from the RAW master files (an order is only rejected if its key does
  not exist at all) or from the PASSED master rows (an order is also rejected if
  its product/customer/store was itself quarantined). See build_context().
* Payment amount vs order amount is a Warning here: the payment row is kept and
  the mismatch is listed in etl_payment_reconciliation.
* Dataset-level checks (volume, freshness) set rows_failed = rows_checked when
  they fire, because the whole batch is suspect.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Optional, Set

import numpy as np
import pandas as pd

CRITICAL, WARNING = "Critical", "Warning"

VALID_STATUS = {"DELIVERED", "CANCELLED", "RETURNED"}
PAYMENT_MODE_MAP = {
    "upi": "UPI",
    "card": "Card",
    "cash on delivery": "Cash on Delivery",
    "cod": "Cash on Delivery",
    "cashh": "Cash on Delivery",  # typo seen in the data -- confirm with client
    "wallet": "Wallet",
}
VALID_PAYMENT_STATUS = {"SUCCESS", "FAILED", "REFUNDED"}
VALID_ZONES = {"North", "South", "East", "West"}
VALID_TIERS = {"bronze": "Bronze", "silver": "Silver", "gold": "Gold", "platinum": "Platinum"}
VALID_CHANNELS = {"App", "Web"}
VALID_UNITS = {"kg", "pack", "unit"}

TS_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"
EMAIL_RE = r"^[\w.+-]+@[\w-]+(\.[\w-]+)+$"


# --------------------------------------------------------------------------- #
# Small parsing helpers (all checks work on raw text columns)
# --------------------------------------------------------------------------- #
def _s(df: pd.DataFrame, col: str) -> pd.Series:
    return df[col].fillna("").astype(str).str.strip()


def _num(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(_s(df, col), errors="coerce")


def _ts(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_datetime(_s(df, col), format=TS_FORMAT, errors="coerce")


def _date(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_datetime(_s(df, col), format=DATE_FORMAT, errors="coerce")


def canonical_payment_mode(s: pd.Series) -> pd.Series:
    """'upi', 'UPI ', 'cod', 'Cashh' ... -> allowed value; NaN if unrecognised."""
    return s.fillna("").astype(str).str.strip().str.lower().map(PAYMENT_MODE_MAP)


def canonical_status(s: pd.Series) -> pd.Series:
    up = s.fillna("").astype(str).str.strip().str.upper()
    return up.where(up.isin(VALID_STATUS))


def canonical_tier(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip().str.lower().map(VALID_TIERS)


def _now() -> pd.Timestamp:
    return pd.Timestamp.now(tz="Asia/Kolkata").tz_localize(None)


# --------------------------------------------------------------------------- #
# Context: reference data and run settings
# --------------------------------------------------------------------------- #
@dataclass
class Context:
    as_of: pd.Timestamp = field(default_factory=_now)  # "now" for timeliness checks (IST, naive)
    customer_ids: Set[str] = field(default_factory=set)
    product_ids: Set[str] = field(default_factory=set)
    store_ids: Set[str] = field(default_factory=set)
    order_ids: Set[str] = field(default_factory=set)
    order_amounts: pd.Series = field(default_factory=lambda: pd.Series(dtype=float))
    baseline_rows: Dict[str, int] = field(default_factory=dict)  # expected rows per table
    volume_drop_threshold: float = 0.9   # warn if rows fall by more than 90% vs baseline
    max_age_days: Optional[float] = 1    # freshness limit; None disables the check


def build_context(
    frames: Dict[str, pd.DataFrame],
    as_of: Optional[pd.Timestamp] = None,
    baseline_rows: Optional[Dict[str, int]] = None,
    max_age_days: Optional[float] = 1,
) -> Context:
    """Collect the key sets used by the consistency checks.

    `frames` maps table name -> DataFrame. Pass the RAW frames to reject only
    orders whose keys do not exist at all; pass the PASSED master frames (and
    raw/passed orders) to also reject orders pointing at quarantined masters.
    """
    def ids(table: str, col: str) -> Set[str]:
        if table not in frames:
            return set()
        return set(_s(frames[table], col)) - {""}

    amounts = pd.Series(dtype=float)
    if "orders" in frames:
        o = frames["orders"]
        amounts = pd.Series(_num(o, "order_amount").values, index=_s(o, "order_id").values)
        amounts = amounts[~amounts.index.duplicated(keep="first")]

    return Context(
        as_of=as_of if as_of is not None else _now(),
        customer_ids=ids("customers", "customer_id"),
        product_ids=ids("products", "product_id"),
        store_ids=ids("stores", "store_id"),
        order_ids=ids("orders", "order_id"),
        order_amounts=amounts,
        baseline_rows=baseline_rows or {},
        max_age_days=max_age_days,
    )


# --------------------------------------------------------------------------- #
# Check definition
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Check:
    table: str
    name: str
    dimension: str
    severity: str
    reason: str
    fn: Callable  # row scope: (df, ctx) -> bool Series, True = row FAILS
    #               dataset scope: (df, ctx) -> bool, True = whole batch FAILS
    scope: str = "row"

    @property
    def full_name(self) -> str:
        return f"{self.table}.{self.name}"


CHECKS: Dict[str, list] = {t: [] for t in ["orders", "payments", "customers", "products", "stores"]}


def _add(table, name, dimension, severity, reason, fn, scope="row"):
    CHECKS[table].append(Check(table, name, dimension, severity, reason, fn, scope))


# =========================================================================== #
# ORDERS (also used for the daily increment file)
# =========================================================================== #
def _amount_mismatch(df, c):
    q, up, d, oa = _num(df, "quantity"), _num(df, "unit_price"), _num(df, "discount_pct"), _num(df, "order_amount")
    calc = (q * up * (1 - d / 100)).round(2)
    return ~((calc - oa).abs() <= 0.01)


_add("orders", "order_id_not_null", "Completeness", CRITICAL, "order_id is missing",
     lambda df, c: _s(df, "order_id") == "")
_add("orders", "customer_id_not_null", "Completeness", CRITICAL, "customer_id is missing",
     lambda df, c: _s(df, "customer_id") == "")
_add("orders", "delivered_has_delivery_minutes", "Completeness", CRITICAL,
     "DELIVERED order has no delivery_minutes",
     lambda df, c: (canonical_status(df["order_status"]) == "DELIVERED") & (_s(df, "delivery_minutes") == ""))
_add("orders", "order_id_unique", "Uniqueness", CRITICAL, "duplicate order_id (repeat of an earlier row)",
     lambda df, c: (_s(df, "order_id") != "") & _s(df, "order_id").duplicated(keep="first"))
_add("orders", "order_status_valid", "Validity", CRITICAL,
     "order_status is not DELIVERED/CANCELLED/RETURNED",
     lambda df, c: canonical_status(df["order_status"]).isna())
_add("orders", "order_status_needs_standardising", "Validity", WARNING,
     "order_status casing/whitespace will be standardised",
     lambda df, c: canonical_status(df["order_status"]).notna() & (df["order_status"] != canonical_status(df["order_status"])))
_add("orders", "payment_mode_valid", "Validity", CRITICAL,
     "payment_mode is not UPI/Card/Cash on Delivery/Wallet",
     lambda df, c: canonical_payment_mode(df["payment_mode"]).isna())
_add("orders", "payment_mode_needs_standardising", "Validity", WARNING,
     "payment_mode spelling/casing will be standardised",
     lambda df, c: canonical_payment_mode(df["payment_mode"]).notna() & (df["payment_mode"] != canonical_payment_mode(df["payment_mode"])))
_add("orders", "order_ts_valid", "Validity", CRITICAL, "order_ts is missing or not a valid timestamp",
     lambda df, c: _ts(df, "order_ts").isna())
_add("orders", "order_ts_not_future", "Timeliness", CRITICAL, "order_ts is in the future",
     lambda df, c: _ts(df, "order_ts") > c.as_of)
_add("orders", "quantity_positive", "Accuracy", CRITICAL, "quantity is zero, negative or not a number",
     lambda df, c: ~(_num(df, "quantity") > 0))
_add("orders", "unit_price_positive", "Accuracy", CRITICAL, "unit_price is zero, negative or not a number",
     lambda df, c: ~(_num(df, "unit_price") > 0))
_add("orders", "discount_pct_in_range", "Validity", CRITICAL, "discount_pct is outside 0-100",
     lambda df, c: ~_num(df, "discount_pct").between(0, 100))
_add("orders", "order_amount_matches_formula", "Accuracy", CRITICAL,
     "order_amount != quantity x unit_price x (1 - discount_pct/100)", _amount_mismatch)
_add("orders", "delivery_minutes_below_120", "Accuracy", CRITICAL,
     "delivery_minutes is 120 or more (impossible for a 10-minute service)",
     lambda df, c: _num(df, "delivery_minutes") >= 120)
_add("orders", "delivery_minutes_normal_range", "Accuracy", WARNING,
     "delivery_minutes outside the normal 8-35 range",
     lambda df, c: (lambda m: (m < 8) | ((m > 35) & (m < 120)))(_num(df, "delivery_minutes")))
_add("orders", "channel_valid", "Validity", WARNING, "channel is not App/Web",
     lambda df, c: ~_s(df, "channel").isin(VALID_CHANNELS))
_add("orders", "customer_id_in_master", "Consistency", CRITICAL, "customer_id not found in customers",
     lambda df, c: (_s(df, "customer_id") != "") & ~_s(df, "customer_id").isin(c.customer_ids))
_add("orders", "product_id_in_master", "Consistency", CRITICAL, "product_id not found in products",
     lambda df, c: ~_s(df, "product_id").isin(c.product_ids))
_add("orders", "store_id_in_master", "Consistency", CRITICAL, "store_id not found in stores",
     lambda df, c: ~_s(df, "store_id").isin(c.store_ids))


def _stale(col):
    def fn(df, c):
        if c.max_age_days is None:
            return False
        ts = _ts(df, col)
        ts = ts[ts <= c.as_of]
        return bool(ts.empty or ts.max() < c.as_of - pd.Timedelta(days=c.max_age_days))
    return fn


_add("orders", "latest_order_not_stale", "Timeliness", WARNING,
     "latest valid order_ts is older than the freshness limit", _stale("order_ts"), scope="dataset")

# =========================================================================== #
# PAYMENTS
# =========================================================================== #
_add("payments", "payment_id_not_null", "Completeness", CRITICAL, "payment_id is missing",
     lambda df, c: _s(df, "payment_id") == "")
_add("payments", "order_id_not_null", "Completeness", CRITICAL, "order_id is missing",
     lambda df, c: _s(df, "order_id") == "")
_add("payments", "gateway_ref_present_for_non_cod", "Completeness", WARNING,
     "non-COD payment has no gateway_ref",
     lambda df, c: (canonical_payment_mode(df["payment_mode"]) != "Cash on Delivery") & (_s(df, "gateway_ref") == ""))
_add("payments", "payment_id_unique", "Uniqueness", CRITICAL, "duplicate payment_id (repeat of an earlier row)",
     lambda df, c: (_s(df, "payment_id") != "") & _s(df, "payment_id").duplicated(keep="first"))
_add("payments", "payment_status_valid", "Validity", CRITICAL, "payment_status is not SUCCESS/FAILED/REFUNDED",
     lambda df, c: ~_s(df, "payment_status").isin(VALID_PAYMENT_STATUS))
_add("payments", "payment_mode_valid", "Validity", CRITICAL,
     "payment_mode is not UPI/Card/Cash on Delivery/Wallet",
     lambda df, c: canonical_payment_mode(df["payment_mode"]).isna())
_add("payments", "payment_mode_needs_standardising", "Validity", WARNING,
     "payment_mode spelling/casing will be standardised",
     lambda df, c: canonical_payment_mode(df["payment_mode"]).notna() & (df["payment_mode"] != canonical_payment_mode(df["payment_mode"])))
_add("payments", "payment_ts_valid", "Validity", CRITICAL, "payment_ts is missing or not a valid timestamp",
     lambda df, c: _ts(df, "payment_ts").isna())
_add("payments", "payment_ts_not_future", "Timeliness", CRITICAL, "payment_ts is in the future",
     lambda df, c: _ts(df, "payment_ts") > c.as_of)
_add("payments", "amount_paid_positive", "Accuracy", CRITICAL, "amount_paid is zero, negative or not a number",
     lambda df, c: ~(_num(df, "amount_paid") > 0))
_add("payments", "order_id_in_orders", "Consistency", CRITICAL, "order_id not found in orders (orphan payment)",
     lambda df, c: (_s(df, "order_id") != "") & ~_s(df, "order_id").isin(c.order_ids))


def _success_amount_mismatch(df, c):
    oid = _s(df, "order_id")
    expected = oid.map(c.order_amounts) if len(c.order_amounts) else pd.Series(np.nan, index=df.index)
    expected = pd.to_numeric(expected, errors="coerce")
    paid = _num(df, "amount_paid")
    return (_s(df, "payment_status") == "SUCCESS") & expected.notna() & ~((paid - expected).abs() <= 0.01)


_add("payments", "success_amount_matches_order", "Consistency", WARNING,
     "SUCCESS amount_paid differs from the order's order_amount (see reconciliation)", _success_amount_mismatch)
_add("payments", "latest_payment_not_stale", "Timeliness", WARNING,
     "latest valid payment_ts is older than the freshness limit", _stale("payment_ts"), scope="dataset")

# =========================================================================== #
# CUSTOMERS
# =========================================================================== #
_add("customers", "customer_id_not_null", "Completeness", CRITICAL, "customer_id is missing",
     lambda df, c: _s(df, "customer_id") == "")
_add("customers", "full_name_not_null", "Completeness", WARNING, "full_name is missing",
     lambda df, c: _s(df, "full_name") == "")
_add("customers", "city_not_null", "Completeness", WARNING, "city is missing",
     lambda df, c: _s(df, "city") == "")
_add("customers", "customer_id_unique", "Uniqueness", CRITICAL, "duplicate customer_id (repeat of an earlier row)",
     lambda df, c: (_s(df, "customer_id") != "") & _s(df, "customer_id").duplicated(keep="first"))
_add("customers", "email_valid", "Validity", WARNING, "email is missing or malformed",
     lambda df, c: ~_s(df, "email").str.match(EMAIL_RE))
_add("customers", "phone_is_10_digits", "Validity", WARNING, "phone is not exactly 10 digits",
     lambda df, c: ~_s(df, "phone").str.match(r"^\d{10}$"))
_add("customers", "pincode_is_6_digits", "Validity", WARNING, "pincode is not exactly 6 digits",
     lambda df, c: ~_s(df, "pincode").str.match(r"^\d{6}$"))
_add("customers", "loyalty_tier_valid", "Validity", CRITICAL, "loyalty_tier is not Bronze/Silver/Gold/Platinum",
     lambda df, c: canonical_tier(df["loyalty_tier"]).isna())
_add("customers", "loyalty_tier_needs_standardising", "Consistency", WARNING,
     "loyalty_tier casing will be standardised",
     lambda df, c: canonical_tier(df["loyalty_tier"]).notna() & (df["loyalty_tier"] != canonical_tier(df["loyalty_tier"])))
_add("customers", "is_active_valid", "Validity", WARNING, "is_active is not Y/N",
     lambda df, c: ~_s(df, "is_active").isin({"Y", "N"}))
_add("customers", "signup_date_valid", "Validity", CRITICAL, "signup_date is missing or not a valid date",
     lambda df, c: _date(df, "signup_date").isna())
_add("customers", "signup_date_not_future", "Timeliness", CRITICAL, "signup_date is in the future",
     lambda df, c: _date(df, "signup_date") > c.as_of)

# =========================================================================== #
# PRODUCTS
# =========================================================================== #
def _product_superseded(df, c):
    """Duplicate product_id -> keep the current row, flag the rest.
    A row whose name contains '(old)' always loses; otherwise the first row wins."""
    pid = _s(df, "product_id")
    is_old = _s(df, "product_name").str.contains(r"\(old\)", case=False, regex=True)
    ranked = pd.DataFrame({"pid": pid, "old": is_old.astype(int)}, index=df.index)
    keep = ranked[ranked.pid != ""].sort_values("old", kind="stable").drop_duplicates("pid").index
    return (pid != "") & ~df.index.isin(keep)


_add("products", "product_id_not_null", "Completeness", CRITICAL, "product_id is missing",
     lambda df, c: _s(df, "product_id") == "")
_add("products", "category_not_null", "Completeness", CRITICAL, "category is missing",
     lambda df, c: _s(df, "category") == "")
_add("products", "product_id_unique", "Uniqueness", CRITICAL,
     "duplicate product_id (superseded or repeated row)", _product_superseded)
_add("products", "mrp_positive", "Accuracy", CRITICAL, "mrp is zero, negative or not a number",
     lambda df, c: ~(_num(df, "mrp") > 0))
_add("products", "cost_price_positive", "Accuracy", CRITICAL, "cost_price is zero, negative or not a number",
     lambda df, c: ~(_num(df, "cost_price") > 0))
_add("products", "cost_price_not_above_mrp", "Accuracy", CRITICAL, "cost_price is higher than mrp",
     lambda df, c: _num(df, "cost_price") > _num(df, "mrp"))
_add("products", "unit_valid", "Validity", WARNING, "unit is not kg/pack/unit",
     lambda df, c: ~_s(df, "unit").isin(VALID_UNITS))
_add("products", "is_perishable_valid", "Validity", WARNING, "is_perishable is not True/False",
     lambda df, c: ~_s(df, "is_perishable").isin({"True", "False"}))
_add("products", "launch_date_valid", "Validity", CRITICAL, "launch_date is missing or not a valid date",
     lambda df, c: _date(df, "launch_date").isna())
_add("products", "launch_date_not_future", "Timeliness", CRITICAL, "launch_date is in the future",
     lambda df, c: _date(df, "launch_date") > c.as_of)

# =========================================================================== #
# STORES
# =========================================================================== #
_add("stores", "store_id_not_null", "Completeness", CRITICAL, "store_id is missing",
     lambda df, c: _s(df, "store_id") == "")
_add("stores", "zone_not_null", "Completeness", CRITICAL, "zone is missing",
     lambda df, c: _s(df, "zone") == "")
_add("stores", "store_id_unique", "Uniqueness", CRITICAL, "duplicate store_id (repeat of an earlier row)",
     lambda df, c: (_s(df, "store_id") != "") & _s(df, "store_id").duplicated(keep="first"))
_add("stores", "zone_valid", "Validity", CRITICAL, "zone is not North/South/East/West",
     lambda df, c: (_s(df, "zone") != "") & ~_s(df, "zone").isin(VALID_ZONES))
_add("stores", "pincode_is_6_digits", "Validity", WARNING, "pincode is not exactly 6 digits",
     lambda df, c: ~_s(df, "pincode").str.match(r"^\d{6}$"))
_add("stores", "capacity_positive", "Accuracy", WARNING, "capacity_orders_per_day is zero, negative or not a number",
     lambda df, c: ~(_num(df, "capacity_orders_per_day") > 0))
_add("stores", "open_date_valid", "Validity", CRITICAL, "open_date is missing or not a valid date",
     lambda df, c: _date(df, "open_date").isna())
_add("stores", "open_date_not_future", "Timeliness", CRITICAL, "open_date is in the future",
     lambda df, c: _date(df, "open_date") > c.as_of)

# =========================================================================== #
# Volume check for every table (only active when a baseline is supplied)
# =========================================================================== #
def _volume_drop(table):
    def fn(df, c):
        base = c.baseline_rows.get(table)
        return bool(base) and len(df) < base * (1 - c.volume_drop_threshold)
    return fn


for _t in list(CHECKS):
    _add(_t, "volume_not_collapsed", "Completeness", WARNING,
         "row count dropped by more than 90% versus the baseline", _volume_drop(_t), scope="dataset")


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #
RESULT_COLUMNS = ["run_id", "dag_id", "check_name", "dimension", "severity",
                  "rows_checked", "rows_failed", "status", "checked_at"]


def run_checks(df: pd.DataFrame, table: str, ctx: Context, run_id: str, dag_id: str):
    """Run every check for `table`.

    Returns (passed, failed, results)
      passed  : rows with no Critical failure. Extra column `dq_warnings`
                (text of any Warning problems) -- drop it before loading.
      failed  : rows with >=1 Critical failure. Extra columns `failure_reason`,
                `failed_checks` and `_source_row` (line number in the CSV file,
                header = line 1) so every rejected row can be traced back.
      results : one row per check in the dq_results layout.
    """
    if not df.index.is_unique:
        raise ValueError("run_checks needs a DataFrame with a unique index (read straight from the CSV)")
    n = len(df)
    crit_reasons = [[] for _ in range(n)]
    crit_names = [[] for _ in range(n)]
    warn_reasons = [[] for _ in range(n)]
    rows = []
    checked_at = _now()

    for chk in CHECKS[table]:
        if chk.scope == "dataset":
            fired = bool(chk.fn(df, ctx))
            failed_count = n if fired else 0
        else:
            mask = chk.fn(df, ctx)
            arr = pd.Series(mask, index=df.index).fillna(False).astype(bool).to_numpy()
            failed_count = int(arr.sum())
            for k in np.flatnonzero(arr):
                if chk.severity == CRITICAL:
                    crit_reasons[k].append(chk.reason)
                    crit_names[k].append(chk.name)
                else:
                    warn_reasons[k].append(chk.reason)

        status = "PASS" if failed_count == 0 else ("FAIL" if chk.severity == CRITICAL else "WARN")
        rows.append({
            "run_id": run_id, "dag_id": dag_id, "check_name": chk.full_name,
            "dimension": chk.dimension, "severity": chk.severity,
            "rows_checked": n, "rows_failed": failed_count, "status": status,
            "checked_at": checked_at,
        })

    has_crit = np.array([bool(r) for r in crit_reasons], dtype=bool)
    passed = df.loc[~has_crit].copy()
    passed["dq_warnings"] = ["; ".join(w) for w, bad in zip(warn_reasons, has_crit) if not bad]
    failed = df.loc[has_crit].copy()
    failed["failure_reason"] = ["; ".join(r) for r, bad in zip(crit_reasons, has_crit) if bad]
    failed["failed_checks"] = [", ".join(r) for r, bad in zip(crit_names, has_crit) if bad]
    failed["_source_row"] = failed.index + 2
    return passed, failed, pd.DataFrame(rows, columns=RESULT_COLUMNS)


# --------------------------------------------------------------------------- #
# Writers
# --------------------------------------------------------------------------- #
def write_quarantine(failed: pd.DataFrame, directory, table: str, run_id: str) -> Optional[Path]:
    """Write rejected rows to <directory>/<table>_failed_<run_id>.csv (None if there are none)."""
    if failed.empty:
        return None
    safe_run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", str(run_id))
    path = Path(directory) / f"{table}_failed_{safe_run_id}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    failed.to_csv(path, index=False)
    return path


DQ_RESULTS_DDL = """
CREATE TABLE IF NOT EXISTS dq_results (
    run_id       VARCHAR,
    dag_id       VARCHAR,
    check_name   VARCHAR,
    dimension    VARCHAR,
    severity     VARCHAR,
    rows_checked BIGINT,
    rows_failed  BIGINT,
    status       VARCHAR,
    checked_at   TIMESTAMP
)
"""


def write_dq_results(con, results: pd.DataFrame) -> None:
    """Append results to dq_results in a DuckDB connection.

    Safe to retry: rows for the same (run_id, dag_id, check_name) are replaced,
    so an Airflow task retry never double-writes.
    """
    con.execute(DQ_RESULTS_DDL)
    con.register("_dq_incoming", results)
    con.execute(
        """DELETE FROM dq_results WHERE (run_id, dag_id, check_name) IN
           (SELECT run_id, dag_id, check_name FROM _dq_incoming)"""
    )
    con.execute(
        """INSERT INTO dq_results
           SELECT run_id, dag_id, check_name, dimension, severity,
                  rows_checked, rows_failed, status, checked_at
           FROM _dq_incoming"""
    )
    con.unregister("_dq_incoming")
