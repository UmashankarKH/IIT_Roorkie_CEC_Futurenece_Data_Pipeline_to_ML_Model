"""
ETL data-quality layer (Python / pandas). One validate_* function per source file.

Each returns (passed_df, failed_df, results):
  * Critical checks quarantine the row (it lands in failed_df with a `failure_reason`).
  * Warning checks keep the row but record `warning_reason` and are counted in `results`.
  * results feeds the dq_results table: one dict per check per run.

The SQL twin of every rule lives in include/sql/elt/20_staging.sql and uses the same check names,
so dq_results from the two pipelines can be compared line by line.
"""
import numpy as np
import pandas as pd

TS_FORMAT = "%Y-%m-%d %H:%M:%S"
VALID_STATUS = {"DELIVERED", "CANCELLED", "RETURNED"}
VALID_PAYMENT_MODE = {"UPI", "Card", "Cash on Delivery", "Wallet"}
VALID_PAYMENT_STATUS = {"SUCCESS", "FAILED", "REFUNDED"}
VALID_CHANNEL = {"App", "Web"}
VALID_ZONE = {"North", "South", "East", "West"}
VALID_TIER = {"Bronze", "Silver", "Gold", "Platinum"}
PAYMENT_MODE_MAP = {"upi": "UPI", "card": "Card", "cash on delivery": "Cash on Delivery",
                    "cod": "Cash on Delivery", "wallet": "Wallet"}
EMAIL_RE = r"[^@\s]+@[^@\s]+\.[A-Za-z]{2,}"


class Checker:
    """Collects check masks (True = row failed) and turns them into reasons + dq_results rows."""

    def __init__(self, df):
        self.n = len(df)
        self.index = df.index
        self.critical = []   # (name, mask)
        self.warning = []
        self.results = []

    def add(self, name, dimension, severity, failed):
        failed = pd.Series(np.asarray(failed, dtype=bool), index=self.index)
        (self.critical if severity == "Critical" else self.warning).append((name, failed))
        self.results.append({"check_name": name, "dimension": dimension, "severity": severity,
                             "rows_checked": self.n, "rows_failed": int(failed.sum())})

    @staticmethod
    def _reasons(checks, index):
        out = pd.Series([""] * len(index), index=index, dtype=object)
        for name, mask in checks:
            out = out.where(~mask, out.where(out == "", out + "; ") + name)
        return out

    def split(self, df):
        df = df.copy()
        df["failure_reason"] = self._reasons(self.critical, self.index)
        df["warning_reason"] = self._reasons(self.warning, self.index)
        failed = df["failure_reason"] != ""
        return df[~failed].drop(columns="failure_reason"), df[failed].drop(columns="warning_reason")


def _num(s):
    return pd.to_numeric(s, errors="coerce")


def _ts(s):
    return pd.to_datetime(s, format=TS_FORMAT, errors="coerce")


def _date(s):
    return pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")


def _blank(s):
    return s.str.strip().eq("")


def _dup(s):
    """True for every occurrence after the first."""
    return s.duplicated(keep="first")


def _trim(df):
    return df.assign(**{c: df[c].str.strip() for c in df.columns if c != "_row_num"})


def standardise_payment_mode(s):
    s = s.str.strip()
    return s.str.lower().map(PAYMENT_MODE_MAP).fillna(s)


# ---------------------------------------------------------------- stores
def validate_stores(df, as_of):
    df = _trim(df)
    c = Checker(df)
    c.add("stores_store_id_not_null", "Completeness", "Critical", _blank(df.store_id))
    c.add("stores_zone_not_null", "Completeness", "Critical", _blank(df.zone))
    c.add("stores_store_id_unique", "Uniqueness", "Critical", _dup(df.store_id))
    c.add("stores_zone_valid", "Validity", "Critical", ~_blank(df.zone) & ~df.zone.isin(VALID_ZONE))
    c.add("stores_pincode_6_digits", "Validity", "Warning", ~df.pincode.str.fullmatch(r"\d{6}"))
    c.add("stores_capacity_positive", "Accuracy", "Warning", ~(_num(df.capacity_orders_per_day) > 0))
    c.add("stores_open_date_not_future", "Timeliness", "Critical", _date(df.open_date) > as_of)
    passed, failed = c.split(df)
    return passed, failed, c.results


# ---------------------------------------------------------------- products
def validate_products(df, as_of):
    df = _trim(df)
    mrp, cost = _num(df.mrp), _num(df.cost_price)
    c = Checker(df)
    c.add("products_product_id_not_null", "Completeness", "Critical", _blank(df.product_id))
    c.add("products_category_not_null", "Completeness", "Critical", _blank(df.category))
    # Keep the current row of a duplicated product_id, never the "(old)" catalogue entry.
    ranked = df.assign(_old=df.product_name.str.endswith("(old)")).sort_values(["_old", "_row_num"])
    rank = ranked.groupby("product_id").cumcount().reindex(df.index)
    c.add("products_product_id_unique", "Uniqueness", "Critical", rank > 0)
    c.add("products_mrp_positive", "Accuracy", "Critical", ~(mrp > 0))
    c.add("products_cost_price_positive", "Accuracy", "Critical", ~(cost > 0))
    c.add("products_cost_not_above_mrp", "Accuracy", "Critical", cost > mrp)
    c.add("products_launch_date_not_future", "Timeliness", "Critical", _date(df.launch_date) > as_of)
    c.add("products_is_perishable_valid", "Validity", "Warning", ~df.is_perishable.isin(["True", "False"]))
    passed, failed = c.split(df)
    return passed, failed, c.results


# ---------------------------------------------------------------- customers
def validate_customers(df, as_of):
    df = _trim(df)
    c = Checker(df)
    c.add("customers_customer_id_not_null", "Completeness", "Critical", _blank(df.customer_id))
    c.add("customers_city_not_null", "Completeness", "Warning", _blank(df.city))
    c.add("customers_customer_id_unique", "Uniqueness", "Critical", _dup(df.customer_id))
    c.add("customers_email_format", "Validity", "Warning", ~df.email.str.fullmatch(EMAIL_RE))
    c.add("customers_phone_10_digits", "Validity", "Warning", ~df.phone.str.fullmatch(r"\d{10}"))
    c.add("customers_pincode_6_digits", "Validity", "Warning", ~df.pincode.str.fullmatch(r"\d{6}"))
    c.add("customers_signup_date_parses", "Validity", "Critical", _date(df.signup_date).isna())
    c.add("customers_is_active_valid", "Validity", "Warning", ~df.is_active.isin(["Y", "N"]))
    c.add("customers_loyalty_tier_casing", "Consistency", "Warning", ~df.loyalty_tier.isin(VALID_TIER))
    c.add("customers_signup_date_not_future", "Timeliness", "Critical", _date(df.signup_date) > as_of)
    passed, failed = c.split(df)
    passed["loyalty_tier"] = passed.loyalty_tier.str.title()
    return passed, failed, c.results


# ---------------------------------------------------------------- orders
def validate_orders(df, as_of, good_customers, good_products, good_stores, product_mrp):
    """good_* are the key sets of the dimension rows that *passed* validation; product_mrp maps product_id -> mrp."""
    df = _trim(df)
    df["order_status"] = df.order_status.str.upper()
    df["payment_mode"] = standardise_payment_mode(df.payment_mode)
    ts = _ts(df.order_ts)
    qty, price, disc = _num(df.quantity), _num(df.unit_price), _num(df.discount_pct)
    amount, mins = _num(df.order_amount), _num(df.delivery_minutes)
    expected = (qty * price * (1 - disc / 100)).round(2)
    delivered = df.order_status.eq("DELIVERED")
    c = Checker(df)
    c.add("orders_order_id_not_null", "Completeness", "Critical", _blank(df.order_id))
    c.add("orders_customer_id_not_null", "Completeness", "Critical", _blank(df.customer_id))
    c.add("orders_order_ts_not_null", "Completeness", "Critical", _blank(df.order_ts))
    c.add("orders_delivered_has_delivery_minutes", "Completeness", "Critical", delivered & mins.isna())
    c.add("orders_order_id_unique", "Uniqueness", "Critical", _dup(df.order_id))
    c.add("orders_status_valid", "Validity", "Critical", ~df.order_status.isin(VALID_STATUS))
    c.add("orders_payment_mode_valid", "Validity", "Critical", ~df.payment_mode.isin(VALID_PAYMENT_MODE))
    c.add("orders_order_ts_parses", "Validity", "Critical", ts.isna() & ~_blank(df.order_ts))
    c.add("orders_discount_pct_0_100", "Validity", "Critical", ~disc.between(0, 100))
    c.add("orders_channel_valid", "Validity", "Warning", ~df.channel.isin(VALID_CHANNEL))
    c.add("orders_quantity_positive", "Accuracy", "Critical", ~(qty > 0))
    c.add("orders_unit_price_positive", "Accuracy", "Critical", ~(price > 0))
    c.add("orders_amount_matches_formula", "Accuracy", "Critical", ~((expected - amount).abs() <= 0.01))
    c.add("orders_delivery_minutes_below_120", "Accuracy", "Critical", mins >= 120)
    c.add("orders_delivery_minutes_normal_8_35", "Accuracy", "Warning",
          delivered & mins.notna() & ((mins < 8) | (mins > 35)))
    mrp = df.product_id.map(product_mrp).astype(float)
    c.add("orders_unit_price_within_mrp", "Accuracy", "Warning", price > 1.1 * mrp)  # drift guard: price vs catalogue
    c.add("orders_customer_in_clean_customers", "Consistency", "Critical",
          ~_blank(df.customer_id) & ~df.customer_id.isin(good_customers))
    c.add("orders_product_in_clean_products", "Consistency", "Critical", ~df.product_id.isin(good_products))
    c.add("orders_store_in_clean_stores", "Consistency", "Critical", ~df.store_id.isin(good_stores))
    c.add("orders_order_ts_not_future", "Timeliness", "Critical", ts > as_of)
    # newest *plausible* order: future-dated junk rows must not hide a stale feed
    plausible = ts[ts <= as_of]
    stale = int(plausible.empty or plausible.max() < as_of - pd.Timedelta(days=1))
    c.results.append({"check_name": "orders_latest_order_not_stale", "dimension": "Timeliness",
                      "severity": "Warning", "rows_checked": 1, "rows_failed": stale})
    passed, failed = c.split(df)
    return passed, failed, c.results


# ---------------------------------------------------------------- payments
def validate_payments(df, as_of, all_order_ids):
    """all_order_ids: every order_id present in orders.csv (trimmed), good or bad."""
    df = _trim(df)
    df["payment_mode"] = standardise_payment_mode(df.payment_mode)
    amt = _num(df.amount_paid)
    ts = _ts(df.payment_ts)
    c = Checker(df)
    c.add("payments_payment_id_not_null", "Completeness", "Critical", _blank(df.payment_id))
    c.add("payments_order_id_not_null", "Completeness", "Critical", _blank(df.order_id))
    c.add("payments_gateway_ref_present", "Completeness", "Warning",
          _blank(df.gateway_ref) & df.payment_mode.ne("Cash on Delivery"))
    c.add("payments_payment_id_unique", "Uniqueness", "Critical", _dup(df.payment_id))
    c.add("payments_payment_mode_valid", "Validity", "Critical", ~df.payment_mode.isin(VALID_PAYMENT_MODE))
    c.add("payments_status_valid", "Validity", "Critical", ~df.payment_status.isin(VALID_PAYMENT_STATUS))
    c.add("payments_payment_ts_parses", "Validity", "Critical", ts.isna())
    c.add("payments_amount_positive", "Accuracy", "Critical", amt.isna() | (amt <= 0))
    c.add("payments_order_exists", "Consistency", "Critical", ~df.order_id.isin(all_order_ids))
    c.add("payments_payment_ts_not_future", "Timeliness", "Critical", ts > as_of)
    passed, failed = c.split(df)
    return passed, failed, c.results
