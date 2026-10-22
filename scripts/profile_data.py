"""
Part 0 - discovery and profiling. Independent of the pipelines (plain pandas) so it can cross-check them.
Writes docs/profile_summary.md and docs/data_issues_log.csv (one row per issue instance).
  python scripts/profile_data.py
"""
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
D = ROOT / "include" / "data" / "nammamart"
OUT = ROOT / "docs"
OUT.mkdir(exist_ok=True)
AS_OF = pd.Timestamp("2026-10-02 09:00:00")

files = ["orders", "payments", "customers", "products", "stores", "orders_2026-10-01_increment"]
df = {f: pd.read_csv(D / f"{f}.csv", dtype=str, keep_default_na=False) for f in files}
o, p, c, pr, s, inc = (df[f] for f in files)
num = lambda x: pd.to_numeric(x, errors="coerce")  # noqa: E731
ts = lambda x: pd.to_datetime(x, format="%Y-%m-%d %H:%M:%S", errors="coerce")  # noqa: E731

issues = []


def log(file, frame, mask, key, column, problem, dim, rule, sev):
    for i in frame.index[mask]:
        issues.append({"file": file, "row_key": frame.at[i, key], "csv_line": i + 2, "column": column,
                       "problem": problem, "quality_dimension": dim, "proposed_rule": rule, "severity": sev})


# ---------------------------------------------------------------- orders
f, k = "orders.csv", "order_id"
log(f, o, o.duplicated(keep="first"), k, "order_id", "exact duplicate of an earlier row", "Uniqueness", "order_id unique", "Critical")
log(f, o, o.customer_id.eq(""), k, "customer_id", "customer_id is empty", "Completeness", "customer_id not null", "Critical")
log(f, o, o.order_ts.eq(""), k, "order_ts", "order_ts is empty", "Completeness", "order_ts not null", "Critical")
t = ts(o.order_ts)
log(f, o, t.isna() & o.order_ts.ne(""), k, "order_ts", "order_ts not a valid timestamp (e.g. N/A, 31/09/2026)", "Validity", "order_ts parses as YYYY-MM-DD HH:MM:SS", "Critical")
log(f, o, t > AS_OF, k, "order_ts", "order_ts is in the future (2027)", "Timeliness", "order_ts <= run time", "Critical")
log(f, o, ~o.order_status.str.strip().str.upper().isin(["DELIVERED", "CANCELLED", "RETURNED"]), k, "order_status", "status not in DELIVERED/CANCELLED/RETURNED (Pending??)", "Validity", "status in allowed list", "Critical")
log(f, o, o.order_status.isin(["delivered"]), k, "order_status", "status has wrong casing (delivered)", "Consistency", "upper-case before validating", "Warning")
mode = o.payment_mode.str.strip().str.lower().map({"upi": "UPI", "card": "Card", "cash on delivery": "Cash on Delivery", "cod": "Cash on Delivery", "wallet": "Wallet"})
log(f, o, mode.isna(), k, "payment_mode", "payment_mode not recognisable (Cashh)", "Validity", "payment_mode in allowed list", "Critical")
log(f, o, mode.notna() & o.payment_mode.isin(["upi", "UPI ", "CARD", "cod"]), k, "payment_mode", "payment_mode needs standardising (upi, 'UPI ', CARD, cod)", "Consistency", "trim + map to canonical value", "Warning")
log(f, o, ~(num(o.quantity) > 0), k, "quantity", "quantity zero or negative", "Accuracy", "quantity > 0", "Critical")
log(f, o, ~num(o.discount_pct).between(0, 100), k, "discount_pct", "discount_pct outside 0-100", "Validity", "0 <= discount_pct <= 100", "Critical")
exp = (num(o.quantity) * num(o.unit_price) * (1 - num(o.discount_pct) / 100)).round(2)
log(f, o, ~((exp - num(o.order_amount)).abs() <= 0.01) & (num(o.quantity) > 0) & num(o.discount_pct).between(0, 100), k, "order_amount", "order_amount != quantity x unit_price x (1 - discount/100)", "Accuracy", "recompute and compare (tolerance 0.01)", "Critical")
log(f, o, o.order_status.str.upper().eq("DELIVERED") & o.delivery_minutes.eq(""), k, "delivery_minutes", "DELIVERED order without delivery_minutes", "Completeness", "delivered => delivery_minutes present", "Critical")
log(f, o, num(o.delivery_minutes) >= 120, k, "delivery_minutes", "impossible delivery time (480-1440 minutes)", "Accuracy", "delivery_minutes < 120", "Critical")
log(f, o, o.order_status.str.upper().eq("DELIVERED") & num(o.delivery_minutes).between(0, 119) & ((num(o.delivery_minutes) < 8) | (num(o.delivery_minutes) > 35)), k, "delivery_minutes", "delivery outside the normal 8-35 minute band", "Accuracy", "warn when outside 8-35", "Warning")
log(f, o, ~o.customer_id.isin(c.customer_id) & o.customer_id.ne(""), k, "customer_id", "customer_id not in customers.csv", "Consistency", "FK to customers", "Critical")
log(f, o, ~o.product_id.isin(pr.product_id), k, "product_id", "product_id not in products.csv (P9xxx)", "Consistency", "FK to products", "Critical")
log(f, o, ~o.store_id.isin(s.store_id), k, "store_id", "store_id not in stores.csv (S99)", "Consistency", "FK to stores", "Critical")

# ---------------------------------------------------------------- payments
f, k = "payments.csv", "payment_id"
log(f, p, p.duplicated(keep="first"), k, "payment_id", "exact duplicate of an earlier row", "Uniqueness", "payment_id unique", "Critical")
log(f, p, ~p.order_id.isin(o.order_id), k, "order_id", "payment for an order that does not exist (ORD97xxxx)", "Consistency", "FK to orders", "Critical")
log(f, p, num(p.amount_paid) <= 0, k, "amount_paid", "SUCCESS payment with zero/negative amount", "Accuracy", "amount_paid > 0", "Critical")
log(f, p, ts(p.payment_ts) > AS_OF, k, "payment_ts", "payment_ts after the run time (2026-10-21, 2026-11-02, 2027-01-01 values)", "Timeliness", "payment_ts <= run time", "Critical")
pm = p.payment_mode.str.strip().str.lower().map({"upi": "UPI", "card": "Card", "cash on delivery": "Cash on Delivery", "cod": "Cash on Delivery", "wallet": "Wallet"})
log(f, p, pm.isna(), k, "payment_mode", "payment_mode not recognisable (Cashh)", "Validity", "payment_mode in allowed list", "Critical")
log(f, p, pm.notna() & p.payment_mode.isin(["upi", "UPI ", "CARD", "cod"]), k, "payment_mode", "payment_mode needs standardising", "Consistency", "trim + map", "Warning")
log(f, p, p.gateway_ref.eq("") & pm.ne("Cash on Delivery"), k, "gateway_ref", "gateway_ref empty for an online payment", "Completeness", "gateway_ref present unless COD", "Warning")
m = p.merge(o.drop_duplicates("order_id"), on="order_id", suffixes=("", "_o"))
bad = m[(m.payment_status == "SUCCESS") & ((num(m.amount_paid) - num(m.order_amount)).abs() > 0.01) & (num(m.amount_paid) > 0)]
for _, r in bad.iterrows():
    issues.append({"file": f, "row_key": r.payment_id, "csv_line": "", "column": "amount_paid", "problem": f"SUCCESS amount {r.amount_paid} != order_amount {r.order_amount} (order {r.order_id})",
                   "quality_dimension": "Consistency", "proposed_rule": "SUCCESS amount == order_amount -> reconciliation table (not quarantine)", "severity": "Warning"})

# ---------------------------------------------------------------- customers
f, k = "customers.csv", "customer_id"
log(f, c, c.customer_id.duplicated(keep="first"), k, "customer_id", "customer_id repeated with different phone/area", "Uniqueness", "one row per customer_id; keep first", "Critical")
log(f, c, c.city.eq(""), k, "city", "city is empty", "Completeness", "city not null", "Warning")
log(f, c, ~c.email.str.fullmatch(r"[^@\s]+@[^@\s]+\.[A-Za-z]{2,}"), k, "email", "invalid email (N/A, missing @ or dot)", "Validity", "email regex", "Warning")
log(f, c, ~c.phone.str.fullmatch(r"\d{10}"), k, "phone", "phone is not 10 digits", "Validity", "phone 10 digits", "Warning")
log(f, c, ~c.pincode.str.fullmatch(r"\d{6}"), k, "pincode", "pincode is not 6 digits", "Validity", "pincode 6 digits", "Warning")
log(f, c, ~c.loyalty_tier.isin(["Bronze", "Silver", "Gold", "Platinum"]), k, "loyalty_tier", "loyalty_tier casing differs (BRONZE, bronze, SILVER)", "Consistency", "standardise to Title case", "Warning")
log(f, c, pd.to_datetime(c.signup_date, errors="coerce") > AS_OF, k, "signup_date", "signup_date in the future (Nov 2026 - Jan 2027)", "Timeliness", "signup_date <= run time", "Critical")

# ---------------------------------------------------------------- products
f, k = "products.csv", "product_id"
log(f, pr, pr.product_id.duplicated(keep=False) & pr.product_name.str.endswith("(old)"), k, "product_id", "stale '(old)' catalogue row shares the product_id with the current row", "Uniqueness", "one row per product_id; prefer the non-(old) row", "Critical")
log(f, pr, pr.category.eq(""), k, "category", "category is empty", "Completeness", "category not null", "Critical")
log(f, pr, ~(num(pr.mrp) > 0), k, "mrp", "mrp zero or negative", "Accuracy", "mrp > 0", "Critical")
log(f, pr, num(pr.cost_price) > num(pr.mrp), k, "cost_price", "cost_price above mrp", "Accuracy", "cost_price <= mrp", "Critical")

# ---------------------------------------------------------------- stores
f, k = "stores.csv", "store_id"
log(f, s, s.zone.eq(""), k, "zone", "zone is empty (S07 BTM Layout)", "Completeness", "zone not null", "Critical")
log(f, s, num(s.capacity_orders_per_day) <= 0, k, "capacity_orders_per_day", "capacity is 0", "Accuracy", "capacity > 0", "Warning")

# ---------------------------------------------------------------- increment
f, k = "orders_2026-10-01_increment.csv", "order_id"
o_ids = set(o.order_id)
corr = inc.order_id.isin(o_ids)
for i in inc.index[corr]:
    issues.append({"file": f, "row_key": inc.at[i, "order_id"], "csv_line": i + 2, "column": "order_status",
                   "problem": "correction of an existing order (status -> RETURNED); must update, not insert", "quality_dimension": "Consistency",
                   "proposed_rule": "upsert on order_id", "severity": "Warning"})
log(f, inc, ~inc.product_id.isin(pr.product_id) | ~inc.store_id.isin(s.store_id), k, "product_id/store_id", "references an unknown product/store", "Consistency", "FK to masters", "Critical")

log_df = pd.DataFrame(issues)
log_df.to_csv(OUT / "data_issues_log.csv", index=False)

# ---------------------------------------------------------------- profile summary
lines = ["# Data profile (generated by scripts/profile_data.py)\n"]
for name in files:
    d = df[name]
    lines.append(f"## {name}.csv - {len(d)} rows\n")
    nulls = {col: int((d[col].str.strip() == "").sum()) for col in d.columns if (d[col].str.strip() == "").any()}
    lines.append(f"- empty values per column: {nulls or 'none'}")
    key = d.columns[0]
    lines.append(f"- duplicate `{key}`: {int(d[key].duplicated().sum())}; fully duplicated rows: {int(d.duplicated().sum())}")
    for col in d.columns:
        n = num(d[col])
        if col not in ("order_id", "payment_id") and n.notna().mean() > 0.9 and not col.endswith("_id") and col not in ("pincode", "phone", "gateway_ref"):
            lines.append(f"- `{col}`: min {n.min():g}, max {n.max():g}")
        elif "ts" in col or col.endswith("_date"):
            t = pd.to_datetime(d[col], errors="coerce")
            lines.append(f"- `{col}`: min {t.min()}, max {t.max()}, unparseable {int(t.isna().sum() - (d[col] == '').sum())}")
    for col in ("order_status", "payment_mode", "payment_status", "channel", "loyalty_tier", "zone", "category", "is_active"):
        if col in d.columns:
            lines.append(f"- distinct `{col}`: {d[col].value_counts().to_dict()}")
    lines.append("")
lines.append("## Referential integrity\n")
lines.append(f"- orders.customer_id -> customers: {int((~o.customer_id.isin(c.customer_id) & o.customer_id.ne('')).sum())} orphans (+ {int(o.customer_id.eq('').sum())} empty)")
lines.append(f"- orders.product_id -> products: {int((~o.product_id.isin(pr.product_id)).sum())} orphans")
lines.append(f"- orders.store_id -> stores: {int((~o.store_id.isin(s.store_id)).sum())} orphans")
lines.append(f"- payments.order_id -> orders: {int((~p.order_id.isin(o.order_id)).sum())} orphans")
lines.append(f"- DELIVERED orders without any payment: {int((o.order_status.str.upper().eq('DELIVERED') & ~o.order_id.isin(p.order_id)).sum())}")
lines.append(f"- CANCELLED orders with a payment: {int((o.order_status.eq('CANCELLED') & o.order_id.isin(p.order_id)).sum())}")
lines.append(f"- increment: {int(corr.sum())} rows correct existing orders, {int((~corr).sum())} are new\n")
lines.append("## Issues by file and dimension\n")
lines.append(log_df.groupby(["file", "quality_dimension"]).size().unstack(fill_value=0).to_markdown() if hasattr(log_df, "to_markdown") else str(log_df.groupby(["file", "quality_dimension"]).size()))
(OUT / "profile_summary.md").write_text("\n".join(lines), encoding="utf-8")
print(f"{len(log_df)} issue instances over {log_df.file.nunique()} files")
print(log_df.groupby(["file", "quality_dimension"]).size().to_string())
