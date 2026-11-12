#!/usr/bin/env python3
"""
NammaMart Part 0 - follow-up checks for the issue review.

The profiler tells you WHAT is wrong. This script answers the open questions
from the review: WHY it is wrong, and whether some flags are really noise.

Run from the project root (or inside `astro dev bash`):
    python include/profiling/followup_checks.py
    python include/profiling/followup_checks.py path/to/data_dir

Personal data (names, emails, phones, manager names) is never printed.
"""
import re
import sys
from pathlib import Path

import pandas as pd

D = Path(sys.argv[1] if len(sys.argv) > 1 else "include/data/nammamart")
pd.set_option("display.width", 200, "display.max_columns", 20,
              "display.max_colwidth", 50, "display.max_rows", 200)

PII = {"full_name", "email", "phone", "manager_name"}
FUTURE = pd.Timestamp("2026-10-02")
SUSPICIOUS = r"(?i)\b(?:test|demo|dummy|sample|qa|internal|staging|do not|fake|xxx)\b"


def load(name):
    path = D / name
    return pd.read_csv(path, dtype=str, keep_default_na=False) if path.exists() else None


def T(s):
    return s.astype(str).str.strip()


def num(s):
    return pd.to_numeric(T(s), errors="coerce")


def ts(s):
    try:
        return pd.to_datetime(T(s), errors="coerce", format="mixed")
    except (TypeError, ValueError):
        return pd.to_datetime(T(s), errors="coerce")


def h(title):
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


o, p, c = load("orders.csv"), load("payments.csv"), load("customers.csv")
pr, s, inc = load("products.csv"), load("stores.csv"), load("orders_2026-10-01_increment.csv")

oid, st = T(o.order_id), T(o.order_status).str.upper()
omode = T(o.payment_mode).str.lower()
pid_pay, ps = T(p.order_id), T(p.payment_status).str.upper()
amt = num(p.amount_paid)
order_status_by_id = dict(zip(oid, st))

# ---------------------------------------------------------------------------
h("1. DELIVERED orders with no SUCCESS payment  (is it just Cash on Delivery?)")
succ_ids = set(pid_pay[ps == "SUCCESS"])
m = (st == "DELIVERED") & ~oid.isin(succ_ids)
print(f"{m.sum()} orders. Their order payment_mode:")
print(omode[m].value_counts().to_string())
print("\nFor comparison, payment_mode of ALL delivered orders:")
print(omode[st == "DELIVERED"].value_counts().to_string())
other = p[pid_pay.isin(oid[m])]
print(f"\nPayment rows that DO exist for these orders (non-SUCCESS): {len(other)}")
if len(other):
    print(ps[other.index].value_counts().to_string())

# ---------------------------------------------------------------------------
h("2. SUCCESS payments without gateway_ref  (is it just Cash on Delivery?)")
g = (ps == "SUCCESS") & (T(p.gateway_ref) == "")
print(f"{g.sum()} payments. By payment_mode:")
print(T(p.payment_mode)[g].str.lower().value_counts().to_string())
print("\nAll SUCCESS payments by mode, and how many of each have a gateway_ref:")
print(pd.crosstab(T(p.payment_mode)[ps == "SUCCESS"].str.lower(),
                  (T(p.gateway_ref) != "")[ps == "SUCCESS"].map({True: "has_ref", False: "no_ref"})))

# ---------------------------------------------------------------------------
h("3. payment_status values, and payments with amount_paid <= 0  (refunds?)")
print(ps.value_counts().to_string())
z = amt <= 0
print(f"\n{z.sum()} payments with amount_paid <= 0. payment_status vs the ORDER's status:")
print(pd.crosstab(ps[z], pid_pay[z].map(order_status_by_id).fillna("(order not found)")))
print(p.loc[z, ["payment_id", "order_id", "payment_mode", "amount_paid", "payment_status"]].to_string(index=False))

# ---------------------------------------------------------------------------
h("4. Payments whose order_id is not in orders.csv  (early payments for the increment?)")
orph = ~pid_pay.isin(set(oid)) & (pid_pay != "")
print(p.loc[orph, ["payment_id", "order_id", "payment_ts", "payment_status", "amount_paid"]].to_string(index=False))
if inc is not None:
    found = pid_pay[orph].isin(set(T(inc.order_id)))
    print(f"\n{found.sum()} of {orph.sum()} orphan payments match an order in the increment file.")

# ---------------------------------------------------------------------------
h("5. Future-dated timestamps  (same root cause in orders and payments?)")
ots, pts = ts(o.order_ts), ts(p.payment_ts)
fut_o = ots >= FUTURE
fut_p = pts >= FUTURE
print("Future order_ts values:", T(o.order_ts)[fut_o].tolist())
print("Future payment_ts values:", T(p.payment_ts)[fut_p].tolist())
print(f"{pid_pay[fut_p].isin(set(oid[fut_o])).sum()} of {fut_p.sum()} future payments belong to future-dated orders.")
bad_ts = (T(o.order_ts) != "") & ots.isna()
print("\nUnparseable order_ts values:", T(o.order_ts)[bad_ts].tolist())

# ---------------------------------------------------------------------------
h("6. Zero / negative quantity  (returns or typos?)")
q, up, d, a = num(o.quantity), num(o.unit_price), num(o.discount_pct), num(o.order_amount)
neg = q <= 0
print(o.loc[neg, ["order_id", "quantity", "unit_price", "discount_pct", "order_amount",
                  "order_status", "payment_mode"]].to_string(index=False))
print("\nDo these orders have payments?", pid_pay.isin(oid[neg]).sum(), "payment rows")

# ---------------------------------------------------------------------------
h("7. order_amount formula mismatches  (what went wrong in each?)")
expected = (q * up * (1 - d.fillna(0) / 100)).round(2)
mm = a.notna() & expected.notna() & ((a - expected).abs() > 0.01)
view = o.loc[mm, ["order_id", "quantity", "unit_price", "discount_pct", "order_amount"]].copy()
view["expected"] = expected[mm]
view["qty_x_price_no_discount"] = (q * up)[mm].round(2)
view["reported/expected"] = (a / expected)[mm].round(3)
print(view.to_string(index=False))
print("\nHint: ratio 1.0 vs the no-discount column means discount was ignored;"
      " a ratio of exactly 2, 10 or 100 points to a typing or unit error.")

# ---------------------------------------------------------------------------
h("8. Invalid category values (raw), and delivery outliers")
print("order_status values:", T(o.order_status).value_counts().to_dict())
print("orders.payment_mode values:", T(o.payment_mode).value_counts().to_dict())
print("payments.payment_mode values:", T(p.payment_mode).value_counts().to_dict())
dm = num(o.delivery_minutes)
print("delivery_minutes >= 120:", sorted(dm[dm >= 120].tolist()))
print("delivery_minutes min/median/max:", dm.min(), dm.median(), dm.max())

# ---------------------------------------------------------------------------
h("9. Unknown product_id / store_id values  (format problem or genuinely missing?)")
pids, sids = set(T(pr.product_id)), set(T(s.store_id))
print("Unknown product_ids in orders:", sorted(set(T(o.product_id)) - pids - {""}))
print("Unknown store_ids in orders:", sorted(set(T(o.store_id)) - sids - {""}))
print("Sample real product_ids:", sorted(pids)[:5], " real store_ids:", sorted(sids))

# ---------------------------------------------------------------------------
h("10. Conflicting duplicates in master files  (which columns differ?)")


def show_diffs(df, key):
    k = T(df[key])
    dup = df[k.duplicated(keep=False)]
    for kid, grp in dup.groupby(T(dup[key])):
        versions = grp.drop_duplicates()
        if len(versions) < 2:
            continue
        cols = [col for col in versions.columns if versions[col].nunique() > 1]
        shown = {col: ("(personal data, hidden)" if col in PII else list(versions[col])) for col in cols}
        print(f"  {kid}: {shown}")


print("customers.csv:")
show_diffs(c, "customer_id")
print("products.csv:")
show_diffs(pr, "product_id")
dup_cust = set(T(c.customer_id)[T(c.customer_id).duplicated()])
print(f"\nOrders placed by the duplicated customer_ids: {T(o.customer_id).isin(dup_cust).sum()}"
      " (these would be double-counted by a naive join)")

# ---------------------------------------------------------------------------
h("11. Products with cost_price > mrp, and orders priced above MRP")
mrp, cost = num(pr.mrp), num(pr.cost_price)
print(pr.loc[(cost > mrp) | (mrp <= 0), ["product_id", "product_name", "category", "mrp", "cost_price"]].to_string(index=False))
mrp_by_id = dict(zip(T(pr.product_id), mrp))
om = T(o.product_id).map(mrp_by_id)
above = up > om + 0.01
print(f"\n{above.sum()} orders priced above MRP. Price / MRP ratios:",
      sorted((up / om)[above].round(2).tolist()))

# ---------------------------------------------------------------------------
h("12. Distinct values a human should eyeball  (spellings the script cannot catch)")
for df, name, cols in [(o, "orders", ["channel"]), (c, "customers", ["city", "loyalty_tier", "is_active"]),
                       (pr, "products", ["category", "sub_category", "unit", "is_perishable"]),
                       (s, "stores", ["zone", "area"])]:
    for col in cols:
        print(f"{name}.{col} ({T(df[col]).nunique()} distinct): {T(df[col]).value_counts().to_dict()}")
print("\nCustomer email domains:", T(c.email).str.split("@").str[-1].str.lower().value_counts().head(15).to_dict())

# ---------------------------------------------------------------------------
h("13. Test / demo entry hunt  (the brief says test entries exist)")
for df, name in [(o, "orders"), (p, "payments"), (c, "customers"), (pr, "products"), (s, "stores")]:
    for col in df.columns:
        hit = T(df[col]).str.contains(SUSPICIOUS, regex=True, na=False)
        if hit.any():
            key = df.columns[0]
            vals = "(personal data, hidden)" if col in PII else T(df[col])[hit].unique()[:5].tolist()
            print(f"{name}.{col}: {hit.sum()} rows, keys {T(df[key])[hit].tolist()[:8]}, values {vals}")
print("\nStore names:", T(s.store_name).tolist())
print("Orders with unit_price < 5:", oid[up < 5].tolist())
cust_orders = T(o.customer_id).value_counts()
print("Customers with the most orders (a test account often has many):", cust_orders.head(5).to_dict())
print("Customer ids in orders that look non-standard:",
      sorted({x for x in T(o.customer_id) if x and not re.match(r"^C\d+$", x)})[:10])

# ---------------------------------------------------------------------------
h("14. Store master rows needing a decision (S07 zone, S12 capacity)")
print(s.drop(columns=[x for x in s.columns if x in PII]).loc[
    T(s.store_id).isin(["S07", "S12"])].to_string(index=False))
for sid in ["S07", "S12"]:
    print(f"Orders at {sid}: {(T(o.store_id) == sid).sum()}")

# ---------------------------------------------------------------------------
if inc is not None:
    h("15. Increment file: new orders vs corrections")
    iid = T(inc.order_id)
    existing = iid.isin(set(oid))
    print(f"rows {len(inc)}, new {(~existing).sum()}, corrections {existing.sum()}")
    old = iid[existing].map(order_status_by_id)
    new = T(inc.order_status).str.upper()[existing]
    print("Transitions:", (old + " -> " + new).value_counts().to_dict())
    print("Dates of new orders:", ts(inc.order_ts)[~existing].dt.date.value_counts().to_dict())

print("\nDone. Paste this output back into the chat to close the open items.")
