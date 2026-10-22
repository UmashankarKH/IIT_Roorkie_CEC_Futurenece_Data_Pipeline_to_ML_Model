"""Part 0: profile the six NammaMart files. Run: python include/nammamart_profile.py"""
from pathlib import Path

import pandas as pd

DATA = Path(__file__).resolve().parent / "data" / "nammamart"
FILES = ["orders", "payments", "customers", "products", "stores", "orders_2026-10-01_increment"]
CATS = ["order_status", "payment_mode", "payment_status", "loyalty_tier", "zone", "category", "channel", "is_active"]


def load(name):
    return pd.read_csv(DATA / f"{name}.csv", dtype=str, keep_default_na=False, na_values=[""])


def section(title):
    print(f"\n{'=' * 8} {title} {'=' * 8}")


def main():
    d = {n: load(n) for n in FILES}
    for n, df in d.items():
        section(f"{n}: {len(df)} rows")
        print("nulls:", {c: int(v) for c, v in df.isna().sum().items() if v})
        for c in df.columns:
            if c in CATS:
                print(f"  {c}:", df[c].value_counts(dropna=False).to_dict())
        key = {"orders": "order_id", "payments": "payment_id", "customers": "customer_id",
               "products": "product_id", "stores": "store_id",
               "orders_2026-10-01_increment": "order_id"}[n]
        print(f"duplicate {key}:", int(df.duplicated(key, keep=False).sum()))
        for c in df.columns:
            if c in ("quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes", "amount_paid",
                     "mrp", "cost_price", "capacity_orders_per_day"):
                s = pd.to_numeric(df[c], errors="coerce")
                print(f"  {c}: min={s.min()} max={s.max()} non-numeric={int((s.isna() & df[c].notna()).sum())}")
            if c.endswith(("_ts", "_date")):
                s = pd.to_datetime(df[c], errors="coerce")
                print(f"  {c}: min={s.min()} max={s.max()} unparseable={int((s.isna() & df[c].notna()).sum())}")

    o, p, c, pr, s = d["orders"], d["payments"], d["customers"], d["products"], d["stores"]
    section("Referential integrity")
    print("orders.customer_id missing:", int((~o.customer_id.isin(c.customer_id) & o.customer_id.notna()).sum()))
    print("orders.product_id missing:", int((~o.product_id.isin(pr.product_id) & o.product_id.notna()).sum()))
    print("orders.store_id missing:", int((~o.store_id.isin(s.store_id) & o.store_id.notna()).sum()))
    print("payments.order_id missing:", int((~p.order_id.isin(o.order_id)).sum()))

    section("Business rules")
    q, up, dp = (pd.to_numeric(o[x], errors="coerce") for x in ("quantity", "unit_price", "discount_pct"))
    amt = pd.to_numeric(o.order_amount, errors="coerce")
    print("order_amount mismatch:", int(((q * up * (1 - dp / 100)).round(2) - amt).abs().gt(0.01).sum()))
    dm = pd.to_numeric(o.delivery_minutes, errors="coerce")
    print("DELIVERED without delivery_minutes:", int(((o.order_status.str.strip().str.upper() == "DELIVERED") & dm.isna()).sum()))
    print("delivery_minutes outside 8-35:", int((dm.notna() & ~dm.between(8, 35)).sum()))
    print("quantity <= 0:", int((q <= 0).sum()))
    print("cost_price > mrp:", int((pd.to_numeric(pr.cost_price, errors="coerce") > pd.to_numeric(pr.mrp, errors="coerce")).sum()))
    print("bad phone:", int((~c.phone.fillna("").str.fullmatch(r"\d{10}")).sum()))
    print("bad email:", int((~c.email.fillna("").str.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+")).sum()))
    print("bad pincode:", int((~c.pincode.fillna("").str.fullmatch(r"\d{6}")).sum()))


if __name__ == "__main__":
    main()
