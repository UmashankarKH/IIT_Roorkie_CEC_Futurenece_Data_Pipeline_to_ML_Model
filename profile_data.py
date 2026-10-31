"""
Part 0 profiler for NammaMart.
Run from the project root:  python profile_data.py
Reads everything as text so nothing is silently "fixed" by pandas.
Writes candidate issues to profile_issues_candidates.csv.
"""
import re
import pandas as pd

D = "include/data/nammamart/"
FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
    "increment": "orders_2026-10-01_increment.csv",
}
KEYS = {"orders": "order_id", "payments": "payment_id", "customers": "customer_id",
        "products": "product_id", "stores": "store_id", "increment": "order_id"}
CATS = ["order_status", "payment_mode", "payment_status", "loyalty_tier",
        "zone", "category", "channel", "city", "is_active"]
NUMS = ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes",
        "amount_paid", "mrp", "cost_price", "capacity_orders_per_day"]
DATES = ["order_ts", "payment_ts", "signup_date", "launch_date", "open_date"]

df = {k: pd.read_csv(D + f, dtype=str, keep_default_na=False) for k, f in FILES.items()}
issues = []


def log(file, key, col, problem, dim, severity):
    issues.append(dict(file=file, row_key=key, column=col, problem=problem,
                       dimension=dim, severity=severity))


def blank(s):
    return s.str.strip().eq("")


# ---------- 1. Generic profile ----------
for name, t in df.items():
    print(f"\n===== {name}: {len(t)} rows =====")
    nulls = t.apply(blank).sum()
    print("Blank per column:\n", nulls[nulls > 0].to_string() or "none")
    k = KEYS[name]
    dups = t[t.duplicated(k, keep=False)]
    print(f"Duplicate {k}: {dups[k].nunique()} keys, {len(dups)} rows")
    for _, r in dups.iterrows():
        log(name, r[k], k, "duplicate key", "Uniqueness", "Critical")
    for c in CATS:
        if c in t:
            print(f"Distinct {c}:", sorted(t[c].unique()))
    for c in NUMS:
        if c in t:
            n = pd.to_numeric(t[c], errors="coerce")
            print(f"{c}: min={n.min()} max={n.max()} non-numeric={int((n.isna() & ~blank(t[c])).sum())}")
    for c in DATES:
        if c in t:
            d = pd.to_datetime(t[c], errors="coerce")
            bad = d.isna() & ~blank(t[c])
            print(f"{c}: min={d.min()} max={d.max()} unparseable={int(bad.sum())}")
            for i in t[bad].index:
                log(name, t.loc[i, k], c, f"unparseable date '{t.loc[i, c]}'", "Validity", "Critical")
            for i in t[d > pd.Timestamp("2026-10-24")].index:
                log(name, t.loc[i, k], c, "future date", "Timeliness", "Critical")

# ---------- 2. Orders / increment business rules ----------
for name in ("orders", "increment"):
    o = df[name].copy()
    q = pd.to_numeric(o.quantity, errors="coerce")
    up = pd.to_numeric(o.unit_price, errors="coerce")
    dp = pd.to_numeric(o.discount_pct, errors="coerce")
    amt = pd.to_numeric(o.order_amount, errors="coerce")
    dm = pd.to_numeric(o.delivery_minutes, errors="coerce")
    calc = (q * up * (1 - dp / 100)).round(2)
    rules = [
        ("order_amount", (calc - amt).abs() > 0.01, "amount != qty*price*(1-disc)", "Accuracy", "Critical"),
        ("quantity", q <= 0, "quantity <= 0", "Accuracy", "Critical"),
        ("discount_pct", (dp < 0) | (dp > 100), "discount outside 0-100", "Validity", "Critical"),
        ("customer_id", blank(o.customer_id), "customer_id null", "Completeness", "Critical"),
        ("order_status", ~o.order_status.isin(["DELIVERED", "CANCELLED", "RETURNED"]),
         "status not in allowed list", "Validity", "Critical"),
        ("payment_mode", ~o.payment_mode.isin(["UPI", "Card", "Cash on Delivery", "Wallet"]),
         "payment_mode not in allowed list (casing/variants?)", "Validity", "Critical"),
        ("delivery_minutes", (o.order_status == "DELIVERED") & dm.isna(),
         "DELIVERED without delivery_minutes", "Completeness", "Critical"),
        ("delivery_minutes", dm > 120, "delivery_minutes > 120 (impossible)", "Accuracy", "Critical"),
        ("delivery_minutes", (dm < 8) | ((dm > 35) & (dm <= 120)), "outside normal 8-35 (check)", "Accuracy", "Warning"),
        ("store_id", ~o.store_id.isin(df["stores"].store_id), "store_id not in stores", "Consistency", "Critical"),
        ("product_id", ~o.product_id.isin(df["products"].product_id), "product_id not in products", "Consistency", "Critical"),
        ("customer_id", ~blank(o.customer_id) & ~o.customer_id.isin(df["customers"].customer_id),
         "customer_id not in customers", "Consistency", "Critical"),
    ]
    for col, mask, prob, dim, sev in rules:
        print(f"[{name}] {prob}: {int(mask.sum())}")
        for i in o[mask].index:
            log(name, o.loc[i, "order_id"], col, prob, dim, sev)

# ---------- 3. Payments ----------
p, o = df["payments"], df["orders"]
pa = pd.to_numeric(p.amount_paid, errors="coerce")
print("\n[payments] order_id not in orders:", int((~p.order_id.isin(o.order_id)).sum()))
for i in p[~p.order_id.isin(o.order_id)].index:
    log("payments", p.loc[i, "payment_id"], "order_id", "orphan payment", "Consistency", "Critical")
m = p.merge(o[["order_id", "order_amount", "order_status"]].drop_duplicates("order_id"), on="order_id")
bad = m[(m.payment_status == "SUCCESS") &
        ((pd.to_numeric(m.amount_paid, errors="coerce") - pd.to_numeric(m.order_amount, errors="coerce")).abs() > 0.01)]
print("[payments] SUCCESS amount != order_amount:", len(bad))
for _, r in bad.iterrows():
    log("payments", r.payment_id, "amount_paid", "SUCCESS amount != order_amount", "Consistency", "Critical")
nopay = o[(o.order_status != "CANCELLED") & ~o.order_id.isin(p.order_id)]
print("[orders] non-cancelled with no payment:", len(nopay))
print("[payments] payments for CANCELLED orders:",
      int(m.order_status.eq("CANCELLED").sum()))

# ---------- 4. Customers ----------
c = df["customers"]
em = ~c.email.str.match(r"^[\w.+-]+@[\w-]+\.[\w.]+$") & ~blank(c.email)
ph = ~c.phone.str.fullmatch(r"\d{10}")
pc = ~c.pincode.str.fullmatch(r"\d{6}")
print("\n[customers] bad email:", int(em.sum()), "| bad phone:", int(ph.sum()), "| bad pincode:", int(pc.sum()))
for mask, col, dim in [(em, "email", "Validity"), (ph, "phone", "Validity"), (pc, "pincode", "Validity")]:
    for i in c[mask].index:
        log("customers", c.loc[i, "customer_id"], col, f"invalid {col} '{c.loc[i, col]}'", dim,
            "Warning" if col != "pincode" else "Warning")
tiers = c.loan if False else c.loyalty_tier
if tiers.nunique() != tiers.str.strip().str.upper().nunique():
    print("[customers] loyalty_tier casing variants:", sorted(tiers.unique()))
    for i in c[tiers != tiers.str.strip().str.title()].index:
        log("customers", c.loc[i, "customer_id"], "loyalty_tier", "inconsistent casing/whitespace", "Consistency", "Warning")
# duplicate customers by same email/phone with different ids
for col in ("email", "phone"):
    d = c[c.duplicated(col, keep=False) & ~blank(c[col])]
    print(f"[customers] duplicate {col} across ids: {len(d)} rows")

# ---------- 5. Products ----------
pr = df["products"]
mrp = pd.to_numeric(pr.mrp, errors="coerce")
cp = pd.to_numeric(pr.cost_price, errors="coerce")
for mask, col, prob, dim in [
    (mrp <= 0, "mrp", "mrp <= 0", "Accuracy"),
    (cp > mrp, "cost_price", "cost_price > mrp", "Accuracy"),
    (blank(pr.category), "category", "category null", "Completeness"),
]:
    print(f"[products] {prob}: {int(mask.sum())}")
    for i in pr[mask].index:
        log("products", pr.loc[i, "product_id"], col, prob, dim, "Critical")

# ---------- 6. Stores ----------
s = df["stores"]
print("\n[stores] zones:", sorted(s.zone.unique()), "| store count:", len(s))
sp = ~s.pincode.str.fullmatch(r"\d{6}")
for i in s[sp].index:
    log("stores", s.loc[i, "store_id"], "pincode", "invalid pincode", "Validity", "Warning")

# ---------- 7. Increment vs orders ----------
inc, base = df["increment"], df["orders"]
print("\n[increment] ids already in orders (corrections):", int(inc.order_id.isin(base.order_id).sum()),
      "| new:", int((~inc.order_id.isin(base.order_id)).sum()))

out = pd.DataFrame(issues)
out.to_csv("profile_issues_candidates.csv", index=False)
print("\nCandidate issues by file/dimension:")
print(out.groupby(["file", "dimension"]).size().to_string())
print("\nWrote profile_issues_candidates.csv (", len(out), "rows )")
