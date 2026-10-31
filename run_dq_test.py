import pandas as pd
from include.dq_checks import CHECKSETS, run_checks
from include.nammamart_config import DATA_DIR

NOW = pd.Timestamp("2026-10-24 09:00")
rd = lambda f: pd.read_csv(DATA_DIR / f, dtype=str, keep_default_na=False)
ctx = {"now": NOW}
all_results = []

for name, key in [("customers", "customer_ids"), ("products", "product_ids"), ("stores", "store_ids")]:
    df = rd(f"{name}.csv")
    p, f, r = run_checks(df, CHECKSETS[name](ctx), name, "test", "run1", NOW)
    ctx[key] = set(df.iloc[:, 0])
    all_results += r
    print(f"{name:10} total={len(df):5} passed={len(p):5} quarantined={len(f)}")

o = rd("orders.csv")
ctx["all_order_ids"] = set(o.order_id)
p, f, r = run_checks(o, CHECKSETS["orders"](ctx), "orders", "test", "run1", NOW)
ctx["order_amounts"] = dict(zip(p.order_id, p.order_amount))
all_results += r
print(f"{'orders':10} total={len(o):5} passed={len(p):5} quarantined={len(f)}")

pay = rd("payments.csv")
p, f, r = run_checks(pay, CHECKSETS["payments"](ctx), "payments", "test", "run1", NOW)
all_results += r
print(f"{'payments':10} total={len(pay):5} passed={len(p):5} quarantined={len(f)}")

res = pd.DataFrame(all_results)
print()
print(res[res.rows_failed > 0][["check_name", "severity", "rows_failed", "status"]].to_string(index=False))