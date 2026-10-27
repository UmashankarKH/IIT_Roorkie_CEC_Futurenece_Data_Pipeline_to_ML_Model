# Run from the project root:  python include/smoke_test_dq.py
import os, sys, tempfile
sys.path.insert(0, ".")
import duckdb
import pandas as pd
from include.dq_checks import build_context, run_checks, write_dq_results
from include.masking import mask_email_series, mask_phone_series, sql_mask_email, sql_mask_phone

D = "include/data/nammamart"
tables = ["orders", "payments", "customers", "products", "stores"]
raw = {t: pd.read_csv(f"{D}/{t}.csv", dtype=str, keep_default_na=False) for t in tables}
ctx = build_context(raw, as_of=pd.Timestamp("2026-10-10"))
con = duckdb.connect(os.path.join(tempfile.mkdtemp(), "t.duckdb"))

for t, df in raw.items():
    passed, failed, res = run_checks(df, t, ctx, "smoke_run", "smoke_dag")
    write_dq_results(con, res)
    print(f"{t:10} rows={len(df):5} passed={len(passed):5} quarantined={len(failed):4}")

n1 = con.execute("select count(*) from dq_results").fetchone()[0]
write_dq_results(con, res)  # same run again: a retry must not add rows
n2 = con.execute("select count(*) from dq_results").fetchone()[0]
print("dq_results rows:", n1, "->", n2, "(must be equal)")

c = raw["customers"].reset_index()
con.register("c", c)
q = f"select {sql_mask_email('email')} e, {sql_mask_phone('phone')} p from c order by index"
sql = con.execute(q).df()
print("email mismatches vs Python:", int((sql.e.values != mask_email_series(c.email).values).sum()))
print("phone mismatches vs Python:", int((sql.p.values != mask_phone_series(c.phone).values).sum()))
