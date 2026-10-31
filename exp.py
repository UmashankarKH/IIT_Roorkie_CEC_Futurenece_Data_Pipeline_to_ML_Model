"""Experiment tool for Part F.
  python3 exp.py apply <n>   back up the CSVs, then apply experiment n  (3, 4, 5, 6, 7, 8 or 9)
  python3 exp.py restore     put the original files back (always safe to run)
  python3 exp.py status      say whether the data folder is original or modified
"""
import shutil
import sys
from pathlib import Path

import pandas as pd

DATA = Path("include/data/nammamart")
BACKUP = Path("include/data/_original")
MARK = BACKUP / ".active"
INCREMENT = "orders_2026-10-01_increment.csv"
TS_FMT = "%Y-%m-%d %H:%M:%S"

rd = lambda name: pd.read_csv(DATA / name, dtype=str, keep_default_na=False)
wr = lambda df, name: df.to_csv(DATA / name, index=False)

DESCRIPTION = {
    3: "increment file removed (September-only load; restore brings it back)",
    4: "products.csv renamed to products.csv.hidden (missing source file)",
    5: "5 bad rows added to orders.csv (null customer, negative quantity, unknown store, SHIPPED, 2027 timestamp)",
    6: "orders.csv column order_amount renamed to total (schema change)",
    7: "orders.csv cut to 50 orders (volume anomaly)",
    8: "unit_price x100 for store S05 in orders.csv (distribution drift)",
    9: "all order_ts shifted so the latest order is 30 days old; increment removed (freshness)",
}


def good_row(o):
    q, p, d, a = (pd.to_numeric(o[c], errors="coerce") for c in ["quantity", "unit_price", "discount_pct", "order_amount"])
    dm = pd.to_numeric(o.delivery_minutes, errors="coerce")
    ok = ((o.order_status == "DELIVERED") & o.customer_id.ne("") & (q > 0) & (p > 0) & d.between(0, 100)
          & ((q * p * (1 - d / 100)).round(2) - a).abs().le(0.01) & dm.between(8, 35)
          & o.store_id.isin(rd("stores.csv").store_id) & o.product_id.isin(rd("products.csv").product_id)
          & pd.to_datetime(o.order_ts, errors="coerce").lt(pd.Timestamp.now()))
    return o[ok].iloc[0]


def apply(n):
    if MARK.exists():
        sys.exit("An experiment is already applied. Run first:  python3 exp.py restore")
    BACKUP.mkdir(parents=True, exist_ok=True)
    for f in DATA.glob("*.csv"):
        shutil.copy2(f, BACKUP / f.name)
    MARK.write_text(str(n))
    o = rd("orders.csv")
    if n == 3:
        (DATA / INCREMENT).unlink()
    elif n == 4:
        (DATA / "products.csv").rename(DATA / "products.csv.hidden")
    elif n == 5:
        base = good_row(o)
        def mk(i, **kw):
            r = base.copy()
            r["order_id"] = f"ORDTEST{i}"
            for k, v in kw.items():
                r[k] = v
            if "order_amount" not in kw:
                r["order_amount"] = str(round(float(r.quantity) * float(r.unit_price) * (1 - float(r.discount_pct) / 100), 2))
            return r
        bad = [mk(1, customer_id=""), mk(2, quantity="-2"), mk(3, store_id="S99"),
               mk(4, order_status="SHIPPED"), mk(5, order_ts="2027-03-01 10:00:00")]
        wr(pd.concat([o, pd.DataFrame(bad)], ignore_index=True), "orders.csv")
    elif n == 6:
        wr(o.rename(columns={"order_amount": "total"}), "orders.csv")
    elif n == 7:
        wr(o.head(50), "orders.csv")
    elif n == 8:
        m = o.store_id == "S05"
        o.loc[m, "unit_price"] = (pd.to_numeric(o.loc[m, "unit_price"]) * 100).astype(str)
        wr(o, "orders.csv")
        print(f"orders changed for store S05: {int(m.sum())}")
    elif n == 9:
        ts = pd.to_datetime(o.order_ts, errors="coerce")
        newest = ts[ts <= pd.Timestamp.now()].max()
        delta = (pd.Timestamp.now().normalize() - pd.Timedelta(days=30)) - newest
        shifted = (ts + delta).dt.strftime(TS_FMT)
        o["order_ts"] = shifted.where(ts.notna(), o.order_ts)
        wr(o, "orders.csv")
        (DATA / INCREMENT).unlink()
        print("latest valid order is now:", pd.to_datetime(o.order_ts, errors="coerce").loc[lambda s: s <= pd.Timestamp.now()].max())
    else:
        MARK.unlink()
        sys.exit("experiments handled by this tool: 3 to 9")
    print(f"APPLIED experiment {n}: {DESCRIPTION[n]}")
    print("When you are done: python3 exp.py restore")


def restore():
    if not MARK.exists():
        print("data folder is original, nothing to restore")
        return
    for f in DATA.glob("*"):
        if f.is_file():
            f.unlink()
    for f in BACKUP.glob("*.csv"):
        shutil.copy2(f, DATA / f.name)
    MARK.unlink()
    print("RESTORED original files:", sorted(p.name for p in DATA.glob("*")))


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "apply" and len(sys.argv) > 2:
        apply(int(sys.argv[2]))
    elif cmd == "restore":
        restore()
    elif cmd == "status":
        print("MODIFIED: experiment " + MARK.read_text() if MARK.exists() else "original data")
    else:
        print(__doc__)
