#!/usr/bin/env python3
"""
NammaMart Part F — prepare the "break it on purpose" experiments, and undo them.

Run from the project root (on your Mac or inside `astro dev bash`):

    python include/experiments/experiment_data.py status
    python include/experiments/experiment_data.py missing-file     # Experiment 4
    python include/experiments/experiment_data.py bad-rows         # Experiment 5
    python include/experiments/experiment_data.py schema-change    # Experiment 6
    python include/experiments/experiment_data.py low-volume       # Experiment 7
    python include/experiments/experiment_data.py drift            # Experiment 8 (unit_price x100, one store)
    python include/experiments/experiment_data.py drift --with-amount   # 8b: order_amount x100 too
    python include/experiments/experiment_data.py restore          # ALWAYS run after each experiment

The first change copies the original files to include/data/.nammamart_original/
(never overwritten afterwards); `restore` copies them back byte for byte.
Experiment 9 (freshness) needs no data change: trigger with as_of = 2026-10-31.
Only the standard library is used.
"""
from __future__ import annotations

import argparse
import csv
import filecmp
import shutil
import sys
from pathlib import Path

DATA = Path("include/data/nammamart")
BACKUP = Path("include/data/.nammamart_original")
ORDERS = DATA / "orders.csv"


def backup() -> None:
    if BACKUP.exists():
        return
    BACKUP.mkdir(parents=True)
    for f in DATA.glob("*.csv"):
        shutil.copy2(f, BACKUP / f.name)
    print(f"Backed up {len(list(BACKUP.glob('*.csv')))} original files to {BACKUP}/")


def restore() -> None:
    if not BACKUP.exists():
        print("No backup found: nothing has been changed by this script.")
        return
    for extra in DATA.iterdir():  # anything an experiment created (e.g. products.csv.missing)
        if extra.is_file() and not (BACKUP / extra.name).exists():
            extra.unlink()
            print(f"removed {extra}")
    for f in BACKUP.glob("*.csv"):
        shutil.copy2(f, DATA / f.name)
    print(f"Restored {len(list(BACKUP.glob('*.csv')))} original files into {DATA}/")
    status()


def status() -> None:
    if not BACKUP.exists():
        print("Data is original (no experiment has been prepared yet).")
        return
    changed = []
    for f in sorted(BACKUP.glob("*.csv")):
        cur = DATA / f.name
        if not cur.exists():
            changed.append(f"{f.name}: MISSING")
        elif not filecmp.cmp(f, cur, shallow=False):
            changed.append(f"{f.name}: modified")
    extras = [p.name for p in DATA.iterdir() if p.is_file() and not (BACKUP / p.name).exists()]
    if changed or extras:
        print("EXPERIMENT ACTIVE:", "; ".join(changed + [f"{e}: extra file" for e in extras]))
        print("Run `restore` before the next experiment.")
    else:
        print("Data matches the originals.")


def read(path: Path) -> tuple[list[str], list[list[str]]]:
    with open(path, newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.reader(fh))
    return rows[0], rows[1:]


def write(path: Path, header: list[str], rows: list[list[str]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def num(v: str):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
def missing_file() -> None:
    backup()
    src = DATA / "products.csv"
    src.rename(DATA / "products.csv.missing")
    print("Experiment 4: products.csv renamed to products.csv.missing")


def bad_rows() -> None:
    backup()
    header, rows = read(ORDERS)
    _, customers = read(DATA / "customers.csv")
    p_head, products = read(DATA / "products.csv")
    _, stores = read(DATA / "stores.csv")
    customer = customers[0][0].strip()
    store = stores[0][0].strip()
    mrp_i = p_head.index("mrp")
    product, price = next((p[0].strip(), num(p[mrp_i])) for p in products if (num(p[mrp_i]) or 0) > 0)
    price = round(price, 2)

    def order(oid, ts="2026-09-15 10:00:00", cust=customer, st=store, qty=1, status="DELIVERED"):
        r = dict(order_id=oid, order_ts=ts, customer_id=cust, store_id=st, product_id=product,
                 quantity=str(qty), unit_price=f"{price:.2f}", discount_pct="0",
                 order_amount=f"{qty * price:.2f}", payment_mode="UPI", order_status=status,
                 channel="App", delivery_minutes="12")
        return [r[c] for c in header]

    new = [
        (order("ORD900001", cust=""), "order_customer_id_not_null"),
        (order("ORD900002", qty=-2), "quantity_positive_integer"),
        (order("ORD900003", st="S99"), "order_store_exists"),
        (order("ORD900004", status="SHIPPED"), "order_status_allowed"),
        (order("ORD900005", ts="2027-01-15 10:00:00"), "order_ts_not_future"),
    ]
    write(ORDERS, header, rows + [r for r, _ in new])
    print(f"Experiment 5: appended 5 bad rows to orders.csv ({len(rows)} -> {len(rows) + 5} rows)")
    print("Each row breaks exactly ONE rule. Expected failure_reason in quarantine / stg_orders:")
    for r, reason in new:
        print(f"  {r[0]}  ->  {reason}")
    print("Expected: orders quarantined 104 -> 109.")


def schema_change() -> None:
    backup()
    header, rows = read(ORDERS)
    header = ["total" if h == "order_amount" else h for h in header]
    write(ORDERS, header, rows)
    print("Experiment 6: column order_amount renamed to total in orders.csv")


def low_volume(n: int = 50) -> None:
    backup()
    header, rows = read(ORDERS)
    write(ORDERS, header, rows[:n])
    print(f"Experiment 7: orders.csv cut from {len(rows)} to {n} rows ({100 * (1 - n / len(rows)):.0f}% drop)")


def drift(store: str, with_amount: bool) -> None:
    backup()
    header, rows = read(ORDERS)
    si, pi, ai = header.index("store_id"), header.index("unit_price"), header.index("order_amount")
    changed = 0
    for r in rows:
        if r[si].strip() == store and num(r[pi]) is not None:
            r[pi] = f"{num(r[pi]) * 100:.2f}"
            if with_amount and num(r[ai]) is not None:
                r[ai] = f"{num(r[ai]) * 100:.2f}"
            changed += 1
    write(ORDERS, header, rows)
    what = "unit_price AND order_amount" if with_amount else "unit_price only"
    print(f"Experiment 8: {what} x100 for {changed} orders of store {store}")
    if not with_amount:
        print("The formula check should now reject these rows (order_amount no longer matches).")
    else:
        print("Every check still passes for these rows: watch store revenue on the mart instead.")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["status", "restore", "missing-file", "bad-rows", "schema-change",
                                        "low-volume", "drift"])
    ap.add_argument("--store", default="S03", help="store for the drift experiment (default S03)")
    ap.add_argument("--with-amount", action="store_true", help="drift: scale order_amount too")
    args = ap.parse_args()
    if not DATA.exists():
        print(f"{DATA} not found: run this from the project root")
        return 1
    if args.command not in ("status", "restore") and BACKUP.exists():
        _, before = None, None
        same = all((DATA / f.name).exists() and filecmp.cmp(f, DATA / f.name, shallow=False)
                   for f in BACKUP.glob("*.csv"))
        if not same:
            print("Another experiment is still active. Run `restore` first.")
            status()
            return 1
    {"status": status, "restore": restore, "missing-file": missing_file, "bad-rows": bad_rows,
     "schema-change": schema_change, "low-volume": low_volume,
     "drift": lambda: drift(args.store, args.with_amount)}[args.command]()
    return 0


if __name__ == "__main__":
    sys.exit(main())
