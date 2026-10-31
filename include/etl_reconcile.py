"""Compares clean orders with the payment gateway. One row per mismatch, with a reason."""
import pandas as pd


def reconcile(fact, payments_raw, all_order_ids):
    pay = payments_raw.drop_duplicates("payment_id").copy()
    pay["amount_paid"] = pd.to_numeric(pay["amount_paid"], errors="coerce")
    pay["pstatus"] = pay["payment_status"].str.strip().str.upper()
    success = pay[pay.pstatus == "SUCCESS"].groupby("order_id")["amount_paid"].sum()
    paid_ids = set(pay.order_id)
    fact_ids = set(fact.order_id)
    cancelled = set(fact[fact.order_status == "CANCELLED"].order_id)
    out = []

    def add(order_id, payment_id, order_amount, amount_paid, reason):
        out.append(dict(order_id=order_id, payment_id=payment_id, order_amount=order_amount,
                        amount_paid=amount_paid, reason=reason))

    # 1) orders that should have been paid
    status_by_order = pay.groupby("order_id")["pstatus"].agg(lambda s: ",".join(sorted(set(s))))
    for r in fact[fact.order_status != "CANCELLED"].itertuples():
        if r.order_id not in paid_ids:
            add(r.order_id, None, r.order_amount, None, "MISSING_PAYMENT")
        elif r.order_id not in success.index:
            st = status_by_order[r.order_id]
            if "REFUNDED" in st and r.order_status == "RETURNED":
                continue          # expected: a returned order was refunded
            add(r.order_id, None, r.order_amount, None, "PAYMENT_" + st.replace(",", "_"))
        elif abs(success[r.order_id] - r.order_amount) > 0.01:
            add(r.order_id, None, r.order_amount, float(success[r.order_id]), "AMOUNT_MISMATCH")

    # 2) payments that do not fit a clean order
    for p in pay.itertuples():
        if p.order_id not in all_order_ids:
            add(p.order_id, p.payment_id, None, p.amount_paid, "ORPHAN_PAYMENT")
        elif p.order_id not in fact_ids:
            add(p.order_id, p.payment_id, None, p.amount_paid, "ORDER_QUARANTINED")
        elif p.order_id in cancelled:
            add(p.order_id, p.payment_id, None, p.amount_paid, "PAYMENT_FOR_CANCELLED_ORDER")

    cols = ["order_id", "payment_id", "order_amount", "amount_paid", "reason"]
    rec = pd.DataFrame(out, columns=cols)
    rec["difference"] = rec["order_amount"].fillna(0) - rec["amount_paid"].fillna(0)
    return rec