"""Runs the observability experiments automatically (Part F).
  python3 run_experiments.py          -> experiments 2, 3, 5, 6, 7, 8, 9  (roughly 10-15 minutes)
  python3 run_experiments.py 7 8      -> only those
Each experiment: apply the data change -> run ETL and ELT (dags test) -> snapshot -> restore the files -> rebuild.
Everything printed is also appended to experiments_log.txt
"""
import re
import subprocess
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd

DB = "include/warehouse_anikesh.duckdb"
ETL, ELT = "nammamart_etl_anikesh", "nammamart_elt_anikesh"
QDIR, DATA = Path("include/quarantine/anikesh"), Path("include/data/nammamart")
INCREMENT = "orders_2026-10-01_increment.csv"
TIMEOUT = 480
LOG = open("experiments_log.txt", "a")


def say(msg=""):
    print(msg, flush=True)
    LOG.write(msg + "\n")
    LOG.flush()


def q(sql, params=None):
    try:
        con = duckdb.connect(DB, read_only=True)
        try:
            return con.execute(sql, params or []).fetchall()
        finally:
            con.close()
    except Exception:
        return None


def val(sql, params=None):
    r = q(sql, params)
    return r[0][0] if r and r[0] else None


def exp_tool(*args):
    p = subprocess.run([sys.executable, "exp.py", *args], capture_output=True, text=True)
    return p.returncode, (p.stdout + p.stderr).strip()


def tag(dag):
    return "ETL" if dag == ETL else "ELT"


def latest(dag):
    r = q("select status, run_id, rows_extracted, rows_loaded, rows_quarantined from pipeline_runs "
          "where dag_id = ? order by start_time desc limit 1", [dag])
    return r[0] if r else None


def dq_run(dag):
    return val("select run_id from dq_results where dag_id = ? order by checked_at desc limit 1", [dag])


def dq_value(dag, check):
    r = q("select rows_failed, status from dq_results where dag_id = ? and run_id = ? and check_name = ?",
          [dag, dq_run(dag), check])
    return r[0] if r else None


def dag_test(dag):
    t0 = time.time()
    try:
        p = subprocess.run(["astro", "dev", "run", "dags", "test", dag], capture_output=True, text=True, timeout=TIMEOUT)
        out, rc = p.stdout + "\n" + p.stderr, p.returncode
    except subprocess.TimeoutExpired as e:
        raw = e.stdout or b""
        out, rc = (raw.decode() if isinstance(raw, bytes) else raw), 124
    m = re.search(r"DagRun Finished:.*?state=(\w+)", out)
    state = m.group(1) if m else ("timeout" if rc == 124 else "crashed (no DagRun Finished line)")
    started = re.findall(r"starting task_id=(\w+)", out)
    errs = re.findall(r"\b(\w*(?:Error|Exception)): ([^\n]{0,150})", out)
    keys = [l.strip() for l in out.splitlines()
            if re.search(r"(increment:|audit:|loaded:)", l) and "[info" not in l and "[warning" not in l]
    return dict(state=state, secs=round(time.time() - t0, 1), keys=keys,
                failed_task=started[-1] if (started and state != "success") else None,
                error=f"{errs[-1][0]}: {errs[-1][1]}" if errs else None)


def run_both(show=("audit:",)):
    before = {d: (latest(d) or (None, None))[1] for d in (ETL, ELT)}
    for dag in (ETL, ELT):
        r = dag_test(dag)
        line = f"  {tag(dag)}: dags test -> {r['state']} in {r['secs']}s"
        if r["state"] != "success":
            line += f" | last task started: {r['failed_task']} | error: {r['error']}"
        say(line)
        for k in r["keys"]:
            if any(s in k for s in show):
                say(f"      log: {k[:160]}")
    for dag in (ETL, ELT):
        a = latest(dag)
        new = "NEW" if a and a[1] != before[dag] else "NO NEW AUDIT ROW"
        say(f"  {tag(dag)} audit row ({new}): " + (f"status={a[0]} extracted={a[2]} loaded={a[3]} set_aside={a[4]}" if a else "none"))


def snapshot():
    return dict(
        etl_fact=val("select count(*) from etl_fact_orders"), raw_orders=val("select count(*) from raw_orders"),
        stg_orders=val("select count(*) from stg_orders"),
        etl_revenue=val("select round(sum(net_revenue),2) from etl_fact_orders where order_status='DELIVERED'"),
        elt_revenue=val("select round(sum(net_revenue),2) from mart_daily_store_kpis"),
        pipeline_runs=val("select count(*) from pipeline_runs"), dq_rows=val("select count(*) from dq_results"))


def d(a, b):
    return "n/a" if a is None or b is None else f"{a - b:+d}"


def fmt(s):
    return " ".join(f"{k}={v}" for k, v in s.items())


def rebuild():
    code, out = exp_tool("restore")
    say("  " + out.splitlines()[-1][:110] if out else "  restored")
    for dag in (ETL, ELT):
        r = dag_test(dag)
        say(f"  rebuild {tag(dag)}: {r['state']} ({r['secs']}s)")
    s = snapshot()
    say(f"  back to normal: ETL revenue={s['etl_revenue']}  ELT revenue={s['elt_revenue']}  etl_fact={s['etl_fact']}")


def apply(n):
    code, out = exp_tool("apply", str(n))
    if code != 0:
        raise SystemExit(f"could not apply experiment {n}: {out}")
    say("  " + out.splitlines()[0][:150] if out else "")


def sql_ids(ids):
    return ",".join("'" + i + "'" for i in ids)


def corrected_ids():
    o = set(pd.read_csv(DATA / "orders.csv", dtype=str).order_id)
    return [i for i in pd.read_csv(DATA / INCREMENT, dtype=str).order_id if i in o]


# ------------------------------------------------------------------ experiments
def exp2():
    say("EXPERIMENT 2: re-run (idempotency). Same DAGs triggered again, data unchanged")
    b = snapshot()
    say("  before: " + fmt(b))
    run_both()
    a = snapshot()
    say("  after : " + fmt(a))
    say(f"  CHANGE: etl_fact {d(a['etl_fact'], b['etl_fact'])} | raw_orders {d(a['raw_orders'], b['raw_orders'])} | "
        f"stg_orders {d(a['stg_orders'], b['stg_orders'])} | pipeline_runs {d(a['pipeline_runs'], b['pipeline_runs'])} | "
        f"dq_results rows {d(a['dq_rows'], b['dq_rows'])}")


def exp3():
    say("EXPERIMENT 3: incremental load (orders_2026-10-01_increment.csv)")
    ids = corrected_ids()
    say(f"  the increment holds {len(pd.read_csv(DATA / INCREMENT))} rows; {len(ids)} of them correct existing orders")
    con = duckdb.connect(DB)
    con.execute("DROP TABLE IF EXISTS etl_fact_orders")      # start the ETL fact from scratch
    con.close()
    say("  step 1: run WITHOUT the increment file (September only)")
    apply(3)
    run_both(show=("increment:", "audit:"))
    s1 = snapshot()
    st = lambda t: q(f"select order_status, count(*) from {t} where order_id in ({sql_ids(ids)}) group by 1 order by 1")
    say(f"  after step 1: etl_fact={s1['etl_fact']} stg_orders={s1['stg_orders']}  | the {len(ids)} corrected orders: "
        f"ETL={st('etl_fact_orders')} ELT={st('stg_orders')}")
    exp_tool("restore")
    say("  step 2: put the increment file back and run again")
    run_both(show=("increment:", "audit:"))
    s2 = snapshot()
    say(f"  after step 2: etl_fact={s2['etl_fact']} stg_orders={s2['stg_orders']}  | the {len(ids)} corrected orders: "
        f"ETL={st('etl_fact_orders')} ELT={st('stg_orders')}")
    say(f"  duplicates: ETL={val('select count(*) - count(distinct order_id) from etl_fact_orders')} "
        f"ELT={val('select count(*) - count(distinct order_id) from stg_orders where order_id <> ''')}")


CHECKS5 = ["orders.customer_id_not_null", "orders.quantity_positive", "orders.store_id_exists",
           "orders.order_status_allowed", "orders.order_ts_not_future"]


def exp5():
    say("EXPERIMENT 5: new bad data (5 planted orders: null customer, negative quantity, unknown store, SHIPPED, 2027)")
    before = {d: {c: dq_value(d, c) for c in CHECKS5} for d in (ETL, ELT)}
    apply(5)
    run_both()
    run = (latest(ETL) or (None, ""))[1]
    qf = QDIR / f"orders_failed_{run}.csv"
    if qf.exists():
        df = pd.read_csv(qf, dtype=str, keep_default_na=False)
        if "order_id" not in df.columns:
            df["order_id"], df["failure_reason"] = "", ""
        t = df[df.order_id.str.startswith("ORDTEST")]
        say(f"  ETL quarantine file {qf.name}: {len(t)} planted rows found")
        for r in t.itertuples():
            say(f"      {r.order_id}: {r.failure_reason}")
    else:
        say("  ETL quarantine file for the latest run not found")
    rows = q("select order_id, dq_status, failure_reason from stg_orders where order_id like 'ORDTEST%' order by 1") or []
    say(f"  ELT stg_orders: {len(rows)} planted rows found")
    for r in rows:
        say(f"      {r[0]}: {r[1]} | {r[2]}")
    leak_etl = val("select count(*) from etl_fact_orders where order_id like 'ORDTEST%'")
    leak_elt = val("select count(*) from stg_orders where order_id like 'ORDTEST%' and dq_status <> 'REJECTED'")
    say(f"  leaked into reporting? ETL fact={leak_etl}   ELT stg (not rejected)={leak_elt}")
    for d in (ETL, ELT):
        say(f"  dq_results ({tag(d)}) rows_failed before -> after:")
        for c in CHECKS5:
            a = dq_value(d, c)
            say(f"      {c}: {before[d][c][0] if before[d][c] else None} -> {a[0] if a else None}")
    rebuild()


def exp6():
    say("EXPERIMENT 6: schema change (order_amount renamed to total in orders.csv)")
    b = snapshot()
    apply(6)
    run_both()
    a = snapshot()
    say("  before: " + fmt(b))
    say("  after : " + fmt(a))
    say(f"  CHANGE: raw_orders {d(a['raw_orders'], b['raw_orders'])}  etl_fact {d(a['etl_fact'], b['etl_fact'])}  "
        f"stg_orders {d(a['stg_orders'], b['stg_orders'])}  ELT revenue {b['elt_revenue']} -> {a['elt_revenue']}")
    rebuild()


def exp7():
    say("EXPERIMENT 7: volume anomaly (orders.csv cut from 2412 to 50 rows)")
    b = snapshot()
    apply(7)
    run_both()
    a = snapshot()
    say("  before: " + fmt(b))
    say("  after : " + fmt(a))
    for d in (ETL, ELT):
        r = q("select check_name, status, rows_failed from dq_results where dag_id = ? and run_id = ? and check_name like '%volume%'",
              [d, dq_run(d)])
        say(f"  {tag(d)} volume check in dq_results: {r if r else 'NONE: nothing warned about the 98% drop'}")
    say(f"  ETL vs ELT revenue now: {a['etl_revenue']} vs {a['elt_revenue']}")
    rebuild()


def store_rev(t, store):
    return val(f"select round(sum(net_revenue),2) from {t} where store_id = '{store}'")


def exp8():
    say("EXPERIMENT 8: distribution drift (unit_price x100 for store S05 in orders.csv)")
    chk = "orders.order_amount_matches_formula"
    bs = snapshot()
    b = dict(etl_s05=store_rev("etl_daily_zone_revenue", "S05"), elt_s05=store_rev("mart_daily_store_kpis", "S05"),
             etl_chk=dq_value(ETL, chk), elt_chk=dq_value(ELT, chk))
    apply(8)
    run_both()
    a = dict(etl_s05=store_rev("etl_daily_zone_revenue", "S05"), elt_s05=store_rev("mart_daily_store_kpis", "S05"),
             etl_chk=dq_value(ETL, chk), elt_chk=dq_value(ELT, chk))
    s = snapshot()
    say(f"  store S05 revenue  ETL: {b['etl_s05']} -> {a['etl_s05']}   ELT: {b['elt_s05']} -> {a['elt_s05']}")
    say(f"  check {chk} rows_failed  ETL: {b['etl_chk']} -> {a['etl_chk']}   ELT: {b['elt_chk']} -> {a['elt_chk']}")
    say(f"  total revenue  ETL: {bs['etl_revenue']} -> {s['etl_revenue']}   ELT: {bs['elt_revenue']} -> {s['elt_revenue']}")
    rebuild()


def exp9():
    say("EXPERIMENT 9: freshness (latest order is 30 days old; increment removed)")
    chk = "orders.latest_order_not_stale"
    b = dict(etl=dq_value(ETL, chk), elt=dq_value(ELT, chk))
    apply(9)
    run_both()
    a = dict(etl=dq_value(ETL, chk), elt=dq_value(ELT, chk))
    say(f"  {chk} (rows_failed, status)  ETL: {b['etl']} -> {a['etl']}   ELT: {b['elt']} -> {a['elt']}")
    cutoff = "current_timestamp::TIMESTAMP"
    etl_max = val(f"select max(order_ts) from etl_fact_orders where order_ts <= {cutoff}")
    elt_max = val(f"select max(order_ts) from stg_orders where order_ts <= {cutoff}")
    say(f"  newest valid order_ts  ETL fact: {etl_max}   ELT stg: {elt_max}")
    rebuild()


def exp6():
    say("EXPERIMENT 6: schema change (order_amount renamed to total in orders.csv)")
    b = snapshot()
    apply(6)
    run_both()
    a = snapshot()
    say("  before: " + fmt(b))
    say("  after : " + fmt(a))
    say(f"  CHANGE: raw_orders {d(a['raw_orders'], b['raw_orders'])}  etl_fact {d(a['etl_fact'], b['etl_fact'])}  "
        f"stg_orders {d(a['stg_orders'], b['stg_orders'])}  ELT revenue {b['elt_revenue']} -> {a['elt_revenue']}")
    rebuild()


def exp7():
    say("EXPERIMENT 7: volume anomaly (orders.csv cut from 2412 to 50 rows)")
    b = snapshot()
    apply(7)
    run_both()
    a = snapshot()
    say("  before: " + fmt(b))
    say("  after : " + fmt(a))
    for d in (ETL, ELT):
        r = q("select check_name, status, rows_failed from dq_results where dag_id = ? and run_id = ? and check_name like '%volume%'",
              [d, dq_run(d)])
        say(f"  {tag(d)} volume check in dq_results: {r if r else 'NONE: nothing warned about the 98% drop'}")
    say(f"  ETL vs ELT revenue now: {a['etl_revenue']} vs {a['elt_revenue']}")
    rebuild()


def store_rev(t, store):
    return val(f"select round(sum(net_revenue),2) from {t} where store_id = '{store}'")


def exp8():
    say("EXPERIMENT 8: distribution drift (unit_price x100 for store S05 in orders.csv)")
    chk = "orders.order_amount_matches_formula"
    bs = snapshot()
    b = dict(etl_s05=store_rev("etl_daily_zone_revenue", "S05"), elt_s05=store_rev("mart_daily_store_kpis", "S05"),
             etl_chk=dq_value(ETL, chk), elt_chk=dq_value(ELT, chk))
    apply(8)
    run_both()
    a = dict(etl_s05=store_rev("etl_daily_zone_revenue", "S05"), elt_s05=store_rev("mart_daily_store_kpis", "S05"),
             etl_chk=dq_value(ETL, chk), elt_chk=dq_value(ELT, chk))
    s = snapshot()
    say(f"  store S05 revenue  ETL: {b['etl_s05']} -> {a['etl_s05']}   ELT: {b['elt_s05']} -> {a['elt_s05']}")
    say(f"  check {chk} rows_failed  ETL: {b['etl_chk']} -> {a['etl_chk']}   ELT: {b['elt_chk']} -> {a['elt_chk']}")
    say(f"  total revenue  ETL: {bs['etl_revenue']} -> {s['etl_revenue']}   ELT: {bs['elt_revenue']} -> {s['elt_revenue']}")
    rebuild()


def exp9():
    say("EXPERIMENT 9: freshness (latest order is 30 days old; increment removed)")
    chk = "orders.latest_order_not_stale"
    b = dict(etl=dq_value(ETL, chk), elt=dq_value(ELT, chk))
    apply(9)
    run_both()
    a = dict(etl=dq_value(ETL, chk), elt=dq_value(ELT, chk))
    say(f"  {chk} (rows_failed, status)  ETL: {b['etl']} -> {a['etl']}   ELT: {b['elt']} -> {a['elt']}")
    say("  newest valid order_ts  ETL fact: " + str(val("select max(order_ts) from etl_fact_orders where order_ts <= current_timestamp::TIMESTAMP"))
        + "   ELT stg: " + str(val("select max(order_ts) from stg_orders where order_ts <= current_timestamp::TIMESTAMP")))
    rebuild()


def exp4():
    say("EXPERIMENT 4: missing source file (products.csv renamed). Retries take about 4 minutes per DAG")
    apply(4)
    run_both()
    exp_tool("restore")
    say("  files restored. NEXT = experiment 10 (recovery): in the Airflow UI open the FAILED run of each DAG,")
    say("  click the failed task extract_products -> Clear Task (keep Downstream ticked) and watch it recover.")


EXPS = {2: exp2, 3: exp3, 5: exp5, 6: exp6, 7: exp7, 8: exp8, 9: exp9, 4: exp4}

if __name__ == "__main__":
    wanted = [int(a) for a in sys.argv[1:]] or list(EXPS)
    say("#" * 90)
    say(f"run_experiments.py started {time.strftime('%Y-%m-%d %H:%M:%S')}  experiments: {wanted}")
    for n in wanted:
        say("=" * 90)
        try:
            EXPS[n]()
        finally:
            exp_tool("restore")
    say("=" * 90)
    say("ALL DONE. Data folder: " + exp_tool("status")[1])
