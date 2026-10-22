"""
Run a NammaMart DAG end-to-end WITHOUT Airflow (eager shim). Used for fast iteration and the experiments.
The real proof is `astro dev run dags test <dag_id>` / the Airflow UI; this runs the very same task bodies.

  python scripts/run_local.py etl|elt [--increment] [--run-id X] [--as-of "2026-10-02 09:00:00"]
Env overrides: NAMMAMART_DATA_DIR, NAMMAMART_WAREHOUSE, NAMMAMART_QUARANTINE_DIR, NAMMAMART_WORK_DIR
"""
import argparse
import importlib.util
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "include"))

STATE = {"results": {}, "params": {}, "run_id": "local", "as_of": None, "failed": set(), "all_done": []}


class Result(dict):
    def __rshift__(self, other):
        return other


class Ti:
    def xcom_pull(self, task_ids=None):
        return STATE["results"].get(task_ids)


def get_current_context():
    return {"run_id": STATE["run_id"], "params": STATE["params"], "ti": Ti(),
            "dag_run": types.SimpleNamespace(start_date=STATE["start"])}


def task(fn=None, **opts):
    def deco(f):
        def call(*a, **k):
            name = f.__name__
            try:
                out = f(*a, **k)
            except Exception as e:  # noqa: BLE001
                STATE["failed"].add(name)
                print(f"!! task {name} FAILED: {type(e).__name__}: {e}")
                if opts.get("trigger_rule") != "all_done":
                    raise
                return None
            STATE["results"][name] = out
            return Result(out) if isinstance(out, dict) else out
        call.__name__ = f.__name__
        if opts.get("trigger_rule") == "all_done":
            STATE["all_done"].append(call)
        return call
    return deco(fn) if fn else deco


def dag(**kw):
    def deco(f):
        return f
    return deco


class Variable:
    @staticmethod
    def get(key, default=None):
        return STATE["as_of"] if key == "nammamart_as_of" and STATE["as_of"] else default


def Param(default, **kw):
    return default


def run(which, increment=False, run_id="manual__local", as_of=None, dag_file=None):
    sdk = types.ModuleType("airflow.sdk")
    sdk.dag, sdk.task, sdk.Param, sdk.Variable, sdk.get_current_context = dag, task, Param, Variable, get_current_context
    sys.modules["airflow"] = types.ModuleType("airflow")
    sys.modules["airflow.sdk"] = sdk
    exc = types.ModuleType("airflow.sdk.exceptions")
    exc.AirflowFailException = type("AirflowFailException", (Exception,), {})
    sys.modules["airflow.sdk.exceptions"] = exc
    STATE.update(results={}, failed=set(), run_id=run_id, as_of=as_of, start=datetime.now(timezone.utc),
                 params={"load_increment": increment})
    path = dag_file or ROOT / "dags" / f"nammamart_{which}_mohanrajs.py"
    spec = importlib.util.spec_from_file_location(f"dag_{which}", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules.pop(f"dag_{which}", None)
    STATE["all_done"] = []
    try:
        spec.loader.exec_module(mod)
    except Exception:
        for fn in STATE["all_done"]:  # trigger_rule="all_done" tasks still run after a failure
            fn()
        raise
    # the module calls nammamart_xxx_mohanrajs() at import with the dag decorator -> body already ran eagerly
    return STATE


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("which", choices=["etl", "elt"])
    ap.add_argument("--increment", action="store_true")
    ap.add_argument("--run-id", default="manual__local")
    ap.add_argument("--as-of")
    a = ap.parse_args()
    try:
        run(a.which, a.increment, a.run_id, a.as_of)
    except Exception as e:  # noqa: BLE001
        print(f"RUN FAILED: {type(e).__name__}: {e}")
        sys.exit(1)
