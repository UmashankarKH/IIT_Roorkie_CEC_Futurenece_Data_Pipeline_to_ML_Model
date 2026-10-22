"""Tiny Airflow REST helper for the experiments (token, trigger, wait, task table).  python scripts/af.py <cmd> ..."""
import json
import sys
import time
import urllib.request

BASE = "http://localhost:8080"
PWD = json.load(open("simple_auth_manager_passwords.json.generated"))["admin"]


def call(method, path, body=None, token=None):
    req = urllib.request.Request(BASE + path, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json", **({"Authorization": f"Bearer {token}"} if token else {})})
    with urllib.request.urlopen(req) as r:
        txt = r.read().decode()
        return json.loads(txt) if txt else {}


def token():
    return call("POST", "/auth/token", {"username": "admin", "password": PWD})["access_token"]


def trigger(dag_id, conf=None, run_id=None):
    t = token()
    body = {"logical_date": None, "conf": conf or {}}
    if run_id:
        body["dag_run_id"] = run_id
    return call("POST", f"/api/v2/dags/{dag_id}/dagRuns", body, t)


def wait(dag_id, run_id, timeout=600):
    t = token()
    end = time.time() + timeout
    while time.time() < end:
        r = call("GET", f"/api/v2/dags/{dag_id}/dagRuns/{run_id}", token=t)
        if r["state"] in ("success", "failed"):
            return r
        time.sleep(3)
    raise TimeoutError(run_id)


def tasks(dag_id, run_id):
    t = token()
    r = call("GET", f"/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances?limit=100", token=t)
    rows = sorted(r["task_instances"], key=lambda x: x["start_date"] or "")
    return [(x["task_id"], x["state"], x["try_number"], x["start_date"], x["end_date"], x["duration"]) for x in rows]


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "dags":
        for d in call("GET", "/api/v2/dags?limit=50", token=token())["dags"]:
            print(d["dag_id"], d["timetable_summary"], d["timetable_description"], "next:", d["next_dagrun_run_after"], "paused:", d["is_paused"])
    elif cmd == "unpause":
        for d in sys.argv[2:]:
            call("PATCH", f"/api/v2/dags/{d}", {"is_paused": False}, token())
    elif cmd == "errors":
        print(call("GET", "/api/v2/importErrors", token=token()))
    elif cmd == "run":
        dag_id, run_id = sys.argv[2], sys.argv[3]
        conf = json.loads(sys.argv[4]) if len(sys.argv) > 4 else None
        trigger(dag_id, conf, run_id)
        print(wait(dag_id, run_id)["state"])
    elif cmd == "tasks":
        for row in tasks(sys.argv[2], sys.argv[3]):
            print(row)
