# Step 8 — Scheduling decisions

Both NammaMart DAGs use:

```text
0 9 * * 1-6
```

This means 09:00 every Monday through Saturday. The DAG start date is timezone-aware and uses `Asia/Kolkata`, so the scheduled run is 03:30 UTC. `catchup=False` prevents a laptop or Docker outage from creating a backlog of historical runs when the environment starts again. Each task may retry twice after a five-minute delay and has a 30-minute execution timeout.

If the laptop or Docker is off at 09:00 IST, the local scheduler cannot create the run at that time. Because catchup is disabled, the missed run is not automatically backfilled; the operator must start the environment and trigger a recovery run manually.
