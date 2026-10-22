# NammaMart Pipeline Submission Notes

## Part C: Schedule

- **Cron:** `0 9 * * 1-6` (09:00, Monday through Saturday).
- **Timezone:** Both DAGs use `pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata")`. The schedule fires at 09:00 IST, which is **03:30 UTC**. In UTC, that is the time shown for the scheduled run; the Airflow UI can display a configured user timezone.
- **Catchup:** `catchup=False` avoids launching a backlog of historical daily runs when these DAGs are first enabled or resumed. That suits this local assignment pipeline, where a missed interval should be handled deliberately rather than replayed automatically.
- **Retries and timeout:** Each DAG retries a failed task twice, with a two-minute delay, and gives each task a 25-minute execution timeout.

### Schedule questions

1. **Cron expression:** `0 9 * * 1-6`.
2. **UTC time:** 03:30 UTC.
3. **If `start_date` were naive/UTC:** Airflow would interpret the cron schedule in UTC, so it would run at 09:00 UTC (14:30 IST), 5 hours 30 minutes after the intended local time. The DAG timezone is derived from its timezone-aware `start_date`.
4. **If the laptop or Docker is off at 9 AM:** The local scheduler cannot create or run the scheduled DAG run while it is stopped. With `catchup=False`, Airflow does not automatically replay the missed scheduled interval after restart. Manually trigger/recover the run if that day's report is still needed. The next scheduled run remains 09:00 IST on the next Monday–Saturday schedule.
