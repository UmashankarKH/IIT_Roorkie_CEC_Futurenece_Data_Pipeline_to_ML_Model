# NammaMart DAG Scheduling Notes

The ETL and ELT DAGs are configured to run automatically at 9:00 AM IST from Monday through Saturday.

## Schedule configuration

```text
0 9 * * 1-6
```

Both DAGs use the `Asia/Kolkata` timezone with a timezone-aware Airflow start date:

```python
start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata")
```

The schedule is defined in:

- `nammamart_etl_rudin.py`
- `nammamart_elt_rudin.py`

To verify the schedule from Astro, use the DAG details command:

```powershell
astro dev run dags details nammamart_etl_rudin
astro dev run dags details nammamart_elt_rudin
```

`astro dev run dags list` confirms that the DAGs are registered, but in the current Airflow 3 CLI its final columns are `bundle_name` and `bundle_version`; it does not display the schedule. A local DAG normally shows `dags-folder | None` in those columns. The `None` value is the local bundle version, not a missing schedule. The list output may also show `is_paused=True` until the DAGs are enabled in the Airflow UI.

Additional settings:

```python
catchup=False
retries=2
retry_delay=timedelta(minutes=2)
execution_timeout=timedelta(minutes=30)
max_active_runs=1
```

## UTC display time

India Standard Time is UTC+5:30. Therefore, a run scheduled for 9:00 AM IST appears in the Airflow UI as:

```text
03:30 UTC
```

## Incorrect UTC start date

If the `start_date` were left as a naive or UTC datetime, Airflow could interpret the DAG timezone as UTC. The cron expression would then run at 9:00 AM UTC, which is 2:30 PM IST. This would miss the client's 9:00 AM IST reporting deadline.

## Catchup behavior

`catchup=False` prevents Airflow from creating historical runs for dates before the current scheduling period or for missed intervals. This is appropriate for the daily operational report because the pipeline should process the current source export rather than unexpectedly replay every missed day.

## Laptop or Docker offline

When the laptop or Docker is offline, the local Airflow scheduler is not running and cannot trigger the DAG. After Docker starts again, `catchup=False` means the missed run is not automatically backfilled. The DAG must be triggered manually.

Manual triggering is available through:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\trigger_nammamart.ps1 -Pipeline both
```

## Retries and timeout

If a task fails, Airflow retries it up to two times, waiting two minutes between attempts. A task that runs longer than 30 minutes fails due to `execution_timeout`. `max_active_runs=1` prevents overlapping runs of the same DAG.

The ETL and ELT pipelines use separate DuckDB files, so they can run at the same time without competing for the same database file lock.
