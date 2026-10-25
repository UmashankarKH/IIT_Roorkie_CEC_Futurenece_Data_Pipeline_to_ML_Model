param(
    [ValidateSet("etl", "elt", "both")]
    [string]$Pipeline = "both"
)

$ErrorActionPreference = "Stop"

# Both DAGs are safe to trigger together because ETL and ELT use separate
# DuckDB files. Airflow still limits each individual DAG to one active run.

# These are the DAG IDs declared in the NammaMart DAG files.
$dagIds = switch ($Pipeline) {
    "etl"  { @("nammamart_etl_rudin") }
    "elt"  { @("nammamart_elt_rudin") }
    "both" { @("nammamart_etl_rudin", "nammamart_elt_rudin") }
}

Write-Host "Triggering NammaMart pipeline(s): $($dagIds -join ', ')"
foreach ($dagId in $dagIds) {
    Write-Host "`n--- Triggering $dagId ---"
    astro dev run dags trigger $dagId
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to trigger $dagId. Confirm that Astro is running with: astro dev start"
    }
}

Write-Host "`nManual trigger request submitted successfully. Monitor the runs at http://localhost:8080."
