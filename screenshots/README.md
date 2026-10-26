# Airflow local setup — build issues and fixes

Notes from getting **Astro CLI + Docker** running for the NammaMart assignment (`astro dev start`).

## 1. `zsh: command not found: astro`

**Cause:** Astronomer CLI was not installed (or Homebrew’s `bin` was not on `PATH`).

**Fix:**

```bash
brew install astro
export PATH="/Users/uday.komati/.brew/bin:$PATH"   # add to ~/.zshrc to persist
astro version
```

---

## 2. `astro dev run …` → “project not running”

**Cause:** Commands like `astro dev run dags test …` need the stack already up.

**Fix:** Start the project first, wait until containers are healthy, then run tests:

```bash
cd IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model
astro dev start
astro dev run dags list-import-errors
```

UI: http://localhost:8080 (`admin` / `admin`)

---

## 3. Docker build failed — `duckdb==2.0.0` download timeout

**Symptom (build log):**

```text
× Failed to download `duckdb==2.0.0`
╰─▶ operation timed out
```

**Cause:** Large Python wheels (`duckdb`, `scipy`, `scikit-learn`, etc.) downloaded from PyPI during the image build; slow or unstable network caused `uv` to time out.

**Fix:**

- Retry `astro dev start` on stable Wi‑Fi (avoid VPN if it slows PyPI).
- In **`Dockerfile`**, increase install timeouts:

  ```dockerfile
  ENV UV_HTTP_TIMEOUT=600
  ENV UV_HTTP_RETRIES=10
  ```

- Keep **`duckdb==2.0.0`** in `requirements.txt` (prebuilt wheel for the Astro runtime).

---

## 4. Docker build failed — `duckdb==1.1.3` / `No such file or directory: 'c++'`

**Symptom:**

```text
Building duckdb==1.1.3
error: [Errno 2] No such file or directory: 'c++'
```

**Cause:** On Astro Runtime **Python 3.14**, there was no matching prebuilt wheel for older DuckDB, so the installer tried to **compile from source** without a C++ toolchain in the image.

**Fix:** Do **not** pin an old DuckDB on this runtime. Use **`duckdb==2.0.0`** (wheel) instead of `1.1.3`.

---

## 5. Smaller / faster Docker build context

**Change:** Updated **`.dockerignore`** to exclude heavy local files from the build context, for example:

- `.venv`
- `include/warehouse*.duckdb`
- `assignment/`

This does not fix PyPI timeouts by itself but keeps builds leaner.

---

## After a successful build

```bash
astro dev run dags list-import-errors          # expect: No data found
astro dev run dags test nammamart_etl_udaykiran_komati
astro dev run dags test nammamart_elt_udaykiran_komati
```

Capture assignment screenshots in this folder (DAG graph, schedule, quarantine, `dq_results`, `pipeline_runs`, reconciliation) and link them from `SUBMISSION.md`.
