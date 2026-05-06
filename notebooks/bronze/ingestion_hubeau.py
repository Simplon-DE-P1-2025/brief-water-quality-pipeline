# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze - Hub'Eau Water Quality Ingestion
# MAGIC
# MAGIC Uses dlt (https://dlthub.com) for Delta Lake writes.
# MAGIC All parameters are in config/config.yaml — do NOT edit this script.
# MAGIC
# MAGIC Pagination strategy (4 levels):
# MAGIC   year -> quarter -> month -> week

# COMMAND ----------

import os
import json
import time
import calendar
import requests
import yaml
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ## Context

# COMMAND ----------


class Context:
    def __init__(self):
        self.dept = None
        self.year = None
        self.month = None


CTX = Context()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logger

# COMMAND ----------


def log(stage, msg, extra=None):
    ts = datetime.now().strftime("%H:%M:%S")
    ctx = f"dept={
        CTX.dept} year={
        CTX.year} month={
            CTX.month}".replace(
                "None",
        "-")
    extras = (
        " | " +
        ", ".join(
            f"{k}={v}" for k,
            v in extra.items())) if extra else ""
    print(f"[{ts}] [{stage}] {ctx} | {msg}{extras}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Departments

# COMMAND ----------


def get_departments():
    """Returns the full list of French department codes."""
    return (
        [f"{i:02d}" for i in range(1, 20)]
        + ["2A", "2B"]
        + [f"{i:02d}" for i in range(21, 96)]
        + ["971", "972", "973", "974", "976"]
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------


def load_config(path="config/config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def resolve_scope(cfg):
    """
    Resolves active mode (dev/prod) from config.
    Expands DEFAULT_FRANCE sentinel to the full department list.
    """
    active = cfg["environment"]["active_mode"]
    scope = cfg["environment"]["mode"][active]
    depts = scope["departments"]
    if depts == "DEFAULT_FRANCE":
        depts = get_departments()
    return {
        "years": scope["years"],
        "departments": depts,
        "max_workers": scope["max_workers"],
        "active_mode": active,
    }


cfg = load_config()
hubeau_cfg = cfg["pipelines"]["hubeau"]

IS_DATABRICKS = (
    cfg["environment"]["is_databricks"]
    or "DATABRICKS_RUNTIME_VERSION" in os.environ
)

BRONZE_PATH = (
    cfg["storage"]["bronze"]["databricks"]
    if IS_DATABRICKS
    else cfg["storage"]["bronze"]["local"]
)

scope = resolve_scope(cfg)
YEARS = scope["years"]
DEPARTMENTS = scope["departments"]
MAX_WORKERS = scope["max_workers"]

API_URL = hubeau_cfg["api_url"]
MAX_DEPTH = hubeau_cfg["max_depth"]
MAX_RETRIES = hubeau_cfg["max_retries"]
SLEEP_S = hubeau_cfg["pagination"]["sleep_between_requests"]
END_DAY = hubeau_cfg["date_format"]["end_of_day_suffix"]
FORCE_EOD = hubeau_cfg["date_format"]["force_end_of_day"]

log("CONFIG", "loaded", {
    "env": "databricks" if IS_DATABRICKS else "local",
    "mode": scope["active_mode"],
    "depts": len(DEPARTMENTS),
    "years": len(YEARS),
    "workers": MAX_WORKERS,
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## API

# COMMAND ----------


def build_params(dept, d_min, d_max):
    """Builds Hub'Eau API parameters. Appends T23:59:59Z on date_max if needed."""
    if FORCE_EOD and "T" not in str(d_max):
        d_max = f"{d_max}{END_DAY}"
    return {
        "code_departement": dept,
        "date_min_prelevement": d_min,
        "date_max_prelevement": d_max,
        "size": MAX_DEPTH,
        "page": 1,
    }


def fetch(params):
    """Calls Hub'Eau API with exponential retry on timeout."""
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(API_URL, params=params, timeout=60)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.ReadTimeout:
            wait = 5 * (2 ** attempt)
            log("API", "timeout", {"attempt": attempt + 1, "wait_s": wait})
            time.sleep(wait)
            if attempt == MAX_RETRIES - 1:
                raise
        except Exception as e:
            log("API", "error", {"error": str(e)})
            return {}
    return {}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pagination

# COMMAND ----------


def get_weeks(year, month):
    """Returns (d_min, d_max) pairs for each week of the month."""
    last = calendar.monthrange(year, month)[1]
    weeks = [
        (f"{year}-{month:02d}-01", f"{year}-{month:02d}-07{END_DAY}"),
        (f"{year}-{month:02d}-08", f"{year}-{month:02d}-14{END_DAY}"),
        (f"{year}-{month:02d}-15", f"{year}-{month:02d}-21{END_DAY}"),
        (f"{year}-{month:02d}-22", f"{year}-{month:02d}-28{END_DAY}"),
    ]
    if last > 28:
        weeks.append((f"{year}-{month:02d}-29",
                      f"{year}-{month:02d}-{last:02d}{END_DAY}"))
    return weeks


def fetch_by_week(dept, year, month):
    """Level 4 - fetches week by week within a month."""
    CTX.dept = dept
    CTX.year = year
    CTX.month = month
    records = []
    for d_min, d_max in get_weeks(year, month):
        data = fetch(build_params(dept, d_min, d_max))
        rows = data.get("data", [])
        log("WEEK", f"{d_min} -> {d_max}", {"rows": len(rows)})
        records.extend(rows)
        time.sleep(SLEEP_S)
    return records


def fetch_by_month(dept, year, month):
    """Level 3 - fetches one month, splits by week if above max_depth."""
    CTX.dept = dept
    CTX.year = year
    CTX.month = month
    last = calendar.monthrange(year, month)[1]
    d_min = f"{year}-{month:02d}-01"
    d_max = f"{year}-{month:02d}-{last:02d}"
    data = fetch(build_params(dept, d_min, d_max))
    count = data.get("count", 0)
    if count == 0:
        return []
    if count > MAX_DEPTH:
        log("MONTH", "split by week", {"month": month, "count": count})
        return fetch_by_week(dept, year, month)
    log("MONTH", "ok", {"month": month, "rows": count})
    return data.get("data", [])


QUARTERS = [
    ("-01-01", "-03-31", 1, 3),
    ("-04-01", "-06-30", 4, 6),
    ("-07-01", "-09-30", 7, 9),
    ("-10-01", "-12-31", 10, 12),
]


def fetch_by_quarter(dept, year):
    """Level 2 - fetches quarter by quarter, splits by month if above max_depth."""
    CTX.dept = dept
    CTX.year = year
    CTX.month = None
    records = []
    for q_min, q_max, m_start, m_end in QUARTERS:
        d_min = f"{year}{q_min}"
        d_max = f"{year}{q_max}"
        data = fetch(build_params(dept, d_min, d_max))
        count = data.get("count", 0)
        if count == 0:
            continue
        if count > MAX_DEPTH:
            log("QUARTER", "split by month", {
                "quarter": d_min, "count": count})
            for m in range(m_start, m_end + 1):
                records.extend(fetch_by_month(dept, year, m))
        else:
            log("QUARTER", "ok", {"quarter": d_min, "rows": count})
            records.extend(data.get("data", []))
        time.sleep(SLEEP_S)
    return records


def fetch_year(dept, year):
    """Level 1 - fetches one year, splits by quarter if above max_depth."""
    CTX.dept = dept
    CTX.year = year
    CTX.month = None
    data = fetch(build_params(dept, f"{year}-01-01", f"{year}-12-31"))
    count = data.get("count", 0)
    if count == 0:
        log("YEAR", "no data")
        return dept, year, []
    if count <= MAX_DEPTH:
        log("YEAR", "ok", {"rows": count})
        return dept, year, data.get("data", [])
    log("YEAR", "split by quarter", {"count": count})
    return dept, year, fetch_by_quarter(dept, year)


def prepare_record(rec, yr):
    """
    Enriches a raw API record before yielding to dlt:
    - Adds annee_partition from date_prelevement
    - Serializes list/dict fields to JSON string
    """
    date = rec.get("date_prelevement", "")
    rec["annee_partition"] = int(date[:4]) if date and len(date) >= 4 else yr
    for k, v in list(rec.items()):
        if isinstance(v, (list, dict)):
            rec[k] = json.dumps(v, ensure_ascii=False)
    return rec

# COMMAND ----------

# MAGIC %md
# MAGIC ## dlt Resource

# COMMAND ----------


@dlt.resource(
    name="water_quality",
    write_disposition="append",
    primary_key=hubeau_cfg["primary_key"],
    columns={
        "annee_partition": {"data_type": "bigint", "partition": True},
        "libelle_parametre_web": {"data_type": "text"},
    },
)
def water_quality():
    """
    dlt generator for Hub'Eau water quality data.
    Collects all records in parallel (ThreadPoolExecutor), then yields to dlt.
    Pool is fully closed before yielding to avoid generator deadlocks.
    """
    tasks = [(d, y) for d in DEPARTMENTS for y in YEARS]
    all_rows = []
    total = 0

    log("DLT", "start", {"tasks": len(tasks), "workers": MAX_WORKERS})

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_year, d, y): (d, y) for d, y in tasks}
        for future in as_completed(futures):
            dept, year = futures[future]
            try:
                _, _, rows = future.result()
            except Exception as exc:
                log("DLT", "task failed", {
                    "dept": dept, "year": year, "error": str(exc)})
                continue
            rows = [prepare_record(r, year) for r in rows]
            total += len(rows)
            all_rows.extend(rows)
            log("DLT",
                "task done",
                {"dept": dept,
                 "year": year,
                 "rows": len(rows),
                 "total": total})

    log("DLT", "fetch complete -- yielding to dlt", {"total": total})
    yield from all_rows
    log("DLT", "yield done", {"total": total})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline

# COMMAND ----------


if __name__ == "__main__":

    os.makedirs(".dlt", exist_ok=True)
    with open(".dlt/config.toml", "w") as f:
        f.write(f'[destination.filesystem]\nbucket_url = "{BRONZE_PATH}"\n')

    pipeline = dlt.pipeline(
        pipeline_name=hubeau_cfg["pipeline_name"],
        destination="filesystem",
        dataset_name=hubeau_cfg["dataset_name"],
        progress=False,
    )

    log("PIPELINE", "start")

    result = pipeline.run(
        water_quality(),
        table_format=hubeau_cfg["file_format"])

    log("PIPELINE", "end", {"result": str(result)})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

    import pandas as pd
    import glob

    table_path = f"{BRONZE_PATH}/{hubeau_cfg['dataset_name']}/water_quality"
    files = glob.glob(f"{table_path}/**/*.parquet", recursive=True)

    if files:
        df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
        log("VALIDATION", "bronze summary", {
            "rows": len(df),
            "columns": len(df.columns),
            "departments": df["code_departement"].nunique(),
            "communes": df["code_commune"].nunique(),
            "parameters": df["libelle_parametre"].nunique(),
            "delta_log": "present" if os.path.exists(f"{table_path}/_delta_log") else "MISSING",
        })
    else:
        log("VALIDATION", "no parquet files found")

# COMMAND ----------
