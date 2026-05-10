# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Hub'Eau Water Quality Ingestion
# MAGIC
# MAGIC Pure PySpark / Pandas ingestion depuis l'API Hub'Eau.
# MAGIC
# MAGIC Features:
# MAGIC - Pagination hiérarchique : année → trimestre → mois → semaine
# MAGIC - Extraction parallèle avec ThreadPoolExecutor
# MAGIC - Écriture Delta Lake via saveAsTable (Unity Catalog)
# MAGIC - Partitionné par annee_partition
# MAGIC - Compatible local + Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import os
import json
import time
import calendar
import requests
import yaml

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logger

# COMMAND ----------


def log(stage, msg, dept=None, year=None, month=None, extra=None):
    ts = datetime.now().strftime("%H:%M:%S")
    ctx = (
        f"dept={dept if dept is not None else '-'} "
        f"year={year if year is not None else '-'} "
        f"month={month if month is not None else '-'}"
    )
    extras = (
        " | " + ", ".join(f"{k}={v}" for k, v in extra.items())
    ) if extra else ""
    print(f"[{ts}] [{stage}] {ctx} | {msg}{extras}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------


def get_departments():
    """Retourne la liste complète des codes départements français."""
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


def load_config():
    """Charge config.yaml selon l'environnement (local ou Databricks)."""
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        base_dir = "/Workspace/Users/krhazlani.ext@simplonformations.co/brief-water-quality-pipeline"
    else:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "config/config.yaml")
    with open(config_path) as f:
        return yaml.safe_load(f)


def resolve_scope(cfg):
    """Résout le mode actif (dev/prod) et expand DEFAULT_FRANCE."""
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
uc_cfg = cfg["unity_catalog"]
storage = cfg["storage"]
secrets = storage["secrets"]

IS_DATABRICKS = (
    cfg["environment"]["is_databricks"]
    or "DATABRICKS_RUNTIME_VERSION" in os.environ
)

# ── Paramètres Unity Catalog (depuis config.yaml) ──────────────────────────
CATALOG = uc_cfg["catalog"]
SCHEMA = uc_cfg["schema"]
TABLE_NAME = uc_cfg["table"]
TABLE_FULL_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"

# ── Paramètres storage (depuis config.yaml) ────────────────────────────────
STORAGE_ACCOUNT = storage["account_name"]
SECRETS_SCOPE = secrets["scope"]
SECRET_KEY_NAME = secrets["storage_account_key"]

BRONZE_BASE = (
    storage["bronze"]["databricks"]
    if IS_DATABRICKS
    else storage["bronze"]["local"]
)

# ── Paramètres API ─────────────────────────────────────────────────────────
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

log("CONFIG", "loaded", extra={
    "env": "databricks" if IS_DATABRICKS else "local",
    "mode": scope["active_mode"],
    "catalog": TABLE_FULL_NAME,
    "depts": len(DEPARTMENTS),
    "years": len(YEARS),
    "workers": MAX_WORKERS,
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark — Configuration storage account key

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
log("SPARK", "session ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## API

# COMMAND ----------


def build_params(dept, d_min, d_max):
    """Construit les paramètres de l'appel Hub'Eau."""
    if FORCE_EOD and "T" not in str(d_max):
        d_max = f"{d_max}{END_DAY}"
    return {
        "code_departement": dept,
        "date_min_prelevement": d_min,
        "date_max_prelevement": d_max,
        "size": MAX_DEPTH,
        "page": 1,
    }


def fetch(params, dept=None, year=None, month=None):
    """Appel Hub'Eau avec retry exponentiel."""
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(API_URL, params=params, timeout=60)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.ReadTimeout:
            wait = 5 * (2 ** attempt)
            log("API", "timeout", dept=dept, year=year, month=month,
                extra={"attempt": attempt + 1, "wait_s": wait})
            time.sleep(wait)
            if attempt == MAX_RETRIES - 1:
                raise
        except Exception as e:
            log("API", "error", dept=dept, year=year, month=month,
                extra={"error": str(e)})
            return {}
    return {}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pagination

# COMMAND ----------


def get_weeks(year, month):
    """Retourne les paires (d_min, d_max) par semaine."""
    last = calendar.monthrange(year, month)[1]
    weeks = [
        (f"{year}-{month:02d}-01", f"{year}-{month:02d}-07{END_DAY}"),
        (f"{year}-{month:02d}-08", f"{year}-{month:02d}-14{END_DAY}"),
        (f"{year}-{month:02d}-15", f"{year}-{month:02d}-21{END_DAY}"),
        (f"{year}-{month:02d}-22", f"{year}-{month:02d}-28{END_DAY}"),
    ]
    if last > 28:
        weeks.append((
            f"{year}-{month:02d}-29",
            f"{year}-{month:02d}-{last:02d}{END_DAY}"
        ))
    return weeks


def fetch_by_week(dept, year, month):
    records = []
    for d_min, d_max in get_weeks(year, month):
        data = fetch(
            build_params(
                dept,
                d_min,
                d_max),
            dept=dept,
            year=year,
            month=month)
        rows = data.get("data", [])
        log("WEEK", f"{d_min} -> {d_max}", dept=dept, year=year, month=month,
            extra={"rows": len(rows)})
        records.extend(rows)
        time.sleep(SLEEP_S)
    return records


def fetch_by_month(dept, year, month):
    last = calendar.monthrange(year, month)[1]
    d_min = f"{year}-{month:02d}-01"
    d_max = f"{year}-{month:02d}-{last:02d}"
    data = fetch(
        build_params(
            dept,
            d_min,
            d_max),
        dept=dept,
        year=year,
        month=month)
    count = data.get("count", 0)
    if count == 0:
        return []
    if count > MAX_DEPTH:
        log("MONTH", "split by week", dept=dept, year=year, month=month,
            extra={"count": count})
        return fetch_by_week(dept, year, month)
    log("MONTH", "ok", dept=dept, year=year,
        month=month, extra={"rows": count})
    return data.get("data", [])


QUARTERS = [
    (1, 3, "01-01", "03-31"),
    (4, 6, "04-01", "06-30"),
    (7, 9, "07-01", "09-30"),
    (10, 12, "10-01", "12-31"),
]


def fetch_by_quarter(dept, year):
    records = []
    for m_start, m_end, q_start, q_end in QUARTERS:
        d_min = f"{year}-{q_start}"
        d_max = f"{year}-{q_end}"
        data = fetch(build_params(dept, d_min, d_max), dept=dept, year=year)
        count = data.get("count", 0)
        if count == 0:
            continue
        if count > MAX_DEPTH:
            log("QUARTER", "split by month", dept=dept, year=year,
                extra={"quarter": d_min, "count": count})
            for m in range(m_start, m_end + 1):
                records.extend(fetch_by_month(dept, year, m))
        else:
            log("QUARTER", "ok", dept=dept, year=year,
                extra={"quarter": d_min, "rows": count})
            records.extend(data.get("data", []))
        time.sleep(SLEEP_S)
    return records


def fetch_year(dept, year):
    data = fetch(build_params(dept, f"{year}-01-01", f"{year}-12-31"),
                 dept=dept, year=year)
    count = data.get("count", 0)
    if count == 0:
        log("YEAR", "no data", dept=dept, year=year)
        return dept, year, []
    if count <= MAX_DEPTH:
        log("YEAR", "ok", dept=dept, year=year, extra={"rows": count})
        return dept, year, data.get("data", [])
    log("YEAR", "split by quarter", dept=dept,
        year=year, extra={"count": count})
    return dept, year, fetch_by_quarter(dept, year)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform

# COMMAND ----------


def prepare_record(rec, yr):
    """Enrichit un enregistrement brut de l'API."""
    date = rec.get("date_prelevement", "")
    rec["annee_partition"] = (
        int(date[:4]) if date and len(date) >= 4 else yr
    )
    for k, v in list(rec.items()):
        if isinstance(v, (list, dict)):
            rec[k] = json.dumps(v, ensure_ascii=False)
    return rec

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extraction principale

# COMMAND ----------


def run_pipeline():
    tasks = [(d, y) for d in DEPARTMENTS for y in YEARS]
    all_rows = []
    total = 0
    log("PIPELINE", "start", extra={
        "tasks": len(tasks), "workers": MAX_WORKERS})

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_year, d, y): (d, y)
            for d, y in tasks
        }
        for future in as_completed(futures):
            dept, year = futures[future]
            try:
                _, _, rows = future.result()
            except Exception as exc:
                log("PIPELINE", "task failed", dept=dept, year=year,
                    extra={"error": str(exc)})
                continue
            rows = [prepare_record(r, year) for r in rows]
            total += len(rows)
            all_rows.extend(rows)
            log("PIPELINE", "task done", dept=dept, year=year,
                extra={"rows": len(rows), "total": total})

    log("PIPELINE", "fetch complete", extra={"total_rows": total})
    return all_rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Écriture Delta — Unity Catalog

# COMMAND ----------


def write_delta(rows):
    """Écrit en Unity Catalog ET dans le container bronze ADLS."""
    if not rows:
        log("WRITE", "no rows to write")
        return

    log("WRITE", "creating spark dataframe", extra={"rows": len(rows)})
    pdf = pd.DataFrame(rows).astype(str)
    sdf = spark.createDataFrame(pdf)
    log("WRITE", "spark dataframe created",
        extra={"rows": sdf.count(), "columns": len(sdf.columns)})

    # ── 1. Unity Catalog (table managée) ──────────────────────
    (
        sdf.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("annee_partition")
        .saveAsTable(TABLE_FULL_NAME)
    )
    log("WRITE", "unity catalog written", extra={"table": TABLE_FULL_NAME})

    # ── 2. ADLS Gen2 container bronze (partitionné) ───────────
    storage_key = dbutils.secrets.get(scope=SECRETS_SCOPE, key=SECRET_KEY_NAME)  # noqa: F821
    adls_path = f"{BRONZE_BASE}/water_quality/"

    (
        sdf.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("annee_partition")
        .option(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", storage_key)
        .save(adls_path)
    )
    log("WRITE", "adls bronze written", extra={"path": adls_path})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------


def validate(table_name):
    """Affiche un résumé de la table Delta écrite."""
    df = spark.read.table(table_name)
    log("VALIDATION", "delta summary", extra={
        "table": table_name,
        "rows": df.count(),
        "columns": len(df.columns),
        "departments": df.select("code_departement").distinct().count(),
        "communes": df.select("code_commune").distinct().count(),
        "parameters": df.select("libelle_parametre").distinct().count(),
        "partitions": df.select("annee_partition").distinct().count(),
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exécution

# COMMAND ----------


if __name__ == "__main__":
    rows = run_pipeline()
    write_delta(rows)
    validate(TABLE_FULL_NAME)

    log("PIPELINE", "end", extra={"rows": len(rows)})

