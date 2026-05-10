# Databricks notebook source
# MAGIC %pip install pyyaml requests

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze — Geo API Ingestion
# MAGIC
# MAGIC Ingestion des référentiels géographiques depuis l'API geo.api.gouv.fr.
# MAGIC
# MAGIC Tables produites :
# MAGIC - `regions` — codes et noms des régions
# MAGIC - `departements` — codes, noms et région de rattachement
# MAGIC - `communes` — codes INSEE, coordonnées GPS, population
# MAGIC
# MAGIC Écriture :
# MAGIC - Unity Catalog (table managée via saveAsTable)
# MAGIC - ADLS Gen2 container bronze (Delta partitionné)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import os
import requests
import yaml

from datetime import datetime

import pandas as pd
from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logger

# COMMAND ----------

_current_table = None


def log(stage, msg, extra=None):
    ts = datetime.now().strftime("%H:%M:%S")
    ctx = f"table={_current_table if _current_table else '-'}"
    extras = (
        " | " + ", ".join(f"{k}={v}" for k, v in extra.items())
    ) if extra else ""
    print(f"[{ts}] [{stage}] {ctx} | {msg}{extras}")

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


cfg = load_config()
geo_cfg = cfg["pipelines"]["geo"]
uc_cfg = cfg["unity_catalog"]
storage = cfg["storage"]
secrets = storage["secrets"]

IS_DATABRICKS = (
    cfg["environment"]["is_databricks"]
    or "DATABRICKS_RUNTIME_VERSION" in os.environ
)

# ── Paramètres Unity Catalog ───────────────────────────────────────────────
CATALOG = uc_cfg["catalog"]
SCHEMA = uc_cfg["schema"]

# ── Paramètres storage ─────────────────────────────────────────────────────
STORAGE_ACCOUNT = storage["account_name"]
SECRETS_SCOPE = secrets["scope"]
SECRET_KEY_NAME = secrets["storage_account_key"]

BRONZE_BASE = (
    storage["bronze"]["databricks"]
    if IS_DATABRICKS
    else storage["bronze"]["local"]
)

# ── Paramètres API ─────────────────────────────────────────────────────────
BASE = geo_cfg["base_url"]
COMMUNE_FIELDS = geo_cfg["commune_fields"]
LIMIT = geo_cfg["limits"]["communes_limit"]

log("CONFIG", "loaded", {
    "env": "databricks" if IS_DATABRICKS else "local",
    "catalog": f"{CATALOG}.{SCHEMA}",
    "base": BASE,
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
log("SPARK", "session ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## API

# COMMAND ----------


def fetch(url, params=None):
    """Appel API avec gestion d'erreur."""
    try:
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log("API", "request failed", {"url": url, "error": str(e)})
        return []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extraction

# COMMAND ----------


def fetch_regions():
    """Récupère toutes les régions françaises."""
    global _current_table
    _current_table = "regions"
    log("REGIONS", "start")

    data = fetch(f"{BASE}/regions", {"fields": "code,nom"})

    rows = [
        {
            "code_region": r["code"],
            "nom_region": r["nom"],
            "source": "geo.api.gouv.fr",
        }
        for r in data
    ]

    log("REGIONS", "done", {"rows": len(rows)})
    return rows


def fetch_departements():
    """Récupère tous les départements français."""
    global _current_table
    _current_table = "departements"
    log("DEPARTEMENTS", "start")

    data = fetch(f"{BASE}/departements", {"fields": "code,nom,codeRegion"})

    rows = [
        {
            "code_departement": d["code"],
            "nom_departement": d["nom"],
            "code_region": d.get("codeRegion"),
            "source": "geo.api.gouv.fr",
        }
        for d in data
    ]

    log("DEPARTEMENTS", "done", {"rows": len(rows)})
    return rows


def fetch_communes():
    """Récupère toutes les communes françaises avec coordonnées GPS."""
    global _current_table
    _current_table = "communes"
    log("COMMUNES", "start")

    data = fetch(
        f"{BASE}/communes",
        {"fields": COMMUNE_FIELDS, "limit": LIMIT}
    )

    rows = []
    for c in data:
        coords = (c.get("centre") or {}).get("coordinates", [None, None])
        rows.append({
            "code_commune": c["code"],
            "nom_commune": c["nom"],
            "code_departement": c.get("codeDepartement"),
            "code_region": c.get("codeRegion"),
            "longitude": coords[0],
            "latitude": coords[1],
            "population": c.get("population"),
            "source": "geo.api.gouv.fr",
        })

    log("COMMUNES", "done", {"rows": len(rows)})
    return rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Écriture Delta — Unity Catalog + ADLS Gen2

# COMMAND ----------


def write_table(rows, table_name):
    """
    Écrit une table geo en Delta Lake :
    1. Unity Catalog (table managée via saveAsTable)
    2. ADLS Gen2 container bronze
    """
    global _current_table
    _current_table = table_name

    if not rows:
        log("WRITE", "no rows to write")
        return

    table_full = f"{CATALOG}.{SCHEMA}.{table_name}"
    adls_path = f"{BRONZE_BASE}/{table_name}/"

    log("WRITE", "creating spark dataframe", {"rows": len(rows)})
    pdf = pd.DataFrame(rows)
    sdf = spark.createDataFrame(pdf)
    log("WRITE", "spark dataframe created",
        {"rows": sdf.count(), "columns": len(sdf.columns)})

    # ── 1. Unity Catalog — table managée ──────────────────────────────────
    (
        sdf.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(table_full)
    )
    log("WRITE", "unity catalog written", {"table": table_full})

    # ── 2. ADLS Gen2 container bronze ─────────────────────────────────────
    storage_key = dbutils.secrets.get(scope=SECRETS_SCOPE, key=SECRET_KEY_NAME)

    (
        sdf.write
        .format("delta")
        .mode("overwrite")
        .option(
            f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
            storage_key
        )
        .save(adls_path)
    )
    log("WRITE", "adls bronze written", {"path": adls_path})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------


def validate():
    """Affiche un résumé des 3 tables écrites dans Unity Catalog."""
    for table_name in ["regions", "departements", "communes"]:
        table_full = f"{CATALOG}.{SCHEMA}.{table_name}"
        try:
            df = spark.read.table(table_full)
            log("VALIDATION", "summary", {
                "table": table_full,
                "rows": df.count(),
                "cols": len(df.columns),
            })
        except Exception as e:
            log("VALIDATION", "error", {"table": table_full, "error": str(e)})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exécution

# COMMAND ----------


if __name__ == "__main__":
    # Régions
    regions_rows = fetch_regions()
    write_table(regions_rows, "regions")

    # Départements
    departements_rows = fetch_departements()
    write_table(departements_rows, "departements")

    # Communes
    communes_rows = fetch_communes()
    write_table(communes_rows, "communes")

    # Validation
    validate()

    log("PIPELINE", "geo ingestion complete", {
        "regions": len(regions_rows),
        "departements": len(departements_rows),
        "communes": len(communes_rows),
    })

