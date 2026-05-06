# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze - Geographical Reference Data Ingestion
# MAGIC
# MAGIC Uses dlt (https://dlthub.com) for Delta Lake writes.
# MAGIC All parameters are in config/config.yaml — do NOT edit this script.
# MAGIC
# MAGIC Sources:
# MAGIC   - geo.api.gouv.fr/regions      -> 18 regions
# MAGIC   - geo.api.gouv.fr/departements -> 101 departments
# MAGIC   - geo.api.gouv.fr/communes     -> ~35,000 communes with GPS + population

# COMMAND ----------

import os
import yaml
import requests
import dlt
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Context

# COMMAND ----------

class Context:
    def __init__(self):
        self.table = None

CTX = Context()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logger

# COMMAND ----------

def log(stage, msg, extra=None):
    ts     = datetime.now().strftime("%H:%M:%S")
    ctx    = f"table={CTX.table}".replace("None", "-")
    extras = (" | " + ", ".join(f"{k}={v}" for k, v in extra.items())) if extra else ""
    print(f"[{ts}] [{stage}] {ctx} | {msg}{extras}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

def load_config(path="config/config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


cfg     = load_config()
geo_cfg = cfg["pipelines"]["geo"]

IS_DATABRICKS = (
    cfg["environment"]["is_databricks"]
    or "DATABRICKS_RUNTIME_VERSION" in os.environ
)

BRONZE_PATH = (
    cfg["storage"]["bronze"]["databricks"]
    if IS_DATABRICKS
    else cfg["storage"]["bronze"]["local"]
)

BASE           = geo_cfg["base_url"]
COMMUNE_FIELDS = geo_cfg["commune_fields"]
LIMIT          = geo_cfg["limits"]["communes_limit"]

log("CONFIG", "loaded", {
    "env" : "databricks" if IS_DATABRICKS else "local",
    "base": BASE,
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## API

# COMMAND ----------

def fetch(url, params=None):
    """Simple GET request with error handling."""
    try:
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log("API", "request failed", {"url": url, "error": str(e)})
        return []

# COMMAND ----------

# MAGIC %md
# MAGIC ## dlt Resources

# COMMAND ----------

@dlt.resource(
    name              = "regions",
    write_disposition = "replace",
    primary_key       = geo_cfg["tables"]["regions"]["primary_key"],
)
def regions():
    """Fetches all 18 French regions."""
    CTX.table = "regions"
    log("REGIONS", "start")
    data = fetch(f"{BASE}/regions", {"fields": "code,nom"})
    for r in data:
        yield {
            "code_region": r["code"],
            "nom_region" : r["nom"],
            "source"     : "geo.api.gouv.fr",
        }
    log("REGIONS", "done", {"rows": len(data)})


@dlt.resource(
    name              = "departements",
    write_disposition = "replace",
    primary_key       = geo_cfg["tables"]["departements"]["primary_key"],
)
def departements():
    """Fetches all 101 French departments with their region code."""
    CTX.table = "departements"
    log("DEPARTEMENTS", "start")
    data = fetch(f"{BASE}/departements", {"fields": "code,nom,codeRegion"})
    for d in data:
        yield {
            "code_departement": d["code"],
            "nom_departement" : d["nom"],
            "code_region"     : d["codeRegion"],
            "source"          : "geo.api.gouv.fr",
        }
    log("DEPARTEMENTS", "done", {"rows": len(data)})


@dlt.resource(
    name              = "communes",
    write_disposition = "replace",
    primary_key       = geo_cfg["tables"]["communes"]["primary_key"],
)
def communes():
    """
    Fetches all French communes.
    Flattens the GeoJSON centre field to extract latitude and longitude.
    """
    CTX.table = "communes"
    log("COMMUNES", "start")
    data  = fetch(f"{BASE}/communes", {"fields": COMMUNE_FIELDS, "limit": LIMIT})
    count = 0
    for c in data:
        coords = (c.get("centre") or {}).get("coordinates", [None, None])
        yield {
            "code_commune"    : c["code"],
            "nom_commune"     : c["nom"],
            "code_departement": c.get("codeDepartement"),
            "code_region"     : c.get("codeRegion"),
            "longitude"       : coords[0] if coords else None,
            "latitude"        : coords[1] if coords else None,
            "population"      : c.get("population"),
            "source"          : "geo.api.gouv.fr",
        }
        count += 1
    log("COMMUNES", "done", {"rows": count})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline

# COMMAND ----------

os.makedirs(".dlt", exist_ok=True)
with open(".dlt/config.toml", "w") as f:
    f.write(f'[destination.filesystem]\nbucket_url = "{BRONZE_PATH}"\n')

pipeline = dlt.pipeline(
    pipeline_name = geo_cfg["pipeline_name"],
    destination   = "filesystem",
    dataset_name  = geo_cfg["dataset_name"],
    progress      = False,
)

log("PIPELINE", "start geo ingestion")

result = pipeline.run(
    [regions(), departements(), communes()],
    table_format = geo_cfg["file_format"],
)

log("PIPELINE", "end", {"result": str(result)})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

import pandas as pd
import glob


def count_rows(table_name):
    path  = f"{BRONZE_PATH}/{geo_cfg['dataset_name']}/{table_name}"
    files = glob.glob(f"{path}/**/*.parquet", recursive=True)
    if not files:
        return 0
    return sum(len(pd.read_parquet(f)) for f in files)


log("VALIDATION", "geo summary", {
    "regions"     : count_rows("regions"),
    "departements": count_rows("departements"),
    "communes"    : count_rows("communes"),
})

# COMMAND ----------