# Databricks notebook source
# %% [markdown]
# # API — Water Quality Pipeline
#
# FastAPI exposing Gold layer tables.
# Reads Delta Lake tables locally via PyArrow.
# On Databricks, replace with Databricks SQL Warehouse REST API.
#
# Usage :
#   uv run uvicorn notebooks.api.main:app --reload
#
# Endpoints :
#   GET /health
#   GET /conformite/departements
#   GET /conformite/communes
#   GET /parametres/risques
#   GET /evolution/mensuelle

# %%
# COMMAND ----------
import os
import yaml
import pyarrow.dataset as ds

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.responses import RedirectResponse

# =========================================================
# CONFIG
# =========================================================


def load_config(path: str = "config/config.yaml") -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


cfg = load_config()
GOLD_PATH = cfg["gold"]["paths"]["local"]["gold"]

# =========================================================
# HELPERS
# =========================================================


def read_gold(table_name: str) -> list[dict]:
    """Reads a Gold Delta table as a list of dicts (JSON-serializable)."""
    path = f"{GOLD_PATH}/{table_name}"
    if not os.path.exists(path):
        raise HTTPException(
            status_code=404,
            detail=f"Table '{table_name}' not found at {path}"
        )
    dataset = ds.dataset(path, format="parquet", partitioning="hive")
    table = dataset.to_table()
    df = table.to_pandas()
    df = df.where(df.notna(), other=None)
    return df.to_dict(orient="records")


# =========================================================
# APP
# =========================================================

app = FastAPI(
    title="Water Quality API",
    description="Exposes Gold layer analytical tables from the Water Quality Pipeline.",
    version="1.0.0",
)

# =========================================================
# ROUTES
# =========================================================


@app.get("/health", tags=["Health"])
def health():
    """Returns API health status and Gold path."""
    return {
        "status": "ok",
        "gold_path": GOLD_PATH,
    }


@app.get("/conformite/departements", tags=["Conformite"])
def conformite_departements(
    annee: int = Query(None, description="Filter by year (e.g. 2024)"),
    departement: str = Query(None, description="Filter by department code (e.g. 13)"),
):
    """
    Returns conformity rates by department and year.
    Source: gold_conformite_dept
    """
    data = read_gold("gold_conformite_dept")
    if annee:
        data = [r for r in data if r.get("annee") == annee]
    if departement:
        data = [r for r in data if r.get("code_departement") == departement]
    return JSONResponse(content={"count": len(data), "data": data})


@app.get("/conformite/communes", tags=["Conformite"])
def conformite_communes(
    annee: int = Query(None, description="Filter by year"),
    departement: str = Query(None, description="Filter by department code"),
    min_taux: float = Query(None, description="Minimum conformity rate (0-100)"),
    max_taux: float = Query(None, description="Maximum conformity rate (0-100)"),
    limit: int = Query(100, ge=1, le=1000, description="Max results (default 100)"),
):
    """
    Returns water quality stats per commune.
    Source: gold_commune_stats
    """
    data = read_gold("gold_commune_stats")
    if annee:
        data = [r for r in data if r.get("annee") == annee]
    if departement:
        data = [r for r in data if r.get("code_departement") == departement]
    if min_taux is not None:
        data = [r for r in data if r.get("taux_conformite_pct") is not None
                and r["taux_conformite_pct"] >= min_taux]
    if max_taux is not None:
        data = [r for r in data if r.get("taux_conformite_pct") is not None
                and r["taux_conformite_pct"] <= max_taux]
    return JSONResponse(content={"count": len(data), "data": data[:limit]})


@app.get("/parametres/risques", tags=["Parametres"])
def parametres_risques(
    annee: int = Query(None, description="Filter by year"),
    departement: str = Query(None, description="Filter by department code"),
    categorie: str = Query(None, description="Filter by parameter category"),
):
    """
    Returns top non-compliant parameters by department.
    Source: gold_parametres_risks
    """
    data = read_gold("gold_parametres_risks")
    if annee:
        data = [r for r in data if r.get("annee") == annee]
    if departement:
        data = [r for r in data if r.get("code_departement") == departement]
    if categorie:
        data = [
            r for r in data if r.get(
                "categorie_parametre",
                "").lower() == categorie.lower()]
    return JSONResponse(content={"count": len(data), "data": data})


@app.get("/evolution/mensuelle", tags=["Evolution"])
def evolution_mensuelle(
    annee: int = Query(None, description="Filter by year"),
    departement: str = Query(None, description="Filter by department code"),
):
    """
    Returns monthly conformity evolution by department.
    Source: gold_evolution_mensuelle
    """
    data = read_gold("gold_evolution_mensuelle")
    if annee:
        data = [r for r in data if r.get("annee") == annee]
    if departement:
        data = [r for r in data if r.get("code_departement") == departement]
    return JSONResponse(content={"count": len(data), "data": data})


@app.get("/", include_in_schema=False)
def root():
    """Redirects to Swagger UI."""
    return RedirectResponse(url="/docs")
