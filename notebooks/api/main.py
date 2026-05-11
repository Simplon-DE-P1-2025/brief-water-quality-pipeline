# Databricks notebook source
import io
import os
import csv
import json
import yaml

from datetime import datetime, timezone
from typing import Optional, Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse

# COMMAND ----------

# ── Config ─────────────────────────────────────────────────────────────────

def load_config(config_path: str = None) -> dict:
    if config_path is None:
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            base_dir = "/Workspace/Users/krhazlani.ext@simplonformations.co/brief-water-quality-pipeline"
            config_path = os.path.join(base_dir, "config/config.yaml")
        else:
            for c in ["config/config.yaml", "../../config/config.yaml"]:
                if os.path.exists(c):
                    config_path = c
                    break
            else:
                raise FileNotFoundError("config.yaml introuvable.")
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def is_databricks(cfg: dict) -> bool:
    return (
        "DATABRICKS_RUNTIME_VERSION" in os.environ
        or cfg.get("environment", {}).get("is_databricks", False)
    )


cfg           = load_config()
UC_CFG        = cfg["unity_catalog"]
IS_DATABRICKS = is_databricks(cfg)
CATALOG       = UC_CFG["catalog"]
GOLD_SCHEMA   = cfg["gold"].get("databricks", {}).get("schema", "gold")
GOLD_LOCAL    = cfg["gold"]["paths"]["local"]["gold"]

TABLES = {
    "conformite_dept":    "gold_conformite_dept",
    "parametres_risks":   "gold_parametres_risks",
    "commune_stats":      "gold_commune_stats",
    "evolution_mensuelle":"gold_evolution_mensuelle",
}

# COMMAND ----------

# ── Helpers lecture ────────────────────────────────────────────────────────

def read_gold(table_name: str) -> list[dict]:
    """Lit une table Gold — UC en Databricks, Delta local sinon."""
    if IS_DATABRICKS:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        full  = f"{CATALOG}.{GOLD_SCHEMA}.{table_name}"
        try:
            df = spark.read.table(full).toPandas()
        except Exception as e:
            raise HTTPException(500, detail=f"Erreur UC {full}: {e}")
    else:
        import pyarrow.dataset as ds
        path = f"{GOLD_LOCAL}/{table_name}"
        if not os.path.exists(path):
            raise HTTPException(404, detail=f"Table introuvable : {path}")
        df = ds.dataset(path, format="parquet", partitioning="hive").to_table().to_pandas()

    return df.where(df.notna(), other=None).to_dict(orient="records")


def export_json(data: list[dict], filename: str) -> StreamingResponse:
    """Retourne un fichier JSON en téléchargement."""
    content = json.dumps(
        {"exported_at": datetime.now(timezone.utc).isoformat(), "count": len(data), "data": data},
        ensure_ascii=False, indent=2, default=str
    )
    return StreamingResponse(
        io.BytesIO(content.encode("utf-8")),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}.json"'},
    )


def export_csv(data: list[dict], filename: str) -> StreamingResponse:
    """Retourne un fichier CSV en téléchargement."""
    if not data:
        raise HTTPException(404, detail="Aucune donnée à exporter.")
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
    return StreamingResponse(
        io.BytesIO(buf.getvalue().encode("utf-8")),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}.csv"'},
    )


def apply_filters(data, annee=None, departement=None) -> list[dict]:
    if annee:
        data = [r for r in data if r.get("annee") == annee]
    if departement:
        data = [r for r in data if r.get("code_departement") == departement]
    return data

# COMMAND ----------

# ── Application ────────────────────────────────────────────────────────────

app = FastAPI(
    title="💧 Water Quality API",
    description="""
## API Qualité de l'Eau Potable — France

Expose les tables analytiques Gold du pipeline de données.

### Sources
- **Ministère des Solidarités et de la Santé** — base SISE-Eaux
- **Hub'Eau** — API nationale qualité eau potable
- **geo.api.gouv.fr** — référentiels géographiques

### Export
Chaque endpoint supporte `?format=json` (défaut) et `?format=csv` pour télécharger les données.

### Couverture
- Données de **2016 à 2026**
- **757 000+** analyses
- **95 départements** métropolitains + DOM
""",
    version="1.0.0",
    contact={"name": "Kaouter Rhazlani", "email": "krhazlani.ext@simplonformations.co"},
    license_info={"name": "Licence Ouverte / Open Licence 2.0"},
)

# COMMAND ----------

# ── Routes ─────────────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")


@app.get(
    "/health",
    tags=["Système"],
    summary="Statut de l'API",
    response_description="Statut et configuration active",
)
def health():
    """Vérifie que l'API est opérationnelle et retourne la configuration active."""
    return {
        "status":       "ok",
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "environment":  "databricks" if IS_DATABRICKS else "local",
        "gold_source":  f"{CATALOG}.{GOLD_SCHEMA}" if IS_DATABRICKS else GOLD_LOCAL,
        "tables":       list(TABLES.values()),
    }


@app.get(
    "/tables",
    tags=["Système"],
    summary="Tables disponibles",
)
def list_tables():
    """Liste les tables Gold disponibles et leur description."""
    return {
        "tables": [
            {"key": k, "table": v, "endpoint": f"/{k.replace('_', '/')}"}
            for k, v in TABLES.items()
        ]
    }


@app.get(
    "/conformite/departements",
    tags=["Conformité"],
    summary="Taux de conformité par département",
    response_description="Agrégation annuelle par département avec taux conformité / non-conformité",
)
def conformite_departements(
    annee: Optional[int] = Query(None, description="Année (ex: 2024)", ge=2016, le=2026),
    departement: Optional[str] = Query(None, description="Code département (ex: 13, 75, 2A)"),
    format: Literal["json", "csv"] = Query("json", description="Format de sortie"),
):
    """
    Taux de conformité sanitaire agrégé par département et année.

    Retourne pour chaque département :
    - `nb_analyses` — nombre total d'analyses
    - `nb_conformes` / `nb_non_conformes` — décompte par statut
    - `taux_conformite_pct` — pourcentage de conformité (0-100)
    - `taux_non_conformite_pct` — pourcentage de non-conformité (0-100)
    """
    data = apply_filters(read_gold("gold_conformite_dept"), annee, departement)
    if format == "csv":
        return export_csv(data, f"conformite_dept_{annee or 'all'}")
    if format == "json":
        return export_json(data, f"conformite_dept_{annee or 'all'}")
    return JSONResponse({"count": len(data), "data": data})


@app.get(
    "/conformite/communes",
    tags=["Conformité"],
    summary="Statistiques qualité par commune",
)
def conformite_communes(
    annee: Optional[int] = Query(None, description="Année", ge=2016, le=2026),
    departement: Optional[str] = Query(None, description="Code département"),
    min_taux: Optional[float] = Query(None, description="Taux de conformité minimum (0-100)", ge=0, le=100),
    max_taux: Optional[float] = Query(None, description="Taux de conformité maximum (0-100)", ge=0, le=100),
    limit: int = Query(100, description="Nombre max de résultats", ge=1, le=5000),
    format: Literal["json", "csv"] = Query("json", description="Format de sortie"),
):
    """
    Statistiques qualité eau par commune avec coordonnées GPS.

    Utile pour cartographie — chaque commune retourne `latitude`, `longitude`,
    `population`, `taux_conformite_pct` et `nb_parametres_distincts`.
    """
    data = apply_filters(read_gold("gold_commune_stats"), annee, departement)
    if min_taux is not None:
        data = [r for r in data if r.get("taux_conformite_pct") is not None
                and r["taux_conformite_pct"] >= min_taux]
    if max_taux is not None:
        data = [r for r in data if r.get("taux_conformite_pct") is not None
                and r["taux_conformite_pct"] <= max_taux]
    data = data[:limit]
    if format == "csv":
        return export_csv(data, f"communes_{annee or 'all'}")
    if format == "json":
        return export_json(data, f"communes_{annee or 'all'}")
    return JSONResponse({"count": len(data), "data": data})


@app.get(
    "/parametres/risques",
    tags=["Paramètres"],
    summary="Top paramètres non conformes",
)
def parametres_risques(
    annee: Optional[int] = Query(None, description="Année", ge=2016, le=2026),
    departement: Optional[str] = Query(None, description="Code département"),
    categorie: Optional[str] = Query(None, description="Catégorie (ex: Microbiologique, Physicochimique)"),
    sous_categorie: Optional[str] = Query(None, description="Sous-catégorie (ex: nitrates, pesticides)"),
    format: Literal["json", "csv"] = Query("json", description="Format de sortie"),
):
    """
    Top 10 paramètres non conformes par département et année.

    Chaque entrée contient le `rank`, `nb_non_conformes`, `pct_non_conformes`,
    `categorie_parametre` et `sous_categorie_parametre`.
    """
    data = apply_filters(read_gold("gold_parametres_risks"), annee, departement)
    if categorie:
        data = [r for r in data
                if r.get("categorie_parametre", "").lower() == categorie.lower()]
    if sous_categorie:
        data = [r for r in data
                if r.get("sous_categorie_parametre", "").lower() == sous_categorie.lower()]
    if format == "csv":
        return export_csv(data, f"parametres_risques_{annee or 'all'}")
    if format == "json":
        return export_json(data, f"parametres_risques_{annee or 'all'}")
    return JSONResponse({"count": len(data), "data": data})


@app.get(
    "/evolution/mensuelle",
    tags=["Évolution"],
    summary="Évolution mensuelle de la conformité",
)
def evolution_mensuelle(
    annee: Optional[int] = Query(None, description="Année", ge=2016, le=2026),
    departement: Optional[str] = Query(None, description="Code département"),
    format: Literal["json", "csv"] = Query("json", description="Format de sortie"),
):
    """
    Évolution mensuelle du taux de conformité par département.

    Inclut `delta_taux_pct` — variation par rapport au mois précédent —
    pour détecter des dégradations ou améliorations soudaines.
    """
    data = apply_filters(read_gold("gold_evolution_mensuelle"), annee, departement)
    if format == "csv":
        return export_csv(data, f"evolution_{annee or 'all'}")
    if format == "json":
        return export_json(data, f"evolution_{annee or 'all'}")
    return JSONResponse({"count": len(data), "data": data})

# COMMAND ----------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
