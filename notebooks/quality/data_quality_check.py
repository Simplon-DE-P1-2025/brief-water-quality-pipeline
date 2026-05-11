# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# dependencies = [
#   "great-expectations",
#   "pyyaml",
#   "pyarrow",
#   "azure-storage-blob",
# ]
# ///
# MAGIC %md
# MAGIC # Data Quality — Water Quality Pipeline
# MAGIC
# MAGIC Validates Silver and Gold layers using Great Expectations 1.x.
# MAGIC
# MAGIC - **Databricks** : lecture UC → rapport persisté dans `quality` schema + container ADLS `quality`
# MAGIC - **Local**      : lecture fichiers Delta → rapport affiché console uniquement
# MAGIC
# MAGIC Layers validated :
# MAGIC - Silver  : water_quality
# MAGIC - Gold    : conformite_dept, parametres_risks, commune_stats,
# evolution_mensuelle

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 — Imports

# COMMAND ----------

import os
import json
import yaml

from datetime import datetime, timezone

import pandas as pd
import great_expectations as gx

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Config

# COMMAND ----------


def load_config(config_path: str = None) -> dict:
    if config_path is None:
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            base_dir = "/Workspace/Users/krhazlani.ext@simplonformations.co/brief-water-quality-pipeline"
            config_path = os.path.join(base_dir, "config/config.yaml")
        else:
            candidates = ["config/config.yaml", "../../config/config.yaml"]
            for c in candidates:
                if os.path.exists(c):
                    config_path = c
                    break
            else:
                raise FileNotFoundError("config.yaml introuvable.")
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def is_databricks(cfg: dict) -> bool:
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return True
    return cfg.get("environment", {}).get("is_databricks", False)


cfg = load_config()
UC_CFG = cfg["unity_catalog"]
STORAGE = cfg["storage"]
SECRETS = STORAGE["secrets"]
IS_DATABRICKS = is_databricks(cfg)

# ── Chemins locaux ─────────────────────────────────────────────────────────
SILVER_PATH_LOCAL = cfg["silver"]["paths"]["local"]["silver"]
GOLD_PATH_LOCAL = cfg["gold"]["paths"]["local"]["gold"]

# ── Unity Catalog ──────────────────────────────────────────────────────────
CATALOG = UC_CFG["catalog"]
SILVER_FULL = f"{CATALOG}.{
    UC_CFG['silver']['schema']}.{
        UC_CFG['silver']['table']}"
GOLD_SCHEMA = cfg["gold"].get("databricks", {}).get("schema", "gold")
QUALITY_SCHEMA = UC_CFG["quality"]["schema"]
QUALITY_TABLE = UC_CFG["quality"]["table"]
QUALITY_FULL = f"{CATALOG}.{QUALITY_SCHEMA}.{QUALITY_TABLE}"

# ── ADLS container quality ─────────────────────────────────────────────────
QUALITY_ADLS = STORAGE["quality"]["databricks"]
STORAGE_ACCOUNT = STORAGE["account_name"]
SECRETS_SCOPE = SECRETS["scope"]
SECRET_KEY_NAME = SECRETS["storage_account_key"]

GOLD_TABLES_UC = {
    "gold_conformite_dept": f"{CATALOG}.{GOLD_SCHEMA}.gold_conformite_dept",
    "gold_parametres_risks": f"{CATALOG}.{GOLD_SCHEMA}.gold_parametres_risks",
    "gold_commune_stats": f"{CATALOG}.{GOLD_SCHEMA}.gold_commune_stats",
    "gold_evolution_mensuelle": f"{CATALOG}.{GOLD_SCHEMA}.gold_evolution_mensuelle",
}

GOLD_TABLES_LOCAL = {
    "gold_conformite_dept": f"{GOLD_PATH_LOCAL}/gold_conformite_dept",
    "gold_parametres_risks": f"{GOLD_PATH_LOCAL}/gold_parametres_risks",
    "gold_commune_stats": f"{GOLD_PATH_LOCAL}/gold_commune_stats",
    "gold_evolution_mensuelle": f"{GOLD_PATH_LOCAL}/gold_evolution_mensuelle",
}

print(f"Environnement  : {'Databricks' if IS_DATABRICKS else 'Local'}")
print(
    f"Silver         : {
        SILVER_FULL if IS_DATABRICKS else SILVER_PATH_LOCAL}")
print(f"Quality UC     : {QUALITY_FULL}")
print(f"Quality ADLS   : {QUALITY_ADLS}/reports/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Helpers lecture

# COMMAND ----------


def read_from_uc(table_full: str) -> pd.DataFrame:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.table(table_full).toPandas()
    print(f"  UC    : {table_full} — {len(df):,} lignes")
    return df


def read_from_delta(path: str) -> pd.DataFrame:
    import pyarrow.dataset as ds
    dataset = ds.dataset(path, format="parquet", partitioning="hive")
    df = dataset.to_table().to_pandas()
    print(f"  Local : {path} — {len(df):,} lignes")
    return df


def read_table(table_name: str) -> pd.DataFrame:
    if IS_DATABRICKS:
        if table_name == "silver":
            return read_from_uc(SILVER_FULL)
        return read_from_uc(GOLD_TABLES_UC[table_name])
    else:
        if table_name == "silver":
            return read_from_delta(f"{SILVER_PATH_LOCAL}/water_quality")
        return read_from_delta(GOLD_TABLES_LOCAL[table_name])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Helper validation GE

# COMMAND ----------


def run_validation(context, df: pd.DataFrame, suite_name: str) -> dict:
    data_source = context.data_sources.add_pandas(name=suite_name)
    asset = data_source.add_dataframe_asset(name=suite_name)
    batch_def = asset.add_batch_definition_whole_dataframe(suite_name)
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    suite = context.suites.get(name=suite_name)
    result = batch.validate(suite)

    total = result["statistics"]["evaluated_expectations"]
    passed = result["statistics"]["successful_expectations"]
    status = "PASSED" if result["success"] else "FAILED"

    print(f"\n[{status}] {suite_name}")
    print(
        f"  Expectations : {total} total | {passed} passed | {
            total -
            passed} failed")

    if not result["success"]:
        for r in result["results"]:
            if not r["success"]:
                exp = r["expectation_config"]["type"]
                col = r["expectation_config"]["kwargs"].get("column", "")
                obs = r["result"].get("observed_value", "")
                print(f"  FAIL  {exp}  col={col}  observed={obs}")

    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Suites Great Expectations

# COMMAND ----------


def add_suite_silver(ctx) -> None:
    suite = ctx.suites.add(gx.ExpectationSuite(name="silver_water_quality"))
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=100000))
    for col in ["code_prelevement", "date_prelevement", "annee", "mois",
                "code_commune", "code_departement", "libelle_parametre",
                "conformite_standard"]:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=col))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="annee", min_value=2016, max_value=2026))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="mois", min_value=1, max_value=12))
    suite.add_expectation(gx.expectations.ExpectColumnValueLengthsToEqual(
        column="code_commune", value=5))
    suite.add_expectation(gx.expectations.ExpectColumnValueLengthsToEqual(
        column="code_departement", value=2))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="conformite_standard",
            value_set=[
                "conforme",
                "non_conforme",
                "conforme_avec_remarque",
                "inconnu"]))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="categorie_parametre",
        value_set=["Microbiologique", "Physico-chimique", "Radiologique",
                   "Physicochimique", "Organoleptique", "Autre"]))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="resultat_numerique", min_value=0, mostly=0.95))


def add_suite_conformite_dept(ctx) -> None:
    suite = ctx.suites.add(gx.ExpectationSuite(name="gold_conformite_dept"))
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=0))
    for col in ["annee", "code_departement", "nom_departement",
                "nb_analyses", "nb_conformes", "taux_conformite_pct"]:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=col))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="annee", min_value=2016, max_value=2026))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="nb_analyses", min_value=1))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="taux_conformite_pct", min_value=0, max_value=100))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="taux_non_conformite_pct", min_value=0, max_value=100))
    suite.add_expectation(
        gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="nb_analyses",
            column_B="nb_conformes",
            or_equal=True))


def add_suite_parametres_risks(ctx) -> None:
    suite = ctx.suites.add(gx.ExpectationSuite(name="gold_parametres_risks"))
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=0))
    for col in ["annee", "code_departement", "code_parametre",
                "libelle_parametre", "nb_non_conformes", "rank"]:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=col))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="rank", min_value=1, max_value=10))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="nb_non_conformes", min_value=1))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="pct_non_conformes", min_value=0, max_value=100))


def add_suite_commune_stats(ctx) -> None:
    suite = ctx.suites.add(gx.ExpectationSuite(name="gold_commune_stats"))
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=0))
    for col in ["annee", "code_commune", "nom_commune",
                "code_departement", "nb_analyses", "taux_conformite_pct"]:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=col))
    suite.add_expectation(gx.expectations.ExpectColumnValueLengthsToEqual(
        column="code_commune", value=5))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="taux_conformite_pct", min_value=0, max_value=100))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="nb_analyses", min_value=1))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="latitude", min_value=-90, max_value=90, mostly=0.95))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="longitude", min_value=-180, max_value=180, mostly=0.95))


def add_suite_evolution_mensuelle(ctx) -> None:
    suite = ctx.suites.add(
        gx.ExpectationSuite(
            name="gold_evolution_mensuelle"))
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=0))
    for col in ["annee", "mois", "code_departement",
                "nb_analyses", "taux_conformite_pct"]:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=col))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="mois", min_value=1, max_value=12))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="annee", min_value=2016, max_value=2026))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="taux_conformite_pct", min_value=0, max_value=100))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="nb_analyses", min_value=1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Persistance du rapport

# COMMAND ----------


def build_report_rows(results: dict, ts: str, run_date: str) -> list:
    """Construit les lignes du rapport depuis les résultats GE."""
    rows = []
    all_passed = all(r["success"] for r in results.values())

    for name, result in results.items():
        total = result["statistics"]["evaluated_expectations"]
        passed = result["statistics"]["successful_expectations"]
        failed = total - passed

        failed_details = []
        if not result["success"]:
            for r in result["results"]:
                if not r["success"]:
                    failed_details.append({
                        "expectation": r["expectation_config"]["type"],
                        "column": r["expectation_config"]["kwargs"].get("column", ""),
                        "observed": str(r["result"].get("observed_value", "")),
                    })

        rows.append({
            "run_timestamp": ts,
            "run_date": run_date,
            "suite_name": name,
            "status": "PASSED" if result["success"] else "FAILED",
            "total_expectations": total,
            "passed_expectations": passed,
            "failed_expectations": failed,
            "failed_details": json.dumps(failed_details, ensure_ascii=False),
            "overall_success": all_passed,
        })

    return rows


def save_report_uc(rows: list) -> None:
    """Écrit le rapport dans databricks_waterquality.quality.quality_reports (append)."""
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    sdf = spark.createDataFrame(pd.DataFrame(rows))
    (
        sdf.write
           .format("delta")
           .mode("append")
           .saveAsTable(QUALITY_FULL)
    )
    print(f"Rapport UC     : {QUALITY_FULL}")


def save_report_adls(rows: list, ts: str) -> None:
    """Écrit le rapport JSON dans le container ADLS quality/reports/."""
    from azure.storage.blob import BlobServiceClient
    import io

    storage_key = dbutils.secrets.get(scope=SECRETS_SCOPE, key=SECRET_KEY_NAME)  # noqa: F821
    report = {"run_timestamp": ts, "results": rows}
    blob_name = f"reports/report_{ts.replace(':', '-')}.json"
    content = json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8")

    client = BlobServiceClient(
        account_url=f"https://{STORAGE_ACCOUNT}.blob.core.windows.net",
        credential=storage_key
    )
    client.get_blob_client(container="quality", blob=blob_name) \
          .upload_blob(io.BytesIO(content), overwrite=True)
    print(f"Rapport ADLS   : quality/{blob_name}")


def save_report_local(rows: list, ts: str) -> None:
    """Écrit le rapport JSON en local — chemin piloté par config.yaml."""
    quality_cfg = cfg["storage"]["quality"]
    reports_dir = os.path.join(
        quality_cfg["local"],
        quality_cfg["reports_dir"])
    os.makedirs(reports_dir, exist_ok=True)

    filename = quality_cfg["report_filename"].format(ts=ts.replace(":", "-"))
    filepath = os.path.join(reports_dir, filename)

    report = {"run_timestamp": ts, "results": rows}
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"Rapport local  : {filepath}")


def save_report(results: dict) -> None:
    """
    Persiste le rapport selon l'environnement :
    - Databricks : Unity Catalog (append) + ADLS container quality (JSON)
    - Local      : fichier JSON dans data/quality/reports/
    """
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = build_report_rows(results, ts, run_date)

    if not IS_DATABRICKS:
        save_report_local(rows, ts)
        return

    save_report_uc(rows)
    save_report_adls(rows, ts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 — Rapport console

# COMMAND ----------


def print_report(results: dict) -> None:
    print("\n" + "=" * 60)
    print("DATA QUALITY REPORT")
    print("=" * 60)
    all_passed = True
    for name, result in results.items():
        total = result["statistics"]["evaluated_expectations"]
        passed = result["statistics"]["successful_expectations"]
        status = "PASSED" if result["success"] else "FAILED"
        if not result["success"]:
            all_passed = False
        print(f"  {status:6}  {name:<35}  {passed}/{total} expectations")
    print("=" * 60)
    overall = "ALL PASSED" if all_passed else "SOME FAILURES — check details above"
    print(f"  Overall : {overall}")
    print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 — Exécution

# COMMAND ----------


def run_quality_checks():
    context = gx.get_context(mode="ephemeral")

    add_suite_silver(context)
    add_suite_conformite_dept(context)
    add_suite_parametres_risks(context)
    add_suite_commune_stats(context)
    add_suite_evolution_mensuelle(context)

    print("=" * 60)
    print("SILVER VALIDATION")
    print("=" * 60)
    result_silver = run_validation(
        context,
        read_table("silver"),
        "silver_water_quality")

    print("\n" + "=" * 60)
    print("GOLD VALIDATION")
    print("=" * 60)
    result_conformite = run_validation(context, read_table(
        "gold_conformite_dept"), "gold_conformite_dept")
    result_risks = run_validation(
        context,
        read_table("gold_parametres_risks"),
        "gold_parametres_risks")
    result_communes = run_validation(
        context,
        read_table("gold_commune_stats"),
        "gold_commune_stats")
    result_evolution = run_validation(
        context,
        read_table("gold_evolution_mensuelle"),
        "gold_evolution_mensuelle")

    results = {
        "silver_water_quality": result_silver,
        "gold_conformite_dept": result_conformite,
        "gold_parametres_risks": result_risks,
        "gold_commune_stats": result_communes,
        "gold_evolution_mensuelle": result_evolution,
    }

    print_report(results)
    save_report(results)

    return results

# COMMAND ----------


if __name__ == "__main__":
    run_quality_checks()
