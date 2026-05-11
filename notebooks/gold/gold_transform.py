# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# dependencies = [
#   "pyyaml",
# ]
# ///
# MAGIC %md
# MAGIC # Gold Layer — Water Quality Pipeline
# MAGIC
# MAGIC Script modulaire — compatible **local** (PySpark standalone) et **Databricks**.
# MAGIC Configuration pilotée par `config/config.yaml` (section `gold`).
# MAGIC
# MAGIC Tables produites :
# MAGIC 1. gold_conformite_dept      → taux conformité par département / année
# MAGIC 2. gold_parametres_risks     → top paramètres non conformes par département
# MAGIC 3. gold_commune_stats        → statistiques qualité par commune
# MAGIC 4. gold_evolution_mensuelle  → évolution mensuelle conformité

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 — Imports

# COMMAND ----------

import os
import yaml

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Configuration

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


def get_gold_cfg(cfg: dict) -> dict:
    return cfg["gold"]


def is_databricks(cfg: dict) -> bool:
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return True
    return cfg.get("environment", {}).get("is_databricks", False)


def get_paths(cfg: dict) -> dict:
    env_key = "databricks" if is_databricks(cfg) else "local"
    return get_gold_cfg(cfg)["paths"][env_key]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Session Spark

# COMMAND ----------


def get_spark(cfg: dict) -> SparkSession:
    spark_cfg = get_gold_cfg(cfg)["spark"]

    if is_databricks(cfg):
        return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    from delta import configure_spark_with_delta_pip
    return configure_spark_with_delta_pip(
        SparkSession.builder
        .appName(spark_cfg["app_name"])
        .master("local[*]")
        .config("spark.driver.memory", spark_cfg["driver_memory"])
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions",
                str(spark_cfg["shuffle_partitions"]))
    ).getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Chargement Silver

# COMMAND ----------


def load_silver(
        spark: SparkSession,
        silver_path: str,
        is_db: bool = False,
        silver_table_full: str = None) -> DataFrame:
    """
    Charge la table Silver.
    - Databricks : lecture depuis Unity Catalog
    - Local      : lecture depuis fichiers Delta
    """
    if is_db and silver_table_full:
        df = spark.read.table(silver_table_full)
        print(f"Silver chargé (UC) : {silver_table_full}")
    else:
        path = f"{silver_path}/water_quality"
        df = spark.read.format("delta").load(path)
        print(f"Silver chargé (local) : {path}")

    print(f"  {df.count():>10,} lignes  | {len(df.columns)} colonnes")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Tables Gold

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4a — gold_conformite_dept

# COMMAND ----------


def build_conformite_dept(df: DataFrame) -> DataFrame:
    """Taux de conformité agrégé par département et année."""
    return (
        df .groupBy(
            "annee",
            "code_departement",
            "nom_departement",
            "nom_region") .agg(
            F.count("*").alias("nb_analyses"),
            F.sum(
                F.when(
                    F.col("conformite_standard") == "conforme",
                    1).otherwise(0)) .alias("nb_conformes"),
            F.sum(
                        F.when(
                            F.col("conformite_standard") == "non_conforme",
                            1).otherwise(0)) .alias("nb_non_conformes"),
            F.sum(
                                F.when(
                                    F.col("conformite_standard") == "inconnu",
                                    1).otherwise(0)) .alias("nb_inconnus"),
        ) .withColumn(
            "taux_conformite_pct",
            F.round(
                F.col("nb_conformes") /
                F.col("nb_analyses") *
                100,
                2)) .withColumn(
            "taux_non_conformite_pct",
            F.round(
                F.col("nb_non_conformes") /
                F.col("nb_analyses") *
                100,
                2)) .orderBy(
            "annee",
            "code_departement"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b — gold_parametres_risks

# COMMAND ----------


def build_parametres_risks(df: DataFrame, top_n: int = 10) -> DataFrame:
    """Top N paramètres non conformes par département et année."""
    window = (
        Window.partitionBy("annee", "code_departement")
              .orderBy(F.col("nb_non_conformes").desc())
    )

    return (
        df .filter(
            F.col("conformite_standard") == "non_conforme") .groupBy(
            "annee",
            "code_departement",
            "nom_departement",
            "code_parametre",
            "libelle_parametre",
            "categorie_parametre",
            "sous_categorie_parametre",
        ) .agg(
            F.count("*").alias("nb_non_conformes")) .withColumn(
            "rank",
            F.rank().over(window)) .filter(
            F.col("rank") <= top_n) .join(
            df.groupBy(
                "annee",
                "code_departement",
                "code_parametre") .agg(
                F.count("*").alias("nb_total")),
            on=[
                "annee",
                "code_departement",
                "code_parametre"],
            how="left",
        ) .withColumn(
            "pct_non_conformes",
            F.round(
                F.col("nb_non_conformes") /
                F.col("nb_total") *
                100,
                2)) .drop("nb_total") .orderBy(
            "annee",
            "code_departement",
            "rank"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4c — gold_commune_stats

# COMMAND ----------


def build_commune_stats(df: DataFrame) -> DataFrame:
    """Statistiques qualité eau par commune et année."""
    return (
        df
        .groupBy(
            "annee", "code_commune", "nom_commune",
            "code_departement", "nom_departement", "nom_region",
            "latitude", "longitude", "population",
        )
        .agg(
            F.count("*").alias("nb_analyses"),
            F.sum(F.when(F.col("conformite_standard") == "conforme", 1).otherwise(0))
             .alias("nb_conformes"),
            F.sum(F.when(F.col("conformite_standard") == "non_conforme", 1).otherwise(0))
             .alias("nb_non_conformes"),
            F.countDistinct("code_parametre").alias("nb_parametres_distincts"),
        )
        .withColumn("taux_conformite_pct",
                    F.round(F.col("nb_conformes") / F.col("nb_analyses") * 100, 2))
        .orderBy("annee", "code_departement", "nom_commune")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4d — gold_evolution_mensuelle

# COMMAND ----------


def build_evolution_mensuelle(df: DataFrame) -> DataFrame:
    """Évolution mensuelle du taux de conformité par département."""
    window_lag = (
        Window.partitionBy("code_departement")
              .orderBy("annee", "mois")
    )

    return (
        df .groupBy(
            "annee",
            "mois",
            "code_departement",
            "nom_departement") .agg(
            F.count("*").alias("nb_analyses"),
            F.sum(
                F.when(
                    F.col("conformite_standard") == "conforme",
                    1).otherwise(0)) .alias("nb_conformes"),
        ) .withColumn(
            "taux_conformite_pct",
            F.round(
                F.col("nb_conformes") /
                F.col("nb_analyses") *
                100,
                2)) .withColumn(
            "taux_precedent",
            F.lag(
                "taux_conformite_pct",
                1).over(window_lag)) .withColumn(
            "delta_taux_pct",
            F.round(
                F.col("taux_conformite_pct") -
                F.col("taux_precedent"),
                2)) .drop("taux_precedent") .orderBy(
            "annee",
            "mois",
            "code_departement"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Écriture Gold (Unity Catalog + ADLS Gen2)

# COMMAND ----------


def write_gold(
    df: DataFrame,
    table_key: str,
    tables_cfg: dict,
    gold_path: str,
    is_databricks_env: bool = False,
    catalog: str = None,
    schema: str = "gold",
    storage_account: str = None,
    secrets_scope: str = None,
    secret_key_name: str = None,
) -> str:
    """
    Écrit une table Gold en Delta Lake.
    - Local      : chemin fichier Delta
    - Databricks : double écriture
        1. Unity Catalog (saveAsTable)
        2. ADLS Gen2 (abfss:// partitionné)
    """
    table_cfg = tables_cfg[table_key]
    out_table = table_cfg["output_table"]
    partitions = table_cfg["partition_by"]
    adls_path = f"{gold_path}/{out_table}"

    if not is_databricks_env:
        os.makedirs(gold_path, exist_ok=True)
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .partitionBy(*partitions)
              .save(adls_path)
        )
        print(f"Gold écrit (local) : {adls_path}")

    else:
        # ── 1. Unity Catalog ──────────────────────────────────────────────
        full_table = f"{catalog}.{schema}.{out_table}"
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .partitionBy(*partitions)
              .saveAsTable(full_table)
        )
        print(f"Gold UC écrit : {full_table}")

        # ── 2. ADLS Gen2 ──────────────────────────────────────────────────
        storage_key = dbutils.secrets.get(  # noqa: F821
            scope=secrets_scope, key=secret_key_name)
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .option(
                  f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
                  storage_key
              )
            .partitionBy(*partitions)
            .save(adls_path)
        )
        print(f"Gold ADLS écrit : {adls_path}")

    return adls_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 — Validation post-écriture

# COMMAND ----------


def validate_gold(spark: SparkSession, paths: dict,
                  is_db: bool = False, catalog: str = None,
                  schema: str = "gold") -> None:
    """Relit chaque table Gold et affiche les métriques clés."""
    print("\n" + "=" * 60)
    print("VALIDATION GOLD")
    print("=" * 60)

    for key, path in paths.items():
        try:
            if is_db and catalog:
                table_name = path.split("/")[-1]
                df = spark.read.table(f"{catalog}.{schema}.{table_name}")
                print(f"\n── {key} (UC)")
            else:
                df = spark.read.format("delta").load(path)
                print(f"\n── {key} (local)")
            print(f"   Lignes   : {df.count():,}")
            print(f"   Colonnes : {len(df.columns)}")
            df.show(5, truncate=False)
        except Exception as e:
            print(f"   Erreur validation {key} : {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 — Exécution

# COMMAND ----------


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Gold Transform — Water Quality Pipeline")
    parser.add_argument("--config", default=None)
    args, _ = parser.parse_known_args()

    cfg = load_config(args.config)
    gold_cfg = get_gold_cfg(cfg)
    paths = get_paths(cfg)
    is_db = is_databricks(cfg)
    uc_cfg = cfg["unity_catalog"]
    storage = cfg["storage"]
    secrets = storage["secrets"]

    s_path = paths["silver"]
    g_path = paths["gold"]
    tables = gold_cfg["tables"]
    catalog = uc_cfg["catalog"]
    gold_schema = gold_cfg.get("databricks", {}).get("schema", "gold")

    schema = uc_cfg["silver"]["schema"]
    table = uc_cfg["silver"]["table"]
    silver_full = f"{catalog}.{schema}.{table}"
    storage_account = storage["account_name"]
    secrets_scope = secrets["scope"]
    secret_key_name = secrets["storage_account_key"]

    print(f"[main] Environnement : {'Databricks' if is_db else 'Local'}")
    print(f"[main] Silver -> {s_path}")
    print(f"[main] Gold   -> {g_path}")

    session = get_spark(cfg)
    silver = load_silver(
        session,
        s_path,
        is_db,
        silver_full if is_db else None)

    gold_tables = {
        "conformite_dept": build_conformite_dept(silver),
        "parametres_risks": build_parametres_risks(silver, top_n=10),
        "commune_stats": build_commune_stats(silver),
        "evolution_mensuelle": build_evolution_mensuelle(silver),
    }

    written = {}
    for key, df in gold_tables.items():
        written[key] = write_gold(
            df, key, tables, g_path, is_db,
            catalog=catalog if is_db else None,
            schema=gold_schema if is_db else "gold",
            storage_account=storage_account if is_db else None,
            secrets_scope=secrets_scope if is_db else None,
            secret_key_name=secret_key_name if is_db else None,
        )

    validate_gold(session, written, is_db,
                  catalog if is_db else None, gold_schema)

    if not is_db:
        session.stop()

    print("[main] Pipeline Gold terminé avec succès")
