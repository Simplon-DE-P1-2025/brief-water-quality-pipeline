# Databricks notebook source
# %% [markdown]
# # Gold Layer — Water Quality Pipeline
#
# Script modulaire — compatible **local** (PySpark standalone) et **Databricks**.
# Configuration pilotée par `config/config.yaml` (section `gold`).
#
# Tables produites :
# 1. gold_conformite_dept      → taux conformité par département / année
# 2. gold_parametres_risks     → top paramètres non conformes par département
# 3. gold_commune_stats        → statistiques qualité par commune
# 4. gold_evolution_mensuelle  → évolution mensuelle conformité

# %% [markdown]
# ## 0 — Imports

# %%
# COMMAND ----------
import os
import yaml

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# %% [markdown]
# ## 1 — Configuration

# %%
# COMMAND ----------


def load_config(config_path: str = None) -> dict:
    if config_path is None:
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

# %% [markdown]
# ## 2 — Session Spark

# %%
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

# %% [markdown]
# ## 3 — Chargement Silver

# %%
# COMMAND ----------


def load_silver(spark: SparkSession, silver_path: str) -> DataFrame:
    path = f"{silver_path}/water_quality"
    df = spark.read.format("delta").load(path)
    print(
        f"Silver chargé : {df.count():>10,} lignes  | {len(df.columns)} colonnes")
    return df

# %% [markdown]
# ## 4 — Tables Gold

# %% [markdown]
# ### 4a — gold_conformite_dept

# %%
# COMMAND ----------


def build_conformite_dept(df: DataFrame) -> DataFrame:
    """
    Taux de conformité agrégé par département et année.

    Colonnes :
        annee, code_departement, nom_departement, nom_region,
        nb_analyses, nb_conformes, nb_non_conformes, nb_inconnus,
        taux_conformite_pct, taux_non_conformite_pct
    """
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

# %% [markdown]
# ### 4b — gold_parametres_risks

# %%
# COMMAND ----------


def build_parametres_risks(df: DataFrame, top_n: int = 10) -> DataFrame:
    """
    Top N paramètres non conformes par département et année.

    Colonnes :
        annee, code_departement, nom_departement,
        code_parametre, libelle_parametre, categorie_parametre,
        sous_categorie_parametre, nb_non_conformes, pct_non_conformes, rank
    """
    window = Window.partitionBy("annee", "code_departement") \
                   .orderBy(F.col("nb_non_conformes").desc())

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

# %% [markdown]
# ### 4c — gold_commune_stats

# %%
# COMMAND ----------


def build_commune_stats(df: DataFrame) -> DataFrame:
    """
    Statistiques qualité eau par commune et année.

    Colonnes :
        annee, code_commune, nom_commune, code_departement,
        nom_departement, nom_region, latitude, longitude, population,
        nb_analyses, nb_conformes, taux_conformite_pct,
        nb_parametres_distincts, nb_non_conformes
    """
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

# %% [markdown]
# ### 4d — gold_evolution_mensuelle

# %%
# COMMAND ----------


def build_evolution_mensuelle(df: DataFrame) -> DataFrame:
    """
    Évolution mensuelle du taux de conformité par département.

    Colonnes :
        annee, mois, code_departement, nom_departement,
        nb_analyses, nb_conformes, taux_conformite_pct,
        delta_taux_pct  (variation vs mois précédent)
    """
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

# %% [markdown]
# ## 5 — Écriture Gold

# %%
# COMMAND ----------


def write_gold(
    df: DataFrame,
    table_key: str,
    tables_cfg: dict,
    gold_path: str,
    is_databricks_env: bool = False,
    catalog: str = "main",
    database: str = "gold",
) -> str:
    """
    Écrit une table Gold en Delta Lake.
    - Local      : chemin fichier  → data/gold/<output_table>
    - Databricks : Unity Catalog   → catalog.database.output_table
    """
    table_cfg = tables_cfg[table_key]
    out_table = table_cfg["output_table"]
    partitions = table_cfg["partition_by"]
    out_path = f"{gold_path}/{out_table}"

    if not is_databricks_env:
        os.makedirs(gold_path, exist_ok=True)

    writer = (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .partitionBy(*partitions)
    )

    if is_databricks_env:
        full_table = f"{catalog}.{database}.{out_table}"
        writer.saveAsTable(full_table)
        print(f"Gold écrit (Databricks) : {full_table}")
    else:
        writer.save(out_path)
        print(f"Gold écrit (local)      : {out_path}")

    return out_path

# %% [markdown]
# ## 6 — Validation post-écriture

# %%
# COMMAND ----------


def validate_gold(spark: SparkSession, paths: dict) -> None:
    """Relit chaque table Gold et affiche les métriques clés."""
    print("\n" + "=" * 60)
    print("VALIDATION GOLD")
    print("=" * 60)

    for key, path in paths.items():
        df = spark.read.format("delta").load(path)
        print(f"\n── {key}")
        print(f"   Lignes   : {df.count():,}")
        print(f"   Colonnes : {len(df.columns)}")
        df.show(5, truncate=False)

# %% [markdown]
# ## 7 — Exécution notebook (Databricks uniquement)

# %%
# COMMAND ----------


_NOTEBOOK_RUN = (
    "ipykernel" in __import__("sys").modules
    or "DATABRICKS_RUNTIME_VERSION" in os.environ
)

if _NOTEBOOK_RUN:
    CFG = load_config()
    GOLD_CFG = get_gold_cfg(CFG)
    PATHS = get_paths(CFG)

    SILVER_PATH = PATHS["silver"]
    GOLD_PATH = PATHS["gold"]
    TABLES = GOLD_CFG["tables"]
    IS_DATABRICKS = is_databricks(CFG)
    DB_CATALOG = GOLD_CFG.get("databricks", {}).get("catalog", "main")
    DB_DATABASE = GOLD_CFG.get("databricks", {}).get("database", "gold")

    print(f"Environnement : {'Databricks' if IS_DATABRICKS else 'Local'}")
    print(f"Silver path   : {SILVER_PATH}")
    print(f"Gold path     : {GOLD_PATH}")

    spark = get_spark(CFG)
    spark.sparkContext.setLogLevel("ERROR")
    print(
        f"Spark {
            spark.version} OK  |  app={
            spark.conf.get('spark.app.name')}")

    df_silver = load_silver(spark, SILVER_PATH)

    df_conformite_dept = build_conformite_dept(df_silver)
    print("=== gold_conformite_dept ===")
    df_conformite_dept.show(truncate=False)

    df_parametres_risks = build_parametres_risks(df_silver, top_n=10)
    print("=== gold_parametres_risks ===")
    df_parametres_risks.show(20, truncate=False)

    df_commune_stats = build_commune_stats(df_silver)
    print("=== gold_commune_stats ===")
    df_commune_stats.show(10, truncate=False)

    df_evolution_mensuelle = build_evolution_mensuelle(df_silver)
    print("=== gold_evolution_mensuelle ===")
    df_evolution_mensuelle.show(20, truncate=False)

    paths_written = {}
    for key, df_gold in [
        ("conformite_dept", df_conformite_dept),
        ("parametres_risks", df_parametres_risks),
        ("commune_stats", df_commune_stats),
        ("evolution_mensuelle", df_evolution_mensuelle),
    ]:
        paths_written[key] = write_gold(
            df_gold,
            key,
            TABLES,
            GOLD_PATH,
            IS_DATABRICKS,
            DB_CATALOG,
            DB_DATABASE)

    validate_gold(spark, paths_written)

    if not IS_DATABRICKS:
        spark.stop()
        print("Spark arrêté")

# %% [markdown]
# ---
# ## `__main__` — Exécution en script standalone

# %%
# COMMAND ----------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Gold Transform — Water Quality Pipeline")
    parser.add_argument("--config", default=None)
    args, _ = parser.parse_known_args()

    # ── 1. Config
    cfg = load_config(args.config)
    gold_cfg = get_gold_cfg(cfg)
    paths = get_paths(cfg)
    is_db = is_databricks(cfg)

    s_path = paths["silver"]
    g_path = paths["gold"]
    tables = gold_cfg["tables"]
    catalog = gold_cfg.get("databricks", {}).get("catalog", "main")
    database = gold_cfg.get("databricks", {}).get("database", "gold")

    print(f"[main] Environnement : {'Databricks' if is_db else 'Local'}")
    print(f"[main] Silver -> {s_path}")
    print(f"[main] Gold   -> {g_path}")

    # ── 2. Spark
    session = get_spark(cfg)
    session.sparkContext.setLogLevel("ERROR")

    # ── 3. Chargement Silver
    silver = load_silver(session, s_path)

    # ── 4. Build tables Gold
    gold_tables = {
        "conformite_dept": build_conformite_dept(silver),
        "parametres_risks": build_parametres_risks(silver, top_n=10),
        "commune_stats": build_commune_stats(silver),
        "evolution_mensuelle": build_evolution_mensuelle(silver),
    }

    # ── 5. Écriture
    written = {}
    for key, df in gold_tables.items():
        written[key] = write_gold(
            df, key, tables, g_path, is_db, catalog, database)

    # ── 6. Validation
    validate_gold(session, written)

    # ── 7. Nettoyage
    if not is_db:
        session.stop()

    print("[main] Pipeline Gold terminé avec succès")
