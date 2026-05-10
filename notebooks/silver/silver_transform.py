# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# dependencies = [
#   "requests",
#   "pyyaml",
# ]
# ///
# MAGIC %md
# MAGIC # Silver Layer — Water Quality Pipeline
# MAGIC
# MAGIC Script modulaire — compatible **local** (PySpark standalone) et **Databricks**.
# MAGIC Configuration pilotée par `config/config.yaml` (section `silver`).
# MAGIC
# MAGIC Étapes :
# MAGIC 1. Config + détection environnement
# MAGIC 2. Session Spark
# MAGIC 3. Chargement Bronze (Unity Catalog ou fichiers locaux)
# MAGIC 4. Analyse exploratoire
# MAGIC 5. Nettoyage (dédup, nulls, types)
# MAGIC 6. Standardisation colonnes
# MAGIC 7. Enrichissement (géo, catégories, conformité)
# MAGIC 8. Sélection finale
# MAGIC 9. Écriture Silver (Unity Catalog + ADLS Gen2 partitionné)
# MAGIC 10. Validation post-écriture
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 — Imports
# MAGIC

# COMMAND ----------

import os
import sys
import yaml

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Configuration
# MAGIC

# COMMAND ----------

def load_config(config_path: str = None) -> dict:
    """
    Charge config/config.yaml.
    Remonte automatiquement si le chemin n'est pas fourni.
    Compatible execution depuis notebooks/silver/ ou depuis la racine.
    """
    if config_path is None:
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            base_dir = "/Workspace/Users/krhazlani.ext@simplonformations.co/brief-water-quality-pipeline"
            config_path = os.path.join(base_dir, "config/config.yaml")
        else:
            candidates = [
                "config/config.yaml",
                "../../config/config.yaml",
            ]
            for c in candidates:
                if os.path.exists(c):
                    config_path = c
                    break
            else:
                raise FileNotFoundError(
                    "config.yaml introuvable. Fournissez --config explicitement."
                )
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_silver_cfg(cfg: dict) -> dict:
    """Retourne la section silver du fichier de config."""
    return cfg["silver"]


def is_databricks(cfg: dict) -> bool:
    """
    Détecte si l'environnement est Databricks.
    Priorité : variable d'environnement Databricks Runtime > config.yaml.
    """
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return True
    return cfg.get("environment", {}).get("is_databricks", False)


def get_paths(cfg: dict) -> dict:
    """Retourne les chemins bronze/silver selon l'environnement."""
    env_key = "databricks" if is_databricks(cfg) else "local"
    return get_silver_cfg(cfg)["paths"][env_key]


# COMMAND ----------

# Chargement global (visible par toutes les cellules du notebook)
# Guard : ne s'execute pas lors d'un import (tests unitaires, etc.)
_NOTEBOOK_RUN = __name__ == "__main__" or (
    "ipykernel" in sys.modules
    or os.environ.get("DATABRICKS_RUNTIME_VERSION")
    or os.environ.get("SILVER_NOTEBOOK_RUN")
)

if _NOTEBOOK_RUN:
    CFG        = load_config()
    SILVER_CFG = get_silver_cfg(CFG)
    PATHS      = get_paths(CFG)
    UC_CFG     = CFG["unity_catalog"]
    STORAGE    = CFG["storage"]
    SECRETS    = STORAGE["secrets"]

    IS_DATABRICKS = is_databricks(CFG)

    BRONZE_PATH  = PATHS["bronze"]
    SILVER_PATH  = PATHS["silver"]
    OUTPUT_TABLE = SILVER_CFG["output_table"]
    DEDUP_KEYS   = SILVER_CFG["dedup_keys"]
    PARTITION_BY = SILVER_CFG["partition_by"]
    CATEGORIES   = SILVER_CFG["categories"]
    SOUS_CATS    = SILVER_CFG["sous_categories"]
    OUTPUT_COLS  = SILVER_CFG["output_columns"]

    # ── Unity Catalog ──────────────────────────────────────────────────────
    CATALOG           = UC_CFG["catalog"]
    BRONZE_UC_SCHEMA  = UC_CFG["bronze"]["schema"]
    SILVER_UC_SCHEMA  = UC_CFG["silver"]["schema"]
    SILVER_TABLE_FULL = f"{CATALOG}.{SILVER_UC_SCHEMA}.{OUTPUT_TABLE}"

    # Tables geo Bronze UC
    GEO_SCHEMA       = UC_CFG["geo"]["schema"]
    UC_REGIONS       = f"{CATALOG}.{GEO_SCHEMA}.{UC_CFG['geo']['regions']}"
    UC_DEPARTEMENTS  = f"{CATALOG}.{GEO_SCHEMA}.{UC_CFG['geo']['departements']}"
    UC_COMMUNES      = f"{CATALOG}.{GEO_SCHEMA}.{UC_CFG['geo']['communes']}"
    UC_WATER_QUALITY = f"{CATALOG}.{BRONZE_UC_SCHEMA}.{UC_CFG['bronze']['table']}"

    # ── Storage ────────────────────────────────────────────────────────────
    STORAGE_ACCOUNT = STORAGE["account_name"]
    SECRETS_SCOPE   = SECRETS["scope"]
    SECRET_KEY_NAME = SECRETS["storage_account_key"]

    print(f"Environnement : {'Databricks' if IS_DATABRICKS else 'Local'}")
    print(f"Bronze UC     : {UC_WATER_QUALITY}")
    print(f"Silver UC     : {SILVER_TABLE_FULL}")
    print(f"Silver ADLS   : {SILVER_PATH}/{OUTPUT_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Session Spark
# MAGIC

# COMMAND ----------

def get_spark(cfg: dict) -> SparkSession:
    """
    Retourne la SparkSession adaptée a l'environnement.
    - Databricks : recupere la session existante.
    - Local      : cree une session avec Delta Lake via delta-spark.
    """
    spark_cfg = get_silver_cfg(cfg)["spark"]

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
# MAGIC ## 3 — Chargement Bronze
# MAGIC

# COMMAND ----------

def load_bronze(spark: SparkSession, bronze_path: str, is_db: bool = False,
                uc_tables: dict = None) -> dict:
    """
    Charge les 4 tables Delta Bronze.
    - Databricks : lecture depuis Unity Catalog (spark.read.table)
    - Local      : lecture depuis fichiers Delta (spark.read.format("delta").load)
    Retourne un dict {table_name: DataFrame}.
    """
    if is_db and uc_tables:
        loaded = {
            "water_quality": spark.read.table(uc_tables["water_quality"]),
            "communes":      spark.read.table(uc_tables["communes"]),
            "departements":  spark.read.table(uc_tables["departements"]),
            "regions":       spark.read.table(uc_tables["regions"]),
        }
    else:
        tables = ["water_quality", "communes", "departements", "regions"]
        loaded = {
            t: spark.read.format("delta").load(f"{bronze_path}/{t}")
            for t in tables
        }

    for name, df in loaded.items():
        print(f"  {name:<15} : {df.count():>10,} lignes  | {len(df.columns)} colonnes")

    return loaded


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Analyse exploratoire Bronze
# MAGIC

# COMMAND ----------

def explore_bronze(df_water: DataFrame) -> None:
    """Affiche les statistiques cles du DataFrame Bronze."""
    total = df_water.count()

    print("=== Schema water_quality ===")
    df_water.printSchema()

    print("\n=== Distribution annee_partition ===")
    df_water.groupBy("annee_partition").count().orderBy("annee_partition").show()

    null_exprs = [
        (F.count(F.when(F.col(c).isNull(), c)) / total * 100).alias(c)
        for c in df_water.columns
    ]
    null_df = (
        df_water.select(null_exprs).toPandas().T
        .rename(columns={0: "null_%"})
        .sort_values("null_%", ascending=False)
    )
    non_zero = null_df[null_df["null_%"] > 0]
    print(f"\n=== Taux de nullite > 0 %  (total={total:,}) ===")
    print(non_zero.to_string() if not non_zero.empty else "  Aucun null detecte")

    print("\n=== Valeurs conformite ===")
    df_water.groupBy("conclusion_conformite_prelevement") \
            .count().orderBy("count", ascending=False).show()

    print("=== Echantillon colonne reseaux ===")
    df_water.select("reseaux").limit(2).show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Nettoyage
# MAGIC

# COMMAND ----------

def clean(df: DataFrame, dedup_keys: list) -> DataFrame:
    """
    - Deduplication sur cle metier (configurable via config.yaml > silver.dedup_keys)
    - Filtres sur champs obligatoires
    - Correction des types
    - Derivation annee / mois
    - Suppression colonnes techniques dlt
    """
    total_before = df.count()

    df_out = (
        df
        .dropDuplicates(dedup_keys)
        .filter(F.col("date_prelevement").isNotNull())
        .filter(F.col("code_commune").isNotNull())
        .filter(F.col("libelle_parametre").isNotNull())
        .withColumn("annee_partition", F.col("annee_partition").cast(IntegerType()))
        .withColumn("resultat_numerique", F.col("resultat_numerique").cast(DoubleType()))
        .withColumn("date_prelevement", F.to_date(F.col("date_prelevement")))
        .withColumn("annee", F.year(F.col("date_prelevement")).cast(IntegerType()))
        .withColumn("mois", F.month(F.col("date_prelevement")).cast(IntegerType()))
        .drop("_dlt_load_id", "_dlt_id")
    )

    removed = total_before - df_out.count()
    print(f"Avant      : {total_before:,}")
    print(f"Apres      : {df_out.count():,}")
    print(f"Supprimees : {removed:,}  ({removed / total_before * 100:.2f}%)")
    return df_out


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 — Standardisation des colonnes
# MAGIC

# COMMAND ----------

def standardize(df: DataFrame) -> DataFrame:
    """
    - Extraction code_reseau / nom_reseau depuis le champ JSON `reseaux`
    - Trim + upper sur les colonnes texte cles
    - lpad sur codes INSEE (commune=5, departement=2)
    - Renommage semantique des colonnes conformite
    """
    return (
        df
        .withColumn("_json", F.regexp_extract(F.col("reseaux"), r"\{.*?\}", 0))
        .withColumn("code_reseau", F.get_json_object(F.col("_json"), "$.code"))
        .withColumn("nom_reseau", F.get_json_object(F.col("_json"), "$.nom"))
        .drop("_json", "reseaux")
        .withColumn("libelle_parametre", F.trim(F.col("libelle_parametre")))
        .withColumn("libelle_parametre_maj", F.upper(F.trim(F.col("libelle_parametre_maj"))))
        .withColumn("nom_commune", F.trim(F.col("nom_commune")))
        .withColumn("code_commune", F.lpad(F.trim(F.col("code_commune")), 5, "0"))
        .withColumn("code_departement", F.lpad(F.trim(F.col("code_departement")), 2, "0"))
        .withColumnRenamed("conclusion_conformite_prelevement", "conformite_globale")
        .withColumnRenamed("conformite_limites_bact_prelevement", "conformite_bact")
        .withColumnRenamed("conformite_limites_pc_prelevement", "conformite_pc")
        .withColumnRenamed("conformite_references_bact_prelevement", "conformite_ref_bact")
        .withColumnRenamed("conformite_references_pc_prelevement", "conformite_ref_pc")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 — Enrichissement
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7a — Jointure geographique (communes -> regions)
# MAGIC

# COMMAND ----------

def enrich_geo(df: DataFrame, df_communes: DataFrame,
               df_regions: DataFrame) -> DataFrame:
    ref_communes = (
        df_communes
        .select(
            F.lpad(F.col("code_commune"), 5, "0").alias("_code_commune"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("population"),
            F.col("code_region"),
        )
        .dropDuplicates(["_code_commune"])
    )

    ref_regions = (
        df_regions
        .select(
            F.col("code_region").alias("_code_region"),
            F.col("nom_region"),
        )
        .dropDuplicates(["_code_region"])
    )

    df_out = (
        df
        .join(ref_communes, df["code_commune"] == ref_communes["_code_commune"], how="left")
        .drop("_code_commune")
        .join(ref_regions, F.col("code_region") == ref_regions["_code_region"], how="left")
        .drop("_code_region")
    )

    n       = df_out.count()
    matched = df_out.filter(F.col("nom_region").isNotNull()).count()
    no_geo  = df_out.filter(F.col("latitude").isNull()).count()
    print(f"Taux jointure region  : {matched / n * 100:.1f}%  ({matched:,}/{n:,})")
    print(f"Communes sans geodata : {no_geo:,} / {n:,}")
    return df_out


# COMMAND ----------

# MAGIC %md
# MAGIC ### 7b — Categories parametres (depuis config.yaml)
# MAGIC

# COMMAND ----------

def enrich_categories(df: DataFrame, categories: dict,
                      sous_categories: dict) -> DataFrame:
    """
    Ajoute deux colonnes derivees, entierement pilotees par config.yaml :
    - categorie_parametre      : depuis code_type_parametre
    - sous_categorie_parametre : depuis regex sur libelle_parametre
    """
    cat_expr = F.lit("Autre")
    for code, label in categories.items():
        cat_expr = F.when(F.col("code_type_parametre") == code, label).otherwise(cat_expr)

    sub_expr = F.lit("Autre")
    for label, pattern in reversed(list(sous_categories.items())):
        sub_expr = (
            F.when(F.lower(F.col("libelle_parametre")).rlike(pattern), label)
             .otherwise(sub_expr)
        )

    return (
        df
        .withColumn("categorie_parametre", cat_expr)
        .withColumn("sous_categorie_parametre", sub_expr)
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### 7c — Conformite standardisee
# MAGIC

# COMMAND ----------

def enrich_conformite(df: DataFrame) -> DataFrame:
    """
    Normalise conformite_globale en :
    - conformite_standard : conforme | non_conforme | conforme_avec_remarque | inconnu
    - est_conforme        : boolean (True / False / null)
    """
    return (
        df
        .withColumn(
            "conformite_standard",
            F.when(F.lower(F.col("conformite_globale")).rlike(r"non.conforme"), "non_conforme")
            .when(F.lower(F.col("conformite_globale")).contains("remarque"), "conforme_avec_remarque")
            .when(F.lower(F.col("conformite_globale")).contains("conforme"), "conforme")
            .otherwise("inconnu")
        )
        .withColumn(
            "est_conforme",
            F.when(F.col("conformite_standard") == "conforme", True)
             .when(F.col("conformite_standard") == "non_conforme", False)
             .otherwise(F.lit(None).cast("boolean"))
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 — Selection finale des colonnes
# MAGIC

# COMMAND ----------

def select_output_columns(df: DataFrame, output_columns: list) -> DataFrame:
    """
    Garde uniquement les colonnes definies dans config.yaml > silver.output_columns.
    Les colonnes absentes sont ignorees silencieusement.
    """
    existing = set(df.columns)
    final    = list(dict.fromkeys(c for c in output_columns if c in existing))
    missing  = [c for c in output_columns if c not in existing]
    if missing:
        print(f"Colonnes absentes ignorees : {missing}")
    print(f"Colonnes Silver selectionnees : {len(final)}")
    return df.select(*final)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 — Ecriture Silver (Unity Catalog + ADLS Gen2)
# MAGIC

# COMMAND ----------

def write_silver(
    df: DataFrame,
    silver_path: str,
    output_table: str,
    partition_by: list,
    is_databricks_env: bool = False,
    silver_table_full: str = None,
    storage_account: str = None,
    secrets_scope: str = None,
    secret_key_name: str = None,
) -> str:
    """
    Ecrit le DataFrame Silver en Delta Lake partitionne.

    - Local      : chemin systeme de fichiers local
    - Databricks : double écriture
        1. Unity Catalog (saveAsTable — table managée)
        2. ADLS Gen2 (abfss:// partitionné par annee x département)

    Retourne le chemin ADLS pour la validation.
    """
    adls_path = f"{silver_path}/{output_table}"

    if not is_databricks_env:
        # ── Local ─────────────────────────────────────────────────────────
        os.makedirs(silver_path, exist_ok=True)
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .partitionBy(*partition_by)
              .save(adls_path)
        )
        print(f"Silver ecrit (local) : {adls_path}")

    else:
        # ── 1. Unity Catalog — table managée ──────────────────────────────
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .partitionBy(*partition_by)
              .saveAsTable(silver_table_full)
        )
        print(f"Silver UC ecrit : {silver_table_full}")

        # ── 2. ADLS Gen2 — partitionné ────────────────────────────────────
        storage_key = dbutils.secrets.get(scope=secrets_scope, key=secret_key_name)
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .option(
                  f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
                  storage_key
              )
              .partitionBy(*partition_by)
              .save(adls_path)
        )
        print(f"Silver ADLS ecrit : {adls_path}")

    print(f"Partitions : {partition_by}")
    return adls_path


# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 — Validation post-ecriture
# MAGIC

# COMMAND ----------

def validate_silver(spark: SparkSession, silver_out_path: str,
                    is_databricks_env: bool = False,
                    silver_table_full: str = None) -> None:
    """
    Relit la table Silver et affiche les metriques cles.
    - Databricks : lecture depuis Unity Catalog
    - Local      : lecture depuis fichiers Delta
    """
    if is_databricks_env and silver_table_full:
        df = spark.read.table(silver_table_full)
        print(f"Source : Unity Catalog ({silver_table_full})")
    else:
        df = spark.read.format("delta").load(silver_out_path)
        print(f"Source : fichiers Delta ({silver_out_path})")

    total = df.count()
    print(f"Total lignes Silver : {total:,}")
    print(f"Colonnes            : {len(df.columns)}")

    print("\n=== Partitions par annee ===")
    df.groupBy("annee").count().orderBy("annee").show()

    print("=== Top 10 departements ===")
    df.groupBy("code_departement", "nom_departement") \
      .count().orderBy(F.col("count").desc()).limit(10) \
      .toPandas().pipe(lambda p: print(p.to_string(index=False)))

    print("\n=== Taux de conformite par annee ===")
    df.groupBy("annee").agg(
        F.count("*").alias("nb_analyses"),
        F.round(
            F.sum(F.when(F.col("conformite_standard") == "conforme", 1).otherwise(0))
            / F.count("*") * 100, 2,
        ).alias("taux_conformite_%"),
    ).orderBy("annee").show()

    print("=== Apercu Silver (2 lignes) ===")
    df.show(2, truncate=False, vertical=True)


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## `__main__` — Execution en script standalone
# MAGIC
# MAGIC **Usage :**
# MAGIC ```bash
# MAGIC python notebooks/silver/silver_transform.py
# MAGIC python notebooks/silver/silver_transform.py --config /chemin/vers/config.yaml
# MAGIC ```
# MAGIC

# COMMAND ----------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Silver Transform — Water Quality Pipeline")
    parser.add_argument("--config", default=None,
                        help="Chemin vers config.yaml (detection automatique si omis)")
    args, _ = parser.parse_known_args()

    cfg        = load_config(args.config)
    silver_cfg = get_silver_cfg(cfg)
    paths      = get_paths(cfg)
    is_db      = is_databricks(cfg)
    uc_cfg     = cfg["unity_catalog"]
    storage    = cfg["storage"]
    secrets    = storage["secrets"]

    b_path       = paths["bronze"]
    s_path       = paths["silver"]
    table        = silver_cfg["output_table"]
    dedup_keys   = silver_cfg["dedup_keys"]
    partition_by = silver_cfg["partition_by"]
    categories   = silver_cfg["categories"]
    sous_cats    = silver_cfg["sous_categories"]
    out_cols     = silver_cfg["output_columns"]

    catalog           = uc_cfg["catalog"]
    silver_table_full = f"{catalog}.{uc_cfg['silver']['schema']}.{table}"
    storage_account   = storage["account_name"]
    secrets_scope     = secrets["scope"]
    secret_key_name   = secrets["storage_account_key"]

    uc_tables = {
        "water_quality": f"{catalog}.{uc_cfg['bronze']['schema']}.{uc_cfg['bronze']['table']}",
        "communes":      f"{catalog}.{uc_cfg['geo']['schema']}.{uc_cfg['geo']['communes']}",
        "departements":  f"{catalog}.{uc_cfg['geo']['schema']}.{uc_cfg['geo']['departements']}",
        "regions":       f"{catalog}.{uc_cfg['geo']['schema']}.{uc_cfg['geo']['regions']}",
    } if is_db else None

    print(f"[main] Environnement : {'Databricks' if is_db else 'Local'}")
    print(f"[main] Bronze -> {b_path}")
    print(f"[main] Silver -> {s_path}")

    session = get_spark(cfg)

    bz      = load_bronze(session, b_path, is_db, uc_tables)
    _clean  = clean(bz["water_quality"], dedup_keys)
    _std    = standardize(_clean)
    _geo    = enrich_geo(_std, bz["communes"], bz["regions"])
    _cat    = enrich_categories(_geo, categories, sous_cats)
    _conf   = enrich_conformite(_cat)
    _final  = select_output_columns(_conf, out_cols)

    out_path = write_silver(
        _final, s_path, table, partition_by, is_db,
        silver_table_full = silver_table_full if is_db else None,
        storage_account   = storage_account   if is_db else None,
        secrets_scope     = secrets_scope     if is_db else None,
        secret_key_name   = secret_key_name   if is_db else None,
    )

    validate_silver(session, out_path, is_db,
                    silver_table_full if is_db else None)

    if not is_db:
        session.stop()

    print("[main] Pipeline Silver termine avec succes")
