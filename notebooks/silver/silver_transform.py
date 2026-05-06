# Databricks notebook source
# %% [markdown]
# # Silver Layer — Water Quality Pipeline
#
# Script modulaire — compatible **local** (PySpark standalone) et **Databricks**.
# Configuration pilotée par `config/config.yaml` (section `silver`).
#
# Étapes :
# 1. Config + détection environnement
# 2. Session Spark
# 3. Chargement Bronze
# 4. Analyse exploratoire
# 5. Nettoyage (dédup, nulls, types)
# 6. Standardisation colonnes
# 7. Enrichissement (géo, catégories, conformité)
# 8. Sélection finale
# 9. Écriture Silver (Delta, partitionné annee x département)
# 10. Validation post-écriture

# %% [markdown]
# ## 0 — Imports

# %%
# COMMAND ----------
import os
import sys
import yaml

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

# %% [markdown]
# ## 1 — Configuration

# %%
# COMMAND ----------


def load_config(config_path: str = None) -> dict:
    """
    Charge config/config.yaml.
    Remonte automatiquement si le chemin n'est pas fourni.
    Compatible execution depuis notebooks/silver/ ou depuis la racine.
    """
    if config_path is None:
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


# %%
# COMMAND ----------
# Chargement global (visible par toutes les cellules du notebook)
# Guard : ne s'execute pas lors d'un import (tests unitaires, etc.)
_NOTEBOOK_RUN = __name__ == "__main__" or (
    "ipykernel" in sys.modules
    or os.environ.get("DATABRICKS_RUNTIME_VERSION")
    or os.environ.get("SILVER_NOTEBOOK_RUN")
)

if _NOTEBOOK_RUN:
    CFG = load_config()
    SILVER_CFG = get_silver_cfg(CFG)
    PATHS = get_paths(CFG)

    BRONZE_PATH = PATHS["bronze"]
    SILVER_PATH = PATHS["silver"]
    OUTPUT_TABLE = SILVER_CFG["output_table"]
    DEDUP_KEYS = SILVER_CFG["dedup_keys"]
    PARTITION_BY = SILVER_CFG["partition_by"]
    CATEGORIES = SILVER_CFG["categories"]
    SOUS_CATS = SILVER_CFG["sous_categories"]
    OUTPUT_COLS = SILVER_CFG["output_columns"]

    IS_DATABRICKS = is_databricks(CFG)

    print(f"Environnement : {'Databricks' if IS_DATABRICKS else 'Local'}")
    print(f"Bronze path   : {BRONZE_PATH}")
    print(f"Silver path   : {SILVER_PATH}")

# %% [markdown]
# ## 2 — Session Spark

# %%
# COMMAND ----------


def get_spark(cfg: dict) -> SparkSession:
    """
    Retourne la SparkSession adaptée a l'environnement.
    - Databricks : recupere la session existante (variable `spark` globale).
    - Local      : cree une session avec Delta Lake configure via delta-spark.
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


# %%
# COMMAND ----------
if _NOTEBOOK_RUN:
    spark = get_spark(CFG)
    spark.sparkContext.setLogLevel("ERROR")
    print(
        f"Spark {
            spark.version} OK  |  app={
            spark.conf.get('spark.app.name')}")

# %% [markdown]
# ## 3 — Chargement Bronze

# %%
# COMMAND ----------


def load_bronze(spark: SparkSession, bronze_path: str) -> dict:
    """
    Charge les 4 tables Delta Bronze.
    Retourne un dict {table_name: DataFrame}.
    """
    tables = ["water_quality", "communes", "departements", "regions"]
    loaded = {t: spark.read.format("delta").load(
        f"{bronze_path}/{t}") for t in tables}
    for name, df in loaded.items():
        print(
            f"  {name:<15} : {df.count():>10,} lignes  | {len(df.columns)} colonnes")
    return loaded


# %%
# COMMAND ----------
if _NOTEBOOK_RUN:
    bronze = load_bronze(spark, BRONZE_PATH)

# %% [markdown]
# ## 4 — Analyse exploratoire Bronze

# %%
# COMMAND ----------


def explore_bronze(df_water: DataFrame) -> None:
    """Affiche les statistiques cles du DataFrame Bronze."""
    total = df_water.count()

    print("=== Schema water_quality ===")
    df_water.printSchema()

    print("\n=== Distribution annee_partition ===")
    df_water.groupBy("annee_partition").count().orderBy(
        "annee_partition").show()

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


# %%
# COMMAND ----------
if _NOTEBOOK_RUN:
    explore_bronze(bronze["water_quality"])

# %% [markdown]
# ## 5 — Nettoyage

# %%
# COMMAND ----------


def clean(df: DataFrame, dedup_keys: list) -> DataFrame:
    """
    - Deduplication sur cle metier (configurable via config.yaml > silver.dedup_keys)
    - Filtres sur champs obligatoires (non-nullable selon doc SISE-Eaux)
    - Correction des types (annee_partition, resultat_numerique, date_prelevement)
    - Derivation annee / mois
    - Suppression colonnes techniques dlt (_dlt_load_id, _dlt_id)
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
    print(f"Avant  : {total_before:,}")
    print(f"Apres  : {df_out.count():,}")
    print(f"Supprimees : {removed:,}  ({removed / total_before * 100:.2f}%)")
    return df_out


# %%
# COMMAND ----------
if _NOTEBOOK_RUN:
    df_clean = clean(bronze["water_quality"], DEDUP_KEYS)

# %% [markdown]
# ## 6 — Standardisation des colonnes

# %%
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
        # Extraction JSON reseaux -> premier reseau
        .withColumn("_json", F.regexp_extract(F.col("reseaux"), r"\{.*?\}", 0))
        .withColumn("code_reseau", F.get_json_object(F.col("_json"), "$.code"))
        .withColumn("nom_reseau", F.get_json_object(F.col("_json"), "$.nom"))
        .drop("_json", "reseaux")
        # Normalisation texte
        .withColumn("libelle_parametre", F.trim(F.col("libelle_parametre")))
        .withColumn("libelle_parametre_maj", F.upper(F.trim(F.col("libelle_parametre_maj"))))
        .withColumn("nom_commune", F.trim(F.col("nom_commune")))
        # Padding codes INSEE
        .withColumn("code_commune", F.lpad(F.trim(F.col("code_commune")), 5, "0"))
        .withColumn("code_departement", F.lpad(F.trim(F.col("code_departement")), 2, "0"))
        # Renommage conformite
        .withColumnRenamed("conclusion_conformite_prelevement", "conformite_globale")
        .withColumnRenamed("conformite_limites_bact_prelevement", "conformite_bact")
        .withColumnRenamed("conformite_limites_pc_prelevement", "conformite_pc")
        .withColumnRenamed("conformite_references_bact_prelevement", "conformite_ref_bact")
        .withColumnRenamed("conformite_references_pc_prelevement", "conformite_ref_pc")
    )


# %%
# COMMAND ----------
if _NOTEBOOK_RUN:
    df_std = standardize(df_clean)
    print("=== Schema post-standardisation ===")
    df_std.printSchema()

# %% [markdown]
# ## 7 — Enrichissement

# %% [markdown]
# ### 7a — Jointure geographique (communes -> regions)

# %%
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
        df .join(
            ref_communes,
            df["code_commune"] == ref_communes["_code_commune"],
            how="left") .drop("_code_commune") .join(
            ref_regions,
            F.col("code_region") == ref_regions["_code_region"],
            how="left") .drop("_code_region"))

    n = df_out.count()
    matched = df_out.filter(F.col("nom_region").isNotNull()).count()
    no_geo = df_out.filter(F.col("latitude").isNull()).count()
    print(
        f"Taux jointure region  : {matched / n * 100:.1f}%  ({matched:,}/{n:,})")
    print(f"Communes sans geodata : {no_geo:,} / {n:,}")
    return df_out


# %%
# COMMAND ----------
if _NOTEBOOK_RUN:
    df_geo = enrich_geo(df_std, bronze["communes"], bronze["regions"])

# %% [markdown]
# ### 7b — Categories parametres (depuis config.yaml)

# %%
# COMMAND ----------


def enrich_categories(
        df: DataFrame,
        categories: dict,
        sous_categories: dict) -> DataFrame:
    """
    Ajoute deux colonnes derivees, entierement pilotees par config.yaml :
    - categorie_parametre      : depuis code_type_parametre (B/P/R -> libelle)
    - sous_categorie_parametre : depuis regex sur libelle_parametre (lowercase)
    """
    # Categorie principale
    cat_expr = F.lit("Autre")
    for code, label in categories.items():
        cat_expr = F.when(
            F.col("code_type_parametre") == code,
            label).otherwise(cat_expr)

    # Sous-categorie (on itere en ordre inverse pour que le premier match
    # gagne)
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


# %%
# COMMAND ----------
if _NOTEBOOK_RUN:
    df_cat = enrich_categories(df_geo, CATEGORIES, SOUS_CATS)
    print("=== Distribution categories parametres ===")
    df_cat.groupBy("categorie_parametre", "sous_categorie_parametre") \
          .count().orderBy("categorie_parametre", F.col("count").desc()) \
          .show(30, truncate=False)

# %% [markdown]
# ### 7c — Conformite standardisee

# %%
# COMMAND ----------


def enrich_conformite(df: DataFrame) -> DataFrame:
    """
    Normalise le champ texte libre `conformite_globale` (SISE-Eaux) en :
    - conformite_standard : 'conforme' | 'non_conforme' | 'conforme_avec_remarque' | 'inconnu'
    - est_conforme        : boolean (True / False / null)
    """
    return (
        df
        .withColumn(
            "conformite_standard",
            F.when(F.lower(F.col("conformite_globale")).rlike(r"non.conforme"),
                   "non_conforme")
            .when(F.lower(F.col("conformite_globale")).contains("remarque"),
                  "conforme_avec_remarque")
            .when(F.lower(F.col("conformite_globale")).contains("conforme"),
                  "conforme")
            .otherwise("inconnu")
        )
        .withColumn(
            "est_conforme",
            F.when(F.col("conformite_standard") == "conforme", True)
             .when(F.col("conformite_standard") == "non_conforme", False)
             .otherwise(F.lit(None).cast("boolean"))
        )
    )


# %%
# COMMAND ----------
if _NOTEBOOK_RUN:
    df_enrich = enrich_conformite(df_cat)
    print("=== Repartition conformite ===")
    df_enrich.groupBy("conformite_standard", "est_conforme") \
             .count().orderBy("count", ascending=False).show()

# %% [markdown]
# ## 8 — Selection finale des colonnes

# %%
# COMMAND ----------


def select_output_columns(df: DataFrame, output_columns: list) -> DataFrame:
    """
    Garde uniquement les colonnes definies dans config.yaml > silver.output_columns.
    Les colonnes absentes sont ignorees silencieusement (robustesse).
    """
    existing = set(df.columns)
    final = list(dict.fromkeys(c for c in output_columns if c in existing))
    missing = [c for c in output_columns if c not in existing]
    if missing:
        print(f"Colonnes absentes ignorees : {missing}")
    print(f"Colonnes Silver selectionnees : {len(final)}")
    return df.select(*final)


# %%
# COMMAND ----------
if _NOTEBOOK_RUN:
    df_silver = select_output_columns(df_enrich, OUTPUT_COLS)
    df_silver.printSchema()

# %% [markdown]
# ## 9 — Ecriture Silver (Delta Lake)

# %%
# COMMAND ----------


def write_silver(
    df: DataFrame,
    silver_path: str,
    output_table: str,
    partition_by: list,
    is_databricks_env: bool = False,
) -> str:
    """
    Ecrit le DataFrame Silver en Delta Lake partitionne.
    - Local      : chemin systeme de fichiers local
    - Databricks : chemin DBFS (dbfs:/mnt/...) ou Unity Catalog
    Retourne le chemin de sortie pour la validation.
    """
    out_path = f"{silver_path}/{output_table}"

    if not is_databricks_env:
        os.makedirs(silver_path, exist_ok=True)

    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .partitionBy(*partition_by)
          .save(out_path)
    )

    print(f"Silver ecrit : {out_path}")
    print(f"Partitions   : {partition_by}")
    return out_path


# %%
# COMMAND ----------
if _NOTEBOOK_RUN:
    silver_out_path = write_silver(
        df_silver, SILVER_PATH, OUTPUT_TABLE, PARTITION_BY, IS_DATABRICKS
    )

# %% [markdown]
# ## 10 — Validation post-ecriture

# %%
# COMMAND ----------


def validate_silver(spark: SparkSession, silver_out_path: str) -> None:
    """Relit la table Silver et affiche les metriques cles."""
    df = spark.read.format("delta").load(silver_out_path)

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
            F.sum(
                F.when(
                    F.col("conformite_standard") == "conforme",
                    1).otherwise(0)) /
            F.count("*") *
            100,
            2,
        ).alias("taux_conformite_%"),
    ).orderBy("annee").show()

    print("=== Apercu Silver (2 lignes) ===")
    df.show(2, truncate=False, vertical=True)


# %%
# COMMAND ----------
if _NOTEBOOK_RUN:
    validate_silver(spark, silver_out_path)

# %% [markdown]
# ## 11 — Arret Spark (local uniquement)

# %%
# COMMAND ----------
if _NOTEBOOK_RUN and not IS_DATABRICKS:
    spark.stop()
    print("Spark arrete")

# %% [markdown]
# ---
# ## `__main__` — Execution en script standalone
#
# **Usage :**
# ```bash
# # Depuis la racine du projet
# python notebooks/silver/silver_transform.py
#
# # Avec chemin de config personnalise
# python notebooks/silver/silver_transform.py --config /chemin/vers/config.yaml
# ```

# %%
# COMMAND ----------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Silver Transform — Water Quality Pipeline"
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Chemin vers config.yaml (detection automatique si omis)",
    )
    args, _ = parser.parse_known_args()

    # ── 1. Config
    cfg = load_config(args.config)
    silver_cfg = get_silver_cfg(cfg)
    paths = get_paths(cfg)
    is_db = is_databricks(cfg)

    b_path = paths["bronze"]
    s_path = paths["silver"]
    table = silver_cfg["output_table"]
    dedup_keys = silver_cfg["dedup_keys"]
    partition_by = silver_cfg["partition_by"]
    categories = silver_cfg["categories"]
    sous_cats = silver_cfg["sous_categories"]
    out_cols = silver_cfg["output_columns"]

    print(f"[main] Environnement : {'Databricks' if is_db else 'Local'}")
    print(f"[main] Bronze -> {b_path}")
    print(f"[main] Silver -> {s_path}")

    # ── 2. Spark
    session = get_spark(cfg)
    session.sparkContext.setLogLevel("ERROR")

    # ── 3. Pipeline
    bz = load_bronze(session, b_path)
    _clean = clean(bz["water_quality"], dedup_keys)
    _std = standardize(_clean)
    _geo = enrich_geo(_std, bz["communes"], bz["regions"])
    _cat = enrich_categories(_geo, categories, sous_cats)
    _conf = enrich_conformite(_cat)
    _final = select_output_columns(_conf, out_cols)
    out_path = write_silver(_final, s_path, table, partition_by, is_db)
    validate_silver(session, out_path)

    # ── 4. Nettoyage
    if not is_db:
        session.stop()
    print("[main] Pipeline Silver termine avec succes")
