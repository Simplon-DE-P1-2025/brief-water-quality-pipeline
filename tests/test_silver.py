"""
Tests unitaires — Silver Transform Pipeline.

Couverture :
  - load_config / get_silver_cfg / is_databricks / get_paths  (pur Python)
  - clean()                (PySpark — dédup, filtres, types, colonnes dérivées)
  - standardize()          (PySpark — JSON reseaux, trim, lpad, renommage)
  - enrich_categories()    (PySpark — mapping code_type_parametre + regex sous-cat)
  - enrich_conformite()    (PySpark — conformite_standard + est_conforme)
  - select_output_columns()(PySpark — sélection + colonnes manquantes ignorées)
  - enrich_geo()           (PySpark — left join communes + regions)

Convention : les tests Spark partagent une seule SparkSession (module-level)
pour limiter le temps de démarrage, comme dans test_bronze.py.
"""

from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, TimestampType,
)
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from notebooks.silver.silver_transform import (
    load_config,
    get_silver_cfg,
    is_databricks,
    get_paths,
    clean,
    standardize,
    enrich_categories,
    enrich_conformite,
    select_output_columns,
    enrich_geo,
)
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ── imports Silver (fonctions pures uniquement, pas les cellules globales)

# =========================================================
# SparkSession partagée (module-level)
# =========================================================


_spark = None


def spark():
    """Retourne (ou crée) la SparkSession de test."""
    global _spark
    if _spark is None:
        from delta import configure_spark_with_delta_pip
        _spark = configure_spark_with_delta_pip(
            SparkSession.builder
            .appName("test_silver")
            .master("local[1]")
            .config("spark.driver.memory", "1g")
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "1")
        ).getOrCreate()
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark


# =========================================================
# Helpers — construction de DataFrames de test
# =========================================================

WATER_SCHEMA = StructType([
    StructField("code_prelevement", StringType(), True),
    StructField("code_parametre", StringType(), True),
    StructField("code_lieu_analyse", StringType(), True),
    StructField("date_prelevement", TimestampType(), True),
    StructField("code_commune", StringType(), True),
    StructField("libelle_parametre", StringType(), True),
    StructField("libelle_parametre_maj", StringType(), True),
    StructField("code_type_parametre", StringType(), True),
    StructField("code_departement", StringType(), True),
    StructField("nom_commune", StringType(), True),
    StructField("nom_departement", StringType(), True),
    StructField("resultat_numerique", DoubleType(), True),
    StructField("resultat_alphanumerique", StringType(), True),
    StructField("libelle_unite", StringType(), True),
    StructField("reseaux", StringType(), True),
    StructField("conclusion_conformite_prelevement", StringType(), True),
    StructField("conformite_limites_bact_prelevement", StringType(), True),
    StructField("conformite_limites_pc_prelevement", StringType(), True),
    StructField("conformite_references_bact_prelevement", StringType(), True),
    StructField("conformite_references_pc_prelevement", StringType(), True),
    StructField("annee_partition", LongType(), True),
    StructField("_dlt_load_id", StringType(), True),
    StructField("_dlt_id", StringType(), True),
    StructField("reference_analyse", StringType(), True),
    StructField("nom_distributeur", StringType(), True),
    StructField("nom_uge", StringType(), True),
])

TS_2024 = datetime(2024, 6, 15, 10, 0, 0)
TS_2025 = datetime(2025, 3, 1, 8, 0, 0)


def make_water_df(rows: list) -> "DataFrame":
    return spark().createDataFrame(rows, schema=WATER_SCHEMA)


def base_row(**kwargs):
    defaults = dict(
        code_prelevement="PLV001",
        code_parametre="1340",
        code_lieu_analyse="UDI",
        date_prelevement=TS_2024,
        code_commune="13055",
        libelle_parametre="Nitrates",
        libelle_parametre_maj="NITRATES",
        code_type_parametre="N",
        code_departement="13",
        nom_commune="Marseille",
        nom_departement="Bouches-du-Rhône",
        resultat_numerique=12.5,
        resultat_alphanumerique=None,
        libelle_unite="mg/L",
        reseaux='[{"code":"130000001","nom":"UDI MARSEILLE"}]',
        conclusion_conformite_prelevement=(
            "Eau d'alimentation conforme aux exigences de qualité en vigueur "
            "pour l'ensemble des paramètres mesurés."
        ),
        conformite_limites_bact_prelevement="C",
        conformite_limites_pc_prelevement="C",
        conformite_references_bact_prelevement="C",
        conformite_references_pc_prelevement="C",
        annee_partition=2024,
        _dlt_load_id="load-01",
        _dlt_id="id-01",
        reference_analyse="REF001",
        nom_distributeur="METROPOLE",
        nom_uge="UGE-13",
    )
    defaults.update(kwargs)
    return tuple(defaults[f.name] for f in WATER_SCHEMA)


# =========================================================
# Helpers — config minimale
# =========================================================

def make_cfg(is_databricks_flag: bool = False) -> dict:
    return {
        "environment": {
            "is_databricks": is_databricks_flag},
        "silver": {
            "output_table": "water_quality",
            "spark": {
                "app_name": "silver_transform",
                "driver_memory": "2g",
                "shuffle_partitions": 4,
            },
            "paths": {
                "local": {
                    "bronze": "data/bronze",
                    "silver": "data/silver"},
                "databricks": {
                    "bronze": "dbfs:/mnt/bronze",
                    "silver": "dbfs:/mnt/silver"},
            },
            "dedup_keys": [
                "code_prelevement",
                "code_parametre",
                "code_lieu_analyse"],
            "partition_by": [
                "annee",
                "code_departement"],
            "categories": {
                "N": "Physicochimique",
                "O": "Organoleptique"},
            "sous_categories": {
                "Azote": "nitrate|nitrite|azote",
                "Pesticide": "pesticide|herbicide|atrazine",
            },
            "output_columns": [
                "code_prelevement",
                "date_prelevement",
                "annee",
                "mois",
                "code_commune",
                "code_departement",
                "resultat_numerique",
            ],
        },
    }


# =========================================================
# 1 — Fonctions de configuration (pur Python)
# =========================================================

class TestLoadConfig:
    def test_loads_real_config(self):
        cfg = load_config("config/config.yaml")
        assert "silver" in cfg
        assert "environment" in cfg

    def test_auto_detection_from_root(self):
        """load_config() sans argument doit trouver config/config.yaml."""
        cfg = load_config()
        assert "silver" in cfg

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_config(str(tmp_path / "inexistant.yaml"))


class TestGetSilverCfg:
    def test_returns_silver_section(self):
        cfg = make_cfg()
        scfg = get_silver_cfg(cfg)
        assert scfg["output_table"] == "water_quality"
        assert "paths" in scfg
        assert "spark" in scfg

    def test_missing_key_raises(self):
        with pytest.raises(KeyError):
            get_silver_cfg({})


class TestIsDatabricks:
    def test_returns_false_by_default(self):
        cfg = make_cfg(is_databricks_flag=False)
        assert is_databricks(cfg) is False

    def test_returns_true_from_config(self):
        cfg = make_cfg(is_databricks_flag=True)
        assert is_databricks(cfg) is True

    def test_env_var_overrides_config(self, monkeypatch):
        cfg = make_cfg(is_databricks_flag=False)
        monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "14.0")
        assert is_databricks(cfg) is True

    def test_env_var_absent_uses_config(self, monkeypatch):
        cfg = make_cfg(is_databricks_flag=False)
        monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
        assert is_databricks(cfg) is False


class TestGetPaths:
    def test_local_paths(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
        cfg = make_cfg(is_databricks_flag=False)
        paths = get_paths(cfg)
        assert paths["bronze"] == "data/bronze"
        assert paths["silver"] == "data/silver"

    def test_databricks_paths(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "14.0")
        cfg = make_cfg(is_databricks_flag=True)
        paths = get_paths(cfg)
        assert paths["bronze"] == "dbfs:/mnt/bronze"
        assert paths["silver"] == "dbfs:/mnt/silver"


# =========================================================
# 2 — clean()
# =========================================================

class TestClean:
    DEDUP_KEYS = ["code_prelevement", "code_parametre", "code_lieu_analyse"]

    def test_removes_duplicates(self):
        row = base_row()
        df = make_water_df([row, row])   # doublon exact
        result = clean(df, self.DEDUP_KEYS)
        assert result.count() == 1

    def test_keeps_distinct_rows(self):
        df = make_water_df([
            base_row(code_prelevement="PLV001"),
            base_row(code_prelevement="PLV002"),
        ])
        result = clean(df, self.DEDUP_KEYS)
        assert result.count() == 2

    def test_filters_null_date(self):
        df = make_water_df([
            base_row(),
            base_row(code_prelevement="PLV_NULL", date_prelevement=None),
        ])
        result = clean(df, self.DEDUP_KEYS)
        assert result.count() == 1

    def test_filters_null_commune(self):
        df = make_water_df([
            base_row(),
            base_row(code_prelevement="PLV_NC", code_commune=None),
        ])
        result = clean(df, self.DEDUP_KEYS)
        assert result.count() == 1

    def test_filters_null_libelle_parametre(self):
        df = make_water_df([
            base_row(),
            base_row(code_prelevement="PLV_NL", libelle_parametre=None),
        ])
        result = clean(df, self.DEDUP_KEYS)
        assert result.count() == 1

    def test_casts_annee_partition_to_int(self):
        df = make_water_df([base_row(annee_partition=2024)])
        result = clean(df, self.DEDUP_KEYS)
        dtype = dict(result.dtypes)["annee_partition"]
        assert dtype == "int"

    def test_casts_resultat_numerique_to_double(self):
        df = make_water_df([base_row()])
        result = clean(df, self.DEDUP_KEYS)
        dtype = dict(result.dtypes)["resultat_numerique"]
        assert dtype == "double"

    def test_derives_annee_column(self):
        df = make_water_df([base_row(date_prelevement=TS_2024)])
        result = clean(df, self.DEDUP_KEYS)
        row = result.select("annee").first()
        assert row["annee"] == 2024

    def test_derives_mois_column(self):
        df = make_water_df([base_row(date_prelevement=TS_2024)])
        result = clean(df, self.DEDUP_KEYS)
        row = result.select("mois").first()
        assert row["mois"] == 6   # juin

    def test_drops_dlt_columns(self):
        df = make_water_df([base_row()])
        result = clean(df, self.DEDUP_KEYS)
        cols = result.columns
        assert "_dlt_load_id" not in cols
        assert "_dlt_id" not in cols

    def test_date_prelevement_becomes_date_type(self):
        df = make_water_df([base_row()])
        result = clean(df, self.DEDUP_KEYS)
        dtype = dict(result.dtypes)["date_prelevement"]
        assert dtype == "date"


# =========================================================
# 3 — standardize()
# =========================================================

class TestStandardize:
    """standardize() attend un DF déjà nettoyé par clean()."""

    def _cleaned(self, **kwargs):
        df = make_water_df([base_row(**kwargs)])
        return clean(df,
                     ["code_prelevement",
                      "code_parametre",
                      "code_lieu_analyse"])

    def test_extracts_code_reseau(self):
        df = self._cleaned(
            reseaux='[{"code":"130000001","nom":"UDI MARSEILLE"}]')
        result = standardize(df)
        row = result.select("code_reseau").first()
        assert row["code_reseau"] == "130000001"

    def test_extracts_nom_reseau(self):
        df = self._cleaned(
            reseaux='[{"code":"130000001","nom":"UDI MARSEILLE"}]')
        result = standardize(df)
        row = result.select("nom_reseau").first()
        assert row["nom_reseau"] == "UDI MARSEILLE"

    def test_drops_reseaux_column(self):
        df = self._cleaned()
        result = standardize(df)
        assert "reseaux" not in result.columns

    def test_trims_libelle_parametre(self):
        df = self._cleaned(libelle_parametre="  Nitrates  ")
        result = standardize(df)
        row = result.select("libelle_parametre").first()
        assert row["libelle_parametre"] == "Nitrates"

    def test_uppercases_libelle_parametre_maj(self):
        df = self._cleaned(libelle_parametre_maj="nitrates")
        result = standardize(df)
        row = result.select("libelle_parametre_maj").first()
        assert row["libelle_parametre_maj"] == "NITRATES"

    def test_pads_code_commune_to_5(self):
        df = self._cleaned(code_commune="1055")   # 4 chiffres
        result = standardize(df)
        row = result.select("code_commune").first()
        assert row["code_commune"] == "01055"

    def test_pads_code_departement_to_2(self):
        df = self._cleaned(code_departement="1")
        result = standardize(df)
        row = result.select("code_departement").first()
        assert row["code_departement"] == "01"

    def test_renames_conformite_globale(self):
        df = self._cleaned()
        result = standardize(df)
        assert "conformite_globale" in result.columns
        assert "conclusion_conformite_prelevement" not in result.columns

    def test_renames_conformite_bact(self):
        df = self._cleaned()
        result = standardize(df)
        assert "conformite_bact" in result.columns
        assert "conformite_limites_bact_prelevement" not in result.columns

    def test_null_reseaux_gives_null_code(self):
        df = self._cleaned(reseaux=None)
        result = standardize(df)
        row = result.select("code_reseau").first()
        assert row["code_reseau"] is None


# =========================================================
# 4 — enrich_categories()
# =========================================================

class TestEnrichCategories:
    CATS = {"N": "Physicochimique", "O": "Organoleptique"}
    SOUS = {
        "Azote": "nitrate|nitrite|azote",
        "Pesticide": "pesticide|herbicide|atrazine",
    }

    def _df(self, code_type, libelle):
        s = spark()
        return s.createDataFrame(
            [(code_type, libelle)],
            ["code_type_parametre", "libelle_parametre"],
        )

    def test_categorie_N(self):
        result = enrich_categories(
            self._df(
                "N",
                "Nitrates"),
            self.CATS,
            self.SOUS)
        assert result.first()["categorie_parametre"] == "Physicochimique"

    def test_categorie_O(self):
        result = enrich_categories(
            self._df(
                "O",
                "Turbidité"),
            self.CATS,
            self.SOUS)
        assert result.first()["categorie_parametre"] == "Organoleptique"

    def test_categorie_unknown_becomes_autre(self):
        result = enrich_categories(
            self._df(
                "X",
                "Inconnu"),
            self.CATS,
            self.SOUS)
        assert result.first()["categorie_parametre"] == "Autre"

    def test_sous_categorie_azote(self):
        result = enrich_categories(
            self._df(
                "N",
                "Nitrates totaux"),
            self.CATS,
            self.SOUS)
        assert result.first()["sous_categorie_parametre"] == "Azote"

    def test_sous_categorie_pesticide(self):
        result = enrich_categories(
            self._df(
                "N",
                "Atrazine déséthyl"),
            self.CATS,
            self.SOUS)
        assert result.first()["sous_categorie_parametre"] == "Pesticide"

    def test_sous_categorie_autre_when_no_match(self):
        result = enrich_categories(
            self._df(
                "N",
                "Calcium"),
            self.CATS,
            self.SOUS)
        assert result.first()["sous_categorie_parametre"] == "Autre"

    def test_sous_categorie_case_insensitive(self):
        result = enrich_categories(
            self._df(
                "N",
                "NITRATE"),
            self.CATS,
            self.SOUS)
        assert result.first()["sous_categorie_parametre"] == "Azote"

    def test_both_columns_added(self):
        result = enrich_categories(self._df("N", "pH"), self.CATS, self.SOUS)
        assert "categorie_parametre" in result.columns
        assert "sous_categorie_parametre" in result.columns

    def test_empty_categories_all_autre(self):
        result = enrich_categories(self._df("N", "Nitrates"), {}, self.SOUS)
        assert result.first()["categorie_parametre"] == "Autre"


# =========================================================
# 5 — enrich_conformite()
# =========================================================

class TestEnrichConformite:
    def _df(self, conformite_globale: str):
        s = spark()
        return s.createDataFrame(
            [(conformite_globale,)],
            ["conformite_globale"],
        )

    def test_conforme(self):
        phrase = (
            "Eau d'alimentation conforme aux exigences de qualité en vigueur "
            "pour l'ensemble des paramètres mesurés.")
        result = enrich_conformite(self._df(phrase))
        row = result.first()
        assert row["conformite_standard"] == "conforme"
        assert row["est_conforme"] is True

    def test_non_conforme_tiret(self):
        phrase = "Eau d'alimentation non-conforme aux exigences de qualité."
        result = enrich_conformite(self._df(phrase))
        row = result.first()
        assert row["conformite_standard"] == "non_conforme"
        assert row["est_conforme"] is False

    def test_non_conforme_espace(self):
        phrase = "Eau d'alimentation non conforme aux limites de qualité."
        result = enrich_conformite(self._df(phrase))
        row = result.first()
        assert row["conformite_standard"] == "non_conforme"
        assert row["est_conforme"] is False

    def test_conforme_avec_remarque(self):
        phrase = ("Eau d'alimentation conforme aux limites de qualité "
                  "et non conforme aux références de qualité.")
        result = enrich_conformite(self._df(phrase))
        row = result.first()
        # "non conforme aux références" → non_conforme prend priorité
        assert row["conformite_standard"] == "non_conforme"

    def test_conforme_priorite_remarque(self):
        # Phrase avec "remarque" explicite sans "non conforme"
        phrase = "Conforme avec remarque sur la teneur en chlore."
        result = enrich_conformite(self._df(phrase))
        row = result.first()
        assert row["conformite_standard"] == "conforme_avec_remarque"
        assert row["est_conforme"] is None

    def test_null_conformite_globale(self):
        from pyspark.sql.types import StructType, StructField, StringType
        s = spark()
        schema = StructType(
            [StructField("conformite_globale", StringType(), True)])
        df = s.createDataFrame([(None,)], schema=schema)
        result = enrich_conformite(df)
        row = result.first()
        assert row["conformite_standard"] == "inconnu"
        assert row["est_conforme"] is None

    def test_unknown_value_is_inconnu(self):
        result = enrich_conformite(self._df("Valeur inconnue quelconque"))
        row = result.first()
        assert row["conformite_standard"] == "inconnu"
        assert row["est_conforme"] is None

    def test_both_columns_added(self):
        result = enrich_conformite(self._df("Conforme."))
        assert "conformite_standard" in result.columns
        assert "est_conforme" in result.columns


# =========================================================
# 6 — select_output_columns()
# =========================================================

class TestSelectOutputColumns:
    def _df(self):
        s = spark()
        return s.createDataFrame(
            [("PLV001", "13055", 12.5)],
            ["code_prelevement", "code_commune", "resultat_numerique"],
        )

    def test_selects_existing_columns(self):
        result = select_output_columns(
            self._df(), ["code_prelevement", "resultat_numerique"]
        )
        assert set(
            result.columns) == {
            "code_prelevement",
            "resultat_numerique"}

    def test_ignores_missing_columns(self):
        result = select_output_columns(
            self._df(), ["code_prelevement", "colonne_inexistante"]
        )
        assert "code_prelevement" in result.columns
        assert "colonne_inexistante" not in result.columns

    def test_preserves_order(self):
        result = select_output_columns(
            self._df(), ["resultat_numerique", "code_prelevement"]
        )
        assert result.columns == ["resultat_numerique", "code_prelevement"]

    def test_deduplicates_column_list(self):
        result = select_output_columns(
            self._df(), [
                "code_prelevement", "code_prelevement", "code_commune"])
        assert result.columns.count("code_prelevement") == 1

    def test_all_missing_returns_empty_schema(self):
        result = select_output_columns(self._df(), ["nope", "nada"])
        assert result.columns == []


# =========================================================
# 7 — enrich_geo()
# =========================================================

class TestEnrichGeo:
    def _dfs(self):
        s = spark()

        df_water = s.createDataFrame(
            [("13055", "13"), ("69123", "69"), ("99999", "99")],
            ["code_commune", "code_departement"],
        )

        df_communes = s.createDataFrame(
            [
                ("13055", 43.296, 5.381, 868277, "93"),
                ("69123", 45.748, 4.847, 515695, "84"),
            ],
            ["code_commune", "latitude", "longitude", "population", "code_region"],
        )

        df_regions = s.createDataFrame(
            [("93", "Provence-Alpes-Côte d'Azur"), ("84", "Auvergne-Rhône-Alpes")],
            ["code_region", "nom_region"],
        )
        return df_water, df_communes, df_regions

    def test_adds_latitude_column(self):
        df_w, df_c, df_r = self._dfs()
        result = enrich_geo(df_w, df_c, df_r)
        assert "latitude" in result.columns

    def test_adds_nom_region_column(self):
        df_w, df_c, df_r = self._dfs()
        result = enrich_geo(df_w, df_c, df_r)
        assert "nom_region" in result.columns

    def test_matched_commune_has_region(self):
        df_w, df_c, df_r = self._dfs()
        result = enrich_geo(df_w, df_c, df_r)
        row = (result.filter(F.col("code_commune") == "13055")
                     .select("nom_region").first())
        assert row["nom_region"] == "Provence-Alpes-Côte d'Azur"

    def test_unmatched_commune_has_null_region(self):
        df_w, df_c, df_r = self._dfs()
        result = enrich_geo(df_w, df_c, df_r)
        row = (result.filter(F.col("code_commune") == "99999")
                     .select("nom_region").first())
        assert row["nom_region"] is None

    def test_row_count_unchanged_left_join(self):
        df_w, df_c, df_r = self._dfs()
        result = enrich_geo(df_w, df_c, df_r)
        assert result.count() == df_w.count()

    def test_no_duplicate_rows_after_join(self):
        df_w, df_c, df_r = self._dfs()
        result = enrich_geo(df_w, df_c, df_r)
        assert result.count() == result.dropDuplicates(
            ["code_commune"]).count()
