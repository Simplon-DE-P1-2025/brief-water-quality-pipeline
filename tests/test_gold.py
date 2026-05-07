"""
Tests unitaires — Gold Transform Pipeline.

Couverture :
  - load_config / get_gold_cfg / is_databricks / get_paths  (pur Python)
  - build_conformite_dept()     (PySpark — agrégation conformité par dept)
  - build_parametres_risks()    (PySpark — top N paramètres non conformes)
  - build_commune_stats()       (PySpark — stats par commune)
  - build_evolution_mensuelle() (PySpark — évolution + delta mensuel)
  - write_gold()                (PySpark — écriture Delta locale)

Convention : SparkSession partagée au niveau module pour limiter le temps
de démarrage, comme dans test_silver.py.
"""

from notebooks.gold.gold_transform import (
    load_config,
    get_gold_cfg,
    is_databricks,
    get_paths,
    build_conformite_dept,
    build_parametres_risks,
    build_commune_stats,
    build_evolution_mensuelle,
    write_gold,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType,
)
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# =========================================================
# SparkSession partagée (module-level)
# =========================================================

_spark = None


def spark():
    global _spark
    if _spark is None:
        from delta import configure_spark_with_delta_pip
        _spark = configure_spark_with_delta_pip(
            SparkSession.builder
            .appName("test_gold")
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
# Helpers
# =========================================================

SILVER_SCHEMA = StructType([
    StructField("annee", IntegerType(), True),
    StructField("mois", IntegerType(), True),
    StructField("code_commune", StringType(), True),
    StructField("nom_commune", StringType(), True),
    StructField("code_departement", StringType(), True),
    StructField("nom_departement", StringType(), True),
    StructField("nom_region", StringType(), True),
    StructField("code_region", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("population", IntegerType(), True),
    StructField("code_parametre", StringType(), True),
    StructField("libelle_parametre", StringType(), True),
    StructField("categorie_parametre", StringType(), True),
    StructField("sous_categorie_parametre", StringType(), True),
    StructField("conformite_standard", StringType(), True),
    StructField("est_conforme", BooleanType(), True),
])


def make_silver_df(rows: list) -> DataFrame:
    return spark().createDataFrame(rows, schema=SILVER_SCHEMA)


def base_row(**kwargs):
    defaults = dict(
        annee=2024,
        mois=6,
        code_commune="13055",
        nom_commune="Marseille",
        code_departement="13",
        nom_departement="Bouches-du-Rhône",
        nom_region="Provence-Alpes-Côte d'Azur",
        code_region="93",
        latitude=43.296,
        longitude=5.381,
        population=868277,
        code_parametre="1340",
        libelle_parametre="Nitrates",
        categorie_parametre="Physicochimique",
        sous_categorie_parametre="Azote",
        conformite_standard="conforme",
        est_conforme=True,
    )
    defaults.update(kwargs)
    return tuple(defaults[f.name] for f in SILVER_SCHEMA)


def make_cfg(is_databricks_flag: bool = False) -> dict:
    return {
        "environment": {"is_databricks": is_databricks_flag},
        "gold": {
            "spark": {
                "app_name": "gold_transform",
                "driver_memory": "2g",
                "shuffle_partitions": 4,
            },
            "paths": {
                "local": {
                    "silver": "data/silver",
                    "gold": "data/gold",
                },
                "databricks": {
                    "silver": "dbfs:/mnt/silver",
                    "gold": "dbfs:/mnt/gold",
                },
            },
            "tables": {
                "conformite_dept": {
                    "output_table": "gold_conformite_dept",
                    "partition_by": ["annee"],
                },
                "parametres_risks": {
                    "output_table": "gold_parametres_risks",
                    "partition_by": ["annee"],
                },
                "commune_stats": {
                    "output_table": "gold_commune_stats",
                    "partition_by": ["annee"],
                },
                "evolution_mensuelle": {
                    "output_table": "gold_evolution_mensuelle",
                    "partition_by": ["annee"],
                },
            },
            "databricks": {"catalog": "main", "database": "gold"},
        },
    }


# =========================================================
# 1 — Fonctions de configuration (pur Python)
# =========================================================

class TestLoadConfig:
    def test_loads_real_config(self):
        cfg = load_config("config/config.yaml")
        assert "gold" in cfg
        assert "environment" in cfg

    def test_auto_detection_from_root(self):
        cfg = load_config()
        assert "gold" in cfg

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_config(str(tmp_path / "inexistant.yaml"))


class TestGetGoldCfg:
    def test_returns_gold_section(self):
        cfg = make_cfg()
        gcfg = get_gold_cfg(cfg)
        assert "tables" in gcfg
        assert "paths" in gcfg
        assert "spark" in gcfg

    def test_missing_key_raises(self):
        with pytest.raises(KeyError):
            get_gold_cfg({})


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
        assert paths["silver"] == "data/silver"
        assert paths["gold"] == "data/gold"

    def test_databricks_paths(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "14.0")
        cfg = make_cfg(is_databricks_flag=True)
        paths = get_paths(cfg)
        assert paths["silver"] == "dbfs:/mnt/silver"
        assert paths["gold"] == "dbfs:/mnt/gold"


# =========================================================
# 2 — build_conformite_dept()
# =========================================================

class TestBuildConformiteDept:
    def test_returns_expected_columns(self):
        df = make_silver_df([base_row()])
        result = build_conformite_dept(df)
        expected = {
            "annee", "code_departement", "nom_departement", "nom_region",
            "nb_analyses", "nb_conformes", "nb_non_conformes", "nb_inconnus",
            "taux_conformite_pct", "taux_non_conformite_pct",
        }
        assert expected.issubset(set(result.columns))

    def test_counts_analyses(self):
        df = make_silver_df([
            base_row(conformite_standard="conforme"),
            base_row(conformite_standard="non_conforme"),
            base_row(conformite_standard="inconnu"),
        ])
        result = build_conformite_dept(df)
        row = result.first()
        assert row["nb_analyses"] == 3
        assert row["nb_conformes"] == 1
        assert row["nb_non_conformes"] == 1
        assert row["nb_inconnus"] == 1

    def test_taux_conformite_pct(self):
        df = make_silver_df([
            base_row(conformite_standard="conforme"),
            base_row(conformite_standard="conforme"),
            base_row(conformite_standard="non_conforme"),
            base_row(conformite_standard="non_conforme"),
        ])
        result = build_conformite_dept(df)
        row = result.first()
        assert row["taux_conformite_pct"] == 50.0
        assert row["taux_non_conformite_pct"] == 50.0

    def test_groups_by_dept_and_annee(self):
        df = make_silver_df([
            base_row(annee=2024, code_departement="13"),
            base_row(annee=2025, code_departement="13"),
            base_row(annee=2024, code_departement="69", nom_departement="Rhône",
                     nom_region="Auvergne-Rhône-Alpes"),
        ])
        result = build_conformite_dept(df)
        assert result.count() == 3

    def test_all_conforme(self):
        df = make_silver_df([base_row(conformite_standard="conforme")] * 4)
        result = build_conformite_dept(df)
        row = result.first()
        assert row["taux_conformite_pct"] == 100.0
        assert row["nb_non_conformes"] == 0

    def test_all_non_conforme(self):
        df = make_silver_df([base_row(conformite_standard="non_conforme")] * 3)
        result = build_conformite_dept(df)
        row = result.first()
        assert row["taux_non_conformite_pct"] == 100.0
        assert row["nb_conformes"] == 0


# =========================================================
# 3 — build_parametres_risks()
# =========================================================

class TestBuildParametresRisks:
    def test_returns_expected_columns(self):
        df = make_silver_df([
            base_row(conformite_standard="non_conforme"),
        ])
        result = build_parametres_risks(df, top_n=10)
        expected = {
            "annee", "code_departement", "nom_departement",
            "code_parametre", "libelle_parametre",
            "categorie_parametre", "sous_categorie_parametre",
            "nb_non_conformes", "rank", "pct_non_conformes",
        }
        assert expected.issubset(set(result.columns))

    def test_only_non_conformes(self):
        df = make_silver_df([
            base_row(conformite_standard="conforme"),
            base_row(conformite_standard="non_conforme"),
        ])
        result = build_parametres_risks(df, top_n=10)
        for row in result.collect():
            assert row["nb_non_conformes"] > 0

    def test_top_n_respected(self):
        rows = []
        for i in range(1, 16):
            # Paramètre i a i occurrences → ranks distincts
            for _ in range(i):
                rows.append(base_row(
                    code_parametre=str(i),
                    libelle_parametre=f"Param {i}",
                    conformite_standard="non_conforme",
                ))
        df = make_silver_df(rows)
        result = build_parametres_risks(df, top_n=5)
        assert result.count() <= 5

    def test_rank_starts_at_one(self):
        df = make_silver_df([
            base_row(conformite_standard="non_conforme"),
        ])
        result = build_parametres_risks(df, top_n=10)
        ranks = [row["rank"] for row in result.collect()]
        assert 1 in ranks

    def test_pct_non_conformes_between_0_and_100(self):
        df = make_silver_df([
            base_row(conformite_standard="non_conforme"),
            base_row(conformite_standard="conforme"),
        ])
        result = build_parametres_risks(df, top_n=10)
        for row in result.collect():
            assert 0 <= row["pct_non_conformes"] <= 100

    def test_empty_result_when_no_non_conformes(self):
        df = make_silver_df([
            base_row(conformite_standard="conforme"),
        ])
        result = build_parametres_risks(df, top_n=10)
        assert result.count() == 0

    def test_groups_by_dept_and_annee(self):
        df = make_silver_df([
            base_row(annee=2024, code_departement="13",
                     conformite_standard="non_conforme"),
            base_row(annee=2024, code_departement="69",
                     nom_departement="Rhône",
                     nom_region="Auvergne-Rhône-Alpes",
                     conformite_standard="non_conforme"),
        ])
        result = build_parametres_risks(df, top_n=10)
        depts = [row["code_departement"] for row in result.collect()]
        assert "13" in depts
        assert "69" in depts


# =========================================================
# 4 — build_commune_stats()
# =========================================================

class TestBuildCommuneStats:
    def test_returns_expected_columns(self):
        df = make_silver_df([base_row()])
        result = build_commune_stats(df)
        expected = {
            "annee", "code_commune", "nom_commune",
            "code_departement", "nom_departement", "nom_region",
            "latitude", "longitude", "population",
            "nb_analyses", "nb_conformes", "nb_non_conformes",
            "nb_parametres_distincts", "taux_conformite_pct",
        }
        assert expected.issubset(set(result.columns))

    def test_counts_by_commune(self):
        df = make_silver_df([
            base_row(conformite_standard="conforme"),
            base_row(conformite_standard="non_conforme"),
        ])
        result = build_commune_stats(df)
        row = result.first()
        assert row["nb_analyses"] == 2
        assert row["nb_conformes"] == 1
        assert row["nb_non_conformes"] == 1

    def test_taux_conformite(self):
        df = make_silver_df([
            base_row(conformite_standard="conforme"),
            base_row(conformite_standard="conforme"),
            base_row(conformite_standard="conforme"),
            base_row(conformite_standard="non_conforme"),
        ])
        result = build_commune_stats(df)
        row = result.first()
        assert row["taux_conformite_pct"] == 75.0

    def test_counts_distinct_parametres(self):
        df = make_silver_df([
            base_row(code_parametre="1340", conformite_standard="conforme"),
            base_row(code_parametre="1302", conformite_standard="conforme"),
            base_row(code_parametre="1340", conformite_standard="non_conforme"),
        ])
        result = build_commune_stats(df)
        row = result.first()
        assert row["nb_parametres_distincts"] == 2

    def test_groups_by_commune_and_annee(self):
        df = make_silver_df([
            base_row(annee=2024, code_commune="13055"),
            base_row(annee=2025, code_commune="13055"),
            base_row(annee=2024, code_commune="69123",
                     nom_commune="Lyon",
                     code_departement="69",
                     nom_departement="Rhône",
                     nom_region="Auvergne-Rhône-Alpes"),
        ])
        result = build_commune_stats(df)
        assert result.count() == 3

    def test_preserves_geo_columns(self):
        df = make_silver_df([base_row(latitude=43.296, longitude=5.381)])
        result = build_commune_stats(df)
        row = result.first()
        assert row["latitude"] == 43.296
        assert row["longitude"] == 5.381


# =========================================================
# 5 — build_evolution_mensuelle()
# =========================================================

class TestBuildEvolutionMensuelle:
    def test_returns_expected_columns(self):
        df = make_silver_df([base_row()])
        result = build_evolution_mensuelle(df)
        expected = {
            "annee", "mois", "code_departement", "nom_departement",
            "nb_analyses", "nb_conformes",
            "taux_conformite_pct", "delta_taux_pct",
        }
        assert expected.issubset(set(result.columns))

    def test_taux_conformite_100_pct(self):
        df = make_silver_df([
            base_row(annee=2024, mois=1, conformite_standard="conforme"),
        ])
        result = build_evolution_mensuelle(df)
        row = result.filter(F.col("mois") == 1).first()
        assert row["taux_conformite_pct"] == 100.0

    def test_first_month_delta_is_null(self):
        df = make_silver_df([
            base_row(annee=2024, mois=1, conformite_standard="conforme"),
        ])
        result = build_evolution_mensuelle(df)
        row = result.filter(F.col("mois") == 1).first()
        assert row["delta_taux_pct"] is None

    def test_delta_computed_correctly(self):
        df = make_silver_df([
            base_row(annee=2024, mois=1, conformite_standard="conforme"),
            base_row(annee=2024, mois=1, conformite_standard="conforme"),
            base_row(annee=2024, mois=2, conformite_standard="conforme"),
            base_row(annee=2024, mois=2, conformite_standard="non_conforme"),
        ])
        result = build_evolution_mensuelle(df)
        row_m2 = result.filter(F.col("mois") == 2).first()
        assert row_m2["taux_conformite_pct"] == 50.0
        assert row_m2["delta_taux_pct"] == -50.0

    def test_groups_by_dept_and_month(self):
        df = make_silver_df([
            base_row(annee=2024, mois=1, code_departement="13"),
            base_row(annee=2024, mois=1, code_departement="69",
                     nom_departement="Rhône",
                     nom_region="Auvergne-Rhône-Alpes"),
            base_row(annee=2024, mois=2, code_departement="13"),
        ])
        result = build_evolution_mensuelle(df)
        assert result.count() == 3

    def test_no_reseaux_column_required(self):
        df = make_silver_df([base_row()])
        result = build_evolution_mensuelle(df)
        assert result.count() >= 1


# =========================================================
# 6 — write_gold()
# =========================================================

class TestWriteGold:
    TABLES_CFG = {
        "conformite_dept": {
            "output_table": "gold_conformite_dept",
            "partition_by": ["annee"],
        },
    }

    def test_writes_delta_locally(self, tmp_path):
        df = make_silver_df([base_row()])
        result_df = build_conformite_dept(df)
        out = write_gold(
            result_df,
            "conformite_dept",
            self.TABLES_CFG,
            str(tmp_path),
            is_databricks_env=False,
        )
        assert os.path.exists(out)
        assert os.path.exists(f"{out}/_delta_log")

    def test_returns_correct_path(self, tmp_path):
        df = make_silver_df([base_row()])
        result_df = build_conformite_dept(df)
        out = write_gold(
            result_df,
            "conformite_dept",
            self.TABLES_CFG,
            str(tmp_path),
            is_databricks_env=False,
        )
        assert out == str(tmp_path) + "/gold_conformite_dept"

    def test_written_data_readable(self, tmp_path):
        df = make_silver_df([
            base_row(conformite_standard="conforme"),
            base_row(conformite_standard="non_conforme"),
        ])
        result_df = build_conformite_dept(df)
        out = write_gold(
            result_df,
            "conformite_dept",
            self.TABLES_CFG,
            str(tmp_path),
            is_databricks_env=False,
        )
        reread = spark().read.format("delta").load(out)
        assert reread.count() == 1
        assert reread.first()["nb_analyses"] == 2

    def test_overwrite_mode(self, tmp_path):
        df = make_silver_df([base_row()])
        result_df = build_conformite_dept(df)
        write_gold(result_df, "conformite_dept",
                   self.TABLES_CFG, str(tmp_path), False)
        write_gold(result_df, "conformite_dept",
                   self.TABLES_CFG, str(tmp_path), False)
        out = str(tmp_path) + "/gold_conformite_dept"
        reread = spark().read.format("delta").load(out)
        assert reread.count() == 1
