# Databricks notebook source
# %% [markdown]
# # Data Quality — Water Quality Pipeline
#
# Validates Silver and Gold layers using Great Expectations 1.x.
# Runs locally with Pandas (Parquet reads).
#
# Layers validated :
#   - Silver  : water_quality
#   - Gold    : conformite_dept, parametres_risks, commune_stats, evolution_mensuelle

# %% [markdown]
# ## 0 — Imports

# %%
# COMMAND ----------

import yaml

import pandas as pd
import great_expectations as gx
import pyarrow.dataset as ds

# %% [markdown]
# ## 1 — Config

# %%
# COMMAND ----------


def load_config(path: str = "config/config.yaml") -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


cfg = load_config()

SILVER_PATH = cfg["silver"]["paths"]["local"]["silver"]
GOLD_PATH = cfg["gold"]["paths"]["local"]["gold"]

print(f"Silver : {SILVER_PATH}")
print(f"Gold   : {GOLD_PATH}")

# %% [markdown]
# ## 2 — Helpers

# %%
# COMMAND ----------


def read_delta(path: str) -> pd.DataFrame:
    """Reads a partitioned Delta Lake table preserving partition columns."""
    dataset = ds.dataset(path, format="parquet", partitioning="hive")
    df = dataset.to_table().to_pandas()
    print(f"  Loaded {len(df):,} rows from {path}")
    return df


def run_validation(context, df: pd.DataFrame, suite_name: str) -> dict:
    """
    Runs a named ExpectationSuite against a Pandas DataFrame.
    Prints a summary and returns the validation result.
    """
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
        f"  Expectations : {total} total | {passed} passed | {total - passed} failed")

    if not result["success"]:
        for r in result["results"]:
            if not r["success"]:
                exp = r["expectation_config"]["type"]
                col = r["expectation_config"]["kwargs"].get("column", "")
                obs = r["result"].get("observed_value", "")
                print(f"  FAIL  {exp}  col={col}  observed={obs}")

    return result


# %% [markdown]
# ## 3 — Context GE (unique)

# %%
# COMMAND ----------

context = gx.get_context(mode="ephemeral")

# %% [markdown]
# ## 4 — Suites

# %%
# COMMAND ----------


def add_suite_silver(ctx) -> None:
    """Expectation suite for Silver water_quality table."""
    suite = ctx.suites.add(gx.ExpectationSuite(name="silver_water_quality"))

    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=100000))

    for col in ["code_prelevement", "date_prelevement", "annee", "mois",
                "code_commune", "code_departement", "libelle_parametre",
                "conformite_standard"]:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column=col))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="annee", min_value=2016, max_value=2026
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="mois", min_value=1, max_value=12
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValueLengthsToEqual(
        column="code_commune", value=5
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValueLengthsToEqual(
        column="code_departement", value=2
    ))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="conformite_standard",
            value_set=[
                "conforme",
                "non_conforme",
                "conforme_avec_remarque",
                "inconnu"],
        ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="categorie_parametre",
        value_set=["Microbiologique", "Physico-chimique", "Radiologique",
                   "Physicochimique", "Organoleptique", "Autre"],
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="resultat_numerique", min_value=0, mostly=0.95
    ))


def add_suite_conformite_dept(ctx) -> None:
    """Expectation suite for gold_conformite_dept."""
    suite = ctx.suites.add(gx.ExpectationSuite(name="gold_conformite_dept"))

    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=0))

    for col in ["annee", "code_departement", "nom_departement",
                "nb_analyses", "nb_conformes", "taux_conformite_pct"]:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column=col))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="annee", min_value=2016, max_value=2026
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="nb_analyses", min_value=1
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="nb_conformes", min_value=0
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="taux_conformite_pct", min_value=0, max_value=100
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="taux_non_conformite_pct", min_value=0, max_value=100
    ))
    suite.add_expectation(
        gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="nb_analyses",
            column_B="nb_conformes",
            or_equal=True))


def add_suite_parametres_risks(ctx) -> None:
    """Expectation suite for gold_parametres_risks."""
    suite = ctx.suites.add(gx.ExpectationSuite(name="gold_parametres_risks"))

    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=0))

    for col in ["annee", "code_departement", "code_parametre",
                "libelle_parametre", "nb_non_conformes", "rank"]:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column=col))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="rank", min_value=1, max_value=10
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="nb_non_conformes", min_value=1
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="pct_non_conformes", min_value=0, max_value=100
    ))


def add_suite_commune_stats(ctx) -> None:
    """Expectation suite for gold_commune_stats."""
    suite = ctx.suites.add(gx.ExpectationSuite(name="gold_commune_stats"))

    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=0))

    for col in ["annee", "code_commune", "nom_commune", "code_departement",
                "nb_analyses", "taux_conformite_pct"]:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column=col))

    suite.add_expectation(gx.expectations.ExpectColumnValueLengthsToEqual(
        column="code_commune", value=5
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="taux_conformite_pct", min_value=0, max_value=100
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="nb_analyses", min_value=1
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="latitude", min_value=-90, max_value=90, mostly=0.95
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="longitude", min_value=-180, max_value=180, mostly=0.95
    ))


def add_suite_evolution_mensuelle(ctx) -> None:
    """Expectation suite for gold_evolution_mensuelle."""
    suite = ctx.suites.add(
        gx.ExpectationSuite(
            name="gold_evolution_mensuelle"))

    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=0))

    for col in ["annee", "mois", "code_departement",
                "nb_analyses", "taux_conformite_pct"]:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column=col))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="mois", min_value=1, max_value=12
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="taux_conformite_pct", min_value=0, max_value=100
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="nb_analyses", min_value=1
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="annee", min_value=2016, max_value=2026
    ))


# %%
# COMMAND ----------

add_suite_silver(context)
add_suite_conformite_dept(context)
add_suite_parametres_risks(context)
add_suite_commune_stats(context)
add_suite_evolution_mensuelle(context)

# %% [markdown]
# ## 5 — Validation

# %%
# COMMAND ----------

print("=" * 60)
print("SILVER VALIDATION")
print("=" * 60)

df_silver = read_delta(f"{SILVER_PATH}/water_quality")
result_silver = run_validation(context, df_silver, "silver_water_quality")

# %%
# COMMAND ----------

print("\n" + "=" * 60)
print("GOLD VALIDATION")
print("=" * 60)

df_conformite = read_delta(f"{GOLD_PATH}/gold_conformite_dept")
result_conformite = run_validation(
    context, df_conformite, "gold_conformite_dept")

df_risks = read_delta(f"{GOLD_PATH}/gold_parametres_risks")
result_risks = run_validation(context, df_risks, "gold_parametres_risks")

df_communes = read_delta(f"{GOLD_PATH}/gold_commune_stats")
result_communes = run_validation(context, df_communes, "gold_commune_stats")

df_evolution = read_delta(f"{GOLD_PATH}/gold_evolution_mensuelle")
result_evolution = run_validation(
    context, df_evolution, "gold_evolution_mensuelle")

# %% [markdown]
# ## 6 — Rapport final

# %%
# COMMAND ----------


def print_report(results: dict) -> None:
    """Prints a consolidated quality report for all validated tables."""
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


# %%
# COMMAND ----------

results = {
    "silver_water_quality": result_silver,
    "gold_conformite_dept": result_conformite,
    "gold_parametres_risks": result_risks,
    "gold_commune_stats": result_communes,
    "gold_evolution_mensuelle": result_evolution,
}

print_report(results)

# COMMAND ----------
