"""
Tests for Bronze ingestion pipeline.
Tests cover config loading, department generation, scope resolution,
and record preparation — without making real API calls.
"""

from notebooks.bronze.ingestion_hubeau import (
    get_departments,
    resolve_scope,
    prepare_record,
    build_params,
)
import json

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# =========================================================
# get_departments
# =========================================================

def test_get_departments_count():
    depts = get_departments()
    assert len(depts) == 101


def test_get_departments_contains_corsica():
    depts = get_departments()
    assert "2A" in depts
    assert "2B" in depts


def test_get_departments_contains_dom():
    depts = get_departments()
    for code in ["971", "972", "973", "974", "976"]:
        assert code in depts


def test_get_departments_format():
    depts = get_departments()
    # Metropolitan departments should be zero-padded
    assert "01" in depts
    assert "09" in depts
    assert "95" in depts
    # Should not contain unpadded single digits
    assert "1" not in depts
    assert "9" not in depts


# =========================================================
# resolve_scope
# =========================================================

def make_cfg(mode="dev", departments=None):
    """Helper to build a minimal config dict."""
    return {
        "environment": {
            "is_databricks": False,
            "active_mode": mode,
            "mode": {
                "dev": {
                    "years": [2024, 2025],
                    "departments": departments or ["13", "69"],
                    "max_workers": 5,
                },
                "prod": {
                    "years": list(range(2016, 2027)),
                    "departments": "DEFAULT_FRANCE",
                    "max_workers": 20,
                },
            },
        },
        "storage": {
            "bronze": {
                "local": "data",
                "databricks": "dbfs:/mnt/bronze",
            }
        },
        "pipelines": {
            "hubeau": {
                "api_url": "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/resultats_dis",
                "pipeline_name": "hubeau_bronze",
                "dataset_name": "bronze",
                "file_format": "delta",
                "max_depth": 20000,
                "max_retries": 3,
                "pagination": {"sleep_between_requests": 0.3},
                "date_format": {
                    "force_end_of_day": True,
                    "end_of_day_suffix": "T23:59:59Z",
                },
                "primary_key": ["code_commune", "libelle_parametre", "date_prelevement"],
            }
        },
    }


def test_resolve_scope_dev():
    cfg = make_cfg(mode="dev")
    scope = resolve_scope(cfg)
    assert scope["active_mode"] == "dev"
    assert scope["years"] == [2024, 2025]
    assert scope["departments"] == ["13", "69"]
    assert scope["max_workers"] == 5


def test_resolve_scope_prod_default_france():
    cfg = make_cfg(mode="prod")
    scope = resolve_scope(cfg)
    assert scope["active_mode"] == "prod"
    assert scope["max_workers"] == 20
    assert len(scope["departments"]) == 101
    assert "2A" in scope["departments"]
    assert "971" in scope["departments"]


def test_resolve_scope_prod_custom_departments():
    cfg = make_cfg(mode="prod")
    cfg["environment"]["mode"]["prod"]["departments"] = ["75", "69"]
    scope = resolve_scope(cfg)
    assert scope["departments"] == ["75", "69"]


# =========================================================
# prepare_record
# =========================================================

def test_prepare_record_adds_partition():
    rec = {
        "date_prelevement": "2024-06-15T10:00:00Z",
        "libelle_parametre": "pH"}
    result = prepare_record(rec, 2024)
    assert result["annee_partition"] == 2024


def test_prepare_record_partition_fallback():
    rec = {"date_prelevement": "", "libelle_parametre": "pH"}
    result = prepare_record(rec, 2025)
    assert result["annee_partition"] == 2025


def test_prepare_record_serializes_list():
    rec = {"reseaux": [{"code": "075000221", "nom": "CENTRE"}],
           "date_prelevement": "2024-01-01T00:00:00Z"}
    result = prepare_record(rec, 2024)
    assert isinstance(result["reseaux"], str)
    parsed = json.loads(result["reseaux"])
    assert parsed[0]["code"] == "075000221"


def test_prepare_record_serializes_dict():
    rec = {"meta": {"key": "value"}, "date_prelevement": "2024-01-01T00:00:00Z"}
    result = prepare_record(rec, 2024)
    assert isinstance(result["meta"], str)


def test_prepare_record_keeps_strings():
    rec = {"libelle_parametre": "Nitrates",
           "date_prelevement": "2024-01-01T00:00:00Z"}
    result = prepare_record(rec, 2024)
    assert result["libelle_parametre"] == "Nitrates"


# =========================================================
# build_params
# =========================================================

def test_build_params_appends_end_of_day():
    """date_max without time should get T23:59:59Z appended."""
    # Temporarily patch globals
    import notebooks.bronze.ingestion_hubeau as m
    original_force = m.FORCE_EOD
    original_end = m.END_DAY
    m.FORCE_EOD = True
    m.END_DAY = "T23:59:59Z"

    params = build_params("13", "2024-01-01", "2024-01-31")
    assert params["date_max_prelevement"] == "2024-01-31T23:59:59Z"

    m.FORCE_EOD = original_force
    m.END_DAY = original_end


def test_build_params_does_not_double_append():
    """date_max already containing T should not be modified."""
    import notebooks.bronze.ingestion_hubeau as m
    original_force = m.FORCE_EOD
    original_end = m.END_DAY
    m.FORCE_EOD = True
    m.END_DAY = "T23:59:59Z"

    params = build_params("13", "2024-01-01", "2024-01-31T23:59:59Z")
    assert params["date_max_prelevement"] == "2024-01-31T23:59:59Z"

    m.FORCE_EOD = original_force
    m.END_DAY = original_end


def test_build_params_structure():
    import notebooks.bronze.ingestion_hubeau as m
    params = build_params("75", "2024-06-01", "2024-06-30T23:59:59Z")
    assert params["code_departement"] == "75"
    assert params["date_min_prelevement"] == "2024-06-01"
    assert params["size"] == m.MAX_DEPTH
    assert params["page"] == 1
