# Water Quality Pipeline

Pipeline de données médaillon sur le contrôle sanitaire de l'eau potable française.

**Réalisé par** : Kaouter Rhazlani  
**Formation** : Simplon — P1 / Data Engineer  
**Date** : Mai 2026  
**Source** : [SISE-Eaux — data.gouv.fr](https://www.data.gouv.fr/datasets/resultats-du-controle-sanitaire-de-leau-distribuee-commune-par-commune/)  
**API** : [Hub'Eau — Qualité Eau Potable](https://hubeau.eaufrance.fr/page/api-qualite-eau-potable)  
**Stack** : PySpark · Delta Lake · Databricks · Azure ADLS Gen2 · GitHub Actions

![CI](https://github.com/kaouterrhazlani/brief-water-quality-pipeline/actions/workflows/ci.yml/badge.svg)

---

## Architecture

```
data.gouv.fr (ZIP annuels) / API Hub'Eau
      |
      v
  BRONZE          Données brutes                  partitionné par année
      |
      v
  SILVER          Nettoyées + enrichies           partitionné par année x département
      |
      v
   GOLD           4 tables analytiques            prêtes à consommer
      |
      v
  QUALITY         Great Expectations              5 suites Silver + Gold
```

## Source des données

Les données proviennent de la base nationale **SISE-Eaux** du Ministère des Solidarités et de la Santé, redistribuées via :

- **data.gouv.fr** — fichiers ZIP annuels (`dis-2024.zip`, `dis-2025.zip`...)
- **API Hub'Eau** — endpoint `GET /v1/qualite_eau_potable/resultats_dis` (JSON/CSV, pagination 20 000 max)

Couverture : prélèvements validés depuis 2016 — mise à jour mensuelle.  
Licence : Licence Ouverte / Open Licence 2.0.

## Setup

```bash
# Installer les dépendances
uv sync

# Lancer les tests
uv run pytest tests/ -v

# Lint
uv run flake8 notebooks/ tests/ --max-line-length=120

# Exécuter le pipeline
uv run python notebooks/bronze/bronze_ingest.py
uv run python notebooks/silver/silver_transform.py
uv run python notebooks/gold/gold_transform.py

# Validation qualité des données
uv run python notebooks/quality/data_quality_check.py
```

## Configuration

Tous les paramètres (chemins, déduplication, catégories, colonnes) sont dans [`config/config.yaml`](config/config.yaml).  
Les chemins locaux et Databricks (`dbfs:/mnt/...`) sont séparés.

## Tables Gold

| Table | Description |
|---|---|
| `gold_conformite_dept` | Taux de conformité par département et année |
| `gold_parametres_risks` | Top 10 paramètres non conformes par département |
| `gold_commune_stats` | Stats qualité et géolocalisation par commune |
| `gold_evolution_mensuelle` | Evolution mensuelle avec variation mois/mois |

## Qualité des données

Validation via **Great Expectations 1.x** — `notebooks/quality/data_quality_check.py`

| Suite | Table | Règles |
|---|---|---|
| `silver_water_quality` | Silver water_quality | Non-nullité, plages, longueurs codes INSEE, conformité standard |
| `gold_conformite_dept` | Gold conformite_dept | Non-nullité, plages, cohérence `nb_analyses >= nb_conformes` |
| `gold_parametres_risks` | Gold parametres_risks | Non-nullité, rank 1-10, pct 0-100 |
| `gold_commune_stats` | Gold commune_stats | Non-nullité, coordonnées GPS, codes INSEE |
| `gold_evolution_mensuelle` | Gold evolution_mensuelle | Non-nullité, mois 1-12, taux 0-100 |

## Tests

```
tests/test_bronze.py      ingestion, schéma
tests/test_silver.py      60 tests — clean, standardize, enrich, conformité
tests/test_gold.py        build_*, write_gold, configuration
```

---

## Points de blocage Azure

L'intégration cloud complète a été confrontée à plusieurs niveaux de restrictions sur la subscription Azure de formation. Voici le détail des blocages rencontrés et les contournements mis en place.

### 1. Permissions insuffisantes — création du workspace Databricks

Le rôle **Contributor** sur la subscription ou le Resource Group n'était pas disponible, bloquant la création directe du workspace Databricks depuis le portail Azure.

**Contournement** : workspace provisionné par l'équipe de formation. Le pipeline a été développé et validé en environnement local avec PySpark standalone + Delta Lake.

### 2. Quotas de cores insuffisants — démarrage des clusters

Après accès au workspace, la création de clusters a été bloquée par les quotas de la subscription Azure (cores insuffisants, y compris en configuration single node).

**Contournement** : exécution complète du pipeline en local (`uv run python`), mode `local[*]`. Les 60 tests unitaires Silver et les validations Great Expectations garantissent la conformité du code.

### 3. Permissions RBAC insuffisantes — role assignment sur le storage

Le compte de formation `krhazlani.ext@simplonformations.co` ne dispose pas du rôle **Owner** ou **User Access Administrator** sur la subscription, ce qui empêche d'assigner le rôle `Storage Blob Data Contributor` à la Managed Identity du Databricks Access Connector (`unity-catalog-access-connector`, principalId : `04466fd6-bcb0-478c-b257-89823dd8300f`).

Conséquence directe : toute écriture Spark vers `abfss://bronze@sawaterquality.dfs.core.windows.net/` échoue avec l'erreur :
```
PERMISSION_DENIED: Request for user delegation key is not authorized.
```

La commande à exécuter par un administrateur de la subscription pour débloquer cette situation est :
```bash
az role assignment create \
  --assignee "04466fd6-bcb0-478c-b257-89823dd8300f" \
  --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/f3ca738a-c0a4-459a-a3b6-f9e9bb4cfd2a/resourceGroups/krhazlaniRG/providers/Microsoft.Storage/storageAccounts/sawaterquality"
```

**Contournement mis en place** : écriture via l'**Azure Storage SDK** (`azure-storage-blob`) avec authentification SAS token, en mode pandas/bytes. Ce contournement permet d'écrire des fichiers CSV et des fichiers Delta (via écriture locale sur `dbfs:/tmp/` puis upload des fichiers Delta générés) dans le container `bronze`.

### 4. Unity Catalog inaccessible en écriture Spark

Sur serverless compute avec Unity Catalog activé :
- `spark.conf.set` pour les credentials de storage est bloqué (`CONFIG_NOT_AVAILABLE`)
- `sc._jsc.hadoopConfiguration()` n'est pas supporté sur serverless (`NotImplementedError`)
- `dbutils.fs.mount()` est désactivé (`Method not whitelisted`)
- `saveAsTable` vers un catalog Unity Catalog échoue (`PERMISSION_DENIED`) car le catalog `qualitywater` pointe vers le même storage non autorisé

**Contournement mis en place** : le pipeline cloud utilise l'Azure Storage SDK pour toutes les écritures sur ADLS Gen2. L'enregistrement des tables dans Unity Catalog reste en attente du role assignment RBAC.

### 5. Déploiement continu (CD) non mis en place

La mise en place du CD vers Databricks est directement conditionnée par la résolution des points 2 et 3. Sans cluster disponible et sans accès Spark au storage, il n'était pas possible de configurer ni tester un pipeline de déploiement automatisé.

**Statut** : en attente de rétablissement des accès Azure.

---

## Documentation

[docs/rapport.md](docs/rapport.md)