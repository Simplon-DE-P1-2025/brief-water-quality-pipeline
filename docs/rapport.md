# Rapport Technique — Water Quality Pipeline

**Réalisé par** : Kaouter Rhazlani  
**Formation** : Simplon — P1 / Data Engineer  
**Date** : Mai 2026  
**Source** : [data.gouv.fr — SISE-Eaux](https://www.data.gouv.fr/datasets/resultats-du-controle-sanitaire-de-leau-distribuee-commune-par-commune/)  
**API** : [Hub'Eau — Qualité Eau Potable](https://hubeau.eaufrance.fr/page/api-qualite-eau-potable)  
**Stack** : PySpark · Delta Lake · Azure ADLS Gen2 · Databricks · GitHub Actions

---

## 1. Contexte et valeur métier

L'eau du robinet est l'aliment le plus contrôlé en France : plus de **300 000 prélèvements** et **12 millions d'analyses** par an, gérés dans la base nationale SISE-Eaux depuis 1994 par le Ministère des Solidarités et de la Santé.

Ce pipeline transforme ces données brutes en intelligence exploitable :

| Besoin | Réponse apportée |
|---|---|
| Suivre la conformité sanitaire par territoire | Table `gold_conformite_dept` agrégée par année x département |
| Identifier les paramètres à risque | Table `gold_parametres_risks` — top 10 non-conformités |
| Cartographier la qualité par commune | Table `gold_commune_stats` avec géocodage lat/lon |
| Détecter les tendances temporelles | Table `gold_evolution_mensuelle` avec delta mois/mois |

---

## 2. Source des données

### 2.1 Dataset data.gouv.fr

**Producteur** : Ministère des Solidarités et de la Santé  
**Licence** : Licence Ouverte / Open Licence 2.0  
**Mise à jour** : mensuelle (année en cours), annuelle (années antérieures)  
**Couverture** : prélèvements validés depuis le 01/01/2016  
**Dernière alimentation** : 23/04/2026 — Dernier prélèvement : 28/02/2026

Fichiers ZIP disponibles par année :

| Fichier | Contenu | Taille |
|---|---|---|
| `dis-2026.zip` | Année en cours | 61 Mo |
| `dis-2025.zip` | Année 2025 | 277 Mo |
| `dis-2024.zip` | Année 2024 | 275 Mo |
| `dis-2023.zip` | Année 2023 | 278 Mo |
| `dis-2022.zip` | Année 2022 | 294 Mo |

Chaque archive contient trois types de fichiers :
- **PLV** : prélèvements (code_prelevement, date, lieu, réseau)
- **RESULT** : résultats d'analyses (paramètre, valeur, conformité) — lien via `code_prelevement`
- **UDI_COM** : lien communes x unités de distribution — lien via `code_reseau`

### 2.2 API Hub'Eau

L'API Hub'Eau redistribue les mêmes données SISE-Eaux en JSON/CSV avec pagination.

**Base URL** : `https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable`

| Endpoint | Description |
|---|---|
| `GET /resultats_dis` | Prélèvements, analyses et conclusions sanitaires par commune/réseau |
| `GET /communes_udi` | Lien entre communes et unités de distribution (réseaux) |

**Paramètres de pagination** :

| Paramètre | Description | Limite |
|---|---|---|
| `page` | Numéro de page | — |
| `size` | Taille de page | max 20 000 |
| Profondeur | `page x size` | max 20 000 enregistrements |

### 2.3 Principaux critères de qualité surveillés

| Catégorie | Paramètres |
|---|---|
| Microbiologie | E. coli, Entérocoques, Bactéries coliformes, Bactéries aérobies revivifiables |
| Chimie | Nitrates (NO3), Nitrites (NO2), Pesticides, Aluminium, Plomb, Fluorures, pH, Conductivité |
| Radioactivité | Tritium, Dose totale indicative |

---

## 3. Architecture médaillon

```
data.gouv.fr (ZIP) / API Hub'Eau
        |
        v
+--------------+
|    BRONZE    |  Données brutes ingérées — partitionné par année
|  Delta Lake  |  Tables : water_quality, communes, departements, regions
+------+-------+
       |  Nettoyage · Typage · Dedup · Standardisation · Enrichissement
       v
+--------------+
|    SILVER    |  Données propres — partitionné par année x département
|  Delta Lake  |  1 table : water_quality (757 K+ lignes, 35 colonnes)
+------+-------+
       |  Agrégations métier
       v
+--------------+
|     GOLD     |  Tables analytiques prêtes à consommer
|  Delta Lake  |  4 tables agrégées
+------+-------+
       |  Validation qualité
       v
+--------------+
|  GREAT EXP.  |  5 suites — Silver + 4 tables Gold
+--------------+
```

### Stockage

| Environnement | Bronze | Silver | Gold |
|---|---|---|---|
| Local | `data/bronze/` | `data/silver/` | `data/gold/` |
| Databricks | `dbfs:/mnt/water-quality/bronze/` | `.../silver/` | `.../gold/` |

---

## 4. Ingestion Bronze

Les données sont ingérées depuis les fichiers ZIP annuels de data.gouv.fr.  
Quatre tables Delta sont produites en Bronze :

| Table | Source | Description |
|---|---|---|
| `water_quality` | Fichiers PLV + RESULT | Prélèvements et résultats d'analyses |
| `communes` | Référentiel géographique | Code INSEE, coordonnées, population |
| `departements` | Référentiel géographique | Code et libellé département |
| `regions` | Référentiel géographique | Code et libellé région |

Partitionnement : `annee_partition`

---

## 5. Transformations Silver

### 5.1 Nettoyage

- **Déduplication** sur clé métier `(code_prelevement, code_parametre, code_lieu_analyse)`
- **Filtres obligatoires** : `date_prelevement`, `code_commune`, `libelle_parametre` non nuls
- **Cast des types** : `annee_partition` -> `IntegerType`, `resultat_numerique` -> `DoubleType`, `date_prelevement` -> `DateType`
- **Dérivation** : colonnes `annee` et `mois` depuis `date_prelevement`
- **Suppression** colonnes techniques DLT (`_dlt_load_id`, `_dlt_id`)

### 5.2 Standardisation

- Extraction `code_reseau` / `nom_reseau` depuis le champ JSON `reseaux`
- Trim + upper sur les libellés texte
- `lpad` codes INSEE : commune (5 chiffres), département (2 chiffres)
- Renommage sémantique des colonnes conformité SISE-Eaux

### 5.3 Enrichissement

**Géographique** — jointure `communes -> regions` :
- Ajout `latitude`, `longitude`, `population`, `code_region`, `nom_region`
- Taux de jointure région mesuré et loggé à chaque exécution

**Catégorisation paramètres** (pilotée par `config.yaml`) :

| `code_type_parametre` | `categorie_parametre` |
|---|---|
| `N` | Physicochimique |
| `O` | Organoleptique |

**Conformité normalisée** :

| Valeur brute SISE-Eaux | `conformite_standard` | `est_conforme` |
|---|---|---|
| "...conforme aux exigences..." | `conforme` | `True` |
| "...non-conforme..." | `non_conforme` | `False` |
| "...conforme avec remarque..." | `conforme_avec_remarque` | `null` |
| Autre / vide | `inconnu` | `null` |

---

## 6. Tables Gold

### `gold_conformite_dept`
Agrégation par `(annee, code_departement)` — taux de conformité, nb analyses, nb non-conformes.

### `gold_parametres_risks`
Top 10 paramètres non conformes par département et année via window function `RANK()`.

### `gold_commune_stats`
Stats qualité par commune avec géolocalisation complète, exploitable pour cartographie.

### `gold_evolution_mensuelle`
Évolution mensuelle par département avec indicateur de tendance calculé par `LAG()`.

---

## 7. Qualité des données — Great Expectations

**Version** : Great Expectations 1.x — mode éphémère (`mode="ephemeral"`)

| Suite | Table validée | Expectations |
|---|---|---|
| `silver_water_quality` | Silver `water_quality` | 15 |
| `gold_conformite_dept` | Gold `gold_conformite_dept` | 12 |
| `gold_parametres_risks` | Gold `gold_parametres_risks` | 10 |
| `gold_commune_stats` | Gold `gold_commune_stats` | 12 |
| `gold_evolution_mensuelle` | Gold `gold_evolution_mensuelle` | 10 |

---

## 8. CI/CD

### CI — Intégration continue (en place)

```
push / PR  ->  flake8 (max-line-length=120)  ->  pytest  ->  pass / fail
```

### CD — Déploiement continu (non mis en place)

Le déploiement continu vers Databricks n'a pas été implémenté en raison des blocages d'accès Azure décrits en section 13. Le code est structuré pour supporter ce déploiement dès que l'accès est rétabli.

---

## 9. Qualité du code

| Fichier | Scope | Tests |
|---|---|---|
| `tests/test_bronze.py` | Ingestion, schéma, partitionnement | ~20 |
| `tests/test_silver.py` | clean, standardize, enrich_*, select_output | 60 / 60 |
| `tests/test_gold.py` | build_*, write_gold, config, chemins | ~40 |

---

## 10. Compatibilité Local / Databricks

Le guard `_NOTEBOOK_RUN` permet d'exécuter chaque script dans quatre contextes sans modification :

| Mode | Déclencheur |
|---|---|
| Notebook Databricks | `DATABRICKS_RUNTIME_VERSION` dans env |
| Jupyter local | `ipykernel` dans `sys.modules` |
| Script standalone | `__name__ == "__main__"` |
| Import (tests) | Aucun des cas -> pas d'exécution Spark |

---

## 11. Structure du projet

```
brief-water-quality-pipeline/
├── config/
│   └── config.yaml
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── notebooks/
│   ├── bronze/bronze_ingest.py
│   ├── silver/silver_transform.py
│   ├── gold/gold_transform.py
│   └── quality/data_quality_check.py
├── tests/
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
├── docs/rapport.md
├── .github/workflows/ci.yml
├── pyproject.toml
└── README.md
```

---

## 12. Données clés

| Métrique | Valeur |
|---|---|
| Source | Ministère des Solidarités et de la Santé / Hub'Eau |
| Période couverte | 2016 -> 2026 |
| Lignes Bronze `water_quality` | ~757 000+ |
| Lignes Silver après nettoyage | ~756 786 |
| Colonnes Silver finales | 35 |
| Tables Gold produites | 4 |
| Suites Great Expectations | 5 (Silver + 4 Gold) |
| Licence source | Licence Ouverte / Open Licence 2.0 |

---

## 13. Points de blocage rencontrés

### 13.1 Permissions insuffisantes — création du workspace Databricks

La création d'un workspace Databricks sur Azure nécessite le rôle **Contributor** sur la subscription ou le Resource Group cible. Ce niveau d'accès n'était pas disponible sur la subscription de formation.

**Impact** : impossible de provisionner le workspace Databricks directement depuis le portail Azure.  
**Contournement** : pipeline développé et validé en local avec PySpark standalone + Delta Lake.

### 13.2 Quotas de cores insuffisants — création des clusters

Après obtention d'un accès partiel au workspace, la création de clusters a été bloquée par les quotas de la subscription Azure (cores insuffisants, y compris en configuration single node).

**Impact** : aucun cluster Databricks n'a pu être démarré.  
**Contournement** : exécution complète en local (`uv run python`), mode `local[*]`.

### 13.3 Absence de droits RBAC — role assignment sur le storage ADLS Gen2

Le compte de formation ne dispose pas du rôle **Owner** ou **User Access Administrator** sur la subscription Azure, ce qui empêche d'assigner le rôle `Storage Blob Data Contributor` à la Managed Identity du Databricks Access Connector.

**Ressources concernées** :
- Access Connector : `unity-catalog-access-connector` (principalId : `04466fd6-bcb0-478c-b257-89823dd8300f`)
- Storage account : `sawaterquality` (Resource Group : `krhazlaniRG`)

**Erreur obtenue** lors de toute tentative d'écriture Spark vers ADLS Gen2 :
```
PERMISSION_DENIED: Request for user delegation key is not authorized. SQLSTATE: 42501
```

Cette erreur se manifeste sur toutes les tentatives d'accès via `abfss://` — que ce soit depuis `spark.conf.set`, `saveAsTable` Unity Catalog, ou l'External Location déjà configurée dans le workspace.

**Commande à exécuter par un administrateur pour débloquer** :
```bash
az role assignment create \
  --assignee "04466fd6-bcb0-478c-b257-89823dd8300f" \
  --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/f3ca738a-c0a4-459a-a3b6-f9e9bb4cfd2a/resourceGroups/krhazlaniRG/providers/Microsoft.Storage/storageAccounts/sawaterquality"
```

**Contournement mis en place** : écriture via l'**Azure Storage SDK** (`azure-storage-blob`) avec authentification SAS token. Ce contournement permet :
- L'écriture de fichiers CSV vers `bronze/` via `BlobServiceClient`
- L'écriture de fichiers Delta via écriture locale sur `dbfs:/tmp/` puis upload fichier par fichier

```python
# Pattern de contournement validé
from azure.storage.blob import BlobServiceClient
sas_token = dbutils.secrets.get(scope="waterquality", key="sas-token")
client = BlobServiceClient(account_url="https://sawaterquality.blob.core.windows.net", credential=sas_token)
```

### 13.4 Unity Catalog inaccessible en écriture Spark — compute serverless

Sur serverless compute avec Unity Catalog activé, les mécanismes habituels de configuration du storage sont tous bloqués :

| Méthode | Erreur |
|---|---|
| `spark.conf.set(fs.azure.sas.*)` | `CONFIG_NOT_AVAILABLE` — bloqué par UC |
| `sc._jsc.hadoopConfiguration()` | `NotImplementedError` — non supporté serverless |
| `dbutils.fs.mount()` | `Method not whitelisted` — désactivé UC |
| `saveAsTable` Unity Catalog | `PERMISSION_DENIED` — même blocage RBAC |
| `CREATE TABLE ... LOCATION 'dbfs:/...'` | `UC_FILE_SCHEME_FOR_TABLE_CREATION_NOT_SUPPORTED` |
| `CREATE CATALOG` sans location | `INVALID_STATE` — metastore storage root absent |

**Contournement** : enregistrement des tables dans Unity Catalog en attente du role assignment RBAC. En production, dès que le rôle est assigné, l'écriture Delta via Spark est rétablie et `saveAsTable("qualitywater.bronze.water_quality")` devient fonctionnel.

### 13.5 Databricks CLI — contraintes Cloud Shell

L'installation du Databricks CLI dans Azure Cloud Shell a nécessité plusieurs contournements :
- `sudo` bloqué (flag `no new privileges`)
- Script d'installation ignorant la variable `DATABRICKS_INSTALL_DIR`
- URL de release incorrecte pour le binaire

**Solution finale** : téléchargement direct du binaire depuis GitHub Releases vers `~/bin/` :
```bash
curl -fsSL https://github.com/databricks/cli/releases/download/v0.299.1/databricks_cli_0.299.1_linux_amd64.zip -o /tmp/databricks.zip
unzip /tmp/databricks.zip -d /tmp/databricks_extract
cp /tmp/databricks_extract/databricks ~/bin/
export PATH="$HOME/bin:$PATH"
```

Note : cette installation est éphémère (session Cloud Shell non persistante) et doit être répétée à chaque session.

---

## 14. Limites et améliorations

| Limite actuelle | Amélioration envisagée |
|---|---|
| Écriture cloud via SDK pandas uniquement | Role assignment RBAC -> écriture Spark native |
| Tables non enregistrées dans Unity Catalog | `saveAsTable` dès role assignment accordé |
| CD non mis en place | Déploiement Databricks via API dès accès rétabli |
| Quotas Azure insuffisants | Demande d'augmentation ou changement de subscription |
| Pagination API Hub'Eau limitée à 20 000 | Boucle sur `next` pour ingestion complète |
| Orchestration manuelle | Databricks Workflows à configurer |
| API données non exposée | Databricks SQL Endpoint ou FastAPI |
| Semantic Release non configuré | `.releaserc.json` + workflow GitHub Actions |