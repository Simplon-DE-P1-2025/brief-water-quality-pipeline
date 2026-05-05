# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze - Ingestion via Hub'Eau API
# MAGIC
# MAGIC Raw ingestion only - no deduplication, no transformation.
# MAGIC Deduplication and cleaning are handled in Silver layer.
# MAGIC
# MAGIC Parallel ingestion via ThreadPoolExecutor.
# MAGIC 4-level pagination: year -> quarter -> month -> week
# MAGIC
# MAGIC Fix: date_max uses T23:59:59Z to capture all records on the last day
# MAGIC      (API interprets date-only as T00:00:00Z, missing after-midnight records)
# MAGIC
# MAGIC DLT: Delta Live Tables blocks are commented out.
# MAGIC      Uncomment when running on Databricks with DLT pipelines.

# COMMAND ----------

import os
import calendar
import time
import threading
import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, year, to_timestamp
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from collections import defaultdict

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

if not IS_DATABRICKS:
    from delta import configure_spark_with_delta_pip

# ---------------------------------------------------------------------------
# DLT — Uncomment when using Delta Live Tables on Databricks
# DLT handles Delta writes, schema evolution and pipeline orchestration
# automatically — no need for write_to_delta() calls.
# ---------------------------------------------------------------------------
# import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters - Edit here

# COMMAND ----------

# ---------------------------------------------------------------------------
# DEV — Local (3 departments, 3 years)
# ---------------------------------------------------------------------------
YEARS       = [2024, 2025, 2026]
DEPARTMENTS = ["13", "69", "75"]
MAX_WORKERS = 5

# ---------------------------------------------------------------------------
# PROD — Databricks only
# Just change the parameters below — no other configuration needed.
# MAX_WORKERS uses Python threading for API requests, no Spark config required.
# The Databricks cluster handles Delta writes elastically on its own workers.
# ---------------------------------------------------------------------------
# YEARS = list(range(2016, 2027))
# DEPARTMENTS = (
#     [str(i).zfill(2) for i in range(1, 96)]
#     + ["2A", "2B"]
#     + ["971", "972", "973", "974", "976"]
# )
# MAX_WORKERS = 20

# ---------------------------------------------------------------------------
# Common parameters
# ---------------------------------------------------------------------------

BRONZE_PATH = "dbfs:/data/bronze/water_quality" if IS_DATABRICKS else "data/bronze/water_quality"

API_URL    = "https://hubeau.eaufrance.fr/api/v1/qualite_eau_potable/resultats_dis"
API_FIELDS = (
    "code_commune,nom_commune,code_departement,"
    "libelle_parametre,resultat_numerique,resultat_alphanumerique,"
    "unite_mesure,limite_qualite_parametre,reference_qualite_parametre,"
    "conclusion_conformite_parametre,date_prelevement,"
    "coordonnee_x,coordonnee_y"
)
MAX_DEPTH   = 20000
MAX_RETRIES = 3
BATCH_SIZE  = 30000

print(f"Environment  : {'Databricks' if IS_DATABRICKS else 'Local'}")
print(f"Mode         : {'PROD' if len(DEPARTMENTS) > 10 else 'DEV'}")
print(f"Years        : {YEARS}")
print(f"Departments  : {DEPARTMENTS}")
print(f"Workers      : {MAX_WORKERS}")
print(f"Bronze path  : {BRONZE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark Session

# COMMAND ----------

if IS_DATABRICKS:
    spark = SparkSession.builder.getOrCreate()
else:
    spark = (
        configure_spark_with_delta_pip(
            SparkSession.builder
            .appName("bronze-hubeau-api")
            .master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.adaptive.enabled", "true")
        )
        .getOrCreate()
    )

spark.sparkContext.setLogLevel("ERROR")
write_lock  = threading.Lock()
debug_lock  = threading.Lock()
debug_stats = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(int))))

print(f"Spark {spark.version} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

def build_params(dept, date_min, date_max):
    """
    Builds API query parameters.
    date_max must include T23:59:59Z to capture all records on the last day.
    The API interprets date-only as T00:00:00Z, missing after-midnight records.
    """
    if "T" not in str(date_max):
        date_max = f"{date_max}T23:59:59Z"
    return {
        "code_departement"    : dept,
        "date_min_prelevement": date_min,
        "date_max_prelevement": date_max,
        "size"                : MAX_DEPTH,
        "page"                : 1,
        "fields"              : API_FIELDS,
    }


def fetch_page(params):
    """Calls Hub'Eau API with exponential retry on timeout."""
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(API_URL, params=params, timeout=60)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.ReadTimeout:
            wait = 5 * (2 ** attempt)
            print(f"  Timeout (attempt {attempt + 1}/{MAX_RETRIES}) -- waiting {wait}s...")
            time.sleep(wait)
            if attempt == MAX_RETRIES - 1:
                raise
    return {}


def write_to_delta(records):
    """
    Writes raw records to Delta Lake Bronze.
    No transformation, no deduplication — raw data as-is from API.
    Thread-safe via write_lock.

    ---------------------------------------------------------------------------
    DLT alternative — Uncomment when using Delta Live Tables on Databricks.
    Replace all write_to_delta() calls with a single @dlt.table decorator.
    DLT handles writes, schema evolution and pipeline orchestration automatically.

    @dlt.table(
        name="bronze_water_quality",
        comment="Raw water quality data from Hub'Eau API. No dedup, no transform.",
        partition_cols=["annee_partition"],
        table_properties={"quality": "bronze"}
    )
    def bronze_water_quality():
        records = run_ingestion()   # call the ingestion pipeline
        pdf     = pd.DataFrame(records)
        df      = spark.createDataFrame(pdf)
        return (
            df.withColumn("ingestion_timestamp", current_timestamp())
              .withColumn("source", lit("hubeau_api"))
              .withColumn(
                  "annee_partition",
                  year(to_timestamp("date_prelevement", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
              )
        )
    ---------------------------------------------------------------------------
    """
    if not records:
        return
    with write_lock:
        pdf = pd.DataFrame(records)
        df  = spark.createDataFrame(pdf)
        df  = (
            df.withColumn("ingestion_timestamp", current_timestamp())
              .withColumn("source", lit("hubeau_api"))
              .withColumn(
                  "annee_partition",
                  year(to_timestamp("date_prelevement", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
              )
        )
        (
            df.write
            .format("delta")
            .mode("append")
            .partitionBy("annee_partition")
            .save(BRONZE_PATH)
        )


def log_debug(dept, yr, month, week, count):
    """Thread-safe debug counter update."""
    with debug_lock:
        debug_stats[dept][yr][month][week] += count


def get_weeks(yr, month):
    """
    Returns list of (date_min, date_max) for each week of the month.
    date_max uses T23:59:59Z to capture all records on the last day of each week.
    """
    last_day = calendar.monthrange(yr, month)[1]
    weeks = [
        (f"{yr}-{month:02d}-01", f"{yr}-{month:02d}-07T23:59:59Z"),
        (f"{yr}-{month:02d}-08", f"{yr}-{month:02d}-14T23:59:59Z"),
        (f"{yr}-{month:02d}-15", f"{yr}-{month:02d}-21T23:59:59Z"),
        (f"{yr}-{month:02d}-22", f"{yr}-{month:02d}-28T23:59:59Z"),
    ]
    if last_day > 28:
        weeks.append((
            f"{yr}-{month:02d}-29",
            f"{yr}-{month:02d}-{last_day:02d}T23:59:59Z"
        ))
    return weeks


def fetch_by_week(dept, yr, month):
    """Level 4 - fetches data week by week for a given month."""
    records = []
    for (d_min, d_max) in get_weeks(yr, month):
        data       = fetch_page(build_params(dept, d_min, d_max))
        rows       = data.get("data", [])
        count      = data.get("count", 0)
        week_label = f"{d_min[8:10]}-{d_max[8:10]}"
        if count > MAX_DEPTH:
            print(f"    WARNING week {d_min} -> {d_max}: {count} > {MAX_DEPTH} -- truncated")
        print(f"    week {d_min} -> {d_max}: {len(rows)} records")
        log_debug(dept, yr, month, week_label, len(rows))
        records.extend(rows)
        time.sleep(0.3)
    return records


def fetch_by_month_range(dept, yr, month_start, month_end):
    """
    Level 3 - fetches data for months between month_start and month_end.
    Used by fetch_by_quarter to fetch only the 3 months of the quarter.
    date_max uses T23:59:59Z to capture all records on the last day of each month.
    """
    records = []
    for m in range(month_start, month_end + 1):
        last_day = calendar.monthrange(yr, m)[1]
        d_min    = f"{yr}-{m:02d}-01"
        d_max    = f"{yr}-{m:02d}-{last_day:02d}T23:59:59Z"
        data     = fetch_page(build_params(dept, d_min, d_max))
        count    = data.get("count", 0)
        rows     = data.get("data", [])
        if count == 0:
            continue
        if count > MAX_DEPTH:
            print(f"  month {d_min[:7]}: {count} > {MAX_DEPTH} -- splitting by week")
            records.extend(fetch_by_week(dept, yr, m))
        else:
            print(f"  month {d_min[:7]}: {count} records")
            log_debug(dept, yr, m, "all", count)
            records.extend(rows)
        time.sleep(0.3)
    return records


# ---------------------------------------------------------------------------
# Quarters with T23:59:59Z on date_max to capture after-midnight records
# ---------------------------------------------------------------------------
QUARTERS = [
    ("-01-01", "-03-31T23:59:59Z", 1,  3),   # Q1: months 1-3
    ("-04-01", "-06-30T23:59:59Z", 4,  6),   # Q2: months 4-6
    ("-07-01", "-09-30T23:59:59Z", 7,  9),   # Q3: months 7-9
    ("-10-01", "-12-31T23:59:59Z", 10, 12),  # Q4: months 10-12
]


def fetch_by_quarter(dept, yr):
    """
    Level 2 - fetches data quarter by quarter.
    Only fetches months within the quarter when splitting.
    """
    records = []
    for (q_min, q_max, month_start, month_end) in QUARTERS:
        d_min = f"{yr}{q_min}"
        d_max = f"{yr}{q_max}"
        data  = fetch_page(build_params(dept, d_min, d_max))
        count = data.get("count", 0)
        rows  = data.get("data", [])
        if count == 0:
            continue
        if count > MAX_DEPTH:
            print(f"  quarter {d_min}: {count} > {MAX_DEPTH} -- splitting by month ({month_start}-{month_end})")
            records.extend(fetch_by_month_range(dept, yr, month_start, month_end))
        else:
            print(f"  quarter {d_min}: {count} records")
            q_label = f"Q{(month_start - 1) // 3 + 1}"
            log_debug(dept, yr, q_label, "all", count)
            records.extend(rows)
        time.sleep(0.3)
    return records


def fetch_year(dept, yr):
    """
    Entry point. Fetches all data for a (dept, year) combination.
    date_max uses T23:59:59Z to include all records on Dec 31.
    Returns (dept, yr, records).
    """
    data  = fetch_page(build_params(dept, f"{yr}-01-01", f"{yr}-12-31T23:59:59Z"))
    count = data.get("count", 0)
    rows  = data.get("data", [])

    if count == 0:
        return dept, yr, []

    if count <= MAX_DEPTH:
        print(f"  dept {dept} year {yr}: {count} records")
        log_debug(dept, yr, "all", "all", count)
        return dept, yr, rows

    print(f"  dept {dept} year {yr}: {count} > {MAX_DEPTH} -- splitting by quarter")
    return dept, yr, fetch_by_quarter(dept, yr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion

# COMMAND ----------

os.makedirs(BRONZE_PATH, exist_ok=True)

print("=" * 50)
print("BRONZE INGESTION -- HUB'EAU API (PARALLEL)")
print(f"Started : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Tasks   : {len(DEPARTMENTS)} depts x {len(YEARS)} years = {len(DEPARTMENTS) * len(YEARS)} tasks")
print(f"Workers : {MAX_WORKERS}")
print("=" * 50)

tasks = [(dept, yr) for dept in DEPARTMENTS for yr in YEARS]
total = 0
batch = []

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {
        executor.submit(fetch_year, dept, yr): (dept, yr)
        for dept, yr in tasks
    }
    for future in as_completed(futures):
        try:
            dept, yr, records = future.result()
        except Exception as e:
            dept, yr = futures[future]
            print(f"  ERROR dept {dept} year {yr}: {e}")
            continue

        if not records:
            print(f"  dept {dept} year {yr}: no data")
            continue

        with write_lock:
            batch.extend(records)
            total += len(records)
            current_batch = len(batch)

        print(f"  OK dept {dept} year {yr}: {len(records)} records -- batch: {current_batch} -- total: {total}")

        if current_batch >= BATCH_SIZE:
            print(f"  Writing batch ({current_batch} records)...")
            write_to_delta(batch)
            with write_lock:
                batch = []
            print(f"  Batch written.")

if batch:
    print(f"\nWriting final batch ({len(batch)} records)...")
    write_to_delta(batch)

print(f"\n{'=' * 50}")
print(f"DONE -- {total:,} records written to {BRONZE_PATH}")
print(f"{'=' * 50}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Debug report — records per dept / year / month / week

# COMMAND ----------

print("\n" + "=" * 60)
print("DEBUG REPORT -- API CALLS BREAKDOWN")
print("=" * 60)

grand_total = 0

for dept in sorted(debug_stats):
    dept_total = 0
    print(f"\nDept {dept}")
    print(f"  {'Year':<6} {'Month/Quarter':<20} {'Week':<12} {'Records':>10}")
    print(f"  {'-'*6} {'-'*20} {'-'*12} {'-'*10}")
    for yr in sorted(debug_stats[dept]):
        yr_total = 0
        for month in sorted(debug_stats[dept][yr], key=lambda x: str(x)):
            for week, count in sorted(debug_stats[dept][yr][month].items()):
                label_m = f"month {month}" if isinstance(month, int) else month
                label_w = f"week {week}" if week != "all" else "full"
                print(f"  {yr:<6} {label_m:<20} {label_w:<12} {count:>10,}")
                yr_total += count
        print(f"  {yr:<6} {'TOTAL':<20} {'':12} {yr_total:>10,}")
        dept_total += yr_total
    print(f"\n  Dept {dept} TOTAL: {dept_total:,}")
    grand_total += dept_total

print(f"\n{'=' * 60}")
print(f"GRAND TOTAL (API calls) : {grand_total:,}")
print(f"WRITTEN TO DELTA        : {total:,}")
print(f"{'=' * 60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

df = spark.read.format("delta").load(BRONZE_PATH)

partitions = sorted([
    r.annee_partition
    for r in df.select("annee_partition").distinct().collect()
    if r.annee_partition
])

print("=" * 50)
print("BRONZE VALIDATION")
print("=" * 50)
print(f"  Rows        : {df.count():,}")
print(f"  Partitions  : {partitions}")
print(f"  Departments : {df.select('code_departement').distinct().count()}")
print(f"  Communes    : {df.select('code_commune').distinct().count()}")
print(f"  Parameters  : {df.select('libelle_parametre').distinct().count()}")
print("=" * 50)

df.show(10, truncate=True)

# COMMAND ----------