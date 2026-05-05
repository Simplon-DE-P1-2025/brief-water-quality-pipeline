# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze - Download Raw Data
# MAGIC
# MAGIC Downloads ZIP files from data.gouv.fr with:
# MAGIC - Parallel downloads (5 workers)
# MAGIC - HTTP Range resume on interruption
# MAGIC - Exponential retry (3 attempts)
# MAGIC - MD5 checksum verification
# MAGIC - Incremental updates (skip if already up to date)

# COMMAND ----------

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------

import os
import hashlib
import json
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ---------------------------------------------------------------------------
# Environment detection
# ---------------------------------------------------------------------------

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

RAW_PATH  = "dbfs:/tmp/eau_potable/raw" if IS_DATABRICKS else "data/raw"
META_PATH = os.path.join("data", "checksums.json")

os.makedirs(RAW_PATH, exist_ok=True)
os.makedirs(os.path.dirname(META_PATH), exist_ok=True)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DATASET_ID  = "5cf8d9ed8b4c4110294c841d"
API_URL     = f"https://www.data.gouv.fr/api/1/datasets/{DATASET_ID}/"
MAX_WORKERS = 5
CHUNK_SIZE  = 1024 * 1024  # 1 MB
MAX_RETRIES = 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters - Edit here

# COMMAND ----------

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

# "dev"  : download only DEV_YEARS
# "prod" : download all available years
MODE = "dev"

# Years to download in dev mode
DEV_YEARS = [2024, 2025]

# Set to True to force re-download even if file is up to date
FORCE_DOWNLOAD = False

# ---------------------------------------------------------------------------

print(f"Environment    : {'Databricks' if IS_DATABRICKS else 'Local'}")
print(f"Mode           : {MODE.upper()}")

if MODE == "dev":
    print(f"Target years   : {DEV_YEARS}")

print(f"Force download : {FORCE_DOWNLOAD}")
print(f"Raw path       : {RAW_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility functions

# COMMAND ----------

# ---------------------------------------------------------------------------
# Checksum helpers
# ---------------------------------------------------------------------------

def compute_md5(path: str) -> str:
    """
    Computes MD5 hash of a local file using chunked reading.
    Avoids loading the entire file into memory.
    """
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()


def load_checksums(meta_path: str) -> dict:
    """Loads previously saved checksums from local JSON file."""
    if os.path.exists(meta_path):
        with open(meta_path, "r") as f:
            return json.load(f)
    return {}


def save_checksums(meta_path: str, checksums: dict) -> None:
    """Persists checksums to local JSON file."""
    with open(meta_path, "w") as f:
        json.dump(checksums, f, indent=2)

# ---------------------------------------------------------------------------
# Resource fetching
# ---------------------------------------------------------------------------

def get_zip_resources(api_url: str, mode: str, dev_years: list) -> list:
    """
    Fetches national ZIP resources from the data.gouv.fr API.
    Excludes department-level files and non-ZIP formats.

    Args:
        api_url   : data.gouv.fr dataset API endpoint
        mode      : "dev" or "prod"
        dev_years : list of years to include in dev mode

    Returns:
        Sorted list of resource dicts
    """

    print("Fetching resources from data.gouv.fr...")

    r = requests.get(api_url, timeout=30)
    r.raise_for_status()

    resources = []

    for res in r.json().get("resources", []):

        title = res.get("title", "")
        fmt   = res.get("format", "").lower()
        url   = res.get("url", "")
        size  = res.get("filesize", 0)

        # Keep national ZIP files only (exclude -dept variants and non-ZIP)
        if fmt != "zip" or "dept" in title or not title.startswith("dis-"):
            continue

        # Extract year from title (e.g. "dis-2024.zip" -> 2024)
        try:
            annee = int(title.replace("dis-", "").replace(".zip", ""))
        except ValueError:
            continue

        # Filter by year in dev mode
        if mode == "dev" and annee not in dev_years:
            continue

        checksum_data = res.get("checksum", {})
        remote_md5    = (
            checksum_data.get("value", "")
            if checksum_data.get("type") == "md5"
            else ""
        )

        resources.append({
            "title"         : title,
            "annee"         : annee,
            "url"           : url,
            "size_mb"       : round(size / 1024 / 1024, 1) if size else 0,
            "remote_md5"    : remote_md5,
            "last_modified" : res.get("last_modified", ""),
        })

    print(f"  -> {len(resources)} ZIP files selected")

    return sorted(resources, key=lambda x: x["annee"])

# ---------------------------------------------------------------------------
# Download decision
# ---------------------------------------------------------------------------

def needs_download(
    resource        : dict,
    local_checksums : dict,
    raw_path        : str,
    force           : bool,
) -> tuple:
    """
    Determines whether a file needs to be (re)downloaded.

    Returns:
        (True,  reason) if download is needed
        (False, reason) if file is already up to date
    """

    title    = resource["title"]
    zip_path = os.path.join(raw_path, title)

    if force:
        return True, "forced"

    if not os.path.exists(zip_path):
        return True, "new file"

    # Detect incomplete download
    local_size  = os.path.getsize(zip_path)
    remote_size = int(resource["size_mb"] * 1024 * 1024)

    if remote_size > 0 and local_size < remote_size * 0.99:
        return (
            True,
            f"incomplete ({round(local_size / 1024 / 1024, 1)} MB"
            f" / {resource['size_mb']} MB)",
        )

    remote_md5 = resource.get("remote_md5", "")
    saved_md5  = local_checksums.get(title, "")

    if not remote_md5:
        if not saved_md5:
            return True, "missing checksum"
        return False, "up to date (no remote md5)"

    if saved_md5 != remote_md5:
        return True, "updated"

    return False, "up to date"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download with retry and HTTP Range resume

# COMMAND ----------

# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_zip(resource: dict, raw_path: str) -> dict:
    """
    Downloads a ZIP file to disk using:
    - Chunked streaming (1 MB at a time, minimal RAM usage)
    - HTTP Range header for resume on interruption
    - Exponential retry: 2s, 4s, 8s between attempts
    - MD5 verification after download

    Args:
        resource : resource dict from get_zip_resources()
        raw_path : local directory to store the ZIP

    Returns:
        dict with title, path, md5, status
    """

    title    = resource["title"]
    url      = resource["url"]
    zip_path = os.path.join(raw_path, title)

    for attempt in range(MAX_RETRIES):
        try:

            # Resume from where we left off if file already partially downloaded
            existing_size = os.path.getsize(zip_path) if os.path.exists(zip_path) else 0
            headers       = {"Range": f"bytes={existing_size}-"} if existing_size > 0 else {}

            if existing_size > 0:
                print(
                    f"  Resuming from"
                    f" {round(existing_size / 1024 / 1024, 1)} MB..."
                )

            with requests.get(url, timeout=300, stream=True, headers=headers) as r:
                r.raise_for_status()

                remaining  = int(r.headers.get("content-length", 0))
                total      = existing_size + remaining
                file_mode  = "ab" if existing_size > 0 else "wb"
                downloaded = existing_size

                with open(zip_path, file_mode) as f:
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                        f.write(chunk)
                        downloaded += len(chunk)

                        if total:
                            pct = round(downloaded / total * 100, 1)
                            print(
                                f"\r    {pct}%"
                                f" -- {round(downloaded / 1024 / 1024, 1)}"
                                f" / {round(total / 1024 / 1024, 1)} MB",
                                end="",
                            )

            print()
            break  # success - exit retry loop

        except Exception as e:
            wait = 2 ** (attempt + 1)  # 2s, 4s, 8s
            print(f"\n  Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")

            if attempt == MAX_RETRIES - 1:
                raise

            print(f"  Waiting {wait}s before retry...")
            time.sleep(wait)

    # ---------------------------------------------------------------------------
    # MD5 verification
    # ---------------------------------------------------------------------------

    local_md5  = compute_md5(zip_path)
    remote_md5 = resource.get("remote_md5", "")

    if remote_md5 and local_md5 != remote_md5:
        os.remove(zip_path)
        raise ValueError(
            f"MD5 mismatch for {title}!"
            f" Expected: {remote_md5}, Got: {local_md5}"
        )

    print(f"  OK {title} -- md5: {local_md5[:8]}...")

    return {
        "title"  : title,
        "path"   : zip_path,
        "md5"    : local_md5,
        "status" : "downloaded",
    }


def process_resource(
    resource        : dict,
    raw_path        : str,
    local_checksums : dict,
) -> dict:
    """
    Checks whether a resource needs downloading and downloads it if so.

    Args:
        resource        : resource dict
        raw_path        : local directory
        local_checksums : previously saved checksums

    Returns:
        dict with title, path, md5, status
    """

    title             = resource["title"]
    should_dl, reason = needs_download(resource, local_checksums, raw_path, FORCE_DOWNLOAD)
    zip_path          = os.path.join(raw_path, title)

    if not should_dl:
        print(f"  SKIP {title} ({reason})")
        local_md5 = local_checksums.get(title, compute_md5(zip_path))
        return {
            "title"  : title,
            "path"   : zip_path,
            "md5"    : local_md5,
            "status" : "skipped",
        }

    print(f"  DOWNLOAD {title} ({reason} -- {resource['size_mb']} MB)...")
    return download_zip(resource, raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline

# COMMAND ----------

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

print("=" * 60)
print("DOWNLOAD PIPELINE -- EAU POTABLE")
print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

resources       = get_zip_resources(API_URL, MODE, DEV_YEARS)
local_checksums = load_checksums(META_PATH)
total_size      = sum(r["size_mb"] for r in resources)

print(f"\nSelected files ({total_size} MB total):")

for r in resources:
    saved_md5  = local_checksums.get(r["title"], "")
    remote_md5 = r.get("remote_md5", "")
    zip_path   = os.path.join(RAW_PATH, r["title"])

    if FORCE_DOWNLOAD:
        status = "FORCE"
    elif not os.path.exists(zip_path):
        status = "NEW"
    elif saved_md5 and saved_md5 == remote_md5:
        status = "OK"
    else:
        status = "UPDATE"

    print(f"  [{status}] {r['title']} ({r['size_mb']} MB)")

print(f"\nStarting parallel download ({MAX_WORKERS} workers)...\n")

results       = []
new_checksums = dict(local_checksums)

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

    futures = {
        executor.submit(process_resource, r, RAW_PATH, local_checksums): r["title"]
        for r in resources
    }

    for future in as_completed(futures):
        try:
            result = future.result()
            results.append(result)
            new_checksums[result["title"]] = result["md5"]
        except Exception as e:
            title = futures[future]
            print(f"  ERROR {title}: {e}")

save_checksums(META_PATH, new_checksums)

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

downloaded = [r for r in results if r["status"] == "downloaded"]
skipped    = [r for r in results if r["status"] == "skipped"]
errors     = len(resources) - len(results)

print(f"\n{'=' * 60}")
print("SUMMARY")
print(f"{'=' * 60}")
print(f"  Downloaded : {len(downloaded)} files")
print(f"  Skipped    : {len(skipped)} files (already up to date)")
print(f"  Errors     : {errors} files")
print(f"\nFiles available in {RAW_PATH}:")

for f in sorted(os.listdir(RAW_PATH)):
    if f.endswith(".zip"):
        size_mb = round(os.path.getsize(os.path.join(RAW_PATH, f)) / 1024 / 1024, 1)
        md5     = new_checksums.get(f, "?")[:8]
        print(f"  - {f} ({size_mb} MB) -- md5: {md5}...")

# COMMAND ----------