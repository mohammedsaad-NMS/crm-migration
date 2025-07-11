#!/usr/bin/env python3
"""
Enrichment Enrollments Loader — National Math Stars CRM Migration
==================================================================
Transforms legacy **STEM Enrichments Progress** records into target-ready
**Enrichment Enrollments** records.

This script executes the following workflow:
1.  Loads legacy data and target schema definitions.
2.  Canonicalizes product information (Enrichments) using a pre-approved deduplication cache.
3.  Transforms legacy column names to their target equivalents based on the mapping file.
4.  Replaces Star ID with Star Full Name using a cached lookup file.
5.  Derives the enrollment "Status" (Upcoming, Ongoing, Completed) from date fields.
6.  Formats the final data to match the target schema and exports it to CSV.
"""

from __future__ import annotations
import logging
from pathlib import Path
import pandas as pd
import numpy as np

# Suppress pandas SettingWithCopyWarning for cleaner output
pd.options.mode.chained_assignment = None

# Assumes 'etl_lib' is in a discoverable 'scripts' subdirectory
from scripts.etl_lib import (
    read_mapping,
    read_target_catalog,
    assert_target_pairs_exist,
    transform_legacy_df,
)

# ======================================================================================
# CONFIGURATION
# ======================================================================================
# --- Path & File Definitions ---
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR.parent / "mapping" / "legacy-exports"
CACHE_DIR = BASE_DIR.parent / "cache"
OUTPUT_DIR = BASE_DIR.parent / "output"

LEGACY_FILE = INPUT_DIR / "STEM_Enrichments_Progress_2025_06_27.csv"
PRODUCT_DECISIONS_FILE = CACHE_DIR / "decisions_Products_2025_06_27.csv"
STAR_LOOKUP_FILE = CACHE_DIR / "star_lookup.csv"
OUTPUT_CSV_FILE = OUTPUT_DIR / "Enrichment_Enrollments.csv"

# --- Module & Field Definitions ---
MODULE_UI = "Enrichment Enrollments"
LEGACY_MODULE = "Stem Enrichments Progress"
LEGACY_PRODUCT_ID_COL = "Enrichment.id"
LEGACY_PRODUCT_NAME_COL = "Enrichment"
STAR_MATCH_KEY_COL = "Star (Match Key)"


# ======================================================================================
# LOGGING SETUP
# ======================================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ======================================================================================
# HELPER FUNCTIONS
# ======================================================================================

def _read_csv(path: Path) -> pd.DataFrame:
    """Reads a CSV file into a DataFrame, raising an error if not found."""
    if not path.exists():
        raise FileNotFoundError(f"Required file not found at: {path}")
    return pd.read_csv(path, dtype=str, keep_default_na=False).replace("", pd.NA)


def _load_product_decisions(cache_file: Path) -> tuple[dict, dict]:
    """Loads product deduplication decisions from the cache file."""
    try:
        df = _read_csv(cache_file)
        merge_only = df[df["user_decision"] == "MERGE"]

        id_remap = pd.Series(
            merge_only.canonical_record_id.values, index=merge_only.duplicate_record_id
        ).to_dict()
        id_to_name = pd.Series(
            merge_only.canonical_name.values, index=merge_only.canonical_record_id
        ).to_dict()

        log.info(f"Loaded {len(id_remap)} product ID remaps from cache.")
        return id_remap, id_to_name

    except FileNotFoundError:
        log.warning("Product decisions cache not found. No product remapping will be applied.")
        return {}, {}


def _derive_status(start: pd.Series, end: pd.Series) -> pd.Series:
    """Derives enrollment status based on start and end dates."""
    today = pd.Timestamp.today().normalize()
    start_dt = pd.to_datetime(start, errors="coerce")
    end_dt = pd.to_datetime(end, errors="coerce")

    conditions = [
        start_dt > today,
        (start_dt <= today) & ((end_dt.isna()) | (end_dt >= today)),
    ]
    choices = ["Upcoming", "In Progress"]
    status = np.select(conditions, choices, default="Completed")

    # Ensure status is blank if dates are missing to prevent bad data
    status[start_dt.isna() & end_dt.isna()] = pd.NA
    return pd.Series(status, index=start.index, name="Status")


# ======================================================================================
# MAIN EXECUTION
# ======================================================================================

def main() -> None:
    """Main ETL script execution."""
    log.info(f"Starting {MODULE_UI} loader...")
    OUTPUT_DIR.mkdir(exist_ok=True)

    # 1. Load mappings, catalogs, and raw data
    mapping = read_mapping()
    catalog = read_target_catalog()
    try:
        df_raw = _read_csv(LEGACY_FILE)
    except FileNotFoundError:
        log.warning(f"Source file {LEGACY_FILE.name} was not found. Aborting process.")
        return

    id_remap, id_to_name = _load_product_decisions(PRODUCT_DECISIONS_FILE)

    # 2. Filter mapping for the relevant modules and validate
    map_this = mapping.query(
        "`Legacy Module` == @LEGACY_MODULE and `Target Module` == @MODULE_UI"
    )
    if map_this.empty:
        log.error("Mapping failed: Could not find a match for the specified Legacy and Target modules in the mapping file.")
        log.error(f"Please check that LEGACY_MODULE='{LEGACY_MODULE}' and MODULE_UI='{MODULE_UI}' are correct.")
        return
        
    assert_target_pairs_exist(MODULE_UI, map_this, catalog)

    # 3. Apply product canonicalization
    if id_remap and LEGACY_PRODUCT_ID_COL in df_raw.columns:
        df_raw[LEGACY_PRODUCT_ID_COL] = df_raw[LEGACY_PRODUCT_ID_COL].replace(id_remap)
    if id_to_name and LEGACY_PRODUCT_NAME_COL in df_raw.columns:
        df_raw[LEGACY_PRODUCT_NAME_COL] = df_raw[LEGACY_PRODUCT_ID_COL].map(id_to_name).fillna(df_raw[LEGACY_PRODUCT_NAME_COL])

    # 4. Perform generic column rename based on mapping
    df_ui = transform_legacy_df(df_raw, map_this)

    # 5. Enrich Star Name
    if STAR_MATCH_KEY_COL in df_ui.columns:
        try:
            log.info("Loading Star lookup file to replace ID with Full Name...")
            df_star_lookup = _read_csv(STAR_LOOKUP_FILE)
            star_name_map = pd.Series(
                df_star_lookup["Full Name"].values, index=df_star_lookup["Record Id"]
            ).to_dict()

            original_ids = df_ui[STAR_MATCH_KEY_COL].nunique()
            df_ui[STAR_MATCH_KEY_COL] = df_ui[STAR_MATCH_KEY_COL].map(star_name_map)
            found_names = df_ui[STAR_MATCH_KEY_COL].notna().sum()
            log.info(f"Successfully mapped {found_names} of {original_ids} unique Star IDs to full names.")

        except FileNotFoundError:
            log.warning(f"Star lookup file not found at '{STAR_LOOKUP_FILE}'. Skipping name enrichment.")
        except Exception as e:
            log.error(f"An error occurred during Star name enrichment: {e}")
            
    # 6. Derive Status
    if {"Start Date", "End Date"}.issubset(df_ui.columns):
        df_ui["Status"] = _derive_status(df_ui["Start Date"], df_ui["End Date"])

    # 7. Align DataFrame with the full target schema
    ui_cols = catalog.query(
        "`User-Facing Module Name` == @MODULE_UI and not `Data Source / Type`.str.contains('Related List', na=False)"
    )["User-Facing Field Name"].tolist()

    for col in ui_cols:
        if col not in df_ui.columns:
            df_ui[col] = pd.NA

    # 8. Save final ordered output
    df_final = df_ui[ui_cols]
    df_final.to_csv(OUTPUT_CSV_FILE, index=False)
    log.info(f"{MODULE_UI} loader complete. Output: {OUTPUT_CSV_FILE.name} ({len(df_final)} rows)")


if __name__ == "__main__":
    main()