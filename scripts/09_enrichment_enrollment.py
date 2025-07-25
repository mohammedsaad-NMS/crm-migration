#!/usr/bin/env python3
"""
Enrichment Enrollments Loader — National Math Stars CRM Migration
==================================================================
Transforms legacy **STEM Enrichments Progress** records into target-ready
**Enrichment Enrollments** records.
"""

from __future__ import annotations
import logging
from pathlib import Path
import pandas as pd
import numpy as np

pd.options.mode.chained_assignment = None

from scripts.helpers.etl_lib import (
    read_mapping,
    read_target_catalog,
    assert_target_pairs_exist,
    transform_legacy_df,
)

# ======================================================================================
# CONFIGURATION
# ======================================================================================
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR.parent / "mapping" / "legacy-exports"
CACHE_DIR = BASE_DIR.parent / "cache"
OUTPUT_DIR = BASE_DIR.parent / "output"

LEGACY_FILE = INPUT_DIR / "STEM Enrichments Progress_C_001.csv"
PRODUCT_DECISIONS_FILE = CACHE_DIR / "decisions_Products_001.csv"
STAR_LOOKUP_FILE = CACHE_DIR / "star_lookup.csv"
# --- NEW --- Using the final product lookup cache from the products script
PRODUCT_LOOKUP_FILE = CACHE_DIR / "product_lookup.csv"
OUTPUT_CSV_FILE = OUTPUT_DIR / "Enrichment Enrollments.csv"

MODULE_UI = "Enrichment Enrollments"
LEGACY_MODULE = "Stem Enrichments Progress"
LEGACY_PRODUCT_ID_COL = "Enrichment.id"
# --- This variable will hold the name of the target field for the enrichment, post-transformation
TARGET_PRODUCT_MATCH_KEY_COL = "Product (Enrichment) (Match Key)"
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
    if not path.exists():
        raise FileNotFoundError(f"Required file not found at: {path}")
    return pd.read_csv(path, dtype=str, keep_default_na=False).replace("", pd.NA)


def _load_product_decisions(cache_file: Path) -> dict:
    """Loads product deduplication decisions to remap duplicate IDs."""
    try:
        df = _read_csv(cache_file)
        merge_only = df[df["user_decision"] == "MERGE"]
        id_remap = pd.Series(
            merge_only.canonical_record_id.values, index=merge_only.duplicate_record_id
        ).to_dict()
        log.info(f"Loaded {len(id_remap)} product ID remaps from cache.")
        return id_remap
    except FileNotFoundError:
        log.warning("Product decisions cache not found. No product remapping will be applied.")
        return {}


def _derive_status(start: pd.Series, end: pd.Series) -> pd.Series:
    today = pd.to_datetime("2025-07-23") # Using a fixed date for consistent output
    start_dt = pd.to_datetime(start, errors="coerce")
    end_dt = pd.to_datetime(end, errors="coerce")
    conditions = [
        start_dt > today,
        (start_dt <= today) & ((end_dt.isna()) | (end_dt >= today)),
    ]
    choices = ["Upcoming", "In Progress"]
    status = np.select(conditions, choices, default="Completed")
    status[start_dt.isna() & end_dt.isna()] = pd.NA
    return pd.Series(status, index=start.index, name="Enrichment Status")


# ======================================================================================
# MAIN EXECUTION
# ======================================================================================

def main() -> None:
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

    id_remap = _load_product_decisions(PRODUCT_DECISIONS_FILE)

    if "Outcome" in df_raw.columns:
        withdrew_mask = df_raw["Outcome"] == "Withdrew"
    else:
        log.warning("Legacy 'Outcome' column not found. Cannot process 'Withdrew' status.")
        withdrew_mask = None

    # 2. Filter mapping and validate
    map_this = mapping.query(f"`Legacy Module` == @LEGACY_MODULE and `Target Module` == @MODULE_UI")
    if map_this.empty:
        log.error(f"Mapping failed for LEGACY_MODULE='{LEGACY_MODULE}' and MODULE_UI='{MODULE_UI}'.")
        return
    assert_target_pairs_exist(MODULE_UI, map_this, catalog)

    # 3. Apply product ID canonicalization
    if id_remap and LEGACY_PRODUCT_ID_COL in df_raw.columns:
        df_raw[LEGACY_PRODUCT_ID_COL] = df_raw[LEGACY_PRODUCT_ID_COL].replace(id_remap)

    # 4. Perform generic column rename based on mapping
    df_ui = transform_legacy_df(df_raw, map_this)

    # 5. Enrich Star and Product Names
    # --- NEW: Enrich Product Name from final cache ---
    if TARGET_PRODUCT_MATCH_KEY_COL in df_ui.columns:
        try:
            log.info(f"Loading final product names from '{PRODUCT_LOOKUP_FILE.name}'...")
            df_product_lookup = _read_csv(PRODUCT_LOOKUP_FILE)
            product_name_map = pd.Series(
                df_product_lookup["Product Name"].values, index=df_product_lookup["Record Id"]
            ).to_dict()
            df_ui[TARGET_PRODUCT_MATCH_KEY_COL] = df_ui[TARGET_PRODUCT_MATCH_KEY_COL].map(product_name_map)
        except FileNotFoundError:
            log.warning(f"Product lookup file not found at '{PRODUCT_LOOKUP_FILE}'. Skipping product name enrichment.")
        except Exception as e:
            log.error(f"An error occurred during product name enrichment: {e}")

    if STAR_MATCH_KEY_COL in df_ui.columns:
        try:
            log.info("Loading Star lookup file to replace ID with Full Name...")
            df_star_lookup = _read_csv(STAR_LOOKUP_FILE)
            star_name_map = pd.Series(
                df_star_lookup["Star Full Name"].values, index=df_star_lookup["Record Id"]
            ).to_dict()
            df_ui[STAR_MATCH_KEY_COL] = df_ui[STAR_MATCH_KEY_COL].map(star_name_map)
        except FileNotFoundError:
            log.warning(f"Star lookup file not found at '{STAR_LOOKUP_FILE}'. Skipping name enrichment.")
        except Exception as e:
            log.error(f"An error occurred during Star name enrichment: {e}")
            
    # 6. Derive and overwrite Status
    if {"Enrichment Start Date", "Enrichment End Date"}.issubset(df_ui.columns):
        df_ui["Enrichment Status"] = _derive_status(df_ui["Enrichment Start Date"], df_ui["Enrichment End Date"])

    if withdrew_mask is not None and withdrew_mask.any() and "Enrichment Status" in df_ui.columns:
        log.info(f"Overwriting status to 'Withdrew' for {withdrew_mask.sum()} records.")
        df_ui.loc[withdrew_mask, "Enrichment Status"] = "Withdrew"

    # 7. Align DataFrame with the full target schema
    ui_cols = catalog.query(
        f"`User-Facing Module Name` == @MODULE_UI and not `Data Source / Type`.str.contains('Related List', na=False)"
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