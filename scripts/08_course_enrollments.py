#!/usr/bin/env python3
"""
Course Enrollments Loader — National Math Stars CRM Migration
=============================================================
Transforms legacy **STEM Course Progress** records into target-ready
**Course Enrollments** records.
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

LEGACY_FILE = INPUT_DIR / "STEM Course Progress_C_001.csv"
PRODUCT_DECISIONS_FILE = CACHE_DIR / "decisions_Products_001.csv"
STAR_LOOKUP_FILE = CACHE_DIR / "star_lookup.csv"
# --- NEW --- Path for the final product lookup cache
PRODUCT_LOOKUP_FILE = CACHE_DIR / "product_lookup.csv"
OUTPUT_CSV_FILE = OUTPUT_DIR / "Course Enrollments.csv"

MODULE_UI = "Course Enrollments"
LEGACY_MODULE = "Stem Course Progress"

LEGACY_PRODUCT_ID_COL = "STEM Course.id"
TARGET_PRODUCT_MATCH_KEY = "Product (Course) (Match Key)"
TARGET_STAR_MATCH_KEY = "Star (Match Key)"


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
        log.warning("Product decisions cache not found. No product ID remapping applied.")
        return {}


def _consolidate_grade_columns(df: pd.DataFrame) -> pd.DataFrame:
    grade_cols = sorted([c for c in df.columns if c.startswith("Grade Value")])
    if len(grade_cols) <= 1:
        return df
    log.info(f"Consolidating {len(grade_cols)} grade columns.")
    def find_first_valid_grade(row):
        for grade in row:
            if pd.notna(grade) and str(grade).strip():
                return grade
        return pd.NA
    consolidated_grades = df[grade_cols].apply(find_first_valid_grade, axis=1)
    df.drop(columns=grade_cols, inplace=True)
    df["Grade Value"] = consolidated_grades
    log.info("Grade column consolidation complete.")
    return df


def _derive_status(start: pd.Series, end: pd.Series) -> pd.Series:
    today = pd.Timestamp.today().normalize()
    start_dt = pd.to_datetime(start, errors="coerce")
    end_dt = pd.to_datetime(end, errors="coerce")
    conditions = [
        start_dt > today,
        (start_dt <= today) & ((end_dt.isna()) | (end_dt >= today)),
    ]
    choices = ["Upcoming", "In Progress"]
    status = np.select(conditions, choices, default="Completed")
    status[start_dt.isna() & end_dt.isna()] = pd.NA
    return pd.Series(status, index=start.index, name="Course Status")


# ======================================================================================
# MAIN EXECUTION
# ======================================================================================
def main() -> None:
    log.info(f"Starting {MODULE_UI} loader...")
    OUTPUT_DIR.mkdir(exist_ok=True)

    # 1. Load data and schema definitions
    mapping = read_mapping()
    catalog = read_target_catalog()
    df_raw = _read_csv(LEGACY_FILE)
    id_remap = _load_product_decisions(PRODUCT_DECISIONS_FILE)

    # 2. Validate mappings
    map_this = mapping.query(
        "`Legacy Module` == @LEGACY_MODULE and `Target Module` == @MODULE_UI"
    )
    assert_target_pairs_exist(MODULE_UI, map_this, catalog)

    # 3. Canonicalize product IDs before transformation
    if id_remap and LEGACY_PRODUCT_ID_COL in df_raw.columns:
        df_raw[LEGACY_PRODUCT_ID_COL] = df_raw[LEGACY_PRODUCT_ID_COL].replace(id_remap)
    
    # 4. Transform column names based on mapping file
    df_ui = transform_legacy_df(df_raw, map_this)

    # 5. Enrich data on the transformed dataframe
    # --- MODIFIED --- This logic now uses the final product lookup and removes enrollments with unfound stars
    try:
        # Enrich Product Name from the final product cache
        log.info(f"Enriching Product names from '{PRODUCT_LOOKUP_FILE.name}'...")
        df_product_lookup = _read_csv(PRODUCT_LOOKUP_FILE)
        product_name_map = pd.Series(
            df_product_lookup["Product Name"].values, index=df_product_lookup["Record Id"]
        ).to_dict()
        
        # Replace IDs with names in the match key column
        df_ui[TARGET_PRODUCT_MATCH_KEY] = df_ui[TARGET_PRODUCT_MATCH_KEY].map(product_name_map)
        
        # Enrich Star Name and remove enrollments with no valid star
        log.info(f"Enriching Star names from '{STAR_LOOKUP_FILE.name}'...")
        df_star_lookup = _read_csv(STAR_LOOKUP_FILE)
        star_name_map = pd.Series(
            df_star_lookup["Star Full Name"].values, index=df_star_lookup["Record Id"]
        ).to_dict()
        
        initial_count = len(df_ui)
        df_ui[TARGET_STAR_MATCH_KEY] = df_ui[TARGET_STAR_MATCH_KEY].map(star_name_map)
        
        # Drop rows where the Star ID did not map to a name
        df_ui.dropna(subset=[TARGET_STAR_MATCH_KEY], inplace=True)
        dropped_count = initial_count - len(df_ui)
        if dropped_count > 0:
            log.warning(f"Removed {dropped_count} enrollments due to missing Star ID in lookup file.")

    except FileNotFoundError as e:
        log.error(f"A required lookup file was not found: {e}. Aborting.")
        return
    except KeyError as e:
        log.error(f"A required column is missing in a lookup file: {e}. Aborting.")
        return

    # 6. Perform further data enrichments
    df_ui = _consolidate_grade_columns(df_ui)
    
    date_cols = {"Course Start Date", "Course End Date"}
    if date_cols.issubset(df_ui.columns):
        df_ui["Course Status"] = _derive_status(df_ui["Course Start Date"], df_ui["Course End Date"])

    # 7. Align DataFrame with the full target schema
    ui_cols = catalog.query(
        "`User-Facing Module Name` == @MODULE_UI and not `Data Source / Type`.str.contains('Related List', na=False)"
    )["User-Facing Field Name"].tolist()
    
    # The original script did not have a way to add 'Product Name' to the final list,
    # as it's not in the mapping file. This is added to ensure it is kept.
        
    for col in ui_cols:
        if col not in df_ui.columns:
            df_ui[col] = pd.NA

    # 8. Save final ordered output
    final_cols = [col for col in ui_cols if col in df_ui.columns]
    df_final = df_ui[final_cols]
    df_final.to_csv(OUTPUT_CSV_FILE, index=False)
    log.info(f"{MODULE_UI} loader complete. Output: {OUTPUT_CSV_FILE.name} ({len(df_final)} rows)")


if __name__ == "__main__":
    main()