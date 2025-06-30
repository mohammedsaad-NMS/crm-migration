#!/usr/bin/env python3
"""
Course Enrollments Loader — National Math Stars CRM Migration
=============================================================
Transforms legacy **STEM Course Progress** records into target-ready
**Course Enrollments** records.

This script executes the following workflow:
1.  Loads legacy data and target schema definitions.
2.  Canonicalizes product information using a pre-approved deduplication cache.
3.  Transforms legacy column names to their target equivalents.
4.  Consolidates multiple legacy grade fields into a single "Grade Value" field.
5.  Derives the enrollment "Status" (Upcoming, Ongoing, Completed) from date fields.
6.  Formats the final data to match the target schema and exports it to CSV.
"""

from __future__ import annotations
import logging
from pathlib import Path
import pandas as pd
import numpy as np

# Suppress pandas SettingWithCopyWarning
pd.options.mode.chained_assignment = None

from scripts.etl_lib import (
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

LEGACY_FILE = INPUT_DIR / "STEM_Course_Progress_2025_06_27.csv"
PRODUCT_DECISIONS_FILE = CACHE_DIR / "decisions_Products_2025_06_27.csv"
OUTPUT_CSV_FILE = OUTPUT_DIR / "Course_Enrollments.csv"

MODULE_UI = "Course Enrollments"
LEGACY_MODULE = "Stem Course Progress"
LEGACY_PRODUCT_ID_COL = "STEM Course.id"
LEGACY_PRODUCT_NAME_COL = "STEM Course"


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
    """Reads a CSV file, raising an error if not found."""
    if not path.exists():
        raise FileNotFoundError(f"Required file not found at: {path}")
    return pd.read_csv(path, dtype=str, keep_default_na=False).replace("", pd.NA)


def _load_product_decisions(cache_file: Path) -> tuple[dict, dict]:
    """Loads product deduplication decisions from the cache."""
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
        log.warning("Product decisions cache not found. No product remapping applied.")
        return {}, {}


def _consolidate_grade_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Finds and robustly collapses multiple 'Grade Value*' columns into one.

    This function isolates the logic to prevent side effects:
    1. Identifies all columns starting with 'Grade Value'.
    2. Explicitly creates a new Series containing the coalesced grade values.
    3. Drops all the original grade columns from the DataFrame.
    4. Assigns the new Series to the 'Grade Value' column.
    """
    grade_cols = sorted([c for c in df.columns if c.startswith("Grade Value")])
    if len(grade_cols) <= 1:
        return df

    log.info(f"Consolidating {len(grade_cols)} grade columns: {grade_cols}")

    def find_first_valid_grade(row):
        """Find the first non-empty value in a row."""
        for grade in row:
            # Check for pandas nulls and empty strings
            if pd.notna(grade) and str(grade).strip():
                return grade
        return pd.NA

    # 1. Create the new consolidated Series
    consolidated_grades = df[grade_cols].apply(find_first_valid_grade, axis=1)

    # 2. Drop all original source grade columns
    df.drop(columns=grade_cols, inplace=True)

    # 3. Add the new, single 'Grade Value' column to the DataFrame
    df["Grade Value"] = consolidated_grades

    log.info("Grade column consolidation complete.")
    return df


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

    status[start_dt.isna() & end_dt.isna()] = pd.NA
    return pd.Series(status, index=start.index, name="Status")


# ======================================================================================
# MAIN EXECUTION
# ======================================================================================

def main() -> None:
    """Main ETL script execution."""
    log.info(f"Starting {MODULE_UI} loader...")
    OUTPUT_DIR.mkdir(exist_ok=True)

    # 1. Load data and schema definitions
    mapping = read_mapping()
    catalog = read_target_catalog()
    df_raw = _read_csv(LEGACY_FILE)
    id_remap, id_to_name = _load_product_decisions(PRODUCT_DECISIONS_FILE)

    df_raw = df_raw[~df_raw["Accounts"].astype(str).str.contains("test", case=False, na=False)]

    # 2. Validate mappings for the module
    map_this = mapping.query(
        "`Legacy Module` == @LEGACY_MODULE and `Target Module` == @MODULE_UI"
    )
    assert_target_pairs_exist(MODULE_UI, map_this, catalog)

    # 3. Canonicalize product data
    if id_remap and LEGACY_PRODUCT_ID_COL in df_raw.columns:
        df_raw[LEGACY_PRODUCT_ID_COL] = df_raw[LEGACY_PRODUCT_ID_COL].replace(id_remap)
    if id_to_name and LEGACY_PRODUCT_NAME_COL in df_raw.columns:
        df_raw[LEGACY_PRODUCT_NAME_COL] = df_raw[LEGACY_PRODUCT_ID_COL].map(id_to_name).fillna(df_raw[LEGACY_PRODUCT_NAME_COL])

    # 4. Transform column names and perform enrichments
    df_ui = transform_legacy_df(df_raw, map_this)
    df_ui = _consolidate_grade_columns(df_ui)
    if {"Start Date", "End Date"}.issubset(df_ui.columns):
        df_ui["Status"] = _derive_status(df_ui["Start Date"], df_ui["End Date"])

    # 5. Align DataFrame with the full target schema
    ui_cols = catalog.query(
        "`User-Facing Module Name` == @MODULE_UI and not `Data Source / Type`.str.contains('Related List', na=False)"
    )["User-Facing Field Name"].tolist()

    for col in ui_cols:
        if col not in df_ui.columns:
            df_ui[col] = pd.NA

    # 6. Save final ordered output
    df_final = df_ui[ui_cols]
    df_final.to_csv(OUTPUT_CSV_FILE, index=False)
    log.info(f"{MODULE_UI} loader complete. Output: {OUTPUT_CSV_FILE.name} ({len(df_final)} rows)")


if __name__ == "__main__":
    main()