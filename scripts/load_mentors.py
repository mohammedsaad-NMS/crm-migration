#!/usr/bin/env python3
"""
Mentors Module Loader — National Math Stars CRM Migration
=========================================================
Creates the target-ready **Mentors.csv** file.

This script does not use a legacy source file. Instead, it generates
the Mentor records from the cached list of mentor names created by
the Contacts loader.

Workflow:
1.  Loads the cached 'math_mentors_names.csv' file.
2.  Creates the 'Mentor Name' by concatenating first and last names.
3.  Aligns the DataFrame with the target schema from the catalog.
4.  Writes the final 'Mentors.csv' to the output directory.
"""

from __future__ import annotations
import logging
from pathlib import Path
import pandas as pd

# It's assumed that 'etl_lib' is in a discoverable 'scripts' subdirectory.
from scripts.etl_lib import read_target_catalog

# ======================================================================================
# CONFIGURATION
# ======================================================================================
MODULE_UI = "Mentors"

BASE_DIR = Path(__file__).resolve().parent
CACHE_DIR = BASE_DIR.parent / "cache"
OUTPUT_DIR = BASE_DIR.parent / "output"

MENTORS_CACHE_FILE = CACHE_DIR / "math_mentors_names.csv"
OUTPUT_CSV_FILE = OUTPUT_DIR / "Mentors.csv"

# This assumes the standard "Name" field for the "Mentors" module is "Mentor Name"
MENTOR_NAME_FIELD = "Mentor"


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
# MAIN EXECUTION
# ======================================================================================

def main() -> None:
    """Main script execution."""
    log.info(f"Starting {MODULE_UI} loader...")
    OUTPUT_DIR.mkdir(exist_ok=True)

    # 1. Load Inputs: Target Schema and Cached Mentor Names
    catalog = read_target_catalog()

    try:
        df_mentors = pd.read_csv(MENTORS_CACHE_FILE, dtype=str)
        log.info(f"Loaded {len(df_mentors)} mentor names from {MENTORS_CACHE_FILE.name}")
    except FileNotFoundError:
        log.error(f"Mentor cache file not found at: {MENTORS_CACHE_FILE}")
        log.error("Please run the Contacts loader first to generate this file.")
        return

    if df_mentors.empty:
        log.warning("Mentor cache file is empty. No records to create.")
        return

    # 2. Populate the Mentor Name field
    if "First Name" not in df_mentors.columns or "Last Name" not in df_mentors.columns:
        log.error("Cache file is missing 'First Name' or 'Last Name' columns.")
        return

    df_mentors[MENTOR_NAME_FIELD] = (
        df_mentors["First Name"].fillna("") + " " + df_mentors["Last Name"].fillna("")
    ).str.strip()
    log.info(f"Populated the '{MENTOR_NAME_FIELD}' field.")

    # 3. Align DataFrame with Target Schema
    try:
        ui_cols = catalog.query(
            "`User-Facing Module Name` == @MODULE_UI and "
            "not `Data Source / Type`.str.contains('Related List', na=False)"
        )["User-Facing Field Name"].tolist()

        if not ui_cols:
            raise ValueError(f"No columns found in catalog for module '{MODULE_UI}'.")

    except (KeyError, ValueError) as e:
        log.error(f"Could not find schema for module '{MODULE_UI}' in the target catalog. {e}")
        log.error("Please ensure the module and its fields are defined in 'Target modules_fields.csv'.")
        return

    # Add any missing columns from the schema to our DataFrame
    for col in ui_cols:
        if col not in df_mentors.columns:
            df_mentors[col] = pd.NA

    # Select and order columns according to the schema
    df_final = df_mentors[ui_cols]

    # 4. Save Final Output
    df_final.to_csv(OUTPUT_CSV_FILE, index=False)
    log.info(f"{MODULE_UI} loader complete. Output: {OUTPUT_CSV_FILE.name} ({len(df_final)} rows)")


if __name__ == "__main__":
    main()