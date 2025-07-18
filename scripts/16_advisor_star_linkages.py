#!/usr/bin/env python3
"""
Advisor-Star Associations Loader — National Math Stars CRM Migration
===================================================================
Creates UI-ready **Advisor-Star Associations** records and writes
`output/Advisor-Star Associations.csv`.

This script handles the migration of the junction object linking Advisors
to the Stars they mentor.

Flow
----
1. Load the legacy *Advisor Star Junction* export.
2. Map and rename columns according to the *Target-Legacy Mapping.csv*.
3. Clean fields: normalize dates, handle statuses, and apply custom overrides.
4. Merge `star_lookup.csv` to replace Star ID with Full Name.
5. Sort records by Star (Match Key) and then by Status ('Past' before 'Current').
6. Finalize column order based on the target catalog.
7. Write the UI-ready CSV to the output directory.
"""

from __future__ import annotations
import logging
from pathlib import Path

import pandas as pd
pd.options.mode.chained_assignment = None

# Assuming 'etl_lib.py' is in a 'scripts' subdirectory relative to the CWD
from scripts.helpers.etl_lib import (
    read_mapping,
    read_target_catalog,
    assert_target_pairs_exist,
    transform_legacy_df,
)

# ───────────────────────── CONFIG ──────────────────────────
BASE_DIR   = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR.parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

LEGACY_CSV = BASE_DIR.parent / "mapping" / "legacy-exports" / "Advisor_Star_Associations_2025_07_15.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────── MAIN ───────────────────────────

def main() -> None:
    """Main ETL script for Advisor-Star Associations."""

    # 1. LOAD LEGACY DATA
    log.info("Loading legacy data from: %s", LEGACY_CSV)
    try:
        df_raw = pd.read_csv(LEGACY_CSV, dtype=str)
    except FileNotFoundError:
        log.error("Legacy export not found at '%s'. Halting.", LEGACY_CSV)
        return

    log.info("Loaded %d raw records.", len(df_raw))

    # 2. MAP / RENAME PER MAPPING FILE
    module_name = "Advisor-Star Associations"
    log.info("Reading and validating mappings for '%s'...", module_name)

    mapping = read_mapping().query(f"`Target Module` == '{module_name}'")
    catalog = read_target_catalog()
    assert_target_pairs_exist(module_name, mapping, catalog)
    log.info("Mappings validated successfully.")

    df_ui = transform_legacy_df(df_raw, mapping)
    log.info("Legacy data transformed to target schema.")

    # 3. FIELD-LEVEL CLEANING & LOOKUPS
    # ----------------------------------------------------------------
    if 'Status' in df_ui.columns and 'End Date' in df_ui.columns:
        log.info("Applying 'End Date' logic for 'Current' status...")
        current_mask = df_ui['Status'].str.strip().eq('Current')
        df_ui.loc[current_mask, 'End Date'] = pd.NA
        log.info("Cleared 'End Date' for %d 'Current' records.", current_mask.sum())

    for col in ["Start Date", "End Date"]:
        if col in df_ui.columns:
            log.info("Normalizing '%s' to date-only format...", col)
            dt_series = pd.to_datetime(df_ui[col], errors='coerce')
            df_ui[col] = dt_series.dt.strftime('%Y-%m-%d')

    log.info("Applying custom date overrides for specific advisors...")
    overrides = {
        "Ambika Dani": {"Start Date": "2023-08-15"},
        "Zachary Chan": {"Start Date": "2024-08-15"},
        "Kayla Heyward": {"Start Date": "2025-01-13"},
        "Bilqis Taiwo": {"Start Date": "2024-08-15", "End Date": "2025-01-12"},
    }
    if 'Family Advisor (Match Key)' in df_ui.columns:
        for advisor, dates in overrides.items():
            mask = df_ui['Family Advisor (Match Key)'].str.strip() == advisor
            for date_col, date_val in dates.items():
                if date_col in df_ui.columns:
                    df_ui.loc[mask, date_col] = date_val
        log.info("Custom overrides applied.")

    # 4. MERGE STAR LOOKUP
    # ----------------------------------------------------------------
    log.info("Merging Star lookup cache...")
    CACHE_DIR = BASE_DIR.parent / "cache"
    LOOKUP_FILE = CACHE_DIR / "star_lookup.csv"

    if LOOKUP_FILE.exists():
        star_lu = pd.read_csv(LOOKUP_FILE, dtype=str)
        star_lu.rename(columns={"Full Name": "Star Full Name"}, inplace=True)
        
        df_ui = df_ui.merge(
            star_lu,
            left_on='Star (Match Key)',
            right_on='Record Id',
            how='left'
        )
        
        matched_count = df_ui["Star Full Name"].notna().sum()
        total_count = len(df_ui)
        log.info("Matched %d of %d records in the Star lookup.", matched_count, total_count)
        if matched_count < total_count:
            log.warning("%d records had no matching Star lookup.", total_count - matched_count)

        df_ui['Star (Match Key)'] = df_ui['Star Full Name']
        df_ui.drop(columns=['Record Id', 'Star Full Name'], inplace=True)
    else:
        log.error("Star lookup file not found at '%s'. Cannot replace IDs.", LOOKUP_FILE)

    # 5. SORTING
    # ----------------------------------------------------------------
    log.info("Sorting records by Star and then by Status...")
    if 'Status' in df_ui.columns and 'Star (Match Key)' in df_ui.columns:
        status_order = pd.CategoricalDtype(['Past', 'Current'], ordered=True)
        df_ui['Status'] = df_ui['Status'].astype(status_order)
        df_ui.sort_values(by=['Star (Match Key)', 'Status'], inplace=True)

    # 6. FINALIZE COLUMN ORDER
    log.info("Finalizing column order based on target catalog...")
    ui_cols = (catalog.query(f"`User-Facing Module Name` == '{module_name}'")
               ["User-Facing Field Name"].tolist())

    for col in ui_cols:
        if col not in df_ui.columns:
            df_ui[col] = pd.NA

    df_ui = df_ui[[c for c in ui_cols if c in df_ui.columns]]

    # 7. WRITE OUTPUT
    output_path = OUTPUT_DIR / "Advisor-Star Associations.csv"
    df_ui.to_csv(output_path, index=False)
    log.info("Wrote %s (%d rows)", output_path.name, len(df_ui))


if __name__ == "__main__":
    main()