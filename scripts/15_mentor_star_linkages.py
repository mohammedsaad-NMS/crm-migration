#!/usr/bin/env python3
"""
Mentor-Star Associations Loader — National Math Stars CRM Migration
=================================================================
Creates UI-ready **Mentor-Star Associations** records and writes
`output/Mentor-Star Associations.csv`.

This script handles the migration of the junction object linking Mentors
to the Stars they guide.

Flow
----
1. Load legacy data, remove test records, and filter out blank associations.
2. Map and rename columns according to the *Target-Legacy Mapping.csv*.
3. Set the Owner for all records.
4. Merge `star_lookup.csv` to replace Star ID with Full Name.
5. Sort records, calculate the correct End Date, and format date fields.
5b. Apply "Current" status logic to nullify invalid future-dated records.
5c. Ensure "Current" records have a blank End Date.
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
CACHE_DIR = BASE_DIR.parent / "cache"

LEGACY_CSV = BASE_DIR.parent / "mapping" / "legacy-exports" / "Mentor_Star_Associations_2025_07_15.csv"
STAR_LOOKUP_FILE = CACHE_DIR / "star_lookup.csv"
# --- NEW --- Path for the mentor lookup file
MENTOR_LOOKUP_FILE = CACHE_DIR / "mentor_lookup.csv"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────── MAIN ───────────────────────────

def main() -> None:
    """Main ETL script for Mentor-Star Associations."""

    # 1. LOAD LEGACY DATA & FILTER
    log.info("Loading legacy data from: %s", LEGACY_CSV)
    try:
        df_raw = pd.read_csv(LEGACY_CSV, dtype=str)
    except FileNotFoundError:
        log.error("Legacy export not found at '%s'. Halting.", LEGACY_CSV)
        return

    log.info("Loaded %d raw records.", len(df_raw))

    # 1a. Remove test records
    if 'National Math Star' in df_raw.columns:
        initial_rows = len(df_raw)
        test_mask = df_raw['National Math Star'].str.contains('Test', case=False, na=False)
        df_raw = df_raw[~test_mask]
        rows_removed = initial_rows - len(df_raw)
        if rows_removed > 0:
            log.info("Removed %d test records.", rows_removed)

    # 1b. Remove rows where either Star or Mentor are blank
    if 'National Math Star' in df_raw.columns and 'Math Mentor' in df_raw.columns:
        initial_rows = len(df_raw)
        star_blank_mask = df_raw['National Math Star'].fillna('').str.strip() == ''
        mentor_blank_mask = df_raw['Math Mentor'].fillna('').str.strip() == ''
        either_blank_mask = star_blank_mask | mentor_blank_mask
        df_raw = df_raw[~either_blank_mask]
        rows_removed = initial_rows - len(df_raw)
        if rows_removed > 0:
            log.info("Removed %d records where either the Star or Mentor was blank.", rows_removed)

    # 2. MAP / RENAME PER MAPPING FILE
    module_name = "Mentor-Star Associations"
    log.info("Reading and validating mappings for '%s'...", module_name)
    mapping = read_mapping().query(f"`Target Module` == '{module_name}'")
    catalog = read_target_catalog()
    assert_target_pairs_exist(module_name, mapping, catalog)
    log.info("Mappings validated successfully.")
    df_ui = transform_legacy_df(df_raw, mapping)
    log.info("Legacy data transformed to target schema.")

    # 3. SET OWNER
    log.info("Setting Owner to 'Al Lucero' for all records...")
    df_ui['Mentor-Star Association Owner'] = 'Al Lucero'

    # 4. MERGE STAR LOOKUP
    log.info("Merging Star lookup cache...")

    if STAR_LOOKUP_FILE.exists():
        star_lu = pd.read_csv(STAR_LOOKUP_FILE, dtype=str)
        star_lu.rename(columns={"Full Name": "Star Full Name"}, inplace=True)
        df_ui = df_ui.merge(star_lu, left_on='Star (Match Key)', right_on='Record Id', how='left')
        matched_count = df_ui["Star Full Name"].notna().sum()
        total_count = len(df_ui)
        log.info("Matched %d of %d records in the Star lookup.", matched_count, total_count)
        if matched_count < total_count:
            log.warning("%d records had no matching Star lookup.", total_count - matched_count)
        df_ui['Star (Match Key)'] = df_ui['Star Full Name'].fillna(df_ui['Star (Match Key)'])
        df_ui.drop(columns=['Record Id', 'Star Full Name'], inplace=True, errors='ignore')
    else:
        log.error("Star lookup file not found at '%s'. Cannot replace IDs.", STAR_LOOKUP_FILE)

    # --- NEW: Step 4.5 ---
    # 4.5 REPLACE MENTOR ID WITH FULL NAME
    log.info("Replacing Mentor IDs with full names from cache...")
    if MENTOR_LOOKUP_FILE.exists():
        try:
            df_mentor_lookup = pd.read_csv(MENTOR_LOOKUP_FILE, dtype=str)
            # Create a 'Full Name' column in the lookup
            df_mentor_lookup['Full Name'] = (
                df_mentor_lookup['First Name'].fillna('') + ' ' + df_mentor_lookup['Last Name'].fillna('')
            ).str.strip()
            
            # Create a mapping from Record Id to Full Name
            mentor_name_map = pd.Series(
                df_mentor_lookup['Full Name'].values,
                index=df_mentor_lookup['Record Id']
            ).to_dict()

            # Apply the mapping to the 'Mentor (Match Key)' column
            df_ui['Mentor (Match Key)'] = df_ui['Mentor (Match Key)'].map(mentor_name_map)
            log.info("Successfully mapped Mentor IDs to full names.")

        except FileNotFoundError:
             log.warning(f"Mentor lookup file not found at '{MENTOR_LOOKUP_FILE}'. Skipping mentor name enrichment.")
        except Exception as e:
            log.error(f"An error occurred during mentor name enrichment: {e}")
    else:
        log.error("Mentor lookup file not found at '%s'. Cannot replace Mentor IDs.", MENTOR_LOOKUP_FILE)
    # --- END NEW ---

    # 5. SORTING & END DATE CALCULATION
    log.info("Sorting records and calculating End Dates...")

    # Convert Start Date to datetime for sorting and calculations
    df_ui['Start Date'] = pd.to_datetime(df_ui['Start Date'], errors='coerce')

    # Sort by Star, then by the Start Date to ensure correct chronological order
    df_ui.sort_values(by=['Star (Match Key)', 'Start Date'], inplace=True)

    # Calculate End Date: It is one day before the next association's Start Date for that same Star.
    # The shift(-1) looks ahead to the next record within each Star's group.
    df_ui['End Date'] = df_ui.groupby('Star (Match Key)')['Start Date'].shift(-1) - pd.Timedelta(days=1)

    log.info("End Dates calculated based on subsequent pairings.")

    # Format dates to 'YYYY-MM-DD', leaving End Date blank for current (last) records.
    df_ui['Start Date'] = df_ui['Start Date'].dt.strftime('%Y-%m-%d')
    df_ui['End Date'] = df_ui['End Date'].dt.strftime('%Y-%m-%d')

    # 5b. APPLY "CURRENT" STATUS LOGIC
    log.info("Applying 'Current' status logic to nullify invalid future-dated records...")
    
    # Create a map of Star -> Start Date for all "Current" records
    # Because dates are now 'YYYY-MM-DD' strings, they sort chronologically correctly.
    current_start_dates = df_ui[df_ui['Status'] == 'Current'].set_index('Star (Match Key)')['Start Date']
    
    # Map this "Current Start Date" to all records
    df_ui['Current Start Date'] = df_ui['Star (Match Key)'].map(current_start_dates)
    
    # Define the mask for records to be nullified:
    # 1. The Star must have a "Current" association.
    # 2. The record's Start Date must be after the "Current" association's Start Date.
    # 3. The record itself is not the "Current" one.
    mask = (
        df_ui['Current Start Date'].notna() &
        (df_ui['Start Date'] > df_ui['Current Start Date']) &
        (df_ui['Status'] != 'Current')
    )
    
    num_to_nullify = mask.sum()
    if num_to_nullify > 0:
        log.info("Found %d future-dated records for Stars with a 'Current' status. Nullifying their dates.", num_to_nullify)
        df_ui.loc[mask, ['Start Date', 'End Date']] = pd.NA

    # Clean up the temporary helper column
    df_ui.drop(columns=['Current Start Date'], inplace=True)

    # 5c. ENSURE "CURRENT" RECORDS HAVE BLANK END DATE
    log.info("Ensuring all 'Current' records have a blank End Date.")
    df_ui.loc[df_ui['Status'] == 'Current', 'End Date'] = pd.NA


    # 6. FINALIZE COLUMN ORDER
    log.info("Finalizing column order based on target catalog...")
    ui_cols = (catalog.query(f"`User-Facing Module Name` == '{module_name}'")["User-Facing Field Name"].tolist())
    for col in ui_cols:
        if col not in df_ui.columns:
            df_ui[col] = pd.NA
    df_ui = df_ui[[c for c in ui_cols if c in df_ui.columns]]

    # 7. WRITE OUTPUT
    output_path = OUTPUT_DIR / "Mentor-Star Associations.csv"
    df_ui.to_csv(output_path, index=False)
    log.info("Wrote %s (%d rows)", output_path.name, len(df_ui))

if __name__ == "__main__":
    main()