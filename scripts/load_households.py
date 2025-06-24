"""
Households Loader — National Math Stars CRM Migration
============================================================
This script processes the legacy Accounts export to create a clean list of
unique Household records.

The process includes:
* Filtering for 'Star' account types.
* Generating a deterministic key for each family to identify duplicates.
* Keeping only the most recent record for each family based on Cohort Entry Year.
* Aggregating all historical notes for a family into a single record.
* Standardizing and cleaning address and text fields.
* Generating final UI-ready CSV files.
"""

from __future__ import annotations
import logging
from pathlib import Path

import pandas as pd

pd.options.mode.chained_assignment = None

from scripts.etl_lib import (
    read_mapping, read_target_catalog, assert_target_pairs_exist,
    transform_legacy_df, to_int_if_whole, strip_translation,
    standardize_address_block, intelligent_title_case, # Import the enhanced function
)

# ───────────────────────── CONFIG ──────────────────────────
COHORT_COL   = "Cohort Entry Year"
BASE_DIR     = Path(__file__).resolve().parent
ACCOUNTS_CSV = BASE_DIR.parent / "mapping" / "legacy-exports" / "Accounts_2025_06_19.csv"
OUTPUT_DIR   = BASE_DIR.parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ───────────────────── HELPERS ──────────────────────
def make_family_key(row: pd.Series) -> str | None:
    """Generates a consistent key for a family based on guardian and zip."""
    fn = str(row.get("Primary Guardian First Name", "")).strip().lower()
    ln = str(row.get("Primary Guardian Last Name", "")).strip().lower()
    zp = str(row.get("Primary Guardian Zip", "")).strip()
    if not fn or not ln or not zp or fn in ("nan", "none") or ln in ("nan", "none"):
        return None
    return f"{fn[0]}|{ln}|{zp}"

# ──────────────────────── MAIN ───────────────────────
def main() -> None:
    # 1. LOAD & PREP LEGACY DATA
    log.info("Loading and preparing legacy Accounts data...")
    # Use dtype=str to prevent pandas from making assumptions about data types
    df_raw = pd.read_csv(ACCOUNTS_CSV, dtype=str) 
    df_raw = df_raw[df_raw["Account Type"].str.strip().eq("Star")].copy()
    log.info(f"Filtered to {len(df_raw)} 'Star' account records.")

    df_raw[COHORT_COL] = pd.to_numeric(df_raw[COHORT_COL], errors="coerce")
    df_raw["family_key"] = df_raw.apply(make_family_key, axis=1)
    df_raw = df_raw[df_raw["family_key"].notna()]
    log.info(f"Successfully generated family keys for {len(df_raw)} records.")

    mapping  = read_mapping().query("`Target Module` == 'Households'")
    catalog  = read_target_catalog()
    assert_target_pairs_exist("Households", mapping, catalog)

    df_ui = transform_legacy_df(df_raw, mapping)

    df_ui["family_key"] = df_raw["family_key"]
    df_ui[COHORT_COL]   = df_raw[COHORT_COL]

    # 2. DEDUPLICATION & AGGREGATION
    log.info("Deduplicating records to keep the most recent for each family...")
    
    # Sort by cohort year to identify the latest record for each family
    latest = (
        df_ui.sort_values(COHORT_COL, na_position="first")
             .groupby("family_key", as_index=False)
             .tail(1)
             .set_index("family_key")
    )
    log.info(f"Finished deduplication. {len(latest)} unique household records remain.")
    
    # Aggregate notes from all historical records for each family
    notes_series = (
        df_ui.groupby("family_key")["Special Circumstances"]
             .apply(lambda s: "; ".join(s.dropna().unique()))
    )
    latest["Notes"] = notes_series

    # 3. FINAL FORMATTING & CLEANING
    log.info("Applying final formatting and cleaning rules...")
    
    # To create the household name, we need the latest guardian names from the raw data
    latest_guardian_info = (
        df_raw.sort_values(COHORT_COL, na_position="first")
              .groupby("family_key")
              .tail(1)
              .set_index("family_key")
    )

    # Clean the guardian names from the raw data
    first_name_clean = latest_guardian_info["Primary Guardian First Name"].apply(intelligent_title_case)
    last_name_clean = latest_guardian_info["Primary Guardian Last Name"].apply(intelligent_title_case)

    # Create the Household Name series, ensuring we handle potential missing names
    household_name_series = (
        first_name_clean.str[0].str.upper() + ". " +
        last_name_clean + " Household"
    )
    
    # Assign the new series to the 'latest' DataFrame, aligning by the family_key index
    latest["Household Name"] = household_name_series
    
    # Apply standard cleaners for text and numeric fields
    latest["Family Size"] = to_int_if_whole(latest["Family Size"])
    for col in ["Highest Level of Education", "Special Circumstances"]:
        if col in latest.columns:
            latest[col] = latest[col].apply(strip_translation)

    # Standardize the full address block, which now uses intelligent_title_case internally
    standardize_address_block(latest, {
        "address_line_1": "Street",
        "city"          : "City",
        "state"         : "State",
        "postal_code"   : "Zip Code",
    })

    # 4. FINALIZE COLUMNS FOR OUTPUT
    log.info("Finalizing columns for output...")
    ui_cols = (
        catalog.query("`User-Facing Module Name` == 'Households'")
               .query("`Data Source / Type`.str.contains('Related List') == False")
               ["User-Facing Field Name"].tolist()
    )
    for col in ui_cols:
        if col not in latest.columns:
            latest[col] = pd.NA
    
    # Add family_key to the list of columns to drop
    helper_cols = ["family_key"]
    latest.drop(columns=helper_cols, inplace=True, errors='ignore')
    
    latest = latest[[col for col in ui_cols if col in latest.columns]]

    # 5. WRITE OUTPUTS
    log.info("Writing output files...")
    
    # Write the UI-ready CSV
    ui_path = OUTPUT_DIR / "Households.csv"
    latest.reset_index(drop=True).to_csv(ui_path, index=False)
    log.info(f"Wrote data to {ui_path}")

if __name__ == "__main__":
    main()
