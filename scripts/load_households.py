"""
Households Loader — National Math Stars CRM Migration
----------------------------------------------------
This script processes the legacy Accounts export to create a clean list of
unique Household records.

The process includes:
* Filtering for 'Star' account types.
* Generating a deterministic key for each family to identify duplicates.
* Keeping only the most recent record for each family based on Cohort Entry Year.
* Aggregating all historical notes for a family into a single record.
* Standardizing and cleaning address and text fields.
* Generating final UI and API-ready CSV files.
"""

from __future__ import annotations
import logging
from pathlib import Path

import pandas as pd

pd.options.mode.chained_assignment = None

from scripts.etl_lib import (
    read_mapping, read_target_catalog, assert_target_pairs_exist,
    transform_legacy_df, ui_to_api_headers,
    to_int_if_whole, strip_translation,
    standardize_address_block,
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
    df_raw = pd.read_csv(ACCOUNTS_CSV)
    df_raw = df_raw[df_raw["Account Type"].str.strip().eq("Star")].copy()
    log.info(f"Filtered to {len(df_raw)} 'Star' account records.")

    df_raw["family_key"] = df_raw.apply(make_family_key, axis=1)
    df_raw = df_raw[df_raw["family_key"].notna()]
    log.info(f"Successfully generated family keys for {len(df_raw)} records.")

    mapping  = read_mapping().query("`Target Module` == 'Households'")
    catalog  = read_target_catalog()
    assert_target_pairs_exist("Households", mapping, catalog)

    df_ui = transform_legacy_df(df_raw, mapping)

    df_ui["family_key"] = df_raw["family_key"]
    df_ui[COHORT_COL]   = pd.to_numeric(df_raw[COHORT_COL], errors="coerce")

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
    
    latest_guard = (
        df_raw.sort_values(COHORT_COL, na_position="first")
              .groupby("family_key")
              .tail(1)
              .set_index("family_key")
    )
    
    # Use .loc to ensure we are modifying the 'latest' DataFrame safely
    latest["Household Name"] = latest_guard.apply(
        lambda r: f"{str(r['Primary Guardian First Name'])[0].upper()}. "
                  f"{str(r['Primary Guardian Last Name']).title()} Household",
        axis=1,
    )
    
    # Apply standard cleaners for text and numeric fields
    latest["Family Size"]               = to_int_if_whole(latest["Family Size"])
    latest["Highest Level of Education"] = latest["Highest Level of Education"].apply(strip_translation)
    latest["Special Circumstances"]      = latest["Special Circumstances"].apply(strip_translation)

    # Standardize the full address block
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
    latest = latest[[col for col in ui_cols if col in latest.columns]]

    # 5. WRITE OUTPUTS
    log.info("Writing output files...")
    
    # Write the UI-ready CSV
    ui_path = OUTPUT_DIR / "Households_ui.csv"
    latest.reset_index(drop=True).to_csv(ui_path, index=False)
    log.info(f"Wrote UI data to {ui_path}")

    # Write the API-ready CSV
    api_df = ui_to_api_headers(latest.reset_index(drop=True), "Households", catalog)
    api_path = OUTPUT_DIR / "Households_api.csv"
    api_df.to_csv(api_path, index=False)
    log.info(f"Wrote API data to {api_path}")

if __name__ == "__main__":
    main()