#!/usr/bin/env python3
"""
Mentor Session Notes Loader — National Math Stars CRM Migration
================================================================
Transforms legacy Mentor Session Notes data, enriching it with cleaned data
from the `mentor_session_lookup.csv` and `star_lookup.csv` caches.

Key Logic:
1.  Loads the legacy Mentor Session Notes export.
2.  Discards a predefined list of test records and notes linked to test sessions.
3.  Loads lookup cache files for sessions and stars.
4.  Enriches the data by replacing IDs with full names for the Mentor Session Event,
    Mentor, and Star.
5.  Constructs the 'Mentor Session Note Name' via concatenation.
6.  Aligns the final data with the target schema for import.
"""

from __future__ import annotations
import logging
from pathlib import Path

import pandas as pd
pd.options.mode.chained_assignment = None

from scripts.etl_lib import (
    read_mapping,
    read_target_catalog,
    assert_target_pairs_exist,
    transform_legacy_df,
)

# ───────────────────────── CONFIG ──────────────────────────
BASE_DIR    = Path(__file__).resolve().parent
OUTPUT_DIR  = BASE_DIR.parent / "output"; OUTPUT_DIR.mkdir(exist_ok=True)
CACHE_DIR   = BASE_DIR.parent / "cache"; CACHE_DIR.mkdir(exist_ok=True)

LEGACY_CSV          = BASE_DIR.parent / "mapping" / "legacy-exports" / "Mentor_Session_Notes_2025_07_14.csv"
SESSION_LOOKUP_CACHE = CACHE_DIR / "mentor_session_lookup.csv"
STAR_LOOKUP_CACHE   = CACHE_DIR / "star_lookup.csv"
TARGET_MODULE       = "Mentor Session Notes"

DISCARD_IDS = {
    "zcrm_6229020000004953004", "zcrm_6229020000005765001", "zcrm_6229020000005765002",
    "zcrm_6229020000006458001", "zcrm_6229020000006458002", "zcrm_6229020000006464001",
    "zcrm_6229020000006464002", "zcrm_6229020000006825001", "zcrm_6229020000008784005",
    "zcrm_6229020000008785009", "zcrm_6229020000008785014", "zcrm_6229020000008809011",
    "zcrm_6229020000008828001", "zcrm_6229020000008830001", "zcrm_6229020000015616001",
    "zcrm_6229020000015625001", "zcrm_6229020000016273060", "zcrm_6229020000016441003",
    "zcrm_6229020000016461069", "zcrm_6229020000016502036", "zcrm_6229020000016516012",
    "zcrm_6229020000022164121", "zcrm_6229020000022200415",
}

DISCARD_SESSION_IDS = {
    "zcrm_6229020000002817001",
    "zcrm_6229020000003517001",
    "zcrm_6229020000003521001",
}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ───────────────────────── MAIN ─────────
def main() -> None:
    """Main execution function."""
    log.info(f"Starting {TARGET_MODULE} loader...")

    # 1. LOAD MAPPING & SCHEMA
    mapping = read_mapping().query("`Target Module` == @TARGET_MODULE")
    catalog = read_target_catalog()
    assert_target_pairs_exist(TARGET_MODULE, mapping, catalog)

    ui_cols = (
        catalog.query(
            "`User-Facing Module Name` == @TARGET_MODULE and "
            "not `Data Source / Type`.str.contains('Related List', na=False)"
        )["User-Facing Field Name"].tolist()
    )

    # 2. LOAD & FILTER DATA
    try:
        df_raw = pd.read_csv(LEGACY_CSV, dtype=str)
        log.info(f"Loaded {len(df_raw)} rows from {LEGACY_CSV.name}")
    except FileNotFoundError:
        log.error(f"Legacy export file not found at: {LEGACY_CSV}")
        return

    df_raw = df_raw[~df_raw["Record Id"].isin(DISCARD_IDS)]
    if "Session.id" in df_raw.columns:
        df_raw = df_raw[~df_raw["Session.id"].isin(DISCARD_SESSION_IDS)]

    # 3. LOAD CACHE FILES
    try:
        df_session_lookup = pd.read_csv(SESSION_LOOKUP_CACHE, dtype=str)
        df_star_lookup = pd.read_csv(STAR_LOOKUP_CACHE, dtype=str)
    except FileNotFoundError as e:
        log.error(f"A required cache file was not found: {e.name}")
        log.error("Please run the prerequisite loaders (Stars, Mentor Session Events) first.")
        return

    # 4. TRANSFORM & ENRICH
    df_ui = transform_legacy_df(df_raw, mapping)

    # Enrich Session and Mentor names
    event_name_map = df_session_lookup.set_index('Record Id')['Mentor Session Event Name'].to_dict()
    mentor_name_map = df_session_lookup.set_index('Record Id')['Mentor (Match Key)'].to_dict()
    df_ui["Mentor Session Event (Match Key)"] = df_raw["Session.id"].map(event_name_map)
    df_ui["Mentor (Match Key)"] = df_raw["Session.id"].map(mentor_name_map)
    
    # Enrich Star name
    star_name_map = df_star_lookup.set_index('Record Id')['Full Name'].to_dict()
    if "Star (Match Key)" in df_ui.columns:
        df_ui["Star (Match Key)"] = df_ui["Star (Match Key)"].map(star_name_map)
        log.info("Enriched Star names using star_lookup cache.")
        
    # Construct Mentor Session Note Name
    log.info("Constructing 'Mentor Session Note Name' from enriched fields...")
    df_ui["Mentor Session Note Name"] = (
        df_ui["Star (Match Key)"].fillna('') + ", " +
        df_ui["Mentor Session Event (Match Key)"].fillna('') + " notes"
    )
    # Clean up cases where a name might have been blank (e.g., ", Some Event notes")
    df_ui["Mentor Session Note Name"] = df_ui["Mentor Session Note Name"].str.replace(r'^, ', '', regex=True)

    # 5. ALIGN COLUMNS TO TARGET SCHEMA
    for col in ui_cols:
        if col not in df_ui.columns:
            df_ui[col] = pd.NA

    df_ui = df_ui[[c for c in ui_cols if c in df_ui.columns]]
    log.info("Aligned DataFrame columns with the target schema.")

    # 6. WRITE OUTPUT
    out_path = OUTPUT_DIR / f"{TARGET_MODULE}.csv"
    df_ui.to_csv(out_path, index=False)
    log.info(f"Wrote {len(df_ui)} rows to {out_path}")

if __name__ == "__main__":
    main()