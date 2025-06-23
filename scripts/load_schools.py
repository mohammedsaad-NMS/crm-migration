#!/usr/bin/env python3
"""
Schools Loader — National Math Stars CRM Migration
---------------------------------------------------
Reads the legacy combined districts/schools CSV, filters to individual schools,
unifies and cleans fields, enriches using NCES School ID first then fuzzy matching,
and writes:
  • output/Schools_ui.csv   – UI headers (all non-related-list fields + Matched School Name)
  • output/Schools_api.csv  – API headers for Zoho Bulk-Write

"""

from __future__ import annotations
import logging
import re
from pathlib import Path
from typing import Dict

import pandas as pd
from rapidfuzz import process, fuzz

pd.options.mode.chained_assignment = None
from scripts.etl_lib import (
    read_mapping,
    read_target_catalog,
    assert_target_pairs_exist,
    transform_legacy_df,
    ui_to_api_headers,
    strip_translation,
    standardize_address_block
)

# ───────────────────── NORMALISERS ─────────────────────
_RE_NONALNUM = re.compile(r"[^A-Za-z0-9 ]+")

def clean_nces(val) -> str | None:
    """Clean NCES identifiers to a zero-padded numeric string"""
    if pd.isna(val):
        return None
    digits = re.sub(r"\D", "", str(val))
    return digits.zfill(7)[:7] if digits else None


def norm_name(s: str) -> str:
    """Normalize names for fuzzy matching"""
    if pd.isna(s):
        return ""
    cleaned = _RE_NONALNUM.sub(" ", str(s))
    return " ".join(cleaned.lower().split())

# ───────────────────────── CONFIG ─────────────────────────
MODULE_UI = "Schools"
BASE_DIR = Path(__file__).resolve().parent
LEGACY_CSV = BASE_DIR.parent / "mapping" / "legacy-exports" / "Districts___Schools_2025_06_20.csv"
CCD_CSV    = BASE_DIR.parent / "reference" / "ccd_sch_029_2324_w_1a_073124.csv"
OUTPUT_DIR = BASE_DIR.parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

ADDRESS_COLS: Dict[str,str] = {
    'address_line_1': 'Street Address',
    'city'          : 'City',
    'state'         : 'State',
    'postal_code'   : 'Zip'
}

# ─────────────────────────── MAIN ───────────────────────────
def main() -> None:
    # 1. LOAD & PREP LEGACY DATA
    log.info("Loading and preparing legacy data...")
    df_raw = pd.read_csv(LEGACY_CSV, dtype=str)
    schools_raw = df_raw[df_raw["Type"].str.strip().eq("School")].copy()
    log.info(f"Filtered {len(schools_raw)} school records.")

    # 2. VALIDATE MAPPING
    mapping = read_mapping().query("`Target Module` == @MODULE_UI")
    catalog = read_target_catalog()
    assert_target_pairs_exist(MODULE_UI, mapping, catalog)

    # 3. TRANSFORM & CLEAN
    df_ui = transform_legacy_df(schools_raw, mapping)
    df_ui = df_ui.applymap(strip_translation)
    df_ui = standardize_address_block(df_ui, ADDRESS_COLS)

    # 4. ENRICH: NCES ID MATCH THEN FUZZY
    log.info("Loading NCES CCD reference data for enrichment...")
    ccd = pd.read_csv(CCD_CSV, dtype=str, low_memory=False)
    # Rename and prepare reference
    ref = ccd.rename(columns={
        'SCH_NAME': 'NCES Name',
        'STATENAME': 'State',
        'NCESSCH': 'NCES School ID'
    })
    ref['State'] = ref['State'].str.title()
    ref['Cleaned ID'] = ref['NCES School ID'].apply(clean_nces)
    ref['norm_NCES Name'] = ref['NCES Name'].apply(norm_name)
    # Build ID->Name lookup
    id_to_name = (
        ref.drop_duplicates(subset=['Cleaned ID'])
           .set_index('Cleaned ID')['NCES Name']
           .to_dict()
    )
    # Reset index and group by state for fuzzy
    ref = ref.reset_index(drop=True)
    ref_by_state = {s: grp.reset_index(drop=True) for s, grp in ref.groupby('State')}

    # Legacy ID is in column 'NCES School ID'
    schools_raw['Cleaned School ID'] = schools_raw['NCES School ID'].apply(clean_nces)
    # Deterministic match count
    df_ui['Matched School Name'] = schools_raw['Cleaned School ID'].map(id_to_name)
    num_deterministic = df_ui['Matched School Name'].notna().sum()
    log.info(f"Deterministic ID matches: {num_deterministic}")
    # Log entries that had an NCES ID but no deterministic match
    mask_had_id = schools_raw['Cleaned School ID'].notna()
    mask_failed = mask_had_id & df_ui['Matched School Name'].isna()
    num_failed = mask_failed.sum()
    if num_failed:
        failed = pd.DataFrame({
        'Name': df_ui.loc[mask_failed, 'Name'],
        'NCES School ID': schools_raw.loc[mask_failed, 'NCES School ID']
    })
        log.info(f"Entries with NCES ID but no deterministic match ({num_failed}):\n{failed.to_string(index=False)}")

    # Fuzzy fallback on unmatched
    mask_needs = df_ui['Matched School Name'].isna()
    def fuzzy_match_school(row: pd.Series) -> str | None:
        target = norm_name(row.get('Name', ''))
        state  = row.get('State', '')
        candidates = ref_by_state.get(state, pd.DataFrame())
        if not candidates.empty:
            hit = process.extractOne(
                target,
                candidates['norm_NCES Name'].tolist(),
                scorer=fuzz.WRatio,
                score_cutoff=85
            )
            if hit:
                return candidates.iloc[hit[2]]['NCES Name']
        hit = process.extractOne(
            target,
            ref['norm_NCES Name'].tolist(),
            scorer=fuzz.WRatio,
            score_cutoff=90
        )
        return ref.iloc[hit[2]]['NCES Name'] if hit else None

    df_ui.loc[mask_needs, 'Matched School Name'] = df_ui[mask_needs].apply(fuzzy_match_school, axis=1)
    num_fuzzy = df_ui['Matched School Name'].notna().sum() - num_deterministic
    log.info(f"Fuzzy matches: {num_fuzzy}")

    # 5. FINALIZE COLUMNS FOR OUTPUT
    ui_cols = (
        catalog
        .query("`User-Facing Module Name` == @MODULE_UI")
        .query("`Data Source / Type`.str.contains('Related List') == False")
        ['User-Facing Field Name']
        .tolist()
    )
    # Debug: print UI columns and actual df_ui columns
    log.info(f"UI columns from catalog: {ui_cols}")
    log.info(f"Actual df_ui columns before reorder: {df_ui.columns.tolist()}")
    # Insert enrichment column after 'Name'
    if 'Name' in ui_cols:
        ui_cols.insert(ui_cols.index('Name')+1, 'Matched School Name')
    for col in ui_cols:
        if col not in df_ui.columns:
            df_ui[col] = pd.NA
    df_ui = df_ui[[c for c in ui_cols if c in df_ui.columns]]

    # 6. WRITE UI CSV
    ui_path = OUTPUT_DIR / f"{MODULE_UI}_ui.csv"
    df_ui.reset_index(drop=True).to_csv(ui_path, index=False)
    log.info(f"Wrote UI data to {ui_path}")

    # 7. CONVERT TO API HEADERS & WRITE API CSV
    df_api = ui_to_api_headers(df_ui.copy(), MODULE_UI, catalog)
    api_path = OUTPUT_DIR / f"{MODULE_UI}_api.csv"
    df_api.to_csv(api_path, index=False)
    log.info(f"Wrote API data to {api_path}")

if __name__ == "__main__":
    main()
