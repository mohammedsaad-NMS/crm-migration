"""
Districts Loader — National Math Stars CRM Migration
============================================================
This script processes the legacy Districts export to create a clean and enriched
list of unique district records.

The process includes:
* Enriching records with official data from the NCES Common Core of Data (CCD).
* Matching records first by NCES ID, then by fuzzy name matching.
* Overwriting legacy data with authoritative data from the CCD upon a successful match.
* Deduplicating records post-enrichment to keep only the most recently modified entry.
* Standardizing and formatting all fields, including addresses and names.
* Generating a direct NCES link for every district with an NCES ID.
* Generating final UI-ready CSV files.
"""

from __future__ import annotations
import logging, re
from pathlib import Path
from typing import Dict

import pandas as pd
from rapidfuzz import process, fuzz

pd.options.mode.chained_assignment = None

from scripts.etl_lib import (
    read_mapping, read_target_catalog, assert_target_pairs_exist,
    transform_legacy_df, standardize_address_block, intelligent_title_case,
)

# ───────────────────────── CONFIG ──────────────────────────
RECENCY_COL = "Modified Time"
BASE_DIR    = Path(__file__).resolve().parent
LEGACY_CSV  = BASE_DIR.parent / "mapping" / "legacy-exports" / "Districts___Schools_2025_06_23.csv"
CCD_CSV     = BASE_DIR.parent / "reference" / "ccd_lea_029_2324_w_1a_073124.csv"
OUTPUT_DIR  = BASE_DIR.parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ───────────────────── NORMALISER & CLEANERS ─────────────────────
_RE_STRIP    = re.compile(r"\b(ISD|I\.S\.D\.|SD|School District)\b", re.I)
_RE_NONALNUM = re.compile(r"[^A-Za-z0-9 ]+")

def norm_name(s: str) -> str:
    if pd.isna(s): return ""
    s = _RE_STRIP.sub("", s)
    s = _RE_NONALNUM.sub(" ", s)
    return " ".join(s.lower().split())

def clean_nces_id(val) -> str | None:
    if pd.isna(val):
        return None
    digits = re.sub(r"\D", "", str(val))
    return digits.zfill(7)[:7] if digits else None

# ───────────────────── MATCHER ─────────────────────

def find_match_index(row: pd.Series, ref_df: pd.DataFrame, id_lookup: dict, state_lookup: dict) -> int | None:
    """
    Finds the best match for a district using a tiered approach and returns its unique index.
    1. Exact match on NCES ID.
    2. Fuzzy name match within the same state.
    3. Fuzzy name match nationwide.
    """
    # Tier 1: Match on NCES ID
    nces_id = row["NCES ID"]
    if nces_id in id_lookup:
        return id_lookup[nces_id]

    # Tier 2 & 3: Fuzzy Name Match (if no ID match)
    target_name = norm_name(row["Original Name"])
    state_key = row.get("STATE_FULL")

    # In-state search
    if state_key in state_lookup:
        cand = state_lookup[state_key]
        hit = process.extractOne(target_name, cand["norm_name"], scorer=fuzz.WRatio, score_cutoff=85)
        if hit:
            return hit[2]

    # Nationwide fallback
    hit = process.extractOne(target_name, ref_df["norm_name"], scorer=fuzz.WRatio, score_cutoff=90)
    if hit:
        return hit[2]

    return None

# ─────────────────────── MAIN ───────────────────────

def main() -> None:
    # 1. LOAD & PREP LEGACY DATA
    log.info("Loading and preparing legacy data...")
    df_raw = pd.read_csv(LEGACY_CSV, dtype=str)
    df_raw = df_raw[df_raw["Type"].str.strip().eq("District")]

    mapping = read_mapping().query("`Target Module` == 'Districts'")
    catalog = read_target_catalog()
    assert_target_pairs_exist("Districts", mapping, catalog)

    df_ui = transform_legacy_df(df_raw, mapping)
    df_ui[RECENCY_COL] = pd.to_datetime(df_raw[RECENCY_COL], errors="coerce")
    df_ui["NCES ID"] = df_raw["NCES District ID"].apply(clean_nces_id)
    df_ui["Original Name"] = df_ui["Name"]

    if "State" not in df_ui.columns:
        df_ui["State"] = df_raw["State"].fillna("")

    # 2. LOAD & PREP CCD REFERENCE DATA
    log.info("Loading and preparing NCES CCD reference data...")
    ccd_to_ui: Dict[str, str] = {
        "LEA_NAME"      : "Name",
        "STATENAME"     : "State",
        "MSTREET1"      : "Street",
        "MCITY"         : "City",
        "MZIP"          : "Zip Code",
        "PHONE"         : "Phone",
        "WEBSITE"       : "Website",
        "LEAID"         : "NCES ID",
        "LEA_TYPE_TEXT" : "Type",
    }
    ref = pd.read_csv(CCD_CSV, dtype=str, usecols=ccd_to_ui.keys(), low_memory=False).rename(columns=ccd_to_ui)
    
    ref["NCES ID"] = ref["NCES ID"].apply(clean_nces_id)
    ref["norm_name"] = ref["Name"].apply(norm_name)
    ref['State'] = ref['State'].str.title()
    ref['Type'] = ref['Type'].str.split(' that is not a component').str[0]

    # 3. ENRICHMENT: POPULATE AUTHORITATIVE DATA FROM CCD
    log.info("Enriching data with CCD information...")
    
    # Create lookups for matching
    df_ui["STATE_FULL"] = df_ui["State"].str.title()
    id_to_idx_map = ref.drop_duplicates(subset=["NCES ID"]).set_index("NCES ID").index.get_indexer(ref.set_index("NCES ID").index)
    id_lookup = dict(zip(ref["NCES ID"].dropna(), ref.index))

    ref_by_state = {s: g for s, g in ref.groupby("State")}

    # Apply the matching function to get the index of the matched row
    df_ui["match_idx"] = df_ui.apply(
        lambda r: find_match_index(r, ref, id_lookup, ref_by_state), axis=1
    )
    
    # Overwrite legacy data using the matched index
    log.info("Overwriting legacy data with authoritative CCD data...")
    matched_mask = df_ui["match_idx"].notna()
    
    cols_to_enrich = ["Name", "NCES ID", "Street", "City", "State", "Zip Code", "Phone", "Website", "Type"]
    
    for col in cols_to_enrich:
        if col in ref.columns:
            df_ui.loc[matched_mask, col] = df_ui.loc[matched_mask, "match_idx"].map(ref[col])

    # 4. DEDUPLICATION (POST-ENRICHMENT)
    log.info(f"Deduplicating {len(df_ui)} records...")
    def district_key(r):
        state_part = str(r.get("State", "")).strip().title()
        return clean_nces_id(r["NCES ID"]) or f"{str(r['Name']).lower()}|{state_part}"

    df_ui["district_key"] = df_ui.apply(district_key, axis=1)
    latest = df_ui.sort_values(RECENCY_COL, na_position="first").drop_duplicates("district_key", keep="last")
    log.info(f"Finished deduplication. {len(latest)} unique records remain.")

    # 5. FINAL FORMATTING
    log.info("Applying final formatting rules...")
    latest["Name"] = latest["Name"].apply(intelligent_title_case)

    # Standardize the full address block
    standardize_address_block(latest, {
        "address_line_1": "Street", "city": "City", "state": "State", "postal_code": "Zip Code"
    })

    # Generate a direct link to the district's page on the NCES website
    log.info("Generating NCES links...")
    NCES_URL_BASE = "https://nces.ed.gov/ccd/districtsearch/district_detail.asp?ID2="
    has_nces_id_mask = latest['NCES ID'].notna() & (latest['NCES ID'] != '')

    latest["NCES District Link"] = ""
    if has_nces_id_mask.any():
        latest.loc[has_nces_id_mask, 'NCES District Link'] = NCES_URL_BASE + latest.loc[has_nces_id_mask, 'NCES ID']
        log.info(f"Generated {has_nces_id_mask.sum()} NCES district links.")

    # 6. FINALIZE COLUMNS AND OUTPUT
    log.info("Finalizing columns for output...")
    ui_cols = (catalog.query("`User-Facing Module Name` == 'Districts'")
               .query("`Data Source / Type`.str.contains('Related List') == False "
                      "and `Data Source / Type`.str.contains('System') == False")
                      ["User-Facing Field Name"].tolist()
    )

    for col in ui_cols:
        if col not in latest.columns:
            latest[col] = pd.NA

    helper_cols = ["district_key", "STATE_FULL", "Original Name", "match_idx"]
    latest.drop(columns=helper_cols, inplace=True, errors='ignore')

    latest = latest[[col for col in ui_cols if col in latest.columns]]

    # 7. WRITE OUTPUTS
    ui_path  = OUTPUT_DIR / "Districts.csv"

    latest.to_csv(ui_path, index=False)
    log.info(f"Wrote data to {ui_path}")

if __name__ == "__main__":
    main()