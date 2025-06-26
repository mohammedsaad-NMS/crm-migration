"""
Schools Loader — National Math Stars CRM Migration
============================================================
This script processes the legacy Schools export to create a clean and enriched
list of unique school records.

The process includes:
* Enriching records with official data from the NCES School Extract.
* Matching records first by a substring search on NCES ID, then by fuzzy name matching.
* Overwriting legacy data with authoritative data from the NCES extract upon a successful match.
* Deduplicating records post-enrichment to keep only the most recently modified entry.
* Standardizing and formatting all fields, including addresses and names.
* Generating a direct NCES link for every school with an NCES ID.
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
    digits_only_phone
)

# ───────────────────────── CONFIG ──────────────────────────
RECENCY_COL = "Modified Time"
BASE_DIR    = Path(__file__).resolve().parent
LEGACY_CSV  = BASE_DIR.parent / "mapping" / "legacy-exports" / "Districts___Schools_2025_06_25.csv"
NCES_CSV    = BASE_DIR.parent / "reference" / "20250623 NCES School Extract.csv"
OUTPUT_DIR  = BASE_DIR.parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ───────────────────── NORMALISER & CLEANERS ─────────────────────
_RE_NONALNUM = re.compile(r"[^A-Za-z0-9 ]+")

def norm_name(s: str) -> str:
    if pd.isna(s): return ""
    s = _RE_NONALNUM.sub(" ", s)
    return " ".join(s.lower().split())

def digits_only(val) -> str | None:
    """Extracts only digits from a value."""
    if pd.isna(val):
        return None
    digits = re.sub(r"\D", "", str(val))
    return digits if digits else None

def clean_nces_id_ref(val) -> str | None:
    """Cleans and pads reference NCES ID to 12 digits."""
    digits = digits_only(val)
    return digits.zfill(12)[:12] if digits else None

def size_bucket(val) -> str | None:
    """Converts student count into a size category: Small, Medium, or Large."""
    try:
        n = int(val)
        return "Small" if n < 600 else "Medium" if n < 2000 else "Large"
    except (ValueError, TypeError):
        return None

# ───────────────────── MATCHER ─────────────────────

def find_match_index(row: pd.Series, ref_df: pd.DataFrame, state_lookup: dict) -> int | None:
    """
    Finds the best match for a school using a tiered approach and returns its unique index.
    1. Substring match on incomplete legacy NCES ID.
    2. Fuzzy name match within the same state.
    3. Fuzzy name match nationwide.
    """
    # Guard Clause: Do not attempt to match private schools against the public school extract.
    if row['School Type'] == 'Private':
        return None
        
    # Tier 1: Match on incomplete Legacy NCES ID
    legacy_id = row["Legacy NCES ID"]
    if legacy_id:
        subset = ref_df[ref_df["NCES ID"].str.contains(legacy_id, na=False, regex=False)]
        if not subset.empty:
            if len(subset) == 1:
                return subset.index[0]  # Return unique index of the single match
            
            # If multiple hits, fuzzy match the subset to find the best one
            target_name = norm_name(row["Original Name"])
            hit = process.extractOne(target_name, subset["norm_name"], scorer=fuzz.WRatio)
            if hit:
                return hit[2] # extractOne returns the index label

    # Tier 2 & 3: Fuzzy Name Match (if no ID match)
    target_name = norm_name(row["Original Name"])
    state_key = row.get("STATE_FULL")

    # In-state search (higher confidence)
    if state_key in state_lookup:
        cand = state_lookup[state_key]
        hit = process.extractOne(target_name, cand["norm_name"], scorer=fuzz.WRatio, score_cutoff=90)
        if hit:
            return hit[2] # Return index label of the match

    # Nationwide fallback (requires higher score)
    hit = process.extractOne(target_name, ref_df["norm_name"], scorer=fuzz.WRatio, score_cutoff=95)
    if hit:
        return hit[2] # Return index label of the match

    return None

# ─────────────────────── MAIN ───────────────────────

def main() -> None:
    # 1. LOAD & PREP LEGACY DATA
    log.info("Loading and preparing legacy school data...")
    df_raw = pd.read_csv(LEGACY_CSV, dtype=str)
    df_raw = df_raw[df_raw["Type"].str.strip().eq("School")]

    mapping = read_mapping().query("`Target Module` == 'Schools'")
    catalog = read_target_catalog()
    assert_target_pairs_exist("Schools", mapping, catalog)

    df_ui = transform_legacy_df(df_raw, mapping)
    df_ui['School Type'] = df_raw['School Type'] # Carry over for matching logic
    df_ui[RECENCY_COL] = pd.to_datetime(df_raw[RECENCY_COL], errors="coerce")
    df_ui["Legacy NCES ID"] = df_raw["NCES School ID"].apply(digits_only) # Keep incomplete ID for matching
    df_ui["Original Name"] = df_ui["Name"] # Preserve for matching
    if "State" not in df_ui.columns:
        df_ui["State"] = df_raw["State"] # Ensure State column exists for matching

    # 2. LOAD & PREP NCES REFERENCE DATA
    log.info("Loading and preparing NCES reference data...")
    nces_to_ui: Dict[str, str] = {
        "School Name [Public School] 2023-24": "NCES Name",
        "School ID (12-digit) - NCES Assigned [Public School] Latest available year": "NCES ID",
        "State Name [Public School] 2023-24": "State",
        "Phone Number [Public School] 2023-24": "Phone",
        "Charter School [Public School] 2023-24": "Charter Status", # New field for Type logic
        "Locale [Public School] 2023-24": "Setting",
        "School Level (SY 2017-18 onward) [Public School] 2023-24": "Grades Served",
        "Total Students All Grades (Excludes AE) [Public School] 2023-24": "Size",
        "Location Address 1 [Public School] 2023-24": "Street",
        "Location City [Public School] 2023-24": "City",
        "Location ZIP [Public School] 2023-24": "Zip Code",
        "Web Site URL [Public School] 2023-24": "Website",
        "Agency ID - NCES Assigned [Public School] Latest available year": "District (Dummy)",
    }
    ref = pd.read_csv(NCES_CSV, dtype=str, usecols=nces_to_ui.keys(), low_memory=False).rename(columns=nces_to_ui)
    
    # Clean and standardize reference data
    ref["NCES ID"] = ref["NCES ID"].apply(clean_nces_id_ref) # Pad to 12 digits
    ref["norm_name"] = ref["NCES Name"].apply(norm_name)
    ref['State'] = ref['State'].str.title() # Apply title case directly to the State column
    ref.loc[ref["Website"] == "†", "Website"] = pd.NA # Clear invalid websites

    # Create the 'Type' column based on the new logic
    type_map = {'1-Yes': 'Charter', '2-No': 'Regular'}
    ref['Type'] = ref.pop('Charter Status').map(type_map)

    # 3. ENRICHMENT: POPULATE AUTHORITATIVE DATA FROM NCES
    log.info("Enriching data with NCES information...")
    
    # Create lookups needed for matching
    df_ui["STATE_FULL"] = df_ui["State"].str.title()
    ref_by_state = {s: g for s, g in ref.groupby("State")} # Group by the corrected State column
    
    # Apply the matching function to get the index of the matched row
    df_ui["match_idx"] = df_ui.apply(
        lambda r: find_match_index(r, ref, ref_by_state), axis=1
    )
    
    # Overwrite legacy data using the matched index
    log.info("Overwriting legacy data with authoritative NCES data...")
    matched_mask = df_ui["match_idx"].notna()
    
    cols_to_enrich = ["Name", "NCES ID", "Street", "City", "State", "Zip Code", "Phone", "Website", "Type", "District (Dummy)", "Setting", "Size", "Grades Served"]
    
    for col_name in cols_to_enrich:
        target_col = col_name
        source_col = "NCES Name" if col_name == "Name" else col_name
        if source_col in ref.columns:
            # Use the unique match_idx to pull data from the correct row in the reference 'ref' dataframe
            df_ui.loc[matched_mask, target_col] = df_ui.loc[matched_mask, "match_idx"].map(ref[source_col])

    # 4. DEDUPLICATION (POST-ENRICHMENT)
    log.info(f"Deduplicating {len(df_ui)} records...")
    def school_key(r):
        state_part = str(r.get("State", "")).strip().upper()
        # Use the final, enriched NCES ID for the key
        return clean_nces_id_ref(r["NCES ID"]) or f"{str(r['Name']).lower()}|{state_part}"
    
    df_ui["school_key"] = df_ui.apply(school_key, axis=1)
    latest = df_ui.sort_values(RECENCY_COL, na_position="first").drop_duplicates("school_key", keep="last")
    log.info(f"Finished deduplication. {len(latest)} unique records remain.")

    # 5. FINAL FORMATTING
    log.info("Applying final formatting rules...")
    # Use the new, more robust function from the ETL library
    latest["Name"] = latest["Name"].apply(intelligent_title_case)

    # Apply school-specific business rules
    latest["Setting"] = latest["Setting"].str.extract(r"-\s*([^:]+):", expand=False).str.title()
    latest["Size"] = latest["Size"].apply(size_bucket)

    # Standardize address block using etl_lib function
    standardize_address_block(latest, {
        "address_line_1": "Street", "city": "City", "state": "State", "postal_code": "Zip Code"
    })

    latest["Phone"] = digits_only_phone(latest["Phone"])
    
    # Generate NCES Link
    log.info("Generating NCES links...")
    NCES_URL_BASE = "https://nces.ed.gov/ccd/schoolsearch/school_detail.asp?ID="
    has_nces_id = latest['NCES ID'].notna() & (latest['NCES ID'] != '')
    latest["NCES School Link"] = ""
    latest.loc[has_nces_id, 'NCES School Link'] = NCES_URL_BASE + latest.loc[has_nces_id, 'NCES ID']
    log.info(f"Generated {has_nces_id.sum()} NCES school links.")

    # 6. FINALIZE COLUMNS AND OUTPUT
    log.info("Finalizing columns for output...")
    # Get the final list of columns required for the UI from the catalog, excluding related lists
    ui_cols = (catalog.query("`User-Facing Module Name` == 'Schools'")
               .query("`Data Source / Type`.str.contains('Related List') == False "
                      "and `Data Source / Type`.str.contains('System') == False")
                      ["User-Facing Field Name"].tolist()
    )
    
    for col in ui_cols:
        if col not in latest.columns:
            latest[col] = pd.NA

    helper_cols = ["school_key", "STATE_FULL", "Original Name", "match_idx", "Legacy NCES ID", "School Type"]
    latest.drop(columns=helper_cols, inplace=True, errors='ignore')
    
    # Reorder columns to match the final specification
    latest = latest[[col for col in ui_cols if col in latest.columns]]

    # 7. WRITE OUTPUTS
    ui_path = OUTPUT_DIR / "Schools.csv"

    latest.to_csv(ui_path, index=False)
    log.info(f"Wrote data to {ui_path}")

if __name__ == "__main__":
    main()
