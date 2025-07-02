"""
Schools Loader — National Math Stars CRM Migration
============================================================
This script processes the legacy Schools export to create a clean and enriched
list of unique school records.

The process includes:
* Enriching records with official data from NCES public and private school extracts.
* Matching records first by a substring search on NCES ID, then by fuzzy name matching.
* Overwriting legacy data with authoritative data from the NCES extracts upon a successful match.
* Deduplicating records post-enrichment to keep only the most recently modified entry.
* Standardizing and formatting all fields, including addresses and names.
* Generating a direct NCES link for every school with an NCES ID.
* Generating final UI-ready CSV files.
* Automatically triggers a refresh of the districts data if new districts are found.
"""

from __future__ import annotations
import logging, re, subprocess
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
PROJECT_ROOT = BASE_DIR.parent
LEGACY_CSV  = BASE_DIR.parent / "mapping" / "legacy-exports" / "Districts___Schools_2025_07_02.csv"
PUBLIC_NCES_CSV  = BASE_DIR.parent / "reference" / "20250623 NCES Public School Extract.csv"
PRIVATE_NCES_CSV = BASE_DIR.parent / "reference" / "20250702 NCES Private School Extract.csv"
CACHE_DIR   = BASE_DIR.parent / "cache"
CACHE_DIR.mkdir(exist_ok=True)
OUTPUT_DIR  = BASE_DIR.parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# Path to the districts script to be triggered for logging purposes
DISTRICTS_SCRIPT_NAME = "load_districts.py"

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

def clean_public_nces_id(val) -> str | None:
    """Cleans and pads public NCES ID to 12 digits."""
    if pd.isna(val):
        return None
    digits = re.sub(r"\D", "", str(val))
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
    Finds the best match for a school using a tiered approach against the correct reference data.
    1. Substring match on incomplete legacy NCES ID.
    2. Fuzzy name match within the same state.
    3. Fuzzy name match nationwide.
    """
    school_type_to_match = row['School Type']
    ref_subset = ref_df[ref_df['School Type'] == school_type_to_match]
    if ref_subset.empty:
        return None

    legacy_id = row["Legacy NCES ID"]
    if pd.notna(legacy_id):
        id_match_subset = ref_subset[ref_subset["NCES ID"].astype(str).str.contains(str(legacy_id), na=False, regex=False)]
        if not id_match_subset.empty:
            if len(id_match_subset) == 1:
                return id_match_subset.index[0]
            target_name = norm_name(row["Original Name"])
            hit = process.extractOne(target_name, id_match_subset["norm_name"], scorer=fuzz.WRatio)
            if hit: return hit[2]

    target_name = norm_name(row["Original Name"])
    state_key = row.get("STATE_FULL")

    if state_key in state_lookup:
        cand_group = state_lookup[state_key]
        cand_df = cand_group[cand_group['School Type'] == school_type_to_match]
        if not cand_df.empty:
            hit = process.extractOne(target_name, cand_df["norm_name"], scorer=fuzz.WRatio, score_cutoff=90)
            if hit: return hit[2]

    hit = process.extractOne(target_name, ref_subset["norm_name"], scorer=fuzz.WRatio, score_cutoff=95)
    if hit: return hit[2]

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
    df_ui['School Type'] = 'Public'
    df_ui.loc[df_raw['School Type'] == 'Private', 'School Type'] = 'Private'

    df_ui[RECENCY_COL] = pd.to_datetime(df_raw[RECENCY_COL], errors="coerce")
    df_ui["Legacy NCES ID"] = df_raw["NCES School ID"]
    df_ui["Original Name"] = df_ui["School Name"]
    df_ui["State"] = df_raw["State"]
    df_ui["Record Id"] = df_raw["Record Id"]

    # 2. LOAD & PREP NCES REFERENCE DATA (PUBLIC & PRIVATE)
    log.info("Loading and preparing all NCES reference data...")

    public_map = { "School Name [Public School] 2023-24": "NCES Name", "School ID (12-digit) - NCES Assigned [Public School] Latest available year": "NCES ID", "State Name [Public School] 2023-24": "State", "Phone Number [Public School] 2023-24": "Phone", "Charter School [Public School] 2023-24": "Charter Status", "Locale [Public School] 2023-24": "Setting", "School Level (SY 2017-18 onward) [Public School] 2023-24": "Grades Served", "Total Students All Grades (Excludes AE) [Public School] 2023-24": "Size", "Location Address 1 [Public School] 2023-24": "Street", "Location City [Public School] 2023-24": "City", "Location ZIP [Public School] 2023-24": "Zip Code", "Web Site URL [Public School] 2023-24": "Website", "Agency ID - NCES Assigned [Public School] Latest available year": "District (Match Key)", }
    ref_pub = pd.read_csv(PUBLIC_NCES_CSV, dtype=str, usecols=public_map.keys(), low_memory=False).rename(columns=public_map)
    ref_pub['NCES ID'] = ref_pub['NCES ID'].apply(clean_public_nces_id)
    ref_pub['School Type'] = 'Public'
    ref_pub['Type'] = ref_pub.pop('Charter Status').map({'1-Yes': 'Charter', '2-No': 'Regular'})

    private_map = { "PINST": "NCES Name", "PPIN": "NCES ID", "PSTABB": "State", "PPHONE": "Phone", "ULOCALE22": "Setting", "LEVEL": "Grades Served", "NUMSTUDS": "Size", "PADDRS": "Street", "PCITY": "City", "PZIP": "Zip Code", }
    ref_priv = pd.read_csv(PRIVATE_NCES_CSV, dtype=str, usecols=private_map.keys(), low_memory=False).rename(columns=private_map)
    ref_priv['School Type'] = 'Private'
    ref_priv['Type'] = 'Private'

    grades_map = {'1': 'Elementary', '2': 'Secondary', '3': 'Combined elementary and secondary'}
    ref_priv['Grades Served'] = ref_priv['Grades Served'].map(grades_map)
    
    state_map = { 'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland', 'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District of Columbia' }
    ref_priv['State'] = ref_priv['State'].map(state_map)

    ref_all = pd.concat([ref_pub, ref_priv], ignore_index=True)
    ref_all["norm_name"] = ref_all["NCES Name"].apply(norm_name)
    ref_all['State'] = ref_all['State'].str.title()
    ref_all.loc[ref_all["Website"] == "†", "Website"] = pd.NA

    # 3. ENRICHMENT
    log.info("Enriching data with NCES information...")
    df_ui["STATE_FULL"] = df_ui["State"].str.title()
    ref_by_state = {s: g for s, g in ref_all.groupby("State")}
    df_ui["match_idx"] = df_ui.apply(lambda r: find_match_index(r, ref_all, ref_by_state), axis=1)

    log.info("Overwriting legacy data with authoritative NCES data...")
    matched_mask = df_ui["match_idx"].notna()
    cols_to_enrich = ["School Name", "NCES ID", "Street", "City", "State", "Zip Code", "Phone", "Website", "Type", "District (Match Key)", "Setting", "Size", "Grades Served"]
    for col_name in cols_to_enrich:
        target_col = col_name
        source_col = "NCES Name" if col_name == "School Name" else col_name
        if source_col in ref_all.columns:
            df_ui.loc[matched_mask, target_col] = df_ui.loc[matched_mask, "match_idx"].map(ref_all[source_col])

    # 4. DEDUPLICATION (POST-ENRICHMENT)
    log.info(f"Deduplicating {len(df_ui)} records...")
    def school_key(r):
        state_part = str(r.get("State", "")).strip().upper()
        return r["NCES ID"] or f"{str(r['School Name']).lower()}|{state_part}"

    df_ui["school_key"] = df_ui.apply(school_key, axis=1)
    latest = df_ui.sort_values(RECENCY_COL, na_position="first").drop_duplicates("school_key", keep="last")
    log.info(f"Finished deduplication. {len(latest)} unique records remain.")

    # 5. FINAL FORMATTING
    log.info("Applying final formatting rules...")
    latest["School Name"] = latest["School Name"].apply(intelligent_title_case)
    latest["Setting"] = latest["Setting"].str.extract(r"-\s*([^:]+):", expand=False).str.title()
    latest["Size"] = latest["Size"].apply(size_bucket)
    standardize_address_block(latest, { "address_line_1": "Street", "city": "City", "state": "State", "postal_code": "Zip Code" })
    latest["Phone"] = digits_only_phone(latest["Phone"])
    
    # 6. NCES LINK GENERATION (CONDITIONAL)
    log.info("Generating NCES links...")
    PUBLIC_URL_BASE = "https://nces.ed.gov/ccd/schoolsearch/school_detail.asp?ID="
    PRIVATE_URL_BASE = "https://nces.ed.gov/surveys/pss/privateschoolsearch/school_detail.asp?ID="

    latest["NCES School Link"] = ""
    
    is_private = latest['School Type'] == 'Private'
    is_public = ~is_private
    has_nces_id = latest['NCES ID'].notna() & (latest['NCES ID'] != '')

    latest.loc[is_public & has_nces_id, 'NCES School Link'] = PUBLIC_URL_BASE + latest.loc[is_public & has_nces_id, 'NCES ID']
    latest.loc[is_private & has_nces_id, 'NCES School Link'] = PRIVATE_URL_BASE + latest.loc[is_private & has_nces_id, 'NCES ID']
    log.info(f"Generated {has_nces_id.sum()} NCES school links.")

    lookup_df = latest[["Record Id", "School Name"]].copy()
    cache_path = CACHE_DIR / "school_lookup.csv"
    lookup_df.to_csv(cache_path, index=False)
    log.info(f"Wrote Record-Id ⇢ School-Name lookup to {cache_path}")

    # 7. FINALIZE COLUMNS AND OUTPUT
    log.info("Finalizing columns for output...")
    ui_cols = (catalog.query("`User-Facing Module Name` == 'Schools'")["User-Facing Field Name"].tolist())
    for col in ui_cols:
        if col not in latest.columns:
            latest[col] = pd.NA
    helper_cols = ["Record Id", "school_key", "STATE_FULL", "Original Name", "match_idx", "Legacy NCES ID", "School Type"]
    latest.drop(columns=helper_cols, inplace=True, errors='ignore')
    latest = latest[[col for col in ui_cols if col in latest.columns]]

    # 8. WRITE OUTPUTS
    ui_path = OUTPUT_DIR / "Schools.csv"
    latest.to_csv(ui_path, index=False)
    log.info(f"Wrote data to {ui_path}")

    # 9. CHECK FOR NEW DISTRICTS AND TRIGGER REFRESH
    log.info("Checking for new districts introduced by schools file...")
    districts_path = OUTPUT_DIR / "Districts.csv"
    if not districts_path.exists():
        log.warning("Districts.csv not found. Skipping district reconciliation.")
        return

    schools_df = pd.read_csv(ui_path, dtype=str)
    districts_df = pd.read_csv(districts_path, dtype=str)

    school_district_ids = set(schools_df["District (Match Key)"].dropna().unique())
    district_ids = set(districts_df["NCES ID"].dropna().unique())

    missing_district_ids = school_district_ids - district_ids

    if missing_district_ids:
        log.info(f"Found {len(missing_district_ids)} new district(s). Triggering full district load script...")
        try:
            # Execute the district script as a module to ensure correct pathing
            subprocess.run(
                ["python", "-m", "scripts.load_districts"], 
                check=True,
                cwd=PROJECT_ROOT # Set the correct working directory
            )
            log.info("District load script completed successfully.")
        except FileNotFoundError:
            log.error(f"Error: The script '{DISTRICTS_SCRIPT_NAME}' could not be found. Ensure it's in the 'scripts' folder.")
        except subprocess.CalledProcessError:
            log.error(f"The district load script ('{DISTRICTS_SCRIPT_NAME}') failed to execute successfully.")
    else:
        log.info("No new districts found. District data is up-to-date.")


if __name__ == "__main__":
    main()