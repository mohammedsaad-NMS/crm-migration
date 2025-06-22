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
* Generating final UI and API-ready CSV files.
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
    transform_legacy_df, ui_to_api_headers,
    to_int_if_whole, strip_translation, standardize_address_block,
)

# ───────────────────────── CONFIG ──────────────────────────
RECENCY_COL = "Modified Time"
BASE_DIR    = Path(__file__).resolve().parent
LEGACY_CSV  = BASE_DIR.parent / "mapping" / "legacy-exports" / "Districts___Schools_2025_06_20.csv"
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
    s = _RE_STRIP.sub("", s)
    s = _RE_NONALNUM.sub(" ", s)
    return " ".join(s.lower().split())

def clean_nces(val) -> str | None:
    if pd.isna(val):
        return None
    digits = re.sub(r"\D", "", str(val))
    return digits.zfill(7)[:7] if digits else None

def intelligent_title_case(text: str) -> str:
    """
    Applies title case but keeps ordinal suffixes like 'th', 'st' lowercase.
    e.g., "18Th Street" -> "18th Street"
    """
    if pd.isna(text):
        return text
    text = str(text).title()
    return re.sub(r'(\d+)(St|Nd|Rd|Th)', lambda m: m.group(1) + m.group(2).lower(), text, flags=re.I)

def district_title_case(text: str) -> str:
    """
    Applies intelligent title case and ensures 'ISD' remains uppercase.
    """
    if pd.isna(text):
        return ""
    titled_text = intelligent_title_case(str(text))
    return re.sub(r'\bIsd\b', 'ISD', titled_text, flags=re.I)


# ───────────────────── FUZZY MATCHER ─────────────────────

def fuzzy_match_name(row: pd.Series, ref_by_state, ref_all):
    """
    Finds the best fuzzy match for a district name and returns the official,
    un-normalized NCES name.
    """
    # Use the original legacy name for matching
    target = norm_name(row["Original Name"])
    state_key = row.get("STATE_FULL")

    # 1 — in‑state search
    cand = ref_by_state.get(state_key, pd.DataFrame())
    if not cand.empty:
        hit = process.extractOne(target, cand["LEA_NAME"].apply(norm_name),
                                 scorer=fuzz.WRatio, score_cutoff=85)
        if hit:
            return cand.iloc[hit[2]]["NCES Name"]

    # 2 — nationwide fallback
    hit = process.extractOne(target, ref_all["LEA_NAME"].apply(norm_name),
                             scorer=fuzz.WRatio, score_cutoff=90)
    if hit:
        return ref_all.iloc[hit[2]]["NCES Name"]

    return None

# ─────────────────────── MAIN ───────────────────────

def main() -> None:
    # 1. LOAD & PREP LEGACY DATA
    log.info("Loading and preparing legacy data...")
    # Load the source CSV and filter for 'District' type records
    df_raw = pd.read_csv(LEGACY_CSV, dtype=str)
    df_raw = df_raw[df_raw["Type"].str.strip().eq("District")]

    # Read the main mapping CSV and validate fields against the target catalog
    mapping = read_mapping().query("`Target Module` == 'Districts'")
    catalog = read_target_catalog()
    assert_target_pairs_exist("Districts", mapping, catalog)

    # Perform initial column renaming and transformations
    df_ui = transform_legacy_df(df_raw, mapping)
    df_ui[RECENCY_COL] = pd.to_datetime(df_raw[RECENCY_COL], errors="coerce")
    df_ui["NCES ID"] = df_raw["NCES District ID"].apply(clean_nces)
    # Preserve original name for matching before any modifications
    df_ui["Original Name"] = df_ui["Name"]

    if "State" not in df_ui.columns:
        df_ui["State"] = df_raw["State"].fillna("")

    # 2. LOAD & PREP CCD REFERENCE DATA
    log.info("Loading and preparing NCES CCD reference data...")
    # Load the official NCES Common Core of Data file
    ccd = pd.read_csv(CCD_CSV, dtype=str, low_memory=False)

    # Define the mapping from CCD source columns to our UI target columns
    ccd_to_ui: Dict[str, str] = {
        "LEA_NAME"      : "NCES Name",
        "STATENAME"     : "State",
        "MSTREET1"      : "Street",
        "MCITY"         : "City",
        "MZIP"          : "Zip Code",
        "PHONE"         : "Phone",
        "WEBSITE"       : "Website",
        "LEAID"         : "LEAID",
        "LEA_TYPE_TEXT" : "Type",
    }

    # Create the clean reference DataFrame with standardized column names
    ref = ccd[list(ccd_to_ui.keys())].rename(columns=ccd_to_ui)
    ref["LEAID"] = ref["LEAID"].apply(clean_nces)
    ref["LEA_NAME"] = ref["NCES Name"]
    ref['Type'] = ref['Type'].apply(lambda x: x.split(' that is not a component')[0] if pd.notna(x) else x)
    ref['State'] = ref['State'].str.title()
    ref['NCES State'] = ref['State'].str.title()

    # 3. ENRICHMENT: POPULATE AUTHORITATIVE DATA FROM CCD
    log.info("Enriching data with CCD information...")

    # Create efficient lookup dictionaries for matching by name and ID
    lookup = ref.drop_duplicates(subset=["NCES Name"]).set_index("NCES Name")
    id_to_name_map = ref.drop_duplicates(subset=["LEAID"]).set_index("LEAID")["NCES Name"]

    # Step 3a: Find official name for all records
    # First, attempt a direct match using the cleaned NCES ID
    df_ui["Official Name"] = df_ui["NCES ID"].map(id_to_name_map)

    # Prepare for fuzzy name matching on records that didn't have an ID match
    unmatched_mask = df_ui["Official Name"].isna()
    df_ui["STATE_FULL"] = df_ui["State"].str.title()
    ref_by_state = {s: g for s, g in ref.groupby("NCES State")}
    ref_all = ref.reset_index(drop=True)

    # Apply the fuzzy matching logic to the remaining unmatched records
    log.info(f"Fuzzy matching {unmatched_mask.sum()} records without a deterministic NCES ID match.")
    df_ui.loc[unmatched_mask, "Official Name"] = df_ui[unmatched_mask].apply(
        lambda r: fuzzy_match_name(r, ref_by_state, ref_all), axis=1)

    # Step 3b: Overwrite all relevant columns using the official name
    log.info("Overwriting legacy data with authoritative CCD data...")
    # Create a mask for all records that were successfully matched (by ID or fuzzy name)
    matched_mask = df_ui["Official Name"].notna()

    # Overwrite the 'Name' field with the official name from the CCD
    df_ui.loc[matched_mask, "Name"] = df_ui.loc[matched_mask, "Official Name"]

    # For all matched records, overwrite key fields with CCD data
    for ui_col in ("NCES ID", "Street", "City", "State", "Zip Code", "Phone", "Website", "Type"):
        source_col = "LEAID" if ui_col == "NCES ID" else ui_col
        if source_col in lookup.columns:
            df_ui.loc[matched_mask, ui_col] = df_ui.loc[matched_mask, "Name"].map(lookup[source_col])

    # 4. DEDUPLICATION (POST-ENRICHMENT)
    log.info(f"Deduplicating {len(df_ui)} records...")
    # Define a function to create a unique key for each district
    def district_key(r):
        state_part = str(r.get("State", "")).strip().title()
        return clean_nces(r["NCES ID"]) or f"{str(r['Name']).lower()}|{state_part}"

    # Apply the key and drop duplicates, keeping the most recently modified record
    df_ui["district_key"] = df_ui.apply(district_key, axis=1)
    latest = df_ui.sort_values(RECENCY_COL, na_position="first")\
                 .drop_duplicates("district_key", keep="last")
    log.info(f"Finished deduplication. {len(latest)} unique records remain.")

    # 5. FINAL FORMATTING
    log.info("Applying final formatting rules...")

    # Apply special title casing to the district name
    if "Name" in latest.columns:
        latest["Name"] = latest["Name"].apply(district_title_case)

    # Standardize the full address block
    standardize_address_block(latest, {
        "address_line_1": "Street", "city": "City", "state": "State", "postal_code": "Zip Code"
    })

    # Apply special title casing to the street address
    if "Street" in latest.columns:
        latest["Street"] = latest["Street"].apply(intelligent_title_case)

    # Generate a direct link to the district's page on the NCES website
    log.info("Generating NCES links...")
    NCES_URL_BASE = "https://nces.ed.gov/ccd/districtsearch/district_detail.asp?ID2="
    has_nces_id_mask = latest['NCES ID'].notna() & (latest['NCES ID'] != '')

    latest["NCES District Link"] = ""
    if has_nces_id_mask.any():
        latest.loc[has_nces_id_mask, 'NCES District Link'] = NCES_URL_BASE + latest.loc[has_nces_id_mask, 'NCES ID']
        log.info(f"Generated {has_nces_id_mask.sum()} NCES district links.")

    # Clean any remaining simple text fields
    if "District Size" in latest.columns:
        latest["District Size"] = to_int_if_whole(latest["District Size"])

    # 6. FINALIZE COLUMNS AND OUTPUT
    log.info("Finalizing columns for output...")
    # Get the final list of columns required for the UI from the catalog
    ui_cols = catalog.query("`User-Facing Module Name` == 'Districts'")\
                    .query("`Data Source / Type`.str.contains('Related List') == False")\
                    ["User-Facing Field Name"].tolist()

    # Ensure any columns in the spec but missing from the data are added as empty columns
    for col in ui_cols:
        if col not in latest.columns:
            latest[col] = pd.NA

    # Drop all temporary helper columns used during processing
    helper_cols = ["district_key", "STATE_FULL", "Original Name", "Official Name", "NCES State"]
    latest.drop(columns=helper_cols, inplace=True, errors='ignore')

    # Reorder columns to match the final specification
    latest = latest[[col for col in ui_cols if col in latest.columns]]

    # 7. WRITE OUTPUTS
    ui_path  = OUTPUT_DIR / "Districts_ui.csv"
    api_path = OUTPUT_DIR / "Districts_api.csv"

    # Write the UI-ready CSV
    latest.reset_index(drop=True).to_csv(ui_path, index=False)
    log.info(f"Wrote UI data to {ui_path}")

    # Write the API-ready CSV
    api_df = ui_to_api_headers(latest.reset_index(drop=True), "Districts", catalog)
    api_df.to_csv(api_path, index=False)
    log.info(f"Wrote API data to {api_path}")

if __name__ == "__main__":
    main()
