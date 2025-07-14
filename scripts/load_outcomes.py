#!/usr/bin/env python3
"""
Outcomes Loader — National Math Stars CRM Migration
===================================================
Transforms legacy Outcomes data into the target Outcomes module, filtering for specific types
and applying custom cleaning, concatenation, and Star name enrichment.
"""

from __future__ import annotations
import logging
from pathlib import Path
from typing import List

import pandas as pd
pd.options.mode.chained_assignment = None

from scripts.etl_lib import (
    read_mapping, read_target_catalog, assert_target_pairs_exist,
    transform_legacy_df
)

# ───────── CONFIG ─────────
BASE        = Path(__file__).resolve().parent
OUTPUT_DIR  = BASE.parent / "output"
CACHE_DIR   = BASE.parent / "cache"
LEGACY_CSV  = BASE.parent / "mapping" / "legacy-exports" / "Outcomes_2025_07_13.csv"
STAR_LOOKUP_FILE = CACHE_DIR / "star_lookup.csv"
OUTPUT_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

logging.basicConfig(level="INFO",
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

# ----------------- DATA CLEANING RULES -----------------
OUTCOME_TYPES_TO_KEEP = [
    'AMC12', 'AMC10', 'AMC8', 'Competition',
    'Math Kangaroo', 'Other Assessment', 'Award, Scholarship'
]

OUTCOME_TYPE_REMAPPING = {
    'AMC12': 'Competition',
    'AMC10': 'Competition',
    'AMC8': 'Competition',
    'Math Kangaroo': 'Competition'
}

AMC_FIELDS = {
    "AMC8": {"AMC8 Score": "Score", "AMC8 Percentile": "Percentile"},
    "AMC10": {"AMC10 Score": "Score", "AMC10 Percentile": "Percentile"},
    "AMC12": {"AMC12 Score": "Score", "AMC12 Percentile": "Percentile"},
}
DESCRIPTION_COLS = ["Outcome Description"] + [col for sublist in AMC_FIELDS.values() for col in sublist.keys()]

def standardize_outcome_name(name: str) -> str:
    if name.endswith(" Scores"):
        name = name[:-7].strip()
    if not name.endswith(" Results"):
        name = f"{name} Results"
    return name

# ----------------- MAIN -----------------
def main() -> None:
    # 0. Load mapping and catalog files
    mapping_full = read_mapping()
    mapping_full['Legacy Field'] = mapping_full['Legacy Field'].str.strip()

    catalog  = read_target_catalog()
    mapping = mapping_full.query("`Legacy Module` == 'Outcomes' and `Target Module` == 'Outcomes'")
    assert_target_pairs_exist("Outcomes", mapping, catalog)

    ui_cols = (
        catalog.query(
            "`User-Facing Module Name` == 'Outcomes' and "
            "not `Data Source / Type`.str.contains('Related List', na=False)"
        )["User-Facing Field Name"].tolist()
    )

    # 1. Load and filter legacy Outcomes data
    df_raw = pd.read_csv(LEGACY_CSV, dtype=str)
    df_raw.columns = df_raw.columns.str.strip()

    df_filtered = df_raw[df_raw["Outcome Type"].isin(OUTCOME_TYPES_TO_KEEP)].copy()
    log.info("Filtered legacy data to %d rows based on original Outcome Type.", len(df_filtered))

    if df_filtered.empty:
        log.warning("No rows matched the specified Outcome Types. Output will be empty.")
        return

    # 2. Apply data cleaning and standardization rules
    df_filtered['Original Outcome Type'] = df_filtered['Outcome Type']
    df_filtered["Outcome Type"] = df_filtered["Outcome Type"].replace(OUTCOME_TYPE_REMAPPING)
    log.info("Remapped 'AMC' and 'Math Kangaroo' types to 'Competition'.")

    needs_standardization = df_filtered["Outcome Type"].isin(['Competition', 'Other Assessment'])
    df_filtered.loc[needs_standardization, "Outcome Name"] = df_filtered.loc[needs_standardization, "Outcome Name"].apply(standardize_outcome_name)
    log.info("Standardized Outcome Names for 'Competition' and 'Other Assessment' types.")

    # 3. Handle static transformations
    static_mapping = mapping[~mapping["Legacy Field"].isin(DESCRIPTION_COLS)]
    df_transformed = transform_legacy_df(df_filtered, static_mapping)
    log.info("Performed static field mapping using `transform_legacy_df`.")
    
    # New: Convert "Extroardinary" column from Yes/No to Boolean
    if "Extroardinary" in df_transformed.columns:
        log.info("Converting 'Extroardinary' column to boolean (True/False)...")
        df_transformed['Extroardinary'] = df_transformed['Extroardinary'].replace({'Yes': True, 'No': False})


    # 4. Handle dynamic 'Description' field
    new_descriptions: List[str] = []
    # Use df_filtered.iterrows() to ensure we have the original index aligned with df_transformed
    for index, row in df_filtered.iterrows():
        original_outcome_type = row.get("Original Outcome Type")
        parts: List[str] = []

        if original_outcome_type in AMC_FIELDS:
            for field, label in AMC_FIELDS[original_outcome_type].items():
                val = row.get(field)
                if pd.notna(val) and str(val).strip():
                    parts.append(f"{label}: {val}")

        base_desc = row.get("Outcome Description")
        if pd.notna(base_desc) and str(base_desc).strip():
            parts.append(base_desc)

        new_descriptions.append(" | ".join(parts))

    # Assign based on the index of df_transformed to ensure alignment
    df_transformed["Description"] = pd.Series(new_descriptions, index=df_transformed.index)
    log.info("Generated new 'Description' column with refined concatenation logic.")

    # 5. Apply Star Name Lookup
    STAR_MATCH_KEY_COL = "Star (Match Key)"
    if STAR_MATCH_KEY_COL in df_transformed.columns:
        try:
            log.info("Loading Star lookup file to replace ID with Full Name...")
            df_star_lookup = pd.read_csv(STAR_LOOKUP_FILE, dtype=str)
            star_name_map = pd.Series(df_star_lookup["Full Name"].values, index=df_star_lookup["Record Id"]).to_dict()

            original_ids = df_transformed[STAR_MATCH_KEY_COL].nunique()
            df_transformed[STAR_MATCH_KEY_COL] = df_transformed[STAR_MATCH_KEY_COL].map(star_name_map)
            found_names = df_transformed[STAR_MATCH_KEY_COL].notna().sum()
            log.info(f"Successfully mapped {found_names} of {original_ids} unique Star IDs to full names.")

        except FileNotFoundError:
            log.warning(f"Star lookup file not found at '{STAR_LOOKUP_FILE}'. Skipping name enrichment.")
        except Exception as e:
            log.error(f"An error occurred during Star name enrichment: {e}")

    # 6. Pad, order, and write the final output
    for col in ui_cols:
        if col not in df_transformed.columns:
            df_transformed[col] = pd.NA
    df_out = df_transformed[ui_cols]

    out_file = OUTPUT_DIR / "Outcomes.csv"
    df_out.to_csv(out_file, index=False)
    log.info("Wrote %s (%d total rows)", out_file, len(df_out))


if __name__ == "__main__":
    main()