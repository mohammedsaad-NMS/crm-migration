#!/usr/bin/env python3
"""
Products Loader — National Math Stars CRM Migration
===================================================
Creates the target-ready **Products** CSV by:

1. Building a base frame from the legacy Products extract.
2. Applying deduplication rules from the specific, corresponding cached output.
3. Sorting raw progress records by 'Modified Time' to ensure the most recent
   data is used.
4. Enriching that frame with course-specific and enrichment-specific data.
5. Flagging any Product that appears in *both* progress tables.
6. Cleaning fields and writing the final output.
"""

from __future__ import annotations
import logging
from pathlib import Path
from typing import Set

import pandas as pd
pd.options.mode.chained_assignment = None

from scripts.etl_lib import (
    read_mapping,
    read_target_catalog,
    assert_target_pairs_exist,
    transform_legacy_df,
    intelligent_title_case,
    strip_translation,
    to_int_if_whole,
)

# ───────────────────────── CONFIG ─────────────────────────
MODULE_UI   = "Products"
BASE_DIR    = Path(__file__).resolve().parent
LEGACY_DIR  = BASE_DIR.parent / "mapping" / "legacy-exports"
LEGACY_FILES = {
    "prod"   : "Products_2025_06_27.csv",
    "course" : "STEM_Course_Progress_2025_06_27.csv",
    "enrich" : "STEM_Enrichments_Progress_2025_06_27.csv",
}

OUTPUT_DIR   = BASE_DIR.parent / "output"
CACHE_DIR    = BASE_DIR.parent / "cache"
OUTPUT_CSV   = OUTPUT_DIR / "Products.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ───────────────────────── HELPERS ─────────────────────────
def _read_csv(fname: str, key_col: str | None = None) -> pd.DataFrame:
    path = LEGACY_DIR / fname
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_csv(path, dtype=str, keep_default_na=False).replace("", pd.NA)
    if key_col and key_col not in df.columns:
        raise KeyError(f"Key column {key_col!r} missing in {fname}")
    return df


# ───────────────────────── MAIN ─────────────────────────
def main() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)

    # 1. Load catalog & mapping slices
    mapping = read_mapping()
    catalog = read_target_catalog()

    map_prod = mapping[
        (mapping['Target Module'] == MODULE_UI) &
        (mapping['Legacy Module'] == 'Products')
    ]
    map_course = mapping[
        (mapping['Target Module'] == MODULE_UI) &
        (mapping['Legacy Module'] == 'Stem Course Progress')
    ]
    map_enrich = mapping[
        (mapping['Target Module'] == MODULE_UI) &
        (mapping['Legacy Module'] == 'Stem Enrichments Progress')
    ]

    if map_course.empty: log.warning("Did not find any mappings for 'Stem Course Progress'.")
    if map_enrich.empty: log.warning("Did not find any mappings for 'Stem Enrichments Progress'.")
        
    assert_target_pairs_exist(MODULE_UI, pd.concat([map_prod, map_course, map_enrich]), catalog)

    # 2. Base Products frame
    prod_raw = _read_csv(LEGACY_FILES["prod"])
    df_prod  = transform_legacy_df(prod_raw, map_prod)
    df_prod["Record Id"] = prod_raw["Record Id"].str.strip()
    log.info(f"Loaded {len(df_prod)} initial products from legacy file.")

    # --- REVISED AND CORRECTED DEDUPLICATION BLOCK ---
    # Step 2.5: Remove non-canonical products based on fuzzy match output
    try:
        # Build the specific, expected filename for the decisions file
        prod_input_stem = Path(LEGACY_FILES["prod"]).stem
        decision_file = CACHE_DIR / f"decisions_{prod_input_stem}.csv"

        if not decision_file.exists():
            raise FileNotFoundError

        log.info(f"Reading deduplication decisions from: {decision_file.name}")
        
        # Read decisions, ensuring IDs are treated as strings to prevent type errors
        df_decisions = pd.read_csv(
            decision_file,
            dtype={'canonical_record_id': str, 'duplicate_record_id': str}
        )
        
        # Get set of IDs to drop using robust boolean indexing
        merge_decisions = df_decisions[df_decisions['user_decision'] == 'MERGE']
        ids_to_drop = set(merge_decisions["duplicate_record_id"])
        
        if ids_to_drop:
            log.info(f"Found {len(ids_to_drop)} product IDs marked for deduplication.")
            initial_count = len(df_prod)
            df_prod = df_prod[~df_prod["Record Id"].isin(ids_to_drop)]
            log.info(f"Removed {initial_count - len(df_prod)} non-canonical products.")
        else:
            log.info("Decisions file contained no 'MERGE' actions. No products removed.")

    except FileNotFoundError:
        log.warning(f"Corresponding decisions file not found at '{decision_file}'. Skipping deduplication.")
    except Exception as e:
        log.error(f"An error occurred while applying deduplication: {e}")
    # ────────────────────────────────────────────────────────

    ui_cols = (
        catalog[catalog["User-Facing Module Name"] == MODULE_UI]
               [~catalog["Data Source / Type"].str.contains('Related List', case=False, na=False)]
               ["User-Facing Field Name"]
               .tolist()
    )
    for col in ui_cols:
        if col not in df_prod.columns:
            df_prod[col] = pd.NA

    # 3. Load and sort raw enrichment data
    log.info("Loading and sorting raw progress records by Modified Time...")
    course_raw = _read_csv(LEGACY_FILES["course"])
    if 'Modified Time' in course_raw.columns:
        course_raw['Modified Time'] = pd.to_datetime(course_raw['Modified Time'], errors='coerce')
        course_raw.sort_values(by='Modified Time', ascending=True, inplace=True)
    else:
        log.warning("'Modified Time' column not found in Course Progress data. Cannot sort by date.")

    enrich_raw = _read_csv(LEGACY_FILES["enrich"])
    if 'Modified Time' in enrich_raw.columns:
        enrich_raw['Modified Time'] = pd.to_datetime(enrich_raw['Modified Time'], errors='coerce')
        enrich_raw.sort_values(by='Modified Time', ascending=True, inplace=True)
    else:
        log.warning("'Modified Time' column not found in Enrichment Progress data. Cannot sort by date.")

    # 4. Transform the pre-sorted data
    df_course  = transform_legacy_df(course_raw, map_course)
    df_course["Record Id"] = course_raw["STEM Course.id"].str.strip()

    df_enrich  = transform_legacy_df(enrich_raw, map_enrich)
    df_enrich["Record Id"] = enrich_raw["Enrichment.id"].str.strip()

    # 6. De-duplicate enrichment frames, keeping the most recent record
    df_course.drop_duplicates(subset=["Record Id"], keep="last", inplace=True)
    df_enrich.drop_duplicates(subset=["Record Id"], keep="last", inplace=True)
    
    # 7. Inject data
    df_prod.set_index("Record Id", inplace=True)
    df_course.set_index("Record Id", inplace=True)
    df_enrich.set_index("Record Id", inplace=True)

    df_prod.update(df_enrich, overwrite=False)
    df_prod.update(df_course, overwrite=False) # Course has precedence

    df_prod.reset_index(inplace=True)

    # 8. Field-specific cleaners
    if "Product Name" in df_prod.columns:
        df_prod["Product Name"] = df_prod["Product Name"].apply(intelligent_title_case)
    if "Description" in df_prod.columns:
        df_prod["Description"] = df_prod["Description"].apply(strip_translation)
    hours_cols = [c for c in df_prod.columns if "Hours per" in c]
    for col in hours_cols:
        df_prod[col] = to_int_if_whole(pd.to_numeric(df_prod[col], errors="coerce"))

    # 9. Final ordering and write
    final_cols = ["Record Id"] + [c for c in ui_cols if c in df_prod.columns]
    df_prod = df_prod[final_cols]
    df_prod.to_csv(OUTPUT_CSV, index=False)
    log.info("Products loader complete → %s (rows: %d)", OUTPUT_CSV.name, len(df_prod))


if __name__ == "__main__":
    main()