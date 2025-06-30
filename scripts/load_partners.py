"""
Partners Loader — National Math Stars CRM Migration
============================================================
This script processes the legacy Accounts export to create a clean list of
partner records.

The process includes:
* Straightforward field rename per mapping
* Deduplication based on a cached decisions file
* Field-specific cleaning
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
    intelligent_title_case,
)

# ───────────────────────── CONFIG ─────────────────────────
BASE_DIR    = Path(__file__).resolve().parent
LEGACY_CSV  = BASE_DIR.parent / "mapping" / "legacy-exports" / "Partners_2025_06_22.csv"
OUTPUT_DIR  = BASE_DIR.parent / "output"
CACHE_DIR   = BASE_DIR.parent / "cache"
OUTPUT_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────── MAIN ───────────────────────────

def main() -> None:
    """Generate Partners.csv"""

    # 1. LOAD LEGACY DATA
    log.info("Loading legacy Partners data…")
    df_raw = pd.read_csv(LEGACY_CSV, dtype=str)

    # 2. LOAD MAPPING & VALIDATE
    mapping  = read_mapping().query("`Target Module` == 'Partners'")
    catalog  = read_target_catalog()
    assert_target_pairs_exist("Partners", mapping, catalog)

    # 3. TRANSFORM COLUMNS PER MAPPING
    log.info("Transforming legacy columns...")
    df_ui = transform_legacy_df(df_raw, mapping)
    
    # --- CORRECTED STEP ---
    # Ensure 'Record Id' from the raw file is present for deduplication.
    df_ui['Record Id'] = df_raw['Record Id'].str.strip()
    log.info(f"Loaded and transformed {len(df_ui)} records.")

    # 4. APPLY DEDUPLICATION DECISIONS FROM CACHE
    try:
        input_stem = Path(LEGACY_CSV).stem
        decision_file = CACHE_DIR / f"decisions_{input_stem}.csv"

        if not decision_file.exists():
            raise FileNotFoundError

        log.info(f"Reading deduplication decisions from: {decision_file.name}")
        df_decisions = pd.read_csv(
            decision_file,
            dtype={'canonical_record_id': str, 'duplicate_record_id': str}
        )
        
        merge_decisions = df_decisions[df_decisions['user_decision'] == 'MERGE']
        ids_to_drop = set(merge_decisions["duplicate_record_id"])
        
        if ids_to_drop:
            log.info(f"Found {len(ids_to_drop)} partner records marked for deduplication.")
            initial_count = len(df_ui)
            # This step is now safe because 'Record Id' is guaranteed to exist.
            df_ui = df_ui[~df_ui["Record Id"].isin(ids_to_drop)]
            log.info(f"Removed {initial_count - len(df_ui)} non-canonical partners.")
        else:
            log.info("Decisions file contained no 'MERGE' actions. No records removed.")

    except FileNotFoundError:
        log.warning(f"Deduplication file not found at '{decision_file}'. Skipping step.")
    except Exception as e:
        log.error(f"An error occurred while applying deduplication: {e}")

    # 5. FIELD-SPECIFIC CLEANERS
    if "Partner Name" in df_ui.columns:
        log.info("Cleaning 'Partner Name' field...")
        df_ui["Partner Name"] = df_ui["Partner Name"].apply(intelligent_title_case)

    # 6. ENSURE ALL UI COLUMNS EXIST (ADD EMPTY ONES AS NEEDED)
    ui_cols = (catalog.query("`User-Facing Module Name` == 'Partners'")
               .query("`Data Source / Type`.str.contains('Related List') == False "
                      "and `Data Source / Type`.str.contains('System') == False")
                      ["User-Facing Field Name"].tolist()
    )
    for col in ui_cols:
        if col not in df_ui.columns:
            df_ui[col] = pd.NA
    df_ui = df_ui[[c for c in ui_cols if c in df_ui.columns]]

    # 7. WRITE OUPUTS
    ui_path  = OUTPUT_DIR / "Partners.csv"

    df_ui.to_csv(ui_path, index=False)
    log.info(f"Wrote {len(df_ui)} final records to {ui_path}")

if __name__ == "__main__":
    main()