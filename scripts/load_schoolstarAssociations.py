#!/usr/bin/env python3
"""
School‑Star Associations Loader — National Math Stars CRM Migration
===================================================================
Creates UI‑ready **School‑Star Associations** records and writes
`output/School_Star_Associations.csv`.

Core rules added (July 2 2025)
-----------------------------
* Remove any association row whose **Schools.id** matches a `Record Id` in
  `cache/district_lookup.csv` where districts were staged.
* Enrich names:
    * **Star Name** → from `cache/star_lookup.csv` via `Star.id`.
    * **School Name** → from `cache/school_lookup.csv` via `Schools.id`.

All other logic (mapping, column ordering, write‑out) follows the standard
pattern shared across loaders.
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
BASE_DIR = Path(__file__).resolve().parent
CACHE_DIR = BASE_DIR.parent / "cache"

LEGACY_CSV = (
    BASE_DIR.parent
    / "mapping"
    / "legacy-exports"
    / "School_Star_Associations_2025_07_01.csv"
)
OUTPUT_DIR = BASE_DIR.parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# cache files
STAR_LU_FILE = CACHE_DIR / "star_lookup.csv"
SCHOOL_LU_FILE = CACHE_DIR / "school_lookup.csv"
DISTRICT_LU_FILE = CACHE_DIR / "district_lookup.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────── MAIN ───────────────────────────

def main() -> None:
    # 1. LOAD raw associations
    df_raw = pd.read_csv(LEGACY_CSV, dtype=str)
    log.info("Loaded %s (%d rows)", LEGACY_CSV.name, len(df_raw))

    # 1a. EXCLUDE rows linked to districts via Schools.id
    if DISTRICT_LU_FILE.exists():
        district_ids = (
            pd.read_csv(DISTRICT_LU_FILE, dtype=str)["Record Id"].dropna().unique()
        )
        before = len(df_raw)
        df_raw = df_raw[~df_raw["Schools.id"].isin(district_ids)]
        removed = before - len(df_raw)
        log.info("Removed %d district‑school rows", removed)
    else:
        log.warning("District lookup %s not found — no filtering applied", DISTRICT_LU_FILE)

    # Preserve IDs for later name enrichment
    star_ids = df_raw.get("Star.id")
    school_ids = df_raw.get("Schools.id")

    # 2. MAP / RENAME via mapping file
    mapping = read_mapping().query("`Target Module` == 'School-Star Associations'")
    catalog = read_target_catalog()
    assert_target_pairs_exist("School-Star Associations", mapping, catalog)

    df_ui = transform_legacy_df(df_raw, mapping)

    # 3. ENRICH CLEANED NAMES FROM LOOKUPS ---------------------------------
    # Star Name lookup
    if STAR_LU_FILE.exists() and "Star.id" in df_raw.columns:
        star_lu = pd.read_csv(STAR_LU_FILE, dtype=str).set_index("Record Id")
        if "Full Name" in star_lu.columns and "Star (Match Key)" in df_ui.columns:
            df_ui["Star (Match Key)"] = star_ids.map(star_lu["Full Name"]).fillna(df_ui["Star (Match Key)"])
        else:
            log.warning("Expected 'Star Name' column not found in star_lookup.csv")
    else:
        log.info("Star lookup not used (file missing or Star.id absent)")

    # School Name lookup
    if SCHOOL_LU_FILE.exists() and "Schools.id" in df_raw.columns:
        school_lu = pd.read_csv(SCHOOL_LU_FILE, dtype=str).set_index("Record Id")
        if "School Name" in school_lu.columns and "School (Match Key)" in df_ui.columns:
            df_ui["School (Match Key)"] = school_ids.map(school_lu["School Name"]).fillna(df_ui["School (Match Key)"])
        else:
            log.warning("Expected 'School Name' column not found in school_lookup.csv")
    else:
        log.info("School lookup not used (file missing or Schools.id absent)")

    # 4. FINAL COLUMN ORDER -------------------------------------------------
    ui_cols = (
        catalog.query("`User-Facing Module Name` == 'School-Star Associations'")[
            "User-Facing Field Name"
        ].tolist()
    )

    for col in ui_cols:
        if col not in df_ui.columns:
            df_ui[col] = pd.NA

    df_ui = df_ui[[c for c in ui_cols if c in df_ui.columns]]

    # 5. WRITE --------------------------------------------------------------
    ui_path = OUTPUT_DIR / "School_Star_Associations.csv"
    df_ui.to_csv(ui_path, index=False)
    log.info("Wrote %s (%d rows)", ui_path, len(df_ui))


if __name__ == "__main__":
    main()
