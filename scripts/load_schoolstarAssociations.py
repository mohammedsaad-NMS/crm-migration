#!/usr/bin/env python3
"""
School‑Star Associations Loader — National Math Stars CRM Migration
===================================================================
Creates UI‑ready **School‑Star Associations** records and writes
`output/School_Star_Associations.csv`.

The loader is deliberately minimalist: it trusts the global ETL library
(`scripts.etl_lib`) for all mapping logic and only performs light, module‑
specific hygiene.

Flow
----
1. **Load** legacy extract `School_Star_Associations_2025_07_01.csv`.
2. **Exclude district rows** – load `Districts___Schools_2025_06_25.csv`,
   keep only rows with `Type == "District"`, collect their `Record Id`s,
   and drop any association whose `Schools.id` matches one of those IDs.
3. **Map & rename** columns using *Target‑Legacy Mapping.csv* (rows where
   `Target Module` == "School‑Star Associations").
4. **Optional lookups** — if `cache/star_lookup.csv` and/or
   `cache/school_lookup.csv` exist, merge to fill technical IDs.
5. **Clean** – ISO‑8601 date coercion for *Start Date* / *End Date*,
   intelligent title‑case on *Role*.
6. **Column order** – align with *Target modules_fields.csv* for the module.
7. **Write** `output/School_Star_Associations.csv`.
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
    transform_legacy_df
)

# ───────────────────────── CONFIG ──────────────────────────
BASE_DIR = Path(__file__).resolve().parent

LEGACY_CSV = (
    BASE_DIR.parent / "mapping" / "legacy-exports" / "School_Star_Associations_2025_07_01.csv"
)
DISTRICT_SCHOOLS_CSV = (
    BASE_DIR.parent / "mapping" / "legacy-exports" / "Districts___Schools_2025_06_25.csv"
)
OUTPUT_DIR = BASE_DIR.parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────── MAIN ───────────────────────────

def main() -> None:
    # 1. LOAD
    df_raw = pd.read_csv(LEGACY_CSV, dtype=str)
    log.info("Loaded %s (%d rows)", LEGACY_CSV.name, len(df_raw))

    # 2. REMOVE DISTRICT ROWS
    if DISTRICT_SCHOOLS_CSV.exists():
        districts = pd.read_csv(DISTRICT_SCHOOLS_CSV, dtype=str)
        district_ids = (
            districts.loc[districts["Type"].str.strip().eq("District"), "Record Id"]
            .dropna()
            .unique()
        )
        before = len(df_raw)
        df_raw = df_raw[~df_raw["Schools.id"].isin(district_ids)]
        removed = before - len(df_raw)
        log.info("Filtered %d district association rows", removed)
    else:
        log.warning("District lookup %s not found – no district rows removed", DISTRICT_SCHOOLS_CSV)

    # 3. MAP / RENAME
    mapping = read_mapping().query("`Target Module` == 'School-Star Associations'")
    catalog = read_target_catalog()
    assert_target_pairs_exist("School-Star Associations", mapping, catalog)

    df_ui = transform_legacy_df(df_raw, mapping)

    # 6. FINAL COLUMN ORDER -------------------------------------------------
    ui_cols = (
        catalog.query("`User-Facing Module Name` == 'School-Star Associations'")
        ["User-Facing Field Name"].tolist()
    )

    for col in ui_cols:
        if col not in df_ui.columns:
            df_ui[col] = pd.NA

    df_ui = df_ui[[c for c in ui_cols if c in df_ui.columns]]

    # 7. WRITE --------------------------------------------------------------
    ui_path = OUTPUT_DIR / "School_Star_Associations.csv"
    df_ui.to_csv(ui_path, index=False)
    log.info("Wrote %s (%d rows)", ui_path, len(df_ui))


if __name__ == "__main__":
    main()
