#!/usr/bin/env python3
"""
Households Loader — National Math Stars CRM Migration
====================================================
Processes the legacy Accounts export to create a clean list of unique Household
records and writes a UI‑ready CSV for bulk import.

Key steps
---------
1. Filter to Star families.
2. Generate a deterministic `family_key` via `make_household_key` (shared in
   `scripts.etl_lib`).
3. Keep the most‑recent record per family (by Cohort Entry Year).
4. Aggregate notes, build a friendly Household Name, clean fields & addresses.
5. Drop helper columns and write `output/Households.csv`.
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
    to_int_if_whole,
    strip_translation,
    standardize_address_block,
    intelligent_title_case,
    make_household_key,  # ← shared helper
)

# ───────────────────────── CONFIG ──────────────────────────
COHORT_COL = "Cohort Entry Year"
BASE_DIR = Path(__file__).resolve().parent
ACCOUNTS_CSV = (
    BASE_DIR.parent / "mapping" / "legacy-exports" / "Accounts_2025_06_24.csv"
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
    # 1. LOAD & PREP LEGACY DATA
    log.info("Loading and preparing legacy Accounts data…")
    df_raw = pd.read_csv(ACCOUNTS_CSV, dtype=str)
    df_raw = df_raw[df_raw["Account Type"].str.strip().eq("Star")].copy()
    log.info(f"Filtered to {len(df_raw)} 'Star' account records.")

    df_raw[COHORT_COL] = pd.to_numeric(df_raw[COHORT_COL], errors="coerce")
    df_raw["family_key"] = df_raw.apply(make_household_key, axis=1)
    df_raw = df_raw[df_raw["family_key"].notna()]
    log.info(f"Successfully generated family keys for {len(df_raw)} records.")

    # mapping + catalog checks
    mapping = read_mapping().query("`Target Module` == 'Households'")
    catalog = read_target_catalog()
    assert_target_pairs_exist("Households", mapping, catalog)

    # 2. TRANSFORM TO UI FIELDS
    df_ui = transform_legacy_df(df_raw, mapping)
    df_ui["family_key"] = df_raw["family_key"]
    df_ui[COHORT_COL] = df_raw[COHORT_COL]

    # 3. DEDUPLICATION & NOTES AGGREGATION
    log.info("Deduplicating records to keep the most recent for each family…")
    latest = (
        df_ui.sort_values(COHORT_COL, na_position="first")
        .groupby("family_key", as_index=False)
        .tail(1)
        .set_index("family_key")
    )
    log.info(f"Finished deduplication. {len(latest)} unique household records remain.")

    notes_series = (
        df_ui.groupby("family_key")["Special Circumstances"].apply(
            lambda s: "; ".join(s.dropna().unique())
        )
    )
    latest["Notes"] = notes_series

    # 4. FINAL FORMATTING & CLEANING
    log.info("Applying final formatting and cleaning rules…")
    latest_guardian_info = (
        df_raw.sort_values(COHORT_COL, na_position="first")
        .groupby("family_key")
        .tail(1)
        .set_index("family_key")
    )

    first_name_clean = latest_guardian_info["Primary Guardian First Name"].apply(
        intelligent_title_case
    )
    last_name_clean = latest_guardian_info["Primary Guardian Last Name"].apply(
        intelligent_title_case
    )

    latest["Household Name"] = (
        first_name_clean.str[0].str.upper() + ". " + last_name_clean + " Household"
    )

    # numeric + text cleaners
    latest["Family Size"] = to_int_if_whole(latest["Family Size"])
    for col in ["Highest Level of Education", "Special Circumstances"]:
        if col in latest.columns:
            latest[col] = latest[col].apply(strip_translation)

    # address normalisation
    standardize_address_block(
        latest,
        {
            "address_line_1": "Street",
            "city": "City",
            "state": "State",
            "postal_code": "Zip Code",
        },
    )

    # 5. FINALISE COLUMNS & WRITE OUTPUT
    ui_cols = (
        catalog.query("`User-Facing Module Name` == 'Households'")["User-Facing Field Name"].tolist()
    )

    for col in ui_cols:
        if col not in latest.columns:
            latest[col] = pd.NA

# --- build two-column lookup & stash it in cache/ ---
    CACHE_DIR   = BASE_DIR.parent / "cache"
    CACHE_DIR.mkdir(exist_ok=True)

    lookup_path = CACHE_DIR / "household_lookup.csv"
    (
        latest.reset_index()[["family_key", "Household Name"]].to_csv(lookup_path, index=False)
    )
    log.info("Wrote household lookup → %s", lookup_path)
    
    latest.drop(columns=["family_key"], inplace=True, errors="ignore")
    latest = latest[[col for col in ui_cols if col in latest.columns]]

    OUTPUT_DIR.mkdir(exist_ok=True)
    ui_path = OUTPUT_DIR / "Households.csv"
    latest.reset_index(drop=True).to_csv(ui_path, index=False)
    log.info(f"Wrote data to {ui_path}")


if __name__ == "__main__":
    main()