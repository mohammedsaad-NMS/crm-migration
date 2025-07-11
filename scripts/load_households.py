#!/usr/bin/env python3
"""
Households Loader — National Math Stars CRM Migration
====================================================
*unchanged header … see prior version*
"""
from __future__ import annotations
import logging
from pathlib import Path
import pandas as pd;  pd.options.mode.chained_assignment = None

from scripts.etl_lib import (
    read_mapping, read_target_catalog, assert_target_pairs_exist,
    transform_legacy_df, to_int_if_whole, strip_translation,
    standardize_address_block, intelligent_title_case, make_household_key,
)

# ───────────────────────── CONFIG ──────────────────────────
COHORT_COL  = "Cohort Entry Year"
BASE_DIR    = Path(__file__).resolve().parent
ACCOUNTS_CSV= BASE_DIR.parent / "mapping" / "legacy-exports" / "Accounts_2025_06_24.csv"
OUTPUT_DIR  = BASE_DIR.parent / "output"; OUTPUT_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────── MAIN ───────────────────────────
def main() -> None:
    # 1. LOAD & PREP LEGACY DATA -------------------------------------------
    log.info("Loading and preparing legacy Accounts data…")
    df_raw = pd.read_csv(ACCOUNTS_CSV, dtype=str)
    df_raw = df_raw[df_raw["Account Type"].str.strip().eq("Star")].copy()
    log.info(f"Filtered to {len(df_raw)} 'Star' account records.")

    df_raw[COHORT_COL] = pd.to_numeric(df_raw[COHORT_COL], errors="coerce")
    df_raw["family_key"] = df_raw.apply(make_household_key, axis=1)
    df_raw = df_raw[df_raw["family_key"].notna()]
    log.info(f"Successfully generated family keys for {len(df_raw)} records.")

    # 2. FIELD MAPPING & BASIC UI FRAME ------------------------------------
    mapping  = read_mapping().query("`Target Module` == 'Households'")
    catalog  = read_target_catalog()
    assert_target_pairs_exist("Households", mapping, catalog)

    df_ui = transform_legacy_df(df_raw, mapping)
    df_ui["family_key"] = df_raw["family_key"]
    df_ui[COHORT_COL]   = df_raw[COHORT_COL]

    # 3. DEDUPLICATE TO ONE-ROW PER FAMILY ---------------------------------
    latest = (
        df_ui.sort_values(COHORT_COL, na_position="first")
        .groupby("family_key", as_index=False)
        .tail(1)
        .set_index("family_key")
    )
    notes_series = (
        df_ui.groupby("family_key")["Special Circumstances"]
        .apply(lambda s: "; ".join(s.dropna().unique()))
    )
    latest["Notes"] = notes_series

    # 4. FINAL FORMATTING & CLEANING ---------------------------------------
    guardian_last = (
        df_raw.sort_values(COHORT_COL, na_position="first")
        .groupby("family_key")
        .tail(1)
        .set_index("family_key")
    )
    first_name_clean = guardian_last["Primary Guardian First Name"].apply(intelligent_title_case)
    last_name_clean  = guardian_last["Primary Guardian Last Name"].apply(intelligent_title_case)
    latest["Household Name"] = (first_name_clean.str[0].str.upper() + ". " + last_name_clean + " Household")

    latest["Family Size"] = to_int_if_whole(latest["Family Size"])
    for col in ["Highest Level of Education", "Special Circumstances"]:
        if col in latest.columns:
            latest[col] = latest[col].apply(strip_translation)

    standardize_address_block(
        latest,
        {"address_line_1": "Street", "city": "City", "state": "State", "postal_code": "Zip Code"},
    )

    # 5. FINALISE COLUMNS & WRITE UI CSV -----------------------------------
    ui_cols = (
        catalog.query("`User-Facing Module Name` == 'Households'")
        ["User-Facing Field Name"]
        .tolist()
    )
    for col in ui_cols:
        if col not in latest.columns:
            latest[col] = pd.NA

    OUTPUT_DIR.mkdir(exist_ok=True)
    ui_path = OUTPUT_DIR / "Households.csv"
    latest.reset_index(drop=True)[ui_cols].to_csv(ui_path, index=False)
    log.info("Wrote data to %s", ui_path)

    # 6. BUILD CACHE (family_key • Household Name • Account Name) ----------
    CACHE_DIR = BASE_DIR.parent / "cache";  CACHE_DIR.mkdir(exist_ok=True)
    lookup_path = CACHE_DIR / "household_lookup.csv"

    # Map family_key → Household Name
    household_name_map = latest["Household Name"]

    # One row per original Star account
    cache_df = df_raw[["family_key", "Account Name"]].copy()
    cache_df["Household Name"] = cache_df["family_key"].map(household_name_map)

    cache_df[["family_key", "Household Name", "Account Name"]].to_csv(lookup_path, index=False)
    log.info("Wrote household lookup → %s (%d rows)", lookup_path, len(cache_df))


if __name__ == "__main__":
    main()
