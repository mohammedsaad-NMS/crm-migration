#!/usr/bin/env python3
"""
Stars Loader — National Math Stars CRM Migration
================================================
Creates UI-ready **Stars** records, writes `output/Stars.csv`,
and persists a lookup file `cache/star_lookup.csv` mapping **Record Id**
(from the legacy extract) → **Full Name** (final value).

Flow
----
1. Load legacy *Accounts* export → keep `Account Type == "Star"`.
2. Generate deterministic `family_key` (shared with Households).
3. Map & rename columns per *Target-Legacy Mapping.csv*.
4. Merge `cache/household_lookup.csv` → populate **Household (Match Key)**.
5. Clean fields (title-case names, grade ints, translation strip …).
6. Persist **Record Id → Full Name** lookup to `cache/star_lookup.csv`.
7. Drop helper columns & write the UI CSV in catalogue order.
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
    strip_translation,
    to_int_if_whole,
    make_household_key,
)

# ───────────────────────── CONFIG ──────────────────────────
BASE_DIR   = Path(__file__).resolve().parent
CACHE_DIR  = BASE_DIR.parent / "cache"
CACHE_DIR.mkdir(exist_ok=True)                      # ensure cache dir exists

LEGACY_CSV = BASE_DIR.parent / "mapping" / "legacy-exports" / "Accounts_2025_06_24.csv"
OUTPUT_DIR = BASE_DIR.parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# handshake lookup written by load_households.py
LOOKUP_FILE = CACHE_DIR / "household_lookup.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────── MAIN ───────────────────────────

def main() -> None:
    # 1. LOAD & FILTER
    df_raw = pd.read_csv(LEGACY_CSV, dtype=str)
    df_raw = df_raw[df_raw["Account Type"].str.strip().eq("Star")].copy()

    # skip rows with blank star names
    blank_mask = (
        df_raw["Star First Name"].fillna("").str.strip().eq("")
        & df_raw["Star Last Name"].fillna("").str.strip().eq("")
    )
    df_raw = df_raw[~blank_mask]

    # 2. COMPUTE FAMILY KEY (shared logic)
    df_raw["family_key"] = df_raw.apply(make_household_key, axis=1)

    # 3. MAP / RENAME PER MAPPING
    mapping = read_mapping().query("`Target Module` == 'Stars'")
    catalog = read_target_catalog()
    assert_target_pairs_exist("Stars", mapping, catalog)

    df_ui = transform_legacy_df(df_raw, mapping)
    df_ui["family_key"] = df_raw["family_key"]              # carry helper key
    df_ui["Record Id"]  = df_raw["Record Id"]               # passthrough for caching

    # 4. MERGE HOUSEHOLD LOOKUP
    if LOOKUP_FILE.exists():
        hh_lu = pd.read_csv(LOOKUP_FILE)
        df_ui = df_ui.merge(hh_lu, on="family_key", how="left", validate="many_to_one")
        df_ui["Household (Match Key)"] = df_ui.pop("Household Name")

        missing = df_ui["Household (Match Key)"].isna().sum()
        if missing:
            log.warning("%d Stars missing household match", missing)
    else:
        log.error("Lookup file %s not found. Run load_households.py first.", LOOKUP_FILE)
        df_ui["Household (Match Key)"] = pd.NA

    # 5. FIELD-LEVEL CLEANING ----------------------------------------
    # 5a. Names → intelligent title-case
    for col in ["First Name", "Last Name", "Middle Name"]:
        if col in df_ui.columns:
            df_ui[col] = df_ui[col].apply(intelligent_title_case)

    # 5b. Grade ordinals → int
    if "Current Grade" in df_ui.columns:
        df_ui["Current Grade"] = df_ui["Current Grade"].str.extract(r"(\d+)")[0]
        df_ui["Current Grade"] = pd.to_numeric(df_ui["Current Grade"], errors="coerce")
        df_ui["Current Grade"] = to_int_if_whole(df_ui["Current Grade"])
        df_ui["Current Grade"] = df_ui["Current Grade"].astype("Int64")

    # 5c. Cohort Entry Year numeric coercion
    if "Cohort Entry Year" in df_ui.columns:
        df_ui["Cohort Entry Year"] = to_int_if_whole(df_ui["Cohort Entry Year"])

    # 5d. Translation strip
    for col in ["Race or Ethnicity", "Gender Identity"]:
        if col in df_ui.columns:
            df_ui[col] = df_ui[col].apply(strip_translation)

    # 5e. Age calculation
    if "Date of Birth" in df_ui.columns:
        dob = pd.to_datetime(df_ui["Date of Birth"], errors="coerce")
        age = (pd.Timestamp.now() - dob).dt.days / 365.25
        df_ui["Age"] = age.apply(lambda x: int(x) if pd.notna(x) else pd.NA).astype("Int64")

    # 5f. Construct Full Name
    if {"First Name", "Last Name"}.issubset(df_ui.columns):
        df_ui["Full Name"] = (
            df_ui["First Name"].astype(str).fillna("") + " " +
            df_ui["Last Name"].astype(str).fillna("")
        ).str.strip()

    # 6. WRITE LOOK-UP CACHE -----------------------------------------
    log.info("Writing star-lookup cache (Record Id → Full Name)…")
    lookup_df  = df_ui[["Record Id", "Full Name"]].copy()
    cache_path = CACHE_DIR / "star_lookup.csv"
    lookup_df.to_csv(cache_path, index=False)
    log.info("Wrote lookup to %s (%d rows)", cache_path, len(lookup_df))

    # 7. FINAL COLUMN ORDER -----------------------------------------
    ui_cols = (catalog.query("`User-Facing Module Name` == 'Stars'")
               ["User-Facing Field Name"].tolist())

    for col in ui_cols:
        if col not in df_ui.columns:
            df_ui[col] = pd.NA

    # drop helpers
    df_ui.drop(columns=["family_key", "Record Id"], inplace=True, errors="ignore")

    df_ui = df_ui[[c for c in ui_cols if c in df_ui.columns]]

    # 8. WRITE OUTPUT ------------------------------------------------
    ui_path = OUTPUT_DIR / "Stars.csv"
    df_ui.to_csv(ui_path, index=False)
    log.info("Wrote %s (%d rows)", ui_path, len(df_ui))


if __name__ == "__main__":
    main()
