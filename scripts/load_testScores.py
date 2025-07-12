#!/usr/bin/env python3
"""
Test-Scores Loader — National Math Stars CRM Migration
======================================================
Transforms legacy Outcomes and Accounts data into the target Test Scores module.
- `Outcomes_2025_07_11.csv` → output/Test Scores.csv
- `Accounts_2025_06_24.csv` → output/Test Scores.csv (appended)
"""

from __future__ import annotations
import logging
from pathlib import Path
from typing import Dict, List
import sys

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
LEGACY_OUTCOMES_CSV = BASE.parent / "mapping" / "legacy-exports" / "Outcomes_2025_07_11.csv"
LEGACY_ACCOUNTS_CSV = BASE.parent / "mapping" / "legacy-exports" / "Accounts_2025_06_24.csv"
STAR_LOOKUP_FILE    = CACHE_DIR / "star_lookup.csv"
OUTPUT_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)


logging.basicConfig(level="INFO",
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-m-%d %H:%M:%S")
log = logging.getLogger(__name__)

# ───────── ETL LIB HELPERS ─────────
def to_int_if_whole(series: pd.Series) -> pd.Series:
    """Converts float values to integers if they are whole numbers."""
    return series.apply(
        lambda x: int(x) if pd.notna(x) and isinstance(x, float) and x.is_integer() else x
    )

def _read_csv(file_path: Path) -> pd.DataFrame:
    """Wrapper for pd.read_csv with default dtype=str."""
    return pd.read_csv(file_path, dtype=str)


# ───────── META DEFINITIONS ─────────
META: Dict[str, tuple] = {
    "ACT Overall Score": ("ACT", "Composite", "Raw", 36),
    "ACT Math Score": ("ACT", "Math", "Raw", 36),
    "ACT Science Score": ("ACT", "Science", "Raw", 36),
    "PSAT Overall Score": ("PSAT", "Composite", "Raw", 1520),
    "PSAT Math Score": ("PSAT", "Math", "Raw", 760),
    "SAT Overall Score": ("SAT", "Composite", "Raw", 1600),
    "SAT Math Score": ("SAT", "Math", "Raw", 800),
    "AP Score": ("AP", "Composite", "Raw", None),
    "Working Memory Scaled Score": ("WISC-V", "Working Memory", "Scaled", None),
    "Processing Speed Scaled Score": ("WISC-V", "Processing Speed", "Scaled", None),
    "Visual Spatial Scaled Score": ("WISC-V", "Visual Spatial", "Scaled", None),
    "Full Scale IQ Scaled Score": ("WISC-V", "Full Scale IQ", "Scaled", None),
    "Fluid Reasoning Scaled Score": ("WISC-V", "Fluid Reasoning", "Scaled", None),
    "Verbal Comprehension Scaled Score": ("WISC-V", "Verbal Comprehension", "Scaled", None),
    "CogAT Standard Nonverbal": ("CogAT", "Standard Nonverbal", "Standard", None),
    "CoGAT Standard Quant": ("CogAT", "Standard Quant", "Standard", None),
}

RAW_PCT_MAP = {
    "ACT Overall Score": "ACT Percentile",
    "PSAT Overall Score": "PSAT Percentile",
    "SAT Overall Score": "SAT Percentile",
    "Working Memory Scaled Score": "Working Memory Percentile",
    "Processing Speed Scaled Score": "Processing Speed Percentile",
    "Visual Spatial Scaled Score": "Visual Spatial Percentile",
    "Full Scale IQ Scaled Score": "Full Scale IQ Percentile",
    "Fluid Reasoning Scaled Score": "Fluid Reasoning Percentile",
    "Verbal Comprehension Scaled Score": "Verbal Comprehension Percentile",
}
OUTCOMES_SCORE_COLS = list(set(RAW_PCT_MAP.keys()) | {"ACT Math Score", "ACT Science Score", "PSAT Math Score", "SAT Math Score", "AP Score"})
ACCOUNTS_SCORE_COLS = ["CogAT Standard Nonverbal", "CoGAT Standard Quant"]
DYNAMIC_TGT = {"Test Name", "Sub-Test Name", "Score Value", "Percentile", "Score Unit/Type", "Score Out Of"}

# ───────── MAIN ─────────
def main() -> None:
    mapping_full = read_mapping()
    mapping_full['Legacy Field'] = mapping_full['Legacy Field'].str.strip()
    catalog = read_target_catalog()
    assert_target_pairs_exist("Test Scores", mapping_full.query("`Target Module` == 'Test Scores'"), catalog)
    ui_cols: List[str] = catalog.query("`User-Facing Module Name` == 'Test Scores'")["User-Facing Field Name"].tolist()
    master_records: List[dict] = []

    # Stage 1: Process Outcomes
    log.info("─" * 20 + " Stage 1: Processing Outcomes " + "─" * 20)
    mapping_outcomes = mapping_full.query("`Legacy Module` == 'Outcomes' and `Target Module` == 'Test Scores'")
    df_outcomes_raw = _read_csv(LEGACY_OUTCOMES_CSV)
    df_outcomes_raw.columns = df_outcomes_raw.columns.str.strip()
    static_mapping_outcomes = mapping_outcomes[~mapping_outcomes["Target Field"].isin(DYNAMIC_TGT)]
    df_static_outcomes = transform_legacy_df(df_outcomes_raw, static_mapping_outcomes)

    for i, row in df_outcomes_raw.iterrows():
        base = df_static_outcomes.loc[i].dropna().to_dict()
        for score_col in OUTCOMES_SCORE_COLS:
            score_val = row.get(score_col)
            if pd.isna(score_val) or str(score_val).strip() == "": continue
            test, sub, unit, out_of = META[score_col]
            pct_col = RAW_PCT_MAP.get(score_col)
            pct_val = row.get(pct_col) if pct_col else None
            if score_col == "AP Score":
                outcome_name = row.get("Outcome Name", "")
                if pd.notna(outcome_name) and "AP " in outcome_name: sub = outcome_name.split("AP ", 1)[1].strip()
            rec = base.copy()
            rec.update({"Test Name": test, "Sub-Test Name": sub, "Score Value": score_val, "Percentile": pct_val if pd.notna(pct_val) else pd.NA, "Score Unit/Type": unit, "Score Out Of": out_of})
            master_records.append(rec)
    log.info("Completed processing Outcomes. Found %d records.", len(master_records))

    # Stage 2: Process Accounts
    log.info("─" * 20 + " Stage 2: Processing Accounts " + "─" * 20)
    mapping_accounts = mapping_full.query("`Legacy Module` == 'Accounts' and `Target Module` == 'Test Scores'")
    df_accounts_raw = _read_csv(LEGACY_ACCOUNTS_CSV)
    df_accounts_raw.columns = df_accounts_raw.columns.str.strip()
    df_accounts_filtered = df_accounts_raw[df_accounts_raw["Account Type"] == "Star"].copy()
    log.info("Found %d Accounts with Account Type 'Star'.", len(df_accounts_filtered))
    static_mapping_accounts = mapping_accounts[~mapping_accounts["Target Field"].isin(DYNAMIC_TGT)]
    df_static_accounts = transform_legacy_df(df_accounts_filtered, static_mapping_accounts)

    for i, row in df_accounts_filtered.iterrows():
        base = df_static_accounts.loc[i].dropna().to_dict()
        for score_col in ACCOUNTS_SCORE_COLS:
            score_val = row.get(score_col)
            if pd.isna(score_val) or str(score_val).strip() == "": continue
            test, sub, unit, out_of = META[score_col]
            rec = base.copy()
            rec.update({"Test Name": test, "Sub-Test Name": sub, "Score Value": score_val, "Percentile": pd.NA, "Score Unit/Type": unit, "Score Out Of": out_of})
            master_records.append(rec)
    log.info("Completed processing Accounts. Total records now: %d.", len(master_records))

    # Stage 3: Combine, Enrich, and Write Output
    if not master_records:
        log.warning("No records produced from any source file.")
        return
    
    df_out = pd.DataFrame(master_records)
    for col in ui_cols:
        if col not in df_out.columns: df_out[col] = pd.NA
    df_out = df_out[ui_cols].copy()

    # Apply Star Name Lookup
    STAR_MATCH_KEY_COL = "Star (Match Key)"
    if STAR_MATCH_KEY_COL in df_out.columns:
        try:
            log.info("Loading Star lookup file to replace ID with Full Name...")
            df_star_lookup = _read_csv(STAR_LOOKUP_FILE)
            star_name_map = pd.Series(df_star_lookup["Full Name"].values, index=df_star_lookup["Record Id"]).to_dict()
            original_ids = df_out[STAR_MATCH_KEY_COL].nunique()
            df_out[STAR_MATCH_KEY_COL] = df_out[STAR_MATCH_KEY_COL].map(star_name_map)
            found_names = df_out[STAR_MATCH_KEY_COL].notna().sum()
            log.info(f"Successfully mapped {found_names} of {original_ids} unique Star IDs to full names.")
        except FileNotFoundError:
            log.warning(f"Star lookup file not found at '{STAR_LOOKUP_FILE}'. Skipping name enrichment.")
        except Exception as e:
            log.error(f"An error occurred during Star name enrichment: {e}")

    # Convert 'Score Out Of' to integer where possible
    if "Score Out Of" in df_out.columns:
        df_out["Score Out Of"] = to_int_if_whole(df_out["Score Out Of"])
        log.info("Converted 'Score Out Of' column to integer where possible.")

    # Final Write
    out_file = OUTPUT_DIR / "Test Scores.csv"
    df_out.to_csv(out_file, index=False)
    log.info("Wrote %s (%d total rows)", out_file, len(df_out))

if __name__ == "__main__":
    main()