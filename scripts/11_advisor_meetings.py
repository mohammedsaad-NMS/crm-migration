#!/usr/bin/env python3
"""
Advisor-Meetings Loader — National Math Stars CRM Migration
===========================================================
Transforms *Advisor_Meetings_2025_07_11.csv* → **output/Advisor Meetings.csv**

Changes in this version
-----------------------
• **Drop rows** whose *legacy* title (`Legacy Advisor Meeting Name`) contains
  the substring “Note” (case-insensitive).  
• Exclude **Legacy Advisor Meeting Name** from the final output file.

All other behaviour (household lookup, date extraction, purpose remap, long-
duration cleanup, etc.) is unchanged.
"""
from __future__ import annotations
import logging, re
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd;  pd.options.mode.chained_assignment = None

from scripts.helpers.etl_lib import (
    read_mapping, read_target_catalog, assert_target_pairs_exist,
    transform_legacy_df,
)

# ───────────────────────── CONFIG ─────────────────────────
BASE_DIR    = Path(__file__).resolve().parent
LEGACY_CSV  = BASE_DIR.parent / "mapping" / "legacy-exports" / "Advisor Meetings_C_001.csv"
LOOKUP_CSV  = BASE_DIR.parent / "cache"   / "household_lookup.csv"
OUTPUT_DIR  = BASE_DIR.parent / "output";  OUTPUT_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ───────── REGEX & HELPERS ─────────
DATE_RE = re.compile(
    r"""^(
          \d{4}-\d{2}-\d{2} |
          \d{1,2}[/-]\d{1,2}[/-]\d{2,4}
        )[\s-]*""",
    flags=re.I | re.X,
)

def _parse_date(ds: str) -> Optional[str]:
    try:    return pd.to_datetime(ds, dayfirst=False).strftime("%Y-%m-%d")
    except: return None

def _extract_date_and_title(raw: str) -> Tuple[Optional[str], str]:
    if pd.isna(raw): return None, ""
    m = DATE_RE.match(raw.strip())
    if not m:       return None, raw.strip()
    return _parse_date(m.group(0)), DATE_RE.sub("", raw, count=1).lstrip()

def _remap_purpose(orig: str, stripped: str) -> str:
    if str(orig).strip().lower() == "monthly check-in":
        return "Initial Check-in" if "initial" in stripped.lower() else "Routine Check-in"
    return orig

# ───────── MAIN ─────────
def main() -> None:
    # LOAD legacy & lookup --------------------------------------------------
    df_raw = pd.read_csv(LEGACY_CSV, dtype=str)
    df_raw["Legacy Advisor Meeting Name"] = df_raw["Advisor Meeting Name"]

    lookup_df = pd.read_csv(LOOKUP_CSV, dtype=str)
    # --- MODIFIED --- Use the legacy "Record Id" from the cache file for the lookup
    acct_to_household = lookup_df.set_index("Record Id")["Household Name"].to_dict()

    # FIELD MAPPING ---------------------------------------------------------
    mapping  = read_mapping().query("`Target Module` == 'Advisor Meetings'")
    catalog  = read_target_catalog()
    assert_target_pairs_exist("Advisor Meetings", mapping, catalog)

    df_ui = transform_legacy_df(df_raw, mapping)
    df_ui["Legacy Advisor Meeting Name"] = df_raw["Legacy Advisor Meeting Name"]

    # REMOVE legacy rows labelled "Note" ------------------------------------
    note_mask = df_ui["Legacy Advisor Meeting Name"].str.contains("note", case=False, na=False)
    if note_mask.any():
        log.info("Discarding %d rows whose legacy title contains 'Note'.", note_mask.sum())
        df_ui  = df_ui[~note_mask]
        df_raw = df_raw.loc[df_ui.index]          # keep alignment with raw helpers

    # HOUSEHOLD (MATCH KEY) via lookup --------------------------------------
    h_col = "Household (Match Key)"
    # --- MODIFIED --- Map using the "Account.id" column from the raw advisor meetings file
    df_ui[h_col] = (
        df_raw["Account.id"].map(acct_to_household)
        .combine_first(df_ui.get(h_col))
        .replace("", pd.NA)
    )

    # DATE EXTRACTION -------------------------------------------------------
    extracted          = df_raw["Advisor Meeting Name"].apply(_extract_date_and_title)
    df_ui["Date"]      = extracted.apply(lambda x: x[0])
    stripped_title     = extracted.apply(lambda x: x[1])
    df_ui["Date"]      = df_ui["Date"].fillna(df_raw["Start Time"].str[:10])

    # PURPOSE REMAP ---------------------------------------------------------
    df_ui["Purpose"] = [
        _remap_purpose(p, st) for p, st in zip(df_raw["Meeting Purpose"], stripped_title)
    ]

    # DROP rows lacking Household ------------------------------------------
    before = len(df_ui)
    df_ui = df_ui[df_ui[h_col].notna()]
    if before - len(df_ui):
        log.info("Dropped %d rows with missing Household (Match Key).", before - len(df_ui))

    # ADVISOR MEETING NAME --------------------------------------------------
    stripped_title = stripped_title.loc[df_ui.index]
    def _build_name(purpose: str, stripped: str, household: str) -> str:
        if purpose in ("Routine Check-in", "Initial Check-in"):
            return f"{purpose} with {household}".strip()
        return stripped or "Untitled Meeting"
    df_ui["Advisor Meeting Name"] = [
        _build_name(p, st, h) for p, st, h in zip(df_ui["Purpose"], stripped_title, df_ui[h_col])
    ]

    # CLEAR START/END > 8 h -------------------------------------------------
    if {"Meeting Start Time", "Meeting End Time"}.issubset(df_ui.columns):
        start_dt  = pd.to_datetime(df_ui["Meeting Start Time"], errors="coerce")
        end_dt    = pd.to_datetime(df_ui["Meeting End Time"],   errors="coerce")
        long_mask = (end_dt - start_dt).dt.total_seconds() > 8 * 3600
        if long_mask.any():
            df_ui.loc[long_mask, ["Meeting Start Time", "Meeting End Time"]] = pd.NA
            log.info("Cleared Start/End Time on %d long-duration rows.", long_mask.sum())

    # FINAL column order (Legacy title removed) -----------------------------
    ui_cols = catalog.query("`User-Facing Module Name` == 'Advisor Meetings'")["User-Facing Field Name"]
    extras  = ["Date"]                              # no Legacy Advisor Meeting Name
    final_cols = extras + [c for c in ui_cols if c not in extras]
    for col in final_cols:
        if col not in df_ui.columns:
            df_ui[col] = pd.NA
    df_ui = df_ui[final_cols]

    # WRITE output ----------------------------------------------------------
    out_path = OUTPUT_DIR / "Advisor Meetings.csv"
    df_ui.to_csv(out_path, index=False)
    log.info("Wrote %d rows → %s", len(df_ui), out_path)


if __name__ == "__main__":
    main()