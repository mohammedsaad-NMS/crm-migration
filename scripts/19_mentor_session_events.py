#!/usr/bin/env python3
"""
Mentor-Session Events Loader — National Math Stars CRM Migration
================================================================
Transforms *Mentor_Sessions_2025_07_13.csv* → **output/Mentor Session Events.csv**

Key logic
---------
1.  **Drop** rows whose *Mentor Account* is 'Test Mentor'.
2.  **Drop** rows whose *Mentor Session Name* (after date-strip + tidy) matches
    one of `DISCARD_TITLES`.
3.  **Strip** leading date strings (and any residual “: ”) from *Mentor Session Name*.
4.  **Map / rename** fields via *Target-Legacy Mapping.csv* → `df_ui`.
5.  **Apply specific corrections** to the 'Mentor (Match Key)' field.
6.  **Back-fill Mentor Session Event Name** with *Session Topic* if blank **after** mapping.
7.  **Apply intelligent title case** to the 'Mentor Session Event Name' column.
8.  **Default Session Format = "Virtual"** unless *Recording URL* equals a value in
    `INVALID_URLS`.
9.  **Cache** a lookup of legacy Record Id → Mentor Session Event Name.
10. **Exclude** Related-List and helper fields from the final output.
"""
from __future__ import annotations
import logging, re
from pathlib import Path
from typing import List

import pandas as pd;  pd.options.mode.chained_assignment = None

from scripts.helpers.etl_lib import (
    read_mapping, read_target_catalog, assert_target_pairs_exist,
    transform_legacy_df,
    intelligent_title_case,
)

# ───────────────────────── CONFIG ──────────────────────────
BASE_DIR    = Path(__file__).resolve().parent
CACHE_DIR   = BASE_DIR.parent / "cache"; CACHE_DIR.mkdir(exist_ok=True)
LEGACY_CSV  = BASE_DIR.parent / "mapping" / "legacy-exports" / "Mentor_Sessions_2025_07_15.csv"
OUTPUT_DIR  = BASE_DIR.parent / "output";  OUTPUT_DIR.mkdir(exist_ok=True)

TARGET_MODULE = "Mentor Session Events"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ───────── CONSTANTS ─────────
DISCARD_TITLES: List[str] = [
    "test",
    "cc test madi session notes",
    "test 2",
    "cc test madi session",
    "test session",
    "test session 002",
    "test sessoin 003",
    "cc test 11",
    "cc test",
    "cc test madi session 02/05",
]
INVALID_URLS = {"http://www.zoom/us", "www.zoom/us", "http://www.zoom", "www.zoom.us", "zoom.us", }

DATE_RE = re.compile(
    r"""^
        (
          \d{4}-\d{2}-\d{2} |            # YYYY-MM-DD
          \d{1,2}[/-]\d{1,2}[/-]\d{2,4}  # M/D/YY, M-D-YYYY, etc.
        )
        \s*[-–—:]?\s* 
    """,
    flags=re.I | re.X,
)

# ───────── HELPERS ─────────
def _clean_title(text: str) -> str:
    """Remove leading date and separator from a title using the robust DATE_RE."""
    if pd.isna(text):
        return text
    return DATE_RE.sub("", str(text).strip(), count=1)

# ───────── MAIN ─────────
def main() -> None:
    # 1. LOAD legacy CSV ----------------------------------------------------
    df_raw = pd.read_csv(LEGACY_CSV, dtype=str)
    log.info("Loaded legacy Mentor Sessions (%d rows)", len(df_raw))

    # 2. DISCARD 'Test Mentor' ACCOUNTS --------------------------------------
    if "Mentor Account" in df_raw.columns:
        mentor_mask = df_raw["Mentor Account"].str.strip().str.lower() == "test mentor"
        if mentor_mask.any():
            log.info("Discarding %d rows assigned to 'Test Mentor'.", mentor_mask.sum())
            df_raw = df_raw[~mentor_mask]

    # 3. DISCARD TEST ROWS BY TITLE -----------------------------------------
    name_series = (
        df_raw["Mentor Session Name"]
            .apply(_clean_title)
            .str.replace(r'\s+', ' ', regex=True) # Normalize internal whitespace
            .str.strip()
            .str.lower()
    )
    discard_mask = name_series.isin(DISCARD_TITLES)
    if discard_mask.any():
        log.info("Discarding %d test rows by title.", discard_mask.sum())
        df_raw = df_raw[~discard_mask]

    # 4. CLEAN TITLES -------------------------------------------------------
    df_raw["Mentor Session Name"] = df_raw["Mentor Session Name"].apply(_clean_title)

    # 5. FIELD MAPPING ------------------------------------------------------
    mapping  = read_mapping().query("`Target Module` == @TARGET_MODULE")
    catalog  = read_target_catalog()
    assert_target_pairs_exist(TARGET_MODULE, mapping, catalog)

    df_ui = transform_legacy_df(df_raw, mapping)
    df_ui["Record Id"] = df_raw["Record Id"] # Passthrough for caching

    # 6. APPLY SPECIFIC MENTOR NAME CORRECTIONS -----------------------------
    if "Mentor (Match Key)" in df_ui.columns:
        log.info("Applying specific mentor name corrections...")
        replacements = {
            "Al Lucero Math Mentor": "Al Lucero",
            "Humberto Leal": "Humberto Leal Acosta"
        }
        df_ui["Mentor (Match Key)"] = df_ui["Mentor (Match Key)"].replace(replacements)

    # 7. BACK-FILL BLANK TITLE FROM SESSION TOPIC --------------------------
    blank_mask = df_ui["Mentor Session Event Name"].fillna("").str.strip().eq("")
    if blank_mask.any():
        df_ui.loc[blank_mask, "Mentor Session Event Name"] = df_ui.loc[blank_mask, "Session Topic"]

    # 8. APPLY INTELLIGENT TITLE CASE TO EVENT NAME ------------------------
    if "Mentor Session Event Name" in df_ui.columns:
        log.info("Applying intelligent title case to 'Mentor Session Event Name'...")
        df_ui["Mentor Session Event Name"] = df_ui["Mentor Session Event Name"].apply(intelligent_title_case)

    # 9. DEFAULT SESSION FORMAT --------------------------------------------
        url_present = df_ui["Session Recording URL"].notna() & df_ui["Session Recording URL"].str.strip().ne('')
        url_valid = ~df_ui["Session Recording URL"].isin(INVALID_URLS)
        
        is_virtual = url_present & url_valid
        
        df_ui.loc[is_virtual, "Session Format"] = "Virtual"

    # 10. WRITE LOOK-UP CACHE ----------------------------------------
    log.info("Writing mentor-session-lookup cache...")
    lookup_df  = df_ui[["Record Id", "Mentor Session Event Name", "Mentor (Match Key)"]].copy()
    cache_path = CACHE_DIR / "mentor_session_lookup.csv"
    lookup_df.to_csv(cache_path, index=False)
    log.info("Wrote lookup to %s (%d rows)", cache_path, len(lookup_df))

    # 11. FINAL COLUMN ORDER -----------------------------------------
    ui_cols = (
        catalog
        .query("`User-Facing Module Name` == @TARGET_MODULE")
        .query("`Data Source / Type`.str.contains('Related List') == False", engine="python")
        ["User-Facing Field Name"]
        .tolist()
    )
    for col in ui_cols:
        if col not in df_ui.columns:
            df_ui[col] = pd.NA

    df_ui.drop(columns=["Record Id"], inplace=True, errors="ignore") # Drop helper
    df_ui = df_ui[[c for c in ui_cols if c in df_ui.columns]]

    # 12. WRITE OUTPUT -------------------------------------------------------
    out_path = OUTPUT_DIR / f"{TARGET_MODULE}.csv"
    df_ui.to_csv(out_path, index=False)
    log.info("Wrote %d rows → %s", len(df_ui), out_path)

if __name__ == "__main__":
    main()