"""
Partners Loader — National Math Stars CRM Migration
---------------------------------------------------
Transfers legacy Partners data into UI‑ready and API‑ready CSVs
using the authoritative Legacy‑Target mapping.

Current scope: straightforward field rename per mapping; no deduplication,
enrichment, or custom transformations yet.
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
    ui_to_api_headers,
)

# ───────────────────────── CONFIG ─────────────────────────
MODULE_UI   = "Partners"
BASE_DIR    = Path(__file__).resolve().parent
LEGACY_CSV  = BASE_DIR.parent / "mapping" / "legacy-exports" / "Partners_2025_06_22.csv"
OUTPUT_DIR  = BASE_DIR.parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────── MAIN ───────────────────────────

def main() -> None:
    """Generate Partners_ui.csv and Partners_api.csv"""

    # 1. LOAD LEGACY DATA
    log.info("Loading legacy Partners data…")
    df_raw = pd.read_csv(LEGACY_CSV, dtype=str)

    # 2. LOAD MAPPING & VALIDATE
    mapping  = read_mapping().query("`Target Module` == @MODULE_UI")
    catalog  = read_target_catalog()
    assert_target_pairs_exist(MODULE_UI, mapping, catalog)

    # 3. TRANSFORM COLUMNS PER MAPPING
    df_ui = transform_legacy_df(df_raw, mapping)

    # 4. ENSURE ALL UI COLUMNS EXIST (ADD EMPTY ONES AS NEEDED)
    ui_cols = (
        catalog.query("`User-Facing Module Name` == @MODULE_UI")
               .query("`Data Source / Type`.str.contains('Related List') == False")
               ["User-Facing Field Name"].tolist()
    )
    for col in ui_cols:
        if col not in df_ui.columns:
            df_ui[col] = pd.NA
    df_ui = df_ui[[c for c in ui_cols if c in df_ui.columns]]

    # 5. WRITE UI & API CSVs
    ui_path  = OUTPUT_DIR / f"{MODULE_UI}_ui.csv"
    api_path = OUTPUT_DIR / f"{MODULE_UI}_api.csv"

    df_ui.to_csv(ui_path, index=False)
    log.info("Wrote UI data to %s", ui_path)

    api_df = ui_to_api_headers(df_ui.copy(), MODULE_UI, catalog)
    api_df.to_csv(api_path, index=False)
    log.info("Wrote API data to %s", api_path)


if __name__ == "__main__":
    main()
