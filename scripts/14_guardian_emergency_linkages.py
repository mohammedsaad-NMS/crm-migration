#!/usr/bin/env python3
"""
Connection Tables Loader — Guardians & Emergency Contacts
=========================================================
Creates two separate junction table files by processing the legacy
Accounts export.

1.  output/Guardian Connections.csv
2.  output/Emergency Contact Connections.csv

This script extracts Guardian and Emergency Contact details associated
with each Star account and structures them as linkable records.
"""

from __future__ import annotations
import logging
from pathlib import Path
from typing import List, Optional

import pandas as pd
pd.options.mode.chained_assignment = None

# Assuming etl_lib.py is in the same directory or accessible
from scripts.helpers.etl_lib import intelligent_title_case, strip_translation

# ───────────────────────── CONFIG ──────────────────────────
BASE_DIR    = Path(__file__).resolve().parent
OUTPUT_DIR  = BASE_DIR.parent / "output"; OUTPUT_DIR.mkdir(exist_ok=True)
ACCOUNTS_CSV = BASE_DIR.parent / "mapping" / "legacy-exports" / "Accounts_2025_06_24.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ───────────────────────── HELPERS ─────────
def get_person_name(row: pd.Series, prefix: str) -> Optional[str]:
    """Extracts and combines first and last name for a given prefix."""
    # Handle the specific column naming for Emergency Contacts
    if prefix == "Emergency Contact":
        first_name_col = "Emergency First Name"
        last_name_col = "Emergency Last Name"
    else:
        first_name_col = f"{prefix} First Name"
        last_name_col = f"{prefix} Last Name"

    first_name = row.get(first_name_col)
    last_name = row.get(last_name_col)
    
    if pd.notna(first_name) or pd.notna(last_name):
        full_name = f"{str(first_name or '')} {str(last_name or '')}".strip()
        return full_name if full_name else None
    return None

# ───────────────────────── MAIN ─────────
def main() -> None:
    """Main execution function."""
    log.info("Starting Connection Tables loader...")

    # 1. LOAD LEGACY ACCOUNTS DATA
    try:
        df_acc = pd.read_csv(ACCOUNTS_CSV, dtype=str)
        log.info(f"Loaded {len(df_acc)} rows from {ACCOUNTS_CSV.name}")
    except FileNotFoundError:
        log.error(f"Legacy Accounts file not found at: {ACCOUNTS_CSV}")
        return

    # Filter for only Star accounts
    df_stars = df_acc[df_acc["Account Type"].str.strip().eq("Star")].copy()
    log.info(f"Filtered to {len(df_stars)} Star accounts.")

    guardian_connections: List[dict] = []
    emergency_connections: List[dict] = []

    # 2. PROCESS EACH STAR ACCOUNT TO BUILD CONNECTIONS
    for _, star_row in df_stars.iterrows():
        star_name = get_person_name(star_row, "Star")
        if not star_name:
            continue # Skip if the Star's name is missing

        # Determine if the Primary Guardian is the only legal guardian
        only_guardian_flag = star_row.get("Primary Guardian is Only Legal Guardian") == "Yes/Sí"

        # Process Guardians
        guardian_roles = {
            "Primary Guardian": "Primary Guardian",
            "Secondary Guardian": "Secondary Guardian",
            "Third Guardian": "Tertiary Guardian",
        }
        for prefix, role in guardian_roles.items():
            guardian_name = get_person_name(star_row, prefix)
            if guardian_name:
                # The flag is only true for the Primary Guardian and only if the source field is "Yes/Sí"
                is_only_guardian = (role == "Primary Guardian") and only_guardian_flag
                
                guardian_connections.append({
                    "Star (Match Key)": star_name,
                    "Guardian (Match Key)": guardian_name,
                    "Relationship Type": role,
                    "Only Legal Guardian": is_only_guardian
                })

        # Process Emergency Contact
        emergency_contact_name = get_person_name(star_row, "Emergency Contact")

        if emergency_contact_name:
            # Get the relationship value from the new column
            relationship = star_row.get("Emergency Relationship to Star")
            emergency_connections.append({
                "Star (Match Key)": star_name,
                "Emergency Contact (Match Key)": emergency_contact_name,
                "Relationship": relationship
            })

    # 3. CREATE AND WRITE GUARDIAN CONNECTIONS FILE
    if guardian_connections:
        df_guardians = pd.DataFrame(guardian_connections)
        # Apply title casing for consistency
        df_guardians["Star (Match Key)"] = df_guardians["Star (Match Key)"].apply(intelligent_title_case)
        df_guardians["Guardian (Match Key)"] = df_guardians["Guardian (Match Key)"].apply(intelligent_title_case)
        
        out_path = OUTPUT_DIR / "Guardian-Star Associations.csv"
        df_guardians.to_csv(out_path, index=False)
        log.info(f"Wrote {len(df_guardians)} rows to {out_path.name}")
    else:
        log.warning("No Guardian connections were generated.")

    # 4. CREATE AND WRITE EMERGENCY CONTACT CONNECTIONS FILE
    if emergency_connections:
        df_emergency = pd.DataFrame(emergency_connections)
        # Apply title casing for consistency
        df_emergency["Star (Match Key)"] = df_emergency["Star (Match Key)"].apply(intelligent_title_case)
        df_emergency["Emergency Contact (Match Key)"] = df_emergency["Emergency Contact (Match Key)"].apply(intelligent_title_case)
        
        # Apply translation stripping to the Relationship column
        df_emergency["Relationship"] = df_emergency["Relationship"].apply(strip_translation)
        
        out_path = OUTPUT_DIR / "Emergency Contact-Star Associations.csv"
        df_emergency.to_csv(out_path, index=False)
        log.info(f"Wrote {len(df_emergency)} rows to {out_path.name}")
    else:
        log.warning("No Emergency Contact connections were generated.")


if __name__ == "__main__":
    main()