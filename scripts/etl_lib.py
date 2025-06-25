"""
ETL Library — National Math Stars CRM Migration
=================================================================
This library provides a centralized set of reusable functions for the
ETL (Extract, Transform, Load) scripts used in the CRM migration.

The library includes helpers for:
* Reading and validating mapping and catalog files.
* Transforming data from legacy to UI-ready formats.
* Cleaning and standardizing various data types (text, numbers).
* Normalizing and formatting address blocks using scourgify.
"""

from __future__ import annotations
import logging
import re
from pathlib import Path
from typing import Dict, List

import pandas as pd
from scourgify import normalize_address_record   # from PyPI package *usaddress-scourgify*

log = logging.getLogger(__name__)

# ───────────────────────────────
# REPOSITORY-RELATIVE CSV PATHS
# ───────────────────────────────
BASE_DIR      = Path(__file__).resolve().parent
MAP_FILE      = BASE_DIR.parent / "mapping" / "Target-Legacy Mapping.csv"
TARGET_FIELDS = BASE_DIR.parent / "mapping" / "Target modules_fields.csv"

# ════════════════════════════════════════════════════════════════════════════
#                               MAPPING HELPERS
# ════════════════════════════════════════════════════════════════════════════

# Read the main mapping file that defines legacy-to-target field relationships.
def read_mapping() -> pd.DataFrame:
    return pd.read_csv(MAP_FILE)

# Read the target system's data catalog, which contains all possible fields.
def read_target_catalog() -> pd.DataFrame:
    return pd.read_csv(TARGET_FIELDS)

# Assert that all fields specified in the mapping file exist in the target data catalog.
def assert_target_pairs_exist(module_ui: str,
                              module_mapping: pd.DataFrame,
                              target_cat: pd.DataFrame) -> None:
    valid_pairs = set(
        target_cat[["User-Facing Module Name", "User-Facing Field Name"]]
        .itertuples(index=False, name=None)
    )
    mapping_pairs = set(
        module_mapping[["Target Module", "Target Field"]]
        .itertuples(index=False, name=None)
    )
    missing = {
        p for p in (mapping_pairs - valid_pairs)
        if p[0].lower() not in ("remove", "remove/hide")
    }
    if missing:
        raise ValueError(f"{module_ui}: target-catalog mismatch → {missing}")

# ════════════════════════════════════════════════════════════════════════════
#                         DATA TRANSFORMATION HELPERS
# ════════════════════════════════════════════════════════════════════════════

# Rename legacy DataFrame columns to the target UI-facing names based on the mapping.
def transform_legacy_df(df_legacy: pd.DataFrame,
                        module_mapping: pd.DataFrame) -> pd.DataFrame:
    keep = module_mapping[
        ~module_mapping["Target Module"].str.lower().isin(["remove", "remove/hide"])
    ]
    rename_map = dict(zip(keep["Legacy Field"], keep["Target Field"]))
    # Select and rename only the columns present in the rename_map
    cols_to_rename = [col for col in rename_map.keys() if col in df_legacy.columns]
    return df_legacy[cols_to_rename].rename(columns=rename_map)

# ════════════════════════════════════════════════════════════════════════════
#                               GENERAL CLEANERS
# ════════════════════════════════════════════════════════════════════════════
def make_household_key(row: pd.Series) -> str | None:
    fn = str(row.get("Primary Guardian First Name", "")).strip().lower()
    ln = str(row.get("Primary Guardian Last Name", "")).strip().lower()
    zp = str(row.get("Primary Guardian Zip", "")).strip()

    if not fn or not ln or not zp or fn in ("nan", "none") or ln in ("nan", "none"):
        return None
    return f"{fn[0]}|{ln}|{zp}"

def intelligent_title_case(text: str) -> str:
    """
    Applies intelligent title casing to a string.
    - Expands common abbreviations.
    - Handles ordinal suffixes (e.g., '1st').
    - Keeps minor words lowercase.
    - Forces specific acronyms to uppercase.
    """
    if pd.isna(text):
        return text

    text = str(text).lower()

    # 1. Expand abbreviations
    replacements = {
        r'\bel\b': 'Elementary School',
        r'\bhts\b': 'Heights',
        r'\bpri\b': 'Primary',
        r'\bmiddle\b': 'Middle School',
        r'\bcharter\b': 'Charter School'
    }
    for pattern, repl in replacements.items():
        text = re.sub(pattern, repl, text, flags=re.I)
    
    # 2. Basic title casing
    words = text.split()
    
    # 3. Handle minor words, ordinals, and acronyms
    minor_words = {'of', 'for', 'and', 'the', 'a', 'an', 'in', 'on', 'at'}
    acronyms = {'IDEA', 'ILTexas', 'ISD'}
    
    final_words = []
    for i, word in enumerate(words):
        # Keep acronyms uppercase
        if word.upper() in acronyms:
            final_words.append(word.upper())
            continue
            
        # Handle ordinals (e.g., 1st, 2nd)
        if re.match(r'^\d+(st|nd|rd|th)$', word):
            final_words.append(word)
            continue
            
        # Title case the word
        titled_word = word.capitalize()
        
        # Lowercase minor words unless it's the first word
        if i > 0 and titled_word.lower() in minor_words:
            final_words.append(word.lower())
        else:
            final_words.append(titled_word)

    return ' '.join(final_words)

# Strip extensions from zip codes (e.g., "78757-1234" -> "78757").
_ZIP_RE = re.compile(r"[\s-].*$")
def root_zip(val: str) -> str:
    return _ZIP_RE.sub("", str(val).strip())

# Remove bilingual text separated by a forward slash (e.g., "Yes/Si" -> "Yes").
def strip_translation(val: str) -> str:
    if pd.isna(val):
        return val
    parts = str(val).split(';')
    cleaned = [p.split('/', 1)[0].strip() for p in parts if p.strip()]
    return '; '.join(cleaned)

# Convert float values to integers if they have no decimal part (e.g., 3.0 -> 3).
def to_int_if_whole(series: pd.Series) -> pd.Series:
    return series.apply(
        lambda x: int(x) if pd.notna(x) and float(x).is_integer() else x
    )

# ════════════════════════════════════════════════════════════════════════════
#                ADDRESS NORMALISER (scourgify)
# ════════════════════════════════════════════════════════════════════════════

# Normalise and format a block of address columns (Street, City, State, Zip).
def standardize_address_block(df: pd.DataFrame,
                              col_map: Dict[str, str]) -> pd.DataFrame:
    """
    Normalises and formats Street/City/State/Zip columns in-place.
    Includes a fallback to prevent data loss if scourgify fails to parse.
    """
    if not col_map:
        return df

    # --- Phase 1: Parsing with Scourgify ---
    records = df.to_dict('records')

    def _parse_row(row_dict: dict) -> dict:
        scourgify_input = {
            key: row_dict.get(val)
            for key, val in col_map.items()
            if pd.notna(row_dict.get(val))
        }
        if not scourgify_input: return {}
        try:
            parsed = normalize_address_record(scourgify_input, long_hand=True)
            # If parsing fails, scourgify returns None. Fall back to the original input.
            return parsed if parsed else scourgify_input
        except Exception as exc:
            log.debug("scourgify failed on %s – %s", scourgify_input, exc)
            return scourgify_input

    parsed_records = [_parse_row(r) for r in records]
    parsed_df = pd.json_normalize(parsed_records)

    # --- Phase 2: Applying Custom Formatting ---
    def _clean(series: pd.Series) -> pd.Series:
        return series.fillna("").astype(str).replace(r"^(none|null|nan)$", "", regex=True).str.strip()

    if 'city' in parsed_df.columns:
        parsed_df['city'] = _clean(parsed_df['city']).str.title()
    
    if "address_line_1" in parsed_df.columns:
        addr1 = _clean(parsed_df["address_line_1"])
        addr2 = _clean(parsed_df.get("address_line_2", pd.Series(index=parsed_df.index)))
        full_street = (addr1 + " " + addr2).str.strip()
        parsed_df["address_line_1"] = full_street.apply(intelligent_title_case)
        if "address_line_2" in parsed_df.columns:
            parsed_df.drop(columns=["address_line_2"], inplace=True)

    if 'postal_code' in parsed_df.columns:
        parsed_df['postal_code'] = _clean(parsed_df['postal_code']).apply(root_zip)

    # --- Phase 3: Update Original DataFrame ---
    rename_map = {
        scourgify_key: df_col 
        for scourgify_key, df_col in col_map.items() 
        if scourgify_key in parsed_df.columns and scourgify_key != 'state'
    }
    if rename_map:
        parsed_df.rename(columns=rename_map, inplace=True)
        parsed_df.index = df.index
        df.update(parsed_df)

    return df
