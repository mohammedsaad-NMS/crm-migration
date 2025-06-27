#!/usr/bin/env python3
"""
Contacts Loader — Guardians • Emergency • Legacy Contacts
"""

from __future__ import annotations
import logging, re
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None

from scripts.etl_lib import (
    read_mapping, read_target_catalog, assert_target_pairs_exist,
    transform_legacy_df,
    intelligent_title_case, strip_translation,
    standardize_address_block, digits_only_phone
)

# ───────── CONFIG ─────────
BASE = Path(__file__).resolve().parent
ACCOUNTS_CSV = BASE.parent / "mapping" / "legacy-exports" / "Accounts_2025_06_24.csv"
CONTACTS_CSV = BASE.parent / "mapping" / "legacy-exports" / "Contacts_2025_06_25.csv"
OUTPUT_DIR   = BASE.parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

logging.basicConfig(level="INFO",
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

# Role labels
ROLES = {
    "Primary":   "Primary Guardian",
    "Secondary": "Secondary Guardian",
    "Third":     "Tertiary Guardian",
    "Emergency": "Emergency Contact",
}

# Opt-out flip
OPT_COLS = ["Opt-out Email", "Opt-out Text (SMS)", "Opt-out Directory"]
OPT_FLIP = {"Yes": "FALSE", "Yes/Sí": "FALSE", "No": "TRUE", "No/No": "TRUE"}

ARTEFACT = ", Address Line 2/Línea de dirección 2"
EXCLUDE_TYPES = {"Primary Guardian","Secondary Guardian","Third Guardian","Star"}

ROLE_FIELD     = "Role (General)"
EMERGENCY_FLAG = "Emergency Contact"   # Boolean flag

# ───────── MAIN ─────────
def main() -> None:
    mapping  = read_mapping().query("`Target Module` == 'Contacts'")
    catalog  = read_target_catalog()
    assert_target_pairs_exist("Contacts", mapping, catalog)

    ui_cols = (catalog
        .query("`User-Facing Module Name` == 'Contacts'")
        .query("`Data Source / Type`.str.contains('Related List') == False "
               "and `Data Source / Type`.str.contains('System') == False")
        ["User-Facing Field Name"].tolist())

    for fld in (ROLE_FIELD, EMERGENCY_FLAG):
        if fld not in ui_cols:
            raise ValueError(f"Target catalog missing required field: {fld}")

    rename_map: Dict[str, str] = dict(zip(mapping["Legacy Field"],
                                          mapping["Target Field"]))

    # ---------- 1. Guardians & Emergency from Accounts ----------
    df_acc = pd.read_csv(ACCOUNTS_CSV, dtype=str)
    df_acc = df_acc[df_acc["Account Type"].str.strip().eq("Star")]

    acc_rows: List[dict] = []

    for _, acc in df_acc.iterrows():
        persons: List[dict] = []

        def grab_block(key: str):
            prefix = f"{key} "
            legacy = [c for c in acc.index if c.startswith(prefix) and c in rename_map]
            if not legacy:
                return None
            sub = acc[legacy].copy()
            if key == "Secondary" and "Secondary Guardian Street" in sub.index:
                sub["Secondary Guardian Street"] = (
                    str(sub["Secondary Guardian Street"]).replace(ARTEFACT, "").strip()
                )
            rec = {rename_map[c]: sub[c] for c in legacy if pd.notna(sub[c])}
            if rec:
                rec[ROLE_FIELD] = ROLES.get(key)
            return rec

        for g_key in ("Primary","Secondary","Third"):
            p = grab_block(g_key)
            if p:
                p[EMERGENCY_FLAG] = False
                persons.append(p)

        em = grab_block("Emergency")
        if em:
            fn = em.get("First Name","").strip().lower()
            ln = em.get("Last Name","").strip().lower()
            matched = False
            for g in persons:
                if fn and ln and \
                   fn == g.get("First Name","").strip().lower() and \
                   ln == g.get("Last Name","").strip().lower():
                    g[EMERGENCY_FLAG] = True
                    matched = True
                    break
            if not matched:
                em[EMERGENCY_FLAG] = True
                persons.append(em)

        for rec in persons:
            for col in OPT_COLS:
                if col in rec:
                    rec[col] = strip_translation(OPT_FLIP.get(rec[col], rec[col]))
            if "Preferred Language" in rec:
                rec["Preferred Language"] = strip_translation(rec["Preferred Language"])

        acc_rows.extend(persons)

    df_accounts = pd.DataFrame(acc_rows)
    df_accounts["_source"] = "Accounts" # Add source for prioritization

    if not df_accounts.empty:
        mask = (
            df_accounts["First Name"].fillna("").str.strip().eq("") &
            df_accounts["Last Name"].fillna("").str.strip().eq("")
        )
        df_accounts = df_accounts[~mask]

    # ---------- 2. Legacy Contacts (filtered) -------------------
    df_legacy_raw = pd.read_csv(CONTACTS_CSV, dtype=str)
    if "Contact Type" in df_legacy_raw.columns:
        df_legacy_raw = df_legacy_raw[
            ~df_legacy_raw["Contact Type"].str.strip().isin(EXCLUDE_TYPES)]

    df_legacy_raw = df_legacy_raw[
        ~df_legacy_raw.apply(
            lambda r: r.astype(str).str.contains(r"test", case=False, na=False).any(),
            axis=1)]

    df_legacy = transform_legacy_df(
        df_legacy_raw,
        mapping[mapping["Legacy Module"] == "Contacts"])
    df_legacy["_source"] = "Contacts" # Add source for prioritization

    dupes = df_legacy.columns[df_legacy.columns.duplicated()].unique()
    if dupes.any():
        raise ValueError(f"Duplicate target columns: {dupes.tolist()}")

    # ---------- 3. Concatenate and Deduplicate -------
    df_all = pd.concat([df_accounts, df_legacy],
                       ignore_index=True, sort=False)
    
    # --- NEW: Populate defaults for records from legacy Contacts source ---
    # These columns are only populated for 'Accounts' records during initial processing.
    # We are setting the default for all other records (from Contacts module) here.
    df_all[EMERGENCY_FLAG].fillna(False, inplace=True)
    df_all['Preferred Language'].fillna('English', inplace=True)
    # --- END NEW ---

    df_all['_original_order'] = df_all.index

    fn_norm = df_all["First Name"].str.lower().str.strip().fillna('')
    ln_norm = df_all["Last Name"].str.lower().str.strip().fillna('')
    df_all['_normalized_name_key'] = np.minimum(fn_norm, ln_norm) + '|' + np.maximum(fn_norm, ln_norm)

    df_all['_source_priority'] = df_all['_source'].map({"Accounts": 0, "Contacts": 1})
    df_all.sort_values(by=['_normalized_name_key', '_source_priority'], inplace=True)

    duplicates_mask = df_all.duplicated(subset=['_normalized_name_key'], keep='first')
    records_to_drop = df_all[duplicates_mask]

    if not records_to_drop.empty:
        log.info("The following duplicate records will be removed (keeping the record from 'Accounts' source where available):")
        for _, row in records_to_drop.iterrows():
            log.info(f"  - Removing: '{row['First Name']} {row['Last Name']}' (Source: {row['_source']})")

    df_all = df_all[~duplicates_mask].copy()
    log.info(f"De-duplication complete. {len(df_all)} unique contacts remain.")


    # ---------- 4-A. Order by Role ------------------------------
    primary_role_order = ["Primary Guardian", "Secondary Guardian", "Tertiary Guardian", "Emergency Contact", "Mentor"]
    other_roles = sorted(
        df_all[ROLE_FIELD].dropna().unique()[~pd.Series(df_all[ROLE_FIELD].dropna().unique()).isin(primary_role_order)]
    )
    full_role_order = primary_role_order + other_roles
    
    df_all[ROLE_FIELD] = pd.Categorical(df_all[ROLE_FIELD], categories=full_role_order, ordered=True)
    df_all.sort_values(by=[ROLE_FIELD, '_original_order'], inplace=True)
    df_all.reset_index(drop=True, inplace=True)


    # ---------- 4-B. Final cleaners -----------------------------
    for name_col in ("First Name","Last Name"):
        if name_col in df_all.columns:
            df_all[name_col] = df_all[name_col].apply(intelligent_title_case)

    standardize_address_block(df_all, {
        "address_line_1": "Mailing Street",
        "city":           "Mailing City",
        "state":          "Mailing State",
        "postal_code":    "Mailing Zip Code",
    })

    df_all['Phone'] = digits_only_phone(df_all['Phone'])
    df_all['Email'] = df_all['Email'].str.lower()


    # --- NEW: Set default opt-out values for any empty rows ---
    if "Opt-out Email" in df_all.columns:
        df_all["Opt-out Email"].fillna("FALSE", inplace=True)
    if "Opt-out Text (SMS)" in df_all.columns:
        df_all["Opt-out Text (SMS)"].fillna("FALSE", inplace=True)
    if "Opt-out Directory" in df_all.columns:
        df_all["Opt-out Directory"].fillna("TRUE", inplace=True)
    # --- END NEW ---

    blank_contact = df_all["Email"].fillna("").str.strip().eq("") & \
                    df_all["Phone"].fillna("").str.strip().eq("")
    df_all = pd.concat([df_all[~blank_contact], df_all[blank_contact]], ignore_index=True)

    # ---------- 5. Column set & order ---------------------------
    helper_cols = [c for c in df_all.columns if c.startswith('_')]
    df_all.drop(columns=helper_cols, inplace=True)
    
    for col in ui_cols:
        if col not in df_all.columns:
            df_all[col] = pd.NA
    df_all = df_all[[c for c in ui_cols if c in df_all.columns]]

    # ---------- 6. Write output ---------------------------------
    out_file = OUTPUT_DIR / "Contacts.csv"
    df_all.to_csv(out_file, index=False)
    log.info("Wrote %s (%d rows)", out_file, len(df_all))


if __name__ == "__main__":
    main()