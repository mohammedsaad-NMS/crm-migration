#!/usr/bin/env python3
"""
Advanced Interactive Fuzzy Deduplication Tool
=============================================
This script finds clusters of duplicates and uses a custom UI dialog to allow
the user to confirm the members of each group in a single step.
"""
import logging
from pathlib import Path
import re
from collections import defaultdict
import tkinter as tk
from tkinter import messagebox, filedialog, Label, Button, Checkbutton, Frame, Toplevel, StringVar

import pandas as pd
from thefuzz import fuzz

# --- Basic Configuration ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

# --- Settings ---
SIMILARITY_THRESHOLD = 85
ID_COLUMN = "Record Id"
MODIFIED_TIME_COLUMN = "Modified Time"
CACHE_DIR = Path("cache")

# --- UI Class for Group Review ---
class GroupReviewDialog(Toplevel):
    """
    A custom dialog to review and confirm members of a duplicate group.
    It now accepts a list of (display_text, unique_id) tuples.
    """
    def __init__(self, parent, group_items_with_ids):
        super().__init__(parent)
        self.title("Review Potential Duplicate Group")
        self.transient(parent)
        self.grab_set()

        self.approved_ids = []
        self.vars = []

        Label(self, text="The following records were found to be similar.\nUn-check any that are NOT duplicates.", justify="left", padx=10).pack(pady=10)
        
        items_frame = Frame(self, relief="sunken", borderwidth=1)
        items_frame.pack(pady=5, padx=10, fill="both", expand=True)

        for display_text, record_id in group_items_with_ids:
            var = StringVar(value=record_id) # Use the ID as the value
            cb = Checkbutton(items_frame, text=display_text, variable=var, onvalue=record_id, offvalue="", anchor='w')
            cb.pack(fill='x', padx=5, pady=2)
            self.vars.append(var)

        button_frame = Frame(self)
        button_frame.pack(pady=10)
        Button(button_frame, text="Confirm Selections", command=self.on_confirm).pack(side="left", padx=10)
        Button(button_frame, text="Skip Group", command=self.on_skip).pack(side="left")

        self.protocol("WM_DELETE_WINDOW", self.on_skip)

    def on_confirm(self):
        self.approved_ids = [var.get() for var in self.vars if var.get()]
        self.destroy()

    def on_skip(self):
        self.approved_ids = []
        self.destroy()


def main():
    root = tk.Tk()
    root.withdraw()

    log.info("Opening file dialog to select input CSV...")
    input_path_str = filedialog.askopenfilename(title="Select the CSV file to analyze", filetypes=[("CSV Files", "*.csv")])
    if not input_path_str:
        log.info("No file selected. Exiting script.")
        return
    input_path = Path(input_path_str)

    prefix = input_path.stem.split('_')[0]
    text_column = (prefix[:-1] if prefix.endswith('s') else prefix) + " Name"
    log.info(f"Dedup column derived from filename: '{text_column}'")

    try:
        csv_columns = pd.read_csv(input_path, nrows=0).columns
        if text_column not in csv_columns:
            messagebox.showerror("Column Not Found", f"Column '{text_column}' not found in the file.")
            return
        
        df = pd.read_csv(input_path, dtype=str).dropna(subset=[text_column, MODIFIED_TIME_COLUMN])
        df[MODIFIED_TIME_COLUMN] = pd.to_datetime(df[MODIFIED_TIME_COLUMN])
    except Exception as e:
        messagebox.showerror("Error", f"Failed to read or process CSV:\n{e}")
        return
        
    log.info("Finding potential duplicate groups...")
    names_to_compare = df[text_column].unique()
    linkage, groups = {}, defaultdict(list)

    for name1 in names_to_compare:
        if name1 in linkage: continue
        linkage[name1] = name1
        groups[name1].append(name1)
        for name2 in names_to_compare:
            if name1 == name2 or name2 in linkage: continue
            if fuzz.token_sort_ratio(name1, name2) >= SIMILARITY_THRESHOLD:
                linkage[name2] = name1
                groups[name1].append(name2)
    
    name_counts = df[text_column].value_counts()
    perfect_dupe_names = name_counts[name_counts > 1].index
    for name in perfect_dupe_names:
        if not any(name in L for L in groups.values()):
            log.info(f"Found standalone perfect-match group: '{name}'")
            groups[name] = [name]

    user_decisions = []
    log.info(f"Found {len(groups)} potential groups. Starting interactive review...")
    for representative_name, similar_names_list in groups.items():
        group_df = df[df[text_column].isin(similar_names_list)].copy()

        if len(group_df) < 2:
            continue

        log.info(f"Presenting group for review: {similar_names_list}")
        
        # *** THE CRITICAL FIX IS HERE ***
        # Create a list of tuples with (display_text, record_id) to ensure
        # each checkbox is unique, even if names are identical.
        items_for_dialog = []
        for _, row in group_df.iterrows():
            display_text = f"{row[text_column]} (ID: {row[ID_COLUMN]})"
            items_for_dialog.append((display_text, row[ID_COLUMN]))
        
        dialog = GroupReviewDialog(root, items_for_dialog)
        root.wait_window(dialog)
        
        approved_record_ids = dialog.approved_ids
        
        if len(approved_record_ids) < 2:
            log.info("Group skipped or less than 2 items selected.")
            continue

        # Filter the group dataframe based on the selected IDs
        final_group_df = group_df[group_df[ID_COLUMN].isin(approved_record_ids)].copy()

        log.info(f"User confirmed {len(final_group_df)} records for merge.")

        canonical_record = final_group_df.loc[final_group_df[MODIFIED_TIME_COLUMN].idxmax()]
        canonical_id = canonical_record[ID_COLUMN]
        canonical_name = canonical_record[text_column]
        log.info(f"Canonical for this group is '{canonical_name}' (ID: {canonical_id})")

        for _, potential_match_record in final_group_df.iterrows():
            if potential_match_record[ID_COLUMN] == canonical_id:
                continue
            
            user_decisions.append({
                'canonical_record_id': canonical_id,
                'canonical_name': canonical_name,
                'duplicate_record_id': potential_match_record[ID_COLUMN],
                'duplicate_name': potential_match_record[text_column],
                'user_decision': "MERGE"
            })
            log.info(f"Decision: MERGE '{potential_match_record[text_column]}' (ID: {potential_match_record[ID_COLUMN]}) into canonical.")

    if not user_decisions:
        log.info("Interactive session complete. No merge decisions were confirmed.")
        messagebox.showinfo("Complete", "No merge decisions were made.")
        return

    result_df = pd.DataFrame(user_decisions)
    CACHE_DIR.mkdir(exist_ok=True)
    cache_file_path = CACHE_DIR / f"decisions_{input_path.stem}.csv"
    result_df.to_csv(cache_file_path, index=False)

    log.info(f"Session complete. Decisions cached to: {cache_file_path}")
    messagebox.showinfo("Complete", f"Session complete.\nDecisions cached to:\n{cache_file_path}")

if __name__ == "__main__":
    main()