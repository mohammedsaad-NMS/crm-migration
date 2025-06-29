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
TEXT_COLUMN = "Product Name"
MODIFIED_TIME_COLUMN = "Modified Time"
CACHE_DIR = Path("cache")

# --- UI Class for Group Review ---
class GroupReviewDialog(Toplevel):
    """A custom dialog to review and confirm members of a duplicate group."""
    def __init__(self, parent, group_items):
        super().__init__(parent)
        self.title("Review Potential Duplicate Group")
        self.transient(parent)
        self.grab_set()

        self.approved_subset = []
        self.vars = []

        Label(self, text="The following items were found to be similar.\nUn-check any items that are NOT duplicates.", justify="left", padx=10).pack(pady=10)
        
        items_frame = Frame(self, relief="sunken", borderwidth=1)
        items_frame.pack(pady=5, padx=10, fill="both", expand=True)

        for item in group_items:
            var = StringVar(value=item)
            cb = Checkbutton(items_frame, text=item, variable=var, onvalue=item, offvalue="", anchor='w')
            cb.pack(fill='x', padx=5, pady=2)
            self.vars.append(var)

        button_frame = Frame(self)
        button_frame.pack(pady=10)
        Button(button_frame, text="Confirm Selections", command=self.on_confirm).pack(side="left", padx=10)
        Button(button_frame, text="Skip Group", command=self.on_skip).pack(side="left")

        self.protocol("WM_DELETE_WINDOW", self.on_skip)

    def on_confirm(self):
        self.approved_subset = [var.get() for var in self.vars if var.get()]
        self.destroy()

    def on_skip(self):
        self.approved_subset = [] # Return an empty list if skipped
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

    try:
        df = pd.read_csv(input_path, dtype=str).dropna(subset=[TEXT_COLUMN, MODIFIED_TIME_COLUMN])
        df[MODIFIED_TIME_COLUMN] = pd.to_datetime(df[MODIFIED_TIME_COLUMN])
    except Exception as e:
        messagebox.showerror("Error", f"Failed to read or process CSV:\n{e}")
        return
        
    log.info("Finding potential duplicate groups...")
    names_to_compare = df[TEXT_COLUMN].unique()
    linkage, groups = {}, defaultdict(list)
    for name1 in names_to_compare:
        if name1 in linkage: continue
        linkage[name1] = name1
        groups[name1].append(name1)
        for name2 in names_to_compare:
            if name1 == name2 or name2 in linkage: continue
            score = fuzz.token_sort_ratio(name1, name2)
            if score >= SIMILARITY_THRESHOLD:
                linkage[name2] = name1
                groups[name1].append(name2)
    
    log.info(f"Found {len(groups)} potential duplicate groups. Starting interactive review...")

    user_decisions = []
    for representative_name, similar_names_list in groups.items():
        if len(similar_names_list) < 2:
            continue

        log.info(f"Presenting group for review: {similar_names_list}")
        dialog = GroupReviewDialog(root, similar_names_list)
        root.wait_window(dialog)
        
        approved_subset = dialog.approved_subset
        
        if len(approved_subset) < 2:
            log.info("Group skipped or not enough items selected for a match.")
            continue

        # Process the user-confirmed subset of duplicates
        log.info(f"User confirmed subset: {approved_subset}")
        group_df = df[df[TEXT_COLUMN].isin(approved_subset)].copy()
        
        # Find the canonical record (newest) within the approved subset
        canonical_record = group_df.loc[group_df[MODIFIED_TIME_COLUMN].idxmax()]
        canonical_id = canonical_record[ID_COLUMN]
        canonical_name = canonical_record[TEXT_COLUMN]
        log.info(f"Canonical for this group is '{canonical_name}'")

        # Create merge decisions for all other items in the subset
        for index, potential_match_record in group_df.iterrows():
            if potential_match_record[ID_COLUMN] == canonical_id:
                continue
            
            user_decisions.append({
                'canonical_record_id': canonical_id,
                'canonical_name': canonical_name,
                'duplicate_record_id': potential_match_record[ID_COLUMN],
                'duplicate_name': potential_match_record[TEXT_COLUMN],
                'user_decision': "MERGE"
            })
            log.info(f"Decision: MERGE '{potential_match_record[TEXT_COLUMN]}' into '{canonical_name}'")

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