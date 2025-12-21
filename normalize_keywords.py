"""
Normalize keywords in the cleaned articles CSV based on a synonym config file.

Reads config/normalization_rules.csv and data/merged_articles_cleaned.csv.
Adds:
1. 'matched_keywords': list of primary keys identified.
2. 'Consolidated_Text': article body with synonyms replaced by primary keys.
"""

import pandas as pd
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config/normalization_rules.csv"
INPUT_PATH = ROOT / "data/merged_articles_cleaned.csv"
OUTPUT_PATH = INPUT_PATH
TMP_PATH = ROOT / "data/merged_articles_cleaned.norm.tmp"

def load_rules():
    """
    Load normalization rules from config file.
    Expected format: PrimaryKey: Synonym1, Synonym2, ...
    """
    rules = {}
    if not CONFIG_PATH.exists():
        print(f"Warning: Configuration file not found at {CONFIG_PATH}")
        return rules

    with open(CONFIG_PATH, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip comments and empty lines
            if not line or line.startswith('#'):
                continue
            if ':' not in line:
                continue
            key, synonyms_str = line.split(':', 1)
            key = key.strip()
            synonyms = [s.strip() for s in synonyms_str.split(',') if s.strip()]
            # Ensure the key itself is not lost if it's not in synonyms
            # But for replacement, we want to replace synonyms with the key.
            # We don't need to replace the key with itself.
            rules[key] = synonyms
    return rules

def normalize():
    if not INPUT_PATH.exists():
        print(f"Error: Input file not found at {INPUT_PATH}")
        return

    rules = load_rules()
    if not rules:
        print("No normalization rules found. Skipping.")
        return

    print(f"Loaded {len(rules)} normalization rules.")

    # Prepare regex patterns for each key
    # We sort synonyms by length (descending) to avoid partial matches of shorter synonyms
    # e.g., if we have "GSK" and "GSK India", we replace "GSK India" first.
    patterns = {}
    for key, synonyms in rules.items():
        if not synonyms:
            # If no synonyms, just use the key itself for matching
            escaped_syns = [re.escape(key)]
        else:
            # Sort synonyms by length descending
            sorted_syns = sorted(synonyms, key=len, reverse=True)
            escaped_syns = [re.escape(s) for s in sorted_syns]
            # Also add the key if it's not in synonyms (for matching purposes)
            if key not in synonyms:
                escaped_syns.append(re.escape(key))
        
        # Pattern for matching/identifying the presence of the keyword
        match_pattern = re.compile(r'\b(' + '|'.join(escaped_syns) + r')\b', re.IGNORECASE)
        
        # Pattern for REPLACING synonyms with the key
        # We only want to replace actual synonyms, not the key itself
        if synonyms:
            replace_pattern = re.compile(r'\b(' + '|'.join([re.escape(s) for s in sorted(synonyms, key=len, reverse=True)]) + r')\b', re.IGNORECASE)
        else:
            replace_pattern = None
            
        patterns[key] = {
            'match': match_pattern,
            'replace': replace_pattern
        }

    # Process in chunks to handle large files
    chunksize = 5000
    first_chunk = True

    column_to_search = "Body/abstract/extract"
    headline_column = "Headline"

    for chunk in pd.read_csv(INPUT_PATH, chunksize=chunksize, low_memory=False):
        if column_to_search not in chunk.columns:
            print(f"Error: Column '{column_to_search}' not found in CSV.")
            return
        
        # Combine Headline and Body for keyword identification
        if headline_column in chunk.columns:
            text_for_id = chunk[headline_column].fillna("") + " " + chunk[column_to_search].fillna("")
        else:
            text_for_id = chunk[column_to_search].fillna("")

        matched_lists = []
        consolidated_texts = []

        for i, text in enumerate(text_for_id):
            text_str = str(text)
            matches = []
            
            # 1. Identify matches
            for key, pats in patterns.items():
                if pats['match'].search(text_str):
                    matches.append(key)
            matched_lists.append("; ".join(matches) if matches else "")

        # 2. Consolidate text (Body only)
        # We use a placeholder approach to prevent double-replacements (e.g. Novo -> Novo Nordisk)
        consolidated_texts = []
        for i in range(len(chunk)):
            body_text = str(chunk.iloc[i][column_to_search]) if pd.notnull(chunk.iloc[i][column_to_search]) else ""
            temp_text = body_text
            
            # Use unique placeholders for each key to avoid recursive replacements
            key_placeholders = {key: f"__NORM_KEY_{idx}__" for idx, key in enumerate(patterns.keys())}
            
            # Step A: Replace all occurrences (keys and synonyms) with placeholders
            for key, pats in patterns.items():
                # For replacement, we include BOTH the key itself and synonyms in one pass
                # to ensure we don't partially replace a key that was already there.
                all_variants = [key]
                if rules[key]:
                    all_variants.extend(rules[key])
                
                # Sort by length descending to catch longest matches first
                all_variants = sorted(list(set(all_variants)), key=len, reverse=True)
                variant_pattern = re.compile(r'\b(' + '|'.join([re.escape(v) for v in all_variants]) + r')\b', re.IGNORECASE)
                
                temp_text = variant_pattern.sub(key_placeholders[key], temp_text)
            
            # Step B: Replace placeholders with the final primary keys (highlighted)
            for key, placeholder in key_placeholders.items():
                temp_text = temp_text.replace(placeholder, f"[{key}]")
            
            consolidated_texts.append(temp_text)

        chunk["matched_keywords"] = matched_lists
        chunk["Consolidated_Text"] = consolidated_texts
        
        mode = 'w' if first_chunk else 'a'
        chunk.to_csv(TMP_PATH, index=False, mode=mode, header=first_chunk)
        first_chunk = False

    # Replace original file with temporary file
    if TMP_PATH.exists():
        TMP_PATH.replace(OUTPUT_PATH)
        print(f"Successfully updated {OUTPUT_PATH} with 'matched_keywords' and 'Consolidated_Text' columns.")
    else:
        print("Error: Temporary file was not created.")

if __name__ == "__main__":
    normalize()
