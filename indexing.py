import pandas as pd

# File paths
REFERENCE_CSV = "search_index.csv"
DATA_CSV = "merged_articles_cleaned.csv"
OUTPUT_CSV = "indexed.csv"

# Column names (change if needed)
REFERENCE_KEYWORD_COL = "Keyword"
REFERENCE_INDEX_COL = "Index"
DATA_TEXT_COL = "Cleaned_Text_G"
NEW_INDEX_COL = "matched_index_id"

# Read CSV files
ref_df = pd.read_csv(REFERENCE_CSV)
data_df = pd.read_csv(DATA_CSV)

# Convert keywords to lowercase for case-insensitive matching
ref_df[REFERENCE_KEYWORD_COL] = ref_df[REFERENCE_KEYWORD_COL].str.lower()

def find_index_id(text):
    if pd.isna(text):
        return None

    text = text.lower()
    for _, row in ref_df.iterrows():
        if row[REFERENCE_KEYWORD_COL] in text:
            return row[REFERENCE_INDEX_COL]
    return None

# Apply matching logic
data_df[NEW_INDEX_COL] = data_df[DATA_TEXT_COL].apply(find_index_id)

# Write output
data_df.to_csv(OUTPUT_CSV, index=False)

print("Processing complete. Output written to:", OUTPUT_CSV)
