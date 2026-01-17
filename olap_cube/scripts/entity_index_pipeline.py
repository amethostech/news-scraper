"""
Entity Index Pipeline

Unified script that merges entity normalization and indexing functionality.
Reads cleaned articles and matches against:
- Entity_input.csv (companies)
- ailment_index.csv (diseases/conditions)
- finevent_index.csv (financial events/deals)
- drug_index.csv (drugs/medications)
- therapy_index.csv (therapeutic areas)
- regulatory_index.csv (regulatory/clinical terms)

Outputs: Entity_Index, Ailment_Index, FinEvent_Index, Drug_Index, Therapy_Index, Regulatory_Index columns
"""

import pandas as pd
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT.parent / "data"
INDICES_DIR = ROOT / "indices"

# Input files
INPUT_CSV = DATA_DIR / "merged_articles_cleaned.csv"
ENTITY_CSV = INDICES_DIR / "Entity_input.csv"
AILMENT_CSV = INDICES_DIR / "ailment_index.csv"
FINEVENT_CSV = INDICES_DIR / "finevent_index.csv"
DRUG_CSV = INDICES_DIR / "drug_index.csv"
THERAPY_CSV = INDICES_DIR / "therapy_index.csv"
REGULATORY_CSV = INDICES_DIR / "regulatory_index.csv"

# Output file
OUTPUT_CSV = DATA_DIR / "output_indexed.csv"

# Column to search in
TEXT_COLUMN = "Cleaned_Text_G"
HEADLINE_COLUMN = "Headline"


def load_entity_index():
    """Load entity index and build lookup patterns"""
    if not ENTITY_CSV.exists():
        print(f"Warning: {ENTITY_CSV} not found")
        return {}, {}
    
    df = pd.read_csv(ENTITY_CSV)
    
    # Build variant -> ID mapping
    variant_to_id = {}
    id_to_canonical = {}
    
    for _, row in df.iterrows():
        variant = str(row['Entity_Variant']).strip()
        entity_id = str(row['Entity_ID']).strip()
        canonical = str(row['Entity_Canonical']).strip()
        
        # Store mapping (lowercase for matching)
        variant_to_id[variant.lower()] = entity_id
        id_to_canonical[entity_id] = canonical
    
    # Sort by length descending for matching (longer phrases first)
    sorted_variants = sorted(variant_to_id.keys(), key=len, reverse=True)
    
    # Build regex patterns grouped by ID
    patterns = {}
    for variant in sorted_variants:
        entity_id = variant_to_id[variant]
        if entity_id not in patterns:
            patterns[entity_id] = []
        patterns[entity_id].append(re.escape(variant))
    
    # Compile patterns
    compiled_patterns = {}
    for entity_id, variants in patterns.items():
        pattern = r'\b(' + '|'.join(variants) + r')\b'
        compiled_patterns[entity_id] = re.compile(pattern, re.IGNORECASE)
    
    return compiled_patterns, id_to_canonical


def load_generic_index(csv_path, index_col_pattern='index', keyword_col_pattern='keyword'):
    """Generic loader for index files with Index and Keyword columns"""
    if not csv_path.exists():
        print(f"Warning: {csv_path} not found")
        return {}
    
    df = pd.read_csv(csv_path)
    
    # Find the keyword column (handle BOM and variations)
    keyword_col = None
    for col in df.columns:
        if keyword_col_pattern.lower() in col.lower():
            keyword_col = col
            break
    
    index_col = None
    for col in df.columns:
        if index_col_pattern.lower() in col.lower():
            index_col = col
            break
    
    if keyword_col is None or index_col is None:
        print(f"Warning: Could not find required columns in {csv_path}")
        print(f"  Columns found: {list(df.columns)}")
        return {}
    
    # Group keywords by index
    patterns = {}
    for _, row in df.iterrows():
        idx = str(row[index_col]).strip()
        keyword = str(row[keyword_col]).strip()
        
        if not idx or idx == 'nan' or not keyword or keyword == 'nan':
            continue
        
        if idx not in patterns:
            patterns[idx] = []
        patterns[idx].append(re.escape(keyword.lower()))
    
    # Compile patterns
    compiled_patterns = {}
    for idx, keywords in patterns.items():
        pattern = r'\b(' + '|'.join(keywords) + r')\b'
        compiled_patterns[idx] = re.compile(pattern, re.IGNORECASE)
    
    return compiled_patterns


def find_matches(text, patterns):
    """Find all matching IDs in text"""
    if pd.isna(text) or not str(text).strip():
        return []
    
    text_str = str(text).lower()
    matched_ids = []
    
    for pattern_id, pattern in patterns.items():
        if pattern.search(text_str):
            matched_ids.append(pattern_id)
    
    return matched_ids


def process_articles():
    """Main processing function"""
    print("=" * 60)
    print("ENTITY INDEX PIPELINE (Extended)")
    print("=" * 60)
    
    # Check input file
    if not INPUT_CSV.exists():
        print(f"Error: Input file not found: {INPUT_CSV}")
        return
    
    # Load indexes
    print("\nLoading indexes...")
    entity_patterns, entity_canonical = load_entity_index()
    print(f"  - Entity patterns: {len(entity_patterns)} unique IDs")
    
    ailment_patterns = load_generic_index(AILMENT_CSV, 'index', 'keyword')
    print(f"  - Ailment patterns: {len(ailment_patterns)} unique IDs")
    
    finevent_patterns = load_generic_index(FINEVENT_CSV, 'index', 'keyword')
    print(f"  - FinEvent patterns: {len(finevent_patterns)} unique IDs")
    
    drug_patterns = load_generic_index(DRUG_CSV, 'index', 'keyword')
    print(f"  - Drug patterns: {len(drug_patterns)} unique IDs")
    
    therapy_patterns = load_generic_index(THERAPY_CSV, 'index', 'keyword')
    print(f"  - Therapy patterns: {len(therapy_patterns)} unique IDs")
    
    regulatory_patterns = load_generic_index(REGULATORY_CSV, 'index', 'keyword')
    print(f"  - Regulatory patterns: {len(regulatory_patterns)} unique IDs")
    
    # Process in chunks
    print(f"\nProcessing articles from: {INPUT_CSV}")
    chunksize = 5000
    first_chunk = True
    total_rows = 0
    
    # Counters for each index type
    match_counts = {
        'entity': 0, 'ailment': 0, 'finevent': 0,
        'drug': 0, 'therapy': 0, 'regulatory': 0
    }
    
    for chunk in pd.read_csv(INPUT_CSV, chunksize=chunksize, low_memory=False):
        # Combine headline and text for matching
        if HEADLINE_COLUMN in chunk.columns and TEXT_COLUMN in chunk.columns:
            search_text = chunk[HEADLINE_COLUMN].fillna("") + " " + chunk[TEXT_COLUMN].fillna("")
        elif TEXT_COLUMN in chunk.columns:
            search_text = chunk[TEXT_COLUMN].fillna("")
        else:
            print(f"Error: Column '{TEXT_COLUMN}' not found in CSV")
            return
        
        # Find matches for each row
        entity_index_list = []
        ailment_index_list = []
        finevent_index_list = []
        drug_index_list = []
        therapy_index_list = []
        regulatory_index_list = []
        matched_entities_list = []
        
        for text in search_text:
            # Entity matches
            e_ids = find_matches(text, entity_patterns)
            entity_index_list.append("; ".join(sorted(e_ids)) if e_ids else "")
            
            # Get canonical names for matched entities
            canonical_names = [entity_canonical.get(eid, "") for eid in e_ids]
            canonical_names = list(set([n for n in canonical_names if n]))
            matched_entities_list.append("; ".join(sorted(canonical_names)) if canonical_names else "")
            
            # Ailment matches
            a_ids = find_matches(text, ailment_patterns)
            ailment_index_list.append("; ".join(sorted(a_ids)) if a_ids else "")
            
            # FinEvent matches
            f_ids = find_matches(text, finevent_patterns)
            finevent_index_list.append("; ".join(sorted(f_ids)) if f_ids else "")
            
            # Drug matches
            d_ids = find_matches(text, drug_patterns)
            drug_index_list.append("; ".join(sorted(d_ids)) if d_ids else "")
            
            # Therapy matches
            t_ids = find_matches(text, therapy_patterns)
            therapy_index_list.append("; ".join(sorted(t_ids)) if t_ids else "")
            
            # Regulatory matches
            r_ids = find_matches(text, regulatory_patterns)
            regulatory_index_list.append("; ".join(sorted(r_ids)) if r_ids else "")
            
            # Count matches
            if e_ids: match_counts['entity'] += 1
            if a_ids: match_counts['ailment'] += 1
            if f_ids: match_counts['finevent'] += 1
            if d_ids: match_counts['drug'] += 1
            if t_ids: match_counts['therapy'] += 1
            if r_ids: match_counts['regulatory'] += 1
        
        # Add new columns
        chunk["Entity_Index"] = entity_index_list
        chunk["Ailment_Index"] = ailment_index_list
        chunk["FinEvent_Index"] = finevent_index_list
        chunk["Drug_Index"] = drug_index_list
        chunk["Therapy_Index"] = therapy_index_list
        chunk["Regulatory_Index"] = regulatory_index_list
        chunk["matched_entities"] = matched_entities_list
        
        # Write output
        mode = 'w' if first_chunk else 'a'
        chunk.to_csv(OUTPUT_CSV, index=False, mode=mode, header=first_chunk)
        first_chunk = False
        total_rows += len(chunk)
        print(f"  Processed {total_rows} rows...")
    
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"Total rows processed: {total_rows}")
    print(f"Rows with Entity matches: {match_counts['entity']}")
    print(f"Rows with Ailment matches: {match_counts['ailment']}")
    print(f"Rows with FinEvent matches: {match_counts['finevent']}")
    print(f"Rows with Drug matches: {match_counts['drug']}")
    print(f"Rows with Therapy matches: {match_counts['therapy']}")
    print(f"Rows with Regulatory matches: {match_counts['regulatory']}")
    print(f"\nOutput saved to: {OUTPUT_CSV}")
    print("=" * 60)


if __name__ == "__main__":
    process_articles()
