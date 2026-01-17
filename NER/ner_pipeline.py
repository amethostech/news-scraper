"""
Enhanced NER Pipeline for Company Name Extraction

Combines multiple NER approaches:
1. spaCy NER - Fast baseline
2. Hugging Face Transformers - Higher accuracy
3. Validation Layer - Cross-reference against SEC/stock data

Usage:
    python ner_pipeline.py                    # Run with spaCy on sample
    python ner_pipeline.py --full             # Run on full dataset
    python ner_pipeline.py --hf               # Use Hugging Face NER
    python ner_pipeline.py --validate         # Include validation
    python ner_pipeline.py --hf --validate    # Full pipeline
"""

import argparse
import pandas as pd
import spacy
from pathlib import Path
from tqdm import tqdm
import re
import sys

# Configuration
ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
INPUT_CSV = DATA_DIR / "test_articles.csv"
OUTPUT_CSV = DATA_DIR / "ner_output.csv"

# Column to extract entities from
TEXT_COLUMN = "Cleaned_Text_G"
HEADLINE_COLUMN = "Headline"

# Batch size for processing
BATCH_SIZE = 100


def load_spacy_model(model_name="en_core_web_sm"):
    """Load spaCy model, download if not available"""
    try:
        nlp = spacy.load(model_name)
        print(f"Loaded spaCy model: {model_name}")
        return nlp
    except OSError:
        print(f"Model {model_name} not found. Downloading...")
        spacy.cli.download(model_name)
        nlp = spacy.load(model_name)
        return nlp


def clean_company_name(name):
    """Clean and normalize company names"""
    if not name:
        return None
    
    # Remove common suffixes that add noise
    suffixes = [
        r'\s+(Inc\.?|Corp\.?|Ltd\.?|LLC|PLC|Co\.?|Company|Corporation|Limited)$',
        r'\s+(Holdings?|Group|International|Intl\.?)$'
    ]
    
    cleaned = name.strip()
    for suffix in suffixes:
        cleaned = re.sub(suffix, '', cleaned, flags=re.IGNORECASE)
    
    return cleaned.strip() if cleaned else None


class EnhancedNERPipeline:
    """Enhanced NER pipeline with multiple extraction methods"""
    
    def __init__(
        self, 
        use_spacy: bool = True,
        use_huggingface: bool = False,
        use_validation: bool = False,
        spacy_model: str = "en_core_web_sm",
        hf_model: str = "fast"
    ):
        """
        Initialize the NER pipeline.
        
        Args:
            use_spacy: Use spaCy NER
            use_huggingface: Use Hugging Face transformer NER
            use_validation: Validate against SEC/stock data
            spacy_model: spaCy model name
            hf_model: Hugging Face model type ('fast', 'accurate', 'balanced')
        """
        self.use_spacy = use_spacy
        self.use_huggingface = use_huggingface
        self.use_validation = use_validation
        
        # Initialize spaCy
        if use_spacy:
            print("\n--- Loading spaCy ---")
            self.spacy_nlp = load_spacy_model(spacy_model)
            # Disable unused components for speed
            self.spacy_nlp.disable_pipes([
                pipe for pipe in self.spacy_nlp.pipe_names if pipe != "ner"
            ])
        
        # Initialize Hugging Face
        if use_huggingface:
            print("\n--- Loading Hugging Face NER ---")
            from huggingface_ner import HuggingFaceNER
            self.hf_ner = HuggingFaceNER(model_type=hf_model)
        
        # Initialize Validator
        if use_validation:
            print("\n--- Loading Company Validator ---")
            from company_validator import CompanyValidator
            self.validator = CompanyValidator()
    
    def extract_with_spacy(self, text: str) -> list:
        """Extract companies using spaCy"""
        if pd.isna(text) or not str(text).strip():
            return []
        
        doc = self.spacy_nlp(str(text)[:100000])
        
        companies = []
        for ent in doc.ents:
            if ent.label_ == "ORG":
                cleaned = clean_company_name(ent.text)
                if cleaned and len(cleaned) > 1:
                    companies.append(cleaned)
        
        return list(dict.fromkeys(companies))  # Remove duplicates
    
    def extract_with_huggingface(self, text: str) -> list:
        """Extract companies using Hugging Face"""
        if pd.isna(text) or not str(text).strip():
            return []
        
        return self.hf_ner.extract_companies(str(text))
    
    def validate_companies(self, companies: list) -> tuple:
        """Validate companies against known sources"""
        validated, unvalidated = self.validator.filter_companies(companies, return_all=True)
        return validated, unvalidated
    
    def extract(self, text: str) -> dict:
        """
        Extract companies using all enabled methods.
        
        Returns:
            dict with keys:
                - spacy_companies: Companies from spaCy
                - hf_companies: Companies from Hugging Face
                - combined: All unique companies
                - validated: Validated companies (if validation enabled)
                - unvalidated: Unvalidated companies (if validation enabled)
        """
        result = {
            "spacy_companies": [],
            "hf_companies": [],
            "combined": [],
            "validated": [],
            "unvalidated": []
        }
        
        # Extract with spaCy
        if self.use_spacy:
            result["spacy_companies"] = self.extract_with_spacy(text)
        
        # Extract with Hugging Face
        if self.use_huggingface:
            result["hf_companies"] = self.extract_with_huggingface(text)
        
        # Combine results
        all_companies = list(dict.fromkeys(
            result["spacy_companies"] + result["hf_companies"]
        ))
        result["combined"] = all_companies
        
        # Validate
        if self.use_validation and all_companies:
            validated, unvalidated = self.validate_companies(all_companies)
            result["validated"] = validated
            result["unvalidated"] = unvalidated
        
        return result


def process_articles(
    input_csv: Path = INPUT_CSV,
    output_csv: Path = OUTPUT_CSV,
    sample_size: int = None,
    use_spacy: bool = True,
    use_huggingface: bool = False,
    use_validation: bool = False,
    spacy_model: str = "en_core_web_sm",
    hf_model: str = "fast"
):
    """
    Process articles and extract company names.
    
    Args:
        input_csv: Path to input CSV
        output_csv: Path to output CSV
        sample_size: If set, only process this many rows
        use_spacy: Use spaCy NER
        use_huggingface: Use Hugging Face NER
        use_validation: Validate against SEC/stock data
        spacy_model: spaCy model name
        hf_model: Hugging Face model type
    """
    print("=" * 70)
    print("ENHANCED NER PIPELINE - Company Name Extraction")
    print("=" * 70)
    
    # Check input file
    if not input_csv.exists():
        print(f"Error: Input file not found: {input_csv}")
        return
    
    # Initialize pipeline
    print("\nInitializing NER Pipeline...")
    print(f"  spaCy: {use_spacy} ({spacy_model if use_spacy else 'N/A'})")
    print(f"  Hugging Face: {use_huggingface} ({hf_model if use_huggingface else 'N/A'})")
    print(f"  Validation: {use_validation}")
    
    pipeline = EnhancedNERPipeline(
        use_spacy=use_spacy,
        use_huggingface=use_huggingface,
        use_validation=use_validation,
        spacy_model=spacy_model,
        hf_model=hf_model
    )
    
    # Read input CSV
    print(f"\nReading input CSV: {input_csv}")
    
    if sample_size:
        df = pd.read_csv(input_csv, nrows=sample_size, low_memory=False)
        print(f"  Loaded sample of {len(df)} rows")
    else:
        df = pd.read_csv(input_csv, low_memory=False)
        print(f"  Loaded {len(df)} rows")
    
    # Prepare text for NER
    if HEADLINE_COLUMN in df.columns and TEXT_COLUMN in df.columns:
        df["_ner_text"] = df[HEADLINE_COLUMN].fillna("") + " " + df[TEXT_COLUMN].fillna("")
    elif TEXT_COLUMN in df.columns:
        df["_ner_text"] = df[TEXT_COLUMN].fillna("")
    elif "content" in df.columns:
        df["_ner_text"] = df["content"].fillna("")
    else:
        # Try to find a text column
        text_cols = [c for c in df.columns if 'text' in c.lower() or 'content' in c.lower()]
        if text_cols:
            df["_ner_text"] = df[text_cols[0]].fillna("")
            print(f"  Using column '{text_cols[0]}' for NER")
        else:
            print(f"Error: Could not find text column. Available columns: {list(df.columns)}")
            return
    
    # Process articles
    print("\nExtracting entities...")
    
    spacy_results = []
    hf_results = []
    combined_results = []
    validated_results = []
    unvalidated_results = []
    
    for text in tqdm(df["_ner_text"], desc="Processing articles"):
        result = pipeline.extract(text)
        
        spacy_results.append("; ".join(result["spacy_companies"]))
        hf_results.append("; ".join(result["hf_companies"]))
        combined_results.append("; ".join(result["combined"]))
        validated_results.append("; ".join(result["validated"]))
        unvalidated_results.append("; ".join(result["unvalidated"]))
    
    # Add columns
    if use_spacy:
        df["NER_Companies_spaCy"] = spacy_results
    
    if use_huggingface:
        df["NER_Companies_HF"] = hf_results
    
    df["NER_Companies"] = combined_results
    
    if use_validation:
        df["NER_Companies_Validated"] = validated_results
        df["NER_Companies_Unvalidated"] = unvalidated_results
    
    # Remove temporary column
    df = df.drop(columns=["_ner_text"])
    
    # Save output
    print(f"\nSaving output to: {output_csv}")
    df.to_csv(output_csv, index=False)
    
    # Statistics
    total_with_companies = sum(1 for x in combined_results if x)
    validated_count = sum(1 for x in validated_results if x) if use_validation else 0
    
    print("\n" + "=" * 70)
    print("RESULTS")
    print("=" * 70)
    print(f"Total rows processed: {len(df)}")
    print(f"Rows with companies found: {total_with_companies} ({100*total_with_companies/len(df):.1f}%)")
    
    if use_validation:
        print(f"Rows with validated companies: {validated_count} ({100*validated_count/len(df):.1f}%)")
    
    print(f"\nOutput saved to: {output_csv}")
    print("=" * 70)
    
    # Show sample results
    print("\n--- Sample Results (first 5 rows with companies) ---")
    sample = df[df["NER_Companies"] != ""].head(5)
    
    for idx, row in sample.iterrows():
        headline = str(row.get(HEADLINE_COLUMN, row.get("title", "")))[:60]
        companies = row["NER_Companies"][:80]
        print(f"\n[{idx}] {headline}...")
        print(f"    Companies: {companies}...")
        
        if use_validation:
            validated = row.get("NER_Companies_Validated", "")[:50]
            print(f"    Validated: {validated}...")
    
    return df


def main():
    """Main entry point with command line arguments"""
    parser = argparse.ArgumentParser(
        description="Enhanced NER Pipeline for Company Extraction"
    )
    
    parser.add_argument(
        "--input", "-i",
        type=str,
        default=str(INPUT_CSV),
        help="Input CSV file path"
    )
    
    parser.add_argument(
        "--output", "-o",
        type=str,
        default=str(OUTPUT_CSV),
        help="Output CSV file path"
    )
    
    parser.add_argument(
        "--sample", "-s",
        type=int,
        default=100,
        help="Sample size (number of rows). Set to 0 for full dataset."
    )
    
    parser.add_argument(
        "--full", "-f",
        action="store_true",
        help="Process full dataset (overrides --sample)"
    )
    
    parser.add_argument(
        "--hf", "--huggingface",
        action="store_true",
        help="Use Hugging Face transformer NER"
    )
    
    parser.add_argument(
        "--validate", "-v",
        action="store_true",
        help="Validate companies against SEC/stock data"
    )
    
    parser.add_argument(
        "--spacy-model",
        type=str,
        default="en_core_web_sm",
        help="spaCy model to use (en_core_web_sm or en_core_web_lg)"
    )
    
    parser.add_argument(
        "--hf-model",
        type=str,
        default="fast",
        choices=["fast", "accurate", "balanced"],
        help="Hugging Face model type"
    )
    
    parser.add_argument(
        "--no-spacy",
        action="store_true",
        help="Disable spaCy (use only Hugging Face)"
    )
    
    args = parser.parse_args()
    
    # Determine sample size
    sample_size = None if args.full else (args.sample if args.sample > 0 else None)
    
    # Run pipeline
    process_articles(
        input_csv=Path(args.input),
        output_csv=Path(args.output),
        sample_size=sample_size,
        use_spacy=not args.no_spacy,
        use_huggingface=args.hf,
        use_validation=args.validate,
        spacy_model=args.spacy_model,
        hf_model=args.hf_model
    )


if __name__ == "__main__":
    main()
