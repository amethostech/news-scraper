import pandas as pd
import re
import numpy as np
from datetime import datetime

file_path = "/Users/a91788/Desktop/hb-022/news-scraper/data/merged_articles.csv"
print("Loading CSV file...")
df = pd.read_csv(file_path, low_memory=False)
print(f"Loaded {len(df)} rows")

source_col = "Body/abstract/extract"

# Source code mapping: Maps source names to 2-digit codes (01-19)
# Format: {source_name: 'SS'} where SS is 2-digit code
SOURCE_CODE_MAPPING = {
    # Normalize source names (case-insensitive matching will be used)
    'GEN': '01',
    'gen': '01',
    'BIOSPACE': '02',
    'biospace': '02',
    'BioSpace': '02',
    'BIOPHARMADIVE': '03',
    'biopharmadive': '03',
    'BioPharmaDive': '03',
    'BioPharma Dive': '03',
    'biopharma dive': '03',
    'BIOWORLD': '04',
    'bioworld': '04',
    'BioWorld': '04',
    'CEN': '05',
    'cen': '05',
    'C&EN News': '05',
    'c&en news': '05',
    'ENDPOINTSNEWS': '06',
    'endpointsnews': '06',
    'EndpointsNews': '06',
    'EUROPEANPHARMACEUTICALREVIEW': '07',
    'europeanpharmaceuticalreview': '07',
    'EuropeanPharmaceuticalReview': '07',
    'European Pharmaceutical Review': '07',
    'european pharmaceutical review': '07',
    'FIERCEBIOTECH': '08',
    'fiercebiotech': '08',
    'FierceBiotech': '08',
    'Fierce Biotech': '08',
    'fierce biotech': '08',
    'FIERCEPHARMA': '09',
    'fiercepharma': '09',
    'FiercePharma': '09',
    'Fierce Pharma': '09',
    'fierce pharma': '09',
    'MDPI': '10',
    'mdpi': '10',
    'NATUREBIOTECH': '11',
    'naturebiotech': '11',
    'NatureBiotech': '11',
    'Nature Biotechnology': '11',
    'nature biotechnology': '11',
    'PHARMAVOICE': '12',
    'pharmavoice': '12',
    'PharmaVoice': '12',
    'Pharma Voice': '12',
    'pharma voice': '12',
    'PHARMACEUTICALTECH': '13',
    'pharmaceuticaltech': '13',
    'PharmaceuticalTech': '13',
    'Pharmaceutical Technology': '13',
    'pharmaceutical technology': '13',
    'PHARMATIMES': '14',
    'pharmatimes': '14',
    'PharmaTimes': '14',
    'Pharma Times': '14',
    'pharma times': '14',
    'PHARMAPHORUM': '15',
    'pharmaphorum': '15',
    'Pharmaphorum': '15',
    'PMLIVE': '16',
    'pmlive': '16',
    'PMLive': '16',
    'PM Live': '16',
    'pm live': '16',
    'PRNEWSWIRE': '17',
    'prnewswire': '17',
    'PRNewswire': '17',
    'PR Newswire': '17',
    'pr newswire': '17',
    'STATNEWS': '18',
    'statnews': '18',
    'STATNews': '18',
    'STAT News': '18',
    'stat news': '18',
    'THESCIENTIST': '19',
    'thescientist': '19',
    'TheScientist': '19',
    'The Scientist': '19',
    'the scientist': '19',
    'FDA': '20',
    'fda': '20',
    'Fda': '20',
    'BUSINESSWIRE': '21',
    'businesswire': '21',
    'BusinessWire': '21',
    'Business Wire': '21',
    'business wire': '21',
    'BUSINESSWEEKLY': '22',
    'businessweekly': '22',
    'BusinessWeekly': '22',
    'Business Weekly': '22',
    'business weekly': '22',
    'BIOSPECTRUM': '23',
    'biospectrum': '23',
    'Biospectrum': '23',
    'PHARMALETTER': '24',
    'pharmaletter': '24',
    'Pharmaletter': '24',
}

def normalize_source_name(source):
    """Normalize source name for lookup (handle variations)"""
    if pd.isna(source):
        return None
    source_str = str(source).strip()
    # Try exact match first
    if source_str in SOURCE_CODE_MAPPING:
        return SOURCE_CODE_MAPPING[source_str]
    # Try case-insensitive match
    source_lower = source_str.lower()
    for key, code in SOURCE_CODE_MAPPING.items():
        if key.lower() == source_lower:
            return code
    return None

def convert_date_to_ddmmyy(date_value):
    """Convert date from YYYY-MM-DD format to DDMMYY format
    Returns '000000' if date is missing (as per requirements)
    """
    if pd.isna(date_value) or date_value == '':
        return '000000'  # Use 00 for missing dates
    
    try:
        # Try parsing as string first
        date_str = str(date_value).strip()
        if not date_str or date_str == 'nan' or date_str.lower() == 'date':
            return '000000'  # Use 00 for missing/invalid dates
        
        # Try parsing YYYY-MM-DD format
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            # Try other common formats
            try:
                date_obj = datetime.strptime(date_str, '%Y/%m/%d')
            except ValueError:
                try:
                    # Try parsing as ISO format
                    date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                except ValueError:
                    # If all parsing fails, return '000000'
                    return '000000'
        
        # Format as DDMMYY
        return date_obj.strftime('%d%m%y')
    except Exception:
        return '000000'  # Use 00 for any errors

def is_numeric_source(source):
    """Check if source is a numeric ID (should be skipped)"""
    if pd.isna(source):
        return True
    source_str = str(source).strip()
    # Check if it's purely numeric (and not a valid source name)
    if source_str.isdigit():
        return True
    # Check for invalid source names
    if source_str.lower() in ['_id', 'date', 'nan']:
        return True
    return False

def generate_amethos_id_base(row):
    """Generate base Amethos ID in format NSSDDMMYY (without sequence number)
    - Skips rows with numeric source IDs
    - Uses '000000' for missing dates
    """
    # Get source code
    source = row.get('Source', '')
    
    # Skip numeric source IDs
    if is_numeric_source(source):
        return ''  # Leave blank for numeric IDs
    
    source_code = normalize_source_name(source)
    
    if source_code is None:
        return ''  # Unknown source (not in 19 listed), leave blank
    
    # Get date and convert to DDMMYY (returns '000000' if missing)
    date_value = row.get('Date', '')
    date_code = convert_date_to_ddmmyy(date_value)
    
    # Combine: N (prefix) + SS (source code) + DDMMYY (date, or 000000 if missing)
    amethos_id_base = f'N{source_code}{date_code}'
    return amethos_id_base

# Generate Amethos Id codes with unique sequence numbers
print("\nGenerating Amethos Id codes...")
if 'Amethos Id' not in df.columns:
    df['Amethos Id'] = ''

# Step 1: Generate base IDs (NSSDDMMYY) for all rows
df['Amethos Id'] = df.apply(generate_amethos_id_base, axis=1)

# Step 2: Add sequence numbers to make IDs unique for same source + date
# For rows with the same base ID, add 3-digit sequence numbers (001, 002, 003, etc.)
mask = df['Amethos Id'] != ''  # Only process rows with base IDs
if mask.sum() > 0:
    # Group by base ID and add sequence numbers using cumcount
    df.loc[mask, 'Amethos Id'] = (
        df.loc[mask, 'Amethos Id'] + 
        df.loc[mask].groupby('Amethos Id').cumcount().add(1).astype(str).str.zfill(3)
    )

# Count how many codes were generated
codes_generated = (df['Amethos Id'] != '').sum()
codes_missing = (df['Amethos Id'] == '').sum()
print(f"Generated {codes_generated} unique Amethos Id codes")
print(f"Missing codes (no date or unknown source): {codes_missing}")

def remove_boilerplate(text):
    """Remove common boilerplate text patterns from article endings"""
    if pd.isna(text) or str(text).strip() == "":
        return text
    
    text_str = str(text)
    
    # Common boilerplate patterns (case-insensitive, match complete sentences/phrases)
    boilerplate_patterns = [
        # Subscription prompts - most common patterns
        r'to read the rest of this story subscribe to[^.]*\.',
        r'to read the full (article|story)[^.]*subscribe[^.]*\.',
        r'to read the full (article|story)[^.]*sign (up|in)[^.]*\.',
        r'to read the full article sign up for free or sign in\.',
        r'to read the full story subscribe or sign in\.',
        r'subscribe to[^.]*stat\+[^.]*\.',
        r'subscribe to[^.]*stat[^.]*\.',
        r'subscribe to[^.]*premium[^.]*\.',
        r'subscribe now[^.]*\.',
        r'sign up for[^.]*premium[^.]*\.',
        
        # GenePool and similar daily/weekly update prompts
        r'get daily news updates[^!.]*[!.]',
        r'get weekly news updates[^!.]*[!.]',
        r'when you subscribe to genepool[^!.]*[!.]',
        r'subscribe to genepool[^!.]*[!.]',
        r'join genepool[^!.]*[!.]',
        r'sign up for genepool[^!.]*[!.]',
        r'get the latest news[^!.]*subscribe[^!.]*[!.]',
        r'stay updated[^!.]*subscribe[^!.]*[!.]',
        r'never miss[^!.]*subscribe[^!.]*[!.]',
        r'don\'t miss[^!.]*subscribe[^!.]*[!.]',
        
        # Correction requests
        r'to submit a correction request[^.]*\.',
        r'to submit a correction[^.]*\.',
        r'correction request[^.]*contact us[^.]*\.',
        r'please visit our contact us page\.',
        r'visit our contact us page\.',
        r'contact us[^.]*correction[^.]*\.',
        
        # Newsletter/signup prompts
        r'sign up for[^.]*newsletter[^.]*\.',
        r'subscribe to[^.]*newsletter[^.]*\.',
        r'get[^.]*newsletter[^.]*\.',
        r'receive[^.]*newsletter[^.]*\.',
        r'get free access to[^.]*articles[^.]*newsletters[^.]*\.',
        r'choose newsletters to get straight to your inbox\.',
        r'sign up to receive[^!.]*[!.]',
        r'subscribe to receive[^!.]*[!.]',
        
        # Social media prompts
        r'follow us on[^.]*\.',
        r'like us on facebook[^.]*\.',
        r'connect with us on[^.]*\.',
        r'join us on[^.]*linkedin[^.]*\.',
        
        # Author bio patterns (only if at end, followed by subscription/correction)
        r'[a-z]+ [a-z]+ covers[^.]*\.\s*(to read|subscribe|to submit)',
        r'[a-z]+ [a-z]+ contributes to[^.]*\.\s*(to read|subscribe|to submit)',
        
        # Generic prompts
        r'for more information[^.]*\.',
        r'read more at[^.]*\.',
        r'click here[^!.]*[!.]',  # Matches click here... ending with . or !
        r'please click here[^!.]*[!.]',
        r'learn more[^.]*\.',
        r'continue reading[^.]*\.',
        
        # Common ending CTAs
        r'start your free trial[^!.]*[!.]',
        r'try it free[^!.]*[!.]',
        r'register now[^!.]*[!.]',
        r'register for free[^!.]*[!.]',
        r'create your free account[^!.]*[!.]',
        
        # Paywall/subscription prompts
        r'signin or subscribe[^!.]*[!.]',
        r'login or subscribe[^!.]*[!.]',
        r'please login or subscribe[^!.]*[!.]',
        r'subscribe now for[^!.]*[!.]',
        r'for instant access[^!.]*[!.]',
        r'\d+ word remain[^!.]*[!.]?',
    ]
    
    # Remove each pattern
    for pattern in boilerplate_patterns:
        text_str = re.sub(pattern, '', text_str, flags=re.IGNORECASE)
    
    # Remove everything after common ending markers (more aggressive cleanup)
    # These patterns remove everything from a marker to the end
    ending_markers = [
        r'\.?\s*to read the rest.*$',           # Everything after "to read the rest"
        r'\.?\s*to read the full.*$',          # Everything after "to read the full"
        r'\.?\s*to continue reading.*$',       # Everything after "to continue reading"
        r'\.?\s*subscribe now.*$',             # Everything after "subscribe now"
        r'\.?\s*signin or subscribe.*$',       # Everything after "signin or subscribe"
        r'\.?\s*login or subscribe.*$',        # Everything after "login or subscribe"
        r'\.?\s*please login or subscribe.*$', # Everything after "please login or subscribe"
        r'\.?\s*to submit a correction.*$',    # Everything after "to submit a correction"
        r'\.?\s*click here to.*$',             # Everything after "click here to"
        r'\.?\s*click here for.*$',            # Everything after "click here for"
        r'\.?\s*and it\'?s all free.*$',       # Everything after "and it's all free"
        r'\.?\s*subscribe today.*$',           # Everything after "subscribe today"
        r'\.?\s*login here.*$',                # Everything after "login here"
        r'\.?\s*for more information and to place your order.*$',  # Sales prompts
        r'\.?\s*\d+ word remain.*$',           # Paywall word count
    ]
    
    for marker in ending_markers:
        text_str = re.sub(marker, '.', text_str, flags=re.IGNORECASE)
    
    # Clean up multiple consecutive periods/spaces
    text_str = re.sub(r'\.{2,}', '.', text_str)
    text_str = re.sub(r'\s{3,}', ' ', text_str)
    
    return text_str.strip()

def clean_text_vectorized(series):
    """Enhanced vectorized text cleaning function for NLP/lemmatization"""
    # Convert to string, handling NaN
    text_series = series.astype(str)
    
    # Replace NaN strings with empty
    text_series = text_series.replace('nan', '')
    
    # Remove boilerplate text (do this before other cleaning to preserve sentence structure)
    text_series = text_series.apply(remove_boilerplate)
    
    # Remove URLs (http, https, www)
    text_series = text_series.str.replace(r'https?://\S+|www\.\S+', '', regex=True)
    
    # Remove email addresses
    text_series = text_series.str.replace(r'\S+@\S+', '', regex=True)
    
    # Remove common unwanted prefixes/suffixes (like "jatsp", "jats", etc.)
    # These are often metadata tags that appear at start/end of text
    # Handle cases where they're attached to words (e.g., "jatspreeclampsia" -> "reeclampsia")
    text_series = text_series.str.replace(r'^jatsp', '', regex=True, case=False)  # Remove "jatsp" at start
    text_series = text_series.str.replace(r'jatsp$', '', regex=True, case=False)  # Remove "jatsp" at end
    text_series = text_series.str.replace(r'^jats', '', regex=True, case=False)    # Remove "jats" at start
    text_series = text_series.str.replace(r'jats$', '', regex=True, case=False)    # Remove "jats" at end
    text_series = text_series.str.replace(r'^abstract\b', '', regex=True, case=False)
    text_series = text_series.str.replace(r'^doi\b', '', regex=True, case=False)
    text_series = text_series.str.replace(r'^pmid\b', '', regex=True, case=False)
    
    # Convert to lowercase
    text_series = text_series.str.lower()
    
    # Remove punctuation (keep only word characters and spaces)
    # Keep apostrophes for contractions (e.g., "don't" -> "dont" is handled, but we can keep it)
    text_series = text_series.str.replace(r'[^\w\s\']', '', regex=True)
    
    # Normalize whitespace (multiple spaces/tabs/newlines to single space)
    text_series = text_series.str.replace(r'\s+', ' ', regex=True)
    
    # Remove standalone single characters (often artifacts)
    text_series = text_series.str.replace(r'\b\w\b', '', regex=True)
    
    # Normalize whitespace again after removing single chars
    text_series = text_series.str.replace(r'\s+', ' ', regex=True)
    
    # Strip leading/trailing whitespace
    text_series = text_series.str.strip()
    
    return text_series

def get_first_word(text):
    """Get first word from text"""
    if pd.isna(text) or str(text).strip() == "":
        return ""
    words = re.sub(r'[^\w\s]', '', str(text)).lower().split()
    return words[0] if words else ""

def get_last_word(text):
    """Get last word from text"""
    if pd.isna(text) or str(text).strip() == "":
        return ""
    words = re.sub(r'[^\w\s]', '', str(text)).lower().split()
    return words[-1] if words else ""

print("\nCleaning all text in one pass...")
# Clean all text at once using vectorized operations
df["Cleaned_Text_G"] = clean_text_vectorized(df[source_col])

# Add lemmatization module
print("Setting up lemmatization...")
try:
    import nltk
    from nltk.stem import WordNetLemmatizer
    from nltk.corpus import stopwords
    
    # Download required NLTK data if not already present
    try:
        nltk.data.find('tokenizers/punkt')
    except LookupError:
        print("Downloading NLTK punkt tokenizer...")
        nltk.download('punkt', quiet=True)
    
    try:
        nltk.data.find('corpora/wordnet')
    except LookupError:
        print("Downloading NLTK wordnet...")
        nltk.download('wordnet', quiet=True)
    
    try:
        nltk.data.find('corpora/stopwords')
    except LookupError:
        print("Downloading NLTK stopwords...")
        nltk.download('stopwords', quiet=True)
    
    lemmatizer = WordNetLemmatizer()
    stop_words = set(stopwords.words('english'))
    
    def lemmatize_text(text):
        """Lemmatize text for NER processing (keeping stopwords as NER may need them)"""
        if pd.isna(text) or str(text).strip() == "":
            return ""
        
        # Tokenize
        words = str(text).split()
        
        # Lemmatize each word (keep all words including stopwords for NER)
        lemmatized_words = []
        for word in words:
            if word:  # Keep all words for NER processing
                lemmatized = lemmatizer.lemmatize(word)
                lemmatized_words.append(lemmatized)
        
        return ' '.join(lemmatized_words)
    
    print("Applying lemmatization...")
    # Apply lemmatization to cleaned text and replace Cleaned_Text_G with lemmatized version
    df["Cleaned_Text_G"] = df["Cleaned_Text_G"].apply(lemmatize_text)
    print("Lemmatization complete! Cleaned_Text_G now contains lemmatized text.")
    
    # Post-lemmatization cleanup for remaining boilerplate (works on lowercased text without punctuation)
    print("Applying post-lemmatization boilerplate cleanup...")
    post_clean_patterns = [
        r'click here.*$',  # Everything from "click here" to end
        r'to register click here.*$',
        r'signin or subscribe.*$',
        r'login or subscribe.*$',
        r'please login or subscribe.*$',
        r'subscribe now.*$',
        r'for instant access.*$',
        r'\d+ word remain.*$',
        r'register now.*$',
        r'subscribe today.*$',
    ]
    
    def post_clean_boilerplate(text):
        if pd.isna(text) or str(text).strip() == "":
            return text
        text_str = str(text)
        for pattern in post_clean_patterns:
            text_str = re.sub(pattern, '', text_str, flags=re.IGNORECASE)
        return text_str.strip()
    
    df["Cleaned_Text_G"] = df["Cleaned_Text_G"].apply(post_clean_boilerplate)
    print("Post-lemmatization cleanup complete!")
    
except ImportError:
    print("NLTK not available. Installing...")
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "nltk"])
    print("Please run the script again after NLTK installation.")
    # Keep cleaned text as is if lemmatization fails
except Exception as e:
    print(f"Error in lemmatization: {e}")
    print("Continuing without lemmatization (using cleaned text as is)...")

print("Performing QC checks...")
# QC: Check that cleaning and lemmatization was applied
mask_empty_orig = df[source_col].isna() | (df[source_col].astype(str).str.strip() == "")
mask_cleaned_empty = (df["Cleaned_Text_G"].astype(str).str.strip() == "") | (df["Cleaned_Text_G"].astype(str).str.len() < 10)
mask_has_cleaned = (df["Cleaned_Text_G"].astype(str).str.strip() != "") & ~mask_empty_orig

# Set QC status:
# - "empty" if original was empty OR if cleaned is empty (boilerplate-only content)
# - "ok" if we have meaningful cleaned/lemmatized text
# - "fail" only if original had content but cleaning failed completely
df["QC_H"] = np.where(mask_empty_orig | mask_cleaned_empty, "empty", 
                     np.where(mask_has_cleaned, "ok", "fail"))

output_path = "/Users/a91788/Desktop/hb-022/news-scraper/data/merged_articles_cleaned.csv"
df.to_csv(output_path, index=False)

# Also update the original merged_articles.csv with Amethos Id codes
original_output_path = "/Users/a91788/Desktop/hb-022/news-scraper/data/merged_articles.csv"
print(f"\nUpdating original file with Amethos Id codes...")
# Read original file to preserve its structure
df_original = pd.read_csv(original_output_path, low_memory=False)
# Update only the Amethos Id column
if 'Amethos Id' in df_original.columns:
    df_original['Amethos Id'] = df['Amethos Id']
else:
    df_original.insert(0, 'Amethos Id', df['Amethos Id'])
# Save original file with codes
df_original.to_csv(original_output_path, index=False)
print(f"Original file updated: {original_output_path}")

print(f"\nProcessing complete! Output saved to: {output_path}")
print(f"Total rows processed: {len(df)}")
print(f"Rows with Amethos Id codes: {(df['Amethos Id'].notna() & (df['Amethos Id'].astype(str).str.strip() != '') & (df['Amethos Id'].astype(str) != 'nan')).sum()}")
print(f"Rows with cleaned & lemmatized text: {(df['Cleaned_Text_G'].astype(str).str.strip() != '').sum()}")
print(f"Empty rows: {(df['QC_H'] == 'empty').sum()}")
print(f"\nNote: Cleaned_Text_G now contains lemmatized text (ready for NER)")
print(f"\nQC status counts:")
print(df["QC_H"].value_counts())
