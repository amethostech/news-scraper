# NER Pipeline for Company Name Extraction

This folder contains the Named Entity Recognition (NER) pipeline for extracting company names from news articles.

## Features

- **spaCy NER**: Fast baseline extraction (~85% accuracy)
- **Hugging Face Transformers**: Higher accuracy extraction (~93%+)
- **SEC/Stock Validation**: Cross-reference against 24,000+ known companies from:
  - SEC EDGAR (10,000+ US public companies)
  - NASDAQ (4,000+ stocks)
  - NYSE (2,700+ stocks)

## Setup

```bash
cd NER

# Install dependencies
pip install -r requirements.txt

# Download spaCy model
python -m spacy download en_core_web_sm
```

## Files

```
NER/
├── README.md              # This file
├── requirements.txt       # Python dependencies
├── ner_pipeline.py        # Main enhanced NER pipeline
├── huggingface_ner.py     # Hugging Face transformer NER module
├── company_validator.py   # SEC/stock exchange validation
└── data/
    ├── test_articles.csv  # Input: Copy of merged_articles_cleaned.csv
    ├── ner_output.csv     # Output: Articles with extracted companies
    └── cache/             # Cached SEC/stock company data
        ├── companies.json
        └── meta.json
```

## Usage

### Quick Test (100 rows, spaCy only)
```bash
python ner_pipeline.py
```

### With Hugging Face NER
```bash
python ner_pipeline.py --hf
```

### With Validation
```bash
python ner_pipeline.py --validate
```

### Full Pipeline (spaCy + Hugging Face + Validation)
```bash
python ner_pipeline.py --hf --validate
```

### Full Dataset
```bash
python ner_pipeline.py --full --hf --validate
```

### Command Line Options

| Option | Description |
|--------|-------------|
| `--sample N` | Process only N rows (default: 100) |
| `--full` | Process entire dataset |
| `--hf` | Enable Hugging Face NER |
| `--validate` | Enable SEC/stock validation |
| `--spacy-model MODEL` | spaCy model (`en_core_web_sm` or `en_core_web_lg`) |
| `--hf-model TYPE` | HF model: `fast`, `accurate`, or `balanced` |
| `--no-spacy` | Disable spaCy (use only HF) |

## Output Columns

| Column | Description |
|--------|-------------|
| `NER_Companies_spaCy` | Companies extracted by spaCy |
| `NER_Companies_HF` | Companies extracted by Hugging Face |
| `NER_Companies` | Combined unique companies |
| `NER_Companies_Validated` | Companies verified in SEC/stock data |
| `NER_Companies_Unvalidated` | Companies not found in SEC/stock data |

## Sample Results

From test run on 50 articles:
- **88%** of articles had companies extracted
- **16%** had companies validated against SEC/stock data
- Validation cached **24,160 companies** from SEC, NASDAQ, NYSE

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    NER Pipeline                              │
├─────────────────────────────────────────────────────────────┤
│  1. spaCy NER (fast)       → Extract ORG entities           │
│  2. Hugging Face (option)  → Higher accuracy extraction     │
│  3. Combine & Dedupe       → Merge results                  │
│  4. Validation (option)    → Check against SEC/stock data   │
└─────────────────────────────────────────────────────────────┘
```

## Notes

1. **First run** will download:
   - Hugging Face model (~430 MB)
   - SEC/stock company data (~24K companies)

2. **Validation cache** is refreshed every 7 days automatically

3. **Unvalidated companies** may still be valid (private companies, international, etc.)

## Next Steps

- [ ] Add Indian stock exchanges (BSE, NSE)
- [ ] Fine-tune model on pharma/healthcare domain
- [ ] Add fuzzy matching for company name variations
- [ ] Export unique validated companies list
