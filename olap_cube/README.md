# OLAP Cube Star Schema Package

This package contains everything needed to transform news articles into OLAP cube-ready star schema format and analyze the data.

## Package Contents

### 📁 Code (parsers/)
- **main_transformer.py** - Main entry point for transformation
- **batch_processor.py** - Handles batch processing of large CSV files
- **star_schema_builder.py** - Builds star schema tables
- **entity_extractor.py** - Extracts company/organization entities
- **tag_matcher.py** - Matches articles to tags
- **text_normalizer.py** - Normalizes article text
- **excel_tag_loader.py** - Loads tag definitions from Excel

### 📚 Documentation (parsers/)
- **README.md** - Main documentation for the transformer
- **ANALYSIS_GUIDE.md** - Guide for analyzing the star schema data
- **POWER_BI_SETUP_GUIDE.md** - Step-by-step Power BI setup instructions
- **COMPANY_NAMES_CSV_GUIDE.md** - Guide for managing company names

### ⚙️ Configuration Files
- **config/company_names.csv** - Curated list of company names for entity extraction
- **config/normalization_rules.csv** - Rules for normalizing keywords
- **News Search Tags 101.xlsx** - Tag definitions and categories

### 📊 Star Schema Data (data/star_schema/)
- **Fact_Document.csv** - Central fact table (78,059 articles)
- **Dim_Time.csv** - Time dimension (4,358 time periods)
- **Dim_Source.csv** - Source dimension (22 sources)
- **Dim_Tag.csv** - Tag dimension (45 tags)
- **Dim_Entity.csv** - Entity dimension (195 companies/organizations)
- **Bridge_Fact_Tag.csv** - Document-Tag relationships (122,573)
- **Bridge_Fact_Entity.csv** - Document-Entity relationships with Mention_Count (29,862)
- **rejected_entities.csv** - Entities rejected during extraction (for review)

## Quick Start

### 1. Transform Your Data

```bash
python3 parsers/main_transformer.py --yes
```

### 2. Import to Power BI

Follow the instructions in `parsers/POWER_BI_SETUP_GUIDE.md`

### 3. Analyze Data

See `parsers/ANALYSIS_GUIDE.md` for analysis examples

## Requirements

- Python 3.7+
- pandas
- openpyxl (for Excel file reading)

## Key Features

✅ **Star Schema Design** - Optimized for OLAP cube analysis  
✅ **Mention Count Tracking** - Tracks how many times entities are mentioned  
✅ **Batch Processing** - Handles large datasets efficiently  
✅ **Entity Deduplication** - Handles name variations (e.g., "AstraZeneca" vs "Astra Zeneca")  
✅ **Rejected Entity Tracking** - Review and improve entity extraction  

## File Sizes

- **Fact_Document.csv**: ~483 MB (main fact table)
- **Bridge_Fact_Tag.csv**: ~1.5 MB
- **Bridge_Fact_Entity.csv**: ~0.34 MB (includes Mention_Count)
- **Dimension Tables**: < 1 MB each

## Support

For questions or issues, refer to the documentation files in the `parsers/` directory.

