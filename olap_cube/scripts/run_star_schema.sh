#!/bin/bash
# Star Schema Transformer - Run Script
# Transforms merged_articles_cleaned.csv into OLAP-ready star schema format

cd "$(dirname "$0")"

echo "=========================================="
echo "Star Schema Cube-Ready Data Transformer"
echo "=========================================="
echo ""

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
    echo "Activated virtual environment"
    echo ""
fi

# Run the transformer
python3 parsers/main_transformer.py

echo ""
echo "=========================================="
echo "Transformation Complete!"
echo "=========================================="
echo ""
echo "Output files are in: data/star_schema/"
echo ""

