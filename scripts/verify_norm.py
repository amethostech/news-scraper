import pandas as pd
from pathlib import Path
import subprocess
import sys

# Create a small sample CSV
sample_csv = Path("data/sample_test.csv")
sample_csv.parent.mkdir(exist_ok=True)

data = {
    "Amethos Id": ["T1", "T2", "T3", "T4"],
    "Headline": ["GSK announces new results", "Mercedes Benz sales up", "Healthcare news", "Glaxo and Merc partnership"],
    "Body/abstract/extract": [
        "GlaxoSmithKline is doing well.", 
        "The new Benze is out.", 
        "Nothing to see here.", 
        "GSK and Mercedes are collaborating."
    ]
}
pd.DataFrame(data).to_csv(sample_csv, index=False)

# Temporarily point normalize_keywords.py to this sample
script_path = "normalize_keywords.py"
with open(script_path, 'r') as f:
    content = f.read()

# Mock the INPUT_PATH and OUTPUT_PATH for testing
test_content = content.replace('INPUT_PATH = ROOT / "data/merged_articles_cleaned.csv"', 'INPUT_PATH = ROOT / "data/sample_test.csv"')
test_content = test_content.replace('TMP_PATH = ROOT / "data/merged_articles_cleaned.norm.tmp"', 'TMP_PATH = ROOT / "data/sample_test.norm.tmp"')

test_script = Path("test_normalize.py")
test_script.write_text(test_content)

# Run the test script
print("Running normalization on sample data...")
subprocess.run([sys.executable, str(test_script)])

# Check results
df = pd.read_csv(sample_csv)
print("\nResults:")
print(df[["Headline", "matched_keywords"]])

# Cleanup
test_script.unlink()
# sample_csv.unlink() # Keep it to show user if needed
