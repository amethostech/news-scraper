import pandas as pd
from pathlib import Path
import subprocess
import sys

# Create a small sample CSV
sample_csv = Path("data/sample_test.csv")
sample_csv.parent.mkdir(exist_ok=True)

data = {
    "Amethos Id": ["T1", "T2", "T3"],
    "Headline": ["Merck and Keytruda results", "Novo and Ozempic update", "Generic names test"],
    "Body/abstract/extract": [
        "Merck Sharp & Dohme announced that pembrolizumab is effective.", 
        "Novo Nordisk announced that semaglutide and Wegovy are selling well.", 
        "We are testing Glaxo and Janssen."
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

test_script = Path("test_normalize_v2.py")
test_script.write_text(test_content)

# Run the test script
print("Running normalization on sample data...")
subprocess.run([sys.executable, str(test_script)])

# Check results
df = pd.read_csv(sample_csv)
print("\nResults:")
pd.set_option('display.max_colwidth', None)
print(df[["Headline", "matched_keywords", "Consolidated_Text"]])

# Cleanup
test_script.unlink()
# sample_csv.unlink() # Keep it for inspection
