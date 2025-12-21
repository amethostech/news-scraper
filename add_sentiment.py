"""
Append VADER sentiment scores to the cleaned articles CSV.

Reads data/merged_articles_cleaned.csv, adds sentiment_score, and writes
back atomically (via a temporary file). Blanks are treated as neutral (0.0).
"""

from pathlib import Path
import sys
import subprocess
import pandas as pd

ROOT = Path(__file__).resolve().parent
INPUT_PATH = ROOT / "data/merged_articles_cleaned.csv"
OUTPUT_PATH = INPUT_PATH  # write in place
TMP_PATH = ROOT / "data/merged_articles_cleaned.sentiment.tmp"


def ensure_analyzer():
    """Load SentimentIntensityAnalyzer, installing vaderSentiment if needed."""
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "vaderSentiment"])
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    
    analyzer = SentimentIntensityAnalyzer()
    
    # Pharmaceutical industry "fine-tuning" lexicon
    # We update the default VADER dictionary with domain-specific keywords and intensities
    pharma_lexicon = {
        # Clinical & Regulatory Success
        'breakthrough': 4.0,
        'innovative': 2.5,
        'approved': 3.5,
        'approval': 3.0,
        'cleared': 2.5,
        'clearance': 2.0,
        'fast-track': 2.5,
        'blockbuster': 3.0,
        'efficacy': 2.5,
        'efficacious': 2.5,
        'effective': 2.0,
        'outperform': 2.0,
        'potency': 1.5,
        
        # Financial & Business
        'acquisition': 2.0,
        'merger': 1.5,
        'collaboration': 2.0,
        'partnership': 2.0,
        'growth': 2.0,
        'milestone': 2.5,
        
        # Clinical & Regulatory Failure
        'failed': -4.0,
        'failure': -3.5,
        'rejected': -3.0,
        'rejection': -3.0,
        'denied': -2.5,
        'disappointing': -2.5,
        'setback': -2.5,
        'delay': -2.0,
        'halted': -3.0,
        'suspended': -3.0,
        'terminated': -3.0,
        'discontinued': -2.5,
        
        # Safety & Legal
        'recall': -3.5,
        'toxicity': -3.0,
        'toxic': -3.0,
        'adverse': -2.5,
        'unfavorable': -2.5,
        'lawsuit': -2.5,
        'litigation': -2.0,
        'violation': -2.5,
        'sanction': -2.5,
        'patent-cliff': -2.5,
        
        # Market & Corporate
        'layoff': -2.0,
        'reduction': -1.5,
        'shortfall': -2.0,
        'decline': -2.0,
        'underperform': -2.0
    }
    
    analyzer.lexicon.update(pharma_lexicon)
    return analyzer


def add_sentiment():
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")

    analyzer = ensure_analyzer()

    if TMP_PATH.exists():
        TMP_PATH.unlink()

    stats = {
        "count": 0,
        "sum": 0.0,
        "min": 1.0,
        "max": -1.0,
        "pos": 0,
        "neg": 0,
        "neu": 0,
        "blank": 0,
    }

    chunksize = 5000
    for idx, chunk in enumerate(pd.read_csv(INPUT_PATH, chunksize=chunksize, low_memory=False)):
        if "Body/abstract/extract" not in chunk.columns:
            raise KeyError("Column 'Body/abstract/extract' not found in input CSV.")

        # Use original text instead of Cleaned_Text_G for better VADER accuracy
        # (VADER handles capitalization and punctuation for intensity)
        text_series = chunk["Body/abstract/extract"].fillna("")
        scores = []

        for text in text_series:
            stripped = str(text).strip()
            if not stripped:
                score = 0.0
                stats["blank"] += 1
                stats["neu"] += 1
            else:
                score = analyzer.polarity_scores(stripped).get("compound", 0.0)
                if score > 0.05:
                    stats["pos"] += 1
                elif score < -0.05:
                    stats["neg"] += 1
                else:
                    stats["neu"] += 1

            stats["count"] += 1
            stats["sum"] += score
            stats["min"] = min(stats["min"], score)
            stats["max"] = max(stats["max"], score)
            scores.append(score)

        chunk["sentiment_score"] = scores
        mode = "w" if idx == 0 else "a"
        header = idx == 0
        chunk.to_csv(TMP_PATH, mode=mode, header=header, index=False)

        if (idx + 1) % 10 == 0:
            processed = (idx + 1) * chunksize
            print(f"Processed ~{processed} rows...")

    if not TMP_PATH.exists():
        raise RuntimeError("Temporary sentiment file was not created.")

    TMP_PATH.replace(OUTPUT_PATH)

    mean = stats["sum"] / stats["count"] if stats["count"] else 0.0
    print("DONE")
    print(
        f"Rows: {stats['count']} Mean: {mean:.4f} "
        f"Min: {stats['min']:.4f} Max: {stats['max']:.4f}"
    )
    print(
        f"Pos: {stats['pos']} Neg: {stats['neg']} "
        f"Neu: {stats['neu']} Blank: {stats['blank']}"
    )
    print(f"Output: {OUTPUT_PATH}")


if __name__ == "__main__":
    add_sentiment()

