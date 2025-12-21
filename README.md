# News Scraper - Data Pipeline & Architecture

This data pipeline autonomously scrapes, cleans, normalizes, and analyzes news articles from various pharmaceutical and biotech sources.

## 1. The Saving Pipeline

### Scraper Execution
Each scraper (located in `scrapers/`) extracts article details: **Title, Author, Date, Link, and Body Text**. The raw content is immediately processed (cleaned) before saving.

### Storage Layers
Data is written to the `data/` directory in two formats simultaneously:

#### A. Individual Source Files
*   **Location**: `data/{source_name}_articles.csv` (e.g., `data/biospace_articles.csv`)
*   **Format**: `NewsSite, Name, Date of publishing the Article, Author, Weblink, Summary`
*   **Purpose**: Keeps a clean, isolated record for each news source.

#### B. Merged Master File
*   **Location**: `data/merged_articles.csv`
*   **Format**: `Amethos Id, Date, Source, News link, Headline, Body/abstract/extract`
*   **Logic**: Articles are appended here automatically. Duplicates are rejected based on the Link. `Amethos Id` is left blank at this stage.

#### C. MongoDB (Secondary)
*   **Condition**: Active only if `MONGO_URI` is set.
*   **Module**: `utils/mongoWriter.js` saves the article object to the database.

---

## 2. Automated Scheduling & Workflow

The system is designed to run continuously or on valid cron intervals.

### Scheduler (`scheduler.js`)
*   **Role**: Orchestrates the entire scraping process.
*   **Function**:
    1.  Iterates through all scrapers defined in `scrapers/index.js`.
    2.  Runs them in parallel batches (controlled concurrency) to respect rate limits.
    3.  Monitors execution time and restarts scrapers if they hang.

### Post-Processing Steps
After scraping is complete (or at defined intervals), the following processing scripts are triggered:

#### A. Keyword Normalization (`normalize_keywords.py`)
*   **Input**: `data/merged_articles.csv`
*   **Configuration**: uses `config/normalization_rules.csv` to map variations (e.g., "Pfizer Inc", "Pfizer Ltd") to a single standard key ("Pfizer").
*   **Output**: Adds/Updates a `matched_keywords` column in the data.

#### B. Sentiment Analysis (VADER)
*   **Script**: `add_sentiment.py` (or integrated Python module).
*   **Logic**: Uses the **VADER** (Valence Aware Dictionary and sEntiment Reasoner) Lexicon to analyze the headline and body text.
*   **Result**: assigns a compound sentiment score (Positive/Negative/Neutral) to each article.

---

## 3. Configuration

*   **`config/normalization_rules.csv`**: The central brain for entity recognition. It maps raw text patterns to normalized entities (Companies, Drugs, Conditions).
*   **`.env`**: Stores sensitive credentials (MongoDB URI).
*   **`.gitignore`**: Ensures `data/` files and secrets are never pushed to the repository.
