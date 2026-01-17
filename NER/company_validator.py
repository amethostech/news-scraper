"""
Company Name Validation Layer

Validates extracted company names against known sources:
1. SEC EDGAR - US publicly traded companies
2. Stock exchanges - NYSE, NASDAQ, BSE, NSE
3. Local cache - Previously validated companies

This helps filter out false positives from NER extraction.

Usage:
    from company_validator import CompanyValidator
    
    validator = CompanyValidator()
    is_valid = validator.validate("Apple Inc")
    validated = validator.filter_companies(["Apple", "random string", "Microsoft"])
"""

import os
import csv
import json
import requests
from pathlib import Path
from typing import List, Dict, Set, Tuple, Optional
from datetime import datetime, timedelta
import re


class CompanyValidator:
    """Validates company names against SEC and stock exchange data"""
    
    # URLs for company data sources
    SOURCES = {
        "sec_cik": "https://www.sec.gov/files/company_tickers.json",
        "nasdaq": "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&exchange=nasdaq",
        "nyse": "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&exchange=nyse",
    }
    
    def __init__(self, cache_dir: Path = None, auto_update: bool = True):
        """
        Initialize the validator.
        
        Args:
            cache_dir: Directory to store cached company lists
            auto_update: Whether to auto-update cache if older than 7 days
        """
        if cache_dir is None:
            cache_dir = Path(__file__).parent / "data" / "cache"
        
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # In-memory cache
        self._companies: Set[str] = set()
        self._company_details: Dict[str, Dict] = {}
        self._ticker_to_name: Dict[str, str] = {}
        self._name_to_ticker: Dict[str, str] = {}
        
        # Load cached data
        self._load_cache()
        
        # Check if update needed
        if auto_update and self._is_cache_stale():
            print("Cache is stale, updating from sources...")
            self.update_from_sources()
    
    def _cache_file(self, name: str) -> Path:
        """Get path to a cache file"""
        return self.cache_dir / f"{name}.json"
    
    def _is_cache_stale(self, max_age_days: int = 7) -> bool:
        """Check if cache is older than max_age_days"""
        meta_file = self._cache_file("meta")
        if not meta_file.exists():
            return True
        
        try:
            with open(meta_file) as f:
                meta = json.load(f)
            last_update = datetime.fromisoformat(meta.get("last_update", "2000-01-01"))
            return datetime.now() - last_update > timedelta(days=max_age_days)
        except:
            return True
    
    def _save_cache(self):
        """Save current data to cache"""
        # Save company details
        with open(self._cache_file("companies"), "w") as f:
            json.dump(self._company_details, f, indent=2)
        
        # Save metadata
        with open(self._cache_file("meta"), "w") as f:
            json.dump({
                "last_update": datetime.now().isoformat(),
                "company_count": len(self._companies)
            }, f, indent=2)
        
        print(f"Cache saved: {len(self._companies)} companies")
    
    def _load_cache(self):
        """Load data from cache"""
        cache_file = self._cache_file("companies")
        if cache_file.exists():
            try:
                with open(cache_file) as f:
                    self._company_details = json.load(f)
                
                self._rebuild_indexes()
                print(f"Loaded {len(self._companies)} companies from cache")
            except Exception as e:
                print(f"Error loading cache: {e}")
    
    def _rebuild_indexes(self):
        """Rebuild in-memory indexes from company details"""
        self._companies = set()
        self._ticker_to_name = {}
        self._name_to_ticker = {}
        
        for key, details in self._company_details.items():
            name = details.get("name", "")
            ticker = details.get("ticker", "")
            
            # Add normalized name
            if name:
                self._companies.add(self._normalize(name))
                if ticker:
                    self._name_to_ticker[self._normalize(name)] = ticker
            
            # Add ticker
            if ticker:
                self._companies.add(ticker.lower())
                self._ticker_to_name[ticker.upper()] = name
    
    def _normalize(self, name: str) -> str:
        """Normalize company name for matching"""
        if not name:
            return ""
        
        # Lowercase
        name = name.lower().strip()
        
        # Remove common suffixes
        suffixes = [
            r'\s+(inc\.?|corp\.?|corporation|company|co\.?|ltd\.?|llc|plc|lp|llp)$',
            r'\s+(holdings?|group|international|intl\.?|enterprises?)$',
            r'\s+(pharmaceuticals?|therapeutics?|biosciences?|biotech)$',
            r',\s*inc\.?$',
        ]
        
        for suffix in suffixes:
            name = re.sub(suffix, '', name, flags=re.IGNORECASE)
        
        # Remove special characters
        name = re.sub(r'[^\w\s]', ' ', name)
        name = re.sub(r'\s+', ' ', name).strip()
        
        return name
    
    def update_from_sources(self):
        """Update company list from SEC and stock exchanges"""
        print("Fetching company data from sources...")
        
        # Fetch SEC CIK data
        self._fetch_sec_companies()
        
        # Fetch NASDAQ companies
        self._fetch_exchange_companies("nasdaq")
        
        # Fetch NYSE companies
        self._fetch_exchange_companies("nyse")
        
        # Rebuild indexes
        self._rebuild_indexes()
        
        # Save to cache
        self._save_cache()
    
    def _fetch_sec_companies(self):
        """Fetch companies from SEC EDGAR"""
        print("Fetching SEC EDGAR data...")
        
        try:
            headers = {
                "User-Agent": "NER-Pipeline contact@example.com",
                "Accept": "application/json"
            }
            response = requests.get(self.SOURCES["sec_cik"], headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            count = 0
            for key, company in data.items():
                cik = str(company.get("cik_str", ""))
                ticker = company.get("ticker", "")
                name = company.get("title", "")
                
                if name and ticker:
                    self._company_details[f"sec_{cik}"] = {
                        "name": name,
                        "ticker": ticker,
                        "cik": cik,
                        "source": "sec"
                    }
                    count += 1
            
            print(f"  Loaded {count} companies from SEC")
            
        except Exception as e:
            print(f"  Error fetching SEC data: {e}")
    
    def _fetch_exchange_companies(self, exchange: str):
        """Fetch companies from a stock exchange"""
        print(f"Fetching {exchange.upper()} data...")
        
        try:
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json"
            }
            response = requests.get(self.SOURCES[exchange], headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            rows = data.get("data", {}).get("table", {}).get("rows", [])
            
            # Alternative structure
            if not rows:
                rows = data.get("data", {}).get("rows", [])
            
            count = 0
            for row in rows:
                ticker = row.get("symbol", "")
                name = row.get("name", "")
                
                if name and ticker:
                    self._company_details[f"{exchange}_{ticker}"] = {
                        "name": name,
                        "ticker": ticker,
                        "source": exchange
                    }
                    count += 1
            
            print(f"  Loaded {count} companies from {exchange.upper()}")
            
        except Exception as e:
            print(f"  Error fetching {exchange} data: {e}")
    
    def add_custom_companies(self, companies: List[Dict]):
        """
        Add custom companies to the validator.
        
        Args:
            companies: List of dicts with 'name' and optionally 'ticker', 'source'
        """
        for company in companies:
            name = company.get("name", "")
            ticker = company.get("ticker", "")
            
            if name:
                key = f"custom_{self._normalize(name)}"
                self._company_details[key] = {
                    "name": name,
                    "ticker": ticker,
                    "source": company.get("source", "custom")
                }
        
        self._rebuild_indexes()
        print(f"Added {len(companies)} custom companies")
    
    def load_from_csv(self, csv_path: Path, name_column: str = "name", ticker_column: str = None):
        """
        Load companies from a CSV file.
        
        Args:
            csv_path: Path to CSV file
            name_column: Column containing company names
            ticker_column: Column containing ticker symbols (optional)
        """
        companies = []
        
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                company = {"name": row.get(name_column, "")}
                if ticker_column and ticker_column in row:
                    company["ticker"] = row[ticker_column]
                company["source"] = "csv"
                companies.append(company)
        
        self.add_custom_companies(companies)
    
    def validate(self, company_name: str, fuzzy: bool = False) -> bool:
        """
        Check if a company name is valid (exists in known sources).
        
        Args:
            company_name: Company name to validate
            fuzzy: Whether to use fuzzy matching (slower)
        
        Returns:
            True if company is found in known sources
        """
        if not company_name:
            return False
        
        normalized = self._normalize(company_name)
        
        # Exact match
        if normalized in self._companies:
            return True
        
        # Check if it's a ticker
        if company_name.upper() in self._ticker_to_name:
            return True
        
        # Fuzzy matching (substring)
        if fuzzy:
            for known in self._companies:
                if normalized in known or known in normalized:
                    return True
        
        return False
    
    def get_company_info(self, company_name: str) -> Optional[Dict]:
        """
        Get details about a company if it exists.
        
        Args:
            company_name: Company name or ticker
        
        Returns:
            Company details dict or None
        """
        normalized = self._normalize(company_name)
        
        # Search by name
        for key, details in self._company_details.items():
            if self._normalize(details.get("name", "")) == normalized:
                return details
        
        # Search by ticker
        ticker_upper = company_name.upper()
        if ticker_upper in self._ticker_to_name:
            for key, details in self._company_details.items():
                if details.get("ticker", "").upper() == ticker_upper:
                    return details
        
        return None
    
    def filter_companies(
        self, 
        companies: List[str], 
        return_all: bool = False
    ) -> Tuple[List[str], List[str]]:
        """
        Filter a list of company names, keeping only validated ones.
        
        Args:
            companies: List of company names to filter
            return_all: If True, also return unvalidated companies
        
        Returns:
            Tuple of (validated_companies, unvalidated_companies if return_all else [])
        """
        validated = []
        unvalidated = []
        
        for company in companies:
            if self.validate(company):
                validated.append(company)
            else:
                unvalidated.append(company)
        
        return (validated, unvalidated) if return_all else (validated, [])
    
    def enrich_company(self, company_name: str) -> Dict:
        """
        Enrich a company name with additional data.
        
        Args:
            company_name: Company name
        
        Returns:
            Dict with original name and enriched data
        """
        info = self.get_company_info(company_name)
        
        if info:
            return {
                "input": company_name,
                "validated": True,
                "canonical_name": info.get("name"),
                "ticker": info.get("ticker"),
                "source": info.get("source"),
                "cik": info.get("cik")
            }
        else:
            return {
                "input": company_name,
                "validated": False,
                "canonical_name": None,
                "ticker": None,
                "source": None,
                "cik": None
            }


def test_validator():
    """Test the company validator"""
    print("=" * 60)
    print("Testing Company Validator")
    print("=" * 60)
    
    # Initialize validator
    validator = CompanyValidator()
    
    # Test companies
    test_companies = [
        "Apple",
        "Microsoft Corporation",
        "Pfizer",
        "Some Random Company XYZ",
        "AAPL",  # Ticker
        "Johnson & Johnson",
        "Not A Real Company 12345",
        "Goldman Sachs",
        "Tesla",
    ]
    
    print("\nValidation Results:")
    print("-" * 60)
    
    for company in test_companies:
        is_valid = validator.validate(company)
        info = validator.get_company_info(company) if is_valid else None
        
        status = "✓ VALID" if is_valid else "✗ NOT FOUND"
        ticker = info.get("ticker", "") if info else ""
        
        print(f"{status:15} | {company:30} | {ticker}")
    
    # Test filtering
    print("\n" + "-" * 60)
    validated, unvalidated = validator.filter_companies(test_companies, return_all=True)
    print(f"\nValidated: {len(validated)}, Unvalidated: {len(unvalidated)}")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    test_validator()
