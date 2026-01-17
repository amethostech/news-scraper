"""
Entity Extractor

Extracts company and organization names from articles to populate Dim_Entity table.
Uses multiple strategies:
1. Matched keywords column (primary source - most reliable)
2. Manual company dataset (if provided)
3. Text pattern matching (secondary, with strict filtering)
"""

import re
import pandas as pd
from typing import List, Set, Dict, Tuple, Optional
from collections import defaultdict, Counter
from pathlib import Path


class EntityExtractor:
    """Extracts company and organization entities from articles."""

    def __init__(self, company_list_path: Optional[str] = None):
        """
        Initialize entity extractor.
        
        Args:
            company_list_path: Optional path to CSV file with company list (default: config/company_names.csv)
                             Expected columns: Company_Name, Entity_Type (optional)
        """
        # Common company suffixes (case-insensitive) - for normalization
        self.company_suffixes = {
            'inc', 'incorporated', 'corp', 'corporation', 'ltd', 'limited',
            'llc', 'llp', 'co', 'company', 'group', 'holdings', 'labs', 'laboratories',
            'therapeutics', 'pharma', 'biotech', 'biosciences', 'biopharmaceuticals',
            'pharmaceuticals', 'biotechnology', 'technologies', 'solutions',
            'systems', 'international', 'global', 'ag', 'sa', 'nv', 'plc',
            'gmbh', 'kk', 'ltda', 'srl', 'spa', 'sas'
        }

        # Common non-entity keywords to filter out
        self.filter_words = {
            'alzheimer', 'oncology', 'neurology', 'immunology', 'hematology',
            'diabetes', 'cancer', 'therapeutic', 'drug', 'treatment', 'therapy',
            'patient', 'clinical', 'trial', 'approval', 'fda', 'ema', 'regulatory',
            'disease', 'disorder', 'syndrome', 'condition', 'biomarker'
        }

        # Known companies (loaded from CSV file)
        self.known_companies = set()
        self.manual_companies = {}
        
        # Default path to company names CSV
        default_path = Path(__file__).parent.parent / 'config' / 'company_names.csv'
        company_csv_path = company_list_path if company_list_path else str(default_path)
        
        if Path(company_csv_path).exists():
            self._load_company_names(company_csv_path)
        else:
            print(f"Warning: Company names file not found at {company_csv_path}, using empty set")
        
        print(f"Initialized EntityExtractor with {len(self.known_companies)} known companies")

    def _load_company_names(self, file_path: str):
        """
        Load company names from CSV file with deduplication of variants.
        Handles synonyms like "AstraZeneca" vs "Astra Zeneca" by keeping the canonical (first) name.
        
        For OLAP cubes:
        - Multiple variants map to one canonical entity
        - Prefers the first/longest name as canonical
        - All variants are recognized but map to the same normalized key
        """
        try:
            df = pd.read_csv(file_path)
            if 'Company_Name' not in df.columns:
                return
            
            # Track normalized -> (canonical_name, entity_type)
            # Use dict to handle duplicates: later entries with same normalized form will override
            normalized_to_canonical = {}
            
            for _, row in df.iterrows():
                company_name = str(row['Company_Name']).strip()
                if not company_name or company_name.lower() in ['nan', 'none', '']:
                    continue
                entity_type = str(row.get('Entity_Type', 'Company')).strip()
                
                # Normalize for deduplication (this handles "AstraZeneca" = "Astra Zeneca")
                normalized = self._normalize_entity_name(company_name)
                if not normalized:
                    continue
                
                # Store canonical name (prefer longer, more complete names)
                # If we already have this normalized form, keep the longer/more complete name
                if normalized in normalized_to_canonical:
                    existing_name, _ = normalized_to_canonical[normalized]
                    # Prefer longer name, or if same length, prefer the one with spaces (more readable)
                    if len(company_name) > len(existing_name) or \
                       (len(company_name) == len(existing_name) and ' ' in company_name and ' ' not in existing_name):
                        normalized_to_canonical[normalized] = (company_name, entity_type)
                else:
                    normalized_to_canonical[normalized] = (company_name, entity_type)
                
                # Add to known_companies set (for validation) - use normalized form
                self.known_companies.add(normalized)
                # Also add original lowercase version (for text matching)
                self.known_companies.add(company_name.lower().strip())
            
            # Store canonical names in manual_companies dict
            for normalized, (canonical_name, entity_type) in normalized_to_canonical.items():
                self.manual_companies[normalized] = (canonical_name, entity_type)
            
            print(f"Loaded {len(self.manual_companies)} unique companies (after deduplication) from {file_path}")
            if len(df) > len(self.manual_companies):
                print(f"  Note: {len(df) - len(self.manual_companies)} duplicate variants were merged")
        except Exception as e:
            print(f"Warning: Could not load company names from {file_path}: {e}")

    def _normalize_entity_name(self, name: str) -> str:
        """
        Normalize entity name for deduplication and matching.
        
        Handles synonyms/variations like:
        - "AstraZeneca" vs "Astra Zeneca" → both normalize to "astrazaneca"
        - "GlaxoSmithKline" vs "Glaxo SmithKline" → both normalize to "glaxosmithkline"
        - "Johnson & Johnson" vs "Johnson and Johnson" → both normalize to "johnsonjohnson"
        
        Strategy for OLAP cubes (ensures clean deduplication):
        1. Remove company suffixes (Inc, Corp, etc.)
        2. Remove all spaces, hyphens, punctuation
        3. Convert to lowercase
        4. Result: All variants map to same normalized key
        
        Args:
            name: Entity name to normalize
            
        Returns:
            Normalized name (lowercase, alphanumeric only, suffixes removed) for matching/deduplication
        """
        if not name or pd.isna(name):
            return ''
        
        name = str(name).strip()
        if not name:
            return ''
        
        # Convert to lowercase first
        name_lower = name.lower().strip()
        
        # Normalize common conjunctions (for OLAP cube consistency)
        # "&" and "and" should be treated the same
        name_lower = re.sub(r'\s*&\s*', ' and ', name_lower)
        name_lower = re.sub(r'\s+and\s+', 'and', name_lower)  # Remove spaces around "and"
        
        # Remove common company suffixes (for matching purposes)
        # This helps match "Pfizer" with "Pfizer Inc"
        for suffix in sorted(self.company_suffixes, key=len, reverse=True):
            # Match suffix at end of name (with optional space/punctuation)
            pattern = r'\s+' + re.escape(suffix) + r'[\s\.,;:]*$'
            name_lower = re.sub(pattern, '', name_lower)
        
        # Remove all spaces, hyphens, punctuation, and special characters
        # This collapses "Astra Zeneca" and "AstraZeneca" to the same form: "astrazaneca"
        # Also handles "Johnson & Johnson" and "Johnson and Johnson" → both become "johnsonandjohnson"
        name_lower = re.sub(r'[\s\-\.,;:+\'"]+', '', name_lower)
        
        # Remove any remaining leading/trailing non-alphanumeric
        name_lower = re.sub(r'^[^a-z0-9]+|[^a-z0-9]+$', '', name_lower)
        
        return name_lower

    def _extract_from_matched_keywords(self, article: pd.Series, 
                                       rejected_entities: Optional[List[str]] = None) -> Set[Tuple[str, str, float]]:
        """
        Extract entities from the matched_keywords column (primary source).
        
        Args:
            article: Article row
            rejected_entities: Optional list to track rejected entity candidates
            
        Returns:
            Set of (normalized_name, original_name, confidence) tuples
        """
        matched_keywords = str(article.get('matched_keywords', '')).strip()
        
        if not matched_keywords or matched_keywords.lower() in ['nan', 'none', '']:
            return set()
        
        entities = set()
        if rejected_entities is None:
            rejected_entities = []
        
        # Split by common delimiters
        keywords_list = re.split(r'[;,|]', matched_keywords)
        
        for keyword in keywords_list:
            keyword = keyword.strip()
            if not keyword or len(keyword) < 2:
                continue
            
            keyword_lower = keyword.lower()
            
            # Filter out common non-entity keywords
            if any(word in keyword_lower for word in self.filter_words):
                if rejected_entities is not None:
                    rejected_entities.append(keyword)
                continue
            
            # Check if it looks like a company name
            if self._is_likely_company_name(keyword):
                normalized = self._normalize_entity_name(keyword)
                if normalized and len(normalized) > 1:  # Must have at least 2 characters
                    # Use original keyword (cleaned up) as display name
                    display_name = keyword.strip()
                    entities.add((normalized, display_name, 0.9))  # High confidence from matched_keywords
                elif rejected_entities is not None:
                    # Rejected: normalization failed or too short
                    rejected_entities.append(keyword)
            else:
                # Rejected: doesn't look like a company name
                if rejected_entities is not None:
                    rejected_entities.append(keyword)
        
        return entities

    def _extract_known_companies_from_text(self, text: str) -> Set[Tuple[str, str, float]]:
        """
        Extract known companies from text (secondary source).
        Only matches against known company names for reliability.
        
        Args:
            text: Text to search
            
        Returns:
            Set of (normalized_name, display_name, confidence) tuples
        """
        entities = set()
        
        if not text:
            return entities
        
        text_lower = text.lower()
        
        # Check for known companies
        for known_company in self.known_companies:
            if known_company in text_lower:
                # Try to find the full company name in context
                # Look for company name with common suffixes
                patterns = [
                    rf'\b{re.escape(known_company)}\s+(?:inc|incorporated|corp|corporation|ltd|limited|llc|pharmaceuticals|pharma|biotech|biotechnology|therapeutics)\b',
                    rf'\b{re.escape(known_company)}\b'
                ]
                
                for pattern in patterns:
                    matches = re.finditer(pattern, text_lower)
                    for match in matches:
                        # Extract a bit of context to get the full name
                        start = max(0, match.start() - 20)
                        end = min(len(text), match.end() + 20)
                        context = text[start:end]
                        
                        # Find the full company name
                        full_name_match = re.search(rf'\b{re.escape(known_company)}[^\s,\.;:]*', context)
                        if full_name_match:
                            display_name = full_name_match.group(0).strip().title()
                            normalized = self._normalize_entity_name(display_name)
                            if normalized:
                                entities.add((normalized, display_name, 0.7))  # Medium confidence
                                break  # Found it, move to next company
        
        return entities

    def _is_likely_company_name(self, text: str) -> bool:
        """
        Check if text looks like a company name.
        
        Args:
            text: Potential company name
            
        Returns:
            True if likely a company name
        """
        if not text or len(text) < 2:
            return False
        
        text_lower = text.lower().strip()
        
        # Too short
        if len(text_lower) < 2:
            return False
        
        # Too long (probably a sentence fragment)
        if len(text_lower) > 50:
            return False
        
        # Contains filter words (medical terms, etc.)
        if any(word in text_lower for word in self.filter_words):
            return False
        
        # Known company (definitely valid)
        if any(company in text_lower for company in self.known_companies):
            return True
        
        # Has company suffix
        for suffix in self.company_suffixes:
            if text_lower.endswith(suffix) or f' {suffix}' in text_lower:
                return True
        
        # Single word that's capitalized (likely a ticker or abbreviation)
        words = text.split()
        if len(words) == 1 and text[0].isupper() and len(text) <= 5:
            return True
        
        # Two or more words (likely a company name)
        if len(words) >= 2 and len(words) <= 5:
            # Check if it looks like a proper noun (starts with capital)
            if words[0][0].isupper():
                return True
        
        return False

    def _classify_entity_type(self, entity_name: str) -> str:
        """
        Classify entity type based on name patterns.
        
        Args:
            entity_name: Entity name
            
        Returns:
            Entity type: 'Company', 'Organization', or 'Other'
        """
        name_lower = entity_name.lower()
        
        # Check for organization patterns
        org_patterns = ['fda', 'ema', 'who', 'nih', 'university', 'college', 'institute', 'hospital']
        if any(pattern in name_lower for pattern in org_patterns):
            return 'Organization'
        
        # Default to Company for pharma/biotech context
        return 'Company'

    def extract_entities_from_article(self, article: pd.Series,
                                     normalized_texts: Dict[str, str],
                                     rejected_entities: Optional[List[str]] = None) -> List[Tuple[str, str, str, int]]:
        """
        Extract entities from a single article.
        
        Args:
            article: Article row as pandas Series
            normalized_texts: Dict with normalized text fields (not used much now)
            rejected_entities: Optional list to track rejected entity candidates
            
        Returns:
            List of (entity_name, entity_type, confidence, mention_count) tuples
            where entity_name is the display name (not normalized)
        """
        entities_dict = {}  # normalized_name -> (display_name, entity_type, max_confidence, mention_count)
        
        # Get combined text for mention counting
        combined_text = normalized_texts.get('combined_normalized', '')
        headline = str(article.get('Headline', '')).lower()
        body = str(article.get('Body/abstract/extract', '')).lower()
        full_text = (headline + ' ' + body).lower()
        
        # Strategy 1: Extract from matched_keywords (primary source)
        keyword_entities = self._extract_from_matched_keywords(article, rejected_entities)
        for normalized, display_name, confidence in keyword_entities:
            # Count mentions of this entity in the article text
            mention_count = self._count_entity_mentions(display_name, full_text)
            
            if normalized not in entities_dict or confidence > entities_dict[normalized][2]:
                entity_type = self._classify_entity_type(display_name)
                entities_dict[normalized] = (display_name, entity_type, confidence, mention_count)
            else:
                # Update mention count if entity already exists (take max)
                old_display, old_type, old_conf, old_count = entities_dict[normalized]
                entities_dict[normalized] = (old_display, old_type, old_conf, max(old_count, mention_count))
        
        # Strategy 2: Extract known companies from text (secondary source)
        if combined_text:
            known_entities = self._extract_known_companies_from_text(combined_text)
            for normalized, display_name, confidence in known_entities:
                # Count mentions of this entity in the article text
                mention_count = self._count_entity_mentions(display_name, full_text)
                
                if normalized not in entities_dict or confidence > entities_dict[normalized][2]:
                    entity_type = self._classify_entity_type(display_name)
                    entities_dict[normalized] = (display_name, entity_type, confidence, mention_count)
                else:
                    # Update mention count if entity already exists (take max)
                    old_display, old_type, old_conf, old_count = entities_dict[normalized]
                    entities_dict[normalized] = (old_display, old_type, old_conf, max(old_count, mention_count))
        
        # Strategy 3: Check manual company list
        for normalized in entities_dict:
            if normalized in self.manual_companies:
                manual_name, manual_type = self.manual_companies[normalized]
                # Override with manual data, but keep mention count
                old_display, old_type, old_conf, old_count = entities_dict[normalized]
                entities_dict[normalized] = (manual_name, manual_type, 1.0, old_count)
        
        # Convert to list format: (display_name, entity_type, confidence_string, mention_count)
        result = []
        for normalized, (display_name, entity_type, confidence, mention_count) in entities_dict.items():
            result.append((display_name, entity_type, str(confidence), mention_count))
        
        return result
    
    def _count_entity_mentions(self, entity_name: str, text: str) -> int:
        """
        Count how many times an entity is mentioned in the text.
        Uses word boundary matching to avoid partial matches.
        
        Args:
            entity_name: Entity name to search for
            text: Text to search in (should be lowercase)
            
        Returns:
            Number of mentions
        """
        if not entity_name or not text:
            return 0
        
        entity_lower = entity_name.lower().strip()
        if not entity_lower:
            return 0
        
        # Escape special regex characters
        entity_escaped = re.escape(entity_lower)
        
        # Pattern: word boundary, entity name, word boundary
        # Also handle common suffixes (Inc, Corp, etc.) as optional
        pattern = rf'\b{entity_escaped}(?:\s+(?:inc|incorporated|corp|corporation|ltd|limited|llc|pharmaceuticals|pharma|biotech|biotechnology|therapeutics|biosciences))?\b'
        
        matches = re.findall(pattern, text)
        return len(matches)

    def batch_extract_entities(self, articles_df: pd.DataFrame,
                             normalized_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Extract entities for a batch of articles and build entity dimension.
        Includes deduplication across the batch.
        
        Args:
            articles_df: Original articles DataFrame
            normalized_df: DataFrame with normalized text columns
            
        Returns:
            Tuple of (articles_with_entities_df, dim_entity_df, rejected_entities_df)
            rejected_entities_df contains entities that were rejected during validation
        """
        result_df = articles_df.copy()
        
        # Add entity columns
        result_df['extracted_entities'] = [[] for _ in range(len(result_df))]
        result_df['entity_confidence_scores'] = [{} for _ in range(len(result_df))]
        result_df['entity_mention_counts'] = [{} for _ in range(len(result_df))]
        
        # Collect all unique entities with their best representation
        # normalized_name -> (display_name, entity_type, max_confidence)
        all_entities_dict = {}
        
        # Track rejected entities (candidate names that failed validation)
        rejected_entities_list = []
        
        for idx, article in articles_df.iterrows():
            # Get normalized texts for this article
            normalized_texts = {
                'headline_normalized': normalized_df.at[idx, 'headline_normalized'],
                'body_normalized': normalized_df.at[idx, 'body_normalized'],
                'consolidated_normalized': normalized_df.at[idx, 'consolidated_normalized'],
                'combined_normalized': normalized_df.at[idx, 'combined_normalized']
            }
            
            # Extract entities (pass rejected_entities_list to track rejected candidates)
            entities = self.extract_entities_from_article(article, normalized_texts, rejected_entities_list)
            
            # Store results (using display names)
            entity_names = [name for name, _, _, _ in entities]
            confidence_dict = {name: float(conf) for name, _, conf, _ in entities}
            mention_count_dict = {name: count for name, _, _, count in entities}
            
            result_df.at[idx, 'extracted_entities'] = entity_names
            result_df.at[idx, 'entity_confidence_scores'] = confidence_dict
            result_df.at[idx, 'entity_mention_counts'] = mention_count_dict
            
            # Collect for Dim_Entity (deduplicate by normalized name)
            for display_name, entity_type, confidence_str, mention_count in entities:
                normalized = self._normalize_entity_name(display_name)
                confidence = float(confidence_str)
                
                if normalized:
                    # Keep the best representation (highest confidence, or longest display name if tie)
                    if normalized not in all_entities_dict:
                        all_entities_dict[normalized] = (display_name, entity_type, confidence)
                    else:
                        old_display, old_type, old_conf = all_entities_dict[normalized]
                        # Update if higher confidence, or same confidence but better display name
                        if confidence > old_conf or (confidence == old_conf and len(display_name) > len(old_display)):
                            all_entities_dict[normalized] = (display_name, entity_type, confidence)
        
        # Build Dim_Entity DataFrame (will be assigned keys later)
        dim_entity_data = []
        for normalized, (display_name, entity_type, _) in sorted(all_entities_dict.items()):
            dim_entity_data.append({
                'Entity_Name': display_name,  # Use display name for output
                'Entity_Type': entity_type,
                'Entity_Domain': 'Healthcare'  # Default for pharma/biotech context
            })
        
        dim_entity_df = pd.DataFrame(dim_entity_data)
        
        # Build rejected entities DataFrame (deduplicate and count occurrences)
        rejected_counter = Counter(rejected_entities_list)
        rejected_entity_data = []
        for entity_name, count in rejected_counter.most_common():
            rejected_entity_data.append({
                'Rejected_Entity': entity_name.strip(),
                'Occurrence_Count': count,
                'Reason': 'Failed validation (not recognized as company name)'
            })
        
        rejected_entities_df = pd.DataFrame(rejected_entity_data)
        
        return result_df, dim_entity_df, rejected_entities_df

    def get_entity_statistics(self, articles_with_entities_df: pd.DataFrame) -> Dict[str, int]:
        """
        Get statistics on entity extraction results.
        
        Args:
            articles_with_entities_df: DataFrame with entity extraction results
            
        Returns:
            Dict with entity frequencies
        """
        from collections import Counter
        entity_counts = Counter()
        
        for entities_list in articles_with_entities_df['extracted_entities']:
            for entity in entities_list:
                entity_counts[entity] += 1
        
        return dict(entity_counts.most_common())


def main():
    """Test the entity extractor with sample data."""
    extractor = EntityExtractor()
    
    # Test article
    test_article = pd.Series({
        'Headline': 'Pfizer Acquires Biotech Firm',
        'Body/abstract/extract': 'Pfizer Inc. announced today that it has acquired a small biotech company.',
        'Consolidated_Text': 'Pfizer Inc acquisition biotech',
        'matched_keywords': 'Pfizer; Eli Lilly; Oncology'
    })
    
    # Mock normalized texts
    normalized_texts = {
        'headline_normalized': 'pfizer acquires biotech firm',
        'body_normalized': 'pfizer inc announced today that it has acquired a small biotech company',
        'consolidated_normalized': 'pfizer inc acquisition biotech',
        'combined_normalized': 'pfizer acquires biotech firm pfizer inc announced today that it has acquired a small biotech company pfizer inc acquisition biotech'
    }
    
    entities = extractor.extract_entities_from_article(test_article, normalized_texts)
    
    print("Entity Extraction Test Results:")
    for entity_name, entity_type, confidence, mention_count in entities:
        print(f"  {entity_name} ({entity_type}): {confidence} (mentioned {mention_count} times)")
    
    print(f"\nTotal entities: {len(entities)}")


if __name__ == "__main__":
    main()
