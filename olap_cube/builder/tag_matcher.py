"""
Tag Matcher

Step B: Keyword-driven tag detection using dual matching strategy:
1. Check existing matched_keywords column
2. Search normalized article text for tag keywords

Returns confidence scores for each matched tag.
"""

import re
import pandas as pd
from typing import Dict, List, Tuple, Set
from collections import defaultdict


class TagMatcher:
    """Matches articles to tags using keywords and existing matched_keywords column."""

    def __init__(self, tag_definitions: pd.DataFrame):
        """
        Initialize with tag definitions from Excel loader.

        Args:
            tag_definitions: DataFrame with Tag_Name, Keywords, Tag_Category, Tag_Domain
        """
        self.tag_definitions = tag_definitions.copy()

        # Pre-compile regex patterns for efficiency
        self.tag_patterns = {}
        # Build keyword-to-tag lookup for faster matching
        self.keyword_to_tags = {}
        
        for _, tag_def in self.tag_definitions.iterrows():
            tag_name = tag_def['Tag_Name']
            keywords = tag_def['Keywords']

            if keywords:
                # Create case-insensitive pattern for keyword matching
                # Use word boundaries to avoid partial matches
                pattern = r'\b(?:' + '|'.join(re.escape(kw) for kw in keywords) + r')\b'
                self.tag_patterns[tag_name] = re.compile(pattern, re.IGNORECASE)
                
                # Build lookup for matched_keywords column
                for kw in keywords:
                    kw_lower = kw.lower()
                    if kw_lower not in self.keyword_to_tags:
                        self.keyword_to_tags[kw_lower] = []
                    self.keyword_to_tags[kw_lower].append(tag_name)
                
                # Also add tag name itself as a keyword
                tag_name_lower = tag_name.lower()
                if tag_name_lower not in self.keyword_to_tags:
                    self.keyword_to_tags[tag_name_lower] = []
                self.keyword_to_tags[tag_name_lower].append(tag_name)

        print(f"Initialized TagMatcher with {len(self.tag_patterns)} tag patterns")

    def match_article_to_tags(self, article: pd.Series,
                            normalized_texts: Dict[str, str]) -> List[Tuple[str, float]]:
        """
        Match a single article to tags using dual strategy.

        Args:
            article: Article row as pandas Series
            normalized_texts: Dict with normalized text fields from text_normalizer

        Returns:
            List of (tag_name, confidence_score) tuples
        """
        matched_tags = {}

        # Strategy 1: Check matched_keywords column
        matched_from_column = self._check_matched_keywords_column(article)
        for tag_name in matched_from_column:
            matched_tags[tag_name] = 0.9  # High confidence for explicit matches

        # Strategy 2: Search normalized text
        matched_from_text = self._search_text_for_tags(normalized_texts)
        for tag_name, confidence in matched_from_text.items():
            if tag_name in matched_tags:
                # If already matched from column, take the higher confidence
                matched_tags[tag_name] = max(matched_tags[tag_name], confidence)
            else:
                matched_tags[tag_name] = confidence

        # Convert to sorted list by confidence (highest first)
        return sorted(matched_tags.items(), key=lambda x: x[1], reverse=True)

    def _check_matched_keywords_column(self, article: pd.Series) -> Set[str]:
        """
        Check the matched_keywords column for existing tag matches.
        Uses pre-built keyword lookup for O(1) matching instead of O(n).

        Args:
            article: Article row

        Returns:
            Set of matched tag names
        """
        matched_keywords = str(article.get('matched_keywords', '')).strip()

        if not matched_keywords or matched_keywords.lower() == 'nan':
            return set()

        # Split by common delimiters
        keywords_list = re.split(r'[;,|]', matched_keywords)

        matched_tags = set()

        for keyword in keywords_list:
            keyword = keyword.strip().lower()
            if not keyword:
                continue

            # Use fast lookup instead of iterating through all tags
            if keyword in self.keyword_to_tags:
                matched_tags.update(self.keyword_to_tags[keyword])

        return matched_tags

    def _search_text_for_tags(self, normalized_texts: Dict[str, str]) -> Dict[str, float]:
        """
        Search normalized text for tag keywords.

        Args:
            normalized_texts: Dict with normalized text fields

        Returns:
            Dict of tag_name -> confidence_score
        """
        matched_tags = defaultdict(float)

        # Use combined normalized text for comprehensive matching
        search_text = normalized_texts.get('combined_normalized', '')

        if not search_text:
            return matched_tags

        # Search for each tag pattern
        for tag_name, pattern in self.tag_patterns.items():
            matches = pattern.findall(search_text)

            if matches:
                # Calculate confidence based on number of matches and context
                confidence = self._calculate_match_confidence(tag_name, matches, search_text)
                matched_tags[tag_name] = confidence

        return dict(matched_tags)

    def _calculate_match_confidence(self, tag_name: str, matches: List[str],
                                  search_text: str) -> float:
        """
        Calculate confidence score for tag matches.

        Factors:
        - Number of unique matches
        - Whether match appears in headline (higher weight)
        - Tag category relevance

        Args:
            tag_name: Name of the matched tag
            matches: List of matched keyword strings
            search_text: Full search text

        Returns:
            Confidence score between 0.0 and 1.0
        """
        if not matches:
            return 0.0

        # Base confidence from number of matches (diminishing returns)
        unique_matches = len(set(matches))
        base_confidence = min(0.8, 0.4 + (unique_matches * 0.1))

        # Boost for headline matches (if we can detect them)
        # Assume first part of combined text might be headline
        text_parts = search_text.split()
        if len(text_parts) > 10:  # If we have substantial text
            headline_estimate = ' '.join(text_parts[:20])  # Rough headline estimate
            headline_matches = len(set(matches) & set(re.findall(r'\b\w+\b', headline_estimate)))
            if headline_matches > 0:
                base_confidence = min(1.0, base_confidence + 0.2)

        # Tag-specific adjustments
        tag_def = self.tag_definitions[self.tag_definitions['Tag_Name'] == tag_name]
        if not tag_def.empty:
            tag_category = tag_def.iloc[0]['Tag_Category']

            # Business events get slight boost if multiple matches
            if tag_category == 'Event' and unique_matches > 1:
                base_confidence = min(1.0, base_confidence + 0.1)

            # Therapy tags get boost for medical context
            if tag_category == 'Therapy':
                medical_terms = ['cancer', 'therapy', 'treatment', 'drug', 'clinical']
                medical_matches = sum(1 for term in medical_terms if term in search_text)
                if medical_matches > 0:
                    base_confidence = min(1.0, base_confidence + 0.1)

        return round(base_confidence, 2)

    def batch_match_articles(self, articles_df: pd.DataFrame,
                           normalized_df: pd.DataFrame) -> pd.DataFrame:
        """
        Match tags for a batch of articles.

        Args:
            articles_df: Original articles DataFrame
            normalized_df: DataFrame with normalized text columns

        Returns:
            DataFrame with additional tag matching columns
        """
        result_df = articles_df.copy()

        # Add tag matching columns
        result_df['matched_tags'] = [[] for _ in range(len(result_df))]
        result_df['tag_confidence_scores'] = [{} for _ in range(len(result_df))]

        for idx, article in articles_df.iterrows():
            # Get normalized texts for this article
            normalized_texts = {
                'headline_normalized': normalized_df.at[idx, 'headline_normalized'],
                'body_normalized': normalized_df.at[idx, 'body_normalized'],
                'consolidated_normalized': normalized_df.at[idx, 'consolidated_normalized'],
                'combined_normalized': normalized_df.at[idx, 'combined_normalized']
            }

            # Match tags
            tag_matches = self.match_article_to_tags(article, normalized_texts)

            # Store results
            matched_tag_names = [tag_name for tag_name, _ in tag_matches]
            confidence_dict = {tag_name: score for tag_name, score in tag_matches}

            result_df.at[idx, 'matched_tags'] = matched_tag_names
            result_df.at[idx, 'tag_confidence_scores'] = confidence_dict

        return result_df

    def get_tag_statistics(self, matched_articles_df: pd.DataFrame) -> Dict[str, int]:
        """
        Get statistics on tag matching results.

        Args:
            matched_articles_df: DataFrame with tag matching results

        Returns:
            Dict with tag frequencies
        """
        tag_counts = defaultdict(int)

        for tags_list in matched_articles_df['matched_tags']:
            for tag in tags_list:
                tag_counts[tag] += 1

        return dict(sorted(tag_counts.items(), key=lambda x: x[1], reverse=True))


def main():
    """Test the tag matcher with sample data."""
    # Mock tag definitions
    tag_data = {
        'Tag_Name': ['acquisition', 'partnership', 'cancer'],
        'Keywords': [
            ['acquisition', 'acquire', 'buy', 'purchase'],
            ['partnership', 'partner', 'alliance'],
            ['cancer', 'oncology', 'tumor']
        ],
        'Tag_Category': ['Event', 'Event', 'Therapy'],
        'Tag_Domain': ['Business', 'Business', 'Healthcare']
    }

    tag_definitions = pd.DataFrame(tag_data)
    matcher = TagMatcher(tag_definitions)

    # Test article
    test_article = pd.Series({
        'Headline': 'Pfizer Acquires Cancer Drug Company',
        'Body/abstract/extract': 'Pfizer announced acquisition of biotech firm specializing in cancer therapies.',
        'Consolidated_Text': 'Pfizer acquisition cancer therapy biotech',
        'matched_keywords': 'acquisition; cancer'
    })

    # Mock normalized texts
    normalized_texts = {
        'headline_normalized': 'pfizer acquires cancer drug company',
        'body_normalized': 'pfizer announced acquisition of biotech firm specializing in cancer therapies',
        'consolidated_normalized': 'pfizer acquisition cancer therapy biotech',
        'combined_normalized': 'pfizer acquires cancer drug company pfizer announced acquisition of biotech firm specializing in cancer therapies pfizer acquisition cancer therapy biotech'
    }

    matches = matcher.match_article_to_tags(test_article, normalized_texts)

    print("Tag Matching Test Results:")
    for tag_name, confidence in matches:
        print(f"  {tag_name}: {confidence}")

    print(f"\nTotal matches: {len(matches)}")


if __name__ == "__main__":
    main()
