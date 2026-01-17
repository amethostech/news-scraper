"""
Text Normalizer

Step A: Normalize text for keyword matching while preserving original text.
This prepares article content for tag and entity detection.

Functions:
- Lowercase text
- Remove punctuation (optional for matching)
- Preserve original text for output
- Handle common article boilerplate
"""

import re
import pandas as pd
from typing import Dict, List, Tuple


class TextNormalizer:
    """Normalizes text for keyword matching while preserving originals."""

    def __init__(self):
        # Common boilerplate patterns to remove
        self.boilerplate_patterns = [
            # Subscription prompts
            r'to read the rest of this story subscribe to[^.]*\.',
            r'to read the full (article|story)[^.]*subscribe[^.]*\.',
            r'to read the full (article|story)[^.]*sign (up|in)[^.]*\.',
            r'subscribe to[^.]*stat\+[^.]*\.',
            r'subscribe to[^.]*stat[^.]*\.',
            r'subscribe to[^.]*premium[^.]*\.',

            # Newsletter/signup prompts
            r'sign up for[^.]*newsletter[^.]*\.',
            r'subscribe to[^.]*newsletter[^.]*\.',

            # Correction requests
            r'to submit a correction request[^.]*\.',
            r'to submit a correction[^.]*\.',

            # Generic prompts
            r'for more information[^.]*\.',
            r'read more at[^.]*\.',
        ]

    def normalize_article_text(self, article: pd.Series) -> Dict[str, str]:
        """
        Normalize text from an article for keyword matching.

        Args:
            article: Pandas Series representing one article row

        Returns:
            Dict with normalized text fields:
            - 'headline_normalized': normalized headline
            - 'body_normalized': normalized body text
            - 'consolidated_normalized': normalized consolidated text
            - 'combined_normalized': normalized combination of all text fields
        """
        # Extract text fields
        headline = str(article.get('Headline', '')).strip()
        body = str(article.get('Body/abstract/extract', '')).strip()
        consolidated = str(article.get('Consolidated_Text', '')).strip()

        # Normalize each field
        headline_norm = self._normalize_text(headline)
        body_norm = self._normalize_text(body)
        consolidated_norm = self._normalize_text(consolidated)

        # Create combined text for comprehensive matching
        combined_texts = []
        if headline_norm:
            combined_texts.append(headline_norm)
        if body_norm:
            combined_texts.append(body_norm)
        if consolidated_norm and consolidated_norm != body_norm:
            combined_texts.append(consolidated_norm)

        combined_norm = ' '.join(combined_texts)

        return {
            'headline_normalized': headline_norm,
            'body_normalized': body_norm,
            'consolidated_normalized': consolidated_norm,
            'combined_normalized': combined_norm
        }

    def _normalize_text(self, text: str) -> str:
        """
        Normalize a single text field.

        Args:
            text: Raw text

        Returns:
            Normalized text ready for keyword matching
        """
        if not text or pd.isna(text):
            return ''

        # Convert to string and strip
        text = str(text).strip()

        # Remove boilerplate text first
        text = self._remove_boilerplate(text)

        # Convert to lowercase
        text = text.lower()

        # Remove URLs
        text = re.sub(r'https?://\S+|www\.\S+', '', text)

        # Remove email addresses
        text = re.sub(r'\S+@\S+', '', text)

        # Remove punctuation (keep only word characters, spaces, and basic punctuation)
        # Keep apostrophes for contractions, hyphens for compound words
        text = re.sub(r'[^\w\s\'\-]', ' ', text)

        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)

        # Remove standalone single characters (often artifacts)
        text = re.sub(r'\b\w\b', '', text)

        # Final whitespace cleanup
        text = text.strip()

        return text

    def _remove_boilerplate(self, text: str) -> str:
        """
        Remove common boilerplate text patterns.

        Args:
            text: Raw text

        Returns:
            Text with boilerplate removed
        """
        if not text:
            return text

        original_text = text

        # Remove each boilerplate pattern
        for pattern in self.boilerplate_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)

        # Remove everything after common ending markers
        ending_markers = [
            r'\.\s*to read the rest.*$',
            r'\.\s*to read the full.*$',
            r'\.\s*subscribe.*$',
            r'\.\s*to submit a correction.*$',
            r'\.\s*contact us.*$',
            r'\.\s*for more information.*$',
        ]

        for marker in ending_markers:
            text = re.sub(marker, '.', text, flags=re.IGNORECASE)

        # Clean up multiple consecutive periods/spaces
        text = re.sub(r'\.{2,}', '.', text)
        text = re.sub(r'\s{3,}', ' ', text)

        return text.strip()

    def batch_normalize_articles(self, articles_df: pd.DataFrame,
                               text_columns: List[str] = None) -> pd.DataFrame:
        """
        Normalize text for a batch of articles.

        Args:
            articles_df: DataFrame with article data
            text_columns: List of text columns to normalize (default: standard columns)

        Returns:
            DataFrame with additional normalized text columns
        """
        if text_columns is None:
            text_columns = ['Headline', 'Body/abstract/extract', 'Consolidated_Text']

        normalized_df = articles_df.copy()

        # Add normalized columns
        normalized_df['headline_normalized'] = ''
        normalized_df['body_normalized'] = ''
        normalized_df['consolidated_normalized'] = ''
        normalized_df['combined_normalized'] = ''

        for idx, article in articles_df.iterrows():
            normalized = self.normalize_article_text(article)

            normalized_df.at[idx, 'headline_normalized'] = normalized['headline_normalized']
            normalized_df.at[idx, 'body_normalized'] = normalized['body_normalized']
            normalized_df.at[idx, 'consolidated_normalized'] = normalized['consolidated_normalized']
            normalized_df.at[idx, 'combined_normalized'] = normalized['combined_normalized']

        return normalized_df


def main():
    """Test the text normalizer with sample data."""
    normalizer = TextNormalizer()

    # Test with sample article
    sample_article = pd.Series({
        'Headline': 'Pfizer Acquires Biotech Firm for $2B Deal',
        'Body/abstract/extract': 'Pfizer announced today that it has acquired a small biotech company specializing in cancer therapies. The $2 billion deal includes multiple CAR-T cell therapies in development. To read the full story, subscribe to our premium service.',
        'Consolidated_Text': 'Pfizer acquisition biotech cancer therapy CAR-T $2B deal'
    })

    normalized = normalizer.normalize_article_text(sample_article)

    print("Sample Article Normalization:")
    print(f"Original Headline: {sample_article['Headline']}")
    print(f"Normalized Headline: {normalized['headline_normalized']}")
    print()
    print(f"Original Body: {sample_article['Body/abstract/extract'][:100]}...")
    print(f"Normalized Body: {normalized['body_normalized'][:100]}...")
    print()
    print(f"Combined Normalized: {normalized['combined_normalized'][:200]}...")


if __name__ == "__main__":
    main()
