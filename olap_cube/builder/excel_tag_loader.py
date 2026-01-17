"""
Excel Tag Loader

Loads tag definitions from News Search Tags 101.xlsx and creates the foundation
for the Dim_Tag dimension table in the star schema.

The Excel file contains:
- Column 3: Main tag names (e.g., "acquisition", "merger", "partnership")
- Columns 4-9: Related keywords for each tag
- Column 2: "Individually" flag indicating whether to split categories

Output: DataFrame with Tag_Name, Tag_Category, Tag_Domain, and keywords
"""

import pandas as pd
import re
from pathlib import Path
from typing import Dict, List, Tuple


class ExcelTagLoader:
    """Loads and processes tag definitions from Excel file."""

    def __init__(self, excel_path: str = None):
        """
        Initialize the loader.

        Args:
            excel_path: Path to the Excel file. If None, uses default location.
        """
        if excel_path is None:
            excel_path = Path(__file__).resolve().parent.parent / "News Search Tags 101.xlsx"
        self.excel_path = Path(excel_path)

    def load_excel_data(self) -> pd.DataFrame:
        """
        Load the Excel file and return the raw data.

        Returns:
            DataFrame with all Excel data
        """
        try:
            df = pd.read_excel(self.excel_path, sheet_name="Sheet1")
            print(f"Loaded Excel file with {len(df)} rows and {len(df.columns)} columns")
            return df
        except Exception as e:
            raise FileNotFoundError(f"Could not load Excel file {self.excel_path}: {e}")

    def extract_tag_definitions(self) -> pd.DataFrame:
        """
        Extract tag definitions from the Excel file.

        This creates the foundation for Dim_Tag table with:
        - Tag_Name: The main tag (from column 3)
        - Tag_Category: Derived category (e.g., "Event", "Therapy", "Manufacturing")
        - Tag_Domain: Domain (e.g., "Business", "Healthcare", "Operations")
        - Keywords: List of related keywords from columns 4-9

        Returns:
            DataFrame with tag definitions ready for Dim_Tag table
        """
        df = self.load_excel_data()

        # Find rows with actual tag data (skip empty rows)
        # Look for rows where column 3 has tag names OR rows where columns 4-9 have keywords
        tag_rows = df[
            (df.iloc[:, 3].notna() & (df.iloc[:, 3].astype(str).str.strip() != '')) |
            (df.iloc[:, 4:].notna().any(axis=1))
        ]

        print(f"Found {len(tag_rows)} rows with tag data")

        tag_definitions = []

        # First, collect all general keywords (like row 2 with therapy keywords)
        general_keywords = []
        for idx, row in df.iterrows():
            if pd.isna(row.iloc[3]) or str(row.iloc[3]).strip() == '':  # No tag name in column 3
                # Check if there are keywords in columns 4-9
                row_keywords = []
                for col_idx in range(4, 10):  # Columns 4-9
                    if col_idx < len(row) and pd.notna(row.iloc[col_idx]):
                        keyword = str(row.iloc[col_idx]).strip()
                        if keyword and keyword.lower() != 'nan':
                            row_keywords.append(keyword)

                if row_keywords:
                    general_keywords.extend(row_keywords)
                    print(f"Found general keywords in row {idx}: {row_keywords}")

        # Now process tag rows (rows with tag names in column 3)
        for idx, row in tag_rows.iterrows():
            if pd.isna(row.iloc[3]) or str(row.iloc[3]).strip() == '':
                continue  # Skip rows without tag names

            tag_name = str(row.iloc[3]).strip()  # Column 3: main tag name

            # Start with keywords from the tag name itself
            keywords = [tag_name.lower()]

            # Extract any additional keywords from columns 4-9 for this specific tag
            for col_idx in range(4, 10):  # Columns 4-9
                if col_idx < len(row) and pd.notna(row.iloc[col_idx]):
                    keyword = str(row.iloc[col_idx]).strip()
                    if keyword and keyword.lower() != 'nan':
                        keywords.append(keyword.lower())

            # Add general keywords for therapy-related tags
            if any(therapy_term in tag_name.lower() for therapy_term in ['therapy', 'cancer', 'oncology', 'tumor', 'immunotherapy', 'car-t', 'adc']):
                keywords.extend([kw.lower() for kw in general_keywords])

            # Add variations and synonyms for common tags
            keywords.extend(self._generate_keyword_variations(tag_name))

            # Remove duplicates and clean
            keywords = list(set([kw.strip() for kw in keywords if kw.strip()]))

            # Determine if this should be processed individually
            is_individual = False
            if len(row) > 2 and pd.notna(row.iloc[2]):
                flag_value = str(row.iloc[2]).strip().lower()
                is_individual = flag_value == 'individually'

            # Derive tag category and domain based on the tag name
            tag_category, tag_domain = self._derive_tag_category_and_domain(tag_name, keywords)

            tag_definitions.append({
                'Tag_Name': tag_name,
                'Tag_Category': tag_category,
                'Tag_Domain': tag_domain,
                'Keywords': keywords,
                'Is_Individual': is_individual
            })

        # Convert to DataFrame
        result_df = pd.DataFrame(tag_definitions)

        # Handle "Individually" flag - split categories if needed
        result_df = self._handle_individual_categories(result_df)

        print(f"Created {len(result_df)} final tag definitions")
        return result_df

    def _derive_tag_category_and_domain(self, tag_name: str, keywords: List[str]) -> Tuple[str, str]:
        """
        Derive tag category and domain based on tag name and keywords.

        Args:
            tag_name: The main tag name
            keywords: List of related keywords

        Returns:
            Tuple of (tag_category, tag_domain)
        """
        tag_lower = tag_name.lower()
        all_text = (tag_name + ' ' + ' '.join(keywords)).lower()

        # Define category mappings based on Excel structure analysis
        category_mappings = {
            # Business/Financial Events
            'Event': [
                'acquisition', 'merger', 'partnership', 'collaboration', 'licensing',
                'buyout', 'takeover', 'biotech deal', 'pharma deal', 'm&a',
                'alliance', 'option agreement', 'co-development', 'in-license',
                'out-license', 'funding', 'financing', 'investment', 'raises',
                'series a', 'series b', 'series c', 'venture capital', 'ipo',
                'private placement', 'oversubscribed', 'seed funding',
                'crossover round', 'pipe', 'dilutive financing', 'non-dilutive funding',
                'led by', 'participated', 'syndicate', 'biotech funding'
            ],

            # Clinical/Regulatory
            'Clinical': [
                'clinical stage', 'phase 2', 'phase 3', 'fda approval'
            ],

            # Manufacturing/Operations
            'Manufacturing': [
                'in-house manufacturing', 'contract manufacturing', 'capacity shortage',
                'manufacturing', 'contract'
            ],

            # Science/Research
            'Therapy': [
                'oncology', 'cancer', 'tumor', 'immunotherapy', 'car-t', 'adc'
            ],

            # Other
            'Entity': [
                'preclinical', 'clinical-stage', 'platform company', 'therapeutic'
            ]
        }

        # Check for category matches
        for category, keywords_list in category_mappings.items():
            if any(keyword in all_text for keyword in keywords_list):
                if category == 'Therapy':
                    return category, 'Healthcare'
                elif category == 'Manufacturing':
                    return category, 'Operations'
                elif category == 'Event':
                    return category, 'Business'
                elif category == 'Clinical':
                    return category, 'Healthcare'
                else:
                    return category, 'Healthcare'  # Default domain

        # Default fallback
        return 'Other', 'General'

    def _generate_keyword_variations(self, tag_name: str) -> List[str]:
        """
        Generate keyword variations and synonyms for a tag.

        Args:
            tag_name: The tag name

        Returns:
            List of keyword variations
        """
        tag_lower = tag_name.lower()
        variations = []

        # Common variations for different tag types
        if tag_lower == 'acquisition':
            variations.extend(['acquire', 'acquired', 'acquires', 'buy', 'purchase', 'purchased'])
        elif tag_lower == 'merger':
            variations.extend(['merge', 'merged', 'merges', 'combine', 'combined'])
        elif tag_lower == 'partnership':
            variations.extend(['partner', 'partnered', 'partners', 'alliance', 'collaborate'])
        elif tag_lower == 'collaboration':
            variations.extend(['collaborate', 'collaborated', 'collaborates', 'cooperation'])
        elif tag_lower == 'licensing':
            variations.extend(['license', 'licensed', 'licenses', 'licence', 'licenced'])
        elif tag_lower == 'buyout':
            variations.extend(['buy out', 'bought out'])
        elif tag_lower == 'takeover':
            variations.extend(['take over', 'took over'])
        elif 'deal' in tag_lower:
            variations.extend(['agreement', 'transaction', 'contract'])
        elif tag_lower == 'm&a  (mergers & acquisitions)':
            variations.extend(['m&a', 'mergers and acquisitions', 'ma', 'mna'])
        elif tag_lower == 'alliance':
            variations.extend(['strategic alliance', 'partnership'])
        elif tag_lower == 'option agreement':
            variations.extend(['option', 'option deal'])
        elif tag_lower == 'co-development':
            variations.extend(['co development', 'joint development'])
        elif tag_lower == 'in-license':
            variations.extend(['in license', 'in-licensing'])
        elif tag_lower == 'out-license':
            variations.extend(['out license', 'out-licensing'])
        elif tag_lower == 'clinical stage':
            variations.extend(['clinical', 'clinical-stage'])
        elif tag_lower == 'phase 2':
            variations.extend(['phase ii', 'phase-2'])
        elif tag_lower == 'phase 3':
            variations.extend(['phase iii', 'phase-3'])
        elif tag_lower == 'fda approval':
            variations.extend(['fda', 'approved', 'approval'])
        elif tag_lower == 'funding':
            variations.extend(['fund', 'funded', 'funds', 'capital'])
        elif tag_lower == 'financing':
            variations.extend(['finance', 'financed'])
        elif tag_lower == 'investment':
            variations.extend(['invest', 'invested', 'investor'])
        elif tag_lower == 'raises':
            variations.extend(['raise', 'raised', 'raising'])
        elif 'series' in tag_lower:
            series_num = tag_lower.split()[-1]  # Extract series letter
            variations.extend([f'series {series_num}', f'series{series_num}', f'{series_num} round'])
        elif tag_lower == 'venture capital':
            variations.extend(['vc', 'venture', 'venture capitalist'])
        elif tag_lower == 'ipo':
            variations.extend(['initial public offering', 'public offering', 'go public'])
        elif tag_lower == 'private placement':
            variations.extend(['private', 'placement'])
        elif tag_lower == 'round':
            variations.extend(['funding round', 'investment round'])
        elif tag_lower == 'capital raise':
            variations.extend(['raise capital', 'capital raising'])
        elif tag_lower == 'oversubscribed':
            variations.extend(['over-subscribed', 'over subscribed'])
        elif tag_lower == 'seed funding':
            variations.extend(['seed', 'seed round'])
        elif tag_lower == 'crossover round':
            variations.extend(['crossover'])
        elif tag_lower == 'pipe':
            variations.extend(['private investment in public equity'])
        elif 'dilutive' in tag_lower or 'non-dilutive' in tag_lower:
            variations.extend(['dilutive financing', 'non-dilutive financing'])
        elif tag_lower == 'led by':
            variations.extend(['lead investor', 'leading'])
        elif tag_lower == 'participated':
            variations.extend(['participant', 'participating'])
        elif tag_lower == 'syndicate':
            variations.extend(['syndicated', 'syndication'])
        elif tag_lower == 'biotech funding':
            variations.extend(['biotech investment', 'biotech capital'])
        elif tag_lower == 'preclinical':
            variations.extend(['pre-clinical'])
        elif tag_lower == 'clinical-stage':
            variations.extend(['clinical stage'])
        elif tag_lower == 'platform company':
            variations.extend(['platform'])
        elif tag_lower == 'therapeutic':
            variations.extend(['therapy'])

        return variations

    def _handle_individual_categories(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle the "Individually" flag by splitting categories that need individual processing.

        Args:
            df: DataFrame with tag definitions

        Returns:
            DataFrame with individual categories split
        """
        result_rows = []

        for _, row in df.iterrows():
            if row['Is_Individual'] and len(row['Keywords']) > 1:
                # Split into individual tags for each keyword
                for keyword in row['Keywords']:
                    individual_row = row.copy()
                    individual_row['Tag_Name'] = keyword
                    individual_row['Keywords'] = [keyword]
                    individual_row['Is_Individual'] = False
                    result_rows.append(individual_row)
            else:
                result_rows.append(row)

        return pd.DataFrame(result_rows)

    def get_keywords_for_tag_matching(self) -> Dict[str, List[str]]:
        """
        Get a dictionary mapping tag names to their keywords for article matching.

        Returns:
            Dict[tag_name: list_of_keywords]
        """
        tag_definitions = self.extract_tag_definitions()
        keywords_dict = {}

        for _, row in tag_definitions.iterrows():
            tag_name = row['Tag_Name']
            keywords = row['Keywords']
            keywords_dict[tag_name] = keywords

        print(f"Prepared keyword dictionary with {len(keywords_dict)} tags")
        return keywords_dict


def main():
    """Main function for testing the loader."""
    loader = ExcelTagLoader()

    print("Loading tag definitions from Excel...")
    tag_definitions = loader.extract_tag_definitions()

    print("\nTag Definitions Preview:")
    print(tag_definitions[['Tag_Name', 'Tag_Category', 'Tag_Domain']].head(10))

    print("\nKeywords Dictionary Preview:")
    keywords_dict = loader.get_keywords_for_tag_matching()
    for i, (tag, keywords) in enumerate(list(keywords_dict.items())[:5]):
        print(f"{tag}: {keywords[:3]}...")  # Show first 3 keywords

    return tag_definitions, keywords_dict


if __name__ == "__main__":
    main()
