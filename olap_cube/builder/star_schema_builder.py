"""
Star Schema Builder

Step C: Build all star schema tables for OLAP cube-ready data.
Creates Fact_Document, dimension tables, and bridge tables for many-to-many relationships.

Tables created:
- Fact_Document (central fact table)
- Dim_Time (time dimension)
- Dim_Source (source dimension)
- Dim_Tag (tag dimension)
- Dim_Entity (entity dimension)
- Bridge_Fact_Tag (document ↔ tags)
- Bridge_Fact_Entity (document ↔ entities)
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple
from pathlib import Path


class StarSchemaBuilder:
    """Builds star schema tables from processed article data."""

    def __init__(self):
        self.fact_id_counter = 1000  # Start at 1001
        self.tag_key_counter = 10     # Start at 10
        self.entity_key_counter = 200 # Start at 200

    def build_all_tables(self, articles_df: pd.DataFrame,
                        tag_definitions: pd.DataFrame,
                        prebuilt_dim_time: pd.DataFrame = None,
                        prebuilt_dim_source: pd.DataFrame = None,
                        prebuilt_dim_entity: pd.DataFrame = None) -> Dict[str, pd.DataFrame]:
        """
        Build all star schema tables from processed article data.

        Args:
            articles_df: DataFrame with articles and extracted data
            tag_definitions: DataFrame with tag definitions from Excel
            prebuilt_dim_time: Optional pre-built Dim_Time DataFrame
            prebuilt_dim_source: Optional pre-built Dim_Source DataFrame
            prebuilt_dim_entity: Optional pre-built Dim_Entity DataFrame (must have Entity_Key assigned)

        Returns:
            Dict mapping table names to DataFrames
        """
        print("Building star schema tables...")

        # Use pre-built dimension tables if provided, otherwise build them
        if prebuilt_dim_time is not None and not prebuilt_dim_time.empty:
            dim_time = prebuilt_dim_time
            print(f"Using pre-built Dim_Time with {len(dim_time)} records")
        else:
            dim_time = self.build_dim_time(articles_df)

        if prebuilt_dim_source is not None and not prebuilt_dim_source.empty:
            dim_source = prebuilt_dim_source
            print(f"Using pre-built Dim_Source with {len(dim_source)} records")
        else:
            dim_source = self.build_dim_source(articles_df)

        dim_tag = self.build_dim_tag(tag_definitions)

        if prebuilt_dim_entity is not None and not prebuilt_dim_entity.empty:
            dim_entity = prebuilt_dim_entity
            print(f"Using pre-built Dim_Entity with {len(dim_entity)} records")
        else:
            dim_entity = self.build_dim_entity(articles_df)

        # Build fact table
        fact_document = self.build_fact_document(articles_df, dim_time, dim_source)

        # Build bridge tables (use the same dim_entity that will be in final output)
        bridge_fact_tag = self.build_bridge_fact_tag(fact_document, articles_df, dim_tag)
        bridge_fact_entity = self.build_bridge_fact_entity(fact_document, articles_df, dim_entity)

        # Update fact table with tag/entity counts
        fact_document = self.update_fact_counts(fact_document, bridge_fact_tag, bridge_fact_entity)

        tables = {
            'Fact_Document': fact_document,
            'Dim_Time': dim_time,
            'Dim_Source': dim_source,
            'Dim_Tag': dim_tag,
            'Dim_Entity': dim_entity,
            'Bridge_Fact_Tag': bridge_fact_tag,
            'Bridge_Fact_Entity': bridge_fact_entity
        }

        print(f"Built {len(tables)} star schema tables")
        print(f"Total facts: {len(fact_document)}")
        print(f"Time periods: {len(dim_time)}")
        print(f"Sources: {len(dim_source)}")
        print(f"Tags: {len(dim_tag)}")
        print(f"Entities: {len(dim_entity)}")
        print(f"Tag relationships: {len(bridge_fact_tag)}")
        print(f"Entity relationships: {len(bridge_fact_entity)}")

        return tables

    def build_dim_time(self, articles_df: pd.DataFrame) -> pd.DataFrame:
        """
        Build Dim_Time dimension table.

        Args:
            articles_df: DataFrame with Date column

        Returns:
            Dim_Time DataFrame
        """
        print("Building Dim_Time...")

        # Extract unique dates
        dates = articles_df['Date'].dropna().unique()

        time_data = []

        for date_str in dates:
            try:
                # Parse date (handle various formats)
                date_obj = pd.to_datetime(date_str)

                # Create date key (YYYYMMDD format)
                date_key = int(date_obj.strftime('%Y%m%d'))

                time_data.append({
                    'Date_Key': date_key,
                    'Year': date_obj.year,
                    'Quarter': f"Q{date_obj.quarter}",
                    'Month': date_obj.month_name(),
                    'Month_Number': date_obj.month,
                    'Day': date_obj.day,
                    'Day_of_Week': date_obj.day_name(),
                    'Week_of_Year': date_obj.isocalendar()[1],
                    'Date_String': date_obj.strftime('%Y-%m-%d')  # ISO format for easy filtering
                })

            except (ValueError, TypeError):
                # For invalid dates, use a default
                time_data.append({
                    'Date_Key': 19000101,  # Default date
                    'Year': 1900,
                    'Quarter': 'Q1',
                    'Month': 'January',
                    'Month_Number': 1,
                    'Day': 1,
                    'Day_of_Week': 'Monday',
                    'Week_of_Year': 1,
                    'Date_String': '1900-01-01'
                })

        dim_time = pd.DataFrame(time_data)

        # Remove duplicates (in case of parsing issues)
        dim_time = dim_time.drop_duplicates(subset=['Date_Key'])

        print(f"Created Dim_Time with {len(dim_time)} unique time periods")

        return dim_time

    def build_dim_source(self, articles_df: pd.DataFrame) -> pd.DataFrame:
        """
        Build Dim_Source dimension table.

        Args:
            articles_df: DataFrame with Source column

        Returns:
            Dim_Source DataFrame
        """
        print("Building Dim_Source...")

        # Extract unique sources and filter out invalid ones
        sources = articles_df['Source'].dropna().unique()
        
        # Filter out invalid sources (numeric-only, empty, too long, etc.)
        valid_sources = []
        for source in sources:
            source_str = str(source).strip()
            # Skip empty or very short sources
            if not source_str or len(source_str) < 2:
                continue
            # Skip if it's purely numeric (likely a data corruption/misalignment issue)
            if source_str.isdigit():
                continue
            # Skip if it's too long (likely a corrupted value)
            if len(source_str) > 100:
                continue
            # Skip if it contains only special characters
            if not any(c.isalnum() for c in source_str):
                continue
            valid_sources.append(source_str)
        
        # Sort for consistent ordering
        valid_sources = sorted(set(valid_sources))

        source_data = []

        for i, source in enumerate(valid_sources, 1):
            # Classify source type (basic classification)
            source_type = self._classify_source_type(source)

            source_data.append({
                'Source_Key': i,
                'Source_Name': source,
                'Source_Type': source_type
            })

        dim_source = pd.DataFrame(source_data)

        print(f"Created Dim_Source with {len(dim_source)} valid sources (filtered out invalid entries)")

        return dim_source

    def _classify_source_type(self, source_name: str) -> str:
        """
        Classify source type based on name patterns.

        Args:
            source_name: Source name

        Returns:
            Source type classification
        """
        source_lower = source_name.lower()

        if any(term in source_lower for term in ['news', 'times', 'post', 'journal', 'report']):
            return 'News'
        elif any(term in source_lower for term in ['fda', 'ema', 'who', 'nih', 'gov']):
            return 'Government'
        elif any(term in source_lower for term in ['university', 'college', 'institute']):
            return 'Academic'
        elif any(term in source_lower for term in ['biotech', 'pharma', 'medical', 'health']):
            return 'Industry'
        else:
            return 'Other'

    def build_dim_tag(self, tag_definitions: pd.DataFrame) -> pd.DataFrame:
        """
        Build Dim_Tag dimension table from tag definitions.

        Args:
            tag_definitions: DataFrame with tag definitions from Excel loader

        Returns:
            Dim_Tag DataFrame
        """
        print("Building Dim_Tag...")

        dim_tag = tag_definitions.copy()

        # Add Tag_Key (auto-increment starting at 10)
        dim_tag['Tag_Key'] = range(self.tag_key_counter, self.tag_key_counter + len(dim_tag))

        # Rename columns to match star schema
        dim_tag = dim_tag.rename(columns={
            'Tag_Name': 'Tag_Name',
            'Tag_Category': 'Tag_Category',
            'Tag_Domain': 'Tag_Domain'
        })

        # Select only needed columns
        dim_tag = dim_tag[['Tag_Key', 'Tag_Name', 'Tag_Category', 'Tag_Domain']]

        print(f"Created Dim_Tag with {len(dim_tag)} tags")

        return dim_tag

    def build_dim_entity(self, articles_df: pd.DataFrame) -> pd.DataFrame:
        """
        Build Dim_Entity dimension table from extracted entities.

        Args:
            articles_df: DataFrame with extracted_entities column

        Returns:
            Dim_Entity DataFrame
        """
        print("Building Dim_Entity...")

        # Collect all unique entities from all articles
        all_entities = set()

        for entities_list in articles_df['extracted_entities'].dropna():
            for entity in entities_list:
                # Assume entity is a string, or extract name if it's a tuple
                if isinstance(entity, str):
                    entity_name = entity
                    entity_type = 'Company'  # Default
                elif isinstance(entity, tuple) and len(entity) >= 2:
                    entity_name = entity[0]
                    entity_type = entity[1]
                else:
                    continue

                all_entities.add((entity_name, entity_type))

        # Create DataFrame
        entity_data = []
        for entity_name, entity_type in sorted(all_entities):
            entity_data.append({
                'Entity_Key': self.entity_key_counter,
                'Entity_Name': entity_name,
                'Entity_Type': entity_type,
                'Entity_Domain': 'Healthcare'  # Default for pharma/biotech
            })
            self.entity_key_counter += 1

        dim_entity = pd.DataFrame(entity_data)

        print(f"Created Dim_Entity with {len(dim_entity)} entities")

        return dim_entity

    def build_fact_document(self, articles_df: pd.DataFrame,
                           dim_time: pd.DataFrame, dim_source: pd.DataFrame) -> pd.DataFrame:
        """
        Build Fact_Document fact table.

        Args:
            articles_df: DataFrame with articles
            dim_time: Dim_Time DataFrame for date key lookup
            dim_source: Dim_Source DataFrame for source key lookup

        Returns:
            Fact_Document DataFrame
        """
        print("Building Fact_Document...")

        fact_data = []

        # Create date key mapping (handle empty dim_time)
        if dim_time.empty or 'Date_Key' not in dim_time.columns:
            print("Warning: Dim_Time is empty, date keys will be generated from article dates")
            date_to_key = {}  # Will compute dates directly from articles
        else:
            date_to_key = dict(zip(dim_time['Date_Key'], dim_time['Date_Key']))
        
        # Create source key mapping (handle empty dim_source)
        if dim_source.empty or 'Source_Key' not in dim_source.columns:
            print("Warning: Dim_Source is empty, using default source key")
            source_to_key = {}  # Will use default
        else:
            source_to_key = dict(zip(dim_source['Source_Name'], dim_source['Source_Key']))

        for idx, article in articles_df.iterrows():
            fact_id = self.fact_id_counter + idx

            # Get document ID (Amethos Id)
            document_id = str(article.get('Amethos Id', f'doc_{idx}'))

            # Get date key
            date_str = str(article.get('Date', ''))
            try:
                date_obj = pd.to_datetime(date_str)
                date_key = int(date_obj.strftime('%Y%m%d'))
            except (ValueError, TypeError):
                date_key = 19000101  # Default date

            # Get source key
            source_name = str(article.get('Source', 'Unknown'))
            if source_to_key:
                source_key = source_to_key.get(source_name, 1)  # Default to first source
            else:
                source_key = 1  # Default when dim_source is empty

            # Extract additional article attributes
            headline = str(article.get('Headline', ''))
            body_text = str(article.get('Body/abstract/extract', ''))
            news_link = str(article.get('News link', ''))
            cleaned_text = str(article.get('Cleaned_Text_G', ''))
            consolidated_text = str(article.get('Consolidated_Text', ''))
            matched_keywords = str(article.get('matched_keywords', ''))
            sentiment_score = article.get('sentiment_score', None)
            qc_status = str(article.get('QC_H', ''))
            
            # Clean up NaN values
            def clean_value(val):
                if pd.isna(val) or str(val).strip().lower() in ['nan', 'none', '']:
                    return ''
                return str(val).strip()
            
            # Handle sentiment_score (might be string or float)
            try:
                if pd.notna(sentiment_score) and str(sentiment_score).strip().lower() not in ['nan', 'none', '']:
                    sentiment_score = float(sentiment_score)
                else:
                    sentiment_score = None
            except (ValueError, TypeError):
                sentiment_score = None

            # Get dimension information for denormalized columns (for easier analysis)
            # Look up time info
            time_info = dim_time[dim_time['Date_Key'] == date_key]
            if not time_info.empty:
                year = time_info.iloc[0]['Year']
                quarter = time_info.iloc[0]['Quarter']
                month = time_info.iloc[0]['Month']
                date_string = time_info.iloc[0].get('Date_String', '')
            else:
                # Fallback if date not in dim_time
                try:
                    date_obj = pd.to_datetime(date_str)
                    year = date_obj.year
                    quarter = f"Q{date_obj.quarter}"
                    month = date_obj.month_name()
                    date_string = date_obj.strftime('%Y-%m-%d')
                except:
                    year = None
                    quarter = None
                    month = None
                    date_string = ''
            
            # Look up source info
            source_info = dim_source[dim_source['Source_Key'] == source_key]
            if not source_info.empty:
                source_name = source_info.iloc[0]['Source_Name']
                source_type = source_info.iloc[0]['Source_Type']
            else:
                source_name = source_name  # Use original
                source_type = 'Unknown'

            fact_data.append({
                'Fact_ID': fact_id,
                'Document_ID': document_id,
                # Foreign keys
                'Date_Key': date_key,
                'Source_Key': source_key,
                # Denormalized dimension data (for easier analysis)
                'Year': year,
                'Quarter': quarter,
                'Month': month,
                'Date_String': date_string,
                'Source_Name': source_name,
                'Source_Type': source_type,
                # Article content
                'Headline': clean_value(headline),
                'Body_Text': clean_value(body_text),  # Full text preserved
                'News_Link': clean_value(news_link),
                'Cleaned_Text': clean_value(cleaned_text),
                'Consolidated_Text': clean_value(consolidated_text),
                'Matched_Keywords': clean_value(matched_keywords),
                'Sentiment_Score': sentiment_score,
                'QC_Status': clean_value(qc_status),
                # Measures
                'Document_Count': 1,  # Always 1 per document
                'Tag_Count': 0,       # Will be updated later
                'Has_Key_Event': 'No' # Will be updated later
            })

        fact_document = pd.DataFrame(fact_data)

        print(f"Created Fact_Document with {len(fact_document)} facts")

        return fact_document

    def build_bridge_fact_tag(self, fact_document: pd.DataFrame,
                             articles_df: pd.DataFrame, dim_tag: pd.DataFrame) -> pd.DataFrame:
        """
        Build Bridge_Fact_Tag bridge table.

        Args:
            fact_document: Fact_Document DataFrame
            articles_df: DataFrame with matched_tags and tag_confidence_scores
            dim_tag: Dim_Tag DataFrame for tag key lookup

        Returns:
            Bridge_Fact_Tag DataFrame
        """
        print("Building Bridge_Fact_Tag...")

        bridge_data = []

        # Create tag name to key mapping
        tag_to_key = dict(zip(dim_tag['Tag_Name'], dim_tag['Tag_Key']))

        for fact_idx, fact_row in fact_document.iterrows():
            fact_id = fact_row['Fact_ID']
            article_idx = fact_idx  # Assuming same order

            if article_idx >= len(articles_df):
                continue

            article = articles_df.iloc[article_idx]

            # Get matched tags and confidence scores
            matched_tags = article.get('matched_tags', [])
            confidence_scores = article.get('tag_confidence_scores', {})

            for tag_name in matched_tags:
                tag_key = tag_to_key.get(tag_name)
                if tag_key is not None:
                    confidence = confidence_scores.get(tag_name, 0.5)
                    bridge_data.append({
                        'Fact_ID': fact_id,
                        'Tag_Key': tag_key,
                        'Confidence_Score': confidence
                    })

        bridge_fact_tag = pd.DataFrame(bridge_data)

        print(f"Created Bridge_Fact_Tag with {len(bridge_fact_tag)} relationships")

        return bridge_fact_tag

    def build_bridge_fact_entity(self, fact_document: pd.DataFrame,
                                articles_df: pd.DataFrame, dim_entity: pd.DataFrame) -> pd.DataFrame:
        """
        Build Bridge_Fact_Entity bridge table.

        Args:
            fact_document: Fact_Document DataFrame
            articles_df: DataFrame with extracted_entities and entity_confidence_scores
            dim_entity: Dim_Entity DataFrame for entity key lookup (must have Entity_Name and Entity_Key)

        Returns:
            Bridge_Fact_Entity DataFrame
        """
        print("Building Bridge_Fact_Entity...")

        bridge_data = []

        # Create entity name to key mapping (exact match)
        entity_to_key = dict(zip(dim_entity['Entity_Name'], dim_entity['Entity_Key']))
        
        # Also create normalized name to key mapping for fuzzy matching
        # This helps match "Pfizer" with "Pfizer Inc" if both are in dim_entity
        normalized_to_key = {}
        # Also create a mapping of core names (first word) to entities for partial matching
        core_name_to_keys = {}  # "reata" -> [(key1, full_name1), (key2, full_name2)]
        
        for _, row in dim_entity.iterrows():
            entity_name = str(row['Entity_Name']).strip()
            entity_key = row['Entity_Key']
            # Normalize for matching
            normalized = entity_name.lower().strip()
            # Remove trailing punctuation (apostrophes, periods, etc.)
            normalized = normalized.rstrip("'\".,;: ")
            
            # Remove common suffixes for matching
            for suffix in [' inc', ' inc.', ' incorporated', ' corp', ' corp.', ' corporation',
                          ' ltd', ' ltd.', ' limited', ' llc', ' company', ' co', ' co.',
                          ' pharmaceuticals', ' pharma', ' biotech', ' biotechnology', 
                          ' therapeutics', ' biosciences']:
                if normalized.endswith(suffix):
                    normalized = normalized[:-len(suffix)].strip()
                    break
            
            normalized_to_key[normalized] = entity_key
            
            # Extract core name (first significant word) for partial matching
            words = normalized.split()
            if words:
                core_name = words[0]  # First word
                if core_name not in core_name_to_keys:
                    core_name_to_keys[core_name] = []
                core_name_to_keys[core_name].append((entity_key, entity_name))

        missing_entities = set()  # Track entities not found in dim_entity
        
        for fact_idx, fact_row in fact_document.iterrows():
            fact_id = fact_row['Fact_ID']
            article_idx = fact_idx  # Assuming same order

            if article_idx >= len(articles_df):
                continue

            article = articles_df.iloc[article_idx]

            # Get extracted entities
            extracted_entities = article.get('extracted_entities', [])
            entity_confidences = article.get('entity_confidence_scores', {})
            entity_mention_counts = article.get('entity_mention_counts', {})

            # Process unique entities (already deduplicated during extraction)
            # Each entity in extracted_entities represents one relationship
            for entity in extracted_entities:
                if isinstance(entity, str):
                    entity_name = entity.strip()
                elif isinstance(entity, tuple) and len(entity) >= 1:
                    entity_name = str(entity[0]).strip()
                else:
                    continue
                
                if not entity_name:
                    continue
                
                # Get mention count for this entity (default to 1 if not found)
                mention_count = entity_mention_counts.get(entity_name, 1)
                
                # Create bridge record for this entity relationship
                entity_key = None
                
                # Try exact match first
                entity_key = entity_to_key.get(entity_name)
                
                # If not found, try normalized match
                if entity_key is None:
                    normalized = entity_name.lower().strip()
                    # Remove trailing punctuation
                    normalized = normalized.rstrip("'\".,;: ")
                    # Remove common suffixes for matching
                    for suffix in [' inc', ' inc.', ' incorporated', ' corp', ' corp.', ' corporation',
                                  ' ltd', ' ltd.', ' limited', ' llc', ' company', ' co', ' co.',
                                  ' pharmaceuticals', ' pharma', ' biotech', ' biotechnology',
                                  ' therapeutics', ' biosciences']:
                        if normalized.endswith(suffix):
                            normalized = normalized[:-len(suffix)].strip()
                            break
                    entity_key = normalized_to_key.get(normalized)
                
                # If still not found, try partial/core name matching
                # This handles cases like "Reata" matching "Reata Pharmaceuticals"
                if entity_key is None:
                    normalized = entity_name.lower().strip().rstrip("'\".,;: ")
                    words = normalized.split()
                    if words:
                        core_name = words[0]  # First word
                        if core_name in core_name_to_keys:
                            # If multiple matches, prefer the shortest name (most specific)
                            candidates = core_name_to_keys[core_name]
                            if len(candidates) == 1:
                                entity_key = candidates[0][0]
                            else:
                                # Multiple matches - prefer shortest or exact core name match
                                # If entity_name is just the core name (one word), match to shortest full name
                                if len(words) == 1:
                                    # Single word entity - match to shortest full name
                                    entity_key = min(candidates, key=lambda x: len(x[1]))[0]
                                else:
                                    # Multi-word entity - try to find best match
                                    entity_key = candidates[0][0]  # Default to first match
                
                if entity_key is not None:
                    bridge_data.append({
                        'Fact_ID': fact_id,
                        'Entity_Key': entity_key,
                        'Mention_Count': mention_count
                    })
                else:
                    # Track missing entities for debugging
                    missing_entities.add(entity_name)

        if missing_entities:
            print(f"Warning: {len(missing_entities)} unique entity names not found in Dim_Entity (first 10: {list(missing_entities)[:10]})")

        bridge_fact_entity = pd.DataFrame(bridge_data)

        print(f"Created Bridge_Fact_Entity with {len(bridge_fact_entity)} relationships")

        return bridge_fact_entity

    def update_fact_counts(self, fact_document: pd.DataFrame,
                          bridge_fact_tag: pd.DataFrame,
                          bridge_fact_entity: pd.DataFrame) -> pd.DataFrame:
        """
        Update Fact_Document with tag counts and key event flags.

        Args:
            fact_document: Fact_Document DataFrame
            bridge_fact_tag: Bridge_Fact_Tag DataFrame
            bridge_fact_entity: Bridge_Fact_Entity DataFrame

        Returns:
            Updated Fact_Document DataFrame
        """
        print("Updating Fact_Document counts...")

        updated_fact = fact_document.copy()

        # Calculate tag counts per fact
        tag_counts = bridge_fact_tag.groupby('Fact_ID').size()
        updated_fact['Tag_Count'] = updated_fact['Fact_ID'].map(tag_counts).fillna(0).astype(int)

        # Set Has_Key_Event based on tag count
        updated_fact['Has_Key_Event'] = updated_fact['Tag_Count'].apply(
            lambda x: 'Yes' if x > 0 else 'No'
        )

        print("Updated tag counts and key event flags")

        return updated_fact

    def save_tables_to_csv(self, tables: Dict[str, pd.DataFrame], output_dir: str = 'data/star_schema'):
        """
        Save all star schema tables to CSV files.

        Args:
            tables: Dict of table names to DataFrames
            output_dir: Output directory path
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        for table_name, df in tables.items():
            file_path = output_path / f"{table_name}.csv"
            df.to_csv(file_path, index=False)
            print(f"Saved {table_name} to {file_path} ({len(df)} rows)")


def main():
    """Test the star schema builder with sample data."""
    builder = StarSchemaBuilder()

    # Mock data for testing
    sample_articles = pd.DataFrame({
        'Amethos Id': ['N010101', 'N020202'],
        'Date': ['2024-01-15', '2024-02-20'],
        'Source': ['BioSpace', 'FierceBiotech'],
        'Headline': ['Pfizer Deal', 'Merck Acquisition'],
        'matched_tags': [['acquisition'], ['merger']],
        'tag_confidence_scores': [{'acquisition': 0.9}, {'merger': 0.8}],
        'extracted_entities': [['Pfizer'], ['Merck']],
        'entity_confidence_scores': [{'Pfizer': 0.9}, {'Merck': 0.8}]
    })

    # Mock tag definitions
    tag_definitions = pd.DataFrame({
        'Tag_Name': ['acquisition', 'merger'],
        'Tag_Category': ['Event', 'Event'],
        'Tag_Domain': ['Business', 'Business']
    })

    tables = builder.build_all_tables(sample_articles, tag_definitions)

    print("\nSample Star Schema Tables:")
    for name, df in tables.items():
        print(f"{name}: {len(df)} rows")
        if len(df) > 0:
            print(df.head(2).to_string())
        print()


if __name__ == "__main__":
    main()
