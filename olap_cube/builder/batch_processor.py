"""
Batch Processor

Orchestrates the complete star schema transformation pipeline:
1. Load Excel tags
2. Process CSV in chunks
3. Apply all transformation steps (normalize, match tags, extract entities, build schema)
4. Accumulate results incrementally
5. Output final star schema CSV files

Handles large datasets by processing in configurable batch sizes.
"""

import pandas as pd
import time
import csv
import sys
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

from excel_tag_loader import ExcelTagLoader
from text_normalizer import TextNormalizer
from tag_matcher import TagMatcher
from entity_extractor import EntityExtractor
from star_schema_builder import StarSchemaBuilder

# Increase CSV field size limit to handle large text fields
# Default is 131072 (128KB), increase to 10MB
csv.field_size_limit(min(2147483647, sys.maxsize))


class BatchProcessor:
    """Processes large CSV files in batches to build star schema."""

    def __init__(self, batch_size: int = 5000, output_dir: str = 'data/star_schema'):
        """
        Initialize batch processor.

        Args:
            batch_size: Number of rows to process per batch
            output_dir: Directory to save output CSV files
        """
        self.batch_size = batch_size
        self.output_dir = Path(output_dir)

        # Initialize components
        self.excel_loader = None
        self.text_normalizer = TextNormalizer()
        self.tag_matcher = None
        self.entity_extractor = EntityExtractor()
        self.schema_builder = StarSchemaBuilder()

        # Accumulators for incremental building
        self.all_tag_definitions = None
        self.dim_time_accumulator = set()
        self.dim_source_accumulator = set()
        self.dim_entity_accumulator = set()
        self.processed_batches = []  # Store processed DataFrames
        self.rejected_entities_accumulator = []  # Store rejected entities across batches

        print(f"Initialized BatchProcessor with batch_size={batch_size}")

    def process_csv_to_star_schema(self, csv_path: str, excel_path: str = None) -> Dict[str, pd.DataFrame]:
        """
        Process entire CSV file to star schema.

        Args:
            csv_path: Path to input CSV file
            excel_path: Path to Excel tags file (optional, uses default if None)

        Returns:
            Dict of all star schema tables
        """
        start_time = time.time()

        # Step 1: Load Excel tag definitions
        print("Step 1: Loading tag definitions from Excel...")
        self.excel_loader = ExcelTagLoader(excel_path)
        self.all_tag_definitions = self.excel_loader.extract_tag_definitions()
        self.tag_matcher = TagMatcher(self.all_tag_definitions)

        # Step 2: Process CSV in batches
        print(f"Step 2: Processing CSV in batches of {self.batch_size} rows...")

        csv_file = Path(csv_path)
        if not csv_file.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        total_rows = 0
        batch_count = 0
        
        # Estimate total rows for progress tracking
        try:
            with open(csv_file, 'r') as f:
                estimated_total = sum(1 for _ in f) - 1
        except:
            estimated_total = None

        # Read CSV in chunks with error handling
        # Use on_bad_lines='skip' to skip malformed rows and continue processing
        # Increase field size limit to handle large text fields in articles
        csv.field_size_limit(min(2147483647, sys.maxsize))
        
        # Use csv.reader to read the entire file properly
        # This handles embedded newlines and other CSV complexities that pandas might miss
        print("Reading CSV using csv.reader to handle all rows properly...")
        
        # First, get the header
        header_df = pd.read_csv(csv_file, nrows=0)
        header = list(header_df.columns)
        print(f"Detected {len(header)} columns")
        
        current_chunk_rows = []
        
        with open(csv_file, 'r', encoding='utf-8', errors='ignore', newline='') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header row
            
            for row in reader:
                # Only process rows with correct column count
                if len(row) == len(header):
                    current_chunk_rows.append(row)
                # Skip rows with wrong column count (malformed)
                
                # When chunk is full, process it
                if len(current_chunk_rows) >= self.batch_size:
                    batch_count += 1
                    chunk_df = pd.DataFrame(current_chunk_rows, columns=header)
                    chunk_size = len(chunk_df)
                    total_rows += chunk_size
                    current_chunk_rows = []  # Reset for next chunk

                    batch_start = time.time()
                    print(f"Processing batch {batch_count} ({chunk_size} rows, total: {total_rows:,})...")

                    # Process this batch
                    self._process_batch(chunk_df)
                    
                    batch_time = time.time() - batch_start
                    elapsed_total = time.time() - start_time
                    rate = total_rows / elapsed_total if elapsed_total > 0 else 0
                    
                    if batch_count % 5 == 0 or batch_count == 1:
                        if estimated_total:
                            progress_pct = (total_rows / estimated_total) * 100
                            remaining_rows = estimated_total - total_rows
                            eta_seconds = remaining_rows / rate if rate > 0 else 0
                            print(f"  Progress: {progress_pct:.1f}% ({total_rows:,}/{estimated_total:,} rows) | "
                                  f"Rate: {rate:.0f} rows/s | "
                                  f"ETA: {eta_seconds/60:.1f} min")
                        else:
                            print(f"  Batch {batch_count} completed in {batch_time:.1f}s | "
                                  f"Rate: {rate:.0f} rows/s")
            
            # Process remaining rows in final chunk
            if current_chunk_rows:
                batch_count += 1
                chunk_df = pd.DataFrame(current_chunk_rows, columns=header)
                chunk_size = len(chunk_df)
                total_rows += chunk_size

                batch_start = time.time()
                print(f"Processing final batch {batch_count} ({chunk_size} rows, total: {total_rows:,})...")
                self._process_batch(chunk_df)
                
                batch_time = time.time() - batch_start
                print(f"  Final batch completed in {batch_time:.1f}s")


        # Step 3: Combine all processed batches and build final star schema
        print("\nStep 3: Combining processed batches and building final star schema...")

        if not self.processed_batches:
            raise ValueError("No processed batches found")

        # Combine all processed batches
        combined_df = pd.concat(self.processed_batches, ignore_index=True)
        print(f"Combined {len(self.processed_batches)} batches into {len(combined_df)} total articles")

        # Build dimension tables from accumulators FIRST (before building fact/bridge tables)
        dim_time_df = self._build_dim_time_from_accumulator()
        dim_source_df = self._build_dim_source_from_accumulator()
        dim_entity_df = self._build_dim_entity_from_accumulator()

        # Build all star schema tables, but pass pre-built dimension tables
        # This ensures consistency between dimension and bridge tables
        all_tables = self.schema_builder.build_all_tables(
            combined_df, 
            self.all_tag_definitions,
            prebuilt_dim_time=dim_time_df if not dim_time_df.empty else None,
            prebuilt_dim_source=dim_source_df if not dim_source_df.empty else None,
            prebuilt_dim_entity=dim_entity_df if not dim_entity_df.empty else None
        )

        print(f"Built star schema with {len(all_tables)} tables")

        # Step 4: Save to CSV files
        print("Step 4: Saving star schema to CSV files...")
        self.schema_builder.save_tables_to_csv(all_tables, self.output_dir)
        
        # Step 5: Save rejected entities for review
        print("Step 5: Saving rejected entities for review...")
        self._save_rejected_entities()

        total_time = time.time() - start_time
        print(f"Total processing time: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")
        print(f"Processed {total_rows:,} articles in {batch_count} batches")

        return all_tables

    def _process_batch(self, chunk: pd.DataFrame):
        """
        Process a single batch of articles.

        Args:
            chunk: DataFrame chunk to process
        """
        # Step A: Normalize text
        normalized_df = self.text_normalizer.batch_normalize_articles(chunk)

        # Step B: Match tags
        tagged_df = self.tag_matcher.batch_match_articles(chunk, normalized_df)

        # Step C: Extract entities (returns 3 DFs: entity_df, dim_entity, rejected_entities)
        entity_df, batch_dim_entity, batch_rejected_entities = self.entity_extractor.batch_extract_entities(chunk, normalized_df)
        tagged_df = pd.concat([tagged_df, entity_df[['extracted_entities', 'entity_confidence_scores', 'entity_mention_counts']]], axis=1)

        # Accumulate dimension data
        self._accumulate_dimensions(chunk, batch_dim_entity)
        
        # Accumulate rejected entities
        if not batch_rejected_entities.empty:
            self.rejected_entities_accumulator.append(batch_rejected_entities)

        # Store processed batch for final schema building
        self._accumulate_processed_batch(tagged_df)

    def _accumulate_processed_batch(self, processed_df: pd.DataFrame):
        """
        Accumulate processed batch DataFrames.

        Args:
            processed_df: Processed DataFrame from a batch
        """
        self.processed_batches.append(processed_df.copy())

    def _accumulate_dimensions(self, chunk: pd.DataFrame, batch_dim_entity: pd.DataFrame):
        """
        Accumulate dimension data across batches.

        Args:
            chunk: Current chunk DataFrame
            batch_dim_entity: Entity dimension from current batch
        """
        # Accumulate time dimension data
        for date_val in chunk['Date'].dropna():
            try:
                date_obj = pd.to_datetime(str(date_val))
                date_key = int(date_obj.strftime('%Y%m%d'))
                self.dim_time_accumulator.add((
                    date_key, 
                    date_obj.year,
                    f"Q{date_obj.quarter}",
                    date_obj.month_name(), 
                    date_obj.day
                ))
            except (ValueError, TypeError):
                pass

        # Accumulate source dimension data (filter invalid sources)
        for source_val in chunk['Source'].dropna():
            source_str = str(source_val).strip()
            # Skip invalid sources (numeric-only, empty, too long, etc.)
            if not source_str or len(source_str) < 2:
                continue
            if source_str.isdigit():  # Skip purely numeric values (data corruption/misalignment)
                continue
            if len(source_str) > 100:  # Skip too long values
                continue
            if not any(c.isalnum() for c in source_str):  # Skip if no alphanumeric chars
                continue
            source_type = self._classify_source_type(source_str)
            self.dim_source_accumulator.add((source_str, source_type))

        # Accumulate entity dimension data
        # Use normalized name for deduplication
        for _, entity_row in batch_dim_entity.iterrows():
            entity_name = entity_row['Entity_Name']
            entity_type = entity_row['Entity_Type']
            entity_domain = entity_row.get('Entity_Domain', 'Healthcare')
            entity_tuple = (entity_name, entity_type, entity_domain)
            self.dim_entity_accumulator.add(entity_tuple)

    def _classify_source_type(self, source_name: str) -> str:
        """
        Classify source type (duplicate from star_schema_builder for efficiency).
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

    def _build_dim_time_from_accumulator(self) -> pd.DataFrame:
        """Build Dim_Time DataFrame from accumulated data."""
        time_data = []
        for date_key, year, quarter, month, day in sorted(self.dim_time_accumulator):
            # Reconstruct date object for additional fields
            try:
                date_str = str(date_key)
                if len(date_str) == 8:  # YYYYMMDD format
                    date_obj = pd.to_datetime(f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}")
                    time_data.append({
                        'Date_Key': date_key,
                        'Year': year,
                        'Quarter': quarter,
                        'Month': month,
                        'Month_Number': date_obj.month,
                        'Day': day,
                        'Day_of_Week': date_obj.day_name(),
                        'Week_of_Year': date_obj.isocalendar()[1],
                        'Date_String': date_obj.strftime('%Y-%m-%d')
                    })
                else:
                    # Fallback for invalid format
                    time_data.append({
                        'Date_Key': date_key,
                        'Year': year,
                        'Quarter': quarter,
                        'Month': month,
                        'Month_Number': 1,
                        'Day': day,
                        'Day_of_Week': 'Monday',
                        'Week_of_Year': 1,
                        'Date_String': f"{year}-01-01"
                    })
            except:
                # Fallback
                time_data.append({
                    'Date_Key': date_key,
                    'Year': year,
                    'Quarter': quarter,
                    'Month': month,
                    'Month_Number': 1,
                    'Day': day,
                    'Day_of_Week': 'Monday',
                    'Week_of_Year': 1,
                    'Date_String': f"{year}-01-01"
                })
        return pd.DataFrame(time_data)

    def _build_dim_source_from_accumulator(self) -> pd.DataFrame:
        """Build Dim_Source DataFrame from accumulated data."""
        source_data = []
        # Filter out invalid sources (numeric-only, empty, too long, etc.)
        valid_sources = []
        for source_name, source_type in self.dim_source_accumulator:
            source_str = str(source_name).strip()
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
            valid_sources.append((source_str, source_type))
        
        # Sort for consistent ordering
        valid_sources = sorted(set(valid_sources))
        
        for i, (source_name, source_type) in enumerate(valid_sources, 1):
            source_data.append({
                'Source_Key': i,
                'Source_Name': source_name,
                'Source_Type': source_type
            })
        return pd.DataFrame(source_data)

    def _build_dim_entity_from_accumulator(self) -> pd.DataFrame:
        """
        Build Dim_Entity DataFrame from accumulated data.
        Assigns Entity_Key starting from 200.
        """
        entity_data = []
        entity_key = 200  # Start key counter
        
        # Sort for consistent ordering
        sorted_entities = sorted(self.dim_entity_accumulator)
        
        for entity_name, entity_type, entity_domain in sorted_entities:
            entity_data.append({
                'Entity_Key': entity_key,
                'Entity_Name': entity_name,
                'Entity_Type': entity_type,
                'Entity_Domain': entity_domain
            })
            entity_key += 1
        
        dim_entity_df = pd.DataFrame(entity_data)
        
        # Update the schema builder's counter to match (for consistency)
        if len(dim_entity_df) > 0:
            self.schema_builder.entity_key_counter = entity_key
        
        return dim_entity_df

    def _save_rejected_entities(self):
        """Combine and save rejected entities across all batches."""
        if not self.rejected_entities_accumulator:
            print("  No rejected entities to save")
            return
        
        # Combine all rejected entity DataFrames
        combined_rejected = pd.concat(self.rejected_entities_accumulator, ignore_index=True)
        
        # Deduplicate and sum occurrence counts
        rejected_summary = combined_rejected.groupby('Rejected_Entity').agg({
            'Occurrence_Count': 'sum',
            'Reason': 'first'  # Take first reason (should be same for all)
        }).reset_index()
        
        # Sort by occurrence count (most frequent first)
        rejected_summary = rejected_summary.sort_values('Occurrence_Count', ascending=False)
        
        # Save to CSV
        output_file = self.output_dir / 'rejected_entities.csv'
        rejected_summary.to_csv(output_file, index=False)
        print(f"  Saved {len(rejected_summary)} unique rejected entities to {output_file}")
        print(f"  Total rejected entity occurrences: {rejected_summary['Occurrence_Count'].sum():,}")


def main():
    """Test the batch processor with a small sample."""
    processor = BatchProcessor(batch_size=100, output_dir='data/test_star_schema')

    # Test with a small subset of the actual CSV
    csv_path = 'data/merged_articles_cleaned.csv'

    try:
        # Read just first 200 rows for testing
        test_df = pd.read_csv(csv_path, nrows=200, low_memory=False)
        print(f"Testing with {len(test_df)} rows from CSV")

        # Process the test data
        tables = processor.process_csv_to_star_schema(csv_path, excel_path=None)

        print("\nTest completed successfully!")
        print(f"Generated {len(tables)} star schema tables")

        # Show summary
        for name, df in tables.items():
            print(f"{name}: {len(df)} rows")

    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
