#!/usr/bin/env python3


import argparse
import sys
import time
from pathlib import Path

from batch_processor import BatchProcessor


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Transform CSV articles to star schema OLAP-ready format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python parsers/main_transformer.py
  python parsers/main_transformer.py --batch-size 10000
  python parsers/main_transformer.py --output-dir data/cube_data
  python parsers/main_transformer.py --csv-path data/my_articles.csv
        """
    )

    parser.add_argument(
        '--csv-path',
        default='data/merged_articles_cleaned.csv',
        help='Path to input CSV file (default: data/merged_articles_cleaned.csv)'
    )

    parser.add_argument(
        '--excel-path',
        default='olap_cube/config/News Search Tags 101.xlsx',
        help='Path to Excel tags file (default: olap_cube/config/News Search Tags 101.xlsx)'
    )

    parser.add_argument(
        '--output-dir',
        default='data/star_schema',
        help='Output directory for star schema CSVs (default: data/star_schema)'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=5000,
        help='Batch size for processing (default: 5000)'
    )

    parser.add_argument(
        '--yes',
        '-y',
        action='store_true',
        help='Skip confirmation prompt and start transformation immediately'
    )

    args = parser.parse_args()

    # Validate inputs
    csv_path = Path(args.csv_path)
    if not csv_path.exists():
        print(f"Error: CSV file not found: {csv_path}")
        sys.exit(1)

    excel_path = args.excel_path
    if excel_path and not Path(excel_path).exists():
        print(f"Error: Excel file not found: {excel_path}")
        sys.exit(1)

    output_dir = Path(args.output_dir)

    print("=== Star Schema Cube-Ready Data Transformer ===")
    print(f"Input CSV: {csv_path}")
    print(f"Excel Tags: {excel_path or 'News Search Tags 101.xlsx'}")
    print(f"Output Directory: {output_dir}")
    print(f"Batch Size: {args.batch_size}")
    print()

    # Check CSV size for progress estimation
    try:
        import csv
        # Increase CSV field size limit to handle large text fields
        csv.field_size_limit(min(2147483647, sys.maxsize))
        # Count actual CSV rows (not physical file lines, which include embedded newlines)
        # Use csv.reader to properly handle quoted fields with embedded newlines
        with open(csv_path, 'r', encoding='utf-8', errors='ignore', newline='') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            row_count = sum(1 for _ in reader)
        print(f"Input CSV contains {row_count:,} articles")
        estimated_batches = (row_count // args.batch_size) + 1
        print(f"Will process in approximately {estimated_batches} batches")
        print()
    except Exception as e:
        print(f"Could not estimate CSV size: {e}")
        print()

    # Confirm start (skip if --yes flag is set)
    if not args.yes:
        try:
            response = input("Start transformation? (y/N): ").strip().lower()
            if response not in ['y', 'yes']:
                print("Transformation cancelled.")
                sys.exit(0)
        except KeyboardInterrupt:
            print("\nTransformation cancelled.")
            sys.exit(0)
    else:
        print("Starting transformation (--yes flag set)...")

    print("\nStarting transformation...")
    start_time = time.time()

    try:
        # Initialize batch processor
        processor = BatchProcessor(
            batch_size=args.batch_size,
            output_dir=str(output_dir)
        )

        # Process the CSV
        tables = processor.process_csv_to_star_schema(
            str(csv_path),
            excel_path
        )

        # Success summary
        total_time = time.time() - start_time
        print("\n" + "="*60)
        print("TRANSFORMATION COMPLETED SUCCESSFULLY!")
        print("="*60)
        print(f"Total time: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")
        print(f"Processed {sum(len(df) for df in tables.values()):,} total records")
        print()

        # Show table summaries
        print("Generated Star Schema Tables:")
        print("-" * 40)
        for name, df in tables.items():
            print(f"  {name:25s}: {len(df):8,} rows")
        print()

        print("Output files saved to:")
        for name in tables.keys():
            csv_file = output_dir / f"{name}.csv"
            if csv_file.exists():
                size_mb = csv_file.stat().st_size / (1024 * 1024)
                print(f"  {name}.csv: {size_mb:.2f} MB")
        print()

        print("Next steps:")
        print("1. Import these CSV files into your OLAP cube tool")
        print("2. Fact_Document is the central fact table")
        print("3. Use Bridge_Fact_Tag and Bridge_Fact_Entity for multi-dimensional analysis")
        print("4. Dim_Time, Dim_Source, Dim_Tag, Dim_Entity are dimension tables")

    except Exception as e:
        print(f"\nERROR: Transformation failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
