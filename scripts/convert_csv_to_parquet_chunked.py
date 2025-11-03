#!/usr/bin/env python3
"""
Robust CSV.gz to Parquet Converter (chunked processing)

This version handles large files by processing them in chunks while maintaining
schema consistency and handling mixed data types properly.

Usage:
    python convert_csv_to_parquet_chunked.py <input_csv.gz> <output_parquet>
    python convert_csv_to_parquet_chunked.py --input <input_csv.gz> --output <output_parquet>
"""

import argparse
import os
import sys
import gzip
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np


def validate_file_path(file_path, file_type="file"):
    """Validate if file path exists and is accessible."""
    path = Path(file_path)
    
    if file_type == "input":
        if not path.exists():
            raise FileNotFoundError(f"Input file does not exist: {file_path}")
        if not path.is_file():
            raise ValueError(f"Input path is not a file: {file_path}")
        if not os.access(file_path, os.R_OK):
            raise PermissionError(f"No read permission for file: {file_path}")
    
    elif file_type == "output":
        # Check if output directory exists and is writable
        output_dir = path.parent
        if not output_dir.exists():
            try:
                output_dir.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                raise PermissionError(f"Cannot create output directory: {e}")
        
        if not os.access(output_dir, os.W_OK):
            raise PermissionError(f"No write permission for directory: {output_dir}")


def normalize_dataframe(df):
    """Normalize DataFrame to handle mixed types and ensure consistency."""
    normalized_df = df.copy()
    
    for col in normalized_df.columns:
        if normalized_df[col].dtype == 'object':
            # Convert all values to strings first
            normalized_df[col] = normalized_df[col].astype(str)
            
            # Replace various null representations
            normalized_df[col] = normalized_df[col].replace([
                'nan', 'None', 'NULL', 'null', 'N/A', 'n/a', '', ' '
            ], None)
            
            # Try to convert to numeric if most values are numeric
            try:
                numeric_values = pd.to_numeric(normalized_df[col], errors='coerce')
                non_null_count = numeric_values.notna().sum()
                total_count = len(normalized_df[col])
                
                if non_null_count > total_count * 0.7:  # If 70%+ are numeric
                    normalized_df[col] = numeric_values
            except:
                pass  # Keep as string if conversion fails
    
    return normalized_df


def convert_csv_to_parquet_chunked(input_path, output_path, chunk_size=50000):
    """
    Convert CSV.gz file to Parquet format using chunked processing.
    
    Args:
        input_path (str): Path to input CSV.gz file
        output_path (str): Path to output Parquet file
        chunk_size (int): Number of rows to process at a time
    
    Returns:
        bool: True if conversion successful, False otherwise
    """
    try:
        # Validate input and output paths
        validate_file_path(input_path, "input")
        validate_file_path(output_path, "output")
        
        print(f"Converting {input_path} to {output_path}...")
        print("📊 Reading CSV.gz file in chunks...")
        
        # First pass: determine schema from first chunk
        print("🔍 Determining schema from first chunk...")
        
        with gzip.open(input_path, 'rt', encoding='utf-8') as f:
            # Read first chunk to determine schema
            first_chunk_df = pd.read_csv(
                f, 
                nrows=chunk_size,
                dtype=str,  # Read as strings initially
                na_values=['', 'NULL', 'null', 'None', 'N/A', 'n/a'],
                keep_default_na=True,
                low_memory=False
            )
        
        # Normalize the first chunk
        first_chunk_df = normalize_dataframe(first_chunk_df)
        
        # Create schema from normalized first chunk
        schema = pa.Schema.from_pandas(first_chunk_df)
        
        print(f"📋 Detected schema with {len(first_chunk_df.columns)} columns")
        print(f"Column names: {list(first_chunk_df.columns)}")
        
        # Create Parquet writer
        writer = pq.ParquetWriter(output_path, schema=schema, compression='snappy')
        
        # Write first chunk
        first_table = pa.Table.from_pandas(first_chunk_df, schema=schema)
        writer.write_table(first_table)
        
        total_rows = len(first_chunk_df)
        print(f"🔢 Processed {total_rows:,} rows...")
        
        # Process remaining chunks
        chunk_num = 2
        with gzip.open(input_path, 'rt', encoding='utf-8') as f:
            # Skip header and first chunk
            f.readline()  # Skip header
            for _ in range(len(first_chunk_df)):
                f.readline()  # Skip first chunk rows
            
            while True:
                try:
                    # Read next chunk
                    chunk_df = pd.read_csv(
                        f, 
                        nrows=chunk_size,
                        dtype=str,  # Read as strings initially
                        na_values=['', 'NULL', 'null', 'None', 'N/A', 'n/a'],
                        keep_default_na=True,
                        low_memory=False,
                        header=None,  # No header for subsequent chunks
                        names=first_chunk_df.columns  # Use column names from first chunk
                    )
                    
                    if chunk_df.empty:
                        break
                    
                    # Normalize chunk
                    chunk_df = normalize_dataframe(chunk_df)
                    
                    # Ensure column order matches schema
                    chunk_df = chunk_df[schema.names]
                    
                    # Convert to PyArrow table
                    chunk_table = pa.Table.from_pandas(chunk_df, schema=schema)
                    writer.write_table(chunk_table)
                    
                    total_rows += len(chunk_df)
                    print(f"🔢 Processed {total_rows:,} rows (chunk {chunk_num})...")
                    chunk_num += 1
                    
                except pd.errors.EmptyDataError:
                    break
                except Exception as e:
                    print(f"⚠️  Warning: Error processing chunk {chunk_num}: {e}")
                    break
        
        # Close the writer
        writer.close()
        
        print(f"✅ Successfully converted {input_path} to {output_path}")
        print(f"📊 Total rows processed: {total_rows:,}")
        
        # Verify the output file was created
        if Path(output_path).exists():
            file_size = Path(output_path).stat().st_size
            print(f"✅ Output file verified: {output_path} ({file_size:,} bytes)")
            return True
        else:
            print(f"❌ Output file was not created: {output_path}")
            return False
            
    except Exception as e:
        print(f"❌ Error during conversion: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main function to handle command-line arguments and execute conversion."""
    parser = argparse.ArgumentParser(
        description="Convert CSV.gz files to Parquet format using chunked processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python convert_csv_to_parquet_chunked.py data.csv.gz data.parquet
  python convert_csv_to_parquet_chunked.py --input data.csv.gz --output data.parquet
  python convert_csv_to_parquet_chunked.py --input data.csv.gz --output data.parquet --chunk-size 25000
        """
    )
    
    parser.add_argument(
        "input_file", 
        nargs="?", 
        help="Path to input CSV.gz file"
    )
    parser.add_argument(
        "output_file", 
        nargs="?", 
        help="Path to output Parquet file"
    )
    parser.add_argument(
        "--input", "-i",
        help="Path to input CSV.gz file (alternative to positional argument)"
    )
    parser.add_argument(
        "--output", "-o",
        help="Path to output Parquet file (alternative to positional argument)"
    )
    parser.add_argument(
        "--chunk-size", "-c",
        type=int,
        default=50000,
        help="Number of rows to process at a time (default: 50000)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    
    args = parser.parse_args()
    
    # Determine input and output paths
    input_path = args.input or args.input_file
    output_path = args.output or args.output_file
    
    # Validate required arguments
    if not input_path:
        print("❌ Error: Input file path is required")
        parser.print_help()
        sys.exit(1)
    
    if not output_path:
        print("❌ Error: Output file path is required")
        parser.print_help()
        sys.exit(1)
    
    # Convert absolute paths
    input_path = os.path.abspath(input_path)
    output_path = os.path.abspath(output_path)
    
    if args.verbose:
        print(f"Input file: {input_path}")
        print(f"Output file: {output_path}")
        print(f"Chunk size: {args.chunk_size}")
    
    # Perform conversion
    success = convert_csv_to_parquet_chunked(input_path, output_path, args.chunk_size)
    
    if success:
        print("🎉 Conversion completed successfully!")
        sys.exit(0)
    else:
        print("💥 Conversion failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
