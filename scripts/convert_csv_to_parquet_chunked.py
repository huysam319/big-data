#!/usr/bin/env python3
"""
Robust CSV.gz to Parquet Converter (chunked processing, row-skip on errors)

- Đọc theo chunks với on_bad_lines="skip" để bỏ các dòng CSV sai định dạng.
- Căn kiểu theo schema của chunk đầu tiên cho các chunk sau.
- Nếu cả chunk vẫn lỗi khi ghi (PyArrow), dùng chiến lược "bisect" để loại đúng
  các hàng gây lỗi và vẫn ghi các hàng còn lại.

Usage:
    python convert_csv_to_parquet_chunked.py <input_csv.gz> <output_parquet>
    python convert_csv_to_parquet_chunked.py --input <input_csv.gz> --output <output_parquet>
"""

import argparse
import os
import sys
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

# --------------------- Helpers ---------------------

def validate_file_path(file_path, file_type="file"):
    path = Path(file_path)
    if file_type == "input":
        if not path.exists():
            raise FileNotFoundError(f"Input file does not exist: {file_path}")
        if not path.is_file():
            raise ValueError(f"Input path is not a file: {file_path}")
        if not os.access(file_path, os.R_OK):
            raise PermissionError(f"No read permission for file: {file_path}")
    elif file_type == "output":
        outdir = path.parent
        if not outdir.exists():
            try:
                outdir.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                raise PermissionError(f"Cannot create output directory: {e}")
        if not os.access(outdir, os.W_OK):
            raise PermissionError(f"No write permission for directory: {outdir}")

def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Chuẩn hóa chunk đầu tiên để suy ra schema hợp lý."""
    out = df.copy()
    for c in out.columns:
        # Đầu tiên ép về string dtype (nullable) để tránh object hỗn tạp
        out[c] = out[c].astype("string")

        # Chuẩn hóa các giá trị null phổ biến
        out[c] = out[c].replace(
            ['nan', 'None', 'NULL', 'null', 'N/A', 'n/a', '', ' '],
            pd.NA
        )

        # Heuristic: nếu đa số là số -> chuyển số
        try:
            num = pd.to_numeric(out[c], errors="coerce")
            if num.notna().mean() >= 0.7:
                # Dùng kiểu số nullable để vẫn chấp nhận NA
                if (num.dropna() % 1 == 0).all():
                    out[c] = num.astype("Int64")
                else:
                    out[c] = num.astype("Float64")
        except Exception:
            pass
    return out

def coerce_to_schema(df: pd.DataFrame, schema: pa.Schema) -> pd.DataFrame:
    """Căn DataFrame theo schema đã có để giảm khả năng lỗi khi viết."""
    out = df.copy()

    # Bổ sung cột thiếu + sắp xếp cột đúng thứ tự schema
    for name in schema.names:
        if name not in out.columns:
            out[name] = pd.NA
    out = out[schema.names]

    # Chuyển kiểu từng cột theo schema Arrow
    for field in schema:
        name = field.name
        atype = field.type

        col = out[name]

        # Bắt đầu từ dtype chuỗi nullable để gọn
        if not pd.api.types.is_string_dtype(col) and not pd.api.types.is_numeric_dtype(col):
            col = col.astype("string")

        try:
            if pa.types.is_integer(atype):
                col = pd.to_numeric(col, errors="coerce").astype("Int64")
            elif pa.types.is_floating(atype):
                col = pd.to_numeric(col, errors="coerce").astype("Float64")
            elif pa.types.is_boolean(atype):
                # Chuẩn hóa boolean từ các dạng phổ biến
                mapping = {
                    "true": True, "false": False,
                    "1": True, "0": False,
                    "yes": True, "no": False,
                    "y": True, "n": False
                }
                s = col.astype("string").str.strip().str.lower()
                col = s.map(mapping).astype("boolean")
            elif pa.types.is_timestamp(atype):
                col = pd.to_datetime(col, errors="coerce")
            else:
                # Mặc định string nullable
                col = col.astype("string").replace(
                    ['nan', 'None', 'NULL', 'null', 'N/A', 'n/a', '', ' '],
                    pd.NA
                )
        except Exception:
            # Nếu ép kiểu thất bại, rơi về string nullable
            col = col.astype("string")

        out[name] = col

    return out

def table_from_df_safe(df: pd.DataFrame, schema: pa.Schema):
    """
    Cố gắng tạo pa.Table từ df theo schema.
    Nếu thất bại, trả về None và phát sinh exception để caller xử lý.
    """
    # preserve_index=False để không sinh cột index
    return pa.Table.from_pandas(df, schema=schema, preserve_index=False, safe=False)

def bisect_write(writer: pq.ParquetWriter, df: pd.DataFrame, schema: pa.Schema) -> int:
    """
    Ghi 'an toàn' một DataFrame:
    - Nếu ghi cả khối thành công => ghi luôn.
    - Nếu lỗi => tách đôi và thử lại từng nửa.
    - Hàng nào không thể ghi sẽ bị loại.
    Trả về số dòng đã ghi.
    """
    if df.empty:
        return 0
    try:
        tbl = table_from_df_safe(df, schema)
        writer.write_table(tbl)
        return len(df)
    except Exception:
        # Nếu chỉ còn 1 hàng mà vẫn lỗi => loại bỏ hàng đó
        if len(df) == 1:
            return 0
        mid = len(df) // 2
        left = df.iloc[:mid]
        right = df.iloc[mid:]
        written_left = bisect_write(writer, left, schema)
        written_right = bisect_write(writer, right, schema)
        return written_left + written_right

def make_chunk_reader(input_path: str, chunk_size: int):
    """
    Tạo reader theo chunks, tự động bỏ dòng lỗi.
    Dùng on_bad_lines='skip'. Nếu phiên bản pandas cũ, fallback error_bad_lines=False.
    """
    base_kwargs = dict(
        compression="gzip",
        chunksize=chunk_size,
        dtype=str,
        na_values=['', 'NULL', 'null', 'None', 'N/A', 'n/a'],
        keep_default_na=True,
        low_memory=False,
        encoding="utf-8",
    )
    # encoding_errors cho pandas >= 1.5
    try:
        return pd.read_csv(input_path, on_bad_lines="skip", encoding_errors="replace", **base_kwargs)
    except TypeError:
        # pandas cũ không có encoding_errors/on_bad_lines
        base_kwargs.pop("encoding", None)  # sẽ để pandas tự suy đoán nếu cần
        return pd.read_csv(input_path, error_bad_lines=False, warn_bad_lines=True, **base_kwargs)

# --------------------- Core ---------------------

def convert_csv_to_parquet_chunked(input_path, output_path, chunk_size=50000, max_rows=10000000):
    try:
        validate_file_path(input_path, "input")
        validate_file_path(output_path, "output")

        # Nếu giới hạn <= 0 → tạo Parquet rỗng
        if max_rows is not None and max_rows <= 0:
            pq.write_table(pa.table({}), output_path, compression="snappy")
            print("✅ Max rows = 0, created empty Parquet.")
            return True

        rows_budget = max_rows  # còn bao nhiêu hàng được phép ghi
        print(f"Converting {input_path} to {output_path}...")
        print("📦 Đọc CSV.gz theo từng chunk...")
        reader = make_chunk_reader(input_path, chunk_size)

        # Chunk đầu để suy schema
        try:
            first_chunk_raw = next(reader)
        except StopIteration:
            print("⚠️  File trống. Tạo Parquet rỗng.")
            pq.write_table(pa.table({}), output_path, compression="snappy")
            return True

        # Cắt theo ngân sách nếu cần
        if rows_budget is not None and len(first_chunk_raw) > rows_budget:
            first_chunk_raw = first_chunk_raw.iloc[:rows_budget]

        first_chunk_df = normalize_dataframe(first_chunk_raw)
        first_table = pa.Table.from_pandas(first_chunk_df, preserve_index=False)
        schema = first_table.schema

        print(f"📋 Detected schema with {len(schema.names)} columns")
        print(f"Columns: {schema.names}")

        writer = pq.ParquetWriter(output_path, schema=schema, compression="snappy")

        # Ghi chunk đầu tiên
        writer.write_table(first_table)
        total_rows_in = len(first_chunk_raw)
        total_rows_written = len(first_chunk_df)
        total_rows_dropped = total_rows_in - total_rows_written

        # Trừ ngân sách
        if rows_budget is not None:
            rows_budget -= total_rows_written
            if rows_budget <= 0:
                writer.close()
                print("⛔ Reached max rows limit; stopping.")
                print(f"📊 Total written rows: {total_rows_written:,}")
                return True

        print(f"🔢 Processed {total_rows_written:,}/{total_rows_in:,} rows (chunk 1)")

        # Các chunk tiếp theo
        chunk_idx = 2
        for raw_df in reader:
            total_rows_in += len(raw_df)

            # Nếu đã hết ngân sách thì dừng
            if rows_budget is not None and rows_budget <= 0:
                print("⛔ Reached max rows limit; stopping.")
                break

            # Cắt theo ngân sách còn lại
            if rows_budget is not None and len(raw_df) > rows_budget:
                raw_df = raw_df.iloc[:rows_budget]

            df = coerce_to_schema(raw_df, schema)
            written = bisect_write(writer, df, schema)
            dropped_here = len(df) - written
            total_rows_written += written
            total_rows_dropped += dropped_here

            if rows_budget is not None:
                rows_budget -= written

            info = f"🔢 Processed {total_rows_written:,}/{total_rows_in:,} rows (chunk {chunk_idx})"
            if dropped_here > 0:
                info += f" — skipped {dropped_here:,} bad row(s)"
            print(info)
            chunk_idx += 1

            if rows_budget is not None and rows_budget <= 0:
                print("⛔ Reached max rows limit; stopping.")
                break

        writer.close()

        print("✅ Conversion finished.")
        print(f"📊 Total input rows (seen): {total_rows_in:,}")
        print(f"📈 Total written rows:      {total_rows_written:,}")
        print(f"🧹 Total skipped rows:      {total_rows_dropped:,}")

        if Path(output_path).exists():
            size = Path(output_path).stat().st_size
            print(f"✅ Output file: {output_path} ({size:,} bytes)")
            return True
        else:
            print(f"❌ Output file was not created: {output_path}")
            return False

    except Exception as e:
        print(f"❌ Error during conversion: {e}")
        import traceback
        traceback.print_exc()
        return False
# --------------------- CLI ---------------------

def main():
    parser = argparse.ArgumentParser(
        description="Convert CSV.gz files to Parquet format using chunked processing (skips bad rows)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python convert_csv_to_parquet_chunked.py data.csv.gz data.parquet
  python convert_csv_to_parquet_chunked.py --input data.csv.gz --output data.parquet
  python convert_csv_to_parquet_chunked.py --input data.csv.gz --output data.parquet --chunk-size 25000
        """
    )
    parser.add_argument("input_file", nargs="?", help="Path to input CSV.gz file")
    parser.add_argument("output_file", nargs="?", help="Path to output Parquet file")
    parser.add_argument("--input", "-i", help="Path to input CSV.gz file (alternative to positional argument)")
    parser.add_argument("--output", "-o", help="Path to output Parquet file (alternative to positional argument)")
    parser.add_argument("--chunk-size", "-c", type=int, default=50000, help="Rows per chunk (default: 50000)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output")
    parser.add_argument("--max-rows", "-m", type=int, default=None, help="Maximum number of rows to convert (default: all rows)")
    args = parser.parse_args()

    input_path = args.input or args.input_file
    output_path = args.output or args.output_file

    if not input_path:
        print("❌ Error: Input file path is required")
        parser.print_help()
        sys.exit(1)
    if not output_path:
        print("❌ Error: Output file path is required")
        parser.print_help()
        sys.exit(1)

    input_path = os.path.abspath(input_path)
    output_path = os.path.abspath(output_path)

    if args.verbose:
        print(f"Input file: {input_path}")
        print(f"Output file: {output_path}")
        print(f"Chunk size: {args.chunk_size}")

    success = convert_csv_to_parquet_chunked(input_path, output_path, args.chunk_size)
    if success:
        print("🎉 Conversion completed successfully!")
        sys.exit(0)
    else:
        print("💥 Conversion failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
