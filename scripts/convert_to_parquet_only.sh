#!/bin/bash

# Simple CSV.gz to Parquet Converter Script
# Converts specified CSV.gz files to Parquet format using convert_csv_to_parquet.py

# set -e  # Exit on any error - commented out to continue processing all files

# Configuration
DATA_DIR="/root/big-data/data"
SCRIPT_DIR="/root/big-data/scripts"
OUTPUT_DIR="/root/big-data/data/parquet"
CONVERTER_SCRIPT="$SCRIPT_DIR/convert_csv_to_parquet_chunked.py"

# Files to convert
FILES_TO_CONVERT=(
    "d_items.csv.gz"
    "prescriptions.csv.gz"
    "icustays.csv.gz"
    "chartevents.csv.gz"
)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if converter script exists
if [[ ! -f "$CONVERTER_SCRIPT" ]]; then
    log_error "Converter script not found: $CONVERTER_SCRIPT"
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

log_info "Starting CSV.gz to Parquet conversion..."
log_info "Files to process: ${FILES_TO_CONVERT[*]}"

success_count=0
total_count=${#FILES_TO_CONVERT[@]}

for csv_file in "${FILES_TO_CONVERT[@]}"; do
    echo
    log_info "Processing: $csv_file"
    
    input_path="$DATA_DIR/$csv_file"
    parquet_file="${csv_file%.csv.gz}.parquet"
    output_path="$OUTPUT_DIR/$parquet_file"
    
    if [[ ! -f "$input_path" ]]; then
        log_error "File not found: $input_path"
        continue
    fi
    
    if python3 "$CONVERTER_SCRIPT" "$input_path" "$output_path"; then
        log_success "Converted: $csv_file -> $parquet_file"
        ((success_count++))
    else
        log_error "Failed to convert: $csv_file"
    fi
done

echo
log_info "Conversion Summary:"
log_info "Total files: $total_count"
log_success "Successfully converted: $success_count"

if [[ $success_count -eq $total_count ]]; then
    log_success "All files converted successfully! 🎉"
    log_info "Parquet files saved to: $OUTPUT_DIR"
else
    log_error "Some conversions failed. Check the logs above."
    exit 1
fi
