#!/bin/bash

# HDFS Upload Script
# Uploads Parquet files from local directory to HDFS using Docker

# set -e  # Exit on any error - commented out to continue processing all files
docker cp data/parquet/. namenode:/tmp/parquet/

# Configuration
LOCAL_PARQUET_DIR="/tmp/parquet"
DOCKER_CONTAINER="namenode"  # Adjust container name as needed
HDFS_PATH="/data"

# Files to upload (based on converted Parquet files)
FILES_TO_UPLOAD=(
    "prescriptions.parquet"
    "icustays.parquet"
    "d_items.parquet"
    "chartevents.parquet"
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

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check Docker container
check_docker_container() {
    log_info "Checking Docker container status..."
    
    if ! docker ps | grep -q "$DOCKER_CONTAINER"; then
        log_warning "Docker container '$DOCKER_CONTAINER' is not running"
        log_info "Available containers:"
        docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
        
        # Try to find alternative container names
        local alternatives=("hadoop" "namenode" "hdfs")
        for alt in "${alternatives[@]}"; do
            if docker ps | grep -q "$alt"; then
                log_info "Found alternative container: $alt"
                DOCKER_CONTAINER="$alt"
                break
            fi
        done
        
        if ! docker ps | grep -q "$DOCKER_CONTAINER"; then
            log_error "No suitable container found. Please ensure the container is running."
            log_info "You can start Hadoop with: docker-compose up -d"
            exit 1
        fi
    fi
    
    log_success "Docker container '$DOCKER_CONTAINER' is running"
}

# Function to upload file to HDFS
upload_to_hdfs() {
    local local_file="$1"
    local hdfs_file="$2"
    
    log_info "Uploading $local_file to HDFS: $hdfs_file"
    
    # Create HDFS directory if it doesn't exist
    docker exec "$DOCKER_CONTAINER" hdfs dfs -mkdir -p "$HDFS_PATH" 2>/dev/null || true
    
    # Upload file to HDFS
    if docker exec "$DOCKER_CONTAINER" hdfs dfs -put "$local_file" "$hdfs_file"; then
        log_success "Successfully uploaded $local_file to HDFS: $hdfs_file"
        
        # Show file info in HDFS
        log_info "HDFS file info:"
        docker exec "$DOCKER_CONTAINER" hdfs dfs -ls -h "$hdfs_file"
        return 0
    else
        log_error "Failed to upload $local_file to HDFS"
        return 1
    fi
}

# Function to show HDFS status
show_hdfs_status() {
    log_info "HDFS Status:"
    docker exec "$DOCKER_CONTAINER" hdfs dfs -df -h
    echo
    log_info "Files in HDFS $HDFS_PATH:"
    docker exec "$DOCKER_CONTAINER" hdfs dfs -ls -h "$HDFS_PATH" 2>/dev/null || log_warning "HDFS path $HDFS_PATH is empty or doesn't exist"
}

# Main execution
main() {
    log_info "Starting HDFS upload process..."
    log_info "Files to upload: ${FILES_TO_UPLOAD[*]}"
    
    # Check Docker container
    check_docker_container
    
    # Check if parquet directory exists inside the container
    if ! docker exec "$DOCKER_CONTAINER" test -d "$LOCAL_PARQUET_DIR"; then
        log_error "Parquet directory not found inside container: $LOCAL_PARQUET_DIR"
        log_info "Please copy Parquet files to the container first"
        log_info "You can do this with: docker cp data/parquet/. namenode:/tmp/parquet/"
        exit 1
    fi
    
    local success_count=0
    local total_count=${#FILES_TO_UPLOAD[@]}
    
    # Upload each file
    for parquet_file in "${FILES_TO_UPLOAD[@]}"; do
        echo
        log_info "Processing: $parquet_file"
        
        local_file="$LOCAL_PARQUET_DIR/$parquet_file"
        hdfs_file="$HDFS_PATH/$parquet_file"
        
        if ! docker exec "$DOCKER_CONTAINER" test -f "$local_file"; then
            log_error "Parquet file not found inside container: $local_file"
            continue
        fi
        
        if upload_to_hdfs "$local_file" "$hdfs_file"; then
            ((success_count++))
        fi
        
        echo "----------------------------------------"
    done
    
    # Summary
    echo
    log_info "Upload Summary:"
    log_info "Total files: $total_count"
    log_success "Successfully uploaded: $success_count"
    
    if [[ $success_count -lt $total_count ]]; then
        log_warning "Failed to upload: $((total_count - success_count)) files"
    fi
    
    # Show final HDFS status
    echo
    show_hdfs_status
    
    if [[ $success_count -eq $total_count ]]; then
        log_success "All files uploaded successfully! 🎉"
        exit 0
    else
        log_warning "Some files failed to upload. Check the logs above."
        exit 1
    fi
}

# Help function
show_help() {
    echo "HDFS Upload Script"
    echo
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Options:"
    echo "  -h, --help          Show this help message"
    echo "  -c, --container     Specify Docker container name (default: namenode)"
    echo "  -p, --hdfs-path     Specify HDFS destination path (default: /data)"
    echo "  -d, --local-dir     Specify local Parquet directory (default: /root/big-data/data/parquet)"
    echo
    echo "This script will upload the following Parquet files to HDFS:"
    for file in "${FILES_TO_UPLOAD[@]}"; do
        echo "  - $file"
    done
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -c|--container)
            DOCKER_CONTAINER="$2"
            shift 2
            ;;
        -p|--hdfs-path)
            HDFS_PATH="$2"
            shift 2
            ;;
        -d|--local-dir)
            LOCAL_PARQUET_DIR="$2"
            shift 2
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Run main function
main
