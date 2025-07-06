#!/bin/bash

# Test script for the NCBI GFF3 workflow
# This script can be run locally to test the logic

set -e

# Configuration
TAXID="2759"
FIELDS="accession"
FTP_SERVER="ftp.ncbi.nlm.nih.gov"
FTP_URL="ftp://ftp.ncbi.nlm.nih.gov"
BASE_DIR="/genomes/all"
OUTPUT_FILE="ncbi.tsv"
PATHS_FILE="paths.tsv"
EXISTING_ACCESSIONS="existing_accessions.tsv"
NCBI_ACCESSIONS="ncbi_accessions.tsv"
DIRS_FILE="directories.txt"

echo "=== NCBI GFF3 Workflow Test ==="

# Install dependencies if not present
if ! command -v lftp &> /dev/null; then
    echo "Installing lftp..."
    sudo apt-get update && sudo apt-get install -y lftp
fi

# Download NCBI datasets tools if not present
if [[ ! -x "./datasets" ]] || [[ ! -x "./dataformat" ]]; then
    echo "Downloading NCBI datasets tools..."
    curl -f -O 'https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/v2/linux-amd64/dataformat' || {
        echo "Error: Failed to download dataformat"
        exit 1
    }
    curl -f -O 'https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/v2/linux-amd64/datasets' || {
        echo "Error: Failed to download datasets"
        exit 1
    }
    chmod +x datasets dataformat
    echo "Successfully downloaded and made executable: datasets, dataformat"
fi

# Initialize output file
if [[ ! -f "$OUTPUT_FILE" ]]; then
    echo -e "accession\tfull_path" > "$OUTPUT_FILE"
    echo "Created new output file: $OUTPUT_FILE"
fi

# Get existing accessions
if [[ -f "$OUTPUT_FILE" ]]; then
    awk 'NR > 1 {print $1}' "$OUTPUT_FILE" | sort > "$EXISTING_ACCESSIONS"
    echo "Found $(wc -l < "$EXISTING_ACCESSIONS") existing accessions"
else
    touch "$EXISTING_ACCESSIONS"
    echo "No existing accessions found"
fi

# Get NCBI Assemblies
echo "Fetching NCBI assemblies for taxon $TAXID..."
./datasets summary genome taxon "$TAXID" --assembly-level chromosome,complete --annotated --as-json-lines |
./dataformat tsv genome --fields "$FIELDS" | 
tail -n +2 > "$NCBI_ACCESSIONS"

# Check if we got any accessions
if [[ ! -s "$NCBI_ACCESSIONS" ]]; then
    echo "Warning: No accessions found from NCBI datasets"
    exit 1
else
    echo "Found $(wc -l < "$NCBI_ACCESSIONS") accessions from NCBI"
    echo "First 5 accessions:"
    head -5 "$NCBI_ACCESSIONS"
fi

# Set FTP paths
echo "Processing accessions and building FTP paths..."
echo -e "accession\tbase_path" > "$PATHS_FILE"

# Process only the first 5 new accessions for testing
while read -r accession; do
    # Skip if accession is empty
    [[ -z "$accession" ]] && continue
    
    # Skip if accession exists
    if grep -Fwq "$accession" "$EXISTING_ACCESSIONS"; then
        echo "Skipping existing accession: $accession"
        continue
    fi

    # Validate accession format (should be like GCA_000001405.1)
    if [[ ! "$accession" =~ ^GCA_[0-9]{9}\.[0-9]+$ ]]; then
        echo "Warning: Skipping invalid accession format: $accession"
        continue
    fi

    # Append new FTP paths to the temporary file
    base_path="$FTP_URL$BASE_DIR/${accession:0:3}/${accession:4:3}/${accession:7:3}/${accession:10:3}"
    echo -e "${accession}\t${base_path}" >> "$PATHS_FILE"

done < "$NCBI_ACCESSIONS"

# Limit to first 5 new accessions for testing
if [[ $(wc -l < "$PATHS_FILE") -gt 1 ]]; then
    # Keep header and first 10 data rows
    head -10 "$PATHS_FILE" > "${PATHS_FILE}.tmp" && mv "${PATHS_FILE}.tmp" "$PATHS_FILE"
    echo "Limited to first 10 new accessions for testing"
fi

# Check if we have any new paths
if [[ $(wc -l < "$PATHS_FILE") -le 1 ]]; then
    echo "No new accessions to process"
    exit 0
fi

echo "Processing $(($(wc -l < "$PATHS_FILE") - 1)) new accessions"

# Search for GFF files directly from base paths
echo "Searching for GFF files in each base path..."

# Get initial line count to track new additions
initial_lines=$(wc -l < "$OUTPUT_FILE" 2>/dev/null || echo 0)

# Loop through base FTP paths and find GFF files directly (skip header)
tail -n +2 "$PATHS_FILE" | while read -r accession base_path; do
    echo "Searching for GFF files in $base_path for accession: $accession"
    
    # Use lftp to connect once and find all gff.gz files recursively
    lftp_output=$(lftp -c "
        set net:timeout 10
        set net:max-retries 3
        open $FTP_SERVER
        cd ${base_path#ftp://ftp.ncbi.nlm.nih.gov}
        find . | grep '\.gff\.gz$'
        quit
    " 2>/dev/null)
    
    if [[ -n "$lftp_output" ]]; then
        echo "  Found GFF files:"
        # Process each GFF file found
        while read -r gff_file; do
            # Convert FTP URL to HTTPS and build full path
            https_base=$(echo "$base_path" | sed 's|^ftp://|https://|')
            # Remove leading ./ from find output and combine with base path
            clean_path=$(echo "$gff_file" | sed 's|^\./||')
            full_path="$https_base/$clean_path"
            echo "    $accession -> $full_path"
            echo -e "$accession\t$full_path" >> "$OUTPUT_FILE"
        done <<< "$lftp_output"
    else
        echo "  No GFF files found for $accession"
    fi

done

# Show summary of what was added
if [[ -f "$OUTPUT_FILE" ]]; then
    final_lines=$(wc -l < "$OUTPUT_FILE")
    new_lines=$((final_lines - initial_lines))
    if [[ $new_lines -gt 0 ]]; then
        echo "Added $new_lines new GFF paths to $OUTPUT_FILE"
    else
        echo "No new GFF paths found"
    fi
fi

# Clean up temporary files
echo "Cleaning up temporary files..."
rm -f "$PATHS_FILE" "$EXISTING_ACCESSIONS" "$NCBI_ACCESSIONS"

echo "=== Test completed ===" 