#!/bin/bash

# Test script for the Ensembl GFF3 workflow
# This script can be run locally to test the logic

set -e

# Configuration
FTP_SERVER="ftp.ensembl.org"
BASE_PATH="/pub/rapid-release/species"
OUTPUT_FILE="ensembl_rapid_release.tsv"
TEMP_JSON="temp_species.json"
EXISTING_ACCESSIONS="existing_accessions.txt"
NEW_PATHS="new_paths.tsv"

echo "=== Ensembl GFF3 Workflow Test ==="

# Install dependencies if not present
if ! command -v jq &> /dev/null; then
    echo "Installing jq..."
    sudo apt-get update && sudo apt-get install -y jq
fi

if ! command -v lftp &> /dev/null; then
    echo "Installing lftp..."
    sudo apt-get update && sudo apt-get install -y lftp
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

# Use local JSON file
if [[ -f "species_metadata.json" ]]; then
    cp species_metadata.json "$TEMP_JSON"
    echo "Using local species_metadata.json file"
elif [[ -f "assemblies.json" ]]; then
    cp assemblies.json "$TEMP_JSON"
    echo "Using local assemblies.json file"
else
    echo "No JSON source found. Please add species_metadata.json or assemblies.json"
    exit 1
fi

echo "Fetched $(jq length "$TEMP_JSON") species records"

# Initialize file for new paths
echo -e "accession\tfull_path" > "$NEW_PATHS"

# Process each species from the JSON
echo "Processing species..."
jq -c '.[]' "$TEMP_JSON" | while read -r species_data; do
    # Extract fields from JSON
    assembly_accession=$(echo "$species_data" | jq -r '.assembly_accession')
    species_name=$(echo "$species_data" | jq -r '.species')
    
    # Skip if assembly accession is empty or null
    if [[ "$assembly_accession" == "null" || -z "$assembly_accession" ]]; then
        echo "Skipping record with null/empty assembly_accession"
        continue
    fi
    
    # Check if accession already exists
    if grep -q "^$assembly_accession$" "$EXISTING_ACCESSIONS"; then
        echo "Skipping $assembly_accession (already exists)"
        continue
    fi
    
    # Convert species name to format with underscores
    species_formatted=$(echo "$species_name" | tr ' ' '_')
    
    # Build the expected directory path
    expected_path="$BASE_PATH/$species_formatted/$assembly_accession"
    
    echo "Checking path: $expected_path for assembly: $assembly_accession"
    
    # Check if directory exists and look for GFF3 files
    lftp_output=$(lftp -e "set ssl:verify-certificate no; find $expected_path | grep -E '\\.gff3?\\.gz$'; bye" $FTP_SERVER 2>/dev/null)
    
    echo "lftp_output: $lftp_output"
    if [[ $? -eq 0 && -n "$lftp_output" ]]; then
        echo "  Found GFF3 files:"
        echo "$lftp_output" | while read -r gff3_path; do
            # The lftp output contains relative path, just prepend the HTTPS base URL
            full_https_path="https://${FTP_SERVER}$gff3_path"
            echo "full_https_path: $full_https_path"
            echo "    $assembly_accession -> $full_https_path"
            echo -e "$assembly_accession\t$full_https_path" >> "$NEW_PATHS"
        done
    else
        echo "  No GFF3 files found or directory does not exist"
    fi
done

# Check if we found any new paths
if [[ $(wc -l < "$NEW_PATHS") -gt 1 ]]; then
    new_count=$(($(wc -l < "$NEW_PATHS") - 1))
    echo "Found $new_count new GFF3 paths"
    
    # Append new paths to output file (skip header)
    tail -n +2 "$NEW_PATHS" >> "$OUTPUT_FILE"
    
    echo "Updated $OUTPUT_FILE with new paths"
    
    # Show summary of new additions
    echo "New paths added:"
    tail -n +2 "$NEW_PATHS"
else
    echo "No new GFF3 paths found"
fi

# Clean up
rm -f "$TEMP_JSON" "$EXISTING_ACCESSIONS" "$NEW_PATHS"

echo "=== Test completed ===" 