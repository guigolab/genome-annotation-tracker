#!/bin/bash

# Define the FTP server and target directory
FTP_SERVER="ftp.ensembl.org"
TARGET_DIR="/pub/rapid-release/species/"

# Define the output file where the paths will be saved
OUTPUT_FILE="gff3_paths.txt"

# Temp file to hold the raw ls -R output
RAW_OUTPUT="raw_ls_output.txt"

# Use lftp to connect to the server and list all files
lftp -c "
  set net:timeout 10
  set net:max-retries 3
  open $FTP_SERVER
  cd $TARGET_DIR
  ls -R
  quit
" > $RAW_OUTPUT

# Process the raw output to get only full paths to .gff3.gz files
awk '
  # When a directory name is printed (ends with a colon), store it
  /:$/ {dir=substr($0, 1, length($0)-1); next}
  # If the line contains .gff3.gz, print the full path (directory + filename)
  /\.gff3\.gz$/ {print dir "/" $NF}
' $RAW_OUTPUT > $OUTPUT_FILE

# Remove the temporary raw output file
rm $RAW_OUTPUT

# Confirm the paths have been saved
echo "All .gff3.gz file paths have been written to $OUTPUT_FILE"
