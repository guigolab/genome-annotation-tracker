---

# Genome Annotation Tracker

The **Genome Annotation Tracker** is an automated system designed to track and mirror eukaryotic genome annotations from major biological databases. It provides a comprehensive method to retrieve, organize, and maintain up-to-date genome assembly annotations from NCBI (RefSeq and GenBank) and Ensembl databases. This repository automates the collection and mapping of genome annotation data with scheduled updates and change detection.

## Overview

This repository provides an automated system for tracking eukaryotic genome annotations from three major sources:

- **NCBI RefSeq**: Curated, non-redundant reference sequences with high-quality annotations
- **NCBI GenBank**: Comprehensive collection of all publicly available DNA sequences
- **Ensembl**: Rapid release and curated annotations from species across the tree of life

## Features

- **Automated Mirroring**: Scheduled workflows that automatically fetch and update annotation files
- **Change Detection**: Only processes and commits files when actual changes are detected
- **Metadata Tracking**: Captures annotation provider, pipeline information, and submission dates


## Data Sources

### NCBI RefSeq
- **Source**: National Center for Biotechnology Information RefSeq database
- **Format**: GFF3 files
- **Update Frequency**: Weekly (Mondays)
- **Assembly Level**: Chromosome and complete genome level assemblies
- **Taxon Focus**: Eukaryotes (TaxID: 2759)

### NCBI GenBank
- **Source**: National Center for Biotechnology Information GenBank database
- **Format**: GFF3 files
- **Update Frequency**: Weekly (Tuesdays)
- **Assembly Level**: Chromosome and complete genome level assemblies
- **Taxon Focus**: Eukaryotes (TaxID: 2759)

### Ensembl
- **Source**: Ensembl Rapid Release server
- **Format**: GFF3 files
- **Update Frequency**: Weekly (Sundays)
- **Assembly Level**: All available assemblies
- **Taxon Focus**: Eukaryotes (TaxID: 2759)

## Output Files

The system generates three main annotation files in the `data/` directory:

### 1. `refseq_annotations.tsv`
Contains NCBI RefSeq annotations with the following columns:
- `assembly_accession`: NCBI assembly accession
- `assembly_name`: Assembly name
- `taxon_id`: NCBI taxonomic identifier
- `organism_name`: Scientific name of the organism
- `source_database`: "RefSeq"
- `annotation_provider`: Annotation provider 
- `access_url`: Direct URL to the annotation file
- `file_format`: "gff"
- `release_date`: Date when the annotation was released
- `retrieval_date`: Date when the annotation was retrieved
- `pipeline_name`: Name of the annotation pipeline if any
- `pipeline_method`: Method used for annotation if any
- `pipeline_version`: Version of the annotation pipeline if any
- `last_modified_date`: Last modification date of the file
- `md5_checksum`:  MD5 checksum of the uncompressed file for integrity verification

### 2. `genbank_annotations.tsv`
Contains NCBI GenBank annotations with the same structure as RefSeq annotations.

### 3. `ensembl_annotations.tsv`
Contains Ensembl annotations with the same structure as NCBI annotations.

## Automated Workflows

The repository includes GitHub Actions workflows that automatically maintain the annotation files:

### Scheduled Workflows
- **Ensembl**: Runs every Sunday at 2 AM UTC
- **NCBI RefSeq**: Runs every Monday at 3 AM UTC  
- **NCBI GenBank**: Runs every Tuesday at 3 AM UTC

