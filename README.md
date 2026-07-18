---

# Genome Annotation Tracker

The **Genome Annotation Tracker** is an automated system designed to track and mirror eukaryotic genome annotations from major biological databases. It provides a comprehensive method to retrieve, organize, and maintain up-to-date genome assembly annotations from NCBI (RefSeq and GenBank) and Ensembl databases. This repository automates the collection and mapping of genome annotation data with scheduled updates and change detection.

## Overview

This repository provides an automated system for tracking eukaryotic genome annotations from four major sources:

- **NCBI RefSeq**: Curated, non-redundant reference sequences with high-quality annotations
- **NCBI GenBank**: Comprehensive collection of all publicly available DNA sequences
- **Ensembl**: Rapid release and curated annotations from species across the tree of life
- **CommunityRegistry (annotrieve-registry)**: Contributor-submitted annotations from the [Annotrieve community registry](https://github.com/guigolab/annotrieve-registry)

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

### CommunityRegistry (annotrieve-registry)
- **Source**: [annotrieve-registry](https://github.com/guigolab/annotrieve-registry) project folders (`manifest.yaml` + `annotations.tsv`)
- **Format**: GFF3 files (plain or gzipped)
- **Update Frequency**: Weekly (Fridays, after NCBI/Ensembl mirrors)
- **Assembly Level**: All assemblies listed in the registry
- **Taxon Focus**: All assemblies with valid NCBI accessions in the registry

## Output Files

The system generates four main annotation files in the `data/` directory. Rows are written in **git-friendly order**: keys that already existed in the TSV keep their previous **line order**, and **new** assemblies are appended at the **end** (sorted by `assembly_accession`, then by the row’s primary key). That way a typical commit shows in-place line edits for updates, new lines at the bottom for additions, and removed lines for deletions.

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
- `release_date`: Date when the annotation was released (from NCBI/Ensembl metadata)
- `retrieval_date`: Date when the mirror last **successfully probed** the annotation URL (HTTP Last-Modified and/or MD5). Rows with a `retrieval_date` within the last **14 days** skip FTP re-probes until that window expires; appearing in the source listing alone does not refresh this field.
- `pipeline_name`: Name of the annotation pipeline if any
- `pipeline_method`: Method used for annotation if any
- `pipeline_version`: Version of the annotation pipeline if any
- `last_modified_date`: Last modification date of the file
- `md5_checksum`:  MD5 checksum of the uncompressed file for integrity verification

### 2. `genbank_annotations.tsv`
Contains NCBI GenBank annotations with the same structure as RefSeq annotations.

### 3. `ensembl_annotations.tsv`
Contains Ensembl annotations with the same structure as NCBI annotations.

### 4. `community_annotations.tsv`
Contains community registry annotations with the same structure as the other TSVs. Rows are keyed by `access_url` (like Ensembl). `source_database` is `"CommunityRegistry"`; `annotation_provider` comes from each project's `manifest.yaml`; `pipeline_name` is the registry project folder name (e.g. `TOGA2`). `release_date` is frozen on first successful import from the GFF file's HTTP `Last-Modified`; `last_modified_date` is refreshed on each mirror run.

## Automated Workflows

The repository includes GitHub Actions workflows that automatically maintain the annotation files:

### Scheduled Workflows
- **Ensembl**: Runs every Friday at 12 AM UTC
- **NCBI RefSeq**: Runs every Friday at 2 AM UTC  
- **NCBI GenBank**: Runs every Friday at 4 AM UTC
- **CommunityRegistry**: Runs every Friday at 6 AM UTC (checks out `guigolab/annotrieve-registry` and mirrors all projects except `sample_project`)

## Development

### Unit tests

From the repository root (stdlib `unittest` only):

```bash
python3 -m unittest discover -s tests -p 'test_*.py' -v
```

Optional: install [pytest](https://pytest.org/) and run `pytest tests/` if you prefer.

### Smoke-testing a mirror locally

Requires the [NCBI `datasets` CLI](https://www.ncbi.nlm.nih.gov/datasets/docs/v2/command-line-tools/) on `PATH` and network access. From `providers/`:

```bash
cd providers
python ncbi.py genbank
python ncbi.py refseq
python ensembl.py
python registry.py
```

Use the same environment variables as CI (`TAXON_ID`, output paths) if you override defaults. For the community provider, also set `REGISTRY_ROOT` to a checkout of [annotrieve-registry](https://github.com/guigolab/annotrieve-registry) (default: `../annotrieve-registry`):

```bash
cd providers
REGISTRY_ROOT=../annotrieve-registry \
OUTPUT_FILE=../data/community_annotations.tsv \
python registry.py
```

