---

# Genome Annotation Tracker

The **Genome Annotation Tracker** is a tool designed to track and map eukaryotic genome annotations from both the NCBI and Ensembl databases. It provides a method to retrieve and organize genome assembly annotations, making it easier to monitor and analyze genomic data from different sources. This repository automates the collection and mapping of genome annotation data and includes important files containing genome assembly information and mappings.

## Table of Contents

- [Overview](#overview)
- [Files](#files)
- [Pipeline](#pipeline)
- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Overview

This repository tracks eukaryotic genome annotations from two major sources:

- **NCBI**: The National Center for Biotechnology Information, which provides eukaryotic genome assemblies at the chromosome or complete genome level.
- **Ensembl**: The Ensembl Rapid Release server, which provides genome assemblies and annotations from species across the tree of life.

The project merges genome annotations from both NCBI and Ensembl, providing users with a comprehensive table containing the species information and the NCBI taxonomic identifier for each genome.

## Files

The repository contains three key TSV (Tab-Separated Values) files:

1. **ensembl_rapid_release.tsv**:
   - Contains genome assembly accessions and full paths to the corresponding genome annotations from the Ensembl Rapid Release server.
   - Columns:
     - `accession`: The Ensembl assembly accession.
     - `full_path`: The URL path to the annotation file on the Ensembl FTP server.

2. **ncbi.tsv**:
   - Contains all eukaryotic genome assemblies from NCBI with assembly levels of chromosome or complete genome.
   - Columns:
     - `accession`: The NCBI assembly accession.
     - `full_path`: The URL path to the annotation file on the NCBI FTP server.

3. **mapped_annotations.tsv**:
   - This file results from mapping the genome annotations between Ensembl and NCBI. It contains genome assembly annotations from both sources along with species and taxonomic information.
   - Columns:
     - `annotation_name`: The name of the annotation file.
     - `full_path`: The full path to the genome annotation (from either NCBI or Ensembl).
     - `accession`: The NCBI or Ensembl assembly accession.
     - `organism-name`: The species name.
     - `organism-tax-id`: The NCBI taxonomic identifier (TaxID).

## Installation

To run the Genome Annotation Tracker locally, ensure you have the following dependencies installed:

- `curl`: For fetching data from the NCBI and Ensembl servers.
- `awk`: Used for processing and filtering data in the TSV files.

1. Clone this repository:

```bash
git clone https://github.com/yourusername/genome-annotation-tracker.git
cd genome-annotation-tracker
```

## Usage

Once you have cloned the repository activate github actions and run the data retrieval and mapping pipeline. 

For example, to retrieve genome assemblies and generate the `mapped_annotations.tsv`, you can follow these steps:

1. **Retrieve NCBI genome assembly information**:

   Run the provided scripts or workflows to gather the latest eukaryotic genome annotations from NCBI.

2. **Retrieve Ensembl genome assembly information**:

   Use the Ensembl Rapid Release data provided in `ensembl_rapid_release.tsv`.

3. **Map the annotations**:

   Merge the two datasets into `mapped_annotations.tsv` by mapping species and taxonomy information.

## Contributing

Contributions are welcome! If you want to add features or fix bugs, please submit a pull request.

---
