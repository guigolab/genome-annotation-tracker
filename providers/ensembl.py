import asyncio
import requests
import os
import subprocess
import json
from tools import file_handler, helper, async_ops
from datetime import datetime

TAXON_ID = os.getenv("TAXON_ID", "2759")
ENSEMBL_FTP_DIR = "https://ftp.ebi.ac.uk/pub/ensemblorganisms"
SPECIES_URL= f"{ENSEMBL_FTP_DIR}/species.json"
TMP_DIR = "tmp"
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "data/ensembl_annotations.tsv")

def mirror_ensembl_annotations():
    """
    Mirror the ensembl genome annotations
    """
    accessions = fetch_eukaryotic_genomes()
    species_path = fetch_ensembl_species()
    existing_annotations_dict = file_handler.load_annotations(OUTPUT_FILE, "access_url")
    parsed_annotations_dict = parse_annotations(species_path, accessions)

    annotations_to_keep = helper.keep_recent_annotations(existing_annotations_dict, parsed_annotations_dict)
    last_modified_dates = asyncio.run(
                                        async_ops.check_last_modified_date_many(
                                            helper.get_tuples_to_check(annotations_to_keep, parsed_annotations_dict), 20)
                                    )
    annotations_to_keep.extend(helper.handle_last_modified_date(existing_annotations_dict, parsed_annotations_dict, last_modified_dates))
    md5_checksums_tuples = asyncio.run(
                                        async_ops.stream_md5_checksum_many(
                                            helper.get_tuples_to_check(annotations_to_keep, parsed_annotations_dict), 20)
                                    )
    annotations_to_keep.extend(helper.handle_md5_checksum(existing_annotations_dict, parsed_annotations_dict, md5_checksums_tuples))
    
    merged_annotations = helper.merge_annotations(existing_annotations_dict, parsed_annotations_dict, annotations_to_keep)
    print(f"Merged {len(merged_annotations)} annotations")
    
    file_handler.write_annotations(merged_annotations, OUTPUT_FILE)
    print(f"Written {len(merged_annotations)} annotations to {OUTPUT_FILE}")

def fetch_eukaryotic_genomes()->list[str]:
    """
    Fetch the eukaryotic genomes, chromosomes and complete level, accessions from ncbi
    """
    cmd = [
        "datasets",
        "summary",
        "genome",
        "taxon",
        TAXON_ID,
        "--assembly-level",
        "chromosome,complete",
        "--report",
        "ids_only",
        "--as-json-lines",
    ]
    accessions = []
    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as proc:
        for line in proc.stdout:
            try:
                #create a dict from the line with accession as key
                accession = json.loads(line)["accession"]
                accessions.append(accession)
            except Exception as e:
                print(f"Error parsing line: {line}")
                print(e)
                continue
    return accessions

def parse_annotations(species_path: str, accessions: list[str]) -> dict:
    """
    Parse the annotations from the species json returns a dict with access url as key and the parsed annotations as the value
    """
    parsed_annotations_dict = {}
    
    with open(species_path, "r") as f:
        species_data = json.load(f).get("species", {})
        
        for species_info in species_data.values():
            species_annotations = _parse_species_annotations(species_info, accessions)
            parsed_annotations_dict.update(species_annotations)
    return parsed_annotations_dict

def _parse_species_annotations(species_info: dict, accessions: list[str]) -> dict:
    """
    Parse annotations for a single species.
    """
    annotations = {}
    taxon_id = species_info.get("taxid")
    organism_name = species_info.get("scientific_name")
    assemblies = species_info.get('assemblies', {})
    
    for assembly_accession, assembly_data in assemblies.items():
        if assembly_accession in accessions:
            assembly_annotations = _parse_assembly_annotations(
                assembly_accession, assembly_data, taxon_id, organism_name
            )
            annotations.update(assembly_annotations)
    
    return annotations

def _parse_assembly_annotations(
    assembly_accession: str, 
    assembly_data: dict, 
    taxon_id: str, 
    organism_name: str
) -> dict:
    """
    Parse annotations for a single assembly.
    """
    annotations = {}
    assembly_name = assembly_data.get("name")
    genebuild_providers = assembly_data.get("genebuild_providers", {})
    
    for provider_name, provider_data in genebuild_providers.items():
        if _should_skip_provider(provider_name):
            continue
            
        provider_annotations = _parse_provider_annotations(
            provider_name, provider_data, assembly_accession, 
            assembly_name, taxon_id, organism_name
        )
        annotations.update(provider_annotations)
    
    return annotations

def _should_skip_provider(provider_name: str) -> bool:
    """
    Check if a provider should be skipped.
    """
    return provider_name in ["genbank", "refseq"]

def _parse_provider_annotations(
    provider_name: str,
    provider_data: dict,
    assembly_accession: str,
    assembly_name: str,
    taxon_id: str,
    organism_name: str
) -> dict:
    """
    Parse annotations for a single provider.
    """
    annotations = {}
    pipeline_name = _get_pipeline_name(provider_name)
    
    for info in provider_data.values():
        annotation = _create_annotation(
            info, provider_name, pipeline_name, assembly_accession,
            assembly_name, taxon_id, organism_name
        )
        if annotation:
            annotations[annotation["access_url"]] = annotation
    
    return annotations

def _get_pipeline_name(provider_name: str) -> str:
    """
    Get the pipeline name for a provider.
    """
    pipeline_mapping = {
        "ensembl": "Ensembl Genebuild",
        "braker": "BRAKER"
    }
    return pipeline_mapping.get(provider_name)

def _create_annotation(
    info: dict,
    provider_name: str,
    pipeline_name: str,
    assembly_accession: str,
    assembly_name: str,
    taxon_id: str,
    organism_name: str
) -> dict:
    """
    Create a single annotation dictionary.
    """
    release_date = format_release_date(info.get("release"))
    retrieval_date = datetime.now().isoformat().split("T")[0]
    
    # Get the file path
    sub_path = info.get('paths', {}).get('genebuild', {}).get('files', {}).get('annotations', {}).get('genes.gff3.gz')
    if not sub_path:
        return None
        
    access_url = f"{ENSEMBL_FTP_DIR}/{sub_path}"
    
    return {
        "assembly_accession": assembly_accession,
        "assembly_name": assembly_name,
        "taxon_id": taxon_id,
        "organism_name": organism_name,
        "source_database": "Ensembl",
        "annotation_provider": provider_name,
        "access_url": access_url,
        "file_format": "gff",
        "release_date": release_date,
        "retrieval_date": retrieval_date,
        "pipeline_name": pipeline_name,
        "pipeline_method": None,
        "pipeline_version": None,
    }



def format_release_date(release_date: str) -> str:
    """
    Format the release date from YYYY_MM to YYYY-MM-DD
    """
    return f"{release_date.split('_')[0]}-{release_date.split('_')[1]}-01"

def fetch_ensembl_species():
    """
    Fetch the ensembl species json
    """
    response = requests.get(SPECIES_URL)
    #store the json in the tmp directory
    os.makedirs(TMP_DIR, exist_ok=True)
    path = os.path.join(TMP_DIR, "species.json")
    with open(path, "w") as f:
        f.write(response.text)
    return path


if __name__ == "__main__":
    print(f"Starting mirror process for ensembl annotations...")
    mirror_ensembl_annotations()
    print(f"Mirror process completed for ensembl")