import asyncio
import aiohttp
import json
import subprocess
import os
import argparse
from tools import file_handler, async_ops, helper
from datetime import datetime
# Environment variables
TAXON_ID = os.getenv("TAXON_ID", "2759")
GENBANK_OUTPUT_FILE = os.getenv("GENBANK_OUTPUT_FILE", "data/genbank_annotations.tsv")
REFSEQ_OUTPUT_FILE = os.getenv("REFSEQ_OUTPUT_FILE", "data/refseq_annotations.tsv")

NCBI_MAPPER = {
    "genbank":{
        "output_file": GENBANK_OUTPUT_FILE,
        "db_name": "GenBank",
    },
    "refseq":{
        "output_file": REFSEQ_OUTPUT_FILE,
        "db_name": "RefSeq",
    },
}

def mirror_ncbi_annotations(db_source: str) -> list[dict]:

    db_map = NCBI_MAPPER.get(db_source)
    if not db_map:
        print(f"Error: {db_source} is not a valid database source, must be one of {NCBI_MAPPER.keys()}")
        return

    existing_annotations_dict = file_handler.load_annotations(db_map["output_file"])
    print(f"Found {len(existing_annotations_dict)} existing annotations")

    parsed_annotations_dict = fetch_and_parse_ncbi_annotated_assemblies(TAXON_ID, db_map["db_name"])
    if not parsed_annotations_dict:
        print("Error fetching genbank annotated assemblies")
        return
    print(f"Found {len(parsed_annotations_dict)} parsed annotations")

    annotations_to_keep = helper.keep_recent_annotations(existing_annotations_dict, parsed_annotations_dict)
    print(f"Found {len(annotations_to_keep)} annotations to keep")

    last_modified_dates = asyncio.run(
                                        async_ops.check_last_modified_date_many(
                                                helper.get_tuples_to_check(annotations_to_keep, parsed_annotations_dict), 20)
                                    )
    annotations_to_keep.extend(helper.handle_last_modified_date(existing_annotations_dict, parsed_annotations_dict, last_modified_dates))
    print(f"Found {len(annotations_to_keep)} annotations to keep after checking last modified dates")

    md5_checksums_tuples = asyncio.run(
                                        fetch_md5_checksum_many(
                                            helper.get_tuples_to_check(annotations_to_keep, parsed_annotations_dict), 20)
                                        )
    annotations_to_keep.extend(helper.handle_md5_checksum(existing_annotations_dict, parsed_annotations_dict, md5_checksums_tuples))
    print(f"Found {len(annotations_to_keep)} annotations to keep after checking md5 checksums")

    merged_annotations = helper.merge_annotations(existing_annotations_dict, parsed_annotations_dict, annotations_to_keep)
    print(f"Merged {len(merged_annotations)} annotations")
    
    file_handler.write_annotations(merged_annotations, db_map["output_file"])
    print(f"Written {len(merged_annotations)} annotations to {db_map['output_file']}")

def fetch_and_parse_ncbi_annotated_assemblies(taxon_id: str, db_source: str) -> dict[str, dict]:
    cmd = [
        "datasets",
        "summary",
        "genome",
        "taxon",
        taxon_id,
        "--annotated",
        "--assembly-level",
        "chromosome,complete",
        "--assembly-source",
        db_source,
        "--as-json-lines",
    ]
    #stream the cmd output and parse each line
    parsed_annotations_dict = {}
    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as proc:
        for line in proc.stdout:
            try:
                #create a dict from the line with accession as key
                parsed_annotation = parse_json_line(json.loads(line), db_source)  # Added db_source parameter
                parsed_annotations_dict[parsed_annotation["assembly_accession"]] = parsed_annotation
            except Exception as e:
                print(f"Error parsing line: {line}")
                print(e)
                continue
    return parsed_annotations_dict


def create_ftp_path(accession: str, assembly_name: str) -> str:
    return f"https://ftp.ncbi.nlm.nih.gov/genomes/all/{accession[0:3]}/{accession[4:7]}/{accession[7:10]}/{accession[10:13]}/{accession}_{assembly_name}/{accession}_{assembly_name}_genomic.gff.gz"


def parse_json_line(line:dict, db_source: str) -> dict:
    organism_info = line.get("organism", {})
    annotation_info = line.get("annotation_info", {})
    assembly_info = line.get("assembly_info", {})
    access_url = create_ftp_path(line["accession"], assembly_info.get("assembly_name"))
    return {
        "assembly_accession": line["accession"],
        "assembly_name": assembly_info.get("assembly_name"),
        "taxon_id": organism_info.get("tax_id"),
        "organism_name": organism_info.get("organism_name"),
        "source_database": db_source,
        "annotation_provider": annotation_info.get("provider"),
        "access_url": access_url,
        "file_format": "gff",
        "release_date": annotation_info.get("release_date"),
        "retrieval_date": datetime.now().isoformat().split("T")[0],
        "pipeline_name": annotation_info.get("pipeline"),
        "pipeline_method": annotation_info.get("method"),
        "pipeline_version": annotation_info.get("software_version"),
    }


async def fetch_md5_checksum(session: aiohttp.ClientSession, ftp_path: str) -> str:
    """
    This function fetches the uncompressed MD5 checksum of the annotation file from ncbi ftp server.
    """
    base_path = ftp_path.rsplit("/", 1)[0]
    url = f"{base_path}/uncompressed_checksums.txt"
    try:
        async with session.get(url, timeout=60) as r:
            r.raise_for_status()
            content = await r.text()
            lines = content.split('\n')
            for line in lines:
                if not line.strip():  # Skip empty lines
                    continue
                splitted = line.split("\t")
                if 'genomic.gff' in splitted[0]:
                    return splitted[1].strip()
    except Exception:
        pass
    return None


async def fetch_md5_checksum_many(tuples: list[tuple[str, str]], concurrency: int = 20) -> list[tuple[str, str]]:
    """
    Fetch the MD5 checksum of multiple annotation files from ncbi ftp server.
    """
    return await async_ops.fetch_data_many(tuples, fetch_md5_checksum, concurrency)

if __name__ == "__main__":
    #get args from command line
    parser = argparse.ArgumentParser(description="Mirror NCBI genome annotations")
    parser.add_argument("db_source", type=str, choices=list(NCBI_MAPPER.keys()), 
                       help="Database source: 'genbank' or 'refseq'")
    args = parser.parse_args()
    
    print(f"Starting mirror process for {args.db_source} annotations...")
    result = mirror_ncbi_annotations(args.db_source)
    print(f"Mirror process completed for {args.db_source}")