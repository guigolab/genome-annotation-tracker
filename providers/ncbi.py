import asyncio
import aiohttp
import json
import re
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

    existing_annotations_dict = file_handler.load_annotations(db_map["output_file"], "assembly_accession")
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

    tuples_to_check = helper.get_tuples_to_check(annotations_to_keep, parsed_annotations_dict)
    md5_checksums_tuples = asyncio.run(fetch_md5_checksum_many(tuples_to_check, 20))
    # Retry failed paths by scraping the minimal FTP directory to resolve assembly folder name
    success_accessions = {acc for acc, _ in md5_checksums_tuples}
    failed_tuples = [(url, acc) for url, acc in tuples_to_check if acc not in success_accessions]
    if failed_tuples:
        scraper_results = asyncio.run(fetch_md5_via_scraper_many(failed_tuples, 10))
        for acc, md5, resolved_url in scraper_results:
            parsed_annotations_dict[acc]["md5_checksum"] = md5
            if resolved_url:
                parsed_annotations_dict[acc]["access_url"] = resolved_url
        md5_checksums_tuples += [(acc, md5) for acc, md5, _ in scraper_results]
        # Scraper-resolved URLs were not in the initial last_modified check; fetch it so they pass merge
        resolved_tuples = [(url, acc) for acc, _, url in scraper_results if url]
        if resolved_tuples:
            last_modified_resolved = asyncio.run(
                async_ops.check_last_modified_date_many(resolved_tuples, 10)
            )
            for acc, last_modified in last_modified_resolved:
                if last_modified:
                    parsed_annotations_dict[acc]["last_modified_date"] = last_modified
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
    assembly_name = assembly_name.replace(" ", "_") # replace spaces with underscores to avoid invalid file names, this fix almost all the issues with the file names
    return f"https://ftp.ncbi.nlm.nih.gov/genomes/all/{accession[0:3]}/{accession[4:7]}/{accession[7:10]}/{accession[10:13]}/{accession}_{assembly_name}/{accession}_{assembly_name}_genomic.gff.gz"


def get_minimal_ftp_path(ftp_path: str) -> str:
    """
    Return the FTP path up to the accession split (no assembly folder).
    E.g. .../all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14/file.gz
    -> .../all/GCF/000/001/405/
    """
    parts = ftp_path.rstrip("/").split("/")
    # Path: [protocol, '', host, 'genomes', 'all', p0, p1, p2, p3, assembly_dir, ...]
    # We want up to and including p3 (index 8 in 0-based).
    if len(parts) < 10:
        return ftp_path
    minimal = "/".join(parts[:9]) + "/"
    return minimal


async def _scrape_ftp_directory_listing(session: aiohttp.ClientSession, url: str) -> list[str]:
    """
    Fetch FTP directory listing (HTML) and return list of subdirectory names
    (links ending with /, excluding . and ..).
    """
    try:
        async with session.get(url, timeout=30) as resp:
            resp.raise_for_status()
            content = await resp.text()
    except Exception:
        return []
    # Match href=".../" - directory links (relative or with path)
    dirs = re.findall(r'href="([^"]+/)"', content)
    result = []
    for d in dirs:
        name = d.rstrip("/").split("/")[-1]
        if name and name not in (".", ".."):
            result.append(name)
    return result


async def resolve_and_fetch_md5(
    session: aiohttp.ClientSession, ftp_path: str, accession: str
) -> tuple[str | None, str | None]:
    """
    When the built FTP path is not found, scrape the minimal path (up to accession split),
    find the actual assembly directory (handles weird characters in assembly name),
    and return (md5_checksum, resolved_gff_url) or (None, None).
    """
    minimal = get_minimal_ftp_path(ftp_path)
    dirs = await _scrape_ftp_directory_listing(session, minimal)
    if not dirs:
        return None, None
    # Prefer directories that match this accession (e.g. GCF_000001405.40_...)
    candidates = [d for d in dirs if d == accession or d.startswith(accession + "_")]
    if not candidates:
        candidates = dirs
    for dirname in candidates:
        base = f"{minimal}{dirname}/"
        checksums_url = f"{base}uncompressed_checksums.txt"
        try:
            async with session.get(checksums_url, timeout=30) as r:
                r.raise_for_status()
                content = await r.text()
        except Exception:
            continue
        for line in content.split("\n"):
            if not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) >= 2 and "genomic.gff" in parts[0]:
                # Filename in checksums may be ./name and is uncompressed (_genomic.gff); server has _genomic.gff.gz
                gff_name = parts[0].strip().lstrip("./")
                if gff_name.endswith(".gff") and not gff_name.endswith(".gff.gz"):
                    gff_name += ".gz"
                resolved_url = f"{base}{gff_name}"
                return parts[1].strip(), resolved_url
    return None, None


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


async def fetch_md5_via_scraper_many(
    failed_tuples: list[tuple[str, str]], concurrency: int = 10
) -> list[tuple[str, str, str | None]]:
    """
    For paths that were not found, scrape the minimal FTP path to discover the real
    assembly directory and fetch MD5. Returns list of (accession, md5, resolved_url).
    """
    results: list[tuple[str, str, str | None]] = []
    sem = asyncio.Semaphore(concurrency)

    async def bound_resolve(session: aiohttp.ClientSession, ftp_path: str, accession: str):
        async with sem:
            md5, resolved_url = await resolve_and_fetch_md5(session, ftp_path, accession)
            if md5:
                results.append((accession, md5, resolved_url))

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            *(bound_resolve(session, ftp_path, accession) for ftp_path, accession in failed_tuples)
        )
    return results

if __name__ == "__main__":
    #get args from command line
    parser = argparse.ArgumentParser(description="Mirror NCBI genome annotations")
    parser.add_argument("db_source", type=str, choices=list(NCBI_MAPPER.keys()), 
                       help="Database source: 'genbank' or 'refseq'")
    args = parser.parse_args()
    
    print(f"Starting mirror process for {args.db_source} annotations...")
    result = mirror_ncbi_annotations(args.db_source)
    print(f"Mirror process completed for {args.db_source}")