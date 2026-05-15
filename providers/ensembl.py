import asyncio
import requests
import os
import subprocess
import json
import time
from tools import file_handler, helper, async_ops, pipeline
from datetime import datetime

TAXON_ID = os.getenv("TAXON_ID", "2759")
ENSEMBL_FTP_DIR = "https://ftp.ebi.ac.uk/pub/ensemblorganisms"
SPECIES_URL = f"{ENSEMBL_FTP_DIR}/species.json"
TMP_DIR = "tmp"
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "data/ensembl_annotations.tsv")
FETCH_ATTEMPTS = 5
DATASETS_ATTEMPTS = 3


def mirror_ensembl_annotations() -> None:
    accessions_holder: list[str] = []

    def load_universe() -> dict[str, dict]:
        nonlocal accessions_holder
        accessions_holder = fetch_eukaryotic_genomes()
        species_path = fetch_ensembl_species()
        return parse_annotations(species_path, accessions_holder)

    def probe_md5(
        tuples: list[tuple[str, str]], concurrency: int, parsed: dict[str, dict]
    ) -> list[async_ops.ProbeResult]:
        return asyncio.run(async_ops.stream_md5_checksum_many(tuples, concurrency))

    stats_path = os.getenv(
        "MIRROR_STATS_FILE",
        os.path.join(os.path.dirname(OUTPUT_FILE), ".mirror_stats_ensembl.json"),
    )
    outcomes_path = os.getenv(
        "MIRROR_OUTCOMES_FILE",
        os.path.join(os.path.dirname(OUTPUT_FILE), ".mirror_outcomes_ensembl.json"),
    )

    pipeline.run_mirror(
        output_file=OUTPUT_FILE,
        key_column="access_url",
        load_universe=load_universe,
        probe_md5=probe_md5,
        source_label="ensembl",
        stats_path=stats_path,
        outcomes_path=outcomes_path,
    )


def fetch_eukaryotic_genomes() -> list[str]:
    cmd = [
        "datasets",
        "summary",
        "genome",
        "taxon",
        TAXON_ID,
        "--report",
        "ids_only",
        "--as-json-lines",
    ]
    last_err: Exception | None = None
    for attempt in range(DATASETS_ATTEMPTS):
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if proc.returncode != 0:
                raise RuntimeError(f"datasets exited {proc.returncode}: {proc.stderr[:500]}")
            accessions = []
            for line in proc.stdout.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    accessions.append(json.loads(line)["accession"])
                except Exception as e:
                    print(f"Error parsing line: {line[:80]}... {e}")
            if accessions:
                return accessions
            raise RuntimeError("datasets returned zero accessions")
        except Exception as e:
            last_err = e
            if attempt < DATASETS_ATTEMPTS - 1:
                time.sleep(min(2**attempt, 30))
    raise RuntimeError(f"Failed to fetch eukaryotic genomes: {last_err}")


def parse_annotations(species_path: str, accessions: list[str]) -> dict:
    accessions_set = set(accessions)
    parsed_annotations_dict: dict = {}
    with open(species_path, "r") as f:
        species_data = json.load(f).get("species", {})
        for species_info in species_data.values():
            species_annotations = _parse_species_annotations(species_info, accessions_set)
            parsed_annotations_dict.update(species_annotations)
    return parsed_annotations_dict


def _parse_species_annotations(species_info: dict, accessions: set[str]) -> dict:
    annotations = {}
    taxon_id = species_info.get("taxid")
    organism_name = species_info.get("scientific_name")
    assemblies = species_info.get("assemblies", {})
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
    organism_name: str,
) -> dict:
    annotations = {}
    assembly_name = assembly_data.get("name")
    genebuild_providers = assembly_data.get("genebuild_providers", {})
    for provider_name, provider_data in genebuild_providers.items():
        if _should_skip_provider(provider_name):
            continue
        provider_annotations = _parse_provider_annotations(
            provider_name,
            provider_data,
            assembly_accession,
            assembly_name,
            taxon_id,
            organism_name,
        )
        annotations.update(provider_annotations)
    return annotations


def _should_skip_provider(provider_name: str) -> bool:
    return provider_name in ["genbank", "refseq"]


def _parse_provider_annotations(
    provider_name: str,
    provider_data: dict,
    assembly_accession: str,
    assembly_name: str,
    taxon_id: str,
    organism_name: str,
) -> dict:
    annotations = {}
    pipeline_name = _get_pipeline_name(provider_name)
    for info in provider_data.values():
        annotation = _create_annotation(
            info,
            provider_name,
            pipeline_name,
            assembly_accession,
            assembly_name,
            taxon_id,
            organism_name,
        )
        if annotation:
            annotations[annotation["access_url"]] = annotation
    return annotations


def _get_pipeline_name(provider_name: str) -> str:
    pipeline_mapping = {"ensembl": "Ensembl Genebuild", "braker": "BRAKER"}
    return pipeline_mapping.get(provider_name)


def _create_annotation(
    info: dict,
    provider_name: str,
    pipeline_name: str,
    assembly_accession: str,
    assembly_name: str,
    taxon_id: str,
    organism_name: str,
) -> dict | None:
    release_date = format_release_date(info.get("release"))
    retrieval_date = datetime.now().isoformat().split("T")[0]
    sub_path = (
        info.get("paths", {})
        .get("genebuild", {})
        .get("files", {})
        .get("annotations", {})
        .get("genes.gff3.gz")
    )
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
    parts = release_date.split("_")
    return f"{parts[0]}-{parts[1]}-01"


def fetch_ensembl_species() -> str:
    last_err: Exception | None = None
    for attempt in range(FETCH_ATTEMPTS):
        try:
            response = requests.get(SPECIES_URL, timeout=60)
            response.raise_for_status()
            data = response.json()
            if not data.get("species"):
                raise RuntimeError("species.json missing 'species' key")
            os.makedirs(TMP_DIR, exist_ok=True)
            path = os.path.join(TMP_DIR, "species.json")
            with open(path, "w") as f:
                json.dump(data, f)
            return path
        except Exception as e:
            last_err = e
            if attempt < FETCH_ATTEMPTS - 1:
                time.sleep(min(2**attempt, 30))
    raise RuntimeError(f"Failed to fetch Ensembl species.json after {FETCH_ATTEMPTS} attempts: {last_err}")


if __name__ == "__main__":
    print("Starting mirror process for ensembl annotations...")
    mirror_ensembl_annotations()
    print("Mirror process completed for ensembl")
