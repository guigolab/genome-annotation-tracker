import asyncio
import aiohttp
import json
import re
import subprocess
import os
import argparse
import time
from tools import file_handler, async_ops, helper, pipeline
TAXON_ID = os.getenv("TAXON_ID", "2759")
GENBANK_OUTPUT_FILE = os.getenv("GENBANK_OUTPUT_FILE", "data/genbank_annotations.tsv")
REFSEQ_OUTPUT_FILE = os.getenv("REFSEQ_OUTPUT_FILE", "data/refseq_annotations.tsv")
DATASETS_ATTEMPTS = 3

NCBI_MAPPER = {
    "genbank": {
        "output_file": GENBANK_OUTPUT_FILE,
        "db_name": "GenBank",
    },
    "refseq": {
        "output_file": REFSEQ_OUTPUT_FILE,
        "db_name": "RefSeq",
    },
}


def mirror_ncbi_annotations(db_source: str) -> None:
    db_map = NCBI_MAPPER.get(db_source)
    if not db_map:
        raise ValueError(
            f"{db_source} is not a valid database source, must be one of {NCBI_MAPPER.keys()}"
        )

    def load_universe() -> dict[str, dict]:
        return fetch_and_parse_ncbi_annotated_assemblies(TAXON_ID, db_map["db_name"])

    def probe_md5(
        tuples: list[tuple[str, str]], concurrency: int, parsed: dict[str, dict]
    ) -> list[async_ops.ProbeResult]:
        return asyncio.run(_probe_ncbi_md5_many(tuples, concurrency, parsed))

    stats_path = os.getenv(
        "MIRROR_STATS_FILE",
        os.path.join(os.path.dirname(db_map["output_file"]), f".mirror_stats_{db_source}.json"),
    )
    outcomes_path = os.getenv(
        "MIRROR_OUTCOMES_FILE",
        os.path.join(os.path.dirname(db_map["output_file"]), f".mirror_outcomes_{db_source}.json"),
    )

    pipeline.run_mirror(
        output_file=db_map["output_file"],
        key_column="assembly_accession",
        load_universe=load_universe,
        probe_md5=probe_md5,
        source_label=db_source,
        stats_path=stats_path,
        outcomes_path=outcomes_path,
    )


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
    last_err: Exception | None = None
    for attempt in range(DATASETS_ATTEMPTS):
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                raise RuntimeError(
                    f"datasets exited {proc.returncode}: {proc.stderr[:500]}"
                )
            parsed: dict[str, dict] = {}
            for line in proc.stdout.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    parsed_annotation = parse_json_line(json.loads(line), db_source)
                    parsed[parsed_annotation["assembly_accession"]] = parsed_annotation
                except Exception as e:
                    print(f"Error parsing line: {line[:120]}... {e}")
            if parsed:
                return parsed
            raise RuntimeError("datasets returned zero assemblies")
        except Exception as e:
            last_err = e
            if attempt < DATASETS_ATTEMPTS - 1:
                time.sleep(min(2**attempt, 30))
    raise RuntimeError(f"Failed to fetch NCBI assemblies after {DATASETS_ATTEMPTS} attempts: {last_err}")


def create_ftp_path(accession: str, assembly_name: str) -> str:
    assembly_name = assembly_name.replace(" ", "_")
    return (
        f"https://ftp.ncbi.nlm.nih.gov/genomes/all/"
        f"{accession[0:3]}/{accession[4:7]}/{accession[7:10]}/{accession[10:13]}/"
        f"{accession}_{assembly_name}/{accession}_{assembly_name}_genomic.gff.gz"
    )


def get_minimal_ftp_path(ftp_path: str) -> str:
    parts = ftp_path.rstrip("/").split("/")
    if len(parts) < 10:
        return ftp_path
    return "/".join(parts[:9]) + "/"


async def _scrape_ftp_directory_listing(session: aiohttp.ClientSession, url: str) -> list[str]:
    result = await async_ops.request_with_retry(session, "GET", url)
    if result is None:
        return []
    status, _ = result
    if status >= 400:
        return []
    try:
        async with session.get(url, allow_redirects=True) as resp:
            content = await resp.text()
    except Exception:
        return []
    dirs = re.findall(r'href="([^"]+/)"', content)
    out = []
    for d in dirs:
        name = d.rstrip("/").split("/")[-1]
        if name and name not in (".", ".."):
            out.append(name)
    return out


async def resolve_and_fetch_md5(
    session: aiohttp.ClientSession, ftp_path: str, accession: str
) -> tuple[str | None, str | None]:
    minimal = get_minimal_ftp_path(ftp_path)
    dirs = await _scrape_ftp_directory_listing(session, minimal)
    if not dirs:
        return None, None
    candidates = [d for d in dirs if d == accession or d.startswith(accession + "_")]
    if not candidates:
        candidates = dirs
    for dirname in candidates:
        base = f"{minimal}{dirname}/"
        checksums_url = f"{base}uncompressed_checksums.txt"
        text_result = await async_ops.fetch_url_text(session, checksums_url, accession)
        if text_result.status != "ok" or not text_result.value:
            continue
        for line in text_result.value.split("\n"):
            if not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) >= 2 and "genomic.gff" in parts[0]:
                gff_name = parts[0].strip().lstrip("./")
                if gff_name.endswith(".gff") and not gff_name.endswith(".gff.gz"):
                    gff_name += ".gz"
                return parts[1].strip(), f"{base}{gff_name}"
    return None, None


def parse_json_line(line: dict, db_source: str) -> dict:
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
        "pipeline_name": annotation_info.get("pipeline"),
        "pipeline_method": annotation_info.get("method"),
        "pipeline_version": annotation_info.get("software_version"),
    }


async def _fetch_md5_from_checksums_file(
    session: aiohttp.ClientSession, ftp_path: str, key: str
) -> async_ops.ProbeResult:
    base_path = ftp_path.rsplit("/", 1)[0]
    url = f"{base_path}/uncompressed_checksums.txt"
    text_result = await async_ops.fetch_url_text(session, url, key)
    if text_result.status == "not_found":
        return async_ops.ProbeResult(key=key, status="not_found", detail=text_result.detail)
    if text_result.status != "ok" or not text_result.value:
        return async_ops.ProbeResult(
            key=key, status="transient_error", detail=text_result.detail or "checksums_fetch_failed"
        )
    for line in text_result.value.split("\n"):
        if not line.strip():
            continue
        splitted = line.split("\t")
        if "genomic.gff" in splitted[0] and len(splitted) >= 2:
            return async_ops.ProbeResult(
                key=key, status="ok", value=splitted[1].strip(), detail="checksums_file"
            )
    return async_ops.ProbeResult(key=key, status="transient_error", detail="no_gff_in_checksums")


async def _probe_ncbi_md5_one(
    session: aiohttp.ClientSession, url: str, key: str, parsed_updates: dict
) -> async_ops.ProbeResult:
    result = await _fetch_md5_from_checksums_file(session, url, key)
    if result.status == "ok":
        return result
    if result.status == "not_found":
        md5, resolved_url = await resolve_and_fetch_md5(session, url, key)
        if md5:
            if key in parsed_updates and resolved_url:
                parsed_updates[key]["access_url"] = resolved_url
            return async_ops.ProbeResult(key=key, status="ok", value=md5, detail="ftp_scraper")
        return async_ops.ProbeResult(key=key, status="not_found", detail="scraper_not_found")
    # transient — try scraper before giving up
    md5, resolved_url = await resolve_and_fetch_md5(session, url, key)
    if md5:
        if key in parsed_updates and resolved_url:
            parsed_updates[key]["access_url"] = resolved_url
        return async_ops.ProbeResult(key=key, status="ok", value=md5, detail="ftp_scraper_fallback")
    return result


async def _probe_ncbi_md5_many(
    tuples: list[tuple[str, str]], concurrency: int, parsed: dict[str, dict]
) -> list[async_ops.ProbeResult]:
    async def bound(session: aiohttp.ClientSession, url: str, key: str) -> async_ops.ProbeResult:
        return await _probe_ncbi_md5_one(session, url, key, parsed)

    return await async_ops.probe_many(tuples, bound, concurrency)


def apply_parsed_updates(parsed: dict[str, dict], updates: dict[str, dict]) -> None:
    for k, v in updates.items():
        if k in parsed and "access_url" in v:
            parsed[k]["access_url"] = v["access_url"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mirror NCBI genome annotations")
    parser.add_argument(
        "db_source",
        type=str,
        choices=list(NCBI_MAPPER.keys()),
        help="Database source: 'genbank' or 'refseq'",
    )
    args = parser.parse_args()
    print(f"Starting mirror process for {args.db_source} annotations...")
    mirror_ncbi_annotations(args.db_source)
    print(f"Mirror process completed for {args.db_source}")
