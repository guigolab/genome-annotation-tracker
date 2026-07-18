"""
Mirror community annotations from annotrieve-registry project folders.
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path

import yaml

from tools import async_ops, file_handler, pipeline

REGISTRY_ROOT = os.getenv("REGISTRY_ROOT", "../annotrieve-registry")
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "data/community_annotations.tsv")
KEY_COLUMN = "access_url"
EXCLUDED_PROJECTS = frozenset({"sample_project"})
REQUIRED_TSV_HEADER = "assembly_accession\taccess_url"
DATASETS_BATCH_SIZE = 2000
DATASETS_ATTEMPTS = 3
DATASETS_TIMEOUT = 300


def mirror_registry_annotations() -> None:
    existing, _ = file_handler.load_annotations_ordered(OUTPUT_FILE, KEY_COLUMN)

    def load_universe() -> dict[str, dict]:
        parsed = scan_registry(REGISTRY_ROOT)
        for key, row in parsed.items():
            if key in existing and existing[key].get("release_date"):
                row["release_date"] = existing[key]["release_date"]
        return parsed

    def probe_md5(
        tuples: list[tuple[str, str]], concurrency: int, parsed: dict[str, dict]
    ) -> list[async_ops.ProbeResult]:
        return asyncio.run(async_ops.stream_md5_checksum_many(tuples, concurrency))

    stats_path = os.getenv(
        "MIRROR_STATS_FILE",
        os.path.join(os.path.dirname(OUTPUT_FILE), ".mirror_stats_community.json"),
    )
    outcomes_path = os.getenv(
        "MIRROR_OUTCOMES_FILE",
        os.path.join(os.path.dirname(OUTPUT_FILE), ".mirror_outcomes_community.json"),
    )

    pipeline.run_mirror(
        output_file=OUTPUT_FILE,
        key_column=KEY_COLUMN,
        load_universe=load_universe,
        probe_md5=probe_md5,
        source_label="community",
        stats_path=stats_path,
        outcomes_path=outcomes_path,
    )
    backfill_release_dates(OUTPUT_FILE)


def discover_projects(registry_root: str | Path) -> list[Path]:
    """Return top-level project dirs that contain manifest.yaml and annotations.tsv."""
    root = Path(registry_root)
    if not root.is_dir():
        raise FileNotFoundError(f"Registry root not found: {root}")

    projects: list[Path] = []
    for entry in sorted(root.iterdir()):
        if not entry.is_dir():
            continue
        if entry.name in EXCLUDED_PROJECTS or entry.name.startswith("."):
            continue
        manifest = entry / "manifest.yaml"
        annotations = entry / "annotations.tsv"
        if manifest.is_file() and annotations.is_file():
            projects.append(entry)
    return projects


def load_manifest(manifest_path: Path) -> dict:
    with open(manifest_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Invalid manifest (expected mapping): {manifest_path}")
    return data


def parse_annotations_tsv(tsv_path: Path) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    with open(tsv_path, encoding="utf-8") as f:
        lines = f.read().splitlines()
    if not lines:
        return rows
    if lines[0] != REQUIRED_TSV_HEADER:
        raise ValueError(
            f"Invalid TSV header in {tsv_path}: expected {REQUIRED_TSV_HEADER!r}"
        )
    for line_no, line in enumerate(lines[1:], start=2):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) != 2:
            raise ValueError(f"{tsv_path}:{line_no}: expected 2 tab-separated columns")
        accession, url = parts[0].strip(), parts[1].strip()
        if not accession or not url:
            raise ValueError(f"{tsv_path}:{line_no}: empty accession or URL")
        rows.append((accession, url))
    return rows


def fetch_assembly_metadata(accessions: list[str]) -> dict[str, dict]:
    """Batch NCBI datasets lookup: assembly_name, taxon_id, organism_name per accession."""
    unique = sorted(set(accessions))
    if not unique:
        return {}

    metadata: dict[str, dict] = {}
    for i in range(0, len(unique), DATASETS_BATCH_SIZE):
        batch = unique[i : i + DATASETS_BATCH_SIZE]
        batch_meta = _fetch_assembly_metadata_batch(batch)
        metadata.update(batch_meta)
    return metadata


def _fetch_assembly_metadata_batch(batch: list[str]) -> dict[str, dict]:
    batch_set = set(batch)
    last_err: Exception | None = None

    for attempt in range(DATASETS_ATTEMPTS):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", prefix="gat_acc_", delete=False
        ) as fh:
            fh.writelines(acc + "\n" for acc in batch)
            acc_file = fh.name

        try:
            proc = subprocess.run(
                [
                    "datasets",
                    "summary",
                    "genome",
                    "accession",
                    "--inputfile",
                    acc_file,
                    "--as-json-lines",
                ],
                capture_output=True,
                text=True,
                timeout=DATASETS_TIMEOUT,
                check=False,
            )
        except subprocess.TimeoutExpired as e:
            last_err = e
            if attempt < DATASETS_ATTEMPTS - 1:
                time.sleep(min(2**attempt, 30))
            continue
        finally:
            Path(acc_file).unlink(missing_ok=True)

        if proc.returncode != 0:
            last_err = RuntimeError(
                f"datasets exited {proc.returncode}: {(proc.stderr or proc.stdout)[:500]}"
            )
            if attempt < DATASETS_ATTEMPTS - 1:
                time.sleep(min(2**attempt, 30))
            continue

        found: dict[str, dict] = {}
        for line in proc.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            acc = obj.get("accession") or obj.get("assembly_accession", "")
            if not acc or acc not in batch_set:
                continue
            organism = obj.get("organism", {})
            assembly_info = obj.get("assembly_info", {})
            found[acc] = {
                "assembly_name": assembly_info.get("assembly_name"),
                "taxon_id": organism.get("tax_id"),
                "organism_name": organism.get("organism_name"),
            }
        return found

    raise RuntimeError(f"Failed to fetch assembly metadata: {last_err}")


def build_row(
    *,
    assembly_accession: str,
    access_url: str,
    project_name: str,
    manifest: dict,
    assembly_meta: dict | None,
) -> dict:
    meta = assembly_meta or {}
    return {
        "assembly_accession": assembly_accession,
        "assembly_name": meta.get("assembly_name"),
        "taxon_id": meta.get("taxon_id"),
        "organism_name": meta.get("organism_name"),
        "source_database": "CommunityRegistry",
        "annotation_provider": manifest.get("provider_name"),
        "access_url": access_url,
        "file_format": "gff",
        "release_date": None,
        "pipeline_name": project_name,
        "pipeline_method": manifest.get("pipeline_method"),
        "pipeline_version": manifest.get("pipeline_version"),
    }


def scan_registry(registry_root: str | Path) -> dict[str, dict]:
    projects = discover_projects(registry_root)
    if not projects:
        print(f"[community] No registry projects found under {registry_root}")
        return {}

    parsed: dict[str, dict] = {}
    all_accessions: list[str] = []
    pending_rows: list[tuple[str, str, str, dict]] = []

    for project_dir in projects:
        project_name = project_dir.name
        manifest = load_manifest(project_dir / "manifest.yaml")
        tsv_rows = parse_annotations_tsv(project_dir / "annotations.tsv")
        print(f"[community] Project {project_name}: {len(tsv_rows)} rows")
        for accession, url in tsv_rows:
            if url in parsed:
                print(
                    f"[community] Warning: duplicate access_url {url!r} "
                    f"(skipping {project_name}/{accession})"
                )
                continue
            all_accessions.append(accession)
            pending_rows.append((accession, url, project_name, manifest))

    print(f"[community] Fetching NCBI metadata for {len(set(all_accessions))} assemblies...")
    metadata = fetch_assembly_metadata(all_accessions)

    for accession, url, project_name, manifest in pending_rows:
        parsed[url] = build_row(
            assembly_accession=accession,
            access_url=url,
            project_name=project_name,
            manifest=manifest,
            assembly_meta=metadata.get(accession),
        )

    return parsed


def backfill_release_dates(output_file: str) -> None:
    """Set release_date from last_modified_date for rows missing release_date."""
    rows_dict, key_order = file_handler.load_annotations_ordered(output_file, KEY_COLUMN)
    if not rows_dict:
        return

    changed = False
    ordered_rows: list[dict] = []
    for key in key_order:
        row = dict(rows_dict[key])
        if not row.get("release_date") and row.get("last_modified_date"):
            row["release_date"] = row["last_modified_date"]
            changed = True
        ordered_rows.append(row)

    if not changed:
        return

    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, ordered_rows[0].keys(), delimiter="\t")
        writer.writeheader()
        writer.writerows(ordered_rows)
    print(f"[community] Backfilled release_date for {output_file}")


if __name__ == "__main__":
    print("Starting mirror process for community registry annotations...")
    mirror_registry_annotations()
    print("Mirror process completed for community")
