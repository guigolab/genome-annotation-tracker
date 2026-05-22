"""Subtract days from retrieval_date in annotation TSV files."""

from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime, timedelta

# Allow running as script from repo root or providers/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tools import file_handler  # noqa: E402


def backdate_file(path: str, key_column: str, days: int) -> int:
    existing, key_order = file_handler.load_annotations_ordered(path, key_column)
    if not existing:
        print(f"Skip empty or missing: {path}")
        return 0
    delta = timedelta(days=days)
    rows: list[dict] = []
    for key in key_order:
        row = dict(existing[key])
        raw = row.get("retrieval_date", "")
        try:
            d = datetime.strptime(raw, "%Y-%m-%d").date()
            row["retrieval_date"] = (d - delta).isoformat()
        except ValueError:
            pass
        rows.append(row)
    file_handler.write_annotations(rows, path)
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Days to subtract from each retrieval_date (default: 30)",
    )
    parser.add_argument(
        "paths",
        nargs="*",
        default=[
            "data/genbank_annotations.tsv",
            "data/refseq_annotations.tsv",
            "data/ensembl_annotations.tsv",
        ],
    )
    args = parser.parse_args()
    key_by_path = {
        "data/genbank_annotations.tsv": "assembly_accession",
        "data/refseq_annotations.tsv": "assembly_accession",
        "data/ensembl_annotations.tsv": "access_url",
    }
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    for rel in args.paths:
        path = rel if os.path.isabs(rel) else os.path.join(repo_root, rel)
        key_col = key_by_path.get(rel.replace("\\", "/"), "assembly_accession")
        if "ensembl" in path:
            key_col = "access_url"
        n = backdate_file(path, key_col, args.days)
        print(f"Backdated {n} rows in {path}")


if __name__ == "__main__":
    main()
