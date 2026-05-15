"""
Mirror merge logic driven by explicit ProbeResult outcomes.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Literal

from tools.async_ops import ProbeResult

LmOutcome = Literal["reuse_existing", "refresh_md5", "gone", "transient"]
FinalOutcome = Literal["emit_existing", "emit_new", "gone", "skip_new"]


def keep_recent_annotations(
    existing_annotations_dict: dict, parsed_annotations_dict: dict
) -> list[str]:
    """Keys recently retrieved (<30d) that still appear in the source listing — skip re-probe."""
    annotations_to_keep: list[str] = []
    one_month_ago = datetime.now().date() - timedelta(days=30)
    for unique_identifier, existing_annotation in existing_annotations_dict.items():
        if unique_identifier not in parsed_annotations_dict:
            continue
        try:
            existing_date = datetime.strptime(
                existing_annotation.get("retrieval_date", ""), "%Y-%m-%d"
            ).date()
        except ValueError:
            continue
        if existing_date > one_month_ago:
            annotations_to_keep.append(unique_identifier)
    return annotations_to_keep


def get_tuples_to_check(
    skip_keys: set[str], parsed_annotations_dict: dict
) -> list[tuple[str, str]]:
    """(access_url, key) for keys not in skip_keys."""
    return [
        (parsed_annotations_dict[k]["access_url"], k)
        for k in parsed_annotations_dict
        if k not in skip_keys
    ]


def decide_last_modified_outcomes(
    existing: dict[str, dict],
    parsed: dict[str, dict],
    lm_results: list[ProbeResult],
    skip_keys: set[str],
) -> dict[str, LmOutcome]:
    """Classify last-modified probe results per key."""
    outcomes: dict[str, LmOutcome] = {}
    by_key = {r.key: r for r in lm_results}

    for key in skip_keys:
        if key in existing:
            outcomes[key] = "reuse_existing"

    for key, row in parsed.items():
        if key in skip_keys:
            continue
        result = by_key.get(key)
        if result is None:
            outcomes[key] = "transient" if key in existing else "refresh_md5"
            continue
        if result.status == "not_found":
            outcomes[key] = "gone"
        elif result.status == "transient_error":
            outcomes[key] = "transient"
        elif result.status == "ok":
            existing_lm = (existing.get(key) or {}).get("last_modified_date")
            if key in existing and result.value == existing_lm:
                existing[key]["retrieval_date"] = row.get("retrieval_date")
                outcomes[key] = "reuse_existing"
            else:
                row["last_modified_date"] = result.value
                outcomes[key] = "refresh_md5"
        else:
            outcomes[key] = "transient"

    return outcomes


def decide_md5_outcomes(
    existing: dict[str, dict],
    parsed: dict[str, dict],
    md5_results: list[ProbeResult],
    lm_outcomes: dict[str, LmOutcome],
    source_keys: set[str],
) -> dict[str, FinalOutcome]:
    """
    Final per-key outcome after MD5 probes.
    source_keys: keys present in the current source listing (parsed dict keys).
    """
    final: dict[str, FinalOutcome] = {}
    by_key = {r.key: r for r in md5_results}

    for key, lm in lm_outcomes.items():
        if lm == "reuse_existing":
            final[key] = "emit_existing"
            continue
        if lm == "gone":
            if key in existing and key not in source_keys:
                final[key] = "gone"
            elif key in existing:
                final[key] = "emit_existing"
            else:
                final[key] = "gone"
            continue
        if lm == "transient":
            if key in existing:
                final[key] = "emit_existing"
            else:
                final[key] = "skip_new"
            continue
        # refresh_md5
        result = by_key.get(key)
        if result is None:
            final[key] = "emit_existing" if key in existing else "skip_new"
            continue
        if result.status == "not_found":
            if key in existing and key not in source_keys:
                final[key] = "gone"
            elif key in existing:
                final[key] = "emit_existing"
            else:
                final[key] = "gone"
        elif result.status == "transient_error":
            final[key] = "emit_existing" if key in existing else "skip_new"
        elif result.status == "ok" and result.value:
            parsed[key]["md5_checksum"] = result.value
            final[key] = "emit_new"
        else:
            final[key] = "emit_existing" if key in existing else "skip_new"

    # Existing rows absent from source listing and not in lm_outcomes → gone
    for key in existing:
        if key not in final and key not in source_keys:
            final[key] = "gone"
        elif key not in final:
            final[key] = "emit_existing"

    return final


def build_merged_rows(
    existing: dict[str, dict],
    parsed: dict[str, dict],
    final_outcomes: dict[str, FinalOutcome],
) -> tuple[list[dict], dict[str, str]]:
    """
    Build merged row list and per-key outcome labels for logging.
    Returns (rows, outcome_log).
    """
    rows: list[dict] = []
    log: dict[str, str] = {}
    seen: set[str] = set()

    for key, outcome in final_outcomes.items():
        log[key] = outcome
        if outcome == "gone" or outcome == "skip_new":
            continue
        if outcome == "emit_existing":
            if key in existing:
                row = dict(existing[key])
                if key in parsed:
                    row["retrieval_date"] = parsed[key].get("retrieval_date", row.get("retrieval_date"))
                rows.append(row)
                seen.add(key)
        elif outcome == "emit_new":
            if key not in parsed:
                continue
            prow = dict(parsed[key])
            eff_lm = (
                prow.get("last_modified_date")
                or (existing.get(key) or {}).get("last_modified_date")
                or prow.get("release_date")
            )
            if not eff_lm or not prow.get("md5_checksum"):
                if key in existing:
                    rows.append(dict(existing[key]))
                    seen.add(key)
                    log[key] = "emit_existing_fallback"
                continue
            prow["last_modified_date"] = eff_lm
            rows.append(prow)
            seen.add(key)

    return rows, log


def merge_annotations(
    existing_annotations_dict: dict,
    parsed_annotations_dict: dict,
    annotations_to_keep: list[str],
) -> list[dict]:
    """Legacy merge — prefer build_merged_rows via pipeline."""
    merged: list[dict] = []
    keep_set = set(annotations_to_keep)
    for uid in annotations_to_keep:
        merged.append(existing_annotations_dict[uid])
    for uid, parsed_annotation in parsed_annotations_dict.items():
        if uid in keep_set:
            continue
        if not parsed_annotation.get("md5_checksum"):
            continue
        existing = existing_annotations_dict.get(uid, {})
        effective_last_modified = (
            parsed_annotation.get("last_modified_date")
            or existing.get("last_modified_date")
            or parsed_annotation.get("release_date")
        )
        if not effective_last_modified:
            continue
        row = dict(parsed_annotation)
        row["last_modified_date"] = effective_last_modified
        merged.append(row)
    return merged


def _row_fingerprint(row: dict) -> tuple:
    return tuple(sorted((k, row.get(k)) for k in row if k != "retrieval_date"))


def order_merged_annotations_for_git(
    merged: list[dict], existing_key_order: list[str], key_column: str
) -> list[dict]:
    by_key: dict[str, dict] = {}
    for row in merged:
        k = row.get(key_column)
        if k:
            by_key[k] = row
    seen: set[str] = set()
    out: list[dict] = []
    for k in existing_key_order:
        if k in by_key:
            out.append(by_key[k])
            seen.add(k)
    new_keys = sorted(
        by_key.keys() - seen,
        key=lambda x: (by_key[x].get("assembly_accession") or "", x),
    )
    for k in new_keys:
        out.append(by_key[k])
    return out


def count_annotation_diffs(
    existing: dict[str, dict], merged_ordered: list[dict], key_column: str
) -> dict[str, int]:
    existing_keys = set(existing.keys())
    by_key: dict[str, dict] = {}
    for row in merged_ordered:
        k = row.get(key_column)
        if k:
            by_key[k] = row
    merged_keys = set(by_key.keys())
    added = len(merged_keys - existing_keys)
    deleted = len(existing_keys - merged_keys)
    updated = sum(
        1
        for k in merged_keys & existing_keys
        if _row_fingerprint(existing[k]) != _row_fingerprint(by_key[k])
    )
    return {"added": added, "updated": updated, "deleted": deleted}


def write_mirror_stats(stats: dict[str, int], path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=0)


def write_mirror_outcomes(outcomes: dict[str, str], path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(outcomes, f, indent=0, sort_keys=True)
