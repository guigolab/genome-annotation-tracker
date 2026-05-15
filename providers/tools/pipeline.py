"""
Shared mirror pipeline for NCBI and Ensembl providers.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable

from tools import file_handler, helper
from tools.async_ops import DEFAULT_CONCURRENCY, ProbeResult, check_last_modified_date_many


def run_mirror(
    *,
    output_file: str,
    key_column: str,
    load_universe: Callable[[], dict[str, dict]],
    probe_md5: Callable[[list[tuple[str, str]], int, dict[str, dict]], list[ProbeResult]],
    source_label: str,
    stats_path: str | None = None,
    outcomes_path: str | None = None,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> None:
    existing, existing_key_order = file_handler.load_annotations_ordered(
        output_file, key_column
    )
    print(f"[{source_label}] Found {len(existing)} existing annotations")

    parsed = load_universe()
    if not parsed:
        raise RuntimeError(f"[{source_label}] Source listing is empty — aborting to avoid wiping TSV")
    print(f"[{source_label}] Found {len(parsed)} annotations in source listing")

    source_keys = set(parsed.keys())
    skip_keys = set(helper.keep_recent_annotations(existing, parsed))
    print(f"[{source_label}] Skipping re-probe for {len(skip_keys)} recently retrieved rows")

    lm_tuples = helper.get_tuples_to_check(skip_keys, parsed)
    print(f"[{source_label}] Probing last-modified for {len(lm_tuples)} rows...")
    lm_results = asyncio.run(check_last_modified_date_many(lm_tuples, concurrency))
    lm_outcomes = helper.decide_last_modified_outcomes(existing, parsed, lm_results, skip_keys)

    md5_keys = {k for k, o in lm_outcomes.items() if o == "refresh_md5"}
    md5_tuples = [(parsed[k]["access_url"], k) for k in md5_keys if k in parsed]
    print(f"[{source_label}] Fetching MD5 for {len(md5_tuples)} rows...")
    md5_results: list[ProbeResult] = []
    if md5_tuples:
        md5_results = probe_md5(md5_tuples, concurrency, parsed)

    final_outcomes = helper.decide_md5_outcomes(
        existing, parsed, md5_results, lm_outcomes, source_keys
    )
    merged_rows, outcome_log = helper.build_merged_rows(existing, parsed, final_outcomes)
    print(f"[{source_label}] Merged {len(merged_rows)} annotations")

    merged_ordered = helper.order_merged_annotations_for_git(
        merged_rows, existing_key_order, key_column
    )
    stats = helper.count_annotation_diffs(existing, merged_ordered, key_column)

    if stats_path is None:
        stats_path = os.path.join(
            os.path.dirname(output_file), f".mirror_stats_{source_label}.json"
        )
    if outcomes_path is None:
        outcomes_path = os.path.join(
            os.path.dirname(output_file), f".mirror_outcomes_{source_label}.json"
        )

    helper.write_mirror_stats(stats, stats_path)
    helper.write_mirror_outcomes(outcome_log, outcomes_path)
    print(
        f"[{source_label}] Stats: added={stats['added']} updated={stats['updated']} "
        f"deleted={stats['deleted']} (stats→{stats_path}, outcomes→{outcomes_path})"
    )

    file_handler.write_annotations(merged_ordered, output_file)
    print(f"[{source_label}] Written {len(merged_ordered)} rows to {output_file}")
