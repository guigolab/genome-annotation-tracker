"""Unit tests for mirror merge and retrieval_date behavior."""

from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

sys.path.insert(0, "providers")

from tools import helper  # noqa: E402
from tools.async_ops import ProbeResult  # noqa: E402


class TestKeepRecentAnnotations(unittest.TestCase):
    def test_skips_rows_retrieved_within_14_days(self):
        recent = (datetime.now().date() - timedelta(days=7)).isoformat()
        old = (datetime.now().date() - timedelta(days=20)).isoformat()
        existing = {
            "a": {"retrieval_date": recent},
            "b": {"retrieval_date": old},
        }
        parsed = {"a": {}, "b": {}}
        kept = helper.keep_recent_annotations(existing, parsed)
        self.assertEqual(kept, ["a"])

    def test_ignores_rows_not_in_parsed_listing(self):
        recent = datetime.now().date().isoformat()
        existing = {"a": {"retrieval_date": recent}}
        parsed = {}
        self.assertEqual(helper.keep_recent_annotations(existing, parsed), [])


class TestBuildMergedRowsRetrievalDate(unittest.TestCase):
    RUN_DATE = "2026-05-17"

    def test_emit_existing_skipped_row_keeps_old_retrieval_date(self):
        existing = {
            "k1": {
                "assembly_accession": "k1",
                "retrieval_date": "2026-05-10",
                "last_modified_date": "2021-01-01",
            }
        }
        parsed = {"k1": {"assembly_accession": "k1"}}
        outcomes = {"k1": "emit_existing"}
        rows, _ = helper.build_merged_rows(
            existing,
            parsed,
            outcomes,
            run_date=self.RUN_DATE,
            lm_probed_keys=set(),
            md5_probed_keys=set(),
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["retrieval_date"], "2026-05-10")

    def test_emit_existing_after_lm_probe_updates_retrieval_date(self):
        existing = {
            "k1": {
                "assembly_accession": "k1",
                "retrieval_date": "2026-04-01",
                "last_modified_date": "2021-01-01",
            }
        }
        parsed = {"k1": {"assembly_accession": "k1"}}
        outcomes = {"k1": "emit_existing"}
        rows, _ = helper.build_merged_rows(
            existing,
            parsed,
            outcomes,
            run_date=self.RUN_DATE,
            lm_probed_keys={"k1"},
            md5_probed_keys=set(),
        )
        self.assertEqual(rows[0]["retrieval_date"], self.RUN_DATE)

    def test_emit_new_sets_retrieval_date(self):
        existing: dict = {}
        parsed = {
            "k1": {
                "assembly_accession": "k1",
                "last_modified_date": "2026-05-13",
                "md5_checksum": "abc",
            }
        }
        outcomes = {"k1": "emit_new"}
        rows, _ = helper.build_merged_rows(
            existing,
            parsed,
            outcomes,
            run_date=self.RUN_DATE,
            lm_probed_keys={"k1"},
            md5_probed_keys={"k1"},
        )
        self.assertEqual(rows[0]["retrieval_date"], self.RUN_DATE)


class TestDecideLastModifiedOutcomes(unittest.TestCase):
    def test_unchanged_lm_does_not_mutate_retrieval_date_on_existing(self):
        existing = {
            "k1": {
                "retrieval_date": "2026-04-01",
                "last_modified_date": "2021-09-27",
            }
        }
        parsed = {"k1": {}}
        results = [
            ProbeResult(key="k1", status="ok", value="2021-09-27"),
        ]
        outcomes = helper.decide_last_modified_outcomes(
            existing, parsed, results, skip_keys=set()
        )
        self.assertEqual(outcomes["k1"], "reuse_existing")
        self.assertEqual(existing["k1"]["retrieval_date"], "2026-04-01")


class TestRecentRetrievalConstant(unittest.TestCase):
    def test_recent_window_is_14_days(self):
        self.assertEqual(helper.RECENT_RETRIEVAL_DAYS, 14)

    @patch("tools.helper.datetime")
    def test_cutoff_uses_14_days(self, mock_dt):
        today = datetime(2026, 5, 17).date()
        mock_dt.now.return_value = datetime(2026, 5, 17)
        mock_dt.strptime = datetime.strptime
        existing = {
            "edge": {"retrieval_date": "2026-05-04"},
            "old": {"retrieval_date": "2026-05-03"},
        }
        parsed = {"edge": {}, "old": {}}
        kept = helper.keep_recent_annotations(existing, parsed)
        self.assertEqual(kept, ["edge"])


if __name__ == "__main__":
    unittest.main()
