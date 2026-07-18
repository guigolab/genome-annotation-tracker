"""Unit tests for annotrieve-registry scanning in providers/registry.py."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, "providers")

from registry import (  # noqa: E402
    REQUIRED_TSV_HEADER,
    backfill_release_dates,
    build_row,
    discover_projects,
    parse_annotations_tsv,
    scan_registry,
)


class TestDiscoverProjects(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def _write_project(self, name: str, *, with_data: bool = True) -> None:
        proj = self.root / name
        proj.mkdir()
        (proj / "manifest.yaml").write_text(
            'provider_name: "Test Lab"\n'
            'pipeline_method: "Test pipeline"\n'
            'pipeline_version: "1.0.0"\n',
            encoding="utf-8",
        )
        tsv = proj / "annotations.tsv"
        if with_data:
            tsv.write_text(
                REQUIRED_TSV_HEADER + "\n"
                "GCA_000001405.4\thttps://example.org/a.gff.gz\n",
                encoding="utf-8",
            )
        else:
            tsv.write_text(REQUIRED_TSV_HEADER + "\n", encoding="utf-8")

    def test_discovers_valid_projects(self):
        self._write_project("TOGA2")
        self._write_project("sample_project")
        projects = discover_projects(self.root)
        names = {p.name for p in projects}
        self.assertEqual(names, {"TOGA2"})
        self.assertNotIn("sample_project", names)

    def test_skips_dirs_without_both_files(self):
        (self.root / "schema").mkdir()
        (self.root / "schema" / "manifest.yaml").write_text("x: 1\n", encoding="utf-8")
        self._write_project("real_project")
        names = {p.name for p in discover_projects(self.root)}
        self.assertEqual(names, {"real_project"})


class TestParseAnnotationsTsv(unittest.TestCase):
    def test_parses_rows_and_ignores_comments(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False) as f:
            f.write(
                REQUIRED_TSV_HEADER + "\n"
                "# comment line\n"
                "GCA_000001405.4\thttps://example.org/a.gff.gz\n"
                "\n"
            )
            path = Path(f.name)
        try:
            rows = parse_annotations_tsv(path)
            self.assertEqual(
                rows,
                [("GCA_000001405.4", "https://example.org/a.gff.gz")],
            )
        finally:
            path.unlink(missing_ok=True)

    def test_rejects_bad_header(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False) as f:
            f.write("wrong\theader\n")
            path = Path(f.name)
        try:
            with self.assertRaises(ValueError):
                parse_annotations_tsv(path)
        finally:
            path.unlink(missing_ok=True)


class TestBuildRow(unittest.TestCase):
    def test_maps_manifest_and_project_fields(self):
        row = build_row(
            assembly_accession="GCA_000001405.4",
            access_url="https://example.org/a.gff.gz",
            project_name="TOGA2",
            manifest={
                "provider_name": "Hiller Lab",
                "pipeline_method": "TOGA2 pipeline",
                "pipeline_version": "2.0.9",
            },
            assembly_meta={
                "assembly_name": "ASM14054v4",
                "taxon_id": 9606,
                "organism_name": "Homo sapiens",
            },
        )
        self.assertEqual(row["source_database"], "CommunityRegistry")
        self.assertEqual(row["annotation_provider"], "Hiller Lab")
        self.assertEqual(row["assembly_accession"], "GCA_000001405.4")
        self.assertEqual(row["access_url"], "https://example.org/a.gff.gz")
        self.assertEqual(row["file_format"], "gff")
        self.assertEqual(row["pipeline_name"], "TOGA2")
        self.assertEqual(row["pipeline_method"], "TOGA2 pipeline")
        self.assertEqual(row["pipeline_version"], "2.0.9")
        self.assertEqual(row["assembly_name"], "ASM14054v4")
        self.assertEqual(row["taxon_id"], 9606)
        self.assertEqual(row["organism_name"], "Homo sapiens")
        self.assertIsNone(row["release_date"])


class TestScanRegistry(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_scan_builds_rows_keyed_by_access_url(self):
        for name in ("sample_project", "my_project"):
            proj = self.root / name
            proj.mkdir()
            (proj / "manifest.yaml").write_text(
                'provider_name: "Lab"\n'
                'pipeline_method: "method"\n'
                'pipeline_version: "1.0"\n',
                encoding="utf-8",
            )
            (proj / "annotations.tsv").write_text(
                REQUIRED_TSV_HEADER + "\n"
                f"GCA_000001405.4\thttps://example.org/{name}.gff.gz\n",
                encoding="utf-8",
            )

        fake_meta = {
            "GCA_000001405.4": {
                "assembly_name": "ASM14054v4",
                "taxon_id": 9606,
                "organism_name": "Homo sapiens",
            }
        }
        with patch("registry.fetch_assembly_metadata", return_value=fake_meta):
            parsed = scan_registry(self.root)

        self.assertEqual(len(parsed), 1)
        row = parsed["https://example.org/my_project.gff.gz"]
        self.assertEqual(row["pipeline_name"], "my_project")
        self.assertEqual(row["source_database"], "CommunityRegistry")


class TestBackfillReleaseDates(unittest.TestCase):
    def test_fills_release_date_from_last_modified(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "community.tsv"
            path.write_text(
                "access_url\trelease_date\tlast_modified_date\n"
                "https://example.org/a.gff.gz\t\t2025-01-15\n"
                "https://example.org/b.gff.gz\t2024-06-01\t2025-02-01\n",
                encoding="utf-8",
            )
            backfill_release_dates(str(path))
            content = path.read_text(encoding="utf-8")
            self.assertIn("https://example.org/a.gff.gz\t2025-01-15\t2025-01-15", content)
            self.assertIn("https://example.org/b.gff.gz\t2024-06-01\t2025-02-01", content)


if __name__ == "__main__":
    unittest.main()
