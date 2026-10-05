import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from canopy_dev import is_revision, java_major_from_text, junit_summary


class CanopyDevParsingTest(unittest.TestCase):
    def test_reads_current_java_release_output(self):
        self.assertEqual(21, java_major_from_text('openjdk version "21.0.12" 2026-08-18'))
        self.assertEqual(25, java_major_from_text('openjdk version "25-ea" 2025-03-18'))

    def test_reads_legacy_java_version(self):
        self.assertEqual(8, java_major_from_text('java version "1.8.0_402"'))

    def test_reads_maven_java_version_line(self):
        self.assertEqual(21, java_major_from_text('Java version: 21.0.10, vendor: Example'))

    def test_requires_full_git_revision(self):
        self.assertTrue(is_revision("7fc49913145fc6195db95995eb622bf9f5592876"))
        self.assertFalse(is_revision("7fc49913"))
        self.assertFalse(is_revision("g" * 40))

    def test_summarizes_junit_reports(self):
        with TemporaryDirectory() as temporary:
            report = Path(temporary) / "TEST-sample.xml"
            report.write_text(
                '<testsuite tests="5" failures="1" errors="0" skipped="2"/>',
                encoding="utf-8",
            )
            self.assertEqual(
                {"reports": 1, "tests": 5, "failures": 1, "errors": 0, "skipped": 2},
                junit_summary([Path(temporary)]),
            )


if __name__ == "__main__":
    unittest.main()
