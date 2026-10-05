import unittest
from contextlib import redirect_stdout
from io import StringIO
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from argparse import Namespace

from canopy_dev import (
    build_status_handshake,
    decode_varint,
    encode_varint,
    is_revision,
    java_major_from_text,
    junit_summary,
    probe_java_status,
    run_audit_summary,
    summarize_audit,
)


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

    def test_encodes_status_handshake_for_a_supported_protocol(self):
        packet = build_status_handshake("example.test", 25565, 774)

        frame_length, offset = decode_varint(packet)
        self.assertEqual(frame_length, len(packet) - offset)
        packet_id, offset = decode_varint(packet, offset)
        self.assertEqual(0, packet_id)
        protocol, _ = decode_varint(packet, offset)
        self.assertEqual(774, protocol)

    def test_status_probe_parses_loopback_response(self):
        import socket
        import threading

        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind((socket.gethostbyname("localhost"), 0))
        listener.listen(1)
        port = listener.getsockname()[1]
        result = {"version": {"name": "Test Server", "protocol": 774},
                  "players": {"online": 2, "max": 20}}

        def respond():
            with listener:
                connection, _ = listener.accept()
                with connection:
                    connection.recv(4096)
                    encoded = json.dumps(result).encode("utf-8")
                    body = encode_varint(0) + encode_varint(len(encoded)) + encoded
                    connection.sendall(encode_varint(len(body)) + body)

        server = threading.Thread(target=respond, daemon=True)
        server.start()
        status, elapsed = probe_java_status("localhost", port, 774, 2.0)
        server.join(timeout=2.0)

        self.assertEqual(result, status)
        self.assertGreaterEqual(elapsed, 0.0)
        self.assertFalse(server.is_alive())

    def test_audit_summary_counts_event_names_without_printing_record_fields(self):
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "events.jsonl"
            path.write_text(
                json.dumps({"event": "session.transfer", "player": "private-player-id", "x": 12345})
                + "\n"
                + json.dumps({"event": "session.transfer", "player": "another-private-id"})
                + "\n"
                + "not-json\n"
                + json.dumps({"event": "BAD EVENT", "player": "private-player-id"})
                + "\n",
                encoding="utf-8",
            )
            counts, malformed = summarize_audit(path)
            self.assertEqual({"session.transfer": 2}, dict(counts))
            self.assertEqual(2, malformed)

            output = StringIO()
            with redirect_stdout(output):
                result = run_audit_summary(Namespace(file=path))
            self.assertEqual(0, result)
            self.assertIn("2 valid, 2 malformed", output.getvalue())
            self.assertIn("session.transfer: 2", output.getvalue())
            self.assertNotIn("private-player-id", output.getvalue())
            self.assertNotIn("12345", output.getvalue())


if __name__ == "__main__":
    unittest.main()
