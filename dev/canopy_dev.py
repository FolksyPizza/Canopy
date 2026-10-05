#!/usr/bin/env python3
"""Repeatable local build and test entry point for Canopy's three deliverables."""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
GATEWAY = ROOT / "session-gateway"
GATEWAY_PATCH = GATEWAY / "canopy-session-gateway.patch"
VELOCITY_URL = "https://github.com/PaperMC/Velocity.git"
SUPPORTED_PROTOCOLS = tuple(range(771, 778))


def java_major_from_text(output: str) -> int | None:
    match = re.search(r'(?:version\s+"?|Java version:\s*)(\d+(?:\.\d+)*)', output)
    if not match:
        return None
    parts = match.group(1).split(".")
    major = int(parts[0])
    return int(parts[1]) if major == 1 and len(parts) > 1 else major


def is_revision(value: str) -> bool:
    return re.fullmatch(r"[0-9a-f]{40}", value) is not None


def encode_varint(value: int) -> bytes:
    if value < 0:
        raise ValueError("VarInt values must be non-negative")
    encoded = bytearray()
    while value > 0x7F:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def decode_varint(data: bytes, offset: int = 0) -> tuple[int, int]:
    value = 0
    for position in range(5):
        if offset >= len(data):
            raise ValueError("Truncated VarInt")
        current = data[offset]
        offset += 1
        value |= (current & 0x7F) << (7 * position)
        if current & 0x80 == 0:
            return value, offset
    raise ValueError("VarInt exceeds five bytes")


def encode_frame(payload: bytes) -> bytes:
    return encode_varint(len(payload)) + payload


def build_status_handshake(host: str, port: int, protocol: int) -> bytes:
    if protocol not in SUPPORTED_PROTOCOLS:
        raise ValueError(f"Supported Java probe protocols are {SUPPORTED_PROTOCOLS[0]}-{SUPPORTED_PROTOCOLS[-1]}")
    if not 1 <= port <= 65535:
        raise ValueError("Port must be in the range 1-65535")
    address = host.encode("utf-8")
    payload = (encode_varint(0) + encode_varint(protocol) + encode_varint(len(address)) + address
               + struct.pack(">H", port) + encode_varint(1))
    return encode_frame(payload)


def read_varint(stream: socket.socket) -> int:
    value = 0
    for position in range(5):
        current = stream.recv(1)
        if not current:
            raise EOFError("Connection closed while reading packet length")
        byte = current[0]
        value |= (byte & 0x7F) << (7 * position)
        if byte & 0x80 == 0:
            return value
    raise ValueError("VarInt exceeds five bytes")


def receive_exact(stream: socket.socket, length: int) -> bytes:
    payload = bytearray()
    while len(payload) < length:
        chunk = stream.recv(length - len(payload))
        if not chunk:
            raise EOFError("Connection closed before the full packet arrived")
        payload.extend(chunk)
    return bytes(payload)


def probe_java_status(host: str, port: int, protocol: int, timeout: float,
                      handshake_host: str | None = None) -> tuple[dict[str, object], float]:
    if timeout <= 0 or timeout > 60:
        raise ValueError("Timeout must be greater than 0 and no more than 60 seconds")
    virtual_host = handshake_host or host
    started = time.perf_counter()
    with socket.create_connection((host, port), timeout=timeout) as stream:
        stream.settimeout(timeout)
        stream.sendall(build_status_handshake(virtual_host, port, protocol))
        stream.sendall(encode_frame(encode_varint(0)))
        packet_length = read_varint(stream)
        if not 1 <= packet_length <= 1_048_576:
            raise ValueError(f"Unexpected status packet length: {packet_length}")
        packet = receive_exact(stream, packet_length)

    packet_id, offset = decode_varint(packet)
    if packet_id != 0:
        raise ValueError(f"Expected a status response packet, received id {packet_id}")
    json_length, offset = decode_varint(packet, offset)
    json_bytes = packet[offset:]
    if json_length != len(json_bytes):
        raise ValueError("Status response JSON length does not match its packet")
    status = json.loads(json_bytes.decode("utf-8"))
    if not isinstance(status, dict):
        raise ValueError("Status response must be a JSON object")
    return status, (time.perf_counter() - started) * 1000


def run_java_probe(args: argparse.Namespace) -> int:
    try:
        status, elapsed_ms = probe_java_status(
            args.host, args.port, args.protocol, args.timeout, args.handshake_host)
    except (OSError, EOFError, ValueError, json.JSONDecodeError) as exc:
        print(f"Java status probe failed: {exc}", file=sys.stderr)
        return 1
    version = status.get("version")
    players = status.get("players")
    if not isinstance(version, dict):
        version = {}
    if not isinstance(players, dict):
        players = {}
    version_name = version.get("name")
    version_name = json.dumps(version_name, ensure_ascii=True) if isinstance(version_name, str) else "unknown"
    server_protocol = version.get("protocol")
    server_protocol = server_protocol if isinstance(server_protocol, int) else "unknown"
    print(f"Java status answered in {elapsed_ms:.1f} ms for requested protocol {args.protocol}.")
    print(f"Server reports version {version_name} (protocol {server_protocol}).")
    online, maximum = players.get("online"), players.get("max")
    if type(online) is int and type(maximum) is int:
        print(f"Players online: {online}/{maximum}.")
    print("This checks only the unauthenticated Java status handshake, not login, Bedrock, or seamless transfers.")
    return 0


def summarize_audit(path: Path) -> tuple[Counter[str], int]:
    counts: Counter[str] = Counter()
    malformed = 0
    with path.open("r", encoding="utf-8") as stream:
        for line in stream:
            if not line.strip():
                continue
            if len(line) > 1_048_576:
                malformed += 1
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                malformed += 1
                continue
            if not isinstance(record, dict):
                malformed += 1
                continue
            event = record.get("event")
            if not isinstance(event, str) or re.fullmatch(r"[a-z][a-z0-9_.]{0,95}", event) is None:
                malformed += 1
                continue
            counts[event] += 1
    return counts, malformed


def run_audit_summary(args: argparse.Namespace) -> int:
    try:
        counts, malformed = summarize_audit(args.file)
    except (OSError, UnicodeError) as exc:
        print(f"Could not read audit file ({type(exc).__name__}).", file=sys.stderr)
        return 1
    valid = sum(counts.values())
    print(f"Audit records: {valid} valid, {malformed} malformed.")
    print(f"Event types: {len(counts)}.")
    for event, count in sorted(counts.items(), key=lambda row: (-row[1], row[0]))[:40]:
        print(f"{event}: {count}")
    if len(counts) > 40:
        print(f"Other event types: {len(counts) - 40}.")
    print("Only event names and counts are shown; record fields and player data are omitted.")
    return 0


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def junit_summary(directories: list[Path]) -> dict[str, int]:
    totals = {"reports": 0, "tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    for directory in directories:
        if not directory.is_dir():
            continue
        for report in sorted(directory.glob("TEST-*.xml")):
            root = ET.parse(report).getroot()
            suites = [root] if root.tag == "testsuite" else root.findall(".//testsuite")
            if not suites:
                continue
            totals["reports"] += 1
            for suite in suites:
                for key in ("tests", "failures", "errors", "skipped"):
                    totals[key] += int(suite.attrib.get(key, 0))
    return totals


def print_junit_summary(label: str, directories: list[Path]) -> dict[str, int]:
    totals = junit_summary(directories)
    if not totals["reports"]:
        raise RuntimeError(f"No JUnit XML reports found for {label}")
    print(f"{label}: {totals['tests']} tests, {totals['failures']} failures, "
          f"{totals['errors']} errors, {totals['skipped']} skipped")
    return totals


def command_output(command: list[str], env: dict[str, str] | None = None) -> str:
    result = subprocess.run(command, env=env, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, check=False)
    return result.stdout if result.returncode == 0 else ""


def java_executable(home: Path) -> Path | None:
    for name in ("java", "java.exe"):
        candidate = home / "bin" / name
        if candidate.is_file():
            return candidate
    return None


def find_jdk(major: int, override: str | None) -> tuple[Path, str] | None:
    candidates: list[Path] = []
    if override:
        candidates.append(Path(override).expanduser())
    configured = os.environ.get(f"CANOPY_JDK{major}_HOME")
    if configured:
        candidates.append(Path(configured).expanduser())
    current_home = os.environ.get("JAVA_HOME")
    if current_home:
        candidates.append(Path(current_home).expanduser())
    on_path = shutil.which("java")
    if on_path:
        candidates.append(Path(on_path).resolve().parent.parent)

    seen: set[Path] = set()
    for candidate in candidates:
        candidate = candidate.resolve()
        if candidate in seen:
            continue
        seen.add(candidate)
        java = java_executable(candidate)
        if java is None:
            # A command-line override names a JDK home and should fail clearly if malformed.
            if override and candidate == Path(override).expanduser().resolve():
                raise ValueError(f"No java executable found under {candidate}")
            continue
        output = command_output([str(java), "-version"])
        if java_major_from_text(output) == major:
            return candidate, output.splitlines()[0] if output else f"Java {major}"
    return None


def java_environment(home: Path) -> dict[str, str]:
    env = os.environ.copy()
    env["JAVA_HOME"] = str(home)
    env["PATH"] = str(home / "bin") + os.pathsep + env.get("PATH", "")
    return env


def run_phase(name: str, command: list[str], cwd: Path,
              env: dict[str, str] | None = None) -> None:
    print(f"\n==> {name}", flush=True)
    print("$ " + shlex.join(command), flush=True)
    subprocess.run(command, cwd=cwd, env=env, check=True)


def check_environment(args: argparse.Namespace) -> int:
    problems: list[str] = []
    print(f"Python: {sys.version.split()[0]} (need 3.10+)")
    if sys.version_info < (3, 10):
        problems.append("Python 3.10 or newer is required")

    for major, override in ((21, args.java21_home), (25, args.java25_home)):
        try:
            found = find_jdk(major, override)
        except ValueError as exc:
            found = None
            problems.append(str(exc))
        if found:
            print(f"Java {major}: {found[1]}")
        else:
            print(f"Java {major}: not found (set CANOPY_JDK{major}_HOME or pass --java{major}-home)")
            if not any(f"Java {major}" in problem for problem in problems):
                problems.append(f"Java {major} is required")

    for tool, minimum in (("mvn", "Maven"), ("git", "Git")):
        executable = shutil.which(tool)
        if not executable:
            print(f"{minimum}: not found")
            problems.append(f"{minimum} is required")
            continue
        version_args = [executable, "-version"] if tool == "mvn" else [executable, "--version"]
        output = command_output(version_args)
        print(f"{minimum}: {output.splitlines()[0] if output else 'available'}")

    if problems:
        for problem in problems:
            print(f"ERROR: {problem}", file=sys.stderr)
        return 1
    print("Environment ready.")
    return 0


def velocity_revision() -> str:
    revision = (GATEWAY / "velocity-revision.txt").read_text(encoding="utf-8").strip()
    if not is_revision(revision):
        raise ValueError("session-gateway/velocity-revision.txt must contain a full 40-character commit")
    return revision


def clone_velocity(destination: Path, source: str | None) -> None:
    if source:
        source_path = Path(source).expanduser().resolve()
        if not source_path.exists():
            raise FileNotFoundError(f"Velocity source does not exist: {source_path}")
        command = ["git", "clone", "--quiet", "--shared", "--no-checkout",
                   str(source_path), str(destination)]
    else:
        command = ["git", "clone", "--quiet", "--filter=blob:none", "--no-checkout",
                   VELOCITY_URL, str(destination)]
    run_phase("Create disposable pinned Velocity checkout", command, ROOT)


def artifact_paths(gateway_checkout: Path) -> list[Path]:
    patterns = (
        ROOT / "target" / "canopy-*.jar",
        ROOT / "velocity-plugin" / "target" / "canopy-switch-*.jar",
        gateway_checkout / "proxy" / "build" / "libs" / "*-all.jar",
    )
    found: set[Path] = set()
    for pattern in patterns:
        found.update(pattern.parent.glob(pattern.name))
    return sorted(path for path in found if path.is_file() and not path.name.startswith("original-"))


def run_tests(args: argparse.Namespace) -> int:
    environment_code = check_environment(args)
    if environment_code:
        return environment_code

    jdk21 = find_jdk(21, args.java21_home)
    jdk25 = find_jdk(25, args.java25_home)
    assert jdk21 is not None and jdk25 is not None
    java21_env = java_environment(jdk21[0])
    java25_env = java_environment(jdk25[0])
    maven = shutil.which("mvn")
    assert maven is not None
    revision = velocity_revision()
    source = args.velocity_source or os.environ.get("CANOPY_VELOCITY_SOURCE")
    workspace = Path(tempfile.mkdtemp(prefix="canopy-dev-"))
    keep_workspace = args.keep_workspace
    current_phase = "developer-tool tests"

    try:
        run_phase(current_phase,
                  [sys.executable, "-m", "unittest", "discover", "-s", "dev", "-p", "test_*.py"],
                  ROOT)
        current_phase = "Canopy core build and tests"
        run_phase(current_phase, [maven, "-B", "-ntp", "clean", "install"], ROOT, java21_env)
        core_tests = print_junit_summary("Canopy core", [ROOT / "target" / "surefire-reports"])
        current_phase = "CanopySwitch build and tests"
        run_phase(current_phase,
                  [maven, "-B", "-ntp", "-f", "velocity-plugin/pom.xml", "clean", "package"],
                  ROOT, java21_env)
        switch_tests = print_junit_summary(
            "CanopySwitch", [ROOT / "velocity-plugin" / "target" / "surefire-reports"])

        checkout = workspace / "velocity"
        current_phase = "pinned Velocity checkout"
        clone_velocity(checkout, source)
        run_phase("Select pinned Velocity revision",
                  ["git", "checkout", "--quiet", "--detach", revision], checkout)
        actual_revision = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=checkout, text=True).strip()
        if actual_revision != revision:
            raise RuntimeError(f"Velocity checkout mismatch: expected {revision}, got {actual_revision}")

        current_phase = "verify and apply Canopy gateway patch"
        run_phase("Check Canopy patch", ["git", "apply", "--check", str(GATEWAY_PATCH)], checkout)
        run_phase("Apply Canopy patch", ["git", "apply", str(GATEWAY_PATCH)], checkout)
        run_phase("Check patched whitespace", ["git", "diff", "--check"], checkout)

        current_phase = "Session Gateway tests and packaged jar"
        gradle = checkout / "gradlew"
        if not gradle.is_file():
            raise FileNotFoundError("Pinned Velocity checkout does not contain gradlew")
        run_phase(current_phase,
                  [str(gradle), ":velocity-proxy:test", ":velocity-proxy:shadowJar",
                   "--no-daemon", "--max-workers=1",
                   "-Dorg.gradle.jvmargs=-Xmx512m -XX:MaxMetaspaceSize=256m"],
                  checkout, java25_env)
        gateway_tests = print_junit_summary(
            "Session Gateway", [checkout / "proxy" / "build" / "test-results" / "test"])

        if any(group["failures"] or group["errors"]
               for group in (core_tests, switch_tests, gateway_tests)):
            raise RuntimeError("one or more test suites reported a failure")

        print("\nBuild artifacts (SHA-256):")
        artifacts = artifact_paths(checkout)
        if not artifacts:
            raise RuntimeError("Build completed but no expected jar artifacts were found")
        for artifact in artifacts:
            try:
                name = artifact.relative_to(ROOT)
            except ValueError:
                name = Path("Session-Gateway") / artifact.name
            print(f"{sha256_file(artifact)}  {name}")
        print("\nPASS: core, switch plugin, and pinned Session Gateway tests/build completed.")
        print("No server or proxy was started.")
        return 0
    except subprocess.CalledProcessError as exc:
        print(f"\nFAIL: {current_phase} returned exit code {exc.returncode}.", file=sys.stderr)
        return exc.returncode or 1
    except (OSError, RuntimeError, ValueError, ET.ParseError) as exc:
        print(f"\nFAIL: {current_phase}: {exc}", file=sys.stderr)
        return 1
    finally:
        if keep_workspace:
            print(f"Disposable workspace kept at: {workspace}")
        else:
            shutil.rmtree(workspace, ignore_errors=True)


def add_jdk_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--java21-home", help="JDK 21 home (or set CANOPY_JDK21_HOME)")
    parser.add_argument("--java25-home", help="JDK 25 home (or set CANOPY_JDK25_HOME)")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build and verify Canopy's plugin, switch plugin, and gateway.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    check = subparsers.add_parser("check", help="check local build tools and JDKs")
    add_jdk_options(check)
    test = subparsers.add_parser("test", help="build all modules and test a disposable pinned gateway")
    add_jdk_options(test)
    test.add_argument("--velocity-source", help="local Velocity git clone to share as the source object store")
    test.add_argument("--keep-workspace", action="store_true", help="keep the disposable patched gateway checkout")
    probe = subparsers.add_parser("probe-java", help="probe an unauthenticated Java server-list status response")
    probe.add_argument("host", help="network host or IP address")
    probe.add_argument("--port", type=int, default=25565)
    probe.add_argument("--protocol", type=int, choices=SUPPORTED_PROTOCOLS, default=774)
    probe.add_argument("--handshake-host", help="virtual host sent in the Minecraft handshake")
    probe.add_argument("--timeout", type=float, default=4.0)
    audit = subparsers.add_parser("audit-summary", help="summarize a local Canopy JSONL audit without exposing record fields")
    audit.add_argument("file", type=Path, help="path to an audit/events.jsonl file")
    args = parser.parse_args()

    if args.command == "check":
        return check_environment(args)
    if args.command == "probe-java":
        return run_java_probe(args)
    if args.command == "audit-summary":
        return run_audit_summary(args)
    return run_tests(args)


if __name__ == "__main__":
    raise SystemExit(main())
