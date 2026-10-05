#!/usr/bin/env python3
"""Repeatable local build and test entry point for Canopy's three deliverables."""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
GATEWAY = ROOT / "session-gateway"
GATEWAY_PATCH = GATEWAY / "canopy-session-gateway.patch"
VELOCITY_URL = "https://github.com/PaperMC/Velocity.git"


def java_major_from_text(output: str) -> int | None:
    match = re.search(r'(?:version\s+"?|Java version:\s*)(\d+(?:\.\d+)*)', output)
    if not match:
        return None
    parts = match.group(1).split(".")
    major = int(parts[0])
    return int(parts[1]) if major == 1 and len(parts) > 1 else major


def is_revision(value: str) -> bool:
    return re.fullmatch(r"[0-9a-f]{40}", value) is not None


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
    args = parser.parse_args()

    if args.command == "check":
        return check_environment(args)
    return run_tests(args)


if __name__ == "__main__":
    raise SystemExit(main())
