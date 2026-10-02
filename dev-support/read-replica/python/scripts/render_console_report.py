#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Generate a Yetus-style console report for the read-replica integration tests.

Reads infrastructure timing from a KEY=VALUE env file and per-test results from
JUnit XML, then produces an HTML fragment matching the format used by the
upstream HBase Nightly Build (Apache Yetus).
"""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path


@dataclass
class TestResult:
    name: str
    time_sec: float
    passed: bool


def parse_timing_env(path: Path) -> dict[str, int]:
    timing: dict[str, int] = {}
    if not path.exists():
        return timing
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        try:
            timing[key.strip()] = int(value.strip())
        except ValueError:
            pass
    return timing


def parse_junit_xml(path: Path) -> list[TestResult]:
    tree = ET.parse(path)
    root = tree.getroot()

    testcases: list[tuple[str, float, bool]] = []
    for tc in root.iter("testcase"):
        name = tc.get("name", "unknown")
        try:
            time_sec = float(tc.get("time", "0"))
        except ValueError:
            time_sec = 0.0
        has_failure = tc.find("failure") is not None or tc.find("error") is not None
        testcases.append((name, time_sec, not has_failure))

    # Group by name; use the last entry (handles pytest-rerunfailures)
    seen: OrderedDict[str, TestResult] = OrderedDict()
    for name, time_sec, passed in testcases:
        seen[name] = TestResult(name=name, time_sec=time_sec, passed=passed)

    return list(seen.values())


def format_runtime(total_sec: int | float) -> str:
    total_sec = int(round(total_sec))
    minutes = total_sec // 60
    seconds = total_sec % 60
    return f"{minutes:>3d}m {seconds:>2d}s"


def _row(vote: str, color: str, subsystem: str, runtime: str,
         log: str, comment: str) -> str:
    return (
        "<tr>\n"
        f'\t\t<td><font color="{color}">{vote}</font></td>\n'
        f'\t\t<td><font color="{color}"> {subsystem} </font></td>\n'
        f'\t\t<td><font color="{color}">{runtime}</font></td>\n'
        f"<td>{log}</td>\n"
        f'<td><font color="{color}"> {comment} </font></td>\n'
        "</tr>\n"
    )


def _infra_row(subsystem: str, seconds: int, comment: str) -> str:
    return _row("0", "blue", subsystem, format_runtime(seconds), "", comment)


def _test_row(result: TestResult) -> str:
    if result.passed:
        vote, color = "+1", "green"
        comment = f"passed {result.name}"
    else:
        vote, color = "-1", "red"
        comment = f"failed {result.name}"
    return _row(vote, color, "pytest", format_runtime(result.time_sec), "", comment)


def _total_row(total_sec: int) -> str:
    return (
        "<tr>\n"
        '\t\t<td><font color="black"></font></td>\n'
        '\t\t<td><font color="black"> </font></td>\n'
        f'\t\t<td><font color="black">{format_runtime(total_sec)}</font></td>\n'
        "<td></td>\n"
        '<td><font color="black"></font></td>\n'
        "</tr>\n"
    )


_INFRA_STAGES = [
    ("Docker test env", "DEV_SUPPORT_IMAGE_BUILD_SEC",
     "Build time for Docker test environment image"),
    ("rsync", "RSYNC_SEC",
     "Copy hbase repo for Docker build context with read-replica image"),
    ("mvn clean", "MVN_CLEAN_SEC",
     "Maven clean to remove previous build artifacts for read-replica image"),
    ("read-replica image", "DOCKER_BUILD_SEC",
     "Docker image used for containers running an HBase read-replica setup"),
]


def build_console_report(timing: dict[str, int],
                         test_results: list[TestResult]) -> str:
    all_passed = all(t.passed for t in test_results)

    if all_passed:
        overall_color, overall_vote = "green", "+1"
    else:
        overall_color, overall_vote = "red", "-1"

    # Table 1: Overall header
    header_table = (
        "<table><tbody>\n"
        f'<tr><th><font color="{overall_color}">'
        f"{overall_vote} overall</font></th></tr>\n"
        "</tbody></table>\n"
    )

    # Title
    title_table = (
        "<table><tbody>\n"
        "<tr><th>Read-Replica Nightly Test Console Report</th></tr>\n"
        "</tbody></table>\n"
    )

    # Table 2: Stage results
    rows: list[str] = []

    for subsystem, timing_key, comment in _INFRA_STAGES:
        seconds = timing.get(timing_key, 0)
        rows.append(_infra_row(subsystem, seconds, comment))

    for result in test_results:
        rows.append(_test_row(result))

    total_sec = timing.get("TOTAL_SEC", 0)
    rows.append(_total_row(total_sec))

    stages_table = (
        "<table><tbody>\n"
        "<tr>\n"
        "<th>Vote</th>\n"
        "<th>Subsystem</th>\n"
        "<th>Runtime</th>\n"
        "<th>Log</th>\n"
        "<th>Comment</th>\n"
        "</tr>\n"
        + "".join(rows)
        + "</tbody></table>\n"
    )

    return (
        header_table
        + "<p></p>\n"
        + title_table
        + "<p></p>\n"
        + stages_table
        + "<p></p>\n"
        + "<p>This message was automatically generated.</p>\n"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate a Yetus-style console report for read-replica tests."
    )
    parser.add_argument(
        "--timing", required=True, type=Path,
        help="Path to timing env file (KEY=VALUE pairs)",
    )
    parser.add_argument(
        "--junit", required=True, type=Path,
        help="Path to JUnit XML results file",
    )
    parser.add_argument(
        "--output", required=True, type=Path,
        help="Path to write the HTML console report",
    )
    args = parser.parse_args(argv)

    if not args.junit.exists():
        print(f"JUnit XML not found: {args.junit}", file=sys.stderr)
        return 1

    timing = parse_timing_env(args.timing)
    test_results = parse_junit_xml(args.junit)
    report_html = build_console_report(timing, test_results)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report_html, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
