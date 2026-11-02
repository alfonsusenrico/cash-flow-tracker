"""Sum Gradle JUnit XML results and fail unless tests ran and none failed.

A Gradle exit code alone once hid a build that ran no tests, so CI and local checks read the XML.
Usage: python3 scripts/ci/junit_summary.py <results-dir> [--summary <markdown-file>]
"""
import argparse
import sys
import xml.etree.ElementTree as ElementTree
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("results_dir", type=Path)
    parser.add_argument("--summary", type=Path, help="append a Markdown line, e.g. $GITHUB_STEP_SUMMARY")
    args = parser.parse_args()

    files = sorted(args.results_dir.glob("TEST-*.xml"))
    totals = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    for path in files:
        suite = ElementTree.parse(path).getroot()
        for key in totals:
            totals[key] += int(suite.get(key, "0"))

    line = (
        f"{len(files)} suites, {totals['tests']} tests, {totals['failures']} failures, "
        f"{totals['errors']} errors, {totals['skipped']} skipped"
    )
    print(line)
    if args.summary:
        with args.summary.open("a", encoding="utf-8") as summary:
            summary.write(f"Unit tests: {line}\n")

    if totals["tests"] == 0:
        print(f"No test results in {args.results_dir}", file=sys.stderr)
        return 1
    if totals["failures"] or totals["errors"]:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
