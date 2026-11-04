"""Score a Sesi uji replay against the expected result of each case (notification-test-session D7).

The expected file names, per case number, only the counts that matter for that case; the others are not
compared. Counts come from `replay_test_session.py`: `movements`, `income`, `expense`, `held` (waiting in
"Terlihat dua kali"), `skipped`, `review`, `questions`, `ignored` and `events`.

    {"cases": {"1": {"movements": 1, "income": 0, "skipped": 1}, "13": {"movements": 2}}}

    python3 scripts/verify/score_test_session.py --replay <scratch>/replay.json --expected <scratch>/expected.json

Exits 1 when a case fails or an expected case is missing from the replay.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def score(replay: dict, expected: dict) -> dict:
    results = []
    for case, wanted in expected.get("cases", {}).items():
        actual = replay.get("cases", {}).get(case)
        if actual is None:
            results.append({"case": case, "passed": False, "missing": True, "differences": {}})
            continue
        differences = {key: {"expected": value, "actual": actual.get(key)}
                       for key, value in wanted.items() if actual.get(key) != value}
        results.append({"case": case, "passed": not differences, "missing": False, "differences": differences})
    passed = sum(result["passed"] for result in results)
    return {"passed": passed, "failed": len(results) - passed, "results": results}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Score a Sesi uji replay per case")
    parser.add_argument("--replay", type=Path, required=True)
    parser.add_argument("--expected", type=Path, required=True)
    arguments = parser.parse_args(argv)
    report = score(json.loads(arguments.replay.read_text(encoding="utf-8")),
                   json.loads(arguments.expected.read_text(encoding="utf-8")))
    for result in report["results"]:
        if result["missing"]:
            line = "missing from the replay"
        elif result["passed"]:
            line = "pass"
        else:
            line = ", ".join(f"{key} expected {value['expected']} got {value['actual']}"
                             for key, value in result["differences"].items())
        print(f"case {result['case']}: {line}")
    print(f"{report['passed']} passed, {report['failed']} failed")
    return 0 if report["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
