"""Compare two API snapshots: every value must be equal, except read-time stamps.

usage: compare.py <before.json> <after.json> [--map-names Renamed=Seed ...]
Fields this change adds (category_system_key, system_key) are counted, not compared, when the
before side lacks them. Read-time stamps: `generated_at`, `as_of`, and an `end`/`to` that is "now" (a running window), within 10 minutes.
`--map-names` maps renamed display names back to the seed names before comparing.
"""
import json
import sys
from datetime import datetime

VOLATILE = {"generated_at", "as_of", "taken_at", "api"}
# Fields this change adds to the API; absent from the V51 code's responses, so they are counted, not compared.
ADDITIVE = {"category_system_key", "system_key"}
RUNNING_END = {"end", "to"}


def running_end(key, before, after):
    if key not in RUNNING_END or not isinstance(before, str) or not isinstance(after, str):
        return False
    try:
        return abs((datetime.fromisoformat(after) - datetime.fromisoformat(before)).total_seconds()) < 600
    except ValueError:
        return False


def walk(before, after, path, out, counter):
    if isinstance(before, dict) and isinstance(after, dict):
        for key in sorted(set(before) | set(after)):
            if key in VOLATILE:
                continue
            if key in ADDITIVE and key not in before:
                counter["additive"] += 1
                continue
            if running_end(key, before.get(key), after.get(key)):
                counter["running_end"] += 1
                continue
            walk(before.get(key), after.get(key), f"{path}/{key}", out, counter)
    elif isinstance(before, list) and isinstance(after, list) and len(before) == len(after):
        for index, pair in enumerate(zip(before, after)):
            walk(*pair, f"{path}[{index}]", out, counter)
    else:
        counter["values"] += 1
        if before != after:
            out.append(f"{path}: {json.dumps(before, ensure_ascii=False)[:120]} -> {json.dumps(after, ensure_ascii=False)[:120]}")


args = sys.argv[1:]
mapping = {}
if "--map-names" in args:
    index = args.index("--map-names")
    mapping = dict(item.split("=", 1) for item in args[index + 1:])
    args = args[:index]
texts = [open(path).read() for path in args]
for renamed, seed in mapping.items():
    texts[1] = texts[1].replace(json.dumps(renamed, ensure_ascii=False), json.dumps(seed, ensure_ascii=False))
before, after = (json.loads(text) for text in texts)
differences, counter = [], {"values": 0, "running_end": 0, "additive": 0}
walk(before["responses"], after["responses"], "", differences, counter)
print(json.dumps({"compared_values": counter["values"], "running_window_ends_skipped": counter["running_end"],
                  "additive_fields_only_after": counter["additive"],
                  "differences": len(differences)}))
for line in differences:
    print(line)
