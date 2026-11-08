"""Figure parity through the API: every number in every parameter-free GET response, before and after."""
import json, sys, urllib.request, http.cookiejar

SERVERS = {"before": 8461, "after_id": 8462, "after_en": 8463}
out = sys.argv[1]


def session(port):
    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    origin = f"http://127.0.0.1:{port}"

    def call(method, path, body=None):
        request = urllib.request.Request(origin + path, method=method, data=json.dumps(body).encode() if body else None,
                                         headers={"Content-Type": "application/json", "Origin": origin})
        try:
            with opener.open(request, timeout=60) as response:
                return response.status, json.loads(response.read() or b"null"), response.headers.get("content-language")
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"null"), error.headers.get("content-language")
    status, _, _ = call("POST", "/api/auth/login", {"username": "dimas_contoh", "password": "contoh-sandi-123"})
    assert status == 200, status
    return call


def numbers(value, path=""):
    found = {}
    if isinstance(value, bool) or value is None:
        return found
    if isinstance(value, (int, float)):
        found[path] = value
    elif isinstance(value, dict):
        for key, child in value.items():
            found.update(numbers(child, f"{path}.{key}"))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.update(numbers(child, f"{path}[{index}]"))
    return found


spec = json.load(urllib.request.urlopen("http://127.0.0.1:8462/openapi.json"))
paths = sorted(path for path, operations in spec["paths"].items()
               if path.startswith("/api/") and "{" not in path and "get" in operations
               and not any(p.get("required") for p in operations["get"].get("parameters", [])))
sessions = {name: session(port) for name, port in SERVERS.items()}
report, totals = {}, {"endpoints": 0, "numbers": 0, "differing": 0, "status_differs": 0}
for path in paths:
    if path in {"/api/auth/logout", "/api/health"}:
        continue
    replies = {name: call("GET", path) for name, call in sessions.items()}
    statuses = {name: reply[0] for name, reply in replies.items()}
    entry = {"status": statuses, "content_language": {name: reply[2] for name, reply in replies.items()}}
    if len(set(statuses.values())) != 1:
        totals["status_differs"] += 1
        entry["status_differs"] = True
    if all(status == 200 for status in statuses.values()):
        figures = {name: numbers(reply[1]) for name, reply in replies.items()}
        # Every number the base wrote must be unchanged in both languages; numbers added by this change
        # (the params of composed text) must agree between the two languages.
        differing = sorted(
            [key for key in figures["before"] if len({json.dumps(figures[name].get(key)) for name in figures}) != 1]
            + [key for key in set(figures["after_id"]) | set(figures["after_en"])
               if key not in figures["before"] and figures["after_id"].get(key) != figures["after_en"].get(key)])
        entry["added"] = len(set(figures["after_id"]) - set(figures["before"]))
        totals["added"] = totals.get("added", 0) + entry["added"]
        entry["numbers"] = len(figures["after_id"])
        entry["differing"] = differing[:20]
        totals["endpoints"] += 1
        totals["numbers"] += len(figures["after_id"])
        totals["differing"] += len(differing)
    report[path] = entry
    print(path, statuses, entry.get("numbers"), len(entry.get("differing", [])))
json.dump({"totals": totals, "endpoints": report}, open(out, "w"), indent=1)
print(json.dumps(totals))
