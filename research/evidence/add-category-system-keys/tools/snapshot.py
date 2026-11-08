"""API figure snapshot of the parity owner (local loopback only, synthetic data).

usage: snapshot.py <api base> <out.json>
"""
import json
import sys
import urllib.error
import urllib.request
from datetime import datetime
from http.cookiejar import CookieJar
from zoneinfo import ZoneInfo

base, out = sys.argv[1], sys.argv[2]
jar = CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
ORIGIN = {"Origin": "http://127.0.0.1:3601", "Content-Type": "application/json"}


def call(path, body=None):
    request = urllib.request.Request(base + path, data=json.dumps(body).encode() if body is not None else None,
                                     headers=ORIGIN, method="POST" if body is not None else "GET")
    with opener.open(request) as response:
        return json.loads(response.read())


call("/auth/login", {"username": "dimas_contoh", "password": "contoh-sandi-123"})
month_start = datetime.now(ZoneInfo("Asia/Jakarta")).date().replace(day=1).isoformat()
paths = [
    "/pulse", "/insights",
    "/dashboard/cycle-summary", "/dashboard/overview?timeframe=cycle", "/dashboard/overview?timeframe=month",
    "/dashboard/analytics", "/dashboard/cash-flow-summary", "/dashboard/net-worth",
    "/reports/monthly", f"/reports/monthly?basis=month&start_date={month_start}", "/reports/monthly/periods",
    "/review/summary", "/review/queue",
    "/transactions?include=type_counts,daily_totals&limit=200",
    *(f"/transactions?type={tab}&limit=200" for tab in ("expense", "income", "movement", "investment", "transfer")),
]
snapshot = {"taken_at": datetime.now(ZoneInfo("Asia/Jakarta")).isoformat(), "api": base, "responses": {}}
for path in paths:
    try:
        snapshot["responses"][path] = call(path)
    except urllib.error.HTTPError as error:
        snapshot["responses"][path] = {"http_status": error.code, "body": json.loads(error.read() or b"null")}
        print("non-200", path, error.code)
with open(out, "w") as handle:
    json.dump(snapshot, handle, ensure_ascii=False, indent=1, sort_keys=True)
print(out, len(paths), "responses")
