"""Rename the parity owner's "Investasi" (expense) to "Investments" and both "Internal Movement" to "Transfer".

usage: rename.py <api base>   (local loopback, synthetic data; through PATCH /categories/{id}, as the owner would)
"""
import json
import sys
import urllib.request
from http.cookiejar import CookieJar

base = sys.argv[1]
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(CookieJar()))
HEADERS = {"Origin": "http://127.0.0.1:3601", "Content-Type": "application/json"}


def call(method, path, body=None):
    request = urllib.request.Request(base + path, data=json.dumps(body).encode() if body is not None else None,
                                     headers=HEADERS, method=method)
    with opener.open(request) as response:
        return json.loads(response.read())


call("POST", "/auth/login", {"username": "dimas_contoh", "password": "contoh-sandi-123"})
targets = {("Investasi", "expense"): "Investments", ("Internal Movement", "expense"): "Transfer",
           ("Internal Movement", "income"): "Transfer"}
for row in call("GET", "/categories")["categories"]:
    new_name = targets.get((row["name"], row["kind"]))
    if new_name:
        renamed = call("PATCH", f"/categories/{row['id']}", {"name": new_name})["category"]
        print(row["kind"], row["name"], "->", renamed["name"], "system_key:", row.get("system_key", "(field absent)"))
