"""Adds the Catat states the parity seed lacks to a disposable owner (apply-markas-catat D24, local only).

Run after `seed_parity.py` and the migration to the latest version. Every addition goes through the app's own
endpoints, so balances, credit bills and debts stay the server's; the only exception is the reimbursement flag of
"Patungan & Ganti Uang", which only seeding sets, so `--database-url` sets it with one UPDATE on a disposable
`cft_markas_*` loopback database.

    MARKAS_PASSWORD=... python scripts/verify/markas/seed_catat.py --api http://127.0.0.1:8763 --username dimas_contoh \
        --origin http://127.0.0.1:3810 --database-url postgresql://...@127.0.0.1:8761/cft_markas_catat [--stress]

- SPayLater gets bill day 1 (its payment day and its bill due today stay as seeded);
- "Kredivo Contoh", a credit account with a limit and no payment day, owing Rp 250.000;
- a third active debt, so the split editor can take three rows;
- the reimbursement category, so Pemasukan offers "Mengurangi pengeluaran";
- with ``--stress``: a bank with 17 pockets, 60-character names and one balance of Rp 1.234.567.890.

It prints a JSON report of every addition and every skip with its reason. Synthetic data only.
"""
from __future__ import annotations

import argparse
import http.cookiejar
import json
import os
import sys
import urllib.error
import urllib.request
from urllib.parse import urlparse, urlsplit

import psycopg

DATABASE_PREFIX = "cft_markas_"
LOOPBACK = ("127.0.0.1", "localhost", "::1")
LONG_NAME = "Kantong Tabungan Pendidikan Anak dan Keperluan Sekolah Lanjut"  # 60 characters


class Api:
    def __init__(self, base: str, origin: str) -> None:
        if urlparse(base).hostname not in LOOPBACK:
            raise SystemExit(f"refused: {base} is not a loopback backend")
        self.base = base.rstrip("/")
        self.origin = origin
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))

    def call(self, method: str, path: str, body: dict | None = None) -> dict:
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            self.base + path, data=data, method=method, headers={"Content-Type": "application/json", "Origin": self.origin},
        )
        try:
            with self.opener.open(request) as response:
                return json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            raise RuntimeError(f"{method} {path} -> {error.code} {error.read()[:300]!r}") from error


def guard_database(url: str) -> None:
    parts = urlsplit(url)
    if parts.hostname not in LOOPBACK or not parts.path.lstrip("/").startswith(DATABASE_PREFIX):
        raise SystemExit(f"refused: only a loopback database named {DATABASE_PREFIX}* is changed")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--origin", default="http://127.0.0.1:3810", help="an origin the backend allows (APP_ORIGINS)")
    parser.add_argument("--database-url", required=True, help="disposable loopback cft_markas_* database for the reimbursement flag")
    parser.add_argument("--stress", action="store_true")
    args = parser.parse_args(argv)
    guard_database(args.database_url)
    password = os.environ.get("MARKAS_PASSWORD", "")
    if not password:
        raise SystemExit("MARKAS_PASSWORD is required")

    api = Api(args.api, args.origin)
    api.call("POST", "/auth/login", {"username": args.username, "password": password})
    report: dict = {"added": [], "skipped": []}

    def add(label: str, method: str, path: str, body: dict | None = None) -> dict:
        result = api.call(method, path, body)
        report["added"].append(label)
        return result

    accounts = {account["name"]: account for account in api.call("GET", "/accounts")["accounts"]}
    spaylater = accounts.get("SPayLater")
    if spaylater:
        add("SPayLater bill day 1", "PATCH", f"/accounts/{spaylater['id']}", {"statement_day": 1})
    else:
        report["skipped"].append({"addition": "SPayLater bill day", "reason": "no SPayLater in this seed"})
    if "Kredivo Contoh" in accounts:
        report["skipped"].append({"addition": "Kredivo Contoh", "reason": "already present"})
    else:
        add("credit Kredivo Contoh: limit, no payment day, owes Rp 250.000", "POST", "/accounts",
            {"name": "Kredivo Contoh", "type": "credit", "initial_balance": -250_000, "credit_limit": 2_000_000})

    debts = {debt["name"] for debt in api.call("GET", "/obligations")["obligations"]}
    if "Tagihan Internet Contoh" in debts:
        report["skipped"].append({"addition": "third debt", "reason": "already present"})
    else:
        add("debt Tagihan Internet Contoh Rp 350.000", "POST", "/obligations",
            {"name": "Tagihan Internet Contoh", "total_amount": 350_000, "remaining_amount": 350_000})

    with psycopg.connect(args.database_url) as connection:
        changed = connection.execute(
            "UPDATE categories SET offsets_spending = TRUE WHERE name = 'Patungan & Ganti Uang' AND kind = 'income' AND NOT offsets_spending"
        ).rowcount
    report["added" if changed else "skipped"].append(
        "reimbursement category Patungan & Ganti Uang" if changed else {"addition": "reimbursement category", "reason": "already set or absent"}
    )

    if args.stress:
        if "Bank Uji Panjang" in accounts:
            report["skipped"].append({"addition": "stress pockets", "reason": "already present"})
        else:
            parent = add("stress bank Bank Uji Panjang", "POST", "/accounts", {"name": "Bank Uji Panjang", "type": "bank", "initial_balance": 0})
            parent_id = parent.get("account", {}).get("id") or parent.get("id") or parent.get("account_id")
            for index in range(17):
                name = LONG_NAME if index == 0 else f"{LONG_NAME[:52]} {index + 1:02d}"
                balance = 1_234_567_890 if index == 0 else 10_000 * (index + 1)
                add(f"stress pocket {index + 1}", "POST", "/accounts",
                    {"name": name[:60], "type": "bank", "initial_balance": balance, "parent_id": parent_id})

    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
