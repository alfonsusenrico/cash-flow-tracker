"""Adds Transaksi's row kinds to a seeded owner through the API (apply-markas-transaksi D24, local only).

Every addition goes through the same endpoints the app uses, so balances, movement pairs, top-ups and credit bills
stay consistent. Run it against a loopback backend serving a disposable `cft_markas_*` database:

    MARKAS_PASSWORD=... python scripts/verify/markas/seed_transaksi.py --api http://127.0.0.1:8711 --username dimas_contoh [--origin <url>] [--stress]

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
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

JAKARTA = timezone(timedelta(hours=7))
LONG_MERCHANT = "Toko Peralatan Rumah Tangga dan Dapur Sejahtera Makmur Abadi"


class Api:
    def __init__(self, base: str, origin: str) -> None:
        host = urlparse(base).hostname
        if host not in ("127.0.0.1", "localhost"):
            raise SystemExit(f"refused: {base} is not a loopback backend")
        self.base = base.rstrip("/")
        # Writes need an allowed origin, as the browser sends; the web's loopback origin is used.
        self.origin = origin
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))

    def call(self, method: str, path: str, body: dict | None = None) -> dict:
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(self.base + path, data=data, method=method, headers={"Content-Type": "application/json", "Origin": self.origin})
        try:
            with self.opener.open(request) as response:
                return json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            raise RuntimeError(f"{method} {path} -> {error.code} {error.read()[:300]!r}") from error


def at(days_ago: int, hour: int, minute: int = 0) -> str:
    day = datetime.now(JAKARTA).replace(hour=hour, minute=minute, second=0, microsecond=0) - timedelta(days=days_ago)
    return day.isoformat()


def flat_accounts(accounts: list[dict]) -> dict[str, dict]:
    found: dict[str, dict] = {}
    for account in accounts:
        found[account["name"]] = account
        for child in account.get("children") or []:
            found[child["name"]] = child
    return found


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--origin", default="http://127.0.0.1:3721", help="an origin the backend allows (APP_ORIGINS)")
    parser.add_argument("--stress", action="store_true", help="1,050 more rows, 60-character merchants and Rp 1.234.567.890")
    args = parser.parse_args()
    password = os.environ.get("MARKAS_PASSWORD")
    if not password:
        raise SystemExit("MARKAS_PASSWORD is required")

    api = Api(args.api, args.origin)
    api.call("POST", "/auth/login", {"username": args.username, "password": password})
    accounts = flat_accounts(api.call("GET", "/accounts")["accounts"])
    categories = {category["name"]: category for category in api.call("GET", "/categories")["categories"]}
    report: dict[str, list] = {"added": [], "skipped": []}

    def need(name: str) -> str | None:
        account = accounts.get(name)
        if account is None:
            report["skipped"].append({"account": name, "reason": "not in this owner's seed"})
        return account["id"] if account else None

    def add(label: str, method: str, path: str, body: dict) -> dict | None:
        try:
            response = api.call(method, path, body)
            report["added"].append(label)
            return response
        except RuntimeError as error:
            report["skipped"].append({"addition": label, "reason": str(error)[:200]})
            return None

    harian = need("Kantong Utama")
    tabungan = need("Liburan")
    transport = need("Transport")
    rdn = need("RDN Stockbit")
    spaylater = need("SPayLater")
    gopay = need("GoPay")
    # The busy day and the stress rows are paid from the account with the most money, so no row is refused.
    busy = need("Utama")
    product = next((account for account in accounts.values() if account.get("instrument_type") == "mutual_fund" and account.get("units") in (None, 0)), None)
    # An ordinary spending category: a system category (a movement, a fee) would make the row something else.
    ordinary = [category for category in categories.values() if category.get("kind") == "expense" and not category.get("system_key")]
    makan = categories.get("Makanan & Minuman") or (ordinary[0] if ordinary else None)
    if makan is None:
        report["skipped"].append({"category": "ordinary spending", "reason": "no spending category without a system key"})

    # One movement per saving direction of D27, today and yesterday, with and without a note.
    if harian and tabungan:
        add("saved: harian → tabungan, noted", "POST", "/movements", {"source_account_id": harian, "target_account_id": tabungan, "amount": 400_000, "notes": "Sisihkan liburan", "date": at(0, 7, 10)})
        add("taken: tabungan → harian, no note", "POST", "/movements", {"source_account_id": tabungan, "target_account_id": harian, "amount": 150_000, "date": at(0, 7, 20)})
    if harian and rdn:
        add("null: harian → dana_investasi", "POST", "/movements", {"source_account_id": harian, "target_account_id": rdn, "amount": 250_000, "notes": "Isi RDN", "date": at(1, 9, 5)})
    if harian and gopay:
        add("null: harian → harian, noted", "POST", "/movements", {"source_account_id": harian, "target_account_id": gopay, "amount": 100_000, "notes": "Isi GoPay", "date": at(0, 8, 0)})
        add("null: harian → harian, no note", "POST", "/movements", {"source_account_id": harian, "target_account_id": gopay, "amount": 50_000, "date": at(1, 8, 0)})
    if transport:
        goal = add("goal linked to the Transport pocket", "POST", "/goals", {"name": "Motor Baru Contoh", "target_amount": 15_000_000, "account_ids": [transport]})
        if goal and harian:
            add("saved: harian → goal-linked pocket", "POST", "/movements", {"source_account_id": harian, "target_account_id": transport, "amount": 120_000, "notes": "Tabungan motor", "date": at(1, 10, 0)})
    if product and tabungan:
        add("null: tabungan → product top-up", "POST", "/investment-topups", {"source_account_id": tabungan, "target_account_id": product["id"], "amount": 200_000, "date": at(1, 11, 0), "notes": "", "idempotency_key": "seed-transaksi-topup-tabungan"})
    elif not product:
        report["skipped"].append({"addition": "tabungan → product top-up", "reason": "no amount-tracked mutual fund in this seed"})
    if spaylater and tabungan:
        add("taken: tabungan → credit", "POST", "/movements", {"source_account_id": tabungan, "target_account_id": spaylater, "amount": 75_000, "notes": "Bayar SPayLater", "date": at(1, 12, 0)})
    if spaylater and harian:
        add("debt_payment with a fee: harian → credit", "POST", "/movements", {"source_account_id": harian, "target_account_id": spaylater, "amount": 60_000, "fee": 2_500, "date": at(2, 9, 0)})

    # Expenses: paid from savings, saving pillar, a credit purchase in instalments, a reimbursement.
    if tabungan and makan:
        add("paid_from_savings: expense from tabungan", "POST", "/transactions", {"type": "expense", "account_id": tabungan, "amount": 85_000, "category_id": makan["id"], "notes": "Tiket kereta liburan", "date": at(0, 12, 30)})
    if harian:
        add("saving_pillar: expense with the saving pillar", "POST", "/transactions", {"type": "expense", "account_id": harian, "amount": 300_000, "kakeibo_type": "saving", "notes": "Arisan keluarga", "date": at(1, 13, 0)})
    if spaylater and makan:
        add("credit purchase in 3 instalments", "POST", "/transactions", {"type": "expense", "account_id": spaylater, "amount": 450_000, "category_id": makan["id"], "notes": "Sepatu lari", "date": at(0, 19, 15), "credit_terms": {"installment_count": 3}})
        add("credit purchase paid in full", "POST", "/transactions", {"type": "expense", "account_id": spaylater, "amount": 120_000, "category_id": makan["id"], "notes": "Makan malam", "date": at(1, 19, 30)})

    # A day with more than one page of rows, so a day continues on the next page.
    if busy and makan:
        for index in range(56):
            add(f"busy day row {index + 1}", "POST", "/transactions", {"type": "expense", "account_id": busy, "amount": 5_000 + index * 100, "category_id": makan["id"], "notes": f"Jajan pasar {index + 1}", "date": at(3, 6 + index // 6, (index % 6) * 9)})

    if args.stress and busy and makan:
        add("Rp 1.234.567.890 income", "POST", "/transactions", {"type": "income", "account_id": busy, "amount": 1_300_000_000, "notes": "Saldo uji", "date": at(0, 5, 0)})
        add("Rp 1.234.567.890 expense", "POST", "/transactions", {"type": "expense", "account_id": busy, "amount": 1_234_567_890, "category_id": makan["id"], "notes": LONG_MERCHANT, "date": at(0, 6, 0)})
        for index in range(1_050):
            add(f"stress row {index + 1}", "POST", "/transactions", {"type": "expense", "account_id": busy, "amount": 10_000 + index, "category_id": makan["id"], "notes": f"{LONG_MERCHANT[:56]} {index:03d}"[:60], "date": at(4 + index // 40, 6 + (index % 40) // 4, (index % 4) * 10)})

    summary = {
        "added": len(report["added"]),
        "skipped": report["skipped"],
        "first": report["added"][:30],
    }
    json.dump(summary, sys.stdout, ensure_ascii=False, indent=1)
    print()


if __name__ == "__main__":
    main()
