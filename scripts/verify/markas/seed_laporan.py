"""Adds Laporan's cases to a seeded owner through the API (apply-markas-laporan D23, local only).

Run after seed_parity.py, seed_transaksi.py and seed_analisis.py against a loopback backend serving a disposable
`cft_markas_*` database. Every write goes through the app's own endpoints:

    MARKAS_PASSWORD=... python scripts/verify/markas/seed_laporan.py --api http://127.0.0.1:8721 --username dimas_contoh [--origin <url>] [--stress]

The base adds whole months of history from the 15th of the month four months back (a partial first month), so the
report has four completed months and the running one; three unexpected items and a Rp 1.600.000 expense with the
Tabungan pillar at the largest merchant of last month (the one-basis case, D29); an expense without a pillar; and
a note on the month before last. `--stress` adds a 1000-character note on last month. It prints a JSON report.
Synthetic data only.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent))
from seed_transaksi import Api, flat_accounts  # noqa: E402

JAKARTA = ZoneInfo("Asia/Jakarta")
SAVING_MERCHANT = "Reksa Dana Contoh"
HISTORY = [
    # (months back, day, category, merchant, amount)
    (4, 15, "Makanan & Minuman", "Warung Makan Contoh", 1_850_000),
    (4, 18, "Transportasi", "SPBU Contoh", 640_000),
    (4, 22, "Belanja Dapur", "Swalayan Contoh", 1_120_000),
    (3, 2, "Makanan & Minuman", "Warung Makan Contoh", 1_920_000),
    (3, 6, "Transportasi", "SPBU Contoh", 610_000),
    (3, 12, "Belanja Dapur", "Swalayan Contoh", 1_310_000),
    (3, 20, "Tagihan & Utilitas", "Internet Rumah Contoh", 385_000),
    (3, 25, "Hiburan", "Bioskop Contoh", 270_000),
    (2, 3, "Makanan & Minuman", "Warung Makan Contoh", 2_040_000),
    (2, 7, "Transportasi", "SPBU Contoh", 590_000),
    (2, 14, "Belanja Dapur", "Swalayan Contoh", 1_180_000),
    (2, 19, "Tagihan & Utilitas", "Internet Rumah Contoh", 385_000),
    (2, 24, "Hiburan", "Bioskop Contoh", 340_000),
]


def month_day(months_back: int, day: int, hour: int = 12) -> str:
    today = datetime.now(JAKARTA).date()
    index = today.year * 12 + today.month - 1 - months_back
    moment = datetime(index // 12, index % 12 + 1, day, hour, 0, tzinfo=JAKARTA)
    return moment.isoformat()


def month_start(months_back: int) -> date:
    today = datetime.now(JAKARTA).date()
    index = today.year * 12 + today.month - 1 - months_back
    return date(index // 12, index % 12 + 1, 1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--origin", default="http://127.0.0.1:3733", help="an origin the backend allows (APP_ORIGINS)")
    parser.add_argument("--stress", action="store_true")
    args = parser.parse_args()
    password = os.environ.get("MARKAS_PASSWORD")
    if not password:
        raise SystemExit("MARKAS_PASSWORD is required")

    api = Api(args.api, args.origin)
    api.call("POST", "/auth/login", {"username": args.username, "password": password})
    accounts = flat_accounts(api.call("GET", "/accounts")["accounts"])
    categories = [category for category in api.call("GET", "/categories")["categories"] if not category.get("system_key")]
    expense = {category["name"]: category for category in categories if category.get("kind") == "expense"}
    income = [category for category in categories if category.get("kind") == "income"]
    report: dict[str, list] = {"added": [], "skipped": []}

    def add(label: str, method: str, path: str, body: dict | None = None) -> dict | None:
        try:
            response = api.call(method, path, body)
            report["added"].append(label)
            return response
        except RuntimeError as error:
            report["skipped"].append({"addition": label, "reason": str(error)[:200]})
            return None

    harian = accounts.get("Utama") or accounts.get("Kantong Utama")
    if not harian or not expense:
        raise SystemExit("the parity seed's Utama account and its spending categories are needed")
    fallback = next(iter(expense.values()))
    pick = lambda name: expense.get(name) or fallback  # noqa: E731

    salary = income[0] if income else None
    for months_back in (4, 3, 2):
        if salary:
            day = 25 if months_back == 4 else 1
            add(f"income {months_back} months back", "POST", "/transactions", {"type": "income", "account_id": harian["id"], "amount": 9_500_000, "category_id": salary["id"], "notes": "Gaji contoh", "date": month_day(months_back, day, 9)})
    for months_back, day, name, merchant, amount in HISTORY:
        add(f"{merchant} {months_back} months back", "POST", "/transactions", {"type": "expense", "account_id": harian["id"], "amount": amount, "category_id": pick(name)["id"], "merchant": merchant, "date": month_day(months_back, day)})

    # Last month: the largest merchant holds only an expense with the Tabungan pillar (D29), three unexpected items
    # and one expense without a pillar.
    add("saving-pillar expense at the largest merchant", "POST", "/transactions", {"type": "expense", "account_id": harian["id"], "amount": 1_600_000, "category_id": pick("Belanja Dapur")["id"], "kakeibo_type": "saving", "merchant": SAVING_MERCHANT, "notes": "Setoran reksa dana", "date": month_day(1, 12, 10)})
    unexpected = next((category for category in expense.values() if category.get("kakeibo_type") == "unexpected"), None)
    for index, (merchant, amount) in enumerate((("Bengkel Motor Contoh", 425_000), ("Apotek Contoh", 118_000), ("Tukang Kunci Contoh", 95_000))):
        body = {"type": "expense", "account_id": harian["id"], "amount": amount, "merchant": merchant, "date": month_day(1, 6 + index * 5, 15)}
        body.update({"category_id": unexpected["id"]} if unexpected else {"category_id": fallback["id"], "kakeibo_type": "unexpected"})
        add(f"unexpected {merchant}", "POST", "/transactions", body)
    no_pillar = next((category for category in expense.values() if not category.get("kakeibo_type")), None)
    if no_pillar:
        add("expense without a pillar", "POST", "/transactions", {"type": "expense", "account_id": harian["id"], "amount": 64_000, "category_id": no_pillar["id"], "merchant": "Toko Serba Ada Contoh", "date": month_day(1, 21, 13)})

    add("note on the month before last", "PUT", "/reports/monthly/reflection", {"basis": "month", "start_date": month_start(2).isoformat(), "note": "Kurangi jajan kopi dan masak di rumah tiga kali seminggu."})
    if args.stress:
        long_note = ("Masak di rumah, bawa bekal ke kantor, dan catat setiap jajan kecil supaya pengeluaran harian tetap terkendali. " * 9)[:1000]
        add("1000-character note on last month", "PUT", "/reports/monthly/reflection", {"basis": "month", "start_date": month_start(1).isoformat(), "note": long_note})
    print(json.dumps(report, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
