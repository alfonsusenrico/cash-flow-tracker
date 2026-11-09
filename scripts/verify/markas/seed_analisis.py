"""Adds Analisis's cases to a seeded owner through the API (apply-markas-analisis D26, local only).

Run after seed_parity.py and seed_transaksi.py against a loopback backend serving a disposable `cft_markas_*`
database. Every write goes through the app's own endpoints:

    MARKAS_PASSWORD=... python scripts/verify/markas/seed_analisis.py --api http://127.0.0.1:8721 --username dimas_contoh [--origin <url>] [--stress]

The base adds budgets (one category over its budget), an archived category with spending in the running period
and an expense without a category. `--stress` adds a 40-character account name, 60-character merchants, a
Rp 1.234.567.890 day, 30 categories and 12 detected charges. It prints a JSON report. Synthetic data only.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from seed_transaksi import LONG_MERCHANT, Api, at, flat_accounts  # noqa: E402

LONG_ACCOUNT = "Rekening Tabungan Pendidikan Anak Pertama"


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
    categories = [category for category in api.call("GET", "/categories")["categories"] if category.get("kind") == "expense" and not category.get("system_key")]
    report: dict[str, list] = {"added": [], "skipped": []}

    def add(label: str, method: str, path: str, body: dict | None = None) -> dict | None:
        try:
            response = api.call(method, path, body)
            report["added"].append(label)
            return response
        except RuntimeError as error:
            report["skipped"].append({"addition": label, "reason": str(error)[:200]})
            return None

    # Spending comes from the account with the most money, so no row is refused for insufficient funds.
    harian = accounts.get("Utama") or accounts.get("Kantong Utama")
    if not harian or len(categories) < 2:
        raise SystemExit("the parity seed's Utama account and two spending categories are needed")
    by_name = {category["name"]: category for category in categories}
    makan = by_name.get("Makanan & Minuman") or categories[0]
    second = next(category for category in categories if category["id"] != makan["id"])
    # One budget comfortably above its spending, one below it, so a row reads over 100 %.
    add(f"budget 5.000.000 on {makan['name']}", "PATCH", f"/categories/{makan['id']}", {"monthly_budget": 5_000_000})
    add(f"budget 100.000 on {second['name']}", "PATCH", f"/categories/{second['id']}", {"monthly_budget": 100_000})
    add(f"spending on {second['name']}", "POST", "/transactions", {"type": "expense", "account_id": harian["id"], "amount": 135_000, "category_id": second["id"], "notes": "Langganan bulanan", "date": at(2, 11, 0)})
    archived = add("category to archive", "POST", "/categories", {"name": "Langganan Lama Contoh", "icon": "film", "color": "#64748b", "kind": "expense", "monthly_budget": None, "kakeibo_type": "want"})
    if archived:
        archived_id = archived.get("id") or archived.get("category", {}).get("id")
        add("spending on the category to archive", "POST", "/transactions", {"type": "expense", "account_id": harian["id"], "amount": 49_000, "category_id": archived_id, "notes": "Langganan terakhir", "date": at(3, 20, 0)})
        add("archive it", "DELETE", f"/categories/{archived_id}")
    add("expense without a category", "POST", "/transactions", {"type": "expense", "account_id": harian["id"], "amount": 27_500, "notes": "Parkir tanpa kategori", "date": at(1, 16, 30)})

    if args.stress:
        add("40-character account name", "PATCH", f"/accounts/{harian['id']}", {"name": LONG_ACCOUNT})
        add("Rp 1.234.567.890 in one day", "POST", "/transactions", {"type": "expense", "account_id": harian["id"], "amount": 1_234_567_890, "category_id": makan["id"], "merchant": LONG_MERCHANT, "notes": "Pembelian besar", "date": at(4, 10, 0)})
        for index in range(len(categories), 30):
            created = add(f"category {index + 1}", "POST", "/categories", {"name": f"Kategori Uji Panjang Nomor {index + 1:02d}", "icon": "tag", "color": "#3b82f6", "kind": "expense", "monthly_budget": 250_000 if index % 3 == 0 else None, "kakeibo_type": ["need", "want", "culture", "unexpected"][index % 4]})
            category_id = created and (created.get("id") or created.get("category", {}).get("id"))
            if category_id:
                add(f"spending in category {index + 1}", "POST", "/transactions", {"type": "expense", "account_id": harian["id"], "amount": 10_000 + index * 1_000, "category_id": category_id, "date": at(index % 6, 9, index % 60)})
        # Monthly charges on the same day of the month for four months, so the detector finds twelve. The detector
        # groups merchants without their digits, so each one is named by a word.
        branches = ["Bandung", "Bekasi", "Bogor", "Cirebon", "Depok", "Garut", "Jakarta", "Malang", "Medan", "Padang", "Solo", "Tegal"]
        for charge, branch in enumerate(branches):
            merchant = f"{LONG_MERCHANT[:44]} {branch}"
            for month in range(4):
                add(f"charge {charge + 1} month {month}", "POST", "/transactions", {"type": "expense", "account_id": harian["id"], "amount": 49_000 + charge * 1_000, "category_id": makan["id"], "merchant": merchant, "date": at(30 * month + charge % 5 + 1, 8, charge)})
    print(json.dumps(report, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
