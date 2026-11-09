"""Adds Target & Tagihan's states to a parity- and Rekening-seeded owner (apply-markas-target D26, local only).

Run after `seed_parity.py`, the migration to V54 and `seed_rekening.py`. Every addition goes through the app's own
endpoints, so goal progress, debt archiving, plan dates, credit bills and the detected charges stay the server's.

    MARKAS_PASSWORD=... python scripts/verify/markas/seed_target.py --api http://127.0.0.1:8741 --username dimas_contoh \
        [--origin http://127.0.0.1:3770] [--language en] [--stress] [--database-url postgresql://...cft_markas_*]

The parity and Rekening seeds already hold the flagged emergency goal, a reached goal, a standalone dated goal, a
paid-off debt, an overdue plan, a credit-bill plan due today, unpaid credit bills due today and overdue, and credit
spending in the cycle; this script verifies each of them and adds only what is missing:
- the emergency goal also linked to a parent and its pocket (the parent and pocket counted once);
- a goal without a date (standalone);
- a debt paid off through the API before the cycle (the server archives it);
- an expense plan due today;
- a monthly charge seen three times without a plan (a detected recurring charge);
- `kosong_contoh`, a second owner with one cash account and no history (unknown essential cost).
`--stress` adds 40 goals, 25 debts (10 paid off), 60 plans, five credit accounts with long names, 60-character names
and an Rp 1.234.567.890 goal. Dates are relative to today in Asia/Jakarta. It prints a JSON report of every addition,
every skip with its reason, and the API proof of each state. Synthetic data only.
"""
from __future__ import annotations

import argparse
import calendar
import http.cookiejar
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlparse

JAKARTA = timezone(timedelta(hours=7))
LOOPBACK = ("127.0.0.1", "localhost", "::1")
DATABASE_PREFIX = "cft_markas_"
EMPTY_OWNER = "kosong_contoh"
INVITE_CODE = "LOCALTEST"
LONG_GOAL = "Target Tabungan Pendidikan Anak dan Keperluan Sekolah Lanjutan"[:60]
LONG_DEBT = "Pinjaman Renovasi Rumah Keluarga Besar di Kampung Halaman Kami"[:60]
LONG_PLAN = "Pembayaran Iuran Bulanan Perumahan dan Keamanan Lingkungan RT"[:60]
CREDIT_NAMES = [
    "Kartu Kredit Platinum Rewards Bank Nusantara Contoh",
    "Paylater Belanja Harian Marketplace Contoh Panjang",
    "Kredit Tanpa Kartu Aplikasi Keuangan Contoh Sekali",
    "Cicilan Elektronik Toko Daring Contoh Nama Panjang",
    "Kartu Kredit Bisnis Usaha Kecil Menengah Contoh Uji",
]


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


def today() -> date:
    return datetime.now(JAKARTA).date()


def at(day: date, hour: int = 10) -> str:
    return datetime(day.year, day.month, day.day, hour, 0, tzinfo=JAKARTA).isoformat()


def months_back(day: date, months: int) -> date:
    index = day.year * 12 + day.month - 1 - months
    year, month = index // 12, index % 12 + 1
    return date(year, month, min(day.day, calendar.monthrange(year, month)[1]))


def flat(accounts: list[dict]) -> dict[str, dict]:
    found: dict[str, dict] = {}
    for account in accounts:
        found[account["name"]] = account
        for child in account.get("children") or []:
            found[child["name"]] = child
    return found


def guard_database(database_url: str | None) -> None:
    """Only a loopback cft_markas_* database may sit behind the API this script writes to."""
    if not database_url:
        return
    target = urlparse(database_url)
    name = target.path.lstrip("/")
    if target.hostname not in LOOPBACK or not name.startswith(DATABASE_PREFIX):
        raise SystemExit(f"refused: only a loopback {DATABASE_PREFIX}* database, not {target.hostname}/{name}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--origin", default="http://127.0.0.1:3770", help="an origin the backend allows (APP_ORIGINS)")
    parser.add_argument("--language", choices=["id", "en"], default=None, help="the owner's interface language")
    parser.add_argument("--stress", action="store_true")
    parser.add_argument("--database-url", default=None, help="the disposable database behind --api, checked by name only")
    args = parser.parse_args()
    password = os.environ.get("MARKAS_PASSWORD")
    if not password:
        raise SystemExit("MARKAS_PASSWORD is required")
    guard_database(args.database_url)

    api = Api(args.api, args.origin)
    api.call("POST", "/auth/login", {"username": args.username, "password": password})
    report: dict = {"added": [], "skipped": [], "present": [], "proof": {}}

    def add(label: str, method: str, path: str, body: dict | None = None) -> dict | None:
        try:
            response = api.call(method, path, body)
            report["added"].append(label)
            return response
        except RuntimeError as error:
            report["skipped"].append({"addition": label, "reason": str(error)[:240]})
            return None

    t = today()
    goals = {goal["name"]: goal for goal in api.call("GET", "/goals")["goals"]}
    if "Kamera Baru Contoh" in goals:
        raise SystemExit("refused: this owner already has the Target additions")
    accounts = flat(api.call("GET", "/accounts")["accounts"])
    categories = api.call("GET", "/categories")["categories"]
    expense_category = next(
        (c for c in categories if c.get("kind") == "expense" and not c.get("is_archived") and c.get("kakeibo_type") == "want"),
        next((c for c in categories if c.get("kind") == "expense" and not c.get("is_archived")), None),
    )
    need_category = next(
        (c for c in categories if c.get("kind") == "expense" and not c.get("is_archived") and c.get("kakeibo_type") == "need"),
        expense_category,
    )
    cash = accounts.get("Tunai")
    summary = api.call("GET", "/dashboard/cycle-summary")
    cycle_start = date.fromisoformat(summary["period"]["start_date"])

    # States the earlier seeds already hold; reported as present, never added twice.
    flagged = [goal for goal in goals.values() if goal.get("is_emergency")]
    reached = [goal for goal in goals.values() if goal["remaining_amount"] <= 0]
    standalone_dated = [goal for goal in goals.values() if not goal.get("linked_accounts") and goal.get("target_date")]
    report["present"] += [f"flagged emergency goal: {goal['name']}" for goal in flagged]
    report["present"] += [f"reached goal: {goal['name']}" for goal in reached]
    report["present"] += [f"standalone dated goal: {goal['name']}" for goal in standalone_dated]

    # The emergency goal also counts a parent with its own balance and one of its pockets (counted once).
    mandiri, pocket = accounts.get("Mandiri Contoh"), accounts.get("Belanja Bulanan")
    if len(flagged) == 1 and mandiri and pocket:
        goal = flagged[0]
        ids = list(dict.fromkeys([*(goal.get("account_ids") or []), mandiri["id"], pocket["id"]]))
        add("emergency goal linked to parent Mandiri Contoh and its pocket Belanja Bulanan", "PATCH", f"/goals/{goal['id']}",
            {"account_ids": ids})
    else:
        report["skipped"].append({"addition": "emergency goal parent and pocket", "reason": "needs one flagged goal and Mandiri Contoh"})

    add("goal without a date, tracked manually", "POST", "/goals",
        {"name": "Kamera Baru Contoh", "target_amount": 6_000_000, "current_amount": 1_250_000})

    # A debt paid off through the API before the cycle, so the server archives it and the cycle's figures stay.
    debt = add("debt Utang Arisan Contoh (Rp 300.000)", "POST", "/obligations",
               {"name": "Utang Arisan Contoh", "total_amount": 300_000, "remaining_amount": 300_000})
    if debt and cash:
        add("its payoff, three days before the cycle", "POST", "/transactions", {
            "type": "expense", "account_id": cash["id"], "amount": 300_000, "obligation_id": debt["obligation"]["id"],
            "notes": "Bayar arisan", "date": at(cycle_start - timedelta(days=3)),
            **({"category_id": need_category["id"]} if need_category else {}),
        })

    rules = {rule["name"]: rule for rule in api.call("GET", "/recurring")["rules"]}
    overdue = [rule for rule in rules.values() if rule["is_active"] and rule["next_due_date"] < t.isoformat()]
    report["present"] += [f"overdue plan: {rule['name']} ({rule['next_due_date']})" for rule in overdue]
    credit_plans = [rule for rule in rules.values() if rule["type"] == "transfer" and accounts.get(rule.get("target_account_name") or "", {}).get("type") == "credit"]
    report["present"] += [f"credit-bill plan: {rule['name']} ({rule['next_due_date']})" for rule in credit_plans]
    if cash and expense_category:
        add("expense plan due today: Iuran Kebersihan Contoh", "POST", "/recurring", {
            "name": "Iuran Kebersihan Contoh", "type": "expense", "amount": 50_000, "source_account_id": cash["id"],
            "category_id": need_category["id"] if need_category else expense_category["id"],
            "schedule_type": "monthly_day", "schedule_day": t.day,
        })
        # Three monthly charges, the last 20 days ago: the next is expected inside the 30-day window.
        for months in (2, 1, 0):
            day = months_back(t - timedelta(days=20), months)
            add(f"charge Langganan Video Contoh {day.isoformat()}", "POST", "/transactions", {
                "type": "expense", "account_id": cash["id"], "amount": 65_000, "notes": "Langganan Video Contoh",
                "category_id": expense_category["id"], "date": at(day),
            })
    else:
        report["skipped"].append({"addition": "plan due today and detected charge", "reason": "no Tunai or expense category"})

    if args.stress:
        source = cash or next(iter(accounts.values()))
        for index in range(40):
            name = LONG_GOAL if index == 0 else f"Target Uji Contoh {index + 1:02d}"
            body = {"name": name, "target_amount": 1_234_567_890 if index == 1 else 5_000_000 + index * 250_000,
                    "current_amount": (index * 137_000) % 5_000_000}
            if index % 3:
                body["target_date"] = (t + timedelta(days=60 + index * 9)).isoformat()
            add(f"stress goal {index + 1}", "POST", "/goals", body)
        for index in range(25):
            name = LONG_DEBT if index == 0 else f"Utang Uji Contoh {index + 1:02d}"
            total = 2_000_000 + index * 100_000
            paid_off = index >= 15
            add(f"stress debt {index + 1}{' (paid off)' if paid_off else ''}", "POST", "/obligations", {
                "name": name, "total_amount": total, "remaining_amount": 0 if paid_off else total - index * 50_000,
                "minimum_payment": 250_000, "due_date": (t + timedelta(days=index % 28 + 1)).isoformat(),
            })
        for index in range(60):
            add(f"stress plan {index + 1}", "POST", "/recurring", {
                "name": LONG_PLAN if index == 0 else f"Rencana Uji Contoh {index + 1:02d}", "type": "expense",
                "amount": 25_000 + index * 1_000, "source_account_id": source["id"],
                "category_id": expense_category["id"] if expense_category else None,
                "schedule_type": "monthly_day", "schedule_day": index % 28 + 1,
            })
        for index, name in enumerate(CREDIT_NAMES):
            add(f"stress credit {name}", "POST", "/accounts", {
                "name": name, "type": "credit", "initial_balance": -(400_000 + index * 150_000), "credit_limit": 3_000_000,
                "payment_day": (t.day + index) % 28 + 1, "statement_day": (t.day + index + 10) % 28 + 1,
            })

    if args.language:
        add(f"interface language {args.language}", "PATCH", "/auth/settings", {"language": args.language})

    # A second owner with no history: Ketahanan Dana cannot be calculated (unknown essential cost).
    empty = Api(args.api, args.origin)
    try:
        empty.call("POST", "/auth/register", {
            "username": EMPTY_OWNER, "password": password, "invite_code": INVITE_CODE, "name": "Kosong Contoh",
            "language": args.language or "id",
        })
        empty.call("POST", "/accounts", {"name": "Dompet Contoh", "type": "cash", "initial_balance": 500_000})
        report["added"].append(f"owner {EMPTY_OWNER} with one cash account and no history")
        runway = empty.call("GET", "/dashboard/net-worth")["runway"]
        report["proof"]["empty_owner_runway"] = {key: runway.get(key) for key in ("status", "coverage_months", "runway_days", "monthly_primary_expense")}
    except RuntimeError as error:
        report["skipped"].append({"addition": f"owner {EMPTY_OWNER}", "reason": str(error)[:240]})

    # Proof, read back through the API.
    obligations = api.call("GET", "/obligations?include_archived=true")["obligations"]
    report["proof"]["paid_off_debts"] = [o["name"] for o in obligations if o["is_archived"] or o["remaining_amount"] <= 0]
    report["proof"]["active_debts"] = [o["name"] for o in obligations if not o["is_archived"] and o["remaining_amount"] > 0]
    bills = api.call("GET", "/credit/bills?status=due_today,overdue")
    report["proof"]["credit_bills_due_today_overdue"] = [
        {key: bill[key] for key in ("account_name", "due_date", "status", "left_to_pay")} for bill in bills["bills"]
    ]
    report["proof"]["due_dates_unavailable"] = [item["account_name"] for item in bills["due_dates_unavailable"]]
    summary = api.call("GET", "/dashboard/cycle-summary")
    report["proof"]["spending_credit"] = {
        key: summary["spending"].get("credit", {}).get(key) for key in ("spent_on_credit", "paid_to_credit")
    }
    report["proof"]["spending"] = summary["spending"]["amount"]
    report["proof"]["scheduled_total"] = summary["scheduled"]["total"]
    report["proof"]["credit_plan_lines"] = [
        {key: line.get(key) for key in ("label", "due_date", "amount", "plan_amount", "counted_elsewhere", "credit_account_id")}
        for line in summary["scheduled"]["lines"] if line.get("credit_account_id")
    ]
    report["proof"]["plans_due_today_or_overdue"] = [
        f"{rule['name']} {rule['next_due_date']}" for rule in api.call("GET", "/recurring")["rules"]
        if rule["is_active"] and rule["next_due_date"] <= t.isoformat()
    ]
    charges = api.call("GET", "/dashboard/recurring-charges")["charges"]
    report["proof"]["detected_charges_without_plan"] = [
        f"{charge['merchant']} next {charge['next_expected_date']}" for charge in charges if not charge.get("existing_plan")
    ]
    runway = api.call("GET", "/dashboard/net-worth")["runway"]
    report["proof"]["runway"] = {key: runway.get(key) for key in (
        "coverage_months", "coverage_pct", "target_amount", "emergency_fund_balance", "monthly_primary_expense", "runway_days", "status",
    )}
    report["proof"]["goals"] = [
        {"name": g["name"], "current": g["current_amount"], "target": g["target_amount"], "emergency": bool(g.get("is_emergency")),
         "date": g.get("target_date"), "linked": len(g.get("linked_accounts") or [])}
        for g in api.call("GET", "/goals")["goals"]
    ][:12]

    json.dump({"added": len(report["added"]), "additions": report["added"], "skipped": report["skipped"],
               "present": report["present"], "proof": report["proof"]}, sys.stdout, ensure_ascii=False, indent=1)
    print()


if __name__ == "__main__":
    main()
