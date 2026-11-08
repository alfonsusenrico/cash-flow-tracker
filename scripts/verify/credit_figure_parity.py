"""Figure parity for add-credit-spending-flow (task 6.3): snapshot every figure endpoint on a synthetic seed.

Run once with the backend before the change on a V53 database, once with the changed backend on a V54
database, then compare the two snapshots:

    PYTHONPATH=<backend> DATABASE_URL=<disposable db> python scripts/verify/credit_figure_parity.py snapshot <out.json>
    python scripts/verify/credit_figure_parity.py compare <before.json> <after.json>

The seed has three synthetic owners: one without credit, one with credit purchases only, and one with credit
purchases and payments. None has a credit purchase term or a transfer plan into a credit account, so every
figure must be equal; the fields this change adds are removed before comparing. Identifiers and the instants
of the run are normalised, and lists are compared as multisets, because they differ between runs by design.
"""
from __future__ import annotations

import json
import os
import re
import sys
from datetime import date, datetime, time, timedelta
from uuid import uuid4
from zoneinfo import ZoneInfo

JAKARTA = ZoneInfo("Asia/Jakarta")
UUID_PATTERN = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
INSTANT_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2})T\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:\d{2}|Z)?$")
VOLATILE_KEYS = {"generated_at", "as_of", "created_at", "updated_at", "linked_at", "last_price_at"}
# Fields added by add-credit-spending-flow; nothing else may differ.
ADDED_FIELDS = {"statement_day", "limit_left", "next_due", "credit"}
ADDED_KEY_VALUES = {"system_key": "credit_fee", "category_system_key": "credit_fee"}


def environment() -> None:
    os.environ.setdefault("SESSION_SECRET", "parity-secret")
    os.environ.setdefault("INVITE_CODE", "PARITY")
    os.environ.setdefault("APP_ENV", "test")
    os.environ.setdefault("COOKIE_SECURE", "false")
    os.environ.setdefault("REDIS_URL", "")
    os.environ.setdefault("PLAN_AUTO_LINK_ENABLED", "false")
    os.environ.setdefault("BACKGROUND_JOBS_ENABLED", "false")


def local(day: date, hour: int) -> str:
    return datetime.combine(day, time(hour), JAKARTA).isoformat()


class Seeder:
    def __init__(self, client, app, label: str):
        from app.db.init_db import seed_user_defaults
        from app.db.pool import db_conn
        from app.services.auth import get_current_user

        self.client, self.app = client, app
        with db_conn() as conn:
            self.user_id = str(conn.execute(
                "INSERT INTO users (username, password_hash, invite_code, language) VALUES (%s, 'synthetic', 'PARITY', 'id') RETURNING id",
                (f"parity_{label}_{uuid4().hex[:8]}",),
            ).fetchone()["id"])
            conn.commit()
        seed_user_defaults(self.user_id, "id")
        user = {"id": self.user_id, "username": f"parity_{label}", "name": "Pemilik Contoh", "currency": "IDR",
                "payday_day": 25, "language": "id", "region": "ID", "monthly_spending_budget": 3_000_000}
        self.user = user
        app.dependency_overrides[get_current_user] = lambda: user
        with db_conn() as conn:
            self.categories = {f"{row['name']}/{row['kind']}": str(row["id"]) for row in conn.execute(
                "SELECT id, name, kind FROM categories WHERE user_id = %s", (self.user_id,)).fetchall()}
        self.accounts: dict[str, str] = {}

    def post(self, path: str, body: dict) -> dict:
        response = self.client.post(path, json=body)
        if response.status_code != 200:
            raise RuntimeError(f"{path}: {response.status_code} {response.text}")
        return response.json()

    def account(self, key: str, name: str, **fields) -> str:
        self.accounts[key] = self.post("/api/accounts", {"name": name, **fields})["account"]["id"]
        return self.accounts[key]

    def record(self, account: str, category: str, amount: int, day: date, hour: int, tx_type="expense", **extra) -> str:
        return self.post("/api/transactions", {
            "account_id": self.accounts[account], "category_id": self.categories[category], "type": tx_type,
            "amount": amount, "date": local(day, hour), **extra,
        })["transaction_id"]

    def move(self, source: str, target: str, amount: int, day: date, hour: int, **extra) -> dict:
        return self.post("/api/movements", {
            "source_account_id": self.accounts[source], "target_account_id": self.accounts[target], "amount": amount,
            "date": local(day, hour), **extra,
        })

    def plan(self, name: str, rule_type: str, amount: int, due: date, source: str, target: str | None = None, category=None):
        body = {"name": name, "type": rule_type, "amount": amount, "source_account_id": self.accounts[source],
                "schedule_type": "monthly_day", "schedule_day": due.day}
        if target:
            body["target_account_id"] = self.accounts[target]
        if category:
            body["category_id"] = self.categories[category]
        return self.post("/api/recurring", body)


def category(seeder: Seeder, seed_key: str, kind: str) -> str:
    from app.db.init_db import STARTER_NAMES

    return f"{STARTER_NAMES['id'][seed_key]}/{kind}"


def seed_common(seeder: Seeder, today: date) -> None:
    seeder.account("bank", "Bank Contoh", type="bank", initial_balance=15_000_000)
    seeder.account("tabungan", "Tabungan Contoh", type="bank", role="tabungan")
    start = today.replace(day=1)
    seeder.record("bank", category(seeder, "salary", "income"), 9_000_000, start, 8, "income")
    seeder.record("bank", category(seeder, "food", "expense"), 250_000, start, 12)
    seeder.record("bank", category(seeder, "shopping", "expense"), 175_000, start, 13)
    seeder.record("bank", category(seeder, "reimbursement", "income"), 50_000, start, 15, "income",
                  offset_category_id=seeder.categories[category(seeder, "food", "expense")])
    seeder.move("bank", "tabungan", 1_000_000, start, 14)
    seeder.plan("Listrik Contoh", "expense", 400_000, today + timedelta(days=4), "bank",
                category=category(seeder, "bills", "expense"))
    seeder.plan("Isi Tabungan Contoh", "transfer", 500_000, today + timedelta(days=6), "bank", target="tabungan")


def seed(client, app) -> list[tuple[str, Seeder]]:
    today = datetime.now(JAKARTA).date()
    start = today.replace(day=1)
    plain = Seeder(client, app, "plain")
    seed_common(plain, today)

    purchases = Seeder(client, app, "purchases")
    seed_common(purchases, today)
    purchases.account("credit", "PayLater Contoh", type="credit", initial_balance=-350_000, credit_limit=2_000_000, payment_day=11)
    purchases.record("credit", category(purchases, "food", "expense"), 120_000, start, 19)
    purchases.record("credit", category(purchases, "shopping", "expense"), 80_000, start, 20)
    purchases.record("credit", category(purchases, "other_income", "income"), 20_000, start, 21, "income")

    payments = Seeder(client, app, "payments")
    seed_common(payments, today)
    payments.account("credit", "PayLater Contoh", type="credit", initial_balance=-470_000, credit_limit=1_000_000, payment_day=11)
    payments.record("credit", category(payments, "food", "expense"), 300_000, start, 19)
    payments.record("credit", category(payments, "shopping", "expense"), 900_000, start, 20)
    payments.move("bank", "credit", 480_000, start, 21, fee=10_000)
    payments.move("tabungan", "credit", 200_000, start, 22)
    payments.move("credit", "bank", 50_000, start, 23)
    return [("plain", plain), ("purchases", purchases), ("payments", payments)]


PATHS = (
    "/api/pulse", "/api/insights", "/api/dashboard/overview?timeframe=cycle", "/api/dashboard/overview?timeframe=month",
    "/api/dashboard/cycle-summary", "/api/dashboard/analytics", "/api/dashboard/cash-flow-summary",
    "/api/dashboard/net-worth", "/api/reports/monthly/periods", "/api/review/summary", "/api/accounts",
    "/api/recurring", "/api/categories", "/api/transactions?include=type_counts,daily_totals&limit=200",
    "/api/transactions?logical_movements=true&period=month&limit=200",
    *(f"/api/transactions?type={tab}&limit=200" for tab in ("expense", "income", "movement", "investment", "transfer")),
)


def signed_in(user: dict):
    """A dependency override without parameters, so FastAPI reads nothing from the request for it."""
    return lambda: user


def snapshot(out_path: str) -> None:
    environment()
    from fastapi.testclient import TestClient

    from app.main import app
    from app.services.auth import get_current_user

    result: dict[str, dict] = {}
    month_start = datetime.now(JAKARTA).date().replace(day=1).isoformat()
    with TestClient(app, base_url="https://testserver") as client:
        client.headers["Origin"] = "https://testserver"
        for label, seeder in seed(client, app):
            app.dependency_overrides[get_current_user] = signed_in(seeder.user)
            owner_result = {}
            for path in (*PATHS, f"/api/reports/monthly?basis=month&start_date={month_start}"):
                response = client.get(path)
                if response.status_code != 200:
                    raise RuntimeError(f"{path}: {response.status_code} {response.text}")
                owner_result[path.replace(month_start, "<month>")] = response.json()
            result[label] = owner_result
        app.dependency_overrides.clear()
    with open(out_path, "w") as handle:
        json.dump(result, handle, sort_keys=True, ensure_ascii=False, indent=1)


def normalise(value, *, strip_added: bool):
    if isinstance(value, dict):
        cleaned = {}
        for key, item in value.items():
            if key in VOLATILE_KEYS:
                continue
            if strip_added and key in ADDED_FIELDS:
                continue
            if strip_added and ADDED_KEY_VALUES.get(key) == item:
                item = None
            # Credit-bill plan fields (D9) never appear in this seed; strip them only if present.
            if strip_added and key in {"plan_amount", "counted_elsewhere", "credit_account_id"}:
                continue
            cleaned[key] = normalise(item, strip_added=strip_added)
        return cleaned
    if isinstance(value, list):
        items = [normalise(item, strip_added=strip_added) for item in value]
        return sorted(items, key=lambda item: json.dumps(item, sort_keys=True, ensure_ascii=False))
    if isinstance(value, str):
        value = UUID_PATTERN.sub("<id>", value)
        match = INSTANT_PATTERN.match(value)
        return f"{match.group(1)}T<time>" if match else value
    return value


def differences(before, after, path="") -> list[str]:
    if isinstance(before, dict) and isinstance(after, dict):
        return [found for key in sorted(set(before) | set(after))
                for found in differences(before.get(key, "<missing>"), after.get(key, "<missing>"), f"{path}/{key}")]
    if isinstance(before, list) and isinstance(after, list) and len(before) == len(after):
        return [found for index, pair in enumerate(zip(before, after)) for found in differences(*pair, f"{path}[{index}]")]
    return [] if before == after else [f"{path}: {before!r} -> {after!r}"]


def compare(before_path: str, after_path: str) -> int:
    with open(before_path) as handle:
        before = normalise(json.load(handle), strip_added=False)
    with open(after_path) as handle:
        after = normalise(json.load(handle), strip_added=True)
    found = differences(before, after)
    endpoints = sum(len(owner) for owner in before.values())
    print(json.dumps({"owners": sorted(before), "endpoints_compared": endpoints, "differences": found}, indent=1, ensure_ascii=False))
    return 1 if found else 0


if __name__ == "__main__":
    command, *arguments = sys.argv[1:]
    if command == "snapshot":
        snapshot(*arguments)
    elif command == "compare":
        sys.exit(compare(*arguments))
    else:
        raise SystemExit("usage: credit_figure_parity.py snapshot <out.json> | compare <before.json> <after.json>")
