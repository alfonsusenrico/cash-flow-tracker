"""Parity seed: loads the prototype's synthetic dataset (shared/data.js) into a disposable database.

Local verification tooling only (apply-markas-shell D13). It is not imported by the app and runs in no
image, CI job or npm script. Every guard runs before the first write, the whole load is one transaction,
and the app's own figures are read back through the API and compared with the figures data.js states.

    MARKAS_PASSWORD=... backend/.venv/bin/python scripts/verify/markas/seed_parity.py \
        --data ../cash-flow-tracker-prototypes/design-options/shared/data.js \
        --database-url postgresql://...@127.0.0.1:<port>/cft_markas_parity \
        [--today YYYY-MM-DD] [--reset] [--api http://127.0.0.1:<port>] [--check-out <dir>]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import sys
import unicodedata
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

# The Flyway version whose tables and constraints this loader writes.
MAPPED_SCHEMA_VERSION = 51
DATABASE_PREFIX = "cft_markas_"
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})
PASSWORD_VARIABLE = "MARKAS_PASSWORD"
DATA_PREFIX = "window.CFT_DATA ="
ZONE = ZoneInfo("Asia/Jakarta")
DEVICE_ID = "markas-parity-dev-1"
CHECK_FILE_NAME = "seed-check.json"

ACCOUNT_TYPES = {
    "bank": "bank", "pocket": "bank", "cash": "cash", "ewallet": "wallet",
    "broker_cash": "bank", "broker": "investment", "credit": "credit",
}
INSTRUMENT_TYPES = {"saham": "stock", "reksa_dana": "mutual_fund", "emas": "gold"}
TRANSACTION_SOURCES = {"notifikasi": "notification", "manual": "manual"}
INSTITUTIONS = {"BCA": "bca", "Bank Jago": "jago", "GoPay": "gopay", "ShopeePay": "shopeepay", "Stockbit": "stockbit"}
PACKAGES = {"bca": "com.bca", "jago": "com.jago.digitalBanking", "gopay": "com.gojek.gopay"}
APP_LABELS = {"bca": "BCA mobile", "jago": "Jago", "gopay": "GoPay"}
# Same colours the app gives a new account (backend/app/services/account_colours.py).
ACCOUNT_COLOURS = {"investment": "#8B5CF6", "credit": "#475569"}
DEFAULT_ACCOUNT_COLOUR = "#3b82f6"
MOVEMENT_CATEGORY = "Internal Movement"
SALARY_CATEGORY = "gaji"

# Every application table; flyway_schema_history is the only public table --reset keeps.
KEPT_TABLES = frozenset({"flyway_schema_history"})


class SeedRefusal(Exception):
    """A guard refused the run; nothing has been written."""


# --- Guards (pure, so the tests need no database) -----------------------------------------------


@dataclass(frozen=True)
class DatabaseTarget:
    host: str
    port: int | None
    name: str

    def describe(self) -> str:
        port = f":{self.port}" if self.port else ""
        return f"{self.host}{port}/{self.name}"


def require_password(env: dict[str, str]) -> str:
    password = env.get(PASSWORD_VARIABLE, "")
    if not password:
        raise SeedRefusal(f"{PASSWORD_VARIABLE} is not set")
    return password


def database_target(url: str) -> DatabaseTarget:
    """The host and database a URL names; refuses anything but a loopback cft_markas_ database."""
    parts = urlsplit(url)
    if parts.scheme not in {"postgresql", "postgres"}:
        raise SeedRefusal("the database URL must be a postgresql:// URL")
    host = (parts.hostname or "").lower()
    if host not in LOOPBACK_HOSTS:
        raise SeedRefusal(f"the database host {host or '(none)'} is not loopback")
    name = parts.path.lstrip("/")
    if not name.startswith(DATABASE_PREFIX):
        raise SeedRefusal(f"the database name {name or '(none)'} does not start with {DATABASE_PREFIX}")
    return DatabaseTarget(host=host, port=parts.port, name=name)


def require_synthetic(data: dict[str, Any]) -> None:
    if (data.get("meta") or {}).get("synthetic") is not True:
        raise SeedRefusal("data.js does not declare meta.synthetic: true")


def require_schema_version(version: int | None) -> None:
    if version != MAPPED_SCHEMA_VERSION:
        raise SeedRefusal(f"the schema is at version {version}, this loader maps version {MAPPED_SCHEMA_VERSION}")


def require_empty_or_reset(user_count: int, reset: bool) -> None:
    if user_count > 0 and not reset:
        raise SeedRefusal(f"the database already has {user_count} user(s); pass --reset to replace them")


def parse_dataset(text: str) -> dict[str, Any]:
    """data.js is `window.CFT_DATA = {...};`, after optional `//` comment lines."""
    start = text.find(DATA_PREFIX)
    header = text[:start] if start >= 0 else text
    if start < 0 or any(line.strip() and not line.strip().startswith("//") for line in header.splitlines()):
        raise SeedRefusal(f"data.js does not hold {DATA_PREFIX} after its comment lines")
    body = text[start + len(DATA_PREFIX):].strip()
    if body.endswith(";"):
        body = body[:-1]
    try:
        return json.loads(body)
    except json.JSONDecodeError as error:
        raise SeedRefusal(f"data.js is not JSON after its prefix: {error}") from None


def schema_version(cur) -> int | None:
    cur.execute("SELECT to_regclass('public.flyway_schema_history') IS NOT NULL AS present")
    if not cur.fetchone()["present"]:
        return None
    cur.execute("SELECT version FROM flyway_schema_history WHERE success AND version IS NOT NULL")
    versions = [int(row["version"].split(".")[0]) for row in cur.fetchall()]
    return max(versions) if versions else None


def preflight(cur, *, reset: bool) -> None:
    """The database guards; read-only statements only."""
    require_schema_version(schema_version(cur))
    cur.execute("SELECT COUNT(*) AS count FROM users")
    require_empty_or_reset(int(cur.fetchone()["count"]), reset)


# --- Dates -----------------------------------------------------------------------------------------


@dataclass(frozen=True)
class DayShift:
    """Moves every data.js date and instant by whole days, so relative state (cycle day, due plans)
    stays as the prototype shows it while the server's today is the real today."""

    days: int

    def day(self, value: str | None) -> date | None:
        if value is None:
            return None
        return date.fromisoformat(value[:10]) + timedelta(days=self.days)

    def instant(self, value: str | None) -> datetime | None:
        if value is None:
            return None
        moment = datetime.fromisoformat(value)
        if moment.tzinfo is None:
            raise ValueError(f"data.js instant without a zone: {value}")
        return moment + timedelta(days=self.days)

    def day_text(self, value: str | None) -> str | None:
        shifted = self.day(value)
        return shifted.isoformat() if shifted else None


def default_today() -> date:
    return datetime.now(ZONE).date()


# --- Small helpers -----------------------------------------------------------------------------


def new_id() -> str:
    return str(uuid.uuid4())


def payload_hash(key: str) -> str:
    return hashlib.sha256(f"markas-parity:{key}".encode()).hexdigest()


def normalize_alias_name(value: str) -> str:
    """Same rule as notification_resolution.normalize_name, which the app applies to account names."""
    value = re.sub(r"\b(?:pocket|kantong|your|my)\b", "", value.lower())
    value = re.sub(r"\s+", " ", value).strip()
    return {"main": "utama", "emergency fund": "dana darurat", "savings": "tabungan"}.get(value, value)


def normalize_mask(value: str) -> str:
    """Same rule as notification_sender.normalize_mask."""
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def hash_password(password: str) -> str:
    # The backend hashes with passlib's bcrypt (backend/app/services/auth.py register_user).
    from passlib.hash import bcrypt

    return bcrypt.hash(password)


def json_value(value: Any):
    from psycopg.types.json import Jsonb

    return Jsonb(value)


# --- Loader --------------------------------------------------------------------------------------


@dataclass
class LoadReport:
    shift_days: int
    unmapped: list[dict[str, str]] = field(default_factory=list)
    added: list[dict[str, str]] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)

    def skip(self, path: str, reason: str) -> None:
        self.unmapped.append({"field": path, "reason": reason})

    def add(self, what: str, reason: str) -> None:
        self.added.append({"row": what, "reason": reason})


class ParityLoader:
    """Writes data.js into the schema of MAPPED_SCHEMA_VERSION through one cursor (one transaction)."""

    def __init__(self, cur, data: dict[str, Any], shift: DayShift, password: str):
        self.cur = cur
        self.data = data
        self.shift = shift
        self.password = password
        self.report = LoadReport(shift_days=shift.days)
        self.user_id = new_id()
        self.category_ids: dict[str, str] = {}
        self.category_pillars: dict[str, str | None] = {}
        self.movement_categories: dict[str, str] = {}
        self.account_ids: dict[str, str] = {}
        self.account_rows: dict[str, dict[str, Any]] = {}
        self.transaction_ids: dict[str, str] = {}
        self.movement_ids: dict[str, str] = {}
        self.goal_linked_ids: set[str] = set()
        self.recurring_ids: dict[str, str] = {}
        self.obligation_ids: dict[str, str] = {}
        self.device_key_id: str | None = None

    # Statement helper ---------------------------------------------------------------------------

    def insert(self, table: str, values: dict[str, Any]) -> None:
        columns = ", ".join(values)
        placeholders = ", ".join(["%s"] * len(values))
        self.cur.execute(f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", list(values.values()))
        self.report.counts[table] = self.report.counts.get(table, 0) + 1

    # Entry point ---------------------------------------------------------------------------------

    def load(self) -> LoadReport:
        self.load_user()
        self.load_categories()
        self.load_goal_links()
        self.load_accounts()
        self.load_transactions()
        self.load_salary_receipts()
        self.load_obligations()
        self.load_recurring_rules()
        self.load_goals()
        self.load_device()
        self.load_review_items()
        self.load_habits()
        self.load_sender_aliases()
        self.load_notification_mappings()
        self.load_test_sessions()
        self.record_unstored_sections()
        self.verify_ledger_balances()
        return self.report

    # User -------------------------------------------------------------------------------------

    def load_user(self) -> None:
        user = self.data["user"]
        settings = self.data.get("settings") or {}
        payday = self.shift.day(self.data["cycle"]["nextPaydayEstimate"])
        self.insert("users", {
            "id": self.user_id,
            "username": user["username"],
            "password_hash": hash_password(self.password),
            "invite_code": "markas-parity-seed",
            # The payday day moves with the shift so the configured day still matches the receipts.
            "payday_day": payday.day if self.shift.days else int(user["paydayDay"]),
            "currency": self.data["meta"]["currency"],
            "name": user["name"],
            "emergency_fund_multiplier": int(settings.get("emergencyTargetMonths") or 6),
            "monthly_spending_budget": settings.get("monthlySpendingLimit"),
            "name_aliases": list(user.get("bankNames") or []),
        })
        self.insert("salary_sources", {
            "user_id": self.user_id, "matcher_field": "merchant", "matcher_text": user["salarySource"],
        })
        for key in ("initials", "theme", "hideBalances"):
            if key in user:
                self.report.skip(f"user.{key}", "browser preference or derived text; the schema stores no such column")
        for key in ("currencyDisplay", "singleKeyShortcuts", "reportBasis"):
            if key in settings:
                self.report.skip(f"settings.{key}", "kept in the browser or chosen per page; no user column")

    # Categories ---------------------------------------------------------------------------------

    def load_categories(self) -> None:
        for category in self.data["categories"]:
            category_id = new_id()
            self.category_ids[category["id"]] = category_id
            self.category_pillars[category["id"]] = category["pillar"]
            self.insert("categories", {
                "id": category_id,
                "user_id": self.user_id,
                "name": category["name"],
                "icon": category["icon"],
                "kind": category["kind"],
                "monthly_budget": category["budget"],
                "kakeibo_type": category["pillar"] if category["kind"] == "expense" else None,
            })
        # The app books every movement leg in these two excluded categories (movement_linkage.py).
        for kind in ("expense", "income"):
            category_id = new_id()
            self.movement_categories[kind] = category_id
            self.insert("categories", {
                "id": category_id, "user_id": self.user_id, "name": MOVEMENT_CATEGORY,
                "icon": "arrows-right-left", "color": DEFAULT_ACCOUNT_COLOUR, "kind": kind,
                "kakeibo_type": None, "is_excluded_from_budget": True,
            })
        self.report.add("categories: Internal Movement (expense, income)",
                        "the app books movement legs in these excluded categories")

    # Accounts -----------------------------------------------------------------------------------

    def load_goal_links(self) -> None:
        for goal in self.data["goals"]:
            self.goal_linked_ids.update(goal.get("linkedAccountIds") or [])
            self.goal_linked_ids.update(goal.get("linkedProductIds") or [])

    def opening_balances(self) -> dict[str, int]:
        """Balance before the first row: the stated balance less every row's effect on the account.

        The effects follow the app's ledger (an account's balance is its initial balance plus incomes
        less expenses): a movement is an expense on its source and an income on its target, a fee is its
        own expense row, and a purchase into a product leaves the product's value to its price.
        """
        effects: dict[str, int] = {}

        def apply(account: str | None, amount: int) -> None:
            if account:
                effects[account] = effects.get(account, 0) + amount

        for row in self.data["transactions"]:
            kind = row["type"]
            if kind == "income":
                apply(row["accountId"], row["amount"])
            elif kind == "expense":
                apply(row["accountId"], -row["amount"])
            else:
                apply(row["accountId"], -row["amount"])
                if row.get("toAccountId") and not row.get("toProductId"):
                    apply(row["toAccountId"], row["amount"])
        for extra in self.pre_window_salaries():
            apply(extra["accountId"], extra["amount"])
        openings = {}
        for account in self.data["accounts"]:
            if self.has_children(account["id"]) or account["type"] == "broker":
                continue
            openings[account["id"]] = int(account["balance"]) - effects.get(account["id"], 0)
        return openings

    def has_children(self, account_id: str) -> bool:
        return any(other.get("parentId") == account_id for other in self.data["accounts"])

    def load_accounts(self) -> None:
        openings = self.opening_balances()
        credit = {entry["accountId"]: entry for entry in self.data.get("credit") or []}
        now = self.shift.instant(self.data["meta"]["now"])
        order = 0
        for account in self.data["accounts"]:
            account_type = ACCOUNT_TYPES[account["type"]]
            account_id = new_id()
            self.account_ids[account["id"]] = account_id
            values: dict[str, Any] = {
                "id": account_id,
                "user_id": self.user_id,
                "name": account["name"],
                "type": account_type,
                "initial_balance": openings.get(account["id"], 0),
                "parent_id": self.account_ids.get(account["parentId"]) if account.get("parentId") else None,
                "color": ACCOUNT_COLOURS.get(account_type, DEFAULT_ACCOUNT_COLOUR),
                "display_order": order,
                "role": account["role"],
            }
            if account["id"] in credit:
                values["credit_limit"] = credit[account["id"]]["limit"]
                values["payment_day"] = self.shifted_day_of_month(credit[account["id"]]["paymentDay"], credit[account["id"]]["nextDue"])
            self.insert("accounts", values)
            self.account_rows[account["id"]] = {**values, "data_id": account["id"]}
            order += 1
            for product in (p for p in self.data["products"] if p["accountId"] == account["id"]):
                self.load_product(product, account_id, order, now)
                order += 1
        for account in self.data["accounts"]:
            default_pocket = next(
                (child for child in self.data["accounts"] if child.get("parentId") == account["id"] and child.get("isDefault")),
                None,
            )
            if default_pocket:
                self.cur.execute(
                    "UPDATE accounts SET default_pocket_id = %s WHERE id = %s",
                    (self.account_ids[default_pocket["id"]], self.account_ids[account["id"]]),
                )
        self.report.skip("accounts[].institution", "no institution column; the parent account name carries it")
        for entry in credit.values():
            self.report.skip("credit[].statementDay", "the schema stores only the payment day of a credit account")
            self.report.skip("credit[].dueAmount, credit[].minPayment",
                             f"stated as the Tagihan SPayLater plan (recurring r-spaylater, {entry['dueAmount']})")
            self.report.skip("credit[].note", "free text; no column")

    def shifted_day_of_month(self, day: int, reference: str) -> int:
        return self.shift.day(reference).day if self.shift.days else int(day)

    def load_product(self, product: dict[str, Any], platform_id: str, order: int, now: datetime) -> None:
        product_id = new_id()
        self.account_ids[product["id"]] = product_id
        values: dict[str, Any] = {
            "id": product_id,
            "user_id": self.user_id,
            "name": product["name"],
            "type": "investment",
            "initial_balance": 0,
            "parent_id": platform_id,
            "instrument_type": INSTRUMENT_TYPES[product["kind"]],
            "instrument_symbol": product.get("code"),
            "color": ACCOUNT_COLOURS["investment"],
            "display_order": order,
            "role": "investasi",
            "last_price_at": now,
        }
        if product["trackedBy"] == "nominal":
            values.update({
                "investment_tracking_mode": "amount",
                "investment_cost_basis": product["costBasis"],
                "last_price": product["value"],
            })
        elif product["kind"] == "saham":
            values.update({
                "units": product["lots"] * product["unitsPerLot"],
                "avg_buy_price": product["avgPrice"],
                "last_price": product["price"],
            })
        else:
            values.update({"units": product["grams"], "avg_buy_price": product["avgPrice"], "last_price": product["price"]})
        self.insert("accounts", values)
        self.account_rows[product["id"]] = {**values, "data_id": product["id"]}
        if product.get("unitLabel"):
            self.report.skip(f"products[{product['id']}].unitLabel", "display text the app derives from units")

    # Transactions -------------------------------------------------------------------------------

    def is_savings_destination(self, data_account_id: str) -> bool:
        """Same rule as ledger_mutations.is_savings_destination."""
        row = self.account_rows[data_account_id]
        return (
            row["type"] == "investment"
            or bool(row.get("instrument_type"))
            or data_account_id in self.goal_linked_ids
            or row["role"] == "tabungan"
        )

    def movement_pillar(self, source: str, target: str, *, is_trade: bool) -> str | None:
        """Same rule as ledger_mutations.movement_kakeibo."""
        if is_trade:
            return None
        if self.is_savings_destination(source) != self.is_savings_destination(target):
            return "saving"
        return None

    def provenance(self, row: dict[str, Any]) -> dict[str, Any]:
        source = TRANSACTION_SOURCES[row["source"]]
        if row.get("status") == "needs_review":
            return {"source": source, "review_status": "needs_review", "classified_by": None}
        return {"source": source, "review_status": "reviewed", "classified_by": "owner" if source == "manual" else "rule"}

    def pre_window_salaries(self) -> list[dict[str, Any]]:
        """Salary receipts before data.js's first row that the expected-salary rule needs.

        The app estimates the next salary from the last three confirmed receipts; data.js holds two
        (25 Sep, 23 Oct). One receipt a month earlier, same payer, amount and time, completes the rule.
        """
        salaries = [row for row in self.data["transactions"] if row.get("categoryId") == SALARY_CATEGORY]
        if len(salaries) >= 3 or not salaries:
            return []
        first = min(salaries, key=lambda row: row["date"])
        first_day = date.fromisoformat(first["date"][:10])
        month, year = (first_day.month - 1, first_day.year) if first_day.month > 1 else (12, first_day.year - 1)
        earlier_day = first_day.replace(year=year, month=month)
        return [{
            **first,
            "id": "pre-window-salary",
            "date": f"{earlier_day.isoformat()}{first['date'][10:]}",
        }]

    def load_transactions(self) -> None:
        rows = sorted(self.data["transactions"], key=lambda row: (row["date"], row["id"]))
        for extra in self.pre_window_salaries():
            self.insert_plain(extra)
            self.report.add(
                f"transaction {extra['date'][:10]} {extra['merchant']} {extra['amount']} (salary receipt)",
                "the app's expected-salary rule needs three receipts; the opening balance of "
                f"{extra['accountId']} is lowered by the same amount so every stated balance holds",
            )
        fee_rows = [row for row in rows if row.get("feeForId")]
        for row in rows:
            if row.get("feeForId"):
                continue
            if row["type"] in ("income", "expense"):
                self.insert_plain(row)
            elif row["type"] == "topup":
                self.insert_topup(row)
            elif row.get("toProductId"):
                self.insert_movement(row, target=row["toProductId"], is_trade=True)
            elif row.get("toAccountId"):
                self.insert_movement(row, target=row["toAccountId"], is_trade=False)
            else:
                self.insert_unpaired_leg(row)
        for row in fee_rows:
            self.insert_plain(row, fee_for=self.movement_ids[row["feeForId"]])
        self.report.skip("transactions[].trade", "a Beli/Jual trade is stored as a movement into the product; "
                         "lots and price live on the product, not on the row")

    def plain_values(self, row: dict[str, Any]) -> dict[str, Any]:
        category = row.get("categoryId")
        pillar = None
        if row["type"] == "expense" and row.get("status") != "needs_review":
            pillar = row.get("pillar") or self.category_pillars.get(category)
        transaction_id = new_id()
        self.transaction_ids[row["id"]] = transaction_id
        return {
            "id": transaction_id,
            "user_id": self.user_id,
            "account_id": self.account_ids[row["accountId"]],
            "category_id": self.category_ids.get(category) if category else None,
            "type": row["type"],
            "amount": row["amount"],
            "notes": row.get("note"),
            "merchant": row.get("merchant"),
            "date": self.shift.instant(row["date"]),
            "created_at": self.shift.instant(row["date"]),
            "kakeibo_type": pillar,
            "idempotency_key": f"markas-parity:{row['id']}",
            **self.provenance(row),
        }

    def insert_plain(self, row: dict[str, Any], *, fee_for: str | None = None) -> None:
        values = self.plain_values(row)
        if fee_for:
            values["fee_for_movement_id"] = fee_for
        self.insert("transactions", values)

    def insert_legs(self, row: dict[str, Any], *, target: str, movement_id: str, pillar: str | None,
                    inbound_source: str | None = None) -> None:
        provenance = self.provenance(row)
        common = {
            "user_id": self.user_id, "amount": row["amount"], "notes": row.get("note"),
            "date": self.shift.instant(row["date"]), "created_at": self.shift.instant(row["date"]),
            "movement_id": movement_id,
        }
        outbound_id = new_id()
        self.transaction_ids[row["id"]] = outbound_id
        self.insert("transactions", {
            **common, "id": outbound_id, "account_id": self.account_ids[row["accountId"]],
            "category_id": self.movement_categories["expense"], "type": "expense", "kakeibo_type": pillar,
            "idempotency_key": f"markas-parity:{row['id']}:out", "movement_role": "outbound", **provenance,
        })
        inbound = dict(provenance)
        if inbound_source:
            inbound = {"source": inbound_source, "review_status": "reviewed", "classified_by": "system"}
        self.insert("transactions", {
            **common, "id": new_id(), "account_id": self.account_ids[target],
            "category_id": self.movement_categories["income"], "type": "income", "kakeibo_type": None,
            "idempotency_key": f"markas-parity:{row['id']}:in", "movement_role": "inbound", **inbound,
        })

    def insert_movement(self, row: dict[str, Any], *, target: str, is_trade: bool) -> None:
        movement_id = new_id()
        self.movement_ids[row["id"]] = movement_id
        pillar = self.movement_pillar(row["accountId"], target, is_trade=is_trade)
        self.insert_legs(row, target=target, movement_id=movement_id, pillar=pillar)

    def insert_topup(self, row: dict[str, Any]) -> None:
        """A bank debit the owner converted into a top-up of an amount-tracked fund
        (investment_topups.convert_transaction_to_topup), so the top-up habit learns from it."""
        topup_id = new_id()
        self.movement_ids[row["id"]] = topup_id
        target = row["toProductId"]
        self.insert_legs(row, target=target, movement_id=topup_id,
                         pillar=self.movement_pillar(row["accountId"], target, is_trade=False),
                         inbound_source="investment")
        outbound_id = self.transaction_ids[row["id"]]
        fingerprint = hashlib.sha256(json.dumps(
            {"transaction": outbound_id, "target": self.account_ids[target]}, sort_keys=True,
        ).encode()).hexdigest()
        self.insert("investment_topups", {
            "id": topup_id,
            "user_id": self.user_id,
            "source_account_id": self.account_ids[row["accountId"]],
            "target_account_id": self.account_ids[target],
            "amount": row["amount"],
            "date": self.shift.instant(row["date"]),
            "notes": row.get("note") or "",
            "idempotency_key": f"transaction:{outbound_id}:{topup_id}",
            "request_fingerprint": fingerprint,
            "converted_from": json_value({
                "transaction_id": outbound_id, "category_id": None, "kakeibo_type": None, "notes": row.get("note"),
            }),
            "created_at": self.shift.instant(row["date"]),
        })

    def insert_unpaired_leg(self, row: dict[str, Any]) -> None:
        """An outgoing own transfer whose counterpart was not seen: one movement-category expense that
        waits in Tinjau's "Pindah saldo belum lengkap" (movement_pairing marks it awaiting_counterpart)."""
        values = self.plain_values(row)
        values.update({"type": "expense", "category_id": self.movement_categories["expense"], "kakeibo_type": None})
        self.insert("transactions", values)

    # Salary ---------------------------------------------------------------------------------------

    def load_salary_receipts(self) -> None:
        salaries = [row for row in self.data["transactions"] if row.get("categoryId") == SALARY_CATEGORY]
        salaries += self.pre_window_salaries()
        for row in salaries:
            receipt_id = new_id()
            confirmed = self.shift.instant(row["date"]) + timedelta(minutes=5)
            self.insert("salary_receipts", {
                "id": receipt_id, "user_id": self.user_id, "transaction_id": self.transaction_ids[row["id"]],
                "confirmed_at": confirmed, "confirmation_method": "owner",
            })
            self.insert("salary_receipt_events", {
                "user_id": self.user_id, "salary_receipt_id": receipt_id, "action": "confirm",
                "recorded_at": confirmed, "confirmation_method": "owner",
            })

    # Obligations and plans ----------------------------------------------------------------------

    def load_obligations(self) -> None:
        for obligation in self.data["obligations"]:
            obligation_id = new_id()
            self.obligation_ids[obligation["id"]] = obligation_id
            values: dict[str, Any] = {
                "id": obligation_id,
                "user_id": self.user_id,
                "name": obligation["name"],
                "total_amount": obligation["total"],
                "remaining_amount": obligation["remaining"],
                "due_date": self.shift.day(obligation.get("nextDue")),
                "minimum_payment": obligation.get("minPayment"),
                "notes": obligation.get("lender"),
                "is_archived": bool(obligation.get("archived")),
            }
            if obligation.get("paidOffOn"):
                values["updated_at"] = datetime.combine(self.shift.day(obligation["paidOffOn"]), datetime.min.time(), ZONE)
            self.insert("obligations", values)
        self.report.skip("obligations[].lender", "stored in obligations.notes; no lender column")
        self.report.skip("obligations[].kind, obligations[].dueDay, obligations[].endsOn",
                         "no column; the due day follows from due_date and the plan")
        self.report.skip("obligations[].paidOffOn", "no column; stored as updated_at of the archived debt")
        for row in self.data["transactions"]:
            if row.get("obligationId"):
                self.insert("transaction_obligation_allocations", {
                    "transaction_id": self.transaction_ids[row["id"]],
                    "obligation_id": self.obligation_ids[row["obligationId"]],
                    "amount": row["amount"],
                })

    def load_recurring_rules(self) -> None:
        for rule in self.data["recurring"]:
            rule_id = new_id()
            self.recurring_ids[rule["id"]] = rule_id
            credit_target = rule.get("creditAccountId")
            next_due = self.shift.day(rule["nextDate"])
            values: dict[str, Any] = {
                "id": rule_id,
                "user_id": self.user_id,
                "name": rule["name"],
                "type": "transfer" if credit_target else "expense",
                "amount": rule["amount"],
                "source_account_id": self.account_ids[rule["accountId"]],
                "target_account_id": self.account_ids[credit_target] if credit_target else None,
                "category_id": self.category_ids.get(rule.get("categoryId")) if rule.get("categoryId") else None,
                "obligation_id": self.obligation_ids.get(rule.get("obligationId")) if rule.get("obligationId") else None,
                "schedule_type": "monthly_day",
                "schedule_day": next_due.day if self.shift.days else int(rule["day"]),
                "auto_post": False,
                "next_due_date": next_due,
                "is_active": True,
            }
            if rule.get("lastPaid"):
                values["last_executed_at"] = datetime.combine(self.shift.day(rule["lastPaid"]), datetime.min.time(), ZONE)
            self.insert("recurring_rules", values)
        self.report.skip("recurring[].mode", "plans never post (chk_recurring_rules_non_posting); "
                         "otomatis is kept as link_method 'auto' on its paid occurrences")
        self.report.skip("recurring[].status", "derived by the app from next_due_date and linked occurrences")
        self.report.skip("recurring[].estimate", "no column for an estimated plan amount")
        self.report.skip("recurring[].cadence", "every data.js plan is monthly; stored as schedule_type monthly_day")
        rules = {rule["id"]: rule for rule in self.data["recurring"]}
        for row in self.data["transactions"]:
            rule = rules.get(row.get("recurringId") or "")
            if not rule:
                continue
            paid_day = date.fromisoformat(row["date"][:10])
            scheduled = self.shift.day(paid_day.replace(day=int(rule["day"])).isoformat())
            method = "auto" if rule["mode"] == "otomatis" else "owner"
            self.insert("recurring_executions", {
                "recurring_rule_id": self.recurring_ids[rule["id"]], "user_id": self.user_id,
                "scheduled_for": scheduled, "status": "linked",
                "transaction_id": self.transaction_ids[row["id"]], "link_method": method,
                "obligation_applied": bool(rule.get("obligationId")),
                "created_at": self.shift.instant(row["date"]),
            })
            self.insert("recurring_link_events", {
                "user_id": self.user_id, "recurring_rule_id": self.recurring_ids[rule["id"]],
                "scheduled_for": scheduled, "transaction_id": self.transaction_ids[row["id"]],
                "action": "link", "method": method, "created_at": self.shift.instant(row["date"]),
            })

    # Goals --------------------------------------------------------------------------------------

    def load_goals(self) -> None:
        for goal in self.data["goals"]:
            goal_id = new_id()
            self.insert("goals", {
                "id": goal_id,
                "user_id": self.user_id,
                "name": goal["name"],
                "target_amount": goal["target"],
                "current_amount": goal["saved"],
                "target_date": self.shift.day(goal.get("deadline")),
                "is_emergency": bool(goal.get("isEmergency")),
            })
            for account in (goal.get("linkedAccountIds") or []) + (goal.get("linkedProductIds") or []):
                self.insert("goal_accounts", {"goal_id": goal_id, "account_id": self.account_ids[account]})
        self.report.skip("goals[].contributions", "no per-goal contribution rows without ledger rows; "
                         "current_amount holds the saved total")
        self.report.skip("goals[].achieved, goals[].pct, goals[].monthlyNeeded, goals[].autoFromBalance",
                         "derived by the app from the target, the saved amount, the date and the links")

    # Device, review items and habits --------------------------------------------------------------

    def load_device(self) -> None:
        device = self.data["devices"][0]
        key_id = new_id()
        self.device_key_id = key_id
        self.insert("api_keys", {
            "id": key_id, "user_id": self.user_id,
            # A random hash: no usable key exists for the synthetic phone.
            "key_hash": hashlib.sha256(secrets.token_bytes(32)).hexdigest(),
            "key_prefix": "cfk_pary",
            "created_at": datetime.combine(self.shift.day(device["pairedOn"]), datetime.min.time(), ZONE),
            "last_used_at": self.shift.instant(device["lastSeen"]),
            "device_name": device["name"], "device_id": DEVICE_ID, "created_via": "pairing",
        })
        health = {item["label"]: item["ok"] for item in device.get("health") or []}
        self.insert("device_health", {
            "api_key_id": key_id,
            "app_version": device["appVersion"],
            "listener_connected": device["status"] == "aktif",
            "notification_access": health.get("Izin baca notifikasi", True),
            "post_notifications": True,
            "battery_unrestricted": health.get("Bebas penghemat baterai", True),
            "self_test_status": "not_run",
            "last_capture_at": self.shift.instant(device["lastSeen"]),
            "queue_pending": 0,
            "reported_at": self.shift.instant(device["lastSeen"]),
        })
        self.report.skip("devices[].health['Mulai otomatis']", "the app's device health has no autostart field")
        self.report.skip("devices[].status", "derived from the key and its last report; stored as listener_connected")
        for key in self.data.get("apiKeys") or []:
            self.insert("api_keys", {
                "id": new_id(), "user_id": self.user_id,
                "key_hash": hashlib.sha256(secrets.token_bytes(32)).hexdigest(),
                "key_prefix": "cft_" + key["prefix"][-4:],
                "created_at": datetime.combine(self.shift.day(key["createdAt"]), datetime.min.time(), ZONE),
                "last_used_at": datetime.combine(self.shift.day(key["lastUsed"]), datetime.min.time(), ZONE),
                "device_name": key["label"], "created_via": "manual",
            })

    def notification_event(self, key: str, *, institution: str, post_time: datetime, values: dict[str, Any],
                           event_id: str | None = None) -> str:
        event_id = event_id or new_id()
        self.insert("notification_events", {
            "id": event_id, "user_id": self.user_id, "device_id": DEVICE_ID,
            "package_name": PACKAGES[institution], "app_label": APP_LABELS[institution],
            "post_time": post_time, "captured_at": post_time, "created_at": post_time,
            "payload_hash": payload_hash(key), "source_version": "markas-parity",
            "processing_mode": "deterministic", **values,
        })
        return event_id

    def recorded_event(self, key: str, *, institution: str, post_time: datetime, snapshot: dict[str, Any],
                       values: dict[str, Any]) -> str:
        """A notification the app recorded; its result_key and snapshot name the event itself
        (notification_application.save_recorded)."""
        event_id = new_id()
        record_key = f"notification:{event_id}"
        return self.notification_event(key, institution=institution, post_time=post_time, event_id=event_id, values={
            "is_financial": True, "processing_state": "recorded", "result_key": record_key,
            "result_snapshot": json_value({"record_key": record_key, **snapshot}), **values,
        })

    def transaction_snapshot(self, row: dict[str, Any]) -> dict[str, Any]:
        """Same fields as notification_application.committed_snapshot, without its record_key."""
        return {"type": "expense", "description": (row.get("merchant") or row.get("note") or "")[:160],
                "source": None, "target": None, "amount": row["amount"], "currency": "IDR"}

    def load_review_items(self) -> None:
        rows = {row["id"]: row for row in self.data["transactions"]}
        accounts = {account["id"]: account for account in self.data["accounts"]}
        for item in self.data["review"]["items"]:
            kind = item["kind"]
            if kind == "belum_dikenali":
                self.load_purpose_question(item, rows[item["transactionIds"][0]], accounts)
            elif kind == "pindah_belum_lengkap":
                self.load_awaiting_counterpart(item, rows[item["transactionIds"][0]], accounts)
            elif kind == "perlu_jawaban" and item.get("transactionIds"):
                # The rows are needs_review transactions; Tinjau groups them without an event.
                if item.get("suggestion"):
                    self.report.skip(f"review.items[{item['id']}].suggestion",
                                     "a group's category guess comes from the habit learner, not a stored row")
            elif kind == "terlihat_dua_kali":
                self.load_seen_twice(item)
            elif kind == "dilewati_otomatis":
                self.load_ignored_notice(item)
            elif kind == "saran":
                self.report.skip(f"review.items[{item['id']}]",
                                 "the six 'old Makanan & Minuman' Kopi Senja rows do not exist in data.js "
                                 "(every Kopi Senja row is already Jajan & Nongkrong), so no history suggestion can point at them")
            elif item.get("subkind") == "top_up":
                self.report.skip(f"review.items[{item['id']}]",
                                 "duplicates t017, which data.js already records as the top-up of the same "
                                 "750,000 at the same minute; loading both would debit BCA twice")
            else:
                self.report.skip(f"review.items[{item['id']}]", f"no mapping for kind {kind}")
        self.report.skip("review.folders, review.keyboard", "Tinjau's own labels and shortcuts, not data")

    def load_purpose_question(self, item: dict[str, Any], row: dict[str, Any], accounts: dict[str, Any]) -> None:
        institution = INSTITUTIONS[accounts[row["accountId"]]["institution"]]
        transaction_id = self.transaction_ids[row["id"]]
        question = {
            "type": "purpose", "question_id": new_id(), "transaction_id": transaction_id,
            "direction": "expense", "amount": item["amount"], "currency": "IDR",
            "prompt": item["question"], "allows_text": True, "text_max_length": 200, "suggestions": [],
        }
        self.recorded_event(item["id"], institution=institution, post_time=self.shift.instant(item["date"]),
                            snapshot=self.transaction_snapshot(row), values={
            "title": APP_LABELS[institution], "body_text": item["rawNotification"], "big_text": item["rawNotification"],
            "transaction_id": transaction_id, "provenance_kind": "observed_leg",
            "interpretation": json_value({"account_id": self.account_ids[row["accountId"]],
                                          "review_reasons": ["purpose_question"]}),
            "active_question": json_value(question),
        })
        self.report.skip(f"review.items[{item['id']}].options",
                         "Tinjau offers suggestions from the owner's history and a free answer; "
                         "the prototype's three generic options are not stored suggestions")

    def load_awaiting_counterpart(self, item: dict[str, Any], row: dict[str, Any], accounts: dict[str, Any]) -> None:
        institution = INSTITUTIONS[accounts[row["accountId"]]["institution"]]
        self.recorded_event(item["id"], institution=institution, post_time=self.shift.instant(item["date"]),
                            snapshot=self.transaction_snapshot(row), values={
            "transaction_id": self.transaction_ids[row["id"]], "provenance_kind": "observed_leg",
            "interpretation": json_value({"account_id": self.account_ids[row["accountId"]],
                                          "review_reasons": ["awaiting_counterpart"]}),
        })
        self.report.skip(f"review.items[{item['id']}].candidates",
                         "the app offers counterpart candidates only from recorded opposite legs; none exist")
        self.report.skip(f"review.items[{item['id']}] notification text", "data.js gives no raw notification text")

    def load_seen_twice(self, item: dict[str, Any]) -> None:
        """The second report of one payment, waiting in "Terlihat dua kali" (seen_twice.py)."""
        second = item["pair"][-1]
        institution = INSTITUTIONS[second["source"]]
        hour, minute = (int(part) for part in second["time"].split("."))
        post_time = self.shift.instant(item["date"]).replace(hour=hour, minute=minute)
        amount_text = f"Rp{item['amount']:,}".replace(",", ".")
        # Constructed text: data.js gives none, and the item's amount and direction are parsed from it.
        text = f"Pembayaran {amount_text} ke {item['title'].rsplit(' Rp', 1)[0]} berhasil."
        self.recorded_event(item["id"], institution=institution, post_time=post_time, snapshot={}, values={
            "title": APP_LABELS[institution], "body_text": text, "big_text": text,
            "interpretation": json_value({"review_reasons": []}), "provenance_kind": "duplicate_observation",
        })
        self.report.add(f"notification text for review.items[{item['id']}]: {text!r}",
                        "constructed so the app can read the amount and direction of the second report")
        self.report.skip(f"review.items[{item['id']}].pair[0]",
                         "the first report's transaction (GoPay, 23,000) is not among data.js's transactions, "
                         "so the item has no kept row")

    def load_ignored_notice(self, item: dict[str, Any]) -> None:
        institution = INSTITUTIONS[item["app"]]
        post_time = self.shift.instant(item["date"])
        self.notification_event(item["id"], institution=institution, post_time=post_time, values={
            "title": item["title"], "body_text": item["detail"], "big_text": item["detail"],
            "is_financial": False, "processing_state": "ignored",
        })
        self.report.skip(f"review.items[{item['id']}] folder",
                         "stored as an ignored non-financial notification; the app's 'Dilewati otomatis' "
                         "lists reports skipped by an account link, so it does not list promos or codes")

    def load_habits(self) -> None:
        """Owner labels behind data.js's habits; the app derives its habit list from them (habit_list.py)."""
        rows = sorted(self.data["transactions"], key=lambda row: row["date"], reverse=True)
        for habit in self.data["habits"]:
            if habit.get("productId"):
                self.report.skip(f"habits[{habit['id']}].answers",
                                 "the top-up habit is learned from the converted top-ups t008 and t017 "
                                 f"(2 in data.js, {habit['answers']} stated)")
                continue
            labelled = [
                row for row in rows
                if row.get("merchant") == habit["label"] and row.get("categoryId") == habit.get("categoryId")
                and row["source"] == "notifikasi"
            ][: habit["answers"]]
            if len(labelled) < habit["answers"]:
                self.report.skip(f"habits[{habit['id']}]",
                                 f"needs {habit['answers']} labelled rows of {habit['pattern']!r}; data.js has {len(labelled)}")
                continue
            category_id = self.category_ids[habit["categoryId"]]
            for row in labelled:
                moment = self.shift.instant(row["date"]).astimezone(ZONE)
                account = next(a for a in self.data["accounts"] if a["id"] == row["accountId"])
                self.insert("classification_feedback", {
                    "user_id": self.user_id, "kind": "correction",
                    "transaction_id": self.transaction_ids[row["id"]],
                    "before_merchant": None, "before_category_id": None, "before_type": "expense",
                    "after_merchant": habit["label"], "after_category_id": category_id,
                    "after_category_name": next(c["name"] for c in self.data["categories"] if c["id"] == habit["categoryId"]),
                    "after_kakeibo_type": self.category_pillars[habit["categoryId"]], "after_type": "expense",
                    "local_weekday": moment.isoweekday(), "local_hour": moment.hour, "amount": row["amount"],
                    "account_id": self.account_ids[row["accountId"]],
                    "institution": INSTITUTIONS[account["institution"]],
                    "text_signature": habit["pattern"], "transaction_source": "notification",
                    "after_classified_by": "owner", "after_review_status": "reviewed",
                    "created_at": moment + timedelta(minutes=1),
                })
                self.cur.execute(
                    "UPDATE transactions SET classified_by = 'owner', owner_edited_at = %s WHERE id = %s",
                    (moment + timedelta(minutes=1), self.transaction_ids[row["id"]]),
                )
        self.report.skip("habits[].match, habits[].active, habits[].note",
                         "derived by the app from the labels' agreement and count")

    def load_sender_aliases(self) -> None:
        salary_account = next(row for row in self.data["transactions"] if row.get("categoryId") == SALARY_CATEGORY)["accountId"]
        for alias in self.data.get("senderAliases") or []:
            self.insert("notification_sender_aliases", {
                "user_id": self.user_id, "institution": "bca",
                "receiving_account_id": self.account_ids[salary_account],
                "mask_normalized": normalize_mask(alias["name"]), "mask_display": alias["name"],
                "sender_name": alias["label"][:80], "state": "active",
            })
        self.report.skip("senderAliases[] receiving account",
                         f"data.js names none; stored for {salary_account} (BCA), where the salary arrives")

    def load_notification_mappings(self) -> None:
        for mapping in self.data.get("notificationMappings") or []:
            account = next(a for a in self.data["accounts"] if a["id"] == mapping["accountId"])
            self.insert("notification_account_aliases", {
                "user_id": self.user_id, "institution": INSTITUTIONS[account["institution"]],
                "name_normalized": normalize_alias_name(mapping["text"]), "name_display": mapping["text"][:150],
                "account_id": self.account_ids[mapping["accountId"]], "source": "owner",
            })

    def load_test_sessions(self) -> None:
        for session in self.data.get("testSessions") or []:
            session_id = new_id()
            started = self.shift.instant(session["startedAt"])
            ended = started + timedelta(minutes=int(session["durationMin"]))
            self.insert("notification_test_sessions", {
                "id": session_id, "user_id": self.user_id, "started_at": started,
                "planned_end_at": ended, "ended_at": ended, "created_at": started,
            })
            for index in range(int(session["captured"])):
                moment = started + timedelta(minutes=5 + index * 7)
                self.notification_event(f"{session['id']}:{index}", institution="jago", post_time=moment, values={
                    "title": "Jago", "body_text": f"Notifikasi uji {index + 1}",
                    "processing_state": "ignored", "test_session_id": session_id,
                })
            self.report.add(f"{session['captured']} test-session notifications",
                            "data.js states only the count; the app counts the session's captured events")

    def record_unstored_sections(self) -> None:
        for section, reason in (
            ("reports", "the app computes monthly reports from the rows"),
            ("notifications", "the app derives its notifications"),
            ("netWorth", "computed by the app"),
            ("ketahananDana", "computed by the app"),
            ("cycle, previousCycle, threeMonthAverage", "computed by the app from the rows"),
            ("explanations, captureExamples, pillars", "page text and examples, not data"),
        ):
            self.report.skip(section, reason)

    def verify_ledger_balances(self) -> None:
        """Every cash and credit account must end at its data.js balance; else the transaction rolls back."""
        self.cur.execute(
            """SELECT a.id, a.initial_balance
                      + COALESCE(SUM(CASE WHEN t.type = 'income' THEN t.amount ELSE 0 END), 0)
                      - COALESCE(SUM(CASE WHEN t.type = 'expense' THEN t.amount ELSE 0 END), 0) AS balance
               FROM accounts a LEFT JOIN transactions t ON t.account_id = a.id
               WHERE a.user_id = %s GROUP BY a.id""",
            (self.user_id,),
        )
        ledger = {str(row["id"]): int(row["balance"]) for row in self.cur.fetchall()}
        mismatches = []
        for account in self.data["accounts"]:
            if self.has_children(account["id"]) or account["type"] == "broker":
                continue
            stated = int(account["balance"])
            loaded = ledger[self.account_ids[account["id"]]]
            if stated != loaded:
                mismatches.append(f"{account['id']}: data.js {stated}, ledger {loaded}")
        if mismatches:
            raise RuntimeError("ledger balances differ from data.js: " + "; ".join(mismatches))


def reset_application_tables(cur, target: DatabaseTarget) -> list[str]:
    if not target.name.startswith(DATABASE_PREFIX):
        raise SeedRefusal("--reset only truncates a cft_markas_ database")
    cur.execute(
        """SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"""
    )
    tables = [row["tablename"] for row in cur.fetchall() if row["tablename"] not in KEPT_TABLES]
    if tables:
        cur.execute("TRUNCATE " + ", ".join(f'public."{name}"' for name in tables) + " RESTART IDENTITY CASCADE")
    return tables


# --- Check through the API -------------------------------------------------------------------------


def dig(payload: Any, *path: Any) -> Any:
    for key in path:
        if payload is None:
            return None
        if isinstance(key, int):
            payload = payload[key] if isinstance(payload, list) and len(payload) > key else None
        else:
            payload = payload.get(key) if isinstance(payload, dict) else None
    return payload


class FigureCheck:
    def __init__(self):
        self.figures: list[dict[str, Any]] = []
        self.errors: list[dict[str, str]] = []

    def compare(self, figure: str, stated: Any, app: Any, source: str, note: str | None = None) -> None:
        entry = {"figure": figure, "data_js": stated, "app": app, "app_source": source,
                 "agree": stated == app if app is not None else False}
        if note:
            entry["note"] = note
        self.figures.append(entry)


def run_api_check(api: str, data: dict[str, Any], shift: DayShift, password: str) -> FigureCheck:
    import httpx

    check = FigureCheck()
    base = api.rstrip("/")
    with httpx.Client(base_url=base, timeout=60, trust_env=False) as client:
        login = client.post("/api/auth/login", json={"username": data["user"]["username"], "password": password})
        if login.status_code != 200:
            raise RuntimeError(f"login through the API failed with {login.status_code}")

        def get(path: str, **params: Any) -> Any:
            response = client.get(path, params=params or None)
            if response.status_code != 200:
                check.errors.append({"request": path, "status": str(response.status_code), "body": response.text[:300]})
                return None
            return response.json()

        net_worth = get("/api/dashboard/net-worth")
        cycle = get("/api/dashboard/cycle-summary")
        accounts = get("/api/accounts")
        review_summary = get("/api/review/summary")
        queue = get("/api/review/queue")
        suggestions = get("/api/review/suggestions")
        goals = get("/api/goals")
        obligations = get("/api/obligations", include_archived="true")
        recurring = get("/api/recurring")
        habits = get("/api/habits")
        devices = get("/api/devices")
        month_start = shift.day(f"{data['meta']['today'][:7]}-01")
        previous_month = (month_start.replace(day=1) - timedelta(days=1)).replace(day=1)
        october_report = get("/api/reports/monthly", basis="month", start_date=previous_month.isoformat())
        previous_cycle = get("/api/reports/monthly", basis="cycle",
                             start_date=shift.day_text(data["previousCycle"]["start"]))

    compare_net_worth(check, data, net_worth, accounts)
    compare_cycle(check, data, shift, cycle)
    compare_accounts(check, data, accounts)
    compare_review(check, data, review_summary, queue, suggestions)
    compare_plans_and_goals(check, data, shift, goals, obligations, recurring)
    compare_reports(check, data, shift, october_report, previous_cycle)
    compare_learning(check, data, habits, devices)
    return check


def flatten_accounts(accounts: Any) -> list[dict[str, Any]]:
    rows = dig(accounts, "accounts") if isinstance(accounts, dict) else accounts
    flat: list[dict[str, Any]] = []

    def walk(items):
        for item in items or []:
            flat.append(item)
            walk(item.get("children"))

    walk(rows)
    unique = {}
    for item in flat:
        unique[item["id"]] = item
    return list(unique.values())


def compare_net_worth(check: FigureCheck, data: dict[str, Any], net_worth: Any, accounts: Any) -> None:
    stated = data["netWorth"]
    source = "GET /api/dashboard/net-worth"
    check.compare("netWorth.netWorth", stated["netWorth"], dig(net_worth, "net_worth"), f"{source} net_worth")
    check.compare("netWorth.assets", stated["assets"], dig(net_worth, "total_assets"), f"{source} total_assets")
    check.compare("netWorth.liquid + netWorth.tabungan", stated["liquid"] + stated["tabungan"], dig(net_worth, "liquid"),
                  f"{source} liquid", "the app's Dana likuid holds harian and tabungan accounts together")
    flat = flatten_accounts(accounts)
    leaves = [row for row in flat if not row.get("children")]
    tabungan = sum(int(row["balance"]) for row in leaves if row.get("role") == "tabungan")
    harian = sum(int(row["balance"]) for row in leaves if row.get("role") == "harian")
    check.compare("netWorth.liquid", stated["liquid"], harian if flat else None,
                  "GET /api/accounts sum of leaf balances with role harian")
    check.compare("netWorth.tabungan", stated["tabungan"], tabungan if flat else None,
                  "GET /api/accounts sum of leaf balances with role tabungan")
    check.compare("netWorth.danaInvestasi", stated["danaInvestasi"], dig(net_worth, "dana_investasi"), f"{source} dana_investasi")
    check.compare("netWorth.investasi", stated["investasi"], dig(net_worth, "investment"), f"{source} investment")
    products = [row for row in leaves if row.get("instrument_type")]
    cost = sum(int(row["cost_basis"] or 0) for row in products)
    check.compare("netWorth.investasiCost", stated["investasiCost"], cost if products else None,
                  "GET /api/accounts sum of product cost_basis")
    check.compare("netWorth.investasiGain", stated["investasiGain"],
                  sum(int(row["capital_gain"] or 0) for row in products) if products else None,
                  "GET /api/accounts sum of product capital_gain")
    check.compare("netWorth.debt", stated["debt"], dig(net_worth, "total_liabilities"), f"{source} total_liabilities")
    check.compare("netWorth.debtCredit", stated["debtCredit"], dig(net_worth, "credit_owed"), f"{source} credit_owed")
    obligations = dig(net_worth, "obligations")
    check.compare("netWorth.debtObligations", stated["debtObligations"],
                  sum(int(o["remaining_amount"]) for o in obligations) if obligations is not None else None,
                  f"{source} obligations[].remaining_amount")
    runway = dig(net_worth, "runway") or {}
    dana = data["ketahananDana"]
    check.compare("ketahananDana.emergencyBalance", dana["emergencyBalance"], runway.get("emergency_fund_balance"),
                  f"{source} runway.emergency_fund_balance")
    check.compare("ketahananDana.monthlyCore", dana["monthlyCore"], runway.get("monthly_primary_expense"),
                  f"{source} runway.monthly_primary_expense",
                  "data.js averages stored Agu/Sep summaries with Okt; the app has rows only from 25 Sep")
    check.compare("ketahananDana.months", dana["months"], runway.get("coverage_months"), f"{source} runway.coverage_months")
    check.compare("ketahananDana.days", dana["days"], runway.get("runway_days"), f"{source} runway.runway_days")
    check.compare("ketahananDana.target", dana["target"], runway.get("target_amount"), f"{source} runway.target_amount")
    check.compare("ketahananDana.targetMonths", dana["targetMonths"],
                  int(runway["target_months"]) if runway.get("target_months") is not None else None,
                  f"{source} runway.target_months")


def compare_cycle(check: FigureCheck, data: dict[str, Any], shift: DayShift, cycle: Any) -> None:
    stated = data["cycle"]
    source = "GET /api/dashboard/cycle-summary"
    check.compare("cycle.start", shift.day_text(stated["start"]), dig(cycle, "period", "start_date"), f"{source} period.start_date")
    check.compare("cycle.day", stated["day"], dig(cycle, "period", "day_index"), f"{source} period.day_index")
    check.compare("cycle.nextPaydayEstimate", shift.day_text(stated["nextPaydayEstimate"]), dig(cycle, "expected_salary", "date"),
                  f"{source} expected_salary.date",
                  "the app expects the latest receipt's day (23) next month; data.js uses the payday day (25)")
    check.compare("cycle.daysLeft", stated["daysLeft"], dig(cycle, "safe_to_spend", "days_left"), f"{source} safe_to_spend.days_left")
    check.compare("cycle.income", stated["income"], dig(cycle, "masuk", "total"), f"{source} masuk.total")
    check.compare("cycle.expense", stated["expense"], dig(cycle, "spending", "amount"), f"{source} spending.amount")
    check.compare("cycle.saved", stated["saved"], dig(cycle, "fresh_saving", "amount"), f"{source} fresh_saving.amount",
                  "data.js counts moves into dana_investasi (RDN) as saving; the app's saving rule does not")
    check.compare("cycle.scheduled", stated["scheduled"], dig(cycle, "scheduled", "total"), f"{source} scheduled.total",
                  "the app adds overdue plans and dated goals' monthly set-aside; data.js counts unpaid plans only")
    check.compare("cycle.scheduled (plans only)", stated["scheduled"], dig(cycle, "scheduled", "plans_total"),
                  f"{source} scheduled.plans_total")
    # data.js's safeToSpend is the salary view that count-safe-amount-from-daily-money replaced; the app's figure now
    # reads balances (safe_amount), so the two are no longer the same quantity and are not compared.
    check.compare("cycle.safePerDay", stated["safePerDay"], dig(cycle, "safe_to_spend", "per_day"), f"{source} safe_to_spend.per_day",
                  "the app rounds down to Rp 1.000 per day")
    lines = {line["key"]: line["amount"] for line in dig(cycle, "keluar", "lines") or []}
    for pillar, key in (("need", "need"), ("want", "want"), ("culture", "culture"), ("unexpected", "unexpected"),
                        ("none", "unclassified"), ("saving", "saving")):
        check.compare(f"cycle.byPillar.{pillar}", stated["byPillar"][pillar], lines.get(key), f"{source} keluar.lines[{key}]")
    check.compare("netWorth.netWorth (Beranda)", data["netWorth"]["netWorth"], dig(cycle, "net_worth", "total"),
                  f"{source} net_worth.total")


def compare_accounts(check: FigureCheck, data: dict[str, Any], accounts: Any) -> None:
    flat = flatten_accounts(accounts)
    by_name: dict[tuple[str | None, str], dict[str, Any]] = {}
    names = {row["id"]: row["name"] for row in flat}
    for row in flat:
        by_name[(names.get(row.get("parent_id")), row["name"])] = row
    stated_names = {account["id"]: account["name"] for account in data["accounts"]}
    for account in data["accounts"]:
        parent = stated_names.get(account.get("parentId")) if account.get("parentId") else None
        row = by_name.get((parent, account["name"]))
        check.compare(f"accounts[{account['id']}].balance", account["balance"], int(row["balance"]) if row else None,
                      "GET /api/accounts balance")
        if row is not None and row.get("role") != account["role"]:
            check.compare(f"accounts[{account['id']}].role", account["role"], row.get("role"), "GET /api/accounts role")
    platforms = {account["id"]: account["name"] for account in data["accounts"]}
    for product in data["products"]:
        row = by_name.get((platforms[product["accountId"]], product["name"]))
        check.compare(f"products[{product['id']}].value", product["value"], int(row["balance"]) if row else None,
                      "GET /api/accounts balance")
        check.compare(f"products[{product['id']}].costBasis", product["costBasis"],
                      int(row["cost_basis"]) if row and row.get("cost_basis") is not None else None, "GET /api/accounts cost_basis")
    for entry in data.get("credit") or []:
        row = by_name.get((None, stated_names[entry["accountId"]]))
        check.compare("credit.limit", entry["limit"], dig(row, "credit_limit"), "GET /api/accounts credit_limit")
        check.compare("credit.used", entry["used"], dig(row, "credit_owed"), "GET /api/accounts credit_owed")
        available = None
        if row and row.get("credit_limit") is not None and row.get("credit_owed") is not None:
            available = int(row["credit_limit"]) - int(row["credit_owed"])
        check.compare("credit.available", entry["available"], available, "GET /api/accounts credit_limit - credit_owed")


def compare_review(check: FigureCheck, data: dict[str, Any], summary: Any, queue: Any, suggestions: Any) -> None:
    items = data["review"]["items"]
    open_items = [item for item in items if not item.get("optional")]
    check.compare("Tinjau badge (open review items)", len(open_items), dig(summary, "attention"),
                  "GET /api/review/summary attention",
                  "the prototype counts items (including Saran and Terlihat dua kali); the app counts waiting rows")
    waiting_rows = sum(1 for row in data["transactions"] if row.get("status") == "needs_review")
    check.compare("needs_review transactions", waiting_rows, dig(summary, "attention"), "GET /api/review/summary attention")
    counts = dig(queue, "counts") or {}

    def folder(kind: str) -> int:
        return sum(1 for item in items if item["kind"] == kind)

    check.compare("review folder belum_dikenali", folder("belum_dikenali"), counts.get("questions"), "GET /api/review/queue counts.questions")
    jawab_items = [item for item in items if item["kind"] == "perlu_jawaban"]
    check.compare("review folder perlu_jawaban (items)", len(jawab_items),
                  sum(1 for group in dig(queue, "groups") or [] if group["needs_review_count"] > 0),
                  "GET /api/review/queue groups with needs_review_count > 0",
                  "rv-8 (the top-up question) is not loaded: it repeats t017, already a top-up")
    check.compare("review folder pindah_belum_lengkap", folder("pindah_belum_lengkap"),
                  (counts.get("awaiting") or 0) + (counts.get("ambiguous") or 0) if counts else None,
                  "GET /api/review/queue counts.awaiting + counts.ambiguous")
    check.compare("review folder saran", folder("saran"), dig(suggestions, "pending"), "GET /api/review/suggestions pending",
                  "rv-4 points at Kopi Senja rows that data.js does not hold")
    check.compare("review folder terlihat_dua_kali", folder("terlihat_dua_kali"), counts.get("seen_twice"),
                  "GET /api/review/queue counts.seen_twice")
    check.compare("review folder dilewati_otomatis", folder("dilewati_otomatis"), counts.get("skipped"),
                  "GET /api/review/queue counts.skipped",
                  "the app's folder lists reports an account link skipped, not promos or codes")


def compare_plans_and_goals(check: FigureCheck, data: dict[str, Any], shift: DayShift,
                            goals: Any, obligations: Any, recurring: Any) -> None:
    goal_rows = {row["name"]: row for row in (dig(goals, "goals") or goals or [])} if goals is not None else {}
    for goal in data["goals"]:
        row = goal_rows.get(goal["name"])
        current = None
        if row:
            current = row.get("current_amount", row.get("saved_amount"))
        check.compare(f"goals[{goal['id']}].saved", goal["saved"], current, "GET /api/goals current_amount")
        check.compare(f"goals[{goal['id']}].target", goal["target"], dig(row, "target_amount"), "GET /api/goals target_amount")
    debt_rows = {row["name"]: row for row in (dig(obligations, "obligations") or obligations or [])} if obligations is not None else {}
    for obligation in data["obligations"]:
        row = debt_rows.get(obligation["name"])
        check.compare(f"obligations[{obligation['id']}].remaining", obligation["remaining"], dig(row, "remaining_amount"),
                      "GET /api/obligations?include_archived=true remaining_amount")
        check.compare(f"obligations[{obligation['id']}].archived", bool(obligation.get("archived")), dig(row, "is_archived"),
                      "GET /api/obligations?include_archived=true is_archived")
    rule_rows = {row["name"]: row for row in (dig(recurring, "rules") or dig(recurring, "recurring") or recurring or [])} \
        if isinstance(recurring, (list, dict)) else {}
    for rule in data["recurring"]:
        row = rule_rows.get(rule["name"])
        check.compare(f"recurring[{rule['id']}].nextDate", shift.day_text(rule["nextDate"]), dig(row, "next_due_date"),
                      "GET /api/recurring next_due_date")


def compare_reports(check: FigureCheck, data: dict[str, Any], shift: DayShift, october: Any, previous_cycle: Any) -> None:
    source = "GET /api/reports/monthly"
    report = next((r for r in data["reports"] if r["month"] == "2026-10"), None)
    if report:
        check.compare("reports[2026-10].income", report["income"], dig(october, "flow", "inflow"),
                      f"{source}?basis=month flow.inflow")
        check.compare("reports[2026-10].expense", report["expense"], dig(october, "flow", "outflow"),
                      f"{source}?basis=month flow.outflow")
        check.compare("reports[2026-10].saved", report["saved"], dig(october, "savings", "fresh_saving"),
                      f"{source}?basis=month savings.fresh_saving",
                      "data.js counts moves into dana_investasi (RDN) as saving; the app's saving rule does not")
    stated = data["previousCycle"]
    check.compare("previousCycle.income", stated["income"], dig(previous_cycle, "flow", "inflow"),
                  f"{source}?basis=cycle flow.inflow")
    check.compare("previousCycle.expense", stated["expense"], dig(previous_cycle, "flow", "outflow"),
                  f"{source}?basis=cycle flow.outflow")
    check.compare("previousCycle.saved", stated["saved"], dig(previous_cycle, "savings", "fresh_saving"),
                  f"{source}?basis=cycle savings.fresh_saving",
                  "data.js counts moves into dana_investasi (RDN) as saving; the app's saving rule does not")
    check.compare("previousCycle.end", shift.day_text(stated["end"]), dig(previous_cycle, "period", "end_date"),
                  f"{source}?basis=cycle period.end_date",
                  "the app's closed cycle ends at the next receipt's instant, so its last local day is that payday")


def compare_learning(check: FigureCheck, data: dict[str, Any], habits: Any, devices: Any) -> None:
    source = "GET /api/habits"
    learned = {entry["label"]: entry for entry in dig(habits, "habits") or []}
    topups = {dig(entry, "product", "name"): entry for entry in dig(habits, "topups") or []}
    products = {product["id"]: product["name"] for product in data["products"]}
    for habit in data["habits"]:
        if habit.get("productId"):
            entry = topups.get(products[habit["productId"]])
            where = f"{source} topups[].confirmations"
        else:
            entry = learned.get(habit["label"])
            where = f"{source} habits[].confirmations"
        check.compare(f"habits[{habit['id']}].answers", habit["answers"], dig(entry, "confirmations"), where)
        state = dig(entry, "state")
        check.compare(f"habits[{habit['id']}].active", habit["active"], state == "ready" if entry else None,
                      f"{source} state == ready")
    device_rows = dig(devices, "devices") if isinstance(devices, dict) else devices
    paired = None
    if isinstance(device_rows, list):
        paired = sum(1 for row in device_rows if row.get("created_via") == "pairing")
    check.compare("devices", len(data["devices"]), paired, "GET /api/devices paired keys")


def write_check(out_dir: Path, report: LoadReport, data: dict[str, Any], data_sha: str,
                check: FigureCheck | None, today: date) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "data_js_sha256": data_sha,
        "data_js_today": data["meta"]["today"],
        "seed_today": today.isoformat(),
        "shift_days": report.shift_days,
        "row_counts": report.counts,
        "added_rows": report.added,
        "unmapped": report.unmapped,
    }
    if check is not None:
        payload["figures"] = check.figures
        payload["agree"] = sum(1 for entry in check.figures if entry["agree"])
        payload["disagree"] = [entry["figure"] for entry in check.figures if not entry["agree"]]
        payload["api_errors"] = check.errors
    path = out_dir / CHECK_FILE_NAME
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str) + "\n")
    return path


# --- Command line ----------------------------------------------------------------------------------


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load data.js into a disposable cft_markas_ database.")
    parser.add_argument("--data", required=True, type=Path)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--today", type=date.fromisoformat, default=None)
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--api", default=None)
    parser.add_argument("--check-out", type=Path, default=None)
    return parser.parse_args(argv)


def connect_database(url: str):
    import psycopg
    from psycopg.rows import dict_row

    return psycopg.connect(url, row_factory=dict_row, autocommit=False)


def main(argv: list[str] | None = None, env: dict[str, str] | None = None,
         connect: Callable[[str], Any] = connect_database) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    env = dict(os.environ) if env is None else env
    try:
        password = require_password(env)
        target = database_target(args.database_url)
        raw = args.data.read_bytes()
        data = parse_dataset(raw.decode("utf-8"))
        require_synthetic(data)
    except (SeedRefusal, OSError) as error:
        print(f"refused: {error}", file=sys.stderr)
        return 2

    today = args.today or default_today()
    shift = DayShift((today - date.fromisoformat(data["meta"]["today"])).days)
    data_sha = hashlib.sha256(raw).hexdigest()
    conn = connect(args.database_url)
    try:
        with conn.cursor() as cur:
            preflight(cur, reset=args.reset)
            if args.reset:
                reset_application_tables(cur, target)
            report = ParityLoader(cur, data, shift, password).load()
        conn.commit()
    except SeedRefusal as error:
        conn.rollback()
        print(f"refused: {error}", file=sys.stderr)
        return 2
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"loaded {target.describe()} in one transaction; shift {shift.days} day(s); data.js sha256 {data_sha}")
    for table, count in sorted(report.counts.items()):
        print(f"  {table}: {count}")

    check = None
    if args.api:
        check = run_api_check(args.api, data, shift, password)
        agreeing = sum(1 for entry in check.figures if entry["agree"])
        print(f"check: {agreeing} of {len(check.figures)} figures agree")
    if args.check_out:
        path = write_check(args.check_out, report, data, data_sha, check, today)
        print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
