"""Tinjau's review kinds on top of a seeded disposable database (apply-markas-tinjau D22).

Local verification tooling only, like seed_parity.py: it runs in no image, CI job or npm script, refuses any
database that is not a loopback `cft_markas_*` one, and writes in one transaction. It adds, for an existing
synthetic owner, the review kinds the parity seed cannot take from data.js, built so that nothing is booked twice:

- `--kinds`: an automatic group, an ambiguous leg with two candidate sends, an awaiting debit beside the
  owner's earlier top-ups (the learned top-up shows when the habit applies), a pending history-suggestion group,
  a report seen twice with its kept leg, and a report an account link skipped;
- `--stress`: a 250-row group with a 60-character merchant, more than 1,000 waiting rows (the queue is then
  truncated) and one Rp 1.234.567.890 spending;
- `--currency USD|IDR`: the owner's display currency.

    backend/.venv/bin/python scripts/verify/markas/seed_tinjau.py --database-url postgresql://...@127.0.0.1:<port>/cft_markas_parity \\
        --username dimas_contoh --kinds [--stress] [--currency USD]
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent))
from seed_parity import SeedRefusal, connect_database, database_target, payload_hash  # noqa: E402

ZONE = ZoneInfo("Asia/Jakarta")
DEVICE_ID = "markas-tinjau-dev-1"
LONG_MERCHANT = "Toko Peralatan Rumah Tangga dan Dapur Sejahtera Makmur Abadi"  # 60 characters
STRESS_GROUP_ROWS = 250
STRESS_WAITING_ROWS = 1_050
BIG_AMOUNT = 1_234_567_890


class TinjauSeed:
    def __init__(self, cur, username: str, now: datetime):
        self.cur = cur
        self.now = now
        self.added: list[str] = []
        self.skipped: list[str] = []
        cur.execute("SELECT id FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
        if not row:
            raise SeedRefusal(f"no owner {username!r}; run the parity or audit seed first")
        self.user_id = str(row["id"])

    def insert(self, table: str, values: dict[str, Any]) -> str:
        values = {"id": str(uuid.uuid4()), **values}
        columns = ", ".join(values)
        placeholders = ", ".join(["%s"] * len(values))
        self.cur.execute(f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", list(values.values()))
        return values["id"]

    def account(self, *types: str, exclude: tuple[str, ...] = ()) -> dict[str, Any]:
        self.cur.execute(
            """SELECT id, name, type FROM accounts
               WHERE user_id = %s AND type = ANY(%s) AND is_archived = FALSE AND instrument_type IS NULL
                 AND NOT EXISTS (SELECT 1 FROM accounts child WHERE child.parent_id = accounts.id)
                 AND id <> ALL(%s::uuid[])
               ORDER BY name LIMIT 1""",
            (self.user_id, list(types), list(exclude)),
        )
        row = self.cur.fetchone()
        if not row:
            raise SeedRefusal(f"the owner has no {'/'.join(types)} account left to use")
        return row

    def category(self, kind: str, *, system_key: str | None = None, offset: int = 0) -> dict[str, Any]:
        if system_key:
            self.cur.execute("SELECT id, kakeibo_type FROM categories WHERE user_id = %s AND kind = %s AND system_key = %s",
                             (self.user_id, kind, system_key))
        else:
            self.cur.execute(
                """SELECT id, kakeibo_type FROM categories WHERE user_id = %s AND kind = %s AND system_key IS NULL
                     AND is_archived = FALSE ORDER BY name OFFSET %s LIMIT 1""",
                (self.user_id, kind, offset),
            )
        row = self.cur.fetchone()
        if not row:
            raise SeedRefusal(f"no {kind} category {system_key or offset}")
        return row

    def transaction(self, key: str, *, account: dict, category: dict | None, kind: str, amount: int, when: datetime,
                    merchant: str | None, notes: str | None, status: str, classified_by: str | None) -> str:
        return self.insert("transactions", {
            "user_id": self.user_id, "account_id": account["id"], "category_id": category["id"] if category else None,
            "type": kind, "amount": amount, "notes": notes, "merchant": merchant, "date": when, "created_at": when,
            "kakeibo_type": category["kakeibo_type"] if category and kind == "expense" and status == "reviewed" else None,
            "idempotency_key": f"markas-tinjau:{key}", "source": "notification", "review_status": status,
            "classified_by": classified_by,
        })

    def event(self, key: str, *, when: datetime, text: str, values: dict[str, Any]) -> str:
        event_id = str(uuid.uuid4())
        record_key = f"notification:{event_id}"
        return self.insert("notification_events", {
            "id": event_id, "user_id": self.user_id, "device_id": DEVICE_ID, "package_name": "com.jago.digitalBanking",
            "app_label": "Jago", "title": "Jago", "body_text": text, "big_text": text, "post_time": when, "captured_at": when,
            "created_at": when, "payload_hash": payload_hash(f"tinjau:{key}"), "source_version": "markas-tinjau",
            "processing_mode": "deterministic", "is_financial": True, "processing_state": "recorded",
            "result_key": record_key, "result_snapshot": json.dumps({"record_key": record_key}), **values,
        })

    # Kinds ------------------------------------------------------------------------------------------

    def automatic_group(self) -> None:
        account = self.account("bank", "ewallet", "wallet", "cash")
        category = self.category("expense", offset=1)
        for index in range(2):
            self.transaction(f"auto-{index}", account=account, category=category, kind="expense", amount=18_000 + index * 4_500,
                             when=self.now - timedelta(hours=5 + index * 24), merchant="Kopi Pojok Contoh", notes="Kopi Pojok Contoh",
                             status="auto", classified_by="rule")
        self.added.append("automatic group: Kopi Pojok Contoh, 2 auto rows")

    def ambiguous_leg(self) -> None:
        receiver = self.account("bank")
        first = self.account("ewallet", "wallet", exclude=(receiver["id"],))
        second = self.account("ewallet", "wallet", "cash", exclude=(receiver["id"], first["id"]))
        moving_in = self.category("income", system_key="internal_movement")
        moving_out = self.category("expense", system_key="internal_movement")
        when = self.now - timedelta(hours=3)
        credit = self.transaction("ambiguous-in", account=receiver, category=moving_in, kind="income", amount=750_000, when=when,
                                  merchant=None, notes="Transfer masuk", status="needs_review", classified_by="system")
        self.event("ambiguous-in", when=when, text="Kamu menerima Rp750.000.", values={
            "transaction_id": credit, "provenance_kind": "observed_leg",
            "interpretation": json.dumps({"account_id": str(receiver["id"]), "review_reasons": ["ambiguous_movement"]}),
        })
        for index, source in enumerate((first, second)):
            self.transaction(f"ambiguous-out-{index}", account=source, category=moving_out, kind="expense", amount=750_000,
                             when=when - timedelta(minutes=1 + index), merchant=None, notes="Kirim saldo",
                             status="reviewed", classified_by="system")
        self.added.append(f"ambiguous leg: Rp 750.000 into {receiver['name']}, sends from {first['name']} and {second['name']}")

    def awaiting_topup(self) -> None:
        # The owner's earlier conversions (the parity seed's t008 and t017) teach the top-up habit.
        self.cur.execute(
            """SELECT source_account_id AS account_id, amount FROM investment_topups
               WHERE user_id = %s AND converted_from IS NOT NULL ORDER BY date DESC LIMIT 1""",
            (self.user_id,),
        )
        learned = self.cur.fetchone()
        if not learned:
            self.skipped.append("awaiting debit with a learned top-up: the owner has no converted top-up to learn from")
            return
        self.cur.execute("SELECT id, name, type FROM accounts WHERE id = %s", (learned["account_id"],))
        account = self.cur.fetchone()
        when = self.now - timedelta(hours=2)
        debit = self.transaction("awaiting-topup", account=account, category=self.category("expense", system_key="internal_movement"),
                                 kind="expense", amount=int(learned["amount"]), when=when, merchant=None, notes="Pindah saldo keluar",
                                 status="needs_review", classified_by="system")
        self.event("awaiting-topup", when=when, text="Transfer keluar.", values={
            "transaction_id": debit, "provenance_kind": "observed_leg",
            "interpretation": json.dumps({"account_id": str(account["id"]), "review_reasons": ["awaiting_counterpart"]}),
        })
        amount = f"{int(learned['amount']):,}".replace(",", ".")
        self.added.append(f"awaiting debit of Rp {amount} on {account['name']}, beside its learned top-ups")

    def history_suggestions(self) -> None:
        account = self.account("bank", "ewallet", "wallet", "cash")
        before = self.category("expense", offset=2)
        after = self.category("expense", offset=3)
        for index in range(3):
            when = self.now - timedelta(days=8 + index, hours=2)
            transaction = self.transaction(f"suggestion-{index}", account=account, category=before, kind="expense", amount=35_000,
                                           when=when, merchant="Kafe Senja Contoh", notes="Kafe Senja Contoh",
                                           status="reviewed", classified_by="rule")
            self.insert("transaction_suggestions", {
                "user_id": self.user_id, "transaction_id": transaction, "proposal_source": "rule",
                "proposed_merchant": "Kafe Senja Contoh", "proposed_notes": "Kafe Senja Contoh",
                "proposed_category_id": after["id"], "proposed_kakeibo_type": after["kakeibo_type"],
                "reason": "Kafe biasanya masuk kategori ini", "before_type": "expense", "before_merchant": "Kafe Senja Contoh",
                "before_notes": "Kafe Senja Contoh", "before_category_id": before["id"],
                "before_kakeibo_type": before["kakeibo_type"] or "need",
            })
        self.added.append("history-suggestion group: Kafe Senja Contoh, 3 pending suggestions")

    def kept_leg(self, key: str, account: dict, when: datetime) -> str:
        return self.transaction(key, account=account, category=self.category("income", offset=0), kind="income", amount=500_000,
                                when=when, merchant=None, notes="Uang masuk", status="reviewed", classified_by="rule")

    def seen_twice(self) -> None:
        account = self.account("ewallet", "wallet")
        when = self.now - timedelta(hours=6)
        kept = self.kept_leg("seen-twice-kept", account, when)
        self.event("seen-twice", when=when + timedelta(minutes=1), text="Kamu menerima Rp500.000 dari GoPay.", values={
            "provenance_kind": "duplicate_observation", "duplicate_of_transaction_id": kept,
            "interpretation": json.dumps({"review_reasons": []}),
        })
        self.added.append(f"seen twice: a Jago report of Rp 500.000, kept leg on {account['name']}")

    def skipped_by_link(self) -> None:
        account = self.account("ewallet", "wallet")
        when = self.now - timedelta(days=1, hours=4)
        kept = self.kept_leg("skipped-kept", account, when)
        link = self.insert("account_report_links", {
            "user_id": self.user_id, "account_id": account["id"], "institution": "jago", "source": "confirmation",
            "created_at": when - timedelta(days=2),
        })
        self.event("skipped", when=when + timedelta(minutes=1), text="Kamu menerima Rp500.000 dari GoPay.", values={
            "provenance_kind": "duplicate_observation", "duplicate_of_transaction_id": kept, "reporting_link_id": link,
            # A link's skip settles the report at once; it waits for nothing (seen_twice.py).
            "reviewed_at": when + timedelta(minutes=1), "interpretation": json.dumps({"review_reasons": []}),
        })
        self.added.append(f"skipped by a link: {account['name']} also reported by Jago")

    # Stress -----------------------------------------------------------------------------------------

    def stress(self) -> None:
        account = self.account("bank", "ewallet", "wallet", "cash")
        category = self.category("expense", offset=0)
        for index in range(STRESS_GROUP_ROWS):
            self.transaction(f"stress-group-{index}", account=account, category=category, kind="expense", amount=12_500 + index,
                             when=self.now - timedelta(minutes=30 + index), merchant=LONG_MERCHANT, notes=LONG_MERCHANT,
                             status="needs_review", classified_by="ai")
        for index in range(STRESS_WAITING_ROWS):
            merchant = f"Warung Contoh {index % 40:02d}"
            self.transaction(f"stress-waiting-{index}", account=account, category=category, kind="expense", amount=9_000 + index,
                             when=self.now - timedelta(days=1, minutes=index), merchant=merchant, notes=merchant,
                             status="needs_review", classified_by="ai")
        self.transaction("stress-big", account=account, category=category, kind="expense", amount=BIG_AMOUNT,
                         when=self.now - timedelta(minutes=10), merchant="Pembelian Rumah Contoh", notes="Pembelian Rumah Contoh",
                         status="needs_review", classified_by="ai")
        self.added.append(f"stress: {STRESS_GROUP_ROWS} rows of {LONG_MERCHANT!r} ({len(LONG_MERCHANT)} characters), "
                          f"{STRESS_WAITING_ROWS} more waiting rows, and one spending of Rp 1.234.567.890")

    def currency(self, code: str) -> None:
        self.cur.execute("UPDATE users SET currency = %s WHERE id = %s", (code, self.user_id))
        self.added.append(f"display currency {code}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--kinds", action="store_true")
    parser.add_argument("--stress", action="store_true")
    parser.add_argument("--currency", choices=["IDR", "USD"])
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        database_target(args.database_url)
        conn = connect_database(args.database_url)
    except SeedRefusal as refusal:
        print(f"refused: {refusal}", file=sys.stderr)
        return 2
    with conn:
        with conn.cursor() as cur:
            try:
                seed = TinjauSeed(cur, args.username, datetime.now(ZONE).replace(second=0, microsecond=0))
                if args.kinds:
                    for step in (seed.automatic_group, seed.ambiguous_leg, seed.awaiting_topup, seed.history_suggestions,
                                 seed.seen_twice, seed.skipped_by_link):
                        step()
                if args.stress:
                    seed.stress()
                if args.currency:
                    seed.currency(args.currency)
            except SeedRefusal as refusal:
                conn.rollback()
                print(f"refused: {refusal}", file=sys.stderr)
                return 2
    print(json.dumps({"added": seed.added, "skipped": seed.skipped}, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
