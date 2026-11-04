"""List past arrivals one owner's ledger may have booked twice (recognise-one-arrival-seen-twice D7).

Read-only: everything runs in a `SET TRANSACTION READ ONLY` transaction, and nothing is changed or
fixed. The owner settles each case in the app. Prints dates, account labels, amounts, ids and match
keys; never a notification's text.

It is self-contained, so it also runs on a release that predates the one-arrival rule:

    PYTHONPATH=backend python3 scripts/verify/find_duplicate_arrivals.py --user-id <uuid> [--since YYYY-MM-DD]
    docker exec -i -e PGOPTIONS="-c default_transaction_read_only=on" ledger_api \\
        python - --user-id <uuid> < scripts/verify/find_duplicate_arrivals.py
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

# From the repository the backend sits two levels up; piped into a container (`python -`) it is already
# importable and `__file__` is "<stdin>".
_SCRIPT = Path(globals().get("__file__", "<stdin>")).resolve()
if len(_SCRIPT.parents) > 2 and (_SCRIPT.parents[2] / "backend").is_dir():
    sys.path.insert(0, str(_SCRIPT.parents[2] / "backend"))

# The same bounds as the backend rule (movement_pairing.SAME_ARRIVAL_WINDOW_SECONDS).
SAME_ARRIVAL_WINDOW_SECONDS = 120
PACKAGES = {
    "com.bca.mybca.omni.android": "bca", "id.co.bca.mybca.omni.android": "bca", "com.bca": "bca",
    "com.jago.digitalbanking": "jago", "com.gojek.gopay": "gopay", "com.gojek.app": "gopay",
    "com.gopay.wallet": "gopay", "com.shopeepay.id": "shopeepay", "com.shopee.id": "shopeepay",
    "com.stockbit.android": "stockbit",
}
MOVEMENT_CATEGORY = "Internal Movement"
LOCAL_ZONE = ZoneInfo("Asia/Jakarta")

LEGS_SQL = """
    SELECT t.id, t.type, t.amount, t.account_id, t.review_status, t.classified_by,
           n.id AS event_id, n.package_name, n.post_time, n.interpretation ->> 'remote_account_id' AS remote_account_id
    FROM notification_events n
    JOIN transactions t ON t.id = n.transaction_id AND t.user_id = n.user_id
    WHERE n.user_id = %s AND n.processing_state = 'recorded' AND n.provenance_kind = 'observed_leg'
      AND t.movement_id IS NULL AND t.fee_for_movement_id IS NULL AND t.type IN ('expense', 'income')
      AND n.post_time >= %s
    ORDER BY n.post_time, n.id
"""


def _local(moment: datetime) -> str:
    return moment.astimezone(LOCAL_ZONE).strftime("%Y-%m-%d %H:%M:%S")


def _accounts(cur, user_id: str) -> dict[str, dict]:
    cur.execute("SELECT id, name, parent_id FROM accounts WHERE user_id = %s", (user_id,))
    rows = {str(row["id"]): row for row in cur.fetchall()}
    for row in rows.values():
        parent = rows.get(str(row["parent_id"])) if row["parent_id"] else None
        row["label"] = f"{parent['name']} · {row['name']}" if parent else row["name"]
        row["root"] = str(row["parent_id"] or row["id"])
    return rows


def _root(accounts: dict[str, dict], account_id) -> str | None:
    account = accounts.get(str(account_id)) if account_id else None
    return account["root"] if account else None


def mirror_pairs(cur, user_id: str, since: datetime, accounts: dict[str, dict]) -> list[dict]:
    """Two unlinked legs of one direction and amount from two apps that name each other (design D1)."""
    cur.execute(LEGS_SQL, (user_id, since))
    legs = cur.fetchall()
    pairs = []
    for index, first in enumerate(legs):
        for second in legs[index + 1:]:
            gap = (second["post_time"] - first["post_time"]).total_seconds()
            if gap > SAME_ARRIVAL_WINDOW_SECONDS:
                break
            institutions = {PACKAGES.get(str(leg["package_name"]).lower()) for leg in (first, second)}
            if (first["type"] != second["type"] or first["amount"] != second["amount"]
                    or first["account_id"] == second["account_id"] or None in institutions or len(institutions) < 2):
                continue
            if not (_root(accounts, first["remote_account_id"]) == _root(accounts, second["account_id"])
                    and _root(accounts, second["remote_account_id"]) == _root(accounts, first["account_id"])):
                continue
            pairs.append({
                "date": _local(first["post_time"]), "seconds_apart": round(gap, 2), "type": first["type"],
                "amount": int(first["amount"]),
                "legs": [{
                    "transaction_id": str(leg["id"]), "event_id": str(leg["event_id"]),
                    "app": PACKAGES.get(str(leg["package_name"]).lower()),
                    "account": accounts.get(str(leg["account_id"]), {}).get("label"),
                    "review_status": leg["review_status"], "classified_by": leg["classified_by"],
                } for leg in (first, second)],
            })
    return pairs


def recorded_without_transaction(cur, user_id: str, since: datetime) -> list[dict]:
    """Recorded legs whose transaction is gone (deleted), excluding second observations of a kept leg."""
    cur.execute(
        """SELECT n.id, n.package_name, n.post_time, n.provenance_kind
           FROM notification_events n
           WHERE n.user_id = %s AND n.processing_state = 'recorded' AND n.transaction_id IS NULL
             AND n.movement_id IS NULL AND n.provenance_kind = 'observed_leg' AND n.post_time >= %s
           ORDER BY n.post_time, n.id""",
        (user_id, since),
    )
    return [{"event_id": str(row["id"]), "app": PACKAGES.get(str(row["package_name"]).lower()),
             "date": _local(row["post_time"])} for row in cur.fetchall()]


def owner_movement_memory(cur, user_id: str) -> list[dict]:
    """Owner memory that files a wording as Internal Movement: it repeats one transfer for the next one."""
    cur.execute(
        """SELECT m.match_key, m.source, m.hit_count, m.updated_at, c.kind
           FROM merchant_memory m JOIN categories c ON c.id = m.category_id AND c.user_id = m.user_id
           WHERE m.user_id = %s AND m.source = 'owner' AND c.name = %s
           ORDER BY m.updated_at DESC""",
        (user_id, MOVEMENT_CATEGORY),
    )
    return [{"match_key": row["match_key"], "kind": row["kind"], "hits": row["hit_count"],
             "updated": _local(row["updated_at"])} for row in cur.fetchall()]


def automatic_memory_of_movement_legs(cur, user_id: str) -> list[dict]:
    """Model memory for a wording whose leg is now part of a movement, in a category that is not one."""
    cur.execute(
        """SELECT DISTINCT m.match_key, c.name AS category, m.updated_at
           FROM merchant_memory m
           JOIN categories c ON c.id = m.category_id AND c.user_id = m.user_id
           JOIN notification_events n ON n.user_id = m.user_id AND n.interpretation ->> 'match_key' = m.match_key
           JOIN transactions t ON t.id = n.transaction_id AND t.user_id = n.user_id
           WHERE m.user_id = %s AND m.source = 'ai' AND c.name <> %s AND t.movement_id IS NOT NULL
           ORDER BY m.updated_at DESC""",
        (user_id, MOVEMENT_CATEGORY),
    )
    return [{"match_key": row["match_key"], "category": row["category"], "updated": _local(row["updated_at"])}
            for row in cur.fetchall()]


def detect(cur, user_id: str, since: datetime) -> dict:
    accounts = _accounts(cur, user_id)
    report = {
        "since": since.date().isoformat(),
        "mirror_pairs": mirror_pairs(cur, user_id, since, accounts),
        "recorded_without_transaction": recorded_without_transaction(cur, user_id, since),
        "owner_movement_memory": owner_movement_memory(cur, user_id),
        "automatic_memory_of_movement_legs": automatic_memory_of_movement_legs(cur, user_id),
    }
    report["counts"] = {key: len(value) for key, value in report.items() if isinstance(value, list)}
    return report


def run(user_id: str, since: datetime) -> dict:
    from app.db.pool import DB_POOL, db_conn

    DB_POOL.open(wait=True)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            report = detect(cur, user_id, since)
        conn.rollback()
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only list of arrivals that may be booked twice")
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--since", type=date.fromisoformat, default=date(2026, 1, 1))
    arguments = parser.parse_args(argv)
    since = datetime.combine(arguments.since, time.min, tzinfo=LOCAL_ZONE).astimezone(timezone.utc)
    print(json.dumps(run(arguments.user_id, since), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
