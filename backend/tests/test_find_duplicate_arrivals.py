"""The read-only detector of past arrivals booked twice (recognise-one-arrival-seen-twice D7).

All texts, names and amounts are synthetic.
"""
import importlib.util
import json
from datetime import timedelta
from pathlib import Path

import psycopg
import pytest

from test_duplicate_arrivals import (
    GOPAY, GOPAY_CREDIT, JAGO, JAGO_CREDIT, START, ai_memory, incomes, send,
)
from test_notification_processing_database import notification_owner  # noqa: F401

ROOT = Path(__file__).resolve().parents[2]


def detector():
    spec = importlib.util.spec_from_file_location("find_duplicate_arrivals", ROOT / "scripts" / "verify" / "find_duplicate_arrivals.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def plant_cases(client, owner, monkeypatch):
    """A mirrored pair recorded before the rule, a deleted leg, and memories that repeat a transfer."""
    from app.db.pool import db_conn
    from app.services import notification_application
    from app.services.movement_pairing import ArrivalDecision

    with monkeypatch.context() as patched:
        patched.setattr(notification_application, "same_arrival", lambda *args, **kwargs: ArrivalDecision())
        send(client, GOPAY_CREDIT, GOPAY, 0)
        send(client, JAGO_CREDIT, JAGO, 0.13)
    deleted = send(client, "Anda telah menerima saldo sebesar Rp75.000.", GOPAY, 600)
    with db_conn() as conn:
        conn.execute("DELETE FROM transactions WHERE id = %s", (deleted["transaction_id"],))
        conn.execute(
            """INSERT INTO merchant_memory (user_id, match_key, key_kind, category_id, source)
               VALUES (%s, 'signature:jago|income|unknown', 'signature', %s, 'owner')""",
            (owner["user_id"], owner["internal_in"]),
        )
        conn.commit()
    return deleted


def test_detector_lists_the_planted_cases_without_text(client, notification_owner, monkeypatch, capsys):
    owner = notification_owner
    deleted = plant_cases(client, owner, monkeypatch)
    module = detector()
    assert module.main(["--user-id", owner["user_id"], "--since", "2026-10-01"]) == 0
    printed = capsys.readouterr().out
    report = json.loads(printed)
    assert report["counts"] == {"mirror_pairs": 1, "recorded_without_transaction": 1, "owner_movement_memory": 1,
                                "automatic_memory_of_movement_legs": 0}
    [pair] = report["mirror_pairs"]
    assert (pair["type"], pair["amount"], pair["seconds_apart"]) == ("income", 500000, 0.13)
    assert [leg["account"] for leg in pair["legs"]] == ["GoPay", "Bank Jago · Kantong Utama"]
    assert report["recorded_without_transaction"][0]["event_id"] == deleted["event_id"]
    assert report["owner_movement_memory"][0]["match_key"] == "signature:jago|income|unknown"
    for text in (GOPAY_CREDIT, JAGO_CREDIT, "menerima", "received", "Tanya Jago"):
        assert text not in printed
    # Nothing was changed: both legs are still there.
    assert len(incomes(owner)) == 2


def test_detector_lists_model_memory_of_a_linked_leg(client, notification_owner):
    from test_duplicate_arrivals import MYBCA, MYBCA_DEBIT

    owner = notification_owner
    send(client, GOPAY_CREDIT, GOPAY, 0)
    send(client, MYBCA_DEBIT, MYBCA, 3.4)
    # Learned before this change retired it on link.
    ai_memory(owner, "merchant:tabungan by jago", "income")
    report = detector().run(owner["user_id"], START - timedelta(days=1))
    assert report["automatic_memory_of_movement_legs"] == [
        {"match_key": "merchant:tabungan by jago", "category": "Transfer Masuk",
         "updated": report["automatic_memory_of_movement_legs"][0]["updated"]},
    ]


def test_detector_cannot_write(notification_owner, monkeypatch):
    module = detector()

    def write(cur, user_id, since):
        cur.execute("UPDATE accounts SET name = name WHERE user_id = %s", (user_id,))

    monkeypatch.setattr(module, "detect", write)
    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        module.run(notification_owner["user_id"], START)
