"""Manual balance check, "Cocokkan saldo" (manual-balance-check).

All names, texts and amounts are synthetic. The owner has a standalone account "Rekening Contoh", an
e-wallet "Dompet Contoh", a parent "Bank Contoh" with two pockets, and one investment product.
"""
import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from category_keys import seeded_key

NOW = datetime.now(timezone.utc)


def _db():
    from app.db.pool import db_conn
    return db_conn()


@pytest.fixture
def owner(client):
    from app.main import app
    from app.services.auth import get_current_user

    user_id = str(uuid4())
    ids = {key: str(uuid4()) for key in (
        "standalone", "wallet", "parent", "main", "saving", "product", "movement_out", "movement_in", "shop", "other_in",
    )}
    with _db() as conn:
        conn.execute(
            "INSERT INTO users (id, username, name, password_hash, invite_code) VALUES (%s, %s, 'Sari Contoh', 'synthetic-hash', 'TEST')",
            (user_id, f"balance_{uuid4().hex}"),
        )
        for key, name, kind, balance, parent in [
            ("standalone", "Rekening Contoh", "bank", 1_000_000, None),
            ("wallet", "Dompet Contoh", "ewallet", 0, None),
            ("parent", "Bank Contoh", "bank", 100_000, None),
            ("main", "Kantong Utama", "bank", 400_000, "parent"),
            ("saving", "Kantong Tabungan", "bank", 500_000, "parent"),
        ]:
            conn.execute(
                "INSERT INTO accounts (id, user_id, name, type, initial_balance, parent_id) VALUES (%s, %s, %s, %s, %s, %s)",
                (ids[key], user_id, name, kind, balance, ids[parent] if parent else None),
            )
        conn.execute(
            """INSERT INTO accounts (id, user_id, name, type, initial_balance, instrument_type)
               VALUES (%s, %s, 'Reksa Dana Contoh', 'investment', 0, 'mutual_fund')""",
            (ids["product"], user_id),
        )
        for key, name, kind in [
            ("movement_out", "Internal Movement", "expense"), ("movement_in", "Internal Movement", "income"),
            ("shop", "Belanja Contoh", "expense"), ("other_in", "Pendapatan Lain", "income"),
        ]:
            conn.execute(
                "INSERT INTO categories (id, user_id, name, kind, system_key) VALUES (%s, %s, %s, %s, %s)",
                (ids[key], user_id, name, kind, seeded_key(name)),
            )
        conn.commit()
    app.dependency_overrides[get_current_user] = lambda: {
        "id": user_id, "username": "balance-owner", "name": "Sari Contoh", "currency": "IDR", "payday_day": 25,
    }
    yield {"user_id": user_id, **ids}
    app.dependency_overrides.clear()
    with _db() as conn:
        conn.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()


def add_row(owner, account, amount, *, tx_type="expense", category="shop", when=None, source="manual", notes="Contoh"):
    with _db() as conn:
        tx_id = conn.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, date, source)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
            (owner["user_id"], owner[account], owner[category] if category else None, tx_type, amount, notes,
             when or NOW - timedelta(hours=1), source),
        ).fetchone()["id"]
        conn.commit()
    return str(tx_id)


def add_report(owner, package, text, *, when, transaction_id=None, account=None, direction=None, **fields):
    """A notification report; `fields` set the processing columns of the case under test."""
    interpretation = {"account_id": owner[account], "direction": direction} if account else None
    columns = {
        "user_id": owner["user_id"], "device_id": "device-contoh", "package_name": package, "title": "Contoh",
        "body_text": text, "post_time": when, "payload_hash": uuid4().hex, "transaction_id": transaction_id,
        "interpretation": json.dumps(interpretation) if interpretation else None, "is_financial": True, **fields,
    }
    names = ", ".join(columns)
    marks = ", ".join(["%s"] * len(columns))
    with _db() as conn:
        event_id = conn.execute(
            f"INSERT INTO notification_events ({names}) VALUES ({marks}) RETURNING id", tuple(columns.values()),
        ).fetchone()["id"]
        conn.commit()
    return str(event_id)


def check(client, owner, account, actual):
    response = client.post(f"/api/accounts/{owner[account]}/balance-check", json={"actual_balance": actual})
    assert response.status_code == 200, response.text
    return response.json()


def keys(result):
    return [candidate["key"] for candidate in result["candidates"]]


def test_exact_match_writes_nothing(client, owner):
    add_row(owner, "main", 150_000)
    with _db() as conn:
        before = conn.execute("SELECT COUNT(*) AS n FROM transactions WHERE user_id = %s", (owner["user_id"],)).fetchone()["n"]

    result = check(client, owner, "main", 250_000)

    assert (result["recorded_balance"], result["difference"], result["explanation"]) == (250_000, 0, None)
    assert result["since"]["basis"] == "window"
    assert result["can_adjust"] is True
    with _db() as conn:
        after = conn.execute("SELECT COUNT(*) AS n FROM transactions WHERE user_id = %s", (owner["user_id"],)).fetchone()["n"]
        matches = conn.execute("SELECT COUNT(*) AS n FROM account_balance_matches WHERE user_id = %s", (owner["user_id"],)).fetchone()["n"]
    assert (after, matches) == (before, 0)


def test_difference_explained_by_a_double_booking_across_apps(client, owner):
    when = NOW - timedelta(minutes=30)
    kept = add_row(owner, "wallet", 500_000, tx_type="income", category="movement_in", when=when, source="notification")
    add_report(owner, "com.gojek.gopay", "Anda telah menerima Rp500.000.", when=when, transaction_id=kept,
               processing_state="recorded", result_key="k1", result_snapshot=json.dumps({}))
    extra = add_row(owner, "main", 500_000, tx_type="income", category="movement_in",
                    when=when + timedelta(seconds=2), source="notification")
    add_report(owner, "com.jago.digitalBanking", "You've received Rp500.000 from GoPay.",
               when=when + timedelta(seconds=2), transaction_id=extra,
               processing_state="recorded", result_key="k2", result_snapshot=json.dumps({}))

    result = check(client, owner, "main", 400_000)

    assert result["difference"] == -500_000
    assert result["explanation"] == [f"tx:{extra}"]
    cause = result["candidates"][0]
    assert (cause["kind"], cause["effect"], cause["app"], cause["link"]) == (
        "duplicate", -500_000, "Bank Jago", {"kind": "transaction", "id": extra},
    )
    assert cause["twin"]["account"] == "Dompet Contoh" and cause["twin"]["app"] == "GoPay"


def test_difference_explained_by_an_unpaired_leg(client, owner):
    leg = add_row(owner, "standalone", 75_000, category="movement_out", notes="Pindah saldo")

    result = check(client, owner, "standalone", 1_000_000)

    assert result["difference"] == 75_000
    assert result["explanation"] == [f"tx:{leg}"]
    assert result["candidates"][0]["kind"] == "unpaired"


def test_held_and_skipped_reports_explain_a_missing_amount(client, owner):
    held = add_report(owner, "com.jago.digitalBanking", "You've received Rp120.000 from GoPay.",
                      when=NOW - timedelta(hours=2), account="main", direction="income",
                      processing_state="recorded", provenance_kind="duplicate_observation",
                      result_key="h1", result_snapshot=json.dumps({}))

    result = check(client, owner, "main", 520_000)

    assert result["difference"] == 120_000
    assert result["explanation"] == [f"ev:{held}"]
    cause = result["candidates"][0]
    assert (cause["kind"], cause["effect"], cause["link"]) == ("held", 120_000, {"kind": "review", "folder": "dua"})


def test_a_report_skipped_by_a_linked_account_counts_as_skipped(client, owner):
    with _db() as conn:
        link_id = conn.execute(
            "INSERT INTO account_report_links (user_id, account_id, institution, source) VALUES (%s, %s, 'jago', 'confirmation') RETURNING id",
            (owner["user_id"], owner["main"]),
        ).fetchone()["id"]
        conn.commit()
    skipped = add_report(owner, "com.jago.digitalBanking", "You've received Rp120.000 from GoPay.",
                         when=NOW - timedelta(hours=2), account="main", direction="income",
                         processing_state="recorded", provenance_kind="duplicate_observation",
                         result_key="s1", result_snapshot=json.dumps({}), reviewed_at=NOW - timedelta(hours=2),
                         reporting_link_id=str(link_id))

    result = check(client, owner, "main", 520_000)

    assert result["explanation"] == [f"ev:{skipped}"]
    cause = result["candidates"][0]
    assert (cause["kind"], cause["effect"], cause["link"]) == ("skipped", 120_000, {"kind": "review", "folder": "lewat"})


def test_unexplained_difference_lists_causes_without_an_explanation(client, owner):
    add_row(owner, "standalone", 75_000, category="movement_out")
    add_report(owner, "com.bca.mybca.omni.android", "You spent IDR 30,000.00 at Payment.",
               when=NOW - timedelta(minutes=5), processing_state="queued")

    result = check(client, owner, "standalone", 900_000)

    assert result["difference"] == -25_000
    assert result["explanation"] is None
    assert [candidate["kind"] for candidate in result["candidates"]] == ["unpaired"]
    assert [(item["kind"], item["amount"]) for item in result["unattributed"]] == [("pending", 30_000)]


def test_saldo_cocok_starts_the_next_window(client, owner):
    old = add_row(owner, "standalone", 75_000, category="movement_out", when=NOW - timedelta(days=2))
    refused = client.post(f"/api/accounts/{owner['standalone']}/balance-matches", json={"actual_balance": 1_000_000})
    assert refused.status_code == 409
    assert refused.json()["detail"] == {"code": "balance_mismatch", "difference": 75_000}

    matched = client.post(f"/api/accounts/{owner['standalone']}/balance-matches", json={"actual_balance": 925_000})
    assert matched.status_code == 200, matched.text
    recent = add_row(owner, "standalone", 40_000, category="movement_out", when=NOW + timedelta(minutes=1))

    result = check(client, owner, "standalone", 885_000)

    assert result["since"]["basis"] == "matched"
    assert f"tx:{old}" not in keys(result)
    assert keys(result) == [f"tx:{recent}"]


def test_reconcile_is_unchanged_and_also_starts_the_window(client, owner):
    add_row(owner, "standalone", 20_000, category="movement_out", when=NOW - timedelta(days=1))
    response = client.post(f"/api/accounts/{owner['standalone']}/reconcile", json={"actual_balance": 1_000_000})
    assert response.status_code == 200, response.text
    assert (response.json()["ok"], response.json()["diff"]) == (True, 20_000)
    assert response.json()["message"] == "Account reconciled with income adjustment of 20000"

    result = check(client, owner, "standalone", 1_000_000)

    assert (result["difference"], result["since"]["basis"], result["candidates"]) == (0, "adjusted", [])


def test_parent_is_checked_as_its_group_without_double_counting(client, owner):
    add_row(owner, "main", 50_000)
    add_row(owner, "saving", 10_000, tx_type="income", category="other_in")

    result = check(client, owner, "parent", 960_000)

    assert (result["recorded_balance"], result["difference"]) == (960_000, 0)
    assert result["account"]["is_parent"] is True
    assert result["can_adjust"] is False
    pocket = check(client, owner, "main", 350_000)
    assert (pocket["recorded_balance"], pocket["can_adjust"]) == (350_000, True)


def test_products_and_other_owners_are_refused(client, owner):
    product = client.post(f"/api/accounts/{owner['product']}/balance-check", json={"actual_balance": 0})
    assert product.status_code == 409
    assert product.json()["detail"] == {"code": "balance_check_not_liquid"}

    stranger = client.post(f"/api/accounts/{uuid4()}/balance-check", json={"actual_balance": 0})
    assert stranger.status_code == 404
    stranger_match = client.post(f"/api/accounts/{uuid4()}/balance-matches", json={"actual_balance": 0})
    assert stranger_match.status_code == 404

    negative = client.post(f"/api/accounts/{owner['main']}/balance-check", json={"actual_balance": -1})
    assert negative.status_code == 422


def test_another_owners_account_answers_404(client, owner):
    from app.main import app
    from app.services.auth import get_current_user

    app.dependency_overrides[get_current_user] = lambda: {
        "id": str(uuid4()), "username": "someone-else", "name": "Lain", "currency": "IDR", "payday_day": 25,
    }
    response = client.post(f"/api/accounts/{owner['main']}/balance-check", json={"actual_balance": 0})
    assert response.status_code == 404
