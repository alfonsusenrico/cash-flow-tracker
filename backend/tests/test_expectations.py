"""Non-posting expectations: due plans change nothing, actual payments are linked once.

All names and amounts are synthetic.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest


@pytest.fixture
def plan_owner(client):
    from app.db.pool import db_conn
    from app.main import app
    from app.services.auth import get_current_user

    user_id = str(uuid4())
    ids = {name: str(uuid4()) for name in ("cash", "savings", "bills", "salary")}
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO users (id, username, name, password_hash, invite_code) VALUES (%s, %s, 'Rina Contoh', 'synthetic-hash', 'TEST')",
            (user_id, f"plans_{uuid4().hex}"),
        )
        for key, name, balance in (("cash", "Kas Contoh", 5_000_000), ("savings", "Tabungan Contoh", 0)):
            conn.execute(
                "INSERT INTO accounts (id, user_id, name, type, initial_balance) VALUES (%s, %s, %s, 'bank', %s)",
                (ids[key], user_id, name, balance),
            )
        conn.execute(
            "INSERT INTO categories (id, user_id, name, kind, kakeibo_type) VALUES (%s, %s, 'Tagihan Contoh', 'expense', 'need')",
            (ids["bills"], user_id),
        )
        conn.execute(
            "INSERT INTO categories (id, user_id, name, kind) VALUES (%s, %s, 'Gaji Contoh', 'income')",
            (ids["salary"], user_id),
        )
        conn.commit()
    app.dependency_overrides[get_current_user] = lambda: {
        "id": user_id, "username": "plans-owner", "name": "Rina Contoh", "currency": "IDR", "payday_day": 25,
    }
    yield {"user_id": user_id, **ids}
    app.dependency_overrides.clear()
    with db_conn() as conn:
        conn.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()


def debt(client, remaining=1_000_000, minimum=500_000):
    response = client.post("/api/obligations", json={
        "name": f"Cicilan Contoh {uuid4().hex[:6]}", "total_amount": 2_000_000,
        "remaining_amount": remaining, "minimum_payment": minimum,
    })
    assert response.status_code == 200, response.text
    return response.json()["obligation"]["id"]


def debt_state(client, obligation_id):
    rows = client.get("/api/obligations", params={"include_archived": True}).json()["obligations"]
    item = next(row for row in rows if row["id"] == obligation_id)
    return item["remaining_amount"], item["is_archived"]


def plan(client, owner, *, amount=500_000, obligation_id=None, plan_type="expense", due=None):
    body = {
        "name": f"Rencana Contoh {uuid4().hex[:6]}", "type": plan_type, "amount": amount,
        "source_account_id": owner["cash"], "schedule_type": "monthly_day", "schedule_day": date.today().day,
    }
    if plan_type == "transfer":
        body["target_account_id"] = owner["savings"]
    else:
        body["category_id"] = owner["bills"]
    if obligation_id:
        body["obligation_id"] = obligation_id
    response = client.post("/api/recurring", json=body)
    assert response.status_code == 200, response.text
    rule_id = response.json()["id"]
    if due:
        from app.db.pool import db_conn
        with db_conn() as conn:
            conn.execute("UPDATE recurring_rules SET next_due_date = %s WHERE id = %s", (due, rule_id))
            conn.commit()
    return rule_id


def expense(client, owner, amount, *, when=None):
    body = {"type": "expense", "account_id": owner["cash"], "category_id": owner["bills"], "amount": amount}
    if when:
        body["date"] = when.isoformat()
    response = client.post("/api/transactions", json=body)
    assert response.status_code == 200, response.text
    return response.json()["transaction_id"]


def cash_balance(client, owner):
    accounts = client.get("/api/accounts").json()["accounts"]
    return next(item for item in accounts if item["id"] == owner["cash"])["balance"]


def transaction_count(owner):
    from app.db.pool import db_conn
    with db_conn() as conn:
        return conn.execute("SELECT COUNT(*) AS count FROM transactions WHERE user_id = %s", (owner["user_id"],)).fetchone()["count"]


def pending_rule(client, rule_id):
    return next(rule for rule in client.get("/api/recurring/pending").json()["rules"] if rule["id"] == rule_id)


def test_due_expectation_changes_no_balance_and_lists_candidates_without_linking(client, plan_owner):
    obligation_id = debt(client)
    rule_id = plan(client, plan_owner, obligation_id=obligation_id, due=date.today() - timedelta(days=1))
    before = (cash_balance(client, plan_owner), transaction_count(plan_owner), debt_state(client, obligation_id))
    due = pending_rule(client, rule_id)
    assert due["match_state"] == "none"
    assert (cash_balance(client, plan_owner), transaction_count(plan_owner), debt_state(client, obligation_id)) == before

    first = expense(client, plan_owner, 500_000)
    assert pending_rule(client, rule_id)["match_state"] == "single"
    expense(client, plan_owner, 500_000)
    expense(client, plan_owner, 120_000)
    ambiguous = pending_rule(client, rule_id)
    assert ambiguous["match_state"] == "ambiguous"
    assert [item["exact_amount"] for item in ambiguous["candidates"]][:3] == [True, True, False]
    # Suggestions never link by themselves.
    assert debt_state(client, obligation_id) == (1_000_000, False)
    occurrences = client.get(f"/api/recurring/{rule_id}/occurrences").json()["occurrences"]
    assert occurrences == []
    assert first in {item["id"] for item in ambiguous["candidates"]}


def test_linking_an_actual_payment_updates_the_debt_once(client, plan_owner):
    obligation_id = debt(client, remaining=500_000)
    due = date.today()
    rule_id = plan(client, plan_owner, obligation_id=obligation_id, due=due)
    payment = expense(client, plan_owner, 500_000)
    count = transaction_count(plan_owner)
    body = {"scheduled_for": due.isoformat(), "transaction_id": payment}

    linked = client.post(f"/api/recurring/{rule_id}/occurrences/link", json=body)
    assert linked.status_code == 200, linked.text
    assert linked.json()["debt_changed"] is True
    replay = client.post(f"/api/recurring/{rule_id}/occurrences/link", json=body)
    assert replay.status_code == 200
    assert replay.json()["changed"] is False
    assert debt_state(client, obligation_id) == (0, True)
    assert transaction_count(plan_owner) == count
    occurrence = client.get(f"/api/recurring/{rule_id}/occurrences").json()["occurrences"][0]
    assert (occurrence["status"], occurrence["transaction_id"]) == ("linked", payment)

    # Deleting the payment restores and reactivates the debt; the occurrence can then be relinked.
    assert client.delete(f"/api/transactions/{payment}").status_code == 200
    assert debt_state(client, obligation_id) == (500_000, False)
    replacement = expense(client, plan_owner, 500_000)
    relinked = client.post(
        f"/api/recurring/{rule_id}/occurrences/link",
        json={"scheduled_for": due.isoformat(), "transaction_id": replacement},
    )
    assert relinked.status_code == 200, relinked.text
    assert debt_state(client, obligation_id) == (0, True)


def test_one_transaction_cannot_meet_two_occurrences_or_two_debts(client, plan_owner):
    first_debt, second_debt = debt(client), debt(client)
    first_rule = plan(client, plan_owner, obligation_id=first_debt, due=date.today())
    second_rule = plan(client, plan_owner, obligation_id=second_debt, due=date.today())
    payment = expense(client, plan_owner, 500_000)
    body = {"scheduled_for": date.today().isoformat(), "transaction_id": payment}
    assert client.post(f"/api/recurring/{first_rule}/occurrences/link", json=body).status_code == 200
    second = client.post(f"/api/recurring/{second_rule}/occurrences/link", json=body)
    assert second.status_code == 409
    assert second.json()["detail"]["code"] == "transaction_already_matched"
    direct = client.post(f"/api/obligations/{second_debt}/payments", json={"transaction_id": payment})
    assert direct.status_code == 409
    assert direct.json()["detail"]["code"] == "transaction_already_linked"
    assert debt_state(client, second_debt) == (1_000_000, False)


def test_concurrent_links_apply_the_debt_once(client, plan_owner):
    from app.main import app

    obligation_id = debt(client)
    rule_id = plan(client, plan_owner, obligation_id=obligation_id, due=date.today())
    payment = expense(client, plan_owner, 500_000)
    clients = [TestClient(app, base_url="https://testserver", headers={"Origin": "https://testserver"}) for _ in range(2)]
    body = {"scheduled_for": date.today().isoformat(), "transaction_id": payment}
    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(lambda each: each.post(f"/api/recurring/{rule_id}/occurrences/link", json=body), clients))
    assert sorted(response.status_code for response in responses) == [200, 200]
    assert sorted(response.json()["changed"] for response in responses) == [False, True]
    assert debt_state(client, obligation_id) == (500_000, False)


def test_manual_record_requires_ruling_out_same_amount_captures(client, plan_owner):
    obligation_id = debt(client)
    due = date.today() - timedelta(days=1)
    rule_id = plan(client, plan_owner, obligation_id=obligation_id, due=due)
    captured = expense(client, plan_owner, 500_000)
    url = f"/api/recurring/{rule_id}/occurrences/record"

    blocked = client.post(url, json={"scheduled_for": due.isoformat()})
    assert blocked.status_code == 409
    assert blocked.json()["detail"]["code"] == "possible_duplicate"
    assert [item["id"] for item in blocked.json()["detail"]["candidates"]] == [captured]
    assert debt_state(client, obligation_id) == (1_000_000, False)

    recorded = client.post(url, json={"scheduled_for": due.isoformat(), "acknowledged_candidate_ids": [captured]})
    assert recorded.status_code == 200, recorded.text
    replay = client.post(url, json={"scheduled_for": due.isoformat(), "acknowledged_candidate_ids": [captured]})
    assert replay.json()["result"]["idempotent"] is True
    assert debt_state(client, obligation_id) == (500_000, False)
    assert cash_balance(client, plan_owner) == 5_000_000 - 1_000_000


def test_skip_records_nothing_and_advances_the_plan(client, plan_owner):
    due = date.today()
    rule_id = plan(client, plan_owner, plan_type="transfer", due=due)
    count = transaction_count(plan_owner)
    skipped = client.post(f"/api/recurring/{rule_id}/occurrences/skip", json={"scheduled_for": due.isoformat()})
    assert skipped.status_code == 200, skipped.text
    assert date.fromisoformat(skipped.json()["next_due_date"]) > due
    assert transaction_count(plan_owner) == count
    stale = client.post(f"/api/recurring/{rule_id}/occurrences/skip", json={"scheduled_for": due.isoformat()})
    assert stale.json()["changed"] is False
    later = client.post(
        f"/api/recurring/{rule_id}/occurrences/link",
        json={"scheduled_for": due.isoformat(), "transaction_id": expense(client, plan_owner, 1_000)},
    )
    assert later.status_code == 409
    assert later.json()["detail"]["code"] == "occurrence_already_resolved"


def test_link_rejects_wrong_type_future_and_unscheduled_occurrences(client, plan_owner):
    rule_id = plan(client, plan_owner, due=date.today())
    income = client.post("/api/transactions", json={
        "type": "income", "account_id": plan_owner["cash"], "category_id": plan_owner["salary"], "amount": 500_000,
    }).json()["transaction_id"]
    wrong = client.post(f"/api/recurring/{rule_id}/occurrences/link", json={"scheduled_for": date.today().isoformat(), "transaction_id": income})
    assert wrong.json()["detail"]["code"] == "transaction_type_mismatch"
    other_day = client.post(
        f"/api/recurring/{rule_id}/occurrences/link",
        json={"scheduled_for": (date.today() - timedelta(days=3)).isoformat(), "transaction_id": expense(client, plan_owner, 500_000)},
    )
    assert other_day.json()["detail"]["code"] == "invalid_scheduled_occurrence"
    future = expense(client, plan_owner, 500_000, when=datetime.now(timezone.utc) + timedelta(days=2))
    ahead = client.post(f"/api/recurring/{rule_id}/occurrences/link", json={"scheduled_for": date.today().isoformat(), "transaction_id": future})
    assert ahead.json()["detail"]["code"] == "future_transaction"


def test_plan_with_history_is_deactivated_not_deleted(client, plan_owner):
    rule_id = plan(client, plan_owner, plan_type="transfer", due=date.today())
    assert client.post(f"/api/recurring/{rule_id}/occurrences/skip", json={"scheduled_for": date.today().isoformat()}).status_code == 200
    refused = client.delete(f"/api/recurring/{rule_id}")
    assert refused.status_code == 409
    assert refused.json()["detail"]["code"] == "expectation_has_history"
    assert client.post(f"/api/recurring/{rule_id}/toggle").json()["is_active"] is False
    unused = plan(client, plan_owner, plan_type="transfer")
    assert client.delete(f"/api/recurring/{unused}").status_code == 200


def test_obligation_candidates_and_direct_link(client, plan_owner):
    obligation_id = debt(client, remaining=500_000, minimum=250_000)
    exact = expense(client, plan_owner, 250_000)
    expense(client, plan_owner, 90_000)
    listing = client.get(f"/api/obligations/{obligation_id}/payment-candidates").json()
    assert listing["match_state"] == "single"
    assert listing["candidates"][0]["id"] == exact
    linked = client.post(f"/api/obligations/{obligation_id}/payments", json={"transaction_id": exact})
    assert linked.status_code == 200, linked.text
    assert linked.json()["obligation"]["remaining_amount"] == 250_000
    again = client.post(f"/api/obligations/{obligation_id}/payments", json={"transaction_id": exact})
    assert again.json()["changed"] is False
    assert debt_state(client, obligation_id) == (250_000, False)
    assert exact not in {item["id"] for item in client.get(f"/api/obligations/{obligation_id}/payment-candidates").json()["candidates"]}
    too_large = expense(client, plan_owner, 400_000)
    refused = client.post(f"/api/obligations/{obligation_id}/payments", json={"transaction_id": too_large})
    assert refused.status_code == 409
    assert refused.json()["detail"]["code"] == "debt_allocation_exceeds_remaining"


def test_rule_api_never_reenables_automatic_posting(client, plan_owner):
    from app.db.pool import db_conn
    import psycopg

    rule_id = plan(client, plan_owner, plan_type="transfer")
    refused = client.patch(f"/api/recurring/{rule_id}", json={"auto_post": True})
    assert refused.status_code == 422
    assert refused.json()["detail"]["code"] == "auto_post_retired"
    with db_conn() as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute("UPDATE recurring_rules SET auto_post = TRUE WHERE id = %s", (rule_id,))


def test_v37_converts_rules_keeps_history_and_can_be_retried(db_available, db_url):
    if not db_available:
        pytest.skip("Disposable PostgreSQL required")
    import psycopg
    from psycopg import sql

    schema = f"expectations_upgrade_{uuid4().hex}"
    folder = Path(__file__).resolve().parents[2] / "db/migrations"
    migrations = sorted(folder.glob("V*.sql"), key=lambda path: int(path.name.split("__")[0][1:]))
    before = [path for path in migrations if int(path.name.split("__")[0][1:]) < 37]
    target = next(path for path in migrations if path.name.startswith("V37__"))
    with psycopg.connect(db_url) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            conn.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
            for path in before:
                conn.execute(path.read_text())
            owner = conn.execute("INSERT INTO users (username, password_hash, invite_code) VALUES ('Migrasi contoh', 'unused', 'fixture') RETURNING id").fetchone()[0]
            cash = conn.execute("INSERT INTO accounts (user_id, name, type, initial_balance) VALUES (%s, 'Kas', 'bank', 1000) RETURNING id", (owner,)).fetchone()[0]
            pocket = conn.execute("INSERT INTO accounts (user_id, name, type, initial_balance) VALUES (%s, 'Kantong', 'bank', 0) RETURNING id", (owner,)).fetchone()[0]
            automatic = conn.execute(
                "INSERT INTO recurring_rules (user_id, name, type, amount, source_account_id, target_account_id, auto_post, is_payroll_allocation, next_due_date) "
                "VALUES (%s, 'Otomatis', 'transfer', 100, %s, %s, TRUE, TRUE, CURRENT_DATE) RETURNING id",
                (owner, cash, pocket),
            ).fetchone()[0]
            manual = conn.execute(
                "INSERT INTO recurring_rules (user_id, name, type, amount, source_account_id, auto_post, next_due_date) "
                "VALUES (%s, 'Manual', 'expense', 50, %s, FALSE, CURRENT_DATE) RETURNING id",
                (owner, cash),
            ).fetchone()[0]
            posted = conn.execute(
                "INSERT INTO transactions (user_id, account_id, type, amount, recurring_rule_id, source) VALUES (%s, %s, 'expense', 50, %s, 'recurring') RETURNING id",
                (owner, cash, manual),
            ).fetchone()[0]
            conn.execute(
                "INSERT INTO recurring_executions (recurring_rule_id, user_id, scheduled_for, status, transaction_id) VALUES (%s, %s, CURRENT_DATE - 30, 'succeeded', %s)",
                (manual, owner, posted),
            )
            history = conn.execute("SELECT * FROM recurring_executions ORDER BY id").fetchall()
            ledger = conn.execute("SELECT * FROM transactions ORDER BY id").fetchall()

            # An interrupted run leaves nothing behind because Flyway applies the file in one transaction.
            conn.commit()
            with pytest.raises(psycopg.errors.QueryCanceled):
                with conn.transaction():
                    conn.execute(target.read_text())
                    conn.execute("SET LOCAL statement_timeout = 1")
                    conn.execute("SELECT pg_sleep(1)")
            assert conn.execute("SELECT auto_post FROM recurring_rules WHERE id = %s", (automatic,)).fetchone()[0] is True

            conn.execute(target.read_text())
            converted_at = conn.execute("SELECT converted_to_expectation_at FROM recurring_rules WHERE id = %s", (automatic,)).fetchone()[0]
            conn.execute(target.read_text())
            rows = {
                row[0]: row[1:]
                for row in conn.execute(
                    "SELECT id, auto_post, legacy_auto_post, is_payroll_allocation, converted_to_expectation_at FROM recurring_rules"
                ).fetchall()
            }
            assert rows[automatic] == (False, True, True, converted_at)
            assert rows[manual][:3] == (False, False, False)
            assert conn.execute("SELECT * FROM recurring_executions ORDER BY id").fetchall() == history
            assert conn.execute("SELECT * FROM transactions ORDER BY id").fetchall() == ledger
            with pytest.raises(psycopg.errors.CheckViolation):
                with conn.transaction():
                    conn.execute("UPDATE recurring_rules SET auto_post = TRUE WHERE id = %s", (manual,))
        finally:
            conn.rollback()
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
            conn.commit()
