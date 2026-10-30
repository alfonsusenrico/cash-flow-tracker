"""Automatic linking of a due plan to its one certain recorded payment, and the owner's undo.

All names and amounts are synthetic.
"""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from test_expectations import debt, debt_state, plan, plan_owner  # noqa: F401 - fixture

JAKARTA = ZoneInfo("Asia/Jakarta")


def run_auto_link(owner):
    from app.db.pool import db_conn
    from app.services.plan_occurrences import link_due_plans_for_user

    with db_conn() as conn:
        return link_due_plans_for_user(conn, owner["user_id"])


def record(client, owner, amount, *, day=None, account="cash", tx_type="expense"):
    when = datetime.combine(day or date.today(), time(9), JAKARTA)
    if when > datetime.now(JAKARTA):
        when = datetime.now(JAKARTA) - timedelta(minutes=1)
    category = owner["bills"] if tx_type == "expense" else owner["salary"]
    response = client.post("/api/transactions", json={
        "type": tx_type, "account_id": owner[account], "category_id": category, "amount": amount, "date": when.isoformat(),
    })
    assert response.status_code == 200, response.text
    return response.json()["transaction_id"]


def occurrences(client, rule_id):
    return client.get(f"/api/recurring/{rule_id}/occurrences").json()["occurrences"]


def link_events(rule_id):
    from app.db.pool import db_conn

    with db_conn() as conn:
        return [(row["action"], row["method"]) for row in conn.execute(
            "SELECT action, method FROM recurring_link_events WHERE recurring_rule_id = %s ORDER BY created_at, id", (rule_id,),
        ).fetchall()]


def pending_ids(client):
    return {rule["id"] for rule in client.get("/api/recurring/pending").json()["rules"]}


def test_one_certain_payment_is_linked_once_and_applies_the_debt_once(client, plan_owner):
    obligation_id = debt(client, remaining=1_000_000)
    due = date.today() - timedelta(days=2)
    rule_id = plan(client, plan_owner, obligation_id=obligation_id, due=due)
    payment = record(client, plan_owner, 500_000, day=due + timedelta(days=1))

    assert run_auto_link(plan_owner) == {"due": 1, "linked": 1}
    assert run_auto_link(plan_owner)["linked"] == 0
    [occurrence] = occurrences(client, rule_id)
    assert (occurrence["status"], occurrence["transaction_id"], occurrence["link_method"]) == ("linked", payment, "auto")
    assert debt_state(client, obligation_id) == (500_000, False)
    assert rule_id not in pending_ids(client)
    assert link_events(rule_id) == [("link", "auto")]
    recent = client.get("/api/recurring/pending").json()["recent_auto_links"]
    assert [(item["rule_id"], item["transaction_id"]) for item in recent] == [(rule_id, payment)]


def test_concurrent_passes_link_once(client, plan_owner):
    obligation_id = debt(client, remaining=2_000_000)
    rule_id = plan(client, plan_owner, obligation_id=obligation_id, due=date.today() - timedelta(days=1))
    record(client, plan_owner, 500_000)

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda _: run_auto_link(plan_owner)["linked"], range(4)))
    assert sum(results) == 1
    assert debt_state(client, obligation_id) == (1_500_000, False)
    assert [item["status"] for item in occurrences(client, rule_id)] == ["linked"]


def test_two_candidates_stay_manual_with_their_candidates(client, plan_owner):
    rule_id = plan(client, plan_owner, due=date.today() - timedelta(days=1))
    record(client, plan_owner, 500_000)
    record(client, plan_owner, 500_000, day=date.today() - timedelta(days=2))

    assert run_auto_link(plan_owner)["linked"] == 0
    due = next(rule for rule in client.get("/api/recurring/pending").json()["rules"] if rule["id"] == rule_id)
    assert due["match_state"] == "ambiguous"
    assert occurrences(client, rule_id) == []


def test_other_account_amount_or_window_never_links(client, plan_owner):
    due = date.today() - timedelta(days=10)
    rule_id = plan(client, plan_owner, due=due)
    record(client, plan_owner, 1_000_000, day=due - timedelta(days=20), account="savings", tx_type="income")
    record(client, plan_owner, 500_000, day=due, account="savings")
    record(client, plan_owner, 499_000, day=due)
    record(client, plan_owner, 500_000, day=due - timedelta(days=4))
    record(client, plan_owner, 500_000, day=due + timedelta(days=8))

    assert run_auto_link(plan_owner)["linked"] == 0
    assert occurrences(client, rule_id) == []
    # The window edges themselves qualify: three days before is in, alone it links.
    edge = record(client, plan_owner, 500_000, day=due - timedelta(days=3))
    assert run_auto_link(plan_owner)["linked"] == 1
    assert occurrences(client, rule_id)[0]["transaction_id"] == edge


def test_income_plans_link_and_transfers_never_do(client, plan_owner):
    from app.db.pool import db_conn

    created = client.post("/api/recurring", json={
        "name": "Sewa Kamar Contoh", "type": "income", "amount": 500_000, "source_account_id": plan_owner["cash"],
        "category_id": plan_owner["salary"], "schedule_type": "monthly_day", "schedule_day": date.today().day,
    })
    assert created.status_code == 200, created.text
    income_rule = created.json()["id"]
    with db_conn() as conn:
        conn.execute("UPDATE recurring_rules SET next_due_date = %s WHERE id = %s", (date.today(), income_rule))
        conn.commit()
    transfer_rule = plan(client, plan_owner, plan_type="transfer", due=date.today())
    record(client, plan_owner, 500_000, tx_type="income")
    assert run_auto_link(plan_owner) == {"due": 1, "linked": 1}
    assert occurrences(client, income_rule)[0]["link_method"] == "auto"
    assert occurrences(client, transfer_rule) == []


def test_a_payment_two_plans_could_claim_is_not_guessed(client, plan_owner):
    first = plan(client, plan_owner, due=date.today() - timedelta(days=1))
    second = plan(client, plan_owner, due=date.today())
    record(client, plan_owner, 500_000)
    assert run_auto_link(plan_owner)["linked"] == 0
    assert occurrences(client, first) == [] and occurrences(client, second) == []


def test_undo_reverses_the_debt_once_and_is_never_relinked(client, plan_owner):
    obligation_id = debt(client, remaining=500_000)
    due = date.today() - timedelta(days=1)
    rule_id = plan(client, plan_owner, obligation_id=obligation_id, due=due)
    payment = record(client, plan_owner, 500_000)
    run_auto_link(plan_owner)
    assert debt_state(client, obligation_id) == (0, True)

    body = {"scheduled_for": due.isoformat()}
    undone = client.post(f"/api/recurring/{rule_id}/occurrences/unlink", json=body)
    assert undone.status_code == 200, undone.text
    assert undone.json()["changed"] is True and undone.json()["debt_changed"] is True
    assert undone.json()["next_due_date"] == due.isoformat()
    again = client.post(f"/api/recurring/{rule_id}/occurrences/unlink", json=body).json()
    assert again["changed"] is False
    assert debt_state(client, obligation_id) == (500_000, False)
    assert rule_id in pending_ids(client)

    assert run_auto_link(plan_owner)["linked"] == 0
    assert occurrences(client, rule_id) == []
    assert link_events(rule_id) == [("link", "auto"), ("unlink", "owner")]
    # The owner may still link the same payment deliberately.
    owner_link = client.post(f"/api/recurring/{rule_id}/occurrences/link", json={**body, "transaction_id": payment})
    assert owner_link.status_code == 200, owner_link.text
    assert occurrences(client, rule_id)[0]["link_method"] == "owner"
    assert debt_state(client, obligation_id) == (0, True)


def test_undo_does_not_reverse_debt_the_link_did_not_apply(client, plan_owner):
    obligation_id = debt(client, remaining=1_000_000)
    due = date.today() - timedelta(days=1)
    rule_id = plan(client, plan_owner, obligation_id=obligation_id, due=due)
    response = client.post("/api/transactions", json={
        "type": "expense", "account_id": plan_owner["cash"], "category_id": plan_owner["bills"],
        "amount": 500_000, "obligation_id": obligation_id,
    })
    assert response.status_code == 200, response.text
    assert debt_state(client, obligation_id) == (500_000, False)
    assert run_auto_link(plan_owner)["linked"] == 1
    assert debt_state(client, obligation_id) == (500_000, False)
    undone = client.post(f"/api/recurring/{rule_id}/occurrences/unlink", json={"scheduled_for": due.isoformat()}).json()
    assert undone["debt_changed"] is False
    assert debt_state(client, obligation_id) == (500_000, False)


def test_resolving_an_undone_occurrence_skips_later_resolved_ones(client, plan_owner):
    due = date.today() - timedelta(days=1)
    rule_id = plan(client, plan_owner, due=due)
    record(client, plan_owner, 500_000)
    run_auto_link(plan_owner)
    next_due = client.get("/api/recurring").json()["rules"]
    later = next(rule for rule in next_due if rule["id"] == rule_id)["next_due_date"]
    assert client.post(f"/api/recurring/{rule_id}/occurrences/skip", json={"scheduled_for": later}).status_code == 200
    undone = client.post(f"/api/recurring/{rule_id}/occurrences/unlink", json={"scheduled_for": due.isoformat()}).json()
    assert undone["next_due_date"] == due.isoformat()
    skipped = client.post(f"/api/recurring/{rule_id}/occurrences/skip", json={"scheduled_for": due.isoformat()}).json()
    # The occurrence after `due` was already skipped, so the plan moves past it.
    assert skipped["next_due_date"] > later


def test_recording_requests_a_link_pass_and_the_request_links(client, plan_owner, monkeypatch):
    import app.services.plan_occurrences as plans

    monkeypatch.setattr(plans, "settings", replace(plans.settings, plan_auto_link_enabled=True))
    plans.take_plan_link_requests()
    rule_id = plan(client, plan_owner, due=date.today())
    assert plans.take_plan_link_requests() == [plan_owner["user_id"]]
    record(client, plan_owner, 500_000)
    assert plan_owner["user_id"] in plans._requests
    assert plans.link_requested_plans() == 1
    assert occurrences(client, rule_id)[0]["link_method"] == "auto"


def test_disabled_auto_linking_ignores_requests(client, plan_owner, monkeypatch):
    import app.services.plan_occurrences as plans

    monkeypatch.setattr(plans, "settings", replace(plans.settings, plan_auto_link_enabled=False))
    plans.take_plan_link_requests()
    record(client, plan_owner, 500_000)
    assert plans.take_plan_link_requests() == []


def test_v38_marks_existing_links_as_owner_links_and_can_be_retried(db_available, db_url):
    import pathlib

    import psycopg
    import pytest

    if not db_available:
        pytest.skip("PostgreSQL is not reachable")
    sql = (pathlib.Path(__file__).resolve().parents[2] / "db" / "migrations" / "V38__plan_link_method.sql").read_text()
    with psycopg.connect(db_url) as conn:
        conn.execute(sql)
        conn.execute(sql)
        unmarked = conn.execute(
            "SELECT COUNT(*) FROM recurring_executions WHERE status = 'linked' AND link_method IS NULL"
        ).fetchone()[0]
        conn.rollback()
    assert unmarked == 0
