from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest


def account(client, *, balance=0, instrument=None, units=None, parent=None):
    payload = {"name": f"Topup fixture {uuid4().hex[:10]}", "type": "investment" if instrument else "bank", "initial_balance": balance}
    if instrument:
        payload.update(instrument_type=instrument, units=units)
    if parent:
        payload["parent_id"] = parent
    response = client.post("/api/accounts", json=payload)
    assert response.status_code == 200, response.text
    return response.json()["account"]["id"]


def balances(client):
    response = client.get("/api/accounts")
    assert response.status_code == 200
    return {item["id"]: item for item in response.json()["accounts"]}


def request(source, target, **overrides):
    return {
        "source_account_id": source, "target_account_id": target,
        "amount": 112_590, "date": datetime.now(timezone.utc).isoformat(),
        "notes": "Monthly fund contribution", "idempotency_key": str(uuid4()), **overrides,
    }


@pytest.fixture
def portfolio(auth_client):
    source = account(auth_client, balance=500_000)
    parent = account(auth_client)
    target = account(auth_client, instrument="mutual_fund", balance=1_000_000, parent=parent)
    assert auth_client.patch(f"/api/accounts/{target}", json={"last_price": 1_050_000}).status_code == 200
    return source, target, parent


def record(client, payload):
    response = client.post("/api/investment-topups", json=payload)
    assert response.status_code == 200, response.text
    return response.json()["topup"]


def test_topup_preserves_capital_gains_ledger_and_aggregate_net_worth(auth_client, portfolio):
    source, target, parent = portfolio
    before = auth_client.get("/api/dashboard/net-worth").json()["net_worth"]
    topup = record(auth_client, request(source, target))
    after = balances(auth_client)
    assert after[source]["balance"] == 387_410
    assert after[target]["balance"] == 1_162_590
    assert after[target]["cost_basis"] == 1_112_590
    assert after[target]["capital_gain"] == 50_000
    assert after[target]["initial_balance"] == 1_000_000
    assert after[target]["units"] is None
    assert after[target]["investment_value_estimated"] is True
    assert after[parent]["balance"] == 1_162_590
    assert auth_client.get("/api/dashboard/net-worth").json()["net_worth"] == before
    listing = auth_client.get("/api/transactions", params={"account_id": source}).json()["transactions"]
    leg = next(item for item in listing if item["movement_id"] == topup["id"])
    assert leg["movement_kind"] == "investment_topup"
    assert leg["investment_topup_id"] == topup["id"]


def test_valuation_correction_and_deletion_preserve_gain(auth_client, portfolio):
    source, target, _ = portfolio
    payload = request(source, target)
    topup = record(auth_client, payload)
    response = auth_client.post(f"/api/accounts/{target}/valuation", json={"current_balance": 1_170_000})
    assert response.status_code == 200, response.text
    assert response.json()["account"]["capital_gain"] == 57_410
    assert response.json()["account"]["investment_value_estimated"] is False
    response = auth_client.patch(f"/api/investment-topups/{topup['id']}", json={"amount": 100_000, "notes": "Corrected"})
    assert response.status_code == 200
    updated = balances(auth_client)
    assert updated[source]["balance"] == 400_000
    assert updated[target]["balance"] == 1_157_410
    assert updated[target]["cost_basis"] == 1_100_000
    assert updated[target]["capital_gain"] == 57_410
    assert auth_client.delete(f"/api/investment-topups/{topup['id']}").status_code == 200
    updated = balances(auth_client)
    assert updated[source]["balance"] == 500_000
    assert updated[target]["balance"] == 1_057_410
    assert updated[target]["cost_basis"] == 1_000_000
    assert record(auth_client, payload)["is_deleted"] is True
    assert balances(auth_client)[source]["balance"] == 500_000


def test_topup_counts_once_as_allocation_and_not_living_cashflow(auth_client, portfolio):
    from app.db.pool import db_conn
    from app.routers.dashboard import get_kakeibo_breakdown

    source, target, _ = portfolio
    owner = auth_client.get("/api/auth/me").json()["user"]["id"]
    overview_before = auth_client.get("/api/dashboard/overview").json()["kpis"]
    start = datetime.now(timezone.utc) - timedelta(seconds=1)
    with db_conn() as conn:
        before = get_kakeibo_breakdown(conn.cursor(), owner, start, start + timedelta(minutes=1), 1_000_000)
    record(auth_client, request(source, target))
    overview_after = auth_client.get("/api/dashboard/overview").json()["kpis"]
    assert overview_after["total_inflow"] == overview_before["total_inflow"]
    assert overview_after["total_outflow"] == overview_before["total_outflow"]
    with db_conn() as conn:
        after = get_kakeibo_breakdown(conn.cursor(), owner, start, start + timedelta(minutes=1), 1_000_000)
    assert after["saving_spent"] - before["saving_spent"] == 112_590


@pytest.mark.parametrize("case", ["units", "zero_units", "parent", "stock", "archived", "foreign", "funding", "insufficient"])
def test_ineligible_topup_is_atomic(auth_client, portfolio, case):
    source, target, parent = portfolio
    if case == "units":
        target = account(auth_client, instrument="mutual_fund", units=2)
    elif case == "zero_units":
        target = account(auth_client, instrument="mutual_fund", units=0)
    elif case == "parent":
        target = parent
    elif case == "stock":
        target = account(auth_client, instrument="stock")
    elif case == "archived":
        auth_client.patch(f"/api/accounts/{target}", json={"is_archived": True})
    elif case == "foreign":
        from app.db.pool import db_conn

        with db_conn() as conn:
            owner = conn.execute("INSERT INTO users (username, password_hash, invite_code) VALUES (%s, 'unused', 'fixture') RETURNING id", (f"Other fixture {uuid4().hex}",)).fetchone()["id"]
            target = str(conn.execute("INSERT INTO accounts (user_id, name, type, instrument_type) VALUES (%s, 'Foreign fund', 'investment', 'mutual_fund') RETURNING id", (owner,)).fetchone()["id"])
    elif case == "funding":
        source = account(auth_client, instrument="mutual_fund")
    elif case == "insufficient":
        source = account(auth_client, balance=100_000)
    before = balances(auth_client)
    response = auth_client.post("/api/investment-topups", json=request(source, target))
    assert response.status_code in (404, 409, 422), response.text
    assert balances(auth_client) == before


@pytest.mark.parametrize("value", [0, 250_000])
def test_empty_or_unknown_cost_basis_and_explicit_zero(auth_client, value):
    source = account(auth_client, balance=500_000)
    target = account(auth_client, instrument="mutual_fund")
    auth_client.patch(f"/api/accounts/{target}", json={"last_price": value})
    record(auth_client, request(source, target))
    state = balances(auth_client)[target]
    assert state["balance"] == value + 112_590
    assert state["cost_basis"] == (112_590 if value == 0 else None)
    assert state["capital_gain"] == (0 if value == 0 else None)
    result = auth_client.post(f"/api/accounts/{target}/valuation", json={"current_balance": 0, "cost_basis": 0})
    assert result.status_code == 200
    assert result.json()["account"]["balance"] == 0
    assert result.json()["account"]["cost_basis"] == 0
    assert result.json()["account"]["initial_balance"] == 0


def test_retry_and_changed_input_conflict(auth_client, portfolio):
    source, target, _ = portfolio
    payload = request(source, target)
    first = record(auth_client, payload)
    assert record(auth_client, payload)["id"] == first["id"]
    assert auth_client.post("/api/investment-topups", json={**payload, "amount": 100_000}).status_code == 409
    assert balances(auth_client)[source]["balance"] == 387_410
    for suffix in ("A", "B"):
        record(auth_client, request(source, target, amount=1, idempotency_key="a" * 127 + suffix))
    assert balances(auth_client)[source]["balance"] == 387_408


def parallel_clients(auth_client):
    from app.main import app

    clients = [TestClient(app, base_url="https://testserver") for _ in range(2)]
    for client in clients:
        client.headers["Origin"] = "https://testserver"
        client.cookies.update(auth_client.cookies)
    return clients


def test_concurrent_replay_and_edits(auth_client, portfolio):
    source, target, _ = portfolio
    clients = parallel_clients(auth_client)
    payload = request(source, target)
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda client: record(client, payload), clients))
    assert results[0]["id"] == results[1]["id"]
    topup_id = results[0]["id"]
    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(lambda pair: pair[0].patch(f"/api/investment-topups/{topup_id}", json={"amount": pair[1]}), zip(clients, (100_000, 120_000))))
    assert all(response.status_code == 200 for response in responses)
    amount = auth_client.get(f"/api/investment-topups/{topup_id}").json()["topup"]["amount"]
    state = balances(auth_client)
    assert state[source]["balance"] == 500_000 - amount
    assert state[target]["cost_basis"] == 1_000_000 + amount
    assert state[target]["capital_gain"] == 50_000


def test_mutation_guards_and_archived_target_reversal(auth_client, portfolio):
    source, target, _ = portfolio
    topup = record(auth_client, request(source, target))
    mid = topup["id"]
    assert auth_client.delete(f"/api/movements/{mid}").status_code == 409
    assert auth_client.post(f"/api/movements/{mid}/split").status_code == 409
    assert auth_client.patch(f"/api/movements/{mid}", json={"source_account_id": source, "target_account_id": target, "amount": 1}).status_code == 409
    assert auth_client.delete(f"/api/transactions/{topup['expense_transaction_id']}").status_code == 409
    assert auth_client.patch(f"/api/transactions/{topup['income_transaction_id']}", json={"amount": 1}).status_code == 409
    assert auth_client.post("/api/movements/merge", json={"expense_transaction_id": topup["expense_transaction_id"], "income_transaction_id": topup["income_transaction_id"]}).status_code == 409
    assert auth_client.patch(f"/api/accounts/{target}", json={"units": 2}).status_code == 409
    assert auth_client.patch(f"/api/accounts/{target}", json={"last_price": 1}).status_code == 409
    assert auth_client.post(f"/api/accounts/{target}/reconcile", json={"actual_balance": 1}).status_code == 409
    assert auth_client.post("/api/accounts", json={"name": f"Child {uuid4().hex}", "type": "investment", "parent_id": target}).status_code == 409
    assert auth_client.post("/api/transactions", json={"account_id": source, "target_account_id": target, "amount": 1, "type": "expense", "investment_action": "buy", "units": 1, "price_per_unit": 1}).status_code == 409
    assert auth_client.delete(f"/api/accounts/{target}").status_code == 200
    assert auth_client.delete(f"/api/investment-topups/{mid}").status_code == 200
    assert balances(auth_client)[source]["balance"] == 500_000


def test_invalid_reversal_and_increased_debit_roll_back(auth_client, portfolio):
    source, target, _ = portfolio
    topup = record(auth_client, request(source, target))
    assert auth_client.patch(f"/api/investment-topups/{topup['id']}", json={"amount": 600_000}).status_code == 409
    auth_client.post(f"/api/accounts/{target}/valuation", json={"current_balance": 1})
    before = balances(auth_client)
    assert auth_client.delete(f"/api/investment-topups/{topup['id']}").status_code == 409
    assert balances(auth_client) == before


def test_concurrent_contributions_cannot_overdraw(auth_client):
    source = account(auth_client, balance=200_000)
    target = account(auth_client, instrument="mutual_fund")
    clients = parallel_clients(auth_client)
    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(lambda client: client.post("/api/investment-topups", json=request(source, target)), clients))
    assert sorted(response.status_code for response in responses) == [200, 409]
    state = balances(auth_client)
    assert state[source]["balance"] == 87_410
    assert state[target]["cost_basis"] == 112_590


def test_valuation_and_correction_serialize_without_losing_capital(auth_client, portfolio):
    source, target, _ = portfolio
    topup = record(auth_client, request(source, target))
    clients = parallel_clients(auth_client)
    with ThreadPoolExecutor(max_workers=2) as executor:
        valuation = executor.submit(clients[0].post, f"/api/accounts/{target}/valuation", json={"current_balance": 1_170_000})
        correction = executor.submit(clients[1].patch, f"/api/investment-topups/{topup['id']}", json={"amount": 100_000})
        assert valuation.result().status_code == 200
        assert correction.result().status_code == 200
    state = balances(auth_client)
    assert state[source]["balance"] == 400_000
    assert state[target]["cost_basis"] == 1_100_000
    assert state[target]["balance"] in (1_157_410, 1_170_000)


def test_funding_parent_resolves_to_its_liquid_pocket(auth_client):
    parent = account(auth_client)
    source = account(auth_client, balance=200_000, parent=parent)
    target = account(auth_client, instrument="mutual_fund")
    assert auth_client.patch(f"/api/accounts/{parent}", json={"default_pocket_id": source}).status_code == 200
    topup = record(auth_client, request(parent, target))
    assert topup["source_account_id"] == source
    assert balances(auth_client)[source]["balance"] == 87_410


def rule_payload(source, target, **overrides):
    return {
        "name": "Monthly fund fixture", "type": "investment_topup", "amount": 112_590,
        "source_account_id": source, "target_account_id": target,
        "schedule_type": "monthly_day", "schedule_day": date.today().day,
        "auto_post": False, **overrides,
    }


def create_rule(client, source, target, **overrides):
    response = client.post("/api/recurring", json=rule_payload(source, target, **overrides))
    assert response.status_code == 200, response.text
    return response.json()["id"]


def confirm(client, rule_id, scheduled=None, **overrides):
    return client.post("/api/recurring/execute", json={
        "rule_ids": [rule_id], "scheduled_dates": {rule_id: scheduled or date.today().isoformat()}, **overrides,
    })


def test_monthly_topup_waits_then_records_once(auth_client, portfolio):
    from app.routers.recurring import process_due_recurring_rules

    source, target, _ = portfolio
    rule_id = create_rule(auth_client, source, target)
    owner = auth_client.get("/api/auth/me").json()["user"]["id"]
    process_due_recurring_rules(user_id=owner)
    assert balances(auth_client)[source]["balance"] == 500_000
    pending = auth_client.get("/api/recurring/pending").json()["rules"]
    assert any(rule["id"] == rule_id for rule in pending)
    first = confirm(auth_client, rule_id)
    assert first.status_code == 200, first.text
    assert first.json()["results"][0]["status"] == "succeeded"
    replay = confirm(auth_client, rule_id)
    assert replay.json()["results"][0]["idempotent"] is True
    assert balances(auth_client)[source]["balance"] == 387_410
    rules = auth_client.get("/api/recurring").json()["rules"]
    advanced = next(rule for rule in rules if rule["id"] == rule_id)
    assert date.fromisoformat(advanced["next_due_date"]) > date.today()
    mid = first.json()["results"][0]["movement_id"]
    assert auth_client.delete(f"/api/investment-topups/{mid}").status_code == 200
    assert confirm(auth_client, rule_id).json()["results"][0]["idempotent"] is True
    assert balances(auth_client)[source]["balance"] == 500_000


@pytest.mark.parametrize("overrides", [
    {"auto_post": True}, {"is_payroll_allocation": True},
    {"category_id": str(uuid4())}, {"obligation_id": str(uuid4())},
])
def test_topup_rule_forbidden_options_rejected_on_create_and_update(auth_client, portfolio, overrides):
    source, target, _ = portfolio
    assert auth_client.post("/api/recurring", json=rule_payload(source, target, **overrides)).status_code == 422
    rule_id = create_rule(auth_client, source, target)
    assert auth_client.patch(f"/api/recurring/{rule_id}", json=overrides).status_code == 422
    assert auth_client.patch(f"/api/recurring/{rule_id}", json={"amount": 100_000}).status_code == 200
    assert auth_client.post(f"/api/recurring/{rule_id}/toggle").status_code == 200


def test_confirmation_failure_and_partial_batch_leave_failed_occurrence_pending(auth_client):
    source = account(auth_client, balance=200_000)
    target = account(auth_client, instrument="mutual_fund")
    ids = [create_rule(auth_client, source, target) for _ in range(2)]
    response = auth_client.post("/api/recurring/execute", json={"rule_ids": ids, "scheduled_dates": dict.fromkeys(ids, date.today().isoformat())})
    assert response.status_code == 200, response.text
    assert sorted(result["status"] for result in response.json()["results"]) == ["failed", "succeeded"]
    failed_id = next(result["rule_id"] for result in response.json()["results"] if result["status"] == "failed")
    pending = auth_client.get("/api/recurring/pending").json()["rules"]
    failed = next(rule for rule in pending if rule["id"] == failed_id)
    assert failed["next_due_date"] == date.today().isoformat()
    assert balances(auth_client)[source]["balance"] == 87_410


def test_concurrent_and_delayed_monthly_confirmation(auth_client, portfolio):
    from app.db.pool import db_conn

    source, target, _ = portfolio
    rule_id = create_rule(auth_client, source, target)
    yesterday = date.today() - timedelta(days=1)
    with db_conn() as conn:
        conn.execute("UPDATE recurring_rules SET next_due_date = %s WHERE id = %s", (yesterday, rule_id))
    clients = parallel_clients(auth_client)
    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(lambda client: confirm(client, rule_id, yesterday.isoformat()), clients))
    assert all(response.status_code == 200 for response in responses)
    assert all(response.json()["results"][0]["status"] == "succeeded" for response in responses)
    assert balances(auth_client)[source]["balance"] == 387_410
    with db_conn() as conn:
        row = conn.execute("SELECT date FROM investment_topups WHERE recurring_execution_id IN (SELECT id FROM recurring_executions WHERE recurring_rule_id = %s)", (rule_id,)).fetchone()
        assert row["date"].date() == date.today()


def test_stale_future_and_missing_occurrence_do_not_execute(auth_client, portfolio):
    source, target, _ = portfolio
    rule_id = create_rule(auth_client, source, target)
    for scheduled in ((date.today() - timedelta(days=1)).isoformat(), (date.today() + timedelta(days=1)).isoformat()):
        response = confirm(auth_client, rule_id, scheduled)
        assert response.json()["results"][0]["status"] == "failed"
    response = auth_client.post("/api/recurring/execute", json={"rule_ids": [rule_id]})
    assert response.json()["results"][0]["error_code"] == "scheduled_date_required"
    assert balances(auth_client)[source]["balance"] == 500_000


def test_migration_preserves_legacy_accounts_and_rules(db_available, db_url):
    if not db_available:
        pytest.skip("Disposable PostgreSQL required")
    import psycopg
    from psycopg import sql

    schema = f"topups_upgrade_{uuid4().hex}"
    migrations = sorted((Path(__file__).resolve().parents[2] / "db/migrations").glob("V*.sql"), key=lambda p: int(p.name.split("__")[0][1:]))
    with psycopg.connect(db_url) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        conn.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
        for path in migrations[:-1]:
            conn.execute(path.read_text())
        owner = conn.execute("INSERT INTO users (username, password_hash, invite_code) VALUES ('Migration fixture', 'unused', 'fixture') RETURNING id").fetchone()[0]
        aid = conn.execute("INSERT INTO accounts (user_id, name, type, initial_balance, units, avg_buy_price, last_price) VALUES (%s, 'Legacy stock', 'investment', 1000, 2, 500, 600) RETURNING id", (owner,)).fetchone()[0]
        conn.execute("INSERT INTO recurring_rules (user_id, name, type, amount, source_account_id, next_due_date) VALUES (%s, 'Legacy bill', 'expense', 100, %s, CURRENT_DATE)", (owner, aid))
        before = conn.execute("SELECT id, initial_balance, units, avg_buy_price, last_price FROM accounts").fetchall()
        rule_before = conn.execute("SELECT * FROM recurring_rules").fetchall()
        conn.execute(migrations[-1].read_text())
        conn.execute(migrations[-1].read_text())
        assert conn.execute("SELECT id, initial_balance, units, avg_buy_price, last_price FROM accounts").fetchall() == before
        assert conn.execute("SELECT * FROM recurring_rules").fetchall() == rule_before
        assert conn.execute("SELECT COUNT(*) FROM investment_topups").fetchone()[0] == 0
        conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
