import re
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from app.services.provenance import Provenance, normalize_merchant, notification_provenance

APP_ROOT = Path(__file__).resolve().parents[1] / "app"


def account(client, name, balance=0, **extra):
    response = client.post("/api/accounts", json={"name": f"{name} {uuid4().hex[:6]}", "type": "bank", "initial_balance": balance, **extra})
    assert response.status_code == 200, response.text
    return response.json()["account"]["id"]


def category(client, name, kind="expense"):
    rows = client.get("/api/categories").json()["categories"]
    return next(row["id"] for row in rows if row["name"] == name and row["kind"] == kind)


def stored(tx_id):
    from app.db.pool import db_conn

    with db_conn() as conn:
        return conn.execute(
            """SELECT source, review_status, classified_by, classification_confidence, owner_edited_at,
                      merchant, notes, category_id, amount, updated_at
               FROM transactions WHERE id = %s""",
            (tx_id,),
        ).fetchone()


def provenance_of(rows):
    return {(row["source"], row["review_status"], row["classified_by"]) for row in rows}


def movement_legs(movement_id):
    from app.db.pool import db_conn

    with db_conn() as conn:
        return conn.execute(
            "SELECT id, source, review_status, classified_by, owner_edited_at FROM transactions WHERE movement_id = %s",
            (movement_id,),
        ).fetchall()


def insert_classified_row(client, account_id, category_id, *, source="notification", review_status="auto",
                          classified_by="ai", confidence=0.7, notes="Bayar Kedai Contoh"):
    from app.db.pool import db_conn

    user_id = client.get("/api/auth/me").json()["user"]["id"]
    with db_conn() as conn:
        tx_id = conn.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, date,
                   source, review_status, classified_by, classification_confidence)
               VALUES (%s, %s, %s, 'expense', 25000, %s, NOW(), %s, %s, %s, %s) RETURNING id""",
            (user_id, account_id, category_id, notes, source, review_status, classified_by, confidence),
        ).fetchone()["id"]
        conn.commit()
    return str(tx_id)


def test_every_transaction_insert_names_provenance():
    statements = []
    for path in APP_ROOT.rglob("*.py"):
        if "services/ledger/" in path.as_posix():
            continue
        text = path.read_text(encoding="utf-8")
        for match in re.finditer(r"INSERT INTO transactions\s*\(([^)]*)\)", text):
            statements.append((path.name, match.group(1)))
    assert len(statements) >= 7
    for name, columns in statements:
        for column in ("source", "review_status", "classified_by"):
            assert re.search(rf"\b{column}\b", columns), f"{name} inserts without {column}"


def test_provenance_vocabulary_and_merchant_normalisation():
    with pytest.raises(ValueError):
        Provenance("bot", "reviewed", "system")
    with pytest.raises(ValueError):
        Provenance("manual", "reviewed", "owner", 1.5)
    assert notification_provenance("ai", 0.82).columns() == ("notification", "auto", "ai", 0.82)
    assert notification_provenance("deterministic", 1.0).columns() == ("notification", "auto", "rule", None)
    assert notification_provenance("manual", None).columns() == ("notification", "reviewed", "owner", None)
    assert normalize_merchant("  Kopi   Contoh  ") == "Kopi Contoh"
    assert normalize_merchant("   ") is None
    with pytest.raises(ValueError):
        normalize_merchant("x" * 121)


def test_manual_create_writes_owner_provenance_and_validates_inputs(auth_client):
    cash = account(auth_client, "Kas", 1_000_000)
    food = category(auth_client, "Makanan & Minuman")
    created = auth_client.post("/api/transactions", json={
        "type": "expense", "account_id": cash, "category_id": food, "amount": 15_000,
        "merchant": "  Kopi   Contoh  ", "notes": "Kopi susu",
    })
    assert created.status_code == 200, created.text
    row = stored(created.json()["transaction_id"])
    assert (row["source"], row["review_status"], row["classified_by"]) == ("manual", "reviewed", "owner")
    assert row["merchant"] == "Kopi Contoh" and row["notes"] == "Kopi susu"
    assert row["classification_confidence"] is None and row["owner_edited_at"] is None

    flagged = auth_client.post("/api/transactions", json={
        "type": "expense", "account_id": cash, "category_id": food, "amount": 1_000,
        "merchant": "   ", "review_status": "needs_review",
    })
    flagged_row = stored(flagged.json()["transaction_id"])
    assert flagged_row["merchant"] is None and flagged_row["review_status"] == "needs_review"

    listed_before = auth_client.get(f"/api/transactions?account_id={cash}").json()["total"]
    for body in ({"review_status": "auto"}, {"review_status": "done"}, {"merchant": "x" * 121}):
        rejected = auth_client.post("/api/transactions", json={
            "type": "expense", "account_id": cash, "category_id": food, "amount": 1_000, **body,
        })
        assert rejected.status_code == 422, body
    assert auth_client.get(f"/api/transactions?account_id={cash}").json()["total"] == listed_before


def test_system_paths_write_their_source(auth_client):
    from app.db.pool import db_conn

    source = account(auth_client, "Sumber", 5_000_000)
    target = account(auth_client, "Tujuan")
    moved = auth_client.post("/api/movements", json={"source_account_id": source, "target_account_id": target, "amount": 100_000})
    assert provenance_of(movement_legs(moved.json()["movement_id"])) == {("movement", "reviewed", "system")}

    payroll = auth_client.post("/api/recurring/payroll/execute", json={
        "items": [{"source_account_id": source, "target_account_id": target, "amount": 50_000}],
    })
    assert payroll.status_code == 200, payroll.text
    assert provenance_of(movement_legs(payroll.json()["results"][0]["movement_id"])) == {("payroll", "reviewed", "system")}

    rule = auth_client.post("/api/recurring", json={
        "name": "Langganan contoh", "type": "expense", "amount": 30_000, "source_account_id": source,
        "category_id": category(auth_client, "Hiburan"), "schedule_type": "monthly_day", "schedule_day": date.today().day,
    })
    assert rule.status_code == 200, rule.text
    executed = auth_client.post("/api/recurring/execute", json={"rule_ids": [rule.json()["id"]]})
    assert executed.json()["results"][0]["status"] == "succeeded", executed.text
    recurring_row = stored(executed.json()["results"][0]["transaction_id"])
    assert (recurring_row["source"], recurring_row["classified_by"]) == ("recurring", "system")

    reconciled = auth_client.post(f"/api/accounts/{target}/reconcile", json={"actual_balance": 140_000})
    assert reconciled.status_code == 200, reconciled.text
    user_id = auth_client.get("/api/auth/me").json()["user"]["id"]
    with db_conn() as conn:
        adjustment = conn.execute(
            "SELECT source, review_status, classified_by FROM transactions WHERE user_id = %s AND account_id = %s AND movement_id IS NULL",
            (user_id, target),
        ).fetchone()
    assert (adjustment["source"], adjustment["review_status"], adjustment["classified_by"]) == ("reconcile", "reviewed", "system")

    parent = account(auth_client, "Reksadana")
    fund = auth_client.post("/api/accounts", json={
        "name": f"Dana pasar uang {uuid4().hex[:6]}", "type": "investment", "instrument_type": "mutual_fund",
        "initial_balance": 0, "parent_id": parent,
    }).json()["account"]["id"]
    topup = auth_client.post("/api/investment-topups", json={
        "source_account_id": source, "target_account_id": fund, "amount": 200_000,
        "date": datetime.now(timezone.utc).isoformat(), "notes": "", "idempotency_key": str(uuid4()),
    })
    assert topup.status_code == 200, topup.text
    assert provenance_of(movement_legs(topup.json()["topup"]["movement_id"])) == {("investment", "reviewed", "system")}

    position = auth_client.post("/api/accounts", json={
        "name": f"Saham contoh {uuid4().hex[:6]}", "type": "investment", "instrument_type": "stock",
        "instrument_symbol": "CNTH.JK", "units": 0, "avg_buy_price": 0, "initial_balance": 0,
    }).json()["account"]["id"]
    bought = auth_client.post("/api/transactions", json={
        "type": "expense", "account_id": source, "target_account_id": position, "amount": 10_000,
        "investment_action": "buy", "units": 5, "price_per_unit": 2_000,
    })
    assert bought.status_code == 200, bought.text
    assert provenance_of(movement_legs(bought.json()["movement_id"])) == {("investment", "reviewed", "system")}
    with_merchant = auth_client.post("/api/transactions", json={
        "type": "expense", "account_id": source, "target_account_id": position, "amount": 10_000,
        "investment_action": "buy", "units": 5, "price_per_unit": 2_000, "merchant": "Broker",
    })
    assert with_merchant.status_code == 422


def test_owner_classification_edit_marks_row_reviewed(auth_client):
    cash = account(auth_client, "Kas", 1_000_000)
    food = category(auth_client, "Makanan & Minuman")
    snacks = category(auth_client, "Jajan & Nongkrong")
    tx_id = insert_classified_row(auth_client, cash, food)
    before = stored(tx_id)

    amount_only = auth_client.patch(f"/api/transactions/{tx_id}", json={"amount": 30_000})
    assert amount_only.status_code == 200, amount_only.text
    row = stored(tx_id)
    assert (row["review_status"], row["classified_by"], row["owner_edited_at"]) == ("auto", "ai", None)
    assert float(row["classification_confidence"]) == 0.7

    # The edit form re-sends the pillar it displays; an unchanged effective pillar is not a reclassification.
    resent = auth_client.patch(f"/api/transactions/{tx_id}", json={
        "type": "expense", "amount": 30_000, "account_id": cash, "category_id": food,
        "kakeibo_type": "need", "notes": "Bayar Kedai Contoh ", "merchant": None,
    })
    assert resent.status_code == 200, resent.text
    assert stored(tx_id)["classified_by"] == "ai"

    recategorised = auth_client.patch(f"/api/transactions/{tx_id}", json={"category_id": snacks})
    assert recategorised.status_code == 200, recategorised.text
    row = stored(tx_id)
    assert (row["review_status"], row["classified_by"]) == ("reviewed", "owner")
    assert row["classification_confidence"] is None and row["owner_edited_at"] is not None
    assert row["updated_at"] > before["updated_at"]

    merchant_tx = insert_classified_row(auth_client, cash, food)
    flagged = auth_client.patch(f"/api/transactions/{merchant_tx}", json={"merchant": "Kedai Contoh", "review_status": "needs_review"})
    assert flagged.status_code == 200
    row = stored(merchant_tx)
    assert (row["merchant"], row["review_status"], row["classified_by"]) == ("Kedai Contoh", "needs_review", "owner")


def test_review_only_patch_keeps_classifier_and_works_on_movement_legs(auth_client):
    cash = account(auth_client, "Kas", 1_000_000)
    other = account(auth_client, "Dompet")
    food = category(auth_client, "Makanan & Minuman")
    tx_id = insert_classified_row(auth_client, cash, food)

    reviewed = auth_client.patch(f"/api/transactions/{tx_id}", json={"review_status": "reviewed"})
    assert reviewed.status_code == 200, reviewed.text
    row = stored(tx_id)
    assert (row["review_status"], row["classified_by"], row["owner_edited_at"]) == ("reviewed", "ai", None)
    assert row["amount"] == 25_000 and row["notes"] == "Bayar Kedai Contoh" and str(row["category_id"]) == food

    assert auth_client.patch(f"/api/transactions/{tx_id}", json={"review_status": "auto"}).status_code == 422
    assert auth_client.patch(f"/api/transactions/{tx_id}", json={"review_status": None}).status_code == 422

    moved = auth_client.post("/api/movements", json={"source_account_id": cash, "target_account_id": other, "amount": 10_000})
    leg = movement_legs(moved.json()["movement_id"])[0]["id"]
    assert auth_client.patch(f"/api/transactions/{leg}", json={"review_status": "needs_review"}).status_code == 200
    assert stored(leg)["review_status"] == "needs_review"
    assert auth_client.patch(f"/api/transactions/{leg}", json={"notes": "x"}).status_code == 409


def test_movement_notes_edit_and_merge_are_owner_edits(auth_client):
    cash = account(auth_client, "Kas", 1_000_000)
    other = account(auth_client, "Dompet")
    moved = auth_client.post("/api/movements", json={"source_account_id": cash, "target_account_id": other, "amount": 10_000})
    movement_id = moved.json()["movement_id"]
    same_notes = auth_client.patch(f"/api/movements/{movement_id}", json={
        "source_account_id": cash, "target_account_id": other, "amount": 12_000,
    })
    assert same_notes.status_code == 200, same_notes.text
    assert all(leg["owner_edited_at"] is None for leg in movement_legs(movement_id))
    renamed = auth_client.patch(f"/api/movements/{movement_id}", json={
        "source_account_id": cash, "target_account_id": other, "amount": 12_000, "notes": "Isi dompet",
    })
    assert renamed.status_code == 200
    assert provenance_of(movement_legs(movement_id)) == {("movement", "reviewed", "owner")}
    assert all(leg["owner_edited_at"] is not None for leg in movement_legs(movement_id))

    food = category(auth_client, "Makanan & Minuman")
    expense = insert_classified_row(auth_client, cash, food)
    income = auth_client.post("/api/transactions", json={
        "type": "income", "account_id": other, "amount": 25_000, "category_id": category(auth_client, "Pendapatan Lain", "income"),
    }).json()["transaction_id"]
    merged = auth_client.post("/api/movements/merge", json={"expense_transaction_id": expense, "income_transaction_id": income})
    assert merged.status_code == 200, merged.text
    legs = movement_legs(merged.json()["movement_id"])
    assert {leg["classified_by"] for leg in legs} == {"owner"} and {leg["review_status"] for leg in legs} == {"reviewed"}
    assert stored(expense)["source"] == "notification"


def test_ledger_filters_search_and_response_fields(auth_client):
    cash = account(auth_client, "Kas", 1_000_000)
    food = category(auth_client, "Makanan & Minuman")
    auto = insert_classified_row(auth_client, cash, food, notes="Satu")
    needs = insert_classified_row(auth_client, cash, food, review_status="needs_review", notes="Dua")
    manual = auth_client.post("/api/transactions", json={
        "type": "expense", "account_id": cash, "category_id": food, "amount": 5_000,
        "merchant": "Warung Contoh", "notes": "Tiga",
    }).json()["transaction_id"]

    def ids(query):
        response = auth_client.get(f"/api/transactions?account_id={cash}&{query}")
        assert response.status_code == 200, response.text
        return {row["id"] for row in response.json()["transactions"]}

    assert ids("review_status=needs_review") == {needs}
    assert ids("review_status=auto") == {auto}
    assert ids("source=notification") == {auto, needs}
    assert ids("source=manual") == {manual}
    assert ids("q=warung") == {manual}
    assert auth_client.get("/api/transactions?source=bot").status_code == 422
    assert auth_client.get("/api/transactions?review_status=done").status_code == 422

    detail = auth_client.get(f"/api/transactions/{auto}").json()["transaction"]
    assert detail["source"] == "notification" and detail["review_status"] == "auto"
    assert detail["classified_by"] == "ai" and detail["classification_confidence"] == 0.7
    assert detail["merchant"] is None and detail["owner_edited_at"] is None
    assert detail["is_reimbursement"] is False and detail["offset_category_id"] is None
    listed = next(row for row in auth_client.get(f"/api/transactions?account_id={cash}").json()["transactions"] if row["id"] == manual)
    assert listed["merchant"] == "Warung Contoh" and listed["source"] == "manual"


def test_new_user_defaults_include_snack_and_reimbursement_categories(auth_client):
    categories = auth_client.get("/api/categories").json()["categories"]
    snacks = next(row for row in categories if row["name"] == "Jajan & Nongkrong")
    assert snacks["kind"] == "expense" and snacks["kakeibo_type"] == "want"
    reimbursement = next(row for row in categories if row["name"] == "Patungan & Ganti Uang")
    assert reimbursement["kind"] == "income" and reimbursement["offsets_spending"] is True
    assert not any(row["offsets_spending"] for row in categories if row["name"] != "Patungan & Ganti Uang")
