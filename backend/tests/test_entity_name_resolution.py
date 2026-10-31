"""Exact name resolution for clients that address accounts and categories by name."""
from uuid import uuid4

import pytest
from fastapi import HTTPException

from app.services.entity_names import CANDIDATE_LIMIT, resolve_account_name, resolve_category_name


def account(name, *, parent=None, symbol=None):
    return {"id": str(uuid4()), "name": name, "parent_id": parent["id"] if parent else None, "instrument_symbol": symbol}


def unresolved(call):
    with pytest.raises(HTTPException) as raised:
        call()
    assert raised.value.status_code == 422
    return raised.value.detail


def test_exact_name_ignores_case_and_repeated_spaces():
    bca = account("BCA Harian")
    accounts = [bca, account("BCA Tabungan")]
    assert resolve_account_name("BCA Harian", accounts, field="account_name") is bca
    assert resolve_account_name("  bca   HARIAN ", accounts, field="account_name") is bca


def test_a_pocket_wins_over_a_parent_with_the_same_name():
    jago = account("Bank Jago")
    pocket = account("Tabungan", parent=jago)
    accounts = [jago, account("Tabungan"), pocket]
    assert resolve_account_name("tabungan", accounts, field="account_name") is pocket


def test_two_matching_pockets_are_ambiguous_and_listed():
    jago, blu = account("Bank Jago"), account("Blu Contoh")
    first, second = account("Cadangan", parent=jago), account("Cadangan", parent=blu)
    detail = unresolved(lambda: resolve_account_name("Cadangan", [jago, blu, first, second], field="target_account_name"))
    assert (detail["code"], detail["field"], detail["reason"]) == ("account_name_unresolved", "target_account_name", "ambiguous")
    assert {(item["id"], item["parent_name"]) for item in detail["candidates"]} == {
        (first["id"], "Bank Jago"), (second["id"], "Blu Contoh"),
    }


def test_two_matching_top_level_accounts_are_ambiguous():
    accounts = [account("Dompet"), account("dompet")]
    assert unresolved(lambda: resolve_account_name("Dompet", accounts, field="account_name"))["reason"] == "ambiguous"


def test_an_unknown_name_is_not_guessed_and_offers_close_candidates():
    accounts = [account(f"BCA Rekening {index}") for index in range(7)] + [account("GoPay")]
    detail = unresolved(lambda: resolve_account_name("BCA", accounts, field="account_name"))
    assert detail["reason"] == "not_found"
    assert len(detail["candidates"]) == CANDIDATE_LIMIT
    assert all(item["name"].startswith("BCA Rekening") for item in detail["candidates"])
    assert unresolved(lambda: resolve_account_name("Emergency Fund", [account("Dana Darurat")], field="account_name"))["candidates"] == []


@pytest.mark.parametrize("name", ["", "   "])
def test_blank_names_never_match(name):
    assert unresolved(lambda: resolve_account_name(name, [account("Kas")], field="account_name"))["reason"] == "not_found"


def test_exact_instrument_symbol_identifies_a_position():
    bbri = account("BBRI Stock", symbol="BBRI.JK")
    accounts = [account("RDN BCA"), bbri]
    assert resolve_account_name("BBRI", accounts, field="target_account_name") is bbri
    assert resolve_account_name("bbri.jk", accounts, field="target_account_name") is bbri
    assert unresolved(lambda: resolve_account_name("BBR", accounts, field="target_account_name"))["reason"] == "not_found"


def test_category_name_shared_by_both_kinds_follows_the_transaction_type():
    out = {"id": str(uuid4()), "name": "Internal Movement", "kind": "expense"}
    back = {"id": str(uuid4()), "name": "Internal Movement", "kind": "income"}
    assert resolve_category_name("internal movement", [out, back], transaction_type="income") is back
    assert resolve_category_name("Internal Movement", [out, back], transaction_type="expense") is out


def test_unknown_or_ambiguous_category_names_are_unresolved():
    food = {"id": str(uuid4()), "name": "Makanan & Minuman", "kind": "expense"}
    detail = unresolved(lambda: resolve_category_name("Makanan", [food], transaction_type="expense"))
    assert (detail["code"], detail["field"], detail["reason"]) == ("category_name_unresolved", "category_name", "not_found")
    assert detail["candidates"] == [{"id": food["id"], "name": "Makanan & Minuman", "kind": "expense"}]
    twins = [{"id": str(uuid4()), "name": "Jajan", "kind": "expense"}, {"id": str(uuid4()), "name": "JAJAN", "kind": "expense"}]
    assert unresolved(lambda: resolve_category_name("jajan", twins, transaction_type="expense"))["reason"] == "ambiguous"


@pytest.fixture
def name_owner(client):
    from app.main import app
    from app.db.pool import db_conn
    from app.services.auth import get_current_user

    # Account names are unique per owner, so a normalised collision needs a case variant.
    user_id, other_id = str(uuid4()), str(uuid4())
    ids = {key: str(uuid4()) for key in (
        "bca", "jago", "main", "darurat", "jago_tabungan", "tabungan", "blu", "jago_cadangan", "blu_cadangan",
        "archived", "other_account", "food", "salary", "other_category",
    )}
    with db_conn() as conn:
        with conn.cursor() as cur:
            for uid, prefix in ((user_id, "names"), (other_id, "names_other")):
                cur.execute(
                    "INSERT INTO users (id, username, name, password_hash, invite_code) VALUES (%s, %s, 'Raka Purnama', 'synthetic-hash', 'TEST')",
                    (uid, f"{prefix}_{uuid4().hex}"),
                )
            for key, name, parent, owner_id in [
                ("bca", "BCA Harian", None, user_id), ("jago", "Bank Jago", None, user_id),
                ("main", "Kantong Utama", "jago", user_id), ("darurat", "Dana Darurat", "jago", user_id),
                ("jago_tabungan", "tabungan", "jago", user_id), ("tabungan", "Tabungan", None, user_id),
                ("blu", "Blu Contoh", None, user_id), ("jago_cadangan", "Cadangan", "jago", user_id),
                ("blu_cadangan", "CADANGAN", "blu", user_id), ("archived", "Rekening Lama", None, user_id),
                ("other_account", "Rekening Rahasia", None, other_id),
            ]:
                cur.execute(
                    "INSERT INTO accounts (id, user_id, name, type, parent_id, initial_balance) VALUES (%s, %s, %s, 'bank', %s, 1000000)",
                    (ids[key], owner_id, name, ids[parent] if parent else None),
                )
            cur.execute("UPDATE accounts SET default_pocket_id = %s WHERE id = %s", (ids["main"], ids["jago"]))
            cur.execute("UPDATE accounts SET is_archived = TRUE WHERE id = %s", (ids["archived"],))
            for key, name, kind, owner_id in [
                ("food", "Makanan & Minuman", "expense", user_id), ("salary", "Gaji", "income", user_id),
                ("other_category", "Kategori Rahasia", "expense", other_id),
            ]:
                cur.execute("INSERT INTO categories (id, user_id, name, kind) VALUES (%s, %s, %s, %s)", (ids[key], owner_id, name, kind))
        conn.commit()
    app.dependency_overrides[get_current_user] = lambda: {
        "id": user_id, "username": "names-owner", "name": "Raka Purnama", "currency": "IDR", "payday_day": 25,
    }
    yield {"user_id": user_id, "other_id": other_id, **ids}
    app.dependency_overrides.clear()
    with db_conn() as conn:
        conn.execute("DELETE FROM users WHERE id = ANY(%s)", ([user_id, other_id],))
        conn.commit()


def entity_counts(owner):
    from app.db.pool import db_conn

    with db_conn() as conn:
        return {
            table: conn.execute(f"SELECT COUNT(*) AS count FROM {table} WHERE user_id = ANY(%s)",
                                ([owner["user_id"], owner["other_id"]],)).fetchone()["count"]
            for table in ("accounts", "categories", "transactions")
        }


def recorded_account(transaction_id):
    from app.db.pool import db_conn

    with db_conn() as conn:
        return str(conn.execute("SELECT account_id FROM transactions WHERE id = %s", (transaction_id,)).fetchone()["account_id"])


def expense(client, account_name, category_name="Makanan & Minuman", tx_type="expense"):
    return client.post("/api/transactions", json={
        "account_name": account_name, "category_name": category_name, "type": tx_type, "amount": 12_500, "notes": "Contoh",
    })


@pytest.mark.parametrize("name,expected", [
    ("BCA Harian", "bca"), ("  bca   HARIAN ", "bca"), ("Tabungan", "jago_tabungan"), ("Bank Jago", "main"),
])
def test_transactions_resolve_exact_names(client, name_owner, name, expected):
    response = expense(client, name)
    assert response.status_code == 200, response.text
    assert recorded_account(response.json()["transaction_id"]) == name_owner[expected]


@pytest.mark.parametrize("name,reason", [
    ("Cadangan", "ambiguous"), ("BCA", "not_found"), ("Emergency Fund", "not_found"),
    ("Rekening Lama", "not_found"), ("Rekening Rahasia", "not_found"),
])
def test_unresolved_account_names_create_nothing(client, name_owner, name, reason):
    before = entity_counts(name_owner)
    response = expense(client, name)
    assert response.status_code == 422, response.text
    detail = response.json()["detail"]
    assert (detail["code"], detail["field"], detail["reason"]) == ("account_name_unresolved", "account_name", reason)
    candidate_ids = {item["id"] for item in detail["candidates"]}
    assert not candidate_ids & {name_owner["other_account"], name_owner["archived"]}
    assert entity_counts(name_owner) == before


def test_not_found_account_lists_close_names(client, name_owner):
    detail = expense(client, "BCA").json()["detail"]
    assert [item["id"] for item in detail["candidates"]] == [name_owner["bca"]]


def test_unknown_and_foreign_category_names_create_nothing(client, name_owner):
    before = entity_counts(name_owner)
    for name in ("Kopi Sore", "Kategori Rahasia"):
        response = expense(client, "BCA Harian", category_name=name)
        assert response.status_code == 422, response.text
        detail = response.json()["detail"]
        assert (detail["code"], detail["reason"]) == ("category_name_unresolved", "not_found")
        assert name_owner["other_category"] not in {item["id"] for item in detail["candidates"]}
    assert entity_counts(name_owner) == before


def test_category_name_must_fit_the_transaction_type(client, name_owner):
    assert expense(client, "BCA Harian", category_name="Gaji").status_code == 400
    income = expense(client, "BCA Harian", category_name="gaji", tx_type="income")
    assert income.status_code == 200, income.text


def test_movements_resolve_names_exactly_and_create_nothing_otherwise(client, name_owner):
    from app.db.pool import db_conn

    moved = client.post("/api/movements", json={
        "source_account_name": "bca harian", "target_account_name": "Dana Darurat", "amount": 50_000,
    })
    assert moved.status_code == 200, moved.text
    with db_conn() as conn:
        legs = conn.execute("SELECT type, account_id FROM transactions WHERE movement_id = %s", (moved.json()["movement_id"],)).fetchall()
    assert {(row["type"], str(row["account_id"])) for row in legs} == {
        ("expense", name_owner["bca"]), ("income", name_owner["darurat"]),
    }

    before = entity_counts(name_owner)
    for target, reason in (("Cadangan", "ambiguous"), ("Dana Cadangan Baru", "not_found")):
        response = client.post("/api/movements", json={
            "source_account_name": "BCA Harian", "target_account_name": target, "amount": 50_000,
        })
        assert response.status_code == 422, response.text
        detail = response.json()["detail"]
        assert (detail["code"], detail["field"], detail["reason"]) == ("account_name_unresolved", "target_account_name", reason)
    missing = client.post("/api/movements", json={"source_account_name": "BCA Harian", "amount": 50_000})
    assert missing.status_code == 422, missing.text
    assert entity_counts(name_owner) == before
