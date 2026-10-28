from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

PILLARS = ("need", "want", "culture", "unexpected", "saving")
MIRROR_SQL = Path(__file__).resolve().parents[1] / "app/db/full_kakeibo_and_adjustment_category.sql"


@pytest.fixture
def owner(client):
    from app.main import app
    from app.db.pool import db_conn
    from app.services.auth import get_current_user

    user_id = str(uuid4())
    ids = {name: str(uuid4()) for name in ("cash", "need", "want", "culture", "plain", "salary")}
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (id, username, name, password_hash, invite_code) VALUES (%s, %s, 'Sari Contoh', 'synthetic-hash', 'TEST')",
                (user_id, f"pillar_{uuid4().hex}"),
            )
            cur.execute(
                "INSERT INTO accounts (id, user_id, name, type, initial_balance) VALUES (%s, %s, 'Kas Contoh', 'bank', 10000000)",
                (ids["cash"], user_id),
            )
            for key, name, kind, pillar in [
                ("need", "Belanja Dapur", "expense", "need"), ("want", "Jajan", "expense", "want"),
                ("culture", "Olahraga", "expense", "culture"), ("plain", "Lain-lain", "expense", None),
                ("salary", "Gaji", "income", None),
            ]:
                cur.execute(
                    "INSERT INTO categories (id, user_id, name, kind, kakeibo_type) VALUES (%s, %s, %s, %s, %s)",
                    (ids[key], user_id, name, kind, pillar),
                )
        conn.commit()
    app.dependency_overrides[get_current_user] = lambda: {
        "id": user_id, "username": "pillar-owner", "name": "Sari Contoh", "currency": "IDR", "payday_day": 25,
    }
    yield {"user_id": user_id, **ids}
    app.dependency_overrides.clear()
    with db_conn() as conn:
        conn.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()


def insert_tx(owner, category, amount, *, pillar=None, tx_type="expense", when=None, notes="Contoh", key=None):
    from app.db.pool import db_conn

    with db_conn() as conn:
        tx_id = conn.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, date, kakeibo_type, idempotency_key)
               VALUES (%s, %s, %s, %s, %s, %s, COALESCE(%s, NOW()), %s, %s) RETURNING id""",
            (owner["user_id"], owner["cash"], owner[category] if category else None, tx_type, amount, notes, when, pillar, key),
        ).fetchone()["id"]
        conn.commit()
    return str(tx_id)


def test_pillar_constraints_accept_full_set_and_reject_unknown(owner):
    for pillar in PILLARS:
        insert_tx(owner, "plain", 1_000, pillar=pillar)
    with pytest.raises(psycopg.errors.CheckViolation):
        insert_tx(owner, "plain", 1_000, pillar="luxury")


def test_api_accepts_every_pillar_and_rejects_unknown(client, owner):
    from app.db.pool import db_conn

    for pillar in PILLARS:
        response = client.post("/api/transactions", json={
            "account_id": owner["cash"], "category_id": owner["plain"], "type": "expense",
            "amount": 10_000, "notes": f"Uji {pillar}", "kakeibo_type": pillar,
        })
        assert response.status_code == 200, response.text
        with db_conn() as conn:
            stored = conn.execute("SELECT kakeibo_type FROM transactions WHERE id = %s", (response.json()["transaction_id"],)).fetchone()
        assert stored["kakeibo_type"] == pillar

    rejected = client.post("/api/transactions", json={
        "account_id": owner["cash"], "category_id": owner["plain"], "type": "expense", "amount": 10_000, "kakeibo_type": "luxury",
    })
    assert rejected.status_code == 422

    created = client.post("/api/categories", json={"name": "Kursus", "kind": "expense", "kakeibo_type": "culture"})
    assert created.status_code == 200, created.text
    assert created.json()["category"]["kakeibo_type"] == "culture"
    category_id = created.json()["category"]["id"]
    assert client.put(f"/api/categories/{category_id}", json={"kakeibo_type": "unexpected"}).status_code == 200
    assert client.put(f"/api/categories/{category_id}", json={"kakeibo_type": "luxury"}).status_code == 422


def test_transaction_without_pillar_follows_its_category(client, owner):
    tx_id = insert_tx(owner, "want", 100_000)

    listed = client.get("/api/transactions", params={"kakeibo_type": "want"}).json()["transactions"]
    row = next(item for item in listed if item["id"] == tx_id)
    assert row["kakeibo_type"] == "want"
    needs = client.get("/api/transactions", params={"kakeibo_type": "need"}).json()["transactions"]
    assert tx_id not in {item["id"] for item in needs}
    assert client.get(f"/api/transactions/{tx_id}").json()["transaction"]["kakeibo_type"] == "want"


def test_income_never_resolves_to_a_pillar(client, owner):
    income_id = insert_tx(owner, "salary", 6_000_000, tx_type="income")

    listed = client.get("/api/transactions").json()["transactions"]
    assert next(item for item in listed if item["id"] == income_id)["kakeibo_type"] is None
    needs = client.get("/api/transactions", params={"kakeibo_type": "need"}).json()["transactions"]
    assert income_id not in {item["id"] for item in needs}


def breakdown(owner, month, inflow):
    from app.db.pool import db_conn
    from app.routers.dashboard import get_kakeibo_breakdown

    start = datetime(2026, month, 1, tzinfo=timezone.utc)
    end = datetime(2026, month, 28, tzinfo=timezone.utc)
    with db_conn() as conn:
        with conn.cursor() as cur:
            return get_kakeibo_breakdown(cur, owner["user_id"], start, end, inflow)


def test_lifestyle_group_over_target_is_a_warning(owner):
    when = datetime(2026, 1, 10, tzinfo=timezone.utc)
    insert_tx(owner, "need", 400_000, pillar="need", when=when)
    insert_tx(owner, "want", 250_000, when=when)  # no own pillar: follows the category
    insert_tx(owner, "culture", 150_000, pillar="culture", when=when)
    insert_tx(owner, "plain", 200_000, pillar="saving", when=when)

    result = breakdown(owner, 1, 1_000_000)
    assert (result["need_pct"], result["want_pct"], result["culture_pct"], result["saving_pct"]) == (40.0, 25.0, 15.0, 20.0)
    assert result["lifestyle_pct"] == 40.0
    assert result["kakeibo_status"] == "warning"
    assert result["want_status"] == "warning"


def test_essentials_group_over_target_is_elevated(owner):
    when = datetime(2026, 2, 10, tzinfo=timezone.utc)
    insert_tx(owner, "need", 450_000, pillar="need", when=when)
    insert_tx(owner, "plain", 150_000, pillar="unexpected", when=when)
    insert_tx(owner, "want", 100_000, pillar="want", when=when)
    insert_tx(owner, "plain", 300_000, pillar="saving", when=when)

    result = breakdown(owner, 2, 1_000_000)
    assert result["unexpected_spent"] == 150_000
    assert (result["essentials_pct"], result["lifestyle_pct"]) == (60.0, 10.0)
    assert result["kakeibo_status"] == "elevated_needs"
    pillar_total = sum(result[f"{p}_pct"] for p in PILLARS)
    assert pillar_total == pytest.approx(100.0, abs=0.2)


def test_unallocated_inflow_and_empty_cycle(owner):
    insert_tx(owner, "plain", 500_000, when=datetime(2026, 3, 10, tzinfo=timezone.utc))  # no pillar anywhere: need
    partial = breakdown(owner, 3, 2_000_000)
    assert partial["need_pct"] == 25.0
    assert sum(partial[f"{p}_pct"] for p in PILLARS) == 25.0

    empty = breakdown(owner, 4, 0)
    assert all(empty[f"{p}_pct"] == 0.0 for p in PILLARS)
    assert empty["essentials_pct"] == empty["lifestyle_pct"] == 0.0
    assert empty["kakeibo_status"] == "healthy"


def test_pulse_counts_culture_and_unexpected_but_not_saving(client, owner):
    insert_tx(owner, "culture", 50_000)
    insert_tx(owner, "plain", 80_000, pillar="unexpected")
    insert_tx(owner, "need", 200_000, pillar="saving")

    pulse = client.get("/api/pulse")
    assert pulse.status_code == 200, pulse.text
    assert pulse.json()["today_spent"] == 130_000
    assert pulse.json()["cycle_spent"] == 130_000


def test_reconcile_adjustments_use_excluded_system_category(client, owner):
    from app.db.pool import db_conn

    down = client.post(f"/api/accounts/{owner['cash']}/reconcile", json={"actual_balance": 9_950_000})
    assert down.status_code == 200, down.text
    up = client.post(f"/api/accounts/{owner['cash']}/reconcile", json={"actual_balance": 10_000_000})
    assert up.status_code == 200, up.text
    assert (down.json()["diff"], up.json()["diff"]) == (-50_000, 50_000)

    with db_conn() as conn:
        rows = conn.execute(
            """SELECT t.type, t.amount, c.name, c.kind, c.is_excluded_from_budget, c.kakeibo_type
               FROM transactions t JOIN categories c ON c.id = t.category_id
               WHERE t.user_id = %s ORDER BY t.type""",
            (owner["user_id"],),
        ).fetchall()
    assert [(r["type"], r["amount"], r["name"], r["kind"]) for r in rows] == [
        ("expense", 50_000, "Penyesuaian Saldo", "expense"),
        ("income", 50_000, "Penyesuaian Saldo", "income"),
    ]
    assert all(r["is_excluded_from_budget"] and r["kakeibo_type"] is None for r in rows)

    pulse = client.get("/api/pulse").json()
    assert pulse["today_spent"] == 0
    assert breakdown(owner, datetime.now(timezone.utc).month, 0)["need_spent"] == 0


def test_v27_mirror_is_idempotent_and_backfills_only_default_adjustments(owner):
    from app.db.pool import db_conn

    note = "Balance Adjustment (Reconciliation)"
    moved = insert_tx(owner, None, 7_000, notes=note)
    moved_income = insert_tx(owner, None, 3_000, tx_type="income", notes=note)
    categorised = insert_tx(owner, "need", 5_000, notes=note)
    custom_note = insert_tx(owner, None, 4_000, notes="Koreksi saldo manual")
    keyed = insert_tx(owner, None, 6_000, notes=note, key=f"client-{uuid4().hex[:8]}")

    with db_conn() as conn:
        conn.execute(MIRROR_SQL.read_text(encoding="utf-8"))
        conn.execute(MIRROR_SQL.read_text(encoding="utf-8"))
        conn.commit()
        rows = {
            str(r["id"]): r for r in conn.execute(
                """SELECT t.id, t.category_id, c.name, c.kind FROM transactions t
                   LEFT JOIN categories c ON c.id = t.category_id WHERE t.user_id = %s""",
                (owner["user_id"],),
            ).fetchall()
        }
        adjustment_categories = conn.execute(
            "SELECT COUNT(*) AS count FROM categories WHERE user_id = %s AND name = 'Penyesuaian Saldo'", (owner["user_id"],)
        ).fetchone()["count"]
        constraints = conn.execute(
            """SELECT pg_get_constraintdef(oid) AS definition FROM pg_constraint
               WHERE conname IN ('transactions_kakeibo_type_check', 'categories_kakeibo_type_check')"""
        ).fetchall()

    assert (rows[moved]["name"], rows[moved]["kind"]) == ("Penyesuaian Saldo", "expense")
    assert (rows[moved_income]["name"], rows[moved_income]["kind"]) == ("Penyesuaian Saldo", "income")
    assert str(rows[categorised]["category_id"]) == owner["need"]
    assert rows[custom_note]["category_id"] is None
    assert rows[keyed]["category_id"] is None
    assert adjustment_categories == 2
    assert len(constraints) == 2 and all("unexpected" in c["definition"] for c in constraints)
