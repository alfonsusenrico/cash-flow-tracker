"""Kakeibo defaults: no silent `need`, category pillar edits teach habit learning, starter pillars.

Synthetic data only; reuses the monthly-report and text-capture owner fixtures.
"""
from uuid import uuid4

import pytest

from test_kakeibo_completeness import feedback_rows, kakeibo, recent, stored
from test_monthly_report import as_user, insert, owners  # noqa: F401 - fixture
from test_text_capture import answered, captured, capture_rows, owner  # noqa: F401 - fixture
from test_notification_processing_database import enable_ai, notification_owner  # noqa: F401 - fixture

STARTER_PILLARS = {
    ("Makanan & Minuman", "expense"): "need",
    ("Jajan & Nongkrong", "expense"): "want",
    ("Belanja Harian", "expense"): "need",
    ("Transportasi", "expense"): "need",
    ("Tagihan & Utilitas", "expense"): "need",
    ("Belanja", "expense"): "want",
    ("Hiburan", "expense"): "want",
    ("Kesehatan", "expense"): "need",
    ("Investasi", "expense"): None,
    ("Internal Movement", "expense"): None,
    ("Internal Movement", "income"): None,
    ("Gaji", "income"): None,
    ("Pendapatan Lain", "income"): None,
    ("Patungan & Ganti Uang", "income"): None,
}


# --- Category pillar edits teach habit learning -------------------------------------------------


def test_category_edit_pillar_change_writes_feedback_like_lengkapi_klasifikasi(client, owners):
    owner, _ = owners
    first = insert(owner, "misc", 30_000, recent(), merchant="Toko Contoh")
    second = insert(owner, "misc", 20_000, recent())
    own_pillar = insert(owner, "misc", 10_000, recent(), pillar="want")

    edited = client.patch(f"/api/categories/{owner['misc']}", json={"name": "Lain-lain", "kakeibo_type": "need"})
    assert edited.status_code == 200, edited.text
    assert (edited.json()["category"]["kakeibo_type"], edited.json()["corrections"]) == ("need", 2)
    rows = feedback_rows(owner, [first, second, own_pillar])
    assert sorted((str(row["transaction_id"]), row["kind"], row["before_kakeibo_type"], row["after_kakeibo_type"]) for row in rows) == sorted(
        [(first, "correction", None, "need"), (second, "correction", None, "need")]
    )
    # As in the category-level quick fix, the rows follow the category and are not themselves edited.
    assert stored(first)["kakeibo_type"] is None and stored(first)["owner_edited_at"] is None
    assert stored(own_pillar)["kakeibo_type"] == "want"
    assert kakeibo(client)["unclassified_spent"] == 0

    changed = client.patch(f"/api/categories/{owner['misc']}", json={"kakeibo_type": "want"})
    assert changed.json()["corrections"] == 2
    after = {(row["before_kakeibo_type"], row["after_kakeibo_type"]) for row in feedback_rows(owner, [first, second])}
    assert after == {(None, "need"), ("need", "want")}


def test_category_edit_without_a_pillar_change_writes_no_feedback(client, owners):
    owner, _ = owners
    row = insert(owner, "groceries", 30_000, recent())
    same = client.patch(f"/api/categories/{owner['groceries']}", json={"kakeibo_type": "need"})
    assert (same.status_code, same.json()["corrections"]) == (200, 0)
    renamed = client.patch(f"/api/categories/{owner['groceries']}", json={"name": "Dapur", "monthly_budget": 500_000})
    assert (renamed.status_code, renamed.json()["corrections"]) == (200, 0)
    assert feedback_rows(owner, [row]) == []


def test_category_edit_is_owner_scoped_and_writes_nothing_for_another_owner(client, owners):
    owner, stranger = owners
    theirs = insert(stranger, "misc", 9_000, recent())
    response = client.patch(f"/api/categories/{stranger['misc']}", json={"kakeibo_type": "need"})
    assert response.status_code == 404
    assert feedback_rows(stranger, [theirs]) == []
    as_user(stranger)
    assert {row["name"]: row["kakeibo_type"] for row in client.get("/api/categories").json()["categories"]}["Lain-lain"] is None


def test_category_edit_of_a_non_reportable_category_writes_no_feedback(client, owners):
    owner, _ = owners
    row = insert(owner, "investment", 50_000, recent())
    edited = client.patch(f"/api/categories/{owner['investment']}", json={"kakeibo_type": "saving"})
    assert (edited.status_code, edited.json()["corrections"]) == (200, 0)
    assert feedback_rows(owner, [row]) == []


# --- No silent `need` ---------------------------------------------------------------------------


@pytest.mark.parametrize("is_primary", [True, False])
def test_category_created_without_a_pillar_stays_unclassified(client, owners, is_primary):
    created = client.post("/api/categories", json={"name": f"Hobi {uuid4().hex[:6]}", "kind": "expense", "is_primary": is_primary})
    assert created.status_code == 200, created.text
    assert created.json()["category"]["kakeibo_type"] is None
    listed = {row["id"]: row["kakeibo_type"] for row in client.get("/api/categories").json()["categories"]}
    assert listed[created.json()["category"]["id"]] is None


def test_category_created_with_a_pillar_keeps_it(client, owners):
    created = client.post("/api/categories", json={"name": "Kursus", "kind": "expense", "kakeibo_type": "culture"})
    assert created.status_code == 200, created.text
    assert created.json()["category"]["kakeibo_type"] == "culture"
    income = client.post("/api/categories", json={"name": "Bonus Contoh", "kind": "income", "kakeibo_type": "need"})
    assert income.status_code == 422


def pillarless_category(owner):
    from app.db.pool import db_conn

    category_id = str(uuid4())
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO categories (id, user_id, name, kind, kakeibo_type) VALUES (%s, %s, 'Lain-lain', 'expense', NULL)",
            (category_id, owner["user_id"]),
        )
        conn.commit()
    return category_id


def test_typed_capture_in_a_category_without_a_pillar_stores_no_pillar(client, owner):
    misc = pillarless_category(owner)
    capture = captured(client, "fotokopi 5rb tunai")
    assert capture["question"]["type"] == "category"
    options = {item["category_id"]: item["kakeibo"] for item in capture["question"]["options"]}
    assert options[misc] is None and options[owner["parkir"]] == "need"
    capture = answered(client, capture, category_id=misc)
    row = capture_rows(owner)[0]
    assert (capture["status"], str(row["category_id"]), row["kakeibo_type"]) == ("recorded", misc, None)
    assert capture["transaction"]["kakeibo"] is None


def test_typed_capture_keeps_a_pillar_the_owner_chose(client, owner):
    misc = pillarless_category(owner)
    capture = captured(client, "fotokopi 5rb tunai")
    capture = answered(client, capture, category_id=misc, kakeibo="culture")
    assert capture_rows(owner)[0]["kakeibo_type"] == "culture"


def test_capture_normalization_leaves_a_pillarless_category_unclassified():
    from app.services.capture_normalization import CaptureNormalization, validate_capture_normalization

    context = {"categories": [{"id": "c-none", "kind": "expense", "default_kakeibo": None}], "accounts": []}
    base = {"merchant": None, "description": "Fotokopi", "type": "expense", "type_confidence": 0.9,
            "category_id": "c-none", "kakeibo": None, "category_confidence": 0.9, "account_id": None,
            "account_confidence": 0, "time": {"span": None, "local_datetime": None, "confidence": 0}}
    assert validate_capture_normalization(CaptureNormalization.model_construct(**base), context)["kakeibo"] is None
    proposed = CaptureNormalization.model_construct(**{**base, "kakeibo": "want"})
    assert validate_capture_normalization(proposed, context)["kakeibo"] == "want"


# --- Starter categories -------------------------------------------------------------------------


def seeded_user():
    from app.db.pool import db_conn

    with db_conn() as conn:
        user = conn.execute(
            "INSERT INTO users (username, password_hash, invite_code) VALUES (%s, 'x', 'x') RETURNING id",
            (f"fictional_seed_{uuid4().hex[:10]}",),
        ).fetchone()["id"]
        conn.commit()
    return user


def categories_of(user):
    from app.db.pool import db_conn

    with db_conn() as conn:
        rows = conn.execute("SELECT name, kind, kakeibo_type FROM categories WHERE user_id = %s", (user,)).fetchall()
    return {(row["name"], row["kind"]): row["kakeibo_type"] for row in rows}


def delete_user(user):
    from app.db.pool import db_conn

    with db_conn() as conn:
        conn.execute("DELETE FROM users WHERE id = %s", (user,))
        conn.commit()


def test_new_user_starter_categories_carry_default_pillars(client, db_available):
    if not db_available:
        pytest.skip("Disposable database unavailable")
    from app.db.init_db import seed_user_defaults

    user = seeded_user()
    try:
        seed_user_defaults(user)
        assert categories_of(user) == STARTER_PILLARS
    finally:
        delete_user(user)


def test_existing_categories_keep_their_pillars_when_defaults_are_seeded_again(client, db_available):
    if not db_available:
        pytest.skip("Disposable database unavailable")
    from app.db.init_db import seed_user_defaults
    from app.db.pool import db_conn

    user = seeded_user()
    try:
        with db_conn() as conn:
            conn.execute(
                "INSERT INTO categories (user_id, name, kind, kakeibo_type) VALUES (%s, 'Makanan & Minuman', 'expense', NULL)",
                (user,),
            )
            conn.commit()
        seed_user_defaults(user)
        assert categories_of(user) == {("Makanan & Minuman", "expense"): None}
    finally:
        delete_user(user)
