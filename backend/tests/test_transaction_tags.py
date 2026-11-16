"""Transaction tags: the tables, the tag API, tags on transactions and the ledger tag filter (transaction-tags 1.1-1.3).

All names, amounts and owners are synthetic.
"""
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from signed_in import signed_in

JAKARTA = timezone(timedelta(hours=7))


def db():
    from app.db.pool import db_conn
    return db_conn()


def query(statement, params=()):
    with db() as conn:
        return conn.execute(statement, params).fetchall()


def insert_user(cur, name):
    user_id = str(uuid4())
    cur.execute(
        "INSERT INTO users (id, username, name, password_hash, invite_code, language) VALUES (%s, %s, %s, 'synthetic-hash', 'TEST', 'id')",
        (user_id, f"tags_{uuid4().hex}", name),
    )
    return user_id


@pytest.fixture
def owner(client):
    from app.main import app
    from app.services.auth import get_current_user

    ids = {}
    with db() as conn:
        with conn.cursor() as cur:
            ids["user_id"] = insert_user(cur, "Sari Contoh")
            for key, name, kind in [("cash", "Kas", "cash"), ("bank", "Bank Contoh", "bank"), ("fund", "Reksa Contoh", "investment")]:
                cur.execute(
                    "INSERT INTO accounts (user_id, name, type, initial_balance) VALUES (%s, %s, %s, 10000000) RETURNING id",
                    (ids["user_id"], name, kind),
                )
                ids[key] = str(cur.fetchone()["id"])
            for key, name, kind in [("food", "Makanan & Minuman", "expense"), ("transport", "Transportasi", "expense"), ("salary", "Gaji Contoh", "income")]:
                cur.execute(
                    "INSERT INTO categories (user_id, name, kind) VALUES (%s, %s, %s) RETURNING id",
                    (ids["user_id"], name, kind),
                )
                ids[key] = str(cur.fetchone()["id"])
            ids["stranger"] = insert_user(cur, "Orang Lain")
            cur.execute(
                "INSERT INTO tags (user_id, name, name_normalized) VALUES (%s, 'Rahasia Lain', 'rahasia lain') RETURNING id",
                (ids["stranger"],),
            )
            ids["stranger_tag"] = str(cur.fetchone()["id"])
        conn.commit()
    app.dependency_overrides[get_current_user] = signed_in({"id": ids["user_id"], "name": "Sari Contoh", "currency": "IDR"})
    yield ids
    app.dependency_overrides.clear()
    with db() as conn:
        conn.execute("DELETE FROM users WHERE id = ANY(%s::uuid[])", ([ids["user_id"], ids["stranger"]],))
        conn.commit()


def record(client, owner, *, amount=25_000, category="food", kind="expense", day=10, expect=200, **extra):
    body = {
        "type": kind, "account_id": owner["cash"], "category_id": owner[category], "amount": amount,
        "name": "Makan Contoh", "date": datetime(2026, 10, day, 12, tzinfo=JAKARTA).isoformat(), **extra,
    }
    response = client.post("/api/transactions", json=body)
    assert response.status_code == expect, response.text
    return response.json()["transaction_id"] if expect == 200 else response.json()["detail"]


def edit(client, transaction_id, expect=200, **body):
    response = client.patch(f"/api/transactions/{transaction_id}", json=body)
    assert response.status_code == expect, response.text
    return response.json()


def tags_on(client, transaction_id):
    response = client.get(f"/api/transactions/{transaction_id}")
    assert response.status_code == 200, response.text
    return response.json()["transaction"]["tags"]


def names_on(client, transaction_id):
    return [tag["name"] for tag in tags_on(client, transaction_id)]


def tag_list(client, include_archived=False):
    response = client.get("/api/tags", params={"include_archived": int(include_archived)})
    assert response.status_code == 200, response.text
    return response.json()["tags"]


def new_tag(client, name):
    response = client.post("/api/tags", json={"name": name})
    assert response.status_code == 200, response.text
    return response.json()["tag"]


def owner_tags(owner):
    return query("SELECT id, name, name_normalized, archived_at FROM tags WHERE user_id = %s ORDER BY name", (owner["user_id"],))


def owner_transaction_count(owner):
    return query("SELECT COUNT(*) AS n FROM transactions WHERE user_id = %s", (owner["user_id"],))[0]["n"]


# --- Migration V58 ----------------------------------------------------------------------------------------------


def test_links_cascade_with_the_transaction_the_tag_and_the_owner(client, owner):
    first = record(client, owner, new_tags=["Pertama Contoh", "Kedua Contoh"])
    second = record(client, owner, new_tags=["Pertama Contoh"])
    tags = {tag["name"]: tag["id"] for tag in tag_list(client)}

    assert client.delete(f"/api/transactions/{first}").status_code == 200
    assert query("SELECT COUNT(*) AS n FROM transaction_tags WHERE transaction_id = %s", (first,))[0]["n"] == 0

    with db() as conn:
        conn.execute("DELETE FROM tags WHERE id = %s", (tags["Pertama Contoh"],))
        conn.commit()
    assert tags_on(client, second) == []

    with db() as conn:
        conn.execute("DELETE FROM users WHERE id = %s", (owner["user_id"],))
        conn.commit()
    assert query("SELECT COUNT(*) AS n FROM tags WHERE user_id = %s", (owner["user_id"],))[0]["n"] == 0


def test_normalized_names_are_unique_per_owner(client, owner):
    import psycopg

    with db() as conn:
        conn.execute("INSERT INTO tags (user_id, name, name_normalized) VALUES (%s, 'Rahasia Lain', 'rahasia lain')", (owner["user_id"],))
        conn.commit()
    with pytest.raises(psycopg.errors.UniqueViolation):
        with db() as conn:
            conn.execute("INSERT INTO tags (user_id, name, name_normalized) VALUES (%s, 'RAHASIA LAIN', 'rahasia lain')", (owner["user_id"],))


# --- Owner-Created Tags -----------------------------------------------------------------------------------------


def test_creating_a_tag_while_recording(client, owner):
    transaction_id = record(client, owner, name="Bayar makan di Kafe Contoh", new_tags=["fellowship coach"])

    [tag] = tags_on(client, transaction_id)
    assert tag["name"] == "fellowship coach" and set(tag) == {"id", "name"}
    assert tag_list(client) == [{"id": tag["id"], "name": "fellowship coach", "archived": False, "transaction_count": 1}]
    stored = owner_tags(owner)
    assert [(row["name"], row["name_normalized"]) for row in stored] == [("fellowship coach", "fellowship coach")]


def test_a_differently_spelled_duplicate_reuses_the_tag(client, owner):
    first = record(client, owner, new_tags=["fellowship coach"])
    second = record(client, owner, new_tags=["  Fellowship  Coach "])

    assert tags_on(client, first) == tags_on(client, second)
    assert len(owner_tags(owner)) == 1
    assert names_on(client, second) == ["fellowship coach"]


def test_names_are_trimmed_and_repeats_in_one_request_count_once(client, owner):
    transaction_id = record(client, owner, new_tags=["  Malang   trip ", "malang trip", "MALANG TRIP"])
    assert names_on(client, transaction_id) == ["Malang trip"]


def test_another_owners_tag_is_refused_and_nothing_is_written(client, owner):
    before = owner_transaction_count(owner)
    detail = record(client, owner, tag_ids=[owner["stranger_tag"]], new_tags=["Tag Baru Contoh"], expect=404)

    assert detail["code"] == "tag_not_found"
    assert "Rahasia" not in str(detail)
    assert owner_transaction_count(owner) == before
    assert owner_tags(owner) == []


def test_another_owners_tag_is_refused_on_edit_and_the_row_keeps_its_tags(client, owner):
    transaction_id = record(client, owner, new_tags=["Milik Sendiri"])
    detail = edit(client, transaction_id, expect=404, tag_ids=[owner["stranger_tag"]], new_tags=["Lain Contoh"])["detail"]

    assert detail["code"] == "tag_not_found"
    assert names_on(client, transaction_id) == ["Milik Sendiri"]
    assert [row["name"] for row in owner_tags(owner)] == ["Milik Sendiri"]


def test_another_owners_tags_are_never_listed_changed_or_merged(client, owner):
    mine = new_tag(client, "Milik Sendiri")
    assert [tag["id"] for tag in tag_list(client, include_archived=True)] == [mine["id"]]

    renamed = client.patch(f"/api/tags/{owner['stranger_tag']}", json={"name": "Diambil"})
    merged_from = client.post(f"/api/tags/{owner['stranger_tag']}/merge", json={"into_id": mine["id"]})
    merged_into = client.post(f"/api/tags/{mine['id']}/merge", json={"into_id": owner["stranger_tag"]})
    for response in (renamed, merged_from, merged_into):
        assert response.status_code == 404 and response.json()["detail"]["code"] == "tag_not_found"
    assert query("SELECT name FROM tags WHERE id = %s", (owner["stranger_tag"],))[0]["name"] == "Rahasia Lain"
    assert query("SELECT COUNT(*) AS n FROM tags WHERE id = %s", (mine["id"],))[0]["n"] == 1


def test_an_unknown_tag_id_and_a_foreign_filter_reveal_nothing(client, owner):
    assert record(client, owner, tag_ids=[str(uuid4())], expect=404)["code"] == "tag_not_found"
    listed = client.get("/api/transactions", params={"tag": owner["stranger_tag"]}).json()
    assert listed["total"] == 0 and listed["transactions"] == []


# --- Limits -----------------------------------------------------------------------------------------------------


def test_at_most_ten_tags_per_transaction(client, owner):
    ten = [f"Tag {number}" for number in range(10)]
    transaction_id = record(client, owner, new_tags=ten)
    assert len(tags_on(client, transaction_id)) == 10

    detail = record(client, owner, new_tags=[*ten, "Tag 10"], expect=422)
    assert (detail["code"], detail["params"]) == ("tag_limit", {"limit": 10})

    existing = [tag["id"] for tag in tag_list(client)]
    detail = record(client, owner, tag_ids=existing[:6], new_tags=["Lain 1", "Lain 2", "Lain 3", "Lain 4", "Lain 5"], expect=422)
    assert detail["code"] == "tag_limit"
    assert len(owner_tags(owner)) == 10

    detail = edit(client, transaction_id, expect=422, tag_ids=existing, new_tags=["Lain 1"])["detail"]
    assert detail["code"] == "tag_limit"
    assert len(tags_on(client, transaction_id)) == 10 and len(owner_tags(owner)) == 10


def test_a_new_name_equal_to_a_given_tag_counts_once_toward_the_limit(client, owner):
    ten = [f"Tag {number}" for number in range(10)]
    record(client, owner, new_tags=ten)
    existing = [tag["id"] for tag in tag_list(client)]

    transaction_id = record(client, owner, tag_ids=existing, new_tags=["TAG 3"])
    assert len(tags_on(client, transaction_id)) == 10


@pytest.mark.parametrize("name", ["", "   ", "x" * 41])
def test_tag_names_must_be_one_to_forty_characters(client, owner, name):
    detail = record(client, owner, new_tags=[name], expect=422)
    assert (detail["code"], detail["params"]) == ("tag_name_invalid", {"max_length": 40})

    created = client.post("/api/tags", json={"name": name})
    assert created.status_code == 422 and created.json()["detail"]["code"] == "tag_name_invalid"
    assert owner_tags(owner) == []


def test_a_forty_character_name_is_accepted(client, owner):
    name = "x" * 40
    assert new_tag(client, f"  {name}  ")["name"] == name


# --- Update semantics -------------------------------------------------------------------------------------------


def test_an_edit_without_tag_fields_keeps_the_tags(client, owner):
    transaction_id = record(client, owner, new_tags=["Tetap Contoh"])
    edit(client, transaction_id, name="Makan malam Contoh", amount=30_000)
    assert names_on(client, transaction_id) == ["Tetap Contoh"]


def test_an_edit_with_tag_fields_sets_exactly_that_set(client, owner):
    transaction_id = record(client, owner, new_tags=["Lama Contoh", "Tetap Contoh"])
    kept = {tag["name"]: tag["id"] for tag in tags_on(client, transaction_id)}["Tetap Contoh"]

    edit(client, transaction_id, tag_ids=[kept], new_tags=["Baru Contoh"])
    assert names_on(client, transaction_id) == ["Baru Contoh", "Tetap Contoh"]

    edit(client, transaction_id, new_tags=["Hanya Ini"])
    assert names_on(client, transaction_id) == ["Hanya Ini"]

    edit(client, transaction_id, tag_ids=[kept], new_tags=None)
    assert names_on(client, transaction_id) == ["Tetap Contoh"]

    edit(client, transaction_id, tag_ids=[])
    assert tags_on(client, transaction_id) == []
    assert len(owner_tags(owner)) == 4


def test_rows_list_their_tags_ordered_by_name(client, owner):
    transaction_id = record(client, owner, new_tags=["zebra", "Apel", "mangga"])
    untagged = record(client, owner, day=11)

    rows = {row["id"]: row for row in client.get("/api/transactions").json()["transactions"]}
    assert [tag["name"] for tag in rows[transaction_id]["tags"]] == ["Apel", "mangga", "zebra"]
    assert rows[transaction_id]["tags"] == tags_on(client, transaction_id)
    assert rows[untagged]["tags"] == []


# --- Movements --------------------------------------------------------------------------------------------------


def test_movement_rows_take_no_tags(client, owner):
    tag = new_tag(client, "Acara Contoh")
    trade = client.post("/api/transactions", json={
        "type": "expense", "account_id": owner["cash"], "target_account_id": owner["fund"], "amount": 100_000,
        "investment_action": "buy", "units": 10, "price_per_unit": 10_000, "tag_ids": [tag["id"]],
    })
    assert trade.status_code == 422 and trade.json()["detail"]["code"] == "trade_fields_not_allowed"

    moved = client.post("/api/movements", json={"source_account_id": owner["cash"], "target_account_id": owner["bank"], "amount": 40_000})
    assert moved.status_code == 200, moved.text
    leg = query("SELECT id FROM transactions WHERE user_id = %s AND movement_id IS NOT NULL LIMIT 1", (owner["user_id"],))[0]["id"]
    detail = edit(client, leg, expect=409, tag_ids=[tag["id"]])["detail"]
    assert detail["code"] == "linked_movement_endpoint"
    assert query("SELECT COUNT(*) AS n FROM transaction_tags WHERE user_id = %s", (owner["user_id"],))[0]["n"] == 0


# --- Managing Tags ----------------------------------------------------------------------------------------------


def test_the_tag_list_puts_active_tags_first_then_names_case_insensitively(client, owner):
    for name in ("beta", "Alpha", "gamma", "Delta"):
        new_tag(client, name)
    gamma = next(tag for tag in tag_list(client) if tag["name"] == "gamma")
    client.patch(f"/api/tags/{gamma['id']}", json={"archived": True})
    archived_alpha = new_tag(client, "aardvark")
    client.patch(f"/api/tags/{archived_alpha['id']}", json={"archived": True})

    assert [tag["name"] for tag in tag_list(client)] == ["Alpha", "beta", "Delta"]
    listed = tag_list(client, include_archived=True)
    assert [(tag["name"], tag["archived"]) for tag in listed] == [
        ("Alpha", False), ("beta", False), ("Delta", False), ("aardvark", True), ("gamma", True),
    ]


def test_creating_an_existing_name_returns_that_tag_and_restores_it(client, owner):
    created = new_tag(client, "Fellowship Coach")
    assert new_tag(client, "fellowship   coach") == created

    client.patch(f"/api/tags/{created['id']}", json={"archived": True})
    restored = new_tag(client, "FELLOWSHIP COACH")
    assert restored == {**created, "archived": False}
    assert len(owner_tags(owner)) == 1


def test_renaming_a_tag(client, owner):
    tag = new_tag(client, "felowship coach")
    other = new_tag(client, "Malang trip")
    transaction_id = record(client, owner, tag_ids=[tag["id"]])

    renamed = client.patch(f"/api/tags/{tag['id']}", json={"name": "Fellowship Coach"})
    assert renamed.status_code == 200, renamed.text
    assert renamed.json()["tag"] == {"id": tag["id"], "name": "Fellowship Coach", "archived": False, "transaction_count": 1}
    assert names_on(client, transaction_id) == ["Fellowship Coach"]

    recased = client.patch(f"/api/tags/{tag['id']}", json={"name": "fellowship coach"})
    assert recased.status_code == 200 and recased.json()["tag"]["name"] == "fellowship coach"

    taken = client.patch(f"/api/tags/{tag['id']}", json={"name": " malang  TRIP"})
    assert taken.status_code == 409 and taken.json()["detail"]["code"] == "tag_name_taken"
    assert {row["name"] for row in owner_tags(owner)} == {"fellowship coach", other["name"]}

    empty = client.patch(f"/api/tags/{tag['id']}", json={})
    assert empty.status_code == 422 and empty.json()["detail"]["code"] == "no_fields_to_update"


def test_merging_a_typo(client, owner):
    kept = new_tag(client, "fellowship coach")
    typo = new_tag(client, "felowship coach")
    for day in range(1, 5):
        record(client, owner, day=day, tag_ids=[kept["id"]])
    typo_row = record(client, owner, day=5, tag_ids=[typo["id"]])

    merged = client.post(f"/api/tags/{typo['id']}/merge", json={"into_id": kept["id"]})
    assert merged.status_code == 200, merged.text
    assert merged.json()["tag"] == {"id": kept["id"], "name": "fellowship coach", "archived": False, "transaction_count": 5}
    assert [tag["name"] for tag in tag_list(client, include_archived=True)] == ["fellowship coach"]
    assert names_on(client, typo_row) == ["fellowship coach"]


def test_merging_overlapping_links_leaves_one_link_per_row(client, owner):
    kept = new_tag(client, "Acara Contoh")
    other = new_tag(client, "acara contoh 2")
    both = record(client, owner, tag_ids=[kept["id"], other["id"]])
    only_other = record(client, owner, tag_ids=[other["id"]])

    merged = client.post(f"/api/tags/{other['id']}/merge", json={"into_id": kept["id"]})
    assert merged.status_code == 200, merged.text
    assert merged.json()["tag"]["transaction_count"] == 2
    assert tags_on(client, both) == [{"id": kept["id"], "name": "Acara Contoh"}]
    assert tags_on(client, only_other) == [{"id": kept["id"], "name": "Acara Contoh"}]


def test_merging_a_tag_into_itself_is_refused(client, owner):
    tag = new_tag(client, "Acara Contoh")
    refused = client.post(f"/api/tags/{tag['id']}/merge", json={"into_id": tag["id"]})
    assert refused.status_code == 422 and refused.json()["detail"]["code"] == "tag_merge_same"
    assert len(owner_tags(owner)) == 1


def test_an_archived_tag_stays_on_its_rows_and_in_the_filter_but_is_not_attached_anew(client, owner):
    tag = new_tag(client, "Selesai Contoh")
    transaction_id = record(client, owner, tag_ids=[tag["id"]], amount=11_000)
    archived = client.patch(f"/api/tags/{tag['id']}", json={"archived": True})
    assert archived.status_code == 200 and archived.json()["tag"]["archived"] is True

    assert names_on(client, transaction_id) == ["Selesai Contoh"]
    filtered = client.get("/api/transactions", params={"tag": tag["id"]}).json()
    assert [row["id"] for row in filtered["transactions"]] == [transaction_id]

    assert record(client, owner, tag_ids=[tag["id"]], expect=404)["code"] == "tag_not_found"
    other = record(client, owner, day=12)
    assert edit(client, other, expect=404, tag_ids=[tag["id"]])["detail"]["code"] == "tag_not_found"

    extra = new_tag(client, "Tambahan Contoh")
    edit(client, transaction_id, tag_ids=[tag["id"], extra["id"]])
    assert names_on(client, transaction_id) == ["Selesai Contoh", "Tambahan Contoh"]

    restored = client.patch(f"/api/tags/{tag['id']}", json={"archived": False})
    assert restored.json()["tag"]["archived"] is False
    assert record(client, owner, tag_ids=[tag["id"]])


def test_a_new_name_matching_an_archived_tag_restores_and_attaches_it(client, owner):
    tag = new_tag(client, "Malang trip")
    client.patch(f"/api/tags/{tag['id']}", json={"archived": True})

    transaction_id = record(client, owner, new_tags=["malang trip"])
    assert tags_on(client, transaction_id) == [{"id": tag["id"], "name": "Malang trip"}]
    assert tag_list(client)[0]["archived"] is False


# --- Filtering the Ledger by Tag --------------------------------------------------------------------------------


def test_what_an_event_cost(client, owner):
    event = new_tag(client, "fellowship coach")
    other = new_tag(client, "Malang trip")
    tagged = [
        record(client, owner, amount=amount, category=category, day=day, tag_ids=[event["id"]])
        for amount, category, day in [
            (85_000, "food", 3), (42_000, "transport", 3), (120_000, "food", 4), (18_000, "transport", 5), (60_000, "food", 6),
        ]
    ]
    record(client, owner, amount=500_000, category="food", day=4)
    record(client, owner, amount=75_000, category="transport", day=5, tag_ids=[other["id"]])
    record(client, owner, amount=3_000_000, category="salary", kind="income", day=1)

    listed = client.get("/api/transactions", params={"tag": event["id"], "include": "daily_totals,type_counts"})
    assert listed.status_code == 200, listed.text
    body = listed.json()
    assert sorted(row["id"] for row in body["transactions"]) == sorted(tagged)
    assert body["total"] == 5
    cumulative = body["summary"]["cumulative"]
    assert (cumulative["outflow"], cumulative["gross_outflow"], cumulative["inflow"]) == (325_000, 325_000, 0)
    assert cumulative["spending"] == 325_000
    assert sum(day["outflow"] for day in body["daily_totals"]) == 325_000
    assert body["type_counts"]["all"] == 5 and body["type_counts"]["expense"] == 5

    either = client.get("/api/transactions", params=[("tag", event["id"]), ("tag", other["id"])]).json()
    assert either["total"] == 6
    assert either["summary"]["cumulative"]["outflow"] == 400_000

    combined = client.get("/api/transactions", params={"tag": event["id"], "category_id": owner["transport"]}).json()
    assert combined["total"] == 2 and combined["summary"]["cumulative"]["outflow"] == 60_000


def test_a_row_with_several_matching_tags_is_listed_once(client, owner):
    first = new_tag(client, "Satu Contoh")
    second = new_tag(client, "Dua Contoh")
    record(client, owner, amount=10_000, tag_ids=[first["id"], second["id"]])

    body = client.get("/api/transactions", params=[("tag", first["id"]), ("tag", second["id"])]).json()
    assert body["total"] == 1 and len(body["transactions"]) == 1
    assert body["summary"]["cumulative"]["outflow"] == 10_000
