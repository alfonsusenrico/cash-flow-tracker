"""GET and PUT /api/accounts/layout (organize-accounts-by-use D5), on fresh synthetic owners.

The owner shape is synthetic: a "BCA Contoh" rekening with an ATM pocket and a gold product, a "Bank Jago
Contoh" rekening with Main Pocket and Dana Darurat (tabungan), and a standalone e-wallet "E-Wallet Contoh".
"""
from uuid import uuid4

import pytest

from test_cycle_summary import make_owner, unlimited_rate_limits  # noqa: F401 - fixtures

LAYOUT = "/api/accounts/layout"
ROLES = ["harian", "tabungan", "dana_investasi", "investasi", "utang"]


def create(owner, key, name, **payload):
    response = owner.session.post("/api/accounts", json={"name": name, **payload})
    assert response.status_code == 200, response.text
    owner.ids[key] = response.json()["account"]["id"]
    return owner.ids[key]


@pytest.fixture
def owner(make_owner):
    """make_owner's own accounts are archived so the layout holds only this structure."""
    owner = make_owner()
    owner.db("UPDATE accounts SET is_archived = TRUE WHERE user_id = %s", (owner.user_id,))
    create(owner, "bca", "BCA Contoh", type="bank", display_order=0)
    create(owner, "atm", "ATM", type="bank", parent_id=owner["bca"], initial_balance=2_000_000, display_order=0)
    create(owner, "gold", "Tabungan Emas", type="investment", instrument_type="gold", parent_id=owner["bca"],
           initial_balance=1_000_000, display_order=1)
    create(owner, "jago", "Bank Jago Contoh", type="bank", display_order=1)
    create(owner, "main", "Main Pocket", type="bank", parent_id=owner["jago"], initial_balance=500_000, display_order=0)
    create(owner, "darurat", "Dana Darurat", type="bank", parent_id=owner["jago"], role="tabungan", display_order=1)
    create(owner, "wallet", "E-Wallet Contoh", type="wallet", display_order=2)
    return owner


def get(owner, grouping):
    response = owner.session.get(LAYOUT, params={"grouping": grouping})
    assert response.status_code == 200, response.text
    return response.json()


def put(owner, grouping, body):
    return owner.session.put(LAYOUT, params={"grouping": grouping}, json=body)


def test_default_layout_per_kegunaan(owner):
    layout = get(owner, "use")
    assert layout["grouping"] == "use" and layout["sections"] == ROLES
    assert layout["cards"] == {
        "harian": [owner["atm"], owner["main"], owner["wallet"]],
        "tabungan": [owner["darurat"]],
        "dana_investasi": [],
        "investasi": [owner["gold"]],
        "utang": [],
    }


def test_default_layout_per_rekening(owner):
    layout = get(owner, "rekening")
    assert layout["sections"] == [owner["bca"], owner["jago"], owner["wallet"]]
    assert layout["cards"] == {
        owner["bca"]: [owner["atm"], owner["gold"]],
        owner["jago"]: [owner["main"], owner["darurat"]],
        owner["wallet"]: [owner["wallet"]],
    }


def test_a_parent_with_its_own_balance_is_a_card(owner):
    owner.db("UPDATE accounts SET initial_balance = 300000 WHERE id = %s", (owner["jago"],))
    assert get(owner, "rekening")["cards"][owner["jago"]] == [owner["jago"], owner["main"], owner["darurat"]]
    assert owner["jago"] in get(owner, "use")["cards"]["harian"]


def test_reorder_and_reload_keeps_each_grouping_apart(owner):
    body = {
        "sections": ["investasi", "harian", "tabungan", "dana_investasi", "utang"],
        "cards": {"harian": [owner["main"], owner["atm"], owner["wallet"]]},
    }
    saved = put(owner, "use", body)
    assert saved.status_code == 200, saved.text
    expected = {
        "grouping": "use",
        "sections": body["sections"],
        "cards": {
            "investasi": [owner["gold"]], "harian": [owner["main"], owner["atm"], owner["wallet"]],
            "tabungan": [owner["darurat"]], "dana_investasi": [], "utang": [],
        },
    }
    assert saved.json() == expected
    assert get(owner, "use") == expected
    assert get(owner, "rekening")["sections"] == [owner["bca"], owner["jago"], owner["wallet"]]


def test_put_is_idempotent(owner):
    body = {"sections": [owner["jago"], owner["bca"]], "cards": {owner["jago"]: [owner["darurat"], owner["main"]]}}
    first = put(owner, "rekening", body).json()
    second = put(owner, "rekening", first)
    assert second.status_code == 200 and second.json() == first
    assert first["sections"] == [owner["jago"], owner["bca"], owner["wallet"]]
    assert first["cards"][owner["jago"]] == [owner["darurat"], owner["main"]]
    from app.db.pool import db_conn
    with db_conn() as conn:
        count = conn.execute("SELECT COUNT(*) AS n FROM account_layouts WHERE user_id = %s", (owner.user_id,)).fetchone()["n"]
    assert count == 1


def test_unknown_foreign_archived_and_misplaced_ids_are_dropped(owner, make_owner):
    stranger = make_owner()
    foreign = stranger["bank"]
    create(owner, "old", "Kantong Lama", type="bank", parent_id=owner["jago"])
    owner.session.patch(f"/api/accounts/{owner['old']}", json={"is_archived": True})
    body = {
        "sections": ["rahasia", "tabungan", foreign, "harian"],
        "cards": {
            "harian": [str(uuid4()), foreign, owner["old"], owner["darurat"], owner["wallet"], owner["wallet"]],
            "rahasia": [owner["atm"]],
            foreign: [foreign],
        },
    }
    response = put(owner, "use", body)
    assert response.status_code == 200, response.text
    layout = response.json()
    assert layout["sections"] == ["tabungan", "harian", "dana_investasi", "investasi", "utang"]
    # The misplaced Dana Darurat goes to its own section; missing cards follow in default order.
    assert layout["cards"]["harian"] == [owner["wallet"], owner["atm"], owner["main"]]
    assert layout["cards"]["tabungan"] == [owner["darurat"]]
    every_id = set(layout["sections"]) | set(layout["cards"]) | {i for ids in layout["cards"].values() for i in ids}
    assert foreign not in every_id and owner["old"] not in every_id
    from app.db.pool import db_conn
    with db_conn() as conn:
        stored = conn.execute("SELECT sections, cards FROM account_layouts WHERE user_id = %s", (owner.user_id,)).fetchone()
    assert foreign not in str(stored) and owner["old"] not in str(stored)


def test_another_owner_never_sees_the_layout(owner, make_owner):
    put(owner, "use", {"sections": ["utang"], "cards": {}})
    stranger = make_owner()
    layout = get(stranger, "use")
    assert layout["sections"] == ROLES
    owned = {i for ids in layout["cards"].values() for i in ids}
    assert owned.isdisjoint({owner["atm"], owner["main"], owner["wallet"], owner["darurat"], owner["gold"]})


def test_new_and_changed_accounts_follow_the_saved_order(owner):
    put(owner, "use", {"sections": ROLES, "cards": {"harian": [owner["wallet"], owner["main"], owner["atm"]]}})
    create(owner, "new", "Kantong Baru", type="bank", parent_id=owner["bca"])
    owner.session.patch(f"/api/accounts/{owner['main']}", json={"role": "tabungan"})
    cards = get(owner, "use")["cards"]
    assert cards["harian"] == [owner["wallet"], owner["atm"], owner["new"]]
    assert cards["tabungan"] == [owner["darurat"], owner["main"]]


@pytest.mark.parametrize("params, body", [
    ({}, {"sections": [], "cards": {}}),
    ({"grouping": "tabel"}, {"sections": [], "cards": {}}),
    ({"grouping": "use"}, {"sections": "harian", "cards": {}}),
    ({"grouping": "use"}, {"sections": [], "cards": ["harian"]}),
    ({"grouping": "use"}, {"sections": [1, 2], "cards": {}}),
    ({"grouping": "use"}, {"cards": {}}),
    ({"grouping": "use"}, {"sections": [], "cards": {"harian": [str(uuid4()) for _ in range(501)]}}),
    ({"grouping": "use"}, {"sections": ["x" * 65], "cards": {}}),
    ({"grouping": "use"}, {"grouping": "rekening", "sections": [], "cards": {}}),
])
def test_malformed_bodies_are_refused(owner, params, body):
    response = owner.session.put(LAYOUT, params=params, json=body)
    assert response.status_code == 422, response.text


def test_get_needs_a_known_grouping(owner):
    assert owner.session.get(LAYOUT).status_code == 422
    assert owner.session.get(LAYOUT, params={"grouping": "tabel"}).status_code == 422
