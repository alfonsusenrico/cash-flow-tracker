"""Account language and region, the coded auth errors and starter data by language (add-language-system).

Tasks 2.2, 2.3, 2.4 and 2.5. Every account, name and amount is synthetic.
"""
from __future__ import annotations

from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

PASSWORD = "synthetic-pass-1"


class OpenLimiter:
    """Never limits, so these registrations neither hit nor use up the shared per-client budget."""

    def exceeded(self, *_args) -> bool:
        return False

    def reset(self, *_keys) -> None:
        return None


class ClosedLimiter(OpenLimiter):
    def exceeded(self, *_args) -> bool:
        return True


@pytest.fixture(autouse=True)
def open_limiter(monkeypatch):
    import app.routers.auth as auth_router

    monkeypatch.setattr(auth_router, "rate_limiter", OpenLimiter())


@pytest.fixture
def browser(client):
    """A fresh browser with its own session cookie, so the shared session client keeps its user."""
    from app.main import app

    created: list[TestClient] = []

    def make() -> TestClient:
        fresh = TestClient(app, base_url="https://testserver")
        fresh.headers["Origin"] = "https://testserver"
        created.append(fresh)
        return fresh

    yield make
    from app.db.pool import db_conn

    names = [getattr(each, "_registered", None) for each in created]
    with db_conn() as conn:
        conn.execute("DELETE FROM users WHERE username = ANY(%s)", ([name for name in names if name],))
        conn.commit()


def register(session: TestClient, expect: int = 200, headers: dict | None = None, **fields):
    username = fields.pop("username", f"lang_{uuid4().hex[:10]}")
    body = {"username": username, "password": PASSWORD, "invite_code": "TESTCODE", **fields}
    response = session.post("/api/auth/register", json=body, headers=headers or {})
    assert response.status_code == expect, response.text
    if expect == 200:
        session._registered = username
    return response.json()


def stored(username: str) -> dict | None:
    from app.db.pool import db_conn

    with db_conn() as conn:
        return conn.execute("SELECT language, region FROM users WHERE username = %s", (username,)).fetchone()


# --- Language and region on the profile endpoints (task 2.2) -------------------------------------------------


def test_register_login_and_me_return_language_and_region(browser):
    session = browser()
    registered = register(session, language="en", region="ID")["user"]
    assert (registered["language"], registered["region"]) == ("en", "ID")
    me = session.get("/api/auth/me").json()["user"]
    assert (me["language"], me["region"]) == ("en", "ID")
    other = browser()
    logged_in = other.post("/api/auth/login", json={"username": session._registered, "password": PASSWORD})
    assert logged_in.status_code == 200, logged_in.text
    assert (logged_in.json()["user"]["language"], logged_in.json()["user"]["region"]) == ("en", "ID")


def test_register_takes_region_united_states(browser):
    session = browser()
    register(session, language="id", region="US")
    assert dict(stored(session._registered)) == {"language": "id", "region": "US"}


def test_settings_save_language_and_region(browser):
    session = browser()
    register(session, language="id")
    response = session.patch("/api/auth/settings", json={"language": "en", "region": "US"})
    assert response.status_code == 200, response.text
    assert (response.json()["user"]["language"], response.json()["user"]["region"]) == ("en", "US")
    me = session.get("/api/auth/me").json()["user"]
    assert (me["language"], me["region"]) == ("en", "US")


@pytest.mark.parametrize("body,code", [({"language": "fr"}, "invalid_language"), ({"region": "SG"}, "invalid_region")])
def test_settings_reject_unsupported_language_or_region_and_save_nothing(browser, body, code):
    session = browser()
    register(session, language="id", region="ID")
    response = session.patch("/api/auth/settings", json={**body, "emergency_fund_multiplier": 9})
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == code
    assert response.json()["detail"]["params"] == {}
    assert dict(stored(session._registered)) == {"language": "id", "region": "ID"}
    me = session.get("/api/auth/me").json()["user"]
    assert me["emergency_fund_multiplier"] == 6


def test_saving_a_language_leaves_other_accounts_untouched(browser):
    first, second = browser(), browser()
    register(first, language="id")
    register(second, language="id", region="US")
    assert first.patch("/api/auth/settings", json={"language": "en"}).status_code == 200
    assert dict(stored(first._registered)) == {"language": "en", "region": "ID"}
    assert dict(stored(second._registered)) == {"language": "id", "region": "US"}


def test_a_shared_language_cookie_never_overrides_the_account(browser):
    english, indonesian = browser(), browser()
    register(english, language="en")
    register(indonesian, language="id")
    shared = browser()
    shared.cookies.set("cft_language", "en")
    response = shared.post("/api/auth/login", json={"username": indonesian._registered, "password": PASSWORD})
    assert response.json()["user"]["language"] == "id"
    assert shared.get("/api/auth/me").json()["user"]["language"] == "id"
    shared.post("/api/auth/logout")
    response = shared.post("/api/auth/login", json={"username": english._registered, "password": PASSWORD})
    assert response.json()["user"]["language"] == "en"
    shared.cookies.set("cft_language", "id")
    assert shared.get("/api/auth/me").json()["user"]["language"] == "en"


# --- Registration falls back to Accept-Language (task 2.3) ---------------------------------------------------


@pytest.mark.parametrize(
    "header,expected",
    [("id-ID", "id"), ("id-ID,id;q=0.9,en;q=0.8", "id"), ("en-US,id;q=0.5", "en"), ("ms, fr", "en"), (None, "en")],
)
def test_register_without_language_uses_accept_language(browser, header, expected):
    session = browser()
    registered = register(session, headers={"Accept-Language": header} if header else None)["user"]
    assert (registered["language"], registered["region"]) == (expected, "ID")


def test_register_language_in_the_request_wins_over_the_header(browser):
    session = browser()
    registered = register(session, language="en", headers={"Accept-Language": "id-ID"})["user"]
    assert registered["language"] == "en"


# --- Coded errors on sign-in, sign-up and settings (task 2.4) ------------------------------------------------


def assert_coded(response, status: int, code: str, message: str, params: dict | None = None):
    assert response.status_code == status, response.text
    assert response.json()["detail"] == {"code": code, "params": params or {}, "message": message}


def test_error_too_many_attempts(browser, monkeypatch):
    import app.routers.auth as auth_router

    monkeypatch.setattr(auth_router, "rate_limiter", ClosedLimiter())
    session = browser()
    login = session.post("/api/auth/login", json={"username": "nobody", "password": "whatever-1"})
    assert_coded(login, 429, "too_many_attempts", "Too many attempts. Please try again later.")
    assert login.headers["retry-after"]
    signup = session.post("/api/auth/register", json={"username": "x", "password": PASSWORD, "invite_code": "TESTCODE"})
    assert_coded(signup, 429, "too_many_attempts", "Too many attempts. Please try again later.")


def test_error_invalid_credentials(browser):
    response = browser().post("/api/auth/login", json={"username": f"none_{uuid4().hex[:8]}", "password": "wrong-pass"})
    assert_coded(response, 401, "invalid_credentials", "Invalid username or password")


def test_error_username_required(browser):
    response = browser().post("/api/auth/register", json={"username": "   ", "password": PASSWORD, "invite_code": "TESTCODE"})
    assert_coded(response, 400, "username_required", "Username is required")


def test_error_password_too_short(browser):
    from app.core.config import settings

    short = "a" * (settings.password_min_len - 1)
    response = browser().post("/api/auth/register", json={"username": f"short_{uuid4().hex[:8]}", "password": short, "invite_code": "TESTCODE"})
    assert_coded(
        response, 400, "password_too_short", f"Password must be at least {settings.password_min_len} characters",
        {"min": settings.password_min_len},
    )


def test_error_invalid_invite_code(browser):
    response = browser().post("/api/auth/register", json={"username": f"inv_{uuid4().hex[:8]}", "password": PASSWORD, "invite_code": "WRONG"})
    assert_coded(response, 400, "invalid_invite_code", "Invalid invite code")


def test_error_username_taken(browser):
    first = browser()
    register(first)
    response = browser().post("/api/auth/register", json={"username": first._registered, "password": PASSWORD, "invite_code": "TESTCODE"})
    assert_coded(response, 409, "username_taken", "Username already exists")


def test_error_display_name_empty(browser):
    session = browser()
    register(session)
    assert_coded(session.patch("/api/auth/settings", json={"name": "   "}), 422, "display_name_empty", "Display name cannot be empty")


def test_error_no_settings_to_update(browser):
    session = browser()
    register(session)
    assert_coded(session.patch("/api/auth/settings", json={}), 400, "no_settings_to_update", "No settings to update")


def test_error_invalid_language(browser):
    session = browser()
    register(session)
    assert_coded(session.patch("/api/auth/settings", json={"language": "fr"}), 422, "invalid_language", "Unsupported language")
    refused = browser().post("/api/auth/register", json={
        "username": f"fr_{uuid4().hex[:8]}", "password": PASSWORD, "invite_code": "TESTCODE", "language": "fr",
    })
    assert_coded(refused, 422, "invalid_language", "Unsupported language")


def test_error_invalid_region(browser):
    session = browser()
    register(session)
    assert_coded(session.patch("/api/auth/settings", json={"region": "SG"}), 422, "invalid_region", "Unsupported region")
    username = f"sg_{uuid4().hex[:8]}"
    refused = browser().post("/api/auth/register", json={
        "username": username, "password": PASSWORD, "invite_code": "TESTCODE", "region": "SG",
    })
    assert_coded(refused, 422, "invalid_region", "Unsupported region")
    assert stored(username) is None


def test_unsupported_currency_stays_a_schema_error(browser):
    session = browser()
    register(session)
    response = session.patch("/api/auth/settings", json={"currency": "EUR"})
    assert response.status_code == 422
    assert isinstance(response.json()["detail"], list)


def test_not_authenticated_stays_a_plain_string(browser):
    response = browser().get("/api/auth/me")
    assert response.status_code == 401
    assert response.json()["detail"] == "Not authenticated"


# --- Starter data by account language (task 2.5) -------------------------------------------------------------

ENGLISH_CATEGORIES = {
    ("Food & drinks", "expense"): "need",
    ("Snacks & going out", "expense"): "want",
    ("Groceries", "expense"): "need",
    ("Transport", "expense"): "need",
    ("Bills & utilities", "expense"): "need",
    ("Shopping", "expense"): "want",
    ("Entertainment", "expense"): "want",
    ("Health", "expense"): "need",
    ("Investments", "expense"): None,
    ("Transfer", "expense"): None,
    ("Transfer", "income"): None,
    ("Salary", "income"): None,
    ("Other income", "income"): None,
    ("Reimbursements", "income"): None,
}


def starter_rows(username: str) -> tuple[list[dict], list[dict]]:
    from app.db.pool import db_conn

    with db_conn() as conn:
        categories = conn.execute(
            """SELECT c.name, c.icon, c.color, c.kind, c.kakeibo_type, c.offsets_spending, c.is_excluded_from_budget,
                      c.system_key, c.is_archived
               FROM categories c JOIN users u ON u.id = c.user_id WHERE u.username = %s ORDER BY c.created_at, c.id""",
            (username,),
        ).fetchall()
        accounts = conn.execute(
            """SELECT a.name, a.type, a.initial_balance FROM accounts a JOIN users u ON u.id = a.user_id
               WHERE u.username = %s ORDER BY a.type""",
            (username,),
        ).fetchall()
    return [dict(row) for row in categories], [dict(row) for row in accounts]


def by_seed(rows: list[dict], language: str, kind_field: str) -> dict:
    """Rows keyed by seed key and kind (or type), names removed, to compare the two languages."""
    from app.db.init_db import STARTER_NAMES

    seed_of = {name: seed for seed, name in STARTER_NAMES[language].items()}
    return {(seed_of[row["name"]], row[kind_field]): {k: v for k, v in row.items() if k != "name"} for row in rows}


def test_english_and_indonesian_starter_data_differ_only_in_names(browser):
    english, indonesian = browser(), browser()
    register(english, language="en")
    register(indonesian, language="id")
    en_categories, en_accounts = starter_rows(english._registered)
    id_categories, id_accounts = starter_rows(indonesian._registered)

    assert {(row["name"], row["kind"]): row["kakeibo_type"] for row in en_categories} == ENGLISH_CATEGORIES
    assert [row["name"] for row in en_accounts] == ["Main account", "Cash"]
    assert [row["name"] for row in id_accounts] == ["Rekening Utama", "Uang Tunai"]
    keys = {(row["name"], row["kind"]): row["system_key"] for row in en_categories if row["system_key"]}
    assert keys == {
        ("Investments", "expense"): "investment",
        ("Transfer", "expense"): "internal_movement",
        ("Transfer", "income"): "internal_movement",
    }
    assert all(row["is_excluded_from_budget"] for row in en_categories if row["system_key"] == "internal_movement")
    assert by_seed(en_categories, "en", "kind") == by_seed(id_categories, "id", "kind")
    assert by_seed(en_accounts, "en", "type") == by_seed(id_accounts, "id", "type")
    assert len(en_categories) == len(id_categories) == len(ENGLISH_CATEGORIES)


def test_switching_language_renames_nothing(browser):
    session = browser()
    register(session, language="id")
    before = starter_rows(session._registered)
    assert session.patch("/api/auth/settings", json={"language": "en"}).status_code == 200
    assert starter_rows(session._registered) == before


@pytest.mark.parametrize("language,names", [
    ("en", {"internal_movement": "Transfer", "balance_adjustment": "Balance adjustment"}),
    ("id", {"internal_movement": "Internal Movement", "balance_adjustment": "Penyesuaian Saldo"}),
])
def test_on_demand_system_categories_take_the_account_language(client, language, names):
    from app.db.pool import db_conn
    from app.services.system_categories import BALANCE_ADJUSTMENT, INTERNAL_MOVEMENT, ensure_system_category

    user_id = str(uuid4())
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users (id, username, password_hash, invite_code, language) VALUES (%s, %s, 'synthetic', 'TEST', %s)",
            (user_id, f"ensure_{uuid4().hex[:8]}", language),
        )
        movement = ensure_system_category(cur, user_id, INTERNAL_MOVEMENT, "expense")
        adjustment = ensure_system_category(cur, user_id, BALANCE_ADJUSTMENT, "income")
        cur.execute("SELECT id, name FROM categories WHERE user_id = %s", (user_id,))
        created = {str(row["id"]): row["name"] for row in cur.fetchall()}
        conn.rollback()
    assert created == {movement: names["internal_movement"], adjustment: names["balance_adjustment"]}


def seeded_session(browser, language: str):
    """A registered account with its starter data and Rp 10.000.000 opening balances."""
    from app.db.pool import db_conn

    session = browser()
    register(session, language=language)
    with db_conn() as conn:
        conn.execute(
            "UPDATE accounts SET initial_balance = 10000000 WHERE user_id = (SELECT id FROM users WHERE username = %s)",
            (session._registered,),
        )
        conn.commit()
    categories = session.get("/api/categories").json()["categories"]
    accounts = session.get("/api/accounts").json()["accounts"]
    ids = {f"{row['name']}/{row['kind']}": row["id"] for row in categories}
    ids.update({row["name"]: row["id"] for row in accounts})
    return session, ids


def figure_snapshot(session) -> dict:
    overview = session.get("/api/dashboard/overview").json()["kpis"]
    summary = session.get("/api/dashboard/cycle-summary").json()
    return {
        "outflow": overview["total_outflow"],
        "inflow": overview["total_inflow"],
        "keluar": summary["keluar"]["total"],
        "keluar_lines": [(line["key"], line["amount"]) for line in summary["keluar"]["lines"]],
        "spending": summary["spending"]["amount"],
        "fresh_saving": summary["fresh_saving"]["amount"],
        "masuk": summary["masuk"]["total"],
        "remaining": summary["remaining"]["total"],
        "net_worth": summary["net_worth"]["total"],
        "net_worth_lines": [(line["key"], line["amount"]) for line in summary["net_worth"]["lines"]],
        "today_spent": session.get("/api/pulse").json()["today_spent"],
    }


def test_english_account_counts_investment_buys_and_movements_like_an_indonesian_one(browser):
    names = {
        "id": {"salary": "Gaji", "food": "Makanan & Minuman", "investment": "Investasi", "main": "Rekening Utama", "cash": "Uang Tunai"},
        "en": {"salary": "Salary", "food": "Food & drinks", "investment": "Investments", "main": "Main account", "cash": "Cash"},
    }
    snapshots = {}
    for language, name in names.items():
        session, ids = seeded_session(browser, language)

        def record(category: str, amount: int, kind: str = "expense") -> None:
            response = session.post("/api/transactions", json={
                "type": kind, "account_id": ids[name["main"]], "category_id": ids[f"{category}/{kind}"], "amount": amount,
            })
            assert response.status_code == 200, response.text

        record(name["salary"], 5_000_000, "income")
        record(name["food"], 200_000)
        record(name["investment"], 1_000_000)
        moved = session.post("/api/movements", json={
            "source_account_id": ids[name["main"]], "target_account_id": ids[name["cash"]], "amount": 500_000,
        })
        assert moved.status_code == 200, moved.text
        snapshots[language] = figure_snapshot(session)
        breakdown = {row["name"] for row in session.get("/api/dashboard/analytics").json()["category_variance"]}
        assert name["investment"] not in breakdown and "Transfer" not in breakdown and "Internal Movement" not in breakdown
    assert (snapshots["en"]["outflow"], snapshots["en"]["inflow"]) == (200_000, 5_000_000)
    assert snapshots["en"] == snapshots["id"]
