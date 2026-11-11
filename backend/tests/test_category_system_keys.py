"""System category keys (add-category-system-keys): the V52 backfill, the owning module, and renames that keep figures.

All owners, names and amounts are synthetic.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

import psycopg
import pytest
from psycopg import sql

from app.services.system_categories import (
    BALANCE_ADJUSTMENT, INTERNAL_MOVEMENT, INVESTMENT, ensure_system_category, is_system_sql, reportable_key_sql,
)
from test_notification_processing_database import ingest, notification, notification_owner  # noqa: F401 - fixture

ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = sorted((ROOT / "db/migrations").glob("V*.sql"), key=lambda path: int(path.name.split("__")[0][1:]))
V52 = ROOT / "db/migrations/V52__category_system_keys.sql"
# Today's predicates, exactly as the code before this change wrote them.
OLD_REPORTABLE = "COALESCE(c.name, '') NOT IN ('Internal Movement', 'Investasi')"
OLD_MOVEMENT = "COALESCE(c.name = 'Internal Movement', FALSE)"
OLD_INVESTMENT = "COALESCE(c.name = 'Investasi', FALSE)"
KEY_OF_NAME = {"Investasi": INVESTMENT, "Internal Movement": INTERNAL_MOVEMENT, "Penyesuaian Saldo": BALANCE_ADJUSTMENT}


# --- V52 migration ------------------------------------------------------------------------------------------


def migrate_schema_to_v51(conn) -> str:
    """A throwaway schema at V51, inside the caller's forced-rollback transaction."""
    schema = "synthetic_category_keys_" + uuid4().hex
    conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    conn.execute(sql.SQL("SET LOCAL search_path TO {}, public").format(sql.Identifier(schema)))
    for path in MIGRATIONS:
        if path.name.startswith("V52__"):
            break
        conn.execute(path.read_text())
    return schema


def synthetic_owner(conn, label: str) -> str:
    return str(conn.execute(
        "INSERT INTO users (username, password_hash, invite_code) VALUES (%s, 'synthetic', 'synthetic') RETURNING id",
        (f"keys_{label}_{uuid4().hex[:8]}",),
    ).fetchone()["id"])


def add_category(conn, owner: str, name: str, kind: str, *, archived: bool = False, excluded: bool = False) -> str:
    return str(conn.execute(
        """INSERT INTO categories (user_id, name, kind, is_archived, is_excluded_from_budget)
           VALUES (%s, %s, %s, %s, %s) RETURNING id""",
        (owner, name, kind, archived, excluded),
    ).fetchone()["id"])


def test_v52_backfill_reproduces_the_name_predicate_for_every_owner_and_row(client, db_available):
    if not db_available:
        pytest.skip("Disposable database unavailable")
    from app.db.pool import db_conn

    with db_conn() as conn, conn.transaction(force_rollback=True):
        schema = migrate_schema_to_v51(conn)
        seeded, edges, plain = (synthetic_owner(conn, label) for label in ("seeded", "edges", "plain"))
        # The parity seed's shape: the seeded system categories next to ordinary ones.
        for name, kind, excluded in [
            ("Makanan & Minuman", "expense", False), ("Investasi", "expense", False),
            ("Internal Movement", "expense", True), ("Internal Movement", "income", True),
            ("Penyesuaian Saldo", "expense", True), ("Gaji", "income", False),
        ]:
            add_category(conn, seeded, name, kind, excluded=excluded)
        # D10's edge rows: an income "Investasi", an archived movement category, a lower-case and a renamed one.
        add_category(conn, edges, "Investasi", "income")
        add_category(conn, edges, "Internal Movement", "income", archived=True, excluded=True)
        lower_case = add_category(conn, edges, "investasi", "expense")
        renamed = add_category(conn, edges, "Invest", "expense")
        add_category(conn, edges, "internal movement", "expense")
        add_category(conn, edges, "Penyesuaian Saldo", "income", excluded=True)
        add_category(conn, plain, "Belanja", "expense")
        old_predicates = conn.execute(f"""
            SELECT c.id, {OLD_REPORTABLE} AS reportable, {OLD_MOVEMENT} AS movement, {OLD_INVESTMENT} AS investment
            FROM categories c""").fetchall()

        conn.execute(V52.read_text())

        rows = conn.execute(f"""
            SELECT c.id, c.user_id, c.name, c.system_key, {reportable_key_sql("c")} AS reportable,
                   {is_system_sql("c", INTERNAL_MOVEMENT)} AS movement, {is_system_sql("c", INVESTMENT)} AS investment
            FROM categories c ORDER BY c.user_id, c.name, c.kind""").fetchall()
        assert len(rows) == 13
        for row in rows:
            assert row["system_key"] == KEY_OF_NAME.get(row["name"]), row
        new_predicates = {str(row["id"]): (row["reportable"], row["movement"], row["investment"]) for row in rows}
        assert {str(row["id"]): (row["reportable"], row["movement"], row["investment"]) for row in old_predicates} \
            == new_predicates
        keyed = {(str(row["user_id"]), row["system_key"]) for row in rows if row["system_key"]}
        assert keyed == {
            (seeded, INVESTMENT), (seeded, INTERNAL_MOVEMENT), (seeded, BALANCE_ADJUSTMENT),
            (edges, INVESTMENT), (edges, INTERNAL_MOVEMENT), (edges, BALANCE_ADJUSTMENT),
        }
        assert {str(row["id"]): row["system_key"] for row in rows}[lower_case] is None
        assert {str(row["id"]): row["system_key"] for row in rows}[renamed] is None

        index = conn.execute(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = %s AND indexname = 'uq_category_user_system_key_kind'",
            (schema,),
        ).fetchone()
        assert "UNIQUE INDEX" in index["indexdef"] and "(user_id, system_key, kind)" in index["indexdef"]
        assert "WHERE (system_key IS NOT NULL)" in index["indexdef"]
        with pytest.raises(psycopg.errors.CheckViolation), conn.transaction():
            conn.execute("UPDATE categories SET system_key = 'salary' WHERE id = %s", (lower_case,))
        with pytest.raises(psycopg.errors.UniqueViolation), conn.transaction():
            conn.execute(
                "UPDATE categories SET system_key = 'investment' WHERE user_id = %s AND name = 'Makanan & Minuman'",
                (seeded,),
            )


def test_v52_fails_without_changes_when_an_owner_has_a_duplicate(client, db_available):
    if not db_available:
        pytest.skip("Disposable database unavailable")
    from app.db.pool import db_conn

    with db_conn() as conn, conn.transaction(force_rollback=True):
        schema = migrate_schema_to_v51(conn)
        # Only reachable without today's UNIQUE (user_id, name, kind); planted to prove the check.
        conn.execute("ALTER TABLE categories DROP CONSTRAINT uq_category_user_name_kind")
        owner = synthetic_owner(conn, "duplicate")
        add_category(conn, owner, "Investasi", "expense")
        add_category(conn, owner, "Investasi", "expense")
        with pytest.raises(psycopg.errors.RaiseException, match="1 owner"), conn.transaction():
            conn.execute(V52.read_text())
        column = conn.execute(
            """SELECT 1 FROM information_schema.columns
               WHERE table_schema = %s AND table_name = 'categories' AND column_name = 'system_key'""",
            (schema,),
        ).fetchone()
        assert column is None


# --- The owning module: ensure_system_category ---------------------------------------------------------------


@pytest.fixture
def bare_owner(client):
    from app.db.pool import db_conn

    user_id = str(uuid4())
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO users (id, username, name, password_hash, invite_code, language) VALUES (%s, %s, 'Sari Contoh', 'synthetic', 'TEST', 'id')",
            (user_id, f"ensure_{uuid4().hex}"),
        )
        conn.commit()
    yield user_id
    with db_conn() as conn:
        conn.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()


def owner_categories(cur, user_id: str) -> list[dict]:
    cur.execute(
        """SELECT id, name, kind, system_key, is_excluded_from_budget, kakeibo_type, is_archived FROM categories
           WHERE user_id = %s ORDER BY name, kind""",
        (user_id,),
    )
    return cur.fetchall()


@pytest.mark.parametrize("key,name", [(INTERNAL_MOVEMENT, "Internal Movement"), (BALANCE_ADJUSTMENT, "Penyesuaian Saldo")])
@pytest.mark.parametrize("kind", ["expense", "income"])
def test_ensure_creates_finds_and_adopts(bare_owner, key, name, kind):
    from app.db.pool import db_conn

    with db_conn() as conn, conn.transaction(force_rollback=True), conn.cursor() as cur:
        created = ensure_system_category(cur, bare_owner, key, kind)
        [row] = owner_categories(cur, bare_owner)
        assert (str(row["id"]), row["name"], row["kind"], row["system_key"]) == (created, name, kind, key)
        assert row["is_excluded_from_budget"] is True and row["kakeibo_type"] is None

        # Found by key after a rename, repaired, never duplicated.
        cur.execute("UPDATE categories SET name = 'Diganti Pemilik', is_excluded_from_budget = FALSE WHERE id = %s", (created,))
        assert ensure_system_category(cur, bare_owner, key, kind) == created
        [row] = owner_categories(cur, bare_owner)
        assert (row["name"], row["is_excluded_from_budget"]) == ("Diganti Pemilik", True)

        # Deleted keyed row: a same-name user category of that kind is adopted, not duplicated.
        cur.execute("DELETE FROM categories WHERE id = %s", (created,))
        cur.execute(
            "INSERT INTO categories (user_id, name, kind, kakeibo_type) VALUES (%s, %s, %s, %s) RETURNING id",
            (bare_owner, name, kind, "want" if kind == "expense" else None),
        )
        user_made = str(cur.fetchone()["id"])
        assert ensure_system_category(cur, bare_owner, key, kind) == user_made
        [row] = owner_categories(cur, bare_owner)
        assert (row["system_key"], row["is_excluded_from_budget"], row["kakeibo_type"]) == (key, True, None)


def test_ensure_keeps_movement_archived_and_restores_adjustment(bare_owner):
    from app.db.pool import db_conn

    with db_conn() as conn, conn.transaction(force_rollback=True), conn.cursor() as cur:
        movement = ensure_system_category(cur, bare_owner, INTERNAL_MOVEMENT, "expense")
        adjustment = ensure_system_category(cur, bare_owner, BALANCE_ADJUSTMENT, "expense")
        cur.execute("UPDATE categories SET is_archived = TRUE WHERE user_id = %s", (bare_owner,))
        assert ensure_system_category(cur, bare_owner, INTERNAL_MOVEMENT, "expense") == movement
        assert ensure_system_category(cur, bare_owner, BALANCE_ADJUSTMENT, "expense") == adjustment
        archived = {str(row["id"]): row["is_archived"] for row in owner_categories(cur, bare_owner)}
        assert archived == {movement: True, adjustment: False}


def test_ensure_refuses_the_seeded_investment_key_and_unknown_input(bare_owner):
    cur = object()  # never reached: the refusals come before any statement
    with pytest.raises(ValueError, match="seeded"):
        ensure_system_category(cur, bare_owner, INVESTMENT, "expense")
    with pytest.raises(ValueError, match="unknown system category"):
        ensure_system_category(cur, bare_owner, "salary", "income")
    with pytest.raises(ValueError, match="unknown category kind"):
        ensure_system_category(cur, bare_owner, INTERNAL_MOVEMENT, "transfer")


def test_new_owner_seeds_keys_only_on_the_system_categories(bare_owner):
    from app.db.init_db import DEFAULT_CATEGORIES, seed_user_defaults
    from app.db.pool import db_conn

    seed_user_defaults(bare_owner)
    with db_conn() as conn, conn.cursor() as cur:
        keyed = {(row["name"], row["kind"]): row["system_key"] for row in owner_categories(cur, bare_owner)}
    assert {name_kind: key for name_kind, key in keyed.items() if key} == {
        ("Investasi", "expense"): INVESTMENT,
        ("Internal Movement", "expense"): INTERNAL_MOVEMENT,
        ("Internal Movement", "income"): INTERNAL_MOVEMENT,
    }
    assert len(keyed) == len(DEFAULT_CATEGORIES)


# --- Seeded owner and API -----------------------------------------------------------------------------------


@pytest.fixture
def seeded_owner(client, bare_owner):
    """A registered owner's defaults (categories with keys, two accounts) with the session override."""
    from app.db.init_db import seed_user_defaults
    from app.db.pool import db_conn
    from app.main import app
    from app.services.auth import get_current_user

    seed_user_defaults(bare_owner)
    with db_conn() as conn:
        categories = conn.execute("SELECT id, name, kind FROM categories WHERE user_id = %s", (bare_owner,)).fetchall()
        accounts = conn.execute("SELECT id, name FROM accounts WHERE user_id = %s", (bare_owner,)).fetchall()
        conn.execute("UPDATE accounts SET initial_balance = 10000000 WHERE user_id = %s", (bare_owner,))
        conn.commit()
    app.dependency_overrides[get_current_user] = lambda: {
        "id": bare_owner, "username": "keys-owner", "name": "Sari Contoh", "currency": "IDR", "payday_day": 25,
        "monthly_spending_budget": 3_000_000,
    }
    ids = {f"{row['name']}/{row['kind']}": str(row["id"]) for row in categories}
    ids.update({row["name"]: str(row["id"]) for row in accounts})
    yield {"user_id": bare_owner, **ids}
    app.dependency_overrides.clear()


def post(client, path, body, expect=200):
    response = client.post(path, json=body)
    assert response.status_code == expect, response.text
    return response.json()


def record(client, owner, category, amount, tx_type="expense", account="Rekening Utama"):
    return post(client, "/api/transactions", {
        "type": tx_type, "account_id": owner[account], "category_id": owner[category], "amount": amount,
    })["transaction_id"]


def test_categories_and_transactions_return_the_read_only_key(client, seeded_owner):
    owner = seeded_owner
    listed = {(row["name"], row["kind"]): row["system_key"] for row in client.get("/api/categories").json()["categories"]}
    assert listed[("Investasi", "expense")] == INVESTMENT
    assert listed[("Internal Movement", "income")] == INTERNAL_MOVEMENT
    assert listed[("Makanan & Minuman", "expense")] is None

    buy = record(client, owner, "Investasi/expense", 1_000_000)
    meal = record(client, owner, "Makanan & Minuman/expense", 50_000)
    rows = {row["id"]: row["category_system_key"] for row in client.get("/api/transactions?limit=50").json()["transactions"]}
    assert (rows[buy], rows[meal]) == (INVESTMENT, None)
    assert client.get(f"/api/transactions/{buy}").json()["transaction"]["category_system_key"] == INVESTMENT

    # A client cannot set a key: "Hobi" stays ordinary and its spending still counts.
    created = post(client, "/api/categories", {"name": "Hobi", "kind": "expense", "system_key": INVESTMENT})["category"]
    patched = client.patch(f"/api/categories/{created['id']}", json={"system_key": INVESTMENT, "color": "#111111"})
    assert patched.status_code == 200, patched.text
    owner["Hobi/expense"] = created["id"]
    before = client.get("/api/dashboard/overview").json()["kpis"]["total_outflow"]
    record(client, owner, "Hobi/expense", 70_000)
    assert client.get("/api/dashboard/overview").json()["kpis"]["total_outflow"] == before + 70_000
    hobi = next(row for row in client.get("/api/categories").json()["categories"] if row["name"] == "Hobi")
    assert hobi["system_key"] is None


def test_kind_change_into_an_existing_system_category_is_refused(client, seeded_owner):
    owner = seeded_owner
    response = client.patch(f"/api/categories/{owner['Internal Movement/expense']}", json={"name": "Pindah", "kind": "income"})
    assert response.status_code == 409 and response.json()["detail"]["code"] == "system_category_exists"


# --- Renames keep every figure ------------------------------------------------------------------------------


RENAMES = {"Investasi": "Investments", "Internal Movement": "Transfer"}
TAB_TYPES = ("expense", "income", "movement", "investment", "transfer")


# Read-time stamps, not figures: when a response was built, and the end of a running window, which is "now".
VOLATILE_KEYS = frozenset({"generated_at", "as_of"})
RUNNING_END_KEYS = frozenset({"end", "to"})


def without_volatile(value):
    if isinstance(value, dict):
        return {key: without_volatile(item) for key, item in value.items() if key not in VOLATILE_KEYS}
    if isinstance(value, list):
        return [without_volatile(item) for item in value]
    return value


def is_running_end(path: str, before, after) -> bool:
    if path.rsplit("/", 1)[-1] not in RUNNING_END_KEYS or not (isinstance(before, str) and isinstance(after, str)):
        return False
    try:
        return abs((datetime.fromisoformat(after) - datetime.fromisoformat(before)).total_seconds()) < 60
    except ValueError:
        return False


def figures(client) -> dict:
    """Every figure the rename must not move, with the renamed display names mapped back to the seed names."""
    month_start = datetime.now(ZoneInfo("Asia/Jakarta")).date().replace(day=1).isoformat()
    paths = [
        "/api/pulse", "/api/insights", "/api/dashboard/overview?timeframe=cycle", "/api/dashboard/overview?timeframe=month",
        "/api/dashboard/cycle-summary", "/api/dashboard/analytics", "/api/dashboard/cash-flow-summary",
        f"/api/reports/monthly?basis=month&start_date={month_start}", "/api/reports/monthly/periods",
        "/api/review/queue", "/api/review/summary", "/api/transactions?include=type_counts,daily_totals&limit=200",
        *(f"/api/transactions?type={tab}&limit=200" for tab in TAB_TYPES),
    ]
    snapshot = {}
    for path in paths:
        response = client.get(path)
        assert response.status_code == 200, (path, response.text)
        snapshot[path] = without_volatile(response.json())
    text = json.dumps(snapshot, sort_keys=True, ensure_ascii=False)
    for seed, renamed in RENAMES.items():
        text = text.replace(json.dumps(renamed, ensure_ascii=False), json.dumps(seed, ensure_ascii=False))
    return json.loads(text)


def differences(before, after, path="") -> list[str]:
    if isinstance(before, dict) and isinstance(after, dict):
        return [found for key in sorted(set(before) | set(after))
                for found in differences(before.get(key), after.get(key), f"{path}/{key}")]
    if isinstance(before, list) and isinstance(after, list) and len(before) == len(after):
        return [found for index, pair in enumerate(zip(before, after)) for found in differences(*pair, f"{path}[{index}]")]
    if before == after or is_running_end(path, before, after):
        return []
    return [f"{path}: {before!r} -> {after!r}"]


def rename(client, category_id, name):
    response = client.patch(f"/api/categories/{category_id}", json={"name": name})
    assert response.status_code == 200, response.text


def test_renaming_system_categories_changes_only_their_names(client, seeded_owner):
    owner = seeded_owner
    record(client, owner, "Gaji/income", 5_000_000, tx_type="income")
    record(client, owner, "Makanan & Minuman/expense", 200_000)
    record(client, owner, "Investasi/expense", 1_000_000)
    post(client, "/api/movements", {
        "source_account_id": owner["Rekening Utama"], "target_account_id": owner["Uang Tunai"], "amount": 500_000,
    })
    unlinked = record(client, owner, "Internal Movement/income", 300_000, tx_type="income", account="Uang Tunai")
    before = figures(client)
    kpis = client.get("/api/dashboard/overview").json()["kpis"]
    assert (kpis["total_outflow"], kpis["total_inflow"]) == (200_000, 5_000_000)

    rename(client, owner["Investasi/expense"], "Investments")
    rename(client, owner["Internal Movement/expense"], "Transfer")
    rename(client, owner["Internal Movement/income"], "Transfer")

    changed = differences(before, figures(client))
    assert changed == [], "\n".join(changed)
    from app.db.pool import db_conn
    from app.services.movement_pairing import load_candidates
    # The unlinked movement leg is still a pairing candidate (CANDIDATE_SQL) under its new name.
    with db_conn() as conn, conn.cursor() as cur:
        candidates = load_candidates(
            cur, owner["user_id"], direction="income", around=[datetime.now(ZoneInfo("UTC"))], gap_seconds=3600,
            min_amount=300_000, max_amount=300_000, exclude=[],
        )
    assert unlinked in {str(row["id"]) for row in candidates}

    # A category created after the rename with a system name is ordinary: its Rp 100.000 is spending.
    owner["Investasi/new"] = post(client, "/api/categories", {"name": "Investasi", "kind": "expense"})["category"]["id"]
    record(client, owner, "Investasi/new", 100_000)
    kpis = client.get("/api/dashboard/overview").json()["kpis"]
    assert (kpis["total_outflow"], kpis["total_inflow"]) == (300_000, 5_000_000)
    pulse = client.get("/api/pulse").json()
    assert pulse["today_spent"] == 300_000
    breakdown = {row["name"] for row in client.get("/api/dashboard/analytics").json()["category_variance"]}
    assert "Investasi" in breakdown and "Investments" not in breakdown and "Transfer" not in breakdown


def test_recent_rows_keep_their_order_when_a_movement_legs_tie(client, seeded_owner):
    """The two legs of a movement share their date and creation time, so only the id can order them."""
    from app.db.pool import db_conn

    owner = seeded_owner
    post(client, "/api/movements", {
        "source_account_id": owner["Rekening Utama"], "target_account_id": owner["Uang Tunai"], "amount": 500_000,
    })

    def listed():
        return [
            [row["id"] for row in client.get("/api/dashboard/overview").json()["recent_transactions"]],
            [row["id"] for row in client.get("/api/pulse").json()["today_transactions"]],
        ]

    for rows in listed():
        assert rows == sorted(rows, reverse=True)
    # Rewriting one leg moves it in the table, which changes the order of rows the query leaves tied.
    with db_conn() as conn:
        for leg in listed()[0]:
            conn.execute("UPDATE transactions SET notes = notes WHERE id = %s", (leg,))
            conn.commit()
            for rows in listed():
                assert rows == sorted(rows, reverse=True)


def test_renamed_investment_is_never_offered_as_a_spending_choice(client, seeded_owner):
    from app.db.pool import db_conn
    from app.services.capture_resolution import reportable_categories
    from app.services.history_suggestions import active_categories, load_categories

    owner = seeded_owner
    rename(client, owner["Investasi/expense"], "Investments")
    rename(client, owner["Internal Movement/expense"], "Transfer")
    system = {owner["Investasi/expense"], owner["Internal Movement/expense"]}
    with db_conn() as conn, conn.cursor() as cur:
        assert system.isdisjoint(reportable_categories(cur, owner["user_id"], "expense"))
        assert system.isdisjoint({str(row["id"]) for row in active_categories(load_categories(cur, owner["user_id"]), "expense")})
    for category in ("Investasi/expense", "Internal Movement/expense"):
        rejected = post(client, "/api/transactions", {
            "type": "income", "account_id": owner["Uang Tunai"], "category_id": owner["Patungan & Ganti Uang/income"],
            "amount": 10_000, "offset_category_id": owner[category],
        }, expect=422)
        assert rejected["detail"]["code"] == "invalid_offset_category"


def test_reconcile_files_under_the_renamed_adjustment_category(client, seeded_owner):
    from app.db.pool import db_conn

    owner = seeded_owner
    account = owner["Rekening Utama"]
    post(client, f"/api/accounts/{account}/reconcile", {"actual_balance": 10_050_000})
    with db_conn() as conn:
        [income] = conn.execute(
            "SELECT id, name, system_key, is_excluded_from_budget FROM categories WHERE user_id = %s AND system_key = %s",
            (owner["user_id"], BALANCE_ADJUSTMENT),
        ).fetchall()
        assert (income["name"], income["is_excluded_from_budget"]) == ("Penyesuaian Saldo", True)
        conn.execute("UPDATE categories SET name = 'Koreksi saldo' WHERE id = %s", (income["id"],))
        conn.commit()
    post(client, f"/api/accounts/{account}/reconcile", {"actual_balance": 10_100_000})
    post(client, f"/api/accounts/{account}/reconcile", {"actual_balance": 9_900_000})
    with db_conn() as conn:
        rows = conn.execute(
            """SELECT c.name, c.kind, COUNT(t.id) AS rows FROM categories c
               LEFT JOIN transactions t ON t.category_id = c.id
               WHERE c.user_id = %s AND c.system_key = %s GROUP BY c.name, c.kind ORDER BY c.kind""",
            (owner["user_id"], BALANCE_ADJUSTMENT),
        ).fetchall()
    assert [(row["name"], row["kind"], row["rows"]) for row in rows] == [
        ("Penyesuaian Saldo", "expense", 1), ("Koreksi saldo", "income", 2),
    ]


# --- Notifications under renamed system categories ----------------------------------------------------------


def rename_in_database(category_ids, name):
    from app.db.pool import db_conn

    with db_conn() as conn:
        conn.execute("UPDATE categories SET name = %s WHERE id = ANY(%s::uuid[])", (name, list(category_ids)))
        conn.commit()


def owner_rows(owner):
    from app.db.pool import db_conn

    with db_conn() as conn:
        return conn.execute(
            """SELECT t.type, t.notes, t.category_id, t.movement_id, c.name AS category FROM transactions t
               JOIN categories c ON c.id = t.category_id WHERE t.user_id = %s ORDER BY t.type""",
            (owner["user_id"],),
        ).fetchall()


def test_jago_self_transfer_files_under_the_renamed_movement_category(client, notification_owner):
    owner = notification_owner
    rename_in_database([owner["internal_out"], owner["internal_in"]], "Transfer")
    result = ingest(client, notification("Raka Purnama has sent Rp125.000 to you. Need help? Contact Tanya Jago at 1500 746."))["results"][0]
    assert result["type"] == "income" and result["description"] == "Pindah saldo masuk"
    [row] = owner_rows(owner)
    assert (str(row["category_id"]), row["category"], row["notes"]) == (owner["internal_in"], "Transfer", "Pindah saldo masuk")
    kpis = client.get("/api/dashboard/overview?timeframe=90d").json()["kpis"]
    assert (kpis["total_outflow"], kpis["total_inflow"]) == (0, 0)


def test_stockbit_buy_books_its_legs_in_the_renamed_movement_categories(client, notification_owner):
    owner = notification_owner
    rename_in_database([owner["internal_out"], owner["internal_in"]], "Transfer")
    recorded = ingest(client, notification("Pembelian 4 lot FIKS match di harga Rp3.340", "com.stockbit.android"))["results"][0]
    assert recorded["status"] == "recorded" and recorded["amount"] == 1_336_000
    legs = owner_rows(owner)
    assert {str(row["category_id"]) for row in legs} == {owner["internal_out"], owner["internal_in"]}
    assert len({row["movement_id"] for row in legs}) == 1


def test_system_hints_resolve_by_key_and_other_hints_by_name():
    from app.services.notification_application import hinted_categories

    categories = [
        {"id": "investments", "name": "Investments", "kind": "expense"},
        {"id": "late-investasi", "name": "Investasi", "kind": "expense"},
        {"id": "transfer", "name": "Transfer", "kind": "expense"},
        {"id": "belanja", "name": "Belanja", "kind": "expense"},
    ]
    context = {"_system_category_keys": {"investments": INVESTMENT, "transfer": INTERNAL_MOVEMENT}}
    assert [row["id"] for row in hinted_categories(context, categories, "Investasi")] == ["investments"]
    assert [row["id"] for row in hinted_categories(context, categories, "Internal Movement")] == ["transfer"]
    assert [row["id"] for row in hinted_categories(context, categories, "shopping")] == ["belanja"]
    assert hinted_categories({"_system_category_keys": {}}, categories, "Internal Movement") == []


def test_model_input_keeps_system_keys_out():
    from app.services.openai_notification_provider import model_input

    context = {"categories": [{"id": "c1", "name": "Transfer", "kind": "expense"}],
               "_system_category_keys": {"c1": INTERNAL_MOVEMENT}}
    assert json.loads(model_input(context)) == {"categories": [{"id": "c1", "name": "Transfer", "kind": "expense"}]}
