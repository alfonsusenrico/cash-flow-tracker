"""Account roles ("Kegunaan") and Dana investasi (organize-accounts-by-use D1, D2, D6), on fresh synthetic owners.

Every name and amount is synthetic: "RDN Contoh" stands for a broker's cash account, "Dana Darurat" for a
savings pocket and "PayLater Contoh" for a credit account.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from test_cycle_summary import make_owner, now_utc, owner, unlimited_rate_limits  # noqa: F401 - fixtures

V51 = Path(__file__).resolve().parents[2] / "db" / "migrations" / "V51__account_roles_and_layouts.sql"
NET_WORTH = "/api/dashboard/net-worth"


def create(owner, **payload):
    response = owner.session.post("/api/accounts", json={"name": f"Akun {uuid4().hex[:6]}", **payload})
    assert response.status_code == 200, response.text
    return response.json()["account"]


def rows(owner):
    response = owner.session.get("/api/accounts")
    assert response.status_code == 200, response.text
    return {row["id"]: row for row in response.json()["accounts"]}


def _select_role(account_id):
    from app.db.pool import db_conn
    with db_conn() as conn:
        return conn.execute("SELECT role FROM accounts WHERE id = %s", (account_id,)).fetchone()["role"]


# --- Account Role -----------------------------------------------------------------------------------


def test_new_accounts_take_the_default_role_and_kind_of_their_type(owner):
    cases = [
        ({"type": "bank"}, "harian", None),
        ({"type": "wallet"}, "harian", None),
        ({"type": "credit", "initial_balance": -10_000}, "utang", None),
        ({"type": "investment"}, "investasi", "lainnya"),
        ({"type": "investment", "instrument_type": "gold"}, "investasi", "emas"),
        ({"type": "investment", "instrument_type": "mutual_fund"}, "investasi", "reksa_dana"),
        ({"type": "investment", "instrument_type": "stock"}, "investasi", "saham"),
        ({"type": "investment", "instrument_type": "crypto"}, "investasi", "lainnya"),
        ({"type": "bank", "role": "dana_investasi"}, "dana_investasi", None),
    ]
    created = []
    for payload, role, kind in cases:
        account = create(owner, **payload)
        assert (account["role"], account["kind"]) == (role, kind), payload
        created.append((account["id"], role, kind))
    listed = rows(owner)
    for account_id, role, kind in created:
        assert (listed[account_id]["role"], listed[account_id]["kind"]) == (role, kind)
    # Existing gain fields stay on every row.
    assert {"capital_gain", "capital_gain_pct", "cost_basis"} <= set(listed[created[0][0]])


def test_setting_a_pocket_to_tabungan(owner):
    jago = create(owner, type="bank", name="Bank Jago Contoh")
    pocket = create(owner, type="bank", name="Dana Darurat", parent_id=jago["id"])
    response = owner.session.patch(f"/api/accounts/{pocket['id']}", json={"role": "tabungan"})
    assert response.status_code == 200, response.text
    assert response.json()["account"]["role"] == "tabungan"
    assert _select_role(pocket["id"]) == "tabungan"
    assert rows(owner)[pocket["id"]]["role"] == "tabungan"


def test_role_contradicting_the_type_is_refused_and_nothing_changes(owner):
    credit = create(owner, type="credit", initial_balance=-10_000, name="PayLater Contoh")
    fund = create(owner, type="investment", instrument_type="mutual_fund", name="Reksa Dana Contoh")
    for account, role in ((credit, "harian"), (fund, "tabungan"), (credit, "dana_investasi"), (fund, "harian")):
        response = owner.session.patch(f"/api/accounts/{account['id']}", json={"role": role, "name": "Nama Baru"})
        assert response.status_code == 422, response.text
        assert response.json()["detail"] == {"code": "account_role_type_mismatch"}
    assert (_select_role(credit["id"]), _select_role(fund["id"])) == ("utang", "investasi")
    listed = rows(owner)
    assert (listed[credit["id"]]["name"], listed[fund["id"]]["name"]) == ("PayLater Contoh", "Reksa Dana Contoh")
    for payload in (
        {"type": "bank", "role": "investasi"}, {"type": "bank", "role": "utang"},
        {"type": "credit", "role": "harian"}, {"type": "investment", "instrument_type": "gold", "role": "tabungan"},
    ):
        response = owner.session.post("/api/accounts", json={"name": f"Ditolak {uuid4().hex[:6]}", **payload})
        assert response.status_code == 422 and response.json()["detail"] == {"code": "account_role_type_mismatch"}
    unknown = owner.session.post("/api/accounts", json={"name": "Tidak Dikenal", "type": "bank", "role": "rahasia"})
    assert unknown.status_code == 422


def test_a_type_change_never_keeps_a_stale_role(owner):
    """Decision: an edit without a role keeps the current role while it fits the new type, else the type's
    default; an edit with a role is validated against the new type."""
    saving = create(owner, type="bank", role="tabungan")
    moved = owner.session.patch(f"/api/accounts/{saving['id']}", json={"type": "wallet"})
    assert moved.status_code == 200 and moved.json()["account"]["role"] == "tabungan"
    product = owner.session.patch(f"/api/accounts/{saving['id']}", json={"type": "investment", "instrument_type": "gold"})
    assert product.status_code == 200 and product.json()["account"]["role"] == "investasi"
    back = owner.session.patch(f"/api/accounts/{saving['id']}", json={"type": "bank", "instrument_type": None})
    assert back.status_code == 200 and back.json()["account"]["role"] == "harian"
    refused = owner.session.patch(f"/api/accounts/{saving['id']}", json={"type": "investment", "role": "tabungan"})
    assert refused.status_code == 422 and refused.json()["detail"] == {"code": "account_role_type_mismatch"}
    assert _select_role(saving["id"]) == "harian"
    both = owner.session.patch(f"/api/accounts/{saving['id']}", json={"type": "wallet", "role": "dana_investasi"})
    assert both.status_code == 200 and both.json()["account"]["role"] == "dana_investasi"


def test_another_owners_account_is_not_found_and_keeps_its_role(make_owner):
    first, second = make_owner(), make_owner()
    pocket = create(first, type="bank")
    response = second.session.patch(f"/api/accounts/{pocket['id']}", json={"role": "tabungan"})
    assert response.status_code == 404
    assert _select_role(pocket["id"]) == "harian"


def test_writers_without_a_role_get_the_role_of_the_type(owner):
    """A broker position created from a notification, or an older release, inserts without a role."""
    owner.account("position", "Saham Contoh", kind="investment")
    owner.account("paylater", "PayLater Contoh", kind="credit")
    assert (_select_role(owner["position"]), _select_role(owner["paylater"])) == ("investasi", "utang")
    owner.db("UPDATE accounts SET type = 'bank' WHERE id = %s", (owner["position"],))
    assert _select_role(owner["position"]) == "harian"


# --- V51 backfill ------------------------------------------------------------------------------------


def _insert_movement(cur, user_id, categories, source_id, target_id, amount, when, *, stored_pillar):
    movement_id = str(uuid4())
    for account_id, tx_type, role, pillar in (
        (source_id, "expense", "outbound", stored_pillar), (target_id, "income", "inbound", None),
    ):
        cur.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, date, movement_id,
                                         movement_role, kakeibo_type, source, review_status)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'movement', 'reviewed')""",
            (user_id, account_id, categories[tx_type], tx_type, amount, when, movement_id, role, pillar),
        )


def test_v51_backfill_keeps_todays_figures_and_a_rerun_keeps_owner_roles():
    from app.db.pool import db_conn
    from app.routers.dashboard import get_kakeibo_breakdown

    user_id = str(uuid4())
    ids = {key: str(uuid4()) for key in ("bank", "goal_pocket", "plain_pocket", "credit", "fund")}
    when = datetime.now(timezone.utc) - timedelta(hours=1)
    window = (when - timedelta(days=1), when + timedelta(days=1))
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (id, username, password_hash, invite_code) VALUES (%s, %s, 'synthetic-hash', 'TEST')",
                (user_id, f"roles_{uuid4().hex[:10]}"),
            )
            for key, name, kind, parent, instrument in (
                ("bank", "Bank Contoh", "bank", None, None),
                ("goal_pocket", "Kantong Target", "bank", "bank", None),
                ("plain_pocket", "Kantong Biasa", "bank", "bank", None),
                ("credit", "PayLater Contoh", "credit", None, None),
                ("fund", "Reksa Dana Contoh", "investment", None, "mutual_fund"),
            ):
                cur.execute(
                    """INSERT INTO accounts (id, user_id, name, type, parent_id, instrument_type, initial_balance)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (ids[key], user_id, name, kind, ids[parent] if parent else None, instrument,
                     -1000 if kind == "credit" else 5_000_000),
                )
            cur.execute("INSERT INTO goals (user_id, name, target_amount) VALUES (%s, 'Target Contoh', 1000000) RETURNING id", (user_id,))
            cur.execute("INSERT INTO goal_accounts (goal_id, account_id) VALUES (%s, %s)", (cur.fetchone()["id"], ids["goal_pocket"]))
            categories = {}
            for kind in ("expense", "income"):
                cur.execute(
                    """INSERT INTO categories (user_id, name, kind, is_excluded_from_budget)
                       VALUES (%s, 'Internal Movement', %s, TRUE) RETURNING id""",
                    (user_id, kind),
                )
                categories[kind] = cur.fetchone()["id"]
            _insert_movement(cur, user_id, categories, ids["bank"], ids["goal_pocket"], 400_000, when, stored_pillar="saving")
            _insert_movement(cur, user_id, categories, ids["bank"], ids["plain_pocket"], 250_000, when, stored_pillar=None)

            # The state before V51: no role column values. DDL is transactional; everything is rolled back.
            cur.execute("ALTER TABLE accounts DISABLE TRIGGER accounts_role_follows_type")
            cur.execute("ALTER TABLE accounts ALTER COLUMN role DROP NOT NULL")
            cur.execute("UPDATE accounts SET role = NULL WHERE user_id = %s", (user_id,))
            before = get_kakeibo_breakdown(cur, user_id, *window, 0)["saving_spent"]

            cur.execute(V51.read_text())
            cur.execute("SELECT id, role FROM accounts WHERE user_id = %s", (user_id,))
            roles = {str(row["id"]): row["role"] for row in cur.fetchall()}
            assert roles == {
                ids["bank"]: "harian", ids["goal_pocket"]: "tabungan", ids["plain_pocket"]: "harian",
                ids["credit"]: "utang", ids["fund"]: "investasi",
            }
            assert before == 400_000
            assert get_kakeibo_breakdown(cur, user_id, *window, 0)["saving_spent"] == before

            # Roles the owner chose survive a second run.
            cur.execute("UPDATE accounts SET role = 'harian' WHERE id = %s", (ids["goal_pocket"],))
            cur.execute("UPDATE accounts SET role = 'dana_investasi' WHERE id = %s", (ids["plain_pocket"],))
            cur.execute(V51.read_text())
            cur.execute("SELECT id, role FROM accounts WHERE id = ANY(%s::uuid[])", ([ids["goal_pocket"], ids["plain_pocket"]],))
            assert {str(row["id"]): row["role"] for row in cur.fetchall()} == {
                ids["goal_pocket"]: "harian", ids["plain_pocket"]: "dana_investasi",
            }
        conn.rollback()


# --- Dana investasi outside Saldo tersedia ----------------------------------------------------------


def rdn_owner(owner):
    """Rp 18.450.000 in Uang harian and Tabungan and an RDN of Rp 1.200.000; no emergency goal."""
    owner.db("UPDATE accounts SET initial_balance = 0 WHERE id = %s", (owner["bank"],))
    owner.account("atm", "ATM", initial=17_450_000, parent="bank")
    owner.db("UPDATE accounts SET default_pocket_id = %s WHERE id = %s", (owner["atm"], owner["bank"]))
    owner.account("savings", "Dana Darurat", initial=1_000_000, parent="bank")
    owner.account("rdn", "RDN Contoh", initial=1_200_000, parent="bank")
    for key, role in (("savings", "tabungan"), ("rdn", "dana_investasi")):
        response = owner.session.patch(f"/api/accounts/{owner[key]}", json={"role": role})
        assert response.status_code == 200, response.text
    return owner


def test_rdn_as_dana_investasi(owner):
    rdn_owner(owner)
    net = owner.summary()["net_worth"]
    assert (net["liquid"], net["dana_investasi"]) == (18_450_000, 1_200_000)
    assert {"key": "dana_investasi", "label": "Dana investasi", "amount": 1_200_000} in net["lines"]
    rekening = owner.session.get(NET_WORTH).json()
    assert (rekening["liquid"], rekening["dana_investasi"]) == (18_450_000, 1_200_000)
    assert rekening["runway"]["balance_basis"] == "all_liquid"
    assert rekening["runway"]["emergency_fund_balance"] == 18_450_000
    kpis = owner.session.get("/api/dashboard/overview?timeframe=month").json()["kpis"]
    assert (kpis["liquid_balance"], kpis["dana_investasi_balance"], kpis["emergency_fund_balance"]) == (
        18_450_000, 1_200_000, 18_450_000,
    )
    pulse = owner.session.get("/api/pulse").json()
    assert (pulse["total_liquid_balance"], pulse["dana_investasi_balance"]) == (18_450_000, 1_200_000)


def test_depositing_into_the_rdn(owner):
    rdn_owner(owner)
    before = owner.summary()["net_worth"]
    owner.move("atm", "rdn", 500_000)
    after = owner.summary()["net_worth"]
    assert after["liquid"] == before["liquid"] - 500_000
    assert after["dana_investasi"] == before["dana_investasi"] + 500_000
    assert after["total"] == before["total"]
    # Moving cash into an RDN is not saving.
    assert owner.summary()["tabungan"]["total"] == 0


def test_dana_investasi_is_an_asset_but_not_liquid(owner):
    rdn_owner(owner)
    owner.account("platform", "Platform Contoh", kind="investment")
    owner.account("fund", "Reksa Dana Contoh", kind="investment", initial=24_300_000, parent="platform")
    beranda = owner.summary()["net_worth"]
    rekening = owner.session.get(NET_WORTH).json()
    assert (rekening["liquid"], rekening["dana_investasi"], rekening["investment"]) == (18_450_000, 1_200_000, 24_300_000)
    assert rekening["total_assets"] == 43_950_000
    assert rekening["net_worth"] == beranda["total"] == 43_950_000
    assert [line["key"] for line in rekening["lines"]] == ["liquid", "dana_investasi", "investment"]
    assert rekening["lines"] == beranda["lines"]


def test_no_dana_investasi_line_without_such_an_account(owner):
    net = owner.summary()["net_worth"]
    assert net["dana_investasi"] == 0
    assert all(line["key"] != "dana_investasi" for line in net["lines"])
