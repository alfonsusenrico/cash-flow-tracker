"""GET /api/dashboard/cycle-summary: Beranda's figures and their lines, on fresh synthetic owners.

Every name, amount and date is synthetic. Dates are relative to the real clock because the summary
always describes the current period.
"""
import calendar
import sys
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta, timezone
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient
from psycopg.types.json import Jsonb

JAKARTA = ZoneInfo("Asia/Jakarta")
SUMMARY = "/api/dashboard/cycle-summary"
SALARY = 9_800_000


class UnlimitedLimiter:
    def exceeded(self, *_args) -> bool:
        return False

    def reset(self, *_keys) -> None:
        pass


@pytest.fixture(autouse=True)
def unlimited_rate_limits(monkeypatch):
    import app.routers.auth as auth_router

    monkeypatch.setattr(auth_router, "rate_limiter", UnlimitedLimiter())


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def today() -> date:
    return now_utc().astimezone(JAKARTA).date()


def at_local(day: date, hour: int = 9) -> datetime:
    return datetime.combine(day, time(hour), JAKARTA)


def shift_months(day: date, months: int) -> date:
    index = day.year * 12 + day.month - 1 + months
    year, month = index // 12, index % 12 + 1
    return date(year, month, min(day.day, calendar.monthrange(year, month)[1]))


def months_between(start: date, target: date) -> int:
    return max(1, (target.year - start.year) * 12 + (target.month - start.month))


class Owner:
    """A signed-in synthetic owner with accounts and categories created directly in the database."""

    def __init__(self, user_id: str, session: TestClient):
        self.user_id = user_id
        self.session = session
        self.ids: dict[str, str] = {}

    def __getitem__(self, key: str) -> str:
        return self.ids[key]

    def db(self, query, params=()):
        from app.db.pool import db_conn

        with db_conn() as conn:
            row = conn.execute(query, params).fetchone() if "RETURNING" in query else conn.execute(query, params)
            conn.commit()
        return row

    def account(self, key, name, kind="bank", initial=0, parent=None):
        account_id = str(uuid4())
        self.db(
            "INSERT INTO accounts (id, user_id, name, type, initial_balance, parent_id) VALUES (%s, %s, %s, %s, %s, %s)",
            (account_id, self.user_id, name, kind, initial, self.ids.get(parent) if parent else None),
        )
        self.ids[key] = account_id
        return account_id

    def category(self, key, name, kind="expense", pillar=None, offsets=False, excluded=False):
        category_id = str(uuid4())
        self.db(
            """INSERT INTO categories (id, user_id, name, kind, kakeibo_type, offsets_spending, is_excluded_from_budget)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (category_id, self.user_id, name, kind, pillar, offsets, excluded),
        )
        self.ids[key] = category_id
        return category_id

    def tx(self, tx_type, amount, *, category=None, account="bank", when=None, merchant=None, notes=None,
           review_status="reviewed", goal=None):
        row = self.db(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, date, merchant, notes,
                                         source, review_status, goal_id)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'manual', %s, %s) RETURNING id""",
            (self.user_id, self.ids[account], self.ids[category] if category else None, tx_type, amount,
             when or now_utc() - timedelta(minutes=5), merchant, notes, review_status,
             self.ids[goal] if goal else None),
        )
        return str(row["id"])

    def expense(self, amount, category=None, **fields):
        return self.tx("expense", amount, category=category, **fields)

    def move(self, source, target, amount, when=None):
        response = self.session.post("/api/movements", json={
            "source_account_id": self.ids[source], "target_account_id": self.ids[target], "amount": amount,
            "date": (when or now_utc() - timedelta(minutes=4)).isoformat(),
        })
        assert response.status_code == 200, response.text
        return response.json()

    def salary(self, day: date, amount=SALARY, *, source=False):
        tx_id = self.tx("income", amount, category="gaji", when=at_local(day), notes="Gaji PT Contoh")
        if source:
            assert self.session.put("/api/salary/source", json={"transaction_id": tx_id}).status_code == 200
        response = self.session.post("/api/salary/receipts", json={"transaction_id": tx_id})
        assert response.status_code == 200, response.text
        return tx_id

    def regular_salaries(self, latest: date, amount=SALARY, *, source=True):
        days = [shift_months(latest, -2), shift_months(latest, -1), latest]
        for index, day in enumerate(days):
            self.salary(day, amount, source=source and index == 0)
        return days

    def goal(self, key, name, target, target_date, accounts=(), current=0):
        goal_id = str(uuid4())
        self.db(
            """INSERT INTO goals (id, user_id, name, target_amount, current_amount, target_date)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (goal_id, self.user_id, name, target, current, target_date),
        )
        for account in accounts:
            self.db("INSERT INTO goal_accounts (goal_id, account_id) VALUES (%s, %s)", (goal_id, self.ids[account]))
        self.ids[key] = goal_id
        return goal_id

    def plan(self, key, name, amount, due: date, *, kind="expense"):
        rule_id = str(uuid4())
        self.db(
            """INSERT INTO recurring_rules (id, user_id, name, type, amount, source_account_id, target_account_id,
                                            schedule_type, schedule_day, next_due_date)
               VALUES (%s, %s, %s, %s, %s, %s, %s, 'monthly_day', %s, %s)""",
            (rule_id, self.user_id, name, kind, amount, self.ids["bank"],
             self.ids["wallet"] if kind == "transfer" else None, due.day, due),
        )
        self.ids[key] = rule_id
        return rule_id

    def summary(self, session=None):
        response = (session or self.session).get(SUMMARY)
        assert response.status_code == 200, response.text
        data = response.json()
        assert_lines_add_up(data)
        return data


def assert_lines_add_up(data):
    for figure in ("keluar", "masuk", "scheduled", "remaining", "net_worth"):
        assert sum(line["amount"] for line in data[figure]["lines"]) == data[figure]["total"], figure
    waiting = data["waiting"]
    assert sum(line["amount"] for line in waiting["lines"]) == waiting["total"]
    assert waiting["in_keluar"] + waiting["not_in_keluar"] == waiting["total"]
    assert waiting["in_keluar"] == sum(line["amount"] for line in waiting["lines"] if line["in_keluar"])
    assert waiting["count"] == len(waiting["lines"])
    assert data["remaining"]["total"] == data["masuk"]["total"] - data["keluar"]["total"] - data["scheduled"]["total"]
    net = data["net_worth"]
    assert net["total"] == (net["liquid"] + net["investment"] - net["reconciliation_pending"] - net["debts"]
                            - net["credit_owed"])
    for line in data["scheduled"]["lines"]:
        if line["kind"] == "goal":
            assert line["amount"] == max(0, line["set_aside"] - line["funded"])
    for value in _numbers(data):
        assert isinstance(value, int) and not isinstance(value, bool)


def _numbers(data):
    yield data["keluar"]["total"]
    yield data["masuk"]["total"]
    yield data["waiting"]["total"]
    yield data["scheduled"]["total"]
    yield data["remaining"]["total"]
    yield data["net_worth"]["total"]
    for figure in ("keluar", "masuk", "scheduled", "remaining", "net_worth"):
        for line in data[figure]["lines"]:
            yield line["amount"]


def keluar_lines(data):
    return {line["key"]: line["amount"] for line in data["keluar"]["lines"]}


@pytest.fixture
def make_owner(client):
    from app.db.pool import db_conn
    from app.main import app
    from passlib.hash import bcrypt

    created = []

    def factory():
        username = f"cycle_summary_{uuid4().hex[:12]}"
        password = "synthetic-pass-1"
        with db_conn() as conn:
            user_id = str(conn.execute(
                "INSERT INTO users (username, password_hash, invite_code) VALUES (%s, %s, 'TEST') RETURNING id",
                (username, bcrypt.hash(password)),
            ).fetchone()["id"])
            conn.commit()
        created.append(user_id)
        session = TestClient(app, base_url="https://testserver")
        session.headers["Origin"] = "https://testserver"
        assert session.post("/api/auth/login", json={"username": username, "password": password}).status_code == 200
        owner = Owner(user_id, session)
        owner.account("bank", "Bank Contoh", initial=20_000_000)
        owner.account("wallet", "Dompet Contoh", kind="ewallet")
        owner.account("pocket", "Kantong Liburan")
        owner.category("makan", "Makan Contoh", pillar="need")
        owner.category("jajan", "Jajan Contoh", pillar="want")
        owner.category("buku", "Buku Contoh", pillar="culture")
        owner.category("servis", "Servis Contoh", pillar="unexpected")
        owner.category("lain", "Lain Contoh")
        owner.category("gaji", "Gaji", kind="income")
        owner.category("patungan", "Patungan & Ganti Uang", kind="income", offsets=True)
        owner.category("movement", "Internal Movement", excluded=True)
        return owner

    yield factory
    with db_conn() as conn:
        conn.execute("DELETE FROM users WHERE id = ANY(%s::uuid[])", (created,))
        conn.commit()


@pytest.fixture
def owner(make_owner):
    return make_owner()


# --- Period ---------------------------------------------------------------------------------------


def test_month_basis_without_a_salary_cycle(owner):
    owner.expense(50_000, "makan")
    data = owner.summary()
    first = today().replace(day=1)
    period = data["period"]
    assert period["basis"] == "month" and period["cycle_offset_basis"] == "month"
    assert period["cycle_status"] == "not_configured"
    assert period["start_date"] == first.isoformat() and period["today"] == today().isoformat()
    assert period["day_index"] == today().day
    assert period["label"].endswith(str(first.year))
    assert data["expected_salary"] == {
        "status": "unknown", "date": None, "basis": None, "receipt_dates": [], "reason": "no_cycle",
    }
    assert data["safe_to_spend"]["status"] == "unavailable"
    assert data["safe_to_spend"]["reason"] == "no_cycle"
    assert data["safe_to_spend"]["per_day"] is None and data["safe_to_spend"]["days_left"] is None
    assert data["keluar"]["total"] == 50_000
    assert all(line["href"].startswith("/ledger?period=month&kakeibo=") for line in data["keluar"]["lines"])


def test_open_cycle_period_and_salary_in_masuk(owner):
    latest = today() - timedelta(days=5)
    owner.regular_salaries(latest)
    data = owner.summary()
    period = data["period"]
    assert period["basis"] == "cycle" and period["cycle_offset_basis"] == "cycle"
    assert period["start_date"] == latest.isoformat() and period["day_index"] == 6
    assert period["label"].startswith("Siklus sejak ")
    assert data["masuk"]["total"] == SALARY
    assert [line["label"] for line in data["masuk"]["lines"]] == ["Gaji"]
    assert data["masuk"]["lines"][0]["href"] == f"/ledger?period=cycle&type=income&category={owner['gaji']}"


# --- Keluar -------------------------------------------------------------------------------------


def test_keluar_is_the_overview_kakeibo_total_and_includes_saving(owner):
    owner.regular_salaries(today() - timedelta(days=5))
    owner.goal("liburan", "Liburan Contoh", 5_000_000, None, accounts=["pocket"])
    for amount, category in [(1_420_000, "makan"), (610_000, "jajan"), (245_000, "buku"), (89_500, "servis"), (40_000, "lain")]:
        owner.expense(amount, category)
    owner.move("bank", "pocket", 800_000)
    owner.move("bank", "wallet", 500_000)

    data = owner.summary()
    assert keluar_lines(data) == {
        "need": 1_420_000, "want": 610_000, "culture": 245_000, "unexpected": 89_500,
        "saving": 800_000, "unclassified": 40_000,
    }
    assert data["keluar"]["total"] == 3_204_500
    assert [line["label"] for line in data["keluar"]["lines"]] == [
        "Kebutuhan", "Keinginan", "Budaya", "Tak Terduga", "Tabungan", "Belum diklasifikasi",
    ]
    saving = next(line for line in data["keluar"]["lines"] if line["key"] == "saving")
    assert "tabungan" in saving["note"]
    assert next(line for line in data["keluar"]["lines"] if line["key"] == "unclassified")["href"] == \
        "/ledger?period=cycle&kakeibo=unclassified"

    overview = owner.session.get("/api/dashboard/overview?timeframe=cycle").json()
    assert data["keluar"]["total"] == overview["kakeibo"]["total_allocated"]
    assert data["masuk"]["total"] == overview["kpis"]["total_inflow"]

    # A move between two of the owner's own everyday accounts is not Keluar.
    owner.move("bank", "wallet", 500_000)
    assert owner.summary()["keluar"]["total"] == 3_204_500


# --- Waiting --------------------------------------------------------------------------------------


def waiting_event(owner, tx_id, *, question=False, reasons=()):
    owner.db(
        """INSERT INTO notification_events (user_id, device_id, package_name, post_time, payload_hash,
                                            processing_state, result_key, result_snapshot, transaction_id,
                                            interpretation, active_question)
           VALUES (%s, 'synthetic-device', 'com.example.bank', NOW(), %s, 'recorded', %s, %s, %s, %s, %s)""",
        (owner.user_id, uuid4().hex, f"synthetic:{uuid4().hex}", Jsonb({"status": "recorded"}), tx_id,
         Jsonb({"review_reasons": list(reasons)}),
         Jsonb({"type": "purpose", "transaction_id": tx_id}) if question else None),
    )


def test_waiting_rows_by_section_and_whether_keluar_holds_them(owner):
    from app.db.pool import db_conn
    from app.services.monthly_report import review_counts

    question = owner.expense(45_000, notes="QRIS 0812-0000", review_status="auto")
    waiting_event(owner, question, question=True, reasons=["purpose_question"])
    awaiting = owner.expense(112_000, "movement", notes="Transfer ke rekening contoh", review_status="needs_review")
    waiting_event(owner, awaiting, reasons=["awaiting_counterpart"])
    ambiguous = owner.tx("income", 500_000, notes="Transfer masuk contoh", review_status="needs_review")
    waiting_event(owner, ambiguous, reasons=["ambiguous_movement"])
    flagged = owner.expense(30_000, "jajan", merchant="Kedai Contoh", review_status="needs_review")
    owner.expense(25_000, "makan", merchant="Warung Contoh", review_status="auto")
    owner.expense(15_000, "makan", merchant="Toko Contoh")

    data = owner.summary()
    waiting = data["waiting"]
    lines = {line["transaction_id"]: line for line in waiting["lines"]}
    assert set(lines) == {question, awaiting, ambiguous, flagged}
    assert [line["section"] for line in waiting["lines"]] == ["questions", "ambiguous", "awaiting", "groups"]
    assert (lines[question]["in_keluar"], lines[question]["in_keluar_as"]) == (True, "unclassified")
    assert lines[question]["label"] == "QRIS 0812-0000" and lines[question]["account_name"] == "Bank Contoh"
    assert (lines[awaiting]["in_keluar"], lines[awaiting]["in_keluar_as"]) == (False, None)
    assert (lines[ambiguous]["in_keluar"], lines[ambiguous]["in_keluar_as"]) == (False, None)
    assert (lines[flagged]["in_keluar"], lines[flagged]["in_keluar_as"]) == (True, "want")
    assert (waiting["count"], waiting["total"]) == (4, 687_000)
    assert (waiting["in_keluar"], waiting["not_in_keluar"]) == (75_000, 612_000)
    # The same rows the Tinjau badge counts.
    with db_conn() as conn:
        with conn.cursor() as cur:
            assert review_counts(cur, owner.user_id)["attention"] == waiting["count"]

    # Answering the question moves its amount between buckets, never in or out of Keluar.
    owner.db("UPDATE transactions SET category_id = %s, review_status = 'reviewed' WHERE id = %s", (owner["makan"], question))
    owner.db("UPDATE notification_events SET active_question = NULL WHERE transaction_id = %s", (question,))
    answered = owner.summary()
    assert answered["keluar"]["total"] == data["keluar"]["total"]
    assert keluar_lines(answered)["unclassified"] == keluar_lines(data)["unclassified"] - 45_000
    assert keluar_lines(answered)["need"] == keluar_lines(data)["need"] + 45_000
    assert answered["waiting"]["total"] == waiting["total"] - 45_000


def test_nothing_waiting(owner):
    owner.expense(20_000, "makan")
    assert owner.summary()["waiting"] == {"count": 0, "total": 0, "in_keluar": 0, "not_in_keluar": 0, "lines": []}


# --- Safe to spend --------------------------------------------------------------------------------


def test_safe_to_spend_divides_by_the_days_before_the_expected_salary(owner):
    latest = today() - timedelta(days=5)
    days = owner.regular_salaries(latest)
    for amount, category in [(1_420_000, "makan"), (610_000, "jajan"), (245_000, "buku"), (89_500, "servis")]:
        owner.expense(amount, category)
    owner.goal("liburan", "Liburan Contoh", 5_000_000, None, accounts=["pocket"])
    owner.move("bank", "pocket", 800_000)
    owner.plan("cicilan", "Cicilan Contoh", 600_000, today() + timedelta(days=3))

    data = owner.summary()
    expected = shift_months(latest, 1)
    assert data["expected_salary"] == {
        "status": "estimated", "date": expected.isoformat(), "basis": "regular_receipts",
        "receipt_dates": [day.isoformat() for day in days], "reason": None,
    }
    assert data["keluar"]["total"] == 3_164_500
    assert data["scheduled"]["window_basis"] == "expected_salary"
    assert data["scheduled"]["window_start"] == latest.isoformat()
    assert data["scheduled"]["window_end"] == (expected - timedelta(days=1)).isoformat()
    assert data["scheduled"]["total"] == 600_000
    remaining = SALARY - 3_164_500 - 600_000
    assert data["remaining"]["total"] == remaining == 6_035_500
    assert data["remaining"]["lines"] == [
        {"key": "masuk", "label": "Masuk", "amount": SALARY},
        {"key": "keluar", "label": "Keluar", "amount": -3_164_500},
        {"key": "scheduled", "label": "Terjadwal", "amount": -600_000},
    ]
    days_left = (expected - today()).days
    assert data["safe_to_spend"] == {
        "status": "available", "reason": None, "detail": None, "days_left": days_left,
        "per_day": remaining // days_left // 1000 * 1000, "rounding": 1000, "over_by": None,
    }


def test_overspent_cycle_gives_zero_per_day_and_the_overrun(owner):
    owner.regular_salaries(today() - timedelta(days=5), amount=1_000_000)
    owner.expense(1_300_000, "makan")
    safe = owner.summary()["safe_to_spend"]
    assert safe["status"] == "available"
    assert (safe["per_day"], safe["over_by"]) == (0, 300_000)


def test_unknown_salary_date_keeps_remaining_and_uses_a_30_day_schedule(owner):
    owner.salary(today() - timedelta(days=5), source=True)
    owner.expense(100_000, "makan")
    soon = owner.plan("listrik", "Listrik Contoh", 300_000, today() + timedelta(days=10))
    owner.plan("asuransi", "Asuransi Contoh", 400_000, today() + timedelta(days=40))

    data = owner.summary()
    assert data["expected_salary"]["status"] == "unknown"
    assert data["expected_salary"]["reason"] == "too_few_receipts"
    assert data["safe_to_spend"]["status"] == "unavailable"
    assert (data["safe_to_spend"]["reason"], data["safe_to_spend"]["detail"]) == ("no_expected_salary", "too_few_receipts")
    assert data["safe_to_spend"]["per_day"] is None
    assert data["remaining"]["total"] == SALARY - 100_000 - 300_000
    scheduled = data["scheduled"]
    assert scheduled["window_basis"] == "next_30_days"
    assert scheduled["window_end"] == (today() + timedelta(days=30)).isoformat()
    assert [line["rule_id"] for line in scheduled["lines"]] == [soon]

    # A skipped occurrence is met; its next one falls outside the window.
    response = owner.session.post(f"/api/recurring/{soon}/occurrences/skip",
                                  json={"scheduled_for": (today() + timedelta(days=10)).isoformat()})
    assert response.status_code == 200, response.text
    assert owner.summary()["scheduled"]["lines"] == []


def test_late_salary_is_overdue_and_the_cycle_stays_open(owner):
    from app.db.pool import db_conn

    latest = today() - timedelta(days=40)
    owner.regular_salaries(latest)
    with db_conn() as conn:
        receipts_before = conn.execute("SELECT COUNT(*) AS n FROM salary_receipts WHERE user_id = %s", (owner.user_id,)).fetchone()["n"]
    data = owner.summary()
    assert data["expected_salary"]["status"] == "overdue"
    assert data["expected_salary"]["date"] == shift_months(latest, 1).isoformat()
    assert (data["safe_to_spend"]["status"], data["safe_to_spend"]["reason"]) == ("unavailable", "salary_overdue")
    assert data["safe_to_spend"]["days_left"] is None
    assert data["scheduled"]["window_basis"] == "next_30_days"
    # The estimate is not a boundary: the cycle and every report window still run to now.
    assert data["period"]["start_date"] == latest.isoformat()
    assert owner.session.get("/api/dashboard/overview?timeframe=cycle").json()["timeframe_label"].endswith("berlanjut")
    with db_conn() as conn:
        receipts_after = conn.execute("SELECT COUNT(*) AS n FROM salary_receipts WHERE user_id = %s", (owner.user_id,)).fetchone()["n"]
    assert receipts_after == receipts_before


def test_receipts_without_a_confirmed_source_give_no_estimate(owner):
    owner.regular_salaries(today() - timedelta(days=5), source=False)
    data = owner.summary()
    assert data["period"]["basis"] == "cycle"
    assert data["expected_salary"]["reason"] == "no_salary_source"
    assert (data["safe_to_spend"]["reason"], data["safe_to_spend"]["detail"]) == ("no_expected_salary", "no_salary_source")


# --- Scheduled ------------------------------------------------------------------------------------


def test_linked_payment_leaves_terjadwal_as_it_enters_keluar(owner):
    owner.regular_salaries(today() - timedelta(days=5))
    due = today() + timedelta(days=3)
    rule = owner.plan("cicilan", "Cicilan Contoh", 600_000, due)
    before = owner.summary()
    assert [line["label"] for line in before["scheduled"]["lines"]] == ["Cicilan Contoh"]
    assert before["scheduled"]["lines"][0] == {
        "kind": "plan", "rule_id": rule, "label": "Cicilan Contoh", "due_date": due.isoformat(),
        "overdue": False, "amount": 600_000,
    }

    payment = owner.expense(600_000, "makan", notes="Cicilan contoh")
    response = owner.session.post(f"/api/recurring/{rule}/occurrences/link",
                                  json={"scheduled_for": due.isoformat(), "transaction_id": payment})
    assert response.status_code == 200, response.text
    after = owner.summary()
    assert after["keluar"]["total"] == before["keluar"]["total"] + 600_000
    assert after["scheduled"]["total"] == before["scheduled"]["total"] - 600_000
    assert after["remaining"]["total"] == before["remaining"]["total"]


def test_funded_goal_set_aside_is_not_counted_twice(owner):
    first = today().replace(day=1)
    target_date = shift_months(first, 3).replace(day=15)
    months = months_between(first, target_date)
    owner.goal("liburan", "Liburan Contoh", 600_000 * months, target_date, accounts=["pocket"])
    owner.goal("laptop", "Laptop Contoh", 300_000 * months, target_date)
    owner.goal("tanpa_tanggal", "Tanpa Tanggal Contoh", 1_000_000, None)
    owner.move("bank", "pocket", 400_000)
    owner.expense(100_000, "lain", goal="laptop")

    data = owner.summary()
    goals = {line["goal_id"]: line for line in data["scheduled"]["lines"] if line["kind"] == "goal"}
    assert set(goals) == {owner["liburan"], owner["laptop"]}
    liburan = goals[owner["liburan"]]
    assert (liburan["set_aside"], liburan["funded"], liburan["amount"]) == (600_000, 400_000, 200_000)
    assert liburan["target_date"] == target_date.isoformat() and liburan["months_left"] == months
    laptop = goals[owner["laptop"]]
    assert (laptop["set_aside"], laptop["funded"], laptop["amount"]) == (300_000, 100_000, 200_000)
    assert data["scheduled"]["total"] == 400_000
    # The funding itself is saving inside Keluar, so the remaining amount sees it once.
    assert keluar_lines(data)["saving"] == 400_000


# --- Reimbursements -------------------------------------------------------------------------------


def test_reimbursement_raises_the_remaining_amount_by_the_offset(owner):
    owner.salary(today() - timedelta(days=5))
    owner.expense(300_000, "jajan")
    before = owner.summary()
    owner.tx("income", 200_000, category="patungan", notes="Ganti uang contoh")
    owner.db("UPDATE transactions SET offset_category_id = %s WHERE category_id = %s", (owner["jajan"], owner["patungan"]))
    after = owner.summary()
    assert after["masuk"]["total"] == before["masuk"]["total"]
    assert after["keluar"]["total"] == before["keluar"]["total"] - 200_000
    assert after["remaining"]["total"] == before["remaining"]["total"] + 200_000
    assert after["keluar"]["offsets_total"] == 200_000
    want = next(line for line in after["keluar"]["lines"] if line["key"] == "want")
    assert want["amount"] == 100_000 and "ganti uang Rp 200.000" in want["note"]


# --- Net worth ------------------------------------------------------------------------------------


def test_net_worth_counts_parent_and_pockets_once_and_lists_each_debt(owner):
    owner.account("platform", "Platform Contoh", kind="investment")
    owner.account("fund", "Reksa Dana Contoh", kind="investment", initial=25_000_000, parent="platform")
    owner.account("child_pocket", "Kantong Anak", initial=1_000_000, parent="bank")
    debt = owner.db(
        """INSERT INTO obligations (user_id, name, total_amount, remaining_amount)
           VALUES (%s, 'Pinjaman Contoh', 3000000, 2400000) RETURNING id""", (owner.user_id,),
    )
    owner.db("INSERT INTO obligations (user_id, name, total_amount, remaining_amount, is_archived) VALUES (%s, 'Lunas Contoh', 100, 0, TRUE)", (owner.user_id,))
    data = owner.summary()
    net = data["net_worth"]
    assert (net["liquid"], net["investment"], net["debts"]) == (21_000_000, 25_000_000, 2_400_000)
    assert net["total"] == 43_600_000
    assert {"key": f"debt:{debt['id']}", "label": "Pinjaman Contoh", "amount": -2_400_000} in net["lines"]
    assert owner.session.get("/api/dashboard/net-worth").json()["net_worth"] == net["total"]
    assert owner.session.get("/api/dashboard/overview").json()["kpis"]["liquid_balance"] == net["liquid"]


NET_WORTH = "/api/dashboard/net-worth"
# The keys `/dashboard/net-worth` answered before it shared Beranda's rule; they must all stay.
NET_WORTH_KEYS_BEFORE = {"ok", "net_worth", "total_assets", "total_liabilities", "accounts", "obligations", "runway"}


def net_worth_owner(owner):
    owner.account("platform", "Platform Contoh", kind="investment")
    owner.account("fund", "Reksa Dana Contoh", kind="investment", initial=25_000_000, parent="platform")
    owner.account("position", "Saham Contoh", kind="investment", initial=0)
    owner.db(
        "UPDATE accounts SET instrument_type = 'stock', instrument_symbol = 'AAAA', units = 200, avg_buy_price = 5000 "
        "WHERE id = %s", (owner["position"],),
    )
    owner.account("child_pocket", "Kantong Anak", initial=1_000_000, parent="bank")
    owner.account("minus_pocket", "Kantong Minus", initial=-50_000, parent="bank")
    owner.db("UPDATE accounts SET reconciliation_required = TRUE WHERE id = %s", (owner["minus_pocket"],))
    owner.db("INSERT INTO obligations (user_id, name, total_amount, remaining_amount) VALUES (%s, 'Pinjaman Contoh', 3000000, 2400000)", (owner.user_id,))
    owner.db("INSERT INTO obligations (user_id, name, total_amount, remaining_amount, is_archived) VALUES (%s, 'Lunas Contoh', 100, 0, TRUE)", (owner.user_id,))
    owner.db("INSERT INTO obligations (user_id, name, total_amount, remaining_amount) VALUES (%s, 'Selesai Contoh', 500000, 0)", (owner.user_id,))
    return owner


def test_rekening_net_worth_equals_beranda_with_a_pocket_waiting_for_reconciliation(owner):
    net_worth_owner(owner)
    beranda = owner.summary()["net_worth"]
    rekening = owner.session.get(NET_WORTH).json()
    assert rekening["net_worth"] == beranda["total"]
    assert rekening["total_assets"] == beranda["liquid"] + beranda["investment"]
    assert (rekening["liquid"], rekening["investment"], rekening["reconciliation_pending"]) == (
        beranda["liquid"], beranda["investment"], beranda["reconciliation_pending"],
    )
    assert rekening["lines"] == beranda["lines"]
    # The minus pocket is its own owed line, never a reduction of the assets; the pockets count once.
    assert rekening["reconciliation_pending"] == 50_000
    assert rekening["liquid"] == 21_000_000
    assert {"key": "reconciliation_pending", "label": "Saldo minus perlu dicocokkan", "amount": -50_000} in rekening["lines"]
    # Archived and paid-off debts are left out.
    assert rekening["total_liabilities"] == 2_400_000
    assert [line["label"] for line in rekening["lines"] if line["key"].startswith("debt:")] == ["Pinjaman Contoh"]
    assert rekening["net_worth"] == rekening["total_assets"] - 50_000 - 2_400_000


def test_rekening_net_worth_keeps_every_earlier_key(owner):
    net_worth_owner(owner)
    body = owner.session.get(NET_WORTH).json()
    assert NET_WORTH_KEYS_BEFORE <= set(body)
    assert {"liquid", "investment", "reconciliation_pending", "lines"} <= set(body)


def test_rekening_net_worth_is_owner_scoped(make_owner):
    first, second = make_owner(), make_owner()
    net_worth_owner(second)
    body = first.session.get(NET_WORTH).json()
    assert body["net_worth"] == first.summary()["net_worth"]["total"]
    assert body["total_liabilities"] == 0 and body["reconciliation_pending"] == 0
    assert all(not line["key"].startswith("debt:") for line in body["lines"])


def test_rekening_net_worth_statement_count_is_constant(owner, monkeypatch):
    net_worth_owner(owner)

    def count() -> int:
        with monkeypatch.context() as patch:
            with counted_statements(patch) as statements:
                assert owner.session.get(NET_WORTH).status_code == 200
                return len(statements)

    baseline = count()
    assert baseline > 0
    for index in range(15):
        owner.account(f"extra{index}", f"Kantong Ekstra {index}", initial=10_000, parent="bank")
        owner.db("INSERT INTO obligations (user_id, name, total_amount, remaining_amount) VALUES (%s, %s, 1000, 500)", (owner.user_id, f"Utang Ekstra {index}"))
    assert count() == baseline


def test_scheduled_names_the_bills_total_apart_from_goal_set_asides(owner):
    owner.salary(today() - timedelta(days=5))
    owner.plan("air", "Air Contoh", 100_000, today() + timedelta(days=3))
    owner.plan("listrik", "Listrik Contoh", 250_000, today() + timedelta(days=4))
    owner.goal("liburan", "Liburan Contoh", 6_000_000, shift_months(today(), 6))
    scheduled = owner.summary()["scheduled"]
    plans = [line for line in scheduled["lines"] if line["kind"] == "plan"]
    assert scheduled["plans_total"] == sum(line["amount"] for line in plans) == 350_000
    assert scheduled["total"] > scheduled["plans_total"]


# --- Comparison -----------------------------------------------------------------------------------


def test_comparison_is_too_short_on_day_two(owner):
    owner.salary(today() - timedelta(days=31))
    owner.salary(today() - timedelta(days=1))
    comparison = owner.summary()["comparison"]
    assert comparison["status"] == "too_short" and comparison["compared_days"] == 2
    for name in ("keluar", "masuk", "belanja"):
        assert comparison[name]["previous"] is None and comparison[name]["change"] is None


def test_comparison_without_a_previous_period(owner):
    owner.salary(today() - timedelta(days=10))
    owner.expense(50_000, "makan")
    comparison = owner.summary()["comparison"]
    assert comparison["status"] == "no_previous" and comparison["previous_label"] is None
    assert comparison["keluar"] == {"current": 50_000, "previous": None, "change": None}


def test_comparison_over_the_same_elapsed_days(owner):
    previous_start, current_start = today() - timedelta(days=40), today() - timedelta(days=10)
    owner.salary(previous_start)
    owner.salary(current_start)
    owner.goal("liburan", "Liburan Contoh", 5_000_000, None, accounts=["pocket"])
    owner.expense(100_000, "makan", when=at_local(previous_start + timedelta(days=2)))
    owner.move("bank", "pocket", 50_000, when=at_local(previous_start + timedelta(days=2), 10))
    # Outside the compared days of the previous cycle.
    owner.expense(999_000, "makan", when=at_local(previous_start + timedelta(days=25)))
    owner.expense(300_000, "makan", when=at_local(current_start + timedelta(days=2)))
    owner.move("bank", "pocket", 80_000, when=at_local(current_start + timedelta(days=2), 10))

    data = owner.summary()
    comparison = data["comparison"]
    assert comparison["status"] == "available" and comparison["compared_days"] == 11
    assert comparison["keluar"] == {"current": 380_000, "previous": 150_000, "change": 230_000}
    assert comparison["keluar"]["current"] == data["keluar"]["total"]
    assert comparison["masuk"] == {"current": SALARY, "previous": SALARY, "change": 0}
    assert comparison["belanja"] == {"current": 300_000, "previous": 100_000, "change": 200_000}
    fair = owner.session.get("/api/dashboard/period-comparison?timeframe=cycle").json()
    assert (fair["current"]["outflow"], fair["previous"]["outflow"]) == (300_000, 100_000)
    assert comparison["label"] == fair["label"] and comparison["previous_label"] == fair["previous_label"]


# --- API ------------------------------------------------------------------------------------------


def test_unauthenticated_requests_get_401(client):
    from app.main import app

    anonymous = TestClient(app, base_url="https://testserver")
    response = anonymous.get(SUMMARY)
    assert response.status_code == 401
    assert "keluar" not in response.json()
    unknown = TestClient(app, base_url="https://testserver", headers={"Authorization": "Bearer synthetic-unknown"})
    assert unknown.get(SUMMARY).status_code == 401


def test_bearer_key_reads_the_same_summary(owner):
    from app.main import app
    from app.services.auth import hash_token

    owner.salary(today() - timedelta(days=5))
    owner.expense(75_000, "makan")
    plain = f"synthetic-key-{uuid4().hex}"
    owner.db("INSERT INTO api_keys (user_id, key_hash, key_prefix) VALUES (%s, %s, 'synthet')", (owner.user_id, hash_token(plain)))
    phone = TestClient(app, base_url="https://testserver", headers={"Authorization": f"Bearer {plain}"})
    by_key = owner.summary(phone)
    by_session = owner.summary()
    for figure in ("keluar", "masuk", "waiting", "scheduled", "remaining", "net_worth", "safe_to_spend", "expected_salary"):
        assert by_key[figure] == by_session[figure], figure


def test_two_owners_on_the_same_day_see_only_their_own_figures(make_owner):
    mine, theirs = make_owner(), make_owner()
    mine.expense(40_000, "makan")
    mine_rule = mine.plan("air", "Air Contoh", 100_000, today() + timedelta(days=5))
    for amount in (900_000, 50_000):
        theirs.expense(amount, "jajan", review_status="needs_review")
    theirs.tx("income", 7_000_000, category="gaji")
    theirs.plan("sewa", "Sewa Contoh", 2_000_000, today() + timedelta(days=5))
    theirs.goal("rumah", "Rumah Contoh", 60_000_000, shift_months(today(), 6))
    theirs.db("INSERT INTO obligations (user_id, name, total_amount, remaining_amount) VALUES (%s, 'Utang Contoh', 900000, 900000)", (theirs.user_id,))

    data = mine.summary()
    assert data["keluar"]["total"] == 40_000 and data["masuk"]["total"] == 0
    assert data["waiting"]["count"] == 0
    assert [line["rule_id"] for line in data["scheduled"]["lines"]] == [mine_rule]
    assert data["net_worth"]["total"] == 20_000_000 - 40_000
    assert data["net_worth"]["debts"] == 0
    other = theirs.summary()
    assert other["keluar"]["total"] == 950_000 and other["waiting"]["count"] == 2
    assert mine_rule not in {line.get("rule_id") for line in other["scheduled"]["lines"]}


# --- Statement count ------------------------------------------------------------------------------


@contextmanager
def counted_statements(monkeypatch):
    """Count every statement sent through any application connection while the summary is built."""
    from app.db import pool

    original = pool.db_conn
    statements: list[str] = []

    class CountingCursor:
        def __init__(self, cursor):
            self._cursor = cursor

        def execute(self, query, *args, **kwargs):
            statements.append(str(query))
            self._cursor.execute(query, *args, **kwargs)
            return self

        def __getattr__(self, name):
            return getattr(self._cursor, name)

        def __iter__(self):
            return iter(self._cursor)

        def __enter__(self):
            self._cursor.__enter__()
            return self

        def __exit__(self, *exc):
            return self._cursor.__exit__(*exc)

    class CountingConnection:
        def __init__(self, conn):
            self._conn = conn

        def cursor(self, *args, **kwargs):
            return CountingCursor(self._conn.cursor(*args, **kwargs))

        def execute(self, query, *args, **kwargs):
            statements.append(str(query))
            return self._conn.execute(query, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._conn, name)

    @contextmanager
    def counting_db_conn():
        with original() as conn:
            yield CountingConnection(conn)

    for name, module in list(sys.modules.items()):
        if name.startswith("app") and getattr(module, "db_conn", None) is original:
            monkeypatch.setattr(module, "db_conn", counting_db_conn)
    yield statements


def count_summary(owner, monkeypatch):
    with monkeypatch.context() as patch:
        with counted_statements(patch) as statements:
            owner.summary()
            return len(statements)


def test_statement_count_does_not_grow_with_rows_plans_or_goals(owner, monkeypatch):
    owner.regular_salaries(today() - timedelta(days=5))
    owner.expense(10_000, "makan")
    owner.plan("plan_0", "Rencana Contoh 0", 10_000, today() + timedelta(days=2))
    target = shift_months(today(), 4)
    owner.goal("goal_0", "Target Contoh 0", 1_000_000, target, accounts=["pocket"])
    waiting = owner.expense(5_000, notes="QRIS contoh", review_status="needs_review")
    waiting_event(owner, waiting, reasons=["purpose_question"])
    baseline = count_summary(owner, monkeypatch)
    assert baseline > 0

    for index in range(39):
        owner.expense(1_000 + index, "jajan", review_status="needs_review" if index % 3 else "reviewed")
    assert count_summary(owner, monkeypatch) == baseline

    for index in range(1, 5):
        owner.plan(f"plan_{index}", f"Rencana Contoh {index}", 10_000, today() + timedelta(days=2 + index))
    assert count_summary(owner, monkeypatch) == baseline

    for index in range(1, 4):
        owner.goal(f"goal_{index}", f"Target Contoh {index}", 1_000_000, target)
    data = owner.summary()
    assert len([line for line in data["scheduled"]["lines"] if line["kind"] == "goal"]) == 4
    assert len([line for line in data["scheduled"]["lines"] if line["kind"] == "plan"]) == 5
    assert count_summary(owner, monkeypatch) == baseline
