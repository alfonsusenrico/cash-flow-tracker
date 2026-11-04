"""Laporan Bulanan: sections, report = ledger, comparison, data quality, reflections and readiness.

Synthetic data only. Months are relative to today in the application timezone, so the latest closed
month is always "last month" (M1) and tracking begins on the 15th of the month before (M2).
"""
from datetime import date, datetime, time, timedelta
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest

TZ = ZoneInfo("Asia/Jakarta")
MONTHS = ("Januari", "Februari", "Maret", "April", "Mei", "Juni", "Juli", "Agustus", "September", "Oktober",
          "November", "Desember")
CATEGORIES = [
    ("groceries", "Belanja Dapur", "expense", "need", False, False),
    ("dinner", "Makan Bareng", "expense", "want", False, False),
    ("misc", "Lain-lain", "expense", None, False, False),
    ("repair", "Servis", "expense", "unexpected", False, False),
    ("saving", "Dana Sosial", "expense", "saving", False, False),
    ("investment", "Investasi", "expense", None, False, False),
    ("salary", "Gaji", "income", None, False, False),
    ("bonus", "Pendapatan Lain", "income", None, False, False),
    ("reimbursement", "Patungan & Ganti Uang", "income", None, True, False),
]


def month_start(offset: int) -> date:
    today = datetime.now(TZ).date()
    index = today.year * 12 + today.month - 1 + offset
    return date(index // 12, index % 12 + 1, 1)


def month_end(start: date) -> date:
    following = month_start(0) if start == month_start(-1) else date(start.year + start.month // 12, start.month % 12 + 1, 1)
    return following - timedelta(days=1)


def label(start: date) -> str:
    return f"{MONTHS[start.month - 1]} {start.year}"


def at(day: date, hour: int = 12, minute: int = 0) -> datetime:
    return datetime.combine(day, time(hour, minute), TZ)


def make_owner(user_id: str):
    from app.db.pool import db_conn

    ids = {key: str(uuid4()) for key, *_ in CATEGORIES}
    ids.update(cash=str(uuid4()), pocket=str(uuid4()))
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO users (id, username, name, password_hash, invite_code) VALUES (%s, %s, 'Rani Contoh', 'synthetic-hash', 'TEST')",
            (user_id, f"report_{uuid4().hex}"),
        )
        for key, name in (("cash", "Rekening Contoh"), ("pocket", "Kantong Contoh")):
            conn.execute(
                "INSERT INTO accounts (id, user_id, name, type, initial_balance) VALUES (%s, %s, %s, 'bank', 20000000)",
                (ids[key], user_id, name),
            )
        for key, name, kind, pillar, offsets, excluded in CATEGORIES:
            conn.execute(
                """INSERT INTO categories (id, user_id, name, kind, kakeibo_type, offsets_spending, is_excluded_from_budget)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (ids[key], user_id, name, kind, pillar, offsets, excluded),
            )
        conn.commit()
    return {"user_id": user_id, **ids}


def as_user(owner, **extra):
    from app.main import app
    from app.services.auth import get_current_user

    app.dependency_overrides[get_current_user] = lambda: {
        "id": owner["user_id"], "username": "report-owner", "name": "Rani Contoh", "currency": "IDR",
        "payday_day": 25, "monthly_spending_budget": None, "emergency_fund_multiplier": 6, **extra,
    }


@pytest.fixture
def owners(client):
    from app.db.pool import db_conn
    from app.main import app

    owner, stranger = make_owner(str(uuid4())), make_owner(str(uuid4()))
    as_user(owner)
    yield owner, stranger
    app.dependency_overrides.clear()
    with db_conn() as conn:
        conn.execute("DELETE FROM users WHERE id = ANY(%s)", ([owner["user_id"], stranger["user_id"]],))
        conn.commit()


def insert(owner, category, amount, when, *, tx_type="expense", account="cash", pillar=None, offset=None,
           merchant=None, review_status="reviewed"):
    from app.db.pool import db_conn

    with db_conn() as conn:
        row = conn.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, date, kakeibo_type,
                   offset_category_id, merchant, notes, source, review_status)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'manual', %s) RETURNING id""",
            (owner["user_id"], owner[account], owner[category] if category else None, tx_type, amount, when, pillar,
             owner[offset] if offset else None, merchant, merchant or "Contoh", review_status),
        ).fetchone()
        conn.commit()
    return str(row["id"])


def fee_movement(owner, when):
    """A Rp 102.500 send paired with a Rp 100.000 credit: a movement and a Rp 2.500 fee expense."""
    from app.db.pool import db_conn
    from app.services.movement_pairing import link_pair

    expense = insert(owner, None, 102_500, when)
    income = insert(owner, None, 100_000, when + timedelta(minutes=1), tx_type="income", account="pocket")
    with db_conn() as conn:
        with conn.cursor() as cur:
            linked = link_pair(cur, owner["user_id"], expense, income, fee=2_500, notes=None)
        conn.commit()
    assert linked["fee_transaction_id"]


def seed_two_months(client, owner):
    """Last month (M1) with every kind of row, and the 15th onwards of the month before (M2)."""
    m1, m2 = month_start(-1), month_start(-2)
    insert(owner, "groceries", 100_000, at(m2.replace(day=15)))
    insert(owner, "salary", 5_000_000, at(m1, 9), tx_type="income")
    insert(owner, "bonus", 250_000, at(m1.replace(day=20)), tx_type="income")
    insert(owner, "groceries", 300_000, at(m1.replace(day=2)), merchant="Pasar Contoh 12")
    insert(owner, "groceries", 25_000, at(m1.replace(day=3), 8, 0), merchant="Warung Contoh")
    insert(owner, "groceries", 25_000, at(m1.replace(day=3), 8, 2), merchant="Warung Contoh")
    insert(owner, "dinner", 200_000, at(m1.replace(day=4)), merchant="Resto Contoh")
    insert(owner, "reimbursement", 50_000, at(m1.replace(day=5)), tx_type="income", offset="dinner")
    insert(owner, "misc", 100_000, at(m1.replace(day=6)))
    insert(owner, None, 50_000, at(m1.replace(day=7)))
    insert(owner, "repair", 80_000, at(m1.replace(day=8)), merchant="Bengkel Contoh")
    insert(owner, "saving", 40_000, at(m1.replace(day=9)))
    insert(owner, "investment", 500_000, at(m1.replace(day=10)))
    insert(owner, "groceries", 60_000, at(m1.replace(day=11)), review_status="needs_review")
    fee_movement(owner, at(m1.replace(day=12)))
    moved = client.post("/api/movements", json={
        "source_account_id": owner["cash"], "target_account_id": owner["pocket"], "amount": 25_000,
        "date": at(m1.replace(day=3), 8, 1).isoformat(),
    })
    assert moved.status_code == 200, moved.text


def report(client, **params):
    response = client.get("/api/reports/monthly", params=params)
    assert response.status_code == 200, response.text
    return response.json()


def ledger_summary(client, period, **filters):
    response = client.get("/api/transactions", params={
        "from_date": period["start"], "to_date": period["end"], "limit": 1, **filters,
    })
    assert response.status_code == 200, response.text
    return response.json()["summary"]["period"]


def test_default_report_is_last_closed_month_with_the_four_answers(client, owners):
    owner, _ = owners
    seed_two_months(client, owner)
    m1 = month_start(-1)
    body = report(client)

    assert body["period"]["label"] == label(m1)
    assert (body["period"]["start_date"], body["period"]["end_date"]) == (m1.isoformat(), month_end(m1).isoformat())
    assert body["period"]["complete"] is True and body["period"]["tracking_started_inside"] is False
    assert body["income"]["total"] == 5_250_000
    assert [(item["name"], item["amount"]) for item in body["income"]["sources"]] == [("Gaji", 5_000_000), ("Pendapatan Lain", 250_000)]

    # 300 + 25 + 25 + 60 + 2.5 fee (need) + 150 want after the reimbursement + 150 unclassified + 80 + 40.
    pillars = {item["pillar"]: item["amount"] for item in body["spending"]["pillars"]}
    assert pillars == {"need": 412_500, "want": 150_000, "culture": 0, "unexpected": 80_000, "saving": 40_000, "unclassified": 150_000}
    assert body["spending"]["total"] == body["flow"]["outflow"] == sum(pillars.values()) == 832_500
    assert body["flow"]["reimbursement_offset"] == 50_000
    assert body["savings"] == {"fresh_saving": 40_000, "saving_expenses": 40_000, "net_saving_transfers": 0, "surplus": 5_250_000 - 832_500}

    top = [(item["name"], item["amount"]) for item in body["spending"]["top_categories"]]
    assert top == [("Belanja Dapur", 410_000), ("Makan Bareng", 150_000), ("Lain-lain", 100_000), ("Servis", 80_000), ("Tanpa kategori", 50_000)]
    assert body["spending"]["top_merchants"][0]["name"] == "Pasar Contoh 12"
    assert body["spending"]["unexpected"] == {
        "amount": 80_000, "count": 1,
        "items": [{**body["spending"]["unexpected"]["items"][0], "amount": 80_000, "merchant": "Bengkel Contoh"}],
    }
    assert body["spending"]["essential"]["as_of"] == "now"
    assert body["reflection"] is None and body["previous_reflection"] is None


def test_report_equals_the_ledger_for_the_same_window(client, owners):
    owner, _ = owners
    seed_two_months(client, owner)
    body = report(client)
    period = body["period"]

    total = ledger_summary(client, period)
    for key in ("inflow", "gross_outflow", "reimbursement_offset", "outflow", "net"):
        assert total[key] == body["flow"][key], key
    for item in body["spending"]["pillars"]:
        assert ledger_summary(client, period, type="expense", kakeibo_type=item["pillar"])["outflow"] == item["amount"], item
    breakdown = client.get("/api/dashboard/overview", params={"timeframe": "month", "cycle_offset": -1}).json()["categories"]
    assert sum(row["spent"] for row in breakdown) == body["flow"]["outflow"]
    for item in body["spending"]["top_categories"]:
        filters = {"uncategorized": "true"} if item["is_uncategorized"] else {"category_id": item["category_id"]}
        assert ledger_summary(client, period, type="expense", **filters)["outflow"] == item["amount"], item
    for source in body["income"]["sources"]:
        assert ledger_summary(client, period, type="income", category_id=source["category_id"])["inflow"] == source["amount"]


def test_period_list_includes_the_partial_first_month(client, owners):
    owner, _ = owners
    seed_two_months(client, owner)
    listing = client.get("/api/reports/monthly/periods").json()
    m1, m2 = month_start(-1), month_start(-2)
    assert [item["start_date"] for item in listing["periods"]] == [month_start(0).isoformat(), m1.isoformat(), m2.isoformat()]
    assert listing["periods"][0]["complete"] is False
    assert listing["periods"][2]["tracking_started_inside"] is True
    assert listing["periods"][2]["first_recorded_date"] == m2.replace(day=15).isoformat()
    assert listing["cycle_available"] is False

    cycle = client.get("/api/reports/monthly", params={"basis": "cycle"})
    assert cycle.status_code == 409 and cycle.json()["detail"]["code"] == "cycle_unavailable"
    unknown = client.get("/api/reports/monthly", params={"start_date": m1.replace(day=2).isoformat()})
    assert unknown.status_code == 404 and unknown.json()["detail"]["code"] == "period_not_found"
    assert client.get("/api/reports/monthly", params={"basis": "week"}).status_code == 422


def test_comparison_marks_a_partial_previous_month_and_has_no_baseline_yet(client, owners):
    owner, _ = owners
    seed_two_months(client, owner)
    comparison = report(client)["comparison"]
    previous = comparison["previous"]
    assert previous["partial"] is True and previous["tracking_from"] == month_start(-2).replace(day=15).isoformat()
    assert previous["totals"]["spending"] == 100_000
    # A partial previous month gets no verdict, and with no whole earlier month there is no baseline.
    assert set(previous["change"].values()) == {None}
    assert comparison["baseline"] is None


def test_report_comparison_is_the_comparison_route(client, owners):
    owner, _ = owners
    seed_two_months(client, owner)
    from_report = report(client)["comparison"]
    routed = client.get("/api/reports/monthly/comparison").json()["comparison"]

    def without_clock(comparison):
        # The running month ends at the request instant, which differs between the two calls.
        return {**comparison, "history": [{**item, "end": None} for item in comparison["history"]]}

    assert without_clock(routed) == without_clock(from_report)


def test_data_quality_counts_duplicates_review_unclassified_and_empty_days(client, owners):
    owner, _ = owners
    seed_two_months(client, owner)
    body = report(client)
    quality = body["data_quality"]
    assert quality["possible_duplicates"] == 2
    # The flagged grocery row needs attention; the automatic transfer-fee row still waits in Tinjau too.
    assert (quality["needs_attention"], quality["unreviewed"], quality["pending_review"]) == (1, 1, 2)
    assert quality["unclassified"] == {"count": 2, "amount": 150_000}
    recorded_days = {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 20, 1}
    m1 = month_start(-1)
    expected = [day.isoformat() for day in (m1 + timedelta(days=i) for i in range((month_end(m1) - m1).days + 1)) if day.day not in recorded_days]
    assert quality["days_without_records"] == {"count": len(expected), "dates": expected}

    duplicates = client.get("/api/transactions", params={
        "from_date": body["period"]["start"], "to_date": body["period"]["end"], "possible_duplicates": "true",
    }).json()["transactions"]
    assert sorted(row["amount"] for row in duplicates) == [25_000, 25_000]
    assert all(row["movement_id"] is None for row in duplicates)


def test_reflection_crud_is_owner_scoped_and_shown_again_next_month(client, owners):
    owner, stranger = owners
    seed_two_months(client, owner)
    m1, m2 = month_start(-1), month_start(-2)
    saved = client.put("/api/reports/monthly/reflection", json={"basis": "month", "start_date": m2.isoformat(), "note": "  Kurangi jajan kopi "})
    assert saved.status_code == 200, saved.text
    assert saved.json()["reflection"]["note"] == "Kurangi jajan kopi"
    body = report(client, start_date=m1.isoformat())
    assert body["previous_reflection"]["note"] == "Kurangi jajan kopi"
    assert body["previous_reflection"]["label"] == label(m2)

    updated = client.put("/api/reports/monthly/reflection", json={"basis": "month", "start_date": m1.isoformat(), "note": "Masak di rumah"})
    assert updated.status_code == 200
    assert report(client, start_date=m1.isoformat())["reflection"]["note"] == "Masak di rumah"
    listing = client.get("/api/reports/monthly/periods").json()["periods"]
    assert [item["has_reflection"] for item in listing] == [False, True, True]

    for note in ("   ", "x" * 1001):
        bad = client.put("/api/reports/monthly/reflection", json={"basis": "month", "start_date": m1.isoformat(), "note": note})
        assert bad.status_code == 422
    unknown = client.put("/api/reports/monthly/reflection", json={"basis": "month", "start_date": month_start(-9).isoformat(), "note": "x"})
    assert unknown.status_code == 404

    as_user(stranger)
    insert(stranger, "groceries", 10_000, at(m1))
    assert report(client, start_date=m1.isoformat())["reflection"] is None
    missing = client.delete("/api/reports/monthly/reflection", params={"basis": "month", "start_date": m1.isoformat()})
    assert missing.status_code == 404
    as_user(owner)
    assert report(client, start_date=m1.isoformat())["reflection"]["note"] == "Masak di rumah"
    removed = client.delete("/api/reports/monthly/reflection", params={"basis": "month", "start_date": m1.isoformat()})
    assert removed.status_code == 200
    assert report(client, start_date=m1.isoformat())["reflection"] is None


def test_budget_is_hidden_without_one_and_compared_once_set(client, owners):
    owner, _ = owners
    seed_two_months(client, owner)
    assert report(client)["budget"] is None
    patched = client.patch(f"/api/categories/{owner['dinner']}", json={"monthly_budget": 100_000})
    assert patched.status_code == 200, patched.text
    budget = report(client)["budget"]
    assert budget["categories"] == [{"category_id": owner["dinner"], "name": "Makan Bareng", "budget": 100_000, "spent": 150_000, "remaining": -50_000}]
    assert budget["overall"] is None and budget["period_basis"] == "month"

    as_user(owner, monthly_spending_budget=1_000_000)
    assert report(client)["budget"]["overall"] == {"budget": 1_000_000, "spent": 832_500, "remaining": 167_500}


def test_salary_cycle_reports_only_closed_cycles(client, owners):
    owner, _ = owners
    from app.db.pool import db_conn

    first, second, third = (at(month_start(offset).replace(day=25), 9) for offset in (-3, -2, -1))
    for when in (first, second, third):
        salary = insert(owner, "salary", 5_000_000, when, tx_type="income")
        with db_conn() as conn:
            conn.execute(
                "INSERT INTO salary_receipts (user_id, transaction_id, confirmation_method) VALUES (%s, %s, 'owner')",
                (owner["user_id"], salary),
            )
            conn.commit()
    insert(owner, "groceries", 70_000, second + timedelta(days=3))
    insert(owner, "groceries", 9_000, third + timedelta(days=1))  # in the open cycle

    listing = client.get("/api/reports/monthly/periods", params={"basis": "cycle"}).json()
    assert listing["cycle_available"] is True
    assert [(item["kind"], item["complete"]) for item in listing["periods"]] == [("cycle", False), ("cycle", True), ("cycle", True)]
    body = report(client, basis="cycle")
    assert body["period"]["start_date"] == second.date().isoformat()
    # A cycle ends the instant before the next receipt, so both share that local day.
    assert body["period"]["end_date"] == third.date().isoformat()
    assert body["flow"]["outflow"] == 70_000 and body["income"]["total"] == 5_000_000
    assert body["comparison"]["previous"]["start_date"] == first.date().isoformat()
    assert client.get("/api/reports/monthly/latest").json()["basis"] == "cycle"


def test_latest_readiness_session_equals_bearer(client, owners):
    from fastapi.testclient import TestClient

    from app.db.pool import db_conn
    from app.main import app
    from app.services.auth import get_current_user, hash_token

    owner, _ = owners
    seed_two_months(client, owner)
    session_view = client.get("/api/reports/monthly/latest").json()
    m1 = month_start(-1)
    assert session_view["ready"] is True and session_view["basis"] == "month"
    assert session_view["period"]["label"] == label(m1) and session_view["period"]["start_date"] == m1.isoformat()
    assert session_view["url"] == f"/laporan?basis=month&start={m1.isoformat()}"
    assert session_view["pending_review"] == 1

    plain = f"synthetic-key-{uuid4().hex}"
    with db_conn() as conn:
        conn.execute("INSERT INTO api_keys (user_id, key_hash, key_prefix) VALUES (%s, %s, 'synthet')", (owner["user_id"], hash_token(plain)))
        conn.commit()
    app.dependency_overrides.pop(get_current_user)
    phone = TestClient(app, base_url="https://testserver", headers={"Authorization": f"Bearer {plain}"})
    assert phone.get("/api/reports/monthly/latest").json() == session_view
    anonymous = TestClient(app, base_url="https://testserver", headers={"Authorization": "Bearer synthetic-unknown"})
    assert anonymous.get("/api/reports/monthly/latest").status_code == 401


def test_first_month_is_not_ready_and_an_empty_ledger_has_no_report(client, owners):
    owner, _ = owners
    empty = report(client)
    assert empty["empty"] is True
    insert(owner, "groceries", 10_000, datetime.now(TZ) - timedelta(minutes=1))
    latest = client.get("/api/reports/monthly/latest").json()
    assert (latest["ready"], latest["period"], latest["url"]) == (False, None, None)
    current = report(client)
    assert current["period"]["complete"] is False and current["period"]["start_date"] == month_start(0).isoformat()
