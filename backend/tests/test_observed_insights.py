"""Observed Analisis insights: fair comparison, cash flow history and recurring charges; synthetic data."""
from datetime import date, datetime, time, timedelta, timezone
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest

from app.services.observed_insights import detect_charge, merchant_key, search_text

JAKARTA = ZoneInfo("Asia/Jakarta")


@pytest.fixture
def shopper(client):
    from app.db.pool import db_conn
    from app.main import app
    from app.services.auth import get_current_user

    user_id = str(uuid4())
    ids = {name: str(uuid4()) for name in ("cash", "groceries", "fun", "bills", "salary")}
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO users (id, username, name, password_hash, invite_code) VALUES (%s, %s, 'Tono Contoh', 'synthetic-hash', 'TEST')",
            (user_id, f"insights_{uuid4().hex}"),
        )
        conn.execute(
            "INSERT INTO accounts (id, user_id, name, type, initial_balance) VALUES (%s, %s, 'Kas Contoh', 'bank', 50000000)",
            (ids["cash"], user_id),
        )
        for key, name, kind, pillar in (
            ("groceries", "Belanja Contoh", "expense", "need"), ("fun", "Hiburan Contoh", "expense", "want"),
            ("bills", "Langganan Contoh", "expense", "need"), ("salary", "Gaji Contoh", "income", None),
        ):
            conn.execute(
                "INSERT INTO categories (id, user_id, name, kind, kakeibo_type) VALUES (%s, %s, %s, %s, %s)",
                (ids[key], user_id, name, kind, pillar),
            )
        conn.commit()
    app.dependency_overrides[get_current_user] = lambda: {
        "id": user_id, "username": "insights-owner", "name": "Tono Contoh", "currency": "IDR", "payday_day": 25,
    }
    yield {"user_id": user_id, **ids}
    app.dependency_overrides.clear()
    with db_conn() as conn:
        conn.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()


def insert(owner, category, amount, when, *, merchant=None, tx_type="expense"):
    from app.db.pool import db_conn

    with db_conn() as conn:
        row = conn.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, date, notes, merchant)
               VALUES (%s, %s, %s, %s, %s, %s, 'Transaksi contoh', %s) RETURNING id""",
            (owner["user_id"], owner["cash"], owner[category], tx_type, amount, when, merchant),
        ).fetchone()
        conn.commit()
    return str(row["id"])


def local(day: date, hour: int = 12) -> datetime:
    return datetime.combine(day, time(hour), JAKARTA)


def compare(owner, current, previous):
    from app.db.pool import db_conn
    from app.services.observed_insights import period_comparison

    with db_conn() as conn:
        with conn.cursor() as cur:
            return period_comparison(cur, owner["user_id"], current=current, previous=previous)


def test_comparison_uses_the_same_elapsed_days(client, shopper):
    insert(shopper, "groceries", 100_000, local(date(2026, 3, 5)), merchant="Pasar Contoh")
    insert(shopper, "fun", 900_000, local(date(2026, 3, 20)), merchant="Bioskop Contoh")
    insert(shopper, "groceries", 250_000, local(date(2026, 4, 3)), merchant="Pasar Contoh")
    insert(shopper, "fun", 40_000, local(date(2026, 4, 9)), merchant="Bioskop Contoh")
    current = (local(date(2026, 4, 1), 0).astimezone(timezone.utc), local(date(2026, 4, 10), 18).astimezone(timezone.utc), "April 2026")
    previous = (local(date(2026, 3, 1), 0).astimezone(timezone.utc),
                datetime.combine(date(2026, 3, 31), time.max, JAKARTA).astimezone(timezone.utc), "Maret 2026")

    result = compare(shopper, current, previous)
    assert result["compared_days"] == 10
    assert result["previous_has_data"] is True
    # March is measured over its first 9 days and 18 hours too, so the 20 March film is left out.
    assert (result["current"]["outflow"], result["previous"]["outflow"]) == (290_000, 100_000)
    assert result["previous"]["end_date"] == "2026-03-10"
    pillars = {item["pillar"]: item for item in result["pillars"]}
    assert (pillars["need"]["current"], pillars["need"]["previous"], pillars["need"]["change"]) == (250_000, 100_000, 150_000)
    assert (pillars["want"]["change"], pillars["saving"]["change"]) == (40_000, 0)
    increases = result["categories"]["increases"]
    assert [(item["name"], item["change"]) for item in increases] == [("Belanja Contoh", 150_000), ("Hiburan Contoh", 40_000)]
    assert result["merchants"]["increases"][0]["name"] == "Pasar Contoh"
    assert result["current_truncated"] is False


def test_comparison_states_when_the_previous_period_has_no_data(client, shopper):
    insert(shopper, "groceries", 100_000, local(date(2026, 4, 3)))
    current = (local(date(2026, 4, 1), 0).astimezone(timezone.utc), local(date(2026, 4, 10)).astimezone(timezone.utc), "April 2026")
    previous = (local(date(2026, 3, 1), 0).astimezone(timezone.utc), local(date(2026, 3, 31)).astimezone(timezone.utc), "Maret 2026")
    result = compare(shopper, current, previous)
    assert result["previous_has_data"] is False
    assert all(item["previous"] is None and item["change"] is None for item in result["pillars"])
    assert result["categories"]["decreases"] == []

    unavailable = compare(shopper, current, None)
    assert unavailable["previous"] is None and unavailable["previous_has_data"] is False


def test_a_shorter_previous_cycle_shortens_both_windows(client, shopper):
    start = local(date(2026, 4, 1), 0).astimezone(timezone.utc)
    current = (start, start + timedelta(days=30), "Siklus")
    previous = (start - timedelta(days=28), start - timedelta(microseconds=1), "Siklus lalu")
    result = compare(shopper, current, previous)
    assert result["current_truncated"] is True
    assert result["compared_days"] == 28


def test_comparison_endpoint_returns_its_windows(client, shopper):
    insert(shopper, "groceries", 50_000, datetime.now(JAKARTA) - timedelta(minutes=5))
    response = client.get("/api/dashboard/period-comparison?timeframe=month")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["current"]["outflow"] == 50_000 and body["timezone"]
    assert body["previous_label"] and body["compared_days"] >= 1


def test_history_lists_completed_periods_and_the_one_in_progress(client, shopper):
    today = datetime.now(JAKARTA).date()
    first = date(today.year - (1 if today.month <= 7 else 0), (today.month - 8) % 12 + 1, 1)
    insert(shopper, "salary", 5_000_000, local(first, 8), tx_type="income")
    insert(shopper, "groceries", 1_250_000, local(first + timedelta(days=40)))
    insert(shopper, "groceries", 75_000, datetime.now(JAKARTA) - timedelta(minutes=5))

    body = client.get("/api/dashboard/cash-flow-history?timeframe=month").json()
    periods = body["periods"]
    assert len(periods) == 7
    assert [item["in_progress"] for item in periods] == [False] * 6 + [True]
    assert periods[-1]["outflow"] == 75_000
    assert sum(item["outflow"] for item in periods[:-1]) == 1_250_000
    assert all(item["net"] == item["inflow"] - item["outflow"] for item in periods)
    assert body["cycle_status"] == "not_configured"


def charge_rows(days, amounts):
    return [
        {"day": day, "amount": amount, "account_id": "a", "account_name": "Kas", "category_id": None, "category_name": None}
        for day, amount in zip(days, amounts)
    ]


def test_detection_rules_on_synthetic_series():
    today = date(2026, 10, 30)
    monthly = charge_rows([date(2026, 6, 28), date(2026, 7, 28), date(2026, 8, 28), date(2026, 9, 28), date(2026, 10, 28)], [54_990] * 5)
    found = detect_charge(monthly, today)
    assert (found["cadence"], found["typical_amount"], found["next_expected_date"]) == ("monthly", 54_990, "2026-11-28")
    assert found["next_date_is_estimate"] is True

    weekly = charge_rows([today - timedelta(days=7 * n) for n in range(5, -1, -1)], [25_000] * 6)
    found_weekly = detect_charge(weekly, today)
    assert found_weekly["cadence"] == "weekly"
    assert found_weekly["monthly_equivalent"] == round(25_000 * 52 / 12)

    irregular = charge_rows([date(2026, 9, 1), date(2026, 9, 4), date(2026, 9, 19), date(2026, 10, 2), date(2026, 10, 25)],
                            [120_000, 340_000, 95_000, 410_000, 210_000])
    assert detect_charge(irregular, today) is None
    steady_dates_varying_amounts = charge_rows([date(2026, 8, 1), date(2026, 9, 1), date(2026, 10, 1)], [100_000, 160_000, 60_000])
    assert detect_charge(steady_dates_varying_amounts, today) is None
    two = charge_rows([date(2026, 9, 28), date(2026, 10, 28)], [54_990] * 2)
    assert detect_charge(two, today) is None
    ended = charge_rows([date(2026, 4, 1), date(2026, 5, 1), date(2026, 6, 1)], [30_000] * 3)
    assert detect_charge(ended, today) is None

    price_change = charge_rows([date(2026, 6, 15), date(2026, 7, 15), date(2026, 8, 15), date(2026, 9, 15), date(2026, 10, 15)],
                               [50_000, 50_000, 50_000, 50_000, 60_000])
    changed = detect_charge(price_change, today)
    assert changed["amount_changed"] is True
    assert (changed["previous_amount"], changed["last_amount"]) == (50_000, 60_000)


def test_merchant_key_groups_reference_numbers_and_case():
    assert merchant_key("NETFLIX.COM 123456") == merchant_key("Netflix.com 987")
    assert merchant_key("  ") is None
    assert search_text("NETFLIX.COM 123456") == "NETFLIX.COM"


def test_recurring_charges_mark_plans_and_become_non_posting_plans(client, shopper):
    from app.db.pool import db_conn

    now = datetime.now(JAKARTA)
    for months_back in range(4, -1, -1):
        insert(shopper, "bills", 54_990, now - timedelta(days=30 * months_back + 1), merchant=f"Musik Contoh {months_back}")
    for weeks_back in range(5, -1, -1):
        insert(shopper, "fun", 25_000, now - timedelta(days=7 * weeks_back + 1), merchant="Kopi Contoh")
    for offset, amount in ((3, 120_000), (9, 340_000), (24, 95_000), (37, 410_000), (60, 210_000)):
        insert(shopper, "groceries", amount, now - timedelta(days=offset), merchant="Pasar Contoh")

    body = client.get("/api/dashboard/recurring-charges").json()
    charges = {item["key"]: item for item in body["charges"]}
    assert set(charges) == {"musik contoh", "kopi contoh"}
    music = charges["musik contoh"]
    assert (music["cadence"], music["typical_amount"], music["existing_plan"]) == ("monthly", 54_990, None)
    assert music["search_text"] == "Musik Contoh"
    assert charges["kopi contoh"]["cadence"] == "weekly"
    assert body["monthly_equivalent_total"] == 54_990 + round(25_000 * 52 / 12)
    assert body["rules"]["max_amount_variation"] == 0.15 and body["window"]["months"] == 6

    with db_conn() as conn:
        before = conn.execute("SELECT COUNT(*) AS n FROM transactions WHERE user_id = %s", (shopper["user_id"],)).fetchone()["n"]
    draft = music["plan_draft"]
    created = client.post("/api/recurring", json=draft)
    assert created.status_code == 200, created.text
    rule = next(item for item in client.get("/api/recurring").json()["rules"] if item["id"] == created.json()["id"])
    assert rule["auto_post"] is False and rule["schedule_type"] == "monthly_day" and rule["amount"] == 54_990
    with db_conn() as conn:
        after = conn.execute("SELECT COUNT(*) AS n FROM transactions WHERE user_id = %s", (shopper["user_id"],)).fetchone()["n"]
    assert after == before

    marked = {item["key"]: item for item in client.get("/api/dashboard/recurring-charges").json()["charges"]}
    assert marked["musik contoh"]["existing_plan"]["id"] == created.json()["id"]
    assert marked["kopi contoh"]["existing_plan"] is None
    assert client.get("/api/dashboard/recurring-charges").json()["monthly_equivalent_without_plan"] == round(25_000 * 52 / 12)
