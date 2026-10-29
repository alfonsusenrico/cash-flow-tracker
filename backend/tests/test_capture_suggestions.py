"""Capture suggestions from the owner's own history: ranking, exclusions, prefill text and boundaries.

All texts, names and amounts are synthetic. No model is reached.
"""
from datetime import datetime, time, timedelta, timezone
from urllib.parse import quote
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest

from app.services.capture_parsing import parse_amount_answer, parse_kind
from app.services.capture_suggestions import amount_shorthand, built_text
from test_notification_processing_database import notification_owner  # noqa: F401 - fixture
from test_text_capture import captured, owner, remember  # noqa: F401 - fixture

WEEKDAYS = {1, 2, 3, 4, 5}
WEEKEND = {6, 7}


def local_zone():
    from app.core.config import settings

    return ZoneInfo(settings.tz)


def latest_past(days: set[int], at: time) -> datetime:
    """The latest moment at this local time, on one of these ISO weekdays, that is not in the future."""
    now = datetime.now(local_zone())
    day = now.date()
    while True:
        moment = datetime.combine(day, at, local_zone())
        if moment <= now and moment.isoweekday() in days:
            return moment
        day -= timedelta(days=1)


def previous_days(before: datetime, days: set[int], count: int, at: time) -> list[datetime]:
    moments, day = [], before.date() - timedelta(days=1)
    while len(moments) < count:
        if day.isoweekday() in days:
            moments.append(datetime.combine(day, at, local_zone()))
        day -= timedelta(days=1)
    return moments


def record(user_id, account_id, moment, notes, amount, *, category_id=None, merchant=None, kind="expense",
           source="manual", review="reviewed", fee_for=None):
    from app.db.pool import db_conn

    with db_conn() as conn:
        conn.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, merchant, date, source,
                                         review_status, classified_by, fee_for_movement_id)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'owner', %s)""",
            (user_id, account_id, category_id, kind, amount, notes, merchant, moment, source, review, fee_for),
        )
        conn.commit()


def suggestions(client, device_time):
    response = client.get(f"/api/capture/suggestions?device_time={quote(device_time.isoformat())}")
    assert response.status_code == 200, response.text
    assert response.json()["ok"] is True
    return response.json()["suggestions"]


def labels(items):
    return [item["label"] for item in items]


# --- Ranking ----------------------------------------------------------------------------------


def test_morning_habit_ranks_first_and_time_of_day_changes_the_order(client, owner):
    day = latest_past(WEEKDAYS, time(15, 30)).date()
    morning = datetime.combine(day, time(8, 30), local_zone())
    afternoon = datetime.combine(day, time(15, 30), local_zone())
    assert suggestions(client, morning) == []

    remember(owner, "merchant:parkir ptc", category="parkir", account="cash", notes="Parkir PTC", merchant="Parkir PTC")
    for moment in previous_days(morning, WEEKDAYS, 5, time(8, 0)):
        assert captured(client, "parkir ptc 10rb", device_time=moment)["status"] == "recorded"
    for index, moment in enumerate(previous_days(morning, WEEKDAYS, 6, time(15, 0))):
        record(owner["user_id"], owner["gopay"], moment, "Kopi Senja", 35_000 if index == 5 else 32_000,
               category_id=owner["jajan"])

    first, second = suggestions(client, morning)
    assert first == {"text": "parkir ptc 10rb", "label": "Parkir PTC", "amount": 10_000, "category_name": "Parkir",
                     "account_name": "Tunai", "reason": "biasanya jam segini"}
    assert second == {"text": "kopi senja 32rb", "label": "Kopi Senja", "amount": 32_000,
                      "category_name": "Jajan & Nongkrong", "account_name": "GoPay", "reason": "sering"}
    later = suggestions(client, afternoon)
    assert labels(later) == ["Kopi Senja", "Parkir PTC"] and later[0]["reason"] == "biasanya jam segini"


def test_weekday_type_decides_between_equal_habits(client, owner):
    saturday = latest_past({6}, time(10, 0))
    for days_back in (6, 7):
        record(owner["user_id"], owner["cash"], saturday - timedelta(days=days_back), "Laundry", 20_000,
               category_id=owner["makan"])
    for days_back in (1, 2):
        record(owner["user_id"], owner["cash"], saturday - timedelta(days=days_back), "Fotokopi", 20_000,
               category_id=owner["makan"])
    assert labels(suggestions(client, saturday)) == ["Laundry", "Fotokopi"]
    friday = saturday - timedelta(days=1) + timedelta(hours=1)
    assert labels(suggestions(client, friday)) == ["Fotokopi", "Laundry"]


def test_bank_notified_movement_fee_and_unreviewed_rows_are_not_suggested(client, owner):
    reference = latest_past(WEEKDAYS, time(12, 0))
    for days_back in range(1, 9):
        record(owner["user_id"], owner["bca"], reference - timedelta(days=days_back, minutes=30), "Kedai Awan",
               48_000, merchant="Kedai Awan", category_id=owner["jajan"], source="notification",
               review="reviewed" if days_back % 2 else "auto")
    record(owner["user_id"], owner["cash"], reference - timedelta(hours=2), "Kedai Awan", 51_000,
           merchant="Kedai Awan", category_id=owner["jajan"], review="needs_review")
    record(owner["user_id"], owner["bca"], reference - timedelta(hours=2), "Biaya transfer", 2_500,
           category_id=owner["expense"], fee_for=str(uuid4()))
    assert captured(client, "pindah 50rb ke GoPay dari bca harian",
                    device_time=reference - timedelta(hours=1))["status"] == "recorded"
    assert suggestions(client, reference) == []

    record(owner["user_id"], owner["cash"], reference - timedelta(days=1), "Kedai Awan", 30_000,
           merchant="Kedai Awan", category_id=owner["jajan"])
    [only] = suggestions(client, reference)
    assert (only["label"], only["amount"], only["account_name"], only["reason"]) == (
        "Kedai Awan", 30_000, "Tunai", "biasanya jam segini")


def test_a_group_whose_last_entry_was_undone_is_skipped(client, owner):
    reference = latest_past(WEEKDAYS, time(9, 0))
    remember(owner, "merchant:parkir ptc", category="parkir", account="cash", notes="Parkir PTC")
    assert captured(client, "parkir ptc 10rb", device_time=reference - timedelta(days=3))["status"] == "recorded"
    regret = captured(client, "parkir ptc 10rb", device_time=reference - timedelta(minutes=5))
    assert client.post(f"/api/capture/{regret['id']}/undo").status_code == 200
    assert suggestions(client, reference) == []

    remember(owner, "merchant:fotokopi", category="makan", account="cash", notes="Fotokopi")
    early = captured(client, "fotokopi 5rb", device_time=reference - timedelta(days=5))
    assert client.post(f"/api/capture/{early['id']}/undo").status_code == 200
    assert captured(client, "fotokopi 6rb", device_time=reference - timedelta(days=2))["status"] == "recorded"
    [only] = suggestions(client, reference)
    assert (only["label"], only["text"], only["amount"]) == ("Fotokopi", "fotokopi 6rb", 6_000)


def test_only_the_last_90_days_count(client, owner):
    reference = latest_past(WEEKDAYS, time(19, 0))
    record(owner["user_id"], owner["cash"], reference - timedelta(days=91), "Servis motor", 150_000,
           category_id=owner["expense"])
    record(owner["user_id"], owner["cash"], reference - timedelta(days=89), "Ganti oli", 60_000,
           category_id=owner["expense"])
    [only] = suggestions(client, reference)
    assert (only["label"], only["reason"]) == ("Ganti oli", "biasanya jam segini")


def test_at_most_six_suggestions_best_first(client, owner):
    reference = latest_past(WEEKDAYS, time(13, 0))
    for index in range(1, 9):
        for week in range(index):
            record(owner["user_id"], owner["cash"], reference - timedelta(days=35 + 7 * week), f"Toko {index}",
                   10_000 * index, category_id=owner["makan"])
    items = suggestions(client, reference)
    assert labels(items) == [f"Toko {index}" for index in range(8, 2, -1)]


# --- Prefill text -------------------------------------------------------------------------------


def test_prefill_text_is_built_from_shorthand_when_no_typed_text_fits(client, owner):
    reference = latest_past(WEEKDAYS, time(18, 0))
    user, cash = owner["user_id"], owner["cash"]
    record(user, owner["bca"], reference - timedelta(days=1), "Pulsa 50rb", 50_000, category_id=owner["pulsa"])
    record(user, owner["bca"], reference - timedelta(days=2), "Freelance desain", 1_500_000,
           category_id=owner["gaji"], kind="income")
    for days_back, amount in ((6, 25_000), (5, 25_000), (4, 30_000)):
        record(user, cash, reference - timedelta(days=days_back), "Nasi Padang", amount, category_id=owner["makan"])
    for days_back, amount in ((8, 20_000), (7, 22_000)):
        record(user, cash, reference - timedelta(days=days_back), "Bakso", amount, category_id=owner["makan"])
    remember(owner, "merchant:parkir ptc", category="parkir", account="cash", notes="Parkir PTC")
    assert captured(client, "kemarin parkir ptc 10rb", device_time=reference)["status"] == "recorded"
    remember(owner, "merchant:fotokopi", category="makan", account="cash", notes="Fotokopi")
    asked = captured(client, "fotokopi tunai", device_time=reference - timedelta(days=3))
    response = client.post(f"/api/capture/{asked['id']}/answer", json={
        "question_id": asked["question"]["question_id"], "reply_id": str(uuid4()), "text": "7rb"})
    assert response.json()["capture"]["status"] == "recorded"

    texts = {item["label"]: (item["text"], item["amount"]) for item in suggestions(client, reference)}
    assert texts == {
        "Pulsa 50rb": ("pulsa 50rb", 50_000),
        "Freelance desain": ("terima freelance desain 1,5jt", 1_500_000),
        "Nasi Padang": ("nasi padang 25rb", 25_000),
        "Bakso": ("bakso 22rb", 22_000),
        "Parkir PTC": ("parkir ptc 10rb", 10_000),
        "Fotokopi": ("fotokopi 7rb", 7_000),
    }


@pytest.mark.parametrize(("amount", "expected"), [
    (50, "rp50"), (500, "500"), (1_005, "1.005"), (10_000, "10rb"), (12_500, "12,5rb"), (12_340, "12,34rb"),
    (999_990, "999,99rb"), (1_000_000, "1jt"), (1_250_000, "1,25jt"), (1_234_000, "1234rb"),
    (1_234_567, "1.234.567"), (2_000_000_000, "2000jt"),
])
def test_amount_shorthand_parses_back_to_the_same_amount(amount, expected):
    assert amount_shorthand(amount) == expected
    assert parse_amount_answer(expected) == amount


def test_built_text_keeps_the_kind_and_the_length_limit():
    assert built_text("Gaji Oktober", "income", 8_000_000) == "gaji oktober 8jt"
    assert built_text("Refund tiket", "expense", 5_000) == "bayar refund tiket 5rb"
    assert parse_kind("bayar refund tiket 5rb")[0] is None
    assert built_text("", "income", 5_000) == "terima 5rb"
    long = built_text("kopi " * 60, "expense", 32_000)
    assert len(long) <= 200 and long.endswith(" 32rb")
    assert built_text("Tagihan tgl 5 kemarin 3/10", "expense", 100_000) == "tagihan 100rb"


# --- Boundaries ---------------------------------------------------------------------------------


def test_invalid_device_time_and_unauthenticated_requests(client, owner):
    from fastapi.testclient import TestClient
    from app.main import app
    from app.services.auth import get_current_user

    assert client.get("/api/capture/suggestions").status_code == 422
    for value in ("2026-10-29T08:30:00", "kemarin", "",
                  (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()):
        response = client.get(f"/api/capture/suggestions?device_time={quote(value)}")
        assert response.status_code == 422, value
        assert response.json()["detail"]["code"] == "invalid_device_time"
    assert client.get(f"/api/capture/suggestions?device_time={'x' * 65}").status_code == 422

    overridden = app.dependency_overrides.pop(get_current_user)
    unauthenticated = TestClient(app, base_url="https://testserver")
    try:
        now = quote(datetime.now(timezone.utc).isoformat())
        assert unauthenticated.get(f"/api/capture/suggestions?device_time={now}").status_code == 401
        assert unauthenticated.get("/api/capture/suggestions?device_time=bad").status_code == 401
    finally:
        unauthenticated.close()
        app.dependency_overrides[get_current_user] = overridden


def test_another_owners_history_is_never_used(client, owner):
    from app.db.pool import db_conn
    from app.main import app
    from app.services.auth import get_current_user

    reference = latest_past(WEEKDAYS, time(11, 0))
    foreign_user, foreign_account = str(uuid4()), str(uuid4())
    with db_conn() as conn:
        conn.execute("INSERT INTO users (id, username, password_hash, invite_code) VALUES (%s, %s, 'synthetic-hash', 'TEST')",
                     (foreign_user, f"foreign_{uuid4().hex}"))
        conn.execute("INSERT INTO accounts (id, user_id, name, type, initial_balance) VALUES (%s, %s, 'Dompet', 'cash', 0)",
                     (foreign_account, foreign_user))
        conn.commit()
    try:
        record(foreign_user, foreign_account, reference - timedelta(days=1), "Toko Tetangga", 15_000)
        record(owner["user_id"], owner["cash"], reference - timedelta(days=1), "Warung Sendiri", 12_000,
               category_id=owner["makan"])
        assert labels(suggestions(client, reference)) == ["Warung Sendiri"]
        app.dependency_overrides[get_current_user] = lambda: {"id": foreign_user, "currency": "IDR"}
        assert labels(suggestions(client, reference)) == ["Toko Tetangga"]
    finally:
        app.dependency_overrides[get_current_user] = lambda: {"id": owner["user_id"], "currency": "IDR"}
        with db_conn() as conn:
            conn.execute("DELETE FROM users WHERE id = %s", (foreign_user,))
            conn.commit()
