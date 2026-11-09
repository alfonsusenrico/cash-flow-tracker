"""Analisis contracts: the calendar, the money flow and its rows, the Kakeibo comparison and explanations.

Every name, amount and date is synthetic. Most fixtures sit in the previous calendar month, so every
day of the window is in the past whatever the real date; the current month is used where "today" matters.
"""
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta, timezone
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest

from test_cycle_summary import (  # noqa: F401 - fixtures and helpers
    at_local,
    counted_statements,
    make_owner,
    owner,
    shift_months,
    today,
    unlimited_rate_limits,
    waiting_event,
)

JAKARTA = ZoneInfo("Asia/Jakarta")
CALENDAR = "/api/dashboard/calendar"
FLOW = "/api/dashboard/flow"
ROWS = "/api/dashboard/flow/rows"


def previous_month() -> tuple[date, date]:
    first = today().replace(day=1)
    last = first - timedelta(days=1)
    return last.replace(day=1), last


def month_day(day: int) -> date:
    start, end = previous_month()
    return min(start + timedelta(days=day - 1), end)


def get(owner, path, **params):
    response = owner.session.get(path, params=params)
    assert response.status_code == 200, response.text
    return response.json()


def calendar(owner, timeframe="month", offset=-1):
    return get(owner, CALENDAR, timeframe=timeframe, cycle_offset=offset)


def flow(owner, timeframe="month", offset=-1, through=None):
    params = {"timeframe": timeframe, "cycle_offset": offset}
    if through is not None:
        params["through"] = through.isoformat()
    return get(owner, FLOW, **params)


def analytics(owner, timeframe="month", offset=-1):
    return get(owner, "/api/dashboard/analytics", timeframe=timeframe, cycle_offset=offset)


def pair(owner, source, target, amount, when, *, category="movement"):
    """A linked movement written as its two legs, as the pairing and the top-up conversion leave them."""
    movement_id = str(uuid4())
    for account, kind, role in ((source, "expense", "outbound"), (target, "income", "inbound")):
        owner.db(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, date, notes, source,
                                         review_status, movement_id, movement_role)
               VALUES (%s, %s, %s, %s, %s, %s, 'Pindah saldo contoh', 'manual', 'reviewed', %s, %s)""",
            (owner.user_id, owner[account], owner[category], kind, amount, when, movement_id, role),
        )
    return movement_id


def ledger_balance(owner, account_ids, until):
    from app.db.pool import db_conn

    with db_conn() as conn:
        row = conn.execute(
            """SELECT COALESCE(SUM(a.initial_balance), 0)
                      + COALESCE((SELECT SUM(CASE WHEN t.type = 'income' THEN t.amount ELSE -t.amount END)
                                  FROM transactions t WHERE t.account_id = ANY(%s::uuid[])
                                    AND t.type IN ('income', 'expense') AND t.date <= %s), 0) AS balance
               FROM accounts a WHERE a.id = ANY(%s::uuid[])""",
            (account_ids, until, account_ids),
        ).fetchone()
    return int(row["balance"])


def assert_balanced(data):
    for node in data["nodes"]:
        if node["kind"] not in ("account", "saving", "investment"):
            continue
        moved_in = sum(link["amount"] for link in data["links"] if link["to"] == node["id"])
        moved_out = sum(link["amount"] for link in data["links"] if link["from"] == node["id"])
        assert moved_in == moved_out, node["label"]
    for link in data["links"]:
        assert isinstance(link["amount"], int) and link["amount"] > 0, link["key"]


@pytest.fixture
def rich_owner(owner):
    """Income, spending in all six buckets, an excluded category, a reimbursement, a transfer between two
    accounts, a move into a pocket of the same account, a move into a goal pocket, a top-up, a sale back to
    cash, an awaiting leg, a question in a bucket and a negative opening balance."""
    owner.category("tabung", "Tabung Contoh", pillar="saving")
    owner.category("excluded", "Kategori Dikecualikan", excluded=True)
    owner.category("investasi", "Investasi", excluded=True)
    owner.account("harian", "Kantong Harian", parent="bank")
    owner.account("dd", "Kantong Dana Darurat", parent="bank")
    owner.account("bibit", "Bibit Contoh", kind="investment")
    owner.db("UPDATE accounts SET instrument_type = 'mutual_fund' WHERE id = %s", (owner.account("rda", "Reksa Dana A", kind="investment", parent="bibit"),))
    owner.account("minus", "Kartu Contoh", initial=-150_000)
    owner.goal("goal", "Dana Darurat", 10_000_000, shift_months(today(), 6), accounts=["dd"])

    owner.tx("income", 8_200_000, category="gaji", when=at_local(month_day(1)), notes="Gaji contoh")
    owner.expense(120_000, "makan", when=at_local(month_day(2)))
    owner.expense(85_000, "jajan", when=at_local(month_day(3)))
    owner.expense(60_000, "buku", when=at_local(month_day(3), 15))
    owner.expense(250_000, "servis", when=at_local(month_day(4)))
    owner.expense(40_000, "lain", when=at_local(month_day(5)))
    owner.expense(300_000, "tabung", when=at_local(month_day(5), 14))
    owner.expense(75_000, "excluded", when=at_local(month_day(6)))
    owner.tx("income", 30_000, category="patungan", when=at_local(month_day(7)), notes="Ganti uang contoh")
    owner.db("UPDATE transactions SET offset_category_id = %s WHERE user_id = %s AND category_id = %s",
             (owner["makan"], owner.user_id, owner["patungan"]))
    pair(owner, "bank", "wallet", 500_000, at_local(month_day(8)))
    pair(owner, "bank", "harian", 200_000, at_local(month_day(8), 11))
    pair(owner, "bank", "dd", 1_000_000, at_local(month_day(9)))
    pair(owner, "bank", "rda", 450_000, at_local(month_day(10)), category="investasi")
    pair(owner, "rda", "bank", 100_000, at_local(month_day(11)), category="investasi")
    awaiting = owner.expense(112_590, "movement", account="wallet", when=at_local(month_day(12)),
                             notes="Transfer keluar contoh", review_status="needs_review")
    waiting_event(owner, awaiting, reasons=["awaiting_counterpart"])
    question = owner.expense(45_000, "jajan", account="wallet", when=at_local(month_day(13)),
                             notes="QRIS contoh", review_status="needs_review")
    waiting_event(owner, question, question=True, reasons=["purpose_question"])
    owner.expense(20_000, "makan", account="minus", when=at_local(month_day(14)))
    owner.ids["awaiting_tx"] = awaiting
    owner.ids["question_tx"] = question
    return owner


# --- Calendar ---------------------------------------------------------------------------------------


def test_calendar_days_add_up_to_belanja_with_a_later_reimbursement(rich_owner):
    data = calendar(rich_owner)
    start, end = previous_month()
    assert len(data["days"]) == (end - start).days + 1
    assert data["window"]["start"] == start.isoformat() and data["window"]["end"] == end.isoformat()
    spends = [day["spend"] for day in data["days"]]
    assert all(amount >= 0 for amount in spends)
    assert sum(spends) == data["totals"]["spend"] == analytics(rich_owner)["comparison"]["total_outflow"]
    assert data["totals"]["gross_spend"] - data["totals"]["offset_applied"] == data["totals"]["spend"]
    assert data["totals"]["offset_applied"] == 30_000
    assert data["upcoming"] == [] and data["upcoming_total"] == 0


def test_calendar_counts_waiting_rows_and_one_movement_per_pair(rich_owner):
    days = {day["date"]: day for day in calendar(rich_owner)["days"]}
    question_day = days[month_day(13).isoformat()]
    assert question_day["spend"] == 45_000 and question_day["waiting_count"] == 1
    assert question_day["waiting"][0]["transaction_id"] == rich_owner["question_tx"]
    awaiting_day = days[month_day(12).isoformat()]
    assert awaiting_day["waiting_count"] == 1 and awaiting_day["spend"] == 0
    move_day = days[month_day(8).isoformat()]
    assert move_day["movement_count"] == 2 and move_day["movement_total"] == 700_000 and move_day["spend"] == 0
    topup_day = days[month_day(10).isoformat()]
    assert topup_day["topup_count"] == 1 and topup_day["topup_total"] == 450_000
    assert topup_day["movement_count"] == 0
    sale_day = days[month_day(11).isoformat()]
    assert sale_day["movement_count"] == 1 and sale_day["topup_count"] == 0


def test_calendar_heat_levels_come_from_the_window(rich_owner):
    from app.services.calendar import heat_level, heat_thresholds

    assert heat_thresholds([0, 0]) == []
    assert heat_thresholds([10_400, 20_600, 30_000, 40_000, 50_000]) == [10_000, 21_000, 30_000, 40_000]
    assert heat_level(0, [1]) == 0 and heat_level(5_000, [10_000, 20_000, 30_000, 40_000]) == 1
    assert heat_level(45_000, [10_000, 20_000, 30_000, 40_000]) == 5
    data = calendar(rich_owner)
    assert data["heat_thresholds"] == heat_thresholds([day["spend"] for day in data["days"]])
    for day in data["days"]:
        assert day["heat"] == heat_level(day["spend"], data["heat_thresholds"])


def test_current_month_lists_future_days_and_upcoming_plans(owner):
    first = today().replace(day=1)
    owner.expense(25_000, "makan")
    soon = today() + timedelta(days=3)
    owner.plan("internet", "Internet Rumah", 350_000, soon)
    past_rule = owner.plan("listrik", "Listrik Contoh", 200_000, first)
    data = calendar(owner, offset=0)
    end = shift_months(first, 1) - timedelta(days=1)
    assert len(data["days"]) == (end - first).days + 1 and data["window"]["is_current"]
    for day in data["days"]:
        if day["date"] > today().isoformat():
            assert day["spend"] == 0 and day["waiting_count"] == 0
    if soon <= end:
        assert [line["label"] for line in data["upcoming"]] == ["Internet Rumah"]
        assert data["upcoming"][0]["status"] == "due" and data["upcoming_total"] == 350_000
    statuses = {plan["status"] for day in data["days"] for plan in day["plans"] if plan["rule_id"] == past_rule}
    assert statuses == ({"overdue"} if first < today() else {"due"})


def test_met_and_skipped_occurrences_are_listed_in_their_day(owner):
    start, _end = previous_month()
    linked = owner.plan("a", "Asuransi Contoh", 100_000, start + timedelta(days=4))
    skipped = owner.plan("b", "Langganan Contoh", 50_000, start + timedelta(days=6))
    for rule, status, offset in ((linked, "linked", 4), (skipped, "skipped", 6)):
        owner.db("INSERT INTO recurring_executions (recurring_rule_id, user_id, scheduled_for, status) VALUES (%s, %s, %s, %s)",
                 (rule, owner.user_id, start + timedelta(days=offset), status))
        owner.db("UPDATE recurring_rules SET next_due_date = %s WHERE id = %s", (shift_months(start, 1) + timedelta(days=offset), rule))
    days = {day["date"]: day for day in calendar(owner)["days"]}
    assert days[(start + timedelta(days=4)).isoformat()]["plans"][0]["status"] == "met"
    assert days[(start + timedelta(days=6)).isoformat()]["plans"][0]["status"] == "skipped"


def test_open_cycle_runs_to_the_day_before_the_expected_salary(owner):
    latest = today() - timedelta(days=8)
    owner.regular_salaries(latest)
    data = calendar(owner, timeframe="cycle", offset=0)
    expected = date.fromisoformat(data["expected_salary"]["date"])
    assert data["expected_salary"]["available"] and data["upcoming_basis"] == "expected_salary"
    assert data["window"]["start"] == latest.isoformat()
    assert len(data["days"]) == (expected - latest).days
    assert data["window"]["day_index"] == 9
    assert data["totals"]["spend"] == analytics(owner, "cycle", 0)["comparison"]["total_outflow"]


def test_open_cycle_without_an_estimate_runs_one_nominal_month(owner):
    latest = today() - timedelta(days=4)
    owner.salary(shift_months(latest, -1), source=True)
    owner.salary(latest)
    data = calendar(owner, timeframe="cycle", offset=0)
    assert not data["expected_salary"]["available"]
    assert data["expected_salary"]["reason"] == "too_few_receipts"
    assert data["upcoming_basis"] == "window_end"
    assert data["window"]["end"] == (shift_months(latest, 1) - timedelta(days=1)).isoformat()


def test_calendar_rejects_bad_parameters(owner):
    for params in ({"timeframe": "30d"}, {"timeframe": "cycle", "cycle_offset": -25}):
        assert owner.session.get(CALENDAR, params=params).status_code == 422


def test_calendar_and_flow_need_a_session(client):
    for path in (CALENDAR, FLOW, ROWS):
        assert client.get(path, headers={"Origin": "https://testserver"}).status_code == 401


def test_calendar_is_owner_scoped(make_owner):
    first, second = make_owner(), make_owner()
    first.expense(10_000, "makan", when=at_local(month_day(3)))
    second.expense(999_000, "makan", when=at_local(month_day(3)))
    second.plan("plan", "Rencana Pemilik Lain", 77_000, month_day(5))
    data = calendar(first)
    assert data["totals"]["spend"] == 10_000
    assert all(plan["label"] != "Rencana Pemilik Lain" for day in data["days"] for plan in day["plans"])


def count_calls(owner, monkeypatch, path, **params):
    with monkeypatch.context() as patch:
        with counted_statements(patch) as statements:
            get(owner, path, **params)
            return len(statements)


def test_calendar_statement_count_is_constant(rich_owner, monkeypatch):
    params = {"timeframe": "month", "cycle_offset": -1}
    baseline = count_calls(rich_owner, monkeypatch, CALENDAR, **params)
    # Nine queries (one is the Pengeluaran basis, apply-markas-analisis D31) plus the read-only isolation statement.
    assert baseline <= 10
    for index in range(20):
        rich_owner.expense(1_000 + index, "jajan", when=at_local(month_day(1 + index % 20)))
        rich_owner.plan(f"p{index}", f"Rencana {index}", 10_000, month_day(2 + index % 20))
    assert count_calls(rich_owner, monkeypatch, CALENDAR, **params) == baseline


# --- Flow -------------------------------------------------------------------------------------------


def node_of(data, label):
    return next(node for node in data["nodes"] if node["label"] == label)


def link(data, source_label, target_label):
    source, target = node_of(data, source_label)["id"], node_of(data, target_label)["id"]
    return next(item for item in data["links"] if item["from"] == source and item["to"] == target)


def test_every_holder_balances_on_every_day_and_closes_at_its_ledger_balance(rich_owner):
    start, end = previous_month()
    day = start
    while day <= end:
        data = flow(rich_owner, through=day)
        assert data["window"]["through"] == day.isoformat()
        assert_balanced(data)
        day += timedelta(days=1)
    data = flow(rich_owner)
    close = datetime.combine(end, time.max, JAKARTA)
    members = {
        "Bank Contoh": [rich_owner["bank"], rich_owner["harian"]],
        "Dompet Contoh": [rich_owner["wallet"]],
        "Bibit Contoh": [rich_owner["bibit"], rich_owner["rda"]],
        "Dana Darurat · Kantong Dana Darurat": [rich_owner["dd"]],
    }
    for label, account_ids in members.items():
        node = node_of(data, label)
        assert node["closing"] == ledger_balance(rich_owner, account_ids, close), label
    harian = node_of(data, "Bank Contoh")
    assert "Kantong Harian" in harian["members"]
    assert node_of(data, "Dana Darurat · Kantong Dana Darurat")["kind"] == "saving"
    assert node_of(data, "Bibit Contoh")["kind"] == "investment"


def test_flow_links_by_row_kind(rich_owner):
    data = flow(rich_owner)
    assert link(data, "Gaji", "Bank Contoh")["amount"] == 8_200_000
    assert link(data, "Bank Contoh", "Kebutuhan")["amount"] == 120_000
    assert link(data, "Bank Contoh", "Tidak dihitung belanja")["amount"] == 75_000
    assert link(data, "Penggantian uang", "Bank Contoh")["amount"] == 30_000
    assert link(data, "Bank Contoh", "Dompet Contoh")["kind"] == "transfer"
    assert link(data, "Bank Contoh", "Dana Darurat · Kantong Dana Darurat")["kind"] == "saving"
    assert link(data, "Bank Contoh", "Bibit Contoh") == {**link(data, "Bank Contoh", "Bibit Contoh"), "kind": "investment", "amount": 450_000}
    assert link(data, "Bibit Contoh", "Bank Contoh")["amount"] == 100_000
    # A move into the account's own pocket stays inside the node.
    assert not any(item["to"] == node_of(data, "Bank Contoh")["id"] and item["from"] == item["to"] for item in data["links"])
    awaiting = link(data, "Dompet Contoh", "Menunggu pasangan")
    assert awaiting["amount"] == 112_590 and awaiting["waiting_count"] == 1
    want = link(data, "Dompet Contoh", "Keinginan")
    assert want["amount"] == 45_000 and want["waiting_count"] == 1 and want["waiting_amount"] == 45_000
    minus = link(data, "Kartu Contoh", "Saldo minus sebelum periode")
    assert minus["amount"] == 150_000 and minus["kind"] == "negative_opening"
    closing = link(data, "Saldo minus di akhir", "Kartu Contoh")
    assert closing["amount"] == 170_000
    assert node_of(data, "Masih ada")["folded_by_default"] and node_of(data, "Saldo sebelum periode")["folded_by_default"]


def test_flow_summary_equals_analytics_at_the_window_end(rich_owner):
    data = flow(rich_owner)
    report = analytics(rich_owner)
    assert data["summary"]["belanja"] == report["comparison"]["total_outflow"]
    assert data["summary"]["keluar"] == report["kakeibo"]["total_allocated"]
    assert data["summary"]["masuk"] == report["comparison"]["total_inflow"]
    assert data["summary"]["waiting_out"] == 112_590
    assert data["summary"]["saving_net"] == 1_000_000 and data["summary"]["investment_net"] == 350_000


def test_replay_keeps_the_node_order(rich_owner):
    first = [node["id"] for node in flow(rich_owner, through=month_day(2))["nodes"]]
    last = [node["id"] for node in flow(rich_owner)["nodes"]]
    assert first == last
    early = flow(rich_owner, through=month_day(2))
    assert {item["kind"] for item in early["links"]} <= {"income", "spend", "opening", "negative_opening", "remaining", "negative_closing"}


def test_through_outside_the_window_is_refused(rich_owner):
    _start, end = previous_month()
    response = rich_owner.session.get(FLOW, params={"timeframe": "month", "cycle_offset": -1, "through": (end + timedelta(days=1)).isoformat()})
    assert response.status_code == 422 and response.json()["detail"]["code"] == "through_outside_window"


def rows_of(owner, **params):
    collected, offset = [], 0
    while True:
        page = get(owner, ROWS, timeframe="month", cycle_offset=-1, offset=offset, **params)
        collected += page["rows"]
        offset += page["limit"]
        if offset >= page["total_count"]:
            return page, collected


def test_rows_add_up_to_every_link(rich_owner):
    for index in range(60):
        rich_owner.expense(1_000 + index, "makan", when=at_local(month_day(15 + index % 10)))
    data = flow(rich_owner)
    for item in data["links"]:
        page, rows = rows_of(rich_owner, link=item["key"])
        if item["kind"] in ("opening", "remaining", "negative_opening", "negative_closing"):
            assert rows == [] and page["total_amount"] == 0
            continue
        assert page["total_amount"] == item["amount"] == sum(row["amount"] for row in rows), item["key"]
        assert page["total_count"] == item["count"] == len(rows)
    bank = node_of(data, "Bank Contoh")["id"]
    page, rows = rows_of(rich_owner, node=bank)
    assert len({row["id"] for row in rows}) == len(rows) == page["total_count"]


def test_rows_refuse_unknown_foreign_and_malformed_keys(make_owner):
    first, second = make_owner(), make_owner()
    first.expense(10_000, "makan", when=at_local(month_day(3)))
    second.expense(10_000, "makan", when=at_local(month_day(3)))
    base = {"timeframe": "month", "cycle_offset": -1}
    unknown = first.session.get(ROWS, params={**base, "link": f"acc:{first['bank']}>pillar:culture"})
    assert unknown.status_code == 404 and unknown.json()["detail"]["code"] == "flow_link_not_found"
    foreign = first.session.get(ROWS, params={**base, "link": f"acc:{second['bank']}>pillar:need"})
    assert foreign.status_code == 404
    assert first.session.get(ROWS, params={**base, "node": f"acc:{second['bank']}"}).status_code == 404
    for bad in ({"link": "acc:not-a-uuid>pillar:need"}, {"node": "drop table"}, {}):
        assert first.session.get(ROWS, params={**base, **bad}).status_code == 422


def test_flow_is_owner_scoped(make_owner):
    first, second = make_owner(), make_owner()
    first.expense(10_000, "makan", when=at_local(month_day(3)))
    second.expense(999_000, "makan", when=at_local(month_day(3)))
    data = flow(first)
    labels = {node["label"] for node in data["nodes"]}
    assert data["summary"]["belanja"] == 10_000 and "Bank Contoh" in labels
    assert all(node.get("account_id") in (None, *first.ids.values()) for node in data["nodes"])


def test_flow_and_rows_statement_counts_are_constant(rich_owner, monkeypatch):
    params = {"timeframe": "month", "cycle_offset": -1}
    flow_count = count_calls(rich_owner, monkeypatch, FLOW, **params)
    data = flow(rich_owner)
    rows_count = count_calls(rich_owner, monkeypatch, ROWS, link=data["links"][0]["key"], **params)
    # One of the flow's statements is the Pengeluaran basis of its summary (apply-markas-analisis D31).
    assert flow_count <= 11 and rows_count <= 4
    for index in range(30):
        rich_owner.account(f"extra{index}", f"Rekening Tambahan {index}")
        rich_owner.expense(2_000 + index, "makan", account=f"extra{index}", when=at_local(month_day(1 + index % 25)))
    assert count_calls(rich_owner, monkeypatch, FLOW, **params) == flow_count
    assert count_calls(rich_owner, monkeypatch, ROWS, link=data["links"][0]["key"], **params) == rows_count


# --- Period comparison and analytics explanations ----------------------------------------------------


def test_period_comparison_kakeibo_includes_saving_transfers(owner):
    owner.category("tabung", "Tabung Contoh", pillar="saving")
    owner.account("dd", "Kantong Dana Darurat")
    owner.goal("goal", "Dana Darurat", 10_000_000, shift_months(today(), 6), accounts=["dd"])
    owner.expense(100_000, "makan")
    moved = pair(owner, "bank", "dd", 400_000, datetime.now(timezone.utc) - timedelta(minutes=3))
    owner.db("UPDATE transactions SET kakeibo_type = 'saving' WHERE movement_id = %s AND movement_role = 'outbound'", (moved,))
    data = get(owner, "/api/dashboard/period-comparison", timeframe="month", cycle_offset=0)
    report = analytics(owner, "month", 0)
    expected = {key: report["kakeibo"][f"{key}_spent"] for key in ("need", "want", "culture", "unexpected", "saving", "unclassified")}
    assert data["kakeibo"]["current"] == expected and expected["saving"] == 400_000
    assert sum(data["kakeibo"]["current"].values()) == report["kakeibo"]["total_allocated"]
    assert data["kakeibo"]["previous"] is None
    for entry in data["pillars"]:
        assert set(entry) == {"pillar", "current", "previous", "change"}


def test_previous_window_with_data_has_kakeibo(owner):
    owner.expense(70_000, "makan", when=at_local(month_day(1)))
    owner.expense(30_000, "makan")
    data = get(owner, "/api/dashboard/period-comparison", timeframe="month", cycle_offset=0)
    if data["previous_has_data"]:
        assert data["kakeibo"]["previous"]["need"] == 70_000


def test_analytics_explanations_add_up(rich_owner):
    report = analytics(rich_owner)
    keluar, belanja = report["explanations"]["keluar"], report["explanations"]["belanja"]
    assert sum(line["amount"] for line in keluar["lines"]) == keluar["total"] == report["kakeibo"]["total_allocated"]
    assert sum(line["amount"] for line in belanja["lines"]) == belanja["total"] == report["comparison"]["total_outflow"]
    assert [line["key"] for line in belanja["lines"]] == ["gross", "reimbursement"]
    assert belanja["lines"][1]["amount"] == -30_000
    assert keluar["lines"][0]["href"] == "/ledger?period=month&offset=-1&kakeibo=need"
    for key in ("daily_cadence", "kakeibo", "comparison", "category_variance", "day_of_week_heatmap"):
        assert key in report
