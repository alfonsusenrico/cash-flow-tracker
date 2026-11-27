"""Regression tests for fix-monthly-report-accuracy. All names, dates and amounts are synthetic."""
from datetime import date, datetime, time, timedelta
from uuid import uuid4
from zoneinfo import ZoneInfo

from test_monthly_report import at, insert, month_start, owners, report  # noqa: F401

from app.services.capture_normalization import (
    CaptureNormalization,
    CaptureTimeInterpretation,
    validate_capture_normalization,
)
from app.services.observed_insights import merchant_key, search_text

TZ = ZoneInfo("Asia/Jakarta")


def _insert_tx(
    owner,
    category,
    amount,
    when,
    *,
    tx_type="expense",
    account="cash",
    merchant=None,
    name=None,
    offset=None,
):
    from app.db.pool import db_conn

    with db_conn() as conn:
        row = conn.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, date,
                   offset_category_id, merchant, name, source, review_status)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'manual', 'reviewed') RETURNING id""",
            (
                owner["user_id"],
                owner[account],
                owner[category] if category else None,
                tx_type,
                amount,
                when,
                owner[offset] if offset else None,
                merchant,
                name,
            ),
        ).fetchone()
        conn.commit()
    return str(row["id"])


def _link_reimbursement(owner, reimbursement_id, expense_id, amount):
    from app.db.pool import db_conn

    with db_conn() as conn:
        conn.execute(
            "INSERT INTO reimbursement_links (reimbursement_id, expense_id, user_id, amount) VALUES (%s, %s, %s, %s)",
            (reimbursement_id, expense_id, owner["user_id"], amount),
        )
        conn.commit()


def test_top_places_include_name_only_rows_subtract_linked_reimbursements_and_skip_placeholders(client, owners):
    owner, _ = owners
    m1 = month_start(-1)
    # Name-only expense (no merchant) should rank #1.
    _insert_tx(owner, "groceries", 1_500_000, at(m1.replace(day=2)), merchant=None, name="Tagihan Cicilan Contoh")
    # Merchant expense 100% repaid by link should disappear from Top Places.
    repaid_exp = _insert_tx(owner, "dinner", 900_000, at(m1.replace(day=3)), merchant="Resto Patungan")
    repaid_inc = _insert_tx(owner, "reimbursement", 900_000, at(m1.replace(day=4)), tx_type="income")
    _link_reimbursement(owner, repaid_inc, repaid_exp, 900_000)
    # Merchant expense partially repaid by link should reflect remaining net share.
    partial_exp = _insert_tx(owner, "dinner", 400_000, at(m1.replace(day=5)), merchant="Kedai Ramai")
    partial_inc = _insert_tx(owner, "reimbursement", 150_000, at(m1.replace(day=6)), tx_type="income")
    _link_reimbursement(owner, partial_inc, partial_exp, 150_000)
    # Unlinked category-only reimbursement must NOT alter merchant totals.
    _insert_tx(owner, "reimbursement", 100_000, at(m1.replace(day=7)), tx_type="income", offset="dinner")
    # Generic placeholder descriptions must be skipped.
    _insert_tx(owner, "groceries", 800_000, at(m1.replace(day=8)), merchant=None, name="Pembayaran")
    _insert_tx(owner, "groceries", 700_000, at(m1.replace(day=9)), merchant=None, name="Payment")

    body = report(client, basis="month", start_date=m1.isoformat())
    places = [(item["name"], item["amount"]) for item in body["spending"]["top_spending_merchants"]]
    assert places == [
        ("Tagihan Cicilan Contoh", 1_500_000),
        ("Kedai Ramai", 250_000),
    ]
    assert body["comparison"]["spending_top_merchant"]["amount"] == 1_500_000
    assert "Tagihan Cicilan Contoh" in body["comparison"]["spending_top_merchant"]["label"]


def test_search_text_strips_formatted_numbers_and_invisible_chars_and_matches_ledger(client, owners):
    owner, _ = owners
    m1 = month_start(-1)
    assert search_text("Warung Contoh 125.000") == "Warung Contoh"
    assert search_text("Warung\u200b Contoh 125.000,00 -") == "Warung Contoh"
    assert merchant_key("Warung\u200b Contoh") == merchant_key("Warung Contoh")

    tx_id = _insert_tx(owner, "dinner", 125_000, at(m1.replace(day=10)), merchant="Warung Contoh 125.000")
    body = report(client, basis="month", start_date=m1.isoformat())
    top = body["spending"]["top_spending_merchants"][0]
    assert top["search_text"] == "Warung Contoh"

    ledger = client.get(
        "/api/transactions",
        params={"from_date": body["period"]["start"], "to_date": body["period"]["end"], "q": top["search_text"]},
    )
    assert ledger.status_code == 200, ledger.text
    assert [row["id"] for row in ledger.json()["transactions"]] == [tx_id]


def test_capture_normalization_strips_parsed_amount_from_merchant_but_keeps_genuine_numbers():
    cat_uuid, acc_uuid = uuid4(), uuid4()
    base_context = {
        "categories": [{"id": str(cat_uuid), "name": "Makan", "kind": "expense", "default_kakeibo": "need"}],
        "accounts": [{"id": str(acc_uuid), "name": "Dompet"}],
    }
    no_time = CaptureTimeInterpretation(span=None, local_datetime=None, confidence=0.0)

    leaked = CaptureNormalization(
        merchant="Warung Contoh 125.000",
        description="Makan Warung Contoh 125.000",
        type="expense",
        type_confidence=0.95,
        category_id=cat_uuid,
        kakeibo="need",
        category_confidence=0.9,
        account_id=acc_uuid,
        account_confidence=0.9,
        time=no_time,
    )
    cleaned = validate_capture_normalization(
        leaked,
        {**base_context, "text": "Warung Contoh 125.000", "facts": {"amount": 125_000}},
    )
    assert cleaned["merchant"] == "Warung Contoh"
    assert cleaned["description"] == "Makan Warung Contoh"

    genuine = CaptureNormalization(
        merchant="Apotek K24",
        description="Beli obat di Apotek K24",
        type="expense",
        type_confidence=0.95,
        category_id=cat_uuid,
        kakeibo="need",
        category_confidence=0.9,
        account_id=acc_uuid,
        account_confidence=0.9,
        time=no_time,
    )
    preserved = validate_capture_normalization(
        genuine,
        {**base_context, "text": "Apotek K24 45.000", "facts": {"amount": 45_000}},
    )
    assert preserved["merchant"] == "Apotek K24"
    assert preserved["description"] == "Beli obat di Apotek K24"


def test_closed_salary_cycle_excludes_partial_closing_morning_from_empty_days(client, owners):
    from app.db.pool import db_conn

    owner, _ = owners
    today = datetime.now(TZ).date()
    start_day = today - timedelta(days=6)
    close_day = today - timedelta(days=1)
    first_payroll = datetime.combine(start_day, time(9, 0), TZ)
    second_payroll = datetime.combine(close_day, time(3, 0), TZ)

    s1 = _insert_tx(owner, "salary", 5_000_000, first_payroll, tx_type="income", name="Gaji 1")
    s2 = _insert_tx(owner, "salary", 5_000_000, second_payroll, tx_type="income", name="Gaji 2")
    with db_conn() as conn:
        for tx_id in (s1, s2):
            conn.execute(
                "INSERT INTO salary_receipts (transaction_id, user_id) VALUES (%s, %s)",
                (tx_id, owner["user_id"]),
            )
        conn.commit()

    # Record a transaction on every full day of the closed cycle EXCEPT one mid-cycle gap, and nothing on close_day
    # between 00:00 and 03:00 WIB.
    gap_day = start_day + timedelta(days=2)
    day = start_day
    while day < close_day:
        if day != gap_day:
            _insert_tx(owner, "groceries", 20_000, at(day, 12), merchant="Warung Harian")
        day += timedelta(days=1)

    body = report(client, basis="cycle", start_date=start_day.isoformat())
    assert body["data_quality"]["days_without_records"] == {"count": 1, "dates": [gap_day.isoformat()]}

    # Filling the true mid-cycle gap yields 0 empty days (partial closing morning is not flagged).
    _insert_tx(owner, "groceries", 20_000, at(gap_day, 12), merchant="Warung Harian")
    body_full = report(client, basis="cycle", start_date=start_day.isoformat())
    assert body_full["data_quality"]["days_without_records"] == {"count": 0, "dates": []}


def test_possible_duplicate_sql_checks_label_and_category_non_contradiction(client, owners):
    owner, _ = owners
    m1 = month_start(-1)

    # 1. Distinct friend split repayments of the same amount within 1 minute -> NOT duplicates.
    _insert_tx(owner, "reimbursement", 50_000, at(m1.replace(day=2), 18, 0), tx_type="income", name="Transfer dari Teman A")
    _insert_tx(owner, "reimbursement", 50_000, at(m1.replace(day=2), 18, 1), tx_type="income", name="Transfer dari Teman B")

    # 2. Equal-amount expenses in different non-null categories within 2 minutes -> NOT duplicates.
    _insert_tx(owner, "groceries", 35_000, at(m1.replace(day=3), 12, 0), merchant="Toko Contoh")
    _insert_tx(owner, "dinner", 35_000, at(m1.replace(day=3), 12, 2), merchant="Toko Contoh")

    # 3. True duplicate: same label (ignoring case/extra space) and same category within 2 minutes -> FLAGGED.
    d1 = _insert_tx(owner, "groceries", 42_000, at(m1.replace(day=4), 9, 0), merchant="Warung  Pagi")
    d2 = _insert_tx(owner, "groceries", 42_000, at(m1.replace(day=4), 9, 2), merchant="warung pagi")

    # 4. Unlabeled duplicate: one labeled, one empty label, same amount & category within 1 minute -> FLAGGED.
    u1 = _insert_tx(owner, "dinner", 65_000, at(m1.replace(day=5), 19, 0), merchant="Kedai Malam")
    u2 = _insert_tx(owner, "dinner", 65_000, at(m1.replace(day=5), 19, 1), merchant=None, name=None)

    body = report(client, basis="month", start_date=m1.isoformat())
    assert body["data_quality"]["possible_duplicates"] == 4

    dups = client.get(
        "/api/transactions",
        params={"from_date": body["period"]["start"], "to_date": body["period"]["end"], "possible_duplicates": "true"},
    )
    assert dups.status_code == 200, dups.text
    assert {row["id"] for row in dups.json()["transactions"]} == {d1, d2, u1, u2}
