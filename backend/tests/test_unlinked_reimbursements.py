"""Tests for apply-unlinked-reimbursements-to-total. Synthetic data only."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from test_monthly_report import at, insert, month_start, owners  # noqa: F401
from test_reimbursements import owner, record  # noqa: F401

TZ = ZoneInfo("Asia/Jakarta")


def _insert_tx(owner, category, amount, when, *, tx_type="expense", offset=None, name="Transaksi contoh"):
    from app.db.pool import db_conn

    with db_conn() as conn:
        row = conn.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, date,
                   offset_category_id, name, source, review_status)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'manual', 'reviewed') RETURNING id""",
            (
                owner["user_id"],
                owner["cash"],
                owner[category] if category else None,
                tx_type,
                amount,
                when,
                owner[offset] if offset else None,
                name,
            ),
        ).fetchone()
        conn.commit()
    return str(row["id"])


def test_unlinked_reimbursements_reduce_totals_consistently_without_altering_categories_or_pillars(client, owners):
    owner, _ = owners
    m1 = month_start(-1)
    # Rp 5.000.000 salary, Rp 300.000 groceries (need), Rp 200.000 dinner (want), Rp 100.000 saving-pillar expense.
    _insert_tx(owner, "salary", 5_000_000, at(m1.replace(day=1)), tx_type="income", name="Gaji")
    _insert_tx(owner, "groceries", 300_000, at(m1.replace(day=2)), name="Belanja")
    _insert_tx(owner, "dinner", 200_000, at(m1.replace(day=3)), name="Makan bareng")
    _insert_tx(owner, "saving", 100_000, at(m1.replace(day=4)), name="Dana sosial")
    # Two split, rounded unlinked reimbursements (no offset_category_id, no reimbursement_links): 50k + 40k = 90k.
    _insert_tx(owner, "reimbursement", 50_000, at(m1.replace(day=5)), tx_type="income", name="Transfer Teman A")
    _insert_tx(owner, "reimbursement", 40_000, at(m1.replace(day=5), 13), tx_type="income", name="Transfer Teman B")

    expected_pengeluaran = 300_000 + 200_000 - 90_000  # 410_000
    expected_belanja = 300_000 + 200_000 + 100_000 - 90_000  # 510_000

    # 1. Laporan Bulanan + comparison
    rep = client.get("/api/reports/monthly", params={"basis": "month", "start_date": m1.isoformat()}).json()
    assert rep["comparison"]["totals"]["spending"] == expected_pengeluaran
    assert rep["comparison"]["unlinked_reimbursement"] == 90_000
    assert rep["flow"]["outflow"] == expected_belanja
    assert rep["flow"]["spending_reimbursement_offset"] == 90_000
    # Individual pillars and categories remain untouched!
    pillars = {item["pillar"]: item["amount"] for item in rep["comparison"]["pillars"]}
    assert pillars["need"] == 300_000
    assert pillars["want"] == 200_000
    assert sum(pillars.values()) - rep["comparison"]["unlinked_reimbursement"] == rep["comparison"]["totals"]["spending"]

    # 2. Analisis (analytics + calendar + flow)
    month = {"timeframe": "month", "cycle_offset": -1}
    analytics = client.get("/api/dashboard/analytics", params=month).json()
    assert analytics["comparison"]["total_outflow"] == expected_belanja
    assert analytics["kakeibo"]["need_spent"] == 300_000
    assert analytics["kakeibo"]["want_spent"] == 200_000
    assert analytics["kakeibo"]["unlinked_reimbursement"] == 90_000
    assert analytics["explanations"]["spending"]["total"] == expected_pengeluaran
    lines = {line["key"]: line for line in analytics["explanations"]["spending"]["lines"]}
    assert lines["unlinked_reimbursement"]["amount"] == -90_000
    assert lines["unlinked_reimbursement"]["label"] == "Ganti uang belum ditautkan"
    assert sum(line["amount"] for line in analytics["explanations"]["spending"]["lines"]) == expected_pengeluaran

    cal = client.get("/api/dashboard/calendar", params=month).json()
    assert cal["totals"]["spending"] == expected_pengeluaran
    assert cal["totals"]["spend"] == expected_belanja

    flow = client.get("/api/dashboard/flow", params=month).json()
    assert flow["summary"]["spending"] == expected_pengeluaran
    assert flow["summary"]["belanja"] == expected_belanja

    # 3. Unfiltered ledger matches, while category/pillar filter stays unaffected!
    ledger_all = client.get("/api/transactions", params={"period": "month", "period_offset": -1}).json()["summary"]["period"]
    assert ledger_all["spending"] == expected_pengeluaran
    assert ledger_all["outflow"] == expected_belanja
    ledger_need = client.get(
        "/api/transactions", params={"period": "month", "period_offset": -1, "kakeibo_type": "need", "type": "expense"}
    ).json()["summary"]["period"]
    assert ledger_need["outflow"] == 300_000
    ledger_dinner = client.get(
        "/api/transactions", params={"period": "month", "period_offset": -1, "category_id": owner["dinner"]}
    ).json()["summary"]["period"]
    assert ledger_dinner["outflow"] == 200_000


def test_unlinked_reimbursement_floors_at_zero_and_reports_excess(client, owner):
    record(client, owner, "salary", 5_000_000, tx_type="income")
    record(client, owner, "dinner", 100_000)
    record(client, owner, "reimbursement", 160_000, tx_type="income")

    overview = client.get("/api/dashboard/overview").json()
    kpis = overview["kpis"]
    assert kpis["total_inflow"] == 5_000_000
    assert kpis["total_outflow"] == 0
    assert kpis["reimbursement_offset"] == 100_000
    assert kpis["reimbursement_excess"] == 60_000
    assert kpis["reimbursement_unallocated"] == 0


def test_explicitly_linked_reimbursement_rest_stays_unallocated(client, owner):
    expense_id = record(client, owner, "dinner", 100_000)["transaction_id"]
    record(client, owner, "groceries", 200_000)
    reimb_id = record(client, owner, "reimbursement", 150_000, tx_type="income")["transaction_id"]
    linked = client.put(f"/api/transactions/{reimb_id}/reimburses", json={"expense_ids": [expense_id]})
    assert linked.status_code == 200, linked.text

    # 100k linked to dinner, 50k left unallocated as 'rest' -> MUST NOT reduce groceries or total!
    overview = client.get("/api/dashboard/overview").json()
    kpis = overview["kpis"]
    assert kpis["gross_outflow"] == 300_000
    assert kpis["reimbursement_offset"] == 100_000
    assert kpis["total_outflow"] == 200_000
    assert kpis["reimbursement_unallocated"] == 50_000


def test_auto_link_exact_single_match_and_edge_cases(client, owner):
    now = datetime.now(TZ)

    # 1. Single exact-amount expense within 14 days -> auto-links on creation!
    exp_exact = _insert_tx(owner, "dinner", 137_500, now - timedelta(days=2), name="Makan siang tepat")
    r_exact = client.post("/api/transactions", json={
        "type": "income", "account_id": owner["cash"], "category_id": owner["reimbursement"],
        "amount": 137_500, "date": now.isoformat(),
    })
    assert r_exact.status_code == 200, r_exact.text
    r_exact_id = r_exact.json()["transaction_id"]
    detail = client.get(f"/api/transactions/{r_exact_id}").json()["transaction"]["reimbursement"]
    assert [row["transaction_id"] for row in detail["repays"]] == [exp_exact]
    assert detail["repays"][0]["amount"] == 137_500

    # 2. Removing the link manually MUST NOT re-link it!
    removed = client.delete(f"/api/transactions/{r_exact_id}/reimburses")
    assert removed.status_code == 200, removed.text
    after_remove = client.get(f"/api/transactions/{r_exact_id}").json()["transaction"]["reimbursement"]
    assert after_remove["repays"] == []

    # 3. Rounded / split amount (no exact match) -> stays unlinked (no guessing).
    _insert_tx(owner, "dinner", 49_714, now - timedelta(days=1), name="Patungan ganjil")
    r_rounded = client.post("/api/transactions", json={
        "type": "income", "account_id": owner["cash"], "category_id": owner["reimbursement"],
        "amount": 50_000, "date": now.isoformat(),
    }).json()["transaction_id"]
    assert client.get(f"/api/transactions/{r_rounded}").json()["transaction"]["reimbursement"]["repays"] == []

    # 4. Ambiguous match (two unlinked expenses of exact same amount within 14 days) -> stays unlinked.
    _insert_tx(owner, "dinner", 75_000, now - timedelta(days=3), name="Kopi A")
    _insert_tx(owner, "groceries", 75_000, now - timedelta(days=1), name="Kopi B")
    r_ambig = client.post("/api/transactions", json={
        "type": "income", "account_id": owner["cash"], "category_id": owner["reimbursement"],
        "amount": 75_000, "date": now.isoformat(),
    }).json()["transaction_id"]
    assert client.get(f"/api/transactions/{r_ambig}").json()["transaction"]["reimbursement"]["repays"] == []

    # 5. Expense older than 14 days -> stays unlinked.
    _insert_tx(owner, "dinner", 88_800, now - timedelta(days=15), name="Makan lama")
    r_old = client.post("/api/transactions", json={
        "type": "income", "account_id": owner["cash"], "category_id": owner["reimbursement"],
        "amount": 88_800, "date": now.isoformat(),
    }).json()["transaction_id"]
    assert client.get(f"/api/transactions/{r_old}").json()["transaction"]["reimbursement"]["repays"] == []

    # 6. Recategorizing an existing income into Patungan & Ganti Uang auto-links when unambiguous!
    exp_recat = _insert_tx(owner, "groceries", 111_222, now - timedelta(days=4), name="Titip beli")
    inc_id = client.post("/api/transactions", json={
        "type": "income", "account_id": owner["cash"], "category_id": owner["salary"],
        "amount": 111_222, "date": now.isoformat(),
    }).json()["transaction_id"]
    assert client.get(f"/api/transactions/{inc_id}").json()["transaction"]["reimbursement"]["repays"] == []
    patched = client.patch(f"/api/transactions/{inc_id}", json={"category_id": owner["reimbursement"]})
    assert patched.status_code == 200, patched.text
    after_patch = client.get(f"/api/transactions/{inc_id}").json()["transaction"]["reimbursement"]
    assert [row["transaction_id"] for row in after_patch["repays"]] == [exp_recat]


def test_beranda_cycle_summary_includes_unlinked_line_and_caps_credit_safely(client, owners):
    from app.db.pool import db_conn
    from uuid import uuid4

    owner, _ = owners
    credit_id = str(uuid4())
    now = datetime.now(TZ)
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO accounts (id, user_id, name, type, initial_balance) VALUES (%s, %s, 'Kartu Kredit Contoh', 'credit', 0)",
            (credit_id, owner["user_id"]),
        )
        # Rp 400.000 spent on credit, Rp 150.000 unlinked reimbursement -> net Pengeluaran is Rp 250.000.
        conn.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, date, name, source, review_status)
               VALUES (%s, %s, %s, 'expense', 400000, %s, 'Belanja kredit', 'manual', 'reviewed')""",
            (owner["user_id"], credit_id, owner["dinner"], now - timedelta(hours=2)),
        )
        conn.commit()
    _insert_tx(owner, "reimbursement", 150_000, now - timedelta(hours=1), tx_type="income", name="Ganti uang patungan")

    res = client.get("/api/dashboard/cycle-summary")
    assert res.status_code == 200, res.text
    summary = res.json()
    assert summary["spending"]["amount"] == 250_000
    assert summary["spending"]["credit"]["spent_on_credit"] == 250_000
    lines = {line["key"]: line for line in summary["spending"]["lines"]}
    assert lines["unlinked_reimbursement"]["amount"] == -150_000
    assert sum(line["amount"] for line in summary["spending"]["lines"]) == 250_000
