"""Every period consumer follows confirmed salary receipts; synthetic data only."""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from test_reimbursements import owner, record  # noqa: F401 - fixture
from test_salary_receipts import income

JAKARTA = ZoneInfo("Asia/Jakarta")


def expense_at(client, owner, amount, when):
    response = client.post("/api/transactions", json={
        "type": "expense", "account_id": owner["cash"], "category_id": owner["groceries"],
        "amount": amount, "date": when.isoformat(),
    })
    assert response.status_code == 200, response.text


def test_unknown_cycle_is_disclosed_everywhere_without_an_allowance(client, owner):
    pulse = client.get("/api/pulse").json()
    overview = client.get("/api/dashboard/overview?timeframe=cycle").json()
    ledger = client.get("/api/transactions?period=cycle").json()
    assert "siklus gaji belum tersedia" in pulse["cycle_label"]
    assert overview["timeframe_label"] == pulse["cycle_label"]
    assert ledger["summary"]["period"]["label"] == pulse["cycle_label"]
    assert pulse["safe_to_spend_today"] is None and pulse["pace_status"] is None
    assert pulse["cycle_days_total"] is None and pulse["cycle_days_remaining"] is None
    assert overview["kpis"]["safe_to_spend_today"] is None


def test_consumers_share_the_receipt_boundary_across_local_midnight(client, owner):
    # The receipt lands at 00:30 local time; spending 45 minutes earlier, on the previous local day,
    # belongs to the previous cycle in every consumer.
    today_local = datetime.now(JAKARTA).date()
    receipt_at = datetime.combine(today_local - timedelta(days=3), datetime.min.time(), JAKARTA) + timedelta(minutes=30)
    previous_receipt = receipt_at - timedelta(days=30)
    old = income(client, owner, 4_000_000, "Gaji Contoh", previous_receipt)
    salary = income(client, owner, 5_000_000, "Gaji Contoh", receipt_at)
    for transaction_id in (old, salary):
        assert client.post("/api/salary/receipts", json={"transaction_id": transaction_id}).status_code == 200
    expense_at(client, owner, 70_000, receipt_at - timedelta(minutes=45))
    expense_at(client, owner, 30_000, receipt_at + timedelta(minutes=5))

    pulse = client.get("/api/pulse").json()
    overview = client.get("/api/dashboard/overview?timeframe=cycle").json()
    analytics = client.get("/api/dashboard/analytics?timeframe=cycle").json()
    ledger = client.get("/api/transactions?period=cycle").json()
    previous = client.get("/api/dashboard/overview?timeframe=cycle&cycle_offset=-1").json()

    assert pulse["cycle_spent"] == 30_000
    assert overview["kpis"]["total_outflow"] == 30_000
    assert analytics["comparison"]["total_outflow"] == 30_000
    assert ledger["summary"]["period"]["outflow"] == 30_000
    assert overview["kpis"]["total_inflow"] == 5_000_000
    assert pulse["cycle_label"] == overview["timeframe_label"] == ledger["summary"]["period"]["label"]
    assert pulse["cycle_label"].endswith("berlanjut")
    assert datetime.fromisoformat(ledger["summary"]["cycle"]["start"]) == receipt_at.astimezone(timezone.utc)
    assert previous["kpis"]["total_outflow"] == 70_000
    assert previous["kpis"]["total_inflow"] == 4_000_000
    assert previous["timeframe_label"].startswith("Siklus ")

    # A revoked receipt removes the boundary; the window falls back to a labelled calendar month.
    assert client.delete(f"/api/salary/receipts/{salary}").json()["changed"] is True
    assert client.get("/api/pulse").json()["cycle_label"].startswith("Siklus sejak")
    assert client.get("/api/salary/cycle").json()["start"] == previous_receipt.astimezone(timezone.utc).isoformat()


def test_old_payday_setting_is_explicitly_deprecated_and_creates_no_boundary(client, owner):
    response = client.patch("/api/auth/settings", json={"payday_day": 29})
    assert response.status_code == 200, response.text
    assert response.headers["Deprecation"] == "true"
    assert response.json()["deprecations"][0]["code"] == "payday_day_deprecated"
    legacy = client.put("/api/auth/payday", json={"day": 5})
    assert legacy.headers["Deprecation"] == "true"
    assert client.get("/api/salary/cycle").json()["status"] == "not_configured"
    assert client.get("/api/salary/receipts").json()["receipts"] == []
