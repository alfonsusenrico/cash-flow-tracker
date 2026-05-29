"""Smoke tests: one authenticated happy path per endpoint family."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from fastapi.testclient import TestClient


SMOKE_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x02\x00\x00\x00\x90wS\xde\x00\x00\x00\x0cIDATx\x9cc```\x00"
    b"\x00\x00\x04\x00\x01\xf6\x178U\x00\x00\x00\x00IEND\xaeB`\x82"
)


def unique(label: str) -> str:
    return f"{label}-{uuid4().hex[:8]}"


def now_iso() -> str:
    return future_iso(5)


def future_iso(minutes: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(minutes=minutes)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def past_iso(minutes: int = 1) -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def next_month_ym() -> str:
    now = datetime.now()
    year = now.year + (1 if now.month == 12 else 0)
    month = 1 if now.month == 12 else now.month + 1
    return f"{year:04d}-{month:02d}"


def assert_ok(res):
    assert res.status_code == 200, res.text
    return res.json()


def summary_cash_totals(summary: dict) -> tuple[int, int]:
    accounts = summary.get("accounts", [])
    return (
        sum(int(account.get("total_in") or 0) for account in accounts),
        sum(int(account.get("total_out") or 0) for account in accounts),
    )


def create_account(client: TestClient, name: str | None = None, initial_balance: int = 0) -> str:
    body = {"account_name": name or unique("account"), "initial_balance": initial_balance}
    return assert_ok(client.post("/accounts", json=body))["account_id"]


def test_health(client: TestClient):
    body = assert_ok(client.get("/health"))
    assert body["ok"] is True


def test_auth_and_api_key(auth_client: TestClient):
    body = assert_ok(auth_client.get("/me"))
    assert body["username"] == auth_client._smoke_username
    categories = assert_ok(auth_client.get("/categories"))["categories"]
    names = {category["name"] for category in categories}
    assert {"Salary", "Food & Drink", "Transfer", "Switching", "Opening Balance"}.issubset(names)
    assert_ok(auth_client.get("/api-key"))
    body = assert_ok(auth_client.post("/api-key/reset"))
    assert body["api_key"]


def test_accounts_budgets_and_payday(auth_client: TestClient):
    month = datetime.now().strftime("%Y-%m")
    account_id = create_account(auth_client, initial_balance=100_000)

    accounts = assert_ok(auth_client.get("/accounts"))["accounts"]
    account = next(a for a in accounts if a["account_id"] == account_id)
    assert account["balance"] == 100_000
    summary_accounts = assert_ok(auth_client.get("/summary"))["accounts"]
    summary_account = next(a for a in summary_accounts if a["account_id"] == account_id)
    assert summary_account["current_balance"] == 100_000
    renamed = unique("renamed-account")
    rename_body = assert_ok(auth_client.put(f"/accounts/{account_id}", json={"account_name": renamed}))
    assert rename_body["account"]["account_name"] == renamed
    accounts = assert_ok(auth_client.get("/accounts"))["accounts"]
    assert next(a for a in accounts if a["account_id"] == account_id)["account_name"] == renamed
    assert_ok(
        auth_client.put(
            f"/accounts/{account_id}/profile",
            json={
                "account_name": renamed,
                "profile_type": "fixed_spending",
                "is_payroll_source": True,
                "is_no_limit": False,
                "is_buffer": True,
                "fixed_limit_amount": 50_000,
                "institution": "Smoke Bank",
                "account_number": "1234",
            },
        )
    )

    budget = assert_ok(auth_client.post("/budgets", json={"account_id": account_id, "month": month, "amount": 50_000}))
    assert_ok(auth_client.get("/budgets", params={"month": month}))
    assert_ok(auth_client.put(f"/budgets/{budget['budget_id']}", json={"amount": 75_000}))
    assert_ok(auth_client.delete(f"/budgets/{budget['budget_id']}"))

    assert_ok(auth_client.get("/payday", params={"month": month}))
    assert_ok(auth_client.put("/payday", json={"month": month, "day": 26}))
    assert_ok(auth_client.put("/payday", json={"month": month, "clear_override": True}))
    assert_ok(auth_client.put("/payday", json={"day": 25}))
    assert_ok(auth_client.delete(f"/accounts/{account_id}"))


def test_payables_and_receivables(auth_client: TestClient):
    account_id = create_account(auth_client, unique("obligation-account"), 1_000_000)
    today = datetime.now(timezone.utc).date().isoformat()

    receivable = assert_ok(
        auth_client.post(
            "/obligations",
            json={
                "kind": "receivable",
                "title": unique("freelance-invoice"),
                "counterparty_name": "Smoke Client",
                "counterparty_type": "client",
                "principal_amount": 500_000,
                "issue_date": today,
                "due_date": today,
                "default_account_id": account_id,
            },
        )
    )
    payable = assert_ok(
        auth_client.post(
            "/obligations",
            json={
                "kind": "payable",
                "title": unique("vendor-bill"),
                "counterparty_name": "Smoke Vendor",
                "counterparty_type": "vendor",
                "principal_amount": 300_000,
                "issue_date": today,
                "due_date": today,
                "default_account_id": account_id,
            },
        )
    )

    listed = assert_ok(auth_client.get("/obligations", params={"status": "open,partial"}))["obligations"]
    assert {receivable["obligation_id"], payable["obligation_id"]}.issubset({row["obligation_id"] for row in listed})

    received = assert_ok(
        auth_client.post(
            f"/obligations/{receivable['obligation_id']}/settlements",
            json={"amount": 200_000, "account_id": account_id, "settled_at": today, "notes": "partial collection"},
        )
    )
    received_detail = assert_ok(auth_client.get(f"/obligations/{receivable['obligation_id']}"))
    assert received_detail["obligation"]["outstanding_amount"] == 300_000
    assert received_detail["obligation"]["status"] == "partial"
    assert received["transaction_id"]

    assert_ok(
        auth_client.post(
            f"/obligations/{payable['obligation_id']}/settlements",
            json={"amount": 300_000, "account_id": account_id, "settled_at": today},
        )
    )
    paid_detail = assert_ok(auth_client.get(f"/obligations/{payable['obligation_id']}"))
    assert paid_detail["obligation"]["outstanding_amount"] == 0
    assert paid_detail["obligation"]["status"] == "settled"

    accounts = assert_ok(auth_client.get("/accounts"))["accounts"]
    assert next(a for a in accounts if a["account_id"] == account_id)["balance"] == 900_000

    summary = assert_ok(auth_client.get("/obligations/summary"))
    assert summary["receivable_outstanding"] == 300_000
    assert summary["payable_outstanding"] == 0
    dashboard = assert_ok(auth_client.get("/dashboard"))
    assert dashboard["obligations"]["receivable_outstanding"] >= 300_000

    settlement_id = received_detail["settlements"][0]["settlement_id"]
    assert_ok(auth_client.delete(f"/obligations/{receivable['obligation_id']}/settlements/{settlement_id}"))
    reversed_detail = assert_ok(auth_client.get(f"/obligations/{receivable['obligation_id']}"))
    assert reversed_detail["obligation"]["outstanding_amount"] == 500_000
    assert reversed_detail["obligation"]["status"] == "open"


def test_switches_are_ledger_rows_not_cash_flow_totals(auth_client: TestClient):
    before_dashboard = assert_ok(auth_client.get("/dashboard"))
    before_summary_in, before_summary_out = summary_cash_totals(assert_ok(auth_client.get("/summary")))
    before_analysis = assert_ok(auth_client.get("/analysis"))["totals"]

    source_id = create_account(auth_client, unique("switch-total-source"), 0)
    target_id = create_account(auth_client, unique("switch-total-target"), 0)
    income_id = assert_ok(
        auth_client.post(
            "/transactions",
            json={
                "account_id": source_id,
                "transaction_type": "debit",
                "transaction_name": "Switch total income",
                "amount": 100_000,
                "date": past_iso(3),
            },
        )
    )["transaction_id"]
    expense_id = assert_ok(
        auth_client.post(
            "/transactions",
            json={
                "account_id": source_id,
                "transaction_type": "credit",
                "transaction_name": "Switch total expense",
                "amount": 10_000,
                "date": past_iso(2),
            },
        )
    )["transaction_id"]
    transfer_id = assert_ok(
        auth_client.post(
            "/switch",
            json={"source_account_id": source_id, "target_account_id": target_id, "amount": 25_000, "date": past_iso(1)},
        )
    )["transfer_id"]

    after_dashboard = assert_ok(auth_client.get("/dashboard"))
    assert after_dashboard["total_in"] - before_dashboard["total_in"] == 100_000
    assert after_dashboard["total_out"] - before_dashboard["total_out"] == 10_000

    after_summary_in, after_summary_out = summary_cash_totals(assert_ok(auth_client.get("/summary")))
    assert after_summary_in - before_summary_in == 100_000
    assert after_summary_out - before_summary_out == 10_000

    after_analysis = assert_ok(auth_client.get("/analysis"))["totals"]
    assert after_analysis["total_in"] - before_analysis["total_in"] == 100_000
    assert after_analysis["total_out"] - before_analysis["total_out"] == 10_000

    ledger_rows = assert_ok(auth_client.get("/ledger", params={"scope": "all", "include_switch": True, "limit": 100}))["rows"]
    switch_row = next(row for row in ledger_rows if row.get("transfer_id") == transfer_id)
    assert switch_row["is_transfer"] is True
    assert switch_row["debit"] == 25_000
    assert switch_row["credit"] == 25_000

    assert_ok(auth_client.delete(f"/switch/{transfer_id}"))
    assert_ok(auth_client.delete(f"/transactions/{expense_id}"))
    assert_ok(auth_client.delete(f"/transactions/{income_id}"))
    assert_ok(auth_client.delete(f"/accounts/{target_id}"))
    assert_ok(auth_client.delete(f"/accounts/{source_id}"))


def test_transactions_receipts_switches_loans_and_reports(auth_client: TestClient):
    source_id = create_account(auth_client, unique("source"), 500_000)
    target_id = create_account(auth_client, unique("target"), 0)
    category_id = assert_ok(auth_client.post("/categories", json={"name": unique("category"), "kind": "expense"}))[
        "category_id"
    ]

    tx = assert_ok(
        auth_client.post(
            "/transactions",
            json={
                "account_id": source_id,
                "transaction_type": "credit",
                "transaction_name": "Smoke purchase",
                "amount": 10_000,
                "date": now_iso(),
                "category_id": category_id,
                "notes": "smoke",
                "currency": "IDR",
                "tags": ["smoke"],
                "is_reviewed": True,
            },
        )
    )
    tx_id = tx["transaction_id"]
    assert_ok(
        auth_client.put(
            f"/transactions/{tx_id}",
            json={
                "account_id": source_id,
                "transaction_type": "credit",
                "transaction_name": "Smoke purchase updated",
                "amount": 12_000,
                "date": now_iso(),
                "category_id": category_id,
                "notes": "updated",
                "currency": "IDR",
                "tags": ["smoke", "updated"],
                "is_reviewed": True,
            },
        )
    )
    assert_ok(
        auth_client.post(
            f"/transactions/{tx_id}/receipt",
            files={"file": ("receipt.png", SMOKE_PNG, "image/png")},
            data={"category": "receipt"},
        )
    )
    assert_ok(auth_client.get(f"/transactions/{tx_id}/receipt"))
    assert auth_client.get(f"/transactions/{tx_id}/receipt/view").status_code == 200
    assert_ok(auth_client.delete(f"/transactions/{tx_id}/receipt"))

    transfer = assert_ok(
        auth_client.post(
            "/switch",
            json={"source_account_id": source_id, "target_account_id": target_id, "amount": 10_000, "date": now_iso()},
        )
    )
    transfer_id = transfer["transfer_id"]
    assert_ok(auth_client.get(f"/switch/{transfer_id}"))
    assert_ok(
        auth_client.put(
            f"/switch/{transfer_id}",
            json={"source_account_id": source_id, "target_account_id": target_id, "amount": 8_000, "date": now_iso()},
        )
    )
    categories = assert_ok(auth_client.get("/categories"))["categories"]
    switching_category = next(category for category in categories if category["name"] == "Switching")
    ledger_before_delete = assert_ok(
        auth_client.get("/ledger", params={"scope": "all", "include_switch": True, "limit": 50})
    )["rows"]
    transfer_rows = [row for row in ledger_before_delete if row.get("transfer_id") == transfer_id]
    assert len(transfer_rows) == 1
    assert transfer_rows[0]["debit"] == 8_000
    assert transfer_rows[0]["credit"] == 8_000
    assert transfer_rows[0]["category_id"] == switching_category["category_id"]
    assert_ok(auth_client.delete(f"/switch/{transfer_id}"))
    assert auth_client.get(f"/switch/{transfer_id}").status_code == 404
    ledger_after_delete = assert_ok(
        auth_client.get("/ledger", params={"scope": "all", "include_switch": True, "limit": 50})
    )["rows"]
    assert [row for row in ledger_after_delete if row.get("transfer_id") == transfer_id] == []

    lender = create_account(auth_client, unique("lender"), 100_000)
    borrower = create_account(auth_client, unique("borrower"), 0)
    shortfall_tx = assert_ok(
        auth_client.post(
            "/transactions",
            json={
                "account_id": borrower,
                "transaction_type": "credit",
                "transaction_name": "Smoke shortfall",
                "amount": 40_000,
                "date": now_iso(),
            },
        )
    )["transaction_id"]
    loan = assert_ok(
        auth_client.post(
            "/loans/from-transaction",
            json={"transaction_id": shortfall_tx, "lender_account_id": lender, "amount": 40_000},
        )
    )
    assert_ok(auth_client.get("/loans"))
    assert_ok(
        auth_client.post(
            "/transactions",
            json={
                "account_id": borrower,
                "transaction_type": "debit",
                "transaction_name": "Borrower top-up",
                "amount": 80_000,
                "date": future_iso(10),
            },
        )
    )
    assert_ok(auth_client.post(f"/loans/{loan['loan_id']}/finalize", json={"date": future_iso(11)}))

    assert_ok(auth_client.get("/ledger", params={"scope": "all", "limit": 10}))
    assert_ok(auth_client.get("/summary"))
    assert_ok(auth_client.get("/analysis"))
    assert_ok(auth_client.get("/analysis/budget-shift"))
    assert_ok(auth_client.get("/safety-net/report"))
    assert_ok(auth_client.post("/balances/recompute", json={}))
    assert_ok(auth_client.get("/transactions/audit"))
    assert_ok(auth_client.get("/export/preview", params={"day": 1, "scope": "all"}))
    assert auth_client.get("/export", params={"day": 1, "scope": "all", "format": "csv"}).status_code == 200
    assert_ok(auth_client.delete(f"/transactions/{tx_id}"))


def test_resource_routers(auth_client: TestClient):
    account_id = create_account(auth_client, unique("bucket-linked"), 100_000)
    account_id_2 = create_account(auth_client, unique("bucket-linked-2"), 50_000)
    bucket_id = assert_ok(
        auth_client.post(
            "/buckets",
            json={
                "name": unique("bucket"),
                "kind": "sinking",
                "target_amount": 100_000,
                "linked_account_ids": [account_id, account_id_2],
                "priority": 10,
            },
        )
    )["bucket_id"]
    buckets = assert_ok(auth_client.get("/buckets"))["buckets"]
    bucket = next(b for b in buckets if b["bucket_id"] == bucket_id)
    assert set(bucket["linked_account_ids"]) == {account_id, account_id_2}
    assert bucket["current_amount"] == 150_000
    assert_ok(
        auth_client.put(
            f"/buckets/{bucket_id}",
            json={
                "name": unique("bucket-updated"),
                "kind": "sinking",
                "target_amount": 120_000,
                "linked_account_id": account_id,
                "priority": 5,
            },
        )
    )
    buckets = assert_ok(auth_client.get("/buckets"))["buckets"]
    bucket = next(b for b in buckets if b["bucket_id"] == bucket_id)
    assert bucket["linked_account_ids"] == [account_id]
    assert bucket["current_amount"] == 100_000

    stale_account_id = create_account(auth_client, unique("stale-linked"), 10_000)
    stale_bucket_id = assert_ok(
        auth_client.post(
            "/buckets",
            json={
                "name": unique("stale-bucket"),
                "kind": "spending",
                "linked_account_id": stale_account_id,
                "priority": 30,
            },
        )
    )["bucket_id"]
    assert_ok(auth_client.delete(f"/accounts/{stale_account_id}"))
    replacement_account_id = create_account(auth_client, unique("replacement-linked"), 20_000)
    assert_ok(
        auth_client.put(
            f"/buckets/{stale_bucket_id}",
            json={
                "name": unique("stale-bucket-updated"),
                "kind": "spending",
                "linked_account_id": replacement_account_id,
                "priority": 30,
            },
        )
    )
    stale_bucket = next(b for b in assert_ok(auth_client.get("/buckets"))["buckets"] if b["bucket_id"] == stale_bucket_id)
    assert stale_bucket["linked_account_ids"] == [replacement_account_id]
    assert stale_bucket["current_amount"] == 20_000

    plan_month = f"2099-{(int(uuid4().hex[:2], 16) % 12) + 1:02d}"
    plan_id = assert_ok(auth_client.post("/allocation-plans", json={"month": plan_month, "expected_income": 500_000}))[
        "plan_id"
    ]
    item_id = assert_ok(
        auth_client.post(
            f"/allocation-plans/{plan_id}/items",
            json={"bucket_id": bucket_id, "label": "Smoke item", "mode": "fixed", "value": 50_000},
        )
    )["item_id"]
    assert_ok(auth_client.get("/allocation-plans"))
    plan_detail = assert_ok(auth_client.get(f"/allocation-plans/{plan_id}"))
    assert plan_detail["items"][0]["bucket_name"]
    assert plan_detail["items"][0]["bucket_kind"] == "sinking"
    assert "emergency_fund" in plan_detail["health"]
    assert plan_detail["items"][0]["include_in_emergency_base"] is False
    assert plan_detail["health"]["emergency_fund"]["monthly_need"] == 0
    assert_ok(
        auth_client.put(
            f"/allocation-plans/{plan_id}/items/{item_id}/emergency-base",
            json={"include_in_emergency_base": True},
        )
    )
    plan_detail = assert_ok(auth_client.get(f"/allocation-plans/{plan_id}"))
    assert plan_detail["health"]["emergency_fund"]["monthly_need"] == 50_000
    assert_ok(auth_client.put(f"/allocation-plans/{plan_id}", json={"expected_income": 600_000}))
    assert_ok(
        auth_client.put(
            f"/allocation-plans/{plan_id}/items/{item_id}",
            json={"bucket_id": bucket_id, "label": "Updated item", "mode": "fixed", "value": 60_000},
        )
    )
    strategy_suggestions = assert_ok(
        auth_client.post("/strategy-rules/from-allocation/preview", json={"plan_id": plan_id})
    )
    assert strategy_suggestions["suggestions"][0]["value"] == 10
    assert strategy_suggestions["suggestions"][0]["group"] == "cash_buffer"
    strategy_apply = assert_ok(auth_client.post("/strategy-rules/from-allocation/apply", json={"plan_id": plan_id}))
    assert strategy_apply["created"] == 1
    for created_rule in strategy_apply["rules"]:
        assert_ok(auth_client.delete(f"/strategy-rules/{created_rule['rule_id']}"))
    assert_ok(auth_client.post(f"/allocation-plans/{plan_id}/items/{item_id}/fund", json={"amount": 10_000}))
    assert_ok(auth_client.delete(f"/allocation-plans/{plan_id}/items/{item_id}"))
    assert_ok(auth_client.delete(f"/allocation-plans/{plan_id}"))

    payroll_id = create_account(auth_client, unique("payroll"), 0)
    spending_id = create_account(auth_client, unique("spending"), 0)
    assert_ok(
        auth_client.put(
            f"/accounts/{payroll_id}/profile",
            json={
                "profile_type": "dynamic_spending",
                "is_payroll_source": True,
                "is_no_limit": True,
                "is_buffer": False,
            },
        )
    )
    assert_ok(
        auth_client.put(
            f"/accounts/{spending_id}/profile",
            json={
                "profile_type": "dynamic_spending",
                "is_payroll_source": False,
                "is_no_limit": False,
                "is_buffer": False,
            },
        )
    )
    assert_ok(auth_client.put("/payday", json={"day": datetime.now().day}))
    payroll_tx_id = assert_ok(
        auth_client.post(
            "/transactions",
            json={
                "account_id": payroll_id,
                "transaction_type": "debit",
                "transaction_name": "Payroll",
                "amount": 500_000,
                "date": past_iso(),
                "is_cycle_topup": True,
                "is_reviewed": True,
            },
        )
    )["transaction_id"]
    # The transfer-aware ledger query must preserve is_cycle_topup on plain
    # cash-in transactions so the UI renders the Payroll badge and the edit
    # modal initializes the "Mark as Payroll" checkbox.
    payroll_rows = assert_ok(
        auth_client.get(
            "/ledger",
            params={"scope": "all", "include_switch": True, "limit": 100},
        )
    )["rows"]
    payroll_row = next(r for r in payroll_rows if r["transaction_id"] == payroll_tx_id)
    assert payroll_row["is_cycle_topup"] is True
    funding_month = next_month_ym()
    funding_plan_id = assert_ok(
        auth_client.post(
            "/allocation-plans",
            json={
                "month": funding_month,
                "expected_income": 500_000,
                "funding_source_account_id": payroll_id,
                "auto_fund_enabled": True,
            },
        )
    )["plan_id"]
    assert_ok(
        auth_client.post(
            f"/allocation-plans/{funding_plan_id}/items",
            json={
                "label": "Monthly spending",
                "mode": "fixed",
                "value": 200_000,
                "target_account_id": spending_id,
            },
        )
    )
    funding_detail = assert_ok(auth_client.get(f"/allocation-plans/{funding_plan_id}"))
    assert funding_detail["items"][0]["include_in_emergency_base"] is True
    assert funding_detail["health"]["emergency_fund"]["monthly_need"] == 200_000
    assert_ok(
        auth_client.put(
            f"/allocation-plans/{funding_plan_id}/items/{funding_detail['items'][0]['item_id']}/emergency-base",
            json={"include_in_emergency_base": False},
        )
    )
    funding_detail = assert_ok(auth_client.get(f"/allocation-plans/{funding_plan_id}"))
    assert funding_detail["health"]["emergency_fund"]["monthly_need"] == 0
    assert_ok(
        auth_client.put(
            f"/allocation-plans/{funding_plan_id}/items/{funding_detail['items'][0]['item_id']}/emergency-base",
            json={"include_in_emergency_base": True},
        )
    )
    assert_ok(auth_client.post(f"/allocation-plans/{funding_plan_id}/activate"))
    status = assert_ok(auth_client.get(f"/allocation-plans/{funding_plan_id}/funding-status"))
    assert status["can_allocate"] is True
    run = assert_ok(auth_client.post(f"/allocation-plans/{funding_plan_id}/allocate-funds", json={}))
    assert run["status"] == "succeeded"
    funded_plan = assert_ok(auth_client.get(f"/allocation-plans/{funding_plan_id}"))
    assert funded_plan["items"][0]["status"] == "funded"
    dashboard = assert_ok(auth_client.get("/dashboard"))
    assert dashboard["allocation_plan"]["expected_income"] == 500_000
    assert dashboard["metrics"]["safe_to_spend"]["source"] == "allocation"
    assert dashboard["metrics"]["safe_to_spend"]["value"] == 200_000
    safe_breakdown = dashboard["metrics"]["safe_to_spend"]["breakdown"]
    assert safe_breakdown["planned_spending"] == 200_000
    assert safe_breakdown["remaining_spend_budget"] == 200_000
    assert safe_breakdown["payables_due_this_cycle"] == 0
    assert safe_breakdown["final_safe_to_spend"] == 200_000
    assert any(account["account_id"] == spending_id and account["balance"] == 200_000 for account in safe_breakdown["spendable_accounts"])
    monthly_spending = next(item for item in safe_breakdown["spending_allocations"] if item["label"] == "Monthly spending")
    assert monthly_spending["target_account_id"] == spending_id
    assert monthly_spending["planned_amount"] == 200_000
    assert monthly_spending["remaining_amount"] == 0
    assert dashboard["metrics"]["emergency_fund"]["breakdown"]["monthly_spending_base"] == 200_000
    budgets = assert_ok(auth_client.get("/budgets", params={"month": funding_month}))["budgets"]
    generated_budget = next(b for b in budgets if b["account_id"] == spending_id)
    assert generated_budget["amount"] == 200_000
    assert generated_budget["source"] == "allocation"
    summary_accounts = assert_ok(auth_client.get("/summary", params={"month": funding_month}))["accounts"]
    spending_summary = next(a for a in summary_accounts if a["account_id"] == spending_id)
    assert spending_summary["budget"] == 200_000
    assert spending_summary["budget_source"] == "allocation"
    protected_delete = auth_client.delete(f"/allocation-plans/{funding_plan_id}")
    assert protected_delete.status_code == 200, protected_delete.text
    # Confirm budgets that were generated by the allocation are cleaned up.
    budgets_after_delete = assert_ok(
        auth_client.get("/budgets", params={"month": funding_month})
    )["budgets"]
    assert all(b["account_id"] != spending_id for b in budgets_after_delete)

    past_plan_month = f"2000-{(int(uuid4().hex[:2], 16) % 12) + 1:02d}"
    past_plan_id = assert_ok(
        auth_client.post("/allocation-plans", json={"month": past_plan_month, "expected_income": 500_000})
    )["plan_id"]
    assert_ok(auth_client.post(f"/allocation-plans/{past_plan_id}/activate"))
    assert_ok(auth_client.delete(f"/allocation-plans/{past_plan_id}"))

    current_plan_month = datetime.now().strftime("%Y-%m")
    current_plan_id = assert_ok(
        auth_client.post("/allocation-plans", json={"month": current_plan_month, "expected_income": 500_000})
    )["plan_id"]
    assert_ok(auth_client.post(f"/allocation-plans/{current_plan_id}/activate"))
    assert_ok(auth_client.delete(f"/allocation-plans/{current_plan_id}"))

    future_plan_month = f"2097-{(int(uuid4().hex[:2], 16) % 12) + 1:02d}"
    future_plan_id = assert_ok(
        auth_client.post("/allocation-plans", json={"month": future_plan_month, "expected_income": 500_000})
    )["plan_id"]
    assert_ok(auth_client.post(f"/allocation-plans/{future_plan_id}/activate"))
    # Active plans must accept edits to expected_income / notes / funding settings.
    assert_ok(
        auth_client.put(
            f"/allocation-plans/{future_plan_id}",
            json={"expected_income": 750_000, "notes": "edited while active"},
        )
    )
    edited_active = assert_ok(auth_client.get(f"/allocation-plans/{future_plan_id}"))
    assert edited_active["expected_income"] == 750_000
    assert edited_active["notes"] == "edited while active"
    # Active plans (including future months) must be deletable.
    assert_ok(auth_client.delete(f"/allocation-plans/{future_plan_id}"))

    rule_id = assert_ok(
        auth_client.post(
            "/strategy-rules",
            json={"name": unique("rule"), "trigger": "income_arrival", "mode": "percent", "value": 10},
        )
    )["rule_id"]
    assert_ok(auth_client.get("/strategy-rules"))
    assert_ok(
        auth_client.put(
            f"/strategy-rules/{rule_id}",
            json={"name": unique("rule-updated"), "trigger": "income_arrival", "mode": "fixed", "value": 10_000},
        )
    )
    fixed_rule_id = rule_id
    target_rule_id = assert_ok(
        auth_client.post(
            "/strategy-rules",
            json={"name": unique("target-rule"), "trigger": "manual", "mode": "target_balance", "target_bucket_id": bucket_id, "value": 99_000},
        )
    )["rule_id"]
    met_bucket_id = assert_ok(
        auth_client.post(
            "/buckets",
            json={
                "name": unique("met-bucket"),
                "kind": "goal",
                "target_amount": 50_000,
                "linked_account_id": account_id,
                "priority": 20,
            },
        )
    )["bucket_id"]
    met_rule_id = assert_ok(
        auth_client.post(
            "/strategy-rules",
            json={"name": unique("met-rule"), "trigger": "manual", "mode": "target_balance", "target_bucket_id": met_bucket_id, "value": 99_000},
        )
    )["rule_id"]
    percent_rule_id = assert_ok(
        auth_client.post(
            "/strategy-rules",
            json={"name": unique("percent-rule"), "trigger": "manual", "mode": "percent", "value": 10},
        )
    )["rule_id"]
    overflow_rule_id = assert_ok(
        auth_client.post(
            "/strategy-rules",
            json={"name": unique("overflow-rule"), "trigger": "manual", "mode": "overflow", "value": 0},
        )
    )["rule_id"]

    preview = assert_ok(auth_client.post("/strategy-rules/preview", json={"income": 200_000}))
    amounts = {a["rule_id"]: a["amount"] for a in preview["allocations"]}
    assert amounts[fixed_rule_id] == 10_000
    assert amounts[target_rule_id] == 20_000
    assert amounts[met_rule_id] == 0
    assert amounts[percent_rule_id] == 20_000
    assert amounts[overflow_rule_id] == 150_000
    assert preview["remaining"] == 0
    assert preview["summary"]
    assert any(group["group"] == "cash_buffer" for group in preview["summary"])

    strategy_month = f"2098-{(int(uuid4().hex[:2], 16) % 12) + 1:02d}"
    applied = assert_ok(
        auth_client.post("/strategy-rules/apply", json={"month": strategy_month, "expected_income": 200_000})
    )
    assert applied["total_allocated"] == 200_000
    applied_plan = assert_ok(auth_client.get(f"/allocation-plans/{applied['plan_id']}"))
    assert len(applied_plan["items"]) == 4
    duplicate = auth_client.post("/strategy-rules/apply", json={"month": strategy_month, "expected_income": 200_000})
    assert duplicate.status_code == 400
    assert_ok(auth_client.delete(f"/allocation-plans/{applied['plan_id']}"))

    for rid in [fixed_rule_id, target_rule_id, met_rule_id, percent_rule_id, overflow_rule_id]:
        assert_ok(auth_client.delete(f"/strategy-rules/{rid}"))

    goal_id = assert_ok(
        auth_client.post(
            "/goals",
            json={"name": unique("goal"), "target_amount": 1_000_000, "target_date": "2026-12-31"},
        )
    )["goal_id"]
    assert_ok(auth_client.get("/goals"))
    assert_ok(auth_client.get(f"/goals/{goal_id}"))
    assert_ok(
        auth_client.put(
            f"/goals/{goal_id}",
            json={"name": unique("goal-updated"), "target_amount": 1_200_000, "target_date": "2026-12-31"},
        )
    )
    assert_ok(auth_client.post(f"/goals/{goal_id}/contribute", json={"amount": 25_000, "source": "manual"}))
    assert_ok(auth_client.get(f"/goals/{goal_id}/projection", params={"monthly_contribution": 100_000}))
    assert_ok(auth_client.delete(f"/goals/{goal_id}"))

    linked_goal_id = assert_ok(
        auth_client.post(
            "/goals",
            json={"name": unique("linked-goal"), "target_amount": 200_000, "linked_bucket_id": bucket_id},
        )
    )["goal_id"]
    linked_goal = assert_ok(auth_client.get(f"/goals/{linked_goal_id}"))
    assert linked_goal["progress_source"] == "linked_bucket"
    assert linked_goal["current_amount"] == 100_000
    dashboard_with_goal = assert_ok(auth_client.get("/dashboard"))
    linked_goal_dash = next(g for g in dashboard_with_goal["goals"] if g["goal"] == linked_goal["name"])
    assert linked_goal_dash["progress_pct"] == 50
    linked_contribution = auth_client.post(f"/goals/{linked_goal_id}/contribute", json={"amount": 25_000})
    assert linked_contribution.status_code == 400
    assert_ok(auth_client.delete(f"/goals/{linked_goal_id}"))

    auto_goal_name = unique("auto-goal")
    auto_goal = assert_ok(
        auth_client.post(
            "/goals",
            json={"name": auto_goal_name, "target_amount": 300_000, "create_linked_bucket": True},
        )
    )
    assert auto_goal["created_bucket"] is True
    assert auto_goal["linked_bucket_id"]
    auto_goal_detail = assert_ok(auth_client.get(f"/goals/{auto_goal['goal_id']}"))
    assert auto_goal_detail["progress_source"] == "linked_bucket"
    assert auto_goal_detail["linked_bucket_id"] == auto_goal["linked_bucket_id"]
    auto_bucket = next(
        b for b in assert_ok(auth_client.get("/buckets"))["buckets"] if b["bucket_id"] == auto_goal["linked_bucket_id"]
    )
    assert auto_bucket["name"] == auto_goal_name
    assert auto_bucket["kind"] == "goal"
    assert auto_bucket["target_amount"] == 300_000
    assert_ok(auth_client.delete(f"/goals/{auto_goal['goal_id']}"))

    asset_id = assert_ok(
        auth_client.post("/assets", json={"name": unique("asset"), "class": "stock", "currency": "IDR"})
    )["asset_id"]
    holding_id = assert_ok(
        auth_client.post(
            f"/assets/{asset_id}/holdings",
            json={"quantity": 2, "cost_basis": 100_000, "acquired_at": "2026-01-01", "account_id": account_id},
        )
    )["holding_id"]
    assert_ok(auth_client.get("/assets"))
    assert_ok(auth_client.put(f"/assets/{asset_id}", json={"name": unique("asset-updated"), "class": "stock"}))
    assert_ok(auth_client.get(f"/assets/{asset_id}/holdings"))
    assert_ok(auth_client.post(f"/assets/{asset_id}/snapshots", json={"unit_price": 75_000, "as_of_date": "2026-01-02"}))
    assert_ok(auth_client.get("/assets/net-worth"))
    assert_ok(auth_client.post("/assets/net-worth/snapshot"))
    assert_ok(auth_client.delete(f"/assets/{asset_id}/holdings/{holding_id}"))
    assert_ok(auth_client.delete(f"/assets/{asset_id}"))
    assert_ok(auth_client.get("/periods"))
    assert_ok(auth_client.get("/dashboard"))


def test_public_v1_smoke(client: TestClient, api_key: str):
    headers = {"Authorization": f"Bearer {api_key}"}
    assert_ok(client.post("/v1/api-key/info", json={}, headers=headers))
    account_id = assert_ok(
        client.post(
            "/v1/accounts",
            json={"account_name": unique("public-account"), "initial_balance": 100_000, "monthly_limit": 50_000},
            headers=headers,
        )
    )["account_id"]
    accounts = assert_ok(client.post("/v1/accounts/list", json={}, headers=headers))["accounts"]
    account = next(a for a in accounts if a["account_id"] == account_id)
    assert account["balance"] == 100_000
    renamed = unique("public-account-renamed")
    rename_body = assert_ok(client.put(f"/v1/accounts/{account_id}", json={"account_name": renamed}, headers=headers))
    assert rename_body["account"]["account_name"] == renamed
    accounts = assert_ok(client.post("/v1/accounts/list", json={}, headers=headers))["accounts"]
    assert next(a for a in accounts if a["account_id"] == account_id)["account_name"] == renamed
    assert_ok(client.get("/v1/categories", headers=headers))
    profile_renamed = unique("public-profile-renamed")
    profile_body = assert_ok(
        client.put(
            f"/v1/accounts/{account_id}/profile",
            json={"account_name": profile_renamed, "profile_type": "dynamic_spending"},
            headers=headers,
        )
    )
    assert profile_body["account"]["account_name"] == profile_renamed
    target_account_id = assert_ok(
        client.post(
            "/v1/accounts",
            json={"account_name": unique("public-target"), "initial_balance": 10_000},
            headers=headers,
        )
    )["account_id"]
    assert_ok(client.get("/v1/periods", headers=headers))
    assert_ok(client.get("/v1/buckets", headers=headers))
    assert_ok(client.get("/v1/allocation-plans", headers=headers))
    assert_ok(client.get("/v1/strategy-rules", headers=headers))
    assert_ok(client.get("/v1/goals", headers=headers))
    assert_ok(client.get("/v1/assets", headers=headers))
    assert_ok(client.get("/v1/assets/net-worth", headers=headers))
    assert_ok(client.get("/v1/dashboard", headers=headers))

    tx_id = assert_ok(
        client.post(
            "/v1/transactions",
            json={
                "account_id": account_id,
                "transaction_type": "credit",
                "transaction_name": "Public smoke expense",
                "amount": 5_000,
                "date": now_iso(),
            },
            headers=headers,
        )
    )["transaction_id"]
    transfer_id = assert_ok(
        client.post(
            "/v1/switch",
            json={
                "source_account_id": account_id,
                "target_account_id": target_account_id,
                "amount": 1_000,
                "date": now_iso(),
            },
            headers=headers,
        )
    )["transfer_id"]
    switch_detail = assert_ok(client.get(f"/v1/switch/{transfer_id}", headers=headers))
    assert switch_detail["amount"] == 1_000
    assert_ok(
        client.put(
            f"/v1/switch/{transfer_id}",
            json={"amount": 2_000, "is_cycle_topup": True},
            headers=headers,
        )
    )
    switch_detail = assert_ok(client.get(f"/v1/switch/{transfer_id}", headers=headers))
    assert switch_detail["amount"] == 2_000
    assert switch_detail["is_cycle_topup"] is True
    assert_ok(client.post("/v1/ledger", json={"scope": "all", "limit": 10}, headers=headers))
    assert_ok(client.post("/v1/ledger", json={"scope": "all", "limit": 10, "kind": "expense", "q": "Public smoke"}, headers=headers))
    assert_ok(client.post("/v1/summary", json={}, headers=headers))
    assert_ok(client.post("/v1/analysis", json={}, headers=headers))
    assert_ok(client.post("/v1/analysis/budget-shift", json={}, headers=headers))
    assert_ok(client.get("/v1/payday", headers=headers))
    assert_ok(client.post("/v1/balances/recompute", json={}, headers=headers))
    assert_ok(client.post("/v1/transactions/audit", json={}, headers=headers))
    assert_ok(client.post("/v1/export/preview", json={"day": 1, "scope": "all"}, headers=headers))
    assert client.post("/v1/export", json={"day": 1, "scope": "all", "format": "csv"}, headers=headers).status_code == 200
    assert_ok(client.delete(f"/v1/switch/{transfer_id}", headers=headers))
    assert_ok(client.delete(f"/v1/transactions/{tx_id}", headers=headers))
