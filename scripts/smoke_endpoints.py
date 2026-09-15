#!/usr/bin/env python3
"""End-to-end smoke test for the live API proxy.

Defaults to the local docker-compose web proxy:

    python scripts/smoke_endpoints.py

Override with:

    SMOKE_BASE_URL=http://127.0.0.1:8090/api SMOKE_INVITE_CODE=CASHFLOWTRACKER python scripts/smoke_endpoints.py
"""
from __future__ import annotations

import json
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from http.cookies import SimpleCookie
from typing import Any
from urllib import error, parse, request


BASE_URL = os.getenv("SMOKE_BASE_URL", "http://127.0.0.1:8090/api").rstrip("/")
INVITE_CODE = os.getenv("SMOKE_INVITE_CODE", "CASHFLOWTRACKER")
SMOKE_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x02\x00\x00\x00\x90wS\xde\x00\x00\x00\x0cIDATx\x9cc```\x00"
    b"\x00\x00\x04\x00\x01\xf6\x178U\x00\x00\x00\x00IEND\xaeB`\x82"
)


class SmokeError(AssertionError):
    pass


class ApiClient:
    def __init__(self, base_url: str, bearer: str | None = None):
        self.base_url = base_url.rstrip("/")
        self.bearer = bearer
        self.cookies: dict[str, str] = {}

    def child_with_bearer(self, bearer: str) -> "ApiClient":
        return ApiClient(self.base_url, bearer=bearer)

    def request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        query: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        multipart: dict[str, tuple[str, bytes, str] | str] | None = None,
        expected: int | tuple[int, ...] = 200,
        raw: bool = False,
    ) -> Any:
        if isinstance(expected, int):
            expected = (expected,)
        url = self.base_url + path
        if query:
            clean_query = {k: v for k, v in query.items() if v is not None}
            url += "?" + parse.urlencode(clean_query)

        body: bytes | None = None
        req_headers = {"Accept": "application/json"}
        if self.bearer:
            req_headers["Authorization"] = f"Bearer {self.bearer}"
        if self.cookies:
            req_headers["Cookie"] = "; ".join(f"{k}={v}" for k, v in self.cookies.items())
        if headers:
            req_headers.update(headers)

        if multipart is not None:
            boundary = "----smoke-" + uuid.uuid4().hex
            chunks: list[bytes] = []
            for name, value in multipart.items():
                chunks.append(f"--{boundary}\r\n".encode())
                if isinstance(value, tuple):
                    filename, content, content_type = value
                    chunks.append(
                        (
                            f'Content-Disposition: form-data; name="{name}"; filename="{filename}"\r\n'
                            f"Content-Type: {content_type}\r\n\r\n"
                        ).encode()
                    )
                    chunks.append(content)
                    chunks.append(b"\r\n")
                else:
                    chunks.append(f'Content-Disposition: form-data; name="{name}"\r\n\r\n{value}\r\n'.encode())
            chunks.append(f"--{boundary}--\r\n".encode())
            body = b"".join(chunks)
            req_headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
        elif json_body is not None:
            body = json.dumps(json_body).encode()
            req_headers["Content-Type"] = "application/json"

        req = request.Request(url, data=body, headers=req_headers, method=method.upper())
        try:
            with request.urlopen(req, timeout=20) as res:
                payload = res.read()
                self._store_cookies(res.headers.get_all("Set-Cookie") or [])
                status = res.status
                content_type = res.headers.get("Content-Type", "")
        except error.HTTPError as exc:
            payload = exc.read()
            status = exc.code
            content_type = exc.headers.get("Content-Type", "")
        except error.URLError as exc:
            raise SmokeError(f"{method} {path}: connection failed: {exc}") from exc

        if status not in expected:
            detail = payload.decode(errors="replace")[:800]
            raise SmokeError(f"{method} {path}: expected {expected}, got {status}: {detail}")
        if raw:
            return payload
        if not payload:
            return {}
        if "application/json" not in content_type:
            return {"raw": payload.decode(errors="replace")}
        return json.loads(payload.decode())

    def get(self, path: str, **kwargs: Any) -> Any:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> Any:
        return self.request("POST", path, **kwargs)

    def put(self, path: str, **kwargs: Any) -> Any:
        return self.request("PUT", path, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> Any:
        return self.request("DELETE", path, **kwargs)

    def _store_cookies(self, set_cookie_headers: list[str]) -> None:
        for header in set_cookie_headers:
            jar = SimpleCookie()
            jar.load(header)
            for key, morsel in jar.items():
                self.cookies[key] = morsel.value


class SmokeRun:
    def __init__(self):
        suffix = f"{int(time.time())}_{uuid.uuid4().hex[:6]}"
        self.username = f"smoke_{suffix}"
        self.password = "smokepassword1"
        self.now = future_iso(5)
        self.month = datetime.now().strftime("%Y-%m")
        self.client = ApiClient(BASE_URL)
        self.api: ApiClient | None = None

    def run(self) -> None:
        self.auth()
        source_id, target_id = self.accounts_and_budgets()
        category_id = self.categories()
        tx_id = self.transactions(source_id, category_id)
        self.switches(source_id, target_id)
        self.loans()
        bucket_id = self.buckets(target_id)
        plan_id = self.allocation(bucket_id)
        self.strategy(bucket_id)
        self.goals(bucket_id)
        self.assets(source_id)
        self.reporting()
        self.public_v1()
        self.cleanup(source_id, target_id, tx_id, plan_id)

    def auth(self) -> None:
        self.client.get("/health")
        self.client.post(
            "/auth/register",
            json_body={
                "username": self.username,
                "password": self.password,
                "full_name": "Smoke Test",
                "invite_code": INVITE_CODE,
            },
        )
        self.client.post("/auth/login", json_body={"username": self.username, "password": self.password})
        me = self.client.get("/me")
        assert_equal(me["username"], self.username, "session user")
        self.client.get("/api-key")
        key = self.client.post("/api-key/reset", json_body={})["api_key"]
        self.api = self.client.child_with_bearer(key)
        self.api.post("/v1/api-key/info", json_body={})
        reset = self.api.post("/v1/api-key/reset", json_body={})
        self.api = self.client.child_with_bearer(reset["api_key"])

    def accounts_and_budgets(self) -> tuple[str, str]:
        self.client.get("/accounts")
        source = self.client.post(
            "/accounts",
            json_body={"account_name": "Smoke Cash", "initial_balance": 500_000},
        )["account_id"]
        target = self.client.post("/accounts", json_body={"account_name": "Smoke Wallet"})["account_id"]
        account = find_by_account_id(self.client.get("/accounts")["accounts"], source, "account")
        assert_equal(account["balance"], 500_000, "created account opening balance")
        summary_account = find_by_account_id(self.client.get("/summary")["accounts"], source, "summary account")
        assert_equal(summary_account["current_balance"], 500_000, "summary opening balance")
        renamed = self.client.put(f"/accounts/{source}", json_body={"account_name": "Smoke Cash Updated"})
        assert_equal(renamed["account"]["account_name"], "Smoke Cash Updated", "renamed account response")
        account = find_by_account_id(self.client.get("/accounts")["accounts"], source, "renamed account")
        assert_equal(account["account_name"], "Smoke Cash Updated", "renamed account list")
        self.client.put(
            f"/accounts/{source}/profile",
            json_body={
                "account_name": "Smoke Cash Updated",
                "profile_type": "fixed_spending",
                "is_payroll_source": True,
                "is_no_limit": False,
                "is_buffer": True,
                "fixed_limit_amount": 250_000,
                "institution": "Smoke Bank",
                "account_number": "1234",
            },
        )
        self.client.get("/budgets", query={"month": self.month})
        budget_id = self.client.post(
            "/budgets",
            json_body={"account_id": source, "month": self.month, "amount": 250_000},
        )["budget_id"]
        self.client.put(f"/budgets/{budget_id}", json_body={"amount": 300_000})
        self.client.delete(f"/budgets/{budget_id}")
        return source, target

    def categories(self) -> str:
        self.client.get("/categories")
        category_id = self.client.post(
            "/categories",
            json_body={"name": "Smoke Expense", "kind": "expense", "color": "#2563eb", "icon": "receipt"},
        )["category_id"]
        self.client.put(
            f"/categories/{category_id}",
            json_body={"name": "Smoke Expense Updated", "kind": "expense", "color": "#0f766e", "icon": "tag"},
        )
        return category_id

    def transactions(self, account_id: str, category_id: str) -> str:
        tx = self.client.post(
            "/transactions",
            json_body={
                "account_id": account_id,
                "transaction_type": "credit",
                "transaction_name": "Smoke Purchase",
                "amount": 10_000,
                "date": self.now,
                "category_id": category_id,
                "notes": "smoke",
                "currency": "IDR",
                "tags": ["smoke"],
                "is_reviewed": True,
            },
        )
        tx_id = tx["transaction_id"]
        self.client.put(
            f"/transactions/{tx_id}",
            json_body={
                "account_id": account_id,
                "transaction_type": "credit",
                "transaction_name": "Smoke Purchase Updated",
                "amount": 12_000,
                "date": self.now,
                "category_id": category_id,
                "notes": "updated",
                "currency": "IDR",
                "tags": ["smoke", "updated"],
                "is_reviewed": True,
            },
        )
        self.client.post(
            f"/transactions/{tx_id}/receipt",
            multipart={"category": "receipt", "file": ("receipt.png", SMOKE_PNG, "image/png")},
        )
        self.client.get(f"/transactions/{tx_id}/receipt")
        self.client.get(f"/transactions/{tx_id}/receipt/view", raw=True)
        self.client.delete(f"/transactions/{tx_id}/receipt")
        return tx_id

    def switches(self, source_id: str, target_id: str) -> None:
        transfer_id = self.client.post(
            "/switch",
            json_body={
                "source_account_id": source_id,
                "target_account_id": target_id,
                "amount": 15_000,
                "date": self.now,
            },
        )["transfer_id"]
        self.client.get(f"/switch/{transfer_id}")
        self.client.put(
            f"/switch/{transfer_id}",
            json_body={
                "source_account_id": source_id,
                "target_account_id": target_id,
                "amount": 12_000,
                "date": self.now,
            },
        )
        self.client.delete(f"/switch/{transfer_id}")

    def loans(self) -> None:
        lender = self.client.post("/accounts", json_body={"account_name": "Smoke Lender", "initial_balance": 100_000})[
            "account_id"
        ]
        borrower = self.client.post("/accounts", json_body={"account_name": "Smoke Borrower"})["account_id"]
        trigger = self.client.post(
            "/transactions",
            json_body={
                "account_id": borrower,
                "transaction_type": "credit",
                "transaction_name": "Smoke Shortfall",
                "amount": 40_000,
                "date": self.now,
            },
        )["transaction_id"]
        loan = self.client.post(
            "/loans/from-transaction",
            json_body={"transaction_id": trigger, "lender_account_id": lender, "amount": 40_000},
        )
        self.client.get("/loans")
        self.client.post(
            "/transactions",
            json_body={
                "account_id": borrower,
                "transaction_type": "debit",
                "transaction_name": "Smoke Borrower Topup",
                "amount": 80_000,
                "date": future_iso(10),
            },
        )
        self.client.post(f"/loans/{loan['loan_id']}/finalize", json_body={"date": future_iso(11)})
        self.client.get("/loans", query={"status": "all"})

    def buckets(self, account_id: str) -> str:
        self.client.get("/buckets")
        bucket_id = self.client.post(
            "/buckets",
            json_body={
                "name": "Smoke Bucket",
                "kind": "sinking",
                "target_amount": 100_000,
                "linked_account_ids": [account_id],
                "priority": 20,
                "notes": "smoke",
            },
        )["bucket_id"]
        self.client.put(
            f"/buckets/{bucket_id}",
            json_body={
                "name": "Smoke Bucket Updated",
                "kind": "sinking",
                "target_amount": 120_000,
                "linked_account_id": account_id,
                "priority": 10,
                "notes": "updated",
            },
        )
        return bucket_id

    def allocation(self, bucket_id: str) -> str:
        self.client.get("/allocation-plans")
        plan_id = self.client.post(
            "/allocation-plans",
            json_body={"month": self.month, "expected_income": 500_000, "notes": "smoke"},
        )["plan_id"]
        self.client.get(f"/allocation-plans/{plan_id}")
        self.client.put(f"/allocation-plans/{plan_id}", json_body={"expected_income": 600_000, "notes": "updated"})
        item_id = self.client.post(
            f"/allocation-plans/{plan_id}/items",
            json_body={"bucket_id": bucket_id, "label": "Smoke Item", "mode": "fixed", "value": 50_000, "priority": 10},
        )["item_id"]
        self.client.put(
            f"/allocation-plans/{plan_id}/items/{item_id}",
            json_body={
                "bucket_id": bucket_id,
                "label": "Smoke Item Updated",
                "mode": "fixed",
                "value": 60_000,
                "priority": 5,
            },
        )
        self.client.post(f"/allocation-plans/{plan_id}/items/{item_id}/fund", json_body={"amount": 10_000})
        self.client.delete(f"/allocation-plans/{plan_id}/items/{item_id}")
        active_plan = self.client.post(
            "/allocation-plans",
            json_body={"month": next_month(), "expected_income": 400_000, "notes": "activate"},
        )["plan_id"]
        self.client.post(f"/allocation-plans/{active_plan}/activate")
        return plan_id

    def strategy(self, bucket_id: str) -> None:
        self.client.get("/strategy-rules")
        rule_id = self.client.post(
            "/strategy-rules",
            json_body={
                "name": "Smoke Rule",
                "trigger": "income_arrival",
                "mode": "percent",
                "target_bucket_id": bucket_id,
                "value": 10,
                "priority": 10,
            },
        )["rule_id"]
        self.client.put(
            f"/strategy-rules/{rule_id}",
            json_body={
                "name": "Smoke Rule Updated",
                "trigger": "income_arrival",
                "mode": "fixed",
                "target_bucket_id": bucket_id,
                "value": 20_000,
                "priority": 8,
                "is_active": True,
            },
        )
        self.client.post("/strategy-rules/preview", json_body={"income": 200_000})
        self.client.delete(f"/strategy-rules/{rule_id}")

    def goals(self, bucket_id: str) -> None:
        self.client.get("/goals")
        goal_id = self.client.post(
            "/goals",
            json_body={
                "name": "Smoke Goal",
                "target_amount": 1_000_000,
                "target_date": "2026-12-31",
                "linked_bucket_id": bucket_id,
                "priority": 10,
            },
        )["goal_id"]
        self.client.get(f"/goals/{goal_id}")
        self.client.put(
            f"/goals/{goal_id}",
            json_body={
                "name": "Smoke Goal Updated",
                "target_amount": 1_200_000,
                "target_date": "2026-12-31",
                "linked_bucket_id": bucket_id,
                "priority": 9,
                "status": "active",
            },
        )
        self.client.post(f"/goals/{goal_id}/contribute", json_body={"amount": 25_000, "source": "manual"})
        self.client.get(f"/goals/{goal_id}/projection", query={"monthly_contribution": 100_000})
        self.client.delete(f"/goals/{goal_id}")

    def assets(self, account_id: str) -> None:
        self.client.get("/assets")
        asset_id = self.client.post(
            "/assets",
            json_body={"name": "Smoke Asset", "class": "stock", "currency": "IDR", "ticker": "SMK"},
        )["asset_id"]
        self.client.put(
            f"/assets/{asset_id}",
            json_body={"name": "Smoke Asset Updated", "class": "stock", "ticker": "SMK2", "is_active": True},
        )
        holding_id = self.client.post(
            f"/assets/{asset_id}/holdings",
            json_body={
                "quantity": 2,
                "cost_basis": 100_000,
                "acquired_at": "2026-01-01",
                "account_id": account_id,
            },
        )["holding_id"]
        self.client.post(f"/assets/{asset_id}/snapshots", json_body={"unit_price": 75_000, "as_of_date": "2026-01-02"})
        self.client.get(f"/assets/{asset_id}/holdings")
        self.client.get("/assets/net-worth")
        self.client.post("/assets/net-worth/snapshot")
        self.client.delete(f"/assets/{asset_id}/holdings/{holding_id}")
        self.client.delete(f"/assets/{asset_id}")

    def reporting(self) -> None:
        self.client.get("/ledger", query={"scope": "all", "limit": 20})
        self.client.get("/summary", query={"month": self.month})
        self.client.get("/analysis", query={"month": self.month})
        self.client.get("/analysis/budget-shift", query={"month": self.month, "mode": "dynamic"})
        self.client.get("/safety-net/report")
        self.client.get("/payday", query={"month": self.month})
        self.client.put("/payday", json_body={"month": self.month, "day": 26})
        self.client.get("/payday", query={"month": self.month})
        self.client.put("/payday", json_body={"month": self.month, "clear_override": True})
        self.client.put("/payday", json_body={"day": 25})
        self.client.post("/balances/recompute", json_body={})
        self.client.get("/transactions/audit")
        self.client.get("/export/preview", query={"day": 1, "scope": "all"})
        self.client.get("/export", query={"day": 1, "scope": "all", "format": "csv"}, raw=True)
        self.client.get("/periods")
        self.client.get("/dashboard")

    def public_v1(self) -> None:
        if self.api is None:
            raise SmokeError("public API client was not initialized")
        api = self.api
        api.post("/v1/accounts/list", json_body={})
        account_id = api.post(
            "/v1/accounts",
            json_body={"account_name": "Smoke Public Account", "initial_balance": 100_000, "monthly_limit": 50_000},
        )["account_id"]
        account = find_by_account_id(api.post("/v1/accounts/list", json_body={})["accounts"], account_id, "public account")
        assert_equal(account["balance"], 100_000, "public created account opening balance")
        renamed = api.put(f"/v1/accounts/{account_id}", json_body={"account_name": "Smoke Public Account Updated"})
        assert_equal(renamed["account"]["account_name"], "Smoke Public Account Updated", "public renamed account response")
        account = find_by_account_id(api.post("/v1/accounts/list", json_body={})["accounts"], account_id, "public renamed account")
        assert_equal(account["account_name"], "Smoke Public Account Updated", "public renamed account list")
        profile = api.put(
            f"/v1/accounts/{account_id}/profile",
            json_body={"account_name": "Smoke Public Account Updated", "profile_type": "dynamic_spending"},
        )
        assert_equal(profile["account"]["account_name"], "Smoke Public Account Updated", "public profile rename response")
        budget_id = api.post("/v1/budgets", json_body={"account_id": account_id, "month": self.month, "amount": 50_000})[
            "budget_id"
        ]
        api.get("/v1/budgets", query={"month": self.month})
        api.put(f"/v1/budgets/{budget_id}", json_body={"amount": 55_000})
        api.delete(f"/v1/budgets/{budget_id}")
        tx_id = api.post(
            "/v1/transactions",
            json_body={
                "account_id": account_id,
                "transaction_type": "credit",
                "transaction_name": "Smoke Public Expense",
                "amount": 5_000,
                "date": self.now,
            },
        )["transaction_id"]
        api.post(
            "/v1/transactions",
            json_body={
                "transaction_id": tx_id,
                "account_id": account_id,
                "transaction_type": "credit",
                "transaction_name": "Smoke Public Expense Updated",
                "amount": 6_000,
                "date": self.now,
            },
        )
        api.post(f"/v1/transactions/{tx_id}/receipt", multipart={"file": ("receipt.png", SMOKE_PNG, "image/png")})
        api.get(f"/v1/transactions/{tx_id}/receipt")
        api.get(f"/v1/transactions/{tx_id}/receipt/view", raw=True)
        api.delete(f"/v1/transactions/{tx_id}/receipt")
        api.post("/v1/ledger", json_body={"scope": "all", "limit": 10})
        api.post("/v1/summary", json_body={})
        api.post("/v1/analysis", json_body={})
        api.post("/v1/analysis/budget-shift", json_body={"mode": "dynamic"})
        api.get("/v1/payday", query={"month": self.month})
        api.put("/v1/payday", json_body={"month": self.month, "day": 24})
        api.post("/v1/balances/recompute", json_body={})
        api.post("/v1/transactions/audit", json_body={})
        api.post("/v1/export/preview", json_body={"day": 1, "scope": "all"})
        api.post("/v1/export", json_body={"day": 1, "scope": "all", "format": "csv"}, raw=True)
        api.delete(f"/v1/transactions/{tx_id}")

        api.get("/v1/categories")
        cat_id = api.post("/v1/categories", json_body={"name": "Smoke Public Category", "kind": "expense"})[
            "category_id"
        ]
        api.delete(f"/v1/categories/{cat_id}")
        api.get("/v1/periods")
        api.get("/v1/buckets")
        api.get("/v1/allocation-plans")
        api.get("/v1/strategy-rules")
        api.get("/v1/goals")
        api.get("/v1/assets")
        api.get("/v1/assets/net-worth")
        api.post("/v1/assets/net-worth/snapshot")
        api.get("/v1/dashboard")

    def cleanup(self, source_id: str, target_id: str, tx_id: str, plan_id: str) -> None:
        self.client.delete(f"/transactions/{tx_id}")
        self.client.get("/transactions/audit")
        self.client.delete(f"/allocation-plans/{plan_id}")
        self.client.delete(f"/accounts/{target_id}")
        self.client.delete(f"/accounts/{source_id}")


def next_month() -> str:
    now = datetime.now()
    year = now.year + (1 if now.month == 12 else 0)
    month = 1 if now.month == 12 else now.month + 1
    return f"{year:04d}-{month:02d}"


def future_iso(minutes: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(minutes=minutes)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def assert_equal(actual: Any, expected: Any, label: str) -> None:
    if actual != expected:
        raise SmokeError(f"{label}: expected {expected!r}, got {actual!r}")


def find_by_account_id(rows: list[dict[str, Any]], account_id: str, label: str) -> dict[str, Any]:
    for row in rows:
        if row.get("account_id") == account_id:
            return row
    raise SmokeError(f"{label}: missing account {account_id}")


def main() -> int:
    print(f"Running smoke tests against {BASE_URL}")
    run = SmokeRun()
    try:
        run.run()
    except Exception as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    print("PASS: endpoint smoke test completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
