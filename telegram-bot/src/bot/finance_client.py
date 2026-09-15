"""Async client for the cash-flow-tracker unified Bearer API.

A single shared httpx.AsyncClient is reused for all users so TCP/TLS
connections are pooled; the per-user API key is injected per request.
"""
from __future__ import annotations

from typing import Any
import httpx


class FinanceError(Exception):
    def __init__(self, status: int, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.detail = detail


class FinanceClient:
    def __init__(self, base_url: str, http: httpx.AsyncClient) -> None:
        self._base = base_url.rstrip("/")
        self._http = http

    def _auth(self, api_key: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {api_key}"}

    async def _request(self, method: str, path: str, api_key: str, **kw: Any) -> Any:
        # Ensure path starts with /api if not already specified
        normalized_path = path if path.startswith("/api") or path.startswith("/v1") else f"/api{path}"
        resp = await self._http.request(
            method, f"{self._base}{normalized_path}", headers=self._auth(api_key), **kw
        )
        if resp.status_code >= 400:
            detail = "Request failed"
            try:
                detail = resp.json().get("detail", detail)
            except Exception:
                pass
            raise FinanceError(resp.status_code, str(detail))
        if resp.content:
            return resp.json()
        return {}

    # --- auth & context ---
    async def api_key_info(self, api_key: str) -> dict[str, Any]:
        return await self._request("GET", "/auth/api-key/info", api_key)

    async def list_accounts(self, api_key: str) -> list[dict[str, Any]]:
        res = await self._request("GET", "/accounts", api_key)
        accounts = res.get("accounts", [])
        for acc in accounts:
            if "account_id" not in acc:
                acc["account_id"] = acc.get("id")
            if "account_name" not in acc:
                acc["account_name"] = acc.get("name")
            if "current_balance" not in acc:
                acc["current_balance"] = acc.get("balance")
        return accounts

    async def create_account(self, api_key: str, payload: dict[str, Any]) -> dict[str, Any]:
        body = {
            "name": payload.get("name") or payload.get("account_name"),
            "type": payload.get("type") or "bank",
            "initial_balance": payload.get("initial_balance", 0),
        }
        res = await self._request("POST", "/accounts", api_key, json=body)
        if "account" in res:
            res["account_id"] = res["account"].get("id")
        return res

    async def update_account(self, api_key: str, account_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        body = {
            "name": payload.get("name") or payload.get("account_name"),
            "type": payload.get("type"),
        }
        return await self._request("PUT", f"/accounts/{account_id}", api_key, json=body)

    async def update_account_profile(self, api_key: str, account_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        # Profile settings consolidated into account update
        return await self.update_account(api_key, account_id, payload)

    async def delete_account(self, api_key: str, account_id: str) -> dict[str, Any]:
        return await self._request("DELETE", f"/accounts/{account_id}", api_key)

    async def list_categories(self, api_key: str) -> list[dict[str, Any]]:
        res = await self._request("GET", "/categories", api_key)
        categories = res.get("categories", [])
        for cat in categories:
            if "category_id" not in cat:
                cat["category_id"] = cat.get("id")
            if "category_name" not in cat:
                cat["category_name"] = cat.get("name")
        return categories

    # --- transactions ---
    async def upsert_transaction(self, api_key: str, payload: dict[str, Any]) -> dict[str, Any]:
        raw_type = payload.get("type") or payload.get("transaction_type")
        tx_type = "income" if raw_type in ("income", "debit") else "expense"
        notes = payload.get("notes") or payload.get("transaction_name") or payload.get("name")

        clean_payload = {
            "type": tx_type,
            "amount": int(payload["amount"]),
            "account_id": payload.get("account_id"),
            "category_id": payload.get("category_id"),
            "notes": notes,
            "date": payload.get("date"),
        }
        res = await self._request("POST", "/transactions", api_key, json=clean_payload)
        tx_id = res.get("transaction_id") or (res.get("transaction") or {}).get("id")
        return {
            "ok": True,
            "transaction_id": tx_id,
            "id": tx_id,
            **res,
        }

    async def delete_transaction(self, api_key: str, tx_id: str) -> dict[str, Any]:
        return await self._request("DELETE", f"/transactions/{tx_id}", api_key)

    async def audit_transactions(self, api_key: str, payload: dict[str, Any]) -> dict[str, Any]:
        # Direct ledger query
        return await self.search_ledger(api_key, payload)

    async def upload_receipt(
        self, api_key: str, tx_id: str, content: bytes, filename: str, content_type: str
    ) -> dict[str, Any]:
        files = {"file": (filename, content, content_type)}
        return await self._request(
            "POST", f"/transactions/{tx_id}/receipt", api_key, files=files
        )

    # --- internal movements (transfers) ---
    async def create_movement(self, api_key: str, payload: dict[str, Any]) -> dict[str, Any]:
        from_id = payload.get("from_account_id") or payload.get("source_account_id") or payload.get("account_id")
        to_id = payload.get("to_account_id") or payload.get("target_account_id") or payload.get("transfer_target_account_id")
        notes = payload.get("notes") or payload.get("description")

        clean_payload = {
            "type": "transfer",
            "amount": int(payload["amount"]),
            "account_id": from_id,
            "transfer_target_account_id": to_id,
            "notes": notes,
            "date": payload.get("date") or payload.get("transacted_at"),
        }
        res = await self._request("POST", "/transactions", api_key, json=clean_payload)
        transfer_id = res.get("transaction_id")
        return {
            "ok": True,
            "transfer_id": transfer_id,
            "id": transfer_id,
            **res,
        }

    async def update_movement(self, api_key: str, transfer_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return await self.upsert_transaction(api_key, payload)

    async def delete_movement(self, api_key: str, transfer_id: str) -> dict[str, Any]:
        return await self.delete_transaction(api_key, transfer_id)

    # --- read endpoints ---
    async def search_ledger(self, api_key: str, payload: dict[str, Any]) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if payload.get("from_date"):
            params["from_date"] = payload["from_date"]
        if payload.get("to_date"):
            params["to_date"] = payload["to_date"]
        if payload.get("limit"):
            params["limit"] = payload["limit"]
        if payload.get("account_id"):
            params["account_id"] = payload["account_id"]
        res = await self._request("GET", "/transactions", api_key, params=params)
        txs = res.get("transactions", [])
        return {"rows": txs, "total_count": res.get("total", len(txs))}

    async def get_summary(self, api_key: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        return await self._request("GET", "/pulse", api_key)

    async def get_analysis(self, api_key: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        return await self._request("GET", "/insights", api_key)

    async def get_budget_shift(self, api_key: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        return await self._request("GET", "/insights", api_key)

    async def get_account_balances(self, api_key: str, account_ids: list[str] | None = None) -> dict[str, Any]:
        accounts = await self.list_accounts(api_key)
        if account_ids:
            accounts = [acc for acc in accounts if acc.get("account_id") in account_ids]
        return {acc["account_id"]: acc for acc in accounts}

    async def query_transactions(
        self,
        api_key: str,
        from_date: str,
        to_date: str,
        account_ids: list[str] | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "from_date": from_date,
            "to_date": to_date,
            "limit": limit,
        }
        result = await self.search_ledger(api_key, payload)
        if account_ids:
            rows = result.get("rows", [])
            result["rows"] = [r for r in rows if r.get("account_id") in account_ids]
        return result

    # --- pruned legacy endpoints (graceful stubs) ---
    async def list_goals(self, api_key: str) -> list[dict[str, Any]]:
        return []

    async def create_goal(self, api_key: str, payload: dict[str, Any]) -> dict[str, Any]:
        return {"ok": True, "message": "Goals are deprecated in clean-core"}

    async def update_goal(self, api_key: str, goal_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return {"ok": True}

    async def delete_goal(self, api_key: str, goal_id: str) -> dict[str, Any]:
        return {"ok": True}

    async def contribute_goal(self, api_key: str, goal_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return {"ok": True}

    async def list_obligations(self, api_key: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        return []

    async def create_obligation(self, api_key: str, payload: dict[str, Any]) -> dict[str, Any]:
        return {"ok": True, "message": "Obligations are deprecated in clean-core"}

    async def update_obligation(self, api_key: str, obligation_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return {"ok": True}

    async def settle_obligation(self, api_key: str, obligation_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return {"ok": True}
