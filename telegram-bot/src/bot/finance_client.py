"""Async client for the finance app's /v1 Bearer API.

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
        resp = await self._http.request(
            method, f"{self._base}{path}", headers=self._auth(api_key), **kw
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

    # --- context ---
    async def api_key_info(self, api_key: str) -> dict[str, Any]:
        return await self._request("POST", "/v1/api-key/info", api_key, json={})

    async def list_accounts(self, api_key: str) -> list[dict[str, Any]]:
        data = await self._request("POST", "/v1/accounts/list", api_key, json={})
        return data.get("accounts", [])

    async def list_categories(self, api_key: str) -> list[dict[str, Any]]:
        data = await self._request("GET", "/v1/categories", api_key)
        return data.get("categories", [])

    # --- transactions ---
    async def upsert_transaction(self, api_key: str, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._request("POST", "/v1/transactions", api_key, json=payload)

    async def delete_transaction(self, api_key: str, tx_id: str) -> dict[str, Any]:
        return await self._request("DELETE", f"/v1/transactions/{tx_id}", api_key)

    async def upload_receipt(
        self, api_key: str, tx_id: str, content: bytes, filename: str, content_type: str
    ) -> dict[str, Any]:
        files = {"file": (filename, content, content_type)}
        return await self._request(
            "POST", f"/v1/transactions/{tx_id}/receipt", api_key, files=files
        )

    # --- movements ---
    async def create_movement(self, api_key: str, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._request("POST", "/v1/account-movements", api_key, json=payload)

    async def update_movement(self, api_key: str, transfer_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._request("PUT", f"/v1/account-movements/{transfer_id}", api_key, json=payload)

    async def delete_movement(self, api_key: str, transfer_id: str) -> dict[str, Any]:
        return await self._request("DELETE", f"/v1/account-movements/{transfer_id}", api_key)

    # --- read ---
    async def search_ledger(self, api_key: str, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._request("POST", "/v1/ledger", api_key, json=payload)

    async def get_account_balances(self, api_key: str, account_ids: list[str] | None = None) -> dict[str, Any]:
        """Get current balances for accounts. Returns all accounts if account_ids is None."""
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
        """
        Query transactions within date range.
        If account_ids is provided, fetches for all accounts and filters client-side.
        """
        payload: dict[str, Any] = {
            "scope": "all",
            "from_date": from_date,
            "to_date": to_date,
            "limit": limit,
            "order": "desc",
        }
        
        result = await self.search_ledger(api_key, payload)
        
        # Client-side filtering if specific accounts requested
        if account_ids:
            rows = result.get("rows", [])
            filtered_rows = [
                row for row in rows
                if row.get("account_id") in account_ids
            ]
            result["rows"] = filtered_rows
        
        return result
