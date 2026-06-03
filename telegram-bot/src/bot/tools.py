"""Tool definitions and schemas for the agentic LLM loop.

The LLM uses these tools to retrieve real-time data (balances, transactions)
before producing its final structured action proposal.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Tool definitions passed to the OpenAI-compatible function-calling API.
# ---------------------------------------------------------------------------

TOOL_DEFINITIONS: list[dict] = [
    {
        "type": "function",
        "function": {
            "name": "get_account_balance",
            "description": (
                "Get the current balance of a specific account by exact name. "
                "Use this when you need to know the current balance to calculate "
                "adjustment deltas (e.g. user says 'adjust X to 246,673' — you must "
                "know the current balance to compute how much to add or subtract)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "account_name": {
                        "type": "string",
                        "description": (
                            "The exact account name from the accounts list "
                            "(e.g. 'Dana Tabungan', 'ATM BCA')."
                        ),
                    }
                },
                "required": ["account_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_all_balances",
            "description": (
                "Get current balances for ALL accounts. Use when the user asks "
                "about their total balance, net worth, or multiple accounts at once."
            ),
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_transactions",
            "description": (
                "Search recent transactions by keyword and/or account name. "
                "Use when you need to locate a specific transaction for deletion or "
                "update (e.g. user says 'hapus monthly interest' — search first to "
                "confirm it exists and retrieve its details)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search keyword (transaction name or description).",
                    },
                    "account_name": {
                        "type": "string",
                        "description": "Optional: filter results to this account name.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results to return (default: 5).",
                    },
                },
                "required": ["query"],
            },
        },
    },
]
