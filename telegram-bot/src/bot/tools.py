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
                "Search transactions. Supports filtering by query/keyword, "
                "account name, category name, and/or time range. "
                "Use when the user asks queries like 'kapanlalu beli kopi latte itu harga brp ya' "
                "or 'liat dong seminggu ini keluar uang buat makan aja berapa'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Optional search keyword (transaction name or description).",
                    },
                    "account_name": {
                        "type": "string",
                        "description": "Optional: filter results to this account name.",
                    },
                    "category_name": {
                        "type": "string",
                        "description": "Optional: filter results to this category name.",
                    },
                    "time_range": {
                        "type": "string",
                        "description": (
                            "Optional: time range for transactions (e.g., 'today', 'yesterday', "
                            "'this_week', 'last_week', 'this_month', 'last_month', 'this_year', "
                            "or specific range like '7_days', '30_days'). Default is last 30 days if not specified."
                        ),
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results to return (default: 50).",
                    },
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "record_transaction",
            "description": (
                "Create a new transaction (income or expense) in an account."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["income", "expense"],
                        "description": "Whether the transaction is income or an expense.",
                    },
                    "amount": {
                        "type": "integer",
                        "description": "The amount in IDR.",
                    },
                    "name": {
                        "type": "string",
                        "description": "Description / name of the transaction (e.g., 'Kopi Latte', 'Gaji').",
                    },
                    "account_name": {
                        "type": "string",
                        "description": "The name of the account to record the transaction in.",
                    },
                    "category_name": {
                        "type": "string",
                        "description": "Optional: The name of the category (e.g., 'Makan & Minum', 'Gaji').",
                    },
                    "date": {
                        "type": "string",
                        "description": "Optional: date of transaction in YYYY-MM-DD format. Default is today.",
                    },
                },
                "required": ["type", "amount", "name", "account_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "record_movement",
            "description": (
                "Transfer money/balance from one account to another."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "amount": {
                        "type": "integer",
                        "description": "The amount to transfer.",
                    },
                    "source_account_name": {
                        "type": "string",
                        "description": "The account from which money is taken.",
                    },
                    "target_account_name": {
                        "type": "string",
                        "description": "The account to which money is transferred.",
                    },
                    "date": {
                        "type": "string",
                        "description": "Optional: date of transfer in YYYY-MM-DD format. Default is today.",
                    },
                },
                "required": ["amount", "source_account_name", "target_account_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "delete_transaction",
            "description": (
                "Delete an existing transaction by its transaction ID. "
                "Always search for the transaction first using search_transactions to get the exact ID, "
                "and ask the user for confirmation before calling this tool."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "transaction_id": {
                        "type": "string",
                        "description": "The ID of the transaction to delete.",
                    }
                },
                "required": ["transaction_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "update_transaction",
            "description": (
                "Update/modify fields of an existing transaction. "
                "Always search for the transaction first using search_transactions to get the exact ID, "
                "and ask the user for confirmation before calling this tool."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "transaction_id": {
                        "type": "string",
                        "description": "The ID of the transaction to update.",
                    },
                    "type": {
                        "type": "string",
                        "enum": ["income", "expense"],
                        "description": "Updated type.",
                    },
                    "amount": {
                        "type": "integer",
                        "description": "Updated amount in IDR.",
                    },
                    "name": {
                        "type": "string",
                        "description": "Updated transaction description / name.",
                    },
                    "account_name": {
                        "type": "string",
                        "description": "Updated account name.",
                    },
                    "category_name": {
                        "type": "string",
                        "description": "Updated category name.",
                    },
                    "date": {
                        "type": "string",
                        "description": "Updated date in YYYY-MM-DD format.",
                    },
                },
                "required": ["transaction_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "update_user_preferences",
            "description": (
                "Save or update user-specific preferences, rules, or recurring instructions. "
                "Use this when the user explicitly asks you to remember a preference, "
                "or when you learn/deduce a preference from their input (e.g., they instruct you "
                "to treat a specific transaction as a movement next time). "
                "Always provide the complete, updated markdown content representing the full preferences list."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "preferences_content": {
                        "type": "string",
                        "description": "The complete, updated markdown content representing the user's preferences list.",
                    }
                },
                "required": ["preferences_content"],
            },
        },
    },
]
