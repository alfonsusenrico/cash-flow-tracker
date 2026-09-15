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
                        "enum": ["expense", "income"],
                        "description": "'expense' for money out, 'income' for money in.",
                    },
                    "amount": {
                        "type": "integer",
                        "description": "Transaction amount in IDR (integer, no decimals/separators).",
                    },
                    "name": {
                        "type": "string",
                        "description": "Description of the transaction (e.g., 'Kopi Kenangan').",
                    },
                    "account_name": {
                        "type": "string",
                        "description": "The exact account name to associate with this transaction.",
                    },
                    "category_name": {
                        "type": "string",
                        "description": "The exact category name to associate with this transaction.",
                    },
                    "date": {
                        "type": "string",
                        "description": (
                            "Date of the transaction in ISO 8601 format WITH timezone offset "
                            "(e.g., '2026-04-18T14:30:00+07:00'). Defaults to right now if not provided."
                        ),
                    },
                },
                "required": ["type", "amount", "name", "account_name", "category_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "record_movement",
            "description": (
                "Transfer / move money between two accounts. "
                "Use when the user transfers money between their own accounts (e.g. 'transfer 500k from BCA to Gopay')."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "amount": {
                        "type": "integer",
                        "description": "Transfer amount in IDR (positive integer).",
                    },
                    "source_account_name": {
                        "type": "string",
                        "description": "Account where money is taken from (exact name).",
                    },
                    "target_account_name": {
                        "type": "string",
                        "description": "Account where money is sent to (exact name).",
                    },
                    "date": {
                        "type": "string",
                        "description": (
                            "Date of the transfer in ISO 8601 format WITH timezone offset "
                            "(e.g., '2026-04-18T14:30:00+07:00'). Defaults to right now if not provided."
                        ),
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
                "Delete an existing transaction by ID. "
                "Always search for the transaction first using search_transactions to get the exact ID."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "transaction_id": {
                        "type": "string",
                        "description": "The exact transaction ID to delete.",
                    },
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
                "Update fields of an existing transaction by ID. "
                "Always search for the transaction first using search_transactions to get the exact ID. "
                "Only specify the fields that need to be changed."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "transaction_id": {
                        "type": "string",
                        "description": "The exact transaction ID to update.",
                    },
                    "type": {
                        "type": "string",
                        "enum": ["expense", "income"],
                        "description": "Updated type ('expense' or 'income').",
                    },
                    "amount": {
                        "type": "integer",
                        "description": "Updated transaction amount in IDR.",
                    },
                    "name": {
                        "type": "string",
                        "description": "Updated description / name.",
                    },
                    "account_name": {
                        "type": "string",
                        "description": "Updated account name (must exist).",
                    },
                    "category_name": {
                        "type": "string",
                        "description": "Updated category name (must exist).",
                    },
                    "date": {
                        "type": "string",
                        "description": "Updated date in ISO 8601 format WITH timezone offset.",
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
                "Save or update the user-specific markdown list of preferences, rules, or recurring instructions. "
                "Call this whenever the user expresses a habit, custom classification rule, or preference "
                "(e.g. 'kalau beli boba masukin ke kategori Treat ya', 'selalu gunakan BCA untuk transfer'). "
                "Pass the FULL updated markdown content incorporating all existing rules plus the new one."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "preferences_content": {
                        "type": "string",
                        "description": "The complete markdown content representing all of the user's active preferences and rules.",
                    },
                },
                "required": ["preferences_content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_summary",
            "description": "Get high-level summary and daily pulse for the current cycle.",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_analysis",
            "description": "Get deep financial insights, top spending categories, and breakdown for a cycle.",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "upload_receipt_to_transaction",
            "description": (
                "Upload/attach a receipt image (from the current user message payload) "
                "to an existing transaction. Always search for the transaction first "
                "using search_transactions to get the exact ID."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "transaction_id": {
                        "type": "string",
                        "description": "The exact transaction ID to attach the receipt to."
                    }
                },
                "required": ["transaction_id"]
            }
        }
    }
]
