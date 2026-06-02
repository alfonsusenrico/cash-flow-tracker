#!/usr/bin/env python3
"""Standalone script to test pending_action context handling by LLM + Resolver."""
import asyncio
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from bot.llm import LLMPlanner
from bot.resolver import resolve


async def main():
    # Load API key and URL from .env or env
    api_key = os.getenv("DEEPSEEK_API_KEY")
    base_url = os.getenv("DEEPSEEK_BASE_URL") or "https://openrouter.ai/api/v1"
    model = os.getenv("DEEPSEEK_MODEL") or "deepseek/deepseek-chat"
    
    # Simple .env parser if running locally
    env_file = Path(__file__).parent / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())
        api_key = os.getenv("DEEPSEEK_API_KEY") or api_key
        base_url = os.getenv("DEEPSEEK_BASE_URL") or base_url
        model = os.getenv("DEEPSEEK_MODEL") or model

    if not api_key:
        print("❌ Error: DEEPSEEK_API_KEY not found in environment or .env file.")
        sys.exit(1)

    print("\n" + "="*80)
    print("Pending Action Context Smoke Test")
    print("="*80)
    print(f"API Base: {base_url}")
    print(f"Model: {model}")
    print("="*80 + "\n")

    planner = LLMPlanner(api_key, base_url, model)

    # Mock accounts and categories
    accounts = [
        {"account_id": "acc1", "account_name": "ATM BCA", "profile_type": "checking", "balance": 5000000},
        {"account_id": "acc2", "account_name": "Cash", "profile_type": "cash", "balance": 500000},
        {"account_id": "acc3", "account_name": "Mandiri", "profile_type": "checking", "balance": 2000000},
    ]

    categories = [
        {"category_id": "cat1", "name": "Food & Drink", "kind": "expense"},
        {"category_id": "cat2", "name": "Transportation", "kind": "expense"},
        {"category_id": "cat3", "name": "Salary", "kind": "income"},
        {"category_id": "cat4", "name": "Shopping", "kind": "expense"},
    ]

    # The pending action that requires confirmation
    # (e.g. "beli makan 50rb pake BCA" resolved to create_transaction)
    pending_action = {
        "intent": "create_transaction",
        "transaction_type": "credit",
        "amount": 50000,
        "transaction_name": "Beli makan",
        "account_name": "ATM BCA",
        "category_name": "Food & Drink",
        "date": None,
        "is_cycle_topup": False,
        "query": None,
        "query_accounts": None,
        "time_range": None,
        "confidence": 0.8,
        "missing_fields": [],
        "ambiguities": []
    }

    test_cases = [
        {
            "name": "Confirmation ('ya')",
            "message": "ya",
            "expect_decision": "execute",
            "expect_intent": "create_transaction",
            "expect_amount": 50000,
            "expect_account": "ATM BCA"
        },
        {
            "name": "Cancellation ('batal')",
            "message": "batal",
            "expect_decision": "clarify", # or whatever None intent resolves to
            "expect_intent": "none",
            "expect_amount": None,
            "expect_account": None
        },
        {
            "name": "Correction ('bukan BCA tapi Mandiri')",
            "message": "bukan BCA tapi Mandiri",
            "expect_decision": "execute", # or confirm if confidence is lower, but should change account
            "expect_intent": "create_transaction",
            "expect_amount": 50000,
            "expect_account": "Mandiri"
        }
    ]

    for i, test in enumerate(test_cases, 1):
        print(f"\n🚀 Running Test {i}/{len(test_cases)}: {test['name']}")
        print(f"💬 User Input: \"{test['message']}\"")
        print(f"📦 Pending Action Context: {pending_action['transaction_name']} Rp{pending_action['amount']:,} via {pending_action['account_name']}")
        
        try:
            print("⏳ Calling LLM...")
            proposal = await planner.propose(
                message_text=test["message"],
                now_iso="2026-06-01T12:00:00+07:00",
                timezone="Asia/Jakarta",
                accounts=accounts,
                categories=categories,
                pending_action=pending_action
            )
            
            print(f"📝 LLM Raw Response Summary:")
            actions = proposal.get("actions", [])
            print(f"   Actions Count: {len(actions)}")
            if actions:
                act = actions[0]
                print(f"   Intent: {act.get('intent')}")
                print(f"   Confidence: {act.get('confidence')}")
                print(f"   Amount: {act.get('amount')}")
                print(f"   Account: {act.get('account_name')}")
            print(f"   Assistant Msg: \"{proposal.get('assistant_message')}\"")

            print("🔍 Resolving with downstream logic...")
            if actions:
                resolved_action = resolve(actions[0], accounts, categories, threshold=0.75)
                print(f"   Resolved Decision: {resolved_action.get('decision')}")
                print(f"   Resolved Intent: {resolved_action.get('intent')}")
                fields = resolved_action.get("fields", {})
                print(f"   Resolved Account: {fields.get('account_name')}")
                print(f"   Resolved Amount: {fields.get('amount')}")
            else:
                print("   Resolved Decision: clarify (No actions returned)")

            print("✅ Verification:")
            if actions:
                act = actions[0]
                intent_ok = act.get('intent') == test['expect_intent']
                amount_ok = act.get('amount') == test['expect_amount']
                account_ok = act.get('account_name') == test['expect_account']
                
                print(f"   Intent match: {'pass' if intent_ok else 'FAIL'}")
                print(f"   Amount match: {'pass' if amount_ok else 'FAIL'}")
                print(f"   Account match: {'pass' if account_ok else 'FAIL'}")
            else:
                print(f"   Intent match: {'pass' if test['expect_intent'] == 'none' else 'FAIL'}")

        except Exception as e:
            print(f"❌ Test encountered error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
