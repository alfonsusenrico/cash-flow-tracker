#!/usr/bin/env python3
"""Smoke test for LLM integration - validates system prompt and model response."""
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from bot.llm import LLMPlanner


async def test_llm_planner():
    """Test the LLM planner with sample inputs."""
    
    # These will need to be set via environment or passed as arguments
    api_key = input("Enter OpenRouter/DeepSeek API key: ").strip()
    base_url = input("Enter base URL (default: https://openrouter.ai/api/v1): ").strip() or "https://openrouter.ai/api/v1"
    model = input("Enter model name (default: deepseek/deepseek-chat): ").strip() or "deepseek/deepseek-chat"
    
    print("\n" + "="*60)
    print("Initializing LLM Planner...")
    print("="*60)
    
    planner = LLMPlanner(api_key, base_url, model)
    
    # Sample context
    accounts = [
        {"account_name": "ATM BCA", "profile_type": "checking", "balance": 5000000},
        {"account_name": "Cash", "profile_type": "cash", "balance": 500000},
        {"account_name": "Mandiri", "profile_type": "checking", "balance": 2000000},
    ]
    
    categories = [
        {"name": "Food & Drink", "kind": "expense"},
        {"name": "Transportation", "kind": "expense"},
        {"name": "Salary", "kind": "income"},
        {"name": "Shopping", "kind": "expense"},
        {"name": "Bills", "kind": "expense"},
    ]
    
    # Test cases
    test_cases = [
        {
            "name": "Simple expense",
            "message": "beli makan 50rb pake BCA",
            "expected_intent": "create_transaction",
            "expected_type": "credit",
        },
        {
            "name": "Income with cycle topup",
            "message": "gaji masuk 7jt ke BCA",
            "expected_intent": "create_transaction",
            "expected_type": "debit",
        },
        {
            "name": "Account movement",
            "message": "pindahin 500rb dari BCA ke Cash",
            "expected_intent": "create_movement",
            "expected_type": None,
        },
        {
            "name": "Query",
            "message": "cek transaksi bulan ini",
            "expected_intent": "query",
            "expected_type": None,
        },
        {
            "name": "Unclear intent",
            "message": "halo",
            "expected_intent": "none",
            "expected_type": None,
        },
    ]
    
    print("\nRunning test cases...\n")
    
    passed = 0
    failed = 0
    
    for i, test in enumerate(test_cases, 1):
        print(f"Test {i}/{len(test_cases)}: {test['name']}")
        print(f"Message: \"{test['message']}\"")
        
        try:
            result = await planner.propose(
                message_text=test["message"],
                now_iso="2026-06-01T10:00:00+07:00",
                timezone="Asia/Jakarta",
                accounts=accounts,
                categories=categories,
            )
            
            intent = result.get("intent")
            tx_type = result.get("transaction_type")
            confidence = result.get("confidence", 0)
            assistant_msg = result.get("assistant_message", "")
            
            print(f"  Intent: {intent}")
            print(f"  Type: {tx_type}")
            print(f"  Confidence: {confidence:.2f}")
            print(f"  Message: {assistant_msg[:80]}...")
            
            # Validate
            intent_match = intent == test["expected_intent"]
            type_match = tx_type == test["expected_type"] if test["expected_type"] else True
            
            if intent_match and type_match:
                print("  ✅ PASS")
                passed += 1
            else:
                print(f"  ❌ FAIL - Expected intent={test['expected_intent']}, type={test['expected_type']}")
                failed += 1
            
            # Show full response for debugging
            print(f"  Full response: {json.dumps(result, indent=2, ensure_ascii=False)[:200]}...")
            
        except Exception as e:
            print(f"  ❌ ERROR: {e}")
            failed += 1
        
        print()
    
    print("="*60)
    print(f"Results: {passed} passed, {failed} failed out of {len(test_cases)} tests")
    print("="*60)
    
    if failed == 0:
        print("\n✅ All tests passed! LLM integration is working correctly.")
        return 0
    else:
        print(f"\n⚠️  {failed} test(s) failed. Review the responses above.")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(test_llm_planner())
    sys.exit(exit_code)
