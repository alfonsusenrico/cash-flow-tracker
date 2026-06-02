#!/usr/bin/env python3
"""Standalone LLM + Resolver test - no database or Telegram required."""
import asyncio
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from bot.llm import LLMPlanner
from bot.resolver import resolve


async def main():
    """Test LLM and resolver without any external dependencies."""
    
    # Get credentials from environment or prompt
    api_key = os.getenv("DEEPSEEK_API_KEY") or input("Enter OpenRouter API key: ").strip()
    base_url = os.getenv("DEEPSEEK_BASE_URL") or "https://openrouter.ai/api/v1"
    model = os.getenv("DEEPSEEK_MODEL") or "deepseek/deepseek-chat"
    
    print("\n" + "="*70)
    print("LLM + Resolver Smoke Test (Standalone)")
    print("="*70)
    print(f"API Base: {base_url}")
    print(f"Model: {model}")
    print("="*70 + "\n")
    
    # Initialize planner
    planner = LLMPlanner(api_key, base_url, model)
    
    # Mock context
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
        {"category_id": "cat5", "name": "Bills", "kind": "expense"},
    ]
    
    # Test cases
    test_cases = [
        {
            "name": "Expense - Cash out",
            "message": "beli makan 50rb pake BCA",
            "expected": {
                "intent": "create_transaction",
                "transaction_type": "credit",
                "amount": 50000,
                "account": "ATM BCA",
                "category": "Food & Drink",
            }
        },
        {
            "name": "Income - Cash in with cycle topup",
            "message": "gaji masuk 7jt ke BCA",
            "expected": {
                "intent": "create_transaction",
                "transaction_type": "debit",
                "amount": 7000000,
                "account": "ATM BCA",
                "category": "Salary",
            }
        },
        {
            "name": "Movement - Transfer between accounts",
            "message": "pindahin 500rb dari BCA ke Cash",
            "expected": {
                "intent": "create_movement",
                "amount": 500000,
                "from_account": "ATM BCA",
                "to_account": "Cash",
            }
        },
        {
            "name": "Query - Search transactions",
            "message": "cek transaksi bulan ini",
            "expected": {
                "intent": "query",
            }
        },
        {
            "name": "Unclear - Should ask for clarification",
            "message": "halo",
            "expected": {
                "intent": "none",
            }
        },
    ]
    
    passed = 0
    failed = 0
    
    for i, test in enumerate(test_cases, 1):
        print(f"\n{'='*70}")
        print(f"Test {i}/{len(test_cases)}: {test['name']}")
        print(f"{'='*70}")
        print(f"📝 Message: \"{test['message']}\"")
        
        try:
            # Step 1: LLM Proposal
            print("\n🤖 Calling LLM...")
            proposal = await planner.propose(
                message_text=test["message"],
                now_iso="2026-06-01T12:00:00+07:00",
                timezone="Asia/Jakarta",
                accounts=accounts,
                categories=categories,
            )
            
            print(f"   Intent: {proposal.get('intent')}")
            print(f"   Confidence: {proposal.get('confidence', 0):.2f}")
            print(f"   Amount: {proposal.get('amount')}")
            print(f"   Account: {proposal.get('account_name')}")
            if proposal.get('target_account_name'):
                print(f"   Target: {proposal.get('target_account_name')}")
            if proposal.get('category_name'):
                print(f"   Category: {proposal.get('category_name')}")
            print(f"   Message: {proposal.get('assistant_message', '')[:80]}")
            
            # Step 2: Resolver
            print("\n🔍 Resolving...")
            action = resolve(proposal, accounts, categories, threshold=0.75)
            
            print(f"   Decision: {action.get('decision')}")
            print(f"   Final Intent: {action.get('intent')}")
            fields = action.get('fields', {})
            if fields.get('account_id'):
                print(f"   Account ID: {fields['account_id']} ({fields.get('account_name')})")
            if fields.get('target_account_id'):
                print(f"   Target ID: {fields['target_account_id']} ({fields.get('target_account_name')})")
            if fields.get('category_id'):
                print(f"   Category ID: {fields['category_id']} ({fields.get('category_name')})")
            if fields.get('amount'):
                print(f"   Amount: Rp{fields['amount']:,}")
            
            questions = action.get('questions', [])
            if questions:
                print(f"   Questions: {len(questions)} field(s) need clarification")
                for q in questions:
                    print(f"      - {q.get('field')}: {q.get('candidates', [])}")
            
            # Validation
            expected = test['expected']
            checks = []
            
            # Check intent
            if action.get('intent') == expected.get('intent'):
                checks.append(('Intent', True))
            else:
                checks.append(('Intent', False, f"Expected {expected.get('intent')}, got {action.get('intent')}"))
            
            # Check transaction type if applicable
            if 'transaction_type' in expected:
                if fields.get('transaction_type') == expected['transaction_type']:
                    checks.append(('Type', True))
                else:
                    checks.append(('Type', False, f"Expected {expected['transaction_type']}, got {fields.get('transaction_type')}"))
            
            # Check amount if applicable
            if 'amount' in expected:
                if fields.get('amount') == expected['amount']:
                    checks.append(('Amount', True))
                else:
                    checks.append(('Amount', False, f"Expected {expected['amount']}, got {fields.get('amount')}"))
            
            # Check account resolution
            if 'account' in expected:
                if fields.get('account_name') == expected['account']:
                    checks.append(('Account', True))
                else:
                    checks.append(('Account', False, f"Expected {expected['account']}, got {fields.get('account_name')}"))
            
            # Check category resolution
            if 'category' in expected:
                if fields.get('category_name') == expected['category']:
                    checks.append(('Category', True))
                else:
                    checks.append(('Category', False, f"Expected {expected['category']}, got {fields.get('category_name')}"))
            
            # Print validation results
            print("\n✓ Validation:")
            all_passed = True
            for check in checks:
                if check[1]:
                    print(f"   ✅ {check[0]}")
                else:
                    print(f"   ❌ {check[0]}: {check[2]}")
                    all_passed = False
            
            if all_passed:
                print("\n🎉 TEST PASSED")
                passed += 1
            else:
                print("\n❌ TEST FAILED")
                failed += 1
            
            # Show raw JSON for debugging
            print("\n📄 Raw Proposal:")
            print(json.dumps(proposal, indent=2, ensure_ascii=False)[:500] + "...")
            
        except Exception as e:
            print(f"\n💥 ERROR: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    print(f"✅ Passed: {passed}/{len(test_cases)}")
    print(f"❌ Failed: {failed}/{len(test_cases)}")
    print("="*70)
    
    if failed == 0:
        print("\n🎊 All tests passed! LLM integration is working correctly.")
        print("✓ System prompt is being followed")
        print("✓ Intent recognition is accurate")
        print("✓ Resolver is matching accounts/categories correctly")
        print("\nYou can now proceed with Telegram bot setup!")
        return 0
    else:
        print(f"\n⚠️  {failed} test(s) failed. Review the output above.")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
