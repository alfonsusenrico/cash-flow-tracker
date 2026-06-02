# Testing the Telegram Bot LLM Integration

## Quick Test (Recommended)

Since you have the `.env` file configured with `DEEPSEEK_API_KEY`, run the test using docker-compose:

```bash
# From the project root directory
docker-compose run --rm telegram-bot python test_llm_standalone.py
```

This will:
1. Build the telegram-bot image with all dependencies
2. Load environment variables from your `.env` file
3. Run the standalone LLM test
4. Show detailed results for 5 test scenarios

## What the Test Does

The test validates:
- ✅ Expense transactions (cash out) - "beli makan 50rb pake BCA"
- ✅ Income transactions (cash in) - "gaji masuk 7jt ke BCA"
- ✅ Account movements (transfers) - "pindahin 500rb dari BCA ke Cash"
- ✅ Query handling - "cek transaksi bulan ini"
- ✅ Unclear intents - "halo"

## Expected Output

For each test, you should see:
```
======================================================================
Test 1/5: Expense - Cash out
======================================================================
📝 Message: "beli makan 50rb pake BCA"

🤖 Calling LLM...
   Intent: create_transaction
   Confidence: 0.85
   Amount: 50000
   Account: ATM BCA
   Category: Food & Drink
   Message: Cash out untuk "Beli makan" dari ATM BCA, kategori Food & Drink...

🔍 Resolving...
   Decision: execute
   Final Intent: create_transaction
   Account ID: acc1 (ATM BCA)
   Category ID: cat1 (Food & Drink)
   Amount: Rp50,000

✓ Validation:
   ✅ Intent
   ✅ Type
   ✅ Amount
   ✅ Account
   ✅ Category

🎉 TEST PASSED
```

## Success Criteria

All 5 tests should pass with:
- Correct intent recognition
- Proper transaction type (debit/credit)
- Accurate amount parsing (Indonesian shorthand)
- Successful account/category fuzzy matching
- Reasonable confidence scores (0.5-1.0)

## Troubleshooting

### If the test fails to start:
```bash
# Rebuild the image
docker-compose build telegram-bot

# Try again
docker-compose run --rm telegram-bot python test_llm_standalone.py
```

### If API key is not found:
Check your `.env` file has:
```env
DEEPSEEK_API_KEY=sk-or-v1-xxxxx
DEEPSEEK_BASE_URL=https://openrouter.ai/api/v1
DEEPSEEK_MODEL=deepseek/deepseek-chat
```

### If tests fail validation:
- Check the "Raw Proposal" JSON output
- Verify the system prompt is being followed
- Check confidence scores (should be >0.5 for clear intents)
- Review the LLM's assistant_message for clues

## Alternative: Local Test (if you have Python 3.12+)

```bash
cd telegram-bot

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Set API key
export DEEPSEEK_API_KEY="your-key-here"

# Run test
python test_llm_standalone.py
```

## Next Steps After Successful Test

Once all tests pass:
1. Set up Telegram bot token from @BotFather
2. Configure webhook URL (use ngrok for local testing)
3. Generate BOT_SECRET: `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`
4. Start the full bot: `docker-compose up -d telegram-bot`
5. Test with real Telegram messages!
