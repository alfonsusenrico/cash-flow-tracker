# Telegram Bot for Cash Flow Tracker

An intelligent Telegram bot that allows you to manage your personal finances through natural language conversations. Powered by DeepSeek LLM for understanding transaction intents.

## Features

- 💬 **Natural Language Processing**: Describe transactions in plain text (Indonesian or English)
- 📸 **Receipt OCR**: Send photos of receipts to automatically extract transaction details
- 🤖 **Smart Intent Recognition**: Automatically categorizes transactions and movements
- ✅ **Confirmation Flow**: High-confidence transactions execute automatically, others ask for confirmation
- 🔐 **Secure**: API keys encrypted at rest using Fernet symmetric encryption
- 📊 **Query Support**: Ask about past transactions and balances

## Architecture

The bot consists of several components:

- **LLM Planner** (`llm.py`): Converts user messages into structured proposals using DeepSeek
- **Resolver** (`resolver.py`): Validates proposals and resolves account/category names to IDs
- **Finance Client** (`finance_client.py`): Communicates with the main finance API
- **Store** (`store.py`): Manages user links and pending confirmations in PostgreSQL
- **App** (`app.py`): Main bot application with webhook handlers

## Setup

### 1. Prerequisites

- Docker and Docker Compose
- A Telegram Bot Token (get from [@BotFather](https://t.me/botfather))
- DeepSeek API Key (get from [DeepSeek Platform](https://platform.deepseek.com))
- Public HTTPS URL for webhook (use ngrok for local testing)

### 2. Environment Variables

Copy the example environment file and fill in the required values:

```bash
cp .env.example .env
```

Required variables:

```env
# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN=your_bot_token_from_botfather
TELEGRAM_WEBHOOK_URL=https://telegram-webhook.your-domain.com/cash-flow-tracker
TELEGRAM_WEBHOOK_SECRET=random_secret_string

# DeepSeek LLM Configuration
DEEPSEEK_API_KEY=your_deepseek_api_key
DEEPSEEK_BASE_URL=https://api.deepseek.com
DEEPSEEK_MODEL=deepseek-chat

# Finance API
FINANCE_API_BASE_URL=http://api:8000/api

# Security
BOT_SECRET=generate_with_fernet_key_command

# Optional
CONFIDENCE_THRESHOLD=0.75  # Auto-execute threshold (0-1)
```

Generate a Fernet key for `BOT_SECRET`:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 3. Database Migrations

The bot uses the same PostgreSQL database as the main application. Migrations are automatically run on container startup.

Manual migration:

```bash
cd telegram-bot
python migrate.py
```

### 4. Start the Bot

Using Docker Compose (recommended):

```bash
docker-compose up -d telegram-bot
```

Or run locally:

```bash
cd telegram-bot
pip install -r requirements.txt
python main.py
```

### 5. Set Up Webhook

The bot appends `/telegram/webhook` to `TELEGRAM_WEBHOOK_URL`. Ensure the configured base URL is:
- Publicly accessible via HTTPS
- Unique to this project, such as `https://telegram-webhook.your-domain.com/cash-flow-tracker`
- Routed by Cloudflare to the web service
- Configured in nginx to forward `/cash-flow-tracker/telegram/webhook` to the bot's internal `/telegram/webhook` endpoint

For local development with ngrok:

```bash
ngrok http 8090
# Use TELEGRAM_WEBHOOK_URL=https://xxxxx.ngrok.io/cash-flow-tracker
```

## Usage

### Linking Your Account

1. Start a chat with your bot on Telegram
2. Send `/start` to see the welcome message
3. Get an API key from the web application (Settings → API Keys)
4. Send `/link YOUR_API_KEY` to link your account

### Recording Transactions

**Cash Out (Expenses):**
```
beli makan 50rb pake BCA
bayar listrik 200rb dari Cash
```

**Cash In (Income):**
```
gaji masuk 7jt ke BCA
dapat refund 150rb
```

**Account Movements:**
```
pindahin 500rb dari BCA ke Cash
transfer 1jt BCA ke Mandiri
```

**With Receipt Photo:**
Just send a photo of your receipt with optional caption. The bot will extract:
- Amount
- Transaction name
- Date (if visible)

### Querying Transactions

```
cek transaksi bulan ini
berapa saldo BCA?
transaksi terakhir kategori Food
```

### Commands

- `/start` - Show welcome message
- `/link <API_KEY>` - Link your finance account
- `/unlink` - Unlink your account

## How It Works

### 1. Message Processing Flow

```
User Message → LLM Planner → Resolver → Decision
                    ↓              ↓         ↓
              Proposal      Validation   Execute/Confirm/Ask
```

### 2. Intent Recognition

The LLM identifies one of these intents:
- `create_transaction` - New income/expense
- `create_movement` - Transfer between accounts
- `update_transaction` - Modify existing transaction
- `delete_transaction` - Remove transaction
- `update_movement` - Modify transfer
- `delete_movement` - Remove transfer
- `query` - Search/view transactions
- `none` - Unclear intent

### 3. Confidence-Based Execution

- **High confidence (≥0.75)**: Auto-execute creates
- **Medium confidence**: Ask for confirmation
- **Low confidence**: Ask clarifying questions
- **Updates/Deletes**: Always confirm

### 4. Fuzzy Matching

Account and category names are matched using:
1. Exact match (case-insensitive)
2. Substring match
3. Fuzzy similarity (difflib)

If multiple matches found, bot asks for clarification.

## Development

### Project Structure

```
telegram-bot/
├── src/bot/
│   ├── app.py           # Main application & handlers
│   ├── llm.py           # LLM integration
│   ├── resolver.py      # Intent resolution logic
│   ├── finance_client.py # API client
│   ├── store.py         # Database operations
│   ├── db.py            # Connection pool
│   ├── crypto.py        # Encryption utilities
│   └── config.py        # Configuration
├── prompts/
│   └── system_prompt.md # LLM system prompt
├── migrations/
│   └── 001_init.sql     # Database schema
├── tests/
│   └── test_*.py        # Unit tests
├── main.py              # Entry point
├── migrate.py           # Migration runner
├── Dockerfile
└── requirements.txt
```

### Running Tests

```bash
cd telegram-bot
pip install -r requirements-dev.txt
pytest
```

### Adding New Features

1. **New Intent**: Update `system_prompt.md` and add handling in `app.py`
2. **New API Endpoint**: Add method to `finance_client.py`
3. **New Command**: Add handler in `app.py` and register in `main()`

## Troubleshooting

### Bot Not Responding

1. Check webhook is set correctly:
   ```bash
   curl https://api.telegram.org/bot<TOKEN>/getWebhookInfo
   ```

2. Check logs:
   ```bash
   docker-compose logs telegram-bot
   ```

3. Verify environment variables are set

### API Key Issues

- Ensure API key is valid and active in the web app
- Check `BOT_SECRET` is correctly set for encryption
- Try unlinking and relinking: `/unlink` then `/link <KEY>`

### LLM Not Understanding

- Check `DEEPSEEK_API_KEY` is valid
- Verify `DEEPSEEK_BASE_URL` is accessible
- Review system prompt in `prompts/system_prompt.md`
- Check LLM response in logs

### Database Connection

- Ensure PostgreSQL is running and accessible
- Verify `BOT_DATABASE_URL` connection string
- Check migrations ran successfully

## Security Considerations

- API keys are encrypted at rest using Fernet
- Webhook uses secret token validation
- All communication with Telegram uses HTTPS
- Rate limiting handled by Telegram
- User data isolated per telegram_user_id

## Performance

- Connection pooling for PostgreSQL (1-10 connections)
- Shared HTTP client for API calls
- Async/await throughout for non-blocking I/O
- LLM calls typically 1-3 seconds
- Webhook responses under 5 seconds

## License

Part of the Cash Flow Tracker project.
