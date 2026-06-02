# Cash Flow Tracker

A self-hosted personal finance app. Track your daily spending, see where your money goes each month, and manage multiple accounts — all from your own server.

---

## What you can do

- Record cash in and cash out across multiple accounts (cash, bank, e-wallet, etc.)
- See a monthly summary: balance per account, total in/out, and budget usage
- Browse your full transaction history with search and date filters
- Analyze spending by category and day
- Move money between owned accounts without counting it as income or spending
- Export your ledger to CSV or PDF
- Attach receipts (image or PDF) to any transaction
- Manage spending categories
- Track monthly periods (open / closed)

---

## Requirements

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose

That's it.

---

## Install and run

```bash
git clone https://github.com/alfonsusenrico/cash-flow-tracker.git
cd cash-flow-tracker

# Copy the example config
cp .env.example .env

# Generate a secure session secret
SESSION_SECRET=$(openssl rand -hex 32)
sed -i.bak "s/^SESSION_SECRET=.*/SESSION_SECRET=${SESSION_SECRET}/" .env && rm .env.bak

# Generate a database password and invite code
POSTGRES_PASSWORD=$(openssl rand -base64 32 | tr -dc 'A-Za-z0-9' | head -c 32)
INVITE_CODE=$(openssl rand -base64 24 | tr -dc 'A-Za-z0-9' | head -c 24)
sed -i.bak "s/^POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=${POSTGRES_PASSWORD}/" .env && rm .env.bak
sed -i.bak "s/^INVITE_CODE=.*/INVITE_CODE=${INVITE_CODE}/" .env && rm .env.bak

# Start the database and run migrations
docker compose up -d db
docker compose run --rm migrate

# Start everything
docker compose up -d
```

Open **http://localhost:8090** in your browser.

---

## First-time setup

1. **Register** — click "First time? Register" on the login page. Use the invite code from `INVITE_CODE` in your `.env` file.
2. **Create your first account** — open **Accounts** and add an account such as "Cash", "BCA", or "GoPay". Set an initial balance if you have one.
3. **Record a transaction** — open **Ledger**, click **+ Add Transaction**, fill in the amount and description, and save.
4. **Check your summary** — open **Dashboard** to see balances, spending, and budget status.

---

## Daily use

| What you want to do | Where to go |
|---|---|
| Record spending or income | Ledger → + Add Transaction |
| Move money between owned accounts | Ledger → Move Accounts |
| See this month's overview | Dashboard |
| See spending by day / category | Analysis |
| Browse all transactions | Ledger |
| Manage accounts | Accounts |
| Manage categories | Categories |
| View monthly periods | Periods |
| Export to CSV or PDF | Ledger → Export |

---

## Set your payday

The app groups your month from payday to payday, not calendar month. To set it:

1. Open **Settings**
2. Set your payday day
3. Save the setting. A payday day of 25 means your cycle runs from the 25th to the 24th of the next month.

---

## Set a budget

1. Go to **Accounts**
2. Edit an account
3. Set a **Monthly Limit** — the Summary page will show how much of that budget you've used

---

## Dashboard metric glossary

| Metric | Formula / meaning |
|---|---|
| Health Score | Average of dashboard metric statuses: `ok = 100`, `warn = 50`, `critical = 0`. |
| Safe to Spend | `min(spendable account balance, remaining spending plan) - payables due this cycle`. Account movements are not counted as income or spending. |
| Net Worth | Current liquid account balances plus invested asset value. |
| Emergency Fund Coverage | Emergency bucket balance divided by the monthly emergency spending base from allocation items. |
| Savings Rate | Allocation-based savings rate when a plan exists: planned savings divided by expected income. |
| Cash Runway | Current liquid assets divided by the monthly emergency spending base, converted to days/months. |
| Monthly Drift | Actual spending minus planned spending. Negative means spending is under plan. |

## Cashflow model

- **Cash In** is new money entering your finances, such as salary, a gift, refund, or interest.
- **Cash Out** is money leaving your finances, such as food, bills, shopping, or giving.
- **Move Between Accounts** is the same owned money moved between tracked accounts, such as BCA to Jago. It changes account balances only.
- **Allocation Funding** moves planned monthly allocation amounts from the funding source to target accounts. It updates allocation funding progress and may create internal movement rows when the target account differs from the source.

Example: if family sends money to BCA and you save it in Jago, record the gift as Cash In once, then use Move Accounts to move it from BCA to Jago. Reports count the gift as income once and exclude the BCA-to-Jago movement from spending, income, and savings-rate calculations.

---

## Update the app

```bash
git pull
docker compose run --rm migrate   # apply any new database migrations
docker compose up -d --build      # rebuild and restart
```

---

## Stop the app

```bash
docker compose down
```

Your data is stored in a Docker volume (`db_data`) and is not deleted when you stop.

---

## Configuration

Edit `.env` to change these settings:

| Variable | Default | What it does |
|---|---|---|
| `SESSION_SECRET` | *(required)* | Signs session cookies — keep this secret |
| `INVITE_CODE` | *(required)* | Code required to register a new account |
| `TZ` | `Asia/Jakarta` | Your timezone — affects how dates are displayed |
| `COOKIE_SECURE` | `false` | Set to `true` when running behind HTTPS |
| `APP_ORIGINS` | `http://localhost:8090` | Comma-separated browser origins allowed for cookie-auth writes |
| `TRUSTED_PROXY_CIDRS` | Docker bridge + localhost | Proxy networks allowed to supply `X-Real-IP` / `X-Forwarded-For` |
| `POSTGRES_PASSWORD` | *(required)* | Database password |
| `RECEIPT_MAX_MB` | `10` | Maximum receipt upload size |
| `RECEIPT_MAX_PIXELS` | `50000000` | Maximum decoded receipt image pixels |
| `LEDGER_EXPORT_MAX_ROWS` | `5000` | Maximum rows allowed in one ledger export |

## Production checklist

Before exposing the app outside your machine:

- Use HTTPS and set `COOKIE_SECURE=true`.
- Set `APP_ORIGINS` to the exact public origin, for example `https://finance.example.com`.
- Replace `SESSION_SECRET`, `POSTGRES_PASSWORD`, and `INVITE_CODE` with random values.
- Keep the backend reachable only through nginx or another trusted reverse proxy.
- Keep `TRUSTED_PROXY_CIDRS` limited to your reverse proxy network.
- Run migrations before each app start after an update.
- Keep regular database backups.

---

## API access

Every account gets a Bearer API key for automation. Find it under **API Key** in the top nav.

```bash
curl -sS -X POST "http://localhost:8090/api/v1/accounts/list" \
  -H "Authorization: Bearer <YOUR_API_KEY>" \
  -H "Content-Type: application/json" \
  -d '{}'
```

Full API reference: see the FastAPI docs at `http://localhost:8090/api/docs` (available when the backend is running).

---

## Telegram Bot (Optional)

Manage your finances through natural language conversations on Telegram. The bot uses AI (DeepSeek) to understand your messages and automatically record transactions, check balances, and query transaction history.

### Features

- 💬 **Natural language**: "beli makan 50rb pake BCA" or "transfer 500k from BCA to Cash"
- 📸 **Receipt OCR**: Send photos of receipts to extract transaction details
- 💰 **Balance queries**: "berapa saldo BCA?" or "cek saldo semua akun"
- 📊 **Transaction history**: "tampilkan transaksi hari ini" or "list spending minggu ini"
- ✅ **Smart confirmations**: High-confidence transactions execute automatically
- 🔐 **Secure**: API keys encrypted at rest

### Setup

1. **Get a Telegram bot token** from [@BotFather](https://t.me/botfather)
2. **Get a DeepSeek API key** from [OpenRouter](https://openrouter.ai/) or [DeepSeek Platform](https://platform.deepseek.com)
3. **Generate security secrets**:

```bash
# Generate webhook secret (validates requests from Telegram)
TELEGRAM_WEBHOOK_SECRET=$(openssl rand -hex 16)
echo "TELEGRAM_WEBHOOK_SECRET=${TELEGRAM_WEBHOOK_SECRET}"

# Generate bot secret (encrypts API keys in database - MUST be Fernet format)
BOT_SECRET=$(openssl rand -base64 32)
echo "BOT_SECRET=${BOT_SECRET}"
```

**Note:** The bot secret is a Fernet-compatible key (32 random bytes, base64-encoded = 44 characters).

4. **Configure environment variables** in `.env`:

```bash
# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN=your_bot_token_from_botfather
TELEGRAM_WEBHOOK_URL=https://your-domain.com/telegram/webhook
TELEGRAM_WEBHOOK_SECRET=<generated_webhook_secret>

# DeepSeek LLM Configuration
DEEPSEEK_API_KEY=your_deepseek_api_key
DEEPSEEK_BASE_URL=https://openrouter.ai/api/v1
DEEPSEEK_MODEL=deepseek/deepseek-chat

# Security
BOT_SECRET=<generated_fernet_key>

# Optional
CONFIDENCE_THRESHOLD=0.75
```

**Important:** 
- `TELEGRAM_WEBHOOK_SECRET`: Random string that validates webhook requests from Telegram
- `BOT_SECRET`: Fernet encryption key (44 chars, base64) that encrypts API keys in database

4. **Start the bot**:

```bash
docker compose up -d telegram-bot
```

5. **Link your account**:
   - Start a chat with your bot on Telegram
   - Send `/start`
   - Get your API key from the web app (top nav → API Key)
   - Send `/link YOUR_API_KEY`

### Usage Examples

**Record transactions:**
- "beli makan 50rb pake BCA"
- "gaji masuk 7jt ke BCA"
- "bayar listrik 200rb dari Cash"

**Transfer between accounts:**
- "pindahin 500rb dari BCA ke Cash"
- "transfer 1jt BCA ke Mandiri"

**Check balances:**
- "berapa saldo BCA?"
- "cek saldo Cash dan Mandiri"
- "tampilkan semua saldo"

**Query transactions:**
- "transaksi hari ini"
- "list spending BCA minggu ini"
- "show transactions last friday"
- "transaksi 2 jam terakhir"

**With receipt photo:**
Just send a photo of your receipt with optional caption. The bot will extract amount, name, and date.

For detailed documentation, see [telegram-bot/README.md](telegram-bot/README.md).

---

---

## Backup and restore

Create a backup:

```bash
docker compose exec db sh -c 'pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB"' > backup.sql
```

Restore into an empty database:

```bash
docker compose exec -T db sh -c 'psql -U "$POSTGRES_USER" "$POSTGRES_DB"' < backup.sql
```

---

## Verification commands

Frontend:

```bash
cd frontend
npm run type-check
npm run lint
npm run build
npm audit --omit=dev --audit-level=moderate
```

Backend:

```bash
python -m pip install -r backend/requirements-dev.txt
python -m pytest backend/tests
uvx pip-audit -r backend/requirements.txt
docker compose config
```

---

## Troubleshooting

**App won't start**
```bash
docker compose logs api
docker compose logs web
```

**Database migration failed**
```bash
docker compose run --rm migrate
```
Migrations are safe to re-run.

**Forgot your invite code**
```bash
grep INVITE_CODE .env
```

---

## License

All rights reserved.
