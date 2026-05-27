# Cash Flow Tracker

A self-hosted personal finance app. Track your daily spending, see where your money goes each month, and manage multiple accounts — all from your own server.

---

## What you can do

- Record cash in and cash out across multiple accounts (cash, bank, e-wallet, etc.)
- See a monthly summary: balance per account, total in/out, and budget usage
- Browse your full transaction history with search and date filters
- Analyze spending by category and day
- Switch money between accounts
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
| Move money between accounts | Ledger → Switch |
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
| Safe to Spend | `min(spendable account balance, remaining spending plan) - payables due this cycle`. Switches are not counted as income or spending. |
| Net Worth | Current liquid account balances plus invested asset value. |
| Emergency Fund Coverage | Emergency bucket balance divided by the monthly emergency spending base from allocation items. |
| Savings Rate | Allocation-based savings rate when a plan exists: planned savings divided by expected income. |
| Cash Runway | Current liquid assets divided by the monthly emergency spending base, converted to days/months. |
| Monthly Drift | Actual spending minus planned spending. Negative means spending is under plan. |

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
