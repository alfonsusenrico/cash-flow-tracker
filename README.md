# Cash Flow Tracker

A self-hosted personal finance app. Track your daily spending, see where your money goes each month, and manage multiple accounts — all from your own server.

---

## What you can do

- Record cash in and cash out across multiple accounts (cash, bank, e-wallet, etc.)
- See a monthly summary: balance per account, total in/out, and budget usage
- Browse your full transaction history with search and date filters
- Analyze spending by category and day
- Transfer money between accounts (Switch)
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

# Start the database and run migrations
docker compose up -d db
docker compose run --rm migrate

# Start everything
docker compose up -d
```

Open **http://localhost:8090** in your browser.

---

## First-time setup

1. **Register** — click "First time? Register" on the login page. You need the invite code from your `.env` file (`INVITE_CODE`, default: `CASHFLOWTRACKER`).
2. **Create your first account** — go to **Accounts** in the top nav and add an account (e.g. "Cash", "BCA", "GoPay"). Set an initial balance if you have one.
3. **Record a transaction** — go to **Transactions**, click **+ Add Transaction**, fill in the amount and description, and save.
4. **Check your summary** — the **Summary** page shows your balance, spending, and budget status for the current month.

---

## Daily use

| What you want to do | Where to go |
|---|---|
| Record spending or income | Transactions → + Add Transaction |
| Move money between accounts | Transactions → Switch Balance |
| See this month's overview | Summary |
| See spending by day / category | Analysis |
| Browse all transactions | Transactions |
| Manage accounts | Accounts (top nav) |
| Manage categories | Categories |
| View monthly periods | Periods |
| Export to CSV or PDF | Transactions → Export |

---

## Set your payday

The app groups your month from payday to payday, not calendar month. To set it:

1. Go to **Summary**
2. Click the month selector
3. Set your payday day (e.g. 25 means your cycle runs from the 25th to the 24th of the next month)

---

## Set a budget

1. Go to **Accounts**
2. Edit an account
3. Set a **Monthly Limit** — the Summary page will show how much of that budget you've used

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
| `INVITE_CODE` | `CASHFLOWTRACKER` | Code required to register a new account |
| `TZ` | `Asia/Jakarta` | Your timezone — affects how dates are displayed |
| `COOKIE_SECURE` | `false` | Set to `true` when running behind HTTPS |
| `POSTGRES_PASSWORD` | `ledgerpass` | Database password |

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
