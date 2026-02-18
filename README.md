# Cash Flow Tracker

![Status](https://img.shields.io/badge/status-production-brightgreen)

## Summary
Self-hosted personal cash flow tracker with monthly summaries, a transaction ledger, and analysis by day, week, and category.

## Success Criteria
- Track monthly cash flow with clear totals and category budgets.
- Filter transactions by date range, account, and search with export support.
- Provide a mobile-friendly UI for daily and weekly analysis.

## Outcome and Demo
**Outcome:** A production-ready personal cash flow tracker for a private home server.

**Demo:** Private (personal use).

## Tech Stack
- Frontend: HTML, CSS, Vanilla JS
- Backend: FastAPI (Python)
- Data: PostgreSQL
- Infra: Docker Compose + Nginx

## Architecture
Nginx serves static assets and proxies `/api` to the FastAPI backend. The backend reads/writes to PostgreSQL, handles auth with session cookies, and serves summary/ledger/analysis endpoints. Redis is used for shared cache and rate-limit state. The frontend is a static app that calls the API and renders the views.

## Quickstart (Fresh Install)
Copy-paste this to get the app running from scratch:
```bash
git clone https://github.com/alfonsusenrico/cash-flow-tracker.git
cd cash-flow-tracker

cp .env.example .env

SESSION_SECRET=$(openssl rand -hex 32)
sed -i.bak "s/^SESSION_SECRET=.*/SESSION_SECRET=${SESSION_SECRET}/" .env
rm .env.bak

docker compose up -d db
docker compose run --rm migrate
docker compose up -d
```

Open: `http://localhost:8090/login.html`

After your first login, create an account via **Manage Accounts** to start recording transactions.

## Migrations
Run migrations any time you update the codebase (safe to re-run):
```bash
docker compose run --rm migrate
```

## Testing
Run backend unit tests:
```bash
cd backend
python -m unittest discover -s tests -p "test_*.py" -v
```

## Environment Variables
| Name | Required | Example | Notes |
|------|----------|---------|-------|
| SESSION_SECRET | yes | change-me | Session cookie signing secret |
| COOKIE_SECURE | no | false | Set true when serving over HTTPS |
| POSTGRES_DB | no | ledger | Database name |
| POSTGRES_USER | no | ledger | Database user |
| POSTGRES_PASSWORD | no | ledgerpass | Database password |
| INVITE_CODE | yes | CASHFLOWTRACKER | Invite-only registration code |
| TZ | no | Asia/Jakarta | Display timezone in UI |
| SUMMARY_CACHE_TTL | no | 30 | Summary cache TTL (seconds) |
| MONTH_SUMMARY_TTL | no | 60 | Monthly summary/analysis cache TTL (seconds) |
| REDIS_URL | no | redis://redis:6379/0 | Redis URL for shared cache/rate limits |
| REDIS_PREFIX | no | cashflow | Redis key prefix |
| RECEIPTS_DIR | no | /app/storage/receipts | Filesystem directory for stored receipts |
| RECEIPT_MAX_MB | no | 10 | Maximum upload size per receipt file |
| RECEIPT_WEBP_QUALITY | no | 75 | WEBP quality used for uploaded image receipts |

## Public API (`/api/v1`)
All endpoints require Bearer token auth unless stated otherwise:

```http
Authorization: Bearer <API_KEY>
```

Base URL example:

```text
https://cash-flow-tracker.alfonsusenrico.com/api/v1
```

### Core endpoints
- Auth/key
  - `POST /auth/register` (invite-based registration)
  - `POST /api-key/info`
  - `POST /api-key/reset`
- Accounts
  - `POST /accounts/list`
  - `POST /accounts`
  - `PUT /accounts/{account_id}`
  - `DELETE /accounts/{account_id}`
- Transactions
  - `POST /transactions` (create, or update when `transaction_id` is provided)
  - `PUT /transactions/{transaction_id}`
  - `DELETE /transactions/{transaction_id}`
- Receipts
  - `POST /transactions/{transaction_id}/receipt`
  - `GET /transactions/{transaction_id}/receipt`
  - `GET /transactions/{transaction_id}/receipt/view`
  - `DELETE /transactions/{transaction_id}/receipt`
- Ledger/reporting
  - `POST /ledger`
  - `POST /summary`
  - `POST /analysis`
- Budgets
  - `POST /budgets` (upsert by `account_id + month`)
  - `GET /budgets?month=YYYY-MM`
  - `PUT /budgets/{budget_id}`
  - `DELETE /budgets/{budget_id}`
- Switch transfer
  - `POST /switch`
  - `GET /switch/{transfer_id}`
  - `PUT /switch/{transfer_id}`
  - `DELETE /switch/{transfer_id}`
- Payday/balances/audit/export
  - `GET /payday?month=YYYY-MM`
  - `PUT /payday`
  - `POST /balances/recompute`
  - `POST /transactions/audit`
  - `POST /export/preview`
  - `POST /export`

### Quick smoke test
```bash
BASE_URL="https://cash-flow-tracker.alfonsusenrico.com/api/v1"
API_KEY="<API_KEY>"

curl -sS -X POST "$BASE_URL/accounts/list" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{}'
```

### Automation examples (curl)
```bash
BASE_URL="https://cash-flow-tracker.alfonsusenrico.com/api/v1"
API_KEY="<API_KEY>"
AUTH=(-H "Authorization: Bearer $API_KEY" -H "Content-Type: application/json")
```

1) Create account:
```bash
curl -sS -X POST "$BASE_URL/accounts" "${AUTH[@]}" -d '{
  "account_name": "Cash",
  "initial_balance": 500000
}'
```

2) Create transaction:
```bash
curl -sS -X POST "$BASE_URL/transactions" "${AUTH[@]}" -d '{
  "account_id": "<ACCOUNT_ID>",
  "transaction_type": "credit",
  "transaction_name": "Makan siang",
  "amount": 35000
}'
```

3) Update and delete transaction:
```bash
curl -sS -X PUT "$BASE_URL/transactions/<TRANSACTION_ID>" "${AUTH[@]}" -d '{
  "account_id": "<ACCOUNT_ID>",
  "transaction_type": "credit",
  "transaction_name": "Makan siang (revisi)",
  "amount": 30000
}'

curl -sS -X DELETE "$BASE_URL/transactions/<TRANSACTION_ID>" -H "Authorization: Bearer $API_KEY"
```

4) Create switch transfer antar akun:
```bash
curl -sS -X POST "$BASE_URL/switch" "${AUTH[@]}" -d '{
  "source_account_id": "<ACCOUNT_ID_SUMBER>",
  "target_account_id": "<ACCOUNT_ID_TUJUAN>",
  "amount": 100000
}'
```

5) Upsert budget bulanan:
```bash
curl -sS -X POST "$BASE_URL/budgets" "${AUTH[@]}" -d '{
  "account_id": "<ACCOUNT_ID>",
  "month": "2026-02",
  "amount": 2000000
}'
```

6) Summary, analysis, dan export CSV:
```bash
curl -sS -X POST "$BASE_URL/summary" "${AUTH[@]}" -d '{"month":"02","year":"2026"}'
curl -sS -X POST "$BASE_URL/analysis" "${AUTH[@]}" -d '{"month":"02","year":"2026"}'

curl -sS -X POST "$BASE_URL/export" "${AUTH[@]}" -d '{
  "day": 1,
  "format": "csv",
  "scope": "all"
}' -o ledger-export.csv
```

## Usage
```bash
xdg-open http://localhost:8090/login.html
docker compose logs -f --tail=200
docker compose down
```

## Deployment
Run with Docker Compose on your server and place it behind HTTPS. Set `COOKIE_SECURE=true` when served over TLS. Nginx serves static files and proxies `/api` to the backend; Postgres persists data in a Docker volume. If you are using a CDN, ensure static assets are not cached aggressively.

## Roadmap / Next Steps
- Recurring transactions and scheduled income/expense entries.
- CSV import and category mapping for bulk data entry.
- Trend charts for net cash flow and category breakdowns.

## License
All rights reserved
