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
Nginx serves static assets and proxies `/api` to the FastAPI backend. The backend reads/writes to PostgreSQL, handles auth with session cookies, and serves summary/ledger/analysis endpoints. The frontend is a static app that calls the API and renders the views.

## Quickstart (Fresh Install)
Copy-paste this to get the app running from scratch:
```bash
git clone https://github.com/alfonsusenrico/cash-flow-tracker.git
cd cash-flow-tracker

cat > .env <<'EOF'
SESSION_SECRET=change-me
COOKIE_SECURE=false
POSTGRES_DB=ledger
POSTGRES_USER=ledger
POSTGRES_PASSWORD=ledgerpass
INVITE_CODE=CASHFLOWTRACKER
EOF

docker compose up -d db
docker compose run --rm migrate
docker compose up -d
```

Open: `http://localhost:8090/login.html`

## Migrations
Run migrations any time you update the codebase (safe to re-run):
```bash
docker compose run --rm migrate
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
