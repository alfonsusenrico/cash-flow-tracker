# CashFlow

A self-hosted personal cash-flow tracker. An Android companion turns bank and e-wallet notifications into ledger
entries, and the web app shows what came in, what went out and what is safe to spend until the next salary.

<p align="center">
  <picture>
    <source media="(prefers-reduced-motion: no-preference)" srcset="docs/assets/screenshots/home-theme.webp" />
    <img src="docs/assets/screenshots/home-light.webp" alt="Home page with the safe-to-spend amount, cycle figures, items that need action, net worth and the Kakeibo split, switching between the light and dark themes" width="880" />
  </picture>
</p>

- **Built for Indonesian banks and e-wallets:** notifications from BCA, Bank Jago, GoPay, ShopeePay and Stockbit
  are captured on the phone and recorded on your server.
- **English by default, Indonesian as a switch:** each account chooses its language. Amounts read "Rp 1.234.567".
- **Explainable figures:** every figure states its period and opens the rows behind it.
- **Your server, your data:** Docker Compose, PostgreSQL and invite-only sign-up.

All screenshots use synthetic data, in the light and the dark theme.

## Contents

- [The web app](#the-web-app)
- [Recording money](#recording-money)
- [Phone companion](#phone-companion)
- [Optional AI interpretation](#optional-ai-interpretation)
- [Architecture](#architecture)
- [Self-hosting quickstart](#self-hosting-quickstart)
- [API highlights](#api-highlights)
- [Development and checks](#development-and-checks)
- [License](#license)

## The web app

| Page | What it is for |
| --- | --- |
| **Home** | Safe to spend until payday; the cycle's income, spending, savings and scheduled payments; what needs action; recent activity; net worth, safety net and the Kakeibo split. |
| **Review** | Notifications that need an answer, transfers waiting for their other half, and what was recorded automatically. Bulk actions can be undone for 10 minutes. |
| **Transactions** | Every income, spending and transfer row, with type and period filters, search and row details. |
| **Analysis** | Spending by Kakeibo pillar against the last whole period and the recent average, plus a calendar and a money-flow view. |
| **Reports** | A monthly Kakeibo report: four questions, a data-quality box, drill-down on every figure and a reflection note. |
| **Accounts & Pockets** | Banks, pockets, cash, e-wallets, credit accounts and investments, grouped by use or by account. |
| **Goals & Bills** | Savings goals, the emergency fund, debts, plans and credit bills. |
| **Settings** | Phone & devices (QR pairing), habits, appearance (language, theme, hidden balances), salary & cycle, currency & budget, notification mapping and sender names. |

| | Light | Dark |
| --- | :---: | :---: |
| **Home** | <img src="docs/assets/screenshots/home-light.webp" alt="Home page, light theme" width="400" /> | <img src="docs/assets/screenshots/home-dark.webp" alt="Home page, dark theme" width="400" /> |
| **Transactions** | <img src="docs/assets/screenshots/transactions-light.webp" alt="Transactions page with type and period filters, light theme" width="400" /> | <img src="docs/assets/screenshots/transactions-dark.webp" alt="Transactions page with type and period filters, dark theme" width="400" /> |
| **Analysis** | <img src="docs/assets/screenshots/analysis-light.webp" alt="Analysis page with the Kakeibo pillars and the comparison with the last cycle, light theme" width="400" /> | <img src="docs/assets/screenshots/analysis-dark.webp" alt="Analysis page with the Kakeibo pillars and the comparison with the last cycle, dark theme" width="400" /> |
| **Reports** | <img src="docs/assets/screenshots/reports-light.webp" alt="Monthly report with income sources and the list of earlier reports, light theme" width="400" /> | <img src="docs/assets/screenshots/reports-dark.webp" alt="Monthly report with income sources and the list of earlier reports, dark theme" width="400" /> |

**Throughout the app**

- **Kakeibo pillars:** Needs, Wants, Culture, Unexpected and Savings. Spending without a pillar stays visible as
  "No pillar yet" until you choose one.
- **Salary cycles:** choose your salary source in Settings. A confirmed salary starts a new cycle; until then,
  periods are calendar months.
- **Plans never post:** a due bill or transfer is met by linking the transaction your phone captured, skipping it,
  or recording it yourself.
- **Hidden balances:** one toggle shows every amount as dots.
- **Light, dark or system theme**, and a phone layout with a bottom navigation bar.
- **Keyboard:** `N` records a transaction and `?` opens help.

| Phone width, light | Phone width, dark |
| :---: | :---: |
| <img src="docs/assets/screenshots/home-phone-light.webp" alt="Home page at phone width with the bottom navigation bar, light theme" width="280" /> | <img src="docs/assets/screenshots/home-phone-dark.webp" alt="Home page at phone width with the bottom navigation bar, dark theme" width="280" /> |

## Recording money

| Way | How it works |
| --- | --- |
| **Phone notifications** | The companion sends supported bank and e-wallet notifications. The server records settled ones and asks when something is unclear. |
| **One sentence** | Type "lunch 35k" or "parking 5k yesterday". Code reads the amount and date; anything still unclear becomes one question at a time. |
| **Manual form** | Spending, income or a transfer between your own accounts, in the same dialog under "Fill in manually". |

| Light | Dark |
| :---: | :---: |
| <img src="docs/assets/screenshots/record-sentence-light.webp" alt="Write a transaction dialog reading lunch 35k as a Rp 35.000 spending, light theme" width="420" /> | <img src="docs/assets/screenshots/record-sentence-dark.webp" alt="Write a transaction dialog reading lunch 35k as a Rp 35.000 spending, dark theme" width="420" /> |

**Credit and pay later.** Each credit card or pay-later provider (for example SPayLater, Kredivo or Akulaku) is its
own credit account. A purchase can be split into 2 to 36 installments. It counts as spending on the purchase day,
and the row says "pay later" because no cash leaves until the bill is paid. Bills, due dates and payments are
tracked per account.

## Phone companion

The Android app (Android 8.0 or newer) lives in [`mobile/`](mobile/README.md). It reads notifications only from
these apps and drops every other notification unread:

| Bank or wallet | Apps |
| --- | --- |
| BCA | myBCA, BCA mobile |
| Bank Jago | Bank Jago |
| GoPay | GoPay, Gojek |
| ShopeePay | ShopeePay, Shopee |
| Stockbit | Stockbit |

The phone keeps a local log, sends batches with its own device key and asks short questions, such as what a payment
was for. It can also record a typed sentence. Its interface is in Indonesian, with a light and a dark theme.

**From phone to web.** A synthetic myBCA notification, "Transfer QRIS Rp 35.000 ke Kopi Senja berhasil", is
captured on the phone, uploaded and recorded on the server. With AI off, the unknown category waits for an answer in
Review.

| | Light | Dark |
| --- | :---: | :---: |
| **1. Captured on the phone** | <img src="docs/assets/screenshots/phone-capture-log-light.webp" alt="Companion capture log with the recorded Kopi Senja payment, light theme" width="300" /> | <img src="docs/assets/screenshots/phone-capture-log-dark.webp" alt="Companion capture log with the recorded Kopi Senja payment, dark theme" width="300" /> |
| **2. What the tracker recorded** | <img src="docs/assets/screenshots/phone-capture-detail-light.webp" alt="Companion detail with the recorded amount, account and the original notification, light theme" width="300" /> | <img src="docs/assets/screenshots/phone-capture-detail-dark.webp" alt="Companion detail with the recorded amount, account and the original notification, dark theme" width="300" /> |
| **3. Arrived on the web** | <img src="docs/assets/screenshots/web-captured-transaction-light.webp" alt="Web transaction detail recorded from the bank notification, light theme" width="300" /> | <img src="docs/assets/screenshots/web-captured-transaction-dark.webp" alt="Web transaction detail recorded from the bank notification, dark theme" width="300" /> |

**Install and pair**

1. Download `cashflow-companion.apk` from the
   [latest release](https://github.com/alfonsusenrico/cash-flow-tracker/releases/latest), or scan the download QR in
   **Settings → Phone & devices**.
2. Check it against the release's `cashflow-companion.apk.sha256`:
   ```bash
   sha256sum -c cashflow-companion.apk.sha256      # Linux
   shasum -a 256 -c cashflow-companion.apk.sha256  # macOS
   ```
3. Install it, choose **Connect a phone** in **Settings → Phone & devices**, and scan the QR code with the app.
   A pairing code works once and expires after 5 minutes.
4. Grant notification access when Android asks.

The released app talks only to the hosted server. For your own server, set `PAIRING_SERVER_URL` to its HTTPS origin
and build a debug APK for it:

```bash
cd mobile
./gradlew assembleDebug -PcompanionServerUrl=https://finance.example.com
adb install -r app/build/outputs/apk/debug/app-debug.apk
```

## Optional AI interpretation

Without AI, the backend records the notifications it can parse and keeps unfamiliar ones for review. An optional
OpenAI-backed interpreter can classify unfamiliar notifications, typed sentences and history suggestions.

- **Off by default:** `NOTIFICATION_AI_ENABLED=false`.
- **Defaults when enabled** ([`config.py`](backend/app/core/config.py)): model `gpt-6-luna`, reasoning effort `none`.
- **Bounded, redacted context:** active accounts, pockets, categories, rules, a limited number of recent
  descriptions and eligible transfer candidates. Owner names, tokens and email addresses are redacted, and requests
  use `store=false`.
- **The backend decides:** the model cannot set amounts or timestamps, create accounts or categories, or call tools.
  The backend validates every reference. Nothing from an invalid or incomplete answer is written; the backend
  records only what it can prove and flags it for review.

```dotenv
NOTIFICATION_AI_ENABLED=false
NOTIFICATION_AI_MODEL=gpt-6-luna
NOTIFICATION_AI_REASONING_EFFORT=none
LLM_ENGLISH_OUTPUT_ENABLED=false
OPENAI_API_KEY=
```

Keep `OPENAI_API_KEY` in your runtime secret store, never in source control. OpenAI API inputs are not used for
training by default; standard abuse-monitoring retention can last up to 30 days.

**Benchmark.** A synthetic benchmark checks the interpreter without touching the database. Each run stops above a
cost cap (`--max-estimated-usd`, default $0.25):

```bash
PYTHONPATH=backend backend/.venv/bin/python -m evaluation.notification_benchmark \
  --live --model gpt-6-luna --reasoning-efforts none --partition development --repeats 3
```

The latest runs (2026-11-09) passed the safety gate but failed the held-out quality gate for classification, so
English model output (`LLM_ENGLISH_OUTPUT_ENABLED`) stays off. See the
[verification record](openspec/changes/archive/2026-11-09-align-notification-output-contract/verification.md).

## Architecture

```
 Android companion ──HTTPS, device key──┐
                                        ▼
 Browser ─────────────────────▶ nginx (web, port 8090)
                                 ├── /       ▶ Next.js frontend
                                 └── /api/*  ▶ FastAPI backend ──▶ PostgreSQL 16
                                                 │                  (schema by Flyway)
                                                 ├──▶ Redis 7 (cache and rate limits)
                                                 └──▶ OpenAI (optional, off by default)
```

- **Schema:** Flyway migrations in [`db/migrations/`](db/migrations/) are the only schema owner. Compose runs them
  before the API starts.
- **Background jobs:** the notification worker, salary detection, price sync and transfer re-pairing run inside the
  API.
- **Backups:** an optional `backup` service dumps the database nightly; see
  [docs/database-backups.md](docs/database-backups.md).
- **Production:** releases go through the deploy workflow; see
  [docs/production-deployment.md](docs/production-deployment.md).

## Self-hosting quickstart

```bash
git clone https://github.com/alfonsusenrico/cash-flow-tracker.git
cd cash-flow-tracker
cp .env.example .env

# Generate the secrets
SESSION_SECRET=$(openssl rand -hex 32)
POSTGRES_PASSWORD=$(openssl rand -base64 32 | tr -dc 'A-Za-z0-9' | head -c 32)
INVITE_CODE=$(openssl rand -base64 24 | tr -dc 'A-Za-z0-9' | head -c 24)
sed -i.bak "s/^SESSION_SECRET=.*/SESSION_SECRET=${SESSION_SECRET}/" .env
sed -i.bak "s/^POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=${POSTGRES_PASSWORD}/" .env
sed -i.bak "s/^INVITE_CODE=.*/INVITE_CODE=${INVITE_CODE}/" .env
rm -f .env.bak

# Migrate and start everything
docker compose -f docker-compose.yml -f docker-compose.local.yml up -d --build
```

Open <http://localhost:8090>, choose **Create one**, and enter a username, a password and the `INVITE_CODE` from
`.env`. Then add your accounts in **Accounts & Pockets** and pair a phone in **Settings → Phone & devices**.

**Local HTTP and cookies.** Session cookies are Secure by default. `docker-compose.local.yml` sets
`APP_ENV=development` and `COOKIE_SECURE=false` so sign-in works over plain HTTP. Production uses the base Compose
file only and refuses to start with insecure cookies. Never include the local override in a release.

## API highlights

The browser signs in with a session cookie; the companion and scripts send `Authorization: Bearer <key>`. Behind
nginx, every API path starts with `/api`.

| Endpoint | Purpose |
| --- | --- |
| `POST /api/transactions` | Record spending or income. Accounts and categories can be given by name. |
| `POST /api/movements` | Move money between your own accounts. |
| `POST /api/capture/text` | Record a typed sentence such as "lunch 35k". |
| `POST /api/ingest/notifications` | Companion batch upload. A repeated event never duplicates a ledger entry. |
| `GET /api/ingest/notifications/{event_id}/result` | The result of one notification; `POST …/retry` retries an unresolved one. |
| `GET /api/dashboard/cycle-summary` | Home's cycle figures and their breakdown. |
| `GET /api/reports/monthly` | The monthly Kakeibo report. |
| `GET /api/accounts` | Balances of every account and pocket. |
| `POST /api/devices/pairing-codes` | A single-use pairing code (browser session only). |

```json
{
  "type": "income",
  "amount": 1500000,
  "account_name": "BCA",
  "category_name": "Salary",
  "notes": "PT Contoh Karya",
  "date": "2026-09-16T15:33:14Z"
}
```

A name must match one active account or category (case and extra spaces are ignored). An unknown or ambiguous name
returns HTTP 422 with `account_name_unresolved` or `category_name_unresolved` and up to five candidates, and nothing
is created. Send `account_id` or `category_id` to be explicit.

The backend serves its OpenAPI docs at `/docs`, for example `http://127.0.0.1:8000/docs` when you run it directly.

## Development and checks

These match CI in [`checks.yml`](.github/workflows/checks.yml) and [`android-ci.yml`](.github/workflows/android-ci.yml).

**Backend** (Python 3.12). CI first migrates a disposable PostgreSQL 16 with `flyway/flyway:10`. Without a reachable
`TEST_DATABASE_URL`, the database tests are skipped.

```bash
pip install -r backend/requirements-dev.txt
cd backend
TEST_DATABASE_URL=postgresql://ledger:ledgerpass@localhost:5432/ledger_test \
SKIP_TEST_MIGRATIONS=1 REQUIRE_TEST_DATABASE=1 \
python -m pytest tests/ -ra
```

**Frontend** (Node 22):

```bash
cd frontend
npm ci
npm run type-check
npm run lint
npm test
npm run build
```

**Operations:**

```bash
docker compose -f docker-compose.yml config --quiet
docker compose -f docker-compose.yml --profile backup config --quiet
scripts/verify/backup_restore_roundtrip.sh
```

**Companion** (JDK 17 in `JAVA_HOME`):

```bash
cd mobile
./gradlew testDebugUnitTest
./gradlew lintDebug
./gradlew assembleDebug
```

**Local backend without Docker:** set `DATABASE_URL`, `SESSION_SECRET`, `APP_ENV=development` and
`COOKIE_SECURE=false`, then run `uvicorn app.main:app --reload --host 127.0.0.1 --port 8000` from `backend/`.

## License

MIT License.
