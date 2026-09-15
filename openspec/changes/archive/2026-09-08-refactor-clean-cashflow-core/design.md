## Context

The legacy Cash Flow Tracker accumulated 34 database migrations, 13 frontend pages, duplicate 2,000-line routers (`routers/web.py` and `routers/public.py`), and 5 overlapping abstraction layers (Buckets, Allocations, Strategies, Goals, Obligations, Assets, Net Worth). This complexity created high cognitive overhead and entry friction, causing user avoidance and missed entries.

The project owner approved a clean-slate refactor with no requirement to maintain backward-compatibility data migrations or retain legacy migration tables. This design establishes a lean, high-performance architecture built around daily mindfulness and effortless transaction capture.

## Goals / Non-Goals

**Goals:**
- Implement a minimal 5-table database baseline (`users`, `accounts`, `categories`, `transactions`, `api_keys`).
- Unify backend authentication and routing into a clean, single-tree FastAPI service.
- Deliver sub-15ms daily endpoints (`/api/pulse`, `/api/transactions`, `/api/insights`, `/api/accounts`).
- Rebuild the frontend around a cohesive 3-Screen Layout (`/` Pulse, `/insights`, `/accounts`).
- Deliver the 3-second quick capture experience with 1-tap category chips and instant numeric pad.
- Establish an anti-AI-slop visual design system inspired by Linear, Wise, and Apple Card.
- Keep the Telegram bot focused on zero-friction chat logging and receipt OCR.

**Non-Goals:**
- No virtual envelopes (Buckets) mapped across physical accounts.
- No allocation state machines (`draft`, `approved`, `active`, `closed`), actuals auditing, or importance tiers.
- No strategy rule templates or income distribution preview engines.
- No long-term financial goal projections or compound calculators.
- No debt/loan obligations tracking system with counterparty overdue statuses.
- No asset portfolio tracking (stocks, crypto, gold) or net worth snapshots.
- No background auto-funding schedulers or cron workers.
- No backward-compatibility data migrations or legacy table retention.

## Decisions

### 1. 5-Table Minimal Relational Schema
**Choice:** A single baseline migration establishing 5 tables:
- `users`: `id` (UUID), `username`, `password_hash`, `invite_code`, `payday_day` (default 25), `currency` (default IDR), `created_at`.
- `accounts`: `id` (UUID), `user_id`, `name`, `type` (`cash`, `bank`, `wallet`), `initial_balance`, `is_archived`, `created_at`.
- `categories`: `id` (UUID), `user_id`, `name`, `icon`, `color`, `monthly_budget` (nullable), `is_archived`.
- `transactions`: `id` (UUID), `user_id`, `account_id`, `category_id` (nullable), `type` (`expense`, `income`, `transfer`), `transfer_target_account_id` (nullable), `amount`, `notes`, `date`, `receipt_path` (nullable), `created_at`.
- `api_keys`: `id` (UUID), `user_id`, `key_hash`, `created_at`.

*Rationale:* Balances are calculated dynamically (`initial_balance + sum(in) - sum(out)`). Transfers are modeled as a single transaction row with `type = 'transfer'` and a target account, eliminating dual-row synchronization bugs.

### 2. Unified FastAPI Architecture & Auth
**Choice:** Eliminate duplicate route trees in `routers/web.py` and `routers/public.py`. Create a single auth dependency `get_current_user`:
- Resolves user from session cookie `ledger_session` if present (browser).
- Resolves user from Bearer token in `Authorization` header if present (Telegram bot / API).
- Rejects unauthenticated requests with HTTP 401.

*Rationale:* Removes over 4,000 lines of duplicate code, eliminates CSRF origin confusion between cookie and token routes, and guarantees that web and Telegram bot consume identical APIs.

### 3. High-Density Daily Pulse Endpoint (`GET /api/pulse`)
**Choice:** A single composite endpoint serving all data required for the home screen:
```json
{
  "today_spent": 125000,
  "safe_to_spend_today": 250000,
  "cycle_remaining": 3200000,
  "cycle_total_budget": 6000000,
  "cycle_days_total": 30,
  "cycle_day_current": 12,
  "pace_status": "on_track",
  "today_transactions": [ ... ],
  "recent_categories": [ ... ]
}
```
*Rationale:* Avoids waterfall HTTP calls on mobile, allowing the Pulse view to render instantly in a single round-trip.

### 4. Anti-AI-Slop Visual System & 3-Screen Layout
**Choice:**
- **Genre:** Craft-focused, dense, and tactile (Linear / Wise / Apple Card).
- **Surfaces:** `#FAFAF9` light / `#0F1012` dark. Subtle 1px structural borders (`border-neutral-200` / `border-neutral-800`).
- **Typography:** Inter with `font-variant-numeric: tabular-nums` for financial numbers.
- **Accents:** Emerald green for Income, coral/rose for Expense, cobalt blue for Transfers.
- **Prohibitions:** Strictly forbid pastel rainbow gradients, purple glow dropshadows, 0–100 circular health gauges, and 24px-padded empty bubbly cards.
- **Screens:**
  - `/` (Pulse): Big daily allowance card, rapid quick-capture bar, and today's feed.
  - `/insights`: Category progress bars against monthly limits, daily spending histogram.
  - `/accounts`: Liquid account cards with instant 1-tap transfer modal.

### 5. Telegram Bot Streamlining
**Choice:** Update the Telegram bot to call `/api/transactions` and `/api/accounts` directly. Prune LLM tool definitions for goals, obligations, strategy, and buckets. Retain natural language logging (e.g. "makan siang 45rb pake bca") and receipt photo OCR.

## Risks / Trade-offs

- **[Risk] Complete data wipe on clean slate** → *Mitigation*: The project owner explicitly confirmed clean slate is fine. Configuration in `.env` (session secret, bot token, invite code) remains intact.
- **[Risk] Users wanting complex budgeting later** → *Mitigation*: Simple monthly limits per category satisfy 95% of budgeting needs. If envelopes are ever needed in the future, they can be introduced via an approved OpenSpec change without coupling them to physical accounts.
- **[Risk] Mobile responsive layout regressions** → *Mitigation*: Design mobile-first with a fixed 3-tab bottom navigation bar and test viewport widths from 375px to 1440px.

## Migration Plan

1. **Database:** Replace legacy migrations with `V1__baseline.sql` in `db/migrations/`.
2. **Backend:** Reorganize `backend/app/` around unified `routes/`, `services/`, and `models/`.
3. **Frontend:** Update `tokens.css`, replace `app/` routes with the 3 screens, and update layout components.
4. **Verification:** Run pytest suite, Next.js build, and verify mobile responsiveness.
