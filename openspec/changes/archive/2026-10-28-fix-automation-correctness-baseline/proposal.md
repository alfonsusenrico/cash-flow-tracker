## Approval

Approved by the owner on 2026-10-28, including the default benchmark grouping: unexpected counts toward essentials and culture toward lifestyle.

## Why

The owner chose the full Kakeibo set. Today, however, the AI and the notification resolve
endpoint accept `culture` and `unexpected`, while the database check constraints, the manual
APIs, the web forms, the reports and the safe-to-spend figure only know `need`, `want` and
`saving`.
- An AI pick of a new pillar fails at insert.
- Spending in a culture or unexpected category would silently drop out of the daily allowance.

Separately, the backend's keyword noise rules throw away settled payments that mention a
promotion, while sending marketing, market news and payment instructions to "needs review".
On the live system every open review item is noise of this kind. Reconcile adjustments carry
no category. The insights status badge can never show a healthy state. A Telegram bot is still
deployed, but it has no inbound use, it is broken against the current API, and it writes its
credential into container logs.

This is phase 1 of `research/notes/2026-10-28-automation-quality-plan.md`. It makes the
existing automation correct before the later phases change its design.

## Expected Outcome

- The owner can mark any expense as need, want, culture, unexpected or saving:
  - in quick capture, in the ledger edit form, and as a category default;
  - from AI notification processing, without failed inserts.
- Reports show all five pillars and judge the 50/30/20 benchmark from three groups:
  - essentials: need and unexpected, target at most 50%;
  - lifestyle: want and culture, target at most 30%;
  - saving: target at least 20%.

  The status badge reads "healthy", "warning" or "elevated needs" correctly.
- One rule decides a transaction's pillar everywhere:
  - its own pillar if set;
  - otherwise its category's pillar;
  - otherwise need.
- Safe-to-spend counts every spending pillar.
- A settled payment that mentions cashback, a promo or a discount is recorded.
- Marketing, market news, payment instructions with a deadline, card authorisation requests
  and amount-less summaries are ignored instead of waiting in review.
- Open review items that the new rules classify as noise leave the review queue after deploy.
- Reconcile adjustments land in a system category, "Penyesuaian Saldo", excluded from budget
  reports. Past uncategorised adjustments are moved into it.
- The Telegram bot, its route, its secrets wiring and its code are gone. The next release
  removes its container.

## What Changes

- Database:
  - widen the transaction and category pillar check constraints to the full set;
  - create the excluded "Penyesuaian Saldo" expense and income categories for every user;
  - backfill reconcile adjustments into them.
- Backend:
  - the full pillar set in the transactions, categories, ingest and AI schemas;
  - a single effective-pillar definition shared by ledger listing and filtering, reports and
    pulse;
  - Kakeibo report fields for culture and unexpected, the three benchmark groups, and a
    corrected status;
  - pulse spending includes every non-saving pillar.
- AI prompt: pillar definitions for the owner's Kakeibo set, with a new prompt version.
- Notification noise classification:
  - promotional words no longer discard a recognised settled transaction;
  - hard-noise and headline-figure rules ignore what cannot be a ledger fact;
  - a startup pass moves matching open review events to ignored.
- Reconcile uses the system category instead of a name search.
- Frontend:
  - a shared pillar list used by quick capture, ledger edit and the category form;
  - token pairs for the two new pillars;
  - insights and dashboard Kakeibo views for five pillars and three groups;
  - a status badge mapped to backend statuses.
- **BREAKING** (operations only): remove `telegram-bot/` and its compose service, nginx
  server, deploy environment, runtime-environment materialisation, example variables,
  documentation and ignore rules.

## Non-goals

- New transaction fields (source, merchant, review status), reimbursement handling, the
  food category split and merchant learning. These are phases 2 and 3.
- Companion capture-policy changes. The one-time-code filter fix is a separate change in the
  companion repository.
- Removing the duplicate startup SQL path. That is phase 8. This change follows the existing
  convention.
- Production authority. Verification is local: tests on a migrated disposable PostgreSQL,
  plus browser checks. Deployment happens only through the GitHub workflow after the owner
  merges. Revoking the bot token, deleting its GitHub secrets and removing its Cloudflare
  route are owner actions.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `kakeibo-decoupling`: the full pillar set in selection, and effective-pillar aggregation.
- `kakeibo-reconciliation`: allocation over five pillars, judged by three benchmark groups.
- `notification-filtering`: backend noise classification that keeps settled payments and
  ignores marketing, news and instructions.
- `account-reconciliation`: adjustments use an excluded system category.
- `clean-core-ledger`: safe-to-spend counts every spending pillar, and API-key access is
  described for the companion instead of the Telegram bot.

The project capability list is unchanged. The money, database, llm and personal-data
standards apply: migration safety, reversible data effects, prompt versioning, and no
personal data in logs.

## Impact

- **Migrations:** `db/migrations/V27__full_kakeibo_and_adjustment_category.sql`, plus its
  startup mirror in `backend/app/db/`.
- **Backend:**
  - routers: `transactions.py`, `categories.py`, `ingest.py`, `dashboard.py`, `pulse.py`, `accounts.py`;
  - services: `notification_evidence.py`, `notification_interpretation.py`, `notification_processing.py`, `movement_linkage.py` or a sibling module;
  - the prompt `services/prompts/notification_interpretation.md`;
  - tests.
- **Frontend:**
  - pages: `app/insights/page.tsx`, `app/ledger/page.tsx`, `app/page.tsx`;
  - components: `components/ui/QuickCaptureModal.tsx`, `components/dashboard/KakeiboPillarCards.tsx`;
  - styling: `tokens.css`, `globals.css`, `components/charts/theme.ts`;
  - `lib/api.ts` types; tests.
- **Operations:**
  - `docker-compose.yml`, `nginx/default.conf`, `.github/workflows/deploy.yml`;
  - `scripts/materialize_runtime_env.py`, `.env.example`, `.gitignore`;
  - `README.md`, `docs/production-deployment.md`.
- **API:**
  - additive fields on the dashboard Kakeibo object;
  - pillar fields accept two more values;
  - `kakeibo_status` values are unchanged.
- **Data:** reconcile adjustments with the default note and no category move into the excluded
  category, so they leave spending and income totals.
