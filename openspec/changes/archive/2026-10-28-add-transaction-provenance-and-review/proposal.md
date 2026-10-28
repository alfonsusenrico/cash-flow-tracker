## Approval

Owner authorized phases 2–8 on 2026-10-28 without per-phase review; coordinator-reviewed.

## Why

A ledger row cannot say where it came from, which merchant it was, who chose its category, or
whether the owner has looked at it. The audit (`research/notes/2026-10-28-automation-quality-audit.md`,
section 1D) found that provenance can only be guessed from event links and key prefixes, and that
nothing records "created automatically, then corrected by the owner". The later phases of the
automation plan (record-and-flag ingestion, the "Apa ini?" prompt, the web review queue, history
suggestions and habit learning) all need these facts as stable columns.

Separately, a friend repaying a shared expense is recorded as income today. It inflates income and
leaves the shared spending at its full amount, so both the 50/30/20 benchmark and safe-to-spend are
wrong. The owner also decided to split food into daily meals and snacks/cafés, and the default
categories have no place for the second.

This is phase 2 of `research/notes/2026-10-28-automation-quality-plan.md`.

## Expected Outcome

- Every transaction, old and new, states its source (manual, notification, recurring, payroll,
  movement, reconcile, investment or import), its review status (auto, needs review, reviewed) and
  who classified it (rule, AI, owner or system). Existing rows get these values from a documented,
  deterministic backfill that does not touch `updated_at`.
- The owner sees the source and review status in Indonesian in the ledger edit dialog, can enter a
  merchant name, and can mark a row as reviewed with one button. Editing the category, pillar,
  notes, merchant, type or offset marks the row as reviewed and classified by the owner.
- The ledger API filters by source and review status, which the phase 6 review queue will use.
- A friend's repayment recorded in "Patungan & Ganti Uang" is not income. When the owner says which
  expense category it repays, it lowers that category's spending, its Kakeibo pillar, total
  spending and the cycle spending behind safe-to-spend. Without a chosen category it is shown as an
  unallocated reimbursement and changes no spending figure.
- Every user has a "Jajan & Nongkrong" expense category with the want pillar. Past rows stay where
  they are.

## What Changes

- Database (V28 plus its identical startup mirror):
  - `transactions`: `source`, `merchant`, `review_status`, `classified_by`,
    `classification_confidence`, `owner_edited_at`, `offset_category_id`, with check constraints
    and a review-queue index;
  - `categories.offsets_spending`;
  - a one-time backfill of provenance for existing rows;
  - per-user categories "Patungan & Ganti Uang" (income, offsets spending) and
    "Jajan & Nongkrong" (expense, want).
- Backend:
  - every transaction insert path writes provenance explicitly;
  - owner-edit tracking on transaction, movement, merge and top-up edits;
  - a review-only PATCH, merchant and review-status validation, and new response fields;
  - ledger filters `source` and `review_status`, and merchant in search;
  - reimbursement offsets in dashboard overview and analytics totals, category breakdowns,
    Kakeibo, and pulse cycle spending and safe-to-spend;
  - reimbursements excluded from every income total;
  - category API exposes `offsets_spending` and protects categories used by offsets;
  - new users get both new categories.
- Frontend:
  - ledger edit dialog: provenance summary, "Tandai sudah ditinjau", merchant field, and the
    offset category select for reimbursement income;
  - quick capture: the offset category select for reimbursement income;
  - insights and dashboard keep reading backend figures.

## Non-goals

- Filling `merchant` from notifications, the confidence gate and merchant memory (phase 3).
- The "Apa ini?" prompt and companion changes (phase 4).
- A web review queue, reimbursement figures on report cards, and net daily series (phase 6).
- Moving past food rows into "Jajan & Nongkrong" or re-classifying history (phase 7).
- Removing the startup mirror and the column defaults kept for rollback (phase 8).
- Production authority. Verification is local: a disposable PostgreSQL migrated V1→V28, the full
  test suites and a browser check. Deployment happens only through the GitHub workflow after the
  owner merges.

## Capabilities

### New Capabilities

- `transaction-provenance`: source, merchant, review status, classifier, confidence and owner-edit
  tracking for every transaction; the backfill; ledger filters and display.
- `reimbursement-offsets`: the reimbursement category, the offset link and its validation, and how
  reimbursements change income, spending, category, Kakeibo and safe-to-spend figures.

### Modified Capabilities

- `custom-category-management`: an added requirement for the "Jajan & Nongkrong" default category.

The project capability list is unchanged. The database, money, personal-data and multi-user
standards apply: migration safety with a rollback path, reversible data effects, owner scoping of
every reference, and no transaction content in logs.

## Impact

- **Migrations:** `db/migrations/V28__transaction_provenance_and_reimbursements.sql` and
  `backend/app/db/transaction_provenance_and_reimbursements.sql`, run by `init_db.py`.
- **Backend:**
  - new `services/provenance.py` and `services/reimbursements.py`;
  - `services/kakeibo.py`, `services/ledger_mutations.py`, `services/movement_linkage.py`,
    `services/notification_application.py`, `services/investment_topups.py`;
  - routers `transactions.py`, `movements.py`, `recurring.py`, `accounts.py`, `categories.py`,
    `dashboard.py`, `pulse.py`; `db/init_db.py`; tests.
- **Frontend:** `app/ledger/page.tsx`, `components/ui/QuickCaptureModal.tsx`, new
  `lib/provenance.ts`; tests.
- **API:** additive fields on transactions, categories, dashboard and pulse responses; new optional
  request fields `merchant`, `review_status`, `offset_category_id`; new ledger query parameters.
  Dashboard and pulse spending and income figures change by the reimbursement rules.
- **Data:** every existing transaction gets provenance values; two categories per user are added.
