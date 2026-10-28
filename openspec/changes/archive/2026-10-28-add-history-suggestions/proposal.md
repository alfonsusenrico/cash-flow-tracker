## Approval

Owner authorized phases 2–8 on 2026-10-28 without per-phase review; coordinator-reviewed.

## Why

Phases 2–6 made new rows honest and teachable, but the months recorded before them still carry what
the old pipeline wrote (audit `research/notes/2026-10-28-automation-quality-audit.md`):

- generic names such as "Pengeluaran makanan dan minuman", "Belanja", "Pembayaran" or "Transfer
  masuk" instead of the merchant or purpose;
- food rows split between need and want without a rule, and cafés and snacks outside "Jajan &
  Nongkrong";
- friends repaying a shared meal filed as income ("Pendapatan Lain") instead of offsetting the
  spending (owner decision: a repayment is not income).

Reports over those months are therefore wrong, and the owner would have to reopen each row by hand.
The memory and the model context of phase 3 already know the owner's answers; this phase uses them
to propose fixes for history. Nothing changes without the owner's acceptance.

This is phase 7 of `research/notes/2026-10-28-automation-quality-plan.md`.

## Expected Outcome

- On "Tinjau" the owner picks a period, sees an estimate (how many automatic rows qualify, how many
  are covered by memory or rules, and how many model calls the rest would need) and starts the run
  only after confirming it.
- The run proposes a better merchant, notes, category, Kakeibo pillar and, for repayments, the
  spending category it offsets. Owner memory comes first, then two rules (friend repayments and
  cafés or snacks), then bounded model batches. Without a model the run still completes with the
  memory and rule suggestions and says why the rest was skipped.
- A "Saran perbaikan riwayat" section groups the suggestions by proposed merchant, category and offset with
  count, total and a before → after preview. The owner accepts or rejects a whole group or one row.
- Accepting applies the values exactly like an owner edit: the row becomes owner-classified and
  reviewed, an `accepted_suggestion` example is kept in the append-only feedback, and merchant memory
  learns. Rejecting is remembered: a later run never proposes the same values for that row again.
  A row edited after the suggestion was made makes it superseded.

## What Changes

- Database (V31 plus its identical startup mirror): `suggestion_runs` (window, counts, status,
  error code, model and prompt version, lease; one running run per owner), `transaction_suggestions`
  (proposal, source, confidence, reason, the row's values when proposed, status; at most one pending
  suggestion per row), and the feedback kind `accepted_suggestion` with a `history_suggestion_id`
  column.
- Backend:
  - new `services/history_suggestions.py`: eligibility, memory and rule proposals, model batches,
    storage, supersession, run lifecycle with a lease, accept and reject;
  - new `routers/history_suggestions.py`: `POST /api/review/suggestions/runs` (estimate, or start with
    `confirm: true`), `GET /api/review/suggestions/runs/latest`, `GET /api/review/suggestions`,
    `POST /api/review/suggestions/accept`, `POST /api/review/suggestions/reject`;
  - `OpenAINotificationProvider.suggest_history` with a strict schema and the prompt
    `prompts/history_suggestions.md` (`history-suggestions-1`);
  - `record_owner_corrections` and `insert_feedback` take the feedback kind and suggestion id;
  - settings `HISTORY_SUGGESTION_MAX_ROWS` (default 300) and `HISTORY_SUGGESTION_BATCH_SIZE`
    (default 20) in config, `.env.example`, Compose, the runtime materialiser and the deploy workflow.
- Frontend: `components/review/HistorySuggestionsSection.tsx` (period, estimate and confirmation,
  progress, last run, groups) and `SuggestionGroupCard.tsx` on `/review`; `lib/historySuggestions.ts`.

## Non-goals

- Applying anything automatically, or changing amounts, dates, accounts, types or movement links.
- Re-pairing old transfers as movements (phase 5 repair covers recent legs; older ones stay manual).
- Rows the owner classified (manual rows, owner edits), movement legs, fee rows, rows linked to goals
  or debts, and system rows (recurring, payroll, reconcile, investment).
- Statistical habit suggestions and decay (phase 9); removing the startup mirrors (phase 8).
- Production authority. Verification is local: a disposable PostgreSQL migrated V1→V31, the full test
  suites with a mocked provider, and a headless browser check against synthetic data. Deployment
  happens only through the GitHub workflow after the owner merges; the first run against real history
  is the owner's action.

## Capabilities

### New Capabilities

- `history-suggestions`: owner-started suggestion runs over past automatic rows, their estimate,
  proposal order, storage and supersession, the accept and reject decisions, and the review section.

### Modified Capabilities

- `merchant-memory`: the append-only feedback also stores accepted history suggestions, with their own
  kind and suggestion id.

The project capability list is unchanged (database, money, personal-data, multi-user, llm,
background-jobs apply). Standards: migration safety with rollback, owner scoping of every reference,
bounded and redacted model input with a mocked provider in tests, logs with counts and codes only,
and an owner-visible cost estimate before any model call.

## Impact

- **Migrations:** `db/migrations/V31__history_suggestions.sql` and
  `backend/app/db/history_suggestions.sql`, run last by `init_db.py`.
- **Backend:** new `services/history_suggestions.py`, `routers/history_suggestions.py`,
  `services/prompts/history_suggestions.md`; changes to `services/merchant_memory.py`,
  `services/openai_notification_provider.py`, `core/config.py`, `db/init_db.py`, `main.py`; tests.
- **API:** additive. New `/api/review/suggestions*` routes; no existing response changes.
- **Data:** two new tables, one new nullable feedback column, a widened feedback kind check. No
  existing row changes. Accepting a suggestion changes that row exactly like an owner edit.
- **Cost:** model calls only after the owner confirms an estimate; at most
  `ceil(max_rows / batch_size)` calls per run (15 with the defaults); memory and rules need none.
- **Config:** two optional settings with defaults; an invalid value disables runs (503) and nothing else.
