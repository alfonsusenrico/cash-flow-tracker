# Proposal

## Approval

Owner approved on 2026-10-31 ('habit learning ok') with defaults: ≥3 answers and ≥90% agreement to record automatically, weaker patterns suggested first, corrections re-ask, income/transfers never automatic, weekly question-rate metric.

## Why

The owner wants the system to "learn from the recorded transactions so in the future it asks fewer and fewer confirmations and 'Apa ini?' questions". Today merchant memory removes questions only for named merchants. Every payment whose notification carries no merchant name, such as a myBCA "Financial Diary: You spent IDR x at <BCA category>", still gets "Apa ini?", however often the owner has given the same answer. A read-only aggregate count of production on 2026-10-31 showed one week of labels: 12 "Apa ini?" answers across 7 categories (none above 33%) and 17 memory rows. The labelled examples that phase 3 started keeping are now enough to start, and the long-deferred phase 9 ("Sepertinya …?") is the next step.

## Expected Outcome

- After the owner has answered the same kind of merchant-less payment (same bank, bank category, account and a similar amount) at least three times, with at least 90% agreement on one category and pillar, the next one is recorded without a question, labelled "Dicatat dari kebiasaan: <kategori>" with its basis ("3 dari 3 jawaban sebelumnya"), and does not appear in Tinjau.
- Below that threshold the owner is still asked, but the habit's best guess is the first suggestion, "Sepertinya <kategori>?".
- One correction of a habit-recorded payment makes the next similar payment ask again.
- Income, movements and transfers are never recorded from a habit, and a habit never changes an amount, date or account.
- Analisis shows "Seberapa sering ditanya" per week from real counts, with an honest empty state, so the owner can see the questions decline.
- `HABIT_AUTO_RECORD_ENABLED=false` returns to asking every time while keeping the guess first.

## What Changes

- A deterministic habit learner (no model call) over the owner's own labels in `classification_feedback`: purpose answers that classified the row, corrections, and captures the owner classified. Automatically classified rows, accepted history suggestions and habit-recorded rows are never evidence.
- The habit key is institution, bank category signature and account, and the amount must be within ±25% (as a ratio) of a labelled payment. Only the latest 10 labelled payments of a key count, and only the latest label of each transaction. A correction weighs 2 and an answer 1. Time of day and weekday only break ties and choose the wording.
- Notification recording: a qualifying merchant-less expense is recorded from the habit with `classified_by = 'habit'`, `review_status = 'reviewed'`, no purpose question and no model call. The basis is stored on the event and shown on the companion result, the ledger row and the ledger edit dialog.
- Purpose suggestions: the habit's guess comes first as "Sepertinya <kategori>?".
- Migration V39: `habit` is added to the transaction classifier check, and feedback rows record the classifier before and after and the review state after.
- `GET /api/dashboard/question-rate?weeks=1..26` and the "Seberapa sering ditanya" panel on Analisis.
- Setting `HABIT_AUTO_RECORD_ENABLED` (default true), passed through `docker-compose.yml` and documented in `.env.example`.
- A deterministic offline simulation, `scripts/verify/habit_simulation.py`, on a synthetic eight-week history.

## Non-goals

- Merchant-named payments (merchant memory already handles them), typed captures (they carry no bank signature) and incomes.
- Model involvement in habit decisions, decay by calendar time, or per-owner tuning of thresholds.
- Android changes. The companion shows server results; a read-only check found nothing that breaks (see design D10), and showing the habit label on the phone is a follow-up.
- Production access, deployment or production data. All verification is local on disposable PostgreSQL with synthetic data. Release goes through the approved pipeline by the owner.

## Capabilities

### New Capabilities
- `habit-learning`: habit evidence, key, band, thresholds and weighting, eligibility, automatic recording, guess, explanation, kill switch and the weekly question-rate metric.

### Modified Capabilities
- `notification-purpose-questions`: payments recorded from a habit get no question, and the habit's guess leads the suggestions.
- `merchant-memory`: the classification order gains the habit step, and feedback rows record the classifier and review state.
- `transaction-provenance`: the `habit` classifier and the provenance a habit record writes.

Standards: the `llm` capability's evaluation and guardrail rules (deterministic statistics, an offline evaluation with recorded results, a confidence gate and a kill switch); money invariants (labels only, idempotent ingestion); Flyway migrations; WCAG 2.2 AA for the new panel and labels. No new capability class is added to the project.

## Impact

- Backend: new `services/habits.py` and `services/question_rate.py`; `notification_context.py`, `notification_application.py`, `notification_processing.py`, `purpose_questions.py`, `merchant_memory.py`, `provenance.py`, `movement_pairing.py`, `capture_resolution.py`, `routers/transactions.py`, `routers/dashboard.py`, `core/config.py`.
- Database: `V39__habit_learning.sql` (additive).
- API: the notification result gains an optional `habit` object (companion 1.8.0 and later); purpose suggestions may have kind `habit` with a `basis`; ledger list and detail gain `habit_basis`; `classified_by` may be `habit`; new `GET /api/dashboard/question-rate`.
- Web: Analisis panel, ledger row mark, edit-dialog explanation and classifier label.
- Android: no change required; a follow-up is recorded.
