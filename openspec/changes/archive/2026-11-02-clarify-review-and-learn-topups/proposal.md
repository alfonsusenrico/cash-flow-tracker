# Proposal

## Approval

Owner approved on 2026-11-02: "approve 1 and 2, learn for 3". That is: (1) Tinjauan shows only what needs the owner, (2) a debit can be settled as an investment top-up from Tinjauan and the ledger, and (3) the "learn" option for top-ups: remember converted debits, suggest the next matching one, and record it automatically once the pattern is confirmed. Phase 0 shared decisions of the same day also apply: the product is public and multi-user, the AI learns each user and asks only when a human would ask, amounts are never guessed, and optional features stay hidden until needed.

## Why

A read-only look at the owner's account on 2026-11-02 found two things that make Tinjauan feel unreliable:

- "Per toko" contained only a completed internal movement (a GoPay pocket to savings, both legs linked). The owner read it as "already resolved but still asking". It recurs with every AI-recorded movement, because linked legs stay `auto` and the queue lists them.
- Two RDN debits of the same amount, in the same second, waited in "Pindah saldo menunggu pasangan". They were the monthly top-ups of two mutual-fund products, but that section's picker lists only liquid accounts, so Tinjauan cannot settle them. The month before, the owner converted the same pair through the ledger edit dialog, whose conversion button sits at the bottom of a long form. Nothing remembered that conversion, so the same question comes back every month.

## Expected Outcome

- Tinjauan, its navigation badge and the monthly report's waiting counts never include a completed (linked) movement leg.
- Tinjauan splits rows that need an answer ("Perlu jawaban") from rows recorded automatically ("Dicatat otomatis"), which is collapsed and labelled optional. The page header shows the same "needs attention" number as the badge and the automatic count separately.
- A waiting outgoing debit from a liquid account offers "Ini top up investasi" with the eligible amount-tracked mutual-fund products ("Induk · Produk"); unit-tracked funds are explained and linked to "Ubah ke pelacakan nominal".
- In the ledger edit dialog the "Jadikan Top up Investasi" block sits next to the type and category fields and is visible without scrolling at 390 px.
- After a debit has been converted once, the next unlinked debit from the same account with the same amount shows "Sepertinya top up ke <produk>, kayak bulan lalu?" with "Ya, top up", "Pilih produk lain" and "Bukan top up". Two equal debits in one window with two learned products are assigned one product each, deterministically and with an explanation; otherwise the owner is asked to choose.
- Once a product has been confirmed for that pattern at least 3 times with at least 90% weighted agreement (corrections and rejections weigh 2), the next matching debit is recorded as a top-up at ingestion, classified `habit` and reviewed. `TOPUP_AUTO_RECORD_ENABLED=false` turns this off while keeping the suggestions.
- The owner's existing conversions (`investment_topups.converted_from`) count as confirmations immediately; nothing is rewritten.

## What Changes

- Review queue, summary and monthly report counts exclude linked movement legs at query time. Stored rows keep their review status; no data migration and no change to how new movements are recorded.
- `GET /api/review/queue` adds `counts.attention` and `counts.unreviewed` (the summary's numbers) and, on awaiting outgoing legs, an optional `topup_suggestion`. The movement group kind and the always-false `editable` flag it needed are removed.
- New service `topup_learning.py`: evidence from live converted top-ups (read-only derivation, which also backfills existing conversions) and from suggestion outcomes; per-product agreement; one-to-one assignment; automatic recording.
- Migration V43 `investment_topup_feedback`: the owner's answers to top-up suggestions (accepted, corrected, rejected), user-scoped.
- `POST /api/investment-topups/from-transaction` accepts an optional `suggested_target_account_id`; new `POST /api/review/topup-suggestions/reject`.
- Notification recording: an unlinked debit awaiting its counterpart is converted automatically when the learned pattern qualifies (D7).
- `GET /api/transactions` returns `recurring_rule_id`, which the ledger already checks before offering a conversion.
- Web: Tinjauan sections and header, top-up option and suggestion on awaiting cards, ledger dialog block moved up.
- Setting `TOPUP_AUTO_RECORD_ENABLED` (default true) in `core/config.py`, `docker-compose.yml` and `.env.example`.

## Non-goals

- Android changes. The companion does not call the review or top-up endpoints; a phone "Labelnya sudah benar" action is a later follow-up.
- Recording system-completed movements as reviewed, or any rewrite of stored review states.
- Learning from manual top-ups that were not converted from a debit, learning amounts (the debit's amount is always the top-up's amount), or suggesting unit-tracked products.
- Production access, deployment or production data. Verification is local on disposable PostgreSQL with synthetic data; release goes through the approved pipeline after the owner merges.

## Capabilities

### New Capabilities
- `topup-learning`: top-up evidence, per-product agreement, suggestion, one-to-one assignment, automatic recording and its switch.

### Modified Capabilities
- `review-queue`: linked legs excluded from the queue and counts, group keys without the movement group, Tinjauan sections, awaiting top-up option and suggestion.
- `investment-amount-conversion`: optional suggested product on the conversion request, ledger block placement, and the recurring-rule field the ledger check needs.

Standards: money invariants (bilateral legs, funding not debited twice, idempotent ingestion, reversible top-ups), multi-user scoping, Flyway as the only schema owner, the `llm` capability's confidence gate and kill switch for automatic actions (deterministic statistics, no model call), and WCAG 2.2 AA for the changed pages. No new capability class is added to the project.

## Impact

- Backend: `routers/review.py`, `routers/investment_topups.py`, `routers/transactions.py`, `services/investment_topups.py`, new `services/topup_learning.py`, `services/notification_application.py`, `services/monthly_report.py`, `core/config.py`.
- Database: `V43__investment_topup_feedback.sql` (additive, new table only).
- API: additive fields and one new endpoint; the review queue drops the `movement` group kind and `editable`, used only by the web app.
- Web: `app/review/page.tsx`, `components/review/*`, `app/ledger/page.tsx`, `lib/review.ts`, `lib/investmentTopups.ts`.
- Android: none.
