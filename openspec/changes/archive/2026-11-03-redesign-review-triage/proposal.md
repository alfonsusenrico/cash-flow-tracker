# Proposal

## Why

Tinjau is still the long page of stacked sections from before the redesign. Every question, group and unfinished transfer is open at once, nothing moves on after an answer, and a wrong tap can only be repaired in Transaksi. The approved phase plan (`redesign-web-workspace`, phase P2) replaces it with an inbox-style triage: one item at a time, keys, auto-advance and "Urungkan". Undo touches money and learning, so it needs a server contract and a new table, which is why phase 2 gets its own approved plan.

## Approval

Binding earlier approvals:

1. **Phase plan, 2026-11-02.** The owner approved `redesign-web-workspace` with "approved with defaults, start phase 1". Its design is binding for this phase: the P2 row (folders, list plus one item, keys J/K/Enter/E/T/1–9/Z/? scoped to the triage region, auto-advance, Urungkan, "Semua beres", phone list to item with Back), the P2 undo contract (`undo_token`, `expires_at`, `POST /api/review/undo`, 404/409/410, single use, owner-scoped, 10 minutes, append-only `classification_feedback`, existing reversal paths for top-ups and movements), D8 (one shortcut registry, region scoping, off switch, WCAG 2.1.4), D14 (`/review?item=<id>&folder=<key>` and no new paths), D9 (rows waiting in Tinjau stay recorded and counted) and D17 (WCAG 2.2 AA verification).
2. **Phase 1 decisions that apply here.** OD1: the product keeps the name "CashFlow". OD7: waiting items are worded "nunggu jawabanmu", and wording must not suggest that waiting rows are left out of totals. The other decisions (OD2–OD6, OD8–OD13) do not touch Tinjau.
3. **Combined preview, 2026-11-02.** The owner approved `cash-flow-tracker-prototypes/2026-11-02-web-redesign/combined.html` as the definition of the redesign. Its Tinjau view (option E) is the visual and interaction target. Where it conflicts with an approved money rule, the rule wins, as recorded in design.md.
4. **Tinjau semantics, 2026-11-02** (`clarify-review-and-learn-topups`, "approve 1 and 2, learn for 3"). Linked movement legs are never listed or counted. "Perlu jawaban" (needs review) is separate from the folded, optional "Dicatat otomatis" (automatic). The header shows the badge's `attention` and `unreviewed` numbers. An awaiting debit offers "Ini top up investasi" and a learned suggestion with accept, pick another and "Bukan top up". Learned top-ups are recorded automatically after the threshold.
5. **Phase 0, 2026-11-02.** The product is public and multi-user, the design is not specific to the owner, optional features stay hidden until needed, the AI asks only when a human would ask, and amounts are never guessed.
6. **Start of phase 2, 2026-11-03.** The owner said "merged, start phase 2". This authorises planning. It does not approve this plan.

7. **Phase 2 plan, 2026-11-03.** The owner approved this plan (proposal, design, specs and tasks) with "approved, defaults ok". OD14–OD17 take their recommended defaults (design.md, "Open decisions for owner").
8. **Acceptance, 2026-11-03.** After reviewing verification.md and the screenshots, the owner merged PR #63 and said "accepted, defaults ok". OD18 and OD19 keep their applied defaults. The header and badge keep counting rows while the inbox position counts merchant groups (OD16), and the focus outline on the item heading stays as built. Recorded debt: a capture undo still leaves its `capture` feedback row (design.md, Risks).

## Expected Outcome

- Opening Tinjau shows a list on the left and one item, large, on the right. Folder chips "Semua", "Apa ini?", "Perlu jawaban" and "Pindah saldo belum lengkap" carry row counts that add up to the sidebar badge. "Saran dari riwayat" and "Dicatat otomatis" are folded below the list and marked optional.
- The owner answers an item with one press. Tinjau saves it, says what happened in a toast with "Urungkan", and moves to the next item. When nothing is left, Tinjau shows "Semua beres" with this cycle's Keluar, Aman dipakai hari ini and Belum diklasifikasi from Beranda's figures.
- J and K move between items. Enter opens a row. E accepts the item's highlighted suggestion. T opens "Ini top up investasi". 1–9 pick a numbered choice. Z undoes. ? lists the keys. The keys work only while focus is inside Tinjau, never while typing, and they turn off with the existing "Pintasan satu tombol" switch.
- "Urungkan" and Z put an answer back exactly as it was within 10 minutes: category, pillar, notes, review state, the open question, the movement legs, the investment product's value and cost, and what the app learned. An undone answer never counts as a learned example. An undo that would overwrite a later change is refused with a clear message instead.
- On a phone, Tinjau is a list. Tapping an item opens it full screen with a "Daftar" back button, and the browser's back gesture returns to the list.
- Links from Beranda's "Kerjakan dulu" (`/review?item=…#review-questions` and the other sections) open the matching item. Every existing URL keeps working.
- `/review` passes WCAG 2.2 AA checks at 1440, 1280, 1024, 768 and 390 px in light and dark.

## What Changes

- **Tinjau page:** the stacked sections are replaced by the triage inbox (folders, list, one item, auto-advance, empty state, phone list and item). The answer logic of `PurposeQuestionCard`, `MovementReviewCards`, `ReviewGroupCard`, `SuggestionGroupCard` and `HistorySuggestionsSection` is reused as item bodies on the phase 1 primitives. The old section layout is removed.
- **Awaiting legs (OD15, default yes):** besides completing the transfer and "Ini top up investasi", an awaiting leg offers "Bukan pindah saldo, ini pengeluaran" through the existing bulk recategorise.
- **Keyboard:** Tinjau registers its keys in the existing shortcut registry, scoped to the triage region. The help dialog lists page keys while their page is open.
- **Undo, backend:** each review action returns `undo_token` and `expires_at`: bulk accept, recategorise, rename and keep separate, movement complete and merge, top-up conversion, "Bukan top up", purpose answers, and history-suggestion accept and reject. The values are `null` when the action changed nothing. New `POST /api/review/undo` restores the action's before-state within 10 minutes. It is owner-scoped and single use, and answers 404, 409 or 410. Undo appends `undo` rows to `classification_feedback` and never updates a row. Top-up undo goes through the existing top-up delete path. Movement undo restores the legs and notification results that the action changed.
- **Learning stays honest:** habits, purpose suggestions, the repayment-offset preference and the weekly question rate ignore undone feedback and the undo rows. Undoing a top-up conversion or a "Bukan top up" removes the answer that action stored. A replay of an undone purpose answer is refused with 409 `purpose_answer_undone`.
- **Database:** Flyway V44 adds the owner-scoped `review_actions` table (token hash, kind, before and after snapshot, expiry). The snapshot is cleared at expiry and the row is deleted after 24 hours. V44 also adds the `undo` kind and `reverts_feedback_id` to `classification_feedback`.
- **No change** to `GET /api/review/queue`, `GET /api/review/summary`, badge semantics or what any total counts.

## Non-goals

- New answer types from the preview that the app does not have: splitting one payment across pillars ("Campur, pisah jumlahnya"), and the "Ingat jawaban ini" checkbox (learning is already automatic).
- Undo of edits made in Transaksi, of deletes (phase 3, OD6), of Kerjakan dulu plan actions (they already have their own unlink), of learned automatic top-ups (deleted from Rekening as today), and of starting a history-suggestion run.
- Any change to ledger semantics, review status meaning, movement pairing, top-up learning thresholds or the phone companion. The companion's endpoints only gain optional response fields.
- Other pages. Transaksi, Analisis, Laporan, the money pages and Pengaturan keep their phase 1 state.
- Production. All verification is local, against a disposable migrated PostgreSQL with synthetic data and a local build. Pushing, opening the pull request, merging and releasing stay with the owner and the `deploy.yml` pipeline.

## Capabilities

### New Capabilities

- `review-undo`: server-side undo of review actions. It covers the token contract on each action, `POST /api/review/undo` and its errors, exact restoration per action kind, conflict detection, append-only feedback reversal, learning that ignores undone labels, retention of the snapshots, and owner scoping.

### Modified Capabilities

- `review-queue`: the Tinjau page becomes the triage inbox (folders and counts, one item, auto-advance, empty state, phone list and item, deep links, keys, Urungkan). The accessible-action and batched-action requirements cover undo.
- `history-suggestions`: the "Saran perbaikan riwayat" section becomes the folded, optional "Saran dari riwayat" part of Tinjau, with the same controls, and accept and reject return undo tokens.
- `notification-purpose-questions`: an answer returns an undo token, and a replay of an undone reply is refused.
- `habit-learning`: undone labels and undo rows are not habit evidence, and the weekly question rate does not count undone corrections.
- `topup-learning`: undoing a conversion or a "Bukan top up" removes the answer that action recorded.
- `executive-sidebar-layout`: the shortcut help lists the keys of the open page, and the off switch covers them.

Standards that apply: web UI and accessibility (WCAG 2.2 AA, including 2.1.4 and 2.4.11), money correctness (balances, movement legs, top-up value and cost, idempotency keys), database migration safety, personal-data retention (snapshots are cleared at expiry), multi-user isolation, and api-consumers compatibility for the companion. The capability list in AGENTS.md does not change. AGENTS.md's line on the owner-edit path gains the undo rule.

## Impact

- **Backend:** new `backend/app/services/review_undo.py` and the route in `backend/app/routers/review.py`. Token issuing is added in `review.py` (bulk, top-up rejection), `routers/movements.py` (complete, merge), `routers/investment_topups.py` (from-transaction), `services/purpose_questions.py` (answer) and `services/history_suggestions.py` (accept, reject). `insert_feedback` returns the feedback id. Feedback readers in `services/habits.py`, `services/purpose_questions.py`, `services/history_suggestions.py` and `services/question_rate.py` exclude undone rows through one shared SQL helper. The capture undo's `memory_row` and `restore_memory` move into `services/merchant_memory.py` and are shared. The expired-snapshot sweep joins the existing `_ledger_matching_loop`. Tests are added for every action kind, conflict, expiry, replay, owner isolation and learning.
- **Database:** `db/migrations/V44__review_undo.sql` (additive: one table, one nullable column, a widened check, two indexes).
- **Frontend:** `src/app/review/page.tsx` is rewritten. New `src/components/review/triage/*`, `src/lib/triage.ts` and `src/lib/reviewUndo.ts`. The existing review cards are restyled as item bodies. `components/workspace/shortcuts.tsx` lists page keys in help. Tests replace `app/review/page.test.tsx` and add triage, keyboard, focus and undo tests.
- **API consumers:** responses only gain fields. The phone companion parses with `org.json` and ignores unknown keys. Its answer retry already treats any 409 as closed.
- **Docs:** AGENTS.md (owner-edit path and undo rule), `docs/NEW_USER_GUIDE.md` (Tinjau section).
- **No new runtime dependency.**
