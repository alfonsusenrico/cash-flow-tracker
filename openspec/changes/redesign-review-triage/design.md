# Design

## Context

See proposal.md for why and what. This section records the trace of 2026-11-03 that the plan rests on. The base is worktree `feat/review-triage` at `origin/main` f8f5bf6, with phase 1 merged. The approved Tinjau preview is `cash-flow-tracker-prototypes/2026-11-02-web-redesign/src/combined/views.js` (`triageList`, `triageItem`), `app.js` (key handler) and `styles.css` (`.triage`). Observations come from code. Assumptions are marked.

### Tinjau today (observed)

- `frontend/src/app/review/page.tsx` (344 lines) reads `GET /review/queue`, `/categories` and `/accounts`. It stacks the sections "Apa ini?" (`#review-questions`), "Perlu jawaban" (`#review-groups`), "Pindah saldo menunggu pasangan" (`#review-awaiting`), "Pasangan pindah saldo belum pasti" (`#review-ambiguous`), a collapsed "Dicatat otomatis" and, always last, `HistorySuggestionsSection`. Status goes to one `aria-live` paragraph. There is no undo and no keyboard layer.
- The cards hold the answer logic. `PurposeQuestionCard` posts `answer-purpose` with a fresh `reply_id`. `ReviewGroupCard` handles accept, recategorise (category and pillar) and rename. Each row links to `/ledger?edit=<id>&return=review`. `MovementReviewCards` handles completion with the `counterpart_candidates` 409 flow, merge, keep separate, "Ini top up investasi", the learned suggestion and "Bukan top up". `SuggestionGroupCard` and `HistorySuggestionsSection` handle runs, accept and reject. `lib/review.ts` holds types, `splitGroups`, `chunkIds` (200 per bulk request) and error messages.
- Beranda's `TodoCard` links to `/review?item=<transaction id>#review-questions|review-awaiting|review-ambiguous|review-groups`. A group links by its first transaction id.
- Phase 1 primitives exist: `ShortcutProvider` with `useShortcut(key, handler, scope)` (single keys skipped in fields, off switch `shortcuts`, scoped definitions fire only while `scope.current.contains(document.activeElement)`); `ToastProvider` (one `role=status` toast, 6.5 s, pauses on hover and focus, optional "Urungkan"); `Drawer`; `EmptyState`; `ErrorState`; `Pill`; `ChipGroup`; and `useEscapeLayer`. The help dialog lists only the global keys. Nothing moves focus on route change.

### Queue data and counts (observed)

- `GET /api/review/queue` (`routers/review.py`) returns `questions` (open purpose questions, by event), `groups` (auto and needs-review rows that are not linked legs, keyed by merchant, signature, notes or category, each with `needs_review_count` and `auto_count`), `awaiting` (unlinked "Internal Movement" legs whose stored reasons include `awaiting_counterpart`, each with an optional `topup_suggestion`), `ambiguous` (legs with candidates), `truncated` (1 000-row limit) and `counts`. `counts.attention` and `counts.unreviewed` come from `review_counts`, the same SQL as the badge (`monthly_report._REVIEW_COUNT_SQL`). Attention counts rows that need review or have an open question. Unreviewed counts automatic rows without a question.
- A group can mix review states. `splitGroups` puts any group with a needs-review row under "Perlu jawaban", so automatic rows of a mixed group are counted in `unreviewed` but shown under "Perlu jawaban".
- History suggestions (`GET /review/suggestions`) are owner-started proposals for rule- or AI-classified rows that the owner never edited. They can exist for reviewed rows, and they are not counted in `attention` or `unreviewed`.

### Review actions and their side effects (observed)

| Action | Endpoint | Writes |
| --- | --- | --- |
| Accept | `POST /review/bulk` `accept` | `review_status = reviewed` only |
| Recategorise, rename | `POST /review/bulk` | Row classification, `review_status`, owner provenance (`OWNER_EDIT_ASSIGNMENTS`). `record_owner_corrections` then writes a `correction` feedback row per changed row, upserts owner `merchant_memory` per key and closes the open purpose question |
| Keep separate | `POST /review/bulk` `keep_separate` | `owner_split_at`, `review_status` |
| Complete | `POST /movements/complete` | Inserts a leg with `idempotency_key = completion:<id>` or links an existing row. Links both legs (category Internal Movement, pillar, notes), sets reviewed, and rewrites the legs' notification events (`movement_id`, `confirmed_role`, `result_key`, `result_snapshot`, closes purpose questions) |
| Merge | `POST /movements/merge` | `link_transactions_as_movement(owner_edit=True)`: links both legs, owner provenance, `correction` feedback, closes questions. No event rewrite |
| Top-up conversion | `POST /investment-topups/from-transaction` | Inserts `investment_topups` (with `converted_from`) and the inbound leg. Turns the debit into the outbound leg with owner provenance, `correction` feedback and a closed question. With a suggestion, writes an `investment_topup_feedback` answer. May initialise amount tracking on the product, and applies `+amount` to its value and cost basis |
| Bukan top up | `POST /review/topup-suggestions/reject` | One `investment_topup_feedback` `rejected` row |
| Purpose answer | `POST /ingest/notifications/{id}/answer-purpose` | Row values, owner provenance, a `purpose_answer` feedback row (unique `reply_id`), memory when reviewed, and on the event: `active_question = NULL`, `interpretation.purpose_answer`, `result_snapshot.description` |
| Suggestion accept, reject | `POST /review/suggestions/accept`, `/reject` | Accept: row values, owner provenance, `accepted_suggestion` feedback, memory, closed questions, suggestion `accepted`. Reject: suggestion `rejected` |

- Reversal paths that exist today: `mutate_topup(delete=True)` with `restore_converted_expense`, which deletes the inbound leg, restores category, pillar and notes, and applies `−amount` to the product. It does not restore review status or classifier for an owner conversion. `POST /movements/{id}/split` unlinks, stamps `owner_split_at` (never pair again), re-derives categories from notification evidence and rebuilds event results. Capture undo (`text_capture.undo_capture`) compares stored row states, deletes rows and restores memory only if the entry is unchanged since.
- Balances are derived (`initial_balance` plus income minus expense rows), so restoring rows restores balances.
- **Debt and goals.** No Tinjau action changes debt or goal progress. `link_transactions_as_movement` and `complete_movement` refuse goal, debt, plan and fee rows. The conversion refuses linked rows. History suggestions exclude them. Bulk actions never touch `obligation_id`, allocations or goals, and `goals.current_amount` is written only by the goals router. The "undoable, debt progress reversed once" rule in AGENTS.md belongs to plan links in Beranda's Kerjakan dulu, not to Tinjau.
- **Feedback readers.** `habits.load_labels` uses the latest feedback row per transaction, of the evidence kinds. `purpose_questions.rank_suggestions` reads recent rows with an after-category. `history_suggestions.preferred_repayment_offset` counts after-offsets. `question_rate` counts `correction` rows. `topup_learning.load_labels` reads live conversions (`is_deleted = FALSE`) and the latest `investment_topup_feedback` per transaction. The capture undo leaves its `capture` feedback row in place. That existing gap is out of scope and recorded under Risks.
- **Locks.** Bulk, complete, answers, suggestion accept and notification processing take the per-owner advisory lock `notification:{user_id}`. Learned auto-record takes `investment-topup-from:{user}:{tx}` inside that lock. The from-transaction route takes only the latter.
- **Companion.** It calls `answer-purpose` and parses with `org.json` (unknown keys ignored). `PurposeAnswers.kt` settles any 409 as "closed". It calls no other review endpoint.

## Goals / Non-Goals

**Goals:**

- One press answers an item, and the owner can take it back exactly. No undone answer is left behind as evidence, money or memory.
- Counts that the badge, the header and the folders agree on, computed from the existing queue without a new query.
- Keys that help without trapping anyone: scoped, skippable, and with an off switch.

**Non-Goals:**

- No change to the queue or summary responses, group keys or review-state meaning.
- No generic audit log or history. Undo is a short-lived capability, not an archive.
- No redo, and no undo of an undo.

## Decisions

### D1. Folders, counts and the header

| Folder (chip) | Source | Count on the chip | One item | In the badge |
| --- | --- | --- | --- | --- |
| Apa ini? | `queue.questions` | number of questions | one question | `attention` |
| Perlu jawaban | `queue.groups` with `needs_review_count > 0` | sum of `needs_review_count` | one group (OD16) | `attention` |
| Pindah saldo belum lengkap | `queue.awaiting` then `queue.ambiguous` | `awaiting.length + ambiguous.length` | one leg | `attention` |
| Saran dari riwayat (folded, optional, OD14) | `GET /review/suggestions` groups | pending suggestions | one suggestion group | not counted |
| Dicatat otomatis (folded, optional) | `queue.groups` with `needs_review_count = 0` | sum of `auto_count` | one group | `unreviewed` |

- "Semua" is the three needed folders in that order. It is the default folder.
- The header reads "Perlu kamu {attention}" and "{unreviewed} dicatat otomatis · opsional", from `queue.counts`, so header and badge always agree. It shows no amount. The preview's "Rp X belum dihitung" contradicts D9 of the phase plan (waiting rows are counted), and an all-time count cannot be paired with the cycle-scoped waiting total.
- Chips count rows, not items, so that without truncation the three needed chips add up to `attention`. The item position reads "2 dari 7" and counts items.
- When a mixed group puts automatic rows under "Perlu jawaban", the "Dicatat otomatis" fold says "{k} lainnya ikut kelompok di Perlu jawaban". The header's `unreviewed` stays the badge number. The difference is then explained, not hidden.
- With `truncated`, the existing notice stays, and chips count only the rows received.
- Known approximation: an awaiting leg that is still `auto` (stored reason but no flag) is listed under Pindah saldo but counted in `unreviewed`. Pairing flags awaiting legs `needs_review`, so the synthetic tests assert the sum equals `attention`. The header never adjusts the server number.

*Rejected:* a new `folders` block in `GET /review/queue`. The client can derive every count from data it already receives, and the companion and Beranda read the same response. Splitting mixed groups by review state on the server: it changes the `review-queue` "Review Group Summary" contract and loses "accept the whole merchant at once".

### D2. Item granularity (OD16)

An item in "Perlu jawaban" and "Dicatat otomatis" is one group, as today. One answer settles every row of that merchant or signature. The item lists its rows (first 10, then "Lihat semua n"), each with "Ubah" to the ledger. Questions, legs and suggestion groups are one item each.

*Rejected (alternative in OD16):* one transaction per item, as the preview shows. It multiplies presses for repeated merchants and gives up the bulk power that "review-queue" approves.

### D3. "Saran dari riwayat" placement (OD14, resolves the phase plan's open question)

It is its own folded, optional section below "Dicatat otomatis", shown when pending suggestions exist. When none exist, a "Cari saran dari riwayat" button at the foot of the list opens the run panel (period, estimate, confirm, progress, last run), unchanged from `HistorySuggestionsSection`. Selecting a suggestion group opens it as an item with "Terima saran" and "Tolak".

*Rejected:* folding suggestions into "Perlu jawaban". They are not in the badge, they can target rows that are already reviewed, and mixing them would break the count rule of D1.

### D4. Item bodies and numbered choices

Every item shows kind pill, title, account, local time, provenance ("Ditangkap dari HP", "Diketik di Catat", "Dicatat manual"), the amount (money tokens, `tabular-nums`, `bal()` so "Sembunyikan saldo" applies) and its reasons via `reviewReasonLabel`. Choices are buttons with a visible key hint. E presses the item's highlighted choice when it has one. Otherwise E announces "Belum ada saran buat diterima".

| Item | Highlighted (E) | Numbered choices (1–9) | T |
| --- | --- | --- | --- |
| Apa ini? | First suggestion, labelled "Saranku" (server order: habit guess, history, current, repayment) | Suggestions in order; "Jawab sendiri" text field after them | — |
| Perlu jawaban group | "Terima" (bulk accept) | 1 Terima, 2 Ubah kategori, 3 Ganti nama | — |
| Awaiting leg with a learned target | "Iya, top up {produk}" | 1 Iya, 2 Pilih lain, 3 Bukan top up; inside an open picker, its options | Opens "Ini top up investasi" |
| Awaiting leg without a target | — | Account and pocket choices of "Pilih pasangannya" (first nine numbered; the rest in the existing select), then "Ini top up investasi", then "Bukan pindah saldo, ini pengeluaran" (OD15) | Opens "Ini top up investasi" |
| Ambiguous leg | — | Candidates (at most 5), then "Biarkan terpisah" | — |
| Suggestion group | "Terima saran" | 1 Terima saran, 2 Tolak | — |
| Dicatat otomatis group | "Udah bener" (accept) | 1 Udah bener, 2 Ubah kategori, 3 Ganti nama | — |

T works only for an outgoing leg from a liquid account, as the existing `topupFor` rule decides. Unit-tracked funds keep the existing explanation and link. The awaiting item's caption is "Selama belum lengkap, ini belum masuk Keluar". That is true for one-sided legs. The preview's "nggak aku hitung ke mana-mana" is not used. "Bukan pindah saldo, ini pengeluaran" (OD15, default yes) opens a category and pillar picker and sends the existing bulk `recategorise` for the leg. The bulk endpoint already accepts an unlinked "Internal Movement" row. The row then becomes a reviewed, owner-classified expense that counts in Keluar. Owner edit time and the new category keep the pairing pass from treating it as a leg again (`movement_pairing.CANDIDATE_SQL`).

### D5. Layout at each width

- The triage is a grid on the main container. With the main column wider than 720 px (container query, the preview's threshold), the list (300–370 px, sticky, own scroll) and the item sit side by side. At 720 px or narrower, one pane shows at a time: the list, or the item with a "Daftar" back button. Widths: 1440 and 1280 (full sidebar) and 1024 (rail) are two panes. 768 (rail, main about 660 px) and 390 (phone) are one pane. At 400% zoom on 1280, one pane.
- A width hook on the triage container, with the same 720 px, decides the pane in JavaScript. The DOM, history and focus then follow the layout, as phase 1 D16 does for Beranda.
- In one-pane mode, opening an item pushes `?item=` to history, so the browser back gesture returns to the list. "Daftar" and Esc (through `useEscapeLayer`) do the same and return focus to the item's list row. In two-pane mode, selection uses `replaceState`.
- Choice buttons are at least 44 px high on phones and 24 px elsewhere. The phone tab bar never covers the last choice (`scroll-padding-bottom` from phase 1).

*Rejected:* the `Drawer` sheet for the phone item. A full-screen pane with Back matches the approved preview, and the item has too many controls for a sheet.

### D6. Order, auto-advance and the empty state

- Order inside folders is the server's: questions by post time, groups by the server sort, legs by date.
- After a successful action, Tinjau computes the next item from the list as it was before the request: the following item, else the previous one. When the folder empties, Tinjau moves to "Semua" if anything is left there. When everything needed is done, it shows "Semua beres". Focus goes to the new item's heading (`tabIndex=-1`). The toast announces "Tersimpan: {apa}. Berikutnya: {judul}" through its `role=status`.
- The queue is then invalidated (with transactions, dashboard and accounts, as today). Selection is held by item id across refetches. A refetch that removes the selected item, for example because another tab answered it, selects the next item by the same rule and says "Item ini udah diberesin di tempat lain".
- "Semua beres" uses `EmptyState`: "Nggak ada yang perlu kamu jawab. Transaksi baru dari HP-mu tetap aku catat." It shows three facts from `GET /dashboard/cycle-summary` (`dashboard.cycleSummary`, already cached by Beranda): Keluar siklus ini, Aman dipakai hari ini or "Sisa" when unavailable, and Belum diklasifikasi (the unclassified Keluar line). It has buttons "Balik ke Beranda" and, if automatic rows exist, "Cek yang otomatis". If the summary fails, the facts are left out, never shown as Rp 0.
- Loading uses `Skeleton` in the final layout. A failed queue uses `ErrorState` with "Coba lagi".

### D7. Deep links and URL state

- `/review?item=<transaction id>` selects the item that contains that transaction: a question's transaction, any row of a group, a leg or a suggestion's transaction. The legacy hash chooses the folder when the id is in several places: `#review-questions` is Apa ini?, `#review-groups` is Perlu jawaban, `#review-awaiting` and `#review-ambiguous` are Pindah saldo. `&folder=apa|jawab|pindah|saran|otomatis` (phase plan D14) does the same and wins over the hash.
- An unknown or already-resolved id opens its folder (or Semua) at the first item and says "Yang kamu buka udah beres".
- A deep link focuses the item heading, so keys work at once (D8). On one-pane widths it opens the item pane.
- The page keeps `?item=&folder=` current, so reload keeps the place. The legacy hash is dropped after it is read. The ledger's `return=review` keeps returning to `/review`, which then selects by the D6 rule. Returning to the exact item is phase 3's drawer work.

### D8. Keys through the registry

- Tinjau registers `j`, `k`, `e`, `t`, `z` and `1`–`9` with `useShortcut(key, handler, triageRegionRef)`. The region is the `section` that holds the list and the item. Keys fire only while focus is inside it, never in fields, and never while the single-key switch is off. Focus enters the region by clicking or tabbing to a row, or through a deep link. The page does not steal focus on a normal visit. A visible hint under the list says "Klik atau Tab ke daftar, lalu J/K".
- Enter is not registered. List rows are buttons, so the browser's Enter activation opens the item. A registered Enter would swallow the activation of other buttons.
- J and K move within the current folder. With focus in the list they move focus to the next row. With focus in the item they select the next item and focus its heading. At either end they announce "Ini yang pertama" or "Ini yang terakhir".
- 1–9 press the nth numbered choice of an open picker, else of the item, matching the preview. Z undoes the newest undoable action of this tab (D15). `?` stays the global help.
- `ShortcutDefinition` gains optional `label` and `group`. While Tinjau is mounted, the help dialog lists a "Di Tinjau" group: J/K pindah, Enter buka, E terima, T top up, 1–9 pilih, Z urungkan. The switch caption becomes "N, ?, dan tombol satu huruf di Tinjau…".
- WCAG 2.1.4 is met twice: by the off switch, and because the keys are active only while the region has focus.

*Rejected:* a page-wide key listener as in the preview. It breaks D8's scoping and conflicts with assistive-technology keys.

### D9. Undo contract

- **Tokens.** Each covered action (D11) returns two top-level fields: `undo_token`, which is 32 random bytes as unpadded base64url (43 characters), and `expires_at` (ISO 8601, 10 minutes after the action, server clock). An action that changed nothing returns `null` for both. That includes an idempotent replay, an accept of rows that were already reviewed, and a "Bukan top up" that was already stored. Only `sha256(token)` is stored, so a database read cannot reuse a token.
- **Endpoint.** `POST /api/review/undo` with body `{"undo_token": "<43 chars>"}`. It is session or Bearer authenticated, like the other review routes, and the global CSRF origin check applies to session requests. Responses:

| Status | Body | When |
| --- | --- | --- |
| 200 | `{"ok": true, "kind", "already_undone": false, "transaction_ids", "removed_transaction_ids", "restored": [{id, type, amount, date, account_id, category_id, kakeibo_type, review_status, movement_id}]}` | Restored |
| 200 | Same shape, `already_undone: true`, `restored: []` | The token was used before. Nothing changes |
| 404 | `{"code": "undo_not_found"}` | Unknown token, another owner's token, or a row past retention |
| 409 | `{"code": "undo_conflict", "transaction_ids": [...]}` | A touched row, event or record changed since the action |
| 410 | `{"code": "undo_expired"}` | `now() >= expires_at` and not used |
| 422 | validation error | Missing or malformed token |

- **Single use.** One token restores at most once. A repeat after success is answered with `already_undone: true` and changes nothing, so a retry after a lost response is safe. This is additive to the approved 404/409/410 set.

*Rejected:* 410 for a reused token. A client whose first response was lost could not tell success from failure. Reusing the action's idempotency for undo (re-posting the inverse action): there is no inverse endpoint for most actions, and an owner edit would stamp `classified_by = owner` (D13).

### D10. `review_actions` (Flyway V44)

```sql
CREATE TABLE IF NOT EXISTS review_actions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash BYTEA NOT NULL UNIQUE,                 -- sha256 of the token; the token is never stored
    kind VARCHAR(30) NOT NULL CHECK (kind IN (
        'review_accept', 'review_recategorise', 'review_rename', 'review_keep_separate',
        'movement_complete', 'movement_merge', 'topup_conversion', 'topup_rejection',
        'purpose_answer', 'suggestion_accept', 'suggestion_reject')),
    transaction_ids UUID[] NOT NULL,                  -- no FK: the record must survive a deleted row
    snapshot JSONB NULL,                              -- before and after images; NULL once used or expired
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    undone_at TIMESTAMPTZ NULL,
    CHECK (expires_at > created_at),
    CHECK (undone_at IS NULL OR snapshot IS NULL)
);
CREATE INDEX IF NOT EXISTS idx_review_actions_expiry ON review_actions (expires_at) WHERE snapshot IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_review_actions_owner ON review_actions (user_id, created_at DESC);
```

The snapshot holds, per action:

- `transactions`: for each touched row, a before and after image of `type, amount, account_id, date, category_id, kakeibo_type, notes, merchant, offset_category_id, movement_id, movement_role, fee_for_movement_id, goal_id, obligation_id, recurring_rule_id, idempotency_key, source, review_status, classified_by, classification_confidence, owner_edited_at, owner_split_at, updated_at`.
- `created_transactions`: ids and after images of rows the action inserted (the completion leg, the top-up inbound leg).
- `events`: before and after of `active_question, movement_id, confirmed_role, result_key, result_snapshot` and `interpretation -> 'purpose_answer'` for the notification events the action changed.
- `memory`: per match key, the entry before (or absent) and its `updated_at` after.
- `feedback_ids` (classification feedback the action appended), `topup_feedback_ids`, `topup` (id, and the product account's before and after `investment_tracking_mode, investment_cost_basis, last_price, investment_value_estimated`), and `suggestions` (id, status before and after).

The record is written in the same database transaction as the action. If the write fails, the action fails and changes nothing. `insert_feedback` and `record_answer` return their ids for this.

*Rejected:* soft-deleting or versioning every table the actions touch (a large migration across transactions, events, memory and suggestions for a 10-minute feature). A client-side undo that replays inverse calls (no inverse for answers, merges or acceptance, and no atomicity across rows). Keeping tokens in Redis (the snapshot must commit atomically with the ledger change, and Redis is not the system of record).

### D11. Restoration per action kind

All kinds run in one transaction and are all-or-nothing. All compare after images first (D12). Restored transaction columns are `category_id, kakeibo_type, notes, merchant, offset_category_id, movement_id, movement_role, review_status, classified_by, classification_confidence, owner_edited_at, owner_split_at`, with `updated_at = NOW()`. Amount, account, date, type, source, idempotency key and goal, debt and plan links are never written. The conflict check proves they are unchanged.

| Kind | Undo |
| --- | --- |
| Accept | Restore `review_status` |
| Recategorise, rename | Restore rows. Append an `undo` row per feedback id (D13). Restore memory per key if unchanged since. Restore the events' open purpose question |
| Keep separate | Restore `owner_split_at` and `review_status`. The repair pass may pair the leg again later, exactly as before the action |
| Complete with a new leg | Restore the original leg from its before image, delete the created leg (its after image must match, so it is still unchanged), and restore the events. Both accounts' derived balances return to before, and the `completion:<id>` key is free for a later completion |
| Complete by linking an existing row, and merge | Restore both legs and the events. Append undo rows for merge feedback (merge writes corrections). No memory is restored, because memory skips movement legs |
| Top-up conversion | Run the existing delete path, `mutate_topup(delete=True)` with `restore_converted_expense`: it deletes the inbound leg, restores category, pillar and notes, applies `−amount` to the product, and marks the top-up `is_deleted`. Then restore the debit's remaining columns (review state, classifier, confidence, owner-edit time, merchant). If the product account is unchanged since the action, restore its tracking mode, cost basis, value and estimated flag exactly. Otherwise leave the delete path's arithmetic, as deleting a top-up does today. Delete the `investment_topup_feedback` row the action stored. Append undo rows for its feedback, and restore the question |
| Bukan top up | Delete the `investment_topup_feedback` row the action stored |
| Purpose answer | Restore the row and the event (`active_question` with its original `question_id`, `interpretation.purpose_answer` as before, `result_snapshot.description`). Append an undo row for its feedback. Restore memory |
| Suggestion accept | Restore rows. Set the suggestion back to `pending` with `decided_at = NULL` if it is still `accepted`. Append undo rows, restore memory and restore questions |
| Suggestion reject | Set the suggestion back to `pending` if it is still `rejected` |

Movement undo restores the stored before images and does not call `split` (OD17). `split` stamps `owner_split_at`, which means "never pair these again". It re-derives categories from evidence instead of the values before the action, and an undo must leave the legs as they were. The top-up undo uses the existing delete path, as the phase plan says, because that path owns the product's value and cost arithmetic.

Debt and goal progress: no covered action writes them (Context), so undo never writes `obligations`, allocations or `goals`. Tests pin this (task 2.3), so a later action that does write them must add its own reversal before it can join D11.

### D12. Conflict detection, locks and concurrency

- **Conflict.** Before any write, undo locks the touched and created transactions (`FOR UPDATE`, id order), the events and the top-up row. It compares each current image with the stored after image. Any difference is answered with 409 `undo_conflict`, naming the changed transaction ids, and nothing is written. This is the capture undo's proven rule. It catches an edit in Transaksi, a later Tinjau action on the same row, a deletion, a re-pairing or a notification retry that touched the event. Memory is not a conflict: a newer entry is the owner's later knowledge and stays (D13). A suggestion that is no longer in its after status stays as it is.
- **Locks.** Undo takes `notification:{user_id}` first, then `investment-topup-from:{user}:{tx}` for conversions, the same order as learned auto-record, then the `review_actions` row `FOR UPDATE`. It serialises with ingestion, bulk, complete, answers and suggestion accept, and with a parallel undo of the same token. The second of two parallel undos sees `undone_at` and answers `already_undone`.
- **Two tabs.** Each tab keeps its own tokens. If tab B changes a row after tab A's action, A's undo is refused (409) and B's undo works. Undo is therefore last-in-first-out per row without any client coordination. A tab learns of the other tab's change on refetch (D6).
- **Ingestion during triage.** A notification that pairs or reclassifies a row between action and undo makes the undo a conflict. It is never overwritten.

### D13. Learning, feedback and memory after undo

- V44 widens `classification_feedback.kind` with `undo` and adds `reverts_feedback_id UUID NULL`. Checks: `(kind = 'undo') = (reverts_feedback_id IS NOT NULL)`, a partial unique index on `reverts_feedback_id` (a label is reverted once), and an index on `(user_id, reverts_feedback_id)`. There is no foreign key, following V29's rule that a labelled example outlives what it describes.
- An undo row copies the reverted row's identity columns (`transaction_id`, `event_id`, `account_id`, `amount`, `institution`, `text_signature`, `match_key`, local weekday and hour, `transaction_source`). It puts the reverted row's after values into its `before_*` columns and leaves every `after_*` column NULL. Rows are only inserted. The V29 append-only trigger stays.
- One SQL helper, `live_feedback_sql(alias)`, means "not an undo row and not reverted". The four readers use it: `habits.load_labels` before choosing the latest row per transaction (so an earlier live label counts again), `purpose_questions.rank_suggestions`, `history_suggestions.preferred_repayment_offset` and `question_rate`. Redoing the same answer after undo writes one new row, which counts once.
- A replay of an undone purpose answer (same `reply_id`) is answered with 409 `purpose_answer_undone`, and nothing is reapplied. The companion treats it as closed. A new answer needs a new `reply_id`, which the web always sends.
- Memory: undo restores an entry only if its `updated_at` still equals the action's. Otherwise the newer value stays. `memory_row` and `restore_memory` move from `text_capture.py` to `merchant_memory.py` and serve both undos.
- Top-up learning: the undone conversion is `is_deleted`, so it is not evidence. The answer row the action stored is deleted, so an earlier answer for the same debit is its latest state again. The `investment_topup_feedback` table has no append-only rule, and deleting the row this action created is the exact inverse.

*Rejected:* writing a compensating correction through the owner-edit path. It would stamp `classified_by = owner` on an undone accept and add a correction that habits weigh twice. Filtering undone rows only in habits: purpose suggestions and the question rate would still count them.

### D14. What cannot be undone, and what the UI shows

- **After 10 minutes, or after a later change.** Tinjau shows the server's reason: "Udah lewat 10 menit, jadi nggak bisa dibatalkan di sini" or "Nggak bisa dibatalkan: transaksinya udah berubah sejak itu". Both come with a link to the row in Transaksi (`/ledger?edit=<id>&return=review`), where the owner edits by hand.
- **Starting a history-suggestion run.** Model calls cannot be taken back. It already asks for confirmation after the estimate. It shows no "Urungkan", and its suggestions can be rejected.
- **The model call behind a text answer.** Undo restores the ledger, the event and the learning. The call itself was made, and that is not reversed. The UI does not mention it, because nothing the owner sees remains.
- **Learned automatic top-ups, edits in Transaksi, deletes, and Kerjakan dulu plan actions** are outside Tinjau's actions. They keep their own paths: deleting the top-up in Rekening, the phase 3 drawer, and plan unlink.
- **A group larger than 200 rows** is saved in batches, each with its own token. "Urungkan" undoes the batches newest first. If one batch is refused, the toast says "n dari m transaksi dibatalkan", and the rest stays saved.

### D15. Frontend undo client (`lib/reviewUndo.ts`)

- An in-memory stack per tab, newest first, at most 20 entries. Each entry holds one or more tokens, `expires_at`, a label and the restored item's ids. Tokens are never written to storage or URLs. They are capabilities.
- "Urungkan" in the toast, and Z, undo the newest entry. Z keeps working after the toast is gone, until the entry expires. That covers WCAG 2.2.1 for a 6.5 s toast. Pressing Z again undoes the next entry. With nothing left, Z says "Nggak ada yang bisa dibatalkan".
- On success: refetch, select the restored item, focus its heading, and toast "Dibatalkan. {judul} balik ke {folder}". A 404, 409 or 410 gives an alert with the message and link from D14, and the entry is dropped. A network failure keeps the entry and says "Gagal membatalkan. Coba lagi". The retry is safe (D9).
- A response without `undo_token` (an older API during a rollout, or a no-op) shows no "Urungkan" and pushes nothing.

### D16. Owner scoping and failure behaviour per endpoint

| Endpoint | Owner scoping | New or changed failure behaviour |
| --- | --- | --- |
| `POST /review/bulk` | Unchanged (404 `transactions_not_found` for any foreign id) | Token only when something changed. A failing token write fails the whole request (500, nothing changed) |
| `POST /movements/complete`, `/merge` | Unchanged | Idempotent replays return `undo_token: null`. A 409 `counterpart_candidates` issues no token |
| `POST /investment-topups/from-transaction` | Unchanged | Idempotent replay: `null`. 409 and 422 refusals issue nothing |
| `POST /review/topup-suggestions/reject` | Unchanged | `recorded: false`: `null` |
| `POST /ingest/notifications/{id}/answer-purpose` | Unchanged (404 for another owner's event) | Replay: `null`. Replay of an undone reply: 409 `purpose_answer_undone` |
| `POST /review/suggestions/accept`, `/reject` | Unchanged | Only applied suggestions enter the snapshot. Skipped ids issue nothing |
| `POST /review/undo` | Token looked up by hash and `user_id`; another owner's token is 404, never 403, so existence never leaks | D9 table. 500: rolled back, nothing changed |

Every restore statement filters by `user_id`. The snapshot holds only rows the action read under the owner's id.

### D17. Retention and privacy

The snapshot contains the owner's merchants, notes and amounts, copies of data the owner already holds. It is set to NULL when used (the record keeps only ids and timestamps for the replay answer) or at expiry. A bounded sweep (500 rows per pass) runs in `_ledger_matching_loop`, plus a lazy clear on any lookup and on each new action of the owner, so it also works with background jobs off. Records are deleted 24 hours after creation. After that an old token gets 404. Nothing about undo is logged beyond the kind and the outcome code. No snapshot, merchant, note or amount appears in logs.

### D18. Tests

- **Backend (pytest, disposable Flyway-migrated PostgreSQL 16, `REQUIRE_TEST_DATABASE=1`).** For each kind in D11: action then undo gives the exact before images of every touched row, equal derived balances on every involved account, an equal product value and cost basis, the question open again with the same `question_id`, equal memory, the suggestion pending, habit decisions and purpose suggestion order equal to before, and the question-rate corrections equal to before. Also: one conflict case per kind (409, nothing written); expiry (410) using an injected clock; replay (`already_undone`); another owner's token (404, A's rows unchanged); malformed body (422); a session request without Origin (403); two parallel undos of one token (one restores, one `already_undone`); retention sweep (snapshot NULL after expiry, row gone after 24 h). Movement legs: both legs, the created leg removed, events restored, completion possible again. Top-up: inbound leg gone, `is_deleted`, product exact when unchanged and arithmetic after a valuation, reconversion works, and replaying the original notification records nothing new (idempotency key kept). Debt: a debt-linked and a goal-linked row accepted or recategorised then undone leave `obligations.remaining_amount`, `is_archived`, allocations and `goals.current_amount` unchanged. Learning: the reverted label stops counting and the earlier label counts again; redo counts once; the undo row is not evidence for any reader; V44 checks reject an `undo` row without `reverts_feedback_id` and a second revert of one row. Responses: old fields unchanged, and the new fields `null` on no-op actions.
- **Frontend (Vitest, `vitest-axe`).** `lib/triage`: folders, counts, the sum rule, the mixed-group caption, deep-link resolution including legacy hashes, and next-item rules. Keys: each key inside the region, nothing outside it, nothing in the text answer, nothing with the switch off, and the help dialog's "Di Tinjau" group. Focus: to the next heading after an action, to the restored item after undo, and back to the row after Back or Esc on one pane. Undo client: stack, Z after the toast closed, batch undo, 404/409/410 and network messages, and a response without a token. Phone pane and history. The empty state with and without the cycle summary. axe for each state in both themes.

### D19. Gates

- Browser matrix on `/review` with a synthetic seeded owner at 1440, 1280, 1024, 768 and 390 px, light and dark. It checks axe (zero serious or critical), measured contrast of chips, counts, key hints, pills and amounts, no horizontal overflow, no console errors and a screenshot per cell. It includes a keyboard-only walk through every folder (J/K, Enter, E, T, a number, Z, ?), the phone list to item to Back (button, Esc and browser back), Beranda's Kerjakan dulu links landing on their items, 200% and 400% zoom at 1280 px, and reduced motion.
- Performance on the synthetic dataset, median of 20 local calls: `GET /review/queue` no slower than the baseline by more than 10% (it is unchanged). `POST /review/undo` under 300 ms for a one-row action and under 1 s for a 200-row recategorise. The undo statement count does not grow with row count, except one memory statement per distinct key.
- Bundle: no new runtime dependency, and `/review` first-load JS (from `next build`) at most 10 kB above the baseline.

## Risks / Trade-offs

- [The 10-minute window is longer than the toast] → Z and the stack keep undo reachable, and each entry disappears at its server expiry.
- [Restoring stored images could overwrite a newer change] → Every touched row is compared with its after image, and any difference refuses the whole undo (D12).
- [A snapshot holds personal data] → It exists only until use or expiry, is never logged, and the record is deleted after 24 hours (D17).
- [After a rollback to code without V44 awareness, reverted labels count again] → Undo rows carry no after values, so old readers ignore them. Only the reverted originals return as evidence, which is the state before undo existed. It is recorded in the Migration Plan.
- [Mixed groups: header `unreviewed` differs from the "Dicatat otomatis" chip] → The fold says how many automatic rows sit in "Perlu jawaban" groups (D1).
- [Scoped keys do nothing until focus enters Tinjau] → The hint under the list, deep links that focus the item, and Tab order starting at the list.
- [Existing gap, not fixed here: a capture undo leaves its `capture` feedback row, which habits can count] → Out of scope. A follow-up can reuse `undo` rows for capture undo. Recorded in PROJECT_STATE.md as debt.
- [Queue N+1: ambiguous candidates and top-up suggestions run per row] → Unchanged by this phase. The performance gate measures it against the baseline.

## Migration Plan

- **V44** is additive: one new table, one nullable column, a widened kind check with the same idempotent `DO $$` guard as V31 and V33, a new check, and two indexes. It takes no rewrite or long lock on `transactions`. `classification_feedback` gets `ADD COLUMN` with NULL default (metadata only on PostgreSQL 16) and a check that holds for every existing row (none is `undo`). The check can be added `NOT VALID` and then validated.
- **Mixed versions during release.** Flyway runs before the new API starts, so the old API may briefly run on V44. It ignores the table and column and never writes `undo`. Old browsers on the new API ignore `undo_token` and `expires_at`. A new browser on an old API sees no token and shows no "Urungkan" (D15). The companion's parser ignores the new fields, and its 409 handling covers `purpose_answer_undone`.
- **Release.** The owner merges, and `deploy.yml` releases backend and frontend in one image set. The companion needs no release.
- **Rollback.** Revert the merge commit and redeploy through the pipeline. V44 stays (Flyway is forward-only). The old code ignores `review_actions` and `reverts_feedback_id`. Outstanding tokens become useless. The old code does not sweep `review_actions`, so snapshots of the last few minutes before the rollback would stay. The rollback therefore ships with a one-statement migration that deletes every `review_actions` row, so no snapshot outlives its window. A forward release can then reuse the empty table. Labels undone before the rollback count again under the old readers, the pre-feature behaviour, and no money figure is affected.

## Open decisions for owner

Each has a recommended default. Answering "defaults ok" accepts all four.

Resolved 2026-11-03: the owner answered "approved, defaults ok", so OD14–OD17 take their recommended defaults below.

| # | Decision | Recommended default and why |
| --- | --- | --- |
| OD14 (resolved: default) | Where "Saran dari riwayat" lives in Tinjau | **Its own folded section marked optional, below "Dicatat otomatis", shown only when suggestions are pending.** Suggestions are not in the badge, can target rows that are already reviewed, and are optional by nature. Folding them into "Perlu jawaban" would make the count disagree with the badge |
| OD15 (resolved: default) | Offer "Bukan pindah saldo, ini pengeluaran" on an awaiting leg, as the preview does | **Yes.** It uses the existing bulk recategorise, which already accepts an unlinked "Internal Movement" row. The owner picks a category and pillar, the row becomes a reviewed, owner-classified expense and enters Keluar, and it is undoable. Without it the only way out is editing in Transaksi. If you prefer to keep Tinjau's money choices as they are, it waits for phase 3 |
| OD16 (resolved: default) | One item per merchant group, or per transaction, in "Perlu jawaban" and "Dicatat otomatis" | **Per group, as today.** One answer settles every row of the same merchant or signature, and each row keeps "Ubah". The preview shows single rows, but it has no repeated merchants |
| OD17 (resolved: default) | Undo of a movement completion or merge: restore the exact state before, or call the existing `split` | **Restore the exact state before.** `split` marks the legs "never pair automatically again" and re-derives their categories. An undo should leave them exactly as they were, still pairable. The approved draft said "existing reversal paths", and this keeps that for top-ups only |
