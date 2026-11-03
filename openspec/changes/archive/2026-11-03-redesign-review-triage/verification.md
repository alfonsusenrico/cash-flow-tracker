# Verification: redesign-review-triage

All checks ran locally on 2026-11-03 against disposable resources: PostgreSQL 16 container `cft-triage-pg` (127.0.0.1:55482), the worktree API on 8182 with AI and background jobs off and outbound HTTP pointed at a dead proxy, `next start` on 3183 behind a loopback proxy on 3182, and headless Brave over CDP on 9482. No production host, phone or real model was used. Data is synthetic only (owners `beranda_contoh` and `pemilik_lain`; seed `seed.py` plus `seed_triage.py` in the scratchpad folder `web-p2`).

Revisions: the baseline is `df492a5` (approval record on top of plan `2152eac`, application code equal to `origin/main` f8f5bf6). The backend under test is `f1efc00` (unchanged afterwards). The frontend under test is `c77ac32`; the browser matrix, the regression pass and the smoke ran on its production build. Later commits touch only OpenSpec documents.

## 1. Baseline (task 1.1, at `df492a5`)

| Check | Result |
| --- | --- |
| Backend suite, Flyway V1–V43, `REQUIRE_TEST_DATABASE=1`, `SKIP_TEST_MIGRATIONS=1` | 1585 passed, 0 failed, 0 skipped, 1 warning (passlib `crypt` deprecation) |
| `npm ci` / `npm test` | ok / 62 files, 564 tests passed |
| `npm run type-check` / `npm run lint` / `npm run build` | ok / 0 errors, 1 warning (`_done` unused in the old `review/page.tsx`) / ok |
| `/review` first-load JS (scripts the prerendered HTML loads) | 14 scripts, 796.3 KB raw, 236.4 KB gzip (CSS 17.0 KB gzip) |
| `GET /api/review/queue`, 20 calls after one warm-up, extended synthetic seed | median 7.8 ms, p95 9.5 ms, max 11.1 ms |
| `GET /api/review/summary`, same | median 2.3 ms, p95 3.9 ms |
| `/review` in the browser at 1280 and 390, light and dark | axe 0 violations in all 4 cells, overflow 0 px, console errors 0. The page was the stacked sections (screenshot `web-p2/baseline/review-1280-light.png`) |

Seeded queue for `beranda_contoh`: attention 9, unreviewed 5 (questions 1; needs-review rows 3 in two groups, "Kedai Awan" mixing 2 needs-review and 2 automatic rows; 4 awaiting legs, one RDN debit with the learned suggestion "Aplikasi Investasi · Reksa Dana A" basis 1 of 1, one Dompet Digital send for OD15; 1 ambiguous credit with 2 candidates; 3 pending history suggestions from a rule-only run, `error_code: ai_disabled`, no model call). `pemilik_lain` keeps one needs-review row for isolation.

## 2. Migration (task 1.2)

| Check | Result |
| --- | --- |
| Fresh database, Flyway 10 `migrate` V1→V44, `info` | "Successfully applied 44 migrations", `Schema version: 44` |
| Second `migrate` | "Schema "public" is up to date. No migration necessary." |
| Copy of the seeded V43 browser database (142 transactions, 1 feedback row) migrated to V44 | 1 migration applied, row counts unchanged, `classification_feedback_kind_check` widened, `classification_feedback_undo_reverts_check` present and `convalidated = t` |
| Constraint tests (`test_v44_feedback_checks_and_the_append_only_trigger`, `test_v44_review_action_checks`) | `undo` without `reverts_feedback_id` rejected; `reverts_feedback_id` on a non-undo row rejected; a second revert of one row rejected (unique index); an update of a feedback row still raises (V29 trigger); a record with `undone_at` and a snapshot, a 5-byte hash and an unknown kind rejected |
| Rollback companion | `db/rollback/V45__clear_review_actions.sql` (one `DELETE FROM review_actions;`), outside Flyway's location, so it is not applied on main's chain (design, Migration Plan) |

## 3. Backend (tasks 2.1–2.12, at `f1efc00`)

Full suite on a Flyway-migrated V44 database with `REQUIRE_TEST_DATABASE=1` and `SKIP_TEST_MIGRATIONS=1`, as `checks.yml` runs it: **1630 passed, 0 failed, 0 skipped**, 1 warning (passlib). Of these, 45 are new in `tests/test_review_undo.py` (42 functions, one parametrised four times). Four existing tests that compared whole response bodies now compare the old fields and check the new `undo_token` and `expires_at` (`test_accept_marks_rows_reviewed_without_reclassifying`, `test_accept_applies_through_owner_edit_hooks`, `test_reject_is_remembered_by_later_runs`, `test_answer_with_suggestion_updates_row_feedback_memory_and_result`; the latter also asserts that a replay returns the same body with both fields `null`). The capture undo tests pass after `memory_row` and `restore_memory` moved to `merchant_memory.py`.

| Area | Covered by (test names in `test_review_undo.py`) |
| --- | --- |
| Token and record (2.1) | 43-character base64url token of 32 bytes, only `sha256` stored, kind and ids recorded, 10-minute expiry (`test_token_is_stored_only_as_its_hash`); a failing record write fails the whole action and leaves the row, feedback and records unchanged (`test_a_failing_record_write_fails_the_action`); no token when nothing changed (`test_no_token_when_nothing_changed`) |
| Endpoint and refusals (2.2) | 200 shape and exact restore, record cleared, replay `already_undone` with nothing changed (`test_undo_an_accept_restores_status_and_answers_the_documented_shape`); 422 for missing, short, invalid and long tokens; 404 unknown, 410 at 10 minutes (injected clock) with the snapshot cleared, 404 after 25 hours with the record gone; another owner's token 404 and A's row unchanged; session request without Origin 403, foreign Origin 403, app Origin 200; two parallel undos of one token: one restores, one `already_undone`, exactly one `undo` feedback row |
| Bulk (2.3) | recategorise of two rows and of a question row undone: every image equal, the question back with the same `question_id` and listed again, memory equal to before, 3 corrections and 3 undo rows, no live label left; rename undone with a newer owner memory entry kept; keep-separate undone, then the repair pass pairs the leg; a Transaksi edit in between: 409 `{"code": "undo_conflict", "transaction_ids": [id]}` and the edit stays; debt- and goal-linked rows recategorised and undone: `remaining_amount`, `is_archived`, allocations and every goal's `current_amount` equal throughout |
| Movements (2.4) | completion with a new leg undone: leg removed, original leg and events exact; BCA Harian balance 1 000 000 before, 1 500 000 after the completion, 1 000 000 after the undo, every account balance equal to before; completing again creates a new leg; completion that linked an existing row undone with both rows exact and balances equal; idempotent replay and `counterpart_candidates` issue nothing; merge undone with both legs exact, no `owner_split_at`, balances equal, no live label; a split between merge and undo gives 409 |
| Top-ups (2.5) | conversion undone: inbound leg gone, top-up `is_deleted`, debit exact, RDN balance unchanged, product mode, cost basis, value and estimate flag equal to before; the original notification sent again records nothing (one debit); reconversion into another product works; after a newer valuation the delete arithmetic applies (value and cost each −112 590); undoing a suggested conversion leaves the earlier "Bukan top up" as the latest answer and the learner's labels equal to before; "Bukan top up" undone removes only its row and the suggestion returns unchanged; a refused conversion stores no record |
| Purpose answers (2.6) | suggestion answer undone: row, event (`active_question` with the same id, `purpose_answer`, `result_snapshot`) and memory exact; replay before undo returns `null` tokens; response keys are today's `{ok, result, answer}` plus the two new fields; replay after undo 409 `purpose_answer_undone` and nothing reapplied; a new `reply_id` answers the reopened question; a text answer that stays `needs_review` is undoable; a text answer through a mocked normaliser (`AsyncMock`, awaited once, no real call) is undone including its memory |
| Suggestions (2.7) | accept undone: rows exact, suggestions pending, no live label; reject undone; a suggestion superseded in between keeps its status; an accept of only skipped ids returns `null` |
| Learning (2.8) | correction undone: habit labels and purpose suggestion order equal to before, redo counts once; three answers with the third undone: the next payment is asked with basis 2 of 2; question rate: an undone answer counts as asked not answered, an undone correction is not counted; repayment preference ignores reverted rows |
| Retention (2.9) | sweep clears the snapshot after expiry (no merchant or amount left, token 410) and deletes after 24 hours (404); with background jobs off, the next action of the owner clears expired snapshots; captured logs of a rename, undo and replay carry no merchant, note, amount or token, only `kind` and `outcome` |
| Concurrency and isolation (2.10) | two tabs: A's undo 409, B's undo 200 and the row back to A's after-state; a notification paired between action and undo gives 409 and the paired leg stays; owner B gets 404 on bulk, complete, merge, from-transaction and "Bukan top up" for A's rows, sees no A item in the queue, and A has no undo record |

Timings and statement counts (task 2.11, synthetic owner `perf_contoh` created and deleted for the run, 20 measured calls after one warm-up each):

| Measurement | Result | Gate |
| --- | --- | --- |
| `POST /api/review/undo` for a one-row accept | median 3.8 ms, max 6.3 ms | < 300 ms |
| `POST /api/review/undo` for a 200-row recategorise | median 23.0 ms, max 29.3 ms | < 1 s |
| Statements of one undo, 1 row against 50 rows of one merchant (`test_undo_statement_count_does_not_grow_with_rows`) | equal | no growth except memory per key |
| `GET /api/review/queue` after the change | median 7.1 ms (baseline 7.8 ms) | ≤ 110% of baseline |
| `GET /api/review/summary` after the change | median 1.9 ms (baseline 2.3 ms) | unchanged |

## 4. Frontend (tasks 3.1–4.8, at `c77ac32`)

| Check | Result |
| --- | --- |
| `npm test` | 65 files, 604 tests passed (baseline 564; +40 in `app/review/page.test.tsx`, +8 `lib/triage.test.ts`, +5 `lib/reviewUndo.test.ts`, +2 `SuggestionGroupCard.test.tsx`, +1 shortcuts; the old page tests were replaced and the history-suggestion section tests now cover the run panel) |
| `npm run type-check` / `npm run lint` | 0 errors / 0 errors, 0 warnings |
| `npm run build` | ok |
| `/review` first-load JS | 14 scripts, 806.2 KB raw (+9.9 KB), 242.5 KB gzip (+6.1 KB), CSS 18.1 KB gzip. Gate ≤ +10 kB: met (raw and gzip). No new runtime dependency |
| `rg` for the removed stacked layout | `Section`, `splitGroups` and the old radio and select completion forms are gone; nothing imports them |

Scenarios with Vitest names kept from the specs: Counts match the badge (chips 1, 2, 2 in the triage fixture; header and badge 5; Semua four items), Folder filter, Optional automatic rows, Automatic rows inside a mixed group, Two panes on a wide screen, One pane at 768 px with history entry, back gesture and "Daftar", Hidden balances, Keyboard group accept with Next item and Urungkan, Failed bulk action, Partial failure ("200 dari 250 transaksi tersimpan") and Batched undo ("50 dari 250 transaksi dibatalkan" with the Transaksi link), Failed undo (410 text and link), Response without a token, Nothing to undo, Tokens are not stored (storage and URL), Undo by Z after the message closed (real 6.5 s toast timeout, then Z restores and focuses the item), Walk and accept, Outside the region, Typing an answer ("1 kopi"), Switched off, Help lists the page keys (with axe), Leg recorded as spending (one `recategorise` with the leg and category; Internal Movement is not offered), the top-up scenarios (settle with unit-fund explanation, none on an incoming leg, accept, correct and reject a learned suggestion, ask without a default), Answered elsewhere, Nothing waiting with the three cycle figures and "Cek yang otomatis", Summary unavailable (no figures, no "Rp 0"), Not counted as waiting, Link from Kerjakan dulu (folder pressed, heading focused, hash dropped, `?item=&folder=pindah`), Beranda links for `#review-questions`, `#review-groups` and `#review-ambiguous`, Resolved meanwhile, and the queue ErrorState with "Coba lagi". axe runs in the default, habit, existing-leg, top-up, suggestion, Semua beres and help states, all with 0 violations.

## 5. Browser (tasks 5.2–5.5, production build of `c77ac32`)

### Matrix on `/review` (seeded owner, attention 9)

| Width | Panes | Light: axe / overflow / console | Dark: axe / overflow / console |
| --- | --- | --- | --- |
| 1440 | two | 0 / 0 px / 0 | 0 / 0 px / 0 |
| 1280 | two | 0 / 0 px / 0 | 0 / 0 px / 0 |
| 1024 (rail) | two | 0 / 0 px / 0 | 0 / 0 px / 0 |
| 768 (rail) | one; item pane also checked | 0 and 0 / 0 px / 0 | 0 and 0 / 0 px / 0 |
| 390 (phone) | one; item pane also checked | 0 and 0 / 0 px / 0 | 0 and 0 / 0 px / 0 |

In every cell: no clipped row title, choice, heading or chip; no control under 24 px; badge, header and chip sum all 9. Measured minimum contrast (light / dark): chips 10.65 / 10.18, counts 9.25 / 9.06, key hints 5.04 / 5.65, pills 5.72 / 7.96, amounts 6.91 / 9.46, captions and row lines 4.99 / 5.23, choice labels and captions 5.39 / 6.35, row titles 14.72 / 11.18, navigation badges ≥ 6.35. 200% and 400% zoom at 1280 px: one pane, overflow 0, nothing clipped, axe 0. Reduced motion: the media query matches and transitions run at 0.01 ms.

Screenshots (`web-p2/matrix/`): `review-{1440,1280,1024,768,390}-{light,dark}.png`, the item panes `review-{768,390}-{light,dark}-item.png`, `review-zoom-200.png`, `review-zoom-400.png`. Each was looked at. Fixed after the first pass: at 400% zoom the one-pane rows cut their titles with an ellipsis, so row texts now wrap in one pane; the transfer rows repeated "Pindah saldo" before their state, now they say only "Belum ketemu pasangannya" or "Top up Reksa Dana A?". Known capture artifact, as in phase 1: full-page shots at 390 px and 400% draw the fixed tab bar where the viewport ended.

### Keyboard walk, top-up round trip and counts in the real UI (`web-p2/interact/interact.json`)

- Beranda's three Kerjakan dulu links (`#review-questions`, `#review-awaiting`, `#review-ambiguous`) open their items in the right folder with the heading focused and the URL rewritten to `?item=…&folder=apa|pindah`.
- Top-up suggestion, E then Z: after E the toast read "Tersimpan: debit dicatat sebagai top up investasi. Berikutnya: Keluar dari RDN Contoh." and focus moved to the next heading; badge, header and chip sum 9 → 8; cycle Keluar 3 826 090 → 3 938 680 and its saving line 912 590 → 1 025 180 (the debit moved into Kakeibo saving); waiting 9 → 8. After Z: "Dibatalkan. Keluar dari RDN Contoh balik ke Pindah saldo belum lengkap.", counts 9, Keluar 3 826 090, saving 912 590, waiting 9 and 1 029 590, and the debit is "Internal Movement" again.
- J and K in each folder move within the folder, keep focus in the list, and say "Ini yang pertama." at the start; Enter opens the row. T on the Dompet Digital send opened "Ini top up investasi. Ke produk mana?", 1 pressed the first product, Z undid it (counts 9 → 8 → 9). ? opened the help with the "Di Tinjau" group and the switch caption naming Tinjau; Esc closed it.
- Answering every needed item from the keyboard (E where a choice is highlighted, else 1): after each answer the badge, the header and the chip sum agreed (8, 6, 5, 4, 3, 2, 1, 0), no alert appeared, and Tinjau ended on "Semua beres" with "Keluar siklus ini Rp 3.688.680 · Aman dipakai hari ini Rp 232.000 · Belum diklasifikasi Rp 312.000"; Beranda's waiting figure (cycle summary) was 0. Z then restored the last answer: counts 1, waiting 1 (Rp 250.000).
- At 390 px: list → item → list by "Daftar", by Esc and by the browser's back; each time the list showed with focus on the item's row and the path stayed `/review`.
- Console errors during the run: none. The database was restored from the pre-run dump afterwards.

### Other routes (regression) and linked pages

`/`, `/ledger`, `/insights`, `/laporan`, `/accounts`, `/goals` and `/settings` at 1280 and 390 px, light and dark, on this build and on a production build of `origin/main` (extracted with `git archive`, same seeded database): identical results. The only serious finding on both is the known `/accounts` color-contrast on two `.opacity-75 text-[10px]` captions at 1280 (phase 1 record; phase 5 work). Overflow 0 and console errors 0 everywhere. Smoke at 1280 and 390, light and dark: a row's "Ubah" link `/ledger?edit=<id>&return=review` opens the edit dialog (axe 0), `/review` loads the triage again, and `/settings` (where the "Pintasan satu tombol" switch lives in the help) has axe 0; console errors 0 in all four runs.

## 6. OpenSpec and diff

- `openspec validate --all --strict`: 73 passed, 0 failed, exit 0.
- `git diff --check` (working tree) and `git diff --check origin/main...HEAD`: clean.
- Backend suite repeated at `c77ac32` on a freshly migrated V44 database: 1630 passed, 0 failed, 0 skipped.

## Not verified

- No phone, companion build or production check (out of scope; the companion's endpoints only gain optional response fields and its 409 handling covers `purpose_answer_undone`, by code reading).
- Screen readers were not run; semantics were checked with axe and role queries.
- Returning from `/ledger?edit=…&return=review` to the exact item is phase 3 work (D7); Tinjau selects by the D6 rule.
- The capture undo still leaves its `capture` feedback row (existing debt, design Risks).

## Acceptance

- 2026-11-03: CI "Checks" run 41568677747 on `6032613` passed (backend on PostgreSQL 16, frontend, compose and backup). The owner merged PR #63 (merge `6992b4b`) and said "accepted, defaults ok". The change was archived afterwards on `docs/archive-redesign-review-triage`, because the pull request merged before the archive commit.
