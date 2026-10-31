# Tasks

## 1. Name-based entity resolution (C1, D1–D3)

- [x] 1.1 Inventory every caller that sends `account_name`, `target_account_name`, `source_account_name` or `category_name`: web, Android companion main 4a11eb0 (read-only), text capture, notification services and the retired Telegram bot. Evidence: caller table in design.md D3; no caller depends on fuzzy matching or auto-creation, so no Android follow-up.
- [x] 1.2 Add `services/entity_names.py` (exact, owner-scoped, non-creating, pocket preference, exact instrument symbol, category kind, coded 422 with up to 5 candidates). Use it in `POST /api/transactions` and `POST /api/movements` and remove the fuzzy resolvers. Evidence: `test_entity_name_resolution.py` unit cases (exact, case and whitespace, pocket vs parent, ambiguous pockets and top-level accounts, unknown with candidates and limit, blank, symbol, category kind, unknown or ambiguous category).
- [x] 1.3 Verify through the API on PostgreSQL. Evidence: `test_entity_name_resolution.py` DB cases cover exact, case and whitespace, pocket preferred, parent routed to its default pocket, ambiguous, not found with candidates, a retired synonym ("Emergency Fund"), an archived account and another owner's account (never matched or listed), no account, category or transaction created, category kind mismatch (400), and movements by name, ambiguous, unknown or missing target. Updated `test_record_investment_buy_rejects_unknown_stock_ticker` to the 422 contract.
- [x] 1.4 Update the README API note. Evidence: README "API Reference Highlights".

Expected Result: a name resolves to exactly one owned, active entity or is answered with 422 and candidates, and nothing is ever created by a name.

## 2. Spec corrections (C2, C3)

- [x] 2.1 REMOVED delta for `notification-filtering` "Discard Non-Financial Notifications at Listener Entrance" with Reason and Migration to the companion's `notification-capture`. Evidence: `openspec validate --all --strict`. The before/after name comparison shows only this requirement and its two scenarios gone.
- [x] 2.2 MODIFIED delta for `tactile-pulse-ui` "Anti-AI-Slop Visual Design Standard" naming Plus Jakarta Sans and Geist Mono, self-hosted, with both scenario names kept. Evidence: `delta_compare.py` reports no missing scenarios; `layout.tsx` uses `next/font/local` with those files.

Expected Result: main specs describe only what this repository implements, with the fonts it ships.

## 3. Reconciliation flag (C4, D6)

- [x] 3.1 `ReconciliationFlag` (icon and text) on flagged account cards and pocket rows, a parent summary of flagged pockets, and a button to "Sesuaikan Saldo" where eligible, on desktop and mobile. Evidence: `accounts/page.test.tsx` "flags accounts and pockets…" (both viewports, no label on unflagged items, axe on the cards, keyboard Enter opens the dialog, flag gone after reconcile) and "shows a flagged amount-tracked product…" (label without action).
- [x] 3.2 Headless Brave check at 390 and 1280 px in light and dark: flags visible, no horizontal overflow, real-browser axe including colour contrast, Tab and Enter open the modal, Escape returns focus, console errors. Evidence: verification.md.

Expected Result: the owner sees which accounts need reconciling and reaches the fix in one step, by pointer or keyboard.

## 4. Owner name (C6, D7)

- [x] 4.1 Decide each occurrence (code-relied or example) and record it. Evidence: table in design.md D7.
- [x] 4.2 Replace examples with "Raka Purnama". Derive the greeting from the username with no built-in fallback. Evidence: `git grep -i -e alfonsus -e enrico -e soebij` on non-archived tracked files leaves only the commit identity in AGENTS.md, URLs and handle, and `/Users/enrico` evidence paths. `utils.test.ts` "greeting name". The pairing, parser, ingest and processing tests pass with synthetic names.

Expected Result: no real owner name in current code, tests, UI copy or specs, and self-transfer recognition still comes from profile data.

## 5. Flaky test (C7, D8, D9)

- [x] 5.1 Reproduce: 50 isolated runs, 5 full runs on a baseline export, and a widened-window experiment. Evidence: hypotheses ledger in design.md, with logs in the scratch area summarised in verification.md.
- [x] 5.2 Add the `BACKGROUND_JOBS_ENABLED` switch, turn it off in `conftest.py`, and document it in `.env.example`. Evidence: `test_disabled_background_jobs_start_no_loop`, `test_background_jobs_are_enabled_unless_switched_off`; the lifespan test pins the switch on; the widened-window experiment passes after the fix; 5 full runs and 50 isolated runs pass.
- [x] 5.3 Make `test_comparison_endpoint_returns_its_windows` calendar-independent. Evidence: it failed in 5 of 5 baseline runs on 2026-10-31 and passes after the change.

Expected Result: the backend suite has no background-worker race and no calendar-day dependence in this test.

## 6. Verification and delivery

- [x] 6.1 Backend on a fresh PostgreSQL 16 migrated V1→V38 with flyway/flyway:10, plus a CI-style run without a database. Evidence: counts in verification.md.
- [x] 6.2 Frontend `npm ci`, test, lint, type-check, build. Evidence: counts in verification.md.
- [x] 6.3 `openspec validate --all` and `--strict`, `git diff --check`, before/after requirement and scenario names, and a final diff review. Evidence: verification.md.
- [x] 6.4 Commit, archive in the same branch, and push `fix/closeout-follow-ups` without opening a pull request. Evidence: `git log` and the pushed branch.

Expected Result: verified, archived and pushed, ready for owner review.
