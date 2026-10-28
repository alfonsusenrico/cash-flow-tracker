## 1. Schema, backfill and categories

- [x] 1.1 Add `db/migrations/V28__transaction_provenance_and_reimbursements.sql` and its identical mirror `backend/app/db/transaction_provenance_and_reimbursements.sql`, run last by `init_db.py` (design D1, D2, D6, D8, D9).
  - Evidence: a migration test that builds a scratch schema at V27 with synthetic rows for every backfill rule, applies V28 and asserts:
    - each source, classifier and review status;
    - `updated_at` and money fields unchanged;
    - both categories created, an existing "Jajan & Nongkrong" kept;
    - re-running the mirror changes nothing;
    - the check constraints reject an unknown source, a confidence of 1.5, an offset on an expense and an offsetting expense category.
- [x] 1.2 Add both categories to `DEFAULT_CATEGORIES` with their pillar and flag, and write them in `seed_user_defaults`.
  - Evidence: a registration test shows "Jajan & Nongkrong" (want) and "Patungan & Ganti Uang" (offsets spending).

Expected Result: every row has provenance and every user has the two categories, on a freshly migrated database and after an upgrade.

## 2. Provenance on every write path

- [x] 2.1 Add `services/provenance.py` (vocabularies, `Provenance`, owner-edit SQL, `normalize_merchant`) and make `create_bilateral_movement` require a provenance (D3, D5).
  - Evidence: unit tests for the vocabulary validation and merchant normalisation; a static test that every `INSERT INTO transactions` in `backend/app` names `source`, `review_status` and `classified_by`.
- [x] 2.2 Pass provenance from manual create, trades, movements, recurring occurrences, payroll, reconcile, top-ups and conversions, and the notification application (AI, deterministic, broker, owner resolution).
  - Evidence: database tests for each path in the D3 table, including AI confidence stored and rule confidence empty.
- [x] 2.3 Owner-edit tracking for transaction PATCH/PUT, movement edit, merge, top-up conversion and top-up notes edit; no tracking for automatic pairing (D4).
  - Evidence: tests: recategorise an AI row; amount-only edit leaves provenance; movement notes edit marks both legs; automatic pairing does not mark owner.
- [x] 2.4 Review-only PATCH, `review_status` and `merchant` on create and update, response fields, ledger filters `source` and `review_status`, merchant search (D4, D5).
  - Evidence: tests: mark reviewed keeps `classified_by = ai`; movement leg marked reviewed; `auto` and unknown filter values return 422; 121-character merchant returns 422; filters and search return the expected rows.

Expected Result: no row can be written without provenance, and the owner's actions are recorded as owner classification.

## 3. Reimbursements

- [x] 3.1 Offset link validation on create and update; category API `offsets_spending`, kind-change and delete protection (D6).
  - Evidence: tests: offset on expense → 422 `offset_not_allowed`; offset on salary → 422; other user's category, income category and excluded category → 422 `invalid_offset_category`; recategorising away clears the offset; deleting a used offset category archives it; kind change → 409.
- [x] 3.2 `services/reimbursements.py` and its use in dashboard overview and analytics, Kakeibo, pulse, insights and the ledger summary (D7).
  - Evidence: tests with the spec's numbers: overview totals, categories and Kakeibo; analytics totals and variance; excess; unallocated; pulse `cycle_spent`, `cycle_reimbursed` and `safe_to_spend_today`; ledger summary inflow.

Expected Result: a friend's repayment never counts as income and lowers only the spending it repays.

## 4. Frontend

- [x] 4.1 `lib/provenance.ts` labels; ledger edit dialog provenance summary, "Tandai sudah ditinjau", merchant field and offset select; quick capture offset select (D10).
  - Evidence: vitest tests for the labels, the dialog (labels shown, review-only request, merchant and offset in the save payload, select hidden for ordinary income) and quick capture (select shown only for an offsetting category, payload). axe on the dialog finds no violations.
- [x] 4.2 Browser check of the new controls at 390 px and 1280 px in light and dark themes, with keyboard focus through merchant, offset select and the review button, and no console errors.
  - Evidence: recorded in `verification.md`.

Expected Result: the owner can see where a row came from, name its merchant, confirm it, and file a repayment against the spending it repays.

## 5. Verification and delivery

- [x] 5.1 Run the full backend suite on a freshly recreated disposable PostgreSQL migrated V1→V28 with `SKIP_TEST_MIGRATIONS=1`; run `npm test`, `npm run lint`, `npm run type-check` and `npm run build`.
  - Evidence: pass and skip counts in `verification.md`.
- [x] 5.2 Run `openspec validate add-transaction-provenance-and-review`, `openspec validate --all` and both with `--strict`; run `git diff --check`.
  - Evidence: zero errors; strict warnings recorded as follow-ups.
- [x] 5.3 Write `verification.md` with revision, commands, counts, synthetic-data note, browser matrix and what was not verified.
  - Evidence: the file is complete.
- [x] 5.4 Archive the change, re-run `openspec validate --all`, commit, push `feat/transaction-provenance-and-review` and report the compare link; record the post-merge production checks in `PROJECT_STATE.md`.
  - Evidence: archive exit status 0, validation output, pushed branch URL.
