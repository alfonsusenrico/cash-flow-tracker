# Verification

All checks ran locally on 2026-11-02 against the working tree of `feat/review-clarity-and-topup-learning` (from `a0e4613`), with synthetic data only. No production system, phone or model provider was used.

## Backend

- Fresh `postgres:16` container, Flyway 10 migrate V1→V43 as in `checks.yml`: "Successfully applied 43 migrations", schema version 43.
- `python -m pytest tests/ -ra` with `SKIP_TEST_MIGRATIONS=1` and `REQUIRE_TEST_DATABASE=1`: **1540 passed**, 0 failed, 0 skipped (baseline on `a0e4613`: 1513 passed). New: 25 in `test_topup_learning.py`, 2 in `test_review_queue.py` (one replaced), 1 in `test_investment_amount_conversion.py`.
- Mutation checks on `topup_learning.py`, each restored afterwards: removing the owner filter of the evidence query, counting learner-recorded top-ups as evidence, and ignoring rejections in the pending set each made exactly one test fail.

## Frontend

- `npm ci`, `npm run type-check` (0 errors), `npm run lint` (0 errors, 1 warning that also exists on `a0e4613`: unused `_done` in `app/review/page.tsx`), `npm test` (**61 files, 397 tests passed**), `npm run build` (compiled successfully).

## Browser

Headless Brave over CDP (port 9462) against `next dev` (3162) and the API (8162) on a disposable `ledger_browser` database seeded with "RDN Contoh", "GoPay Contoh", "Bibit Contoh" with "Reksa Dana A", "Reksa Dana B" and the unit-tracked "Reksa Dana Unit", a converted pair last month, two equal debits this month, one GoPay debit, an AI-finished movement stored as `auto`, two automatic and one flagged merchant rows. At 390×844 and 1280×900, light and dark (4 runs):

- Header "4 perlu jawaban · 2 dicatat otomatis (opsional)", equal to `GET /api/review/summary` (attention 4, unreviewed 2); the finished movement appears nowhere.
- Sections "Perlu jawaban (1)", "Pindah saldo menunggu pasangan (3)", "Dicatat otomatis (2)" (closed; opened with Enter from the keyboard).
- The pair shows "Sepertinya top up ke Reksa Dana A, kayak bulan lalu?" and "… Reksa Dana B …", one per debit with the explanation; the GoPay debit offers "Ini top up investasi" with "Bibit Contoh · Reksa Dana A/B" and explains "Reksa Dana Unit".
- Tab reaches "Ini top up investasi", "Ya, top up", "Pilih produk lain", "Bukan top up" and the optional section.
- Ledger dialog of an RDN debit: "Jadikan Top up Investasi" at 567–611 px of 844 (390 px) and 357–401 of 900 (1280 px), above the amount field, without any scrolling.
- Horizontal overflow 0 px, 0 console errors, axe (WCAG 2.0/2.1/2.2 A and AA) 0 violations on Tinjauan closed, Tinjauan with the section and product form open, and the ledger dialog.

## Specification and diff

- `openspec validate --all --strict`: 70 passed, 0 failed.
- Every scenario name of the 8 MODIFIED requirements is kept (script comparison against `openspec/specs`).
- `git diff --check` clean; `docker compose config --quiet` (default and backup profile) passed.

## Not verified

- No phone or companion run: the companion does not call the changed endpoints, and ingestion was exercised through `POST /api/ingest/notifications` in tests.
- Real production data and the October conversions themselves; the backfill is proven on synthetic conversions of the same shape.

## After archiving

`openspec archive` applied the deltas (7 added, 8 modified). Strict validation then flagged five merged requirements as longer than 500 characters and the new spec's placeholder purpose. In the main specs, the additions of this change were moved into five requirements of their own ("Finished Movements Are Not Counted", "Tinjau Sections by Need", "Top-up From an Awaiting Debit", "Undoing an automatic top-up", "Conversion placement and eligibility data") with their scenarios, and the purpose was written. No scenario was removed or renamed. `openspec validate --all --strict`: 70 passed, 0 failed.
