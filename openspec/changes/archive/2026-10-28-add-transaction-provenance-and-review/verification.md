# Verification: add-transaction-provenance-and-review

Date: 2026-10-28.

**Revision tested:** branch `feat/transaction-provenance-and-review`, based on `origin/main` 6cb8c9f. The
working tree was uncommitted when tested; the commit that follows contains exactly that tree.

**Data:** synthetic users, accounts, categories, transactions and notification texts only.

## Backend

| Check | Result |
|---|---|
| Disposable `postgres:16`, recreated, migrated V1→V28 by `flyway/flyway:10` | 28 migrations applied |
| Full suite: `PYTHONPATH=backend pytest backend/tests` with `SKIP_TEST_MIGRATIONS=1` | **763 passed, 0 skipped, 0 failed** |
| Of which new | 21 tests: `test_transaction_provenance.py` (9), `test_reimbursements.py` (8), `test_transaction_provenance_migration.py` (1), three in `test_notification_processing_database.py` |
| Existing tests before any new test, with V28 applied and before code changes | 742 passed (the column defaults keep old-style inserts working) |
| Existing tests adjusted | Fake-cursor tests that script every query now pass an empty `SpendingOffsets` or patch `load_spending_offsets`; two direct `create_bilateral_movement` calls pass a provenance |

The new tests exercise columns, fields and endpoints that do not exist on `origin/main`, so they were not
run against the old code.

What the new tests prove:
- **Migration** (scratch schema at V27 with synthetic rows, then V28, then the mirror): every source rule,
  classifier and review-status rule in design D2; every pre-existing column unchanged, including
  `updated_at`; both categories created, an existing "Jajan & Nongkrong" kept, no row moved; the mirror is a
  no-op after an owner review and a category deletion; check constraints reject an unknown source, review
  status and classifier, a confidence of 1.5, a blank merchant, an offset on an expense and an offsetting
  expense category.
- **Write paths:** a static test that every `INSERT INTO transactions` in `backend/app` names `source`,
  `review_status` and `classified_by`; database tests for manual, movement, payroll, recurring, reconcile,
  top-up and trade rows; deterministic, broker, AI (confidence 0.9 stored) and owner-resolved notification
  rows; automatic pairing does not mark the owner.
- **Owner edits:** recategorising an AI row; amount-only edit and a re-sent unchanged form leave provenance;
  review-only PATCH keeps `classified_by = ai`; movement legs accept review-only PATCH and still reject other
  edits; movement notes edit and merge mark both legs.
- **Validation:** `review_status = auto` or unknown → 422; 121-character merchant → 422 with no row written;
  offset on expense or salary → `offset_not_allowed`; other owner's, income, excluded, "Internal Movement"
  and "Investasi" categories → `invalid_offset_category`; recategorising away clears the offset; categories
  used as offsets are archived, not deleted, and keep their kind.
- **Reports** with the spec's numbers: overview, analytics, insights, ledger summary inflow, pulse
  `cycle_spent`, `cycle_reimbursed` and `safe_to_spend_today`, excess, unallocated, and a pillar-less offset
  category reducing `need`.

## Frontend

| Check | Result |
|---|---|
| `npm test` | 37 files, **173 passed** (163 before plus 10 new) |
| `npm run lint` | passed |
| `npm run type-check` | passed |
| `npm run build` | compiled successfully |
| axe (vitest-axe) on the ledger edit dialog with the provenance block, and on quick capture with the offset select | 0 violations |

## Browser check

Headless Brave over DevTools with a throwaway profile, against `next dev` and `uvicorn` on the test
database (AI disabled), with a user seeded through the API plus one synthetic AI-classified row.

Live API figures for the seeded cycle (salary Rp6.000.000; Rp300.000 dinner in a want category, Rp200.000
groceries, Rp45.000 snack in "Jajan & Nongkrong"; Rp200.000 repayment offsetting the dinner category;
Rp80.000 repayment without offset): `total_inflow` 6.000.000, `gross_outflow` 545.000,
`reimbursement_offset` 200.000, `total_outflow` 345.000, `reimbursement_unallocated` 80.000, `want_spent`
145.000. All match hand calculation.

| Width | Theme | Views | Result |
|---|---|---|---|
| 390 px | light, dark | Ledger edit dialog (AI row, repayment row), quick capture income | No page or dialog horizontal overflow; provenance reads "Notifikasi bank / Otomatis, belum ditinjau / AI"; repayment shows "Mengurangi pengeluaran: Makan Bareng" |
| 1280 px | light, dark | Same | Same results; the three provenance fields sit in one row |

- The `dark` class followed the emulated `prefers-color-scheme`.
- Console errors: 0.
- **Keyboard** at 390 px, light:
  - Tab reaches "Tandai sudah ditinjau" (44 px), then the existing controls, then "Merchant / tempat
    (opsional)" (44 px), then "Catatan";
  - Enter on "Tandai sudah ditinjau" changes the status to "Sudah ditinjau" and removes the button;
  - Tab from "Kategori" reaches "Mengurangi pengeluaran" (44 px) in both the ledger dialog and quick
    capture, with `:focus-visible` and a visible outline or ring.
- Quick capture offers only reportable expense categories ("Internal Movement" and "Investasi" are not
  offered).

## OpenSpec and diff

| Check | Result |
|---|---|
| `openspec validate add-transaction-provenance-and-review` | valid, also with `--strict` |
| `openspec validate --all` before archive | 45 passed, 0 failed |
| `openspec validate --all` after archive | 46 passed, 0 failed |
| `openspec validate --all --strict` | 41 passed, 5 failed: the main specs `account-reconciliation`, `clean-core-ledger`, `kakeibo-decoupling`, `kakeibo-reconciliation` and `notification-filtering` each have one requirement over 500 characters. The same 5 warnings occur on `origin/main` 6cb8c9f without this change (39 passed, 5 failed of 44) |
| `git diff --check` | clean |

## Not verified, and why

- **Production:** not touched. The post-merge checks are listed in the design's migration plan.
- **Upgrade on a real production copy:** the backfill was proven on synthetic rows built at V27; no
  production data was read.
- **Screen reader:** not run. Names and roles were checked with Testing Library queries and axe.
- **200% text resize:** not tested separately; reflow was checked at 390 px.

## Follow-ups

- Existing edit-dialog controls ("Bayar dari", "Kategori", "Kaitkan ke target", "Catatan") are 38–42 px
  high, below the 44 px used by the new controls. They predate this change.
- New users' seeded "Internal Movement" categories are not flagged `is_excluded_from_budget` until the first
  movement; offsets and reports exclude them by name. Seeding the flag is a phase 8 cleanup candidate.
- Daily series, heatmap, burn rates, runway and `today_spent` stay gross; the ledger summary outflow stays
  gross (design D7). Phase 6 reworks these reports.
- The column defaults `manual`/`reviewed` exist for rollback and can be dropped in phase 8.
- Five main specs carry one over-long requirement each (strict warnings above). Shortening them needs an
  owner-approved spec-only change, so this change does not reshape them.
