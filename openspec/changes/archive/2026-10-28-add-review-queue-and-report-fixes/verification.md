# Verification: add-review-queue-and-report-fixes

Date: 2026-10-28. Branch `feat/review-queue-and-reports` from f55dd46 (phase 5, now merged into main).
All checks ran locally on the working tree before the commit. Synthetic data only. No production access.

## Backend

- Disposable PostgreSQL 16 (`cft-p6-db`, loopback port 55506), `ledger_test` dropped and recreated,
  Flyway 10 applied V1→V30: "Successfully applied 30 migrations … now at version v30". No migration in
  this change.
- `PYTHONPATH=backend TEST_DATABASE_URL=… SKIP_TEST_MIGRATIONS=1 pytest backend/tests -q -rs`:
  **874 passed, 0 skipped, 0 failed** (exit 0). Baseline on the base was 842 passed.
- New: `tests/test_review_queue.py` (21 tests incl. 9 parametrised invalid bodies) and
  `tests/test_report_figures.py` (11 tests incl. 4 month-window cases).
- Changed existing tests, each for an intended contract change:
  - `test_reimbursements.py::test_analytics_insights_and_ledger_follow_the_same_figures`: ledger cycle
    outflow 500.000 → 300.000 net, with `gross_outflow` 500.000 (design D9, phase 2 open decision 6).
  - `test_kakeibo_and_insights.py` (two mocked-cursor tests) and `test_personal_finance_os.py`
    (one mocked cursor) and `test_transactions.py` (one mocked cursor): fixtures follow the new query
    order (breakdown helper, per-window burn queries, offsets lookup); assertions unchanged.

## Frontend

- `npm test`: **41 files, 195 tests passed** (exit 0); baseline on the base was 173.
  New: `app/review/page.test.tsx` (7, axe on full and empty page), ledger "filters from the URL" (3, axe),
  insights "report drill-down" (2, axe), `ReviewNavBadge.test.tsx` (2), `lib/ledgerFilters.test.ts` (4),
  `lib/review.test.ts` (4).
- `npm run lint`: exit 0. `npm run type-check`: exit 0. `npm run build`: exit 0 (`/review` prerendered;
  the ledger reads the URL inside a suspense boundary).

## Browser (headless Brave over CDP)

Setup: uvicorn on 127.0.0.1:8016 against a separate `ledger_browser` database (V1→V30), AI disabled;
`next dev` on 127.0.0.1:3016. Seed (`seed_review.py`, scratch folder) through the public API: one
merchant-less myBCA payment with an open question, two "Kedai Awan" payments, a flagged manual row, a fee
transfer (302.500 → 300.000 + 2.500 fee), a one-sided GoPay send, two GoPay sends competing for one BCA
credit, a reimbursement offsetting "Makan Bareng", spending in a category archived afterwards and a row in
the previous month. Queue counts after seeding: 1 question, 1 needs review, 5 automatic in 4 groups,
1 awaiting, 3 ambiguous.

Final run (`browser-run2`, after the last code change):

| Page | 390 light | 1280 light | 390 dark | 1280 dark |
|---|---|---|---|---|
| `/review` (4 sections rendered) | no overflow | no overflow | no overflow | no overflow |
| `/ledger?period=month&type=expense` (label "Oktober 2026", 10 review marks) | no overflow | no overflow | no overflow | no overflow |
| `/insights` month view (drill links incl. "Tanpa kategori") | no overflow | no overflow | no overflow | no overflow |

- Keyboard: from the top of `/review`, 17 Tab presses reach "Terima semua (1)"; focus is visible
  (`:focus-visible`, 2 px outline, coral); Enter accepts the group and the live region reports
  "Belanja pasar pagi: 1 transaksi ditandai sudah ditinjau." "Ubah kategori semua" opens by Enter and Tab
  moves Ganti nama → Kategori → Pilar Kakeibo. Typing in "Jawab sendiri" and Enter answers the question
  (question cards 1 → 0, status message shown).
- Deep link `/ledger?edit=<id>&return=review` opens "Ubah Transaksi"; Escape returns to `/review`.
- Badge: sidebar "Tinjau 5" with the words ", 5 perlu ditinjau"; phone menu button
  "Buka menu, 5 transaksi perlu ditinjau".
- Console errors: **0** across all page loads.
- Live data check after the run: month-view `category_variance` sum = `total_outflow` = Σ daily expense
  = Rp 1.146.000, with "Tanpa kategori" and the archived "Langganan Lama" listed; the paired movement group
  shows no stale `awaiting_counterpart` reason.

axe-core 4 (WCAG 2.0/2.1/2.2 A and AA rules) on the rendered pages: every reported node was attributed in a
node-level dump (`axe-run1/axe-nodes.txt`); **none is in an element added or changed by this change**. All
are pre-existing styles the owner asked not to restyle (design open decision 12):
- white text on the coral active navigation pill and the "Catat" button (3.6:1 light, 3.33:1 dark);
- bottom dock inactive labels (2.2–2.6:1, phone, light);
- emerald and rose amount colours on light surfaces (ledger summary ribbon, mobile feed amounts, insights
  burn tiles and badges, the "Tambah Kategori" and "Catat Transaksi" emerald buttons, type pills);
- the avatar initials chip and the pre-existing desktop "Hapus filter" link.
The review page content, the period selector, active filter chips, review marks, fee notes and drill
links have no findings in either theme.

Not done: a screen-reader pass (no assistive technology run), 200 % text and text-spacing overrides,
320 px width. This is not a conformance audit.

## OpenSpec

- `openspec validate add-review-queue-and-report-fixes`: valid (exit 0).
- `openspec validate --all`: 51 passed, 0 failed (exit 0).
- `--strict`: exit 1 with 6 warnings, all "Requirement text is very long (>500 characters)" on the new
  requirements. Recorded as a follow-up (split the requirements when they are next touched); specs were
  not reshaped only to silence warnings.

## Follow-ups

1. Pulse `today_spent`, the pulse insights histogram and the emergency-fund runway still use gross
   spending.
2. Pre-existing contrast findings listed above.
3. A confirmation feedback kind (migration) if phase 9 needs explicit "accepted as is" examples.
4. Fee-tolerant manual linking from the review page (merge requires equal amounts).
5. Split the long requirements flagged by `--strict`.
