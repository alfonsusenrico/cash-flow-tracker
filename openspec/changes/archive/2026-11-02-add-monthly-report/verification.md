# Verification — 2026-11-02

Branch `feat/monthly-report` was cut from origin/main 5004b11. All checks ran locally with synthetic data. No production host, no phone and no real AI call was used. The Android companion was only read (`financial-tracker-mobile-listener` main 35dfbeb).

## Baseline (5004b11, before any change)

- Backend on a fresh PostgreSQL 16 database migrated V1→V39 by `flyway/flyway:10`: **1228 passed, 1 failed**. The failure is `test_plan_auto_link.py::test_one_certain_payment_is_linked_once_and_applies_the_debt_once`. It is date-dependent (a monthly plan due 2 days before 2 November) and also fails when run alone on a fresh database. This change does not touch it.
- CI-style run without a database: **663 passed, 566 skipped**.
- Frontend `npm ci` exit 0. `vitest`: **56 files, 308 tests** passed.

## Backend (working tree at the final commit)

- A freshly recreated database was migrated by `flyway/flyway:10`: "Successfully applied 40 migrations … now at version v40".
- Full suite (`SKIP_TEST_MIGRATIONS=1` after that Flyway run): **1246 passed, 1 failed**. The failure is the same baseline `test_plan_auto_link` failure. That is +18 tests:
  - `test_monthly_report.py` (11):
    - the four answers;
    - report = ledger reconciliation for flow, every pillar, the top categories and the income sources;
    - the period list with a partial first month;
    - 409/404/422 paths;
    - the partial previous month and the median without history;
    - the median of three full months over the same first days;
    - data quality (2 duplicates with a movement leg excluded, review counts, unclassified, days without records, the `possible_duplicates` ledger filter);
    - reflection CRUD with owner isolation, blank, too-long and unknown-period rejections, and the previous note shown again;
    - budget hidden, then shown per category and overall;
    - salary-cycle basis with closed cycles only;
    - latest readiness, where the Bearer response equals the session response and an unknown key gets 401;
    - the first month not ready and an empty ledger.
  - `test_kakeibo_completeness.py` (7):
    - a missing pillar is `unclassified`, not `need`, with a `null` row pillar, the ledger filter sum, 422 for an unknown pillar, and `null` in categories and `top_categories`;
    - six buckets add up, with a reimbursement apportioned to the unclassified share;
    - the essential cost keeps unclassified spending and reports `unclassified_amount`;
    - a row in a category without a pillar stays unclassified, while an explicit category pillar is still copied;
    - the category fix writes one `correction` feedback row per changed row (before `null`, after `need`), rows with their own pillar are unchanged, a repeat writes nothing, and income or `unclassified` gets 422;
    - the row fix sets 3 rows as owner edits with feedback, is all or nothing on income (422), and gives 409 on a movement leg;
    - owner scope (404 for another owner's category or rows, the gaps list is scoped, 422 for an incomplete window).
- Four existing tests changed their expectation from "no pillar counts as `need`" to the unclassified bucket. Each keeps its scenario:
  - `test_automation_correctness_database.py`;
  - `test_cash_flow_top_categories.py`;
  - `test_reimbursements.py` (renamed `..._reduces_unclassified`);
  - `test_transaction_provenance.py`, where the edit form re-sends `null` for an unclassified row.
- CI-style run without a database (`cd backend && python -m pytest tests/` with an unreachable `TEST_DATABASE_URL`): **663 passed, 584 skipped**, exit 0.
- Not run: the new tests against the 5004b11 code. The routes they call do not exist there.

## Frontend

- `npm ci` exit 0.
- `npm test`: **57 files, 323 tests** passed. That is +1 file and +15 tests:
  - `/laporan` page (8, with axe on the page and on both dialogs);
  - Kakeibo sixth bucket (1, axe);
  - Analisis without budgets and the unclassified category form (2);
  - ledger edit of an unclassified row (1);
  - essential-cost unclassified share (1, axe);
  - filters (1) and the kakeibo bucket (1).
- `npm run lint`: 0 errors, 1 warning. The warning is the existing `_done` in `app/review/page.tsx`, also present at baseline.
- `npm run type-check`: exit 0.
- `npm run build`: exit 0, with `/laporan` listed as a static route.

## Browser check (headless Brave over CDP)

- **Setup**:
  - a local API on 127.0.0.1:55672 with background jobs and AI off;
  - `next dev` on 127.0.0.1:55673;
  - the disposable `ledger_browser` database migrated V1→V40;
  - `seed_report.py`, which seeds a synthetic owner (`laporan_contoh`) with transactions from 15 September, a full October, unclassified rows, a needs-review row and a duplicate pair.
- Scripts and outputs are in the session scratch folder `scratchpad/monthly-report/` (`browser_check.mjs`, `evidence/browser-check.json`, screenshots `laporan-{390,1280}-{light,dark}.png`, `laporan-320-light.png`, `laporan-390-light-classify-dialog.png`). They are not tracked in Git.
- `/laporan` (default: Oktober 2026) at 390 and 1280 px in light and dark, plus 320 px light:
  - horizontal overflow was **0 px** in every variant;
  - console errors and exceptions: **0**;
  - the `dark` class was applied in both dark runs.
- **axe 4.11** (WCAG 2.0/2.1/2.2 A and AA tags):
  - 390 dark, 1280 light and 1280 dark had **0 violations**.
  - 390 light and 320 light each had **1 rule, `color-contrast`, on 4 nodes**: the inactive labels of the shared phone dock (`BottomNav`). They are `--muted` text on the always-dark `.glass-dock`. This branch did not change that markup. By hand, `#4E5D70` on the dock's `rgba(23, 26, 33, 0.94)` is about 2.4:1. On `/ledger` and `/insights` at 390 px light, axe listed other `color-contrast` nodes (9 and 2) but not the dock labels, probably because it could not resolve the background behind them there. The finding is recorded in design.md Follow-ups and was not fixed in this change.
- **Keyboard** (390 px, light, 80 Tab presses):
  - every report stop had a visible focus indicator;
  - the only exceptions were the 2 stops on the Next.js dev overlay (`NEXTJS-PORTAL`), which exists only in development;
  - the basis switch, period select, "Lengkapi klasifikasi" and the reflection textarea were all reached;
  - Enter on "Lengkapi klasifikasi" opened the dialog with focus inside, and the dialog had 0 axe violations;
  - Escape closed it, and focus returned to the trigger.
- **Visual review of the screenshots**:
  - the four questions are in order, and on the phone "Kualitas data" comes first;
  - the comparison says September was recorded only from 15 September;
  - the median says three full periods are needed;
  - the budget panel offers "Atur anggaran" without figures;
  - the global month stepper is hidden on `/laporan`.
- This is not a conformance audit. No screen reader was used. 200% text and WCAG text-spacing overrides were not checked.

## Android compatibility (read only)

The companion was read at main 35dfbeb:
- `ApiModels.shownTopCategories()` rejects a non-null `kakeibo_type` outside the five pillars and accepts `null`;
- `pillarLabel(null)` and `CapturePresentation.kakeiboLabel(null)` return `null`.

The server sends `null`, never `unclassified`, on every row and in `top_categories`. The phone therefore keeps its tile and shows no pillar for such entries. No build or device run was done.

## Repository

- `git diff --check`: exit 0.
- `openspec validate add-monthly-report --strict` passed before the archive. After the archive, `openspec validate --all --strict` gives **65 passed, 0 failed**. Archiving surfaced four modified requirements over the 500-character limit and two placeholder purposes. They were shortened (their scenarios carry the detail) and written in the main specs and the archived deltas.
