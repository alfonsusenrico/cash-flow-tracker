# Verification: compare-whole-months

Each entry names the commit or working-tree state it was measured on. Data is synthetic only: the preview seed shaped like the owner's account (`src/seed_owner.py` of the 2026-11-04 preview), extended in the scratchpad with tracking from 1 Juni and salary cycles from 30 Juni, so Juli to Oktober are whole months and November runs. Each seeded cycle has its own random spending; rows dated after the run were removed (8). Backend tests use their own synthetic owners.

## 1. Rebase and baseline (tasks 1.1–1.2)

- **Rebase onto the merged `refine-workspace-layout`: pending.** On 2026-11-04 `origin/main` is still `2f66187`; the layout change has not merged. The branch is built on `2f66187`, and the integration points are listed under "Not verified".
- **Baseline at `2f66187`** (a detached build of main in the scratchpad, on the same database and seed):
  - `GET /api/reports/monthly`, median of 20: Oktober 20.3 ms, November 17.6 ms.
  - First-load JS: `/laporan` 240.0 kB gzip, `/insights` 249.1 kB gzip (CSS 20.2 kB).
  - Screenshots of Laporan Oktober, Laporan November and Ringkasan at 1440 dark, 1440 light and 390 dark: `/private/tmp/claude-501/-Users-enrico-enrico-project-enrico-cash-flow-tracker/148d8a0e-cf25-4ec5-9de4-a0f135ad4203/scratchpad/web-months/shots/baseline/`.
  - Backend suite on main: 1709 passed (phase 5 CI); this branch adds 12 tests.

## 2. Backend (tasks 2.1–2.6)

- **Window tests** (`tests/test_monthly_comparison.py`, PostgreSQL 16 migrated by Flyway V1–V44, `REQUIRE_TEST_DATABASE=1`):
  - `test_baseline_is_the_average_of_the_three_whole_months_before`: the owner's example, Juli Rp 3.810.000, Agustus Rp 3.710.000, September Rp 3.218.500, Oktober Rp 4.802.000. Baseline Rp 3.579.500 over Juli–September, change Rp 1.222.500, previous change Rp 1.583.500, history Juli–November with the running month last.
  - `test_running_month_shows_progress_and_no_verdict`: share of Oktober, every `change` and `baseline_change` null, facts limited to top merchant, recurring and unexpected.
  - `test_two_usable_months_give_a_two_month_average_and_fewer_give_none`.
  - `test_comparison_marks_a_partial_previous_month_and_has_no_baseline_yet` (in `test_monthly_report.py`): `partial: true`, `tracking_from`, no verdict.
  - `test_salary_cycles_compare_closed_cycles_and_show_progress_for_the_open_one`.
  - `test_a_new_category_counts_as_zero_in_the_previous_month`.
  - `test_the_last_local_evening_belongs_to_its_month`: 23.30 on the last local day stays in that month.
  - `test_rounding_is_half_up_and_shares_are_whole_percent`.
- **Ledger equality** (`test_every_figure_equals_the_ledger_summary`): every pillar and every category row equals `GET /api/transactions` for its window and filters, including a reimbursed category split over two pillars; every history period's Pengeluaran equals the sum of the ledger's spending pillars. This needed the ledger to net a filter of both category and pillar by that pair's reimbursement share (OD110).
- **Facts** (`test_category_facts_respect_the_threshold_edge_and_new_categories`): Rp 49.999 above the average is not a fact, Rp 50.000 is; a new category from Rp 100.000, not at Rp 99.999; each category fact's amount equals its ledger reference; at most seven facts; the Keinginan share.
- **Route, scope and unchanged endpoints** (`test_route_is_owner_scoped_and_validates_the_period`, `test_report_comparison_is_the_comparison_route`): another owner's spending never appears; 404 `period_not_found`; 409 `cycle_unavailable`; 401 without a session; the report's `comparison` equals the route's. `/reports/monthly/latest` and `/dashboard/period-comparison` are untouched and their existing tests pass unchanged.
- **Speed and statement count** (`0afe48c`, same seed, median of 20):

  | Route | Main `2f66187` | This branch | Gate |
  | --- | --- | --- | --- |
  | `GET /api/reports/monthly` Oktober | 20.3 ms | 20.7 ms (+2%) | ≤ +10% |
  | `GET /api/reports/monthly` November | 17.6 ms | 18.6 ms (+6%) | ≤ +10% |
  | `GET /api/reports/monthly/comparison` Oktober | — | 11.3 ms | < 150 ms |
  | `GET /api/reports/monthly/comparison` November | — | 11.6 ms | < 150 ms |

  The first measurement missed the report gate (+19% and +21%), because the comparison loaded the period's recurring charges and shops again; `c6ece86` passes the report's own. `test_statement_count_does_not_grow_with_rows`: the same statement count for 1 and 200 rows per period.
- **Full suite** at `0afe48c`: 1721 passed, 0 failed, 0 skipped.

## 3. Laporan (task 3.1)

`app/laporan/page.test.tsx` (12 tests): whole-month KPIs with "vs Sep" and "vs rata-rata 3 bulan"; the pillar table in order with ledger links; facts with links; the history with the running month; no written lead; the running month with progress only and no ▲/▼ or verdict word; "belum lengkap" and "Rata-rata muncul setelah 2 bulan lengkap"; data quality as one line with its fixes and the green complete line; the reflection hidden without a note and shown read-only with "Ubah"; the stepper reaching the running month; one column in reading order; budget, classification and empty states; axe.

## 4. Analisis Ringkasan and docs (tasks 4.1–4.2)

- `components/analysis/analysis.test.tsx`: the progress card per pillar without a verdict; the last completed period against its average as amounts with ▲/▼; the no-baseline sentence; axe.
- `app/insights/page.test.tsx`: Ringkasan requests the comparison of the last completed month and never `/dashboard/period-comparison`; Pengeluaran's explanation; the compact cards.
- Docs: `docs/NEW_USER_GUIDE.md` (Analisis, Laporan) and AGENTS.md (the whole-period rule), commit `0d7f7c9`.

## 5. Verification (tasks 5.1–5.7)

- **Frontend CI-equivalent run** at `0afe48c`: `npm ci`, `npm run type-check` 0, `npm run lint` 0 errors and 0 warnings, `npm test` 77 files, 710 tests, 0 unhandled errors, `npm run build` 0.
- **Bundle gate:** `/laporan` 240.0 → 242.4 kB gzip (+2.4), `/insights` 249.1 → 250.5 kB (+1.4), CSS 20.2 → 21.3 kB. Gate: +8 kB.
- **Preview match:** `/private/tmp/claude-501/-Users-enrico-enrico-project-enrico-cash-flow-tracker/148d8a0e-cf25-4ec5-9de4-a0f135ad4203/scratchpad/web-months/compare.html` puts the approved preview, this build and main side by side for Laporan Oktober, Laporan November and Ringkasan at 1440 dark, 1440 light and 390 dark. Differences and reasons:
  - colours, sidebar, separate KPI cards and the removed Tanya belong to `refine-workspace-layout`, so these screens use main's look until it merges;
  - KPIs use the existing `KpiStrip`;
  - the running month has no "Hari ke-x dari y" card (D4 rejects pace hints);
  - new categories read "{nama}: kategori baru" (OD91);
  - "Rincian pengeluaran" keeps the old report's unexpected items, recurring charges and essential cost, folded;
  - sample figures differ from the preview, whose Juli–September were typed by hand.
  Fixed after reading the screens: pillar-row header cells, a cut-off "vs rata-rata 3 bulan" chip, the uneven Laporan columns, Ringkasan's empty right column, the cut-off progress line at 390 and the facts' place on a phone.
- **Browser matrix** (`/private/tmp/claude-501/-Users-enrico-enrico-project-enrico-cash-flow-tracker/148d8a0e-cf25-4ec5-9de4-a0f135ad4203/scratchpad/web-months/shots/matrix/`, production build of `0afe48c`'s UI): `/laporan` Oktober, `/laporan` November and `/insights?tab=ringkasan` at 1440, 1280, 1024, 768 and 390 px in light and dark, plus 200% and 400% zoom at 1280: 34 cells, axe serious or critical 0, horizontal overflow 0, console errors 0, controls under 24 px 0 (one habits link fixed in `ac2c508`).
- **Real-UI checks** (`/private/tmp/claude-501/-Users-enrico-enrico-project-enrico-cash-flow-tracker/148d8a0e-cf25-4ec5-9de4-a0f135ad4203/scratchpad/web-months/shots/flows/flows.json`):
  - Oktober KPI Rp 4.513.000, previous change Rp 1.398.000 and baseline change Rp 744.167 equal the API; the history card names the baseline Rp 3.768.833.
  - The fact "Makanan & Minuman di atas rata-rata 3 bulan" opens Transaksi; its ledger summary is Rp 644.000, equal to the fact and shown on the page.
  - "Bulan berikutnya" from Oktober opens November (`?basis=month&start=2026-11-01`, fixed in `0afe48c`): "40% dari total Oktober 2026", no ▲/▼, no "naik" or "turun".
  - "Siklus gaji" recomputes: Siklus 30 Sep – 30 Okt, Rp 4.017.500, equal to the API.
  - Request log: Laporan Oktober and November and Ringkasan make 0 requests to `/dashboard/period-comparison`; Beranda still makes 1 (owned by `refine-workspace-layout`).
- **Route regression** (`/private/tmp/claude-501/-Users-enrico-enrico-project-enrico-cash-flow-tracker/148d8a0e-cf25-4ec5-9de4-a0f135ad4203/scratchpad/web-months/shots/regression-new/`, `regression-main/`): `/`, `/review`, `/ledger`, `/accounts`, `/goals`, `/settings`, Kalender and Peta Aliran at 1280 and 390 light, on this build and on main: identical results; the only serious finding is Kalender at 390 `color-contrast` (2 nodes), present on main too.
- **OpenSpec and diff:** `openspec validate --all --strict` 80 passed; after an archive rehearsal on a scratch copy 79 passed with no placeholder Purpose; `git diff --check` clean.
- **Clean-up evidence** (2026-11-04, after the runs): every process this change started (API 8262, web 3263, proxy 3262, main API 8264, main web 3265, main proxy 3266, headless Brave 9562) was stopped by PID; the container `cft-months-pg` and the scratch main checkout are removed. `lsof` shows 55562, 8262, 8264, 3262, 3263, 3265, 3266 and 9562 free; `docker ps -a` lists no `cft-months` container. The only matching processes left (8252, 3252, 3253) belong to the parallel `refine-workspace-layout` work and were not touched.

## Not verified

- **The rebase onto `refine-workspace-layout` (task 1.1)**, and with it:
  - Ringkasan's Pengeluaran explanation, shown only when the analytics `belanja` explanation equals the comparison's figure; the layout change's `spending` field and spending explanation finish it;
  - Beranda's same-day chips and insight banner, which still call `/dashboard/period-comparison` until the layout change removes them;
  - the MODIFIED spec blocks, to be refreshed from the merged main specs.
- **Ringkasan on a salary-cycle basis** picks the viewed period from the report's cycles (confirmed salary receipts); where Analisis's own cycle window differs, the comparison cards follow the report's cycles.
- Screen readers, a real phone and touch.
- Production.

## Acceptance
