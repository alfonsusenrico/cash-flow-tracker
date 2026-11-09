# Verification: apply-markas-analisis (Markas+ phase 6)

Dates: 2026-11-08 to 2026-11-09 · Branch `feat/markas-analisis` · Base: `origin/main` `98cdb49` (phases 1 to 5 and 8,
merged during this work) · Code under test: `d29dea0` unless a check names another revision.

## Approval, decisions and dependencies (tasks 1.1, 1.2)

- Approval: the owner's standing instruction "complete all until finished" (2026-11-06, renewed 2026-11-08) and
  "continue all" (2026-11-09). The flagged defaults D5, D6, D15 and D29 were accepted by the coordinator under that
  instruction (D29 stands: no credit figures on Analisis).
- **Owner decision D31 (2026-11-08, "one basis"):** found during implementation and paused for. The plan renamed the
  Belanja-basis figures (Kalender, Peta Aliran, the daily chart, the category list) to "Pengeluaran", which showed two
  different "Pengeluaran" amounts whenever a period has an expense in the Tabungan pillar (Rp 4.355.990 against
  Rp 4.655.990 on the parity owner). The owner chose read-only Pengeluaran-basis server fields, shown everywhere on
  Analisis; the Belanja-basis fields are unchanged. Recorded in `proposal.md`, `design.md` D31, `tasks.md` 2.2 and the
  `analysis-workspace`, `cycle-calendar` and `money-flow-map` deltas (scenario "Figures from the summary" corrected) in
  `ec3b24b`.
- **Coordinator decision (2026-11-09):** the untranslated-page test example moves from Analisis to Pengaturan
  (`/settings`), not to Laporan, because phase 7 translates Laporan next; `tactile-pulse-ui` names Laporan and Target &
  Tagihan, since phase 8 redesigned Rekening.
- Base: the plan was committed on `0308b7b` (`31317fa`), phase 5 was merged from `origin/feat/markas-transaksi`
  (`3df1055`, `370e0c1` with `36b2113`), then `origin/main` `98cdb49` with phases 5 and 8 (`1e4a25f`). Phase 5's and
  phase 8's pieces matched D24 and D28 as planned (`RowAmount`, `TransactionDialog`, `HeroFigure` `aside`/`line`,
  `FigureBlock bar`, `RadioSegments`, `ShellDialog`); no plan text needed re-basing besides the two example pages above.

## For PROJECT_STATE.md

`PROJECT_STATE.md` is untracked in the owner's checkout, which this run does not touch; the coordinator carries these
over:
- Phase 6 (`apply-markas-analisis`) built 2026-11-08/09 under the standing instruction; branch `feat/markas-analisis`
  pushed, no PR (coordinator's instruction).
- Owner decision 2026-11-08 (D31, "one basis"): every "Pengeluaran" on Analisis equals Beranda and Transaksi; new
  read-only fields `daily_cadence[].spending`, `category_variance[].spending`/`spending_percentage_used`,
  calendar `totals.spending`/`days[].spending`/`days[].spending_heat`/`spending_heat_thresholds`, flow
  `summary.spending`; Belanja-basis fields unchanged.
- Untranslated-page example is Pengaturan (`/settings`) until phase 10.
- End-review list: the proposal's 16 deviations plus the deviations below; "Seberapa sering ditanya" now in
  Pengaturan › Kebiasaan (old styling until phase 10).
- Follow-ups: phase 7 reuses `Meter`, `ChangeFigure`, `ChartTable`, `format.compactMoney` and `BudgetDialog`; phase 8's
  `ProgressBar` and phase 6's `Meter` both draw `.prog` and could become one component in phase 7; `ComparisonParts`'
  `Change` is still Laporan's.

## Environment

- Local only, synthetic data. PostgreSQL 16 containers `cft-analisis-pg` (127.0.0.1:8728, throwaway password) and
  `cft-analisis-pytest` (8729); Flyway `flyway/flyway:10`. No production system was touched; no paid service or AI run.
- Owners: `cft_markas_an` = `seed_parity.py` (data.js shifted to 2026-11-09) migrated V51 → V54, then
  `seed_transaksi.py` and the new `seed_analisis.py` through the API (budgets with one category over 100 %, an archived
  category with spending, an uncategorised expense; the parity seed already has a Rp 300.000 saving-pillar expense,
  waiting rows, plans, transfers, top-ups and credit); `cft_markas_an_stress` = the same with `--stress` (1,050 rows of
  60-character merchants, Rp 1.234.567.890, 30 categories, 12 detected charges) and a 40-character account name; `_en`
  twins with the account in English.
- Backend from this worktree in a `uv` Python 3.12 venv; jobs, AI and outbound HTTP off. "Before": `98cdb49`'s
  `frontend/` and `backend/` exported with `git archive`, built and run the same way (web 3731, API 8725).
- Headless browser over CDP (`scripts/verify/markas/cdp.mjs`), axe-core 4 (wcag2a, wcag2aa, wcag21a, wcag21aa,
  wcag22aa). Captures and JSON stay in the session scratchpad (`…/scratchpad/markas-analisis/`).

## What changed

- **Backend (additive, read-only):** `progress.baseline_share` in the comparison (D5, `0894c60`); the Pengeluaran basis
  (D31, `a63f980`): `services/spending_basis.py` (one statement per window; reimbursements netted per category and
  pillar, floored, spread by `apportion`), `saving_flow.saving_pillar_sql` (the existing predicate, now named and used
  by both the saving direction and the new helper), the new fields in the analytics, the calendar and the flow. No
  schema, migration, write path or existing field changed.
- **Analisis page:** title block from the catalogue, basis segment and stepper with the period's dates, underline
  tabs with the report link; Ringkasan (hero with one Rincian, comparison rows, Kakeibo rows, SVG daily chart with its
  table, categories by pillar with the two dialogs, bills panel and report link); Kalender (Monday-first grid, compact
  amounts, 6 px markers, the server's heat strip, day panel with the transaction dialog, bills until payday, the whole
  grid on the phone); Peta Aliran (Masuk hero and four figures, highlight and view segments, a four-column map with
  every label inside the frame and a legend, the table, details in a centered dialog). Replay, fold, week strip and
  `MiniMap` removed.
- **Shared:** `Meter`, `ChangeFigure`, `ChartTable`, `format.compactMoney`/`shortMonth`/`weekdayShort`, `BudgetDialog`,
  `CategoryManagerDialog`, `CategoryForm`; `charts/theme.ts` removed; `--heat-*`, `--flow-*`, `--band-op` removed.
- **Moves:** "Seberapa sering ditanya" to Pengaturan › Kebiasaan; "Kebiasaan dihafal" removed; Laporan's frame styles
  to `components/report/laporan.module.css`; Laporan imports `BudgetDialog`.
- **Language:** 261 messages per catalogue (`analysis.*`, `terms.stillLeft`, `terms.waitingForPair`,
  `common.change.*`); `/insights` translated; copy register covers `analysis.*` with planted fixtures.

## Backend checks (tasks 2.1, 2.2)

- `tests/test_monthly_comparison.py` (3 new): 37 % against Rp 3.910.167, no share without a baseline or for a completed
  period, a two-period baseline and a zero baseline.
- `tests/test_spending_basis.py` (5 new): calendar, flow, daily and category Pengeluaran equal
  `comparison.totals.spending` (Rp 585.000) on an owner with Rp 340.000 of saving-pillar expenses and a Rp 60.000
  reimbursement; every Belanja-basis field keeps Rp 925.000; day and category values; levels by the same percentile
  rule; budget share 75 % against Belanja's 150 %; another owner never counted. Two statement budgets grew by the one
  Pengeluaran-basis statement (still constant), and a mocked analytics test gained its row (`b058a6c`).
- API before/after (`tools/api_diff.py`, base backend `98cdb49` on the same database): 30 requests (analytics,
  calendar and flow for both bases and three offsets, periods, eight comparisons, recurring charges, latest report,
  categories); with the additive keys removed **0 differences** besides the request-time "now" instants.
- API one-basis check (`tools/one_basis.py`): for both bases and offsets 0, −1, −2 on the parity and the stress owner,
  comparison, explanation, daily sum, category sum, calendar total and days and flow summary are **all equal** (12 of
  12 windows); on the running cycle Belanja is Rp 4.490.490 and Pengeluaran Rp 4.190.490.
- Full suite on a fresh `postgres:16` (`cft-analisis-pytest`) migrated V1 → V54 by `flyway/flyway:10`: **2188 passed** (`PYTEST-EXIT 0`) on `6a39b51` (the backend has not changed since `b058a6c`). The first run on `1e4a25f` found the two statement budgets and the mocked analytics test above (3 failed, 2185 passed), fixed in `b058a6c`.

## Captures, measurement and accessibility (tasks 8.1 to 8.3, 8.6, 8.7)

`analisis-checklist.js` (tabs 40/44 with the 2 px bar, report link from 768 px, tabs at y = 192, `.cols` 728 | 352 and
gap 24 from 1200 px, panel padding and radius, one 44 px figure, comparison rows ≥ 48 with 120/72/120 columns, 8 px
square pillar dots, the bar-width formula, the chart at its column width with 12 px labels and no overlapping date
labels, rails 2 px with 16 px padding, 160 × 4 meters, rows ≥ 56/64, cells ≥ 80/56 with 6 × 6 markers, 4 px strip and
2 px selection bar and every amount inside its cell, flow labels inside the frame, 8 px nodes, band opacities 0.30/0.06,
dialogs centred at their width or full-width sheets, stepper buttons 36/44 and label ≥ 168, phone segments ≥ 40, sizes,
weights, ≤ 8 text styles, no other text ≥ 33 px, no dotted underline, no border ≥ 2 px, at most one Rincian, no
horizontal overflow) runs on every capture, with axe, console and language records:

| Set | Revision | Captures | Analisis checklist | axe serious/critical | Console | Overflow | `lang="id"` or notice on Analisis |
| --- | --- | --- | --- | --- | --- | --- | --- |
| App, Indonesian, 23 scenarios at 1440/1024/390/320 × 2 themes (`final-id`) | `6dcc719` | 184 | 4,266 of 4,270 | 0 | 0 | 0 | 0 |
| App, English, the same (`final-en`) | `6dcc719` | 184 | **4,270 of 4,270** | 0 | 0 | 0 | 0 |
| Kalender at 320 after the cell-padding fix (`fix320`) | `6a39b51` | 6 | **166 of 166** | 0 | 0 | 0 | 0 |
| 1280, 1200, 1199, 768, 767 (`widths`) | `6a39b51` | 35 | 833 of 833 | 0 | 0 | 0 | 0 |
| Failure (comparison, calendar, flow and charges answer 500), English (`fail`) | `6a39b51` | 6 | 131 of 131 | 0 | 8 (the induced 500s) | 0 | 0 |
| Loading (the same requests held), Indonesian (`hold`) | `6a39b51` | 6 | 114 of 114 | 0 | 0 | 0 | 0 |
| Stress owner, Indonesian / English (`stress-id`, `stress-en`) | `6a39b51` | 72 / 72 | 1,430 / 1,430 each | 0 | 0 | 0 | 0 |
| Stress owner in the USD display, Indonesian / English (`usd-id`, `usd-en`) | `d29dea0` | 20 / 20 | 449 / 449 each | 0 | 0 | 0 | 0 |
| Prototype, 1440/1024/390 (`prototype`) | — | 12 | 238 of 241 | 1 | 1 (favicon) | 0 | — |

- The four failures of the first Indonesian set were "every cell amount fits its cell" at 320 px ("310rb" in a 41 px
  cell); `6a39b51` gives cells 2 px side padding below 360 px and the rerun passes. The checklist's own first run also
  found the phone Kakeibo dot on its own line, nine text styles on Kalender, the flow scale shrunk by slots and the
  progress bars not drawn; each was fixed (`bee00bf`, `5a230fd`, `6dcc719`) before these sets.
- The stress owner has 12 detected charges (the seed first named them by digits, which the detector ignores; fixed in
  `5ab20ee`), Rp 1.234.567.890 in one day ("1M"/"1B" compact, "1M" in the 320 px cell), 30 categories, 60-character
  merchants and a 40-character account name; the USD display reads "$75,384.30" and "$116" in cells.

- Scenarios: running cycle, calendar month, last month, last cycle, chart table, Kalender, a day with waiting rows,
  Peta Aliran (default, map, table), highlight, hidden balances on each tab, and the dialogs Rincian, Kelola kategori,
  Ubah, Atur anggaran, Tagihan & langganan, a flow node, a flow link and a day's transaction; plus Pengaturan ›
  Kebiasaan with the moved panel.
- Known items: Kalender has no `color-contrast` finding at any width or theme (markers are no longer text on fills; every
  marker colour is ≥ 3:1 on `--window`, `--surface` and `--surface-2`, pinned in `tokens.test.ts`); every flow label is
  inside the frame at 1440, 1200 and 1024 (checklist and unit test with a 40-character name); the English stepper
  reads "Oct 2026", "Cycle since 27 Oct" and "Cycle 29 Sept – 27 Oct" (no Indonesian month); no overflow at 320; no dotted
  figure.
- English captures show no `lang="id"` element and no notice on Analisis; the 24 `lang="id"` findings are Pengaturan ›
  Kebiasaan, which stays untranslated until phase 10.
- Prototype (`shots/prototype`, 1440/1024/390): the prototype's own Kalender has nine text styles and its Peta Aliran
  fails `scrollable-region-focusable` at 1024 px; the app passes both.

## Keyboard (task 8.4)

`analisis-keys.mjs` (12 flows, 33 checks per width) at 1440 and 390 on `6dcc719`: **Indonesian 66 of 66, English 66 of
66**: tabs,
Rincian and focus return, the chart/table toggle, a category row to the ledger, Kelola kategori list → form → Kembali →
Esc, Atur anggaran, the calendar walk (arrows, Home, End, PageUp, PageDown) with single-key shortcuts on and off, a
day row to the transaction dialog and back, Lihat di Transaksi for that day only, the flow node walk with Enter and
Esc, the flow table link, the stepper and the basis segment.

## Figure parity (task 8.5 and D31)

`analisis-parity.mjs` on the parity owner, before (`98cdb49` build) and after (`6dcc719` build) on the same data, for the
running cycle,
the running month and the last whole month:
- **Every "Pengeluaran" on the page is one figure:** Ringkasan's hero, the sum of the daily table, the sum of the
  category rows, Kalender's hero and Peta Aliran's Pengeluaran equal `comparison.totals.spending` in all three periods
  (Rp 4.190.490, Rp 3.717.990, Rp 5.785.830).
- **Before against after: 0 changed values** for Masuk, Pengeluaran and Ditabung & investasi (the former KPI row against
  the hero and its line).
- **English against Indonesian (region ID): byte-identical** on every tab of the three periods (289 amounts).
- Intended changes by D31, not counted above: where a period has saving-pillar expenses, Kalender's hero, its cells
  and strip, the daily chart, the category amounts and Peta Aliran's spending figure now show Pengeluaran instead of
  Belanja (running cycle Rp 4.490.490 → Rp 4.190.490; the whole months have none, so they are unchanged). New figure:
  `baseline_share` (shown as "…% dari rata-rata …"), proven against `share_percent` in pytest.

## Frontend and repository checks (task 8.8)

On `d29dea0`:
- `npm test`: **97 files, 1,008 tests passed** (the first run after the merge found the waiting marker's coloured
  box-shadow, fixed with a `currentColor` border); `npm run type-check` passed; `npm run lint` passed; `npm run build`
  passed.
- `git diff --check origin/main HEAD`: clean. `openspec validate --all --strict`: 85 of 85 before archiving.
- `git diff --stat origin/main -- db mobile nginx` is empty. `git diff --stat origin/main -- backend` lists only
  `routers/dashboard.py`, `services/calendar.py`, `services/money_flow.py`, `services/monthly_comparison.py`,
  `services/saving_flow.py`, the new `services/spending_basis.py` and four test files (D5 and D31).
- No package file changed, so no lockfile step was needed.

## Deviations and notes for the owner's end review

The proposal's sixteen deviations hold, with D31 (one basis). Further:
1. The running cycle's stepper and hero read "Siklus sejak 27 Okt" / "Cycle since 27 Oct": the periods list has no end
   date for a running cycle, and the expected end is only in the calendar's response.
2. The prototype's `--m-*` mark tokens are the app's `--*-solid` tokens.
3. Laporan's frame styles moved to `laporan.module.css`; its history bars use `format.compactMoney` with Indonesian units
   ("1,2jt", no space) until phase 7.
4. `ChangeFigure` takes its words from `common.change`; "Keluar" and "Keluar ke" in the flow detail are allowed
   direction words in the copy guard.
5. The `shopping-cart` icon option is "Keranjang", not "Belanja".
6. Today's date in a calendar cell is the ink colour without the 500 weight, and the phone cell amount is 12/400, so
   Kalender stays within eight text styles (SPEC §13); below 360 px cells have 2 px side padding so "310rb" fits.
7. The waiting marker is drawn with a `currentColor` border (the calendar-marker exception), as the stylesheet rules
   allow no coloured box-shadow.
8. The flow map's 24 px slots let the drawing grow beyond 320 px instead of shrinking the scale (as the prototype).
9. Phase 8's `ProgressBar` and this phase's `Meter` both draw `.prog`; `.prog` is now `display: block` for both.
10. Spending of categories that only hold saving-pillar expenses is Rp 0 and waits behind "Tampilkan … kategori tanpa
    pengeluaran"; those expenses stay in Ditabung & investasi.

## Not verified

- The full "before" capture set of task 1.3: the before state is the `98cdb49` build, read by the parity run and the
  API diff; no before screenshots or before axe run were kept.
- Composed side-by-side images (task 8.1): app and prototype captures exist separately.
- 200 % and 400 % browser zoom and a real screen reader; the 320 px captures cover reflow.
- The empty Ringkasan and the empty Peta Aliran are unit-tested only (the seeded owners have spending).
- The English cycle labels written by the shared formatter read "Sept" in region Indonesia ("Cycle 29 Sept – 27 Oct")
  while server labels read "Sep"; this is the formatter's `en-ID` month, unchanged here.
- A real device; phone captures are the browser's device mode.

## Owner acceptance

Accepted under the owner's standing instruction ("complete all until finished", renewed 2026-11-08; "continue all",
2026-11-09) with the decision D31 of 2026-11-08. No separate review of this phase was requested before archiving; the
deviations above stay on the end-review list.
