# Verification: apply-markas-laporan (Markas+ phase 7)

Date: 2026-11-09 · Branch `feat/markas-laporan` · Base: `origin/main` `3cbd371` (phases 1 to 6 and 8, merged in
`10d814e`) · Code under test: `21806d0` (frontend), `16e3d87` (backend) unless a check names another revision.

## Approval, decisions and dependencies (tasks 1.1, 1.2)

- Approval: the owner's standing instruction "complete all until finished" (2026-11-06, renewed 2026-11-08) and
  "continue all" (2026-11-09). The coordinator accepted the flagged defaults D11 (one series of month bars), D13 (the
  first note is writable from a quiet button) and D26 (no credit figures on Laporan) under that instruction.
- **Owner decision 2026-11-08, "one spending basis" (D29):** found while checking every spending figure Laporan shows.
  The hero, the Q3 table and its total, "Pengeluaran per bulan", Selisih, the running shares and every fact but one
  already used the comparison (Pengeluaran). "Tempat belanja terbesar", the budget rows and their total, the
  reimbursement caption and the top-merchant fact used the Belanja basis, which counts expenses with the Tabungan
  pillar. Following phase 6's pattern, the report and the comparison gained additive read-only Pengeluaran-basis fields
  from `services/spending_basis.py` (`16e3d87`); no existing field changed. Recorded in the plan in `4594c5f`
  (`proposal.md`, `design.md` D29, `tasks.md` 2.4, `monthly-report` "One Pengeluaran on Laporan").
- Base: the plan was committed on `0308b7b` (`42c7a1f`), phase 6's branch merged (`7674f45`), then `origin/main`
  `3cbd371` after PR #95 merged (`10d814e`, no content change against the branch).
- Re-based plan statements (`4594c5f`, design "Re-based on the merged code"): the untranslated-page example stays on
  Pengaturan (`/settings`), since phase 8 translated Rekening before this phase; `tactile-pulse-ui` names Target &
  Tagihan only; token names (`--out-solid`, `--window`, `--surface`, `--surface-2`); Laporan's frame styles in
  `laporan.module.css` (phase 6) are removed with `report.module.css`; Tinjau's `.box` is overridden as
  `details.box`; facts and period labels are rendered by the server in the account's language from their codes;
  the stepper gains `monthStyle="long"` so English reads "October 2026"; no pull request (coordinator).

## For PROJECT_STATE.md

`PROJECT_STATE.md` is untracked in the owner's checkout, which this run does not touch; the coordinator carries these
over:
- Phase 7 (`apply-markas-laporan`) built 2026-11-09 under the standing instruction; branch `feat/markas-laporan`
  pushed, no PR (coordinator's instruction). Flags D11, D13, D26 accepted with their defaults.
- Owner decision 2026-11-08 (one basis) applied to Laporan as D29: new read-only fields
  `spending.top_spending_merchants`, `budget.categories[].spending`/`spending_remaining`, `budget.total_spending`,
  `budget.overall.spending`/`spending_remaining`, `flow.spending_reimbursement_offset`, `comparison.spending_top_merchant`;
  Belanja-basis fields unchanged.
- Untranslated-page example stays Pengaturan (`/settings`) until phase 10. Translated routes now include `/laporan`.
- `KpiStrip`, `Kpi` and `.ws-kpi*` are removed (Laporan was their last user).
- Follow-ups: `Meter` (phase 6) and `ProgressBar` (phase 8) still overlap (phase 12); "Rincian tabungan" cannot add up
  when money was paid from savings until the report sends `paid_from_savings` (D6); the recurring charges keep their
  detection basis (a charge with the Tabungan pillar is still listed as a repeating bill, as on Analisis).
- End-review list: the proposal's 14 deviations plus the deviations below.

## Environment

- Local only, synthetic data. PostgreSQL 16 containers `cft-laporan-pg` (127.0.0.1:8728, throwaway password) and
  `cft-laporan-pytest` (8729); Flyway `flyway/flyway:10`. No production system was touched; no paid service or AI run.
- Owners: `cft_markas_lp` = `seed_parity.py` (data.js shifted to 2026-11-09) migrated V51 → V54, then
  `seed_transaksi.py`, `seed_analisis.py` and the new `seed_laporan.py` through the API: history from 15 July (a partial
  first month) to the running November, a Rp 1.600.000 expense with the Tabungan pillar at "Reksa Dana Contoh" (the
  largest merchant of October on the Belanja basis), three unexpected items, an expense without a pillar, budgets with
  two categories over their limit and a note on September. `cft_markas_lp_stress` = the same with `--stress`
  (60-character merchants and category names, Rp 1.234.567.890 in a day, 30 categories, 12 detected charges, a
  1000-character note on October). `_en` twins with the account in English; `cft_markas_lp_usd` (`_en`) = the stress
  owner in the USD display.
- Backend from this worktree in a `uv` Python 3.12 venv; jobs, AI and outbound HTTP off. "Before": `3cbd371`'s
  `frontend/` exported with `git archive`, built and served on 3731 against the same API (the old page reads only the
  existing fields, whose values `tests/test_report_spending_basis.py` proves unchanged).
- Headless browser over CDP (`scripts/verify/markas/cdp.mjs`), axe-core 4 (wcag2a, wcag2aa, wcag21a, wcag21aa,
  wcag22aa). Captures and JSON stay in the session scratchpad (`…/scratchpad/markas-laporan/`).

## What changed

- **Backend (additive, read-only, D29):** `spending_basis.SpendingBasis.reimbursed`, `spending_basis.spending_merchants`,
  `observed_insights.merchant_spending(expense_sql=…)`, the report's and the comparison's new fields. No schema,
  migration, write path or existing field changed.
- **Laporan page:** title block from the catalogue with the basis segment and the stepper (full month names); the
  article (hero "Pengeluaran {period}" with its line against the whole previous period and the 3-month average, or the
  running shares with a 240 px meter) and four numbered questions: Masuk with green income sources; Ditabung &
  investasi with "Rincian tabungan"; the pillar and category table with whole-row links, the top places in red, the
  "Rincian pengeluaran" disclosure (unexpected items open the transaction dialog), the budget rows with 160 px meters
  and "{used} dari {budget}", and "Pengeluaran per bulan" as one series of proportional bars with a legend and a table;
  the facts as whole-row links and the note (read-only with "Ubah", or "Tulis catatan untuk {period}", form in place).
  The context panel holds Kualitas data in one sentence with its fixes, Selisih (completed periods) and Laporan
  sebelumnya with the straight selection bar and `aria-current="page"`. The classification dialog moved to
  `ShellDialog` 560.
- **Removed:** `ComparisonParts`, `ReportLayout`, `ReportParts`, `SpendingAnswer`, `IncomeSavingsAnswers`,
  `ReflectionAnswer`, `DataQualityPanel`, `report.module.css`, `laporan.module.css`, `KpiStrip`, `Kpi`, `.ws-kpi*`,
  `monthlyReport`'s `id-ID` helpers.
- **Language:** `report` namespace (137 messages per catalogue) and `terms.difference`; `/laporan` in
  `translatedRoutes.ts` and out of `untranslatedPages.ts`; the copy register checks `report.*` (with planted
  `idReport`/`enReport` fixtures; "How much was spent?" allowed as a question).
- **Shared (small):** `SectionHeader level`, `PeriodStepper monthStyle`, `essentialBasisText` accepts the report's
  essential cost.

## Backend checks (task 2.4)

- `tests/test_report_spending_basis.py` (9 new): the report's Pengeluaran for a closed month (Rp 635.000) equals
  Analisis's comparison, daily, category, calendar and flow Pengeluaran; top places and the top-merchant fact leave out
  the Tabungan-pillar merchant while `top_merchants` and `facts` keep it; budget rows, the overall budget and the
  reimbursement caption on the Pengeluaran basis (a reimbursement of a Tabungan-pillar expense is not subtracted from
  Pengeluaran); no Pengeluaran merchant gives no fact; another owner is never counted. `test_monthly_report.py`'s two
  exact budget assertions gained the additive keys.
- Full suite on a fresh `postgres:16` (`cft-laporan-pytest`) migrated V1 → V54 by `flyway/flyway:10`: **2197 passed**
  (exit 0) on `d6f1df0` (backend as `16e3d87`).
- API on the seeded owner, October 2026: comparison `totals.spending` Rp 6.423.830 = Analisis comparison = analytics
  daily sum = category sum; Belanja `flow.outflow` Rp 8.023.830. Top merchant on the Belanja basis "Reksa Dana Contoh"
  Rp 1.600.000; on the Pengeluaran basis "Leasing Motor Sentosa" Rp 850.000. Belanja Dapur budget: `spent`
  Rp 2.845.000, `spending` Rp 1.245.000.

## Captures, measurement and accessibility (tasks 4.2, 4.3, 4.6, 4.7)

`laporan-checklist.js` (first content at y = 192, article 728 | panel 352 and gap 24 at 1440, one column below
1200 px and the article ≤ 880, panel padding and radius, the 2 px full-height selection bar at x 0, one 44 px figure
(32/40 below 360), hero 48 px above question 1, four `h2` questions with the 32 px number column, gap 16 (8), 48 + 48
(32 + 32) with a 1 px rule, `.q-num` 20/32, `.q-h` 24/32, `t-fig` 20/28, table rows 48, header padding 8, total rule
`--line-strong`, share and change columns hidden on the phone, row links stretched over their rows, rows ≥ 56 (64),
160 × 4 budget meters, a 112 px bar area with the tallest bar filling it and one shared base, the 56 px summary,
stepper 36/44 and label ≥ 168, dialogs within the viewport and the classification dialog at 560, sizes, weights, at
most eight text styles, no other text ≥ 33 px, no dotted underline, no border ≥ 2 px, no KPI card or icon tile, no
horizontal overflow) runs on every capture, with axe, console and language records:

| Set | Build | Captures | Laporan checklist | axe serious/critical | Console | Overflow | `lang="id"` or notice |
| --- | --- | --- | --- | --- | --- | --- | --- |
| App, Indonesian, 13 scenarios at 1440/1024/390/320 × 2 themes (`final-id`) | `058b341` + CSS fixes | 104 | 4,484 of 4,492 | 0 | 0 | 0 | 0 |
| App, English, the same (`final-en`) | same | 104 | 4,484 of 4,492 | 0 | 0 | 0 | 0 |
| The open note form after `696732c`, Indonesian / English (`note-id`, `note-en`) | `696732c` | 8 / 8 | **396 / 396 each** | 0 | 0 | 0 | 0 |
| 1280, 1200, 1199, 1100, 768, 767 (`widths`) | `696732c` | 78 | 3,388 of 3,388 | 0 | 0 | 0 | 0 |
| Stress owner, Indonesian / English (`stress-id`, `stress-en`) | `696732c` | 32 / 32 | 1,560 / 1,560 each | 0 | 0 | 0 | 0 |
| Stress owner in the USD display, Indonesian / English (`usd-id`, `usd-en`) | `696732c` | 8 / 8 | 396 / 396 each | 0 | 0 | 0 | 0 |
| Failure (the report answers 500), Indonesian (`fail`) | `d6f1df0` | 2 | 19 of 19 | 0 | 2 (the induced 500s) | 0 | 0 |
| Loading (the report held), Indonesian (`hold`) | `d6f1df0` | 2 | 27 of 27 | 0 | 0 | 0 | 0 |
| Final build smoke, Indonesian / English (`smoke-id`, `smoke-en`) | `d6f1df0` | 4 / 4 | 200 / 200 each | 0 | 0 | 0 | 0 |
| Prototype, 1440/1024/390 (`prototype`) | — | 25 | 190 of 215 | 0 | 0 | 0 | — |

Scenarios: October (`laporan`), July (first, partial month, no previous), August (partial previous month), the running
November, the last salary cycle, September with its saved note, the first-note form open, the disclosure open, the
chart as a table, hidden balances, the classification dialog (`?lengkapi=1`), the budget dialog (`?atur=anggaran`) and
an unexpected item's transaction dialog. The eight failed checks of each first set are the note form's ninth text
style, fixed below. The 1100 and 767 widths were captured in dark, the others in light.

- The first sets found the open note form's 12/500 field label as a ninth text style (8 captures per language); the
  label now reads as the saved note's 14/500 heading (`696732c`) and the reruns pass. Two layout fixes went into the
  same commit: the disclosure summary's meta line had collapsed to no height, and budget amounts now share one column
  so every meter starts at the same x.
- The failure capture showed the stepper reading "Memuat…" after a failed report; it now names the requested period
  from the list (`21806d0`, unit test added).
- Known items: no horizontal overflow at 320 (the stepper takes the full width under the segment; the table hides
  "Porsi" and the change column); hidden balances mask every amount, change, bar label ("••") and the chart's
  accessible name (unit test and `laporan-sembunyi`); the first note is writable (unit test and keyboard flow 8);
  English Laporan shows no notice and no `lang="id"` element.
- Prototype (`shots/prototype`, 1440/1024/390): 25 captures, 190 of 215 checks; every failure is the prototype's own
  ninth text style (its note form's 12/500 label), which the app avoids.

## Keyboard (task 4.4)

`laporan-keys.mjs` (10 flows, 22 checks per width) at 1440 and 390 on `696732c`: **Indonesian 44 of 44, English 44 of
44**: the stepper, its reset and the basis segment; a past report row; a table row and a top place to the ledger; the
disclosure with Enter and Space, an unexpected item's dialog with focus back on the item; Atur anggaran and Lengkapi
klasifikasi with focus back on their buttons; writing, saving (focus on Ubah), reopening and deleting a note (focus on
Tulis catatan; the note is removed again); the chart/table toggle with `aria-pressed`; Buka Tinjau.

## Figure parity (task 4.5 and D29)

`laporan-parity.mjs` on the parity owner, before (`3cbd371` build) and after on the same data, for July (partial first
month), August (partial previous month), September (with its note), October and the running November:
- **Before against after: 0 changed values** for Pengeluaran, Masuk, Ditabung & investasi, Selisih, every pillar and
  category amount and share in the table, every income source and every month of the history.
- **English against Indonesian (region ID): byte-identical** figure sequences on all five reports.
- **One basis on the page:** Laporan's October hero and table total read Rp 6.423.830, and Analisis's October hero
  (calendar month, one step back) reads Rp 6.423.830 (`parity/one-basis-page.json`).
- Intended changes, not counted above: "Tempat belanja terbesar" and the top-merchant fact no longer list "Reksa Dana
  Contoh" (Rp 1.600.000 with the Tabungan pillar); budget rows read Pengeluaran (Belanja Dapur Rp 1.245.000 instead of
  Rp 2.845.000) and the overall line Rp 6.423.830 instead of Rp 8.023.830; the average change is now in the hero and
  the running average share is shown (`progress.baseline_share`).

## Frontend and repository checks (task 4.8)

On the tree of `d6f1df0` (frontend as `21806d0`):
- `npm test`: **97 files, 1,018 tests passed**; `npm run type-check` passed; `npm run lint` passed; `npm run build`
  passed.
- `git diff --check origin/main HEAD`: clean. `openspec validate --all --strict`: 85 of 85 before archiving.
- `git diff --stat origin/main -- db mobile nginx` is empty. `git diff --stat origin/main -- backend` lists only
  `services/monthly_comparison.py`, `services/monthly_report.py`, `services/observed_insights.py`,
  `services/spending_basis.py`, `tests/test_monthly_report.py` and the new `tests/test_report_spending_basis.py` (D29).
- No package file changed, so no lockfile step was needed.

## Deviations and notes for the owner's end review

The proposal's fourteen deviations hold, with D29 (one basis). Further:
1. Pillar marks are phase 6's 8 px square `PillarDot`, as on Analisis (the prototype draws round dots).
2. The table has no sticky first column at 320 px: with "Porsi" and the change column hidden below 768 px two columns
   fit without scrolling.
3. Budget rows are sorted by what is left on the Pengeluaran basis, most over first.
4. The previous period's note line uses the server's period label ("Catatan September 2026: …").
5. The note form's label reads 14/500 like the saved note's heading (the prototype's 12/500 label is a ninth text
   style).
6. The empty-ledger state is unit-tested only; no seeded owner without transactions was captured.
7. Category budgets on categories whose spending is all in the Tabungan pillar read Rp 0 (as on Analisis, phase 6
   deviation 10).

## Not verified

- The full "before" capture set of task 1.3: the before state is the `3cbd371` build, read by the parity run; no before
  screenshots or before axe run were kept.
- Composed side-by-side images (task 4.1) and the component sheet (`/komponen`, task 2.3): app and prototype captures
  exist separately; the sheet does not show a question block yet.
- The copy-key list file of task 2.1 (`research/evidence/…/copy-keys.md`): the keys are in both catalogues and design
  Appendix A; the copy register checks them.
- 200 % and 400 % browser zoom and a real screen reader (VoiceOver); the 320 px captures cover reflow.
- A real device; phone captures are the browser's device mode.

## Owner acceptance

Accepted under the owner's standing instruction ("complete all until finished", renewed 2026-11-08; "continue all",
2026-11-09) with the decision of 2026-11-08 (one basis). No separate review of this phase was requested before
archiving; the deviations above stay on the end-review list.
