## 1. Approval, dependencies and baseline

- [x] 1.1 Record the approval and the flagged review. Evidence:
  - `proposal.md` "Approval" names the standing instruction (2026-11-06, renewed 2026-11-08) and the three flagged decisions D11, D13 and D26 with their fallbacks;
  - `verification.md` "For PROJECT_STATE.md" names the same, with the date and the reviewer's outcome per flag (this worktree may not touch the owner's checkout);
  - `openspec validate apply-markas-laporan --strict` passes after the edits.
- [x] 1.2 Confirm the dependencies and update the branch (D25). `localize-server-text`, `apply-markas-tinjau`, `apply-markas-transaksi` and `apply-markas-analisis` must be merged into `main`; stop and report if one is not. Then update `feat/markas-laporan` from `origin/main`. Evidence:
  - `git merge-base --is-ancestor` succeeds for the four merge commits;
  - `Meter`, `ChangeFigure`, `ChartTable`, `format.compactMoney`, `components/categories/BudgetDialog.tsx`, `RowAmount`, `TransactionDialog` and `HeroFigure` without a mark exist; whether `progress.baseline_share` exists is recorded (D3 falls back without it);
  - the report's facts and period labels carry their codes from `localize-server-text`; an English request renders them in English; otherwise stop and report;
  - an English Laporan shows "This page is in Indonesian for now." and the untranslated-page test uses `/settings` (re-based);
  - every difference between the merged code and D21, and between the merged `monthly-navigation-stepper` and `tactile-pulse-ui` wording and this change's deltas, is written to `verification.md`, and the two deltas are re-based on the merged text;
  - `openspec validate --all --strict` passes on the updated branch, and the revision is recorded.
- [ ] 1.3 Capture the "before" state on that `main` revision, tied to its hash. Evidence in `research/evidence/apply-markas-laporan/before/`:
  - Oktober, Juli, the running month, a salary cycle, a partial previous month, with and without a budget, with and without notes, with the parity seed and the audit seed, in `en` and `id`, at 1440, 1280, 1200, 1199, 1024, 768, 767, 390 and 320 px, light and dark, with and without hidden balances;
  - the classification dialog and the budget dialog open;
  - `parity-before.json`: every amount, count, percent and date on the page and in the two dialogs;
  - the overflow list at 320 and the hidden-balance leaks (change chips, chart list).
- [x] 1.4 Extend the seeds (D23), local and disposable only. Evidence: the seeds load into a disposable database and task 1.3 reruns on them without error:
  - the parity seed gains five months of history (Juli to the running November), a partial first month, a note on September, a category over its budget, three unexpected items, two recurring charges, a reimbursement, rows waiting in Tinjau, an unclassified expense, two possible duplicates and a day without records;
  - the stress and USD seed: the audit seed, 60-character merchants and category names, Rp 1.234.567.890 in one category, 25 categories, a 1000-character note and the USD display currency.

Expected Result: the phase starts from a verified base with its dependencies merged and a recorded "before".

## 2. Shared pieces and language

- [ ] 2.1 Write every Laporan copy key with both languages into `research/evidence/apply-markas-laporan/copy-keys.md` (Appendix A plus the classification dialog, `aria-label`s and errors). Evidence: the file lists every literal of `app/laporan` and `components/report` with a key; a check against the copy register finds no slang, no "you/your", no "Belanja" or "Keluar" figure label and no near-duplicate term.
- [x] 2.2 Add the `report` namespace and `terms.difference` to both catalogues, `/laporan` to `lib/i18n/translatedRoutes.ts`, and remove `/laporan` from `untranslatedPages.ts` (D19). The untranslated-page test example stays on Pengaturan (`/settings`). Update the copy register roots and extend `SPENDING_ID` to `report.*` with `report.merchants.title` allowed. Evidence:
  - the catalogue parity test and `type-check` pass, and a planted missing English key fails both;
  - an English Laporan has no notice and no `lang="id"` region; an English Pengaturan still has both;
  - a planted Indonesian "Belanja bulan ini" figure label and a planted English "Expenses" in `terms` each fail the copy register.
- [ ] 2.3 Add the Laporan CSS from `styles.css` (`.q`, `.q-num`, `.q-h`, `.bars-v`, `.b`, the `details.box` summary and the hatch) (D21). Evidence: the component sheet (`/komponen`) shows a question block and the bars at 1440 and 390 in both themes, measured equal to the prototype's `#/komponen` or `laporan` capture within 1 px.

- [x] 2.4 One spending basis (D29). Add the read-only Pengeluaran-basis fields to the report and the comparison from `services/spending_basis.py`. Evidence: `tests/test_report_spending_basis.py` (Laporan's Pengeluaran equals Analisis's for a closed month; top places, budgets, the overall budget, the reimbursement caption and the top-merchant fact leave out Tabungan-pillar rows; the Belanja-basis fields keep their values; another owner is never counted); the full backend suite passes on a fresh database migrated V1 → V54; the page reads only the new fields for these figures.

Expected Result: every message and rule exists before the page uses it.

## 3. Laporan page

- [x] 3.1 Frame, title block, segment and stepper with the report's dates (D1, D2). Evidence: tests for "Stepping to the previous report" and "Report period name"; the cycle item is hidden without cycles; no `useWidthAtMost` or `ReportLayout` import in Laporan (`rg`).
- [x] 3.2 Hero and its line (D3). Evidence: tests for "Hero against both comparisons", "November in progress" (with and without `baseline_share`), "No previous whole period", the partial previous month ("belum lengkap") and "Rata-rata muncul setelah 2 bulan lengkap"; no ▲/▼ while running.
- [x] 3.3 Questions 1 and 2 (D4–D6). Evidence: tests for "Numbered questions" and "Savings separate from surplus"; source rows link to `type=income` with the category and window; the saving-pillar row links and the net-move row does not; income amounts are green "+Rp".
- [x] 3.4 Question 3 table (D7). Evidence: the existing table tests move and pass (row order, category link names and hrefs with `category=` and `kakeibo=`, "belum lengkap", running shares); the whole row is a target (a click on the share cell opens the link); `ChangeFigure` masks under hidden balances; axe clean with the caption, row headers and total row.
- [x] 3.5 Merchants, the details box and the budget (D8–D10). Evidence: tests for the top five merchants with red amounts and `q=` links; the disclosure toggles with Enter and Space and its chevron turns; an unexpected item opens `TransactionDialog` and focus returns; the budget tests move and pass ("No budget", "Budget set" with "Rp 620.000 dari Rp 500.000 · lebih Rp 120.000"); `?atur=anggaran` opens the shared dialog.
- [x] 3.6 "Pengeluaran per bulan" (D11, or its fallback). Evidence: tests for "Five bars", "Proportional heights" and "Table alternative"; the legend names the viewed and the running period; the chart's `aria-label` is masked under hidden balances.
- [x] 3.7 Question 4 facts and the note (D12, D13, or its fallback). Evidence: tests for "No reflection note", "Writing the first note", "Shown again next month" (the previous note line), edit and delete with their statuses and errors, "Sejauh ini" while running, and the facts' links ("Lihat transaksi", "Lihat daftar" to `/insights?tab=ringkasan`).
- [x] 3.8 Context panel (D14). Evidence: tests for "Past reports", the complete and the open data-quality lines with their four fixes, Selisih only on completed periods; no green notice fill.
- [x] 3.9 Classification dialog (D15). Evidence: the existing classification test passes on `ShellDialog` with the same requests; `?lengkapi=1` opens it; focus returns to "Lengkapi klasifikasi"; axe clean.
- [x] 3.10 States (D16). Evidence: tests for loading (`aria-busy`), failure with "Coba lagi", and "New user" without zero figures.
- [x] 3.11 Remove what has no user left (D22). Evidence: `rg` finds no `ComparisonParts`, `ReportLayout`, `ReportParts`, `SpendingAnswer`, `IncomeSavingsAnswers`, `ReflectionAnswer`, `report.module.css`, `KpiStrip`, `Kpi` or `.ws-kpi`; `npm run lint` reports no unused export; Analisis and Beranda still build.

Expected Result: Laporan matches the prototype, reads as one narrative and keeps every figure, link and action.

## 4. Verification gate

- [ ] 4.1 Side by side (D23). Evidence: prototype and app captures with composed left/right images for every D23 scenario at 1440 × 900, 1024 × 768 and 390 × 844, light and dark, `en` and `id`, in `research/evidence/apply-markas-laporan/after/` and `compare/`.
- [x] 4.2 Measurement checklist and SPEC §13 rules. Evidence: `laporan-checklist.js` passes on the prototype (dry run) and on the app with the counts recorded; first content at `y = 192`; one 44 px figure; sizes, weights and ≤ 8 text styles; no dotted underline; no border ≥ 2 px except the allowed marks; no horizontal overflow at 1440, 1280, 1200, 1024, 768, 390 and 320, including the line that overflowed at 320 before.
- [ ] 4.3 Accessibility. Evidence: axe-core (wcag2a, wcag2aa, wcag21a, wcag21aa, wcag22aa) with zero serious or critical findings in both themes and both languages at 1440, 1024, 390 and 320: each scenario, both dialogs, the note form, hidden balances, loading, failure and empty; zero console errors; correct `lang` attributes; 200 % and 400 % zoom reflow; a VoiceOver spot check of the table's row links and the questions' headings.
- [x] 4.4 Keyboard flows. Evidence: `laporan-keys.mjs` runs the D23 flows in both languages at 1440 and 390; every flow passes, recorded with the revision.
- [x] 4.5 Figure parity. Evidence: `parity.mjs` and a `verification.md` table with zero changed values: before (1.3) against after with the same seed, and `en` against `id` (region `ID`), byte-identical figure strings; the intended differences (the average change now in the hero, the running average share) are listed with their server sources.
- [x] 4.6 Known items. Evidence: a `verification.md` section proves the 320 px overflow gone, the hidden-balance leaks gone, the first note writable, and `/laporan` out of the untranslated set.
- [x] 4.7 Stress and USD seed at 1440, 1024, 390 and 320 in both themes and both languages. Evidence: captures with no clipping, overlap or horizontal scroll for 60-character names, Rp 1.234.567.890, 25 categories, the 1000-character note and the USD display; recorded in `verification.md`.
- [x] 4.8 Frontend and repository checks on the exact revision. Evidence, each recorded with the commit hash: `npm test`, `npm run type-check`, `npm run lint`, `npm run build`, `git diff --check` and `openspec validate --all --strict` pass; `git diff --stat origin/main -- db mobile nginx` is empty and `backend` lists only phase 6's files and D29's.
- [x] 4.9 Complete `verification.md` and present the result. Evidence: it names what changed, the gate counts, the deviations, the flag outcomes, the "Rincian tabungan" follow-up (D6) and what was not verified and why; the deviations are listed for the owner's end-review list in its "For PROJECT_STATE.md" section; the report to the owner quotes them.

Expected Result: the phase passes the plan's common gate and the both-language gate on its exact revision, with every deviation recorded.

## Workflow follow-up

- Archive: `openspec archive apply-markas-laporan`, then `openspec validate --all --strict` must pass. The archive path is under `openspec/changes/archive/`.
- Commit with subject-only Conventional Commits messages, for example `feat(report): apply Markas+ to Laporan`, and push `feat/markas-laporan`.
- Push `feat/markas-laporan` and report the revision. The coordinator creates no pull request for this phase (instruction of 2026-11-09); merging stays with the owner.
- After merge: delete the branch locally and on the remote, and remove the `cash-flow-tracker-markas-laporan` worktree.
- Write in `verification.md` "For PROJECT_STATE.md" that the untranslated-page example stays Pengaturan until phase 10, and that `Meter` and `ProgressBar` still overlap (phase 12).
