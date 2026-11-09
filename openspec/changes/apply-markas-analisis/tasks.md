## 1. Approval, dependencies and baseline

- [ ] 1.1 Record the approval and the flagged review. Evidence:
  - `proposal.md` "Approval" names the standing instruction (2026-11-06, renewed 2026-11-08) and the four flagged decisions D5, D6, D15 and D29 with their fallbacks;
  - `PROJECT_STATE.md` names the same, with the date and the reviewer's outcome per flag (handed to the coordinator when this worktree may not touch the owner's checkout);
  - `openspec validate apply-markas-analisis --strict` passes after the edits.
- [ ] 1.2 Confirm the dependencies and update the branch (D28). `localize-server-text`, `apply-markas-tinjau` and `apply-markas-transaksi` must be merged into `main`; stop and report if one is not. Then update `feat/markas-analisis` from `origin/main`. Evidence:
  - `git merge-base --is-ancestor` succeeds for the three merge commits;
  - `PeriodStepper` accepts `period` and renders "Cycle 23 Oct – 24 Nov" in a component test; `RowAmount`, `TransactionDialog`, `HeroFigure` (`mark` optional, `aside`, `line`) and `FigureBlock bar={false}` exist;
  - an English Analisis shows "This page is in Indonesian for now." and the untranslated-page test uses `/insights`;
  - every difference between the merged phase 4 and 5 code and design D24 or the copied `monthly-navigation-stepper` and `tactile-pulse-ui` wording is written to `verification.md`, and the two deltas are re-based on the merged text;
  - `openspec validate --all --strict` passes on the updated branch, and the revision is recorded in `verification.md`.
- [ ] 1.3 Capture the "before" state on that `main` revision, tied to its hash. Evidence in `research/evidence/apply-markas-analisis/before/`:
  - the three tabs with the parity seed and the audit seed, in `en` and `id`, at 1440, 1280, 1200, 1199, 1024, 768, 767, 390 and 320 px, light and dark, with and without hidden balances;
  - the dotted explanation open, the category modal, the flow drawer, the day sheet, the recurring-charge list open, Pengaturan › Kebiasaan;
  - `parity-before.json`: every amount, count, percent and date on the three tabs and in those layers, for the running cycle, the previous cycle, the current month and October;
  - axe results (the known `color-contrast` findings on Kalender), the overflow list at 320, and the positions of the flow labels past the frame.
- [ ] 1.4 Extend the seeds (D26), local and disposable only. Evidence: the seeds load into a disposable database and the before capture of task 1.3 reruns on them without error:
  - the parity seed gains a 33-day running cycle, a partial first month, two and three whole months before the running one, an archived category with spending, an uncategorised expense, a category over budget, a day with waiting rows and plans, a transfer, a top-up, an awaiting leg and a negative opening balance;
  - the stress and USD seed: the audit seed (17 pockets), a 40-character account name, 60-character merchants, Rp 1.234.567.890 on one day, 30 categories, 12 detected charges and the USD display currency.

Expected Result: the phase starts from a verified base with its dependencies merged and a recorded "before".

## 2. Backend addition (read-only, flagged D5)

- [ ] 2.1 Add `progress.baseline_share` to `build_comparison` (D5), only if the D5 flag was accepted; otherwise mark this task not applicable in `verification.md` and build the fallback row in 4.2. Evidence:
  - tests in `backend/tests/test_monthly_comparison.py` for the spec scenarios "Running share of the average" (37) and "No baseline share", for a two-period baseline, and for a baseline of 0 (`null`);
  - a test proves every other field of the comparison byte-identical to a baseline run on the same data;
  - the full backend suite passes on migrated disposable PostgreSQL, with its count recorded.

- [ ] 2.2 Add the Pengeluaran-basis fields (owner decision D31, 2026-11-08). Evidence:
  - `tests/test_spending_basis.py` on an owner with a saving-pillar expense and a reimbursement: the calendar's `totals.spending` and its days, the flow's `summary.spending`, the analytics daily and category `spending` all equal `comparison.totals.spending`; every Belanja-basis field equals a run without the new fields;
  - the saving-pillar predicate is one helper in `services/saving_flow.py`, used by the saving direction and by the new helper;
  - the full backend suite passes on migrated disposable PostgreSQL.

Expected Result: the comparison carries the share the page needs, every Pengeluaran on Analisis is one figure, and nothing else changes.

## 3. Shared pieces and language

- [ ] 3.1 Write every Analisis copy key with both languages into `research/evidence/apply-markas-analisis/copy-keys.md` (Appendix A plus the remaining `aria-label`s, errors and dialog texts). Evidence: the file lists every literal of `app/insights`, `components/analysis` and the two category dialogs, each with a key; a reviewer check against the copy register finds no slang, no "you/your", no "Belanja" or "Keluar" figure label and no near-duplicate term.
- [ ] 3.2 Add `format.compactMoney` to `lib/format.ts` (D22). Evidence: unit tests for 80.500 → "81rb"/"81K", 1.303.500 → "1,3jt"/"1.3M", 1.000.000 → "1jt"/"1M", region US decimals, the narrow form, USD display and the masked "••"; `components/analysis/format.ts` no longer exports a display formatter.
- [ ] 3.3 Build `Meter` and `ChangeFigure` in `components/workspace/figures.tsx` (D24). Evidence: component tests for clamping 0–100, the tone token, ▲ in `--expense`, ▼ in `--income`, "sama", the hidden "naik"/"turun", the masked amount under hidden balances, and both languages; axe clean.
- [ ] 3.4 Build `ChartTable` in `components/charts/` and remove the dead `components/charts/theme.ts` (D24, D25). Evidence: a component test toggles chart and table with `aria-pressed` and keeps focus on the button; `rg 'charts/theme'` finds nothing.
- [ ] 3.5 Move the budget editor to `components/categories/BudgetDialog.tsx` on `ShellDialog` 560 (D9). Evidence: `BudgetPanel.test.tsx` passes against the moved dialog with the same `PATCH` requests; Laporan's capture shows the dialog with the SPEC frame and no other Laporan change.
- [ ] 3.6 Add the `analysis` namespace and the new `terms` keys to both catalogues, `/insights` to `lib/i18n/translatedRoutes.ts`, and remove `/insights` from `untranslatedPages.ts` (D22). Move the untranslated-page test example from Analisis to Pengaturan (`/settings`). Update the copy register roots and extend `SPENDING_ID` to `analysis.*`, with `analysis.flowDetail.out` in the allowed direction keys. Evidence:
  - the catalogue parity test and `type-check` pass, and a planted missing English key fails both;
  - an English Analisis has no notice and no `lang="id"` region; an English Laporan still has both;
  - a planted Indonesian "Belanja harian" label and a planted English "Spent" in `terms` each fail the copy register.
- [ ] 3.7 Tokens (D7, D11, D25). Evidence: `tokens.test.ts` adds `--k-none`, `--m-expense` and the four marker colours (`--m-income`, `--m-transfer`, `--m-invest`, `--m-warning`) against `--s0`, `--s1` and `--s2` in both themes at ≥ 3:1, and names the selected-cell inks D11 needs; the `--heat-1..5` tokens and their pairs are removed and `rg -- '--heat-'` finds nothing.

Expected Result: every shared piece and every message exists and is tested before a page uses it.

## 4. Ringkasan

- [ ] 4.1 Page frame, title block, segment, stepper with the period's dates, the rolling note and the tabs (D1–D3). Evidence: page tests for the four `analysis-workspace` tab scenarios and "Underline tabs and the report link"; the stepper reads "Siklus 23 Okt – 24 Nov", "Cycle 23 Oct – 24 Nov" and "Oct 2026" in tests; no `useWidthAtMost` or container query is left in Analisis (`rg`).
- [ ] 4.2 Hero with one "Rincian" (D4) and the comparison rows (D5, or its fallback). Evidence: tests for "Keluar equals Beranda", "Explanation of Belanja", "Older API without explanations", "No KPI row", "Running period", "Completed period", "No average yet"; no `ExplainableFigure` import in `components/analysis` (`rg`).
- [ ] 4.3 Kakeibo per pilar rows (D6). Evidence: tests for "Kakeibo against the same day" and "Unclassified spending" (the amount link and "Perlu dipilih" with `/laporan?lengkapi=1` and the window); whole-row links carry the period and pillar.
- [ ] 4.4 Pengeluaran harian chart and table (D7). Evidence: tests for "Daily bars", "Hovering a bar", "Tapping a bar" and "Labels at every width" (rendered at 1440, 1024, 390 and 320 widths with no overlapping label boxes); the bar width equals `max(4, floor((W − 8)/n) − 4)` in a unit test; the legend names both encodings; axe clean on the chart and the table.
- [ ] 4.5 Pengeluaran per kategori (D8). Evidence: tests for the four "Spending by Category on Ringkasan" scenarios, "No budget set" and "Uncategorised row"; the archived row shows "diarsipkan".
- [ ] 4.6 "Kelola kategori" and "Atur anggaran" dialogs (D9). Evidence: the seven existing category-management tests move to the dialog and pass with the same requests (create with icon, colour, pillar and budget; the explicit `null` pillar; no budget columns without budgets; income without expense inputs; loaded values; archive confirmation; restore); "Setting a category budget limit" and "Clearing a budget" pass; focus returns to the opening button; axe clean in both themes.
- [ ] 4.7 Context panel (D10). Evidence: tests for "Bills panel", "Report link" and "One Rincian per page"; "Jadikan rencana" still opens the prefilled plan form only after confirmation (existing observed-insights test moved).

Expected Result: Ringkasan matches the prototype and keeps every figure and action.

## 5. Kalender

- [ ] 5.1 Hero, grid, markers, heat strip and legend (D11). Evidence: tests for "Thresholds in the legend", "Strip follows the server level", "Keyboard walk", "Keys without single-key shortcuts", "Expected salary unavailable" and the cell's accessible name; the axe run of "Marker contrast" at 1440, 1024, 768, 390 and 320 in both themes reports no `color-contrast` finding.
- [ ] 5.2 Day panel and "Sampai akhir periode" (D12). Evidence: tests for "Waiting row link", "Open a transaction" (expense in `--out-fg`, focus back) and "Transaksi for the day" (`from`/`to` of that day only).
- [ ] 5.3 The phone grid (D13). Evidence: tests for "Phone grid" and "Narrow phone"; captures at 390 and 320 in both languages with no overflow; `WeekStrip` is gone (`rg`).

Expected Result: Kalender matches the prototype, its markers pass contrast, and the phone shows the whole month.

## 6. Peta Aliran

- [ ] 6.1 Hero, supporting figures, "Sorot aliran" and "Peta · Tabel" (D14). Evidence: tests for "Figures from the summary" and "Highlight in the table"; "Belanja" no longer appears in the flow's labels.
- [ ] 6.2 The four-column map, gutter, patterns and legend (D15). Evidence: the existing `sankey.ts` conservation and ordering tests pass unchanged; new tests for "Labels inside the frame" (label boxes inside the viewBox at 1440, 1200 and 1024 with a 40-character name), "Bands and legend" and "Balances off the map"; "Keyboard node walk" passes.
- [ ] 6.3 Table and the details dialog (D16). Evidence: tests for "Table complete", "Phone", "Link rows in the drawer", "Link detail on a wide window" and "Node detail on a phone"; no `Drawer` import in `components/analysis` (`rg`).
- [ ] 6.4 Remove the replay and the fold (D15, D17). Evidence: no request with `through` from the web in the page tests; `rg 'REPLAY_MS|flow-replay|analysis.flow.folded|MiniMap|mapWide'` finds nothing; the backend flow tests for `through` still pass.

Expected Result: Peta Aliran matches the prototype with every label inside the frame and every link in the table.

## 7. Moves and removals

- [ ] 7.1 Move "Seberapa sering ditanya" to Pengaturan › Kebiasaan and remove "Kebiasaan dihafal" (D18). Evidence: `QuestionRate.test.tsx` passes at its new path; tests for the three `habit-learning` scenarios, "Not on Analisis" included; `HabitsSettings.test.tsx` drops the "Kebiasaan dihafal on Ringkasan" block; a capture of `/settings/kebiasaan` in both themes.
- [ ] 7.2 Remove the elements of D25 that have no user left. Evidence: `rg` finds no `CategoryBudgetSection`, `HabitsLearnedCard`, `ObservedInsights`, `ProgressCard`, `BaselineCard`, `ProgressBand`, `UpcomingCard`, `components/insights` or `KpiStrip`/`Kpi` import in Analisis; `ReportLayout`, `ComparisonParts`, `KpiStrip`, `Kpi` and `ExplainableFigure` still build for Laporan and Rekening; no unused export is reported by `npm run lint`.

Expected Result: nothing of the old page remains, and no feature was lost.

## 8. Verification gate

- [ ] 8.1 Side by side (D26). Evidence: prototype and app captures with composed left/right images for every D26 scenario at 1440 × 900, 1024 × 768 and 390 × 844, light and dark, `en` and `id`, in `research/evidence/apply-markas-analisis/after/` and `compare/`.
- [ ] 8.2 Measurement checklist and SPEC §13 rules. Evidence: `analisis-checklist.js` passes on the prototype (dry run) and on the app with the item counts recorded; first content at `y = 192`; one 44 px figure per tab; sizes, weights and ≤ 8 text styles; no dotted underline; no border ≥ 2 px except the allowed marks; no horizontal overflow at 1440, 1280, 1200, 1024, 768, 390 and 320 on any tab or dialog.
- [ ] 8.3 Accessibility. Evidence: axe-core (wcag2a, wcag2aa, wcag21a, wcag21aa, wcag22aa) with zero serious or critical findings in both themes and both languages at 1440, 1024, 390 and 320: each tab, each dialog open, hidden balances, and loading, failure and empty states; zero console errors; correct `lang` attributes; 200 % and 400 % zoom captures reflow without horizontal scroll; reduced motion has no animation.
- [ ] 8.4 Keyboard flows. Evidence: `analisis-keys.mjs` runs the D26 flows in both languages at 1440 and 390, every flow passes, recorded with the revision.
- [ ] 8.5 Figure parity. Evidence: `parity.mjs` and a `verification.md` table with zero changed values: before (1.3) against after with the same seed, and `en` against `id` (region `ID`), byte-identical figure strings; intended additions (`baseline_share`) listed separately and checked against `share_percent` of the shown amounts.
- [ ] 8.6 Known items. Evidence: a `verification.md` section proves each of the five items fixed: the Kalender axe runs, the flow label positions, the English stepper strings, the 320 px overflow list empty, and no dotted figure on Analisis.
- [ ] 8.7 Stress and USD seed at 1440, 1024, 390 and 320 in both themes and both languages. Evidence: captures with no clipping, overlap or horizontal scroll for 40-character account names, 60-character merchants, Rp 1.234.567.890, 30 categories, 12 charges and the USD display; recorded in `verification.md`.
- [ ] 8.8 Frontend and repository checks on the exact revision. Evidence, each recorded with the commit hash:
  - `npm test`, `npm run type-check`, `npm run lint`, `npm run build`, `git diff --check` and `openspec validate --all --strict` pass;
  - the full backend suite passes on migrated disposable PostgreSQL (when 2.1 applied);
  - `git diff --stat origin/main -- db mobile nginx` is empty, and `git diff --stat origin/main -- backend` lists only `services/monthly_comparison.py` and its test.
- [ ] 8.9 Complete `verification.md` and present the result. Evidence: it names what changed, the gate counts, the deviations, the flag outcomes and what was not verified and why; the deviations and the moved Pengaturan panel are added to the owner's end-review list in `PROJECT_STATE.md`; the report to the owner quotes them.

Expected Result: the phase passes the plan's common gate and the both-language gate on its exact revision, with every deviation recorded.

## Workflow follow-up

- Archive: `openspec archive apply-markas-analisis`, then `openspec validate --all --strict` must pass. The archive path is under `openspec/changes/archive/`.
- Commit with subject-only Conventional Commits messages, for example `feat(analysis): apply Markas+ to Analisis`, and push `feat/markas-analisis`.
- Create the pull request into `main` with `gh pr create` (the owner's standing instruction of 2026-11-06), specs and code together, and report its URL with the pushed revision. The description names the read-only `progress.baseline_share` field. Merging stays with the owner.
- After merge: delete the branch locally and on the remote, and remove the `cash-flow-tracker-markas-analisis` worktree.
- Record in `PROJECT_STATE.md` that phase 7 (`apply-markas-laporan`) starts from this phase's merged state.
