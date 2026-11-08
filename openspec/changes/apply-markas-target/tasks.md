## 1. Approval, dependencies and baseline

- [ ] 1.1 Record the approval and the flagged review:
  - in `proposal.md` (already written: "Approved under the owner's standing instruction 'complete all until finished' (2026-11-06, renewed 2026-11-08); decisions take their recommended defaults") and in `PROJECT_STATE.md`, with the date;
  - the review outcome of the flagged decisions D3, D7, D8, D9, D14, D16 and D20 (default, or the recorded fallback) in `design.md` and the spec delta before any code.

  Evidence: both files name the date and each flag's outcome; `openspec validate apply-markas-target --strict` passes after the edits.
- [ ] 1.2 Confirm the dependencies and update the branch (D25). `localize-server-text`, `apply-markas-transaksi`, `add-credit-spending-flow` and `apply-markas-rekening` must be merged into `main`; stop and report if one is not. Then update `feat/markas-target` from `origin/main`. Evidence:
  - `git merge-base --is-ancestor` succeeds for the four merge commits;
  - `GET /api/credit/bills` and the credit fields of `GET /api/accounts` (`credit_owed`, `limit_left`, `over_limit`, `next_due`) answer on a migrated disposable database, and `scheduled.lines` carries `plan_amount` and `counted_elsewhere` for a credit-bill plan;
  - `ProgressBar`, `AccountDetailDialog`, `creditFacts`, the `terms` keys `debt`, `limitLeft`, `overLimit`, `leftToPay`, `billStatus.*` and `openMovementCapture` exist; every difference from the contract in design Context is written to `verification.md`;
  - an English Target & Tagihan shows "This page is in Indonesian for now.";
  - `openspec validate --all --strict` passes on the updated branch, and the revision is recorded in `verification.md`.
- [ ] 1.3 Capture the "before" state on that `main` revision, tied to its hash:
  - Target & Tagihan with the parity seed and the audit seed, in `en` and `id`, at 1440, 1280, 1200, 1199, 1024, 768, 767, 390 and 320 px, light and dark, with and without hidden balances;
  - every D26 scenario that exists before the change (the drawers, the goal and debt forms, the progress and pay dialogs, the Rencana modal from each caller, loading, failure and empty), Beranda's "Perlu tindakan", the safe-amount Rincian with Pengeluaran open, and the bell;
  - `parity-before.json`: the runway figures and the browser's bar width and target, each bill row's date and amount with the section's count and total, each goal's amounts and %, each debt's amounts and %, Beranda's Terjadwal and Pengeluaran, and the page's request count.

  Evidence: captures and the JSON in `research/evidence/apply-markas-target/before/`; the Indonesian rendered-text snapshot `copy-before-id.json` for task 4.4.
- [ ] 1.4 Extend the seeds (D26), local and disposable only: the parity seed additions of D26 (flagged goal linked to a parent and its pocket, reached, dateless and standalone goals, a paid-off debt, overdue and due-today plans, a detected charge, two credit accounts with unpaid bills due today and overdue, a credit-bill plan, credit purchases and a payment in the cycle), and the stress and USD seed of D26. Evidence:
  - `test_seed_parity.py` passes;
  - the seed report lists every addition and every skip with its reason;
  - on the seeded database `GET /api/obligations?include_archived=true` returns the paid-off debt, `GET /api/credit/bills?status=due_today,overdue` returns both bills, and `cycle-summary` has non-zero `spending.credit` amounts;
  - `git diff --stat origin/main -- backend db mobile` is empty.
- [ ] 1.5 Add the Target scenarios of D26 to `scenarios-app.json` and `scenarios-prototype.json` (`en` and `id` for the app), `target-checklist.js` for `measure.js`, and `target-keys.mjs`. Evidence: a dry run over the prototype at 1440, 1024 and 390 in both themes writes captures and a checklist JSON whose expected values equal D1 to D20 and the prototype's computed values; `target-keys.mjs --dry-run` lists the thirteen flows of D26.

Expected Result: the dependencies are on the branch, every section, state, dialog and H9 surface can be seeded and captured, and the pre-change state is recorded in both languages.

## 2. Shared pieces

- [ ] 2.1 Make `ExplanationDialog`'s `link` optional (D4, D23). Evidence: a component test renders a Rincian without a footer link and with one; Beranda's Rincian tests pass unchanged.
- [ ] 2.2 Extract `DuePlanActions` from `PendingScheduledBanner` (D20, D23) without changing a request or a confirmation. Evidence: `PendingScheduledBanner.test.tsx` passes unchanged (labels, undo, never posts by itself, ambiguous matches, missed payment after naming captures, skip with refusal); a new test renders `DuePlanActions` alone for one rule and sends the same link, skip and record requests.
- [ ] 2.3 Add `.stacked`, `.goal-row`, `.rc-grid` (and `.cols`, `.stack`, `.supports` where phase 3 has not) from `styles.css` with the phase 1 token names (D23). Evidence: the `/komponen` sheet shows a stacked row, a goal row and the 880 list/detail grid in both themes, and their checklist values pass there; `selectionMarks.test.ts` passes.

Expected Result: the shared parts this phase needs exist once, and Beranda and the bell behave as before.

## 3. H9 on Beranda and the bell

- [ ] 3.1 Rank due credit bills in "Perlu tindakan" (D16). Evidence: `app/page.test.tsx` cases:
  - spec scenario "Credit bill ranked with plans" (question, then the due-today plan and bill by due date and name);
  - an overdue bill ranks with overdue plans before awaiting transfers; a bill due tomorrow ranks with plans within two days, after the automatic-link check;
  - a paid bill (`left_to_pay` 0, `paid`, `paid_late`) is absent (spec "Bill already paid");
  - selecting the bill calls `openMovementCapture({ targetAccountId, amount: left_to_pay })` and Perpindahan shows "Ke" and the amount with "Dari" empty; no request is sent until "Simpan" (spec "Paying from Perlu tindakan");
  - the existing six "Perlu tindakan" tests pass unchanged.
- [ ] 3.2 List due credit bills in the bell (D16). Evidence: `workspace.test.tsx` cases: spec scenario "Due credit bill"; one item per `credit-{account}-{due_date}`; a bill matched by a credit-bill plan is listed once, as the plan; English titles; the existing notification tests pass unchanged.
- [ ] 3.3 Add the two credit lines to the Pengeluaran parts of the safe-amount Rincian (D17). Evidence: `explanations.test.ts` and `app/page.test.tsx` cases for spec scenarios "Credit lines" and "No credit use"; the Pengeluaran total and the dialog's total are unchanged; exactly one "Rincian" on Beranda.

Expected Result: H9's Beranda and bell parts work on today's Beranda without changing any figure.

## 4. Language

- [ ] 4.1 Add the `goals` and `plans` namespaces and the Beranda, bell and breakdown keys of Appendix A to `messages/en.json` and `id.json` (D22). Write the full key list to `research/evidence/apply-markas-target/copy-keys.md`. Evidence: the catalogue parity test passes (same keys, same arguments, `one`/`other` in English); `npm run type-check` fails on a planted missing key and passes without it.
- [ ] 4.2 Move every Target & Tagihan, goal, debt and plan string to keys, and every date, amount and month figure to `useFormat()`. This covers `components/goals`, `components/recurring` and the H9 parts of Beranda and the shell. Evidence: `rg 'id-ID|Intl\.DateTimeFormat|toLocaleString' frontend/src/components/goals frontend/src/components/recurring` finds nothing; formatter cases for `id-ID`, `en-ID` and `en-US` render "5,4 bulan" / "5.4 months", "2 Nov" / "2 Nov" and "Rp 434.000" / "Rp 434,000" (or the region's form).
- [ ] 4.3 Add `/goals` to `lib/i18n/translatedRoutes.ts`, remove its `untranslatedPages.ts` entry and `RecurringRulesModal`'s `untranslated` flag, and add the D22 roots to the copy register's translated roots. Move the untranslated-page test example, which phase 8 put on `/goals`, to the first of `/insights` and `/laporan` still untranslated on the updated branch (D22 rule). Evidence:
  - an English Target & Tagihan, each of its dialogs and "Rencana & Pengingat" opened from Analisis have no "This page is in Indonesian for now." line and no `lang="id"` region;
  - the page chosen for the example still has both, and the choice is recorded in `verification.md`;
  - the copy register passes in both languages, and one planted fixture per language fails: an Indonesian "Belanja" figure label in `goals`, and an English "Expenses" label in `plans`.
- [ ] 4.4 Check the copy. Evidence: the Indonesian rendered-text snapshot after the change equals `copy-before-id.json` for every unchanged key; the differences are exactly the intended ones of D22, listed in `verification.md`; every English string is checked against Appendix B, and no English label uses "Expense", "Outflow", "you/your" or slang.

Expected Result: the page, its dialogs and the Rencana dialog read from the catalogues in both languages, Indonesian reads as today except the listed changes, and `/goals` is a translated route.

## 5. Target & Tagihan page and dialogs

- [ ] 5.1 Compose the page and its breakpoints (D1, D2, D12): the title actions and "…", the hero, `.cols` with the reading column and the panel, viewport queries, the phone order. Remove `.trio` and its container rule. Evidence: `page.test.tsx` covers one `h1`, the DOM order hero → Tagihan → Target → Utang & kredit, the "…" items for one, none and several flagged goals, arrow keys and Esc in the menu; spec scenarios "Balanced desktop layout", "Order at 1024 px" and "Phone order".
- [ ] 5.2 Build the hero (D3). Evidence: tests for spec scenarios "Known coverage", "No designated reserve", "Phone figures", "Unknown essential cost" and "Zero runway on the hero"; the bar's width is `coverage_pct` and its name states the %; no figure is computed in the browser (`rg 'target_months \*|monthly_primary_expense \*' frontend/src/components/goals` finds nothing).
- [ ] 5.3 Build the Ketahanan Dana Rincian (D4) and remove `EmergencyCoverageBasis`. Evidence: tests for spec scenarios "Rincian" and "Basis in the Rincian" (periods linked with `type=expense`, `kakeibo=need` and the window, the unclassified link when above zero, limited history, minimum instalments); focus returns to "Rincian"; `EmergencyCoverage.test.tsx` keeps its `essentialBasisText` cases; `rg EmergencyCoverageBasis frontend/src` finds nothing.
- [ ] 5.4 Build "Tagihan 30 hari ke depan" (D5) with the credit bills (D14). Evidence: `bills.test.ts` keeps today's `billRows` case ("adds charges seen recurring that no plan covers…") and adds: five rows and the toggle, the salary-window title, the empty line, spec scenarios "Eight bills", "Unpaid SPayLater bill", "Credit-bill plan" and "Bills unavailable"; the header's count and total equal `parity-before.json`, and Beranda's Terjadwal is unchanged with an unpaid credit bill seeded.
- [ ] 5.5 Build "Target tabungan" (D6) and the emergency goal placement (D7). Evidence: tests for spec scenarios "Goal in words", "Reached goal", "One emergency goal" and "No emergency goal", several flagged goals listed with "Dana darurat", the empty state with "Tambah target", and `?target=` opening the dialog.
- [ ] 5.6 Build "Utang & kredit" (D9, D10, D15). Evidence: tests for spec scenarios "Debt row in the panel", "One paid-off debt", "Reversal reactivates", "Credit row" and "Over the limit and no limit"; Catat's debt picker and the plan form still request the active list only (query key check); `?utang=` opens the dialog; the credit row opens `AccountDetailDialog` and focus returns to the row.
- [ ] 5.7 Build the goal dialogs (D8, D18). Evidence: the existing `page.test.tsx` cases pass in their new place: "offers adjustment only for standalone goals and never posts a transaction", "requires confirmation before changing a standalone goal to account-backed progress", "changes the target of an account-backed goal without sending manual progress", "opens a goal from ?target= and confirms before archiving without touching an account"; new cases for spec "Progress source in the goal dialog" (the linked total counts a pocket and its parent once), "Archiving a goal", the edit request carrying the stored `color` unchanged and no colour control.
- [ ] 5.8 Build the debt dialogs (D19). Evidence: the existing cases "validates an obligation against the final total and remaining amounts" and "links an already recorded payment first and shows no payoff-time estimate" pass; new cases for spec "Paying a debt", the lifecycle note on a paid-off debt, "Arsipkan" with its confirmation and focus return.
- [ ] 5.9 Build "Rencana & Pengingat" (D20) and remove `RecurringRulesModal`. Evidence: the seven `RecurringRulesModal.test.tsx` cases pass against `PlansDialog` (weekly transfer plan, detected-charge prefill, refused delete, weekday edit, product shortcut plan, cleared category, credit for expenses only); new cases for spec scenarios "Editing a plan", "A due occurrence" and "Same entry points" from Rekening, Analisis, Beranda, the "Rencana jatuh tempo" view and a top-up; the full-height sheet at 390 px.
- [ ] 5.10 States, hidden balances and clean-up (D11, D13, D23, D24). Evidence:
  - tests for spec "Net worth fails", each section's own error box and skeleton with `aria-busy`;
  - with hidden balances every amount on the page and in each dialog reads "Rp ••••••" with the hidden name, and month and day counts stay;
  - `rg` finds no user of `RunwayCard`, `DebtsCard`, `BillsCard`, `GoalDrawer`, `DebtDrawer`, `useGoalDialogs`, `RecurringRulesModal`, `trio`, `goalTile`, `ws-dana-big`, `ws-dana-foot`, `ws-bar` (or each remaining user is named in `verification.md`), and the removed CSS is gone;
  - `selectionMarks.test.ts` passes.

Expected Result: Target & Tagihan and its dialogs are rebuilt to the prototype with every action, request and figure as before, and H9 is complete.

## 6. Verification gate

- [ ] 6.1 Side by side, on the parity seed: prototype and app captures of every D26 scenario at 1440 × 900, 1024 × 768 and 390 × 844, light and dark, the app in `en` and `id`, plus composed left/right images; add the overflow widths 1280, 1200, 1199, 768, 767 and 320, and the after-captures of the Rencana callers and Beranda of task 1.3. Evidence: the images in `research/evidence/apply-markas-target/after/`, tied to the branch revision, with every visible difference listed in `verification.md` and each one fixed, named as a deviation of the proposal, or added to the owner's end-review list.
- [ ] 6.2 The SPEC measurement checklist (`target-checklist.js`) and the SPEC §13 global rules (D26), in both languages. Evidence: the checklist JSON with pass and fail counts in `verification.md`, zero fails or each fail with its reason, including `.cols` at 1440, one column at 1199 and 1024, first content `y = 192`, one 44 px figure and no other ≥ 33 px, rows, panel and dialog measurements, sizes, weights and ≤ 8 text styles, no border ≥ 2 px and no coloured border except the bars and focus, status word colours per theme, and no horizontal overflow at every width of 6.1, with the English hero line and panel lines at 390 and 320.
- [ ] 6.3 Accessibility. Evidence: axe-core (wcag2a, wcag2aa, wcag21a, wcag21aa, wcag22aa) with zero serious or critical findings in both themes and both languages at 1440, 1024 and 390: the page with known, unknown and zero coverage, the bills expanded and "Sudah lunas" open; each dialog and the "…" menu open; Beranda with a credit bill and its Rincian open; the bell open; hidden balances; loading, failure and empty states. Zero console errors; no `lang="id"` region on an English page. Results in `verification.md`.
- [ ] 6.4 Keyboard flows: `target-keys.mjs` runs the thirteen flows of D26 in both languages at 1440 and 390. Evidence: every flow passes, recorded in `verification.md` with the revision.
- [ ] 6.5 Figure parity. Evidence: `parity.mjs` and a `verification.md` table with zero changed values:
  - before (task 1.3) against after, same seed within the same minutes: the hero months and four figures, the bar width (browser value against `coverage_pct`, equal within 0.1 %, flag D3) and the target (browser value against `target_amount`), each bill row and the section's count and total, each goal's and debt's amounts and %, Beranda's Terjadwal and Pengeluaran;
  - `en` against `id` (region `ID`) on the same data, byte-identical figure strings;
  - with the credit seed, Terjadwal and the Tagihan total are unchanged by the unpaid bills (flag D14).
- [ ] 6.6 Stress and USD seed at 1440, 1024, 390 and 320 in both themes and both languages. Evidence: captures with no clipping, overlap or horizontal scroll for 40 goals, 25 debts with 10 paid off, 60 plans in the dialog, five credit accounts with long names, 60-character names, Rp 1.234.567.890, the unknown-cost account and the USD display; recorded in `verification.md`.
- [ ] 6.7 Frontend and repository checks on the exact revision. Evidence: every result recorded in `verification.md` with the commit hash: `npm test`, `npm run type-check`, `npm run lint`, `npm run build`, `git diff --check` and `openspec validate --all --strict` pass; `git diff --stat origin/main -- backend db mobile nginx` is empty; the page's request count before and after is recorded.
- [ ] 6.8 Complete `verification.md` and present the result. Record the owner's standing instruction (2026-11-06, renewed 2026-11-08) as the acceptance basis, and add the deviations, the flag outcomes and the Rencana relayout seen from other pages to the end-review list in `PROJECT_STATE.md`. Evidence: `verification.md` names what changed, the gate counts, the deviations, the H9 coverage, and what was not verified and why; the report to the owner quotes them.

Expected Result: the phase passes the plan's common gate and the both-language gate on its exact revision, with every deviation and flag recorded.

## Workflow follow-up

- Archive: `openspec archive apply-markas-target`, then `openspec validate --all --strict` must pass. The archive path is under `openspec/changes/archive/`.
- Commit with a subject-only Conventional Commits message, and push `feat/markas-target`.
- Create the pull request into `main` (the owner's standing instruction of 2026-11-06, renewed 2026-11-08), and report its URL with the pushed revision. Merging stays with the owner.
