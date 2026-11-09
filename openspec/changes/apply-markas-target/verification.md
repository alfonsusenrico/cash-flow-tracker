# Verification: apply-markas-target (Markas+ phase 9)

Branch `feat/markas-target`. Acceptance basis: the owner's standing instruction "complete all until finished"
(2026-11-06, renewed 2026-11-08, and "continue all" on 2026-11-09); every flagged decision took its recorded default.
Local verification only, synthetic data, disposable databases. No production access, no paid service or AI run.

## Approval, flags and dependencies (tasks 1.1, 1.2)

- **Flags D3, D7, D8, D9, D14, D16, D20:** all take their defaults (server `coverage_pct` and `target_amount` for the
  bar and Target; one flagged goal leaves the list; no colour field, stored colour sent unchanged; paid-off debts read
  with `include_archived=true` under their own key; credit bills beside the plans and outside the total and Terjadwal;
  a due credit bill opens Perpindahan prefilled; the shared Rencana dialog rebuilt as list and detail). None needed its
  fallback.
- **Base:** the plan was committed at `f8323d8` on old main `0308b7b`, then `origin/feat/markas-rekening` was merged
  (`ab459f6`), after PR #93 and #94 merged `origin/main` `98cdb49` (`ba89278`), and after phase 6 (#95) merged
  `origin/main` `3cbd371` (`6235452`; conflicts in the catalogues, `globals.css`, the route registries and the copy
  register resolved by keeping both phases' additions; phase 6's new `BillsPanel` now opens `PlansDialog`). `localize-server-text`, phase 5,
  `add-credit-spending-flow` (#91) and phase 8 (#94) are ancestors of the branch.
- **Credit API as planned** (on the seeded disposable database): `GET /api/credit/bills?from&to&status` with
  `due_dates_unavailable`; `GET /api/accounts` credit rows with `credit_owed`, `limit_left`, `over_limit`, `next_due`;
  `scheduled.lines` of a credit-bill plan with `plan_amount` 434.000, `counted_elsewhere` 124.000, `amount` 310.000;
  `spending.credit` 539.000 spent on credit and 12.000 paid to credit (seed report `env/logs/seed-tg-cft_markas_tg.json`).
- **Phase 5 and 8 pieces:** `ProgressBar`, `GainText`, `creditFacts`, `AccountDetailDialog({ accountId, onClose })`,
  `openMovementCapture`, the `terms` keys and `RowAmount`/`KindAmount` exist as planned. Differences, recorded in
  design.md "Deviations recorded at implementation" (docs commit `73b495a`): `ExplanationDialog` `link` was already
  optional; the untranslated-page test example stays on `/settings`; `ShellDialog` gained `initialFocus`; the
  frontend `PlanLine` and `CycleSummary.spending` types lacked the credit fields (added as optional); bell items needed
  an `onSelect`; the Rincian's parts are one level deep.

## For PROJECT_STATE.md

- 2026-11-09: phase 9 `apply-markas-target` built on `feat/markas-target` on top of `origin/main` `3cbd371` (phase 6
  merged as #95 and merged into this branch at `6235452`), archived
  and pushed without a PR (coordinator). The owner opens the PR.
- `RecurringRulesModal` is now `PlansDialog` (`components/recurring/PlansDialog.tsx`, same props plus `planId`);
  every caller, including phase 6's `components/analysis/BillsPanel.tsx` and the `app/insights` test mock, uses it.
- Shared additions other phases can use: `components/workspace/Field.tsx` (moved out of `AccountDialogs`),
  `lib/creditBills.ts` (`creditBillsPath`, `dueCreditBills`), `queryKeys.obligationsWithArchived`,
  `queryKeys.transactions.creditBills`, `WorkspaceNotification.onSelect`, `RincianLine.plain` / `total.plain`.
- End-review list for the owner: the deviations below and the seven flags above (all defaults).

## Environment

- Disposable `postgres:16` `cft-target-db` (127.0.0.1:9788): `cft_markas_tg` (id), `cft_markas_tg_en` (en, same data),
  `cft_markas_tg_stress` (id, USD display, stress additions), each migrated V1 → V51, `seed_parity.py`, → V54,
  `seed_rekening.py`, `seed_target.py`. Synthetic owners `dimas_contoh` and `kosong_contoh` (no history: unknown
  essential cost).
- APIs 8741 (id), 8742 (en), 8743 (stress) from this worktree's backend with AI, jobs and outbound HTTP off. Production
  build of this worktree on 3771 behind loopback proxies 3770 (id), 3776 (en), 3778 (stress); the `98cdb49` build
  (`git archive` into the scratchpad) on 3773 behind 3772, 3774, 3779 for "before"; fault proxies 3780–3783 for states.
  Scripts and README in the session scratchpad `markas-target/env/`.

## Seeds (task 1.4)

`scripts/verify/markas/seed_target.py` (new, loopback only, through the API, refuses a database not named
`cft_markas_*`): 9 additions in id, 10 in en, 139 with `--stress`, 0 skips. Added: the flagged Dana Darurat also linked
to parent Mandiri Contoh and its pocket Belanja Bulanan (counted once: Rp 21.550.400), Kamera Baru Contoh (no date,
manual), Utang Arisan Contoh paid off through the API (archived by the server), Iuran Kebersihan Contoh (due today), the
detected charge Langganan Video Contoh, and `kosong_contoh`. Already present from the parity and Rekening seeds: a
reached goal, a standalone dated goal, a paid-off debt, an overdue plan, the credit-bill plan Tagihan SPayLater, Kartu
Kredit Contoh bills overdue (Rp 15.000) and due today (Rp 300.000), SPayLater due today (Rp 434.000), Kredivo Contoh
without due dates. Stress: 40 goals (one 60-character name, one Rp 1.234.567.890 target), 25 debts (10 paid off), 60
plans, five credit accounts with long names. `seed_parity.py` is unchanged, so `test_seed_parity.py` was not re-run.

## What changed

- **Page (`components/goals/`):** `GoalsView` (title actions Rencana and Tambah target, "…" with Tambah utang and
  Ubah/Buat target dana darurat; hero, then `.cols` with the reading column and the panel; `?target=` and `?utang=`;
  focus return; stale ids cleared), `TargetHero` (server `coverage_months`, basis line, 8 px bar of `coverage_pct`,
  four figures, two on a phone, words when unknown), `safetyNet.ts` (the Rincian: reserve with its goals, essential
  cost with each period linked to the ledger, basis notes, coverage), `BillsSection` + `bills.ts` (today's rows plus
  status words, schedule, "perkiraan", five rows and the toggle; credit bills outside the count and total; a
  credit-bill plan shows `plan_amount` and its counted part), `GoalsSection` (goal rows, D7), `DebtsPanel` (debt rows,
  credit rows opening `AccountDetailDialog`, "Sudah lunas · N"), `useGoalData` (one query per section).
- **Dialogs on `ShellDialog`:** `GoalDialog` 560 (progress figure and bar, form with "Sumber progres", picker with
  the parent and pocket rules and "Total saldo terhubung", emergency checkbox, backing confirmation, "Arsipkan"
  confirmed in the footer), `GoalProgressDialog` 480, `DebtDialog` 560 (left, bar, lifecycle note or due line, form,
  focus on the failing field, "Kaitkan atau catat pembayaran", "Arsipkan"), `DebtPaymentDialog` 560 (candidates first,
  then the manual record). Every request body is today's (asserted in `app/goals/page.test.tsx`).
- **Rencana & Pengingat:** `components/recurring/PlansDialog.tsx` (880, filter, list and detail, the plan form,
  activate/deactivate/delete, the due occurrence via `DuePlanActions`), `DuePlanActions.tsx` extracted from
  `PendingScheduledBanner` with unchanged requests; `RecurringRulesModal` removed; the five callers changed only their
  import.
- **H9 on Beranda and the bell:** `todoItems` ranks unpaid credit bills with plans of the same urgency (by due date,
  then name) and opens Perpindahan through `openMovementCapture({ targetAccountId, amount: left_to_pay })`;
  `planNotifications` lists them once per account and due date, not when a listed plan pays the same bill;
  `spendingExplanation` appends "Pengeluaran pakai kredit" and "Tagihan kredit dibayar" with their accounts.
- **Language:** `goals` (176 messages) and `plans` (98) namespaces, the H9 keys in `home`, `shell` and `breakdown`;
  `/goals` in `TRANSLATED_ROUTES`, its `untranslatedPages.ts` entry removed, the title from the catalogue; the copy
  register's translated roots gain `app/goals`, `components/goals`, `components/recurring`, and its spending-term check
  covers `goals` and `plans`.
- **Removed:** `GoalDialogs.tsx` (`useGoalDialogs`, drawers, colour swatches), the trio cards and goal tiles,
  `RecurringRulesModal` and its test, `EmergencyCoverageBasis` and `coverageTone` (the basis lives in the Rincian; the
  types, `coverageText` and `essentialBasisText` stay for Beranda), the six `breakdown` keys only it used, the Target
  rules of `money.module.css` (the file stays for Pengaturan), `ws-dana-big` and `ws-dana-foot`. `ws-bar` stays:
  `/komponen` still draws it.

## Measurement, accessibility and states (tasks 6.1 to 6.3, 6.6)

Gate on the final build (`6235452` code, production build, both languages), session scratchpad `markas-target/final/`:

| Run | Captures | Checklist (`target-checklist.js`) | axe serious/critical: page / open dialog alone | Console errors | Horizontal overflow |
| --- | --- | --- | --- | --- | --- |
| `final/id`: 21 scenarios × 1440, 1024, 390, 320 × light, dark, plus hidden balances | 170 | 2702 pass, 0 fail | 4 / 0 (see below) | 0 | 0 |
| `final/en`: the same | 170 | 2702 pass, 0 fail | 2 / 0 (see below) | 0 | 0 |
| `final/wide-{id,en}`: page at 1280, 1200, 1199, 768, 767 × light, dark | 10 + 10 | 404 + 404 pass, 0 fail | 0 | 0 | 0 |
| `final/stress-id` (USD, 40 goals, 25 debts, 60 plans, five long credit names): 6 scenarios × 4 widths × 2 themes | 48 | 1232 pass, 0 fail | 0 | 0 | 0 |
| `final/states-{id,en}`: loading, two failure sets, empty, unknown cost (`kosong_contoh`), 1440 and 390 | 10 + 10 | as expected (no hero figure or Rincian while loading or failed) | 0 | only the injected 500 responses (3 per failure capture) | 0 |

The checklist covers `.tg-cols` 1fr/352 gap 24 from 1200 and one column below, first content at y = 192 on 1440, one
44 px figure and no other text ≥ 33 px outside dialogs, the hero bar 8 px at margin-top 24, four (two on a phone)
supporting figures, section headers, 56/64 px rows with the 40 px time column, 72 px goal rows with a 4 px bar capped
at 320 px, stacked panel rows, panel padding 24/16 and radius 12, dialog widths 480/560/880 and phone sheets, font
sizes and weights, borders and status colours. An earlier run of the same checklist on `f560882` (the plans fork's and
gate fork's first pass) is in `after/`, `after-wide/`, `stress/`, `states/`.

- Found and fixed during the gate: the goal bar was not capped at 320 px (a more specific `.r-main.col > *` rule won;
  now `.row.goal-row .r-main.col > .prog`); the hero skeleton's figures sat on the bar placeholder (missing
  `tg-supports`); at 320 px the hero figures wrapped after "Rp" (now 16 px on one line below 360 px); the
  missing-due-date line read as an error (now muted); focus after archiving now goes to the goal or debt list.
- The axe findings are all `target-size (1) .ws-btn-quiet` with Perpindahan open as a phone sheet over Beranda (id at
  390 and 320, en at 390): the flagged control is Beranda's "Rincian" behind the sheet; axe on the open dialog alone
  finds 0.
- English pages have no untranslated notice and no `lang="id"` part, except the Catat form (Perpindahan), which phase
  11 translates.
- States: loading skeletons with `aria-busy`; each failure
  (net worth, cycle summary, goals, obligations, credit bills) shows its own error box or line with "Coba lagi" while
  the other sections keep working; empty lines and "Tambah target"; the unknown-cost owner sees "Belum dapat dihitung"
  with no months, bar or days. Console errors only for the injected 500 responses. axe 0 serious.

## Keyboard flows (task 6.4)

`target-keys.mjs`, the 13 flows of D26 at 1440 and 390 on the final build (`final/keys/{id,en}`): **Indonesian 36/36,
English 36/36**. Flows 6, 11 and 12 assert that no write request is sent (the backing confirmation stops before
saving; Perpindahan from "Perlu tindakan" and from the bell opens with "Ke" and the amount and closes with Esc without
a request).

## Figure parity (task 6.5)

`target-parity.mjs` (`markas-target/parity/`, `markas-target/final/`):
- **Before (`origin/main` `3cbd371`, rebuilt from `git archive`) against after (`6235452`), Indonesian, same seed
  within the same minutes (the same comparison against `98cdb49` gave the same result):** 34 values, 2 changed, both
  named: the credit-bill plan row "Tagihan SPayLater" shows its `plan_amount` Rp 434.000 (was the counted Rp 310.000)
  with "Rp 124.000 sudah dihitung di Pengeluaran" (D14), and the flagged Dana Darurat left the goal list (D7). Equal:
  9 bulan, Rp 21.550.400, Rp 14.387.520 (= server `target_amount`), Rp 2.397.920, 273 hari, Tagihan "7 · Rp 1.130.000"
  (the unpaid credit bills are outside it), every goal and debt amount and %, Beranda Terjadwal Rp 5.498.334 and
  Pengeluaran Rp 3.530.490 (Rp 3.502.490 against `98cdb49`: phase 6's spending basis moved it on `main`, equally
  before and after).
- **Bar (flag D3), stress seed:** browser division 60,4551 % before, `coverage_pct` 60,5 after (0,0449 apart, within
  0,1); 214 values compared, the only other change is D7. The main seed's coverage is 150 %, capped at 100 % in both.
- **English against Indonesian** (region ID) on the final build: 34 differences, all words; 0 figure differences.
- **Requests on `/goals`:** 13 before, 15 after: `GET /credit/bills` for the page's window and the shell's two-day
  window, and `/obligations?include_archived=true` in place of `/obligations`.

## Copy (tasks 4.1 to 4.4)

- Catalogue parity (same keys and arguments, `one`/`other` in English) and both copy registers pass.
- Indonesian rendered text before against after (`parity/copy-diff-id.json`): 111 strings kept, 21 differ only by case
  (the D22 sentence-case labels, and labels the CSS upper-cases), 139 left and 121 arrived. Left: the drawers, the
  Rencana tabs ("Daftar Rencana", "+ Tambah Rencana"), the colour swatches, "Ubah Tagihan", "Tambah Tagihan / Utang
  Baru", "🛡️ Tandai sebagai Dana Darurat (Emergency Fund)", "{n} bulan biaya pokok", "Minimal · {date}", "Tampilkan
  {n} utang lunas". Arrived: the hero lines and captions, "Utang & kredit", "Sudah lunas · N", the credit rows and
  lines, the dialogs' new labels ("Ubah target tabungan", "Ubah utang", "Tambah utang", "Sumber progres", "Pilih
  rencana"), and the plan rows' meta. Both lists also carry account and transaction names that the old drawers and the
  new dialogs show differently. Every listed string is an intended change of D22 or a deviation below.
- English checked against Appendix B; no label uses "Expense", "Outflow", "you" or "your" (copy register).

## Frontend and repository checks (task 6.7)

Code at `6235452` (merge of `origin/main` `3cbd371`); later commits are docs only.

| Check | Result |
| --- | --- |
| `npm test` (vitest) | 99 files, **1040 passed**, 0 failed |
| `npm run type-check` | 0 errors |
| `npm run lint` | 0 problems |
| `npm run build` (`next build --webpack`) | passes (the build the gate captured) |
| Backend `pytest` on a fresh `postgres:16` (`cft-target-pytest`, 127.0.0.1:9789) migrated V1 → V54 by `flyway/flyway:10` | **2188 passed**, 1 warning, exit 0, on `6235452`; container removed |
| `openspec validate --all --strict` | 85 passed, 0 failed before the archive; ARCHIVED_COUNT after it |
| `git diff --check origin/main` | clean |
| `git diff --stat origin/main -- backend db mobile nginx` | empty |
| `frontend/package.json`, `package-lock.json` | unchanged against `origin/main` |

## Deviations and notes for the owner's end review

1. Credit bills under Tagihan are read from today (D14), so an already overdue credit bill appears in "Perlu
   tindakan", the bell and its credit row, not in Tagihan.
2. Perpindahan opened for a credit bill fills "Ke" and the amount; the Catat form itself then preselects its default
   "Dari" account, as it does for phase 8's "Bayar tagihan". Nothing is posted until the owner saves (D16 said "Dari"
   empty; Catat is phase 11's).
3. Credit lines in the Pengeluaran Rincian name their accounts in the note instead of a nested disclosure.
4. Debt and credit amounts in "Utang & kredit" use phase 8's Utang style (red, minus sign, hidden "Utang").
5. The fund caption is the server's `coverage_pct` rounded to a whole percent; it can exceed 100 % ("150% dari
   target") while the bar is capped.
6. The unknown hero figure "Belum dapat dihitung" uses the shared 20 px words style, not 24 px.
7. "Sumber progres": choosing Manual clears the chosen accounts and Otomatis restores the stored ones; saving still
   asks for confirmation when the source changes.
8. The debt dialog shows the intro line only when adding a debt; editing shows the due line, or the lifecycle note for
   a paid-off debt.
9. Bill rows take their schedule word from `GET /recurring` (already read by the shell, no new request).
10. In "Perlu tindakan" and the bell, plans and bills due on the same day are ordered by name (D16); two plans due the
    same day used to keep the API order.
11. The Ketahanan Dana Rincian links each period by its label ("Oktober 2026 (31 hari)"), not by its amount.
12. The coverage total in the Rincian and the month and day counts stay visible while balances are hidden
    (`RincianLine.plain`, `total.plain`).
13. At 320 px the title block's "…" wraps under "Rencana" and "Tambah target".
14. "Rencana & Pengingat" (D20): the plan marks and route line moved from the old cards to the detail; activate,
    deactivate and delete are in the detail only; a saved edit keeps the plan open, a new plan returns to "Pilih
    rencana"; validation messages come from `plans.errors`; "Nominal" without "(IDR)"; sentence-case options; one form
    instead of two `FormSection`s; on a phone, choosing a plan moves focus to the detail heading; `initialFocus`
    reaches a named plan's row only when the list is cached.
15. The proposal's deviations 1 to 12 apply as written.

## Not verified

- The prototype side-by-side composites of task 6.1 were not produced; the measurement checklist compares against
  `styles.css` values, and the prototype scenarios added to `scenarios-prototype.json` were not run.
- The stress and USD seed exists for an Indonesian owner only; English stress captures were not run.
- Pointer-only interactions beyond clicks were not driven; keyboard flows cover the dialogs and menus.

## Owner acceptance

Pending the owner's end review under the standing instruction.
