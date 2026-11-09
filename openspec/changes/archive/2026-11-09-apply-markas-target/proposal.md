## Why

On 2026-11-05 the owner chose design option 6 "Markas+" ("ok accepted") and expects the app to match it exactly. Target & Tagihan is the last money page still built from the pre-redesign cards inside the new shell.

The audit (`design-options/audit/AUDIT.md` §Target & Tagihan) measured why it reads scattered:
- three cards of different structure share one row: Ketahanan Dana with two grey sub-boxes, Utang with two grey sub-boxes, and a Tagihan list;
- four nested cards, and the goals follow as a grid of tiles with pills;
- the first card starts under a crowded header, and nothing says which figure matters most.

Five more issues belong to this phase:
- **The Ketahanan Dana bar is computed in the browser.** `RunwayCard` divides the emergency balance by `target_months × monthly_primary_expense`, although the server sends `coverage_pct` and `target_amount` (design D3).
- **"Utang lunas" can never show.** The page asks `GET /obligations` without `include_archived=true`, so the server never returns a paid-off debt and the toggle under Utang is dead in production; only the test mock feeds it (D9).
- **The phone order disagrees with its spec.** `mobile-native-layout` asks for Ketahanan Dana, then Target tabungan, Utang and the bills; the page renders Ketahanan Dana, Utang, Tagihan, Target. The prototype's order (hero, Tagihan, Target, Utang) replaces both (D12).
- **The emergency goal is invisible as such.** The goal flagged as Dana Darurat sits in the goal grid like any other target, and the only way to see that it is the reserve is the checkbox in its form (D7).
- **`EmergencyCoverageBasis` is dead code.** The basis component that `emergency-cost-estimation` requires is exported and tested but rendered nowhere; its content moves into the Ketahanan Dana "Rincian" (D4).

The credit change `add-credit-spending-flow` (archived on `feat/credit-spending-flow`, design D12) hands block **H9** to this phase: credit bills beside the plans, credit in Utang & kredit, due credit bills in Beranda's "Perlu tindakan" and the bell, and the two credit lines in Beranda's Pengeluaran "Rincian". It is server and API only; this phase renders it (D14 to D17).

Phases 1 to 3 (foundation, shell, Beranda), `add-category-system-keys` and `add-language-system` are merged. `localize-server-text`, phase 5 (`apply-markas-transaksi`), `add-credit-spending-flow` and phase 8 (`apply-markas-rekening`) merge before this phase (design D25). English is the default interface language, and every page rebuilt from phase 4 on ships English and Indonesian.

The phased plan is `cash-flow-tracker-prototypes/research/2026-11-05-markas-implementation-plan.md` (phase 9 of twelve). The fidelity source is `design-options/option-6/`: `styles.css` (it wins over `SPEC.md`), `js/pages2.js` `PAGES.target`, `js/dialogs.js` (`rincian:ketahananDana`, `target`, `target-tambah`, `utang`, `utang-tambah`, `rencana` and the `target` menu), `SPEC.md` §6 to §8, §11 and §13, and the captures `target-*`, `dlg-rincian-ketahanan-*`, `dlg-target-detail-*`, `dlg-target-manual-*`, `dlg-target-tambah-*`, `dlg-utang-*`, `dlg-utang-tambah-*` and `dlg-rencana-*`.

## Expected Outcome

- At 1440 px Target & Tagihan reads top to bottom as in the prototype:
  - the title "Target & Tagihan" with its subtitle, "Rencana" and "Tambah target", and one "…" with "Tambah utang" and "Ubah target dana darurat" ("Buat target dana darurat" when no goal is flagged);
  - one hero "Ketahanan Dana" in 44 px ("5,4 bulan") with the page's only "Rincian", one line ("biaya pokok tertutup dana darurat · target 6 bulan"), an 8 px bar of the server's coverage and four supporting figures: Dana darurat, Target, Biaya pokok / bulan, Jika pemasukan berhenti;
  - a reading column with "Tagihan 30 hari ke depan" (five rows, "dan N tagihan lain", "Kelola") and "Target tabungan" (one whole-row line per goal with a 4 px progress bar), and a 352 px panel "Utang & kredit" with each debt's progress, each credit account and "Sudah lunas · N".
- On a phone the hero shows two supporting figures, then Tagihan, Target and Utang & kredit follow in one column; every detail opens as a bottom sheet.
- When the essential cost is unknown, the hero says "Belum dapat dihitung" with the reason, and no number of months, bar or day count appears.
- The emergency goal leaves the goal list when exactly one goal is flagged; "…" › "Ubah target dana darurat" opens it. With no flagged goal, the hero line says the reserve is counted from all cash, as today.
- A credit bill that is not paid yet is listed under Tagihan as "Tagihan SPayLater" with its due date and what is left, marked "sudah dihitung di Pengeluaran". It is not added to the section's total or to Terjadwal. A plan that pays a credit bill shows its full amount and the part already counted.
- Each credit account is a row under Utang & kredit with what it owes, its limit left and its next bill; the row opens the account's detail from phase 8 in place.
- An overdue credit bill, one due today and one due within two days appear in Beranda's "Perlu tindakan" and in the bell, ranked with plans of the same urgency. Choosing one opens Catat's Perpindahan into that credit account with the amount left; nothing is recorded until the owner saves.
- Beranda's "Rincian" of the safe amount, under Pengeluaran, shows "Pengeluaran pakai kredit" as part of Pengeluaran and "Tagihan kredit dibayar" as cash out that is not counted again.
- Paid-off debts appear under "Sudah lunas · N" and open their dialog, which says they were archived automatically and become active again if a payment is reversed.
- Goals, debts, plans and the Ketahanan Dana basis open in centered dialogs on the prototype's widths: "Ubah target tabungan" 560, "Buat target tabungan baru" 560, "Ubah utang" 560, "Tambah utang" 560, "Rincian" 480 and "Rencana & Pengingat" 880. Every action sends exactly the request it sends today.
- An English account reads the page in plain English ("Goals & Bills", "Safety net", "Bills in the next 30 days", "Savings goals", "Debts & credit"). An Indonesian account reads today's Indonesian with neutral firm labels. The page no longer shows "This page is in Indonesian for now."
- With "Sembunyikan saldo" every amount reads "Rp ••••••" and is announced as hidden.
- Every count, amount, month figure and date equals today's value for the same data, in both languages. The coverage bar's width equals today's within rounding.

## Approval

Approved under the owner's standing instruction 'complete all until finished' (2026-11-06, renewed 2026-11-08); decisions take their recommended defaults.

Every decision, its default and the rejected alternatives are in `design.md` (D1 to D27, summarized in "Decisions and their defaults"). No default changes money logic: every figure is a server field shown as today, every action sends today's request, and no backend, schema or data change is made. Seven decisions are flagged because they touch money presentation or go beyond the phased plan's scope; each says why:
- **D3 (money presentation):** the coverage bar reads the server's `coverage_pct` instead of today's browser division. The value is the same rule; the gate proves the widths equal.
- **D7 (scope):** a single flagged emergency goal leaves the goal list, as the prototype and the plan say, and is edited from "…". With several flagged goals they stay listed with a "Dana darurat" meta. No goal, flag or progress changes.
- **D8 (scope):** the goal form drops the "Warna Label" swatches. No page shows a goal's colour since the redesign, so the field would be unused; stored colours are kept and never sent as changed.
- **D9 (scope correction):** "Sudah lunas · N" reads `GET /obligations?include_archived=true`, an existing read-only parameter. Today's toggle is dead because the active-only request never returns a paid-off debt. The debt lifecycle (archive on payoff, reactivation on reversal) does not change.
- **D14 (H9, money presentation):** credit bills sit beside the plans but are never added to the section total or Terjadwal; a plan that pays a credit bill shows its `plan_amount` with the `counted_elsewhere` part, and the bill and the plan are listed once.
- **D16 (H9, scope):** a due credit bill in "Perlu tindakan" or the bell opens Catat's Perpindahan prefilled with the account and the amount left, through phase 8's `openMovementCapture`. The owner still confirms; nothing is posted.
- **D20 (scope):** the "Rencana & Pengingat" dialog, shared by Rekening, Analisis, Beranda and the shell, is rebuilt as the prototype's 880 px list and detail. Its filters stay the app's (Semua, Cicilan, Rutin) and its requests are unchanged; a due occurrence of the selected plan shows the same link, skip and record actions as "Rencana jatuh tempo".

## Deviations from the prototype

Each is a decision in `design.md` and is captured for the owner's end review:
1. Debts carry no lender and no paid-off date in the app's data, so the debt rows' meta reads the due date and "N% terbayar", and a paid-off debt reads "lunas" without a date (D9, D10).
2. The credit row and the credit dialog show no "Minimal bayar": credit accounts have no minimum payment in the API. Phase 8 records the same for the account detail (D15).
3. "Rencana & Pengingat" keeps the app's filters "Semua · Cicilan · Rutin" instead of the prototype's "Semua · Terjadwal · Terlewat · Dibayar". The app's plans have no "dibayar" state; a plan's next date and due occurrence give "terlewat" and "jatuh tempo hari ini" in the row meta instead (D20).
4. The goal dialog keeps the account picker with parent and pocket rows, the "Total saldo terhubung" line and the confirmation when the progress source changes, inside the prototype's "Sumber progres" segment (D18).
5. The goal dialog keeps "Tambah progres" for a standalone goal as its own small dialog, which says that no account changes; the prototype's "Riwayat setoran" list is not drawn, because the app stores no deposit history (D18).
6. The debt dialog shows "Kaitkan atau catat pembayaran" instead of the prototype's list of linked payments, because no endpoint lists a debt's payments today; the pay dialog (link first, then manual record) is kept (D19).
7. The debt dialog's footer action is "Arsipkan" with its confirmation, as today. The prototype says "Hapus" (D19).
8. The hero line names the basis: "biaya pokok tertutup dana darurat" or, without a flagged goal, "Belum ada dana darurat khusus — dihitung dari seluruh saldo kas", which `emergency-cost-estimation` requires (D3).
9. The Dana darurat supporting figure's caption is "{n}% dari target"; the prototype's "· 2 sumber" is not shown, because the count of sources depends on the parent and pocket rule and would add a second number to verify (D3).
10. Tagihan rows include the detected recurring charges no plan covers, marked "perkiraan", as today (D5).
11. A credit bill row and the credit row in Utang & kredit come from H9, which the prototype draws only as one "SPayLater · kredit" row (D14, D15).
12. The Ketahanan Dana "Rincian" keeps the app's basis lines (reserve, essential cost, the periods with their ledger links, minimum instalments, limited history) instead of the prototype's three plain lines (D4).

## What Changes

- **Title block:** the subtitle from `add-language-system`, "Rencana" (secondary, calendar icon) and "Tambah target" (secondary, plus), and one "…" menu with "Tambah utang" and "Ubah target dana darurat" or "Buat target dana darurat" (D2, D7).
- **Hero (D3):** the shared `HeroFigure` with "Ketahanan Dana", "Rincian", `coverage_months` as "5,4 bulan" in Manrope 44, the basis line, the shared 8 px `ProgressBar` of `coverage_pct`, and four `FigureBlock`s: Dana darurat (`emergency_fund_balance`), Target (`target_amount`, "{target_months} × biaya pokok"), Biaya pokok / bulan (`monthly_primary_expense`, its basis), Jika pemasukan berhenti (`runway_days` "hari"). Two on a phone. The unknown state shows words only.
- **"Rincian" (D4):** the shared `ExplanationDialog` (480) with the reserve balance and its goals, the essential cost and its periods linked to the ledger, the minimum instalments, the coverage line and the basis notes. `EmergencyCoverageBasis` is removed into it.
- **Tagihan 30 hari ke depan (D5, D14):** a `SectionHeader` with "{n} · {total}" and "Kelola", today's `billRows` as shared `ListRow`s with a 40 px date column, status words (terlewat in the expense text, jatuh tempo in warning) and "perkiraan" for detected charges; five rows and "dan N tagihan lain" / "Tampilkan lebih sedikit". Unpaid credit bills from `GET /api/credit/bills` join the list as "Tagihan {account_name}" with `left_to_pay` and "sudah dihitung di Pengeluaran", outside the total. A credit-bill plan line shows `plan_amount` and its `counted_elsewhere` part.
- **Target tabungan (D6, D7):** one whole-row button per goal (min-h 72): name, meta "sampai 1 Apr 2027 · Rp 2.580.000/bln · 1 kantong" ("Tercapai", "Tanpa tanggal", "dicatat manual"), a 4 px progress bar (ink, income green when reached), and "Rp 5.100.000" with "dari Rp 18.000.000 · 28%". Empty: "Belum ada target tabungan." with "Tambah target".
- **Utang & kredit (D9, D10, D15):** the shared `Panel` with a `SectionHeader` "{n} aktif", one stacked row per active debt (name, what is left, 4 px paid bar, due date, "sisa · N% terbayar"), one stacked row per credit account (H9) and "Sudah lunas · N" listing paid-off debts.
- **Dialogs (D18 to D21), all on `ShellDialog`:**
  - "Ubah target tabungan" (560): the progress figure and bar, "Perlu {pace} per bulan sampai {date}", the goal form, "Tambah progres" for a standalone goal, "Arsipkan" with confirmation;
  - "Buat target tabungan baru" (560), the same form;
  - "Tambah progres" (480), unchanged rule;
  - "Ubah utang" (560): what is left, the paid bar, the lifecycle note, the debt form, "Kaitkan atau catat pembayaran", "Arsipkan";
  - "Tambah utang" (560);
  - "Pembayaran: {name}" (560): link a recorded expense first, then record manually, unchanged requests;
  - "Rencana & Pengingat" (880): list and detail, the plan form, the due occurrence's actions; its props stay for every caller.
- **H9 on Beranda and the bell (D16, D17):** credit bills join "Perlu tindakan" and the bell's due items; the Pengeluaran line of the safe-amount "Rincian" gains the two credit lines.
- **Language (D22):** a `goals` namespace and a `plans` namespace (the Rencana dialog) in `messages/en.json` and `id.json`; `/goals` added to the translated routes and removed from `untranslatedPages.ts`; the copy register's translated roots gain the page, `components/goals` and `components/recurring`.
- **Removed** (each with its destination in D24):
  - the three-card row `.trio`, `RunwayCard`, `DebtsCard`, `BillsCard` and the goal tiles grid (`goalTiles`, `goalTile`, `goalTop`, `goalPct`, `goalMeta`, `goalSources`);
  - the goal status `Pill`s, `Card` and `CardHeader` on `/goals`;
  - `GoalDrawer` and `DebtDrawer` (into the dialogs);
  - the browser's coverage division and the client sum of debts in a big figure;
  - the dead "Tampilkan {n} utang lunas" toggle (replaced by a working "Sudah lunas · N");
  - `EmergencyCoverageBasis` (into the "Rincian");
  - the goal colour swatches (flag D8);
  - `ws-dana-big`, `ws-dana-foot` and `ws-bar` where no user remains, and the Target rules of `components/accounts/money.module.css` (the file goes when phase 8 has removed its other users);
  - the `/goals` entry of `untranslatedPages.ts`;
  - the tabs "Daftar Rencana" / "+ Tambah Rencana" of the Rencana modal (into list and detail).
- **BREAKING (spec):** `financial-goals-tracker` "Target & Tagihan Layout" and "Goal Progress Action Presentation", `obligations-debt-tracker` "Payoff Progress & Estimated Timeline", `mobile-native-layout` "Phone Target & Tagihan Composition", `net-worth-analytics` "Cash Flow Runway Forecast" (item 4), `emergency-cost-estimation` "Essential cost basis disclosure", `beranda-cycle-summary` "Kerjakan Dulu" and `executive-sidebar-layout` "Notifications Include Due Plans" change their presentation. `credit-accounts` gains the four H9 requirements.

## Non-goals

- No backend, API, schema, migration or data change, and no change to the Android companion. `include_archived=true` and every credit field already exist (the credit change). `db/`, `backend/` and `mobile/` stay untouched.
- No change to Ketahanan Dana, the essential cost, the emergency balance and its parent and pocket deduplication, goal progress, debt payoff archive and reactivation, plan statuses, linking, skipping or recording, the credit bill rules, or any figure.
- No new plan behaviour: plans stay reminders that never post; the Rencana dialog sends today's requests.
- No goal deposit history, no debt payment list endpoint, no lender field, no minimum payment for credit.
- No other page body. Catat's own redesign is phase 11; this phase only opens today's Perpindahan with a target and an amount. The account detail is phase 8's.
- No new npm dependency.
- Local verification only, with the parity seed, the audit seed and the stress and USD seed in disposable databases. Release and production stay with the owner through the approved GitHub workflow.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `financial-goals-tracker`: "Target & Tagihan Layout" becomes the prototype's hero, reading column and panel; "Goal Progress Action Presentation" names the goal dialog; added "Target & Tagihan Hero", "Tagihan in the Next 30 Days", "Savings Goal Rows", "Emergency Goal Placement", "Goal and Debt Dialogs", "Target & Tagihan Loading, Failure and Empty States" and "Target & Tagihan in Both Languages".
- `obligations-debt-tracker`: "Payoff Progress & Estimated Timeline" for the panel rows; added "Paid-Off Debts Stay Reachable".
- `mobile-native-layout`: "Phone Target & Tagihan Composition" in the prototype's order.
- `net-worth-analytics`: "Cash Flow Runway Forecast" item 4 names the hero and its "Rincian".
- `emergency-cost-estimation`: "Essential cost basis disclosure" names the "Rincian" as the place of the basis.
- `automated-transactions`: added "Plans Dialog" (the shared 880 px list and detail, no posting).
- `beranda-cycle-summary`: "Kerjakan Dulu" ranks credit bills with plans of the same urgency.
- `executive-sidebar-layout`: "Notifications Include Due Plans" lists due credit bills.
- `credit-accounts`: added the H9 requirements "Credit Bills Beside Plans", "Credit in Utang & kredit", "Due Credit Bills Need Action" and "Credit Lines in the Pengeluaran Breakdown", with the wording of the credit change's design D12.

`interface-language` and `ui-copy-register` need no delta: adding `/goals` to the translated routes follows their rule that the set of untranslated pages only shrinks.

Standards:
- UI and accessibility (WCAG 2.2 AA): landmarks and headings, focus order and return through every dialog and sheet, focus not obscured by the sticky top bar or the tab bar, 44 px targets on the phone, reflow at 320 px, non-colour cues (status words, the "%" in text beside every bar), reduced motion, 3.1.1 and 3.1.2;
- the copy register in both languages (`ui-copy-register`);
- `money`: figures identical before and after, and in English and Indonesian; credit bills never added to Terjadwal; parent and pocket counted once in the goal picker's linked total, as today;
- `api-consumers`: no API change;
- `personal-data`: no rendered text or user data in logs.

No capability in `AGENTS.md` changes.

## Impact

- **Frontend Target & Tagihan:**
  - `app/goals/page.tsx` and `page.test.tsx`;
  - `components/goals/GoalsView.tsx` (split into `TargetHero.tsx`, `BillsSection.tsx`, `GoalsSection.tsx`, `DebtsPanel.tsx`), `GoalDialogs.tsx` (split into `GoalDialog.tsx`, `DebtDialog.tsx`, `DebtPaymentDialog.tsx`, `GoalProgressDialog.tsx`, `useGoalData.ts`), `types.ts`;
  - new `components/goals/safetyNet.ts` (the "Rincian" builder, moved from `EmergencyCoverageBasis`), `components/goals/bills.ts` (today's `billRows` plus the credit bill rows);
  - `components/dashboard/EmergencyCoverage.tsx` (`EmergencyCoverageBasis` removed; types and `essentialBasisText` kept for Beranda) and its test.
- **Rencana:** `components/recurring/RecurringRulesModal.tsx` rebuilt as `PlansDialog.tsx` on `ShellDialog` (same props), its test; `PendingScheduledBanner.tsx` gives its per-plan block (`DuePlanActions`) to the dialog without changing its requests.
- **Beranda and the bell (H9):** `components/beranda/BerandaView.tsx` and `TodoSection.tsx` (credit bill items), `components/beranda/explanations.ts` (`spendingExplanation` gains the credit lines), `components/layout/AppLayout.tsx` (`planNotifications` gains credit bills), and their tests.
- **Shared:** `components/workspace/ExplanationDialog.tsx` (`link` optional), phase 8's `ProgressBar`, `AccountDetailDialog`, `creditFacts` and `openMovementCapture` used as they are; `app/globals.css` gains `.stacked`, `.goal-row` and `.cols` from `styles.css` where phase 3 and 8 did not add them, and loses the Target-only `ws-*` rules; `components/accounts/money.module.css` loses its Target rules.
- **Language:** `messages/en.json` and `id.json` (`goals`, `plans`), `lib/i18n/translatedRoutes.ts` (`/goals`), `components/workspace/untranslatedPages.ts` (entry removed), the copy register roots, while the untranslated-page test example stays on `/settings`, where phase 8 put it (design "Deviations recorded at implementation").
- **Tooling (local only):** `scripts/verify/markas/`: the Target scenarios in both scenario files, `target-checklist.js`, `target-keys.mjs`, the parity seed extended with a flagged emergency goal, a reached goal, a goal without a date, a paid-off debt, an overdue and a due-today plan, a detected charge, a credit account with an unpaid bill due today and a credit-bill plan; the stress and USD seed for Target.
- **No change** to `backend/`, `db/`, `mobile/` or `nginx/`. Rollback is a revert of the merge commit; no data is involved.
