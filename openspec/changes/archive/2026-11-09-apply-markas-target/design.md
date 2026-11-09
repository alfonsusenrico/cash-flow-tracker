## Context

Target & Tagihan (`/goals`) on `main` `0308b7b`, inside the phase 2 shell (frame 1200 px, title block from `untranslatedPages.ts`, `PageActions`):
- `app/goals/page.tsx` renders `GoalsView` (400 lines) in `Suspense`. The title actions are "Rencana" (opens `RecurringRulesModal`), "Tambah target" and "…" › "Tambah utang".
- The body is `.trio` (three equal columns, one column below a 900 px container) with `RunwayCard`, `DebtsCard` and `BillsCard`, then a full-width `Card` "Target tabungan" with a grid of goal tiles (`Pill` status, % and a sky or green bar).
  - `RunwayCard` reads `GET /dashboard/net-worth` `.runway`. Its bar divides `emergency_fund_balance` by `target_months × monthly_primary_expense` **in the browser**, although the server sends `coverage_pct` and `target_amount` (`routers/dashboard.py` `calculate_ketahanan_dana`).
  - `DebtsCard` sums `remaining_amount` in a big figure, shows a combined paid bar and the minimum of the earliest debt, lists names, and has a toggle "Tampilkan {n} utang lunas". The page reads `GET /obligations` without `include_archived`, and the server returns only `is_archived = false AND remaining_amount > 0` (`routers/obligations.py:66-95`), **so the toggle never appears in production**.
  - `BillsCard` builds rows with `billRows(lines, charges, today, windowEnd)`: the plan lines of `GET /dashboard/cycle-summary` `scheduled.lines` and the charges of `GET /dashboard/recurring-charges` that no plan covers, inside the window. It shows three rows, "dan N tagihan lain", "{count} · {sum}" and "Kelola".
- `GoalDialogs.tsx` (1076 lines, `useGoalDialogs`) holds the data hooks (`GET /goals`, `/obligations`, `/accounts`), the goal and debt drawers, the goal form (account picker with the parent and pocket rule, the backing-change confirmation, colour swatches, the emergency checkbox), the debt form, the progress dialog and the pay dialog (link a recorded expense, then record manually). Every dialog is the legacy `components/ui/Modal`; nothing uses `ShellDialog`, `figures.tsx`, `lists.tsx` or `MaskedMoney`. Money is formatted by `useAppCtx().bal`.
- `?target=` and `?utang=` open the drawers.
- The goal flagged as the emergency fund (`is_emergency`) is a tile like any other. `EmergencyCoverageBasis` (`components/dashboard/EmergencyCoverage.tsx:77`) is exported and tested but not rendered anywhere.
- `RecurringRulesModal` ("Rencana & Pengingat", 726 lines, `Modal untranslated`, tabs "Daftar Rencana" and "+ Tambah Rencana", filter Semua / Cicilan / Rutin) is opened from `/goals`, Rekening, Analisis (`expenseDraft`), Beranda, the shell (from "Rencana jatuh tempo") and Transaksi (until phase 5 removes "Rutin"). Due occurrences are linked, skipped and recorded in `PendingScheduledBanner`, shown in the shell's "Rencana jatuh tempo" drawer from the bell and Beranda.
- Beranda's "Perlu tindakan" (`BerandaView.tsx`, `TodoSection.tsx`) ranks questions, overdue plans, due-today plans, awaiting transfers, ambiguous pairs, groups, automatic links and plans within two days (`GET /recurring`, `/recurring/pending`, `/review/queue`). The bell (`AppLayout.tsx` `planNotifications`) lists plans due within two days and the automatic-link item.
- Copy is hard-coded Indonesian. `/goals` is in `untranslatedPages.ts` and not in `TRANSLATED_ROUTES`.

The prototype (`option-6/js/pages2.js` `PAGES.target`; `js/dialogs.js` `rincian`, `target`, `target-tambah`, `utang`, `utang-tambah`, `rencana` and the `target` menu; `styles.css` §Target & Tagihan, `.hero`, `.supports`, `.prog`, `.cols`, `.panel`, `.stacked`, `.goal-row`, `.rc-grid`):
- the title with "Rencana", "Tambah target" and "…" (Tambah utang, Ubah target dana darurat);
- the hero "Ketahanan Dana" "5,4 bulan" with "Rincian", one line, a thick bar and four supporting figures;
- `.cols`: the reading column with "Tagihan 30 hari ke depan" (five rows of date · name · status · schedule · amount, "dan 3 tagihan lain", "Kelola") and "Target tabungan" (rows with a 4 px bar), and the panel "Utang & kredit" (stacked rows with progress, one credit row, "Sudah lunas · 1");
- the phone: hero with two figures, Tagihan, Target, Utang.

Inputs this phase builds on:
- **Phase 1:** tokens, type roles, the straight selection bar, buttons, fields, segmented control, menu, dialog and chip primitives, the guard `selectionMarks.test.ts`.
- **Phase 2:** the title block, `PageActions` with "…", `ShellDialog`, the bell and the "Rencana jatuh tempo" drawer, frame 1200.
- **Phase 3:** `HeroFigure`, `FigureBlock`, `SplitBar`, `SectionHeader`, `Panel`, `PanelBlock`, `ListRow`, `MaskedMoney`, `ExplanationDialog`, the "Perlu tindakan" ranking, the skeleton and error rules.
- **Phase 4:** `ListRow` `layout="stacked"`, `current` and `rowKey`.
- **Phase 5:** `RowAmount` with the expense in `--out-fg` (owner feedback, red expense amounts), the 560 px dialog conventions, the removal of "Rutin" from Transaksi.
- **`add-language-system`:** next-intl catalogues, `terms`, `breakdown.safetyNet*` and `breakdown.essential*` keys, `lib/format.ts`, the translated-route registry, the copy register per language and the both-language gate.
- **`localize-server-text`:** errors as `{code, params, message}`.
- **`add-credit-spending-flow` (H9):** `GET /api/credit/bills?from&to&status` (`{ok, bills[{account_id, account_name, due_date, statement_date, amount, paid, left_to_pay, status, settled_on}], due_dates_unavailable[{account_id, account_name}]}`, statuses `open`, `upcoming`, `due_today`, `overdue`, `paid`, `paid_late`); `GET /api/accounts` credit rows with `credit_owed`, `limit_left`, `over_limit`, `next_due {due_date, amount, left_to_pay, status}`, `statement_day`, `payment_day`; credit-bill plan lines in `scheduled.lines` with `amount` (the counted part), `plan_amount`, `counted_elsewhere` and `credit_account_id`; `spending.credit {spent_on_credit, paid_to_credit, lines[{key, account_id, label, spent_on_credit, paid_to_credit}]}`.
- **Phase 8 (`apply-markas-rekening`), the shared contract this phase uses:** `ProgressBar` (`.prog`, 4 px, `is-thick` 8 px) in `components/workspace/figures.tsx`; the `terms` keys `debt`, `limitLeft`, `overLimit`, `leftToPay` and `billStatus.*`; `components/accounts/credit.ts` (`creditFacts(account)` and the bill status labels); the standalone `AccountDetailDialog` (fetches its own data and opens outside `/accounts`); the debt amount style it defines for Utang; `openMovementCapture({ targetAccountId, amount })` in the workspace context.

Constraints:
- No money logic, API or data change. Every figure is a server field formatted by `lib/format.ts`. The only client arithmetic left is today's: the bill rows' merge and window filter, the section's count and sum of those rows, the goal picker's linked total (parent and pocket counted once, mirroring `dashboard.py:333-342`), and the "Perlu tindakan" ranking.
- Every action, URL parameter (`?target=`, `?utang=`), confirmation and request keeps its behaviour and tests.
- The held branch `fix/selection-bars-and-tinjau` (`64909ee`) has no Target part. Its goal-picker group rail is already on `main` (phase 1), and its Rekening "Tagihan" link points to this page; phase 8 takes its Rekening parts and the branch is deleted after phase 8 merges, not by this phase.

## Goals / Non-Goals

**Goals:**
- Target & Tagihan and its dialogs match the prototype at 1440, 1024 and 390 px in both themes and both languages, measured against `styles.css`, except the recorded deviations.
- One hero figure, every other figure once, and no card inside a card.
- H9 complete: credit bills beside the plans, credit rows in Utang & kredit, due credit bills in "Perlu tindakan" and the bell, and the two credit lines in Beranda's Pengeluaran "Rincian".
- The page's copy lives in `goals` and `plans` in English and Indonesian, and `/goals` leaves the untranslated set.
- Nothing obsolete remains: the trio cards, the tile grid, the drawers, the dead toggle, `EmergencyCoverageBasis` and the Target-only CSS.

**Non-Goals:**
- Backend, data, schema or companion changes; a deposit history; a payment list per debt; a lender field; a credit minimum payment.
- Changing Ketahanan Dana, the essential cost, the emergency balance rule, goal progress, the debt lifecycle, plan behaviour or the credit bill rules.
- Catat's redesign (phase 11), the account detail (phase 8), the Analisis and Laporan pages (phases 6 and 7).

## Decisions

### D1. Page structure and breakpoints

- **≥ 1200 px:** `.stack` (sections 48 apart): the hero at full width (frame 1200), then `.cols` `minmax(0,1fr) 352px`, gap 24, `align-items: start`. The reading column holds Tagihan, then Target tabungan (48 apart); the panel (`--s1`, radius 12, padding 24) holds Utang & kredit.
- **768–1199 px:** one column (`.cols` collapses as `styles.css` does at 1199): hero, Tagihan, Target, Utang & kredit, each at full width.
- **< 768 px:** the phone composition of D12.
- Viewport media queries only; the `@container main (max-width: 900px)` rule of `.trio` goes.
- First content `y = 192` at 1440 (top bar 56 + title block 136).

Rejected:
- Keeping three equal cards: the audit and the prototype replace them; their sub-boxes are cards in cards.
- A container query: phase 5 found that container widths put phone layouts on laptops.

### D2. Title block and "…" menu

- Title `nav.goals`, subtitle `goals.page.subtitle`.
- Visible: "Rencana" (secondary, `calendar`) opens "Rencana & Pengingat" (D20); "Tambah target" (secondary, `plus`) opens "Buat target tabungan baru". On a phone both stay visible (mobile-native-layout "Phone Page Actions and Targets").
- "…" (`common.moreActions`): "Tambah utang" (`plus`), then "Ubah target dana darurat" (`edit`) when exactly one goal is flagged, or "Buat target dana darurat" (`plus`) when none is (D7). With several flagged goals the second item is left out.

Rejected: a visible "Tambah utang" (≤ 2 visible actions; the prototype puts it in "…").

### D3. Hero "Ketahanan Dana" — flagged: money presentation

`HeroFigure` without a colour mark:
- label row: "Ketahanan Dana" (`terms.safetyNet`) and the quiet small "Rincian" (h 28, padding 0 8), the page's only one;
- figure: `runway.coverage_months` as "5,4 bulan" (`goals.hero.months`, one decimal, region formatting), Manrope 44/600;
- line (14/20 `--muted`): "biaya pokok tertutup dana darurat · target 6 bulan" (`emergency_goals`) or "Belum ada dana darurat khusus — dihitung dari seluruh saldo kas · target 6 bulan" (`all_liquid`, the sentence `emergency-cost-estimation` requires);
- bar: the shared `ProgressBar` `is-thick` (8 px, `--s2` track, `--m-income` fill, square), margin-top 24, width `min(100, coverage_pct)` %, `role="img"` named "{pct}% dari target dana darurat";
- `.supports` `--n: 4`, gap 24, margin-top 24, four `FigureBlock`s (12 label, Inter 20/600 figure, 12 caption):
  1. "Dana darurat" (or "Saldo kas" with `all_liquid`): `emergency_fund_balance`, caption "{coverage_pct}% dari target";
  2. "Target": `target_amount`, caption "{target_months} × biaya pokok";
  3. "Biaya pokok / bulan": `monthly_primary_expense`, caption from `essential_cost` (`breakdown.essentialMedian` short form "rata-rata 3 bulan", "dari anggaran", or "riwayat terbatas"); `phoneHidden`;
  4. "Jika pemasukan berhenti": "{runway_days} hari", caption "sampai dana darurat habis"; `phoneHidden`.
- **Unknown** (`status: unknown`): the figure slot reads "Belum dapat dihitung" in the 24 px title role, the line gives the reason (`breakdown.essentialUnknown`), and there is no bar; Biaya pokok reads "Belum diketahui" and the last figure "—". No 44 px figure appears, as SPEC §13 allows for states.
- **Zero:** "0 bulan", an empty bar and "0 hari"; no solvency word.

**Flag (money presentation):** the bar width comes from the server's `coverage_pct` and the Target figure from `target_amount`, replacing today's browser division. Both are the server's own rule (`coverage()`), so the gate compares the old and new widths and the old client target with `target_amount` on the parity and stress seeds and expects equality within the rounding of one decimal of a percent.

Rejected:
- Keeping the browser division: it duplicates a money rule that the server already returns.
- The prototype's "· 2 sumber" caption (deviation 9): the source count depends on the parent and pocket rule; repeating it in the browser adds a second count to keep equal.
- A coloured status word ("Aman", "Kritis"): the server's `status` is not shown today, and `net-worth-analytics` forbids false solvency indicators.

### D4. "Rincian" of Ketahanan Dana

The shared `ExplanationDialog` (480, header "Ketahanan Dana" + "Rincian hitungan"), built by `components/goals/safetyNet.ts` from the `runway` object (the content of today's unused `EmergencyCoverageBasis`):
- rule: `breakdown.safetyNetRule`;
- line "Saldo cadangan" (`breakdown.reserveBalance`) `emergency_fund_balance`, whose parts are the flagged goals (`emergency_goal_names`, each with its `current_amount` from `GET /goals`) or, with `all_liquid`, the note `breakdown.noDedicatedFund`;
- line "Biaya hidup pokok bulanan" (`breakdown.monthlyEssentialCosts`) `monthly_primary_expense`, whose parts are the periods of `essential_cost` with their ledger links (`breakdown.viewNeedsTransactions`, `viewNoPillarTransactions`), the notes `limitedHistory` and `includesMinimumInstallments`;
- total "Ketahanan Dana" with `breakdown.coverage` ("5,4x biaya pokok · 162 hari"), `coverageZero` or `coverageUnknown`;
- no footer link: `RincianData.link` becomes optional (the dialog is on its own page).
With hidden balances every amount is masked.

Rejected: the prototype's three plain lines (deviation 12): `emergency-cost-estimation` requires the basis and the period links somewhere, and this is their only place.

### D5. "Tagihan 30 hari ke depan"

- `SectionHeader`: `h2.t-label` "Tagihan 30 hari ke depan" (or "Tagihan sampai gaji berikutnya" when `window_basis` is the expected salary, as today), aside "{count} · {total}" (12 `--muted`, tabular) and the quiet small "Kelola", which opens "Rencana & Pengingat".
- Rows: today's `billRows` (moved to `components/goals/bills.ts`, unchanged rules) plus the credit bills of D14, sorted by date. Each is the shared `ListRow` (min-h 56, 64 on the phone) with:
  - time column 40: "2 Nov" (two lines when it wraps, as the prototype);
  - title: the plan or charge name;
  - meta: the status word ("terlewat" `c-expense`, "jatuh tempo hari ini" `c-warning`, "terjadwal" muted), " · {schedule}" ("bulanan", "mingguan"), " · perkiraan" for a detected charge; on the phone the date moves into the meta;
  - amount: `RowAmount` neutral (`t-amt`, ink): a plan is not an expense yet.
  - A plan row opens "Rencana & Pengingat" with that plan chosen; a detected charge opens it with the charge as the new plan's draft (today's `expenseDraft`).
- Five rows, then the quiet "dan {n} tagihan lain" / "Tampilkan lebih sedikit" (`aria-expanded`), which shows the rest in place; focus stays on the toggle.
- Count and total cover the plan and charge rows, computed as today; credit bills are outside them (D14).
- Empty: "Tidak ada tagihan sampai 30 hari ke depan." (or "… sampai gaji berikutnya.") in one muted line.

Rejected: listing every row (the prototype caps at five); a separate "Terdeteksi" section (two lists of bills).

### D6. "Target tabungan"

- `SectionHeader` "Target tabungan" with "{n} target" (`plural`).
- One `button.row.goal-row` per active, non-archived goal (min-h 72, padding 12): `r-main col` with the name (`r-title`), the meta (`r-meta`): "Tercapai" (`c-income`) when `remaining_amount ≤ 0`, else "sampai {d MMM yyyy} · {pace}/bln" (`monthly_target_pace`), "Tanpa tanggal" without a date or "Belum dapat dihitung" without a pace; then " · {n} kantong" (`account_ids.length`) or " · dicatat manual"; and the `ProgressBar` 4 px, width 100 %, max 320, margin-top 8, fill `--ink` or `--m-income` when reached (`aria-hidden`; the % is in the amount column). The amount column: `t-amt` `current_amount` and the 12 px line "dari {target_amount} · {round(percentage_completed)}%".
- The whole row opens "Ubah target tabungan" and sets `?target=`.
- Empty: "Belum ada target tabungan." with the secondary "Tambah target".

Rejected: today's tiles with a `Pill` (two badges per tile, a grid where the prototype has rows); a coloured bar per goal (colour carries no meaning here).

### D7. Emergency goal placement — flagged: scope

- Exactly one active flagged goal: it is left out of "Target tabungan" and the count, and "…" › "Ubah target dana darurat" opens it in "Ubah target tabungan". The hero is its summary.
- No flagged goal: "…" › "Buat target dana darurat" opens "Buat target tabungan baru" with "Tandai sebagai dana darurat" checked.
- Several flagged goals: each stays in the list with " · Dana darurat" in its meta, and the "…" item is left out.

**Flag (scope):** the plan names this removal; it is flagged because a goal leaves the visible list. No goal, flag or figure changes, and the goal stays reachable in one step.

Rejected: listing it with a badge (the hero already shows it, and the prototype leaves it out); hiding all flagged goals (several reserves could not be reached).

### D8. Goal colour field — flagged: scope

The goal form drops "Warna Label". Since the redesign no page draws a goal's colour (rows use ink or income green), so the field would be an unused control. The form keeps the stored `color` and sends it unchanged on edit; a new goal is created with today's default colour.

**Flag (scope):** a visible field is removed. Rejected: keeping it (an input with no visible effect); showing the colour as a dot on the row (colour without meaning, against SPEC §3).

### D9. Paid-off debts — flagged: scope correction

`useGoalData` reads `GET /obligations?include_archived=true` under its own query key (`queryKeys.obligationsWithArchived`), so Catat's debt picker and the Rencana form keep reading the active list. Active = `!is_archived && remaining_amount > 0` (today's `isArchived` rule); the rest are paid off.
- Under the panel's list: the quiet "Sudah lunas · {n}" (`aria-expanded`), shown only when n > 0; expanded, it lists each as a `ListRow` with the name, the meta "lunas" and the muted `total_amount`, which opens "Ubah utang".
- The dialog of a paid-off debt says: "Diarsipkan otomatis saat lunas; aktif lagi jika pembayarannya dibatalkan."

**Flag (scope correction):** today's toggle is dead in production. The parameter exists and is read-only; the debt lifecycle (archive on payoff, reactivation on reversal) is the server's and does not change.

Rejected: leaving it dead; a second request only when the toggle opens (a count is needed for the label).

### D10. "Utang & kredit" — debt rows

`Panel` with `SectionHeader` "Utang & kredit" and "{n} aktif" (active debts plus credit accounts listed under D15). One `button.row.stacked` (phase 4 stacked layout, padding 12 0) per active debt, ordered by due date (none last), then name:
- line 1: the name (`t-body`, ellipsis) and `remaining_amount` in phase 8's debt amount style;
- `ProgressBar` 4 px, fill `--ink`, width `payoff_percentage` %, margin 8 0 4, `aria-hidden`;
- line 2 (12 `--muted`): "{due date d MMM}" (or nothing) and "sisa · {payoff_percentage}% terbayar".
The row opens "Ubah utang" and sets `?utang=`. No payoff time is shown (spec). Empty: "Belum ada utang aktif." when no debt and no credit account.

Removed: the sum of all debts as a big figure (each debt is listed with its own amount, and Rekening's Rincian carries the total), the combined paid bar, "Minimal · {due}".

Rejected: the lender in line 2 (deviation 1: the app has no lender field).

### D11. Loading, failure and empty states

- Loading: the hero as a skeleton (label, 44 px bar 240 wide, 8 px bar, four figure skeletons), skeleton rows (SPEC: 56 h, bars `--s2`, radius 6, no pulse) in each section, `aria-busy="true"`.
- Failure, per source: `GET /dashboard/net-worth` → the hero's error box "Ketahanan Dana belum dapat dihitung." with "Coba lagi"; `GET /dashboard/cycle-summary` or `/recurring-charges` → Tagihan's box "Tagihan belum dapat dimuat."; `GET /goals` → Target's box; `GET /obligations` → the panel's box; `GET /api/credit/bills` → one line under Tagihan (D14). The other sections keep working.
- Empty states as in D5, D6 and D10.

### D12. Phone composition (below 768 px)

Hero (figure 44, 32 below 360 px) with Dana darurat and Target → Tagihan (rows of 64 with the date in the meta) → Target tabungan → Utang & kredit (the panel with padding 16). Sections 32 apart, side gutter 16. Dialogs are bottom sheets (max-h 92vh, grab 32 × 4, footer buttons share the width); "Rencana & Pengingat" is a full-height sheet (`fullHeight`, phase 4). The `mobile-native-layout` requirement's old order (Ketahanan Dana, Target, Utang, bills) differs from both main's render (Ketahanan, Utang, Tagihan, Target) and the prototype; the delta takes the prototype's.

### D13. Hidden balances

Every amount renders through `MaskedMoney` (`common.hidden`): the four supporting figures, the Rincian, bill and goal and debt amounts, the credit rows, "Total saldo terhubung", the dialogs' figures and the pay dialog's candidates. The month and day counts are not money and stay visible. Inputs keep what the owner types.

### D14. Credit bills beside plans (H9) — flagged: money presentation

- `GET /api/credit/bills?from={today}&to={window_end}&status=open,upcoming,due_today,overdue` (the window of the cycle summary).
- Each bill with `left_to_pay > 0` becomes a row: title "Tagihan {account_name}" (`goals.bills.creditTitle`), time column its `due_date`, meta "{status word} · sudah dihitung di Pengeluaran" (status from phase 8's `billStatus`: "jatuh tempo hari ini" warning, "lewat jatuh tempo" expense text, else the date), amount `left_to_pay`. The row opens the account's detail (`AccountDetailDialog`).
- **Not counted:** credit bill rows are outside the header's count and total and never change Terjadwal. When there are any, the aside adds " · {k} tagihan kredit".
- **Credit-bill plan:** a plan line with `credit_account_id` shows `plan_amount` and the meta "{counted_elsewhere} sudah dihitung di Pengeluaran". If an unpaid bill has the same `account_id` and `due_date`, only the plan row is shown. The header total keeps summing the line's `amount` (the counted-once part), as today's `billRows` and Terjadwal do after the credit change.
- `due_dates_unavailable` accounts: one muted line "{account_name}: tanggal jatuh tempo belum diisi" under the list, linking to the account's detail. A failed request: one line "Tagihan kredit belum dapat dimuat." with "Coba lagi"; the plan rows stay.

**Flag (money presentation):** the H9 rule that a bill already counted in Pengeluaran is never added again; the gate checks the section total and Beranda's Terjadwal unchanged with a seeded unpaid bill.

Rejected: adding credit bills to the total (double counting what Pengeluaran already holds); listing the bill and its plan twice; a separate "Tagihan kredit" section (one list of what is due).

### D15. Credit in "Utang & kredit" (H9)

After the debts, one `button.row.stacked` per credit account from `GET /api/accounts` (`type = credit`, not archived), by name:
- line 1: "{name} · kredit" and `credit_owed` in the debt amount style ("Lebih bayar {amount}" from phase 8's `creditFacts` when overpaid);
- `ProgressBar` 4 px of `credit_owed / credit_limit` with the `--m-expense` fill, only with a limit (`aria-hidden`);
- line 2: the next bill "Jatuh tempo hari ini · sisa {left_to_pay}" (warning), "Lewat jatuh tempo · sisa …" (expense text) or "jatuh tempo {d MMM} · sisa …", from `next_due`; none without one; at the right "Sisa limit {limit_left}", "Melewati limit" (`c-expense`) or nothing without a limit.
- The row opens phase 8's `AccountDetailDialog` for that account in place (bills, instalments, "Bayar tagihan"), focus returning to the row.
- No "Minimal bayar" (deviation 2).

Rejected: linking to `/accounts?akun=` (the owner would leave the page); repeating the bills list here (it is in the dialog).

### D16. Due credit bills in "Perlu tindakan" and the bell (H9) — flagged: scope

- **Data:** Beranda and the shell read `GET /api/credit/bills?to={today + 2}&status=open,upcoming,due_today,overdue`, keeping bills with `left_to_pay > 0`.
- **Perlu tindakan:** each bill is an item ranked with plans of the same urgency: overdue (with overdue plans), due today (with due-today plans), within two days (with those plans), each group by due date then name. Row: icon `calendar`, title "Tagihan {account_name}", meta "Lewat jatuh tempo · Kredit", "Jatuh tempo hari ini · Kredit" (warning) or "{Besok | d MMM} · Kredit", amount `left_to_pay`. Three rows at most, as phase 3.
- **Bell:** items "Tagihan {account_name} {amount} lewat jatuh tempo | hari ini | besok | lusa" (the existing `shell.plan*` sentences with the bill's name), detail "Kredit · jatuh tempo {day}", deduplicated by `credit-{account_id}-{due_date}`. A bill whose plan is listed (same account and due date, D14) is listed once, as the plan.
- **Action:** selecting a bill calls phase 8's `openMovementCapture({ sourceAccountId?, targetAccountId?, amount? })` (its D18) as `openMovementCapture({ targetAccountId: account_id, amount: left_to_pay })`, which opens today's Perpindahan (QuickCaptureModal movement mode) with "Ke" and the amount filled and "Dari" empty. Nothing is posted until the owner saves; the movement's request is today's.

**Flag (scope):** the action prefills a capture form from another page. It adds no money behaviour; the owner's save remains the only write.

Rejected: opening `/goals` (the owner wants to pay); opening the account detail (one more step before paying); recording automatically (never: plans and bills are reminders).

### D17. Credit lines in Beranda's Pengeluaran "Rincian" (H9)

`spendingExplanation` (`components/beranda/explanations.ts`) appends, after the five buckets, when `spending.credit` has a non-zero amount:
- "Pengeluaran pakai kredit" `spent_on_credit` with the note "sudah termasuk di atas", its parts the accounts of `lines` with `spent_on_credit > 0`;
- "Tagihan kredit dibayar" `paid_to_credit` with the note "uang keluar, tidak dihitung lagi", its parts the accounts with `paid_to_credit > 0`.
Neither changes the Pengeluaran total; the one "Rincian" per page rule holds. Account names are user data (`label_source: user`).

Rejected: a separate dialog (a second "Rincian" on Beranda); subtracting or adding them (they are already inside Pengeluaran, or outside it by rule).

### D18. Goal dialogs

- **"Ubah target tabungan"** (`ShellDialog` 560, meta: the goal's name): body blocks 24 apart:
  1. "Terkumpul" (or "Tercapai") 12 label, `current_amount` as `t-fig-l` 32 with " dari {target} · {pct}%" (12 muted), the `ProgressBar` `is-thick` (ink, or income green when reached or flagged), and "Perlu {pace} per bulan sampai {date}" when there is a pace;
  2. the goal form: "Nama target", "Target", "Tanggal (opsional)" (`.form-grid` two columns, one on the phone); "Sumber progres" as a segmented control ("Otomatis dari saldo kantong" · "Manual") that shows today's account picker (parent and pocket rows with the group rail, the parent and pocket selection rules, "Total saldo terhubung" counting each balance once) or, for a standalone goal, the line "Progres diatur manual" and the quiet "Tambah progres"; "Tandai sebagai dana darurat" (checkbox, today's help line, no emoji);
  3. the backing-change alert and its "Konfirmasi & simpan" step, as today.
  Footer: quiet destructive "Arsipkan" at the left (confirm in the footer: "Arsipkan target "{name}"? Saldo rekening tidak berubah."), "Batal", primary "Simpan". Requests unchanged: `PATCH /goals/{id}` (no `current_amount` for a linked goal), `DELETE /goals/{id}`.
- **"Buat target tabungan baru"** (560): the same form, footer "Batal" / "Simpan"; `POST /goals`.
- **"Tambah progres"** (480, standalone only): "Tambahan progres" money input (56 px) and the line "Ini hanya memperbarui angka progres target. Tidak ada transaksi dan tidak ada saldo rekening yang berubah."; `POST /goals/{id}/adjust`.
- `GoalDrawer` goes: its facts are the dialog's first block.

Rejected: the prototype's "Riwayat setoran" (deviation 5: no stored history); a read-only detail before the form (the prototype edits in place, and the drawer was one more step).

### D19. Debt dialogs

- **"Ubah utang"** (560, meta: name): "Sisa" (or "Lunas") label, `remaining_amount` `t-fig-l` with " dari {total} · terbayar {pct}%", the thick bar (ink), the lifecycle note for a paid-off debt (D9) or "Jatuh tempo {date} · minimal {minimum} per bulan"; the debt form ("Nama", "Total", "Sisa" with the hint "Kosongkan jika sama dengan total.", "Cicilan minimum (opsional)", "Jatuh tempo (opsional)", "Catatan (opsional)") with today's validation and focus on the failing field; the secondary "Kaitkan atau catat pembayaran" (active debts only). Footer: quiet destructive "Arsipkan" with its confirmation, "Batal", "Simpan". Requests unchanged.
- **"Tambah utang"** (560): the form with today's intro line; `POST /obligations`.
- **"Pembayaran: {name}"** (560): today's two parts in SPEC parts: the candidate rows (`ListRow` with date, merchant, account, amount, "nominal cicilan" when exact, "Kaitkan" as the row's action), the ambiguity line, then "Tidak tertangkap ponsel? Catat manual" with "Bayar dari" (select, no credit accounts as today) and "Nominal" (prefilled with the minimum), primary "Catat pembayaran manual" (no longer the danger fill: it is not destructive). Requests unchanged.
- `DebtDrawer` goes.

Rejected: "Hapus" as the footer action (deviation 7: today's action archives); a payment list (deviation 6: no endpoint).

### D20. "Rencana & Pengingat" — flagged: scope

`components/recurring/PlansDialog.tsx` replaces `RecurringRulesModal` with the same props (`open`, `onClose`, `investmentDraft`, `expenseDraft`) and an optional `planId`, so every caller keeps its call; the callers (phase 8's Rekening "…" › "Rencana", Analisis, Beranda, the shell, the top-up dialog) change only their import:
- `ShellDialog` 880, meta "{n} rencana · pengingat, bukan pembayaran otomatis kecuali ditandai"; full-height sheet on the phone.
- Body: the small segmented filter "Semua · Cicilan · Rutin" (today's), then `.rc-grid` `minmax(0,1fr) minmax(0,1fr)`, gap 24 (one column on the phone, the list then the form):
  - list: `ListRow`s (title, meta "{status} · {next date} · {schedule}", amount), the chosen one `aria-current` with the straight bar; inactive plans read "nonaktif";
  - detail: the chosen plan's name, its status line, today's plan form (two `FormSection`s become one form with `.form-grid`), and, when `GET /recurring/pending` has an occurrence for it, the "Rencana jatuh tempo" block for that plan (`DuePlanActions`, extracted from `PendingScheduledBanner` without changing its requests or confirmations);
  - nothing chosen: the empty state "Pilih rencana" with "Tambah rencana".
- Footer: quiet "Tambah rencana" (left), "Tutup", primary "Simpan" when a plan or a new draft is open. Activate, deactivate and delete stay in the detail ("Nonaktifkan", "Hapus" with today's refusal message).
- A draft (`expenseDraft`, `investmentDraft`) opens the new-plan form directly.

**Flag (scope):** a dialog shared by five callers changes its layout; the filters stay the app's (deviation 3), and every request (`GET/POST/PATCH/DELETE /recurring`, `/toggle`, the occurrence endpoints) is today's. Fallback: keep today's tabs inside the new frame.

Rejected: the prototype's "Terjadwal · Terlewat · Dibayar" filters (plans have no paid state); moving occurrence actions out of "Rencana jatuh tempo" (the bell and Beranda still open it).

### D21. Keys, focus and URL

- `?target={id}` and `?utang={id}` open their dialogs as today and are cleared on close; a stale id is cleared silently.
- Tab order: skip link, sidebar, top bar, title actions, "…", "Rincian", Tagihan rows, its toggle and "Kelola", goal rows, debt rows, credit rows, "Sudah lunas".
- Every dialog traps focus, closes with Esc and returns focus to its opener (the row, "Rincian", "…" item's menu button, "Kelola").
- "…" uses arrow keys and Esc (phase 2 menu).
- No new single-key shortcuts.

### D22. Language

- **Namespaces** in `messages/en.json` and `id.json` with identical key trees:
  - `goals`: `page`, `hero`, `rincian`, `bills`, `list`, `goal`, `debts`, `credit`, `dialog`, `goalForm`, `progress`, `debtForm`, `pay`, `states`, `toast`, `announce`;
  - `plans`: `dialog`, `filter`, `row`, `status`, `form`, `schedule`, `kind`, `due`, `toast` (the Rencana dialog, shared).
  Appendix A lists the keys.
- **Terms:** `terms.safetyNet`, `emergencyFund`, `bills`, `plans`, `scheduled`, `spending`, `due`, `overdue`; phase 8's `terms.debt`, `limitLeft`, `overLimit`, `leftToPay` and `billStatus.*`; the `breakdown.safetyNet*`, `reserveBalance`, `monthlyEssentialCosts`, `essential*`, `coverage*`, `limitedHistory` and period link keys for the Rincian. This phase adds `terms.goals` ("Target" / "Goals") only if `nav.goals` cannot serve.
- **Indonesian:** today's text for every existing string, with these intended changes, listed in `verification.md`: "Tagihan 30 hari ke depan" kept; "Utang & kredit" (was "Utang"); "Sudah lunas · {n}" (was "Tampilkan/Sembunyikan {n} utang lunas"); "Ubah target tabungan" and "Ubah utang" (were "Ubah Target Tabungan" and "Ubah Tagihan"); "Tambah utang" (was "Tambah Tagihan / Utang Baru"); sentence case for the form labels ("Nama target", "Target", "Tanggal (opsional)"); "Tandai sebagai dana darurat" (without the emoji and "(Emergency Fund)"); "Catat pembayaran manual" (unchanged text, no danger fill); "Rencana & Pengingat" and its "Tambah rencana", "Pilih rencana". New strings follow the neutral register: no "kamu" or "Anda", "Pengeluaran" for spending.
- **English:** plain and firm, US spelling, sentence case: "Goals & Bills", "Safety net", "Bills in the next 30 days", "Savings goals", "Debts & credit", "Paid off". "Spending" only; never "Expense", "Outflow". No "you/your" in labels.
- **Whole ICU sentences** with named arguments; counts by `plural`; amounts, dates and months from `lib/format.ts`.
- **User data:** goal, debt, plan, category and account names are shown as stored, with no `lang`.
- **Registry and guards:** `/goals` joins `TRANSLATED_ROUTES`; its `untranslatedPages.ts` entry goes; the untranslated-page test example in `workspace.test.tsx` stays on `/settings`, where phase 8 put it (see "Deviations recorded at implementation"); the copy register's translated roots gain `app/goals`, `components/goals`, `components/recurring`, and the touched parts of `components/beranda` and `AppLayout`; catalogue parity covers `goals` and `plans`. `RecurringRulesModal`'s `untranslated` flag goes with it.
- **Lengths** measured in both languages at 1440, 1024, 390 and 320: the hero line "No dedicated emergency fund yet — counted from all cash balances · target 6 months" wraps to two lines; supporting labels "If income stops" and "Essential costs a month"; the panel's line 2 "Due today · Rp 434.000 left" and "Limit left Rp 2.107.000" ellipsize the left part first.

Rejected: putting the Rencana copy under `goals` (five callers on other pages would read a page namespace).

### D23. Shared components and CSS

**Reused as they are:** `ShellDialog` (with phase 4's `fullHeight`), `PageActions`, `HeroFigure`, `FigureBlock` (`phoneHidden`), `SectionHeader`, `Panel`, `ListRow` (with phase 4's `stacked`, `current`, `rowKey`), `RowAmount`, `MaskedMoney`, `ExplanationDialog`, `Segmented`, `EmptyState`, `ErrorState`, `Skeleton`, phase 8's `ProgressBar`, `AccountDetailDialog`, `creditFacts`, bill status labels and `openMovementCapture`.

**Extended:**
- `ExplanationDialog`: `link` optional.
- `PendingScheduledBanner`: its per-plan block becomes `DuePlanActions`, used by the banner and by `PlansDialog`.

**Token names.** This design writes colours with the prototype's names; they map to phase 1's app tokens as phase 8 records (`apply-markas-foundation` D3): `--s0`/`--s1`/`--s2` → `--canvas`/`--surface`/`--surface-2`, `--ink` → `--text`, `--income` → `--in-fg`, `--expense` → `--out-fg`, `--warning` → `--warn-fg`, `--m-expense` → `--out-solid`, `--m-invest` → `--inv-solid`, `--m-income` → `--in-solid`. The checklist compares computed values, so names never block fidelity; task 1.2 checks the mapping against the merged phase 1 and 8 code.

**Shared terms from phase 8** used here: `terms.debt`, `limitLeft`, `overLimit`, `leftToPay`, `everydayMoney`, and `billStatus.{open, upcoming, dueToday, overdue, paid, paidLate}` ("Terbuka · Akan datang · Jatuh tempo hari ini · Lewat jatuh tempo · Lunas · Lunas terlambat" / "Open · Upcoming · Due today · Overdue · Paid · Paid late"). The lower-case status words in rows ("jatuh tempo hari ini", "lewat jatuh tempo") are the same keys in the row's sentence case.

**New:** `components/goals/` `TargetHero.tsx`, `BillsSection.tsx`, `GoalsSection.tsx`, `DebtsPanel.tsx`, `GoalDialog.tsx`, `GoalProgressDialog.tsx`, `DebtDialog.tsx`, `DebtPaymentDialog.tsx`, `useGoalData.ts`, `safetyNet.ts`, `bills.ts`; `components/recurring/PlansDialog.tsx`. CSS from `styles.css` with phase 1 token names: `.stacked`, `.stacked .line`, `.goal-row`, `.rc-grid`, and `.cols`, `.stack`, `.supports` where phase 3 has not added them.

**Removed** (no user remains, checked with `rg` in task 5.10):
- `RunwayCard`, `DebtsCard`, `BillsCard`, `GoalDrawer`, `DebtDrawer`, the goal tiles, `goalStatus`'s `Pill` tones, `useGoalDialogs` (split);
- `RecurringRulesModal.tsx` (replaced; its test rewritten for `PlansDialog`);
- `EmergencyCoverageBasis` and its test cases (the types and `essentialBasisText` stay for Beranda);
- the Target rules of `components/accounts/money.module.css` (`trio`, `goalTiles`, `goalTile`, `goalTop`, `goalPct`, `goalMeta`, `goalSources`, `debtBig`, `debtNames`, `archivedList`, `billList`, `drawer*`, `detailFacts`, `factsTwo` where only `/goals` uses them), and the file when phase 8 leaves no other user;
- `ws-dana-big`, `ws-dana-foot`, `ws-bar` in `globals.css` when no user remains (Beranda's phase 3 panel no longer uses them);
- `Card` and `CardHeader` on `/goals`; they stay in `primitives.tsx` while other pages use them.

**Kept:** `Modal` (other pages), `FormSection` and `AccountSelectOptions` (inside the new dialogs), `ConfirmActionButton` (restyled by phase 1).

### D24. What happens to each removed element

| Removed | Destination |
| --- | --- |
| Three equal cards in one row | Hero (Ketahanan Dana), Tagihan section, Utang & kredit panel (D1) |
| Ketahanan card's two sub-boxes | Four supporting figures and the Rincian (D3, D4) |
| Browser coverage division | Server `coverage_pct` and `target_amount` (D3) |
| Utang big sum and combined bar | One row per debt with its own amount and bar; the total stays in Rekening's Rincian (D10) |
| "Minimal · {due}" | The debt dialog's line "Jatuh tempo … · minimal … per bulan" (D19) |
| Dead "utang lunas" toggle | "Sudah lunas · N" with the archived list (D9) |
| Goal tiles with a status pill | Goal rows with words in the meta (D6) |
| The emergency goal tile | "…" › "Ubah target dana darurat" and the hero (D7) |
| Goal colour swatches | None; stored colours kept (D8) |
| Goal and debt drawers | The 560 px dialogs (D18, D19) |
| `EmergencyCoverageBasis` | The Rincian (D4) |
| Rencana tabs | List and detail (D20) |

### D25. Dependencies and order

Must be merged into `main` before this phase starts (task 1.2 stops and reports otherwise):
1. `add-language-system` (merged) and `localize-server-text`;
2. phase 5 `apply-markas-transaksi` (`RowAmount` with the red expense, dialog conventions, "Rutin" gone from Transaksi);
3. `add-credit-spending-flow` (every H9 field; its migration V54);
4. phase 8 `apply-markas-rekening` (the shared contract in Context).
Phases 6 (Analisis) and 7 (Laporan) are independent; Analisis keeps opening the Rencana dialog through the same props. If phase 8's merged API differs from the contract (names, props), this phase adapts to the merged code and records each difference in `verification.md`; it never re-implements a phase 8 piece.

The held branch `fix/selection-bars-and-tinjau` gives this phase nothing to take: its goal-picker rail is on `main`, and its Rekening parts are phase 8's. Its deletion is phase 8's follow-up.

### D26. Verification tooling and gate

In `scripts/verify/markas/` (local only):
- **Scenarios** (`scenarios-app.json`, `scenarios-prototype.json`), each in `en` and `id` for the app: `target`, `target-tanpa-dana-darurat` (`all_liquid`), `target-belum-dihitung` (unknown cost), `target-tagihan-semua` (all rows shown), `target-lunas` (paid-off list open), `target-kredit` (unpaid and due-today credit bills, credit-bill plan); `state-target-memuat`, `state-target-galat`, `state-target-kosong`; `dlg-rincian-ketahanan`, `dlg-target-detail` (account-backed), `dlg-target-manual` (standalone), `dlg-target-tambah`, `dlg-target-progres`, `dlg-utang`, `dlg-utang-lunas`, `dlg-utang-tambah`, `dlg-utang-bayar`, `dlg-rencana`, `dlg-rencana-jatuh-tempo`, `dlg-menu-target`; Beranda `beranda-kredit` (Perlu tindakan with a credit bill), `dlg-rincian-aman-kredit`, `dlg-notifikasi-kredit`; deep links `?target=`, `?utang=`.
- **`target-checklist.js` for `measure.js`**, generated from D1 to D20 and SPEC §2, §5 to §7 and §13:
  - `.cols` 1fr / 352, gap 24 at 1440; one column at 1199, 1024; first content `y = 192`;
  - hero: label row 32, figure 44 Manrope 600, line 14/20, thick bar 8 px at margin-top 24, `.supports` four columns gap 24 (two at 390); one 44 px figure and no other ≥ 33 px (dialog figures 32 pass);
  - section header min-h 32, margin-bottom 12; rows min-h 56 (64 phone), padding 0 12, time column 40; goal rows min-h 72, bar 4 px max 320; stacked rows padding 12 0, bar 4 px with margin 8 0 4; panel padding 24 (16 phone), radius 12, `--s1`;
  - dialog widths 480, 560, 880, radius 16, header 24 24 16, body 0 24 24, footer 16 24; sheets at 390;
  - sizes ⊆ {12, 14, 16, 20, 24, 32, 44}, weights ⊆ {400, 500, 600}, ≤ 8 text styles;
  - no border ≥ 2 px and no coloured border except the selection bar and focus; bars square;
  - status word colours (`--expense`, `--warning`) and the reached bar (`--m-income`) per theme;
  - no horizontal overflow at 1440, 1280, 1200, 1199, 1024, 768, 767, 390 and 320.
- **`target-keys.mjs`**, the keyboard flows:
  1. tab order of D21;
  2. "Rincian" opens, Tab stays inside, Esc returns focus;
  3. "dan N tagihan lain" toggles with Enter, focus stays;
  4. a bill row opens "Rencana & Pengingat" with that plan chosen; arrow-free list by Tab; Esc returns;
  5. a goal row → dialog → "Arsipkan" → "Batal" keeps focus on "Arsipkan" → Esc closes → focus on the row;
  6. "Sumber progres" segment by arrows, the confirmation step by keyboard;
  7. a debt row → "Kaitkan atau catat pembayaran" → Esc → focus back;
  8. "Sudah lunas · N" by Enter, `aria-expanded` toggles;
  9. a credit row opens the account dialog, Esc returns;
  10. "…" by arrows: "Tambah utang", "Ubah target dana darurat";
  11. Beranda: Tab to a credit bill in "Perlu tindakan", Enter opens Perpindahan with "Ke" and the amount filled, Esc closes without a request;
  12. the bell: a credit bill item by keyboard opens the same;
  13. phone: sheet open and close with focus return, nothing hidden under the tab bar.
- **Parity seed (`seed_parity.py`)** gains: a flagged emergency goal linked to a parent and one of its pockets (the dedup case), a reached goal, a goal without a date, a standalone goal, a paid-off (archived) debt, an overdue and a due-today plan, a detected recurring charge without a plan, a credit account with an unpaid bill due today and another overdue, a credit-bill plan for one of them, credit purchases and a credit payment in the cycle (for `spending.credit`). Each addition is listed in the seed report.
- **Parity (`parity.mjs`):** before and after on the same seed: the hero's months, the four figures, the bar width (old browser value vs `coverage_pct`, D3), Tagihan's count, total and each row's date and amount, each goal's amounts and %, each debt's amount and %, Beranda's Terjadwal and Pengeluaran totals; and `en` against `id` byte-identical figure strings.
- **Stress and USD seed:** the audit seed, 40 goals, 25 debts (10 paid off), 60 plans, five credit accounts with long names, 60-character names, Rp 1.234.567.890, an unknown-cost account, and the USD display currency.

The gate (tasks group 6) is the plan's common gate plus the both-language gate of `add-language-system`.

### D27. Spec reconciliation

- `financial-goals-tracker`: **Modified** "Target & Tagihan Layout", "Goal Progress Action Presentation"; **Added** "Target & Tagihan Hero", "Tagihan in the Next 30 Days", "Savings Goal Rows", "Emergency Goal Placement", "Goal and Debt Dialogs", "Target & Tagihan Loading, Failure and Empty States", "Target & Tagihan in Both Languages". **Unchanged:** "Financial Goals Management", "Goal Milestone Progress & Monthly Pacing", "Account-Backed Goals", "Standalone Goal Progress Adjustments".
- `obligations-debt-tracker`: **Modified** "Payoff Progress & Estimated Timeline"; **Added** "Paid-Off Debts Stay Reachable". **Unchanged:** management, linking payments, form validation.
- `mobile-native-layout`: **Modified** "Phone Target & Tagihan Composition". "Phone Page Actions and Targets" holds as written.
- `net-worth-analytics`: **Modified** "Cash Flow Runway Forecast" (item 4 and one scenario).
- `emergency-cost-estimation`: **Modified** "Essential cost basis disclosure" (the Rincian as its place).
- `automated-transactions`: **Added** "Plans Dialog".
- `beranda-cycle-summary`: **Modified** "Kerjakan Dulu" (credit bills in the ranking). "Rincian of the Safe Amount" holds: the credit lines sit inside its Pengeluaran parts, specified by `credit-accounts`.
- `executive-sidebar-layout`: **Modified** "Notifications Include Due Plans".
- `credit-accounts`: **Added** the four H9 requirements with the names and wording of the credit change's D12.
- `clean-core-ledger` ("Emergency Fund Goal Flagging", "Real-World Ketahanan Dana Calculation"), `interface-language`, `ui-copy-register` and `tactile-pulse-ui` need no delta.

## Decisions and their defaults

Approved under the owner's standing instruction "complete all until finished" (2026-11-06, renewed 2026-11-08). Every decision takes its recommended default. **Flag** marks a decision that touches money presentation or goes beyond the phased plan's scope; it is reviewed before implementation (task 1.1).

| # | Decision | Default (taken) | Rejected alternatives | Flag |
| --- | --- | --- | --- | --- |
| D1 | Layout and breakpoints | Hero, then `.cols` 1fr / 352 from 1200; one column below; viewport queries | Three equal cards; container query | — |
| D2 | Title actions | Rencana, Tambah target; "…" Tambah utang, Ubah/Buat target dana darurat | Visible Tambah utang | — |
| D3 | Hero | `coverage_months`, basis line, bar of `coverage_pct`, four server figures; words when unknown | Browser division; "· N sumber"; status word | **Flag: money presentation** (server coverage replaces browser division; equality gated) |
| D4 | Rincian | `ExplanationDialog` with reserve, essential cost periods and links, coverage | Prototype's three lines | — (deviation 12) |
| D5 | Tagihan section | Today's rows, five shown, status words, neutral amounts, "Kelola" | All rows; separate detected list | — (deviation 10) |
| D6 | Goal rows | Whole-row rows with meta in words and a 4 px bar | Tiles with pills; coloured bars | — |
| D7 | Emergency goal | One flagged goal leaves the list, edited from "…"; several stay listed | Badge in the list; hide all flagged | **Flag: scope** |
| D8 | Goal colour | Field removed, stored values kept | Keep the field; colour dot | **Flag: scope** |
| D9 | Paid-off debts | `include_archived=true` under its own key; "Sudah lunas · N" | Leave dead; fetch on open | **Flag: scope correction** |
| D10 | Debt rows | Stacked rows with paid bar and "sisa · N% terbayar" | Big sum; lender | — (deviation 1) |
| D11 | States | Per-section skeletons and error boxes | One page error | — |
| D12 | Phone | Hero + 2 → Tagihan → Target → Utang; sheets | Old spec order | — |
| D13 | Hidden balances | `MaskedMoney` on every amount | — | — |
| D14 | Credit bills beside plans (H9) | Rows outside the total and Terjadwal; plan shows `plan_amount` and `counted_elsewhere`; one row per bill | Add to total; list twice; own section | **Flag: money presentation** |
| D15 | Credit in Utang & kredit (H9) | Row per credit account opening the phase 8 dialog | Link to `/accounts`; bills inline | — (deviation 2) |
| D16 | Due credit bills (H9) | Ranked with plans; open Perpindahan prefilled via `openMovementCapture` | Open `/goals`; open account detail; auto-record | **Flag: scope** |
| D17 | Credit lines in Rincian (H9) | Two lines inside Pengeluaran's parts | Separate dialog; arithmetic | — |
| D18 | Goal dialogs | 560 edit in place with today's rules; 480 progress | Deposit history; read-only detail first | — (deviations 4, 5) |
| D19 | Debt dialogs | 560 edit with lifecycle note and pay entry; "Arsipkan" | "Hapus"; payment list | — (deviations 6, 7) |
| D20 | Rencana dialog | 880 list and detail, app filters, `DuePlanActions` | Prototype filters; tabs | **Flag: scope** (shared dialog relayout) |
| D21 | Keys, focus, URL | Unchanged parameters; focus return everywhere | — | — |
| D22 | Language | `goals`, `plans`; `/goals` translated | Rencana copy under `goals` | — |
| D23 | Shared pieces | Reuse phases 3, 4, 5, 8; extract `DuePlanActions` | Page-only copies | — |
| D24 | Removed elements | As the table | — | — |
| D25 | Dependencies | Language changes, phase 5, credit change, phase 8 merged first | Stacked branches | — |
| D26 | Tooling | Scenarios, checklist, key flows, extended seeds | Existing seed only | — |
| D27 | Specs | As listed | — | — |

No default changes money logic, an existing request, a count's rule or a figure's value. D3 and D14 are flagged because they decide how money figures are presented; D7, D8, D9, D16 and D20 because they go beyond the plan's wording.

## Deviations recorded at implementation

Task 1.2 compared the plan with the merged code on `origin/main` `98cdb49` (phases 5 and 8 merged as #93 and #94). Each difference below is adapted to, not re-implemented (D25):
- **`ExplanationDialog` `link`** is already optional (phase 8). Task 2.1 needs no code change; its evidence is the existing Rincian tests.
- **Untranslated-page test example:** phase 8 moved it from `/accounts` to `/settings`, not to `/goals`. It stays on `/settings` (coordinator, 2026-11-09); D22's rule to move it to `/insights` or `/laporan` does not apply.
- **`ShellDialog`** gained an `initialFocus` selector prop with phase 5's fix `1b7f1cb`; the dialogs of this phase use it where the first field is not the right first stop.
- **Frontend types:** `PlanLine` in `components/beranda/types.ts` lacks the credit change's `plan_amount`, `counted_elsewhere` and `credit_account_id`, and `CycleSummary.spending` lacks `credit`. Both are added as optional fields; the API already sends them.
- **Bell items:** `WorkspaceNotification` can only link (`href`) or open "Rencana jatuh tempo". A due credit bill needs an action (D16), so the item gains an optional `onSelect`, used only by credit bill items.
- **Credit lines in the Pengeluaran Rincian (D17):** a Rincian line's parts are one level deep (`ExplanationDialog` `Parts`), so "Pengeluaran pakai kredit" and "Tagihan kredit dibayar" are two parts of the Pengeluaran line whose note names the accounts with their amounts, instead of a nested disclosure per line.
- **Evidence location:** captures, parity JSON and the copy snapshot go to the session scratchpad (`markas-target/`), as phases 5 and 8 did, instead of `research/evidence/apply-markas-target/`; `verification.md` lists the paths.
- **`PROJECT_STATE.md`:** this change does not edit it; the lines meant for it are in `verification.md` under "For PROJECT_STATE.md" (coordinator).
- **Pull request:** the branch is pushed without a pull request (coordinator, 2026-11-09); the owner opens it.

## Risks / Trade-offs

- [Phase 8 contract drift] `ProgressBar`, `AccountDetailDialog`, `creditFacts` and `openMovementCapture` are planned, not merged. → D25's adapt-and-record rule and task 1.2's stop condition.
- [Credit change not merged] Every H9 field comes from `add-credit-spending-flow`. → Task 1.2 stops; the H9 tasks never fake fields.
- [Shared Rencana dialog] Five callers see the new layout. → Their entry points are captured before and after (task 1.3, 6.1), and `PlansDialog` keeps the props and requests (tests per caller).
- [Bill and plan matching] A credit-bill plan whose date differs from the bill's due date shows both rows. → The match uses the server's `credit_account_id` and the due date; a mismatch is visible, never hidden, and listed in the end review.
- [Perlu tindakan crowding] Credit bills compete for three rows. → They rank with plans of the same urgency only; the bell lists every item.
- [Emergency goal hidden] An owner may look for it in the list. → The hero names it, "…" reaches it, and several flagged goals stay listed (D7).
- [Request count] The page adds `GET /api/credit/bills` and Beranda and the shell one each. → One request per surface, cached by React Query under one key; the gate records the page's request count before and after.
- [Copy volume] About 260 strings move to keys. → A rendered-text snapshot of the Indonesian page before and after (task 4.4).

## Migration and rollback

No data, schema or API change and no migration. Rollback is a revert of the merge commit; the previous release's page reads the same endpoints. `?target=` and `?utang=` keep working across a rollback.

## Open Questions

None. Every flagged item takes its default under the standing instruction; task 1.1 records the review outcome.

## Appendix A. Copy as message keys (Indonesian · English)

Arguments in braces arrive formatted. `{count}` selects plurals (English `one`/`other`, Indonesian `other`). Existing Indonesian text is kept byte for byte unless marked "new" or "changed". Existing strings not listed (validation messages, the backing-change alerts, the pay dialog's states, plan form hints) move under the same sub-namespaces with their Indonesian unchanged and English written with Appendix B; task 4.1 lists them in full and task 4.4 reviews them.

### `goals.page`, `goals.hero`, `goals.rincian`

| Key | Indonesian | English |
| --- | --- | --- |
| `page.subtitle` | Dana darurat, tagihan, utang dan target tabungan | Emergency fund, bills, debts and savings goals |
| `page.plans` | Rencana | Plans |
| `page.addGoal` | Tambah target | Add goal |
| `page.more` | Tindakan lain | More actions |
| `page.addDebt` | Tambah utang | Add debt |
| `page.editEmergency` (new) | Ubah target dana darurat | Edit emergency fund goal |
| `page.createEmergency` (new) | Buat target dana darurat | Create emergency fund goal |
| `hero.label` | `terms.safetyNet` (Ketahanan Dana) | Safety net |
| `hero.months` (new) | {months, plural, other {{monthsText} bulan}} | {months, plural, one {{monthsText} month} other {{monthsText} months}} |
| `hero.lineGoals` (new) | biaya pokok tertutup dana darurat · target {months, plural, other {{monthsText} bulan}} | essential costs covered by the emergency fund · target {months, plural, one {{monthsText} month} other {{monthsText} months}} |
| `hero.lineCash` (new) | Belum ada dana darurat khusus — dihitung dari seluruh saldo kas · target {months, plural, other {{monthsText} bulan}} | No dedicated emergency fund yet — counted from all cash balances · target {months, plural, one {{monthsText} month} other {{monthsText} months}} |
| `hero.unknown` | Belum dapat dihitung | Cannot be calculated yet |
| `hero.barLabel` (new) | {pct}% dari target dana darurat | {pct}% of the emergency fund target |
| `hero.fund` | Dana darurat | Emergency fund |
| `hero.cash` | Saldo kas | Cash balance |
| `hero.fundCaption` (new) | {pct}% dari target | {pct}% of target |
| `hero.target` | Target | Target |
| `hero.targetCaption` (new) | {months} × biaya pokok | {months} × essential costs |
| `hero.essential` | Biaya pokok / bulan | Essential costs a month |
| `hero.essentialMedian` (new) | rata-rata {count} bulan | average of {count, plural, one {# month} other {# months}} |
| `hero.essentialBudget` (new) | dari anggaran | from budgets |
| `hero.ifIncomeStops` | Jika pemasukan berhenti | If income stops |
| `hero.days` | {days, plural, other {# hari}} | {days, plural, one {# day} other {# days}} |
| `hero.daysCaption` (new) | sampai dana darurat habis | until the emergency fund runs out |
| `rincian.title` | `terms.safetyNet` | Safety net |
| `rincian.goalsPart` (new) | {name} | {name} (user data) |

### `goals.bills`, `goals.list`, `goals.goal`

| Key | Indonesian | English |
| --- | --- | --- |
| `bills.title30` | Tagihan 30 hari ke depan | Bills in the next 30 days |
| `bills.titleSalary` | Tagihan sampai gaji berikutnya | Bills until the next salary |
| `bills.aside` | {count} · {total} | {count} · {total} |
| `bills.creditAside` (new) | {count, plural, other {# tagihan kredit}} | {count, plural, one {# credit bill} other {# credit bills}} |
| `bills.manage` | Kelola | Manage |
| `bills.overdue` | terlewat | overdue |
| `bills.dueToday` | jatuh tempo hari ini | due today |
| `bills.scheduled` | terjadwal | scheduled |
| `bills.monthly` · `bills.weekly` | bulanan · mingguan | monthly · weekly |
| `bills.estimate` | perkiraan | estimate |
| `bills.more` | dan {count, plural, other {# tagihan lain}} | and {count, plural, one {# more bill} other {# more bills}} |
| `bills.less` (new) | Tampilkan lebih sedikit | Show fewer |
| `bills.none30` | Tidak ada tagihan sampai 30 hari ke depan. | No bills in the next 30 days. |
| `bills.noneSalary` | Tidak ada tagihan sampai gaji berikutnya. | No bills until the next salary. |
| `bills.creditTitle` (new) | Tagihan {account} | {account} bill |
| `bills.countedInSpending` (new) | sudah dihitung di Pengeluaran | already counted in Spending |
| `bills.planCounted` (new) | {amount} sudah dihitung di Pengeluaran | {amount} already counted in Spending |
| `bills.noDueDates` (new) | {account}: tanggal jatuh tempo belum diisi | {account}: due dates not set |
| `bills.creditError` (new) | Tagihan kredit belum dapat dimuat. | Credit bills could not be loaded. |
| `bills.error` | Tagihan belum dapat dimuat. | Bills could not be loaded. |
| `list.title` | Target tabungan | Savings goals |
| `list.count` | {count, plural, other {# target}} | {count, plural, one {# goal} other {# goals}} |
| `list.empty` | Belum ada target tabungan. | No savings goals yet. |
| `goal.reached` | Tercapai | Reached |
| `goal.until` (new) | sampai {date} · {pace}/bln | until {date} · {pace}/mo |
| `goal.noDate` | Tanpa tanggal | No date |
| `goal.paceUnknown` | Belum dapat dihitung | Cannot be calculated yet |
| `goal.pockets` | {count, plural, other {# kantong}} | {count, plural, one {# pocket} other {# pockets}} |
| `goal.manual` | dicatat manual | tracked manually |
| `goal.emergency` (new) | Dana darurat | Emergency fund |
| `goal.of` (new) | dari {target} · {pct}% | of {target} · {pct}% |

### `goals.debts`, `goals.credit`

| Key | Indonesian | English |
| --- | --- | --- |
| `debts.title` (changed) | Utang & kredit | Debts & credit |
| `debts.active` | {count, plural, other {# aktif}} | {count, plural, other {# active}} |
| `debts.left` (new) | sisa · {pct}% terbayar | left · {pct}% paid |
| `debts.empty` | Belum ada utang aktif. | No active debts. |
| `debts.paidOff` (changed) | Sudah lunas · {count} | Paid off · {count} |
| `debts.paidOffMeta` (new) | lunas | paid off |
| `credit.rowTitle` (new) | {name} · kredit | {name} · credit |
| `credit.dueToday` (new) | Jatuh tempo hari ini · sisa {amount} | Due today · {amount} left |
| `credit.overdue` (new) | Lewat jatuh tempo · sisa {amount} | Overdue · {amount} left |
| `credit.dueOn` (new) | jatuh tempo {date} · sisa {amount} | due {date} · {amount} left |
| `credit.limitLeft` | `terms.limitLeft` {amount} | Limit left {amount} |
| `credit.overLimit` | `terms.overLimit` | Over limit |

### `goals.dialog`, `goals.goalForm`, `goals.progress`

| Key | Indonesian | English |
| --- | --- | --- |
| `dialog.editGoal` (changed) | Ubah target tabungan | Edit savings goal |
| `dialog.newGoal` (changed) | Buat target tabungan baru | Create a savings goal |
| `dialog.saved` | Terkumpul | Saved so far |
| `dialog.of` (new) | dari {target} · {pct}% | of {target} · {pct}% |
| `dialog.pace` (new) | Perlu {amount} per bulan sampai {date} | Needs {amount} a month until {date} |
| `dialog.archive` | Arsipkan | Archive |
| `dialog.archiveGoalConfirm` | Arsipkan target "{name}"? Saldo rekening tidak berubah. | Archive the goal "{name}"? Account balances do not change. |
| `goalForm.name` (changed) | Nama target | Goal name |
| `goalForm.target` (changed) | Target | Target |
| `goalForm.date` (changed) | Tanggal (opsional) | Date (optional) |
| `goalForm.source` | Sumber progres | Progress source |
| `goalForm.sourceAuto` (new) | Otomatis dari saldo kantong | From pocket balances |
| `goalForm.sourceManual` (new) | Manual | Manual |
| `goalForm.linkedTotal` | Total saldo terhubung | Linked balance total |
| `goalForm.emergency` (changed) | Tandai sebagai dana darurat | Mark as the emergency fund |
| `goalForm.manualLine` (new) | Progres diatur manual. | Progress is set by hand. |
| `progress.title` (changed) | Tambah progres | Add progress |
| `progress.amount` | Tambahan progres | Progress to add |
| `progress.note` | Ini hanya memperbarui angka progres target. Tidak ada transaksi dan tidak ada saldo rekening yang berubah. | This only updates the goal's progress. No transaction is made and no account balance changes. |

### `goals.debtForm`, `goals.pay`

| Key | Indonesian | English |
| --- | --- | --- |
| `debtForm.editTitle` (changed) | Ubah utang | Edit debt |
| `debtForm.newTitle` (changed) | Tambah utang | Add debt |
| `debtForm.left` | Sisa | Left |
| `debtForm.paidOff` (new) | Lunas | Paid off |
| `debtForm.ofPaid` (new) | dari {total} · terbayar {pct}% | of {total} · {pct}% paid |
| `debtForm.lifecycle` (new) | Diarsipkan otomatis saat lunas; aktif lagi jika pembayarannya dibatalkan. | Archived automatically when paid off; active again if its payment is reversed. |
| `debtForm.dueLine` (new) | Jatuh tempo {date} · minimal {amount} per bulan | Due {date} · at least {amount} a month |
| `debtForm.name` (changed) | Nama | Name |
| `debtForm.total` (changed) | Total | Total |
| `debtForm.remaining` (changed) | Sisa | Left |
| `debtForm.remainingHint` | Kosongkan jika sama dengan total. | Leave empty if it equals the total. |
| `debtForm.minimum` (changed) | Cicilan minimum (opsional) | Minimum installment (optional) |
| `debtForm.due` (changed) | Jatuh tempo (opsional) | Due date (optional) |
| `debtForm.notes` (changed) | Catatan (opsional) | Notes (optional) |
| `debtForm.payEntry` | Kaitkan atau catat pembayaran | Link or record a payment |
| `debtForm.archiveConfirm` | Arsipkan utang "{name}"? | Archive the debt "{name}"? |
| `pay.title` | Pembayaran: {name} | Payment: {name} |
| `pay.linkTitle` | Kaitkan transaksi yang sudah tercatat | Link a recorded transaction |
| `pay.manualTitle` | Tidak tertangkap ponsel? Catat manual | Not caught by the phone? Record it manually |
| `pay.link` | Kaitkan | Link |
| `pay.exact` | nominal cicilan | installment amount |
| `pay.record` | Catat pembayaran manual | Record a payment manually |

### `plans`

| Key | Indonesian | English |
| --- | --- | --- |
| `dialog.title` | Rencana & Pengingat | Plans & reminders |
| `dialog.meta` (new) | {count, plural, other {# rencana}} · pengingat, bukan pembayaran otomatis kecuali ditandai | {count, plural, one {# plan} other {# plans}} · reminders, not automatic payments unless marked |
| `dialog.add` | Tambah rencana | Add plan |
| `dialog.close` | Tutup | Close |
| `dialog.save` | Simpan | Save |
| `dialog.pick` (new) | Pilih rencana | Choose a plan |
| `dialog.pickLine` (new) | Atau tambah rencana baru. | Or add a new plan. |
| `filter.label` | Saring rencana | Filter plans |
| `filter.all` · `filter.installments` · `filter.routine` | Semua · Cicilan · Rutin | All · Installments · Routine |
| `row.meta` (new) | {status} · {date} · {schedule} | {status} · {date} · {schedule} |
| `status.overdue` · `status.dueToday` · `status.scheduled` · `status.inactive` | terlewat · jatuh tempo hari ini · terjadwal · nonaktif | overdue · due today · scheduled · inactive |
| `form.name` | Nama rencana | Plan name |
| `form.kind` | Jenis rencana | Plan type |
| `form.amount` | Nominal | Amount |
| `form.from` · `form.into` | Dari rekening · Masuk ke rekening | From account · Into account |
| `form.debt` | Bayar tagihan / utang (opsional) | Pay a bill or debt (optional) |
| `form.schedule` | Jadwal | Schedule |
| `form.active` | Rencana aktif | Plan active |
| `form.deactivate` · `form.activate` · `form.delete` | Nonaktifkan · Aktifkan · Hapus | Deactivate · Activate · Delete |
| `due.title` | `shell.duePlans.title` (Rencana jatuh tempo) | Plans due |

### `goals.states`, Beranda and bell additions

| Key | Indonesian | English |
| --- | --- | --- |
| `states.safetyNetError` | Ketahanan Dana belum dapat dihitung. | The safety net cannot be calculated yet. |
| `states.goalsError` | Target belum dapat dimuat. | Goals could not be loaded. |
| `states.debtsError` | Utang belum dapat dimuat. | Debts could not be loaded. |
| `home.creditBill` (new) | Tagihan {account} | {account} bill |
| `home.creditWhen` (new) | {when} · Kredit | {when} · Credit |
| `shell.creditDetail` (new) | Kredit · jatuh tempo {day} | Credit · due {day} |
| `breakdown.spentOnCredit` (new) | Pengeluaran pakai kredit | Spending on credit |
| `breakdown.spentOnCreditNote` (new) | sudah termasuk di atas | already included above |
| `breakdown.paidToCredit` (new) | Tagihan kredit dibayar | Credit bills paid |
| `breakdown.paidToCreditNote` (new) | uang keluar, tidak dihitung lagi | money out, not counted again |

## Appendix B. Glossary (Indonesian → English)

Consistent with `add-language-system` Appendix A, the credit change's D14 glossary and phases 4, 5 and 8. Figure and concept names are `terms.*` and are spelled the same everywhere.

| Indonesian | English | Note |
| --- | --- | --- |
| Target & Tagihan | Goals & Bills | page title (`nav.goals`) |
| Ketahanan Dana | Safety net | `terms.safetyNet`; the figure is months of essential costs |
| Dana darurat · Saldo kas | Emergency fund · Cash balance | the reserve; cash when no goal is flagged |
| Biaya pokok · Biaya hidup pokok bulanan | Essential costs · Monthly essential living costs | |
| Jika pemasukan berhenti | If income stops | |
| Tagihan · Tagihan 30 hari ke depan | Bills · Bills in the next 30 days | `terms.bills` |
| Rencana · Rencana & Pengingat | Plans · Plans & reminders | `terms.plans`; reminders, never payments |
| Terjadwal | Scheduled | `terms.scheduled` |
| terlewat · jatuh tempo hari ini · Lewat jatuh tempo | overdue · due today · Overdue | `terms.overdue`, `terms.due` |
| perkiraan | estimate | detected recurring charge |
| Target tabungan · Target | Savings goals · Goal / Target | "Target" the amount, "goal" the thing |
| Tercapai · Tanpa tanggal · dicatat manual | Reached · No date · tracked manually | |
| Terkumpul · Sumber progres | Saved so far · Progress source | |
| Tambah progres | Add progress | changes no account |
| Utang · Utang & kredit | Debt · Debts & credit | `terms.debt` (phase 8) |
| Sisa · terbayar · Sudah lunas · lunas | Left · paid · Paid off · paid off | |
| Cicilan minimum | Minimum installment | |
| Kaitkan · Catat pembayaran manual | Link · Record a payment manually | |
| Kredit · Sisa limit · Melewati limit · Sisa tagihan | Credit · Limit left · Over limit · Left to pay | phase 8 and the credit glossary |
| Tagihan {akun} | {account} bill | credit bill row |
| sudah dihitung di Pengeluaran | already counted in Spending | credit bills and credit-bill plans |
| Pengeluaran pakai kredit · Tagihan kredit dibayar | Spending on credit · Credit bills paid | Beranda Rincian |
| Pengeluaran | Spending | never "Expense", "Outflow" |
| Arsipkan · Ubah · Simpan · Batal · Tutup | Archive · Edit · Save · Cancel · Close | |
| Rincian | Breakdown | `terms.breakdown` |
