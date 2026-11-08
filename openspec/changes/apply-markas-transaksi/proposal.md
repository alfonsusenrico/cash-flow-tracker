## Why

On 2026-11-05 the owner chose design option 6 "Markas+" ("ok accepted") and expects the app to match it exactly. Transaksi is the page the owner scans "all the way down", and it is still the pre-redesign page inside the new shell.

The audit (`design-options/audit/AUDIT.md` §Transaksi) measured why it reads heavy:
- each row says "Pengeluaran" three times: an icon tile, a "Pengeluaran" pill and a coral amount, with 51 tiles, 55 pills and 291 bordered boxes on one page;
- the amount (14/400) is lighter than the name (14/600);
- two-line rows (69 px) make 50 rows 4,313 px tall, and the second line repeats the account;
- on the phone, "8 lagi di hari ini · Muat lagi" wraps in a 30 px column.

Four known issues belong to this phase:
- **Phone feed on a laptop.** Between 1024 and about 1100 px Transaksi shows the phone feed. The cause is `useWidthAtMost(listRegionRef, 767)`: the page switches layout on the list card's own width, which drops below 767 px when the 240 px sidebar and the 48 px margins take their share (design D1).
- **Expense colour.** Phase 3 recorded that Beranda shows expenses in the ink and Transaksi in coral. The owner's feedback of 2026-11-06 settles it the other way: one shared amount component paints "−Rp" in the expense red on Transaksi, in the dialog and on Beranda (D29).
- **The KPI cards.** `KpiCard` and `.ws-kcards` stayed on Transaksi until this phase. They are replaced by the hero and two supporting figures, and removed.
- **The English error "Source and target accounts must be different".** It is a plain-string server error from `POST /movements`, `PATCH /movements/{id}` and `ledger_mutations`. `localize-server-text` gives it a code and both languages. Its Catat path (`POST /movements`) is phase 11. Its Transaksi path, the movement edit opened from the transaction dialog (`PATCH /movements/{id}`), is this phase: the rebuilt edit dialog renders it from `errors.<code>` in the interface language (design D16).

Phases 1 to 3 (foundation, shell, Beranda), `add-category-system-keys`, `add-language-system` and `localize-server-text` are merged or merge before this phase, and phase 4 (`apply-markas-tinjau`) merges before it. English is the default interface language, and every page rebuilt from phase 4 on ships English and Indonesian.

The phased plan is `cash-flow-tracker-prototypes/research/2026-11-05-markas-implementation-plan.md` (phase 5 of twelve). The fidelity source is `design-options/option-6/`: `styles.css` (it wins over `SPEC.md`), `js/pages.js` `PAGES.transaksi` and the row helpers, `js/dialogs.js` (`transaksi`, `transaksi-ubah`, `filter` and the `transaksi` menu), `SPEC.md` §6 to §8, §11 and §13, and the captures `transaksi-*`, `state-transaksi-*`, `dlg-filter-*`, `dlg-menu-transaksi-*`, `dlg-transaksi-detail-*`, `dlg-transaksi-ubah-*` and `dlg-transaksi-perpindahan-*`.

## Expected Outcome

- At 1440 px Transaksi reads top to bottom as in the prototype:
  - the title "Transaksi" with its subtitle, the period stepper "‹ Siklus 23 Okt – 24 Nov ›" and one "…" button that holds "Gabungkan dua transaksi" and "Filter transaksi";
  - one hero "Pengeluaran" in 44 px with two supporting figures beside it, "Masuk" (+, income green) and "Bersih · masuk − pengeluaran";
  - a sticky 200 px rail with JENIS (Semua, Pengeluaran, Masuk, Pindah saldo, Investasi, Tabungan & investasi with counts) and PERIODE (Siklus gaji, Bulan kalender, Semua waktu) beside the list, within the 1040 px frame;
  - the list: a search field and "Filter · N", the active filters as removable chips, day groups under sticky headings with muted day totals, and one-line 56 px rows: time · title and meta · category · pillar · amount.
- An expense reads "−Rp 30.000" in red (the expense text colour, owner feedback 1), on Transaksi, in the transaction dialog and on Beranda. Income reads "+Rp" in green, a transfer "Rp" in sky, a top-up "Rp" in lavender. No row carries an icon tile, a type pill or a coloured border.
- A transfer row is titled by its note, such as "Isi GoPay", with the route "Jago · Tabungan → Jago · Bayar Kredit" as its meta. Without a note the route is the title (owner feedback 2).
- A transfer's pillar column says what the move did to savings, by the server's own savings rules: "Ditabung", "Diambil dari tabungan", "Investasi", "Bayar utang", or "—". "Tidak dihitung" no longer appears for transfers (owner feedback 3).
- "Tabungan & investasi" in the rail and the type control lists every row that changes Beranda's "Ditabung & investasi". For the cycle, its hero equals Beranda's figure (owner feedback 4).
- From 768 to 1199 px the page keeps its desktop rows. The rail becomes the type control above the search, and the period mode is in "Filter". Below 768 px the phone layout follows the prototype: hero, two supporting figures, the scrolling type control, search and Filter, then two-line rows. No width from 768 px up shows the phone feed.
- A row opens a centered 560 px transaction dialog (a bottom sheet on a phone) with the amount, a label and value list and "Hapus" and "Ubah". "Ubah" opens the 600 px "Ubah transaksi" dialog, and "Filter" the 640 px "Filter transaksi" dialog. Every action sends exactly the request it sends today.
- Every filter, the URL state, every deep link (`?edit=`, `return=review&folder=`, `fokus=cari`, Laporan and Analisis links), J/K/Enter/Z, the merge into one transfer, "Tandai sudah dicek" with Urungkan, delete after confirmation, the type switch and receipts work as today.
- An English account sees Transaksi in plain English ("Transactions", "Spending", "Income", "Net", "Transfer"). An Indonesian account sees today's Indonesian with neutral firm labels. Transaksi no longer shows "This page is in Indonesian for now."
- With "Sembunyikan saldo" every Transaksi amount reads "Rp ••••••" and is announced as hidden.
- Every count, amount and date on Transaksi equals today's value for the same data, in both languages.

## Owner feedback (2026-11-06)

After looking at production on 2026-11-06, the owner gave five points for Transaksi. They override the prototype where they differ, and each is a decision in `design.md`:
1. **Expense amounts are red** ("expense value in transactions still in plain white color, better make it red"). Expense amounts ("−Rp …") use the expense text colour `--out-fg` on Transaksi, in the transaction dialog and on Beranda (Aktivitas terbaru and Perlu tindakan), through the one shared amount component. This overrides the prototype's ink amounts and the phase 3 choice. `--out-fg` measures 5.16 to 6.17:1 in light and 5.42 to 6.62:1 in dark on every surface it sits on, and a token test pins it. Income stays as today (D29).
2. **Transfer rows show the note first.** The title is the movement's note, as the owner wrote it or as the server stored it; the meta is the route. Without a note, the route is the title (D11).
3. **No "Tidak dihitung" for transfers.** The pillar column names the saving direction from the account roles, classified exactly as Beranda's "Ditabung & investasi" is computed (`dashboard.py` `get_kakeibo_breakdown`, `cycle_summary.savings_change`). The server computes the classification as an additive, read-only row field (D27).
4. **A "Tabungan & investasi" filter** ("Savings & investments") in the rail and the type control. For the cycle its total equals Beranda's figure, which the gate checks. The API gets one additive filter value and one additive summary field (D28).
5. **Room for credit spending.** `add-credit-spending-flow` (per-provider credit purchases and settlements, 0 % installments, "no cash out until settlement") is planned separately and is not built here. The type list takes a "Kredit" item without restructuring, and credit purchase rows keep their provider account in the meta (D30).

The owner rejected several roles per account (one role stays, to avoid double counting). This plan adds no roles and no tags.

## Approval

Approved 2026-11-06 under the owner's standing instruction "complete all until finished"; decisions take their recommended defaults.

Every decision, its default and the rejected alternatives are in `design.md` (D1 to D30, summarized in "Decisions and their defaults"). No default changes money behaviour: every figure is a server field shown as today, every action sends today's request, and the backend additions are read-only.

The owner feedback of 2026-11-06 is applied under the same instruction. Ten items in seven decisions were flagged; the coordinator decided all of them on 2026-11-06 under the standing instruction, recorded in `design.md` "Flag decisions". Every default was taken, except that D27's labels were tightened as described below:
- **D27 (backend addition, read-only):** every row of `GET /transactions` and `GET /transactions/{id}` gains `saving_flow` (`saved`, `invested`, `taken`, `paid_from_savings`, `saving_pillar`, `debt_payment` or `null`). It is computed from the SQL predicates that `get_kakeibo_breakdown` uses, moved into one shared helper so both read one definition. No write path, schema, migration or existing figure changes. Fallback if rejected: classify in the browser from `/accounts` roles and goal links (a second copy of the money rule, rejected for drift).
- **D27 (owner rule reconciled with the server rule) — decided 2026-11-06:** a saving label appears only when the move changes Beranda's "Ditabung & investasi":
  - a move into a `dana_investasi` account shows "—" (the route names the target; Beranda counts the later top-up into a product);
  - a top-up from a `tabungan` pocket into a product shows "—", because it moves money inside savings and investments and changes nothing;
  - a payment from a `tabungan` account into a credit account stays "Diambil dari tabungan", matching Beranda;
  - "Bayar utang" stays for credit payments from non-saving accounts as an informational label; Beranda's rule leaves them outside the figure, and they are not in the filter.
  Changing what Beranda counts is a money-logic change and stays out of a UI phase.
- **D28 (backend addition, read-only):** `GET /transactions` accepts `type=saving`, `type_counts` gains `saving`, and the summary gains `saving_net`, which is `savings_change(...)["total"]` for the window (the function Beranda uses). It is set only for `type=saving` without other row filters. Fallback: a separate backend change merged before this phase.
- **D28 (filter content beyond the owner's list) — decided 2026-11-06, default taken:** the filter lists every part of Beranda's figure: counted moves, counted top-ups, expenses paid from a savings account and Tabungan-pillar expenses (`belanja`, `pilar_tabungan`). The rows add up to the figure, and each shows its own label.
- **D2 (scope):** "Rutin" leaves Transaksi's "…" menu, as in the prototype. Plans stay reachable from Target & Tagihan, the bell and Beranda's Terjadwal. Fallback if rejected: a third menu item "Rencana".
- **D8 (scope, filter behaviour):** the four single-select chips (Menunggu jawaban, Belum dicek, Dari HP, Lewat Catat) become independent controls in the Filter dialog: Status, Sumber and a "Menunggu jawaban" check. Combinations the URL and API already accept become reachable from the interface. Fallback: one single-select "Tampilkan" segment with the four chips' options.
- **D8 (scope, differs from the plan text):** the plan lists "the period inside Filter" as removed. The prototype's Filter dialog keeps "Periode", and below 1200 px, where the rail is hidden, it is the only period-mode control. The default keeps it in the dialog at every width. Fallback: only below 1200 px.
- **D11 (correction of the plan):** the plan expects a "Kemungkinan duplikat" badge on rows. Rows carry no duplicate flag today, only the `dup=1` filter, so no badge is shown and the filter stays a chip. A row flag needs a backend change, which this phase does not make.
- **D14 (correction of the plan, money behaviour):** the plan lists "delete with Urungkan" as unchanged. Delete has no undo today: it asks first, names its balance, goal and debt effects, and says it cannot be undone. The prototype's undo toast is not built, because restoring a deleted row is a new money operation.
- **D4 (money presentation, rejected alternative):** the prototype's hero sums every expense, and its "Bersih" is income less that sum. The app keeps the server's `spending` (without saving-pillar expenses, net of reimbursements) and `spending_net`, as today and as on Beranda. The rejected option is recorded so it is not reopened by accident.

## Deviations from the prototype

Each is a decision in `design.md` and is captured for the owner's end review:
1. From 1024 to 1199 px the list takes the full width with a type control above it, instead of the 200 px rail (D1). With the rail, the one-line row needs 504 px (528 px with deviation 17) plus its title and overflows a 456 to 632 px column.
2. The hero keeps one muted line when it carries a qualifier, such as "sudah dikurangi penggantian Rp 250.000" or "tanpa Rp 400.000 yang ditabung" (D4). The prototype's hero has no line.
3. The loading state shows skeleton figures, because the figures arrive with the rows in one response (D13). The prototype shows real figures while its rows load.
4. The period stepper keeps its "Siklus ini" / "Bulan ini" reset after it has stepped back (D3). The prototype has none, and `monthly-navigation-stepper` requires it.
5. The Filter dialog uses one select for Rekening, Kategori and Pilar, because the URL and API filter by one account, one category and one pillar (D8). The prototype uses checkboxes.
6. The merge bar stays in view under the top bar while selecting (D9). The prototype's bar is inline and scrolls away.
7. The pillar column keeps "Uang masuk" for income, as on Beranda, and names a transfer's or top-up's saving direction (owner feedback 3, D11, D27). The prototype says "Tidak dihitung" for all three.
8. The delete confirmation keeps the list of effects on balances, goals and debts in the dialog body (D14). The prototype has one sentence.
9. The source labels keep today's provenance wording ("Notifikasi bank", "Diketik di ponsel"). The prototype says "Ditangkap dari HP" (D14).
10. A movement's "Status pasangan" says "Lengkap" or "Pasangan belum ditemukan" (D14). The prototype's "tidak dihitung sebagai pengeluaran" is a money claim that the dialog does not compute.
11. A top-up's title is "Top up {product}" on Transaksi and Beranda, which share the row helper, and "Top up Investasi" when the product is not known (D11).
12. The phone rows keep the long-press that starts a merge, besides "…" › "Gabungkan dua transaksi" (D9).
13. The transaction dialog is 560 px as in `dialogs.js`, not the 480 px the plan names (D14).
14. **Owner-directed:** expense amounts are red (`--out-fg`) on Transaksi, in the dialog and on Beranda (D29). The prototype and phase 3 use the ink.
15. **Owner-directed:** a transfer is titled by its note, with the route as meta (D11). The prototype titles it by the route.
16. **Owner-directed:** the rail and the type control have a sixth type, "Tabungan & investasi", whose hero is "Ditabung & investasi" (D4, D28). The prototype has five types.
17. The pillar column is 136 px instead of 112, on Transaksi and Beranda, so "Diambil dari tabungan" and "Taken from savings" fit on one line (D11).

## What Changes

- **Title block:** the subtitle from phase 2, the restyled shared period stepper (SPEC `.stepper`, D3), and one "…" menu with "Gabungkan dua transaksi" ("Batal gabungkan" while selecting) and "Filter transaksi" (D2).
- **Hero:** "Pengeluaran" (`spending`) in Manrope 44, with "· semua waktu" or "· {range}" in the label outside a period, and the supporting figures "Masuk" (`inflow`, "+", income green) and "Bersih · masuk − pengeluaran" (`spending_net`, sign shown, ink) beside it (D4).
- **Layout:** `200px minmax(0,1fr)`, gap 32, from 1200 px with the sticky rail; one column with the type control from 768 to 1199 px; the phone layout below 768 px. All switches are viewport media queries, which replace the list-width measurement (D1).
- **Rail** (phase 4 `Rail`): JENIS with `type_counts` (six types, including "Tabungan & investasi", from one list that can take "Kredit" later), a rule, PERIODE. The chosen item has the `--s1` fill and the 2 × 20 px bar (D5). Below 1200 px the prototype's `.tx-types` segmented control with counts (D6).
- **Toolbar:** the search field (`LEDGER_SEARCH_ID`, 300 ms delay as today) and the secondary "Filter" button with " · N"; the active filters, including the search text, as SPEC chips with "Hapus filter …" buttons (D7).
- **Filter dialog** (640 px, `ShellDialog`): Rekening, Kategori, Pilar, Status, Sumber, "Menunggu jawaban", "Kemungkinan duplikat" while set, and Periode; "Atur ulang", "Batal" and "Terapkan", which writes the URL once (D8).
- **Merge:** the selection mode from "…" or a long-press on the phone, a 16 px checkbox per row with the whole row as its label, and the SPEC selection bar "{n} dari 2 dipilih · Batal gabungkan · Gabungkan jadi pindah saldo". The `POST /movements/merge` request, its checks and the "Nominal tidak sama" dialog are unchanged (D9).
- **Day groups:** the shared `DayHeading`, sticky, "KAMIS, 5 NOVEMBER · HARI INI", with muted totals "Masuk … · Pengeluaran … · Pindah saldo … · N menunggu jawaban" (warning). They come from the server's `daily_totals` as today (D10).
- **Rows:** the shared `ListRow` in its full variant (time 40 · title and meta · category 168 · pillar 136 · amount ≥ 120), with the amount variants (expense red by owner feedback), transfers titled by their note with the route as meta, the saving direction in the pillar column, `MaskedMoney`, the selected row on `--surface-2` with the 2 px bar, and the phone's two lines. `ListRow` gains a selectable mode for the merge (D11, D22).
- **Loading more:** "N lagi di hari ini · Muat lagi" (quiet, centered), "Muat 50 lagi" (secondary), "N transaksi · akhir daftar" (D12).
- **States:** skeleton rows and figures with `aria-busy`, the SPEC error box with "Coba lagi", and two empty states: "Belum ada transaksi" with "Catat transaksi", and "Tidak ada transaksi yang cocok" with "Hapus semua filter" (D13).
- **Transaction dialog** (560 px): replaces the drawer for Transaksi, Beranda, Kalender and Peta Aliran. It holds the amount, the SPEC label and value list per kind (including the transfer fee and "Biaya untuk" links from the fields the API already sends), the original text, the Tinjau box, and a footer with "Hapus" and "Ubah". Delete confirms in the footer with its effects listed (D14).
- **"Ubah transaksi"** (600 px): the edit form moved out of the drawer into its own dialog, with the Jenis and Pilar segmented controls and one-line hints instead of info icons. A transfer row opens it in movement mode, with Dari, Ke, Nominal, Tanggal, Catatan and "Pisahkan jadi dua transaksi". This replaces `InternalMovementModal`. A top-up still opens `InvestmentTopupModal`, which phase 8 owns (D15, D16).
- **"Catat sebagai top up investasi"** (520 px): `ExpenseTopupConversionModal` moves onto `ShellDialog`, with the same request (D17).
- **Backend, additive and read-only (flagged, D27, D28):** the row field `saving_flow`, the filter value `type=saving`, `type_counts.saving` and the summary field `saving_net`, from one shared savings-classification helper that `get_kakeibo_breakdown` also uses. No schema, migration, write path or existing figure changes.
- **Expense colour (D29):** one shared amount component paints "−Rp" with `--out-fg` on Transaksi, the dialog, Beranda's Aktivitas terbaru and Perlu tindakan, and the Rekening detail; `tokens.test.ts` pins `--out-fg` at ≥ 4.5:1 on the canvas, every surface and the dialog in both themes.
- **Keys and URL:** J, K, Enter and Z as today, with rows found by `data-row`; every URL parameter and history entry as today (D18, D19).
- **Language:**
  - a `transactions` namespace in `messages/en.json` and `id.json` (copy in design Appendix A);
  - the movement and merge error codes in `errors`;
  - dates, times, amounts and ranges through `lib/format.ts`;
  - `/ledger` added to the translated routes;
  - the roots of the page added to the copy register's translated roots (D20).
- **Removed** (each with its destination in D23):
  - `KpiCard` and `.ws-kcards`/`.ws-kcard*`, whose last user was Transaksi;
  - `LedgerTable`, `LedgerFeed`, both `KIND_PILL` maps, `RowAvatar` and `.ws-ava`;
  - `FilterPopover` and the chip helpers `LEDGER_CHIPS`, `chipOf` and `withChip`;
  - the "Daftar transaksi" visible heading line;
  - the Drawer use of `TransactionDrawer` and the inline edit mode;
  - `InternalMovementModal` and its test;
  - the dead `ReviewStatusMark`, `HabitMark` and `feeLinkText` exports of `components/ledger/LedgerRowMarks.tsx` and the file itself (the `FeeLinkFields` type moves to `components/transactions/types.ts`);
  - the Transaksi-only `ws-tx-*` rules;
  - the `useWidthAtMost` call on Transaksi;
  - "Rutin" in the menu (flagged).
- **BREAKING (spec):** `transaction-ledger-management` "Searchable & Filterable Ledger View", "Transaction Detail & Inline Editing", "Consolidated Internal Movement Ledger Representation", "Ledger Table Columns", "Loading More Ledger Rows", "Ledger Type Switch", "Confirming a Row With Undo", "Transaksi Keys" and "Return to the Tinjau Item" change their presentation. "Ledger Day Heading Colours" is removed: the day totals become muted. "Ledger Day Totals and Type Counts" and "Ledger Type and Waiting Filters" gain the additive `saving` count and filter. `mobile-native-layout`, `monthly-navigation-stepper`, `tactile-pulse-ui`, `transaction-provenance` and `beranda-cycle-summary` change the requirements that describe Transaksi's rows, colours or dialog.

## Non-goals

- No change to any existing API field, request, figure, count, money rule, schema, data or the Android companion. The only backend work is the additive, read-only `saving_flow`, `type=saving`, `type_counts.saving` and `saving_net` (D27, D28), with the savings predicates moved into one shared helper and Beranda's figures proven unchanged. `db/` and `mobile/` are untouched, and there is no migration. Server text, such as error messages, is shown as `localize-server-text` sends it.
- No change to what Beranda counts as "Ditabung & investasi": the row labels follow the server's rule, including the three cases where it differs from the owner's wording (flagged, D27).
- No credit spending flow (`add-credit-spending-flow` is a separate change, D30), no second role per account and no tags (the owner rejected them on 2026-11-06).
- No change to what a filter matches, how rows are paged, grouped or consolidated (`consolidateLedgerMovements`, `listsLogicalMovements`, `groupByDay`), or what an action sends.
- No undo for delete, no row-level duplicate flag and no receipt preview, because the API has none and adding them is backend work.
- No other page body. Catat and its Perpindahan are phase 11. `InvestmentTopupModal` and the Rekening detail are phase 8. Analisis and Laporan adopt the restyled stepper as it is, and their pages wait for phases 6 and 7.
- No new npm dependency.
- Local verification only, with the parity seed, the audit seed and the stress and USD seed in disposable databases. Release and production stay with the owner through the approved GitHub workflow.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `transaction-ledger-management`:
  - the page layout, hero, rail, toolbar, filter dialog, rows, day headings, loading more and states;
  - the transaction dialog, the edit dialog and the movement edit;
  - the merge selection and its bar;
  - the keys and the return to Tinjau, worded for the dialog.
  - Removed: "Ledger Day Heading Colours". Added: "Transaksi Hero Figures", "Transaksi Type and Period Rail", "Ledger Filter Dialog", "Ledger Day Headings", "Ledger Loading, Failure and Empty States" and "Transaksi in Both Languages".
- `mobile-native-layout`: "Mobile Date-Grouped Transaction Ledger" as the prototype's phone composition.
- `monthly-navigation-stepper`: the stepper sits in the title block in the SPEC look and keeps its reset.
- `tactile-pulse-ui`: expense amounts in the expense red everywhere they carry "−", by owner feedback (replacing the ink rule of phase 3); Transaksi leaves the "not yet redesigned" examples of the icon-tile rule.
- `beranda-cycle-summary`: "Recent Activity on Beranda" reads the expense in red, and a transfer by its note, route and saving direction instead of "Tidak dihitung".
- `transaction-ledger-management` also gains "Ledger Saving Flow" (the row field) and the `saving` filter, count and `saving_net` in its API requirements.
- `transaction-provenance`: "Provenance in the Ledger Edit Dialog" names the dialog's labels and the bulk accept that "Tandai sudah dicek" sends today.

`interface-language` and `ui-copy-register` (from `add-language-system`) need no delta. Adding `/ledger` to the translated routes follows their rule that the set of untranslated pages only shrinks.

Standards:
- UI and accessibility (WCAG 2.2 AA): landmarks and headings, focus order and focus return through every dialog and the phone sheet, focus not obscured by the sticky top bar, day headings or the selection bar, target size on the phone, reflow at 320 px, non-colour cues on amounts (the sign and the kind's words), reduced motion, 3.1.1 and 3.1.2 language of page and parts;
- the copy register in both languages (`ui-copy-register`);
- `money`: figures identical before and after, and in English and Indonesian; "Tabungan & investasi" for the cycle equal to Beranda's "Ditabung & investasi"; the shared savings helper proven by the existing Kakeibo and cycle-summary tests;
- `api-consumers`: additive fields and one additive filter value, announced in the delta; the companion does not read them;
- `database`: no migration; the backend suite runs on migrated disposable PostgreSQL because the backend changes;
- `personal-data`: no rendered text or user data in logs.

No capability in `AGENTS.md` changes.

## Impact

- **Frontend Transaksi:**
  - `app/ledger/page.tsx` (split into the page, `LedgerHero`, `LedgerRail`, `LedgerToolbar`, `LedgerList` and the merge bar) and `app/ledger/page.test.tsx`;
  - `components/transactions/LedgerList.tsx` (rewritten on `ListRow` and `DayHeading`), `TransactionDrawer.tsx` (becomes `TransactionDialog.tsx`), `TransactionEditForm.tsx` (into `TransactionEditDialog.tsx`), new `MovementEditFields.tsx` and `LedgerFilterDialog.tsx`, `types.ts` and `useTransactionUndo.ts` (messages to keys);
  - `components/ui/ExpenseTopupConversionModal.tsx` (onto `ShellDialog`), `components/ui/InternalMovementModal.tsx` and its test (removed), `components/ui/PeriodStepper.tsx` (restyled);
  - `lib/ledgerFilters.ts` (chip helpers removed; parsing, URL and API params unchanged), `lib/ledgerDays.ts` (labels to the formatter), `lib/movementFormSchema.ts` (messages to keys);
  - `components/beranda/activity.tsx` (the shared row helpers move to `components/transactions/rows.tsx`: note-first transfer titles, `rowCategory`, the saving-direction labels; `RowAvatar` removed), `components/beranda/TodoSection.tsx` and `ActivitySection.tsx` (the shared red amount), and the callers of the drawer in `BerandaView.tsx`, `KalenderTab.tsx` and `PetaAliranTab.tsx` (renamed import only);
  - `components/ledger/LedgerRowMarks.tsx` (removed).
- **Shared:**
  - `components/workspace/lists.tsx` (`ListRow` selectable mode), `figures.tsx` (`HeroFigure` `aside` and optional `line`; `FigureBlock` without the bar slot), `primitives.tsx` (`KpiCard` removed);
  - `app/globals.css`: `.tx`, `.tx-rail`, `.tx-types`, `.toolbar`, `.chips`, `.chip`, `.sel-bar`, `.stepper`, `.dl`, `.box`, `.more-row` and the hero split from `styles.css`; the Transaksi-only `ws-*` rules removed;
  - `app/komponen/ComponentSheet.tsx` (the chip sample on `.chip`).
- **Language:** `messages/en.json` and `id.json` (`transactions`, `errors` codes, the dead `common.rowMarks.needsReview` and `automatic` keys removed), `lib/i18n/translatedRoutes.ts` (`/ledger`), the copy register roots, and the untranslated-page test of `add-language-system`, which moves its example from Transaksi to Analisis (`/insights`).
- **Backend (additive, read-only, flagged):**
  - new `backend/app/services/saving_flow.py`: the savings-destination, product and top-up predicates now in `routers/dashboard.py`, as one SQL helper;
  - `routers/dashboard.py` `get_kakeibo_breakdown` reads the helper (no behaviour change);
  - `routers/transactions.py`: `saving_flow` on list and detail rows, `type=saving` in `TYPE_FILTER_SQL`, `saving` in `TAB_KEYS` for `type_counts`, and `saving_net` in the summary;
  - tests in `backend/tests/` for each `saving_flow` value, the filter, the count, and `saving_net` equal to `cycle_summary` `tabungan.total` for the cycle; the existing Kakeibo, cycle-summary and ledger tests unchanged.
- **Tooling (local only):** `scripts/verify/markas/`:
  - the Transaksi scenarios in both scenario files;
  - `transaksi-checklist.js` for `measure.js`;
  - the keyboard flow `transaksi-keys.mjs`;
  - the parity seed extended with a fee-linked transfer, a top-up, a reimbursement offset, a 60-row day and one transfer per saving direction;
  - the stress and USD seed for Transaksi.
- **No change** to `db/`, `mobile/` or `nginx/`. Rollback is a revert of the merge commit; the backend additions are read-only, so a rollback drops the fields without data effects.
- **Follow-up dependency:** `add-credit-spending-flow` builds on this phase: it adds the "Kredit" type to the same list and its own labels for credit purchases and settlements (D30).
